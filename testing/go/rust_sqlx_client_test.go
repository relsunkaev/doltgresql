// Copyright 2026 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package _go

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	gms "github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
)

// TestRustSQLxClientSmoke runs the real Rust sqlx PostgreSQL client
// fixture against Doltgres. It pins the async pool path, startup
// connection, parameter binding, EXISTS query shape, UUID binding and
// decoding, and chrono timestamptz/date decoding used by Rust apps.
func TestRustSQLxClientSmoke(t *testing.T) {
	cargo, err := exec.LookPath("cargo")
	if err != nil {
		t.Skip("cargo not on PATH; install Rust to enable this harness")
	}
	if testing.Short() {
		t.Skip("rust sqlx harness builds dependencies; skipped under -short")
	}

	port, err := gms.GetEmptyPort()
	require.NoError(t, err)
	ctx, defaultConn, controller := CreateServerWithPort(t, "postgres", port)
	t.Cleanup(func() {
		defaultConn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})
	_, err = defaultConn.Exec(ctx, "CREATE TABLE test_table (pk INT PRIMARY KEY);")
	require.NoError(t, err)

	repoRoot, err := os.Getwd()
	require.NoError(t, err)
	manifest := filepath.Join(repoRoot, "..", "postgres-client-tests", "rust", "Cargo.toml")
	targetDir := filepath.Join(t.TempDir(), "target")
	cmdCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	cmd := exec.CommandContext(cmdCtx, cargo, "run", "--quiet", "--manifest-path", manifest, "--", "postgres", fmt.Sprint(port))
	cmd.Env = append(os.Environ(),
		"CARGO_TARGET_DIR="+targetDir,
		"NO_COLOR=1",
	)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "rust sqlx probe failed: %s", string(out))
	require.Contains(t, string(out), "exists=true")
}
