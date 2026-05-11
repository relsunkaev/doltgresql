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
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// TestSharedAdvisoryLocksCoexistAndBlockExclusiveRepro reproduces a concurrency
// correctness bug: PostgreSQL supports shared advisory locks, allowing multiple
// shared holders while blocking an exclusive holder for the same key.
func TestSharedAdvisoryLocksCoexistAndBlockExclusiveRepro(t *testing.T) {
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)
	ctx, defaultConn, controller := CreateServerWithPort(t, "postgres", port)
	t.Cleanup(func() {
		defaultConn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	dial := func(t *testing.T) *pgx.Conn {
		t.Helper()
		conn, err := pgx.Connect(ctx, fmt.Sprintf(
			"postgres://postgres:password@127.0.0.1:%d/postgres?sslmode=disable",
			port,
		))
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = conn.Close(context.Background())
		})
		return conn
	}

	first := dial(t)
	second := dial(t)
	third := dial(t)

	_, err = first.Exec(ctx, `SELECT pg_advisory_lock_shared(42);`)
	require.NoError(t, err)

	var secondShared bool
	require.NoError(t, second.QueryRow(ctx, `SELECT pg_try_advisory_lock_shared(42);`).Scan(&secondShared))
	require.True(t, secondShared)

	var exclusive bool
	require.NoError(t, third.QueryRow(ctx, `SELECT pg_try_advisory_lock(42);`).Scan(&exclusive))
	require.False(t, exclusive)

	var firstUnlocked bool
	require.NoError(t, first.QueryRow(ctx, `SELECT pg_advisory_unlock_shared(42);`).Scan(&firstUnlocked))
	require.True(t, firstUnlocked)

	var secondUnlocked bool
	require.NoError(t, second.QueryRow(ctx, `SELECT pg_advisory_unlock_shared(42);`).Scan(&secondUnlocked))
	require.True(t, secondUnlocked)

	require.NoError(t, third.QueryRow(ctx, `SELECT pg_try_advisory_lock(42);`).Scan(&exclusive))
	require.True(t, exclusive)
}
