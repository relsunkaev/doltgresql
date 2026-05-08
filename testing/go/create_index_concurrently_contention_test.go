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
	"time"

	gms "github.com/dolthub/go-mysql-server/sql"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	pgnodes "github.com/dolthub/doltgresql/server/node"
)

// TestCreateIndexConcurrentlyCrossSessionVisibility is the cross-session
// evidence that PostgreSQL's CONCURRENTLY state machine is visible to
// other sessions: while session A is mid-build, session B querying
// pg_index must see the index as (indisready=false, indisvalid=false),
// and once A's CONCURRENTLY completes, B must see (true, true).
//
// The state machine's window is normally synchronous (Phase 1 build
// then Phase 2 flip back-to-back), so we use a test-only hook that
// pauses session A between phases on a channel. This makes the
// in-progress state deterministic without a sleep race.
func TestCreateIndexConcurrentlyCrossSessionVisibility(t *testing.T) {
	port, err := gms.GetEmptyPort()
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
			"postgres://postgres:password@127.0.0.1:%d/postgres?sslmode=disable", port))
		require.NoError(t, err)
		return conn
	}

	// Set up a table that will get an index created CONCURRENTLY.
	setup := dial(t)
	defer setup.Close(ctx)
	_, err = setup.Exec(ctx, "CREATE TABLE cross_t (id INT PRIMARY KEY, v INT)")
	require.NoError(t, err)
	_, err = setup.Exec(ctx, "INSERT INTO cross_t VALUES (1, 10), (2, 20)")
	require.NoError(t, err)

	// The pause/resume channels coordinate the test:
	//   - paused: session A signals it is between Phase 1 and Phase 2
	//   - resume: the test releases A so it can finish Phase 2
	paused := make(chan struct{})
	resume := make(chan struct{})
	pgnodes.SetTestHookBetweenPhases(func(_ *gms.Context) {
		close(paused)
		<-resume
	})
	t.Cleanup(func() { pgnodes.SetTestHookBetweenPhases(nil) })

	// Session A: CREATE INDEX CONCURRENTLY runs in its own goroutine
	// because the hook will block it mid-flight.
	sessionA := dial(t)
	defer sessionA.Close(ctx)
	createDone := make(chan error, 1)
	go func() {
		_, execErr := sessionA.Exec(ctx, "CREATE INDEX CONCURRENTLY cross_t_v_idx ON cross_t (v)")
		createDone <- execErr
	}()

	// Wait for Phase 1 to finish and the hook to pause.
	select {
	case <-paused:
	case <-time.After(15 * time.Second):
		t.Fatal("CREATE INDEX CONCURRENTLY never reached the inter-phase hook")
	}

	// Session B observes the in-progress state via pg_index.
	sessionB := dial(t)
	defer sessionB.Close(ctx)
	var indisready, indisvalid bool
	row := sessionB.QueryRow(ctx, `
		SELECT i.indisready, i.indisvalid
		FROM pg_catalog.pg_index i
		JOIN pg_catalog.pg_class c ON i.indexrelid = c.oid
		WHERE c.relname = 'cross_t_v_idx'`)
	require.NoError(t, row.Scan(&indisready, &indisvalid))
	require.False(t, indisready, "during CONCURRENTLY build, indisready must be false")
	require.False(t, indisvalid, "during CONCURRENTLY build, indisvalid must be false")

	// Release session A; it finishes Phase 2.
	close(resume)
	select {
	case execErr := <-createDone:
		require.NoError(t, execErr)
	case <-time.After(15 * time.Second):
		t.Fatal("CREATE INDEX CONCURRENTLY never finished Phase 2")
	}

	// Session B re-queries: bits must now be (true, true).
	row = sessionB.QueryRow(ctx, `
		SELECT i.indisready, i.indisvalid
		FROM pg_catalog.pg_index i
		JOIN pg_catalog.pg_class c ON i.indexrelid = c.oid
		WHERE c.relname = 'cross_t_v_idx'`)
	require.NoError(t, row.Scan(&indisready, &indisvalid))
	require.True(t, indisready, "post-CONCURRENTLY indisready must flip to true")
	require.True(t, indisvalid, "post-CONCURRENTLY indisvalid must flip to true")

	_ = context.Background
}
