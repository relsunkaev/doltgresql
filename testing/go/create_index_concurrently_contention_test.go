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
	"strings"
	"sync"
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

func TestCreateIndexConcurrentlyMetadataBackedCrossSessionVisibility(t *testing.T) {
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

	setup := dial(t)
	defer setup.Close(ctx)
	_, err = setup.Exec(ctx, "CREATE TABLE include_t (id INT PRIMARY KEY, tenant_id INT, amount INT)")
	require.NoError(t, err)
	_, err = setup.Exec(ctx, "INSERT INTO include_t VALUES (1, 10, 100), (2, 20, 200)")
	require.NoError(t, err)
	_, err = setup.Exec(ctx, "CREATE TABLE partial_t (id INT PRIMARY KEY, tenant_id INT, active BOOL)")
	require.NoError(t, err)
	_, err = setup.Exec(ctx, "INSERT INTO partial_t VALUES (1, 10, true), (2, 20, false)")
	require.NoError(t, err)
	_, err = setup.Exec(ctx, "CREATE TABLE partial_unique_t (id INT PRIMARY KEY, tenant_id INT, active BOOL)")
	require.NoError(t, err)
	_, err = setup.Exec(ctx, "INSERT INTO partial_unique_t VALUES (1, 10, true), (2, 10, false)")
	require.NoError(t, err)
	_, err = setup.Exec(ctx, "CREATE TABLE expr_t (id INT PRIMARY KEY, email TEXT)")
	require.NoError(t, err)
	_, err = setup.Exec(ctx, "INSERT INTO expr_t VALUES (1, 'Alice@X'), (2, 'bob@x')")
	require.NoError(t, err)
	_, err = setup.Exec(ctx, "CREATE TABLE unique_expr_t (id INT PRIMARY KEY, email TEXT)")
	require.NoError(t, err)
	_, err = setup.Exec(ctx, "INSERT INTO unique_expr_t VALUES (1, 'Alice@X'), (2, 'bob@x')")
	require.NoError(t, err)
	_, err = setup.Exec(ctx, "CREATE TABLE gin_t (id INT PRIMARY KEY, doc JSONB)")
	require.NoError(t, err)
	_, err = setup.Exec(ctx, `INSERT INTO gin_t VALUES (1, '{"kind":"click"}'), (2, '{"kind":"view"}')`)
	require.NoError(t, err)

	assertConcurrentIndexCrossSessionVisibility(
		t,
		ctx,
		dial,
		"CREATE INDEX CONCURRENTLY include_t_tenant_idx ON include_t (tenant_id) INCLUDE (amount)",
		"include_t_tenant_idx",
	)
	assertConcurrentIndexCrossSessionVisibility(
		t,
		ctx,
		dial,
		"CREATE INDEX CONCURRENTLY partial_t_tenant_active_idx ON partial_t (tenant_id) WHERE active",
		"partial_t_tenant_active_idx",
	)
	assertConcurrentIndexCrossSessionVisibility(
		t,
		ctx,
		dial,
		"CREATE UNIQUE INDEX CONCURRENTLY partial_unique_t_tenant_active_idx ON partial_unique_t (tenant_id) WHERE active",
		"partial_unique_t_tenant_active_idx",
	)
	assertConcurrentIndexCrossSessionVisibility(
		t,
		ctx,
		dial,
		"CREATE INDEX CONCURRENTLY expr_t_lower_email_idx ON expr_t ((lower(email)))",
		"expr_t_lower_email_idx",
	)
	assertConcurrentIndexCrossSessionVisibility(
		t,
		ctx,
		dial,
		"CREATE UNIQUE INDEX CONCURRENTLY unique_expr_t_lower_email_idx ON unique_expr_t ((lower(email)))",
		"unique_expr_t_lower_email_idx",
	)
	_, err = setup.Exec(ctx, "INSERT INTO unique_expr_t VALUES (3, 'ALICE@x')")
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate")
	_, err = setup.Exec(ctx, "INSERT INTO unique_expr_t VALUES (3, 'carol@x')")
	require.NoError(t, err)
	assertConcurrentIndexCrossSessionVisibility(
		t,
		ctx,
		dial,
		"CREATE INDEX CONCURRENTLY gin_t_doc_idx ON gin_t USING gin (doc jsonb_path_ops)",
		"gin_t_doc_idx",
	)
}

func TestCreateIndexConcurrentlyAllowsWritersDuringPhase1(t *testing.T) {
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

	setup := dial(t)
	defer setup.Close(ctx)
	_, err = setup.Exec(ctx, "CREATE TABLE nonblock_t (id INT PRIMARY KEY, v INT)")
	require.NoError(t, err)
	_, err = setup.Exec(ctx, "INSERT INTO nonblock_t VALUES (1, 10), (2, 20)")
	require.NoError(t, err)

	paused := make(chan struct{})
	resume := make(chan struct{})
	setCreationTestHookBeforeBuildSecondaryIndex(t, func(_ *gms.Context) {
		close(paused)
		<-resume
	})
	var resumeOnce sync.Once
	releaseBuild := func() {
		resumeOnce.Do(func() { close(resume) })
	}
	defer releaseBuild()

	sessionA := dial(t)
	defer sessionA.Close(ctx)
	createDone := make(chan error, 1)
	go func() {
		_, execErr := sessionA.Exec(ctx, "CREATE UNIQUE INDEX CONCURRENTLY nonblock_t_v_idx ON nonblock_t (v)")
		createDone <- execErr
	}()

	select {
	case <-paused:
	case execErr := <-createDone:
		require.NoError(t, execErr)
		t.Fatal("CREATE INDEX CONCURRENTLY completed before reaching the Phase 1 build hook")
	case <-time.After(15 * time.Second):
		t.Fatal("CREATE INDEX CONCURRENTLY never reached the Phase 1 build hook")
	}

	sessionB := dial(t)
	defer sessionB.Close(ctx)
	writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err = sessionB.Exec(writeCtx, "INSERT INTO nonblock_t VALUES (3, 30)")
	require.NoError(t, err, "writer should not block while CREATE INDEX CONCURRENTLY is in Phase 1")
	_, err = sessionB.Exec(writeCtx, "UPDATE nonblock_t SET v = 25 WHERE id = 2")
	require.NoError(t, err, "updater should not block while CREATE INDEX CONCURRENTLY is in Phase 1")
	_, err = sessionB.Exec(writeCtx, "DELETE FROM nonblock_t WHERE id = 1")
	require.NoError(t, err, "deleter should not block while CREATE INDEX CONCURRENTLY is in Phase 1")

	releaseBuild()
	select {
	case execErr := <-createDone:
		require.NoError(t, execErr)
	case <-time.After(15 * time.Second):
		t.Fatal("CREATE INDEX CONCURRENTLY never finished after releasing the Phase 1 build hook")
	}

	var id int
	require.NoError(t, sessionB.QueryRow(ctx, "SELECT id FROM nonblock_t WHERE v = 30").Scan(&id))
	require.Equal(t, 3, id)
	require.NoError(t, sessionB.QueryRow(ctx, "SELECT id FROM nonblock_t WHERE v = 25").Scan(&id))
	require.Equal(t, 2, id)
	var count int
	require.NoError(t, sessionB.QueryRow(ctx, "SELECT count(*) FROM nonblock_t WHERE v = 10").Scan(&count))
	require.Equal(t, 0, count)

	rows, err := sessionB.Query(ctx, "EXPLAIN SELECT id FROM nonblock_t WHERE v = 30")
	require.NoError(t, err)
	defer rows.Close()
	var plan strings.Builder
	for rows.Next() {
		var line string
		require.NoError(t, rows.Scan(&line))
		plan.WriteString(line)
		plan.WriteByte('\n')
	}
	require.NoError(t, rows.Err())
	require.Contains(t, plan.String(), "IndexedTableAccess")
}

func assertConcurrentIndexCrossSessionVisibility(
	t *testing.T,
	ctx context.Context,
	dial func(*testing.T) *pgx.Conn,
	createStatement string,
	indexName string,
) {
	t.Helper()

	paused := make(chan struct{})
	resume := make(chan struct{})
	pgnodes.SetTestHookBetweenPhases(func(_ *gms.Context) {
		close(paused)
		<-resume
	})
	defer pgnodes.SetTestHookBetweenPhases(nil)

	sessionA := dial(t)
	defer sessionA.Close(ctx)
	createDone := make(chan error, 1)
	go func() {
		_, execErr := sessionA.Exec(ctx, createStatement)
		createDone <- execErr
	}()

	select {
	case <-paused:
	case execErr := <-createDone:
		require.NoError(t, execErr)
		t.Fatalf("%s completed without reaching the CONCURRENTLY inter-phase hook", createStatement)
	case <-time.After(15 * time.Second):
		t.Fatalf("%s never reached the inter-phase hook", createStatement)
	}

	sessionB := dial(t)
	defer sessionB.Close(ctx)
	indisready, indisvalid := indexReadiness(t, ctx, sessionB, indexName)
	require.False(t, indisready, "during CONCURRENTLY build, indisready must be false for %s", indexName)
	require.False(t, indisvalid, "during CONCURRENTLY build, indisvalid must be false for %s", indexName)

	close(resume)
	select {
	case execErr := <-createDone:
		require.NoError(t, execErr)
	case <-time.After(15 * time.Second):
		t.Fatalf("%s never finished Phase 2", createStatement)
	}

	indisready, indisvalid = indexReadiness(t, ctx, sessionB, indexName)
	require.True(t, indisready, "post-CONCURRENTLY indisready must flip to true for %s", indexName)
	require.True(t, indisvalid, "post-CONCURRENTLY indisvalid must flip to true for %s", indexName)
}

func indexReadiness(t *testing.T, ctx context.Context, conn *pgx.Conn, indexName string) (bool, bool) {
	t.Helper()
	var indisready, indisvalid bool
	row := conn.QueryRow(ctx, `
		SELECT i.indisready, i.indisvalid
		FROM pg_catalog.pg_index i
		JOIN pg_catalog.pg_class c ON i.indexrelid = c.oid
		WHERE c.relname = $1`, indexName)
	require.NoError(t, row.Scan(&indisready, &indisvalid))
	return indisready, indisvalid
}
