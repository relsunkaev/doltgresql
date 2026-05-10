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
	"errors"
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

func TestRefreshMaterializedViewConcurrentlyReadsOldSnapshotDuringBuild(t *testing.T) {
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
	_, err = setup.Exec(ctx, "CREATE TABLE mv_source (id INT PRIMARY KEY, v INT)")
	require.NoError(t, err)
	_, err = setup.Exec(ctx, "INSERT INTO mv_source VALUES (1, 10), (2, 20)")
	require.NoError(t, err)
	_, err = setup.Exec(ctx, "CREATE MATERIALIZED VIEW mv_snapshot (account_id, amount) AS SELECT id, v FROM mv_source")
	require.NoError(t, err)
	_, err = setup.Exec(ctx, "CREATE UNIQUE INDEX mv_snapshot_account_id_idx ON mv_snapshot (account_id)")
	require.NoError(t, err)
	_, err = setup.Exec(ctx, "CREATE INDEX mv_snapshot_amount_idx ON mv_snapshot (amount)")
	require.NoError(t, err)
	_, err = setup.Exec(ctx, "UPDATE mv_source SET v = 200 WHERE id = 2")
	require.NoError(t, err)
	_, err = setup.Exec(ctx, "INSERT INTO mv_source VALUES (3, 30)")
	require.NoError(t, err)

	paused := make(chan struct{})
	resume := make(chan struct{})
	pgnodes.SetTestHookAfterConcurrentRefreshBuild(func(_ *gms.Context) {
		close(paused)
		<-resume
	})
	t.Cleanup(func() { pgnodes.SetTestHookAfterConcurrentRefreshBuild(nil) })

	var resumeOnce sync.Once
	releaseRefresh := func() {
		resumeOnce.Do(func() { close(resume) })
	}
	defer releaseRefresh()

	sessionA := dial(t)
	defer sessionA.Close(ctx)
	refreshDone := make(chan error, 1)
	go func() {
		_, execErr := sessionA.Exec(ctx, "REFRESH MATERIALIZED VIEW CONCURRENTLY mv_snapshot")
		refreshDone <- execErr
	}()

	select {
	case <-paused:
	case execErr := <-refreshDone:
		require.NoError(t, execErr)
		t.Fatal("REFRESH MATERIALIZED VIEW CONCURRENTLY completed before reaching the staged-build hook")
	case <-time.After(15 * time.Second):
		t.Fatal("REFRESH MATERIALIZED VIEW CONCURRENTLY never reached the staged-build hook")
	}

	sessionB := dial(t)
	defer sessionB.Close(ctx)
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	rows, err := sessionB.Query(readCtx, "SELECT account_id, amount FROM mv_snapshot ORDER BY account_id")
	require.NoError(t, err, "reader should not block while CONCURRENTLY builds replacement rows")
	var oldRows [][2]int
	for rows.Next() {
		var accountID, amount int
		require.NoError(t, rows.Scan(&accountID, &amount))
		oldRows = append(oldRows, [2]int{accountID, amount})
	}
	require.NoError(t, rows.Err())
	rows.Close()
	require.Equal(t, [][2]int{{1, 10}, {2, 20}}, oldRows)

	releaseRefresh()
	select {
	case execErr := <-refreshDone:
		require.NoError(t, execErr)
	case <-time.After(15 * time.Second):
		t.Fatal("REFRESH MATERIALIZED VIEW CONCURRENTLY never finished after releasing the staged-build hook")
	}

	rows, err = sessionB.Query(ctx, "SELECT account_id, amount FROM mv_snapshot ORDER BY account_id")
	require.NoError(t, err)
	var refreshedRows [][2]int
	for rows.Next() {
		var accountID, amount int
		require.NoError(t, rows.Scan(&accountID, &amount))
		refreshedRows = append(refreshedRows, [2]int{accountID, amount})
	}
	require.NoError(t, rows.Err())
	rows.Close()
	require.Equal(t, [][2]int{{1, 10}, {2, 200}, {3, 30}}, refreshedRows)

	indexRows, err := sessionB.Query(ctx, `
		SELECT indexname, indexdef
		FROM pg_indexes
		WHERE tablename = 'mv_snapshot'
		ORDER BY indexname`)
	require.NoError(t, err)
	var indexes [][2]string
	for indexRows.Next() {
		var name, def string
		require.NoError(t, indexRows.Scan(&name, &def))
		indexes = append(indexes, [2]string{name, def})
	}
	require.NoError(t, indexRows.Err())
	indexRows.Close()
	require.Equal(t, [][2]string{
		{"mv_snapshot_account_id_idx", "CREATE UNIQUE INDEX mv_snapshot_account_id_idx ON public.mv_snapshot USING btree (account_id)"},
		{"mv_snapshot_amount_idx", "CREATE INDEX mv_snapshot_amount_idx ON public.mv_snapshot USING btree (amount)"},
	}, indexes)
}

func TestRefreshMaterializedViewConcurrentlySwapFailurePreservesOldSnapshot(t *testing.T) {
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
	_, err = setup.Exec(ctx, "CREATE TABLE swap_source (id INT PRIMARY KEY, v INT)")
	require.NoError(t, err)
	_, err = setup.Exec(ctx, "INSERT INTO swap_source VALUES (1, 10), (2, 20)")
	require.NoError(t, err)
	_, err = setup.Exec(ctx, "CREATE MATERIALIZED VIEW swap_snapshot (account_id, amount) AS SELECT id, v FROM swap_source")
	require.NoError(t, err)
	_, err = setup.Exec(ctx, "CREATE UNIQUE INDEX swap_snapshot_account_id_idx ON swap_snapshot (account_id)")
	require.NoError(t, err)
	_, err = setup.Exec(ctx, "UPDATE swap_source SET v = 200 WHERE id = 2")
	require.NoError(t, err)
	_, err = setup.Exec(ctx, "INSERT INTO swap_source VALUES (3, 30)")
	require.NoError(t, err)

	pgnodes.SetTestHookBeforeConcurrentRefreshSwap(func(_ *gms.Context) error {
		return errors.New("forced concurrent refresh swap failure")
	})
	t.Cleanup(func() { pgnodes.SetTestHookBeforeConcurrentRefreshSwap(nil) })

	_, err = setup.Exec(ctx, "REFRESH MATERIALIZED VIEW CONCURRENTLY swap_snapshot")
	require.Error(t, err)
	require.Contains(t, err.Error(), "forced concurrent refresh swap failure")

	rows, err := setup.Query(ctx, "SELECT account_id, amount FROM swap_snapshot ORDER BY account_id")
	require.NoError(t, err)
	var preservedRows [][2]int
	for rows.Next() {
		var accountID, amount int
		require.NoError(t, rows.Scan(&accountID, &amount))
		preservedRows = append(preservedRows, [2]int{accountID, amount})
	}
	require.NoError(t, rows.Err())
	rows.Close()
	require.Equal(t, [][2]int{{1, 10}, {2, 20}}, preservedRows)
}

func TestRefreshMaterializedViewConcurrentlyGuardrails(t *testing.T) {
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

	conn := dial(t)
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx, "CREATE TABLE large_source (id INT PRIMARY KEY, v INT)")
	require.NoError(t, err)
	_, err = conn.Exec(ctx, "INSERT INTO large_source VALUES "+largeSourceValues(1, 512, 10))
	require.NoError(t, err)
	_, err = conn.Exec(ctx, "CREATE MATERIALIZED VIEW large_snapshot (account_id, amount) AS SELECT id, v FROM large_source")
	require.NoError(t, err)
	_, err = conn.Exec(ctx, "CREATE UNIQUE INDEX large_snapshot_account_id_idx ON large_snapshot (account_id)")
	require.NoError(t, err)
	_, err = conn.Exec(ctx, "CREATE INDEX large_snapshot_amount_idx ON large_snapshot (amount)")
	require.NoError(t, err)

	_, err = conn.Exec(ctx, "UPDATE large_source SET v = v + 1 WHERE id <= 128")
	require.NoError(t, err)
	_, err = conn.Exec(ctx, "DELETE FROM large_source WHERE id > 500")
	require.NoError(t, err)
	_, err = conn.Exec(ctx, "INSERT INTO large_source VALUES "+largeSourceValues(513, 540, 10))
	require.NoError(t, err)
	_, err = conn.Exec(ctx, "REFRESH MATERIALIZED VIEW CONCURRENTLY large_snapshot")
	require.NoError(t, err)

	var count int
	var total int64
	err = conn.QueryRow(ctx, "SELECT count(*), sum(amount)::bigint FROM large_snapshot").Scan(&count, &total)
	require.NoError(t, err)
	require.Equal(t, 528, count)
	require.Equal(t, int64(1400048), total)

	indexRows, err := conn.Query(ctx, `
		SELECT indexname
		FROM pg_indexes
		WHERE tablename = 'large_snapshot'
		ORDER BY indexname`)
	require.NoError(t, err)
	var indexNames []string
	for indexRows.Next() {
		var name string
		require.NoError(t, indexRows.Scan(&name))
		indexNames = append(indexNames, name)
	}
	require.NoError(t, indexRows.Err())
	indexRows.Close()
	require.Equal(t, []string{"large_snapshot_account_id_idx", "large_snapshot_amount_idx"}, indexNames)

	_, err = conn.Exec(ctx, "CREATE TABLE duplicate_source (id INT PRIMARY KEY, grp INT)")
	require.NoError(t, err)
	_, err = conn.Exec(ctx, "INSERT INTO duplicate_source VALUES (1, 1), (2, 2)")
	require.NoError(t, err)
	_, err = conn.Exec(ctx, "CREATE MATERIALIZED VIEW duplicate_snapshot AS SELECT grp FROM duplicate_source")
	require.NoError(t, err)
	_, err = conn.Exec(ctx, "CREATE UNIQUE INDEX duplicate_snapshot_grp_idx ON duplicate_snapshot (grp)")
	require.NoError(t, err)
	_, err = conn.Exec(ctx, "UPDATE duplicate_source SET grp = 1 WHERE id = 2")
	require.NoError(t, err)
	_, err = conn.Exec(ctx, "REFRESH MATERIALIZED VIEW CONCURRENTLY duplicate_snapshot")
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate")

	dupRows, err := conn.Query(ctx, "SELECT grp FROM duplicate_snapshot ORDER BY grp")
	require.NoError(t, err)
	var preservedGroups []int
	for dupRows.Next() {
		var grp int
		require.NoError(t, dupRows.Scan(&grp))
		preservedGroups = append(preservedGroups, grp)
	}
	require.NoError(t, dupRows.Err())
	dupRows.Close()
	require.Equal(t, []int{1, 2}, preservedGroups)

	var stagingTables int
	err = conn.QueryRow(ctx, "SELECT count(*) FROM pg_class WHERE relname LIKE '__doltgres_refresh_%'").Scan(&stagingTables)
	require.NoError(t, err)
	require.Equal(t, 0, stagingTables)
}

func largeSourceValues(start int, end int, multiplier int) string {
	var builder strings.Builder
	for id := start; id <= end; id++ {
		if id > start {
			builder.WriteString(", ")
		}
		fmt.Fprintf(&builder, "(%d, %d)", id, id*multiplier)
	}
	return builder.String()
}
