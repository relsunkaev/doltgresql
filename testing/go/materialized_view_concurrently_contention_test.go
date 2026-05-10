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
}
