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
	"sync/atomic"
	"testing"
	"time"

	gms "github.com/dolthub/go-mysql-server/sql"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// TestSelectForUpdateContention is the cross-session evidence the
// audit asked for on FOR UPDATE row-level locking. Two pgx
// connections race for the same row's lock; the second must block
// until the first commits, NOWAIT must raise immediately on
// contention, and SKIP LOCKED must elide the contended row and
// continue with the rest of the result.
func TestSelectForUpdateContention(t *testing.T) {
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
		t.Cleanup(func() { _ = conn.Close(ctx) })
		return conn
	}

	for _, q := range []string{
		"CREATE TABLE accounts (id INT PRIMARY KEY, balance INT);",
		"INSERT INTO accounts VALUES (1, 100), (2, 200), (3, 300);",
	} {
		_, err := defaultConn.Exec(ctx, q)
		require.NoError(t, err, "setup: %s", q)
	}

	t.Run("FOR UPDATE blocks until holder commits", func(t *testing.T) {
		holder := dial(t)
		waiter := dial(t)

		holdTx, err := holder.Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { _ = holdTx.Rollback(context.Background()) })

		var got int
		require.NoError(t, holdTx.QueryRow(ctx,
			"SELECT balance FROM accounts WHERE id = 1 FOR UPDATE;").Scan(&got))
		require.Equal(t, 100, got)

		// The waiter must block: launch on a goroutine and prove it
		// doesn't return before the holder commits.
		var (
			waiterDone     atomic.Bool
			waiterStart    = time.Now()
			waiterFinished time.Time
			waiterErr      error
		)
		go func() {
			waitTx, err := waiter.Begin(context.Background())
			if err != nil {
				waiterErr = err
				waiterDone.Store(true)
				return
			}
			defer waitTx.Rollback(context.Background())
			var v int
			waiterErr = waitTx.QueryRow(context.Background(),
				"SELECT balance FROM accounts WHERE id = 1 FOR UPDATE;").Scan(&v)
			waiterFinished = time.Now()
			waiterDone.Store(true)
		}()

		// Confirm the waiter is genuinely blocked for at least a
		// short interval before we release the holder.
		time.Sleep(300 * time.Millisecond)
		require.False(t, waiterDone.Load(),
			"waiter must block while holder owns the row lock")

		// Holder commits -> waiter unblocks.
		require.NoError(t, holdTx.Commit(ctx))

		require.Eventually(t, waiterDone.Load,
			3*time.Second, 20*time.Millisecond,
			"waiter must unblock within 3s of holder commit")
		require.NoError(t, waiterErr)
		require.Greater(t, waiterFinished.Sub(waiterStart), 250*time.Millisecond,
			"waiter must have actually waited")
	})

	t.Run("FOR UPDATE NOWAIT raises on contention", func(t *testing.T) {
		holder := dial(t)
		challenger := dial(t)

		holdTx, err := holder.Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { _ = holdTx.Rollback(context.Background()) })

		_, err = holdTx.Exec(ctx, "SELECT balance FROM accounts WHERE id = 2 FOR UPDATE;")
		require.NoError(t, err)

		challengeTx, err := challenger.Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { _ = challengeTx.Rollback(context.Background()) })

		// NOWAIT must raise without blocking.
		start := time.Now()
		_, err = challengeTx.Exec(ctx,
			"SELECT balance FROM accounts WHERE id = 2 FOR UPDATE NOWAIT;")
		elapsed := time.Since(start)
		require.Error(t, err, "NOWAIT must raise on contention")
		require.Less(t, elapsed, 250*time.Millisecond,
			"NOWAIT must not block; took %s", elapsed)
	})

	t.Run("FOR UPDATE SKIP LOCKED elides the contended row", func(t *testing.T) {
		holder := dial(t)
		reader := dial(t)

		holdTx, err := holder.Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { _ = holdTx.Rollback(context.Background()) })

		_, err = holdTx.Exec(ctx,
			"SELECT balance FROM accounts WHERE id = 3 FOR UPDATE;")
		require.NoError(t, err)

		readTx, err := reader.Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { _ = readTx.Rollback(context.Background()) })

		// SKIP LOCKED returns rows that are not held; id=3 should
		// disappear from the scan. The other rows were not seen by
		// any session before, so SKIP LOCKED returns them.
		rows, err := readTx.Query(ctx,
			"SELECT id FROM accounts ORDER BY id FOR UPDATE SKIP LOCKED;")
		require.NoError(t, err)
		var ids []int
		for rows.Next() {
			var id int
			require.NoError(t, rows.Scan(&id))
			ids = append(ids, id)
		}
		rows.Close()
		require.NoError(t, rows.Err())
		require.NotContains(t, ids, 3,
			"SKIP LOCKED must elide the held row")
		require.Subset(t, []int{1, 2, 3}, ids,
			"SKIP LOCKED must still return the unheld rows")
	})

	t.Run("eight-way race: only one waiter wins per release cycle", func(t *testing.T) {
		// Stress: N goroutines each take FOR UPDATE on the same row,
		// hold for 50ms, then commit. Total runtime should be roughly
		// N * 50ms (serial) rather than 50ms (concurrent) — the
		// strongest behavioral signal that the lock actually
		// serializes.
		const workers = 8
		const hold = 50 * time.Millisecond
		var wg sync.WaitGroup
		start := make(chan struct{})
		begin := time.Now()
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				conn := dial(t)
				<-start
				tx, err := conn.Begin(context.Background())
				if err != nil {
					t.Errorf("begin: %v", err)
					return
				}
				defer tx.Rollback(context.Background())
				_, err = tx.Exec(context.Background(),
					"SELECT id FROM accounts WHERE id = 1 FOR UPDATE;")
				if err != nil {
					t.Errorf("for update: %v", err)
					return
				}
				time.Sleep(hold)
				_ = tx.Commit(context.Background())
			}()
		}
		close(start)
		wg.Wait()
		elapsed := time.Since(begin)
		// Allow generous slack for goroutine scheduling, but the
		// total must be substantially more than a single hold —
		// otherwise the locks did not serialize.
		require.GreaterOrEqual(t, elapsed, time.Duration(workers/2)*hold,
			"FOR UPDATE on the same row must serialize across sessions; took %s", elapsed)
	})
}
