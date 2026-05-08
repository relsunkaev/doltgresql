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
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

// TestSelectForUpdateDeadlock verifies that two sessions taking
// FOR UPDATE row locks in opposite orders are not allowed to hang
// indefinitely: one of them detects the cycle in the wait-graph,
// receives a SQLSTATE 40P01 deadlock_detected error, and aborts
// - at which point the other session's wait completes.
//
// Every PostgreSQL ORM and transaction helper is built around the
// assumption that deadlocks are *errors* it can retry, not hangs
// it has to time out. Without this detector, doltgres' synthesized
// row locks would hang both sessions forever (until somebody's
// statement timeout fires).
//
// The audit caveat we are closing here was: "Two sessions taking
// opposite-order locks will hang until one's transaction times
// out. PG detects the cycle and aborts one."
func TestSelectForUpdateDeadlock(t *testing.T) {
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
		"CREATE TABLE dl_t (id INT PRIMARY KEY, v INT);",
		"INSERT INTO dl_t VALUES (1, 10), (2, 20);",
	} {
		_, err := defaultConn.Exec(ctx, q)
		require.NoError(t, err, "setup: %s", q)
	}

	// Two sessions, opposite lock orders:
	//   A: row 1 (held), then row 2 (waits)
	//   B: row 2 (held), then row 1 (waits)
	//
	// Once both first locks are held, each will block on the
	// other. The poll-based deadlock detector must eventually
	// (within ~1s) catch the cycle and abort exactly one.
	a := dial(t)
	b := dial(t)

	aTx, err := a.Begin(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = aTx.Rollback(context.Background()) })
	bTx, err := b.Begin(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = bTx.Rollback(context.Background()) })

	// First locks: both succeed, no contention.
	_, err = aTx.Exec(ctx, "SELECT v FROM dl_t WHERE id = 1 FOR UPDATE;")
	require.NoError(t, err)
	_, err = bTx.Exec(ctx, "SELECT v FROM dl_t WHERE id = 2 FOR UPDATE;")
	require.NoError(t, err)

	// Second locks: race them on goroutines. Whichever loses the
	// cycle-detection race must roll back inside the goroutine so
	// its held lock is released; the survivor's Exec stays blocked
	// until that happens.
	var (
		wg              sync.WaitGroup
		aErr, bErr      error
		aFinished, bFin atomic.Int64
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, err := aTx.Exec(context.Background(),
			"SELECT v FROM dl_t WHERE id = 2 FOR UPDATE;")
		aErr = err
		if err != nil {
			_ = aTx.Rollback(context.Background())
		}
		aFinished.Store(time.Now().UnixNano())
	}()
	go func() {
		defer wg.Done()
		_, err := bTx.Exec(context.Background(),
			"SELECT v FROM dl_t WHERE id = 1 FOR UPDATE;")
		bErr = err
		if err != nil {
			_ = bTx.Rollback(context.Background())
		}
		bFin.Store(time.Now().UnixNano())
	}()

	// Should resolve in well under the 30s test timeout - usually
	// within a few hundred ms once both locks are acquired.
	doneCh := make(chan struct{})
	go func() { wg.Wait(); close(doneCh) }()
	select {
	case <-doneCh:
	case <-time.After(15 * time.Second):
		t.Fatal("deadlock detector did not break the cycle within 15s")
	}

	// Exactly one side should have the deadlock error; the other
	// should have succeeded (its second lock was free after the
	// loser's cleanup released the first).
	aIsDeadlock := aErr != nil && isDeadlockErr(aErr)
	bIsDeadlock := bErr != nil && isDeadlockErr(bErr)
	require.True(t, aIsDeadlock != bIsDeadlock,
		"exactly one transaction should report a deadlock error\nA: %v\nB: %v", aErr, bErr)

	if aIsDeadlock {
		require.NoError(t, bErr,
			"survivor B should have acquired its lock after A's abort")
	} else {
		require.NoError(t, aErr,
			"survivor A should have acquired its lock after B's abort")
	}
}

// isDeadlockErr returns whether err carries SQLSTATE 40P01
// (deadlock_detected) - the canonical error every PG client
// branches on to decide to retry the transaction.
func isDeadlockErr(err error) bool {
	if err == nil {
		return false
	}
	var pgErr *pgconn.PgError
	for cur := err; cur != nil; {
		if pe, ok := cur.(*pgconn.PgError); ok {
			pgErr = pe
			break
		}
		// pgx wraps; unwrap manually since the loop is small.
		type unwrapper interface{ Unwrap() error }
		if u, ok := cur.(unwrapper); ok {
			cur = u.Unwrap()
			continue
		}
		break
	}
	return pgErr != nil && pgErr.Code == "40P01"
}
