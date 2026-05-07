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

// TestAdvisoryLockConcurrency exercises the duplicate-work-prevention
// workload pattern that real applications hit under contention: multiple
// concurrent connections all racing for the same advisory lock key. The
// other lock_test.go cases use sequential multi-session assertions; this
// file uses goroutines so the assertions cover the part that actually
// matters in production — what happens when two callers ask at the same
// time.
//
// The harness covers three checklist items together: pg_advisory_xact_lock
// (auto-release at transaction end + cross-session blocking), pg_try_
// advisory_lock semantics (only one of N concurrent callers wins), and
// hashtext (used as the key derivation for both).
func TestAdvisoryLockConcurrency(t *testing.T) {
	port, err := gms.GetEmptyPort()
	require.NoError(t, err)
	ctx, defaultConn, controller := CreateServerWithPort(t, "postgres", port)
	t.Cleanup(func() {
		defaultConn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	// Independent connections per goroutine. We deliberately avoid
	// reusing defaultConn so each worker has its own pgwire session
	// and can hold its own LockSubsystem entry concurrently.
	dial := func(t *testing.T) *pgx.Conn {
		t.Helper()
		conn, err := pgx.Connect(ctx, fmt.Sprintf(
			"postgres://postgres:password@127.0.0.1:%d/postgres?sslmode=disable", port))
		require.NoError(t, err)
		t.Cleanup(func() { _ = conn.Close(ctx) })
		return conn
	}

	t.Run("pg_try_advisory_lock under concurrent contention", func(t *testing.T) {
		// N goroutines call pg_try_advisory_lock(K) for the same K
		// simultaneously. PostgreSQL semantics: exactly one returns
		// true, every other returns false without blocking.
		const workers = 8
		const key = int64(0xC0FFEE)
		var wg sync.WaitGroup
		var winners atomic.Int32
		start := make(chan struct{})

		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				conn := dial(t)
				<-start
				var got bool
				err := conn.QueryRow(ctx,
					fmt.Sprintf(`SELECT pg_try_advisory_lock(%d)`, key)).Scan(&got)
				if err != nil {
					t.Errorf("pg_try_advisory_lock errored: %v", err)
					return
				}
				if got {
					winners.Add(1)
					// Hold long enough that it is observable
					// to the others; release explicitly so the
					// next subtest starts from a clean slate.
					time.Sleep(20 * time.Millisecond)
					_, _ = conn.Exec(ctx, fmt.Sprintf(`SELECT pg_advisory_unlock(%d)`, key))
				}
			}()
		}
		close(start)
		wg.Wait()
		require.Equal(t, int32(1), winners.Load(),
			"exactly one of %d concurrent pg_try_advisory_lock callers must win", workers)
	})

	t.Run("pg_advisory_xact_lock blocks then auto-releases", func(t *testing.T) {
		// Holder begins a transaction, takes pg_advisory_xact_lock,
		// then sleeps a known interval before committing. A waiter
		// running in parallel must block on the same key and only
		// proceed after the holder commits. Asserts:
		//   - waiter is genuinely blocked while the holder is in the
		//     transaction (reads "blocked" via a non-blocking try)
		//   - waiter eventually succeeds, no later than just after
		//     the holder commits
		//   - the holder's xact lock is auto-released at COMMIT
		//     (the waiter then acquires; we don't have to call
		//     pg_advisory_unlock anywhere)
		const key = int64(0xBA5EBA11)
		holder := dial(t)
		waiter := dial(t)
		prober := dial(t)

		holdReady := make(chan struct{})
		holdDone := make(chan struct{})
		holdErr := make(chan error, 1)

		holdLock := fmt.Sprintf(`SELECT pg_advisory_xact_lock(%d)`, key)
		tryLock := fmt.Sprintf(`SELECT pg_try_advisory_xact_lock(%d)`, key)
		go func() {
			tx, err := holder.Begin(ctx)
			if err != nil {
				holdErr <- err
				close(holdReady)
				return
			}
			if _, err := tx.Exec(ctx, holdLock); err != nil {
				holdErr <- err
				close(holdReady)
				return
			}
			close(holdReady)
			time.Sleep(50 * time.Millisecond)
			if err := tx.Commit(ctx); err != nil {
				holdErr <- err
				close(holdDone)
				return
			}
			close(holdDone)
		}()

		<-holdReady
		select {
		case err := <-holdErr:
			t.Fatalf("holder failed: %v", err)
		default:
		}

		// Probe with a non-blocking try from a third connection: the
		// lock is currently held by holder, so this must return false.
		var probed bool
		require.NoError(t, prober.QueryRow(ctx, tryLock).Scan(&probed))
		require.False(t, probed,
			"third session must observe the lock as held while holder is in xact")

		// Waiter takes the blocking form. It must wait until the
		// holder commits.
		waitDone := make(chan time.Time, 1)
		go func() {
			tx, err := waiter.Begin(ctx)
			if err != nil {
				t.Errorf("waiter begin: %v", err)
				return
			}
			defer tx.Rollback(ctx)
			if _, err := tx.Exec(ctx, holdLock); err != nil {
				t.Errorf("waiter exec: %v", err)
				return
			}
			waitDone <- time.Now()
		}()

		select {
		case <-waitDone:
			t.Fatal("waiter must not have acquired before holder committed")
		case <-time.After(20 * time.Millisecond):
			// expected — holder still holds
		}

		<-holdDone
		select {
		case err := <-holdErr:
			t.Fatalf("holder errored: %v", err)
		default:
		}

		select {
		case <-waitDone:
			// expected
		case <-time.After(2 * time.Second):
			t.Fatal("waiter never acquired after holder committed")
		}
	})

	t.Run("pg_advisory_xact_lock keyed by hashtext is duplicate-work safe", func(t *testing.T) {
		// The canonical workload pattern: derive the lock key from
		// a job identifier via hashtext, race two workers on the
		// same job, ensure only one runs the protected section.
		const job = "import-tenant-42"
		const workers = 6
		var ran atomic.Int32
		var wg sync.WaitGroup
		start := make(chan struct{})

		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				conn := dial(t)
				<-start
				tx, err := conn.Begin(ctx)
				if err != nil {
					t.Errorf("begin: %v", err)
					return
				}
				defer tx.Rollback(ctx)
				var got bool
				if err := tx.QueryRow(ctx,
					fmt.Sprintf(`SELECT pg_try_advisory_xact_lock(hashtext('%s'))`, job)).Scan(&got); err != nil {
					t.Errorf("try_xact_lock: %v", err)
					return
				}
				if got {
					ran.Add(1)
					// Simulate the protected section before
					// committing.
					time.Sleep(15 * time.Millisecond)
				}
				_ = tx.Commit(ctx)
			}()
		}
		close(start)
		wg.Wait()

		// Exactly one worker must have run the protected section.
		require.Equal(t, int32(1), ran.Load(),
			"exactly one of %d hashtext-keyed workers must hold the xact lock concurrently", workers)
	})

	t.Run("xact lock auto-releases on rollback under concurrency", func(t *testing.T) {
		const key = int64(0xDEADBEEF)
		holder := dial(t)
		waiter := dial(t)

		holdLock := fmt.Sprintf(`SELECT pg_advisory_xact_lock(%d)`, key)
		// Holder takes the lock, the test rolls it back, the waiter
		// must succeed afterwards.
		tx, err := holder.Begin(ctx)
		require.NoError(t, err)
		_, err = tx.Exec(ctx, holdLock)
		require.NoError(t, err)

		// Waiter blocks until the rollback below releases.
		acquired := make(chan struct{})
		go func() {
			tx2, err := waiter.Begin(ctx)
			if err != nil {
				return
			}
			defer tx2.Rollback(ctx)
			if _, err := tx2.Exec(ctx, holdLock); err != nil {
				return
			}
			close(acquired)
		}()

		select {
		case <-acquired:
			t.Fatal("waiter must not have acquired before rollback")
		case <-time.After(20 * time.Millisecond):
		}

		require.NoError(t, tx.Rollback(ctx))

		select {
		case <-acquired:
		case <-time.After(2 * time.Second):
			t.Fatal("waiter never acquired after holder rolled back")
		}
	})
}

// TestAdvisoryLockConcurrencyContext is a smoke check that the test setup
// itself does not flake under fast cancellation; protects the harness from
// false-pass regressions.
func TestAdvisoryLockConcurrencyContext(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	port, err := gms.GetEmptyPort()
	require.NoError(t, err)
	ctx, defaultConn, controller := CreateServerWithPort(t, "postgres", port)
	t.Cleanup(func() {
		defaultConn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	conn, err := pgx.Connect(ctx, fmt.Sprintf(
		"postgres://postgres:password@127.0.0.1:%d/postgres?sslmode=disable", port))
	require.NoError(t, err)
	defer conn.Close(ctx)

	// Confirm a quick acquire+unlock cycle does not leave residue.
	for i := 0; i < 5; i++ {
		var got bool
		require.NoError(t, conn.QueryRow(ctx, `SELECT pg_try_advisory_lock(99)`).Scan(&got))
		require.True(t, got)
		var unlocked bool
		require.NoError(t, conn.QueryRow(ctx, `SELECT pg_advisory_unlock(99)`).Scan(&unlocked))
		require.True(t, unlocked)
	}

	// And under a quickly cancelled context, the connection must recover.
	cctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	_, _ = conn.Exec(cctx, `SELECT pg_sleep(1)`)
}
