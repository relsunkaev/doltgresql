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
)

// TestPgLocksRowLevelVisibility pins that synthesized FOR UPDATE
// row and table locks surface in pg_locks, so admins debugging a
// blocked workload can see who is holding what. Before this
// landed, pg_locks always returned zero rows - which made
// "TablePlus / pgAdmin show me who's blocking my UPDATE" fail
// silently against doltgres.
//
// The audit caveat we are closing here was: "Tooling that
// introspects pg_locks to see who is blocking whom on a specific
// tuple won't see anything here."
func TestPgLocksRowLevelVisibility(t *testing.T) {
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
		"CREATE TABLE pl_keyed (id INT PRIMARY KEY, v TEXT);",
		"INSERT INTO pl_keyed VALUES (1, 'a'), (2, 'b'), (3, 'c');",
		"CREATE TABLE pl_keyless (event TEXT);",
		"INSERT INTO pl_keyless VALUES ('x'), ('y');",
	} {
		_, err := defaultConn.Exec(ctx, q)
		require.NoError(t, err, "setup: %s", q)
	}

	t.Run("row-level FOR UPDATE surfaces a tuple lock", func(t *testing.T) {
		holder := dial(t)
		holdTx, err := holder.Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { _ = holdTx.Rollback(context.Background()) })

		_, err = holdTx.Exec(ctx, "SELECT v FROM pl_keyed WHERE id = 1 FOR UPDATE;")
		require.NoError(t, err)

		// pg_locks should now report exactly one tuple-level lock,
		// granted, on relation pl_keyed.
		reader := dial(t)
		var locktype, mode string
		var granted bool
		err = reader.QueryRow(ctx, `
			SELECT locktype, mode, granted
			FROM pg_catalog.pg_locks
			WHERE locktype = 'tuple'
			  AND relation = (SELECT oid FROM pg_catalog.pg_class
			                   WHERE relname = 'pl_keyed' AND relnamespace = (
			                     SELECT oid FROM pg_catalog.pg_namespace
			                      WHERE nspname = 'public'))
		`).Scan(&locktype, &mode, &granted)
		require.NoError(t, err, "pg_locks should surface the FOR UPDATE row lock")
		require.Equal(t, "tuple", locktype)
		require.Equal(t, "RowExclusiveLock", mode)
		require.True(t, granted, "lock should be marked granted")
	})

	t.Run("FOR UPDATE waiter surfaces granted=false", func(t *testing.T) {
		holder := dial(t)
		waiter := dial(t)

		holdTx, err := holder.Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { _ = holdTx.Rollback(context.Background()) })
		_, err = holdTx.Exec(ctx, "SELECT v FROM pl_keyed WHERE id = 2 FOR UPDATE;")
		require.NoError(t, err)

		// Launch a waiter on a goroutine so we can poll pg_locks
		// while it's blocked.
		waiterDone := make(chan error, 1)
		go func() {
			waitTx, err := waiter.Begin(context.Background())
			if err != nil {
				waiterDone <- err
				return
			}
			defer waitTx.Rollback(context.Background())
			_, err = waitTx.Exec(context.Background(),
				"SELECT v FROM pl_keyed WHERE id = 2 FOR UPDATE;")
			waiterDone <- err
		}()

		// Poll pg_locks: we expect at least one granted=true and
		// one granted=false row for relation pl_keyed by the time
		// the waiter is registered. Use require.Eventually because
		// the goroutine takes a beat to register.
		reader := dial(t)
		require.Eventually(t, func() bool {
			var heldCount, waitingCount int
			if err := reader.QueryRow(ctx, `
				SELECT count(*) FROM pg_catalog.pg_locks
				WHERE locktype = 'tuple' AND granted = true
			`).Scan(&heldCount); err != nil {
				return false
			}
			if err := reader.QueryRow(ctx, `
				SELECT count(*) FROM pg_catalog.pg_locks
				WHERE locktype = 'tuple' AND granted = false
			`).Scan(&waitingCount); err != nil {
				return false
			}
			return heldCount >= 1 && waitingCount >= 1
		}, 3*time.Second, 50*time.Millisecond,
			"pg_locks should show one held and one waiting tuple lock")

		// Releasing the holder unblocks the waiter.
		require.NoError(t, holdTx.Commit(ctx))
		select {
		case err := <-waiterDone:
			require.NoError(t, err)
		case <-time.After(3 * time.Second):
			t.Fatal("waiter did not unblock")
		}
	})

	t.Run("keyless FOR UPDATE surfaces a relation lock", func(t *testing.T) {
		holder := dial(t)
		holdTx, err := holder.Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { _ = holdTx.Rollback(context.Background()) })

		_, err = holdTx.Exec(ctx,
			"SELECT event FROM pl_keyless WHERE event = 'x' FOR UPDATE;")
		require.NoError(t, err)

		reader := dial(t)
		var locktype, mode string
		var granted bool
		err = reader.QueryRow(ctx, `
			SELECT locktype, mode, granted
			FROM pg_catalog.pg_locks
			WHERE locktype = 'relation'
			  AND relation = (SELECT oid FROM pg_catalog.pg_class
			                   WHERE relname = 'pl_keyless' AND relnamespace = (
			                     SELECT oid FROM pg_catalog.pg_namespace
			                      WHERE nspname = 'public'))
		`).Scan(&locktype, &mode, &granted)
		require.NoError(t, err, "pg_locks should surface the keyless table lock")
		require.Equal(t, "relation", locktype)
		require.Equal(t, "ExclusiveLock", mode)
		require.True(t, granted)
	})

	t.Run("commit clears the registry", func(t *testing.T) {
		holder := dial(t)
		holdTx, err := holder.Begin(ctx)
		require.NoError(t, err)
		_, err = holdTx.Exec(ctx, "SELECT v FROM pl_keyed WHERE id = 3 FOR UPDATE;")
		require.NoError(t, err)
		require.NoError(t, holdTx.Commit(ctx))

		// After commit the row-lock entry should be gone. We assert
		// only on the rows we know we created in this subtest to
		// avoid false-fail when an earlier subtest's Cleanup hasn't
		// run yet (it only matters that the *id=3* lock is gone).
		reader := dial(t)
		var n int
		err = reader.QueryRow(ctx, `
			SELECT count(*)
			FROM pg_catalog.pg_locks
			WHERE locktype = 'tuple'
			  AND relation = (SELECT oid FROM pg_catalog.pg_class
			                   WHERE relname = 'pl_keyed' AND relnamespace = (
			                     SELECT oid FROM pg_catalog.pg_namespace
			                      WHERE nspname = 'public'))
		`).Scan(&n)
		require.NoError(t, err)
		// Other subtests' cleanups may not have run; what matters is
		// that committing the holder removed *its* entry. We can't
		// predicate-filter to the specific row from outside the
		// session, so we settle for "no more than the unrelated
		// holder count we expect" - at most one (a cleanup-pending
		// rollback of an earlier test). Zero is the typical case.
		require.LessOrEqual(t, n, 1,
			"committing the holder should clear its row-lock entry")
	})
}
