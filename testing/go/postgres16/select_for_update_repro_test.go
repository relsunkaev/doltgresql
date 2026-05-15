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

package postgres16

import (
	. "github.com/dolthub/doltgresql/testing/go"

	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// TestSelectForUpdateRequiresUpdatePrivilegeRepro reproduces a security bug:
// SELECT ... FOR UPDATE takes update-strength row locks and requires UPDATE
// privilege, not just SELECT privilege.
func TestSelectForUpdateRequiresUpdatePrivilegeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "SELECT FOR UPDATE requires UPDATE privilege",
			SetUpScript: []string{
				`CREATE USER for_update_select_only PASSWORD 'reader';`,
				`CREATE TABLE for_update_privilege_private (
					id INT PRIMARY KEY,
					balance INT
				);`,
				`INSERT INTO for_update_privilege_private VALUES (1, 100);`,
				`GRANT USAGE ON SCHEMA public TO for_update_select_only;`,
				`GRANT SELECT ON for_update_privilege_private TO for_update_select_only;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, balance
						FROM for_update_privilege_private
						WHERE id = 1
						FOR UPDATE;`,

					Username: `for_update_select_only`,
					Password: `reader`, PostgresOracle: ScriptTestPostgresOracle{

						// TestSelectForUpdateRejectsNonLockableQueryShapesRepro reproduces a row-lock
						// correctness bug: PostgreSQL rejects FOR UPDATE when result rows cannot be
						// mapped directly back to lockable base-table rows.
						ID: "select-for-update-repro-test-testselectforupdaterequiresupdateprivilegerepro-0001-select-id-balance-from-for_update_privilege_private", Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestSelectForUpdateRejectsNonLockableQueryShapesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "SELECT FOR UPDATE rejects non-lockable query shapes",
			SetUpScript: []string{
				`CREATE TABLE for_update_shape_items (
					id INT PRIMARY KEY,
					grp INT
				);`,
				`INSERT INTO for_update_shape_items VALUES (1, 1), (2, 1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT count(*) FROM for_update_shape_items FOR UPDATE;`, PostgresOracle: ScriptTestPostgresOracle{ID: "select-for-update-repro-test-testselectforupdaterejectsnonlockablequeryshapesrepro-0001-select-count-*-from-for_update_shape_items", Compare: "sqlstate"},
				},
				{
					Query: `SELECT grp, count(*)
						FROM for_update_shape_items
						GROUP BY grp
						FOR UPDATE;`, PostgresOracle: ScriptTestPostgresOracle{ID: "select-for-update-repro-test-testselectforupdaterejectsnonlockablequeryshapesrepro-0002-select-grp-count-*-from", Compare: "sqlstate"},
				},
				{
					Query: `SELECT DISTINCT grp FROM for_update_shape_items FOR UPDATE;`, PostgresOracle: ScriptTestPostgresOracle{ID: "select-for-update-repro-test-testselectforupdaterejectsnonlockablequeryshapesrepro-0003-select-distinct-grp-from-for_update_shape_items", Compare: "sqlstate"},
				},
				{
					Query: `SELECT id FROM for_update_shape_items
						UNION
						SELECT id FROM for_update_shape_items
						FOR UPDATE;`, PostgresOracle: ScriptTestPostgresOracle{ID: "select-for-update-repro-test-testselectforupdaterejectsnonlockablequeryshapesrepro-0004-select-id-from-for_update_shape_items-union",

						// TestSelectForUpdateBlocksConcurrentWritersRepro reproduces a data consistency
						// bug: Doltgres accepts SELECT ... FOR UPDATE, but concurrent writers do not
						// block behind the held row lock.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestSelectForUpdateBlocksConcurrentWritersRepro(t *testing.T) {
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)
	ctx, defaultConn, controller := CreateServerWithPort(t, "postgres", port)
	t.Cleanup(func() {
		defaultConn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	for _, stmt := range []string{
		`CREATE TABLE for_update_lock_accounts (id INT PRIMARY KEY, balance INT);`,
		`INSERT INTO for_update_lock_accounts VALUES (1, 100);`,
	} {
		_, err = defaultConn.Exec(ctx, stmt)
		require.NoError(t, err, stmt)
	}

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

	locker := dial(t)
	writer := dial(t)

	lockTx, err := locker.Begin(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = lockTx.Rollback(context.Background())
	})

	var balance int
	require.NoError(t, lockTx.QueryRow(ctx,
		`SELECT balance FROM for_update_lock_accounts WHERE id = 1 FOR UPDATE;`,
	).Scan(&balance))
	require.Equal(t, 100, balance)

	writeDone := make(chan error, 1)
	go func() {
		_, err := writer.Exec(ctx,
			`UPDATE for_update_lock_accounts SET balance = balance + 50 WHERE id = 1;`,
		)
		writeDone <- err
	}()

	select {
	case err := <-writeDone:
		require.NoError(t, err)
		t.Fatalf("writer completed while SELECT FOR UPDATE row lock was still held")
	case <-time.After(50 * time.Millisecond):
	}

	require.NoError(t, lockTx.Rollback(ctx))
	select {
	case err := <-writeDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("writer did not complete after releasing SELECT FOR UPDATE row lock")
	}
}

// TestSelectForShareBlocksConcurrentWritersRepro reproduces a data consistency
// bug: PostgreSQL's FOR SHARE row lock conflicts with concurrent writers, but
// Doltgres lets the writer update the locked row immediately.
func TestSelectForShareBlocksConcurrentWritersRepro(t *testing.T) {
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)
	ctx, defaultConn, controller := CreateServerWithPort(t, "postgres", port)
	t.Cleanup(func() {
		defaultConn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	for _, stmt := range []string{
		`CREATE TABLE for_share_lock_accounts (id INT PRIMARY KEY, balance INT);`,
		`INSERT INTO for_share_lock_accounts VALUES (1, 100);`,
	} {
		_, err = defaultConn.Exec(ctx, stmt)
		require.NoError(t, err, stmt)
	}

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

	locker := dial(t)
	writer := dial(t)

	lockTx, err := locker.Begin(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = lockTx.Rollback(context.Background())
	})

	var balance int
	require.NoError(t, lockTx.QueryRow(ctx,
		`SELECT balance FROM for_share_lock_accounts WHERE id = 1 FOR SHARE;`,
	).Scan(&balance))
	require.Equal(t, 100, balance)

	writeDone := make(chan error, 1)
	go func() {
		_, err := writer.Exec(ctx,
			`UPDATE for_share_lock_accounts SET balance = balance + 50 WHERE id = 1;`,
		)
		writeDone <- err
	}()

	select {
	case err := <-writeDone:
		require.NoError(t, err)
		t.Fatalf("writer completed while SELECT FOR SHARE row lock was still held")
	case <-time.After(50 * time.Millisecond):
	}

	require.NoError(t, lockTx.Rollback(ctx))
	select {
	case err := <-writeDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("writer did not complete after releasing SELECT FOR SHARE row lock")
	}
}

// TestSelectForNoKeyUpdateBlocksConcurrentWritersRepro reproduces a data
// consistency bug: PostgreSQL's FOR NO KEY UPDATE row lock conflicts with
// concurrent writers, but Doltgres lets the writer update the locked row
// immediately.
func TestSelectForNoKeyUpdateBlocksConcurrentWritersRepro(t *testing.T) {
	assertSelectRowLockBlocksStatement(
		t,
		"for_no_key_update_lock_accounts",
		"FOR NO KEY UPDATE",
		`UPDATE for_no_key_update_lock_accounts SET balance = balance + 50 WHERE id = 1;`,
		"SELECT FOR NO KEY UPDATE",
		"writer",
	)
}

// TestSelectForKeyShareBlocksConcurrentDeletesRepro reproduces a data
// consistency bug: PostgreSQL's FOR KEY SHARE row lock conflicts with row
// deletes, but Doltgres lets the delete remove the locked row immediately.
func TestSelectForKeyShareBlocksConcurrentDeletesRepro(t *testing.T) {
	assertSelectRowLockBlocksStatement(
		t,
		"for_key_share_lock_accounts",
		"FOR KEY SHARE",
		`DELETE FROM for_key_share_lock_accounts WHERE id = 1;`,
		"SELECT FOR KEY SHARE",
		"deleter",
	)
}

func assertSelectRowLockBlocksStatement(t *testing.T, tableName, lockClause, writeSQL, lockName, actorName string) {
	t.Helper()

	port, err := sql.GetEmptyPort()
	require.NoError(t, err)
	ctx, defaultConn, controller := CreateServerWithPort(t, "postgres", port)
	t.Cleanup(func() {
		defaultConn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	for _, stmt := range []string{
		fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY, balance INT);`, tableName),
		fmt.Sprintf(`INSERT INTO %s VALUES (1, 100);`, tableName),
	} {
		_, err = defaultConn.Exec(ctx, stmt)
		require.NoError(t, err, stmt)
	}

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

	locker := dial(t)
	writer := dial(t)

	lockTx, err := locker.Begin(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = lockTx.Rollback(context.Background())
	})

	var balance int
	require.NoError(t, lockTx.QueryRow(ctx,
		fmt.Sprintf(`SELECT balance FROM %s WHERE id = 1 %s;`, tableName, lockClause),
	).Scan(&balance))
	require.Equal(t, 100, balance)

	writeDone := make(chan error, 1)
	go func() {
		_, err := writer.Exec(ctx, writeSQL)
		writeDone <- err
	}()

	select {
	case err := <-writeDone:
		require.NoError(t, err)
		t.Fatalf("%s completed while %s row lock was still held", actorName, lockName)
	case <-time.After(50 * time.Millisecond):
	}

	require.NoError(t, lockTx.Rollback(ctx))
	select {
	case err := <-writeDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatalf("%s did not complete after releasing %s row lock", actorName, lockName)
	}
}
