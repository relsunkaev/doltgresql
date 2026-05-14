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
	"os"
	"path/filepath"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/doltgresql/server/sessionstate"
)

func TestPreparedTransactions(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "commit prepared transaction",
			SetUpScript: []string{
				"CREATE TABLE prepared_tx_items (id INT PRIMARY KEY, label TEXT);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "BEGIN;",
				},
				{
					Query: "INSERT INTO prepared_tx_items VALUES (1, 'one');",
				},
				{
					Query: "PREPARE TRANSACTION 'dg_prepared_commit';",
				},
				{
					Query: "SELECT count(*) FROM prepared_tx_items;",
					Expected: []sql.Row{
						{0},
					},
				},
				{
					Query: "SELECT gid, database FROM pg_catalog.pg_prepared_xacts WHERE gid = 'dg_prepared_commit';",
					Expected: []sql.Row{
						{"dg_prepared_commit", "postgres"},
					},
				},
				{
					Query: "COMMIT PREPARED 'dg_prepared_commit';",
				},
				{
					Query: "SELECT id, label FROM prepared_tx_items;",
					Expected: []sql.Row{
						{1, "one"},
					},
				},
				{
					Query:    "SELECT gid FROM pg_catalog.pg_prepared_xacts WHERE gid = 'dg_prepared_commit';",
					Expected: []sql.Row{},
				},
			},
		},
		{
			Name: "rollback prepared transaction",
			SetUpScript: []string{
				"CREATE TABLE prepared_tx_items (id INT PRIMARY KEY, label TEXT);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "BEGIN;",
				},
				{
					Query: "INSERT INTO prepared_tx_items VALUES (1, 'one');",
				},
				{
					Query: "PREPARE TRANSACTION 'dg_prepared_rollback';",
				},
				{
					Query: "SELECT gid FROM pg_catalog.pg_prepared_xacts WHERE gid = 'dg_prepared_rollback';",
					Expected: []sql.Row{
						{"dg_prepared_rollback"},
					},
				},
				{
					Query: "ROLLBACK PREPARED 'dg_prepared_rollback';",
				},
				{
					Query:    "SELECT * FROM prepared_tx_items;",
					Expected: []sql.Row{},
				},
				{
					Query:    "SELECT gid FROM pg_catalog.pg_prepared_xacts WHERE gid = 'dg_prepared_rollback';",
					Expected: []sql.Row{},
				},
			},
		},
		{
			Name: "prepared transaction errors",
			SetUpScript: []string{
				"CREATE TABLE prepared_tx_items (id INT PRIMARY KEY, label TEXT);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "PREPARE TRANSACTION 'dg_no_transaction';", PostgresOracle: ScriptTestPostgresOracle{ID: "prepared-transaction-test-testpreparedtransactions-0008-prepare-transaction-dg_no_transaction"},
				},
				{
					Query: "COMMIT PREPARED 'dg_missing';", PostgresOracle: ScriptTestPostgresOracle{ID: "prepared-transaction-test-testpreparedtransactions-0009-commit-prepared-dg_missing", Compare: "sqlstate"},
				},
				{
					Query: "ROLLBACK PREPARED 'dg_missing';", PostgresOracle: ScriptTestPostgresOracle{ID: "prepared-transaction-test-testpreparedtransactions-0010-rollback-prepared-dg_missing", Compare: "sqlstate"},
				},
				{
					Query: "BEGIN;",
				},
				{
					Query: "COMMIT PREPARED 'dg_missing';", PostgresOracle: ScriptTestPostgresOracle{ID: "prepared-transaction-test-testpreparedtransactions-0011-commit-prepared-dg_missing",

						// TestCommitPreparedRequiresTransactionOwnerRepro reproduces a security bug:
						// Doltgres lets a role commit a prepared transaction that was prepared by a
						// different role.
						Compare: "sqlstate"},
				},
				{
					Query: "ROLLBACK;",
				},
			},
		},
	})
}

func TestCommitPreparedRequiresTransactionOwnerRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "COMMIT PREPARED requires prepared transaction owner",
			SetUpScript: []string{
				`CREATE USER commit_prepared_owner PASSWORD 'owner';`,
				`CREATE USER commit_prepared_intruder PASSWORD 'intruder';`,
				`CREATE TABLE commit_prepared_private (id INT PRIMARY KEY, label TEXT);`,
				`GRANT USAGE ON SCHEMA public TO commit_prepared_owner, commit_prepared_intruder;`,
				`GRANT INSERT ON commit_prepared_private TO commit_prepared_owner;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `BEGIN;`,
					Username: `commit_prepared_owner`,
					Password: `owner`,
				},
				{
					Query:    `INSERT INTO commit_prepared_private VALUES (1, 'owner');`,
					Username: `commit_prepared_owner`,
					Password: `owner`,
				},
				{
					Query:    `PREPARE TRANSACTION 'dg_commit_requires_owner';`,
					Username: `commit_prepared_owner`,
					Password: `owner`,
				},
				{
					Query:       `COMMIT PREPARED 'dg_commit_requires_owner';`,
					ExpectedErr: `permission denied`,
					Username:    `commit_prepared_intruder`,
					Password:    `intruder`,
				},
			},
		},
	})
}

// TestRollbackPreparedRequiresTransactionOwnerRepro reproduces a security bug:
// Doltgres lets a role roll back a prepared transaction that was prepared by a
// different role.
func TestRollbackPreparedRequiresTransactionOwnerRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK PREPARED requires prepared transaction owner",
			SetUpScript: []string{
				`CREATE USER rollback_prepared_owner PASSWORD 'owner';`,
				`CREATE USER rollback_prepared_intruder PASSWORD 'intruder';`,
				`CREATE TABLE rollback_prepared_private (id INT PRIMARY KEY, label TEXT);`,
				`GRANT USAGE ON SCHEMA public TO rollback_prepared_owner, rollback_prepared_intruder;`,
				`GRANT INSERT ON rollback_prepared_private TO rollback_prepared_owner;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `BEGIN;`,
					Username: `rollback_prepared_owner`,
					Password: `owner`,
				},
				{
					Query:    `INSERT INTO rollback_prepared_private VALUES (1, 'owner');`,
					Username: `rollback_prepared_owner`,
					Password: `owner`,
				},
				{
					Query:    `PREPARE TRANSACTION 'dg_rollback_requires_owner';`,
					Username: `rollback_prepared_owner`,
					Password: `owner`,
				},
				{
					Query:       `ROLLBACK PREPARED 'dg_rollback_requires_owner';`,
					ExpectedErr: `permission denied`,
					Username:    `rollback_prepared_intruder`,
					Password:    `intruder`,
				},
			},
		},
	})
}

func TestPreparedTransactionSurvivesRestart(t *testing.T) {
	dbDir, err := os.MkdirTemp(os.TempDir(), t.Name())
	require.NoError(t, err)

	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerLocalInDirWithPort(t, "postgres", dbDir, port)
	_, err = conn.Current.Exec(ctx, "CREATE TABLE prepared_restart_items (id INT PRIMARY KEY, label TEXT);")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "BEGIN;")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "INSERT INTO prepared_restart_items VALUES (1, 'one');")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "PREPARE TRANSACTION 'dg_prepared_restart';")
	require.NoError(t, err)

	var count int64
	require.NoError(t, conn.Current.QueryRow(ctx, "SELECT count(*) FROM prepared_restart_items;").Scan(&count))
	require.EqualValues(t, 0, count)

	conn.Close(ctx)
	controller.Stop()
	require.NoError(t, controller.WaitForStop())
	sessionstate.ResetPreparedTransactionsForTests()

	ctx, conn, controller = CreateServerLocalInDirWithPort(t, "postgres", dbDir, port)
	defer func() {
		conn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	var database string
	require.NoError(t, conn.Current.QueryRow(ctx, `
		SELECT database
		FROM pg_catalog.pg_prepared_xacts
		WHERE gid = 'dg_prepared_restart';`).Scan(&database))
	require.Equal(t, "postgres", database)

	_, err = conn.Current.Exec(ctx, "COMMIT PREPARED 'dg_prepared_restart';")
	require.NoError(t, err)

	var label string
	require.NoError(t, conn.Current.QueryRow(ctx, "SELECT label FROM prepared_restart_items WHERE id = 1;").Scan(&label))
	require.Equal(t, "one", label)
}

func TestPreparedTransactionRollbackSurvivesRestart(t *testing.T) {
	dbDir, err := os.MkdirTemp(os.TempDir(), t.Name())
	require.NoError(t, err)

	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerLocalInDirWithPort(t, "postgres", dbDir, port)
	_, err = conn.Current.Exec(ctx, "CREATE TABLE prepared_restart_rollback_items (id INT PRIMARY KEY, label TEXT);")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "BEGIN;")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "INSERT INTO prepared_restart_rollback_items VALUES (1, 'one');")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "PREPARE TRANSACTION 'dg_prepared_restart_rollback';")
	require.NoError(t, err)

	conn.Close(ctx)
	controller.Stop()
	require.NoError(t, controller.WaitForStop())
	sessionstate.ResetPreparedTransactionsForTests()

	ctx, conn, controller = CreateServerLocalInDirWithPort(t, "postgres", dbDir, port)
	defer func() {
		conn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	var gid string
	require.NoError(t, conn.Current.QueryRow(ctx, `
		SELECT gid
		FROM pg_catalog.pg_prepared_xacts
		WHERE gid = 'dg_prepared_restart_rollback';`).Scan(&gid))
	require.Equal(t, "dg_prepared_restart_rollback", gid)

	_, err = conn.Current.Exec(ctx, "ROLLBACK PREPARED 'dg_prepared_restart_rollback';")
	require.NoError(t, err)

	var count int64
	require.NoError(t, conn.Current.QueryRow(ctx, "SELECT count(*) FROM prepared_restart_rollback_items;").Scan(&count))
	require.EqualValues(t, 0, count)
	require.NoError(t, conn.Current.QueryRow(ctx, `
		SELECT count(*)
		FROM pg_catalog.pg_prepared_xacts
		WHERE gid = 'dg_prepared_restart_rollback';`).Scan(&count))
	require.EqualValues(t, 0, count)
}

func TestRecoveredPreparedTransactionMergesChangedWorkingSet(t *testing.T) {
	dbDir, err := os.MkdirTemp(os.TempDir(), t.Name())
	require.NoError(t, err)

	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerLocalInDirWithPort(t, "postgres", dbDir, port)
	_, err = conn.Current.Exec(ctx, "CREATE TABLE prepared_restart_conflict_items (id INT PRIMARY KEY, label TEXT);")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "BEGIN;")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "INSERT INTO prepared_restart_conflict_items VALUES (1, 'prepared');")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "PREPARE TRANSACTION 'dg_prepared_restart_conflict';")
	require.NoError(t, err)

	conn.Close(ctx)
	controller.Stop()
	require.NoError(t, controller.WaitForStop())
	sessionstate.ResetPreparedTransactionsForTests()

	ctx, conn, controller = CreateServerLocalInDirWithPort(t, "postgres", dbDir, port)
	defer func() {
		conn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	_, err = conn.Current.Exec(ctx, "INSERT INTO prepared_restart_conflict_items VALUES (2, 'concurrent');")
	require.NoError(t, err)

	_, err = conn.Current.Exec(ctx, "COMMIT PREPARED 'dg_prepared_restart_conflict';")
	require.NoError(t, err)

	var count int64
	require.NoError(t, conn.Current.QueryRow(ctx, `
		SELECT count(*)
		FROM pg_catalog.pg_prepared_xacts
		WHERE gid = 'dg_prepared_restart_conflict';`).Scan(&count))
	require.EqualValues(t, 0, count)
	require.NoError(t, conn.Current.QueryRow(ctx, "SELECT count(*) FROM prepared_restart_conflict_items WHERE id = 1;").Scan(&count))
	require.EqualValues(t, 1, count)
	require.NoError(t, conn.Current.QueryRow(ctx, "SELECT count(*) FROM prepared_restart_conflict_items WHERE id = 2;").Scan(&count))
	require.EqualValues(t, 1, count)
}

func TestPreparedTransactionRejectsDuplicateGID(t *testing.T) {
	ctx, conn, controller := CreateServer(t, "postgres")
	defer func() {
		conn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	_, err := conn.Current.Exec(ctx, "CREATE TABLE prepared_duplicate_items (id INT PRIMARY KEY);")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "BEGIN;")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "INSERT INTO prepared_duplicate_items VALUES (1);")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "PREPARE TRANSACTION 'dg_prepared_duplicate';")
	require.NoError(t, err)

	_, err = conn.Current.Exec(ctx, "BEGIN;")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "INSERT INTO prepared_duplicate_items VALUES (2);")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "PREPARE TRANSACTION 'dg_prepared_duplicate';")
	require.ErrorContains(t, err, "already exists")
	_, err = conn.Current.Exec(ctx, "ROLLBACK;")
	require.NoError(t, err)

	_, err = conn.Current.Exec(ctx, "COMMIT PREPARED 'dg_prepared_duplicate';")
	require.NoError(t, err)

	var count int64
	require.NoError(t, conn.Current.QueryRow(ctx, "SELECT count(*) FROM prepared_duplicate_items;").Scan(&count))
	require.EqualValues(t, 1, count)
}

func TestMultiplePreparedTransactionsSurviveRestart(t *testing.T) {
	dbDir, err := os.MkdirTemp(os.TempDir(), t.Name())
	require.NoError(t, err)

	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerLocalInDirWithPort(t, "postgres", dbDir, port)
	_, err = conn.Current.Exec(ctx, "CREATE TABLE prepared_multiple_items (id INT PRIMARY KEY, label TEXT);")
	require.NoError(t, err)
	for _, query := range []string{
		"BEGIN;",
		"INSERT INTO prepared_multiple_items VALUES (1, 'one');",
		"PREPARE TRANSACTION 'dg_prepared_multiple_one';",
		"BEGIN;",
		"INSERT INTO prepared_multiple_items VALUES (2, 'two');",
		"PREPARE TRANSACTION 'dg_prepared_multiple_two';",
	} {
		_, err = conn.Current.Exec(ctx, query)
		require.NoError(t, err, query)
	}

	conn.Close(ctx)
	controller.Stop()
	require.NoError(t, controller.WaitForStop())
	sessionstate.ResetPreparedTransactionsForTests()

	ctx, conn, controller = CreateServerLocalInDirWithPort(t, "postgres", dbDir, port)
	defer func() {
		conn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	var count int64
	require.NoError(t, conn.Current.QueryRow(ctx, `
		SELECT count(*)
		FROM pg_catalog.pg_prepared_xacts
		WHERE gid LIKE 'dg_prepared_multiple_%';`).Scan(&count))
	require.EqualValues(t, 2, count)

	_, err = conn.Current.Exec(ctx, "COMMIT PREPARED 'dg_prepared_multiple_two';")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "COMMIT PREPARED 'dg_prepared_multiple_one';")
	require.NoError(t, err)
	require.NoError(t, conn.Current.QueryRow(ctx, "SELECT count(*) FROM prepared_multiple_items;").Scan(&count))
	require.EqualValues(t, 2, count)
}

func TestRecoveredPreparedTransactionErrorsWhenDatabaseMissing(t *testing.T) {
	dbDir, err := os.MkdirTemp(os.TempDir(), t.Name())
	require.NoError(t, err)

	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerLocalInDirWithPort(t, "prepared_missing_db", dbDir, port)
	_, err = conn.Current.Exec(ctx, "CREATE TABLE prepared_missing_items (id INT PRIMARY KEY);")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "BEGIN;")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "INSERT INTO prepared_missing_items VALUES (1);")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "PREPARE TRANSACTION 'dg_prepared_missing_database';")
	require.NoError(t, err)

	conn.Close(ctx)
	controller.Stop()
	require.NoError(t, controller.WaitForStop())
	sessionstate.ResetPreparedTransactionsForTests()
	require.NoError(t, os.RemoveAll(filepath.Join(dbDir, "prepared_missing_db")))

	ctx, conn, controller = CreateServerLocalInDirWithPort(t, "postgres", dbDir, port)
	defer func() {
		conn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	_, err = conn.Current.Exec(ctx, "COMMIT PREPARED 'dg_prepared_missing_database';")
	require.ErrorContains(t, err, "database not found")
	_, err = conn.Current.Exec(ctx, "ROLLBACK PREPARED 'dg_prepared_missing_database';")
	require.NoError(t, err)
}
