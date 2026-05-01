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
					Query:       "PREPARE TRANSACTION 'dg_no_transaction';",
					ExpectedErr: "can only be used in transaction blocks",
				},
				{
					Query:       "COMMIT PREPARED 'dg_missing';",
					ExpectedErr: "does not exist",
				},
				{
					Query:       "ROLLBACK PREPARED 'dg_missing';",
					ExpectedErr: "does not exist",
				},
				{
					Query: "BEGIN;",
				},
				{
					Query:       "COMMIT PREPARED 'dg_missing';",
					ExpectedErr: "cannot run inside a transaction block",
				},
				{
					Query: "ROLLBACK;",
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

func TestRecoveredPreparedTransactionRejectsChangedWorkingSet(t *testing.T) {
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
	require.ErrorContains(t, err, "working set changed since PREPARE TRANSACTION")

	var count int64
	require.NoError(t, conn.Current.QueryRow(ctx, `
		SELECT count(*)
		FROM pg_catalog.pg_prepared_xacts
		WHERE gid = 'dg_prepared_restart_conflict';`).Scan(&count))
	require.EqualValues(t, 1, count)
	require.NoError(t, conn.Current.QueryRow(ctx, "SELECT count(*) FROM prepared_restart_conflict_items WHERE id = 1;").Scan(&count))
	require.EqualValues(t, 0, count)

	_, err = conn.Current.Exec(ctx, "ROLLBACK PREPARED 'dg_prepared_restart_conflict';")
	require.NoError(t, err)
}
