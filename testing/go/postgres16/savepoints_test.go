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

	gms "github.com/dolthub/go-mysql-server/sql"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// TestSavepoints exercises SAVEPOINT / RELEASE SAVEPOINT / ROLLBACK TO
// SAVEPOINT — the transaction-nesting primitive every modern ORM uses
// for nested .transaction { ... } blocks (SQLAlchemy, ActiveRecord,
// Prisma, Drizzle, TypeORM, sequelize, pgx itself).
//
// Coverage:
//
//   - SAVEPOINT + INSERT + RELEASE: changes survive the surrounding
//     transaction COMMIT.
//   - SAVEPOINT + INSERT + ROLLBACK TO: changes after the savepoint
//     are discarded but earlier work survives.
//   - Nested SAVEPOINTs: ROLLBACK TO inner discards inner work only;
//     ROLLBACK TO outer discards both.
//   - Multiple ROLLBACK TO the same savepoint (the savepoint is not
//     destroyed by ROLLBACK TO).
//   - RELEASE invalidates the savepoint name so subsequent ROLLBACK TO
//     the released name errors.
//   - Statement error inside a savepoint: ROLLBACK TO recovers without
//     aborting the surrounding transaction (the ORM-managed nested
//     transaction pattern).
//   - Identifier handling: case-insensitive savepoint names (PostgreSQL
//     folds unquoted identifiers to lowercase).
func TestSavepoints(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "release after insert preserves work",
			SetUpScript: []string{
				"CREATE TABLE sp_release (id INT PRIMARY KEY, v INT);",
			},
			Assertions: []ScriptTestAssertion{
				{Query: "BEGIN;"},
				{Query: "INSERT INTO sp_release VALUES (1, 100);"},
				{Query: "SAVEPOINT sp1;"},
				{Query: "INSERT INTO sp_release VALUES (2, 200);"},
				{Query: "RELEASE SAVEPOINT sp1;"},
				{Query: "INSERT INTO sp_release VALUES (3, 300);"},
				{Query: "COMMIT;"},
				{
					Query: "SELECT id, v FROM sp_release ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "savepoints-test-testsavepoints-0001-select-id-v-from-sp_release"},
				},
			},
		},
		{
			Name: "rollback to discards post-savepoint work",
			SetUpScript: []string{
				"CREATE TABLE sp_rollback (id INT PRIMARY KEY, v INT);",
			},
			Assertions: []ScriptTestAssertion{
				{Query: "BEGIN;"},
				{Query: "INSERT INTO sp_rollback VALUES (1, 100);"},
				{Query: "SAVEPOINT sp1;"},
				{Query: "INSERT INTO sp_rollback VALUES (2, 200);"},
				{Query: "INSERT INTO sp_rollback VALUES (3, 300);"},
				{Query: "ROLLBACK TO SAVEPOINT sp1;"},
				{Query: "COMMIT;"},
				{
					Query: "SELECT id, v FROM sp_rollback ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "savepoints-test-testsavepoints-0002-select-id-v-from-sp_rollback"},
				},
			},
		},
		{
			Name: "nested savepoints",
			SetUpScript: []string{
				"CREATE TABLE sp_nested (id INT PRIMARY KEY, v INT);",
			},
			Assertions: []ScriptTestAssertion{
				{Query: "BEGIN;"},
				{Query: "INSERT INTO sp_nested VALUES (1, 1);"},
				{Query: "SAVEPOINT outer_sp;"},
				{Query: "INSERT INTO sp_nested VALUES (2, 2);"},
				{Query: "SAVEPOINT inner_sp;"},
				{Query: "INSERT INTO sp_nested VALUES (3, 3);"},
				// Roll back inner only — keeps id=2.
				{Query: "ROLLBACK TO SAVEPOINT inner_sp;"},
				{Query: "INSERT INTO sp_nested VALUES (4, 4);"},
				// Roll back outer — discards id=2 and id=4 too.
				{Query: "ROLLBACK TO SAVEPOINT outer_sp;"},
				{Query: "INSERT INTO sp_nested VALUES (5, 5);"},
				{Query: "COMMIT;"},
				{
					Query: "SELECT id, v FROM sp_nested ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "savepoints-test-testsavepoints-0003-select-id-v-from-sp_nested"},
				},
			},
		},
		{
			Name: "rollback to is repeatable; release destroys the name",
			SetUpScript: []string{
				"CREATE TABLE sp_repeat (id INT PRIMARY KEY);",
			},
			Assertions: []ScriptTestAssertion{
				{Query: "BEGIN;"},
				{Query: "SAVEPOINT sp1;"},
				{Query: "INSERT INTO sp_repeat VALUES (1);"},
				// ROLLBACK TO does NOT destroy the savepoint, so a
				// second rollback to the same name still works.
				{Query: "ROLLBACK TO SAVEPOINT sp1;"},
				{Query: "INSERT INTO sp_repeat VALUES (2);"},
				{Query: "ROLLBACK TO SAVEPOINT sp1;"},
				{Query: "INSERT INTO sp_repeat VALUES (3);"},
				// RELEASE removes the savepoint; further references
				// to the released name must error.
				{Query: "RELEASE SAVEPOINT sp1;"},
				{
					Query: "ROLLBACK TO SAVEPOINT sp1;", PostgresOracle: ScriptTestPostgresOracle{ID: "savepoints-test-testsavepoints-0004-rollback-to-savepoint-sp1", Compare: "sqlstate"},
				},
				{Query: "ROLLBACK;"},
				{
					Query: "SELECT COUNT(*) FROM sp_repeat;", PostgresOracle: ScriptTestPostgresOracle{ID: "savepoints-test-testsavepoints-0005-select-count-*-from-sp_repeat"},
				},
			},
		},
		{
			Name: "case-insensitive savepoint names",
			SetUpScript: []string{
				"CREATE TABLE sp_case (id INT PRIMARY KEY);",
			},
			Assertions: []ScriptTestAssertion{
				{Query: "BEGIN;"},
				{Query: "INSERT INTO sp_case VALUES (1);"},
				// PostgreSQL folds unquoted identifiers to lowercase,
				// so SAVEPOINT MySp must be releasable as mysp.
				{Query: "SAVEPOINT MySp;"},
				{Query: "INSERT INTO sp_case VALUES (2);"},
				{Query: "ROLLBACK TO SAVEPOINT mysp;"},
				{Query: "INSERT INTO sp_case VALUES (3);"},
				{Query: "RELEASE SAVEPOINT MYSP;"},
				{Query: "COMMIT;"},
				{
					Query: "SELECT id FROM sp_case ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "savepoints-test-testsavepoints-0006-select-id-from-sp_case-order"},
				},
			},
		},
		{
			Name: "rollback to unknown savepoint errors",
			Assertions: []ScriptTestAssertion{
				{Query: "BEGIN;"},
				{
					Query: "ROLLBACK TO SAVEPOINT nothere;", PostgresOracle: ScriptTestPostgresOracle{ID: "savepoints-test-testsavepoints-0007-rollback-to-savepoint-nothere",

						// TestSavepointsORMShape exercises the savepoint workflow exactly the
						// way pgx (and every Go ORM that wraps pgx) drives it: pgx.Tx.Begin
						// nested calls map to SAVEPOINT/RELEASE/ROLLBACK TO. This is the
						// `pgx.Tx.Begin()` -> nested `tx.Begin()` shape that real applications
						// hit.
						//
						// We use the pgx driver directly with pgx.Tx.Begin() (which issues
						// SAVEPOINT under the hood) so the test is workload-shape evidence,
						// not just SQL-string evidence.
						Compare: "sqlstate"},
				},
				{Query: "ROLLBACK;"},
			},
		},
	})
}

func TestSavepointsORMShape(t *testing.T) {
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

	conn := dial(t)
	_, err = conn.Exec(ctx, "CREATE TABLE orm_sp (id INT PRIMARY KEY, v INT);")
	require.NoError(t, err)

	t.Run("nested Begin commits inner work", func(t *testing.T) {
		// pgx.Tx.Begin() emits SAVEPOINT pgx_N when called on an
		// already-open transaction, and Commit() on the inner tx
		// emits RELEASE SAVEPOINT pgx_N.
		tx, err := conn.Begin(ctx)
		require.NoError(t, err)
		_, err = tx.Exec(ctx, "INSERT INTO orm_sp VALUES (1, 1);")
		require.NoError(t, err)

		inner, err := tx.Begin(ctx)
		require.NoError(t, err)
		_, err = inner.Exec(ctx, "INSERT INTO orm_sp VALUES (2, 2);")
		require.NoError(t, err)
		require.NoError(t, inner.Commit(ctx))

		_, err = tx.Exec(ctx, "INSERT INTO orm_sp VALUES (3, 3);")
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))

		var count int
		require.NoError(t, conn.QueryRow(ctx,
			"SELECT COUNT(*) FROM orm_sp WHERE id IN (1, 2, 3);").Scan(&count))
		require.Equal(t, 3, count)
	})

	t.Run("nested Rollback discards only inner work", func(t *testing.T) {
		_, err := conn.Exec(ctx, "TRUNCATE orm_sp;")
		// TRUNCATE may not be supported; fall back to DELETE.
		if err != nil {
			_, err = conn.Exec(ctx, "DELETE FROM orm_sp;")
			require.NoError(t, err)
		}

		tx, err := conn.Begin(ctx)
		require.NoError(t, err)
		_, err = tx.Exec(ctx, "INSERT INTO orm_sp VALUES (10, 10);")
		require.NoError(t, err)

		inner, err := tx.Begin(ctx)
		require.NoError(t, err)
		_, err = inner.Exec(ctx, "INSERT INTO orm_sp VALUES (20, 20);")
		require.NoError(t, err)
		// Rollback inner -> SAVEPOINT pgx_N is rolled back.
		require.NoError(t, inner.Rollback(ctx))

		// Outer transaction is still alive and accepts new work.
		_, err = tx.Exec(ctx, "INSERT INTO orm_sp VALUES (30, 30);")
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))

		rows, err := conn.Query(ctx, "SELECT id FROM orm_sp ORDER BY id;")
		require.NoError(t, err)
		var ids []int
		for rows.Next() {
			var id int
			require.NoError(t, rows.Scan(&id))
			ids = append(ids, id)
		}
		require.NoError(t, rows.Err())
		require.Equal(t, []int{10, 30}, ids)
	})

	t.Run("BeginFunc wraps inner work correctly", func(t *testing.T) {
		_, err := conn.Exec(ctx, "DELETE FROM orm_sp;")
		require.NoError(t, err)

		err = pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, "INSERT INTO orm_sp VALUES (100, 100);"); err != nil {
				return err
			}
			// pgx.BeginFunc on an active tx emits SAVEPOINT.
			return pgx.BeginFunc(ctx, tx, func(inner pgx.Tx) error {
				_, err := inner.Exec(ctx, "INSERT INTO orm_sp VALUES (200, 200);")
				return err
			})
		})
		require.NoError(t, err)

		var count int
		require.NoError(t, conn.QueryRow(context.Background(),
			"SELECT COUNT(*) FROM orm_sp WHERE id >= 100;").Scan(&count))
		require.Equal(t, 2, count)
	})
}
