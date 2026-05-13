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
	"os"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/doltgresql/server/deferrable"
)

// TestDeferrableConstraintsProbe pins how DEFERRABLE FK DDL behaves
// today so we know exactly what shapes silently fall through to
// immediate-check semantics versus what hard-rejects. PG semantics:
// `DEFERRABLE INITIALLY DEFERRED` defers FK validation to commit time;
// `SET CONSTRAINTS ALL DEFERRED` toggles at runtime. Per the
// Schema/DDL TODO in docs/app-compatibility-checklist.md.
func TestDeferrableConstraintsProbe(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "DEFERRABLE keyword acceptance probe",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					// We expect this to either land cleanly or reject
					// with a clear error. ExpectedErr matches a
					// substring; if both work the test must be
					// updated to assert the correct semantics.
					Query: `CREATE TABLE parent (id INT PRIMARY KEY);`,
				},
				{
					Query: `CREATE TABLE child (
						id INT PRIMARY KEY,
						parent_id INT REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED
					);`,
				},
			},
		},
		{
			// In autocommit mode the statement boundary is also the
			// transaction boundary, so a deferred FK violation is still
			// visible to the client on the INSERT statement.
			Name: "DEFERRED FK autocommit violation is rejected at statement boundary",
			SetUpScript: []string{
				`CREATE TABLE p (id INT PRIMARY KEY);`,
				`CREATE TABLE c (
					id INT PRIMARY KEY,
					pid INT REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO c VALUES (1, 999);`, PostgresOracle: ScriptTestPostgresOracle{ID: "deferrable-constraints-probe-test-testdeferrableconstraintsprobe-0001-insert-into-c-values-1", Compare: "sqlstate"},
				},
			},
		},
		{
			// SET CONSTRAINTS ALL DEFERRED is the runtime toggle
			// applications use to switch deferrable constraints
			// on/off mid-transaction. Outside an explicit transaction
			// it has no lasting effect, but PostgreSQL accepts it.
			Name:        "SET CONSTRAINTS ALL DEFERRED is accepted outside transaction",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SET CONSTRAINTS ALL DEFERRED;`,
				},
			},
		},
		{
			Name: "pg_constraint exposes DEFERRABLE timing flags",
			SetUpScript: []string{
				`CREATE TABLE parent_catalog (id INT PRIMARY KEY);`,
				`CREATE TABLE child_deferred_catalog (
					id INT PRIMARY KEY,
					parent_id INT,
					CONSTRAINT child_deferred_catalog_parent_fk
						FOREIGN KEY (parent_id) REFERENCES parent_catalog(id)
						DEFERRABLE INITIALLY DEFERRED
				);`,
				`CREATE TABLE child_immediate_catalog (
					id INT PRIMARY KEY,
					parent_id INT,
					CONSTRAINT child_immediate_catalog_parent_fk
						FOREIGN KEY (parent_id) REFERENCES parent_catalog(id)
						DEFERRABLE INITIALLY IMMEDIATE
				);`,
				`CREATE TABLE child_not_deferrable_catalog (
					id INT PRIMARY KEY,
					parent_id INT,
					CONSTRAINT child_not_deferrable_catalog_parent_fk
						FOREIGN KEY (parent_id) REFERENCES parent_catalog(id)
						NOT DEFERRABLE
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT conname, condeferrable, condeferred
						FROM pg_constraint
						WHERE conname IN (
							'child_deferred_catalog_parent_fk',
							'child_immediate_catalog_parent_fk',
							'child_not_deferrable_catalog_parent_fk'
						)
						ORDER BY conname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "deferrable-constraints-probe-test-testdeferrableconstraintsprobe-0002-select-conname-condeferrable-condeferred-from"},
				},
				{
					Query: `SELECT conname, pg_get_constraintdef(oid)
						FROM pg_constraint
						WHERE conname IN (
							'child_deferred_catalog_parent_fk',
							'child_immediate_catalog_parent_fk',
							'child_not_deferrable_catalog_parent_fk'
						)
						ORDER BY conname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "deferrable-constraints-probe-test-testdeferrableconstraintsprobe-0003-select-conname-pg_get_constraintdef-oid-from"},
				},
			},
		},
	})
}

func TestDeferrableForeignKeyTransactionSemantics(t *testing.T) {
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)
	ctx, defaultConn, controller := CreateServerWithPort(t, "postgres", port)
	t.Cleanup(func() {
		defaultConn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	conn, err := pgx.Connect(ctx, fmt.Sprintf(
		"postgres://postgres:password@127.0.0.1:%d/postgres?sslmode=disable",
		port,
	))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, conn.Close(context.Background()))
	})

	for _, stmt := range []string{
		`CREATE TABLE parent_deferred (id INT PRIMARY KEY)`,
		`CREATE TABLE child_deferred (
			id INT PRIMARY KEY,
			parent_id INT,
			CONSTRAINT child_deferred_parent_fk
				FOREIGN KEY (parent_id) REFERENCES parent_deferred(id)
				DEFERRABLE INITIALLY DEFERRED
		)`,
		`CREATE TABLE child_recreated (
			id INT PRIMARY KEY,
			parent_id INT,
			CONSTRAINT child_recreated_parent_fk
				FOREIGN KEY (parent_id) REFERENCES parent_deferred(id)
				DEFERRABLE INITIALLY DEFERRED
		)`,
		`CREATE TABLE child_immediate (
			id INT PRIMARY KEY,
			parent_id INT,
			CONSTRAINT child_immediate_parent_fk
				FOREIGN KEY (parent_id) REFERENCES parent_deferred(id)
				DEFERRABLE INITIALLY IMMEDIATE
		)`,
	} {
		_, err = conn.Exec(ctx, stmt)
		require.NoError(t, err, stmt)
	}

	_, err = conn.Exec(ctx, `BEGIN`)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, `INSERT INTO child_deferred VALUES (1, 10)`)
	require.NoError(t, err, "DEFERRABLE INITIALLY DEFERRED must not reject before commit")
	_, err = conn.Exec(ctx, `INSERT INTO parent_deferred VALUES (10)`)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, `COMMIT`)
	require.NoError(t, err)

	var count int
	err = conn.QueryRow(ctx, `SELECT count(*) FROM child_deferred`).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count)

	_, err = conn.Exec(ctx, `BEGIN`)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, `INSERT INTO child_deferred VALUES (2, 20)`)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, `COMMIT`)
	require.Error(t, err)
	requireForeignKeyViolation(t, err)

	err = conn.QueryRow(ctx, `SELECT count(*) FROM child_deferred`).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count)

	_, err = conn.Exec(ctx, `BEGIN`)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, `UPDATE parent_deferred SET id = 70 WHERE id = 10`)
	require.NoError(t, err, "DEFERRABLE INITIALLY DEFERRED parent-key updates should wait until commit")
	_, err = conn.Exec(ctx, `UPDATE child_deferred SET parent_id = 70 WHERE id = 1`)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, `COMMIT`)
	require.NoError(t, err)

	_, err = conn.Exec(ctx, `BEGIN`)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, `DELETE FROM parent_deferred WHERE id = 70`)
	require.NoError(t, err, "DEFERRABLE INITIALLY DEFERRED parent deletes should wait until commit")
	_, err = conn.Exec(ctx, `COMMIT`)
	requireForeignKeyViolation(t, err)

	err = conn.QueryRow(ctx, `SELECT count(*) FROM parent_deferred WHERE id = 70`).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count)

	_, err = conn.Exec(ctx, `BEGIN`)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, `DELETE FROM parent_deferred WHERE id = 70`)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, `DELETE FROM child_deferred WHERE id = 1`)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, `COMMIT`)
	require.NoError(t, err)

	_, err = conn.Exec(ctx, `BEGIN`)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, `SET CONSTRAINTS ALL DEFERRED`)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, `INSERT INTO child_immediate VALUES (1, 40)`)
	require.NoError(t, err, "SET CONSTRAINTS ALL DEFERRED must defer DEFERRABLE INITIALLY IMMEDIATE checks")
	_, err = conn.Exec(ctx, `INSERT INTO parent_deferred VALUES (40)`)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, `COMMIT`)
	require.NoError(t, err)

	_, err = conn.Exec(ctx, `BEGIN`)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, `SET CONSTRAINTS child_immediate_parent_fk DEFERRED`)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, `INSERT INTO child_immediate VALUES (2, 41)`)
	require.NoError(t, err, "SET CONSTRAINTS <name> DEFERRED must defer the named FK")
	_, err = conn.Exec(ctx, `INSERT INTO parent_deferred VALUES (41)`)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, `COMMIT`)
	require.NoError(t, err)

	_, err = conn.Exec(ctx, `BEGIN`)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, `SET CONSTRAINTS child_deferred_parent_fk IMMEDIATE`)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, `INSERT INTO child_deferred VALUES (5, 60)`)
	requireForeignKeyViolation(t, err)
	_, err = conn.Exec(ctx, `ROLLBACK`)
	require.NoError(t, err)

	_, err = conn.Exec(ctx, `BEGIN`)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, `SET CONSTRAINTS ALL IMMEDIATE`)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, `INSERT INTO child_deferred VALUES (3, 30)`)
	requireForeignKeyViolation(t, err)
	_, err = conn.Exec(ctx, `ROLLBACK`)
	require.NoError(t, err)

	_, err = conn.Exec(ctx, `BEGIN`)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, `INSERT INTO child_deferred VALUES (4, 50)`)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, `SET CONSTRAINTS ALL IMMEDIATE`)
	requireForeignKeyViolation(t, err)
	_, err = conn.Exec(ctx, `ROLLBACK`)
	require.NoError(t, err)

	_, err = conn.Exec(ctx, `DROP TABLE child_recreated`)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, `CREATE TABLE child_recreated (
		id INT PRIMARY KEY,
		parent_id INT,
		CONSTRAINT child_recreated_parent_fk
			FOREIGN KEY (parent_id) REFERENCES parent_deferred(id)
			NOT DEFERRABLE
	)`)
	require.NoError(t, err)

	_, err = conn.Exec(ctx, `BEGIN`)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, `INSERT INTO child_recreated VALUES (1, 30)`)
	requireForeignKeyViolation(t, err)
	_, err = conn.Exec(ctx, `ROLLBACK`)
	require.NoError(t, err)
}

func TestDeferrableForeignKeyTimingSurvivesRestart(t *testing.T) {
	dbDir, err := os.MkdirTemp(os.TempDir(), t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dbDir)

	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerLocalInDirWithPort(t, "postgres", dbDir, port)
	for _, stmt := range []string{
		`CREATE TABLE parent_restart (id INT PRIMARY KEY)`,
		`CREATE TABLE child_restart_deferred (
			id INT PRIMARY KEY,
			parent_id INT,
			CONSTRAINT child_restart_deferred_parent_fk
				FOREIGN KEY (parent_id) REFERENCES parent_restart(id)
				DEFERRABLE INITIALLY DEFERRED
		)`,
		`CREATE TABLE child_restart_immediate (
			id INT PRIMARY KEY,
			parent_id INT,
			CONSTRAINT child_restart_immediate_parent_fk
				FOREIGN KEY (parent_id) REFERENCES parent_restart(id)
				DEFERRABLE INITIALLY IMMEDIATE
		)`,
		`SELECT DOLT_COMMIT('-Am', 'deferrable fk timing restart')`,
	} {
		_, err = conn.Current.Exec(ctx, stmt)
		require.NoError(t, err, stmt)
	}
	conn.Close(ctx)
	controller.Stop()
	require.NoError(t, controller.WaitForStop())

	deferrable.ResetForTests()

	ctx, conn, controller = CreateServerLocalInDirWithPort(t, "postgres", dbDir, port)
	defer func() {
		conn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	rows, err := conn.Current.Query(ctx, `
		SELECT conname, condeferrable, condeferred, pg_get_constraintdef(oid)
		FROM pg_constraint
		WHERE conname IN (
			'child_restart_deferred_parent_fk',
			'child_restart_immediate_parent_fk'
		)
		ORDER BY conname;`)
	require.NoError(t, err)
	actual, _, err := ReadRows(rows, true)
	require.NoError(t, err)
	require.Equal(t, []sql.Row{
		{
			"child_restart_deferred_parent_fk",
			"t",
			"t",
			"FOREIGN KEY (parent_id) REFERENCES parent_restart(id) DEFERRABLE INITIALLY DEFERRED",
		},
		{
			"child_restart_immediate_parent_fk",
			"t",
			"f",
			"FOREIGN KEY (parent_id) REFERENCES parent_restart(id) DEFERRABLE",
		},
	}, actual)

	_, err = conn.Current.Exec(ctx, `BEGIN`)
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, `INSERT INTO child_restart_deferred VALUES (1, 10)`)
	require.NoError(t, err, "DEFERRABLE INITIALLY DEFERRED timing must survive restart")
	_, err = conn.Current.Exec(ctx, `INSERT INTO parent_restart VALUES (10)`)
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, `COMMIT`)
	require.NoError(t, err)

	_, err = conn.Current.Exec(ctx, `BEGIN`)
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, `SET CONSTRAINTS ALL DEFERRED`)
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, `INSERT INTO child_restart_immediate VALUES (1, 20)`)
	require.NoError(t, err, "DEFERRABLE INITIALLY IMMEDIATE timing must remain switchable after restart")
	_, err = conn.Current.Exec(ctx, `INSERT INTO parent_restart VALUES (20)`)
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, `COMMIT`)
	require.NoError(t, err)

	_, err = conn.Current.Exec(ctx, `BEGIN`)
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, `INSERT INTO child_restart_immediate VALUES (2, 30)`)
	requireForeignKeyViolation(t, err)
	_, err = conn.Current.Exec(ctx, `ROLLBACK`)
	require.NoError(t, err)
}

func TestFailedDeferrableForeignKeyDDLDoesNotPersistTiming(t *testing.T) {
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerLocalWithPort(t, "postgres", port)
	defer func() {
		conn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	for _, stmt := range []string{
		`CREATE TABLE parent_failed_ddl (id INT PRIMARY KEY)`,
		`CREATE TABLE child_failed_ddl (
			id INT PRIMARY KEY,
			parent_id INT,
			CONSTRAINT child_failed_ddl_parent_fk
				FOREIGN KEY (parent_id) REFERENCES parent_failed_ddl(id)
		)`,
	} {
		_, err = conn.Current.Exec(ctx, stmt)
		require.NoError(t, err, stmt)
	}

	_, err = conn.Current.Exec(ctx, `CREATE TABLE child_failed_ddl (
		id INT PRIMARY KEY,
		parent_id INT,
		CONSTRAINT child_failed_ddl_parent_fk
			FOREIGN KEY (parent_id) REFERENCES parent_failed_ddl(id)
			DEFERRABLE INITIALLY DEFERRED
	)`)
	require.Error(t, err)

	rows, err := conn.Current.Query(ctx, `
		SELECT condeferrable, condeferred, pg_get_constraintdef(oid)
		FROM pg_constraint
		WHERE conname = 'child_failed_ddl_parent_fk';`)
	require.NoError(t, err)
	actual, _, err := ReadRows(rows, true)
	require.NoError(t, err)
	require.Equal(t, []sql.Row{{
		"f",
		"f",
		"FOREIGN KEY (parent_id) REFERENCES parent_failed_ddl(id)",
	}}, actual)
}

func requireForeignKeyViolation(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr)
	require.Equal(t, "23503", pgErr.Code)
}
