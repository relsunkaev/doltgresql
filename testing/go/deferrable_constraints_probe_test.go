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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
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
					Query:       `INSERT INTO c VALUES (1, 999);`,
					ExpectedErr: "Foreign key violation",
				},
			},
		},
		{
			// SET CONSTRAINTS ALL DEFERRED is the runtime toggle
			// applications use to switch deferrable constraints
			// on/off mid-transaction. Doltgres accepts the statement
			// for dump and migration compatibility, but the
			// enforcement mode remains immediate as pinned above.
			Name:        "SET CONSTRAINTS ALL DEFERRED is accepted as no-op",
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
						ORDER BY conname;`,
					Expected: []sql.Row{
						{"child_deferred_catalog_parent_fk", "t", "t"},
						{"child_immediate_catalog_parent_fk", "t", "f"},
						{"child_not_deferrable_catalog_parent_fk", "f", "f"},
					},
				},
				{
					Query: `SELECT conname, pg_get_constraintdef(oid)
						FROM pg_constraint
						WHERE conname IN (
							'child_deferred_catalog_parent_fk',
							'child_immediate_catalog_parent_fk',
							'child_not_deferrable_catalog_parent_fk'
						)
						ORDER BY conname;`,
					Expected: []sql.Row{
						{
							"child_deferred_catalog_parent_fk",
							"FOREIGN KEY (parent_id) REFERENCES parent_catalog(id) DEFERRABLE INITIALLY DEFERRED",
						},
						{
							"child_immediate_catalog_parent_fk",
							"FOREIGN KEY (parent_id) REFERENCES parent_catalog(id) DEFERRABLE",
						},
						{
							"child_not_deferrable_catalog_parent_fk",
							"FOREIGN KEY (parent_id) REFERENCES parent_catalog(id)",
						},
					},
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

func requireForeignKeyViolation(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr)
	require.Equal(t, "23503", pgErr.Code)
}
