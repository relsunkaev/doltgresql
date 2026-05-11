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
	"github.com/stretchr/testify/require"
)

// TestTemporaryTableOnCommitDeleteRowsRepro reproduces a temp-table persistence
// gap: PostgreSQL supports ON COMMIT DELETE ROWS for transaction-scoped temp
// table contents.
func TestTemporaryTableOnCommitDeleteRowsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "temporary table ON COMMIT DELETE ROWS clears rows at commit",
			SetUpScript: []string{
				`CREATE TEMPORARY TABLE temp_on_commit_delete_items (
					id INT
				) ON COMMIT DELETE ROWS;`,
				`BEGIN;`,
				`INSERT INTO temp_on_commit_delete_items VALUES (1), (2);`,
				`COMMIT;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT count(*) FROM temp_on_commit_delete_items;`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
	})
}

// TestTemporaryTableOnCommitDropRepro reproduces a temp-table persistence gap:
// PostgreSQL supports ON COMMIT DROP for transaction-scoped temp table
// lifetime.
func TestTemporaryTableOnCommitDropRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "temporary table ON COMMIT DROP drops table at commit",
			SetUpScript: []string{
				`BEGIN;`,
				`CREATE TEMPORARY TABLE temp_on_commit_drop_items (
						id INT
					) ON COMMIT DROP;`,
				`INSERT INTO temp_on_commit_drop_items VALUES (1);`,
				`COMMIT;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `SELECT count(*) FROM temp_on_commit_drop_items;`,
					ExpectedErr: `does not exist`,
				},
			},
		},
	})
}

// TestTemporaryTableRejectsPersistentSchemaRepro reproduces a namespace
// correctness bug: PostgreSQL rejects temporary tables explicitly created in a
// persistent schema.
func TestTemporaryTableRejectsPersistentSchemaRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "temporary table rejects persistent schema",
			Assertions: []ScriptTestAssertion{
				{
					Query:       `CREATE TEMPORARY TABLE public.temp_in_public_items (id INT);`,
					ExpectedErr: `cannot create temporary relation in non-temporary schema`,
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_class c
						JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
						WHERE c.relname = 'temp_in_public_items'
							AND n.nspname = 'public';`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
	})
}

// TestTemporaryTablesAreSessionLocalGuard guards PostgreSQL temporary-table
// isolation: a temp table created in one session must not be visible in another
// session.
func TestTemporaryTablesAreSessionLocalGuard(t *testing.T) {
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)
	ctx, defaultConn, controller := CreateServerWithPort(t, "postgres", port)
	t.Cleanup(func() {
		defaultConn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	for _, stmt := range []string{
		`CREATE TEMPORARY TABLE session_temp_items (id INT);`,
		`INSERT INTO session_temp_items VALUES (1);`,
	} {
		_, err = defaultConn.Exec(ctx, stmt)
		require.NoError(t, err, stmt)
	}

	var ownerCount int
	require.NoError(t, defaultConn.Current.QueryRow(ctx,
		`SELECT count(*) FROM session_temp_items;`,
	).Scan(&ownerCount))
	require.Equal(t, 1, ownerCount)

	otherConn, err := pgx.Connect(ctx, fmt.Sprintf(
		"postgres://postgres:password@127.0.0.1:%d/postgres?sslmode=disable",
		port,
	))
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = otherConn.Close(context.Background())
	})

	var otherCount int
	err = otherConn.QueryRow(ctx, `SELECT count(*) FROM session_temp_items;`).Scan(&otherCount)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not found")
}

// TestCreateTemporaryTableAsIsSessionLocalRepro guards that CTAS preserves the
// requested temporary-table lifetime and does not create a persistent table.
func TestCreateTemporaryTableAsIsSessionLocalRepro(t *testing.T) {
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)
	ctx, defaultConn, controller := CreateServerWithPort(t, "postgres", port)
	t.Cleanup(func() {
		defaultConn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	_, err = defaultConn.Exec(ctx, `CREATE TEMPORARY TABLE temp_ctas_session_items AS
		SELECT 1 AS id;`)
	require.NoError(t, err)

	var ownerCount int
	require.NoError(t, defaultConn.Current.QueryRow(ctx,
		`SELECT count(*) FROM temp_ctas_session_items;`,
	).Scan(&ownerCount))
	require.Equal(t, 1, ownerCount)

	otherConn, err := pgx.Connect(ctx, fmt.Sprintf(
		"postgres://postgres:password@127.0.0.1:%d/postgres?sslmode=disable",
		port,
	))
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = otherConn.Close(context.Background())
	})

	var otherCount int
	err = otherConn.QueryRow(ctx, `SELECT count(*) FROM temp_ctas_session_items;`).Scan(&otherCount)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not found")
}

// TestExplicitPgTempFunctionLookupResolvesTemporaryFunctionRepro reproduces a
// function lookup correctness bug: explicitly schema-qualified pg_temp function
// calls should resolve to the temp-schema function, not a same-name public
// function.
func TestExplicitPgTempFunctionLookupResolvesTemporaryFunctionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "explicit pg_temp function lookup resolves temporary function",
			SetUpScript: []string{
				`CREATE FUNCTION public.pgtemp_lookup_probe() RETURNS TEXT AS $$
					SELECT 'public'::text
				$$ LANGUAGE sql;`,
				`CREATE FUNCTION pg_temp.pgtemp_lookup_probe() RETURNS TEXT AS $$
					SELECT 'temp'::text
				$$ LANGUAGE sql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `RESET search_path;`,
				},
				{
					Query:    `SELECT pgtemp_lookup_probe();`,
					Expected: []sql.Row{{"public"}},
				},
				{
					Query: `SET search_path = pg_temp, public;`,
				},
				{
					Query:    `SELECT pgtemp_lookup_probe();`,
					Expected: []sql.Row{{"public"}},
				},
				{
					Query:    `SELECT pg_temp.pgtemp_lookup_probe();`,
					Expected: []sql.Row{{"temp"}},
				},
			},
		},
	})
}

// TestTemporaryTableShadowsPersistentTableRepro reproduces a temp-table
// namespace correctness bug: PostgreSQL lets a temporary table shadow a
// persistent table with the same name within only the creating session.
func TestTemporaryTableShadowsPersistentTableRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "temporary table shadows persistent table in same session",
			SetUpScript: []string{
				`CREATE TABLE temp_shadow_items (id INT PRIMARY KEY, label TEXT);`,
				`INSERT INTO temp_shadow_items VALUES (1, 'persistent');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TEMPORARY TABLE temp_shadow_items (id INT PRIMARY KEY, label TEXT);`,
				},
				{
					Query: `INSERT INTO temp_shadow_items VALUES (2, 'temporary');`,
				},
				{
					Query:    `SELECT id, label FROM temp_shadow_items;`,
					Expected: []sql.Row{{2, "temporary"}},
				},
				{
					Query:    `SELECT id, label FROM public.temp_shadow_items;`,
					Expected: []sql.Row{{1, "persistent"}},
				},
				{
					Query: `DROP TABLE temp_shadow_items;`,
				},
				{
					Query:    `SELECT id, label FROM temp_shadow_items;`,
					Expected: []sql.Row{{1, "persistent"}},
				},
			},
		},
	})
}

// TestSchemaQualifiedInsertIgnoresTemporaryTableShadowRepro reproduces a
// namespace correctness bug with data-corruption risk: a schema-qualified write
// to the persistent table should not be redirected to a same-name temporary
// table.
func TestSchemaQualifiedInsertIgnoresTemporaryTableShadowRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "schema-qualified insert targets persistent table despite temporary shadow",
			SetUpScript: []string{
				`CREATE TABLE temp_shadow_insert_items (id INT PRIMARY KEY, label TEXT);`,
				`INSERT INTO temp_shadow_insert_items VALUES (1, 'persistent');`,
				`CREATE TEMPORARY TABLE temp_shadow_insert_items (id INT PRIMARY KEY, label TEXT);`,
				`INSERT INTO temp_shadow_insert_items VALUES (2, 'temporary');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO public.temp_shadow_insert_items VALUES (3, 'qualified persistent insert');`,
				},
				{
					Query: `DROP TABLE temp_shadow_insert_items;`,
				},
				{
					Query: `SELECT id, label
						FROM temp_shadow_insert_items
						ORDER BY id;`,
					Expected: []sql.Row{
						{1, "persistent"},
						{3, "qualified persistent insert"},
					},
				},
			},
		},
	})
}

// TestSchemaQualifiedUpdateIgnoresTemporaryTableShadowRepro reproduces a
// namespace correctness bug with data-corruption risk: a schema-qualified
// update to the persistent table should not update a same-name temporary table.
func TestSchemaQualifiedUpdateIgnoresTemporaryTableShadowRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "schema-qualified update targets persistent table despite temporary shadow",
			SetUpScript: []string{
				`CREATE TABLE temp_shadow_update_items (id INT PRIMARY KEY, label TEXT);`,
				`INSERT INTO temp_shadow_update_items VALUES (1, 'persistent');`,
				`CREATE TEMPORARY TABLE temp_shadow_update_items (id INT PRIMARY KEY, label TEXT);`,
				`INSERT INTO temp_shadow_update_items VALUES (1, 'temporary');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE public.temp_shadow_update_items
						SET label = 'qualified persistent update'
						WHERE id = 1;`,
				},
				{
					Query: `DROP TABLE temp_shadow_update_items;`,
				},
				{
					Query:    `SELECT id, label FROM temp_shadow_update_items;`,
					Expected: []sql.Row{{1, "qualified persistent update"}},
				},
			},
		},
	})
}

// TestSchemaQualifiedDeleteIgnoresTemporaryTableShadowRepro reproduces a
// namespace correctness bug with data-loss risk: a schema-qualified delete from
// the persistent table should not delete from a same-name temporary table.
func TestSchemaQualifiedDeleteIgnoresTemporaryTableShadowRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "schema-qualified delete targets persistent table despite temporary shadow",
			SetUpScript: []string{
				`CREATE TABLE temp_shadow_delete_items (id INT PRIMARY KEY, label TEXT);`,
				`INSERT INTO temp_shadow_delete_items VALUES (1, 'persistent');`,
				`CREATE TEMPORARY TABLE temp_shadow_delete_items (id INT PRIMARY KEY, label TEXT);`,
				`INSERT INTO temp_shadow_delete_items VALUES (2, 'temporary');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DELETE FROM public.temp_shadow_delete_items WHERE id = 1;`,
				},
				{
					Query: `DROP TABLE temp_shadow_delete_items;`,
				},
				{
					Query:    `SELECT count(*) FROM temp_shadow_delete_items;`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
	})
}

// TestSchemaQualifiedAlterIgnoresTemporaryTableShadowRepro reproduces a
// namespace correctness bug: a schema-qualified ALTER TABLE should alter the
// persistent table, not resolve to a same-name temporary table and fail.
func TestSchemaQualifiedAlterIgnoresTemporaryTableShadowRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "schema-qualified alter targets persistent table despite temporary shadow",
			SetUpScript: []string{
				`CREATE TABLE temp_shadow_alter_items (id INT PRIMARY KEY);`,
				`INSERT INTO temp_shadow_alter_items VALUES (1);`,
				`CREATE TEMPORARY TABLE temp_shadow_alter_items (id INT PRIMARY KEY);`,
				`INSERT INTO temp_shadow_alter_items VALUES (2);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE public.temp_shadow_alter_items
						ADD COLUMN marker TEXT DEFAULT 'qualified persistent alter';`,
				},
				{
					Query: `DROP TABLE temp_shadow_alter_items;`,
				},
				{
					Query:    `SELECT id, marker FROM temp_shadow_alter_items;`,
					Expected: []sql.Row{{1, "qualified persistent alter"}},
				},
			},
		},
	})
}

// TestSchemaQualifiedTruncateIgnoresTemporaryTableShadowRepro reproduces a
// namespace correctness bug: a schema-qualified TRUNCATE should truncate the
// persistent table, not resolve to a same-name temporary table and fail.
func TestSchemaQualifiedTruncateIgnoresTemporaryTableShadowRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "schema-qualified truncate targets persistent table despite temporary shadow",
			SetUpScript: []string{
				`CREATE TABLE temp_shadow_truncate_items (id INT PRIMARY KEY);`,
				`INSERT INTO temp_shadow_truncate_items VALUES (1);`,
				`CREATE TEMPORARY TABLE temp_shadow_truncate_items (id INT PRIMARY KEY);`,
				`INSERT INTO temp_shadow_truncate_items VALUES (2);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `TRUNCATE TABLE public.temp_shadow_truncate_items;`,
				},
				{
					Query: `DROP TABLE temp_shadow_truncate_items;`,
				},
				{
					Query:    `SELECT count(*) FROM temp_shadow_truncate_items;`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
	})
}

// TestSchemaQualifiedDropIgnoresTemporaryTableShadowRepro reproduces a
// namespace correctness bug with data-loss risk: DROP TABLE public.name should
// drop the persistent table, not a same-name temporary table in the session.
func TestSchemaQualifiedDropIgnoresTemporaryTableShadowRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "schema-qualified drop targets persistent table despite temporary shadow",
			SetUpScript: []string{
				`CREATE TABLE temp_shadow_drop_items (id INT PRIMARY KEY, label TEXT);`,
				`INSERT INTO temp_shadow_drop_items VALUES (1, 'persistent');`,
				`CREATE TEMPORARY TABLE temp_shadow_drop_items (id INT PRIMARY KEY, label TEXT);`,
				`INSERT INTO temp_shadow_drop_items VALUES (2, 'temporary');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP TABLE public.temp_shadow_drop_items;`,
				},
				{
					Query:    `SELECT id, label FROM temp_shadow_drop_items;`,
					Expected: []sql.Row{{2, "temporary"}},
				},
				{
					Query: `DROP TABLE temp_shadow_drop_items;`,
				},
				{
					Query:       `SELECT id, label FROM public.temp_shadow_drop_items;`,
					ExpectedErr: `not found`,
				},
			},
		},
	})
}
