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
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
)

// TestColumnSelectGrantAllowsGrantedColumnsRepro reproduces an authorization
// correctness bug: PostgreSQL column-level SELECT privileges allow reading
// exactly the granted columns.
func TestColumnSelectGrantAllowsGrantedColumnsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "column SELECT grant allows granted columns",
			SetUpScript: []string{
				`CREATE USER column_select_allowed_user PASSWORD 'column';`,
				`CREATE TABLE column_select_allowed_private (
					id INT PRIMARY KEY,
					public_value TEXT,
					private_value TEXT
				);`,
				`INSERT INTO column_select_allowed_private VALUES (1, 'public', 'private');`,
				`GRANT USAGE ON SCHEMA public TO column_select_allowed_user;`,
				`GRANT SELECT (id, public_value)
					ON column_select_allowed_private TO column_select_allowed_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, public_value
						FROM column_select_allowed_private;`,
					Expected: []sql.Row{{1, "public"}},
					Username: `column_select_allowed_user`,
					Password: `column`,
				},
				{
					Query:       `SELECT private_value FROM column_select_allowed_private;`,
					ExpectedErr: `permission denied`,
					Username:    `column_select_allowed_user`,
					Password:    `column`,
				},
			},
		},
	})
}

// TestColumnInsertGrantDoesNotAllowOtherColumnsGuard covers column-level INSERT
// authorization: a grant on one set of columns must not allow writes to other
// columns on the same table.
func TestColumnInsertGrantDoesNotAllowOtherColumnsGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "column INSERT grant does not allow other columns",
			SetUpScript: []string{
				`CREATE USER column_insert_user PASSWORD 'column';`,
				`CREATE TABLE column_insert_private (
					id INT PRIMARY KEY,
					public_value TEXT,
					private_value TEXT
				);`,
				`GRANT USAGE ON SCHEMA public TO column_insert_user;`,
				`GRANT INSERT (id, public_value)
					ON column_insert_private TO column_insert_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO column_insert_private
						(id, public_value, private_value)
						VALUES (1, 'public', 'private');`,
					ExpectedErr: `permission denied`,
					Username:    `column_insert_user`,
					Password:    `column`,
				},
				{
					Query:    `SELECT COUNT(*) FROM column_insert_private;`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestColumnInsertGrantAllowsGrantedColumnsRepro reproduces an authorization
// correctness bug: PostgreSQL column-level INSERT privileges allow inserting
// exactly the granted columns.
func TestColumnInsertGrantAllowsGrantedColumnsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "column INSERT grant allows granted columns",
			SetUpScript: []string{
				`CREATE USER column_insert_allowed_user PASSWORD 'column';`,
				`CREATE TABLE column_insert_allowed_private (
					id INT PRIMARY KEY,
					public_value TEXT,
					private_value TEXT
				);`,
				`GRANT USAGE ON SCHEMA public TO column_insert_allowed_user;`,
				`GRANT INSERT (id, public_value)
					ON column_insert_allowed_private TO column_insert_allowed_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO column_insert_allowed_private
						(id, public_value)
						VALUES (1, 'public');`,
					Username: `column_insert_allowed_user`,
					Password: `column`,
				},
				{
					Query: `SELECT id, public_value, private_value
						FROM column_insert_allowed_private;`,
					Expected: []sql.Row{{1, "public", nil}},
				},
			},
		},
	})
}

// TestCteSelectUsesUnderlyingTablePrivilegesRepro reproduces an authorization
// correctness bug: SELECT through a CTE should use the underlying table's
// privileges rather than requiring privileges on the CTE alias.
func TestCteSelectUsesUnderlyingTablePrivilegesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CTE SELECT uses underlying table privileges",
			SetUpScript: []string{
				`CREATE USER cte_select_user PASSWORD 'column';`,
				`CREATE TABLE cte_select_private (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO cte_select_private VALUES (1, 'visible'), (2, 'also visible');`,
				`GRANT USAGE ON SCHEMA public TO cte_select_user;`,
				`GRANT SELECT ON cte_select_private TO cte_select_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `WITH visible_items AS (
							SELECT id, label FROM cte_select_private
						)
						SELECT id, label FROM visible_items ORDER BY id;`,
					Expected: []sql.Row{{1, "visible"}, {2, "also visible"}},
					Username: `cte_select_user`,
					Password: `column`,
				},
			},
		},
	})
}

// TestCreateTableAsAllowsGrantedSourceColumnsRepro reproduces an authorization
// correctness bug: CREATE TABLE AS should be able to read source columns
// covered by column-level SELECT grants.
func TestCreateTableAsAllowsGrantedSourceColumnsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE AS allows granted source columns",
			SetUpScript: []string{
				`CREATE USER ctas_column_user PASSWORD 'column';`,
				`CREATE TABLE ctas_column_source_private (
					id INT PRIMARY KEY,
					public_value TEXT,
					private_value TEXT
				);`,
				`INSERT INTO ctas_column_source_private VALUES (1, 'public', 'private');`,
				`GRANT USAGE, CREATE ON SCHEMA public TO ctas_column_user;`,
				`GRANT SELECT (id, public_value)
					ON ctas_column_source_private TO ctas_column_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE ctas_column_copy AS
						SELECT id, public_value FROM ctas_column_source_private;`,
					Username: `ctas_column_user`,
					Password: `column`,
				},
				{
					Query:    `SELECT id, public_value FROM ctas_column_copy;`,
					Expected: []sql.Row{{1, "public"}},
					Username: `ctas_column_user`,
					Password: `column`,
				},
			},
		},
	})
}

// TestCreateTableAsRejectsUngrantedSourceColumnsGuard covers CTAS source-column
// authorization: creating a table from a query cannot read columns for which
// the role lacks SELECT privilege.
func TestCreateTableAsRejectsUngrantedSourceColumnsGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE AS rejects ungranted source columns",
			SetUpScript: []string{
				`CREATE USER ctas_column_denied_user PASSWORD 'column';`,
				`CREATE TABLE ctas_column_denied_private (
					id INT PRIMARY KEY,
					public_value TEXT,
					private_value TEXT
				);`,
				`INSERT INTO ctas_column_denied_private VALUES (1, 'public', 'private');`,
				`GRANT USAGE, CREATE ON SCHEMA public TO ctas_column_denied_user;`,
				`GRANT SELECT (id, public_value)
					ON ctas_column_denied_private TO ctas_column_denied_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE ctas_column_denied_copy AS
						SELECT id, private_value FROM ctas_column_denied_private;`,
					ExpectedErr: `permission denied`,
					Username:    `ctas_column_denied_user`,
					Password:    `column`,
				},
				{
					Query:    `SELECT to_regclass('public.ctas_column_denied_copy')::text;`,
					Expected: []sql.Row{{nil}},
				},
			},
		},
	})
}

// TestCreateViewAllowsGrantedSourceColumnsRepro reproduces an authorization
// correctness bug: CREATE VIEW should be able to read source columns covered by
// column-level SELECT grants.
func TestCreateViewAllowsGrantedSourceColumnsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE VIEW allows granted source columns",
			SetUpScript: []string{
				`CREATE USER view_column_user PASSWORD 'column';`,
				`CREATE TABLE view_column_source_private (
					id INT PRIMARY KEY,
					public_value TEXT,
					private_value TEXT
				);`,
				`INSERT INTO view_column_source_private VALUES (1, 'public', 'private');`,
				`GRANT USAGE, CREATE ON SCHEMA public TO view_column_user;`,
				`GRANT SELECT (id, public_value)
					ON view_column_source_private TO view_column_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE VIEW view_column_reader AS
						SELECT id, public_value FROM view_column_source_private;`,
					Username: `view_column_user`,
					Password: `column`,
				},
				{
					Query:    `SELECT id, public_value FROM view_column_reader;`,
					Expected: []sql.Row{{1, "public"}},
				},
			},
		},
	})
}

// TestCreateMaterializedViewAllowsGrantedSourceColumnsRepro reproduces an
// authorization correctness bug: CREATE MATERIALIZED VIEW should be able to
// read source columns covered by column-level SELECT grants.
func TestCreateMaterializedViewAllowsGrantedSourceColumnsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE MATERIALIZED VIEW allows granted source columns",
			SetUpScript: []string{
				`CREATE USER mv_column_user PASSWORD 'column';`,
				`CREATE TABLE mv_column_source_private (
					id INT PRIMARY KEY,
					public_value TEXT,
					private_value TEXT
				);`,
				`INSERT INTO mv_column_source_private VALUES (1, 'public', 'private');`,
				`GRANT USAGE, CREATE ON SCHEMA public TO mv_column_user;`,
				`GRANT SELECT (id, public_value)
					ON mv_column_source_private TO mv_column_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE MATERIALIZED VIEW mv_column_reader AS
						SELECT id, public_value FROM mv_column_source_private;`,
					Username: `mv_column_user`,
					Password: `column`,
				},
				{
					Query:    `SELECT id, public_value FROM mv_column_reader;`,
					Expected: []sql.Row{{1, "public"}},
				},
			},
		},
	})
}

// TestColumnInsertReturningRequiresSelectPrivilegeGuard guards that INSERT
// RETURNING cannot expose columns for which the role lacks SELECT privilege.
func TestColumnInsertReturningRequiresSelectPrivilegeGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "INSERT RETURNING requires SELECT on returned columns",
			SetUpScript: []string{
				`CREATE USER column_insert_returning_user PASSWORD 'column';`,
				`CREATE TABLE column_insert_returning_private (
					id INT PRIMARY KEY,
					public_value TEXT,
					private_value TEXT
				);`,
				`GRANT USAGE ON SCHEMA public TO column_insert_returning_user;`,
				`GRANT INSERT (id, public_value, private_value), SELECT (id)
					ON column_insert_returning_private TO column_insert_returning_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO column_insert_returning_private
						(id, public_value, private_value)
						VALUES (1, 'public', 'private')
						RETURNING private_value;`,
					ExpectedErr: `permission denied`,
					Username:    `column_insert_returning_user`,
					Password:    `column`,
				},
				{
					Query:    `SELECT COUNT(*) FROM column_insert_returning_private;`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestInsertSelectRequiresSelectOnSourceTableRepro guards that INSERT ...
// SELECT cannot read a source table for which the role lacks SELECT privilege.
func TestInsertSelectRequiresSelectOnSourceTableRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "INSERT SELECT requires SELECT on source table",
			SetUpScript: []string{
				`CREATE USER insert_select_source_user PASSWORD 'column';`,
				`CREATE TABLE insert_select_target_private (
					id INT PRIMARY KEY,
					public_value TEXT
				);`,
				`CREATE TABLE insert_select_source_private (
					id INT PRIMARY KEY,
					private_value TEXT
				);`,
				`INSERT INTO insert_select_source_private VALUES (1, 'private');`,
				`GRANT USAGE ON SCHEMA public TO insert_select_source_user;`,
				`GRANT INSERT
					ON insert_select_target_private TO insert_select_source_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO insert_select_target_private (id, public_value)
						SELECT id, private_value FROM insert_select_source_private;`,
					ExpectedErr: `permission denied`,
					Username:    `insert_select_source_user`,
					Password:    `column`,
				},
				{
					Query:    `SELECT COUNT(*) FROM insert_select_target_private;`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestColumnUpdateGrantDoesNotAllowOtherColumnsRepro guards column-level
// UPDATE authorization: a grant on one column must not allow updates to other
// columns on the same table.
func TestColumnUpdateGrantDoesNotAllowOtherColumnsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "column UPDATE grant does not allow other columns",
			SetUpScript: []string{
				`CREATE USER column_update_user PASSWORD 'column';`,
				`CREATE TABLE column_update_private (
					id INT PRIMARY KEY,
					public_value TEXT,
					private_value TEXT
				);`,
				`INSERT INTO column_update_private VALUES (1, 'public', 'private');`,
				`GRANT USAGE ON SCHEMA public TO column_update_user;`,
				`GRANT SELECT (id, public_value), UPDATE (public_value)
					ON column_update_private TO column_update_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `UPDATE column_update_private SET private_value = 'changed' WHERE id = 1;`,
					ExpectedErr: `permission denied`,
					Username:    `column_update_user`,
					Password:    `column`,
				},
				{
					Query:    `SELECT id, public_value, private_value FROM column_update_private;`,
					Expected: []sql.Row{{1, "public", "private"}},
				},
			},
		},
	})
}

// TestColumnUpdateGrantAllowsGrantedColumnsRepro reproduces an authorization
// correctness bug: PostgreSQL column-level UPDATE privileges allow updating
// exactly the granted columns.
func TestColumnUpdateGrantAllowsGrantedColumnsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "column UPDATE grant allows granted columns",
			SetUpScript: []string{
				`CREATE USER column_update_allowed_user PASSWORD 'column';`,
				`CREATE TABLE column_update_allowed_private (
					id INT PRIMARY KEY,
					public_value TEXT,
					private_value TEXT
				);`,
				`INSERT INTO column_update_allowed_private VALUES (1, 'public', 'private');`,
				`GRANT USAGE ON SCHEMA public TO column_update_allowed_user;`,
				`GRANT SELECT (id, public_value), UPDATE (public_value)
					ON column_update_allowed_private TO column_update_allowed_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE column_update_allowed_private
						SET public_value = 'changed'
						WHERE id = 1;`,
					Username: `column_update_allowed_user`,
					Password: `column`,
				},
				{
					Query: `SELECT id, public_value, private_value
						FROM column_update_allowed_private;`,
					Expected: []sql.Row{{1, "changed", "private"}},
				},
			},
		},
	})
}

// TestColumnUpdateGrantRequiresSelectForSourceColumnsRepro guards that UPDATE
// expressions cannot read columns for which the role lacks SELECT privilege.
func TestColumnUpdateGrantRequiresSelectForSourceColumnsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "column UPDATE grant requires SELECT on source columns",
			SetUpScript: []string{
				`CREATE USER column_update_source_user PASSWORD 'column';`,
				`CREATE TABLE column_update_source_private (
					id INT PRIMARY KEY,
					public_value TEXT,
					private_value TEXT
				);`,
				`INSERT INTO column_update_source_private VALUES (1, 'public', 'private');`,
				`GRANT USAGE ON SCHEMA public TO column_update_source_user;`,
				`GRANT SELECT (id), UPDATE (public_value)
					ON column_update_source_private TO column_update_source_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `UPDATE column_update_source_private SET public_value = private_value WHERE id = 1;`,
					ExpectedErr: `permission denied`,
					Username:    `column_update_source_user`,
					Password:    `column`,
				},
				{
					Query:    `SELECT id, public_value, private_value FROM column_update_source_private;`,
					Expected: []sql.Row{{1, "public", "private"}},
				},
			},
		},
	})
}

// TestColumnUpdateRequiresSelectForWhereColumnsRepro guards that UPDATE
// predicates cannot read columns for which the role lacks SELECT privilege.
func TestColumnUpdateRequiresSelectForWhereColumnsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "column UPDATE grant requires SELECT on WHERE columns",
			SetUpScript: []string{
				`CREATE USER column_update_where_user PASSWORD 'column';`,
				`CREATE TABLE column_update_where_private (
					id INT PRIMARY KEY,
					public_value TEXT,
					private_value TEXT
				);`,
				`INSERT INTO column_update_where_private VALUES (1, 'public', 'private');`,
				`GRANT USAGE ON SCHEMA public TO column_update_where_user;`,
				`GRANT SELECT (id), UPDATE (public_value)
					ON column_update_where_private TO column_update_where_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE column_update_where_private
						SET public_value = 'changed'
						WHERE private_value = 'private';`,
					ExpectedErr: `permission denied`,
					Username:    `column_update_where_user`,
					Password:    `column`,
				},
				{
					Query:    `SELECT id, public_value, private_value FROM column_update_where_private;`,
					Expected: []sql.Row{{1, "public", "private"}},
				},
			},
		},
	})
}

// TestUpdateFromRequiresSelectOnSourceTableRepro guards that UPDATE ... FROM
// cannot read a source table for which the role lacks SELECT privilege.
func TestUpdateFromRequiresSelectOnSourceTableRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "UPDATE FROM requires SELECT on source table",
			SetUpScript: []string{
				`CREATE USER update_from_source_user PASSWORD 'column';`,
				`CREATE TABLE update_from_target_private (
					id INT PRIMARY KEY,
					public_value TEXT
				);`,
				`CREATE TABLE update_from_source_private (
					id INT PRIMARY KEY,
					private_value TEXT
				);`,
				`INSERT INTO update_from_target_private VALUES (1, 'public');`,
				`INSERT INTO update_from_source_private VALUES (1, 'private');`,
				`GRANT USAGE ON SCHEMA public TO update_from_source_user;`,
				`GRANT SELECT (id), UPDATE (public_value)
					ON update_from_target_private TO update_from_source_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE update_from_target_private AS t
						SET public_value = s.private_value
						FROM update_from_source_private AS s
						WHERE t.id = s.id;`,
					ExpectedErr: `permission denied`,
					Username:    `update_from_source_user`,
					Password:    `column`,
				},
				{
					Query:    `SELECT id, public_value FROM update_from_target_private;`,
					Expected: []sql.Row{{1, "public"}},
				},
			},
		},
	})
}

// TestColumnOnConflictUpdateRequiresSelectForSourceColumnsRepro guards that
// ON CONFLICT DO UPDATE expressions cannot read columns for which the role
// lacks SELECT privilege.
func TestColumnOnConflictUpdateRequiresSelectForSourceColumnsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ON CONFLICT UPDATE requires SELECT on source columns",
			SetUpScript: []string{
				`CREATE USER column_upsert_source_user PASSWORD 'column';`,
				`CREATE TABLE column_upsert_source_private (
					id INT PRIMARY KEY,
					public_value TEXT,
					private_value TEXT
				);`,
				`INSERT INTO column_upsert_source_private VALUES (1, 'public', 'private');`,
				`GRANT USAGE ON SCHEMA public TO column_upsert_source_user;`,
				`GRANT INSERT (id, public_value), SELECT (id), UPDATE (public_value)
					ON column_upsert_source_private TO column_upsert_source_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO column_upsert_source_private (id, public_value)
						VALUES (1, 'ignored')
						ON CONFLICT (id) DO UPDATE
						SET public_value = column_upsert_source_private.private_value;`,
					ExpectedErr: `permission denied`,
					Username:    `column_upsert_source_user`,
					Password:    `column`,
				},
				{
					Query:    `SELECT id, public_value, private_value FROM column_upsert_source_private;`,
					Expected: []sql.Row{{1, "public", "private"}},
				},
			},
		},
	})
}

// TestColumnOnConflictUpdateRequiresSelectForWhereColumnsRepro guards that ON
// CONFLICT DO UPDATE predicates cannot read columns for which the role lacks
// SELECT privilege.
func TestColumnOnConflictUpdateRequiresSelectForWhereColumnsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ON CONFLICT UPDATE requires SELECT on WHERE columns",
			SetUpScript: []string{
				`CREATE USER column_upsert_where_user PASSWORD 'column';`,
				`CREATE TABLE column_upsert_where_private (
					id INT PRIMARY KEY,
					public_value TEXT,
					private_value TEXT
				);`,
				`INSERT INTO column_upsert_where_private VALUES (1, 'public', 'private');`,
				`GRANT USAGE ON SCHEMA public TO column_upsert_where_user;`,
				`GRANT INSERT (id, public_value), SELECT (id), UPDATE (public_value)
					ON column_upsert_where_private TO column_upsert_where_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO column_upsert_where_private (id, public_value)
						VALUES (1, 'changed')
						ON CONFLICT (id) DO UPDATE
						SET public_value = EXCLUDED.public_value
						WHERE column_upsert_where_private.private_value = 'private';`,
					ExpectedErr: `permission denied`,
					Username:    `column_upsert_where_user`,
					Password:    `column`,
				},
				{
					Query:    `SELECT id, public_value, private_value FROM column_upsert_where_private;`,
					Expected: []sql.Row{{1, "public", "private"}},
				},
			},
		},
	})
}

// TestColumnOnConflictUpdateRequiresUpdatePrivilegeRepro guards that ON
// CONFLICT DO UPDATE cannot update target columns unless the role has UPDATE
// privilege on those columns.
func TestColumnOnConflictUpdateRequiresUpdatePrivilegeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ON CONFLICT UPDATE requires UPDATE on target columns",
			SetUpScript: []string{
				`CREATE USER column_upsert_update_user PASSWORD 'column';`,
				`CREATE TABLE column_upsert_update_private (
					id INT PRIMARY KEY,
					public_value TEXT
				);`,
				`INSERT INTO column_upsert_update_private VALUES (1, 'original');`,
				`GRANT USAGE ON SCHEMA public TO column_upsert_update_user;`,
				`GRANT INSERT (id, public_value), SELECT (id)
					ON column_upsert_update_private TO column_upsert_update_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO column_upsert_update_private (id, public_value)
						VALUES (1, 'changed')
						ON CONFLICT (id) DO UPDATE
						SET public_value = EXCLUDED.public_value;`,
					ExpectedErr: `permission denied`,
					Username:    `column_upsert_update_user`,
					Password:    `column`,
				},
				{
					Query:    `SELECT id, public_value FROM column_upsert_update_private;`,
					Expected: []sql.Row{{1, "original"}},
				},
			},
		},
	})
}

// TestColumnDeleteRequiresSelectForWhereColumnsRepro guards that DELETE
// predicates cannot read columns for which the role lacks SELECT privilege.
func TestColumnDeleteRequiresSelectForWhereColumnsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "column DELETE grant requires SELECT on WHERE columns",
			SetUpScript: []string{
				`CREATE USER column_delete_where_user PASSWORD 'column';`,
				`CREATE TABLE column_delete_where_private (
					id INT PRIMARY KEY,
					public_value TEXT,
					private_value TEXT
				);`,
				`INSERT INTO column_delete_where_private VALUES (1, 'public', 'private');`,
				`GRANT USAGE ON SCHEMA public TO column_delete_where_user;`,
				`GRANT SELECT (id)
					ON column_delete_where_private TO column_delete_where_user;`,
				`GRANT DELETE
					ON column_delete_where_private TO column_delete_where_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DELETE FROM column_delete_where_private WHERE private_value = 'private';`,
					ExpectedErr: `permission denied`,
					Username:    `column_delete_where_user`,
					Password:    `column`,
				},
				{
					Query:    `SELECT id, public_value, private_value FROM column_delete_where_private;`,
					Expected: []sql.Row{{1, "public", "private"}},
				},
			},
		},
	})
}

// TestColumnDeleteReturningRequiresSelectPrivilegeRepro guards that DELETE
// RETURNING cannot expose columns for which the role lacks SELECT privilege.
func TestColumnDeleteReturningRequiresSelectPrivilegeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DELETE RETURNING requires SELECT on returned columns",
			SetUpScript: []string{
				`CREATE USER column_delete_returning_user PASSWORD 'column';`,
				`CREATE TABLE column_delete_returning_private (
					id INT PRIMARY KEY,
					public_value TEXT,
					private_value TEXT
				);`,
				`INSERT INTO column_delete_returning_private VALUES (1, 'public', 'private');`,
				`GRANT USAGE ON SCHEMA public TO column_delete_returning_user;`,
				`GRANT SELECT (id)
					ON column_delete_returning_private TO column_delete_returning_user;`,
				`GRANT DELETE
					ON column_delete_returning_private TO column_delete_returning_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DELETE FROM column_delete_returning_private
						WHERE id = 1
						RETURNING private_value;`,
					ExpectedErr: `permission denied`,
					Username:    `column_delete_returning_user`,
					Password:    `column`,
				},
				{
					Query:    `SELECT id, public_value, private_value FROM column_delete_returning_private;`,
					Expected: []sql.Row{{1, "public", "private"}},
				},
			},
		},
	})
}

// TestColumnUpdateReturningRequiresSelectPrivilegeGuard guards that UPDATE
// RETURNING cannot expose columns for which the role lacks SELECT privilege.
func TestColumnUpdateReturningRequiresSelectPrivilegeGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "UPDATE RETURNING requires SELECT on returned columns",
			SetUpScript: []string{
				`CREATE USER column_update_returning_user PASSWORD 'column';`,
				`CREATE TABLE column_update_returning_private (
					id INT PRIMARY KEY,
					public_value TEXT,
					private_value TEXT
				);`,
				`INSERT INTO column_update_returning_private VALUES (1, 'public', 'private');`,
				`GRANT USAGE ON SCHEMA public TO column_update_returning_user;`,
				`GRANT SELECT (id), UPDATE (public_value)
					ON column_update_returning_private TO column_update_returning_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE column_update_returning_private
						SET public_value = 'changed'
						WHERE id = 1
						RETURNING private_value;`,
					ExpectedErr: `permission denied`,
					Username:    `column_update_returning_user`,
					Password:    `column`,
				},
				{
					Query:    `SELECT id, public_value, private_value FROM column_update_returning_private;`,
					Expected: []sql.Row{{1, "public", "private"}},
				},
			},
		},
	})
}
