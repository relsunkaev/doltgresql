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

// TestDropColumnUsedByViewRequiresCascadeRepro reproduces a dependency
// correctness bug: Doltgres lets ALTER TABLE DROP COLUMN remove a base-table
// column that a view still depends on.
func TestDropColumnUsedByViewRequiresCascadeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP COLUMN rejects columns referenced by views",
			SetUpScript: []string{
				`CREATE TABLE view_dependency_source (
					id INT PRIMARY KEY,
					keep_value INT,
					drop_value INT
				);`,
				`CREATE VIEW view_dependency_reader AS
					SELECT id, drop_value FROM view_dependency_source;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `ALTER TABLE view_dependency_source DROP COLUMN drop_value;`,
					ExpectedErr: `depends on`,
				},
			},
		},
	})
}

// TestDropColumnUsedByMaterializedViewRequiresCascadeRepro reproduces a
// dependency correctness bug: Doltgres lets ALTER TABLE DROP COLUMN remove a
// base-table column that a materialized view still depends on.
func TestDropColumnUsedByMaterializedViewRequiresCascadeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP COLUMN rejects columns referenced by materialized views",
			SetUpScript: []string{
				`CREATE TABLE matview_column_dependency_source (
					id INT PRIMARY KEY,
					keep_value INT,
					drop_value INT
				);`,
				`INSERT INTO matview_column_dependency_source VALUES (1, 10, 20);`,
				`CREATE MATERIALIZED VIEW matview_column_dependency_reader AS
					SELECT id, drop_value FROM matview_column_dependency_source;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `ALTER TABLE matview_column_dependency_source DROP COLUMN drop_value;`,
					ExpectedErr: `depends on`,
				},
			},
		},
	})
}

// TestDropColumnCascadeDropsDependentViewRepro reproduces a dependency
// correctness bug: ALTER TABLE DROP COLUMN ... CASCADE should drop views that
// depend on the removed base-table column.
func TestDropColumnCascadeDropsDependentViewRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP COLUMN CASCADE drops dependent view",
			SetUpScript: []string{
				`CREATE TABLE cascade_column_source (
					id INT PRIMARY KEY,
					keep_value INT,
					drop_value INT
				);`,
				`CREATE VIEW cascade_column_reader AS
					SELECT id, drop_value FROM cascade_column_source;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE cascade_column_source
						DROP COLUMN drop_value CASCADE;`,
				},
				{
					Query:    `SELECT to_regclass('cascade_column_reader') IS NULL;`,
					Expected: []sql.Row{{"t"}},
				},
				{
					Query: `SELECT column_name
						FROM information_schema.columns
						WHERE table_name = 'cascade_column_source'
						ORDER BY ordinal_position;`,
					Expected: []sql.Row{{"id"}, {"keep_value"}},
				},
			},
		},
	})
}

// TestDropColumnCascadeDropsDependentMaterializedViewRepro reproduces a
// dependency correctness bug: ALTER TABLE DROP COLUMN ... CASCADE should drop
// materialized views that depend on the removed base-table column.
func TestDropColumnCascadeDropsDependentMaterializedViewRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP COLUMN CASCADE drops dependent materialized view",
			SetUpScript: []string{
				`CREATE TABLE cascade_mat_column_source (
					id INT PRIMARY KEY,
					keep_value INT,
					drop_value INT
				);`,
				`INSERT INTO cascade_mat_column_source VALUES (1, 10, 20);`,
				`CREATE MATERIALIZED VIEW cascade_mat_column_reader AS
					SELECT id, drop_value FROM cascade_mat_column_source;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE cascade_mat_column_source
						DROP COLUMN drop_value CASCADE;`,
				},
				{
					Query:    `SELECT to_regclass('cascade_mat_column_reader') IS NULL;`,
					Expected: []sql.Row{{"t"}},
				},
				{
					Query: `SELECT column_name
						FROM information_schema.columns
						WHERE table_name = 'cascade_mat_column_source'
						ORDER BY ordinal_position;`,
					Expected: []sql.Row{{"id"}, {"keep_value"}},
				},
			},
		},
	})
}

// TestDropTableUsedByViewRequiresCascadeRepro reproduces a dependency
// correctness bug: Doltgres lets DROP TABLE remove a base table that a view
// still depends on.
func TestDropTableUsedByViewRequiresCascadeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP TABLE rejects tables referenced by views",
			SetUpScript: []string{
				`CREATE TABLE view_table_dependency_source (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`CREATE VIEW view_table_dependency_reader AS
					SELECT id, label FROM view_table_dependency_source;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DROP TABLE view_table_dependency_source;`,
					ExpectedErr: `depends on`,
				},
			},
		},
	})
}

// TestDropTableUsedByMaterializedViewRequiresCascadeRepro reproduces a
// dependency correctness bug: Doltgres lets DROP TABLE remove a base table that
// a materialized view still depends on.
func TestDropTableUsedByMaterializedViewRequiresCascadeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP TABLE rejects tables referenced by materialized views",
			SetUpScript: []string{
				`CREATE TABLE matview_table_dependency_source (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO matview_table_dependency_source VALUES (1, 'alpha');`,
				`CREATE MATERIALIZED VIEW matview_table_dependency_reader AS
					SELECT id, label FROM matview_table_dependency_source;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DROP TABLE matview_table_dependency_source;`,
					ExpectedErr: `depends on`,
				},
			},
		},
	})
}

// TestDropTableCascadeDropsDependentViewRepro reproduces a dependency
// correctness bug: DROP TABLE ... CASCADE should drop views that depend on the
// removed base table.
func TestDropTableCascadeDropsDependentViewRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP TABLE CASCADE drops dependent view",
			SetUpScript: []string{
				`CREATE TABLE cascade_view_table_source (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`CREATE VIEW cascade_view_table_reader AS
					SELECT id, label FROM cascade_view_table_source;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP TABLE cascade_view_table_source CASCADE;`,
				},
				{
					Query: `SELECT to_regclass('cascade_view_table_source') IS NULL,
							to_regclass('cascade_view_table_reader') IS NULL;`,
					Expected: []sql.Row{{"t", "t"}},
				},
			},
		},
	})
}

// TestDropTableCascadeDropsDependentMaterializedViewRepro reproduces a
// dependency correctness bug: DROP TABLE ... CASCADE should drop materialized
// views that depend on the removed base table.
func TestDropTableCascadeDropsDependentMaterializedViewRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP TABLE CASCADE drops dependent materialized view",
			SetUpScript: []string{
				`CREATE TABLE cascade_matview_table_source (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO cascade_matview_table_source VALUES (1, 'alpha');`,
				`CREATE MATERIALIZED VIEW cascade_matview_table_reader AS
					SELECT id, label FROM cascade_matview_table_source;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP TABLE cascade_matview_table_source CASCADE;`,
				},
				{
					Query: `SELECT to_regclass('cascade_matview_table_source') IS NULL,
							to_regclass('cascade_matview_table_reader') IS NULL;`,
					Expected: []sql.Row{{"t", "t"}},
				},
			},
		},
	})
}

// TestDropViewUsedByViewRequiresCascadeRepro reproduces a dependency
// correctness bug: Doltgres lets DROP VIEW remove a view that another view
// still depends on.
func TestDropViewUsedByViewRequiresCascadeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP VIEW rejects views referenced by views",
			SetUpScript: []string{
				`CREATE TABLE view_chain_source (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`CREATE VIEW view_chain_base AS
					SELECT id, label FROM view_chain_source;`,
				`CREATE VIEW view_chain_reader AS
					SELECT id, label FROM view_chain_base;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DROP VIEW view_chain_base;`,
					ExpectedErr: `depends on`,
				},
			},
		},
	})
}

// TestDropMaterializedViewUsedByViewRequiresCascadeRepro reproduces a
// dependency correctness bug: Doltgres lets DROP MATERIALIZED VIEW remove a
// materialized view that a normal view still depends on.
func TestDropMaterializedViewUsedByViewRequiresCascadeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP MATERIALIZED VIEW rejects materialized views referenced by views",
			SetUpScript: []string{
				`CREATE TABLE matview_chain_source (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO matview_chain_source VALUES (1, 'alpha');`,
				`CREATE MATERIALIZED VIEW matview_chain_base AS
					SELECT id, label FROM matview_chain_source;`,
				`CREATE VIEW matview_chain_reader AS
					SELECT id, label FROM matview_chain_base;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DROP MATERIALIZED VIEW matview_chain_base;`,
					ExpectedErr: `depends on`,
				},
			},
		},
	})
}

// TestRenameColumnUsedByViewKeepsViewUsableRepro reproduces a dependency
// correctness bug: renaming a base-table column leaves dependent view
// definitions stale.
func TestRenameColumnUsedByViewKeepsViewUsableRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "RENAME COLUMN rewrites dependent views",
			SetUpScript: []string{
				`CREATE TABLE view_rename_dependency_source (
					id INT PRIMARY KEY,
					old_label TEXT
				);`,
				`INSERT INTO view_rename_dependency_source VALUES (1, 'before rename');`,
				`CREATE VIEW view_rename_dependency_reader AS
					SELECT id, old_label FROM view_rename_dependency_source;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE view_rename_dependency_source
						RENAME COLUMN old_label TO new_label;`,
				},
				{
					Query: `SELECT id, old_label
						FROM view_rename_dependency_reader
						ORDER BY id;`,
					Expected: []sql.Row{{1, "before rename"}},
				},
			},
		},
	})
}

// TestRenameTableUsedByViewKeepsViewUsableRepro reproduces a dependency
// correctness bug: renaming a base table leaves dependent view definitions
// stale.
func TestRenameTableUsedByViewKeepsViewUsableRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "RENAME TABLE rewrites dependent views",
			SetUpScript: []string{
				`CREATE TABLE view_rename_table_source (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO view_rename_table_source VALUES (1, 'before table rename');`,
				`CREATE VIEW view_rename_table_reader AS
					SELECT id, label FROM view_rename_table_source;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE view_rename_table_source
						RENAME TO view_rename_table_source_new;`,
				},
				{
					Query: `SELECT id, label
						FROM view_rename_table_reader
						ORDER BY id;`,
					Expected: []sql.Row{{1, "before table rename"}},
				},
			},
		},
	})
}

// TestRenameTableUsedByMaterializedViewKeepsRefreshUsableRepro reproduces a
// dependency correctness bug: renaming a base table leaves dependent
// materialized-view refresh definitions stale.
func TestRenameTableUsedByMaterializedViewKeepsRefreshUsableRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "RENAME TABLE keeps dependent materialized views refreshable",
			SetUpScript: []string{
				`CREATE TABLE matview_rename_table_source (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO matview_rename_table_source VALUES (1, 'before rename');`,
				`CREATE MATERIALIZED VIEW matview_rename_table_reader AS
					SELECT id, label FROM matview_rename_table_source;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE matview_rename_table_source
						RENAME TO matview_rename_table_source_new;`,
				},
				{
					Query: `UPDATE matview_rename_table_source_new
						SET label = 'after rename'
						WHERE id = 1;`,
				},
				{
					Query: `REFRESH MATERIALIZED VIEW matview_rename_table_reader;`,
				},
				{
					Query: `SELECT id, label
						FROM matview_rename_table_reader
						ORDER BY id;`,
					Expected: []sql.Row{{1, "after rename"}},
				},
			},
		},
	})
}

// TestRenameViewUsedByViewKeepsViewUsableRepro reproduces a dependency
// correctness bug: renaming a referenced view leaves dependent view definitions
// stale.
func TestRenameViewUsedByViewKeepsViewUsableRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "RENAME VIEW rewrites dependent views",
			SetUpScript: []string{
				`CREATE TABLE view_rename_view_source (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO view_rename_view_source VALUES (1, 'before view rename');`,
				`CREATE VIEW view_rename_view_base AS
					SELECT id, label FROM view_rename_view_source;`,
				`CREATE VIEW view_rename_view_reader AS
					SELECT id, label FROM view_rename_view_base;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE view_rename_view_base
						RENAME TO view_rename_view_base_new;`,
				},
				{
					Query: `SELECT id, label
						FROM view_rename_view_reader
						ORDER BY id;`,
					Expected: []sql.Row{{1, "before view rename"}},
				},
			},
		},
	})
}

// TestRenameMaterializedViewUsedByViewKeepsViewUsableRepro reproduces a
// dependency correctness bug: renaming a referenced materialized view leaves
// dependent view definitions stale.
func TestRenameMaterializedViewUsedByViewKeepsViewUsableRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "RENAME MATERIALIZED VIEW rewrites dependent views",
			SetUpScript: []string{
				`CREATE TABLE view_rename_matview_source (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO view_rename_matview_source VALUES (1, 'before rename');`,
				`CREATE MATERIALIZED VIEW view_rename_matview_base AS
					SELECT id, label FROM view_rename_matview_source;`,
				`CREATE VIEW view_rename_matview_reader AS
					SELECT id, label FROM view_rename_matview_base;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER MATERIALIZED VIEW view_rename_matview_base
						RENAME TO view_rename_matview_base_new;`,
				},
				{
					Query: `SELECT id, label
						FROM view_rename_matview_reader
						ORDER BY id;`,
					Expected: []sql.Row{{1, "before rename"}},
				},
			},
		},
	})
}
