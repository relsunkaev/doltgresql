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
)

// TestCreateZeroColumnTableRepro reproduces a DDL correctness/stability bug:
// PostgreSQL allows zero-column tables, which can store rows inserted with
// DEFAULT VALUES.
func TestCreateZeroColumnTableRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE with zero columns",
			SetUpScript: []string{
				`CREATE TABLE zero_column_items ();`,
				`INSERT INTO zero_column_items DEFAULT VALUES;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT count(*)::text FROM zero_column_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-correctness-repro-test-testcreatezerocolumntablerepro-0001-select-count-*-::text-from"},
				},
			},
		},
	})
}

// TestCreateTablePrimaryKeyConstraintNameGuard guards PostgreSQL catalog
// semantics for explicitly named primary-key constraints and later DDL that
// addresses the constraint by that name.
func TestCreateTablePrimaryKeyConstraintNameGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE preserves primary key constraint name",
			SetUpScript: []string{
				`CREATE TABLE named_pk_items (
					id INT,
					label TEXT,
					CONSTRAINT named_pk_items_custom_pkey PRIMARY KEY (id)
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT conname
						FROM pg_catalog.pg_constraint
						WHERE conrelid = 'named_pk_items'::regclass
							AND contype = 'p';`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-correctness-repro-test-testcreatetableprimarykeyconstraintnameguard-0001-select-conname-from-pg_catalog.pg_constraint-where"},
				},
				{
					Query: `ALTER TABLE named_pk_items
						DROP CONSTRAINT named_pk_items_custom_pkey;`,
				},
				{
					Query: `SELECT conname
						FROM pg_catalog.pg_constraint
						WHERE conrelid = 'named_pk_items'::regclass
							AND contype = 'p';`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-correctness-repro-test-testcreatetableprimarykeyconstraintnameguard-0002-select-conname-from-pg_catalog.pg_constraint-where"},
				},
			},
		},
	})
}

// TestCreateTableReloptionsPersistRepro reproduces a catalog persistence gap:
// PostgreSQL stores table reloptions from CREATE TABLE ... WITH (...) in
// pg_class.reloptions.
func TestCreateTableReloptionsPersistRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE reloptions persist in pg_class",
			SetUpScript: []string{
				`CREATE TABLE table_reloptions_items (id INT)
					WITH (fillfactor=30, autovacuum_enabled=false, autovacuum_analyze_scale_factor=0.2);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT CAST(reloptions AS TEXT)
						FROM pg_catalog.pg_class
						WHERE oid = 'table_reloptions_items'::regclass;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-correctness-repro-test-testcreatetablereloptionspersistrepro-0001-select-cast-reloptions-as-text"},
				},
			},
		},
	})
}

// TestCreateTableDefaultTablespace guards that CREATE TABLE accepts the
// explicit pg_default tablespace, since that is the only tablespace Doltgres
// exposes and PostgreSQL allows spelling out the default.
func TestCreateTableDefaultTablespace(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE with default tablespace",
			SetUpScript: []string{
				`CREATE TABLE table_default_tablespace_items (
					id INT PRIMARY KEY,
					label TEXT
				) TABLESPACE pg_default;`,
				`INSERT INTO table_default_tablespace_items VALUES (1, 'ok');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, label
						FROM table_default_tablespace_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-correctness-repro-test-testcreatetabledefaulttablespace-0001-select-id-label-from-table_default_tablespace_items"},
				},
			},
		},
	})
}

// TestCreateTableUnknownTablespaceErrors guards that a CREATE TABLE targeting
// a tablespace that does not exist returns PostgreSQL's catalog-style error
// rather than silently creating the table in the default tablespace.
func TestCreateTableUnknownTablespaceErrors(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE rejects unknown tablespace",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE table_unknown_tablespace_items (
							id INT PRIMARY KEY
						) TABLESPACE custom_space;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-correctness-repro-test-testcreatetableunknowntablespaceerrors-0001-create-table-table_unknown_tablespace_items-id-int", Compare: "sqlstate"},
				},
			},
		},
	})
}

// TestCreateTableUsingHeap guards that CREATE TABLE accepts the explicit
// heap access method, matching PostgreSQL's default behavior for ordinary
// tables.
func TestCreateTableUsingHeap(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE using heap access method",
			SetUpScript: []string{
				`CREATE TABLE table_using_heap_items (
					id INT PRIMARY KEY,
					label TEXT
				) USING heap;`,
				`INSERT INTO table_using_heap_items VALUES (1, 'ok');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, label
						FROM table_using_heap_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-correctness-repro-test-testcreatetableusingheap-0001-select-id-label-from-table_using_heap_items"},
				},
			},
		},
	})
}

// TestCreateTableUsingUnknownAccessMethodErrors guards that CREATE TABLE
// rejects any access method other than heap with PostgreSQL's
// "access method ... does not exist" error.
func TestCreateTableUsingUnknownAccessMethodErrors(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE rejects non-heap access method",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE table_unknown_access_method_items (
							id INT PRIMARY KEY
						) USING btree;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-correctness-repro-test-testcreatetableusingunknownaccessmethoderrors-0001-create-table-table_unknown_access_method_items-id-int", Compare: "sqlstate"},
				},
			},
		},
	})
}
