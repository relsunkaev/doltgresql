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

// TestPgClassReloftypeForOrdinaryTables verifies that pg_class.reloftype
// reports 0 (the PG-correct value) for ordinary, untyped tables. Real
// PostgreSQL only sets reloftype to a nonzero composite-type OID when
// a table is created with `CREATE TABLE name OF composite_type`. The previous
// audit flagged a constant id.Null as suspect; this test pins ordinary-table
// behavior separately from the typed-table support below.
func TestPgClassReloftypeForOrdinaryTables(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ordinary tables report reloftype=0 (PG-correct)",
			SetUpScript: []string{
				`CREATE TABLE plain_t (id INT PRIMARY KEY, v INT);`,
				`CREATE TABLE with_unique (id INT PRIMARY KEY, code TEXT UNIQUE);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT relname, reloftype
FROM pg_catalog.pg_class
WHERE relname IN ('plain_t', 'with_unique')
ORDER BY relname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-class-reloftype-test-testpgclassreloftypeforordinarytables-0001-select-relname-reloftype-from-pg_catalog.pg_class"},
				},
			},
		},
	})
}

func TestTypedTableFromCompositeType(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE OF derives columns and records reloftype",
			SetUpScript: []string{
				`CREATE TYPE typed_person AS (id INT, name TEXT, active BOOLEAN);`,
				`CREATE TABLE typed_people OF typed_person;`,
				`CREATE TABLE plain_t (id INT);`,
				`INSERT INTO typed_people VALUES (1, 'Ada', TRUE);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, name, active FROM typed_people;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-class-reloftype-test-testtypedtablefromcompositetype-0001-select-id-name-active-from"},
				},
				{
					Query: `SELECT c.reloftype = t.oid
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_type t ON t.typname = 'typed_person'
WHERE c.relname = 'typed_people';`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-class-reloftype-test-testtypedtablefromcompositetype-0002-select-c.reloftype-=-t.oid-from"},
				},
				{
					Query: `SELECT table_name, is_typed, user_defined_type_catalog, user_defined_type_schema, user_defined_type_name
FROM information_schema.tables
WHERE table_name IN ('plain_t', 'typed_people')
ORDER BY table_name;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-class-reloftype-test-testtypedtablefromcompositetype-0003-select-table_name-is_typed-user_defined_type_catalog-user_defined_type_schema", ColumnModes: []string{"structural", "structural", "structural", "schema"}},
				},
			},
		},
		{
			Name: "CREATE TABLE OF requires a composite type",
			SetUpScript: []string{
				`CREATE TYPE mood_enum AS ENUM ('sad', 'ok');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE typed_missing OF missing_type;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-class-reloftype-test-testtypedtablefromcompositetype-0004-create-table-typed_missing-of-missing_type", Compare: "sqlstate"},
				},
				{
					Query: `CREATE TABLE typed_mood OF mood_enum;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-class-reloftype-test-testtypedtablefromcompositetype-0005-create-table-typed_mood-of-mood_enum", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "CREATE TABLE OF supports schema-qualified type and table names",
			SetUpScript: []string{
				`CREATE SCHEMA app;`,
				`CREATE TYPE app.typed_address AS (street TEXT, zip INT);`,
				`CREATE TABLE app.addresses OF app.typed_address;`,
				`INSERT INTO app.addresses VALUES ('Main', 85001);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT street, zip FROM app.addresses;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-class-reloftype-test-testtypedtablefromcompositetype-0006-select-street-zip-from-app.addresses"},
				},
				{
					Query: `SELECT table_schema, table_name, is_typed, user_defined_type_catalog, user_defined_type_schema, user_defined_type_name
FROM information_schema.tables
WHERE table_schema = 'app' AND table_name = 'addresses';`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-class-reloftype-test-testtypedtablefromcompositetype-0007-select-table_schema-table_name-is_typed-user_defined_type_catalog"},
				},
			},
		},
		{
			Name: "CREATE TEMP TABLE OF derives columns for session-local table",
			SetUpScript: []string{
				`CREATE TYPE typed_scratch AS (id INT, note TEXT);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TEMP TABLE typed_scratch_rows OF typed_scratch;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-class-reloftype-test-testtypedtablefromcompositetype-0008-create-temp-table-typed_scratch_rows-of"},
				},
				{
					Query: `INSERT INTO typed_scratch_rows VALUES (1, 'temp row');`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-class-reloftype-test-testtypedtablefromcompositetype-0009-insert-into-typed_scratch_rows-values-1"},
				},
				{
					Query: `SELECT id, note FROM typed_scratch_rows;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-class-reloftype-test-testtypedtablefromcompositetype-0010-select-id-note-from-typed_scratch_rows"},
				},
				{
					Query: `CREATE TEMP TABLE IF NOT EXISTS typed_scratch_rows OF missing_type;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-class-reloftype-test-testtypedtablefromcompositetype-0011-create-temp-table-if-not"},
				},
			},
		},
		{
			Name: "CREATE TABLE OF supports primary key and column options",
			SetUpScript: []string{
				`CREATE TYPE typed_task AS (id INT, code TEXT, note TEXT);`,
				`CREATE TABLE typed_tasks OF typed_task (
					PRIMARY KEY (id),
					code WITH OPTIONS NOT NULL
				);`,
				`INSERT INTO typed_tasks (id, code) VALUES (1, 'A');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, code, note FROM typed_tasks;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-class-reloftype-test-testtypedtablefromcompositetype-0012-select-id-code-note-from"},
				},
				{
					Query: `SELECT column_name, is_nullable
FROM information_schema.columns
WHERE table_name = 'typed_tasks' AND column_name IN ('id', 'code', 'note')
ORDER BY ordinal_position;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-class-reloftype-test-testtypedtablefromcompositetype-0013-select-column_name-is_nullable-from-information_schema.columns"},
				},
				{
					Query: `SELECT constraint_name, constraint_type
FROM information_schema.table_constraints
WHERE table_name = 'typed_tasks' AND constraint_type = 'PRIMARY KEY';`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-class-reloftype-test-testtypedtablefromcompositetype-0014-select-constraint_name-constraint_type-from-information_schema.table_constraints"},
				},
				{
					Query: `INSERT INTO typed_tasks (id, code) VALUES (2, NULL);`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-class-reloftype-test-testtypedtablefromcompositetype-0015-insert-into-typed_tasks-id-code", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "CREATE TABLE OF supports unique options",
			SetUpScript: []string{
				`CREATE TYPE typed_unique_task AS (id INT, code TEXT, note TEXT);`,
				`CREATE TABLE typed_unique_tasks OF typed_unique_task (
					UNIQUE (code),
					note WITH OPTIONS UNIQUE
				);`,
				`INSERT INTO typed_unique_tasks VALUES (1, 'A', 'first');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO typed_unique_tasks VALUES (2, 'B', 'second');`,
				},
				{
					Query: `INSERT INTO typed_unique_tasks VALUES (3, 'A', 'third');`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-class-reloftype-test-testtypedtablefromcompositetype-0016-insert-into-typed_unique_tasks-values-3", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO typed_unique_tasks VALUES (4, 'C', 'second');`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-class-reloftype-test-testtypedtablefromcompositetype-0017-insert-into-typed_unique_tasks-values-4", Compare: "sqlstate"},
				},
				{
					Query: `SELECT constraint_name, constraint_type
FROM information_schema.table_constraints
WHERE table_name = 'typed_unique_tasks' AND constraint_type = 'UNIQUE'
ORDER BY constraint_name;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-class-reloftype-test-testtypedtablefromcompositetype-0018-select-constraint_name-constraint_type-from-information_schema.table_constraints"},
				},
				{
					Query: `SELECT indexname
FROM pg_catalog.pg_indexes
WHERE tablename = 'typed_unique_tasks'
ORDER BY indexname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-class-reloftype-test-testtypedtablefromcompositetype-0019-select-indexname-from-pg_catalog.pg_indexes-where"},
				},
			},
		},
		{
			Name: "CREATE TEMP TABLE OF supports unique options",
			SetUpScript: []string{
				`CREATE TYPE typed_temp_unique_task AS (id INT, code TEXT, note TEXT);`,
				`CREATE TEMP TABLE typed_temp_unique_tasks OF typed_temp_unique_task (
					UNIQUE (code),
					note WITH OPTIONS UNIQUE
				);`,
				`INSERT INTO typed_temp_unique_tasks VALUES (1, 'A', 'first');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO typed_temp_unique_tasks VALUES (2, 'B', 'second');`,
				},
				{
					Query: `INSERT INTO typed_temp_unique_tasks VALUES (3, 'A', 'third');`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-class-reloftype-test-testtypedtablefromcompositetype-0020-insert-into-typed_temp_unique_tasks-values-3", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO typed_temp_unique_tasks VALUES (4, 'C', 'second');`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-class-reloftype-test-testtypedtablefromcompositetype-0021-insert-into-typed_temp_unique_tasks-values-4", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO typed_temp_unique_tasks VALUES (5, NULL, NULL);`,
				},
				{
					Query: `INSERT INTO typed_temp_unique_tasks VALUES (6, NULL, NULL);`,
				},
				{
					Query: `UPDATE typed_temp_unique_tasks SET code = 'B' WHERE id = 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-class-reloftype-test-testtypedtablefromcompositetype-0022-update-typed_temp_unique_tasks-set-code-=", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO typed_temp_unique_tasks VALUES (7, 'D', 'fourth'), (8, 'D', 'fifth');`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-class-reloftype-test-testtypedtablefromcompositetype-0023-insert-into-typed_temp_unique_tasks-values-7", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "CREATE TABLE OF accepts unique index element options",
			SetUpScript: []string{
				`CREATE TYPE typed_index_option_task AS (id INT, code TEXT);`,
				`CREATE TABLE typed_index_option_tasks OF typed_index_option_task (
					CONSTRAINT typed_index_option_code_key UNIQUE (code text_pattern_ops DESC NULLS LAST)
				);`,
				`INSERT INTO typed_index_option_tasks VALUES (1, 'A');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `INSERT INTO typed_index_option_tasks VALUES (2, 'A');`,
					ExpectedErr: `duplicate unique key`,
				},
				{
					Query: `SELECT constraint_name, constraint_type
FROM information_schema.table_constraints
WHERE table_name = 'typed_index_option_tasks' AND constraint_type = 'UNIQUE';`,
					Expected: []sql.Row{
						{"typed_index_option_code_key", "UNIQUE"},
					},
				},
			},
		},
		{
			Name: "CREATE TABLE OF supports unique nulls not distinct options",
			SetUpScript: []string{
				`CREATE TYPE typed_unique_nnd_task AS (id INT, code TEXT, note TEXT);`,
				`CREATE TABLE typed_unique_nnd_tasks OF typed_unique_nnd_task (
					UNIQUE NULLS NOT DISTINCT (code),
					note WITH OPTIONS UNIQUE NULLS NOT DISTINCT
				);`,
				`INSERT INTO typed_unique_nnd_tasks VALUES (1, NULL, 'first');`,
				`INSERT INTO typed_unique_nnd_tasks VALUES (2, 'A', NULL);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO typed_unique_nnd_tasks VALUES (3, NULL, 'second');`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-class-reloftype-test-testtypedtablefromcompositetype-0026-insert-into-typed_unique_nnd_tasks-values-3", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO typed_unique_nnd_tasks VALUES (4, 'B', NULL);`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-class-reloftype-test-testtypedtablefromcompositetype-0027-insert-into-typed_unique_nnd_tasks-values-4", Compare: "sqlstate"},
				},
				{
					Query: `SELECT c.relname, i.indnullsnotdistinct
	FROM pg_catalog.pg_class c
	JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid
	WHERE c.relname IN ('typed_unique_nnd_tasks_code_key', 'typed_unique_nnd_tasks_note_key')
	ORDER BY c.relname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-class-reloftype-test-testtypedtablefromcompositetype-0028-select-c.relname-i.indnullsnotdistinct-from-pg_catalog.pg_class"},
				},
			},
		},
		{
			Name: "CREATE TEMP TABLE OF supports unique nulls not distinct options",
			SetUpScript: []string{
				`CREATE TYPE typed_temp_unique_nnd_task AS (id INT, code TEXT, note TEXT);`,
				`CREATE TEMP TABLE typed_temp_unique_nnd_tasks OF typed_temp_unique_nnd_task (
					UNIQUE NULLS NOT DISTINCT (code),
					note WITH OPTIONS UNIQUE NULLS NOT DISTINCT
				);`,
				`INSERT INTO typed_temp_unique_nnd_tasks VALUES (1, NULL, 'first');`,
				`INSERT INTO typed_temp_unique_nnd_tasks VALUES (2, 'A', NULL);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO typed_temp_unique_nnd_tasks VALUES (3, NULL, 'second');`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-class-reloftype-test-testtypedtablefromcompositetype-0029-insert-into-typed_temp_unique_nnd_tasks-values-3", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO typed_temp_unique_nnd_tasks VALUES (4, 'B', NULL);`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-class-reloftype-test-testtypedtablefromcompositetype-0030-insert-into-typed_temp_unique_nnd_tasks-values-4", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "CREATE TABLE OF supports literal column defaults",
			SetUpScript: []string{
				`CREATE TYPE typed_default_task AS (id INT, code TEXT, active BOOLEAN, priority INT);`,
				`CREATE TABLE typed_default_tasks OF typed_default_task (
						code WITH OPTIONS DEFAULT ('new'),
						active WITH OPTIONS DEFAULT TRUE,
						priority WITH OPTIONS DEFAULT -7
					);`,
				`INSERT INTO typed_default_tasks (id) VALUES (1);`,
				`INSERT INTO typed_default_tasks (id, code, active, priority) VALUES (2, 'custom', FALSE, 3);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, code, active, priority FROM typed_default_tasks ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-class-reloftype-test-testtypedtablefromcompositetype-0031-select-id-code-active-priority"},
				},
				{
					Query: `SELECT column_name, (column_default IS NOT NULL)::text
	FROM information_schema.columns
	WHERE table_name = 'typed_default_tasks' AND column_name IN ('code', 'active', 'priority')
	ORDER BY ordinal_position;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-class-reloftype-test-testtypedtablefromcompositetype-0032-select-column_name-column_default-is-not"},
				},
			},
		},
		{
			Name: "CREATE TEMP TABLE OF supports literal column defaults",
			SetUpScript: []string{
				`CREATE TYPE typed_temp_default_task AS (id INT, code TEXT, priority INT);`,
				`CREATE TEMP TABLE typed_temp_default_tasks OF typed_temp_default_task (
						code WITH OPTIONS DEFAULT 'temp',
						priority WITH OPTIONS DEFAULT 5
					);`,
				`INSERT INTO typed_temp_default_tasks (id) VALUES (1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, code, priority FROM typed_temp_default_tasks;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-class-reloftype-test-testtypedtablefromcompositetype-0033-select-id-code-priority-from"},
				},
			},
		},
		{
			Name: "CREATE TABLE OF supports expression column defaults",
			SetUpScript: []string{
				`CREATE TYPE typed_expr_default_task AS (id INT, code TEXT, priority INT);`,
				`CREATE TABLE typed_expr_default_tasks OF typed_expr_default_task (
						code WITH OPTIONS DEFAULT lower('NEW'),
						priority WITH OPTIONS DEFAULT (2 + 3)
					);`,
				`INSERT INTO typed_expr_default_tasks (id) VALUES (1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, code, priority FROM typed_expr_default_tasks;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-class-reloftype-test-testtypedtablefromcompositetype-0034-select-id-code-priority-from"},
				},
				{
					Query: `SELECT column_name, (column_default IS NOT NULL)::text
	FROM information_schema.columns
	WHERE table_name = 'typed_expr_default_tasks' AND column_name IN ('code', 'priority')
	ORDER BY ordinal_position;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-class-reloftype-test-testtypedtablefromcompositetype-0035-select-column_name-column_default-is-not"},
				},
			},
		},
		{
			Name: "CREATE TEMP TABLE OF supports expression column defaults",
			SetUpScript: []string{
				`CREATE TYPE typed_temp_expr_default_task AS (id INT, code TEXT);`,
				`CREATE TEMP TABLE typed_temp_expr_default_tasks OF typed_temp_expr_default_task (
						code WITH OPTIONS DEFAULT upper('temp')
					);`,
				`INSERT INTO typed_temp_expr_default_tasks (id) VALUES (1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, code FROM typed_temp_expr_default_tasks;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-class-reloftype-test-testtypedtablefromcompositetype-0036-select-id-code-from-typed_temp_expr_default_tasks"},
				},
			},
		},
		{
			Name: "CREATE TABLE OF supports generated columns",
			SetUpScript: []string{
				`CREATE TYPE typed_generated_task AS (id INT, width INT, height INT, area INT);`,
				`CREATE TABLE typed_generated_tasks OF typed_generated_task (
						area WITH OPTIONS GENERATED ALWAYS AS (width * height) STORED
					);`,
				`INSERT INTO typed_generated_tasks (id, width, height) VALUES (1, 4, 5);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `INSERT INTO typed_generated_tasks (id, width, height, area) VALUES (2, 4, 5, 99);`,
					ExpectedErr: `The value specified for generated column "area" in table "typed_generated_tasks" is not allowed.`,
				},
				{
					Query: `SELECT id, area FROM typed_generated_tasks;`,
					Expected: []sql.Row{
						{int32(1), int32(20)},
					},
				},
				{
					Query: `SELECT column_name, is_generated
	FROM information_schema.columns
	WHERE table_name = 'typed_generated_tasks' AND column_name = 'area';`,
					Expected: []sql.Row{
						{"area", "ALWAYS"},
					},
				},
			},
		},
		{
			Name: "CREATE TEMP TABLE OF supports generated columns",
			SetUpScript: []string{
				`CREATE TYPE typed_temp_generated_task AS (id INT, width INT, height INT, area INT);`,
				`CREATE TEMP TABLE typed_temp_generated_tasks OF typed_temp_generated_task (
						area WITH OPTIONS GENERATED ALWAYS AS (width * height) STORED
					);`,
				`INSERT INTO typed_temp_generated_tasks (id, width, height) VALUES (1, 6, 7);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, area FROM typed_temp_generated_tasks;`,
					Expected: []sql.Row{
						{int32(1), int32(42)},
					},
				},
			},
		},
		{
			Name: "CREATE TABLE OF supports check constraints",
			SetUpScript: []string{
				`CREATE TYPE typed_check_task AS (id INT, code TEXT, priority INT);`,
				`CREATE TABLE typed_check_tasks OF typed_check_task (
						CONSTRAINT typed_check_priority CHECK (priority > 0),
						code WITH OPTIONS CONSTRAINT typed_check_code CHECK (code <> '')
					);`,
				`INSERT INTO typed_check_tasks VALUES (1, 'A', 5);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO typed_check_tasks VALUES (2, 'B', 0);`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-class-reloftype-test-testtypedtablefromcompositetype-0041-insert-into-typed_check_tasks-values-2", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO typed_check_tasks VALUES (3, '', 5);`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-class-reloftype-test-testtypedtablefromcompositetype-0042-insert-into-typed_check_tasks-values-3", Compare: "sqlstate"},
				},
				{
					Query: `UPDATE typed_check_tasks SET priority = -1 WHERE id = 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-class-reloftype-test-testtypedtablefromcompositetype-0043-update-typed_check_tasks-set-priority-=", Compare: "sqlstate"},
				},
				{
					Query: `SELECT constraint_name, constraint_type
	FROM information_schema.table_constraints
	WHERE table_name = 'typed_check_tasks' AND constraint_type = 'CHECK'
	ORDER BY constraint_name;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-class-reloftype-test-testtypedtablefromcompositetype-0044-select-constraint_name-constraint_type-from-information_schema.table_constraints"},
				},
			},
		},
		{
			Name: "CREATE TEMP TABLE OF supports check constraints",
			SetUpScript: []string{
				`CREATE TYPE typed_temp_check_task AS (id INT, priority INT);`,
				`CREATE TEMP TABLE typed_temp_check_tasks OF typed_temp_check_task (
						CONSTRAINT typed_temp_check_priority CHECK (priority > 0)
					);`,
				`INSERT INTO typed_temp_check_tasks VALUES (1, 5);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO typed_temp_check_tasks VALUES (2, 0);`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-class-reloftype-test-testtypedtablefromcompositetype-0045-insert-into-typed_temp_check_tasks-values-2", Compare: "sqlstate"},
				},
				{
					Query: `UPDATE typed_temp_check_tasks SET priority = 0 WHERE id = 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-class-reloftype-test-testtypedtablefromcompositetype-0046-update-typed_temp_check_tasks-set-priority-=", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "CREATE TABLE OF supports foreign key constraints",
			SetUpScript: []string{
				`CREATE TABLE typed_fk_parent (id INT PRIMARY KEY);`,
				`INSERT INTO typed_fk_parent VALUES (1);`,
				`CREATE TYPE typed_fk_task AS (id INT, parent_id INT, owner_id INT);`,
				`CREATE TABLE typed_fk_tasks OF typed_fk_task (
						CONSTRAINT typed_fk_tasks_parent_id_fkey FOREIGN KEY (parent_id) REFERENCES typed_fk_parent(id),
						owner_id WITH OPTIONS CONSTRAINT typed_fk_tasks_owner_id_fkey REFERENCES typed_fk_parent(id)
					);`,
				`INSERT INTO typed_fk_tasks VALUES (1, 1, 1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO typed_fk_tasks VALUES (2, 2, 1);`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-class-reloftype-test-testtypedtablefromcompositetype-0047-insert-into-typed_fk_tasks-values-2", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO typed_fk_tasks VALUES (3, 1, 3);`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-class-reloftype-test-testtypedtablefromcompositetype-0048-insert-into-typed_fk_tasks-values-3", Compare: "sqlstate"},
				},
				{
					Query: `SELECT constraint_name, constraint_type
	FROM information_schema.table_constraints
	WHERE table_name = 'typed_fk_tasks' AND constraint_type = 'FOREIGN KEY'
	ORDER BY constraint_name;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-class-reloftype-test-testtypedtablefromcompositetype-0049-select-constraint_name-constraint_type-from-information_schema.table_constraints"},
				},
			},
		},
		{
			Name: "CREATE TABLE OF rejects unsupported table-definition options",
			SetUpScript: []string{
				`CREATE TYPE typed_options AS (id INT, code TEXT);`,
				`CREATE TABLE typed_options_parent (id INT PRIMARY KEY);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE typed_unknown OF typed_options (missing WITH OPTIONS NOT NULL);`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-class-reloftype-test-testtypedtablefromcompositetype-0050-create-table-typed_unknown-of-typed_options", Compare: "sqlstate"},
				},
				{
					Query: `CREATE TABLE typed_unique_missing OF typed_options (UNIQUE (missing));`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-class-reloftype-test-testtypedtablefromcompositetype-0051-create-table-typed_unique_missing-of-typed_options", Compare: "sqlstate"},
				},
				{
					Query: `CREATE TABLE typed_unique_duplicate OF typed_options (UNIQUE (code, code));`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-class-reloftype-test-testtypedtablefromcompositetype-0052-create-table-typed_unique_duplicate-of-typed_options", Compare: "sqlstate"},
				},
				{
					Query: `CREATE TABLE typed_unique_include OF typed_options (UNIQUE (code) INCLUDE (id));`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-class-reloftype-test-testtypedtablefromcompositetype-0053-create-table-typed_unique_include-of-typed_options", Compare: "sqlstate"},
				},
				{
					Query: `CREATE TABLE typed_partitioned OF typed_options PARTITION BY LIST (id);`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-class-reloftype-test-testtypedtablefromcompositetype-0054-create-table-typed_partitioned-of-typed_options", Compare: "sqlstate"},
				},
				{
					Query: `CREATE TABLE typed_exclude OF typed_options (EXCLUDE USING gist (id WITH =));`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-class-reloftype-test-testtypedtablefromcompositetype-0055-create-table-typed_exclude-of-typed_options", Compare: "sqlstate"},
				},
				{
					Query: `CREATE TEMP TABLE typed_temp_fk OF typed_options (CONSTRAINT typed_temp_fk_parent FOREIGN KEY (id) REFERENCES typed_options_parent(id));`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-class-reloftype-test-testtypedtablefromcompositetype-0056-create-temp-table-typed_temp_fk-of", Compare: "sqlstate"},
				},
			},
		},
	})
}
