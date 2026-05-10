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
ORDER BY relname;`,
					Expected: []sql.Row{
						{"plain_t", uint32(0)},
						{"with_unique", uint32(0)},
					},
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
					Query: `SELECT id, name, active FROM typed_people;`,
					Expected: []sql.Row{
						{int32(1), "Ada", "t"},
					},
				},
				{
					Query: `SELECT c.reloftype = t.oid
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_type t ON t.typname = 'typed_person'
WHERE c.relname = 'typed_people';`,
					Expected: []sql.Row{
						{"t"},
					},
				},
				{
					Query: `SELECT table_name, is_typed, user_defined_type_catalog, user_defined_type_schema, user_defined_type_name
FROM information_schema.tables
WHERE table_name IN ('plain_t', 'typed_people')
ORDER BY table_name;`,
					Expected: []sql.Row{
						{"plain_t", "NO", nil, nil, nil},
						{"typed_people", "YES", "postgres", "public", "typed_person"},
					},
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
					Query:       `CREATE TABLE typed_missing OF missing_type;`,
					ExpectedErr: `type "missing_type" does not exist`,
				},
				{
					Query:       `CREATE TABLE typed_mood OF mood_enum;`,
					ExpectedErr: `type "mood_enum" is not a composite type`,
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
					Query: `SELECT street, zip FROM app.addresses;`,
					Expected: []sql.Row{
						{"Main", int32(85001)},
					},
				},
				{
					Query: `SELECT table_schema, table_name, is_typed, user_defined_type_catalog, user_defined_type_schema, user_defined_type_name
FROM information_schema.tables
WHERE table_schema = 'app' AND table_name = 'addresses';`,
					Expected: []sql.Row{
						{"app", "addresses", "YES", "postgres", "app", "typed_address"},
					},
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
					Query:    `CREATE TEMP TABLE typed_scratch_rows OF typed_scratch;`,
					Expected: []sql.Row{},
				},
				{
					Query:    `INSERT INTO typed_scratch_rows VALUES (1, 'temp row');`,
					Expected: []sql.Row{},
				},
				{
					Query: `SELECT id, note FROM typed_scratch_rows;`,
					Expected: []sql.Row{
						{int32(1), "temp row"},
					},
				},
				{
					Query:    `CREATE TEMP TABLE IF NOT EXISTS typed_scratch_rows OF missing_type;`,
					Expected: []sql.Row{},
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
					Query: `SELECT id, code, note FROM typed_tasks;`,
					Expected: []sql.Row{
						{int32(1), "A", nil},
					},
				},
				{
					Query: `SELECT column_name, is_nullable
FROM information_schema.columns
WHERE table_name = 'typed_tasks' AND column_name IN ('id', 'code', 'note')
ORDER BY ordinal_position;`,
					Expected: []sql.Row{
						{"id", "NO"},
						{"code", "NO"},
						{"note", "YES"},
					},
				},
				{
					Query: `SELECT constraint_name, constraint_type
FROM information_schema.table_constraints
WHERE table_name = 'typed_tasks' AND constraint_type = 'PRIMARY KEY';`,
					Expected: []sql.Row{
						{"typed_tasks_pkey", "PRIMARY KEY"},
					},
				},
				{
					Query:       `INSERT INTO typed_tasks (id, code) VALUES (2, NULL);`,
					ExpectedErr: `non-nullable`,
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
					Query:       `INSERT INTO typed_unique_tasks VALUES (3, 'A', 'third');`,
					ExpectedErr: `duplicate unique key`,
				},
				{
					Query:       `INSERT INTO typed_unique_tasks VALUES (4, 'C', 'second');`,
					ExpectedErr: `duplicate unique key`,
				},
				{
					Query: `SELECT constraint_name, constraint_type
FROM information_schema.table_constraints
WHERE table_name = 'typed_unique_tasks' AND constraint_type = 'UNIQUE'
ORDER BY constraint_name;`,
					Expected: []sql.Row{
						{"typed_unique_tasks_code_key", "UNIQUE"},
						{"typed_unique_tasks_note_key", "UNIQUE"},
					},
				},
				{
					Query: `SELECT indexname
FROM pg_catalog.pg_indexes
WHERE tablename = 'typed_unique_tasks'
ORDER BY indexname;`,
					Expected: []sql.Row{
						{"typed_unique_tasks_code_key"},
						{"typed_unique_tasks_note_key"},
					},
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
					Query:       `INSERT INTO typed_temp_unique_tasks VALUES (3, 'A', 'third');`,
					ExpectedErr: `duplicate unique key`,
				},
				{
					Query:       `INSERT INTO typed_temp_unique_tasks VALUES (4, 'C', 'second');`,
					ExpectedErr: `duplicate unique key`,
				},
				{
					Query: `INSERT INTO typed_temp_unique_tasks VALUES (5, NULL, NULL);`,
				},
				{
					Query: `INSERT INTO typed_temp_unique_tasks VALUES (6, NULL, NULL);`,
				},
				{
					Query:       `UPDATE typed_temp_unique_tasks SET code = 'B' WHERE id = 1;`,
					ExpectedErr: `duplicate unique key`,
				},
				{
					Query:       `INSERT INTO typed_temp_unique_tasks VALUES (7, 'D', 'fourth'), (8, 'D', 'fifth');`,
					ExpectedErr: `duplicate unique key`,
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
					Query: `SELECT id, code, active, priority FROM typed_default_tasks ORDER BY id;`,
					Expected: []sql.Row{
						{int32(1), "new", "t", int32(-7)},
						{int32(2), "custom", "f", int32(3)},
					},
				},
				{
					Query: `SELECT column_name, (column_default IS NOT NULL)::text
	FROM information_schema.columns
	WHERE table_name = 'typed_default_tasks' AND column_name IN ('code', 'active', 'priority')
	ORDER BY ordinal_position;`,
					Expected: []sql.Row{
						{"code", "true"},
						{"active", "true"},
						{"priority", "true"},
					},
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
					Query: `SELECT id, code, priority FROM typed_temp_default_tasks;`,
					Expected: []sql.Row{
						{int32(1), "temp", int32(5)},
					},
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
					Query: `SELECT id, code, priority FROM typed_expr_default_tasks;`,
					Expected: []sql.Row{
						{int32(1), "new", int32(5)},
					},
				},
				{
					Query: `SELECT column_name, (column_default IS NOT NULL)::text
	FROM information_schema.columns
	WHERE table_name = 'typed_expr_default_tasks' AND column_name IN ('code', 'priority')
	ORDER BY ordinal_position;`,
					Expected: []sql.Row{
						{"code", "true"},
						{"priority", "true"},
					},
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
					Query: `SELECT id, code FROM typed_temp_expr_default_tasks;`,
					Expected: []sql.Row{
						{int32(1), "TEMP"},
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
					Query:       `INSERT INTO typed_check_tasks VALUES (2, 'B', 0);`,
					ExpectedErr: `Check constraint "typed_check_priority" violated`,
				},
				{
					Query:       `INSERT INTO typed_check_tasks VALUES (3, '', 5);`,
					ExpectedErr: `Check constraint "typed_check_code" violated`,
				},
				{
					Query:       `UPDATE typed_check_tasks SET priority = -1 WHERE id = 1;`,
					ExpectedErr: `Check constraint "typed_check_priority" violated`,
				},
				{
					Query: `SELECT constraint_name, constraint_type
	FROM information_schema.table_constraints
	WHERE table_name = 'typed_check_tasks' AND constraint_type = 'CHECK'
	ORDER BY constraint_name;`,
					Expected: []sql.Row{
						{"typed_check_code", "CHECK"},
						{"typed_check_priority", "CHECK"},
					},
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
					Query:       `INSERT INTO typed_temp_check_tasks VALUES (2, 0);`,
					ExpectedErr: `Check constraint "typed_temp_check_priority" violated`,
				},
				{
					Query:       `UPDATE typed_temp_check_tasks SET priority = 0 WHERE id = 1;`,
					ExpectedErr: `Check constraint "typed_temp_check_priority" violated`,
				},
			},
		},
		{
			Name: "CREATE TABLE OF rejects unsupported table-definition options",
			SetUpScript: []string{
				`CREATE TYPE typed_options AS (id INT, code TEXT);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `CREATE TABLE typed_unknown OF typed_options (missing WITH OPTIONS NOT NULL);`,
					ExpectedErr: `column "missing" does not exist in composite type "typed_options"`,
				},
				{
					Query:       `CREATE TABLE typed_unique_missing OF typed_options (UNIQUE (missing));`,
					ExpectedErr: `column "missing" does not exist in composite type "typed_options"`,
				},
				{
					Query:       `CREATE TABLE typed_unique_duplicate OF typed_options (UNIQUE (code, code));`,
					ExpectedErr: `column "code" appears twice in unique constraint`,
				},
				{
					Query:       `CREATE TABLE typed_unique_nulls_not_distinct OF typed_options (UNIQUE NULLS NOT DISTINCT (code));`,
					ExpectedErr: `CREATE TABLE OF UNIQUE NULLS NOT DISTINCT constraints are not yet supported`,
				},
			},
		},
	})
}
