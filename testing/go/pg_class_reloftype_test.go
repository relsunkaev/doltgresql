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
	})
}
