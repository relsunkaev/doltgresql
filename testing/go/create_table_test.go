// Copyright 2024 Dolthub, Inc.
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

func TestCreateTable(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			// https://github.com/dolthub/doltgresql/issues/2580
			Name: "create table with UTF8 identifiers",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE foo😏(data🍆 TEXT);`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetable-0001-create-table-foo😏-data🍆-text"},
				},
				{
					Query: `CREATE INDEX idx🍤 ON foo😏(data🍆);`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetable-0002-create-index-idx🍤-on-foo😏"},
				},
				{
					Query: `Insert into foo😏 (data🍆) VALUES ('foo');`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetable-0003-insert-into-foo😏-data🍆-values"},
				},
				{
					Query: `SELECT data🍆 FROM foo😏;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetable-0004-select-data🍆-from-foo😏"},
				},
			},
		},
		{
			Name: "create table with primary key",
			Assertions: []ScriptTestAssertion{
				{
					// TODO: we don't currently have a way to check for warnings in these tests, but this query was incorrectly
					//  producing a warning. Would be nice to assert no warnings on most queries.
					Query: "create table employees (" +
						"    id int8," +
						"    last_name text," +
						"    first_name text," +
						"    primary key(id));",
				},
				{
					Query: "insert into employees (id, last_name, first_name) values (1, 'Doe', 'John');",
				},
				{
					Query: "select * from employees;",
					Expected: []sql.Row{
						{1, "Doe", "John"},
					},
				},
				{
					// Test that the PK constraint shows up in the information schema
					Query:    "SELECT conname FROM pg_constraint WHERE conrelid = 'employees'::regclass AND contype = 'p';",
					Expected: []sql.Row{{"employees_pkey"}},
				},
				{
					Query:    "ALTER TABLE employees DROP CONSTRAINT employees_pkey;",
					Expected: []sql.Row{},
				},
			},
		},
		{
			// TODO: We don't currently support storing a custom name for a primary key constraint.
			Skip: true,
			Name: "create table with primary key, using custom constraint name",
			SetUpScript: []string{
				"CREATE TABLE users (id SERIAL, name TEXT, CONSTRAINT users_primary_key PRIMARY KEY (id));",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT conname FROM pg_constraint WHERE conrelid = 'users'::regclass AND contype = 'p';", PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetable-0008-select-conname-from-pg_constraint-where"},
				},
				{
					Query: "ALTER TABLE users DROP CONSTRAINT users_primary_key;", PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetable-0009-alter-table-users-drop-constraint"},
				},
			},
		},
		{
			Name: "Create table with column default expression using function",
			Assertions: []ScriptTestAssertion{
				{
					// Test with a function in the column default expression
					Query: "create table t1 (pk int primary key, c1 TEXT default length('Hello World!'));", PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetable-0010-create-table-t1-pk-int"},
				},
				{
					Query: "insert into t1(pk) values (1);", PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetable-0011-insert-into-t1-pk-values"},
				},
				{
					Query: "select * from t1;", PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetable-0012-select-*-from-t1"},
				},
			},
		},
		{
			Name: "Create table with named default column constraint",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE zero_version_history (
						"dataVersion" int NOT NULL,
						lock char(1) NOT NULL CONSTRAINT DF_schema_meta_lock DEFAULT 'v',
						CONSTRAINT CK_schema_meta_lock CHECK (lock='v')
					);`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetable-0013-create-table-zero_version_history-dataversion-int"},
				},
				{
					Query: `INSERT INTO zero_version_history ("dataVersion") VALUES (1);`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetable-0014-insert-into-zero_version_history-dataversion-values"},
				},
				{
					Query: `SELECT "dataVersion", lock FROM zero_version_history;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetable-0015-select-dataversion-lock-from-zero_version_history"},
				},
				{
					Query: `INSERT INTO zero_version_history ("dataVersion", lock) VALUES (2, 'x');`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetable-0016-insert-into-zero_version_history-dataversion-lock", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "Create table with table check constraint",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE products (name text, price numeric, discounted_price numeric, CHECK (price > discounted_price));`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetable-0017-create-table-products-name-text"},
				},
				{
					Query: "insert into products values ('apple', 1.20, 0.80);", PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetable-0018-insert-into-products-values-apple"},
				},
				{
					// TODO: the correct error message: `new row for relation "products" violates check constraint "products_chk_rqcthh8j"`
					Query: "insert into products values ('peach', 1.20, 1.80);", PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetable-0019-insert-into-products-values-peach", Compare: "sqlstate"},
				},
				{
					Query: "select * from products;", PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetable-0020-select-*-from-products"},
				},
			},
		},
		{
			Name: "Create table with column check constraint",
			Assertions: []ScriptTestAssertion{
				{
					Query: "create table mytbl (pk int, v1 int constraint v1constraint check (v1 < 100));", PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetable-0021-create-table-mytbl-pk-int"},
				},
				{
					Query: "insert into mytbl values (1, 20);", PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetable-0022-insert-into-mytbl-values-1"},
				},
				{
					Query: "insert into mytbl values (2, 200);", PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetable-0023-insert-into-mytbl-values-2", Compare: "sqlstate"},
				},
				{
					Query: "select * from mytbl;", PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetable-0024-select-*-from-mytbl"},
				},
			},
		},
		{
			Name: "check constraint with a function",
			Assertions: []ScriptTestAssertion{
				{
					Query: "CREATE TABLE mytbl (a text CHECK (length(a) > 2) PRIMARY KEY, b text);", PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetable-0025-create-table-mytbl-a-text"},
				},
				{
					Query: "insert into mytbl values ('abc', 'def');", PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetable-0026-insert-into-mytbl-values-abc"},
				},
				{
					Query: "insert into mytbl values ('de', 'abc');", PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetable-0027-insert-into-mytbl-values-de", Compare: "sqlstate"},
				},
				{
					Query: "select * from mytbl;", PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetable-0028-select-*-from-mytbl"},
				},
			},
		},
		{
			Name: "check constraint with JSONB cast expression",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE json_checks (payload jsonb CHECK (((payload->>'amount')::int) > 0));`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetable-0029-create-table-json_checks-payload-jsonb"},
				},
				{
					Query: `INSERT INTO json_checks VALUES ('{"amount": 3}'::jsonb);`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetable-0030-insert-into-json_checks-values-{"},
				},
				{
					Query: `INSERT INTO json_checks VALUES ('{"amount": -1}'::jsonb);`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetable-0031-insert-into-json_checks-values-{", Compare: "sqlstate"},
				},
				{
					Query: `SELECT payload FROM json_checks;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetable-0032-select-payload-from-json_checks", ColumnModes: []string{"json"}},
				},
			},
		},
		{
			Skip: true, // TODO: vitess does not support multiple check constraint on a single column
			Name: "Create table with multiple check constraints on a single column",
			Assertions: []ScriptTestAssertion{
				{
					Query: "create table mytbl (pk int, v1 int constraint v1constraint check (v1 < 100) check (v1 > 10));", PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetable-0033-create-table-mytbl-pk-int"},
				},
				{
					Query: "insert into mytbl values (1, 20);", PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetable-0034-insert-into-mytbl-values-1"},
				},
				{
					Query: "insert into mytbl values (2, 200);", PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetable-0035-insert-into-mytbl-values-2", Compare: "sqlstate"},
				},
				{
					Query: "insert into mytbl values (3, 5);", PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetable-0036-insert-into-mytbl-values-3", Compare: "sqlstate"},
				},
				{
					Query: "select * from mytbl;", PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetable-0037-select-*-from-mytbl"},
				},
			},
		},
		{
			Name: "Create table with a check constraints on a single column and a table check constraint",
			Assertions: []ScriptTestAssertion{
				{
					Query: "create table mytbl (pk int, v1 int constraint v1constraint check (v1 < 100), check (v1 > 10));", PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetable-0038-create-table-mytbl-pk-int"},
				},
				{
					Query: "insert into mytbl values (1, 20);", PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetable-0039-insert-into-mytbl-values-1"},
				},
				{
					Query: "insert into mytbl values (2, 200);", PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetable-0040-insert-into-mytbl-values-2", Compare: "sqlstate"},
				},
				{
					Query: "insert into mytbl values (3, 5);", PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetable-0041-insert-into-mytbl-values-3", Compare: "sqlstate"},
				},
				{
					Query: "select * from mytbl;", PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetable-0042-select-*-from-mytbl"},
				},
			},
		},
		{
			Name: "create table with generated column",
			SetUpScript: []string{
				"create table t1 (a int primary key, b int, c int generated always as (a + b) stored);",
				"insert into t1 (a, b) values (1, 2);",
				"create table t2 (a int primary key, b int, c int generated always as (b * 10) stored);",
				"insert into t2 (a, b) values (1, 2);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "select * from t1;", PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetable-0043-select-*-from-t1"},
				},
				{
					Query: "select * from t2;", PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetable-0044-select-*-from-t2"},
				},
			},
		},
		{
			Name: "create table with function in generated column",
			SetUpScript: []string{
				"create table t1 (a varchar(10) primary key, b varchar(10), c varchar(20) generated always as (concat(a,b)) stored);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "insert into t1 (a, b) values ('foo', 'bar');",
				},
				{
					Query:    "select * from t1;",
					Expected: []sql.Row{{"foo", "bar", "foobar"}},
				},
			},
		},
		{
			Name: "generated column with complex expression",
			SetUpScript: []string{
				`create table t1 (a varchar(10) primary key,
				b varchar(20) generated always as 
				    ((
				        ("substring"(TRIM(BOTH FROM a), '([^ ]+)$'::text) || ' '::text)
				          || "substring"(TRIM(BOTH FROM a), '^([^ ]+)'::text)
				    )) stored
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "insert into t1 (a) values (' foo ');",
				},
				{
					Query: "select * from t1;", PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetable-0046-select-*-from-t1"},
				},
			},
		},
		{
			Name: "generated column with reference to another column",
			SetUpScript: []string{
				`create table t1 (
    			a varchar(10) primary key,
    			b varchar(20),
				  b_not_null bool generated always as ((b is not null)) stored
				);`,
				"insert into t1 (a, b) values ('foo', 'bar');",
				"insert into t1 (a) values ('foo2');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "select * from t1 order by a;", PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetable-0047-select-*-from-t1-order"},
				},
			},
		},
		{
			Name: "generated column with space in column name",
			SetUpScript: []string{
				`create table t1 (
    			a varchar(10) primary key,
    			"b 2" varchar(20),
				  b_not_null bool generated always as (("b 2" is not null)) stored
				);`,
				`insert into t1 (a, "b 2") values ('foo', 'bar');`,
				"insert into t1 (a) values ('foo2');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "select * from t1 order by a;", PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetable-0048-select-*-from-t1-order"},
				},
			},
		},
		{
			Name: "primary key GENERATED ALWAYS AS IDENTITY",
			SetUpScript: []string{
				`create table t1 (
    			a BIGINT NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
				  b varchar(100)
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "insert into t1 (b) values ('foo') returning a;", PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetable-0049-insert-into-t1-b-values"},
				},
				{
					Query: "insert into t1 (a, b) values (2, 'foo') returning a;", PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetable-0050-insert-into-t1-a-b", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "create table with default value",
			SetUpScript: []string{
				"create table t1 (a varchar(10) primary key, b varchar(10) default (concat('foo', 'bar')));",
				"insert into t1 (a) values ('abc');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "select * from t1;", PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetable-0051-select-*-from-t1"},
				},
			},
		},
		{
			Name: "create table with collation",
			SetUpScript: []string{
				`CREATE TABLE collate_test1 (
    a int,
        b text COLLATE "en-x-icu" NOT NULL
        )`,
				"insert into collate_test1 (a, b) values (1, 'foo');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "select * from collate_test1;", PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetable-0052-select-*-from-collate_test1"},
				},
			},
		},
		{
			Name: "inline comments",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE inline_comments (
	a int,
	b int,
	c int, -- comment on end of line
	CONSTRAINT check_b CHECK (b IS NULL OR b = 'a'),
	CONSTRAINT check_a CHECK (a IS NOT NULL AND a = 7)
);`,
				},
				{
					Query: `CREATE TABLE block_comments (
	a int,
	b /* block comment */ /* one more thing */ int, -- comment on end of line
	c int, -- comment on end of line /* block comment */
	CONSTRAINT check_b CHECK (b IS NULL OR b = 'a'),
	CONSTRAINT check_a CHECK (a IS NOT NULL AND a = 7)
);`,
				},
			},
		},
		{
			Name: "create temporary table with serial column",
			Assertions: []ScriptTestAssertion{
				{
					Query: "CREATE TEMP TABLE temp (id serial primary key)", PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetable-0053-create-temp-table-temp-id"},
				},
				{
					Query: "INSERT INTO temp DEFAULT VALUES", PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetable-0054-insert-into-temp-default-values"},
				},
				{
					Query: "INSERT INTO temp DEFAULT VALUES", PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetable-0055-insert-into-temp-default-values"},
				},
				{
					Query: "SELECT id FROM temp ORDER BY id", PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetable-0056-select-id-from-temp-order"},
				},
			},
		},
		{
			Name: "table with check constraint with ANY expression",
			SetUpScript: []string{
				`CREATE TABLE location (
    id integer NOT NULL,
    name character varying(100) NOT NULL,
    type character varying(100),
    CONSTRAINT location_type_check CHECK (((type)::text = ANY ((ARRAY['Внутренни'::character varying, 'Покупатель'::character varying, 'Поставщик'::character varying])::text[])))
);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "insert into location values (1, 'Склад Москва', 'Внутренни'), (2, 'Склад Спб', null);", PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetable-0057-insert-into-location-values-1"},
				},
				{
					Query: "SELECT * FROM location;", PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetable-0058-select-*-from-location"},
				},
			},
		},
	})
}

func TestCreateTableInherit(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "Create table with inheritance",
			SetUpScript: []string{
				"create table t1 (a int);",
				"create table t2 (b int);",
				"create table t3 (c int);",
				"create table t11 (a int);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "create table t4 (d int) inherits (t1, t2, t3);", PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetableinherit-0001-create-table-t4-d-int"},
				},
				{
					Query: "insert into t4(a, b, c, d) values (1, 2, 3, 4);", PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetableinherit-0002-insert-into-t4-a-b"},
				},
				{
					Query: "select * from t4;", PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetableinherit-0003-select-*-from-t4"},
				},
				{
					Query: "create table t111 () inherits (t1, t11);", PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetableinherit-0004-create-table-t111-inherits-t1"},
				},
				{
					Query: "insert into t111(a) values (1);", PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetableinherit-0005-insert-into-t111-a-values"},
				},
				{
					Query: "select * from t111;", PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetableinherit-0006-select-*-from-t111"},
				},
				{
					Query: "create table t1t1 (a int) inherits (t1);", PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetableinherit-0007-create-table-t1t1-a-int"},
				},
				{
					Query: "insert into t1t1(a) values (1);", PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetableinherit-0008-insert-into-t1t1-a-values"},
				},
				{
					Query: "select * from t1t1;", PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetableinherit-0009-select-*-from-t1t1"},
				},
				{
					Query: "create table TT1t1 (A int) inherits (t1);", PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetableinherit-0010-create-table-tt1t1-a-int"},
				},
				{
					Query: "insert into TT1t1(a) values (1);", PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetableinherit-0011-insert-into-tt1t1-a-values"},
				},
				{
					Query: "select * from TT1t1;", PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-test-testcreatetableinherit-0012-select-*-from-tt1t1"},
				},
			},
		},
	})
}
