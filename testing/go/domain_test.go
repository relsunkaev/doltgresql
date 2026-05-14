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

func TestDomain(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "create domain",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE DOMAIN year AS integer CONSTRAINT not_null_c NOT NULL CONSTRAINT null_c  NULL;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-test-testdomain-0001-create-domain-year-as-integer", Compare: "sqlstate"},
				},
				{
					Query: `CREATE DOMAIN year AS integer NULL NOT NULL;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-test-testdomain-0002-create-domain-year-as-integer", Compare: "sqlstate"},
				},
				{
					Query: `CREATE DOMAIN year AS integer DEFAULT 1999 NOT NULL CONSTRAINT year_check CHECK (((VALUE >= 1901) AND (VALUE <= 2155)));`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-test-testdomain-0003-create-domain-year-as-integer"},
				},
				{
					Query: `CREATE DOMAIN year AS integer CONSTRAINT year_check CHECK (((VALUE >= 1901) AND (VALUE <= 2155)));`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-test-testdomain-0004-create-domain-year-as-integer", Compare: "sqlstate"},
				},
				{
					Query: `CREATE DOMAIN year_with_check AS integer CONSTRAINT year_check CHECK (((VALUE >= 1901) AND (VALUE <= 2155)));`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-test-testdomain-0005-create-domain-year_with_check-as-integer"},
				},
				{
					Query: `CREATE DOMAIN year_with_two_checks AS integer CONSTRAINT year_check_min CHECK (VALUE >= 1901) CONSTRAINT year_check_max CHECK (VALUE <= 2155);`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-test-testdomain-0006-create-domain-year_with_two_checks-as-integer"},
				},
				{
					Query: `CREATE TABLE test_table (id int primary key, v non_existing_domain);`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-test-testdomain-0007-create-table-test_table-id-int", Compare: "sqlstate"},
				},
				{
					Query: `SELECT conname, contype, conrelid, contypid from pg_constraint WHERE conname IN ('year_check', 'year_check_min', 'year_check_max') ORDER BY conname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-test-testdomain-0008-select-conname-contype-conrelid-contypid"},
				},
			},
		},
		{
			Name:        "multiple checks",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE DOMAIN d1 AS integer CONSTRAINT check1 CHECK (VALUE > 100) CONSTRAINT check2 CHECK (VALUE < 200);`,
				},
				{
					Query: `SELECT conname, contype, conrelid, contypid from pg_constraint WHERE conname IN ('check1', 'check2') ORDER BY conname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-test-testdomain-0009-select-conname-contype-conrelid-contypid"},
				},
				{
					Query: "create table t1 (pk int primary key, v d1);",
				},
				{
					Query: "insert into t1 values (1, 50);", PostgresOracle: ScriptTestPostgresOracle{ID: "domain-test-testdomain-0010-insert-into-t1-values-1", Compare: "sqlstate"},
				},
				{
					Query: "insert into t1 values (2, 150);",
				},
				{
					Query: "insert into t1 values (3, 250);", PostgresOracle: ScriptTestPostgresOracle{ID: "domain-test-testdomain-0011-insert-into-t1-values-3", Compare: "sqlstate"},
				},
				{
					Query: "CREATE DOMAIN d2 AS integer CHECK (VALUE > 300) CHECK (VALUE < 400);",
				},
				{
					Query: `SELECT conname, contype, conrelid, contypid from pg_constraint WHERE conname IN ('d2_check', 'd2_check1') ORDER BY conname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-test-testdomain-0012-select-conname-contype-conrelid-contypid"},
				},
				{
					Query: "CREATE DOMAIN d3 AS integer CONSTRAINT d3_check1 CHECK (VALUE > 300) CHECK (VALUE < 400);",
				},
				{
					// TODO: this is slightly different behavior from Postgres, but the important thing is two different names are generated
					Query: `SELECT conname, contype, conrelid, contypid from pg_constraint WHERE conname IN ('d3_check1', 'd3_check') ORDER BY conname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-test-testdomain-0013-select-conname-contype-conrelid-contypid"},
				},
			},
		},
		{
			Name: "create table with domain type",
			SetUpScript: []string{
				`CREATE DOMAIN year AS integer CONSTRAINT year_check CHECK (((VALUE >= 1901) AND (VALUE <= 2155)));`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE table_with_domain (pk int primary key, y year);`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-test-testdomain-0014-create-table-table_with_domain-pk-int"},
				},
				{
					Query: `INSERT INTO table_with_domain VALUES (1, 1999)`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-test-testdomain-0015-insert-into-table_with_domain-values-1"},
				},
				{
					Query: `INSERT INTO table_with_domain VALUES (2, 1899)`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-test-testdomain-0016-insert-into-table_with_domain-values-2", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM table_with_domain`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-test-testdomain-0017-select-*-from-table_with_domain"},
				},
			},
		},
		{
			Name: "create table with domain type with default value",
			SetUpScript: []string{
				`CREATE DOMAIN year AS integer DEFAULT 2000;`,
				`CREATE TABLE table_with_domain_with_default (pk int primary key, y year);`,
				`INSERT INTO table_with_domain_with_default VALUES (1, 1999)`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO table_with_domain_with_default(pk) VALUES (2)`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-test-testdomain-0018-insert-into-table_with_domain_with_default-pk-values"},
				},
				{
					Query: `SELECT * FROM table_with_domain_with_default`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-test-testdomain-0019-select-*-from-table_with_domain_with_default"},
				},
			},
		},
		{
			Name: "create table with domain type with not null constraint",
			SetUpScript: []string{
				`CREATE DOMAIN year AS integer NOT NULL;`,
				`CREATE TABLE tbl_not_null (pk int primary key, y year);`,
				`INSERT INTO tbl_not_null VALUES (1, 1999)`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO tbl_not_null VALUES (2, null)`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-test-testdomain-0020-insert-into-tbl_not_null-values-2", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO tbl_not_null(pk) VALUES (2)`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-test-testdomain-0021-insert-into-tbl_not_null-pk-values", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM tbl_not_null`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-test-testdomain-0022-select-*-from-tbl_not_null"},
				},
			},
		},
		{
			Name: "update on table with domain type",
			SetUpScript: []string{
				`CREATE DOMAIN year AS integer NOT NULL CONSTRAINT year_check_min CHECK (VALUE >= 1901) CONSTRAINT year_check_max CHECK (VALUE <= 2155);`,
				`CREATE TABLE test_table (pk int primary key, y year);`,
				`INSERT INTO test_table VALUES (1, 1999), (2, 2000)`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE test_table SET y = 1902 WHERE pk = 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-test-testdomain-0023-update-test_table-set-y-="},
				},
				{
					Query: `UPDATE test_table SET y = 1900 WHERE pk = 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-test-testdomain-0024-update-test_table-set-y-=", Compare: "sqlstate"},
				},
				{
					Query: `UPDATE test_table SET y = null WHERE pk = 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-test-testdomain-0025-update-test_table-set-y-=", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM test_table`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-test-testdomain-0026-select-*-from-test_table"},
				},
			},
		},
		{
			Name: "domain type as text type",
			SetUpScript: []string{
				`CREATE DOMAIN non_empty_string AS text NULL CONSTRAINT name_check CHECK (VALUE <> '');`,
				`CREATE TABLE non_empty_string_t (id int primary key, first_name non_empty_string, last_name non_empty_string);`,
				`INSERT INTO non_empty_string_t VALUES (1, 'John', 'Doe')`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO non_empty_string_t VALUES (2, 'Jane', 'Doe')`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-test-testdomain-0027-insert-into-non_empty_string_t-values-2"},
				},
				{
					Query: `UPDATE non_empty_string_t SET last_name = '' WHERE first_name = 'Jane'`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-test-testdomain-0028-update-non_empty_string_t-set-last_name-=", Compare: "sqlstate"},
				},
				{
					Query: `UPDATE non_empty_string_t SET last_name = NULL WHERE first_name = 'Jane'`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-test-testdomain-0029-update-non_empty_string_t-set-last_name-="},
				},
				{
					Query: `SELECT * FROM non_empty_string_t`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-test-testdomain-0030-select-*-from-non_empty_string_t"},
				},
			},
		},
		{
			Name: "drop domain",
			SetUpScript: []string{
				`CREATE DOMAIN year AS integer CONSTRAINT year_check CHECK (((VALUE >= 1901) AND (VALUE <= 2155)));`,
				`CREATE TABLE table_with_domain (pk int primary key, y year);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP DOMAIN year;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-test-testdomain-0031-drop-domain-year", Compare: "sqlstate"},
				},
				{
					Query: `DROP TABLE table_with_domain;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-test-testdomain-0032-drop-table-table_with_domain"},
				},
				{
					Query: `DROP DOMAIN year;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-test-testdomain-0033-drop-domain-year"},
				},
				{
					Query: `DROP DOMAIN IF EXISTS year;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-test-testdomain-0034-drop-domain-if-exists-year"},
				},
				{
					Query:    `DROP DOMAIN IF EXISTS postgres.public.year;`,
					Expected: []sql.Row{},
				},
				{
					Query:       `DROP DOMAIN IF EXISTS mydb.public.year;`,
					ExpectedErr: `DROP DOMAIN is currently only supported for the current database`,
				},
				{
					Query:       `DROP DOMAIN non_existing_domain;`,
					ExpectedErr: `type "non_existing_domain" does not exist`,
				},
			},
		},
		{
			Name: "explicit cast to domain type",
			SetUpScript: []string{
				`CREATE DOMAIN year_not_null AS integer NOT NULL CONSTRAINT year_check CHECK (((VALUE >= 1901) AND (VALUE <= 2155)));`,
				`CREATE TABLE test_table (year integer);`,
				`INSERT INTO test_table VALUES (2000), (2024);`,
				`CREATE TABLE my_table (id integer);`,
				`INSERT INTO my_table VALUES (2000), (2002);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT 1903::year_not_null;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-test-testdomain-0038-select-1903::year_not_null"},
				},
				{
					Query: `SELECT 1903::year_not_null::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-test-testdomain-0039-select-1903::year_not_null::text"},
				},
				{
					Query: `SELECT 1900::year_not_null;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-test-testdomain-0040-select-1900::year_not_null", Compare: "sqlstate"},
				},
				{
					Query: `SELECT NULL::year_not_null;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-test-testdomain-0041-select-null::year_not_null", Compare: "sqlstate"},
				},
				{
					Query: `SELECT year::year_not_null from test_table order by year;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-test-testdomain-0042-select-year::year_not_null-from-test_table-order"},
				},
				{
					Query: `INSERT INTO test_table VALUES (null);`,
				},
				{
					Query: `SELECT year::year_not_null from test_table;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-test-testdomain-0043-select-year::year_not_null-from-test_table", Compare: "sqlstate"},
				},
				{
					Query: `SELECT id::year_not_null from my_table order by id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-test-testdomain-0044-select-id::year_not_null-from-my_table-order"},
				},
				{
					Query: `INSERT INTO my_table VALUES (2156);`,
				},
				{
					Query: `SELECT id::year_not_null from my_table;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-test-testdomain-0045-select-id::year_not_null-from-my_table", Compare: "sqlstate"},
				},
			},
		},
	})
}
