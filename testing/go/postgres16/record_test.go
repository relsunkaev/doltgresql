// Copyright 2025 Dolthub, Inc.
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

package postgres16

import (
	. "github.com/dolthub/doltgresql/testing/go"

	"testing"
)

func TestRecords(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "Record cannot be used as column type",
			SetUpScript: []string{
				"CREATE TABLE t2 (pk INT PRIMARY KEY, c1 VARCHAR(100));",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "CREATE TABLE t (pk INT PRIMARY KEY, r RECORD);", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0001-create-table-t-pk-int", Compare: "sqlstate"},
				},
				{
					Query: "ALTER TABLE t2 ADD COLUMN c2 RECORD;", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0002-alter-table-t2-add-column", Compare: "sqlstate"},
				},
				{
					Query: "ALTER TABLE t2 ALTER COLUMN c1 TYPE RECORD;", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0003-alter-table-t2-alter-column", Compare: "sqlstate"},
				},
				{
					Query: "CREATE DOMAIN my_domain AS record;", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0004-create-domain-my_domain-as-record", Compare: "sqlstate"},
				},
				{
					Query: "CREATE SEQUENCE my_seq AS record;", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0005-create-sequence-my_seq-as-record", Compare: "sqlstate"},
				},
				{
					Query: "CREATE TYPE outer_type AS (id int, payload record);", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0006-create-type-outer_type-as-id", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "Casting to record",
			Assertions: []ScriptTestAssertion{
				{
					Query: "select row(1, 1)::record;", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0007-select-row-1-1-::record"},
				},
			},
		},
		{
			// TODO: Wrapping table rows with ROW() is not supported yet. Planbuilder assumes the
			//       table alias is a column name and not a table.
			Name: "ROW() wrapping table rows",
			SetUpScript: []string{
				"create table users (name text, location text, age int);",
				"insert into users values ('jason', 'SEA', 42), ('max', 'SFO', 31);",
			},
			Assertions: []ScriptTestAssertion{
				{
					// TODO: ERROR: column "p" could not be found in any table in scope
					Skip:  true,
					Query: "select row(p) from users p;", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0008-select-row-p-from-users"},
				},
				{
					// TODO: ERROR: name resolution on this statement is not yet supported
					Skip:  true,
					Query: "select row(p.*, 42) from users p;", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0009-select-row-p.*-42-from"},
				},
				{
					// TODO: ERROR: (E).x is not yet supported
					Skip:  true,
					Query: "SELECT (u).location FROM users u;", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0010-select-u-.location-from-users"},
				},
			},
		},
		{
			Name: "ROW() wrapping values",
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT ROW(1, 2, 3) as myRow;", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0011-select-row-1-2-3"},
				},
				{
					Query: "SELECT (4, 5, 6) as myRow;", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0012-select-4-5-6-as"},
				},
				{
					Query: "SELECT (NULL, 'foo', NULL) as myRow;", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0013-select-null-foo-null-as"},
				},
				{
					Query: "SELECT (NULL, (1 > 0), 'baz') as myRow;", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0014-select-null-1->-0"},
				},
			},
		},
		{
			Name: "ROW() equality and comparison",
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT ROW(1, 'x') = ROW(1, 'x');", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0015-select-row-1-x-="},
				},
				{
					Query: "SELECT ROW(1, 'x') = ROW(1, 'y');", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0016-select-row-1-x-="},
				},
				{
					Query: "SELECT ROW(1, NULL) = ROW(1, 1);", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0017-select-row-1-null-="},
				},
				{
					Query: "SELECT ROW(1, 2) < ROW(1, 3);", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0018-select-row-1-2-<"},
				},
				{
					Query: "SELECT ROW(1, 2) < ROW(2, NULL);", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0019-select-row-1-2-<"},
				},
				{
					Query: "SELECT ROW(2, 2) < ROW(2, NULL);", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0020-select-row-2-2-<"},
				},
				{
					Query: "SELECT ROW(2, 2, 1) < ROW(2, NULL, 2);", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0021-select-row-2-2-1"},
				},
				{
					Query: "SELECT ROW(1, 2) < ROW(NULL, 3);", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0022-select-row-1-2-<"},
				},
				{
					Query: "SELECT ROW(NULL, NULL, NULL) < ROW(NULL, NULL, NULL);", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0023-select-row-null-null-null"},
				},
				{
					Query: "SELECT ROW(1, 2) <= ROW(1, 3);", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0024-select-row-1-2-<="},
				},
				{
					Query: "SELECT ROW(1, 2) <= ROW(1, 2);", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0025-select-row-1-2-<="},
				},
				{
					Query: "SELECT ROW(1, NULL) <= ROW(1, 2);", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0026-select-row-1-null-<="},
				},
				{
					Query: "SELECT ROW(2, 1) > ROW(1, 999);", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0027-select-row-2-1->"},
				},
				{
					Query: "SELECT ROW(2, 1) > ROW(1, NULL);", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0028-select-row-2-1->"},
				},
				{
					Query: "SELECT ROW(2, 1) >= ROW(1, 999);", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0029-select-row-2-1->="},
				},
				{
					Query: "SELECT ROW(2, 1) >= ROW(2, 1);", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0030-select-row-2-1->="},
				},
				{
					Query: "SELECT ROW(NULL, 1) >= ROW(2, 1);", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0031-select-row-null-1->="},
				},
				{
					Query: "SELECT ROW(1, 2) != ROW(3, 4);", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0032-select-row-1-2-!="},
				},
				{
					Query: "SELECT ROW(1, 2) != ROW(NULL, 4);", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0033-select-row-1-2-!="},
				},
				{
					Query: "SELECT ROW(NULL, 4) != ROW(NULL, 4);", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0034-select-row-null-4-!="},
				},
				{
					// TODO: IS NOT DISTINCT FROM is not yet supported
					Skip:  true,
					Query: "SELECT ROW(1, NULL) IS NOT DISTINCT FROM ROW(1, NULL);", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0035-select-row-1-null-is"},
				},
				{
					Query: "SELECT ROW(1, '2') = ROW(1, 2::TEXT);", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0036-select-row-1-2-="},
				},
			},
		},
		{
			Name: "ROW() use inserting and selecting composite rows",
			SetUpScript: []string{
				"CREATE TYPE user_info AS (id INT, name TEXT, email TEXT);",
				"CREATE TABLE accounts (info user_info);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "INSERT INTO accounts VALUES (ROW(1, 'alice', 'a@example.com'));", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0037-insert-into-accounts-values-row"},
				},
				{
					Query: "SELECT info FROM accounts;", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0038-select-info-from-accounts"},
				},
				{
					Query: "SELECT (a.info).name FROM accounts a;", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0039-select-a.info-.name-from-accounts"},
				},
			},
		},
		{
			Name: "ROW() use in WHERE clause",
			SetUpScript: []string{
				"create table users (id int primary key, name text, email text);",
				"insert into users values (1, 'John', 'j@a.com'), (2, 'Joe', 'joe@joe.com');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT * FROM users WHERE ROW(id, name, email) = ROW(1, 'John', 'j@a.com');", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0040-select-*-from-users-where"},
				},
				{
					// TODO: IS NOT DISTINCT FROM is not yet supported
					Skip:  true,
					Query: "SELECT * FROM users WHERE ROW(id, name) IS NOT DISTINCT FROM ROW(2, 'Jane');", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0041-select-*-from-users-where"},
				},
			},
		},
		{
			Name: "ROW() casting and type inference",
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT ROW(1, 'a')::record;", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0042-select-row-1-a-::record"},
				},
				{
					Query: "SELECT ROW(1, 2) = ROW(1, 'two');", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0043-select-row-1-2-=", Compare: "sqlstate"},
				},
				{
					Query: "SELECT ROW(1, 2) = ROW(1, '2');", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0044-select-row-1-2-="},
				},
			},
		},
		{
			Name: "ROW() error cases and edge conditions",
			SetUpScript: []string{
				"create table users (id int primary key, name text, email text);",
				"insert into users values (1, 'John', 'j@a.com'), (2, 'Joe', 'joe@joe.com');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT ROW(1, 2) = ROW(1);", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0045-select-row-1-2-=", Compare: "sqlstate"},
				},
				{
					Query: "SELECT ROW(1, 2) = ROW(1, 2, 3);", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0046-select-row-1-2-=", Compare: "sqlstate"},
				},
				{
					Query: "SELECT ROW(1, 2) < ROW(1);", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0047-select-row-1-2-<", Compare: "sqlstate"},
				},
				{
					Query: "SELECT ROW(1, 2) <= ROW(1);", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0048-select-row-1-2-<=", Compare: "sqlstate"},
				},
				{
					Query: "SELECT ROW(1, 2) > ROW(1);", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0049-select-row-1-2->", Compare: "sqlstate"},
				},
				{
					Query: "SELECT ROW(1, 2) >= ROW(1);", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0050-select-row-1-2->=", Compare: "sqlstate"},
				},
				{
					Query: "SELECT ROW(1, 2) != ROW(1);", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0051-select-row-1-2-!=", Compare: "sqlstate"},
				},
				{
					Query: "SELECT NULL::record IS NULL", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0052-select-null::record-is-null"},
				},
				{
					Query: "SELECT ROW(NULL) IS NULL", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0053-select-row-null-is-null"},
				},
				{
					Query: "SELECT ROW(NULL, NULL, NULL) IS NULL;", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0054-select-row-null-null-null"},
				},
				{
					Query: "SELECT ROW(NULL, 42, NULL) IS NULL;", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0055-select-row-null-42-null"},
				},
				{
					Query: "SELECT ROW(42) IS NULL", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0056-select-row-42-is-null"},
				},
				{
					Query: "SELECT ROW(NULL) IS NOT NULL;", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0057-select-row-null-is-not"},
				},
				{
					Query: "SELECT ROW(NULL, NULL) IS NOT NULL;", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0058-select-row-null-null-is"},
				},
				{
					Query: "SELECT ROW(NULL, 1) IS NOT NULL;", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0059-select-row-null-1-is"},
				},
				{
					Query: "SELECT ROW(1, 1) IS NOT NULL;", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0060-select-row-1-1-is"},
				},
				{
					Query: "SELECT ROW(42) IS NOT NULL;", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0061-select-row-42-is-not"},
				},
				{
					Query: "SELECT ROW(id, name), COUNT(*) FROM users GROUP BY ROW(id, name);", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0062-select-row-id-name-count"},
				},
			},
		},
		{
			Name: "ROW() nesting",
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT ROW(ROW(1, 'x'), true);", PostgresOracle: ScriptTestPostgresOracle{ID: "record-test-testrecords-0063-select-row-row-1-x"},
				},
			},
		},
	})
}
