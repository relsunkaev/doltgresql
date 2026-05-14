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

package _go

import (
	"testing"
)

func TestDropTable(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP TABLE on table type on column",
			SetUpScript: []string{
				`CREATE TABLE test1 (pk INT4 PRIMARY KEY, v1 TEXT);`,
				`CREATE TABLE test2 (v1 test1);`,
				`INSERT INTO test2 VALUES (ROW(1, 'abc')::test1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP TABLE test1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "drop-table-test-testdroptable-0001-drop-table-test1", Compare: "sqlstate"},
				},
				{
					Query: `DROP TABLE test2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "drop-table-test-testdroptable-0002-drop-table-test2"},
				},
				{
					Query: `DROP TABLE test1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "drop-table-test-testdroptable-0003-drop-table-test1"},
				},
			},
		},
		{
			Name: "DROP TABLE on table type on function parameter",
			SetUpScript: []string{
				`CREATE TABLE test (pk INT4 PRIMARY KEY, v1 TEXT);`,
				`CREATE FUNCTION example_func(t test) RETURNS INT4 AS $$ BEGIN RETURN t.pk * 2; END; $$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP TABLE test;`, PostgresOracle: ScriptTestPostgresOracle{ID: "drop-table-test-testdroptable-0004-drop-table-test", Compare: "sqlstate"},
				},
				{
					Query: `DROP FUNCTION example_func(test);`, PostgresOracle: ScriptTestPostgresOracle{ID: "drop-table-test-testdroptable-0005-drop-function-example_func-test"},
				},
				{
					Query: `DROP TABLE test;`, PostgresOracle: ScriptTestPostgresOracle{ID: "drop-table-test-testdroptable-0006-drop-table-test"},
				},
			},
		},
		{
			Name:        "DROP TABLE CASCADE keyword without dependent objects",
			SetUpScript: []string{`CREATE TABLE cascade_ok (pk INT4 PRIMARY KEY);`},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP TABLE IF EXISTS missing_cascade_ok CASCADE;`, PostgresOracle: ScriptTestPostgresOracle{ID: "drop-table-test-testdroptable-0007-drop-table-if-exists-missing_cascade_ok"},
				},
				{
					Query: `DROP TABLE cascade_ok CASCADE;`, PostgresOracle: ScriptTestPostgresOracle{ID: "drop-table-test-testdroptable-0008-drop-table-cascade_ok-cascade"},
				},
			},
		},
		{
			Name: "DROP TABLE on table type on procedure parameter",
			SetUpScript: []string{
				`CREATE TABLE test1 (pk INT4 PRIMARY KEY, v1 TEXT);`,
				`CREATE TABLE test2 (v1 INT4);`,
				`CREATE PROCEDURE example_proc(input test1) AS $$ BEGIN INSERT INTO test2 VALUES (input.pk); END; $$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP TABLE test1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "drop-table-test-testdroptable-0009-drop-table-test1", Compare: "sqlstate"},
				},
				{
					Query: `DROP PROCEDURE example_proc(test1);`, PostgresOracle: ScriptTestPostgresOracle{ID: "drop-table-test-testdroptable-0010-drop-procedure-example_proc-test1"},
				},
				{
					Query: `DROP TABLE test1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "drop-table-test-testdroptable-0011-drop-table-test1"},
				},
			},
		},
		{
			Name: "DROP TABLE on table type on column concurrent",
			SetUpScript: []string{
				`CREATE TABLE test1 (pk INT4 PRIMARY KEY, v1 TEXT);`,
				`CREATE TABLE test2 (v1 test1);`,
				`INSERT INTO test2 VALUES (ROW(1, 'abc')::test1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP TABLE test1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "drop-table-test-testdroptable-0012-drop-table-test1", Compare: "sqlstate"},
				},
				{
					Query: `DROP TABLE test1, test2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "drop-table-test-testdroptable-0013-drop-table-test1-test2"},
				},
			},
		},
	})
}
