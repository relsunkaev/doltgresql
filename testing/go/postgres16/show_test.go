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

package postgres16

import (
	. "github.com/dolthub/doltgresql/testing/go"
	"testing"
)

func TestShowTables(t *testing.T) {
	RunScripts(
		t,
		[]ScriptTest{
			{
				Name: "show tables in single schema",
				SetUpScript: []string{
					// Create a sequence to ensure it isn't included
					`CREATE SEQUENCE seq1;`,
					`CREATE TABLE t1 (a INT PRIMARY KEY, name TEXT)`,
					`CREATE TABLE t2 (b INT PRIMARY KEY, name TEXT)`,
					`create schema schema2`,
					`create database db2`,
				},
				Assertions: []ScriptTestAssertion{
					{
						Query: `SHOW TABLES`, PostgresOracle: ScriptTestPostgresOracle{ID: "show-test-testshowtables-0001-show-tables", Compare: "sqlstate"},
					},
					{
						Query: `SHOW TABLES from public`, PostgresOracle: ScriptTestPostgresOracle{ID: "show-test-testshowtables-0002-show-tables-from-public", Compare: "sqlstate"},
					},
					{
						Query: `SHOW TABLES from schema2`, PostgresOracle: ScriptTestPostgresOracle{ID: "show-test-testshowtables-0003-show-tables-from-schema2", Compare: "sqlstate"},
					},
					{
						Query: `SHOW TABLES from schema3`, PostgresOracle: ScriptTestPostgresOracle{ID: "show-test-testshowtables-0004-show-tables-from-schema3", Compare: "sqlstate"},
					},
					{
						Query: `SHOW TABLES from postgres.public`, PostgresOracle: ScriptTestPostgresOracle{ID: "show-test-testshowtables-0005-show-tables-from-postgres.public", Compare: "sqlstate"},
					},
					{
						Query: `SHOW TABLES from postgres.schema2`, PostgresOracle: ScriptTestPostgresOracle{ID: "show-test-testshowtables-0006-show-tables-from-postgres.schema2", Compare: "sqlstate"},
					},
					{
						Query: `SHOW TABLES from postgres.schema3`, PostgresOracle: ScriptTestPostgresOracle{ID: "show-test-testshowtables-0007-show-tables-from-postgres.schema3", Compare: "sqlstate"},
					},
					{
						Query: `SHOW TABLES from db3`, PostgresOracle: ScriptTestPostgresOracle{ID: "show-test-testshowtables-0008-show-tables-from-db3", Compare: "sqlstate"},
					},
				},
			},
		},
	)
}
