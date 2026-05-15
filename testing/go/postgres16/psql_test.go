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

func TestPsqlCommands(t *testing.T) {
	RunScripts(
		t,
		[]ScriptTest{
			{
				// Many of the psql commands use the OPERATOR(pg_catalog.+) syntax, testing it here directly in a simpler context
				Name: "operator keyword",
				Assertions: []ScriptTestAssertion{
					{
						Query: "select 1 OPERATOR(pg_catalog.+) 1", PostgresOracle: ScriptTestPostgresOracle{ID: "psql-test-testpsqlcommands-0001-select-1-operator-pg_catalog.+-1"},
					},
					{
						Query: "select 1 OPERATOR(PG_CATALOG.+) 1", PostgresOracle: ScriptTestPostgresOracle{ID: "psql-test-testpsqlcommands-0002-select-1-operator-pg_catalog.+-1"},
					},
					{
						Query: "select 1 OPERATOR(myschema.+) 1", PostgresOracle: ScriptTestPostgresOracle{ID: "psql-test-testpsqlcommands-0003-select-1-operator-myschema.+-1", Compare: "sqlstate"},
					},
					{
						Query: "select 1 OPERATOR(pg_catalog.<) 1", PostgresOracle: ScriptTestPostgresOracle{ID: "psql-test-testpsqlcommands-0004-select-1-operator-pg_catalog.<-1"},
					},
					{
						Query: "select 1 OPERATOR(myschema.<) 1", PostgresOracle: ScriptTestPostgresOracle{ID: "psql-test-testpsqlcommands-0005-select-1-operator-myschema.<-1", Compare: "sqlstate"},
					},
					{
						Query: "select 1 OPERATOR(pg_catalog.<=) 1", PostgresOracle: ScriptTestPostgresOracle{ID: "psql-test-testpsqlcommands-0006-select-1-operator-pg_catalog.<=-1"},
					},
					{
						Query: "select 1 OPERATOR(pg_catalog.=) 1", PostgresOracle: ScriptTestPostgresOracle{ID: "psql-test-testpsqlcommands-0007-select-1-operator-pg_catalog.=-1"},
					},
					{
						Query: "select 'hello' OPERATOR(pg_catalog.~) 'hello';", PostgresOracle: ScriptTestPostgresOracle{ID: "psql-test-testpsqlcommands-0008-select-hello-operator-pg_catalog.~-hello"},
					},
				},
			},
		},
	)
}
