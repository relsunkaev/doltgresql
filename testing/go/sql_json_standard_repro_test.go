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

// TestSqlJsonConstructorFunctionsRepro reproduces a PostgreSQL 16 SQL/JSON
// constructor compatibility gap.
func TestSqlJsonConstructorFunctionsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "SQL/JSON JSON constructor evaluates JSON values",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT JSON('{"a":123, "b":[true,"foo"]}')::jsonb::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "sql-json-standard-repro-test-testsqljsonconstructorfunctionsrepro-0001-select-json-{-a-:123"},
				},
			},
		},
	})
}

// TestPostgres16SqlJsonConstructorSyntaxRepro reproduces PostgreSQL 16
// compatibility gaps for SQL/JSON constructor syntax and aggregate variants.
func TestPostgres16SqlJsonConstructorSyntaxRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "PostgreSQL 16 SQL/JSON constructor syntax evaluates",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT JSON_ARRAY(1, true, 'x' RETURNING jsonb)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "sql-json-standard-repro-test-testpostgres16sqljsonconstructorsyntaxrepro-0001-select-json_array-1-true-x"},
				},
				{
					Query: `SELECT JSON_OBJECT('a' VALUE 1, 'b' VALUE 'x' RETURNING jsonb)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "sql-json-standard-repro-test-testpostgres16sqljsonconstructorsyntaxrepro-0002-select-json_object-a-value-1"},
				},
				{
					Query: `SELECT JSON_ARRAYAGG(v ORDER BY v RETURNING jsonb)::text FROM (VALUES (2), (1)) AS t(v);`, PostgresOracle: ScriptTestPostgresOracle{ID: "sql-json-standard-repro-test-testpostgres16sqljsonconstructorsyntaxrepro-0003-select-json_arrayagg-v-order-by"},
				},
				{
					Query: `SELECT JSON_OBJECTAGG(k VALUE v RETURNING jsonb)::text FROM (VALUES ('a', 1), ('b', 2)) AS t(k, v);`, PostgresOracle: ScriptTestPostgresOracle{ID: "sql-json-standard-repro-test-testpostgres16sqljsonconstructorsyntaxrepro-0004-select-json_objectagg-k-value-v"},
				},
			},
		},
	})
}
