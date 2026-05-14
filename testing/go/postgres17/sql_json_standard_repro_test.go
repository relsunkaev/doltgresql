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

package postgres17

import (
	. "github.com/dolthub/doltgresql/testing/go"

	"testing"
)

// TestPostgres17SqlJsonConstructorFunctionsRepro reproduces PostgreSQL 17
// SQL/JSON constructor-function compatibility gaps.
func TestPostgres17SqlJsonConstructorFunctionsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "PostgreSQL 17 SQL/JSON constructor functions evaluate JSON values",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT JSON_SCALAR('plain'::text)::text, JSON_SCALAR(123.45)::text, JSON_SCALAR(NULL::text)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "postgres17/sql-json-standard-repro-test-testpostgres17sqljsonconstructorfunctionsrepro-0001-select-json_scalar-plain-::text-::text"},
				},
				{
					Query: `SELECT JSON_SERIALIZE('{"a":1}' RETURNING text);`, PostgresOracle: ScriptTestPostgresOracle{ID: "postgres17/sql-json-standard-repro-test-testpostgres17sqljsonconstructorfunctionsrepro-0002-select-json_serialize-{-a-:1}"},
				},
			},
		},
	})
}

// TestPostgres17SqlJsonQueryFunctionsRepro reproduces PostgreSQL 17 SQL/JSON
// query-function compatibility gaps.
func TestPostgres17SqlJsonQueryFunctionsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "PostgreSQL 17 SQL/JSON query functions evaluate jsonpath expressions",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT JSON_EXISTS(
						'{"key1":[1,2,3]}'::jsonb,
						'strict $.key1[*] ? (@ > $x)' PASSING 2 AS x);`, PostgresOracle: ScriptTestPostgresOracle{ID: "postgres17/sql-json-standard-repro-test-testpostgres17sqljsonqueryfunctionsrepro-0001-select-json_exists-{-key1-:[1"},
				},
				{
					Query: `SELECT JSON_VALUE(
						'{"a":123}'::jsonb,
						'$.a' RETURNING int);`, PostgresOracle: ScriptTestPostgresOracle{ID: "postgres17/sql-json-standard-repro-test-testpostgres17sqljsonqueryfunctionsrepro-0002-select-json_value-{-a-:123}"},
				},
				{
					Query: `SELECT JSON_QUERY(
						'{"a":[1,2]}'::jsonb,
						'$.a')::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "postgres17/sql-json-standard-repro-test-testpostgres17sqljsonqueryfunctionsrepro-0003-select-json_query-{-a-:[1"},
				},
			},
		},
	})
}
