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

// TestPostgres17JsonTableRepro reproduces PostgreSQL 17 JSON_TABLE
// compatibility gaps.
func TestPostgres17JsonTableRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "JSON_TABLE",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM JSON_TABLE('[{"a":1,"b":"x"},{"a":2}]'::jsonb, '$[*]' COLUMNS (a int PATH '$.a', b text PATH '$.b')) AS jt ORDER BY a;`, PostgresOracle: ScriptTestPostgresOracle{ID: "postgres17/functions-test-testpostgres17jsontablerepro-0001-select-*-from-json_table-[{"},
				},
				{
					Query: `SELECT * FROM JSON_TABLE('{"items":[{"a":1},{"a":2}]}'::jsonb, '$.items[*]' COLUMNS (n FOR ORDINALITY, a int PATH '$.a')) AS jt;`, PostgresOracle: ScriptTestPostgresOracle{ID: "postgres17/functions-test-testpostgres17jsontablerepro-0002-select-*-from-json_table-{"},
				},
				{
					Query: `SELECT * FROM JSON_TABLE('[{"a":"3"},{"b":2}]'::jsonb, '$[*]' COLUMNS (a int)) AS jt;`, PostgresOracle: ScriptTestPostgresOracle{ID: "postgres17/functions-test-testpostgres17jsontablerepro-0003-select-*-from-json_table-[{"},
				},
				{
					Query: `SELECT * FROM JSON_TABLE('[1,2]'::jsonb, '$[*]' COLUMNS (v int PATH '$')) AS jt;`, PostgresOracle: ScriptTestPostgresOracle{ID: "postgres17/functions-test-testpostgres17jsontablerepro-0004-select-*-from-json_table-[1"},
				},
				{
					Query: `SELECT count(*) FROM JSON_TABLE(NULL::jsonb, '$[*]' COLUMNS (a int PATH '$.a')) AS jt;`, PostgresOracle: ScriptTestPostgresOracle{ID: "postgres17/functions-test-testpostgres17jsontablerepro-0005-select-count-*-from-json_table"},
				},
				{
					Query: `SELECT * FROM JSON_TABLE('[{"a":"bad"}]'::jsonb, '$[*]' COLUMNS (a int PATH '$.a')) AS jt;`, PostgresOracle: ScriptTestPostgresOracle{ID: "postgres17/functions-test-testpostgres17jsontablerepro-0006-select-*-from-json_table-[{"},
				},
			},
		},
	})
}
