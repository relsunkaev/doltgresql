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

package postgres18

import (
	. "github.com/dolthub/doltgresql/testing/go"

	"testing"
)

// TestPostgres18JsonStripNullsStripInArraysRepro reproduces a PostgreSQL 18
// compatibility gap: json_strip_nulls/jsonb_strip_nulls accept a second
// strip_in_arrays argument that removes null array elements when true.
func TestPostgres18JsonStripNullsStripInArraysRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "json strip nulls supports strip_in_arrays",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT json_strip_nulls('[1,null,{"a":null,"b":2}]'::json, true)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "postgres18/json-correctness-repro-test-testpostgres18jsonstripnullsstripinarraysrepro-0001-select-json_strip_nulls-[1-null-{"},
				},
				{
					Query: `SELECT jsonb_strip_nulls('[1,null,{"a":null,"b":2}]'::jsonb, true)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "postgres18/json-correctness-repro-test-testpostgres18jsonstripnullsstripinarraysrepro-0002-select-jsonb_strip_nulls-[1-null-{"},
				},
				{
					Query: `SELECT jsonb_strip_nulls('[1,null,{"a":null,"b":2}]'::jsonb, false)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "postgres18/json-correctness-repro-test-testpostgres18jsonstripnullsstripinarraysrepro-0003-select-jsonb_strip_nulls-[1-null-{"},
				},
				{
					Query: `SELECT json_strip_nulls('null'::json, true)::text, jsonb_strip_nulls('null'::jsonb, true)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "postgres18/json-correctness-repro-test-testpostgres18jsonstripnullsstripinarraysrepro-0004-select-json_strip_nulls-null-::json-true"},
				},
			},
		},
	})
}

// TestPostgres18JsonbNullCastsToSqlNullRepro reproduces a PostgreSQL 18
// compatibility gap: jsonb null values cast to scalar SQL types as SQL NULL
// instead of raising an error.
func TestPostgres18JsonbNullCastsToSqlNullRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "jsonb null casts to scalar SQL null",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT
						(('null'::jsonb)::int4 IS NULL)::text,
						(('null'::jsonb)::bool IS NULL)::text,
						(('null'::jsonb)::numeric IS NULL)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "postgres18/json-correctness-repro-test-testpostgres18jsonbnullcaststosqlnullrepro-0001-select-null-::jsonb-::int4-is"},
				},
			},
		},
	})
}
