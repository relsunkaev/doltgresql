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

	"github.com/dolthub/go-mysql-server/sql"
)

// TestSqlJsonConstructorFunctionsRepro reproduces SQL/JSON constructor
// compatibility gaps beyond JSON_TABLE.
func TestSqlJsonConstructorFunctionsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "SQL/JSON constructor functions evaluate JSON values",
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT JSON('{"a":123, "b":[true,"foo"]}')::jsonb::text;`,
					Expected: []sql.Row{{`{"a": 123, "b": [true, "foo"]}`}},
				},
				{
					Query:    `SELECT JSON_SCALAR('plain'::text)::text, JSON_SCALAR(123.45)::text, JSON_SCALAR(NULL::text)::text;`,
					Expected: []sql.Row{{`"plain"`, "123.45", nil}},
				},
				{
					Query:    `SELECT JSON_SERIALIZE('{"a":1}' RETURNING text);`,
					Expected: []sql.Row{{`{"a":1}`}},
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
					Query:    `SELECT JSON_ARRAY(1, true, 'x' RETURNING jsonb)::text;`,
					Expected: []sql.Row{{`[1, true, "x"]`}},
				},
				{
					Query:    `SELECT JSON_OBJECT('a' VALUE 1, 'b' VALUE 'x' RETURNING jsonb)::text;`,
					Expected: []sql.Row{{`{"a": 1, "b": "x"}`}},
				},
				{
					Query:    `SELECT JSON_ARRAYAGG(v ORDER BY v RETURNING jsonb)::text FROM (VALUES (2), (1)) AS t(v);`,
					Expected: []sql.Row{{`[1, 2]`}},
				},
				{
					Query:    `SELECT JSON_OBJECTAGG(k VALUE v RETURNING jsonb)::text FROM (VALUES ('a', 1), ('b', 2)) AS t(k, v);`,
					Expected: []sql.Row{{`{"a": 1, "b": 2}`}},
				},
			},
		},
	})
}

// TestSqlJsonQueryFunctionsRepro reproduces SQL/JSON query-function
// compatibility gaps beyond JSON_TABLE.
func TestSqlJsonQueryFunctionsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "SQL/JSON query functions evaluate jsonpath expressions",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT JSON_EXISTS(
						'{"key1":[1,2,3]}'::jsonb,
						'strict $.key1[*] ? (@ > $x)' PASSING 2 AS x);`,
					Expected: []sql.Row{{"t"}},
				},
				{
					Query: `SELECT JSON_VALUE(
						'{"a":123}'::jsonb,
						'$.a' RETURNING int);`,
					Expected: []sql.Row{{123}},
				},
				{
					Query: `SELECT JSON_QUERY(
						'{"a":[1,2]}'::jsonb,
						'$.a')::text;`,
					Expected: []sql.Row{{`[1, 2]`}},
				},
			},
		},
	})
}
