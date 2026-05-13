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

// TestSqlFunctionReturnExpressionBodyRepro reproduces a SQL routine
// compatibility gap: PostgreSQL supports SQL-standard RETURN expression bodies
// for SQL-language functions.
func TestSqlFunctionReturnExpressionBodyRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "SQL function RETURN expression body executes",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE FUNCTION sql_return_expression_body(input_value INT)
						RETURNS INT
						LANGUAGE SQL
						RETURN input_value + 1;`,
				},
				{
					Query: `SELECT sql_return_expression_body(7);`, PostgresOracle: ScriptTestPostgresOracle{ID: "routine-sql-body-repro-test-testsqlfunctionreturnexpressionbodyrepro-0001-select-sql_return_expression_body-7"},
				},
			},
		},
	})
}
