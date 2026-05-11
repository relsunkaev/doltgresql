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

import "testing"

// TestFunctionDefinitionsRejectInvalidPlannerOptionsRepro reproduces
// correctness bugs: Doltgres accepts function planner options that PostgreSQL
// rejects before creating the function.
func TestFunctionDefinitionsRejectInvalidPlannerOptionsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "function definitions reject invalid planner options",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE FUNCTION routine_scalar_rows_option()
						RETURNS INT
						LANGUAGE SQL
						ROWS 10
						AS $$ SELECT 1 $$;`,
					ExpectedErr: `ROWS is not applicable when function does not return a set`,
				},
				{
					Query: `CREATE FUNCTION routine_zero_rows_option()
						RETURNS SETOF INT
						LANGUAGE SQL
						ROWS 0
						AS $$ SELECT 1 $$;`,
					ExpectedErr: `ROWS must be positive`,
				},
				{
					Query: `CREATE FUNCTION routine_zero_cost_option()
						RETURNS INT
						LANGUAGE SQL
						COST 0
						AS $$ SELECT 1 $$;`,
					ExpectedErr: `COST must be positive`,
				},
				{
					Query: `CREATE FUNCTION routine_negative_cost_option()
						RETURNS INT
						LANGUAGE SQL
						COST -1
						AS $$ SELECT 1 $$;`,
					ExpectedErr: `COST must be positive`,
				},
			},
		},
	})
}
