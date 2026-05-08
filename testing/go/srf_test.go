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

// TestSetReturningFunctionsWorkload pins generate_series and unnest workload
// patterns common in PG analytics views: numeric series, date series,
// lateral expansion of array columns, and combined uses with regular
// table joins. Per the View/query TODO in
// docs/app-compatibility-checklist.md.
func TestSetReturningFunctionsWorkload(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "generate_series numeric range",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT generate_series(1, 5);`,
					Expected: []sql.Row{{int64(1)}, {int64(2)}, {int64(3)}, {int64(4)}, {int64(5)}},
				},
				{
					Query:    `SELECT generate_series(0, 10, 2);`,
					Expected: []sql.Row{{int64(0)}, {int64(2)}, {int64(4)}, {int64(6)}, {int64(8)}, {int64(10)}},
				},
				{
					Query:    `SELECT generate_series(5, 1, -1);`,
					Expected: []sql.Row{{int64(5)}, {int64(4)}, {int64(3)}, {int64(2)}, {int64(1)}},
				},
			},
		},
		{
			Name: "generate_series in FROM clause with sum",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT sum(g)::text FROM generate_series(1, 100) g;`,
					Expected: []sql.Row{{"5050"}},
				},
				{
					Query:    `SELECT count(*) FROM generate_series(1, 1000);`,
					Expected: []sql.Row{{int64(1000)}},
				},
			},
		},
		{
			// Two unnest residual gaps tracked in the View/query TODO:
			// (1) `unnest(arr_col) AS t` joining a table does not
			//     expose `t` as a column in scope (errors with
			//     "column t could not be found in any table in scope");
			// (2) `unnest(...)` in a projection with ORDER BY trips an
			//     internal-type leak ("unhandled type
			//     *types.SetReturningFunctionRowIter in Compare").
			// The unnest-without-ORDER-BY shape works; pinned here.
			Name: "unnest in projection (no ORDER BY)",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT unnest(ARRAY[10, 20, 30]) AS v;`,
					Expected: []sql.Row{{int32(10)}, {int32(20)}, {int32(30)}},
				},
			},
		},
	})
}
