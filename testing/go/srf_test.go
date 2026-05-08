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
			Name:        "generate_series numeric range",
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
			Name:        "generate_series in FROM clause with sum",
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
			Name: "unnest in projection and table joins",
			SetUpScript: []string{
				`CREATE TABLE items (id INT PRIMARY KEY, vals INT[]);`,
				`INSERT INTO items VALUES (1, ARRAY[10, 20]), (2, ARRAY[30]);`,
				`CREATE TABLE alias_collision (id INT PRIMARY KEY, v INT, vals INT[]);`,
				`INSERT INTO alias_collision VALUES (1, 99, ARRAY[3, 1, 2]);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT unnest(ARRAY[10, 20, 30]) AS v;`,
					Expected: []sql.Row{{int32(10)}, {int32(20)}, {int32(30)}},
				},
				{
					Query:    `SELECT unnest(ARRAY[3, 1, 2]) AS v ORDER BY v;`,
					Expected: []sql.Row{{int32(1)}, {int32(2)}, {int32(3)}},
				},
				{
					Query:    `SELECT unnest(ARRAY[3, 1, 2]) AS v ORDER BY v LIMIT 2;`,
					Expected: []sql.Row{{int32(1)}, {int32(2)}},
				},
				{
					Query:    `SELECT unnest(vals) AS v FROM alias_collision ORDER BY v;`,
					Expected: []sql.Row{{int32(1)}, {int32(2)}, {int32(3)}},
				},
				{
					Query: `SELECT id, t
						FROM items
						CROSS JOIN unnest(vals) AS t
						ORDER BY id, t;`,
					Expected: []sql.Row{
						{int32(1), int32(10)},
						{int32(1), int32(20)},
						{int32(2), int32(30)},
					},
				},
			},
		},
	})
}
