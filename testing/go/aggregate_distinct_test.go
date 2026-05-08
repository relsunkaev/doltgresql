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

// TestAggregateDistinct pins string_agg(DISTINCT ...) and
// array_agg(DISTINCT ...) shapes that real reporting/grid views use to
// produce de-duplicated lists in a single column. Per the View/query
// TODO in docs/app-compatibility-checklist.md.
func TestAggregateDistinct(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "string_agg(DISTINCT ...)",
			SetUpScript: []string{
				`CREATE TABLE tags (
					entity_id INT,
					tag TEXT
				);`,
				`INSERT INTO tags VALUES
					(1, 'a'),
					(1, 'b'),
					(1, 'a'),
					(2, 'c'),
					(2, 'c'),
					(2, 'd');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					// ORDER BY inside the aggregate is a separate gap;
					// pin DISTINCT alone here. Real workloads use a
					// pre-sorted subquery when stable ordering matters.
					Query: `SELECT entity_id,
						length(string_agg(DISTINCT tag, '')) AS distinct_tag_chars
						FROM tags
						GROUP BY entity_id
						ORDER BY entity_id;`,
					Expected: []sql.Row{
						{int32(1), int32(2)}, // {a, b}
						{int32(2), int32(2)}, // {c, d}
					},
				},
			},
		},
		{
			Name: "array_agg(DISTINCT ...)",
			SetUpScript: []string{
				`CREATE TABLE memberships (
					user_id INT,
					group_id INT
				);`,
				`INSERT INTO memberships VALUES
					(1, 10),
					(1, 20),
					(1, 10),
					(2, 30),
					(2, 30),
					(2, 40);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					// Use cardinality to pin DISTINCT semantics without
					// also depending on agg-internal ORDER BY (separate gap).
					Query: `SELECT user_id,
						array_length(array_agg(DISTINCT group_id), 1) AS distinct_count
						FROM memberships
						GROUP BY user_id
						ORDER BY user_id;`,
					Expected: []sql.Row{
						{int32(1), int32(2)},
						{int32(2), int32(2)},
					},
				},
			},
		},
	})
}
