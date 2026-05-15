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

package postgres16

import (
	"testing"

	"github.com/dolthub/go-mysql-server/sql"

	. "github.com/dolthub/doltgresql/testing/go"
)

// TestAggregateCoalesceComparisonKeepsNumericTypeRepro covers aggregate result
// coercion through COALESCE. Numeric aggregates should remain comparable to int
// literals, including over pgcatalog virtual-table columns.
func TestAggregateCoalesceComparisonKeepsNumericTypeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "coalesce min int aggregate remains numeric",
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT COALESCE(min(v), 0) >= 0 FROM (VALUES (1::int4), (2::int4)) AS t(v);`,
					Expected: []sql.Row{{"t"}},
				},
				{
					Query:    `SELECT COALESCE(min(v), 0) >= 0 FROM (SELECT 1::int4 AS v WHERE false) AS t;`,
					Expected: []sql.Row{{"t"}},
				},
				{
					Query:    `SELECT pg_typeof(prefetch)::text FROM pg_catalog.pg_stat_recovery_prefetch;`,
					Expected: []sql.Row{{"bigint"}},
				},
				{
					Query:    `SELECT pg_typeof(min(prefetch))::text FROM pg_catalog.pg_stat_recovery_prefetch;`,
					Expected: []sql.Row{{"bigint"}},
				},
				{
					Query:    `SELECT COALESCE(min(prefetch), 0) >= 0 FROM pg_catalog.pg_stat_recovery_prefetch;`,
					Expected: []sql.Row{{"t"}},
				},
			},
		},
	})
}
