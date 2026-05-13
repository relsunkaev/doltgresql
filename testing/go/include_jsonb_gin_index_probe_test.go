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

// TestIncludeAndJsonbGinIndexProbe pins the DDL boundary for two
// btree-adjacent index forms ORM tools and dumps emit:
// (1) covering indexes with INCLUDE columns, and (2) JSONB GIN
// indexes used for `@>` containment queries. Per the Index/planner
// TODO in docs/app-compatibility-checklist.md.
func TestIncludeAndJsonbGinIndexProbe(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "INCLUDE columns on a btree index",
			SetUpScript: []string{
				`CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, amount INT, status TEXT);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE INDEX orders_customer_inc_amount_idx
						ON orders (customer_id) INCLUDE (amount, status);`,
				},
				{
					Query: `SELECT count(*)::text FROM pg_indexes WHERE indexname = 'orders_customer_inc_amount_idx';`, PostgresOracle: ScriptTestPostgresOracle{ID: "include-jsonb-gin-index-probe-test-testincludeandjsonbginindexprobe-0001-select-count-*-::text-from"},
				},
			},
		},
		{
			Name: "GIN index on a JSONB column for containment",
			SetUpScript: []string{
				`CREATE TABLE events (id INT PRIMARY KEY, payload JSONB);`,
				`INSERT INTO events VALUES
					(1, '{"kind": "click", "user": 100}'),
					(2, '{"kind": "view",  "user": 200}'),
					(3, '{"kind": "click", "user": 300}');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE INDEX events_payload_gin_idx
						ON events USING gin (payload);`,
				},
				{
					Query: `SELECT count(*)::text FROM pg_indexes WHERE indexname = 'events_payload_gin_idx';`, PostgresOracle: ScriptTestPostgresOracle{ID: "include-jsonb-gin-index-probe-test-testincludeandjsonbginindexprobe-0002-select-count-*-::text-from"},
				},
				{
					// Containment subset that real apps issue:
					// `payload @> '{"kind": "click"}'` should match
					// rows 1 and 3.
					Query: `SELECT count(*)::text FROM events WHERE payload @> '{"kind": "click"}';`, PostgresOracle: ScriptTestPostgresOracle{ID: "include-jsonb-gin-index-probe-test-testincludeandjsonbginindexprobe-0003-select-count-*-::text-from"},
				},
			},
		},
	})
}
