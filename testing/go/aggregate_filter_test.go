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

// TestAggregateFilter pins the aggregate FILTER (WHERE pred) shapes that
// real reporting/grid views and dashboards rely on. Per the checklist's
// View/query TODO this proves count/sum/avg with FILTER, FILTER mixed
// with GROUP BY, and FILTER inside a CASE-style "two-axis" report.
func TestAggregateFilter(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "FILTER on count(*) and sum",
			SetUpScript: []string{
				`CREATE TABLE orders (
					id INT PRIMARY KEY,
					status TEXT,
					amount INT
				);`,
				`INSERT INTO orders VALUES
					(1, 'paid', 100),
					(2, 'pending', 50),
					(3, 'paid', 200),
					(4, 'cancelled', 75),
					(5, 'paid', 25);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT
						count(*) AS total,
						count(*) FILTER (WHERE status = 'paid') AS paid_count,
						count(*) FILTER (WHERE status = 'pending') AS pending_count
					FROM orders;`, PostgresOracle: ScriptTestPostgresOracle{ID: "aggregate-filter-test-testaggregatefilter-0001-select-count-*-as-total"},
				},
				{
					Query: `SELECT
						sum(amount)::text AS total_revenue,
						sum(amount) FILTER (WHERE status = 'paid')::text AS paid_revenue,
						sum(amount) FILTER (WHERE status = 'cancelled')::text AS cancelled_revenue
					FROM orders;`, PostgresOracle: ScriptTestPostgresOracle{ID: "aggregate-filter-test-testaggregatefilter-0002-select-sum-amount-::text-as"},
				},
			},
		},
		{
			Name: "FILTER with GROUP BY",
			SetUpScript: []string{
				`CREATE TABLE events (
					id INT PRIMARY KEY,
					day TEXT,
					kind TEXT,
					n INT
				);`,
				`INSERT INTO events VALUES
					(1, '2026-05-01', 'click', 10),
					(2, '2026-05-01', 'view', 100),
					(3, '2026-05-01', 'click', 5),
					(4, '2026-05-02', 'view', 200),
					(5, '2026-05-02', 'click', 20);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT day,
						sum(n) FILTER (WHERE kind = 'click')::text AS clicks,
						sum(n) FILTER (WHERE kind = 'view')::text AS views
					FROM events
					GROUP BY day
					ORDER BY day;`, PostgresOracle: ScriptTestPostgresOracle{ID: "aggregate-filter-test-testaggregatefilter-0003-select-day-sum-n-filter"},
				},
			},
		},
		{
			Name: "FILTER returns NULL when no rows match",
			SetUpScript: []string{
				`CREATE TABLE t (k INT, v INT);`,
				`INSERT INTO t VALUES (1, 10), (2, 20);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT
						sum(v) FILTER (WHERE k > 100) AS no_match,
						count(*) FILTER (WHERE k > 100) AS no_match_count
					FROM t;`, PostgresOracle: ScriptTestPostgresOracle{ID: "aggregate-filter-test-testaggregatefilter-0004-select-sum-v-filter-where"},
				},
			},
		},
		{
			Name: "FILTER inside expression alongside non-filtered aggregate",
			SetUpScript: []string{
				`CREATE TABLE sales (
					id INT PRIMARY KEY,
					region TEXT,
					amount INT,
					refunded BOOL
				);`,
				`INSERT INTO sales VALUES
					(1, 'US', 100, false),
					(2, 'US', 200, true),
					(3, 'EU', 50, false),
					(4, 'EU', 75, true),
					(5, 'US', 300, false);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					// Net revenue: sum minus refunded sum.
					Query: `SELECT region,
						(sum(amount) - COALESCE(sum(amount) FILTER (WHERE refunded), 0))::text AS net
					FROM sales
					GROUP BY region
					ORDER BY region;`, PostgresOracle: ScriptTestPostgresOracle{ID: "aggregate-filter-test-testaggregatefilter-0005-select-region-sum-amount-coalesce"},
				},
			},
		},
	})
}
