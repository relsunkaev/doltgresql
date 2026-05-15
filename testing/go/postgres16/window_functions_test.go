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
	. "github.com/dolthub/doltgresql/testing/go"

	"testing"
)

// TestWindowFunctions pins the window-function shapes real PG views and
// reporting queries use: row_number, rank, dense_rank, lag/lead, partitioned
// running sums, and explicit frame specifications. Per the View/query TODO
// in docs/app-compatibility-checklist.md.
func TestWindowFunctions(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "lag / lead",
			SetUpScript: []string{
				`CREATE TABLE prices (
					day INT PRIMARY KEY,
					price INT
				);`,
				`INSERT INTO prices VALUES (1, 100), (2, 110), (3, 95), (4, 120);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT day, price,
						lag(price)  OVER (ORDER BY day) AS prev_price,
						lead(price) OVER (ORDER BY day) AS next_price,
						price - lag(price) OVER (ORDER BY day) AS change
						FROM prices
						ORDER BY day;`, PostgresOracle: ScriptTestPostgresOracle{ID: "window-functions-test-testwindowfunctions-0001-select-day-price-lag-price"},
				},
			},
		},
		{
			Name: "count over partitioned window",
			SetUpScript: []string{
				`CREATE TABLE events (
					id INT PRIMARY KEY,
					grp TEXT,
					ts INT
				);`,
				`INSERT INTO events VALUES
					(1, 'a', 10), (2, 'a', 20), (3, 'a', 30),
					(4, 'b', 5), (5, 'b', 15);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, grp,
						count(*) OVER (PARTITION BY grp) AS group_total,
						count(*) OVER () AS overall_total
						FROM events
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "window-functions-test-testwindowfunctions-0002-select-id-grp-count-*"},
				},
			},
		},
		{
			Name: "running aggregates over explicit row frames",
			SetUpScript: []string{
				`CREATE TABLE measurements (
					id INT PRIMARY KEY,
					grp TEXT,
					amount INT
				);`,
				`INSERT INTO measurements VALUES
					(1, 'a', 10), (2, 'a', 20), (3, 'a', 30),
					(4, 'b', 5), (5, 'b', 15);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, grp,
						sum(amount) OVER (
							PARTITION BY grp
							ORDER BY id
							ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
						) AS rolling_sum
						FROM measurements
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "window-functions-test-testwindowfunctions-0003-select-id-grp-sum-amount"},
				},
				{
					Query: `SELECT id, grp,
						avg(amount) OVER (
							PARTITION BY grp
							ORDER BY id
							ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
						) AS rolling_avg
						FROM measurements
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "window-functions-test-testwindowfunctions-0004-select-id-grp-avg-amount"},
				},
				{
					Query: `SELECT
						pg_typeof(sum(amount) OVER (
							PARTITION BY grp
							ORDER BY id
							ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
						)),
						pg_typeof(avg(amount) OVER (
							PARTITION BY grp
							ORDER BY id
							ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
						))
						FROM measurements
						ORDER BY id
						LIMIT 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "window-functions-test-testwindowfunctions-0005-select-pg_typeof-sum-amount-over"},
				},
			},
		},
		{
			Name: "first_value / last_value over partition",
			SetUpScript: []string{
				`CREATE TABLE events (id INT, grp TEXT, n INT);`,
				`INSERT INTO events VALUES
					(1, 'a', 10),
					(2, 'a', 20),
					(3, 'a', 30),
					(4, 'b', 5),
					(5, 'b', 15);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, grp, n,
						first_value(n) OVER (PARTITION BY grp ORDER BY id) AS first_n,
						last_value(n)  OVER (PARTITION BY grp ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_n
						FROM events
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "window-functions-test-testwindowfunctions-0006-select-id-grp-n-first_value"},
				},
			},
		},
		{
			Name: "rank family over partition",
			SetUpScript: []string{
				`CREATE TABLE scores (
					id INT PRIMARY KEY,
					grp TEXT,
					score INT
				);`,
				`INSERT INTO scores VALUES
					(1, 'a', 10), (2, 'a', 20), (3, 'a', 20),
					(4, 'b', 7), (5, 'b', 9), (6, 'b', 9);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, grp, score,
						row_number() OVER (PARTITION BY grp ORDER BY score DESC, id) AS row_num,
						rank() OVER (PARTITION BY grp ORDER BY score DESC) AS rank_num,
						dense_rank() OVER (PARTITION BY grp ORDER BY score DESC) AS dense_rank_num,
						percent_rank() OVER (PARTITION BY grp ORDER BY score DESC) AS pct_rank,
						ntile(2) OVER (PARTITION BY grp ORDER BY score DESC, id) AS bucket
						FROM scores
						ORDER BY grp, score DESC, id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "window-functions-test-testwindowfunctions-0007-select-id-grp-score-row_number"},
				},
				{
					Query: `SELECT
						pg_typeof(row_number() OVER (ORDER BY id)),
						pg_typeof(rank() OVER (ORDER BY score DESC)),
						pg_typeof(dense_rank() OVER (ORDER BY score DESC)),
						pg_typeof(percent_rank() OVER (ORDER BY score DESC)),
						pg_typeof(ntile(2) OVER (ORDER BY score DESC, id))
						FROM scores
						ORDER BY id
						LIMIT 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "window-functions-test-testwindowfunctions-0008-select-pg_typeof-row_number-over-order"},
				},
			},
		},
	})
}
