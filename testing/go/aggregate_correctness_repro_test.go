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

// TestBoolAndInfersValuesBooleanTypeGuard guards that PostgreSQL-style type
// inference works for a VALUES-derived column containing true, false, and an
// untyped NULL.
func TestBoolAndInfersValuesBooleanTypeGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "bool_and infers VALUES boolean type with untyped NULL",
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT bool_and(a) FROM (VALUES (true), (false), (null)) r(a);`,
					Expected: []sql.Row{{"f"}},
				},
			},
		},
	})
}

// TestCreateAggregateSqlTransitionFunctionRepro reproduces an aggregate
// correctness gap: PostgreSQL lets users define aggregates with SQL transition
// functions and then use those aggregates in ordinary GROUP queries.
func TestCreateAggregateSqlTransitionFunctionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE AGGREGATE with SQL transition function is usable",
			SetUpScript: []string{
				`CREATE FUNCTION custom_sum_sfunc(state INT, next_value INT)
					RETURNS INT
					LANGUAGE SQL
					IMMUTABLE
					AS $$ SELECT COALESCE(state, 0) + COALESCE(next_value, 0) $$;`,
				`CREATE AGGREGATE custom_sum(INT) (
					SFUNC = custom_sum_sfunc,
					STYPE = INT,
					INITCOND = '0'
				);`,
				`CREATE TABLE custom_aggregate_items (
					grp TEXT,
					v INT
				);`,
				`INSERT INTO custom_aggregate_items VALUES
					('a', 1),
					('a', 2),
					('b', 10);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT grp, custom_sum(v)
						FROM custom_aggregate_items
						GROUP BY grp
						ORDER BY grp;`,
					Expected: []sql.Row{
						{"a", 3},
						{"b", 10},
					},
				},
			},
		},
	})
}

// TestGroupByPrimaryKeyAllowsDependentColumnsRepro guards PostgreSQL
// functional-dependency grouping: grouping by a table's primary key permits
// selecting other columns from that same base table.
func TestGroupByPrimaryKeyAllowsDependentColumnsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "GROUP BY primary key allows dependent columns",
			SetUpScript: []string{
				`CREATE TABLE group_by_pk_items (
					id INT PRIMARY KEY,
					label TEXT,
					amount INT
				);`,
				`INSERT INTO group_by_pk_items VALUES
					(1, 'one', 10),
					(2, 'two', 20);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, label, amount
						FROM group_by_pk_items
						GROUP BY id
						ORDER BY id;`,
					Expected: []sql.Row{
						{1, "one", 10},
						{2, "two", 20},
					},
				},
			},
		},
	})
}

// TestGroupByNonKeyRejectsUngroupedColumnsRepro guards PostgreSQL grouping
// correctness: grouping by a non-key column cannot select arbitrary ungrouped
// columns from the same table.
func TestGroupByNonKeyRejectsUngroupedColumnsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "GROUP BY non-key rejects ungrouped columns",
			SetUpScript: []string{
				`CREATE TABLE group_by_nonkey_items (
					id INT PRIMARY KEY,
					label TEXT,
					amount INT
				);`,
				`INSERT INTO group_by_nonkey_items VALUES
					(1, 'same', 10),
					(2, 'same', 20);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, label
						FROM group_by_nonkey_items
						GROUP BY label;`,
					ExpectedErr: `not functionally dependent`,
				},
			},
		},
	})
}

// TestGroupByPrimaryKeyThroughJoinAllowsDependentColumnsRepro guards
// PostgreSQL functional-dependency grouping through joins: grouping by the
// left table's primary key permits selecting other columns from that table.
func TestGroupByPrimaryKeyThroughJoinAllowsDependentColumnsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "GROUP BY primary key through join allows dependent columns",
			SetUpScript: []string{
				`CREATE TABLE group_by_join_articles (
					id INT PRIMARY KEY,
					title TEXT,
					body TEXT
				);`,
				`CREATE TABLE group_by_join_categories (
					article_id INT REFERENCES group_by_join_articles(id),
					category_id INT,
					PRIMARY KEY (article_id, category_id)
				);`,
				`INSERT INTO group_by_join_articles VALUES
					(1, 'first', 'body one'),
					(2, 'second', 'body two');`,
				`INSERT INTO group_by_join_categories VALUES
					(1, 10),
					(1, 20),
					(2, 10);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT a.id, a.title, a.body
						FROM group_by_join_articles AS a
						JOIN group_by_join_categories AS c
							ON a.id = c.article_id
						WHERE c.category_id IN (10, 20)
						GROUP BY a.id
						ORDER BY a.id;`,
					Expected: []sql.Row{
						{1, "first", "body one"},
						{2, "second", "body two"},
					},
				},
			},
		},
	})
}

// TestGroupByGroupingSetsRepro reproduces an aggregate correctness gap:
// PostgreSQL GROUPING SETS produces multiple aggregate grouping levels from one
// scan.
func TestGroupByGroupingSetsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "GROUP BY GROUPING SETS computes multiple grouping levels",
			SetUpScript: []string{
				`CREATE TABLE grouping_sets_sales (
					region TEXT,
					product TEXT,
					amount INT
				);`,
				`INSERT INTO grouping_sets_sales VALUES
					('east', 'a', 10),
					('east', 'b', 20),
					('west', 'a', 5);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT COALESCE(region, 'ALL') AS region,
							COALESCE(product, 'ALL') AS product,
							sum(amount)::text AS total
						FROM grouping_sets_sales
						GROUP BY GROUPING SETS ((region, product), (region), ())
						ORDER BY region NULLS LAST, product NULLS LAST;`,
					Expected: []sql.Row{
						{"east", "a", "10"},
						{"east", "b", "20"},
						{"east", "ALL", "30"},
						{"west", "a", "5"},
						{"west", "ALL", "5"},
						{"ALL", "ALL", "35"},
					},
				},
			},
		},
	})
}

// TestGroupByRollupRepro reproduces an aggregate correctness gap: PostgreSQL
// ROLLUP is shorthand for hierarchical grouping sets.
func TestGroupByRollupRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "GROUP BY ROLLUP computes subtotal rows",
			SetUpScript: []string{
				`CREATE TABLE rollup_sales (
					region TEXT,
					product TEXT,
					amount INT
				);`,
				`INSERT INTO rollup_sales VALUES
					('east', 'a', 10),
					('east', 'b', 20),
					('west', 'a', 5);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT COALESCE(region, 'ALL') AS region,
							COALESCE(product, 'ALL') AS product,
							sum(amount)::text AS total
						FROM rollup_sales
						GROUP BY ROLLUP (region, product)
						ORDER BY region NULLS LAST, product NULLS LAST;`,
					Expected: []sql.Row{
						{"east", "a", "10"},
						{"east", "b", "20"},
						{"east", "ALL", "30"},
						{"west", "a", "5"},
						{"west", "ALL", "5"},
						{"ALL", "ALL", "35"},
					},
				},
			},
		},
	})
}

// TestGroupByCubeRepro reproduces an aggregate correctness gap: PostgreSQL CUBE
// computes all grouping combinations for the listed grouping keys.
func TestGroupByCubeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "GROUP BY CUBE computes all subtotal combinations",
			SetUpScript: []string{
				`CREATE TABLE cube_sales (
					region TEXT,
					product TEXT,
					amount INT
				);`,
				`INSERT INTO cube_sales VALUES
					('east', 'a', 10),
					('east', 'b', 20),
					('west', 'a', 5);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT COALESCE(region, 'ALL') AS region,
							COALESCE(product, 'ALL') AS product,
							sum(amount)::text AS total
						FROM cube_sales
						GROUP BY CUBE (region, product)
						ORDER BY region NULLS LAST, product NULLS LAST;`,
					Expected: []sql.Row{
						{"east", "a", "10"},
						{"east", "b", "20"},
						{"east", "ALL", "30"},
						{"west", "a", "5"},
						{"west", "ALL", "5"},
						{"ALL", "a", "15"},
						{"ALL", "b", "20"},
						{"ALL", "ALL", "35"},
					},
				},
			},
		},
	})
}

// TestPercentileContWithinGroupRepro reproduces an aggregate correctness gap:
// PostgreSQL supports continuous percentile ordered-set aggregates.
func TestPercentileContWithinGroupRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "percentile_cont ordered-set aggregate",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT (percentile_cont(0.5) WITHIN GROUP (ORDER BY v))::text
						FROM (VALUES (10), (20), (30), (40)) AS t(v);`,
					Expected: []sql.Row{{"25"}},
				},
			},
		},
	})
}

// TestPercentileDiscWithinGroupRepro reproduces an aggregate correctness gap:
// PostgreSQL supports discrete percentile ordered-set aggregates.
func TestPercentileDiscWithinGroupRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "percentile_disc ordered-set aggregate",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT (percentile_disc(0.5) WITHIN GROUP (ORDER BY v))::text
						FROM (VALUES (10), (20), (30), (40)) AS t(v);`,
					Expected: []sql.Row{{"20"}},
				},
			},
		},
	})
}

// TestModeWithinGroupRepro reproduces an aggregate correctness gap:
// PostgreSQL supports mode() as an ordered-set aggregate.
func TestModeWithinGroupRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "mode ordered-set aggregate",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT mode() WITHIN GROUP (ORDER BY v)
						FROM (VALUES ('b'), ('a'), ('b'), ('c')) AS t(v);`,
					Expected: []sql.Row{{"b"}},
				},
			},
		},
	})
}

// TestHypotheticalRankWithinGroupRepro reproduces an aggregate correctness gap:
// PostgreSQL supports hypothetical-set rank aggregates.
func TestHypotheticalRankWithinGroupRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "hypothetical rank aggregate",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT (rank(25) WITHIN GROUP (ORDER BY v))::text
						FROM (VALUES (10), (20), (30), (40)) AS t(v);`,
					Expected: []sql.Row{{"3"}},
				},
			},
		},
	})
}
