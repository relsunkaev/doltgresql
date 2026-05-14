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

// TestBoolAndInfersValuesBooleanTypeGuard guards that PostgreSQL-style type
// inference works for a VALUES-derived column containing true, false, and an
// untyped NULL.
func TestBoolAndInfersValuesBooleanTypeGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "bool_and infers VALUES boolean type with untyped NULL",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT bool_and(a) FROM (VALUES (true), (false), (null)) r(a);`, PostgresOracle: ScriptTestPostgresOracle{ID: "aggregate-correctness-repro-test-testboolandinfersvaluesbooleantypeguard-0001-select-bool_and-a-from-values"},
				},
			},
		},
	})
}

// TestUnaryMinusOverSumDistinctRepro guards aggregate return type propagation
// through a parent projection. SUM over int input yields an int8 runtime value,
// so a stale float8 unary-minus overload must not be reused after aggregate
// casts have been added.
func TestUnaryMinusOverSumDistinctRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "unary minus over SUM DISTINCT uses aggregate result type",
			SetUpScript: []string{
				`CREATE TABLE unary_sum_items (v INT);`,
				`INSERT INTO unary_sum_items VALUES (1), (2);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT (- SUM(DISTINCT - - 71))::text
						FROM unary_sum_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "aggregate-correctness-repro-test-testunaryminusoversumdistinctrepro-0001-select-sum-distinct-71-::text"},
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
						ORDER BY grp;`, PostgresOracle: ScriptTestPostgresOracle{ID: "aggregate-correctness-repro-test-testcreateaggregatesqltransitionfunctionrepro-0001-select-grp-custom_sum-v-from"},
				},
			},
		},
	})
}

// TestCreateAggregatePgAggregateCatalogRowRepro reproduces a catalog
// correctness gap: PostgreSQL records every aggregate in pg_aggregate, including
// user-defined aggregates created in the current database.
func TestCreateAggregatePgAggregateCatalogRowRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE AGGREGATE populates pg_aggregate",
			SetUpScript: []string{
				`CREATE FUNCTION catalog_custom_sum_sfunc(state INT, next_value INT)
					RETURNS INT
					LANGUAGE SQL
					IMMUTABLE
					AS $$ SELECT COALESCE(state, 0) + COALESCE(next_value, 0) $$;`,
				`CREATE AGGREGATE catalog_custom_sum(INT) (
					SFUNC = catalog_custom_sum_sfunc,
					STYPE = INT,
					INITCOND = '0'
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT
							aggfnoid::text,
							aggtransfn::text,
							aggtranstype::regtype::text,
							agginitval
						FROM pg_catalog.pg_aggregate
						WHERE aggfnoid::text = 'catalog_custom_sum';`, PostgresOracle: ScriptTestPostgresOracle{ID: "aggregate-correctness-repro-test-testcreateaggregatepgaggregatecatalogrowrepro-0001-select-aggfnoid::text-aggtransfn::text-aggtranstype::regtype::text-agginitval"},
				},
			},
		},
	})
}

// TestDropAggregateRemovesUserAggregateRepro reproduces a PostgreSQL
// compatibility gap: user-defined aggregates can be dropped by signature.
func TestDropAggregateRemovesUserAggregateRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP AGGREGATE removes user aggregate",
			SetUpScript: []string{
				`CREATE FUNCTION drop_custom_sum_sfunc(state INT, next_value INT)
					RETURNS INT
					LANGUAGE SQL
					IMMUTABLE
					AS $$ SELECT COALESCE(state, 0) + COALESCE(next_value, 0) $$;`,
				`CREATE AGGREGATE drop_custom_sum(INT) (
					SFUNC = drop_custom_sum_sfunc,
					STYPE = INT,
					INITCOND = '0'
				);`,
				`CREATE TABLE drop_custom_sum_items (v INT);`,
				`INSERT INTO drop_custom_sum_items VALUES (1), (2);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP AGGREGATE drop_custom_sum(INT);`,
				},
				{
					Query: `SELECT drop_custom_sum(v) FROM drop_custom_sum_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "aggregate-correctness-repro-test-testdropaggregateremovesuseraggregaterepro-0001-select-drop_custom_sum-v-from-drop_custom_sum_items", Compare: "sqlstate"},

					// TestAlterAggregateRenameRepro reproduces a PostgreSQL compatibility gap:
					// ALTER AGGREGATE can rename a user-defined aggregate while preserving its
					// implementation.

				},
			},
		},
	})
}

func TestAlterAggregateRenameRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER AGGREGATE RENAME TO updates aggregate lookup",
			SetUpScript: []string{
				`CREATE FUNCTION rename_custom_sum_sfunc(state INT, next_value INT)
					RETURNS INT
					LANGUAGE SQL
					IMMUTABLE
					AS $$ SELECT COALESCE(state, 0) + COALESCE(next_value, 0) $$;`,
				`CREATE AGGREGATE rename_custom_sum_old(INT) (
					SFUNC = rename_custom_sum_sfunc,
					STYPE = INT,
					INITCOND = '0'
				);`,
				`CREATE TABLE rename_custom_sum_items (v INT);`,
				`INSERT INTO rename_custom_sum_items VALUES (1), (2);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER AGGREGATE rename_custom_sum_old(INT)
						RENAME TO rename_custom_sum_new;`,
				},
				{
					Query: `SELECT rename_custom_sum_new(v) FROM rename_custom_sum_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "aggregate-correctness-repro-test-testalteraggregaterenamerepro-0001-select-rename_custom_sum_new-v-from-rename_custom_sum_items"},
				},
				{
					Query: `SELECT rename_custom_sum_old(v) FROM rename_custom_sum_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "aggregate-correctness-repro-test-testalteraggregaterenamerepro-0002-select-rename_custom_sum_old-v-from-rename_custom_sum_items", Compare: "sqlstate"},
				},
			},
		},
	})
}

// TestCreateAggregateSqlTransitionFunctionEdges covers the supported
// CREATE AGGREGATE slice beyond the happy path: SQL transition functions with
// no INITCOND, NULL inputs, empty inputs, and old-style BASETYPE syntax.
func TestCreateAggregateSqlTransitionFunctionEdges(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "SQL aggregate transition handles NULL state and old syntax",
			SetUpScript: []string{
				`CREATE FUNCTION custom_count_seen_sfunc(state INT, next_value INT)
					RETURNS INT
					LANGUAGE SQL
					IMMUTABLE
					AS $$ SELECT COALESCE(state, 0) + CASE WHEN next_value IS NULL THEN 0 ELSE 1 END $$;`,
				`CREATE AGGREGATE custom_count_seen(
					BASETYPE = INT,
					SFUNC = custom_count_seen_sfunc,
					STYPE = INT
				);`,
				`CREATE TABLE custom_count_seen_items (
					grp TEXT,
					v INT
				);`,
				`INSERT INTO custom_count_seen_items VALUES
					('a', 1),
					('a', NULL),
					('a', 3),
					('b', NULL);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT grp, custom_count_seen(v)
						FROM custom_count_seen_items
						GROUP BY grp
						ORDER BY grp;`, PostgresOracle: ScriptTestPostgresOracle{ID: "aggregate-correctness-repro-test-testcreateaggregatesqltransitionfunctionedges-0001-select-grp-custom_count_seen-v-from"},
				},
				{
					Query: `SELECT custom_count_seen(v)
						FROM custom_count_seen_items
						WHERE grp = 'missing';`, PostgresOracle: ScriptTestPostgresOracle{ID: "aggregate-correctness-repro-test-testcreateaggregatesqltransitionfunctionedges-0002-select-custom_count_seen-v-from-custom_count_seen_items"},
				},
			},
		},
		{
			Name: "overloaded SQL aggregates use their resolved transition function",
			SetUpScript: []string{
				`CREATE FUNCTION overloaded_custom_sum_int_sfunc(state INT, next_value INT)
					RETURNS INT
					LANGUAGE SQL
					IMMUTABLE
					AS $$ SELECT COALESCE(state, 0) + COALESCE(next_value, 0) + 1 $$;`,
				`CREATE FUNCTION overloaded_custom_sum_bigint_sfunc(state BIGINT, next_value BIGINT)
					RETURNS BIGINT
					LANGUAGE SQL
					IMMUTABLE
					AS $$ SELECT COALESCE(state, 0::BIGINT) + COALESCE(next_value, 0::BIGINT) + 100::BIGINT $$;`,
				`CREATE AGGREGATE overloaded_custom_sum(INT) (
					SFUNC = overloaded_custom_sum_int_sfunc,
					STYPE = INT,
					INITCOND = '0'
				);`,
				`CREATE AGGREGATE overloaded_custom_sum(BIGINT) (
					SFUNC = overloaded_custom_sum_bigint_sfunc,
					STYPE = BIGINT,
					INITCOND = '0'
				);`,
				`CREATE TABLE overloaded_custom_sum_items (
					v_int INT,
					v_bigint BIGINT
				);`,
				`INSERT INTO overloaded_custom_sum_items VALUES
					(1, 10),
					(2, 20);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT overloaded_custom_sum(v_int), overloaded_custom_sum(v_bigint)
						FROM overloaded_custom_sum_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "aggregate-correctness-repro-test-testcreateaggregatesqltransitionfunctionedges-0003-select-overloaded_custom_sum-v_int-overloaded_custom_sum-v_bigint"},
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
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "aggregate-correctness-repro-test-testgroupbyprimarykeyallowsdependentcolumnsrepro-0001-select-id-label-amount-from"},
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
						GROUP BY label;`, PostgresOracle: ScriptTestPostgresOracle{ID: "aggregate-correctness-repro-test-testgroupbynonkeyrejectsungroupedcolumnsrepro-0001-select-id-label-from-group_by_nonkey_items", Compare: "sqlstate"},

					// TestGroupByPrimaryKeyThroughJoinAllowsDependentColumnsRepro guards
					// PostgreSQL functional-dependency grouping through joins: grouping by the
					// left table's primary key permits selecting other columns from that table.

				},
			},
		},
	})
}

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
						ORDER BY a.id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "aggregate-correctness-repro-test-testgroupbyprimarykeythroughjoinallowsdependentcolumnsrepro-0001-select-a.id-a.title-a.body-from"},
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
						ORDER BY region NULLS LAST, product NULLS LAST;`, PostgresOracle: ScriptTestPostgresOracle{ID: "aggregate-correctness-repro-test-testgroupbygroupingsetsrepro-0001-select-coalesce-region-all-as"},
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
						ORDER BY region NULLS LAST, product NULLS LAST;`, PostgresOracle: ScriptTestPostgresOracle{ID: "aggregate-correctness-repro-test-testgroupbyrolluprepro-0001-select-coalesce-region-all-as"},
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
						ORDER BY region NULLS LAST, product NULLS LAST;`, PostgresOracle: ScriptTestPostgresOracle{ID: "aggregate-correctness-repro-test-testgroupbycuberepro-0001-select-coalesce-region-all-as"},
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
						FROM (VALUES (10), (20), (30), (40)) AS t(v);`, PostgresOracle: ScriptTestPostgresOracle{ID: "aggregate-correctness-repro-test-testpercentilecontwithingrouprepro-0001-select-percentile_cont-0.5-within-group"},
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
						FROM (VALUES (10), (20), (30), (40)) AS t(v);`, PostgresOracle: ScriptTestPostgresOracle{ID: "aggregate-correctness-repro-test-testpercentilediscwithingrouprepro-0001-select-percentile_disc-0.5-within-group"},
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
						FROM (VALUES ('b'), ('a'), ('b'), ('c')) AS t(v);`, PostgresOracle: ScriptTestPostgresOracle{ID: "aggregate-correctness-repro-test-testmodewithingrouprepro-0001-select-mode-within-group-order"},
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
						FROM (VALUES (10), (20), (30), (40)) AS t(v);`, PostgresOracle: ScriptTestPostgresOracle{ID: "aggregate-correctness-repro-test-testhypotheticalrankwithingrouprepro-0001-select-rank-25-within-group"},
				},
			},
		},
	})
}
