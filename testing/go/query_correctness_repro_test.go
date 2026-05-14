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

// TestValuesQuotedCaseDistinctAggregateColumnsRepro reproduces a correctness
// bug: quoted VALUES aliases that differ only by case resolve to the wrong
// column inside aggregates.
func TestValuesQuotedCaseDistinctAggregateColumnsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "quoted VALUES aliases remain case-distinct inside aggregates",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT SUM("Val"), SUM("val")
						FROM (VALUES(1, 10), (2.5, 20)) v("Val", "val");`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testvaluesquotedcasedistinctaggregatecolumnsrepro-0001-select-sum-val-sum-val"},
				},
				{
					Query: `SELECT SUM(v."Val"), SUM(v."val")
						FROM (VALUES(1, 10), (2.5, 20)) v("Val", "val");`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testvaluesquotedcasedistinctaggregatecolumnsrepro-0002-select-sum-v.-val-sum"},
				},
			},
		},
	})
}

// TestWholeRowReferenceAllowsDuplicateFieldNamesRepro reproduces a query
// correctness bug: PostgreSQL allows whole-row references even when the row
// type has duplicate field names.
func TestWholeRowReferenceAllowsDuplicateFieldNamesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "whole-row references allow duplicate field names",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT r::text,
						row_to_json(r)::text
						FROM (SELECT 1 AS a, 2 AS a) AS r;`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testwholerowreferenceallowsduplicatefieldnamesrepro-0001-select-r::text-row_to_json-r-::text"},
				},
			},
		},
	})
}

// TestNumericPowerFractionalExponentRepro reproduces a numeric correctness
// bug: power(numeric, numeric) truncates or otherwise mishandles fractional
// exponents.
func TestNumericPowerFractionalExponentRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "numeric power supports fractional exponents",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT power(2::numeric, 0.5::numeric)::float8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testnumericpowerfractionalexponentrepro-0001-select-power-2::numeric-0.5::numeric-::float8"},
				},
			},
		},
	})
}

// TestSqrtNumericMatchesPostgresPrecisionRepro reproduces a numeric correctness
// bug: sqrt(numeric) should preserve PostgreSQL numeric precision, but Doltgres
// computes through float64.
func TestSqrtNumericMatchesPostgresPrecisionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "numeric sqrt preserves PostgreSQL precision",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT sqrt(2::numeric)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testsqrtnumericmatchespostgresprecisionrepro-0001-select-sqrt-2::numeric-::text"},
				},
				{
					Query: `SELECT sqrt(10000000000000000000000000000000000000000000000000000000000000000::numeric)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testsqrtnumericmatchespostgresprecisionrepro-0002-select-sqrt-::text"},
				},
			},
		},
	})
}

// TestNumericLogarithmsPreserveSmallDeltasRepro reproduces numeric correctness
// bugs: ln(numeric) and log(numeric) should preserve exact numeric values close
// to 1, but Doltgres converts through float64 and collapses tiny deltas.
func TestNumericLogarithmsPreserveSmallDeltasRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "numeric logarithms preserve small deltas",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT ln(1.0000000000000000000001::numeric)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testnumericlogarithmspreservesmalldeltasrepro-0001-select-ln-1.0000000000000000000001::numeric-::text"},
				},
				{
					Query: `SELECT log(1.0000000000000000000001::numeric)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testnumericlogarithmspreservesmalldeltasrepro-0002-select-log-1.0000000000000000000001::numeric-::text"},
				},
				{
					Query: `SELECT log(1.0000000000000000000001::numeric, 1.0000000000000000000003::numeric)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testnumericlogarithmspreservesmalldeltasrepro-0003-select-log-1.0000000000000000000001::numeric-1.0000000000000000000003::numeric-::text"},
				},
			},
		},
	})
}

// TestWidthBucketReversedBoundsUnderflowGuard protects PostgreSQL's reversed
// bounds semantics: values below the lower endpoint return bucket count + 1
// when the bucket range is reversed, and Doltgres currently matches that.
func TestWidthBucketReversedBoundsUnderflowGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "width_bucket reversed bounds underflow",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT width_bucket((-1)::float8, 10::float8, 0::float8, 5)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testwidthbucketreversedboundsunderflowguard-0001-select-width_bucket-1-::float8-10::float8"},
				},
				{
					Query: `SELECT width_bucket((-1)::numeric, 10::numeric, 0::numeric, 5)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testwidthbucketreversedboundsunderflowguard-0002-select-width_bucket-1-::numeric-10::numeric"},
				},
			},
		},
	})
}

// TestCaseExpressionShortCircuitsRepro guards PostgreSQL CASE expression
// semantics: only the selected branch should be evaluated.
func TestCaseExpressionShortCircuitsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CASE expression short-circuits unselected branches",
			SetUpScript: []string{
				`CREATE TABLE case_short_circuit_items (id INT PRIMARY KEY);`,
				`INSERT INTO case_short_circuit_items VALUES (1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT CASE
							WHEN id = 1 THEN 42
							ELSE 1 / (id - id)
						END
						FROM case_short_circuit_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testcaseexpressionshortcircuitsrepro-0001-select-case-when-id-="},
				},
			},
		},
	})
}

// TestCoalesceShortCircuitsRepro guards PostgreSQL COALESCE semantics: once a
// non-NULL argument is found, later arguments are not evaluated.
func TestCoalesceShortCircuitsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "COALESCE short-circuits after first non-null argument",
			SetUpScript: []string{
				`CREATE TABLE coalesce_short_circuit_items (id INT PRIMARY KEY, value INT);`,
				`INSERT INTO coalesce_short_circuit_items VALUES (1, 7);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT COALESCE(value, 1 / (id - id))
						FROM coalesce_short_circuit_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testcoalesceshortcircuitsrepro-0001-select-coalesce-value-1-/"},
				},
			},
		},
	})
}

// TestNumericToIntegerCastRoundsRepro guards PostgreSQL rounding semantics for
// casts from numeric values to int4.
func TestNumericToIntegerCastRoundsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "numeric to int4 casts round instead of truncate",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT 37.89::int4, (-37.89)::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testnumerictointegercastroundsrepro-0001-select-37.89::int4-37.89-::int4"},
				},
			},
		},
	})
}

// TestArrayToStringUsesRoundedIntegerCastsRepro guards PostgreSQL numeric-to-int
// rounding semantics inside array expressions.
func TestArrayToStringUsesRoundedIntegerCastsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "array element integer casts round numeric inputs",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT array_to_string(ARRAY[37.89::int4, 1.2::int4], '_');`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testarraytostringusesroundedintegercastsrepro-0001-select-array_to_string-array[37.89::int4-1.2::int4]"},
				},
			},
		},
	})
}

// TestByteaArrayCastToTextUsesPostgresEscapingRepro reproduces a correctness
// bug: casting bytea arrays to text arrays does not preserve PostgreSQL's bytea
// text escaping.
func TestByteaArrayCastToTextUsesPostgresEscapingRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "bytea array cast to text array uses PostgreSQL escaping",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT '{"\x68656c6c6f", "\x776f726c64", "\x6578616d706c65"}'::bytea[]::text[];`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testbyteaarraycasttotextusespostgresescapingrepro-0001-select-{-\\x68656c6c6f-\\x776f726c64-\\x6578616d706c65"},
				},
				{
					Query: `SELECT '{"\\x68656c6c6f", "\\x776f726c64", "\\x6578616d706c65"}'::bytea[]::text[];`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testbyteaarraycasttotextusespostgresescapingrepro-0002-select-{-\\\\x68656c6c6f-\\\\x776f726c64-\\\\x6578616d706c65"},
				},
			},
		},
	})
}

// TestArrayAggOverArrayColumnReturnsHigherDimensionalArrayRepro reproduces a
// correctness bug: array_agg over array-typed input should return a
// higher-dimensional array instead of failing during result output.
func TestArrayAggOverArrayColumnReturnsHigherDimensionalArrayRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "array_agg over array input returns higher-dimensional array",
			SetUpScript: []string{
				`CREATE TABLE array_agg_array_items (id INT PRIMARY KEY, vals FLOAT[]);`,
				`INSERT INTO array_agg_array_items VALUES
					(1, '{1.0, 2.0}'),
					(2, '{3.0, 4.0}'),
					(3, '{5.0, 6.0}');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT array_agg(vals)
						FROM array_agg_array_items
						ORDER BY min(id);`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testarrayaggoverarraycolumnreturnshigherdimensionalarrayrepro-0001-select-array_agg-vals-from-array_agg_array_items"},
				},
			},
		},
	})
}

// TestIntegerPrimaryKeyComparedToFractionalFloatRepro guards integer-index
// predicates compared against fractional float literals.
func TestIntegerPrimaryKeyComparedToFractionalFloatRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "integer primary key comparison with fractional float literal",
			SetUpScript: []string{
				`CREATE TABLE int_float_predicate_items (i INT PRIMARY KEY);`,
				`INSERT INTO int_float_predicate_items VALUES (-1), (0), (1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT i
						FROM int_float_predicate_items
						WHERE i > 0.1 OR i >= 0.1
						ORDER BY i;`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testintegerprimarykeycomparedtofractionalfloatrepro-0001-select-i-from-int_float_predicate_items-where"},
				},
				{
					Query: `SELECT i
						FROM int_float_predicate_items
						WHERE i < 0.1
						ORDER BY i;`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testintegerprimarykeycomparedtofractionalfloatrepro-0002-select-i-from-int_float_predicate_items-where"},
				},
			},
		},
	})
}

// TestFloatInListMatchesExplicitFloatCastRepro guards float IN-list matching
// when the matching value is produced by an explicit float cast.
func TestFloatInListMatchesExplicitFloatCastRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "float IN-list comparison with explicit float cast",
			SetUpScript: []string{
				`CREATE TABLE float_in_list_items (f FLOAT);`,
				`INSERT INTO float_in_list_items VALUES (0.8);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT COUNT(*)
						FROM float_in_list_items
						WHERE f IN (NULL, CAST(0.8 AS FLOAT));`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testfloatinlistmatchesexplicitfloatcastrepro-0001-select-count-*-from-float_in_list_items"},
				},
			},
		},
	})
}

// TestIntersectAllPreservesDuplicateCountsRepro guards PostgreSQL multiset
// semantics: INTERSECT ALL returns each row as many times as it appears in both
// inputs, using the lower duplicate count.
func TestIntersectAllPreservesDuplicateCountsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "INTERSECT ALL preserves duplicate counts",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT x FROM (VALUES (1), (1), (2), (3)) AS lhs(x)
						INTERSECT ALL
						SELECT x FROM (VALUES (1), (1), (1), (3), (4)) AS rhs(x)
						ORDER BY x;`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testintersectallpreservesduplicatecountsrepro-0001-select-x-from-values-1"},
				},
			},
		},
	})
}

// TestExceptAllPreservesDuplicateCountsRepro guards PostgreSQL multiset
// semantics: EXCEPT ALL subtracts duplicate counts from the right input instead
// of applying DISTINCT semantics.
func TestExceptAllPreservesDuplicateCountsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "EXCEPT ALL preserves duplicate counts",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT x FROM (VALUES (1), (1), (1), (2), (3)) AS lhs(x)
						EXCEPT ALL
						SELECT x FROM (VALUES (1), (3), (4)) AS rhs(x)
						ORDER BY x;`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testexceptallpreservesduplicatecountsrepro-0001-select-x-from-values-1"},
				},
			},
		},
	})
}

// TestInSubqueryReturnsBooleanForEmptyResultRepro guards that an IN predicate
// over an empty subquery returns the boolean value false.
func TestInSubqueryReturnsBooleanForEmptyResultRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "IN subquery returns boolean false for empty result",
			SetUpScript: []string{
				`CREATE TABLE in_subquery_left (x INT);`,
				`CREATE TABLE in_subquery_right (y INT PRIMARY KEY);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT 1 IN (SELECT x + y FROM in_subquery_left, in_subquery_right);`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testinsubqueryreturnsbooleanforemptyresultrepro-0001-select-1-in-select-x"},
				},
			},
		},
	})
}

// TestAnySubqueryRejectsMultipleColumnsRepro guards PostgreSQL scalar ANY
// semantics: the subquery must return exactly one column.
func TestAnySubqueryRejectsMultipleColumnsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "ANY subquery rejects multiple columns",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT 1 = ANY (SELECT 1, 2);`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testanysubqueryrejectsmultiplecolumnsrepro-0001-select-1-=-any-select",

						// TestAllSubqueryRejectsMultipleColumnsRepro guards PostgreSQL scalar ALL
						// semantics: the subquery must return exactly one column.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestAllSubqueryRejectsMultipleColumnsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "ALL subquery rejects multiple columns",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT 1 = ALL (SELECT 1, 2);`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testallsubqueryrejectsmultiplecolumnsrepro-0001-select-1-=-all-select",

						// TestInSubqueryRejectsMultipleColumnsRepro guards PostgreSQL scalar IN
						// semantics: the subquery must return exactly one column.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestInSubqueryRejectsMultipleColumnsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "IN subquery rejects multiple columns",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT 1 IN (SELECT 1, 2);`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testinsubqueryrejectsmultiplecolumnsrepro-0001-select-1-in-select-1",

						// TestRowInSubqueryAcceptsMultipleColumnsRepro reproduces a query correctness
						// bug: a PostgreSQL row constructor on the left may compare with a subquery
						// that returns the same number of columns.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestRowInSubqueryAcceptsMultipleColumnsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "row IN subquery accepts matching multiple columns",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT ROW(1, 2) IN (SELECT 1, 2);`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testrowinsubqueryacceptsmultiplecolumnsrepro-0001-select-row-1-2-in"},
				},
				{
					Query: `SELECT ROW(1, 3) IN (SELECT 1, 2);`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testrowinsubqueryacceptsmultiplecolumnsrepro-0002-select-row-1-3-in"},
				},
			},
		},
	})
}

// TestRowConstructorExpandsTableAliasStarRepro reproduces a row/composite
// correctness bug: PostgreSQL lets table-alias star expansion contribute the
// current row's fields inside row constructors.
func TestRowConstructorExpandsTableAliasStarRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROW constructor expands table alias star",
			SetUpScript: []string{
				`CREATE TABLE row_alias_users (
					name TEXT,
					location TEXT,
					age INT
				);`,
				`INSERT INTO row_alias_users VALUES
					('jason', 'SEA', 42),
					('max', 'SFO', 31);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT ROW(p.*, 99)::text
						FROM row_alias_users p
						ORDER BY name;`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testrowconstructorexpandstablealiasstarrepro-0001-select-row-p.*-99-::text"},
				},
			},
		},
	})
}

// TestTableAliasCompositeFieldSelectionGuard guards PostgreSQL row/composite
// semantics where a table alias stands for the current row and field selection
// can project one column from that composite value.
func TestTableAliasCompositeFieldSelectionGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "table alias composite field selection",
			SetUpScript: []string{
				`CREATE TABLE row_field_users (
					name TEXT,
					location TEXT,
					age INT
				);`,
				`INSERT INTO row_field_users VALUES
					('jason', 'SEA', 42),
					('max', 'SFO', 31);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT (p).location
						FROM row_field_users p
						ORDER BY name;`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testtablealiascompositefieldselectionguard-0001-select-p-.location-from-row_field_users"},
				},
			},
		},
	})
}

// TestInSubqueryCrossTypeEqualityRepro reproduces a correctness bug: IN
// subqueries should use PostgreSQL equality semantics, including implicit
// cross-type equality, instead of relying only on raw value hashes.
func TestInSubqueryCrossTypeEqualityRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "IN subquery cross-type equality",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT 1::int8 IN (SELECT 1::numeric);`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testinsubquerycrosstypeequalityrepro-0001-select-1::int8-in-select-1::numeric"},
				},
				{
					Query: `SELECT 1::numeric IN (SELECT 1::int8);`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testinsubquerycrosstypeequalityrepro-0002-select-1::numeric-in-select-1::int8"},
				},
				{
					Query: `SELECT 1::int4 IN (SELECT 1.0::float8);`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testinsubquerycrosstypeequalityrepro-0003-select-1::int4-in-select-1.0::float8"},
				},
			},
		},
	})
}

// TestScalarSubqueryEqualityGuard guards that PostgreSQL evaluates equality
// between scalar subquery results using the returned value types' equality
// operators.
func TestScalarSubqueryEqualityGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "scalar subquery equality",
			SetUpScript: []string{
				`CREATE TABLE scalar_subquery_equality_items (
					id INT PRIMARY KEY,
					label VARCHAR
				);`,
				`INSERT INTO scalar_subquery_equality_items VALUES
					(1, 'a'),
					(2, 'b'),
					(3, 'b');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT
						(SELECT id FROM scalar_subquery_equality_items WHERE id = 2) =
							(SELECT id FROM scalar_subquery_equality_items WHERE id = 2),
						(SELECT label FROM scalar_subquery_equality_items WHERE id = 2) =
							(SELECT label FROM scalar_subquery_equality_items WHERE id = 3),
						(SELECT label FROM scalar_subquery_equality_items WHERE id = 1) =
							(SELECT label FROM scalar_subquery_equality_items WHERE id = 2);`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testscalarsubqueryequalityguard-0001-select-select-id-from-scalar_subquery_equality_items"},
				},
			},
		},
	})
}

// TestScalarSubqueryRejectsMultipleRowsInDmlRepro guards PostgreSQL DML
// semantics: scalar subqueries used as INSERT or UPDATE expressions must fail
// if they return more than one row, instead of choosing an arbitrary row to
// store.
func TestScalarSubqueryRejectsMultipleRowsInDmlRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "INSERT scalar subquery rejects multiple rows",
			SetUpScript: []string{
				`CREATE TABLE scalar_subquery_insert_source (value INT);`,
				`CREATE TABLE scalar_subquery_insert_target (id INT PRIMARY KEY, value INT);`,
				`INSERT INTO scalar_subquery_insert_source VALUES (10), (20);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO scalar_subquery_insert_target
						VALUES (1, (SELECT value FROM scalar_subquery_insert_source ORDER BY value));`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testscalarsubqueryrejectsmultiplerowsindmlrepro-0001-insert-into-scalar_subquery_insert_target-values-1", Compare: "sqlstate"},
				},
				{
					Query: `SELECT count(*) FROM scalar_subquery_insert_target;`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testscalarsubqueryrejectsmultiplerowsindmlrepro-0002-select-count-*-from-scalar_subquery_insert_target"},
				},
			},
		},
		{
			Name: "UPDATE scalar subquery rejects multiple rows",
			SetUpScript: []string{
				`CREATE TABLE scalar_subquery_update_source (value INT);`,
				`CREATE TABLE scalar_subquery_update_target (id INT PRIMARY KEY, value INT);`,
				`INSERT INTO scalar_subquery_update_source VALUES (10), (20);`,
				`INSERT INTO scalar_subquery_update_target VALUES (1, 1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE scalar_subquery_update_target
						SET value = (
							SELECT value FROM scalar_subquery_update_source ORDER BY value
						)
						WHERE id = 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testscalarsubqueryrejectsmultiplerowsindmlrepro-0003-update-scalar_subquery_update_target-set-value-=", Compare: "sqlstate"},
				},
				{
					Query: `SELECT value FROM scalar_subquery_update_target WHERE id = 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testscalarsubqueryrejectsmultiplerowsindmlrepro-0004-select-value-from-scalar_subquery_update_target-where"},
				},
			},
		},
	})
}

// TestRowIsNotDistinctFromHandlesNullsRepro reproduces a row-comparison
// correctness bug: PostgreSQL treats NULL fields as equal under IS NOT
// DISTINCT FROM.
func TestRowIsNotDistinctFromHandlesNullsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "row IS NOT DISTINCT FROM handles NULL fields",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT ROW(1, NULL) IS NOT DISTINCT FROM ROW(1, NULL);`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testrowisnotdistinctfromhandlesnullsrepro-0001-select-row-1-null-is"},
				},
				{
					Query: `SELECT ROW(NULL, 4) IS DISTINCT FROM ROW(NULL, 4);`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testrowisnotdistinctfromhandlesnullsrepro-0002-select-row-null-4-is"},
				},
			},
		},
	})
}

// TestRowValueComparisonsUseLexicographicSemanticsRepro guards PostgreSQL row
// comparison semantics: row values compare left-to-right using the first
// unequal field.
func TestRowValueComparisonsUseLexicographicSemanticsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "row value comparisons use lexicographic semantics",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT ROW(1, 2) < ROW(1, 3);`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testrowvaluecomparisonsuselexicographicsemanticsrepro-0001-select-row-1-2-<"},
				},
				{
					Query: `SELECT ROW(2, 1) > ROW(1, 999);`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testrowvaluecomparisonsuselexicographicsemanticsrepro-0002-select-row-2-1->"},
				},
				{
					Query: `SELECT ROW(1, 2) <= ROW(1, 2);`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testrowvaluecomparisonsuselexicographicsemanticsrepro-0003-select-row-1-2-<="},
				},
			},
		},
	})
}

// TestRowValueComparisonsHandleNullsRepro guards PostgreSQL row comparison
// semantics around NULL fields: comparisons short-circuit on earlier unequal
// fields, but return NULL when the decisive field comparison is NULL.
func TestRowValueComparisonsHandleNullsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "row value comparisons handle NULL fields",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT ROW(1, 2) < ROW(1, NULL);`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testrowvaluecomparisonshandlenullsrepro-0001-select-row-1-2-<"},
				},
				{
					Query: `SELECT ROW(1, 2, 3) < ROW(1, 3, NULL);`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testrowvaluecomparisonshandlenullsrepro-0002-select-row-1-2-3"},
				},
				{
					Query: `SELECT ROW(1, 2, 3) = ROW(1, NULL, 4);`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testrowvaluecomparisonshandlenullsrepro-0003-select-row-1-2-3"},
				},
				{
					Query: `SELECT ROW(1, 2, 3) <> ROW(1, NULL, 4);`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testrowvaluecomparisonshandlenullsrepro-0004-select-row-1-2-3"},
				},
			},
		},
	})
}

// TestOrderByUsesPostgresNullOrderingRepro guards PostgreSQL NULL ordering
// defaults: NULLS LAST for ascending ORDER BY and NULLS FIRST for descending
// ORDER BY unless the query specifies otherwise.
func TestOrderByUsesPostgresNullOrderingRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ORDER BY uses PostgreSQL NULL ordering defaults",
			SetUpScript: []string{
				`CREATE TABLE order_by_null_items (id INT PRIMARY KEY, v INT);`,
				`INSERT INTO order_by_null_items VALUES (1, 10), (2, NULL), (3, 20);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, v
						FROM order_by_null_items
						ORDER BY v ASC;`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testorderbyusespostgresnullorderingrepro-0001-select-id-v-from-order_by_null_items"},
				},
				{
					Query: `SELECT id, v
						FROM order_by_null_items
						ORDER BY v DESC;`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testorderbyusespostgresnullorderingrepro-0002-select-id-v-from-order_by_null_items"},
				},
			},
		},
	})
}

// TestOrderByUsingOperatorRepro reproduces a query correctness gap:
// PostgreSQL accepts ORDER BY ... USING <operator> and sorts according to the
// named ordering operator.
func TestOrderByUsingOperatorRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ORDER BY USING operator controls sort direction",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT v
						FROM (VALUES (1), (3), (2), (NULL)) AS t(v)
						ORDER BY v USING > NULLS LAST;`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testorderbyusingoperatorrepro-0001-select-v-from-values-1"},
				},
				{
					Query: `SELECT v
						FROM (VALUES (1), (3), (2), (NULL)) AS t(v)
						ORDER BY v USING < NULLS FIRST;`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testorderbyusingoperatorrepro-0002-select-v-from-values-1"},
				},
			},
		},
	})
}

// TestVarcharComparedToBlankPaddedCharRepro guards PostgreSQL comparison
// semantics for fixed-length char values compared against varchar.
func TestVarcharComparedToBlankPaddedCharRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "varchar comparison with blank-padded char",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT 'def'::varchar = CAST('def' AS char(6));`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testvarcharcomparedtoblankpaddedcharrepro-0001-select-def-::varchar-=-cast"},
				},
				{
					Query: `SELECT 'def'::varchar < CAST('def' AS char(6));`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testvarcharcomparedtoblankpaddedcharrepro-0002-select-def-::varchar-<-cast"},
				},
				{
					Query: `SELECT 'def'::varchar >= CAST('def' AS char(6));`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testvarcharcomparedtoblankpaddedcharrepro-0003-select-def-::varchar->=-cast"},
				},
			},
		},
	})
}

// TestVarcharColumnComparedToBlankPaddedCharRepro guards varchar column
// comparisons against blank-padded char casts.
func TestVarcharColumnComparedToBlankPaddedCharRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "varchar column comparison with blank-padded char",
			SetUpScript: []string{
				`CREATE TABLE varchar_char_compare_items (v VARCHAR(10) PRIMARY KEY);`,
				`INSERT INTO varchar_char_compare_items VALUES ('abc'), ('def'), ('ghi');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT v
						FROM varchar_char_compare_items
						WHERE v = CAST('def' AS char(6));`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testvarcharcolumncomparedtoblankpaddedcharrepro-0001-select-v-from-varchar_char_compare_items-where"},
				},
				{
					Query: `SELECT v
						FROM varchar_char_compare_items
						WHERE v < CAST('def' AS char(6))
						ORDER BY v;`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testvarcharcolumncomparedtoblankpaddedcharrepro-0002-select-v-from-varchar_char_compare_items-where"},
				},
				{
					Query: `SELECT v
						FROM varchar_char_compare_items
						WHERE v >= CAST('def' AS char(6))
						ORDER BY v;`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testvarcharcolumncomparedtoblankpaddedcharrepro-0003-select-v-from-varchar_char_compare_items-where"},
				},
			},
		},
	})
}

// TestBooleanInPredicateWithIndexedBooleanColumnRepro guards boolean IN
// predicates when another table has a boolean secondary index.
func TestBooleanInPredicateWithIndexedBooleanColumnRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "boolean IN predicate does not panic with boolean indexes present",
			SetUpScript: []string{
				`CREATE TABLE bool_in_scan_items (b BOOL);`,
				`INSERT INTO bool_in_scan_items VALUES (false);`,
				`CREATE TABLE bool_in_index_items (b BOOL);`,
				`CREATE INDEX bool_in_index_items_b_idx ON bool_in_index_items(b);`,
				`INSERT INTO bool_in_index_items VALUES (false);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM bool_in_scan_items WHERE b IN (false);`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testbooleaninpredicatewithindexedbooleancolumnrepro-0001-select-*-from-bool_in_scan_items-where"},
				},
				{
					Query: `SELECT * FROM bool_in_index_items WHERE b IN (false);`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testbooleaninpredicatewithindexedbooleancolumnrepro-0002-select-*-from-bool_in_index_items-where"},
				},
			},
		},
	})
}

// TestDistinctOnRequiresMatchingLeadingOrderByRepro guards query correctness:
// PostgreSQL rejects DISTINCT ON queries whose leading ORDER BY expressions do
// not match the DISTINCT ON expressions.
func TestDistinctOnRequiresMatchingLeadingOrderByRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DISTINCT ON requires matching leading ORDER BY expressions",
			SetUpScript: []string{
				`CREATE TABLE distinct_on_order_items (
					id INT PRIMARY KEY,
					account_id INT,
					created_at INT
				);`,
				`INSERT INTO distinct_on_order_items VALUES
					(1, 10, 100),
					(2, 10, 200),
					(3, 20, 150);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT DISTINCT ON (account_id) account_id, id
						FROM distinct_on_order_items
						ORDER BY created_at DESC;`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testdistinctonrequiresmatchingleadingorderbyrepro-0001-select-distinct-on-account_id-account_id",

						// TestFetchFirstWithTiesIncludesPeerRowsRepro reproduces a query correctness
						// gap: FETCH FIRST ... WITH TIES should include rows tied with the last row in
						// the limited ordered prefix.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestFetchFirstWithTiesIncludesPeerRowsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "FETCH FIRST WITH TIES includes peer rows",
			SetUpScript: []string{
				`CREATE TABLE fetch_ties_items (
					id INT PRIMARY KEY,
					score INT
				);`,
				`INSERT INTO fetch_ties_items VALUES
					(1, 10),
					(2, 20),
					(3, 20),
					(4, 30);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, score
						FROM fetch_ties_items
						ORDER BY score
						FETCH FIRST 2 ROWS WITH TIES;`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testfetchfirstwithtiesincludespeerrowsrepro-0001-select-id-score-from-fetch_ties_items"},
				},
				{
					Query: `SELECT id, score
						FROM fetch_ties_items
						ORDER BY score
						OFFSET 1 ROW
						FETCH FIRST 1 ROW WITH TIES;`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testfetchfirstwithtiesincludespeerrowsrepro-0002-select-id-score-from-fetch_ties_items"},
				},
				{
					Query: `SELECT id FROM fetch_ties_items FETCH FIRST 1 ROW WITH TIES;`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testfetchfirstwithtiesincludespeerrowsrepro-0003-select-id-from-fetch_ties_items-fetch",

						// TestTableSampleSystemHundredReturnsAllRowsRepro reproduces a query
						// correctness gap: PostgreSQL supports TABLESAMPLE, and SYSTEM (100) should
						// return every row in the sampled relation.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestTableSampleSystemHundredReturnsAllRowsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "TABLESAMPLE SYSTEM 100 returns all rows",
			SetUpScript: []string{
				`CREATE TABLE tablesample_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO tablesample_items VALUES
					(1, 'a'),
					(2, 'b');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT count(*) FROM tablesample_items TABLESAMPLE SYSTEM (100);`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testtablesamplesystemhundredreturnsallrowsrepro-0001-select-count-*-from-tablesample_items"},
				},
			},
		},
	})
}

// TestRecursiveCteSearchClauseRepro reproduces a query compatibility gap:
// PostgreSQL recursive CTEs support SQL-standard SEARCH ordering clauses.
func TestRecursiveCteSearchClauseRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "WITH RECURSIVE supports SEARCH clause",
			Assertions: []ScriptTestAssertion{
				{
					Query: `WITH RECURSIVE tree(id, parent_id) AS (
							VALUES (1, NULL::INT)
							UNION ALL
							SELECT child.id, child.parent_id
							FROM (VALUES (2, 1), (3, 1)) AS child(id, parent_id)
							JOIN tree ON child.parent_id = tree.id
						) SEARCH BREADTH FIRST BY id SET bfs_order
						SELECT id FROM tree ORDER BY bfs_order;`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testrecursivectesearchclauserepro-0001-with-recursive-tree-id-parent_id"},
				},
			},
		},
	})
}

// TestRecursiveCteCycleClauseRepro reproduces a query compatibility gap:
// PostgreSQL recursive CTEs support SQL-standard CYCLE clauses that add cycle
// marker and path columns.
func TestRecursiveCteCycleClauseRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "WITH RECURSIVE supports CYCLE clause",
			Assertions: []ScriptTestAssertion{
				{
					Query: `WITH RECURSIVE nums(n) AS (
							VALUES (1)
							UNION ALL
							SELECT n + 1 FROM nums WHERE n < 3
						) CYCLE n SET is_cycle USING path
						SELECT n, is_cycle FROM nums ORDER BY n;`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testrecursivectecycleclauserepro-0001-with-recursive-nums-n-as"},
				},
			},
		},
	})
}

// TestSelectCanProjectTableoidRepro reproduces a query correctness gap:
// PostgreSQL exposes tableoid as a system column for ordinary base-table
// scans.
func TestSelectCanProjectTableoidRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "SELECT can project tableoid",
			SetUpScript: []string{
				`CREATE TABLE select_tableoid_items (
					id INT PRIMARY KEY
				);`,
				`INSERT INTO select_tableoid_items VALUES (1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT tableoid::regclass::text, id
						FROM select_tableoid_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testselectcanprojecttableoidrepro-0001-select-tableoid::regclass::text-id-from-select_tableoid_items"},
				},
			},
		},
	})
}

// TestRowsFromMultipleSetReturningFunctionsRepro reproduces a query
// correctness gap: ROWS FROM can zip multiple set-returning functions and pad
// shorter result sets with NULLs.
func TestRowsFromMultipleSetReturningFunctionsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROWS FROM pads shorter set-returning functions",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT *
						FROM ROWS FROM (
							generate_series(1, 2),
							unnest(ARRAY['a','b','c'])
						) AS t(n, label);`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testrowsfrommultiplesetreturningfunctionsrepro-0001-select-*-from-rows-from"},
				},
			},
		},
	})
}

// TestUnnestMultipleArraysPadsShorterInputsRepro reproduces a query
// correctness gap: PostgreSQL's multi-argument unnest zips arrays and pads
// shorter inputs with NULLs.
func TestUnnestMultipleArraysPadsShorterInputsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "multi-array unnest pads shorter arrays",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT *
						FROM unnest(
							ARRAY[10, 20],
							ARRAY['foo', 'bar', 'baz']
						) AS u(n, label);`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testunnestmultiplearrayspadsshorterinputsrepro-0001-select-*-from-unnest-array[10"},
				},
			},
		},
	})
}

// TestGenerateSeriesWithOrdinalityRepro reproduces a query correctness gap:
// PostgreSQL allows WITH ORDINALITY on generate_series table functions.
func TestGenerateSeriesWithOrdinalityRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "generate_series supports WITH ORDINALITY",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT v, ord
						FROM generate_series(4, 8, 2) WITH ORDINALITY AS g(v, ord);`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testgenerateserieswithordinalityrepro-0001-select-v-ord-from-generate_series"},
				},
			},
		},
	})
}

// TestGenerateSeriesTimestampUnknownStepGuard guards that PostgreSQL resolves
// an unknown string step argument to interval for timestamp generate_series
// calls.
func TestGenerateSeriesTimestampUnknownStepGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "timestamp generate_series coerces unknown step to interval",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT *
						FROM generate_series(
							'2008-03-02 12:00'::timestamp,
							'2008-03-01 00:00'::timestamp,
							'-10 hours'
						);`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testgenerateseriestimestampunknownstepguard-0001-select-*-from-generate_series-2008-03-02"},
				},
			},
		},
	})
}

// TestWindowFrameExcludeCurrentRowRepro reproduces a query correctness bug:
// EXCLUDE CURRENT ROW should remove the current row from the window frame.
func TestWindowFrameExcludeCurrentRowRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "window frame EXCLUDE CURRENT ROW excludes current row",
			Assertions: []ScriptTestAssertion{
				{
					Query: `WITH items(id, v) AS (VALUES (1, 10), (2, 20), (3, 30))
						SELECT id, sum(v) OVER (
							ORDER BY id
							ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
							EXCLUDE CURRENT ROW
						)
						FROM items
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testwindowframeexcludecurrentrowrepro-0001-with-items-id-v-as"},
				},
			},
		},
	})
}

// TestWindowFrameGroupsRepro reproduces a query correctness gap: PostgreSQL
// supports GROUPS window frames, which advance by peer groups rather than
// physical row counts or value ranges.
func TestWindowFrameGroupsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "window frame GROUPS computes peer-group frame",
			Assertions: []ScriptTestAssertion{
				{
					Query: `WITH items(id, v) AS (VALUES (1, 10), (2, 20), (3, 30), (4, 40))
						SELECT id, sum(v) OVER (
							ORDER BY id
							GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW
						)
						FROM items
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testwindowframegroupsrepro-0001-with-items-id-v-as"},
				},
			},
		},
	})
}

// TestWindowFrameExcludeGroupRepro reproduces a query correctness bug: EXCLUDE
// GROUP should remove the current row and all ordering peers from the window
// frame.
func TestWindowFrameExcludeGroupRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "window frame EXCLUDE GROUP excludes peer group",
			Assertions: []ScriptTestAssertion{
				{
					Query: `WITH items(id, score, v) AS (
							VALUES (1, 10, 1), (2, 20, 2), (3, 20, 3), (4, 30, 4)
						)
						SELECT id, sum(v) OVER (
							ORDER BY score
							ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
							EXCLUDE GROUP
						)
						FROM items
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testwindowframeexcludegrouprepro-0001-with-items-id-score-v"},
				},
			},
		},
	})
}

// TestWindowFrameExcludeTiesRepro reproduces a query correctness bug: EXCLUDE
// TIES should remove ordering peers from the window frame while keeping the
// current row.
func TestWindowFrameExcludeTiesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "window frame EXCLUDE TIES excludes peer rows",
			Assertions: []ScriptTestAssertion{
				{
					Query: `WITH items(id, score, v) AS (
							VALUES (1, 10, 1), (2, 20, 2), (3, 20, 3), (4, 30, 4)
						)
						SELECT id, sum(v) OVER (
							ORDER BY score
							ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
							EXCLUDE TIES
						)
						FROM items
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testwindowframeexcludetiesrepro-0001-with-items-id-score-v"},
				},
			},
		},
	})
}

// TestWindowFrameRangeOffsetRepro reproduces a query correctness bug:
// PostgreSQL supports RANGE frames with value offsets against a single ORDER BY
// expression.
func TestWindowFrameRangeOffsetRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "window frame RANGE offset computes value-distance frame",
			SetUpScript: []string{
				`CREATE TABLE window_range_items (
					id INT PRIMARY KEY,
					v INT
				);`,
				`INSERT INTO window_range_items VALUES
					(1, 10),
					(2, 20),
					(4, 40),
					(5, 50);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, sum(v) OVER (
							ORDER BY id
							RANGE BETWEEN 1 PRECEDING AND CURRENT ROW
						)
						FROM window_range_items
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testwindowframerangeoffsetrepro-0001-select-id-sum-v-over"},
				},
			},
		},
	})
}

// TestCumeDistWindowFunctionRepro reproduces a query correctness gap:
// PostgreSQL supports cume_dist() as a ranking window function.
func TestCumeDistWindowFunctionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "cume_dist window function",
			Assertions: []ScriptTestAssertion{
				{
					Query: `WITH scores(id, score) AS (
							VALUES (1, 10), (2, 20), (3, 20), (4, 30)
						)
						SELECT id, cume_dist() OVER (ORDER BY score)
						FROM scores
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testcumedistwindowfunctionrepro-0001-with-scores-id-score-as"},
				},
			},
		},
	})
}

// TestNthValueWindowFunctionRepro reproduces a query correctness gap:
// PostgreSQL supports nth_value over an explicit window frame.
func TestNthValueWindowFunctionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "nth_value window function",
			Assertions: []ScriptTestAssertion{
				{
					Query: `WITH items(id, v) AS (VALUES (1, 10), (2, 20), (3, 30))
						SELECT id, nth_value(v, 2) OVER (
							ORDER BY id
							ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
						)
						FROM items
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testnthvaluewindowfunctionrepro-0001-with-items-id-v-as"},
				},
			},
		},
	})
}

// TestWindowAggregateFilterReturnsNullForEmptyFrameRepro reproduces a query
// correctness bug: a filtered SUM over a window frame with no selected input
// rows should return NULL, matching ordinary aggregate semantics.
func TestWindowAggregateFilterReturnsNullForEmptyFrameRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "window aggregate FILTER returns NULL for empty filtered frame",
			Assertions: []ScriptTestAssertion{
				{
					Query: `WITH items(id, grp, v, paid) AS (
							VALUES
								(1, 'a', 10, true),
								(2, 'a', 20, false),
								(3, 'a', 30, true),
								(4, 'b', 40, false)
						)
						SELECT id, sum(v) FILTER (WHERE paid) OVER (
							PARTITION BY grp
							ORDER BY id
							ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
						)
						FROM items
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testwindowaggregatefilterreturnsnullforemptyframerepro-0001-with-items-id-grp-v"},
				},
			},
		},
	})
}

// TestAvgWindowAggregateFilterReturnsNullForEmptyFrameRepro reproduces a query
// correctness bug: a filtered AVG over a window frame with no selected input
// rows should return NULL rather than trying to materialize NaN.
func TestAvgWindowAggregateFilterReturnsNullForEmptyFrameRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "window avg FILTER returns NULL for empty filtered frame",
			Assertions: []ScriptTestAssertion{
				{
					Query: `WITH items(id, grp, v, paid) AS (
							VALUES
								(1, 'a', 10, true),
								(2, 'a', 20, false),
								(3, 'b', 30, false)
						)
						SELECT id, avg(v) FILTER (WHERE paid) OVER (
							PARTITION BY grp
							ORDER BY id
							ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
						)
						FROM items
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testavgwindowaggregatefilterreturnsnullforemptyframerepro-0001-with-items-id-grp-v"},
				},
			},
		},
	})
}

// TestBooleanAggregatesCanBeWindowFunctionsRepro reproduces a query
// correctness gap: PostgreSQL allows bool_or and bool_and as ordinary window
// aggregate functions.
func TestBooleanAggregatesCanBeWindowFunctionsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "boolean aggregates can be window functions",
			Assertions: []ScriptTestAssertion{
				{
					Query: `WITH items(id, grp, paid) AS (
							VALUES
								(1, 'a', true),
								(2, 'a', false),
								(3, 'b', false)
						)
						SELECT id,
							bool_or(paid) OVER (
								PARTITION BY grp
								ORDER BY id
								ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
							),
							bool_and(paid) OVER (
								PARTITION BY grp
								ORDER BY id
								ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
							)
						FROM items
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testbooleanaggregatescanbewindowfunctionsrepro-0001-with-items-id-grp-paid"},
				},
			},
		},
	})
}

// TestArrayAggCanBeWindowFunctionRepro reproduces a query correctness gap:
// PostgreSQL allows array_agg as a window aggregate without requiring GROUP BY.
func TestArrayAggCanBeWindowFunctionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "array_agg can be a window function",
			Assertions: []ScriptTestAssertion{
				{
					Query: `WITH items(id, grp, label) AS (
							VALUES
								(1, 'a', 'x'::text),
								(2, 'a', 'y'::text),
								(3, 'b', 'z'::text)
						)
						SELECT id,
							array_agg(label) OVER (
								PARTITION BY grp
								ORDER BY id
								ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
							)
						FROM items
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testarrayaggcanbewindowfunctionrepro-0001-with-items-id-grp-label"},
				},
			},
		},
	})
}

// TestLagLeadConstantOffsetAndDefaultRepro reproduces a query correctness gap:
// PostgreSQL supports lag/lead with a constant offset and default value.
func TestLagLeadConstantOffsetAndDefaultRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "lag and lead accept constant offset and default",
			SetUpScript: []string{
				`CREATE TABLE lag_lead_constant_items (
					id INT PRIMARY KEY,
					v INT
				);`,
				`INSERT INTO lag_lead_constant_items VALUES
					(1, 10),
					(2, 20),
					(3, 30),
					(4, 40);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id,
							lag(v, 2, 0) OVER (ORDER BY id),
							lead(v, 2, 99) OVER (ORDER BY id)
						FROM lag_lead_constant_items
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testlagleadconstantoffsetanddefaultrepro-0001-select-id-lag-v-2"},
				},
			},
		},
	})
}

// TestLagLeadDynamicOffsetRepro reproduces a query correctness gap:
// PostgreSQL supports lag/lead offsets that are evaluated from the current row.
func TestLagLeadDynamicOffsetRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "lag and lead accept dynamic row offsets",
			SetUpScript: []string{
				`CREATE TABLE lag_lead_dynamic_items (
					id INT PRIMARY KEY,
					v INT,
					off INT
				);`,
				`INSERT INTO lag_lead_dynamic_items VALUES
					(1, 10, 1),
					(2, 20, 2),
					(3, 30, 1),
					(4, 40, 2);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id,
							lag(v, off, -1) OVER (ORDER BY id),
							lead(v, off, -2) OVER (ORDER BY id)
						FROM lag_lead_dynamic_items
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testlagleaddynamicoffsetrepro-0001-select-id-lag-v-off"},
				},
			},
		},
	})
}

// TestNtileDynamicBucketCountRepro reproduces a query correctness bug:
// PostgreSQL allows the ntile bucket-count argument to be an expression
// evaluated for the current row.
func TestNtileDynamicBucketCountRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ntile accepts dynamic bucket count",
			SetUpScript: []string{
				`CREATE TABLE ntile_dynamic_items (
					id INT PRIMARY KEY,
					buckets INT
				);`,
				`INSERT INTO ntile_dynamic_items VALUES
					(1, 2),
					(2, 2),
					(3, 2),
					(4, 2);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, ntile(buckets) OVER (ORDER BY id)
						FROM ntile_dynamic_items
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testntiledynamicbucketcountrepro-0001-select-id-ntile-buckets-over"},
				},
			},
		},
	})
}

// TestCompositeStarArgumentToFunctionRepro reproduces a query correctness bug:
// table.* should be accepted as a single composite argument when the function
// expects that table's row type.
func TestCompositeStarArgumentToFunctionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "table star passes composite row to function argument",
			SetUpScript: []string{
				`CREATE TABLE composite_star_items (
					id INT4 PRIMARY KEY,
					name TEXT NOT NULL,
					qty INT4 NOT NULL
				);`,
				`INSERT INTO composite_star_items VALUES
					(1, 'apple', 3),
					(2, 'banana', 5);`,
				`CREATE FUNCTION composite_star_score(t composite_star_items) RETURNS INT4 AS $$
				BEGIN
					RETURN t.id + t.qty;
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT composite_star_score(composite_star_items.*)
						FROM composite_star_items
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testcompositestarargumenttofunctionrepro-0001-select-composite_star_score-composite_star_items.*-from-composite_star_items"},
				},
			},
		},
	})
}

// TestUuidEqualityAfterPrimaryKeyRewriteRepro guards UUID values generated
// before an ALTER TABLE rewrite remaining comparable with normal PostgreSQL
// equality.
func TestUuidEqualityAfterPrimaryKeyRewriteRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "UUID equality works after adding primary key",
			SetUpScript: []string{
				`CREATE TABLE uuid_equality_items (
					id INT NOT NULL,
					uid UUID DEFAULT gen_random_uuid() NOT NULL
				);`,
				`INSERT INTO uuid_equality_items (id) VALUES (1), (2);`,
				`ALTER TABLE ONLY public.uuid_equality_items
					ADD CONSTRAINT uuid_equality_items_pkey PRIMARY KEY (id);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT
							(SELECT uid FROM uuid_equality_items WHERE id = 2) =
							(SELECT uid FROM uuid_equality_items WHERE id = 1);`,
					Expected: []sql.Row{{"f"}},
				},
			},
		},
	})
}

// TestSubstringForCountSyntaxRepro reproduces a string-function correctness
// bug: PostgreSQL supports the SQL-standard substring(string for count) form.
func TestSubstringForCountSyntaxRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "substring for count syntax",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT substring('hello' for 3);`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testsubstringforcountsyntaxrepro-0001-select-substring-hello-for-3"},
				},
			},
		},
	})
}

// TestSubstringSimilarEscapeSyntaxRepro reproduces a string-function
// correctness bug: PostgreSQL supports SQL-standard SIMILAR substring syntax
// with an ESCAPE marker for the capture expression.
func TestSubstringSimilarEscapeSyntaxRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "substring similar escape syntax",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT substring('hello.' similar 'hello#.' escape '#');`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testsubstringsimilarescapesyntaxrepro-0001-select-substring-hello.-similar-hello#."},
				},
				{
					Query: `SELECT substring('Thomas' similar '%#"o_a#"_' escape '#');`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testsubstringsimilarescapesyntaxrepro-0002-select-substring-thomas-similar-%#"},
				},
			},
		},
	})
}

// TestRegexpMatchesSupportedFlagsRepro reproduces a regex correctness bug:
// PostgreSQL supports expanded and newline-sensitive regular expression flags
// for regexp_matches.
func TestRegexpMatchesSupportedFlagsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "regexp_matches supports PostgreSQL regex flags",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT regexp_matches('ab', 'a b', 'x');`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testregexpmatchessupportedflagsrepro-0001-select-regexp_matches-ab-a-b"},
				},
				{
					Query: `SELECT regexp_matches(E'a\nb', '^b', 'n');`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testregexpmatchessupportedflagsrepro-0002-select-regexp_matches-e-a\\nb-^b"},
				},
			},
		},
	})
}

// TestRegexpReplaceReplacesMatchesRepro reproduces a string-function
// correctness bug: PostgreSQL supports regexp_replace for regex-based text
// replacement.
func TestRegexpReplaceReplacesMatchesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "regexp_replace replaces matches",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT regexp_replace('foobarbaz', 'b..', 'X'),
							regexp_replace('foobarbaz', 'b..', 'X', 'g');`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testregexpreplacereplacesmatchesrepro-0001-select-regexp_replace-foobarbaz-b..-x"},
				},
			},
		},
	})
}

// TestRegexpSplitToArraySplitsTextRepro reproduces a string-function
// correctness bug: PostgreSQL supports regexp_split_to_array for regex-based
// text splitting.
func TestRegexpSplitToArraySplitsTextRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "regexp_split_to_array splits text",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT regexp_split_to_array('a,b,c', ',');`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testregexpsplittoarraysplitstextrepro-0001-select-regexp_split_to_array-a-b-c"},
				},
			},
		},
	})
}

// TestRegexpLikeReturnsBooleanRepro reproduces a string-function correctness
// bug: PostgreSQL regexp_like returns a boolean result, not a text value.
func TestRegexpLikeReturnsBooleanRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "regexp_like returns boolean",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT regexp_like('abc', '^a');`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testregexplikereturnsbooleanrepro-0001-select-regexp_like-abc-^a"},
				},
			},
		},
	})
}

// TestRegexpCountCountsMatchesRepro reproduces a string-function correctness
// bug: PostgreSQL supports regexp_count for counting regex matches.
func TestRegexpCountCountsMatchesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "regexp_count counts matches",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT regexp_count('abcabc', 'a');`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testregexpcountcountsmatchesrepro-0001-select-regexp_count-abcabc-a"},
				},
			},
		},
	})
}

// TestRegexpSubstrReturnsMatchGuard guards that PostgreSQL regexp_substr
// returns the matching substring.
func TestRegexpSubstrReturnsMatchGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "regexp_substr returns match",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT regexp_substr('abcabc', 'b.');`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testregexpsubstrreturnsmatchguard-0001-select-regexp_substr-abcabc-b."},
				},
			},
		},
	})
}

// TestRegexpInstrReturnsPositionGuard guards that PostgreSQL regexp_instr
// returns the one-based match position.
func TestRegexpInstrReturnsPositionGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "regexp_instr returns position",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT regexp_instr('abcabc', 'b.');`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testregexpinstrreturnspositionguard-0001-select-regexp_instr-abcabc-b."},
				},
			},
		},
	})
}

// TestConcatWsSkipsNullsRepro reproduces a text-function correctness bug:
// PostgreSQL supports concat_ws with NULL-skipping separator semantics.
func TestConcatWsSkipsNullsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "concat_ws skips null arguments",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT concat_ws(',', 10, 20, NULL, 30),
							concat_ws('', 10, 20, NULL, 30),
							concat_ws(NULL, 10, 20) IS NULL;`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testconcatwsskipsnullsrepro-0001-select-concat_ws-10-20-null"},
				},
			},
		},
	})
}

// TestFormatDynamicWidthRepro reproduces a text-function correctness bug:
// PostgreSQL format supports field widths supplied by format arguments.
func TestFormatDynamicWidthRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "format dynamic width arguments",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT format('%*s|%*s', 5, 'x', -5, 'y');`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testformatdynamicwidthrepro-0001-select-format-%*s|%*s-5-x"},
				},
			},
		},
	})
}

// TestParseIdentSplitsQualifiedNamesRepro reproduces a name parsing
// correctness bug: PostgreSQL exposes parse_ident for splitting SQL
// identifiers with quoting and case-folding rules.
func TestParseIdentSplitsQualifiedNamesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "parse_ident splits qualified identifiers",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT parse_ident('Schemax.Tabley')::text[],
							parse_ident('"SchemaX"."TableY"')::text[],
							parse_ident('foo.boo[]', false)::text[];`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testparseidentsplitsqualifiednamesrepro-0001-select-parse_ident-schemax.tabley-::text[]-parse_ident"},
				},
			},
		},
	})
}

// TestStringToTableSplitsRowsRepro reproduces a string set-returning
// correctness bug: PostgreSQL supports string_to_table for splitting text into
// rows.
func TestStringToTableSplitsRowsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "string_to_table splits text into rows",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT v, v IS NULL
						FROM string_to_table('1|2|3', '|') AS g(v);`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-teststringtotablesplitsrowsrepro-0001-select-v-v-is-null"},
				},
			},
		},
	})
}

// TestFunctionNamedArgumentNotationRepro reproduces a function-call
// correctness bug: PostgreSQL supports named and mixed named argument
// notation for functions with argument names.
func TestFunctionNamedArgumentNotationRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "named argument notation",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT make_date(year => 2026, month => 5, day => 10)::text,
							make_date(2026, day => 10, month => 5)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testfunctionnamedargumentnotationrepro-0001-select-make_date-year-=>-2026"},
				},
			},
		},
	})
}

// TestCurrentCatalogColumnNameRepro reproduces a result-metadata correctness
// bug: PostgreSQL names the current_catalog value-expression column
// current_catalog.
func TestCurrentCatalogColumnNameRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:     "current_catalog column name",
			Database: "test",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT current_catalog;`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testcurrentcatalogcolumnnamerepro-0001-select-current_catalog"},
				},
			},
		},
	})
}

// TestCurrentSchemaColumnNameRepro reproduces a result-metadata correctness
// bug: PostgreSQL names the current_schema function-call column current_schema.
func TestCurrentSchemaColumnNameRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "current_schema column name",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT current_schema();`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testcurrentschemacolumnnamerepro-0001-select-current_schema"},
				},
			},
		},
	})
}

// TestCurrentDatabaseFromFunctionGuard guards that PostgreSQL allows scalar
// system-information functions in FROM.
func TestCurrentDatabaseFromFunctionGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:     "current_database function in FROM",
			Database: "test",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM current_database();`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testcurrentdatabasefromfunctionguard-0001-select-*-from-current_database"},
				},
			},
		},
	})
}

// TestCurrentCatalogFromExpressionGuard guards that PostgreSQL allows
// current_catalog in FROM as a single-row relation.
func TestCurrentCatalogFromExpressionGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:     "current_catalog expression in FROM",
			Database: "test",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM current_catalog;`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testcurrentcatalogfromexpressionguard-0001-select-*-from-current_catalog"},
				},
			},
		},
	})
}

// TestCurrentSchemaFromFunctionGuard guards that PostgreSQL allows
// current_schema() in FROM as a single-row function.
func TestCurrentSchemaFromFunctionGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "current_schema function in FROM",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM current_schema();`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testcurrentschemafromfunctionguard-0001-select-*-from-current_schema"},
				},
			},
		},
	})
}

// TestXmlWellFormedFunctionsRepro reproduces an XML correctness bug:
// PostgreSQL exposes text-based XML well-formedness predicates.
func TestXmlWellFormedFunctionsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "XML well-formedness predicates",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT xml_is_well_formed('<a/>'),
							xml_is_well_formed('<a>'),
							xml_is_well_formed_document('<a/>'),
							xml_is_well_formed_content('plain text');`, PostgresOracle: ScriptTestPostgresOracle{ID: "query-correctness-repro-test-testxmlwellformedfunctionsrepro-0001-select-xml_is_well_formed-<a/>-xml_is_well_formed-<a>"},
				},
			},
		},
	})
}
