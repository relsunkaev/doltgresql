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
	"context"
	"fmt"
	"strings"
	"testing"
)

const (
	jsonbGinBenchmarkRows = 1024
	btreeBenchmarkRows    = 4096
	btreeJoinProbeRows    = 512
)

func TestBtreeIndexPlannerShape(t *testing.T) {
	ctx, conn, controller := CreateServer(t, "postgres")
	t.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		if err := controller.WaitForStop(); err != nil {
			t.Fatalf("error stopping test server: %v", err)
		}
	})

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE btree_plan (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, score INTEGER NOT NULL, label TEXT NOT NULL)")
	insertBtreePlanRows(t, ctx, conn, "btree_plan", 128)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX btree_plan_tenant_score_idx ON btree_plan (tenant, score)")

	queries := []struct {
		name        string
		query       string
		want        int64
		indexedPlan bool
	}{
		{
			name:        "leading_column_equality",
			query:       `SELECT count(id) FROM btree_plan WHERE tenant = 4`,
			want:        16,
			indexedPlan: true,
		},
		{
			name:        "leading_column_range",
			query:       `SELECT count(id) FROM btree_plan WHERE tenant >= 2 AND tenant <= 5`,
			want:        64,
			indexedPlan: true,
		},
		{
			name:        "multi_column_prefix_equality",
			query:       `SELECT count(id) FROM btree_plan WHERE tenant = 4 AND score = 36`,
			want:        2,
			indexedPlan: true,
		},
		{
			name:        "multi_column_prefix_range",
			query:       `SELECT count(id) FROM btree_plan WHERE tenant = 4 AND score >= 32`,
			want:        8,
			indexedPlan: true,
		},
		{
			name:  "suffix_without_prefix",
			query: `SELECT count(id) FROM btree_plan WHERE score = 36`,
			want:  2,
		},
	}

	for _, query := range queries {
		query := query
		t.Run(query.name, func(t *testing.T) {
			assertBenchmarkPlanShape(t, ctx, conn, query.query, query.indexedPlan)
			assertCountResult(t, ctx, conn, query.query, query.want)
		})
	}
}

func TestBtreeStatsBackedIndexChoice(t *testing.T) {
	ctx, conn, controller := CreateServer(t, "postgres")
	t.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		if err := controller.WaitForStop(); err != nil {
			t.Fatalf("error stopping test server: %v", err)
		}
	})

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE btree_stats_choice (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, score INTEGER NOT NULL, label TEXT NOT NULL)")
	insertBtreeStatsChoiceRows(t, ctx, conn, "btree_stats_choice", 1024)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX btree_stats_choice_tenant_idx ON btree_stats_choice (tenant)")
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX btree_stats_choice_score_idx ON btree_stats_choice (score)")
	execBenchmarkSQL(t, ctx, conn, "ANALYZE btree_stats_choice")

	selectiveScore := `SELECT count(id) FROM btree_stats_choice WHERE tenant = 1 AND score = 777`
	assertBenchmarkPlanContains(t, ctx, conn, selectiveScore, "index: [btree_stats_choice.score]")
	assertCountResult(t, ctx, conn, selectiveScore, 1)

	insertBtreeConstantScoreRows(t, ctx, conn, "btree_stats_choice", 2000, 4096, 777)
	execBenchmarkSQL(t, ctx, conn, "ANALYZE btree_stats_choice")

	broadScore := `SELECT count(id) FROM btree_stats_choice WHERE tenant = 1 AND score = 777`
	assertBenchmarkPlanContains(t, ctx, conn, broadScore, "index: [btree_stats_choice.tenant]")
	assertCountResult(t, ctx, conn, broadScore, 1025)

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE btree_stats_composite_choice (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, score INTEGER NOT NULL, label TEXT NOT NULL)")
	insertBtreeStatsChoiceRows(t, ctx, conn, "btree_stats_composite_choice", 1024)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX btree_stats_composite_choice_tenant_idx ON btree_stats_composite_choice (tenant)")
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX btree_stats_composite_choice_tenant_score_idx ON btree_stats_composite_choice (tenant, score)")
	execBenchmarkSQL(t, ctx, conn, "ANALYZE btree_stats_composite_choice")

	compositePrefix := `SELECT count(id) FROM btree_stats_composite_choice WHERE tenant = 1 AND score = 777`
	assertBenchmarkPlanContains(t, ctx, conn, compositePrefix, "index: [btree_stats_composite_choice.tenant,btree_stats_composite_choice.score]")
	assertCountResult(t, ctx, conn, compositePrefix, 1)

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE btree_stats_missing_choice (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, score INTEGER NOT NULL, label TEXT NOT NULL)")
	insertBtreeStatsChoiceRows(t, ctx, conn, "btree_stats_missing_choice", 128)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX btree_stats_missing_choice_tenant_score_idx ON btree_stats_missing_choice (tenant, score)")

	missingStats := `SELECT count(id) FROM btree_stats_missing_choice WHERE tenant = 1 AND score = 77`
	assertBenchmarkPlanShape(t, ctx, conn, missingStats, true)
	assertCountResult(t, ctx, conn, missingStats, 1)
}

func TestBtreeBigIntStatsInequalityAcceptsIntLiteral(t *testing.T) {
	ctx, conn, controller := CreateServer(t, "postgres")
	t.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		if err := controller.WaitForStop(); err != nil {
			t.Fatalf("error stopping test server: %v", err)
		}
	})

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE btree_bigint_stats_cmp (id INTEGER PRIMARY KEY, big_int_col BIGINT NOT NULL)")
	execBenchmarkSQL(t, ctx, conn, "INSERT INTO btree_bigint_stats_cmp VALUES (1, -1), (2, 0), (3, 1), (4, 2)")
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX btree_bigint_stats_cmp_big_int_idx ON btree_bigint_stats_cmp (big_int_col)")
	execBenchmarkSQL(t, ctx, conn, "ANALYZE btree_bigint_stats_cmp")

	assertCountResult(t, ctx, conn, `SELECT count(id) FROM btree_bigint_stats_cmp WHERE big_int_col > 0`, 2)
}

func TestExpressionBtreeIndexPlannerShape(t *testing.T) {
	ctx, conn, controller := CreateServer(t, "postgres")
	t.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		if err := controller.WaitForStop(); err != nil {
			t.Fatalf("error stopping test server: %v", err)
		}
	})

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE expression_btree_plan (id INTEGER PRIMARY KEY, title TEXT NOT NULL)")
	insertExpressionBtreePlanRows(t, ctx, conn, "expression_btree_plan", 128)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX expression_btree_plan_lower_idx ON expression_btree_plan (lower(title))")

	queries := []struct {
		name        string
		query       string
		want        int64
		indexedPlan bool
	}{
		{
			name:        "matching_expression_equality",
			query:       `SELECT count(id) FROM expression_btree_plan WHERE lower(title) = 'title-4'`,
			want:        8,
			indexedPlan: true,
		},
	}

	for _, query := range queries {
		query := query
		t.Run(query.name, func(t *testing.T) {
			assertBenchmarkPlanShape(t, ctx, conn, query.query, query.indexedPlan)
			assertCountResult(t, ctx, conn, query.query, query.want)
		})
	}
}

func TestMixedExpressionBtreeIndexPlannerBoundary(t *testing.T) {
	ctx, conn, controller := CreateServer(t, "postgres")
	t.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		if err := controller.WaitForStop(); err != nil {
			t.Fatalf("error stopping test server: %v", err)
		}
	})

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE mixed_expression_btree_plan (id INTEGER PRIMARY KEY, title TEXT NOT NULL, code TEXT NOT NULL)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO mixed_expression_btree_plan VALUES
		(1, 'Alpha', 'a1'),
		(2, 'ALPHA', 'a1'),
		(3, 'alpha', 'a2'),
		(4, 'Beta', 'b1')`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX mixed_expression_btree_plan_idx ON mixed_expression_btree_plan (lower(title), code)")

	// Mixed expression btree indexes are currently metadata-backed by ordinary
	// storage columns. Keep expression predicates on the safe scan path until
	// the executor can seek by the PostgreSQL-facing logical key.
	query := `SELECT count(id) FROM mixed_expression_btree_plan WHERE lower(title) = 'alpha' AND code = 'a1'`
	assertBenchmarkPlanShape(t, ctx, conn, query, false)
	assertCountResult(t, ctx, conn, query, 2)
}

func TestBtreeCrossTypeNumericRangePlannerShape(t *testing.T) {
	ctx, conn, controller := CreateServer(t, "postgres")
	t.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		if err := controller.WaitForStop(); err != nil {
			t.Fatalf("error stopping test server: %v", err)
		}
	})

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE btree_cross_type_range_plan (id INTEGER PRIMARY KEY, v INTEGER NOT NULL)")
	execBenchmarkSQL(t, ctx, conn, "INSERT INTO btree_cross_type_range_plan VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)")
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX btree_cross_type_range_plan_v_idx ON btree_cross_type_range_plan (v)")

	queries := []struct {
		name  string
		query string
		want  int64
	}{
		{
			name:  "integer_less_than_non_integral_float",
			query: `SELECT count(id) FROM btree_cross_type_range_plan WHERE v < 4.9::float8`,
			want:  4,
		},
		{
			name:  "integer_greater_than_non_integral_float",
			query: `SELECT count(id) FROM btree_cross_type_range_plan WHERE v > 2.1::float8`,
			want:  3,
		},
		{
			name:  "integer_equal_non_integral_float",
			query: `SELECT count(id) FROM btree_cross_type_range_plan WHERE v = 2.1::float8`,
			want:  0,
		},
	}

	for _, query := range queries {
		query := query
		t.Run(query.name, func(t *testing.T) {
			assertBenchmarkPlanShape(t, ctx, conn, query.query, true)
			assertCountResult(t, ctx, conn, query.query, query.want)
		})
	}
}

func TestBtreePatternOpclassPlannerBoundary(t *testing.T) {
	ctx, conn, controller := CreateServer(t, "postgres")
	t.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		if err := controller.WaitForStop(); err != nil {
			t.Fatalf("error stopping test server: %v", err)
		}
	})

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE btree_pattern_opclass_plan (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
	execBenchmarkSQL(t, ctx, conn, "INSERT INTO btree_pattern_opclass_plan VALUES (1, 'alpha'), (2, 'alphabet'), (3, 'beta'), (4, 'alpaca')")
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX btree_pattern_opclass_plan_name_idx ON btree_pattern_opclass_plan (name text_pattern_ops)")

	indexDef := queryBenchmarkString(t, ctx, conn, "SELECT indexdef FROM pg_catalog.pg_indexes WHERE indexname = 'btree_pattern_opclass_plan_name_idx'")
	if !strings.Contains(indexDef, "text_pattern_ops") {
		t.Fatalf("expected pg_indexes to preserve text_pattern_ops, got %q", indexDef)
	}

	query := `SELECT count(id) FROM btree_pattern_opclass_plan WHERE name LIKE 'alph%'`
	assertBenchmarkPlanShape(t, ctx, conn, query, true)
	assertCountResult(t, ctx, conn, query, 2)

	query = `SELECT count(id) FROM btree_pattern_opclass_plan WHERE name LIKE '%lph%'`
	assertBenchmarkPlanShape(t, ctx, conn, query, false)
	assertCountResult(t, ctx, conn, query, 2)

	query = `SELECT count(id) FROM btree_pattern_opclass_plan WHERE name LIKE 'alph_'`
	assertBenchmarkPlanShape(t, ctx, conn, query, false)
	assertCountResult(t, ctx, conn, query, 1)
}

func TestBtreePatternOpclassCacheDoesNotLeakAcrossIndexRecreate(t *testing.T) {
	ctx, conn, controller := CreateServer(t, "postgres")
	t.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		if err := controller.WaitForStop(); err != nil {
			t.Fatalf("error stopping test server: %v", err)
		}
	})

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE btree_pattern_cache_recreate (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
	execBenchmarkSQL(t, ctx, conn, "INSERT INTO btree_pattern_cache_recreate VALUES (1, 'alpha'), (2, 'alphabet'), (3, 'beta'), (4, 'alpaca')")
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX btree_pattern_cache_recreate_name_idx ON btree_pattern_cache_recreate (name text_pattern_ops)")

	query := `SELECT count(id) FROM btree_pattern_cache_recreate WHERE name LIKE 'alph%'`
	assertBenchmarkPlanShape(t, ctx, conn, query, true)
	assertCountResult(t, ctx, conn, query, 2)

	execBenchmarkSQL(t, ctx, conn, "DROP INDEX btree_pattern_cache_recreate_name_idx")
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX btree_pattern_cache_recreate_name_idx ON btree_pattern_cache_recreate (name)")

	assertBenchmarkPlanShape(t, ctx, conn, query, false)
	assertCountResult(t, ctx, conn, query, 2)
}

func TestBtreeDMLRollbackPreservesIndex(t *testing.T) {
	ctx, conn, controller := CreateServer(t, "postgres")
	t.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		if err := controller.WaitForStop(); err != nil {
			t.Fatalf("error stopping test server: %v", err)
		}
	})

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE btree_dml_rollback (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, score INTEGER NOT NULL)")
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX btree_dml_rollback_tenant_score_idx ON btree_dml_rollback (tenant, score)")
	execBenchmarkSQL(t, ctx, conn, "BEGIN")
	execBenchmarkSQL(t, ctx, conn, "INSERT INTO btree_dml_rollback VALUES (1, 4, 10), (2, 4, 11)")
	execBenchmarkSQL(t, ctx, conn, "ROLLBACK")

	query := `SELECT count(id) FROM btree_dml_rollback WHERE tenant = 4 AND score >= 10`
	assertBenchmarkPlanShape(t, ctx, conn, query, true)
	assertCountResult(t, ctx, conn, query, 0)

	execBenchmarkSQL(t, ctx, conn, "INSERT INTO btree_dml_rollback VALUES (1, 4, 10), (2, 5, 11)")
	assertBenchmarkPlanShape(t, ctx, conn, query, true)
	assertCountResult(t, ctx, conn, query, 1)
}

func TestBtreeInferredPredicateHashJoinPlannerShape(t *testing.T) {
	ctx, conn, controller := CreateServer(t, "postgres")
	t.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		if err := controller.WaitForStop(); err != nil {
			t.Fatalf("error stopping test server: %v", err)
		}
	})

	setupBtreeJoinBenchmark(t, ctx, conn)

	selective := `SELECT count(*)
FROM btree_join_left_idx
JOIN btree_join_right_idx
  ON btree_join_right_idx.tenant = btree_join_left_idx.tenant
 AND btree_join_right_idx.score = btree_join_left_idx.score
WHERE btree_join_left_idx.tenant = 4`
	assertBenchmarkPlanContains(t, ctx, conn, selective, "HashJoin")
	assertBenchmarkPlanContains(t, ctx, conn, selective, "IndexedTableAccess(btree_join_right_idx)")
	assertBenchmarkPlanNotContains(t, ctx, conn, selective, "LookupJoin")
	assertCountResult(t, ctx, conn, selective, btreeJoinProbeRows/8*(btreeBenchmarkRows/64))

	nonselective := `SELECT count(*)
FROM btree_join_left_idx
JOIN btree_join_right_idx
  ON btree_join_right_idx.tenant = btree_join_left_idx.tenant
 AND btree_join_right_idx.score = btree_join_left_idx.score`
	assertCountResult(t, ctx, conn, nonselective, btreeJoinProbeRows*(btreeBenchmarkRows/64))
}

func TestBtreeJoinInfersConstantPredicateForIndexedSide(t *testing.T) {
	ctx, conn, controller := CreateServer(t, "postgres")
	t.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		if err := controller.WaitForStop(); err != nil {
			t.Fatalf("error stopping test server: %v", err)
		}
	})

	setupBtreeJoinBenchmark(t, ctx, conn)

	query := `SELECT count(*)
FROM btree_join_left_idx AS l
JOIN btree_join_right_idx AS r
  ON r.tenant = l.tenant
	AND r.score = l.score
WHERE l.tenant = 4`
	assertBenchmarkPlanContains(t, ctx, conn, query, "HashJoin")
	assertBenchmarkPlanContains(t, ctx, conn, query, "IndexedTableAccess(btree_join_right_idx)")
	assertBenchmarkPlanNotContains(t, ctx, conn, query, "LookupJoin")
	assertCountResult(t, ctx, conn, query, btreeJoinProbeRows/8*(btreeBenchmarkRows/64))
}

func TestBtreeJoinPredicateInferenceBoundaries(t *testing.T) {
	ctx, conn, controller := CreateServer(t, "postgres")
	t.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		if err := controller.WaitForStop(); err != nil {
			t.Fatalf("error stopping test server: %v", err)
		}
	})

	setupBtreeJoinBenchmark(t, ctx, conn)

	rightConstant := `SELECT count(*)
FROM btree_join_left_idx AS l
JOIN btree_join_right_idx AS r
  ON r.tenant = l.tenant
 AND r.score = l.score
WHERE r.tenant = 4`
	assertBenchmarkPlanContains(t, ctx, conn, rightConstant, "l.tenant = 4")
	assertCountResult(t, ctx, conn, rightConstant, btreeJoinProbeRows/8*(btreeBenchmarkRows/64))

	conflicting := `SELECT count(*)
FROM btree_join_left_idx AS l
JOIN btree_join_right_idx AS r
  ON r.tenant = l.tenant
 AND r.score = l.score
WHERE l.tenant = 4
  AND r.tenant = 5`
	assertBenchmarkPlanContains(t, ctx, conn, conflicting, "keys: 5, l.score")
	assertBenchmarkPlanNotContains(t, ctx, conn, conflicting, "r.tenant = 4")
	assertCountResult(t, ctx, conn, conflicting, 0)

	leftJoin := `SELECT count(*)
FROM btree_join_left_idx AS l
LEFT JOIN btree_join_right_idx AS r
  ON r.tenant = l.tenant
 AND r.score = l.score
WHERE l.tenant = 4`
	assertBenchmarkPlanNotContains(t, ctx, conn, leftJoin, "r.tenant = 4")

	nullConstant := `SELECT count(*)
FROM btree_join_left_idx AS l
JOIN btree_join_right_idx AS r
  ON r.tenant = l.tenant
 AND r.score = l.score
WHERE l.tenant = NULL`
	assertBenchmarkPlanNotContains(t, ctx, conn, nullConstant, "r.tenant = NULL")
	assertCountResult(t, ctx, conn, nullConstant, 0)

	explicitLookupHint := `SELECT /*+ lookup_join(l, r) */ HINT count(*)
FROM btree_join_left_idx AS l
JOIN btree_join_right_idx AS r
  ON r.tenant = l.tenant
 AND r.score = l.score
WHERE l.tenant = 4`
	assertBenchmarkPlanContains(t, ctx, conn, explicitLookupHint, "LookupJoin")
	assertBenchmarkPlanContains(t, ctx, conn, explicitLookupHint, "IndexedTableAccess(btree_join_right_idx)")
	assertBenchmarkPlanNotContains(t, ctx, conn, explicitLookupHint, "HashJoin")
	assertCountResult(t, ctx, conn, explicitLookupHint, btreeJoinProbeRows/8*(btreeBenchmarkRows/64))
}

func TestBtreeJoinInfersRangePredicateForIndexedSide(t *testing.T) {
	ctx, conn, controller := CreateServer(t, "postgres")
	t.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		if err := controller.WaitForStop(); err != nil {
			t.Fatalf("error stopping test server: %v", err)
		}
	})

	setupBtreeJoinBenchmark(t, ctx, conn)

	queries := []string{
		`SELECT count(*)
FROM btree_join_left_idx AS l
JOIN btree_join_right_idx AS r
  ON r.tenant = l.tenant
 AND r.score = l.score
WHERE l.tenant = 4
  AND l.score >= 32`,
		`SELECT count(*)
FROM btree_join_left_idx AS l
JOIN btree_join_right_idx AS r
  ON r.tenant = l.tenant
 AND r.score = l.score
WHERE l.tenant = 4
  AND 32 <= l.score`,
	}
	for _, query := range queries {
		assertBenchmarkPlanContains(t, ctx, conn, query, "HashJoin")
		assertBenchmarkPlanContains(t, ctx, conn, query, "filters: [{[4, 4], [32,")
		assertBenchmarkPlanNotContains(t, ctx, conn, query, "LookupJoin")
		assertCountResult(t, ctx, conn, query, btreeJoinProbeRows/16*(btreeBenchmarkRows/64))
	}

	leftJoinRange := `SELECT count(*)
FROM btree_join_left_idx AS l
LEFT JOIN btree_join_right_idx AS r
  ON r.tenant = l.tenant
 AND r.score = l.score
WHERE l.tenant = 4
  AND l.score >= 32`
	assertBenchmarkPlanNotContains(t, ctx, conn, leftJoinRange, "filters: [{[4, 4], [32,")
}

func TestBtreeCoveredProjectionPlannerShape(t *testing.T) {
	ctx, conn, controller := CreateServer(t, "postgres")
	t.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		if err := controller.WaitForStop(); err != nil {
			t.Fatalf("error stopping test server: %v", err)
		}
	})

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE btree_covered_projection_plan (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, score INTEGER NOT NULL, label TEXT NOT NULL)")
	insertBtreePlanRows(t, ctx, conn, "btree_covered_projection_plan", 128)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX btree_covered_projection_plan_tenant_score_idx ON btree_covered_projection_plan (tenant, score)")

	query := `SELECT tenant, score FROM btree_covered_projection_plan WHERE tenant = 4 AND score >= 32`
	assertBenchmarkPlanShape(t, ctx, conn, query, true)
	assertBenchmarkPlanContains(t, ctx, conn, query, "columns: [tenant score]")
}

func TestBtreeAggregateProjectionPlannerShape(t *testing.T) {
	ctx, conn, controller := CreateServer(t, "postgres")
	t.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		if err := controller.WaitForStop(); err != nil {
			t.Fatalf("error stopping test server: %v", err)
		}
	})

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE btree_aggregate_projection_plan (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, score INTEGER NOT NULL, label TEXT NOT NULL)")
	insertBtreePlanRows(t, ctx, conn, "btree_aggregate_projection_plan", 128)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX btree_aggregate_projection_plan_tenant_score_idx ON btree_aggregate_projection_plan (tenant, score)")

	query := `SELECT count(*) FROM btree_aggregate_projection_plan WHERE tenant = 4 AND score >= 32`
	assertBenchmarkPlanShape(t, ctx, conn, query, true)
	assertBenchmarkPlanContains(t, ctx, conn, query, "columns: [tenant score]")
	assertBenchmarkPlanNotContains(t, ctx, conn, query, "label")
	assertCountResult(t, ctx, conn, query, 8)
}

func TestBtreeJoinProjectionPlannerShape(t *testing.T) {
	ctx, conn, controller := CreateServer(t, "postgres")
	t.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		if err := controller.WaitForStop(); err != nil {
			t.Fatalf("error stopping test server: %v", err)
		}
	})

	setupBtreeJoinBenchmark(t, ctx, conn)

	query := `SELECT count(*)
FROM btree_join_left_idx AS l
JOIN btree_join_right_idx AS r
  ON r.tenant = l.tenant
 AND r.score = l.score
WHERE l.tenant = 4`
	assertBenchmarkPlanContains(t, ctx, conn, query, "HashJoin")
	assertBenchmarkPlanContains(t, ctx, conn, query, "IndexedTableAccess(btree_join_right_idx)")
	assertBenchmarkPlanNotContains(t, ctx, conn, query, "label")
	assertCountResult(t, ctx, conn, query, btreeJoinProbeRows/8*(btreeBenchmarkRows/64))
}

func TestBtreePatternOpclassCoveredProjectionPlannerShape(t *testing.T) {
	ctx, conn, controller := CreateServer(t, "postgres")
	t.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		if err := controller.WaitForStop(); err != nil {
			t.Fatalf("error stopping test server: %v", err)
		}
	})

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE btree_pattern_covered_projection_plan (id INTEGER PRIMARY KEY, name TEXT NOT NULL, label TEXT NOT NULL)")
	execBenchmarkSQL(t, ctx, conn, "INSERT INTO btree_pattern_covered_projection_plan VALUES (1, 'alpha', 'a'), (2, 'alphabet', 'b'), (3, 'beta', 'c'), (4, 'alpaca', 'd')")
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX btree_pattern_covered_projection_plan_name_idx ON btree_pattern_covered_projection_plan (name text_pattern_ops)")

	query := `SELECT name FROM btree_pattern_covered_projection_plan WHERE name LIKE 'alph%'`
	assertBenchmarkPlanShape(t, ctx, conn, query, true)
	assertBenchmarkPlanContains(t, ctx, conn, query, "columns: [name]")
}

func TestBtreeCollationPlannerBoundary(t *testing.T) {
	ctx, conn, controller := CreateServer(t, "postgres")
	t.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		if err := controller.WaitForStop(); err != nil {
			t.Fatalf("error stopping test server: %v", err)
		}
	})

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE btree_collation_plan (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
	execBenchmarkSQL(t, ctx, conn, "INSERT INTO btree_collation_plan VALUES (1, 'Alpha'), (2, 'alpha'), (3, 'beta')")
	execBenchmarkSQL(t, ctx, conn, `CREATE INDEX btree_collation_plan_name_c_idx ON btree_collation_plan (name COLLATE "C")`)

	indexDef := queryBenchmarkString(t, ctx, conn, "SELECT indexdef FROM pg_catalog.pg_indexes WHERE indexname = 'btree_collation_plan_name_c_idx'")
	if !strings.Contains(indexDef, `COLLATE "C"`) {
		t.Fatalf(`expected pg_indexes to preserve COLLATE "C", got %q`, indexDef)
	}

	query := `SELECT count(id) FROM btree_collation_plan WHERE name >= 'A' AND name < 'b'`
	assertBenchmarkPlanShape(t, ctx, conn, query, false)
	assertCountResult(t, ctx, conn, query, 2)

	execBenchmarkSQL(t, ctx, conn, `CREATE INDEX btree_collation_plan_name_default_idx ON btree_collation_plan (name)`)
	query = `SELECT count(id) FROM btree_collation_plan WHERE name COLLATE "C" >= 'A' AND name COLLATE "C" < 'b'`
	assertBenchmarkPlanShape(t, ctx, conn, query, false)
	assertCountResult(t, ctx, conn, query, 2)
}

func BenchmarkBtreeSQLLookup(b *testing.B) {
	ctx, conn := newBenchmarkServer(b)
	setupBtreeLookupBenchmark(b, ctx, conn)

	queries := []struct {
		name        string
		query       string
		want        int64
		indexedPlan bool
	}{
		{
			name:  "table_scan/leading_column_equality",
			query: `SELECT count(id) FROM btree_bench_scan WHERE tenant = 4`,
			want:  btreeBenchmarkRows / 8,
		},
		{
			name:        "indexed/leading_column_equality",
			query:       `SELECT count(id) FROM btree_bench_idx WHERE tenant = 4`,
			want:        btreeBenchmarkRows / 8,
			indexedPlan: true,
		},
		{
			name:        "indexed/leading_column_range",
			query:       `SELECT count(id) FROM btree_bench_idx WHERE tenant >= 2 AND tenant <= 5`,
			want:        btreeBenchmarkRows / 2,
			indexedPlan: true,
		},
		{
			name:        "indexed/leading_column_in_list",
			query:       `SELECT count(id) FROM btree_bench_idx WHERE tenant IN (2, 4, 6)`,
			want:        3 * (btreeBenchmarkRows / 8),
			indexedPlan: true,
		},
		{
			name:        "indexed/duplicate_heavy_in_list",
			query:       `SELECT count(id) FROM btree_bench_idx WHERE tenant IN (4, 2, 4, 2, 4)`,
			want:        2 * (btreeBenchmarkRows / 8),
			indexedPlan: true,
		},
		{
			name:        "indexed/multi_column_prefix_equality",
			query:       `SELECT count(id) FROM btree_bench_idx WHERE tenant = 4 AND score = 36`,
			want:        btreeBenchmarkRows / 64,
			indexedPlan: true,
		},
		{
			name:        "indexed/multi_column_prefix_range",
			query:       `SELECT count(id) FROM btree_bench_idx WHERE tenant = 4 AND score >= 32`,
			want:        btreeBenchmarkRows / 16,
			indexedPlan: true,
		},
		{
			name:  "table_scan/suffix_without_prefix",
			query: `SELECT count(id) FROM btree_bench_idx WHERE score = 36`,
			want:  btreeBenchmarkRows / 64,
		},
	}

	for _, query := range queries {
		assertBenchmarkPlanShape(b, ctx, conn, query.query, query.indexedPlan)
	}
	for _, query := range queries {
		query := query
		b.Run(query.name, func(b *testing.B) {
			benchmarkCountQuery(b, ctx, conn, query.query, query.want)
		})
	}
}

func BenchmarkBtreeStatsBackedIndexChoice(b *testing.B) {
	ctx, conn := newBenchmarkServer(b)
	execBenchmarkSQL(b, ctx, conn, "CREATE TABLE btree_stats_choice_bench (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, score INTEGER NOT NULL, label TEXT NOT NULL)")
	insertBtreeStatsChoiceRows(b, ctx, conn, "btree_stats_choice_bench", btreeBenchmarkRows)
	execBenchmarkSQL(b, ctx, conn, "CREATE INDEX btree_stats_choice_bench_tenant_idx ON btree_stats_choice_bench (tenant)")
	execBenchmarkSQL(b, ctx, conn, "CREATE INDEX btree_stats_choice_bench_score_idx ON btree_stats_choice_bench (score)")
	execBenchmarkSQL(b, ctx, conn, "ANALYZE btree_stats_choice_bench")

	query := `SELECT count(id) FROM btree_stats_choice_bench WHERE tenant = 1 AND score = 777`
	assertBenchmarkPlanContains(b, ctx, conn, query, "index: [btree_stats_choice_bench.score]")
	b.Run("selective_secondary_index", func(b *testing.B) {
		benchmarkCountQuery(b, ctx, conn, query, 1)
	})
}

func BenchmarkBtreeProjectionPushdown(b *testing.B) {
	ctx, conn := newBenchmarkServer(b)
	setupBtreeLookupBenchmark(b, ctx, conn)
	setupBtreeJoinBenchmark(b, ctx, conn)

	rowQueries := []struct {
		name            string
		query           string
		wantRows        int
		planContains    []string
		planNotContains []string
	}{
		{
			name:            "indexed/projected_columns",
			query:           `SELECT tenant, score FROM btree_bench_idx WHERE tenant = 4 AND score >= 32`,
			wantRows:        btreeBenchmarkRows / 16,
			planContains:    []string{"IndexedTableAccess", "columns: [tenant score]"},
			planNotContains: []string{"label"},
		},
		{
			name:         "indexed/full_row",
			query:        `SELECT id, tenant, score, label FROM btree_bench_idx WHERE tenant = 4 AND score >= 32`,
			wantRows:     btreeBenchmarkRows / 16,
			planContains: []string{"IndexedTableAccess", "label"},
		},
		{
			name: "join/projected_columns",
			query: `SELECT l.tenant, r.score
FROM btree_join_left_idx AS l
JOIN btree_join_right_idx AS r
  ON r.tenant = l.tenant
 AND r.score = l.score
WHERE l.tenant = 4`,
			wantRows:        btreeJoinProbeRows / 8 * (btreeBenchmarkRows / 64),
			planContains:    []string{"HashJoin", "IndexedTableAccess(btree_join_right_idx)"},
			planNotContains: []string{"label"},
		},
	}
	for _, query := range rowQueries {
		for _, expected := range query.planContains {
			assertBenchmarkPlanContains(b, ctx, conn, query.query, expected)
		}
		for _, unexpected := range query.planNotContains {
			assertBenchmarkPlanNotContains(b, ctx, conn, query.query, unexpected)
		}
	}
	for _, query := range rowQueries {
		query := query
		b.Run(query.name, func(b *testing.B) {
			benchmarkRowQuery(b, ctx, conn, query.query, query.wantRows)
		})
	}

	aggregate := `SELECT count(*) FROM btree_bench_idx WHERE tenant = 4 AND score >= 32`
	assertBenchmarkPlanContains(b, ctx, conn, aggregate, "IndexedTableAccess")
	assertBenchmarkPlanContains(b, ctx, conn, aggregate, "columns: [tenant score]")
	assertBenchmarkPlanNotContains(b, ctx, conn, aggregate, "label")
	b.Run("aggregate/filter_columns", func(b *testing.B) {
		benchmarkCountQuery(b, ctx, conn, aggregate, btreeBenchmarkRows/16)
	})
}

func BenchmarkBtreeSQLJoin(b *testing.B) {
	ctx, conn := newBenchmarkServer(b)
	setupBtreeJoinBenchmark(b, ctx, conn)

	queries := []struct {
		name            string
		query           string
		want            int64
		planContains    []string
		planNotContains []string
	}{
		{
			name: "table_scan/composite_join",
			query: `SELECT count(*)
FROM btree_join_left_scan
JOIN btree_join_right_scan
  ON btree_join_right_scan.tenant = btree_join_left_scan.tenant
 AND btree_join_right_scan.score = btree_join_left_scan.score
WHERE btree_join_left_scan.tenant = 4`,
			want: btreeJoinProbeRows / 8 * (btreeBenchmarkRows / 64),
		},
		{
			name: "indexed/inferred_composite_hash_join",
			query: `SELECT count(*)
FROM btree_join_left_idx
JOIN btree_join_right_idx
  ON btree_join_right_idx.tenant = btree_join_left_idx.tenant
 AND btree_join_right_idx.score = btree_join_left_idx.score
WHERE btree_join_left_idx.tenant = 4`,
			want:            btreeJoinProbeRows / 8 * (btreeBenchmarkRows / 64),
			planContains:    []string{"HashJoin", "IndexedTableAccess(btree_join_right_idx)"},
			planNotContains: []string{"LookupJoin"},
		},
		{
			name: "indexed/explicit_composite_lookup_join",
			query: `SELECT /*+ lookup_join(btree_join_left_idx, btree_join_right_idx) */ HINT count(*)
FROM btree_join_left_idx
JOIN btree_join_right_idx
  ON btree_join_right_idx.tenant = btree_join_left_idx.tenant
 AND btree_join_right_idx.score = btree_join_left_idx.score
WHERE btree_join_left_idx.tenant = 4`,
			want:            btreeJoinProbeRows / 8 * (btreeBenchmarkRows / 64),
			planContains:    []string{"LookupJoin", "IndexedTableAccess(btree_join_right_idx)"},
			planNotContains: []string{"HashJoin"},
		},
	}

	for _, query := range queries {
		for _, expected := range query.planContains {
			assertBenchmarkPlanContains(b, ctx, conn, query.query, expected)
		}
		for _, unexpected := range query.planNotContains {
			assertBenchmarkPlanNotContains(b, ctx, conn, query.query, unexpected)
		}
	}
	for _, query := range queries {
		query := query
		b.Run(query.name, func(b *testing.B) {
			benchmarkCountQuery(b, ctx, conn, query.query, query.want)
		})
	}
}

func BenchmarkBtreeIndexBuild(b *testing.B) {
	ctx, conn := newBenchmarkServer(b)
	execBenchmarkSQL(b, ctx, conn, "CREATE TABLE btree_bench_build (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, score INTEGER NOT NULL, label TEXT NOT NULL)")
	insertBtreePlanRows(b, ctx, conn, "btree_bench_build", btreeBenchmarkRows)

	b.Run("composite_backfill", func(b *testing.B) {
		benchmarkCreateDropBtreeIndex(b, ctx, conn, "btree_bench_build", "btree_bench_build_idx", "tenant, score")
	})
}

func BenchmarkBtreeDMLMaintenance(b *testing.B) {
	ctx, conn := newBenchmarkServer(b)

	b.Run("insert", func(b *testing.B) {
		createBtreeDMLBenchmarkTable(b, ctx, conn, "btree_bench_dml_insert")
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			insertBtreeDMLBenchmarkRow(b, ctx, conn, "btree_bench_dml_insert", i+1)
		}
	})

	b.Run("update", func(b *testing.B) {
		createBtreeDMLBenchmarkTable(b, ctx, conn, "btree_bench_dml_update")
		insertBtreeDMLBenchmarkRow(b, ctx, conn, "btree_bench_dml_update", 1)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			execBenchmarkSQL(b, ctx, conn,
				"UPDATE btree_bench_dml_update SET tenant = $1, score = $2 WHERE id = 1",
				(i+2)%8, (i+2)%64)
		}
	})

	b.Run("delete", func(b *testing.B) {
		createBtreeDMLBenchmarkTable(b, ctx, conn, "btree_bench_dml_delete")
		insertBtreeDMLBenchmarkRow(b, ctx, conn, "btree_bench_dml_delete", 1)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			execBenchmarkSQL(b, ctx, conn, "DELETE FROM btree_bench_dml_delete WHERE id = 1")
			b.StopTimer()
			insertBtreeDMLBenchmarkRow(b, ctx, conn, "btree_bench_dml_delete", i+2)
			b.StartTimer()
		}
	})
}

func BenchmarkJsonbGinSQLLookup(b *testing.B) {
	ctx, conn := newBenchmarkServer(b)
	setupJsonbGinLookupBenchmark(b, ctx, conn)

	queries := []struct {
		name        string
		query       string
		want        int64
		indexedPlan bool
	}{
		{
			name:  "table_scan/selective_containment",
			query: `SELECT count(*) FROM jsonb_gin_bench_scan WHERE doc @> '{"tenant":8,"status":"open"}'`,
			want:  32,
		},
		{
			name:  "table_scan/broad_containment",
			query: `SELECT count(*) FROM jsonb_gin_bench_scan WHERE doc @> '{"status":"open"}'`,
			want:  256,
		},
		{
			name:  "table_scan/key_exists",
			query: `SELECT count(*) FROM jsonb_gin_bench_scan WHERE doc ? 'vip'`,
			want:  102,
		},
		{
			name:  "table_scan/key_exists_any",
			query: `SELECT count(*) FROM jsonb_gin_bench_scan WHERE doc ?| ARRAY['vip','archived']`,
			want:  136,
		},
		{
			name:  "table_scan/key_exists_all",
			query: `SELECT count(*) FROM jsonb_gin_bench_scan WHERE doc ?& ARRAY['tenant','vip']`,
			want:  102,
		},
		{
			name:        "jsonb_ops/selective_containment",
			query:       `SELECT count(id) FROM jsonb_gin_bench_ops WHERE doc @> '{"tenant":8,"status":"open"}'`,
			want:        32,
			indexedPlan: true,
		},
		{
			name:        "jsonb_ops/broad_containment",
			query:       `SELECT count(id) FROM jsonb_gin_bench_ops WHERE doc @> '{"status":"open"}'`,
			want:        256,
			indexedPlan: true,
		},
		{
			name:        "jsonb_ops/key_exists",
			query:       `SELECT count(id) FROM jsonb_gin_bench_ops WHERE doc ? 'vip'`,
			want:        102,
			indexedPlan: true,
		},
		{
			name:        "jsonb_ops/key_exists_any",
			query:       `SELECT count(id) FROM jsonb_gin_bench_ops WHERE doc ?| ARRAY['vip','archived']`,
			want:        136,
			indexedPlan: true,
		},
		{
			name:  "jsonb_ops/key_exists_all",
			query: `SELECT count(id) FROM jsonb_gin_bench_ops WHERE doc ?& ARRAY['tenant','vip']`,
			want:  102,
		},
		{
			name:        "jsonb_path_ops/path_containment",
			query:       `SELECT count(id) FROM jsonb_gin_bench_path WHERE doc @> '{"payload":{"category":"cat-3"}}'`,
			want:        64,
			indexedPlan: true,
		},
	}

	for _, query := range queries {
		assertBenchmarkPlanShape(b, ctx, conn, query.query, query.indexedPlan)
	}
	for _, query := range queries {
		query := query
		b.Run(query.name, func(b *testing.B) {
			benchmarkCountQuery(b, ctx, conn, query.query, query.want)
		})
	}
}

func TestJsonbGinJsonPathBoundary(t *testing.T) {
	ctx, conn, controller := CreateServer(t, "postgres")
	t.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		if err := controller.WaitForStop(); err != nil {
			t.Fatalf("error stopping test server: %v", err)
		}
	})

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE jsonb_gin_jsonpath_boundary (id INTEGER PRIMARY KEY, doc JSONB NOT NULL)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO jsonb_gin_jsonpath_boundary VALUES
		(1, '{"a":2,"items":[{"v":1},{"v":2}]}'::jsonb),
		(2, '{"a":3,"items":[]}'::jsonb),
		(3, '{"b":1}'::jsonb)`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX jsonb_gin_jsonpath_boundary_idx ON jsonb_gin_jsonpath_boundary USING gin (doc)")

	for _, query := range []string{
		`SELECT count(id) FROM jsonb_gin_jsonpath_boundary WHERE doc @? '$.items[*].v'`,
		`SELECT count(id) FROM jsonb_gin_jsonpath_boundary WHERE doc @@ '$.a == 2'`,
		`SELECT count(id) FROM jsonb_gin_jsonpath_boundary WHERE jsonb_path_exists(doc, '$.items[*].v')`,
	} {
		plan := explainBenchmarkQuery(t, ctx, conn, query)
		if strings.Contains(plan, "IndexedTableAccess") {
			t.Fatalf("JSONPath boundary query unexpectedly used JSONB GIN index\nquery: %s\nplan:\n%s", query, plan)
		}
		assertCountResult(t, ctx, conn, query, 1)
	}
}

func TestJsonbGinSelectivityPlannerShape(t *testing.T) {
	ctx, conn, controller := CreateServer(t, "postgres")
	t.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		if err := controller.WaitForStop(); err != nil {
			t.Fatalf("error stopping test server: %v", err)
		}
	})

	createJsonbGinBenchmarkTable(t, ctx, conn, "jsonb_gin_selectivity_plan")
	insertJsonbGinBenchmarkRows(t, ctx, conn, "jsonb_gin_selectivity_plan", 1, jsonbGinBenchmarkRows)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX jsonb_gin_selectivity_plan_idx ON jsonb_gin_selectivity_plan USING gin (doc)")

	queries := []struct {
		name        string
		query       string
		want        int64
		indexedPlan bool
	}{
		{
			name:        "selective_containment_uses_gin",
			query:       `SELECT count(id) FROM jsonb_gin_selectivity_plan WHERE doc @> '{"tenant":8,"status":"open"}'`,
			want:        32,
			indexedPlan: true,
		},
		{
			name:        "broad_containment_uses_gin_when_combined_candidate_set_is_bounded",
			query:       `SELECT count(id) FROM jsonb_gin_selectivity_plan WHERE doc @> '{"status":"open"}'`,
			want:        256,
			indexedPlan: true,
		},
		{
			name:        "selective_key_exists_uses_gin",
			query:       `SELECT count(id) FROM jsonb_gin_selectivity_plan WHERE doc ? 'vip'`,
			want:        102,
			indexedPlan: true,
		},
		{
			name:  "broad_key_exists_scans",
			query: `SELECT count(id) FROM jsonb_gin_selectivity_plan WHERE doc ? 'tenant'`,
			want:  jsonbGinBenchmarkRows,
		},
		{
			name:        "broad_key_exists_all_uses_gin_when_intersection_is_bounded",
			query:       `SELECT count(id) FROM jsonb_gin_selectivity_plan WHERE doc ?& ARRAY['tenant','vip']`,
			want:        102,
			indexedPlan: true,
		},
	}

	for _, query := range queries {
		query := query
		t.Run(query.name, func(t *testing.T) {
			assertBenchmarkPlanShape(t, ctx, conn, query.query, query.indexedPlan)
			assertCountResult(t, ctx, conn, query.query, query.want)
		})
	}
}

func TestJsonbGinChunkMetadataPlannerShape(t *testing.T) {
	ctx, conn, controller := CreateServer(t, "postgres")
	t.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		if err := controller.WaitForStop(); err != nil {
			t.Fatalf("error stopping test server: %v", err)
		}
	})

	createJsonbGinBenchmarkTable(t, ctx, conn, "jsonb_gin_metadata_plan")
	insertJsonbGinBenchmarkRows(t, ctx, conn, "jsonb_gin_metadata_plan", 1, jsonbGinBenchmarkRows)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX jsonb_gin_metadata_plan_idx ON jsonb_gin_metadata_plan USING gin (doc)")

	createJsonbGinBenchmarkTable(t, ctx, conn, "jsonb_gin_metadata_path_plan")
	insertJsonbGinBenchmarkRows(t, ctx, conn, "jsonb_gin_metadata_path_plan", 1, jsonbGinBenchmarkRows)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX jsonb_gin_metadata_path_plan_idx ON jsonb_gin_metadata_path_plan USING gin (doc jsonb_path_ops)")

	queries := []struct {
		name        string
		query       string
		want        int64
		indexedPlan bool
	}{
		{
			name:        "selective_containment_uses_gin",
			query:       `SELECT count(id) FROM jsonb_gin_metadata_plan WHERE doc @> '{"tenant":8,"status":"open"}'`,
			want:        32,
			indexedPlan: true,
		},
		{
			name:        "broad_containment_uses_gin_when_candidate_set_is_bounded",
			query:       `SELECT count(id) FROM jsonb_gin_metadata_plan WHERE doc @> '{"status":"open"}'`,
			want:        256,
			indexedPlan: true,
		},
		{
			name:        "selective_key_exists_uses_gin",
			query:       `SELECT count(id) FROM jsonb_gin_metadata_plan WHERE doc ? 'vip'`,
			want:        102,
			indexedPlan: true,
		},
		{
			name:  "broad_key_exists_scans",
			query: `SELECT count(id) FROM jsonb_gin_metadata_plan WHERE doc ? 'tenant'`,
			want:  jsonbGinBenchmarkRows,
		},
		{
			name:  "broad_key_exists_any_scans",
			query: `SELECT count(id) FROM jsonb_gin_metadata_plan WHERE doc ?| ARRAY['tenant','missing']`,
			want:  jsonbGinBenchmarkRows,
		},
		{
			name:        "broad_key_exists_all_uses_gin_when_intersection_is_bounded",
			query:       `SELECT count(id) FROM jsonb_gin_metadata_plan WHERE doc ?& ARRAY['tenant','vip']`,
			want:        102,
			indexedPlan: true,
		},
		{
			name:        "jsonb_path_ops_containment_uses_gin",
			query:       `SELECT count(id) FROM jsonb_gin_metadata_path_plan WHERE doc @> '{"payload":{"category":"cat-3"}}'`,
			want:        64,
			indexedPlan: true,
		},
	}

	for _, query := range queries {
		query := query
		t.Run(query.name, func(t *testing.T) {
			assertBenchmarkPlanShape(t, ctx, conn, query.query, query.indexedPlan)
			assertCountResult(t, ctx, conn, query.query, query.want)
		})
	}

	postDMLHotKey := `SELECT count(id) FROM jsonb_gin_metadata_plan WHERE doc ? 'post_dml_hot'`
	assertBenchmarkPlanShape(t, ctx, conn, postDMLHotKey, true)
	assertCountResult(t, ctx, conn, postDMLHotKey, 0)
	insertJsonbGinConstantKeyRows(t, ctx, conn, "jsonb_gin_metadata_plan", jsonbGinBenchmarkRows+1, 129, "post_dml_hot")
	assertBenchmarkPlanShape(t, ctx, conn, postDMLHotKey, false)
	assertCountResult(t, ctx, conn, postDMLHotKey, 129)
}

func TestJsonbGinDirectFetchPreservesRecheck(t *testing.T) {
	ctx, conn, controller := CreateServer(t, "postgres")
	t.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		if err := controller.WaitForStop(); err != nil {
			t.Fatalf("error stopping test server: %v", err)
		}
	})

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE jsonb_gin_recheck_plan (id INTEGER PRIMARY KEY, doc JSONB NOT NULL)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO jsonb_gin_recheck_plan VALUES
		(1, '{"status":"open","payload":{"note":"closed"}}'::jsonb),
		(2, '{"status":"closed","payload":{"note":"open"}}'::jsonb),
		(3, '{"payload":{"status":"open"}}'::jsonb)`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX jsonb_gin_recheck_plan_idx ON jsonb_gin_recheck_plan USING gin (doc)")

	query := `SELECT count(id) FROM jsonb_gin_recheck_plan WHERE doc @> '{"status":"open"}'`
	assertBenchmarkPlanShape(t, ctx, conn, query, true)
	assertCountResult(t, ctx, conn, query, 1)
}

func TestJsonbGinIndexedProjectionPlannerShape(t *testing.T) {
	ctx, conn, controller := CreateServer(t, "postgres")
	t.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		if err := controller.WaitForStop(); err != nil {
			t.Fatalf("error stopping test server: %v", err)
		}
	})

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE jsonb_gin_projection_plan (id INTEGER PRIMARY KEY, label TEXT NOT NULL, doc JSONB NOT NULL)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO jsonb_gin_projection_plan VALUES
		(1, 'keep', '{"vip":true,"tenant":1}'::jsonb),
		(2, 'drop', '{"tenant":2}'::jsonb)`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX jsonb_gin_projection_plan_idx ON jsonb_gin_projection_plan USING gin (doc)")

	query := `SELECT id FROM jsonb_gin_projection_plan WHERE doc ? 'vip'`
	assertBenchmarkPlanShape(t, ctx, conn, query, true)
	assertBenchmarkPlanContains(t, ctx, conn, query, "columns: [id doc]")
	assertBenchmarkPlanNotContains(t, ctx, conn, query, "columns: [id label doc]")
	assertCountResult(t, ctx, conn, `SELECT count(*) FROM (`+query+`) AS projected`, 1)
}

func TestJsonbGinLiteralCacheDoesNotLeakAcrossIndexRecreate(t *testing.T) {
	ctx, conn, controller := CreateServer(t, "postgres")
	t.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		if err := controller.WaitForStop(); err != nil {
			t.Fatalf("error stopping test server: %v", err)
		}
	})

	createJsonbGinBenchmarkTable(t, ctx, conn, "jsonb_gin_literal_cache_recreate")
	insertJsonbGinBenchmarkRows(t, ctx, conn, "jsonb_gin_literal_cache_recreate", 1, jsonbGinBenchmarkRows)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX jsonb_gin_literal_cache_recreate_idx ON jsonb_gin_literal_cache_recreate USING gin (doc)")

	query := `SELECT count(id) FROM jsonb_gin_literal_cache_recreate WHERE doc @> '{"payload":{"category":"cat-3"}}'`
	assertBenchmarkPlanShape(t, ctx, conn, query, true)
	assertCountResult(t, ctx, conn, query, 64)

	execBenchmarkSQL(t, ctx, conn, "DROP INDEX jsonb_gin_literal_cache_recreate_idx")
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX jsonb_gin_literal_cache_recreate_idx ON jsonb_gin_literal_cache_recreate USING gin (doc jsonb_path_ops)")

	assertBenchmarkPlanShape(t, ctx, conn, query, true)
	assertCountResult(t, ctx, conn, query, 64)
}

func TestJsonbGinDMLRollbackPreservesPostingIndex(t *testing.T) {
	ctx, conn, controller := CreateServer(t, "postgres")
	t.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		if err := controller.WaitForStop(); err != nil {
			t.Fatalf("error stopping test server: %v", err)
		}
	})

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE jsonb_gin_dml_rollback (id INTEGER PRIMARY KEY, doc JSONB NOT NULL)")
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX jsonb_gin_dml_rollback_doc_idx ON jsonb_gin_dml_rollback USING gin (doc)")
	execBenchmarkSQL(t, ctx, conn, "BEGIN")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO jsonb_gin_dml_rollback VALUES
		(1, '{"vip":true,"tenant":1}'::jsonb),
		(2, '{"vip":true,"tenant":2}'::jsonb)`)
	execBenchmarkSQL(t, ctx, conn, "ROLLBACK")

	assertCountResult(t, ctx, conn, `SELECT count(*) FROM jsonb_gin_dml_rollback WHERE doc ? 'vip'`, 0)
	assertCountResult(t, ctx, conn, `SELECT count(*) FROM dg_gin_jsonb_gin_dml_rollback_jsonb_gin_dml_rollback_doc_idx_posting_chunks`, 0)

	execBenchmarkSQL(t, ctx, conn, `INSERT INTO jsonb_gin_dml_rollback VALUES
		(1, '{"vip":true,"tenant":1}'::jsonb),
		(2, '{"tenant":2}'::jsonb)`)
	assertBenchmarkPlanShape(t, ctx, conn, `SELECT count(*) FROM jsonb_gin_dml_rollback WHERE doc ? 'vip'`, true)
	assertCountResult(t, ctx, conn, `SELECT count(*) FROM jsonb_gin_dml_rollback WHERE doc ? 'vip'`, 1)
}

func BenchmarkJsonbGinIndexBuild(b *testing.B) {
	ctx, conn := newBenchmarkServer(b)
	createJsonbGinBenchmarkTable(b, ctx, conn, "jsonb_gin_bench_build")
	insertJsonbGinBenchmarkRows(b, ctx, conn, "jsonb_gin_bench_build", 1, jsonbGinBenchmarkRows)

	b.Run("jsonb_ops_backfill", func(b *testing.B) {
		benchmarkCreateDropJsonbGinIndex(b, ctx, conn, "jsonb_gin_bench_build", "jsonb_gin_bench_build_idx", "doc")
	})
	b.Run("jsonb_path_ops_backfill", func(b *testing.B) {
		benchmarkCreateDropJsonbGinIndex(b, ctx, conn, "jsonb_gin_bench_build", "jsonb_gin_bench_build_idx", "doc jsonb_path_ops")
	})
}

func BenchmarkJsonbGinDMLMaintenance(b *testing.B) {
	ctx, conn := newBenchmarkServer(b)

	b.Run("insert", func(b *testing.B) {
		createJsonbGinDMLBenchmarkTable(b, ctx, conn, "jsonb_gin_bench_dml_insert")
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			execBenchmarkSQL(b, ctx, conn,
				"INSERT INTO jsonb_gin_bench_dml_insert VALUES ($1, $2::jsonb)",
				i+1, benchmarkJsonbDocument(i+1))
		}
	})

	b.Run("update", func(b *testing.B) {
		createJsonbGinDMLBenchmarkTable(b, ctx, conn, "jsonb_gin_bench_dml_update")
		execBenchmarkSQL(b, ctx, conn,
			"INSERT INTO jsonb_gin_bench_dml_update VALUES ($1, $2::jsonb)",
			1, benchmarkJsonbDocument(1))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			execBenchmarkSQL(b, ctx, conn,
				"UPDATE jsonb_gin_bench_dml_update SET doc = $1::jsonb WHERE id = 1",
				benchmarkJsonbDocument(i+2))
		}
	})

	b.Run("delete", func(b *testing.B) {
		createJsonbGinDMLBenchmarkTable(b, ctx, conn, "jsonb_gin_bench_dml_delete")
		execBenchmarkSQL(b, ctx, conn,
			"INSERT INTO jsonb_gin_bench_dml_delete VALUES ($1, $2::jsonb)",
			1, benchmarkJsonbDocument(1))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			execBenchmarkSQL(b, ctx, conn, "DELETE FROM jsonb_gin_bench_dml_delete WHERE id = 1")
			b.StopTimer()
			execBenchmarkSQL(b, ctx, conn,
				"INSERT INTO jsonb_gin_bench_dml_delete VALUES ($1, $2::jsonb)",
				1, benchmarkJsonbDocument(i+2))
			b.StartTimer()
		}
	})
}

func newBenchmarkServer(b *testing.B) (context.Context, *Connection) {
	b.Helper()
	ctx, conn, controller := CreateServer(b, "postgres")
	b.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		if err := controller.WaitForStop(); err != nil {
			b.Fatalf("error stopping benchmark server: %v", err)
		}
	})
	return ctx, conn
}

func setupJsonbGinLookupBenchmark(b *testing.B, ctx context.Context, conn *Connection) {
	b.Helper()
	for _, table := range []string{"jsonb_gin_bench_scan", "jsonb_gin_bench_ops", "jsonb_gin_bench_path"} {
		createJsonbGinBenchmarkTable(b, ctx, conn, table)
		insertJsonbGinBenchmarkRows(b, ctx, conn, table, 1, jsonbGinBenchmarkRows)
	}
	execBenchmarkSQL(b, ctx, conn, "CREATE INDEX jsonb_gin_bench_ops_idx ON jsonb_gin_bench_ops USING gin (doc)")
	execBenchmarkSQL(b, ctx, conn, "CREATE INDEX jsonb_gin_bench_path_idx ON jsonb_gin_bench_path USING gin (doc jsonb_path_ops)")
}

func setupBtreeLookupBenchmark(tb testing.TB, ctx context.Context, conn *Connection) {
	tb.Helper()
	for _, table := range []string{"btree_bench_scan", "btree_bench_idx"} {
		execBenchmarkSQL(tb, ctx, conn, fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, score INTEGER NOT NULL, label TEXT NOT NULL)", table))
		insertBtreePlanRows(tb, ctx, conn, table, btreeBenchmarkRows)
	}
	execBenchmarkSQL(tb, ctx, conn, "CREATE INDEX btree_bench_idx_tenant_score_idx ON btree_bench_idx (tenant, score)")
}

func setupBtreeJoinBenchmark(tb testing.TB, ctx context.Context, conn *Connection) {
	tb.Helper()
	for _, suffix := range []string{"scan", "idx"} {
		leftTable := "btree_join_left_" + suffix
		rightTable := "btree_join_right_" + suffix
		execBenchmarkSQL(tb, ctx, conn, fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, score INTEGER NOT NULL, label TEXT NOT NULL)", leftTable))
		execBenchmarkSQL(tb, ctx, conn, fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, score INTEGER NOT NULL, label TEXT NOT NULL)", rightTable))
		insertBtreePlanRows(tb, ctx, conn, leftTable, btreeJoinProbeRows)
		insertBtreePlanRows(tb, ctx, conn, rightTable, btreeBenchmarkRows)
	}
	execBenchmarkSQL(tb, ctx, conn, "CREATE INDEX btree_join_right_idx_tenant_score_idx ON btree_join_right_idx (tenant, score)")
}

func createBtreeDMLBenchmarkTable(b *testing.B, ctx context.Context, conn *Connection, table string) {
	b.Helper()
	execBenchmarkSQL(b, ctx, conn, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
	execBenchmarkSQL(b, ctx, conn, fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, score INTEGER NOT NULL, label TEXT NOT NULL)", table))
	execBenchmarkSQL(b, ctx, conn, fmt.Sprintf("CREATE INDEX %s_idx ON %s (tenant, score)", table, table))
}

func insertBtreeDMLBenchmarkRow(b *testing.B, ctx context.Context, conn *Connection, table string, id int) {
	b.Helper()
	execBenchmarkSQL(b, ctx, conn,
		fmt.Sprintf("INSERT INTO %s VALUES ($1, $2, $3, $4)", table),
		id, id%8, id%64, fmt.Sprintf("label-%d", id%16))
}

func createJsonbGinDMLBenchmarkTable(b *testing.B, ctx context.Context, conn *Connection, table string) {
	b.Helper()
	execBenchmarkSQL(b, ctx, conn, fmt.Sprintf("DROP TABLE IF EXISTS dg_gin_%s_%s_idx_posting_chunks", table, table))
	execBenchmarkSQL(b, ctx, conn, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
	createJsonbGinBenchmarkTable(b, ctx, conn, table)
	execBenchmarkSQL(b, ctx, conn, fmt.Sprintf("CREATE INDEX %s_idx ON %s USING gin (doc)", table, table))
}

func createJsonbGinBenchmarkTable(tb testing.TB, ctx context.Context, conn *Connection, table string) {
	tb.Helper()
	execBenchmarkSQL(tb, ctx, conn, fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY, doc JSONB NOT NULL)", table))
}

func insertJsonbGinBenchmarkRows(tb testing.TB, ctx context.Context, conn *Connection, table string, firstID int, rowCount int) {
	tb.Helper()
	const chunkSize = 128
	for chunkStart := firstID; chunkStart < firstID+rowCount; chunkStart += chunkSize {
		chunkEnd := chunkStart + chunkSize
		if chunkEnd > firstID+rowCount {
			chunkEnd = firstID + rowCount
		}

		var query strings.Builder
		fmt.Fprintf(&query, "INSERT INTO %s VALUES ", table)
		for id := chunkStart; id < chunkEnd; id++ {
			if id > chunkStart {
				query.WriteString(", ")
			}
			fmt.Fprintf(&query, "(%d, '%s'::jsonb)", id, benchmarkJsonbDocument(id))
		}
		execBenchmarkSQL(tb, ctx, conn, query.String())
	}
}

func insertJsonbGinConstantKeyRows(tb testing.TB, ctx context.Context, conn *Connection, table string, firstID int, rowCount int, key string) {
	tb.Helper()
	const chunkSize = 128
	for chunkStart := firstID; chunkStart < firstID+rowCount; chunkStart += chunkSize {
		chunkEnd := chunkStart + chunkSize
		if chunkEnd > firstID+rowCount {
			chunkEnd = firstID + rowCount
		}

		var query strings.Builder
		fmt.Fprintf(&query, "INSERT INTO %s VALUES ", table)
		for id := chunkStart; id < chunkEnd; id++ {
			if id > chunkStart {
				query.WriteString(", ")
			}
			fmt.Fprintf(&query, "(%d, '{\"%s\":true,\"tenant\":1,\"payload\":{\"category\":\"dml\"}}'::jsonb)", id, key)
		}
		execBenchmarkSQL(tb, ctx, conn, query.String())
	}
}

func insertBtreePlanRows(tb testing.TB, ctx context.Context, conn *Connection, table string, rowCount int) {
	tb.Helper()
	const chunkSize = 64
	for chunkStart := 1; chunkStart <= rowCount; chunkStart += chunkSize {
		chunkEnd := chunkStart + chunkSize
		if chunkEnd > rowCount+1 {
			chunkEnd = rowCount + 1
		}

		var query strings.Builder
		fmt.Fprintf(&query, "INSERT INTO %s VALUES ", table)
		for id := chunkStart; id < chunkEnd; id++ {
			if id > chunkStart {
				query.WriteString(", ")
			}
			fmt.Fprintf(&query, "(%d, %d, %d, 'label-%d')", id, id%8, id%64, id%16)
		}
		execBenchmarkSQL(tb, ctx, conn, query.String())
	}
}

func insertBtreeStatsChoiceRows(tb testing.TB, ctx context.Context, conn *Connection, table string, rowCount int) {
	tb.Helper()
	const chunkSize = 64
	for chunkStart := 1; chunkStart <= rowCount; chunkStart += chunkSize {
		chunkEnd := chunkStart + chunkSize
		if chunkEnd > rowCount+1 {
			chunkEnd = rowCount + 1
		}

		var query strings.Builder
		fmt.Fprintf(&query, "INSERT INTO %s VALUES ", table)
		for id := chunkStart; id < chunkEnd; id++ {
			if id > chunkStart {
				query.WriteString(", ")
			}
			fmt.Fprintf(&query, "(%d, %d, %d, 'label-%d')", id, id%4, id, id%16)
		}
		execBenchmarkSQL(tb, ctx, conn, query.String())
	}
}

func insertBtreeConstantScoreRows(tb testing.TB, ctx context.Context, conn *Connection, table string, firstID, rowCount, score int) {
	tb.Helper()
	const chunkSize = 64
	for chunkStart := firstID; chunkStart < firstID+rowCount; chunkStart += chunkSize {
		chunkEnd := chunkStart + chunkSize
		if chunkEnd > firstID+rowCount {
			chunkEnd = firstID + rowCount
		}

		var query strings.Builder
		fmt.Fprintf(&query, "INSERT INTO %s VALUES ", table)
		for id := chunkStart; id < chunkEnd; id++ {
			if id > chunkStart {
				query.WriteString(", ")
			}
			fmt.Fprintf(&query, "(%d, %d, %d, 'label-%d')", id, id%4, score, id%16)
		}
		execBenchmarkSQL(tb, ctx, conn, query.String())
	}
}

func insertExpressionBtreePlanRows(tb testing.TB, ctx context.Context, conn *Connection, table string, rowCount int) {
	tb.Helper()
	const chunkSize = 64
	for chunkStart := 1; chunkStart <= rowCount; chunkStart += chunkSize {
		chunkEnd := chunkStart + chunkSize
		if chunkEnd > rowCount+1 {
			chunkEnd = rowCount + 1
		}

		var query strings.Builder
		fmt.Fprintf(&query, "INSERT INTO %s VALUES ", table)
		for id := chunkStart; id < chunkEnd; id++ {
			if id > chunkStart {
				query.WriteString(", ")
			}
			fmt.Fprintf(&query, "(%d, 'Title-%d')", id, id%16)
		}
		execBenchmarkSQL(tb, ctx, conn, query.String())
	}
}

func benchmarkJsonbDocument(id int) string {
	status := "closed"
	if id%4 == 0 {
		status = "open"
	}

	var optionalKeys strings.Builder
	if id%10 == 0 {
		optionalKeys.WriteString(`,"vip":true`)
	}
	if id%15 == 0 {
		optionalKeys.WriteString(`,"archived":true`)
	}

	return fmt.Sprintf(
		`{"tenant":%d,"status":"%s","payload":{"category":"cat-%d","region":"r-%d","score":%d},"tags":["tag-%d","group-%d"]%s}`,
		id%32,
		status,
		id%16,
		id%8,
		id%100,
		id%64,
		id%7,
		optionalKeys.String(),
	)
}

func benchmarkCreateDropJsonbGinIndex(b *testing.B, ctx context.Context, conn *Connection, table string, indexName string, columnDef string) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		execBenchmarkSQL(b, ctx, conn, fmt.Sprintf("CREATE INDEX %s ON %s USING gin (%s)", indexName, table, columnDef))
		b.StopTimer()
		execBenchmarkSQL(b, ctx, conn, fmt.Sprintf("DROP INDEX %s", indexName))
		b.StartTimer()
	}
}

func benchmarkCreateDropBtreeIndex(b *testing.B, ctx context.Context, conn *Connection, table string, indexName string, columnDef string) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		execBenchmarkSQL(b, ctx, conn, fmt.Sprintf("CREATE INDEX %s ON %s (%s)", indexName, table, columnDef))
		b.StopTimer()
		execBenchmarkSQL(b, ctx, conn, fmt.Sprintf("DROP INDEX %s", indexName))
		b.StartTimer()
	}
}

func benchmarkCountQuery(b *testing.B, ctx context.Context, conn *Connection, query string, want int64) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		assertCountResult(b, ctx, conn, query, want)
	}
}

func benchmarkRowQuery(b *testing.B, ctx context.Context, conn *Connection, query string, wantRows int) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := conn.Query(ctx, query)
		if err != nil {
			b.Fatalf("query failed: %v\nquery: %s", err, query)
		}
		count := 0
		for rows.Next() {
			if _, err = rows.Values(); err != nil {
				rows.Close()
				b.Fatalf("query values failed: %v\nquery: %s", err, query)
			}
			count++
		}
		rows.Close()
		if err = rows.Err(); err != nil {
			b.Fatalf("query rows failed: %v\nquery: %s", err, query)
		}
		if count != wantRows {
			b.Fatalf("query returned %d rows, expected %d\nquery: %s", count, wantRows, query)
		}
	}
}

func assertCountResult(tb testing.TB, ctx context.Context, conn *Connection, query string, want int64) {
	tb.Helper()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		tb.Fatalf("count query failed: %v\nquery: %s", err, query)
	}
	var got int64
	if !rows.Next() {
		rows.Close()
		tb.Fatalf("count query returned no rows: %s", query)
	}
	if err = rows.Scan(&got); err != nil {
		rows.Close()
		tb.Fatalf("count query scan failed: %v\nquery: %s", err, query)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		tb.Fatalf("count query rows failed: %v\nquery: %s", err, query)
	}
	if got != want {
		tb.Fatalf("count query returned %d, expected %d\nquery: %s", got, want, query)
	}
}

func queryBenchmarkString(tb testing.TB, ctx context.Context, conn *Connection, query string) string {
	tb.Helper()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		tb.Fatalf("query failed: %v\nquery: %s", err, query)
	}
	defer rows.Close()

	var got string
	if !rows.Next() {
		tb.Fatalf("query returned no rows: %s", query)
	}
	if err = rows.Scan(&got); err != nil {
		tb.Fatalf("query scan failed: %v\nquery: %s", err, query)
	}
	if rows.Next() {
		tb.Fatalf("query returned more than one row: %s", query)
	}
	if err = rows.Err(); err != nil {
		tb.Fatalf("query rows failed: %v\nquery: %s", err, query)
	}
	return got
}

func assertBenchmarkPlanShape(tb testing.TB, ctx context.Context, conn *Connection, query string, indexedPlan bool) {
	tb.Helper()
	plan := explainBenchmarkQuery(tb, ctx, conn, query)
	hasIndexedAccess := strings.Contains(plan, "IndexedTableAccess")
	if indexedPlan && !hasIndexedAccess {
		tb.Fatalf("expected benchmark query to use IndexedTableAccess\nplan:\n%s", plan)
	}
	if !indexedPlan && hasIndexedAccess {
		tb.Fatalf("expected benchmark query to use table-scan fallback\nplan:\n%s", plan)
	}
}

func assertBenchmarkPlanContains(tb testing.TB, ctx context.Context, conn *Connection, query string, expected string) {
	tb.Helper()
	plan := explainBenchmarkQuery(tb, ctx, conn, query)
	if !strings.Contains(plan, expected) {
		tb.Fatalf("expected benchmark query plan to contain %q\nplan:\n%s", expected, plan)
	}
}

func assertBenchmarkPlanNotContains(tb testing.TB, ctx context.Context, conn *Connection, query string, unexpected string) {
	tb.Helper()
	plan := explainBenchmarkQuery(tb, ctx, conn, query)
	if strings.Contains(plan, unexpected) {
		tb.Fatalf("expected benchmark query plan not to contain %q\nplan:\n%s", unexpected, plan)
	}
}

func explainBenchmarkQuery(tb testing.TB, ctx context.Context, conn *Connection, query string) string {
	tb.Helper()
	rows, err := conn.Query(ctx, "EXPLAIN "+query)
	if err != nil {
		tb.Fatalf("EXPLAIN failed: %v\nquery: %s", err, query)
	}
	defer rows.Close()

	var lines []string
	for rows.Next() {
		var line string
		if err = rows.Scan(&line); err != nil {
			tb.Fatalf("EXPLAIN scan failed: %v\nquery: %s", err, query)
		}
		lines = append(lines, line)
	}
	if err = rows.Err(); err != nil {
		tb.Fatalf("EXPLAIN rows failed: %v\nquery: %s", err, query)
	}
	return strings.Join(lines, "\n")
}

func execBenchmarkSQL(tb testing.TB, ctx context.Context, conn *Connection, query string, args ...any) {
	tb.Helper()
	if _, err := conn.Exec(ctx, query, args...); err != nil {
		tb.Fatalf("benchmark SQL failed: %v\nquery: %s", err, query)
	}
}
