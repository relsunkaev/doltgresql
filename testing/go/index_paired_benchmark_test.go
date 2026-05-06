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
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

const pairedIndexBenchmarkIterations = 25
const jsonbGinPostingStorageVersionEnv = "DOLTGRES_JSONB_GIN_POSTING_STORAGE_VERSION"

type pairedBenchmarkConn interface {
	Exec(context.Context, string, ...any) (any, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

type pairedDoltgresConn struct {
	conn *Connection
}

func (c pairedDoltgresConn) Exec(ctx context.Context, query string, args ...any) (any, error) {
	return c.conn.Exec(ctx, query, args...)
}

func (c pairedDoltgresConn) Query(ctx context.Context, query string, args ...any) (pgx.Rows, error) {
	return c.conn.Query(ctx, query, args...)
}

type pairedPostgresConn struct {
	conn *pgx.Conn
}

func (c pairedPostgresConn) Exec(ctx context.Context, query string, args ...any) (any, error) {
	return c.conn.Exec(ctx, query, args...)
}

func (c pairedPostgresConn) Query(ctx context.Context, query string, args ...any) (pgx.Rows, error) {
	return c.conn.Query(ctx, query, args...)
}

type pairedBenchmarkCase struct {
	name                      string
	doltgresScanSQL           string
	doltgresIndexSQL          string
	doltgresV2SQL             string
	postgresSQL               string
	want                      int64
	planBoundary              bool
	doltgresV2DirectFetchLoss bool
}

func BenchmarkPairedIndexBaselines(b *testing.B) {
	postgresURL := os.Getenv("DOLTGRES_POSTGRES_BASELINE_URL")
	if postgresURL == "" {
		b.Skip("set DOLTGRES_POSTGRES_BASELINE_URL to run paired Doltgres/PostgreSQL index baselines")
	}

	ctx := context.Background()
	dgCtx, dgConn := newBenchmarkServer(b)
	pgConn, err := pgx.Connect(ctx, postgresURL)
	if err != nil {
		b.Fatalf("connect PostgreSQL baseline: %v", err)
	}
	b.Cleanup(func() {
		_ = pgConn.Close(context.Background())
	})

	dg := pairedDoltgresConn{conn: dgConn}
	pg := pairedPostgresConn{conn: pgConn}
	setupPairedBtreeBenchmark(b, dgCtx, dg, "dg_pair")
	setupPairedBtreeBenchmark(b, ctx, pg, "pg_pair")
	setupPairedJsonbGinBenchmark(b, dgCtx, dg, "dg_pair", true)
	setupPairedJsonbGinBenchmark(b, ctx, pg, "pg_pair", false)

	for _, benchCase := range pairedIndexBenchmarkCases() {
		benchCase := benchCase
		b.Run(benchCase.name, func(b *testing.B) {
			assertPairedCount(b, dgCtx, dg, benchCase.doltgresScanSQL, benchCase.want)
			assertPairedCount(b, dgCtx, dg, benchCase.doltgresIndexSQL, benchCase.want)
			if benchCase.doltgresV2SQL != "" {
				assertPairedCount(b, dgCtx, dg, benchCase.doltgresV2SQL, benchCase.want)
			}
			assertPairedCount(b, ctx, pg, benchCase.postgresSQL, benchCase.want)
			if !benchCase.planBoundary {
				assertBenchmarkPlanShape(b, dgCtx, dgConn, benchCase.doltgresIndexSQL, true)
				if benchCase.doltgresV2SQL != "" {
					assertBenchmarkPlanShape(b, dgCtx, dgConn, benchCase.doltgresV2SQL, true)
				}
			} else {
				assertBenchmarkPlanShape(b, dgCtx, dgConn, benchCase.doltgresIndexSQL, false)
				if benchCase.doltgresV2SQL != "" {
					assertBenchmarkPlanShape(b, dgCtx, dgConn, benchCase.doltgresV2SQL, false)
				}
			}
			pgPlan := explainPairedPostgres(b, ctx, pg, benchCase.postgresSQL)

			iterations := pairedBenchmarkIterationCount()
			dgScan := measurePairedCountQuery(b, dgCtx, dg, benchCase.doltgresScanSQL, benchCase.want, iterations)
			dgIndexed := measurePairedCountQuery(b, dgCtx, dg, benchCase.doltgresIndexSQL, benchCase.want, iterations)
			pgIndexed := measurePairedCountQuery(b, ctx, pg, benchCase.postgresSQL, benchCase.want, iterations)

			b.ReportMetric(float64(dgScan.Microseconds())/float64(iterations), "dg_scan_us/op")
			b.ReportMetric(float64(dgIndexed.Microseconds())/float64(iterations), "dg_index_us/op")
			b.ReportMetric(float64(pgIndexed.Microseconds())/float64(iterations), "pg_us/op")
			b.ReportMetric(ratio(dgIndexed, dgScan), "dg_index_vs_scan")
			b.ReportMetric(ratio(dgIndexed, pgIndexed), "dg_index_vs_pg")
			if benchCase.doltgresV2SQL == "" {
				b.Logf("paired-index-baseline name=%s iterations=%d dg_scan=%s dg_index=%s pg=%s dg_index_vs_scan=%.2fx dg_index_vs_pg=%.2fx pg_plan=%s",
					benchCase.name, iterations, dgScan, dgIndexed, pgIndexed, ratio(dgIndexed, dgScan), ratio(dgIndexed, pgIndexed), oneLinePlan(pgPlan))
				return
			}

			dgV2Indexed := measurePairedCountQuery(b, dgCtx, dg, benchCase.doltgresV2SQL, benchCase.want, iterations)
			b.ReportMetric(float64(dgIndexed.Microseconds())/float64(iterations), "dg_v1_index_us/op")
			b.ReportMetric(float64(dgV2Indexed.Microseconds())/float64(iterations), "dg_v2_index_us/op")
			b.ReportMetric(ratio(dgIndexed, dgScan), "dg_v1_index_vs_scan")
			b.ReportMetric(ratio(dgIndexed, pgIndexed), "dg_v1_index_vs_pg")
			b.ReportMetric(ratio(dgV2Indexed, dgScan), "dg_v2_index_vs_scan")
			b.ReportMetric(ratio(dgV2Indexed, pgIndexed), "dg_v2_index_vs_pg")
			b.ReportMetric(ratio(dgV2Indexed, dgIndexed), "dg_v2_vs_v1")
			b.Logf("paired-index-baseline name=%s iterations=%d dg_scan=%s dg_v1_index=%s dg_v2_index=%s pg=%s dg_v1_index_vs_scan=%.2fx dg_v2_index_vs_scan=%.2fx dg_v1_index_vs_pg=%.2fx dg_v2_index_vs_pg=%.2fx dg_v2_vs_v1=%.2fx dg_v2_direct_fetch=%t pg_plan=%s",
				benchCase.name, iterations, dgScan, dgIndexed, dgV2Indexed, pgIndexed, ratio(dgIndexed, dgScan), ratio(dgV2Indexed, dgScan), ratio(dgIndexed, pgIndexed), ratio(dgV2Indexed, pgIndexed), ratio(dgV2Indexed, dgIndexed), !benchCase.doltgresV2DirectFetchLoss, oneLinePlan(pgPlan))
		})
	}
	runPairedIndexBuildBenchmarks(b, dgCtx, dg, ctx, pg)
	runPairedIndexDMLBenchmarks(b, dgCtx, dg, ctx, pg)
	runPairedJsonbGinDMLBucketBenchmarks(b, dgCtx, dg, ctx, pg)
}

func pairedBenchmarkIterationCount() int {
	if raw := os.Getenv("DOLTGRES_PAIRED_INDEX_BENCH_ITERS"); raw != "" {
		var parsed int
		if _, err := fmt.Sscanf(raw, "%d", &parsed); err == nil && parsed > 0 {
			return parsed
		}
	}
	return pairedIndexBenchmarkIterations
}

func pairedIndexBenchmarkCases() []pairedBenchmarkCase {
	return []pairedBenchmarkCase{
		{
			name:             "btree/equality",
			doltgresScanSQL:  `SELECT count(id) FROM dg_pair_btree_scan WHERE tenant = 4`,
			doltgresIndexSQL: `SELECT count(id) FROM dg_pair_btree_idx WHERE tenant = 4`,
			postgresSQL:      `SELECT count(id) FROM pg_pair_btree_idx WHERE tenant = 4`,
			want:             btreeBenchmarkRows / 8,
		},
		{
			name:             "btree/range",
			doltgresScanSQL:  `SELECT count(id) FROM dg_pair_btree_scan WHERE tenant >= 2 AND tenant <= 5`,
			doltgresIndexSQL: `SELECT count(id) FROM dg_pair_btree_idx WHERE tenant >= 2 AND tenant <= 5`,
			postgresSQL:      `SELECT count(id) FROM pg_pair_btree_idx WHERE tenant >= 2 AND tenant <= 5`,
			want:             btreeBenchmarkRows / 2,
		},
		{
			name:             "btree/in_list",
			doltgresScanSQL:  `SELECT count(id) FROM dg_pair_btree_scan WHERE tenant IN (2, 4, 6)`,
			doltgresIndexSQL: `SELECT count(id) FROM dg_pair_btree_idx WHERE tenant IN (2, 4, 6)`,
			postgresSQL:      `SELECT count(id) FROM pg_pair_btree_idx WHERE tenant IN (2, 4, 6)`,
			want:             3 * (btreeBenchmarkRows / 8),
		},
		{
			name:             "btree/duplicate_heavy_in_list",
			doltgresScanSQL:  `SELECT count(id) FROM dg_pair_btree_scan WHERE tenant IN (4, 2, 4, 2, 4)`,
			doltgresIndexSQL: `SELECT count(id) FROM dg_pair_btree_idx WHERE tenant IN (4, 2, 4, 2, 4)`,
			postgresSQL:      `SELECT count(id) FROM pg_pair_btree_idx WHERE tenant IN (4, 2, 4, 2, 4)`,
			want:             2 * (btreeBenchmarkRows / 8),
		},
		{
			name:             "btree/covered_projection",
			doltgresScanSQL:  `SELECT count(*) FROM (SELECT tenant, score FROM dg_pair_btree_scan WHERE tenant = 4 AND score >= 32) covered`,
			doltgresIndexSQL: `SELECT count(*) FROM (SELECT tenant, score FROM dg_pair_btree_idx WHERE tenant = 4 AND score >= 32) covered`,
			postgresSQL:      `SELECT count(*) FROM (SELECT tenant, score FROM pg_pair_btree_idx WHERE tenant = 4 AND score >= 32) covered`,
			want:             btreeBenchmarkRows / 16,
		},
		{
			name:             "btree/stats_selective_index",
			doltgresScanSQL:  `SELECT count(id) FROM dg_pair_stats_scan WHERE tenant = 1 AND score = 777`,
			doltgresIndexSQL: `SELECT count(id) FROM dg_pair_stats_idx WHERE tenant = 1 AND score = 777`,
			postgresSQL:      `SELECT count(id) FROM pg_pair_stats_idx WHERE tenant = 1 AND score = 777`,
			want:             1,
		},
		{
			name:             "btree/join",
			doltgresScanSQL:  `SELECT count(*) FROM dg_pair_join_left_scan JOIN dg_pair_join_right_scan ON dg_pair_join_right_scan.tenant = dg_pair_join_left_scan.tenant AND dg_pair_join_right_scan.score = dg_pair_join_left_scan.score WHERE dg_pair_join_left_scan.tenant = 4`,
			doltgresIndexSQL: `SELECT count(*) FROM dg_pair_join_left_idx JOIN dg_pair_join_right_idx ON dg_pair_join_right_idx.tenant = dg_pair_join_left_idx.tenant AND dg_pair_join_right_idx.score = dg_pair_join_left_idx.score WHERE dg_pair_join_left_idx.tenant = 4`,
			postgresSQL:      `SELECT count(*) FROM pg_pair_join_left_idx JOIN pg_pair_join_right_idx ON pg_pair_join_right_idx.tenant = pg_pair_join_left_idx.tenant AND pg_pair_join_right_idx.score = pg_pair_join_left_idx.score WHERE pg_pair_join_left_idx.tenant = 4`,
			want:             btreeJoinProbeRows / 8 * (btreeBenchmarkRows / 64),
		},
		{
			name:             "jsonb_gin/selective_containment",
			doltgresScanSQL:  `SELECT count(*) FROM dg_pair_jsonb_scan WHERE doc @> '{"tenant":8,"status":"open"}'`,
			doltgresIndexSQL: `SELECT count(id) FROM dg_pair_jsonb_ops WHERE doc @> '{"tenant":8,"status":"open"}'`,
			doltgresV2SQL:    `SELECT count(id) FROM dg_pair_jsonb_ops_v2 WHERE doc @> '{"tenant":8,"status":"open"}'`,
			postgresSQL:      `SELECT count(id) FROM pg_pair_jsonb_ops WHERE doc @> '{"tenant":8,"status":"open"}'`,
			want:             32,
		},
		{
			name:             "jsonb_gin/broad_containment",
			doltgresScanSQL:  `SELECT count(*) FROM dg_pair_jsonb_scan WHERE doc @> '{"status":"open"}'`,
			doltgresIndexSQL: `SELECT count(id) FROM dg_pair_jsonb_ops WHERE doc @> '{"status":"open"}'`,
			doltgresV2SQL:    `SELECT count(id) FROM dg_pair_jsonb_ops_v2 WHERE doc @> '{"status":"open"}'`,
			postgresSQL:      `SELECT count(id) FROM pg_pair_jsonb_ops WHERE doc @> '{"status":"open"}'`,
			want:             256,
		},
		{
			name:             "jsonb_gin/key_exists",
			doltgresScanSQL:  `SELECT count(*) FROM dg_pair_jsonb_scan WHERE doc ? 'vip'`,
			doltgresIndexSQL: `SELECT count(id) FROM dg_pair_jsonb_ops WHERE doc ? 'vip'`,
			doltgresV2SQL:    `SELECT count(id) FROM dg_pair_jsonb_ops_v2 WHERE doc ? 'vip'`,
			postgresSQL:      `SELECT count(id) FROM pg_pair_jsonb_ops WHERE doc ? 'vip'`,
			want:             102,
		},
		{
			name:             "jsonb_gin/key_exists_any",
			doltgresScanSQL:  `SELECT count(*) FROM dg_pair_jsonb_scan WHERE doc ?| ARRAY['vip','archived']`,
			doltgresIndexSQL: `SELECT count(id) FROM dg_pair_jsonb_ops WHERE doc ?| ARRAY['vip','archived']`,
			doltgresV2SQL:    `SELECT count(id) FROM dg_pair_jsonb_ops_v2 WHERE doc ?| ARRAY['vip','archived']`,
			postgresSQL:      `SELECT count(id) FROM pg_pair_jsonb_ops WHERE doc ?| ARRAY['vip','archived']`,
			want:             136,
		},
		{
			name:             "jsonb_gin/key_exists_all",
			doltgresScanSQL:  `SELECT count(*) FROM dg_pair_jsonb_scan WHERE doc ?& ARRAY['tenant','vip']`,
			doltgresIndexSQL: `SELECT count(id) FROM dg_pair_jsonb_ops WHERE doc ?& ARRAY['tenant','vip']`,
			doltgresV2SQL:    `SELECT count(id) FROM dg_pair_jsonb_ops_v2 WHERE doc ?& ARRAY['tenant','vip']`,
			postgresSQL:      `SELECT count(id) FROM pg_pair_jsonb_ops WHERE doc ?& ARRAY['tenant','vip']`,
			want:             102,
			planBoundary:     true,
		},
		{
			name:             "jsonb_gin/path_containment",
			doltgresScanSQL:  `SELECT count(*) FROM dg_pair_jsonb_scan WHERE doc @> '{"payload":{"category":"cat-3"}}'`,
			doltgresIndexSQL: `SELECT count(id) FROM dg_pair_jsonb_path WHERE doc @> '{"payload":{"category":"cat-3"}}'`,
			doltgresV2SQL:    `SELECT count(id) FROM dg_pair_jsonb_path_v2 WHERE doc @> '{"payload":{"category":"cat-3"}}'`,
			postgresSQL:      `SELECT count(id) FROM pg_pair_jsonb_path WHERE doc @> '{"payload":{"category":"cat-3"}}'`,
			want:             64,
		},
		{
			name:             "jsonb_gin/skewed_rare_containment",
			doltgresScanSQL:  `SELECT count(*) FROM dg_pair_jsonb_skew_scan WHERE doc @> '{"payload":{"skew":"rare"}}'`,
			doltgresIndexSQL: `SELECT count(id) FROM dg_pair_jsonb_skew_ops WHERE doc @> '{"payload":{"skew":"rare"}}'`,
			doltgresV2SQL:    `SELECT count(id) FROM dg_pair_jsonb_skew_ops_v2 WHERE doc @> '{"payload":{"skew":"rare"}}'`,
			postgresSQL:      `SELECT count(id) FROM pg_pair_jsonb_skew_ops WHERE doc @> '{"payload":{"skew":"rare"}}'`,
			want:             32,
		},
		{
			name:             "jsonb_gin/skewed_hot_key_exists",
			doltgresScanSQL:  `SELECT count(*) FROM dg_pair_jsonb_skew_scan WHERE doc ? 'hot'`,
			doltgresIndexSQL: `SELECT count(id) FROM dg_pair_jsonb_skew_ops WHERE doc ? 'hot'`,
			doltgresV2SQL:    `SELECT count(id) FROM dg_pair_jsonb_skew_ops_v2 WHERE doc ? 'hot'`,
			postgresSQL:      `SELECT count(id) FROM pg_pair_jsonb_skew_ops WHERE doc ? 'hot'`,
			want:             896,
			planBoundary:     true,
		},
		{
			name:                      "jsonb_gin/fallback_numeric_pk_containment",
			doltgresScanSQL:           `SELECT count(*) FROM dg_pair_jsonb_numeric_scan WHERE doc @> '{"tenant":8,"status":"open"}'`,
			doltgresIndexSQL:          `SELECT count(id) FROM dg_pair_jsonb_numeric_ops WHERE doc @> '{"tenant":8,"status":"open"}'`,
			doltgresV2SQL:             `SELECT count(id) FROM dg_pair_jsonb_numeric_ops_v2 WHERE doc @> '{"tenant":8,"status":"open"}'`,
			postgresSQL:               `SELECT count(id) FROM pg_pair_jsonb_numeric_ops WHERE doc @> '{"tenant":8,"status":"open"}'`,
			want:                      32,
			doltgresV2DirectFetchLoss: true,
		},
		{
			name:             "boundary/btree_suffix_scan",
			doltgresScanSQL:  `SELECT count(id) FROM dg_pair_btree_scan WHERE score = 36`,
			doltgresIndexSQL: `SELECT count(id) FROM dg_pair_btree_idx WHERE score = 36`,
			postgresSQL:      `SELECT count(id) FROM pg_pair_btree_idx WHERE score = 36`,
			want:             btreeBenchmarkRows / 64,
			planBoundary:     true,
		},
	}
}

func setupPairedBtreeBenchmark(tb testing.TB, ctx context.Context, conn pairedBenchmarkConn, prefix string) {
	tb.Helper()
	for _, table := range []string{prefix + "_btree_scan", prefix + "_btree_idx"} {
		execPairedSQL(tb, ctx, conn, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
		execPairedSQL(tb, ctx, conn, fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, score INTEGER NOT NULL, label TEXT NOT NULL)", table))
		insertPairedBtreeRows(tb, ctx, conn, table, btreeBenchmarkRows)
	}
	execPairedSQL(tb, ctx, conn, fmt.Sprintf("CREATE INDEX %s_btree_idx_tenant_score_idx ON %s_btree_idx (tenant, score)", prefix, prefix))

	for _, suffix := range []string{"scan", "idx"} {
		leftTable := prefix + "_join_left_" + suffix
		rightTable := prefix + "_join_right_" + suffix
		execPairedSQL(tb, ctx, conn, fmt.Sprintf("DROP TABLE IF EXISTS %s", leftTable))
		execPairedSQL(tb, ctx, conn, fmt.Sprintf("DROP TABLE IF EXISTS %s", rightTable))
		execPairedSQL(tb, ctx, conn, fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, score INTEGER NOT NULL, label TEXT NOT NULL)", leftTable))
		execPairedSQL(tb, ctx, conn, fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, score INTEGER NOT NULL, label TEXT NOT NULL)", rightTable))
		insertPairedBtreeRows(tb, ctx, conn, leftTable, btreeJoinProbeRows)
		insertPairedBtreeRows(tb, ctx, conn, rightTable, btreeBenchmarkRows)
	}
	execPairedSQL(tb, ctx, conn, fmt.Sprintf("CREATE INDEX %s_join_right_idx_tenant_score_idx ON %s_join_right_idx (tenant, score)", prefix, prefix))

	for _, table := range []string{prefix + "_stats_scan", prefix + "_stats_idx"} {
		execPairedSQL(tb, ctx, conn, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
		execPairedSQL(tb, ctx, conn, fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, score INTEGER NOT NULL, label TEXT NOT NULL)", table))
		insertPairedStatsChoiceRows(tb, ctx, conn, table, btreeBenchmarkRows)
	}
	execPairedSQL(tb, ctx, conn, fmt.Sprintf("CREATE INDEX %s_stats_idx_tenant_idx ON %s_stats_idx (tenant)", prefix, prefix))
	execPairedSQL(tb, ctx, conn, fmt.Sprintf("CREATE INDEX %s_stats_idx_score_idx ON %s_stats_idx (score)", prefix, prefix))
	execPairedSQL(tb, ctx, conn, fmt.Sprintf("ANALYZE %s_stats_idx", prefix))
}

func setupPairedJsonbGinBenchmark(tb testing.TB, ctx context.Context, conn pairedBenchmarkConn, prefix string, includeDoltgresV2 bool) {
	tb.Helper()
	for _, table := range []string{prefix + "_jsonb_scan", prefix + "_jsonb_ops", prefix + "_jsonb_path", prefix + "_jsonb_skew_scan", prefix + "_jsonb_skew_ops"} {
		execPairedSQL(tb, ctx, conn, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
		execPairedSQL(tb, ctx, conn, fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY, doc JSONB NOT NULL)", table))
		if strings.Contains(table, "_jsonb_skew_") {
			insertPairedSkewedJsonbRows(tb, ctx, conn, table, jsonbGinBenchmarkRows)
		} else {
			insertPairedJsonbRows(tb, ctx, conn, table, jsonbGinBenchmarkRows)
		}
	}
	for _, table := range []string{prefix + "_jsonb_numeric_scan", prefix + "_jsonb_numeric_ops"} {
		execPairedSQL(tb, ctx, conn, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
		execPairedSQL(tb, ctx, conn, fmt.Sprintf("CREATE TABLE %s (id NUMERIC PRIMARY KEY, doc JSONB NOT NULL)", table))
		insertPairedJsonbRows(tb, ctx, conn, table, jsonbGinBenchmarkRows)
	}
	if includeDoltgresV2 {
		for _, table := range []string{prefix + "_jsonb_ops_v2", prefix + "_jsonb_path_v2", prefix + "_jsonb_skew_ops_v2"} {
			execPairedSQL(tb, ctx, conn, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
			execPairedSQL(tb, ctx, conn, fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY, doc JSONB NOT NULL)", table))
			if strings.Contains(table, "_jsonb_skew_") {
				insertPairedSkewedJsonbRows(tb, ctx, conn, table, jsonbGinBenchmarkRows)
			} else {
				insertPairedJsonbRows(tb, ctx, conn, table, jsonbGinBenchmarkRows)
			}
		}
		execPairedSQL(tb, ctx, conn, fmt.Sprintf("DROP TABLE IF EXISTS %s_jsonb_numeric_ops_v2", prefix))
		execPairedSQL(tb, ctx, conn, fmt.Sprintf("CREATE TABLE %s_jsonb_numeric_ops_v2 (id NUMERIC PRIMARY KEY, doc JSONB NOT NULL)", prefix))
		insertPairedJsonbRows(tb, ctx, conn, prefix+"_jsonb_numeric_ops_v2", jsonbGinBenchmarkRows)
	}
	withJsonbGinPostingStorageVersion(tb, "1", func() {
		execPairedSQL(tb, ctx, conn, fmt.Sprintf("CREATE INDEX %s_jsonb_ops_idx ON %s_jsonb_ops USING gin (doc)", prefix, prefix))
		execPairedSQL(tb, ctx, conn, fmt.Sprintf("CREATE INDEX %s_jsonb_path_idx ON %s_jsonb_path USING gin (doc jsonb_path_ops)", prefix, prefix))
		execPairedSQL(tb, ctx, conn, fmt.Sprintf("CREATE INDEX %s_jsonb_skew_ops_idx ON %s_jsonb_skew_ops USING gin (doc)", prefix, prefix))
		execPairedSQL(tb, ctx, conn, fmt.Sprintf("CREATE INDEX %s_jsonb_numeric_ops_idx ON %s_jsonb_numeric_ops USING gin (doc)", prefix, prefix))
	})
	if includeDoltgresV2 {
		withJsonbGinPostingStorageVersion(tb, "2", func() {
			execPairedSQL(tb, ctx, conn, fmt.Sprintf("CREATE INDEX %s_jsonb_ops_v2_idx ON %s_jsonb_ops_v2 USING gin (doc)", prefix, prefix))
			execPairedSQL(tb, ctx, conn, fmt.Sprintf("CREATE INDEX %s_jsonb_path_v2_idx ON %s_jsonb_path_v2 USING gin (doc jsonb_path_ops)", prefix, prefix))
			execPairedSQL(tb, ctx, conn, fmt.Sprintf("CREATE INDEX %s_jsonb_skew_ops_v2_idx ON %s_jsonb_skew_ops_v2 USING gin (doc)", prefix, prefix))
			execPairedSQL(tb, ctx, conn, fmt.Sprintf("CREATE INDEX %s_jsonb_numeric_ops_v2_idx ON %s_jsonb_numeric_ops_v2 USING gin (doc)", prefix, prefix))
		})
	}
}

func runPairedIndexBuildBenchmarks(b *testing.B, dgCtx context.Context, dg pairedBenchmarkConn, pgCtx context.Context, pg pairedBenchmarkConn) {
	for _, target := range []struct {
		name       string
		table      string
		createSQL  string
		dropSQL    string
		jsonbGin   bool
		setupTable func(testing.TB, context.Context, pairedBenchmarkConn, string)
	}{
		{
			name:      "btree/build",
			table:     "pair_build_btree",
			createSQL: "CREATE INDEX %s_idx ON %s (tenant, score)",
			dropSQL:   "DROP INDEX %s_idx",
			setupTable: func(tb testing.TB, ctx context.Context, conn pairedBenchmarkConn, table string) {
				execPairedSQL(tb, ctx, conn, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
				execPairedSQL(tb, ctx, conn, fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, score INTEGER NOT NULL, label TEXT NOT NULL)", table))
				insertPairedBtreeRows(tb, ctx, conn, table, btreeBenchmarkRows)
			},
		},
		{
			name:      "jsonb_gin/build_jsonb_ops",
			table:     "pair_build_jsonb",
			createSQL: "CREATE INDEX %s_idx ON %s USING gin (doc)",
			dropSQL:   "DROP INDEX %s_idx",
			jsonbGin:  true,
			setupTable: func(tb testing.TB, ctx context.Context, conn pairedBenchmarkConn, table string) {
				execPairedSQL(tb, ctx, conn, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
				execPairedSQL(tb, ctx, conn, fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY, doc JSONB NOT NULL)", table))
				insertPairedJsonbRows(tb, ctx, conn, table, jsonbGinBenchmarkRows)
			},
		},
		{
			name:      "jsonb_gin/build_jsonb_path_ops",
			table:     "pair_build_jsonb_path",
			createSQL: "CREATE INDEX %s_idx ON %s USING gin (doc jsonb_path_ops)",
			dropSQL:   "DROP INDEX %s_idx",
			jsonbGin:  true,
			setupTable: func(tb testing.TB, ctx context.Context, conn pairedBenchmarkConn, table string) {
				execPairedSQL(tb, ctx, conn, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
				execPairedSQL(tb, ctx, conn, fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY, doc JSONB NOT NULL)", table))
				insertPairedJsonbRows(tb, ctx, conn, table, jsonbGinBenchmarkRows)
			},
		},
	} {
		target := target
		b.Run(target.name, func(b *testing.B) {
			dgTable := "dg_" + target.table
			pgTable := "pg_" + target.table
			target.setupTable(b, dgCtx, dg, dgTable)
			target.setupTable(b, pgCtx, pg, pgTable)
			iterations := pairedBenchmarkIterationCount()
			pgBuild := measurePairedIndexBuild(b, pgCtx, pg, fmt.Sprintf(target.createSQL, pgTable, pgTable), fmt.Sprintf(target.dropSQL, pgTable), iterations)
			if target.jsonbGin {
				dgV1Build := measurePairedJsonbGinIndexBuild(b, dgCtx, dg, "1", fmt.Sprintf(target.createSQL, dgTable, dgTable), fmt.Sprintf(target.dropSQL, dgTable), iterations)
				dgV2Build := measurePairedJsonbGinIndexBuild(b, dgCtx, dg, "2", fmt.Sprintf(target.createSQL, dgTable, dgTable), fmt.Sprintf(target.dropSQL, dgTable), iterations)
				b.ReportMetric(float64(dgV1Build.Microseconds())/float64(iterations), "dg_v1_index_us/op")
				b.ReportMetric(float64(dgV2Build.Microseconds())/float64(iterations), "dg_v2_index_us/op")
				b.ReportMetric(float64(pgBuild.Microseconds())/float64(iterations), "pg_us/op")
				b.ReportMetric(ratio(dgV1Build, pgBuild), "dg_v1_index_vs_pg")
				b.ReportMetric(ratio(dgV2Build, pgBuild), "dg_v2_index_vs_pg")
				b.ReportMetric(ratio(dgV2Build, dgV1Build), "dg_v2_vs_v1")
				b.Logf("paired-index-baseline name=%s iterations=%d dg_v1_index=%s dg_v2_index=%s pg=%s dg_v1_index_vs_pg=%.2fx dg_v2_index_vs_pg=%.2fx dg_v2_vs_v1=%.2fx",
					target.name, iterations, dgV1Build, dgV2Build, pgBuild, ratio(dgV1Build, pgBuild), ratio(dgV2Build, pgBuild), ratio(dgV2Build, dgV1Build))
				return
			}
			dgBuild := measurePairedIndexBuild(b, dgCtx, dg, fmt.Sprintf(target.createSQL, dgTable, dgTable), fmt.Sprintf(target.dropSQL, dgTable), iterations)
			b.ReportMetric(float64(dgBuild.Microseconds())/float64(iterations), "dg_index_us/op")
			b.ReportMetric(float64(pgBuild.Microseconds())/float64(iterations), "pg_us/op")
			b.ReportMetric(ratio(dgBuild, pgBuild), "dg_index_vs_pg")
			b.Logf("paired-index-baseline name=%s iterations=%d dg_index=%s pg=%s dg_index_vs_pg=%.2fx",
				target.name, iterations, dgBuild, pgBuild, ratio(dgBuild, pgBuild))
		})
	}
}

func measurePairedJsonbGinIndexBuild(tb testing.TB, ctx context.Context, conn pairedBenchmarkConn, storageVersion string, createSQL string, dropSQL string, iterations int) time.Duration {
	tb.Helper()
	var elapsed time.Duration
	withJsonbGinPostingStorageVersion(tb, storageVersion, func() {
		elapsed = measurePairedIndexBuild(tb, ctx, conn, createSQL, dropSQL, iterations)
	})
	return elapsed
}

func runPairedIndexDMLBenchmarks(b *testing.B, dgCtx context.Context, dg pairedBenchmarkConn, pgCtx context.Context, pg pairedBenchmarkConn) {
	for _, target := range []struct {
		name       string
		setupTable func(testing.TB, context.Context, pairedBenchmarkConn, string, bool)
		insertSQL  func(string) string
		updateSQL  func(string) string
		deleteSQL  func(string) string
		insertArgs func(int) []any
		updateArgs func(int) []any
	}{
		{
			name: "btree/dml",
			setupTable: func(tb testing.TB, ctx context.Context, conn pairedBenchmarkConn, table string, indexed bool) {
				execPairedSQL(tb, ctx, conn, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
				execPairedSQL(tb, ctx, conn, fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, score INTEGER NOT NULL, label TEXT NOT NULL)", table))
				if indexed {
					execPairedSQL(tb, ctx, conn, fmt.Sprintf("CREATE INDEX %s_idx ON %s (tenant, score)", table, table))
				}
			},
			insertSQL: func(table string) string { return fmt.Sprintf("INSERT INTO %s VALUES ($1, $2, $3, $4)", table) },
			updateSQL: func(table string) string {
				return fmt.Sprintf("UPDATE %s SET tenant = $1, score = $2 WHERE id = 1", table)
			},
			deleteSQL:  func(table string) string { return fmt.Sprintf("DELETE FROM %s WHERE id = 1", table) },
			insertArgs: func(id int) []any { return []any{id, id % 8, id % 64, fmt.Sprintf("label-%d", id%16)} },
			updateArgs: func(id int) []any { return []any{id % 8, id % 64} },
		},
		{
			name: "jsonb_gin/dml",
			setupTable: func(tb testing.TB, ctx context.Context, conn pairedBenchmarkConn, table string, indexed bool) {
				execPairedSQL(tb, ctx, conn, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
				execPairedSQL(tb, ctx, conn, fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY, doc JSONB NOT NULL)", table))
				if indexed {
					execPairedSQL(tb, ctx, conn, fmt.Sprintf("CREATE INDEX %s_idx ON %s USING gin (doc)", table, table))
				}
			},
			insertSQL:  func(table string) string { return fmt.Sprintf("INSERT INTO %s VALUES ($1, $2::jsonb)", table) },
			updateSQL:  func(table string) string { return fmt.Sprintf("UPDATE %s SET doc = $1::jsonb WHERE id = 1", table) },
			deleteSQL:  func(table string) string { return fmt.Sprintf("DELETE FROM %s WHERE id = 1", table) },
			insertArgs: func(id int) []any { return []any{id, benchmarkJsonbDocument(id)} },
			updateArgs: func(id int) []any { return []any{benchmarkJsonbDocument(id)} },
		},
	} {
		target := target
		b.Run(target.name, func(b *testing.B) {
			iterations := pairedBenchmarkIterationCount()
			dgScan := measurePairedDML(b, dgCtx, dg, "dg_"+strings.ReplaceAll(target.name, "/", "_")+"_scan", false, iterations, target)
			dgIndexed := measurePairedDML(b, dgCtx, dg, "dg_"+strings.ReplaceAll(target.name, "/", "_")+"_idx", true, iterations, target)
			pgIndexed := measurePairedDML(b, pgCtx, pg, "pg_"+strings.ReplaceAll(target.name, "/", "_")+"_idx", true, iterations, target)
			b.ReportMetric(float64(dgScan.Microseconds())/float64(iterations), "dg_scan_us/op")
			b.ReportMetric(float64(dgIndexed.Microseconds())/float64(iterations), "dg_index_us/op")
			b.ReportMetric(float64(pgIndexed.Microseconds())/float64(iterations), "pg_us/op")
			b.ReportMetric(ratio(dgIndexed, dgScan), "dg_index_vs_scan")
			b.ReportMetric(ratio(dgIndexed, pgIndexed), "dg_index_vs_pg")
			b.Logf("paired-index-baseline name=%s iterations=%d dg_scan=%s dg_index=%s pg=%s dg_index_vs_scan=%.2fx dg_index_vs_pg=%.2fx",
				target.name, iterations, dgScan, dgIndexed, pgIndexed, ratio(dgIndexed, dgScan), ratio(dgIndexed, pgIndexed))
		})
	}
}

func runPairedJsonbGinDMLBucketBenchmarks(b *testing.B, dgCtx context.Context, dg pairedBenchmarkConn, pgCtx context.Context, pg pairedBenchmarkConn) {
	for _, operation := range []string{"insert", "update", "delete"} {
		operation := operation
		b.Run("jsonb_gin/dml_"+operation, func(b *testing.B) {
			iterations := pairedBenchmarkIterationCount()
			dgScan := measurePairedJsonbGinDMLBucket(b, dgCtx, dg, "dg_jsonb_gin_dml_"+operation+"_scan", false, "1", operation, iterations)
			dgV1Indexed := measurePairedJsonbGinDMLBucket(b, dgCtx, dg, "dg_jsonb_gin_dml_"+operation+"_v1", true, "1", operation, iterations)
			dgV2Indexed := measurePairedJsonbGinDMLBucket(b, dgCtx, dg, "dg_jsonb_gin_dml_"+operation+"_v2", true, "2", operation, iterations)
			pgIndexed := measurePairedJsonbGinDMLBucket(b, pgCtx, pg, "pg_jsonb_gin_dml_"+operation+"_idx", true, "1", operation, iterations)
			b.ReportMetric(float64(dgScan.Microseconds())/float64(iterations), "dg_scan_us/op")
			b.ReportMetric(float64(dgV1Indexed.Microseconds())/float64(iterations), "dg_v1_index_us/op")
			b.ReportMetric(float64(dgV2Indexed.Microseconds())/float64(iterations), "dg_v2_index_us/op")
			b.ReportMetric(float64(pgIndexed.Microseconds())/float64(iterations), "pg_us/op")
			b.ReportMetric(ratio(dgV1Indexed, dgScan), "dg_v1_index_vs_scan")
			b.ReportMetric(ratio(dgV2Indexed, dgScan), "dg_v2_index_vs_scan")
			b.ReportMetric(ratio(dgV1Indexed, pgIndexed), "dg_v1_index_vs_pg")
			b.ReportMetric(ratio(dgV2Indexed, pgIndexed), "dg_v2_index_vs_pg")
			b.ReportMetric(ratio(dgV2Indexed, dgV1Indexed), "dg_v2_vs_v1")
			b.Logf("paired-index-baseline name=jsonb_gin/dml_%s iterations=%d dg_scan=%s dg_v1_index=%s dg_v2_index=%s pg=%s dg_v1_index_vs_scan=%.2fx dg_v2_index_vs_scan=%.2fx dg_v1_index_vs_pg=%.2fx dg_v2_index_vs_pg=%.2fx dg_v2_vs_v1=%.2fx",
				operation, iterations, dgScan, dgV1Indexed, dgV2Indexed, pgIndexed, ratio(dgV1Indexed, dgScan), ratio(dgV2Indexed, dgScan), ratio(dgV1Indexed, pgIndexed), ratio(dgV2Indexed, pgIndexed), ratio(dgV2Indexed, dgV1Indexed))
		})
	}
}

func measurePairedJsonbGinDMLBucket(tb testing.TB, ctx context.Context, conn pairedBenchmarkConn, table string, indexed bool, storageVersion string, operation string, iterations int) time.Duration {
	tb.Helper()
	setupPairedJsonbGinDMLTable(tb, ctx, conn, table, indexed, storageVersion)
	switch operation {
	case "insert":
		start := time.Now()
		for i := 0; i < iterations; i++ {
			execPairedSQL(tb, ctx, conn, fmt.Sprintf("INSERT INTO %s VALUES ($1, $2::jsonb)", table), i+1, benchmarkJsonbDocument(i+1))
		}
		return time.Since(start)
	case "update":
		execPairedSQL(tb, ctx, conn, fmt.Sprintf("INSERT INTO %s VALUES ($1, $2::jsonb)", table), 1, benchmarkJsonbDocument(1))
		start := time.Now()
		for i := 0; i < iterations; i++ {
			execPairedSQL(tb, ctx, conn, fmt.Sprintf("UPDATE %s SET doc = $1::jsonb WHERE id = 1", table), benchmarkJsonbDocument(i+2))
		}
		return time.Since(start)
	case "delete":
		execPairedSQL(tb, ctx, conn, fmt.Sprintf("INSERT INTO %s VALUES ($1, $2::jsonb)", table), 1, benchmarkJsonbDocument(1))
		var elapsed time.Duration
		for i := 0; i < iterations; i++ {
			start := time.Now()
			execPairedSQL(tb, ctx, conn, fmt.Sprintf("DELETE FROM %s WHERE id = 1", table))
			elapsed += time.Since(start)
			execPairedSQL(tb, ctx, conn, fmt.Sprintf("INSERT INTO %s VALUES ($1, $2::jsonb)", table), 1, benchmarkJsonbDocument(i+2))
		}
		return elapsed
	default:
		tb.Fatalf("unknown JSONB GIN DML operation: %s", operation)
		return 0
	}
}

func setupPairedJsonbGinDMLTable(tb testing.TB, ctx context.Context, conn pairedBenchmarkConn, table string, indexed bool, storageVersion string) {
	tb.Helper()
	execPairedSQL(tb, ctx, conn, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
	execPairedSQL(tb, ctx, conn, fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY, doc JSONB NOT NULL)", table))
	if !indexed {
		return
	}
	withJsonbGinPostingStorageVersion(tb, storageVersion, func() {
		execPairedSQL(tb, ctx, conn, fmt.Sprintf("CREATE INDEX %s_idx ON %s USING gin (doc)", table, table))
	})
}

func measurePairedIndexBuild(tb testing.TB, ctx context.Context, conn pairedBenchmarkConn, createSQL string, dropSQL string, iterations int) time.Duration {
	tb.Helper()
	start := time.Now()
	for i := 0; i < iterations; i++ {
		execPairedSQL(tb, ctx, conn, createSQL)
		execPairedSQL(tb, ctx, conn, dropSQL)
	}
	return time.Since(start)
}

func measurePairedDML(tb testing.TB, ctx context.Context, conn pairedBenchmarkConn, table string, indexed bool, iterations int, target struct {
	name       string
	setupTable func(testing.TB, context.Context, pairedBenchmarkConn, string, bool)
	insertSQL  func(string) string
	updateSQL  func(string) string
	deleteSQL  func(string) string
	insertArgs func(int) []any
	updateArgs func(int) []any
}) time.Duration {
	tb.Helper()
	target.setupTable(tb, ctx, conn, table, indexed)
	execPairedSQL(tb, ctx, conn, target.insertSQL(table), target.insertArgs(1)...)
	start := time.Now()
	for i := 0; i < iterations; i++ {
		execPairedSQL(tb, ctx, conn, target.updateSQL(table), target.updateArgs(i+2)...)
		execPairedSQL(tb, ctx, conn, target.deleteSQL(table))
		execPairedSQL(tb, ctx, conn, target.insertSQL(table), target.insertArgs(i+2)...)
	}
	return time.Since(start)
}

func insertPairedBtreeRows(tb testing.TB, ctx context.Context, conn pairedBenchmarkConn, table string, rowCount int) {
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
		execPairedSQL(tb, ctx, conn, query.String())
	}
}

func insertPairedStatsChoiceRows(tb testing.TB, ctx context.Context, conn pairedBenchmarkConn, table string, rowCount int) {
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
		execPairedSQL(tb, ctx, conn, query.String())
	}
}

func insertPairedJsonbRows(tb testing.TB, ctx context.Context, conn pairedBenchmarkConn, table string, rowCount int) {
	tb.Helper()
	const chunkSize = 128
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
			fmt.Fprintf(&query, "(%d, '%s'::jsonb)", id, benchmarkJsonbDocument(id))
		}
		execPairedSQL(tb, ctx, conn, query.String())
	}
}

func insertPairedSkewedJsonbRows(tb testing.TB, ctx context.Context, conn pairedBenchmarkConn, table string, rowCount int) {
	tb.Helper()
	const chunkSize = 128
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
			fmt.Fprintf(&query, "(%d, '%s'::jsonb)", id, benchmarkSkewedJsonbDocument(id))
		}
		execPairedSQL(tb, ctx, conn, query.String())
	}
}

func measurePairedCountQuery(tb testing.TB, ctx context.Context, conn pairedBenchmarkConn, query string, want int64, iterations int) time.Duration {
	tb.Helper()
	start := time.Now()
	for i := 0; i < iterations; i++ {
		assertPairedCount(tb, ctx, conn, query, want)
	}
	return time.Since(start)
}

func assertPairedCount(tb testing.TB, ctx context.Context, conn pairedBenchmarkConn, query string, want int64) {
	tb.Helper()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		tb.Fatalf("paired count query failed: %v\nquery: %s", err, query)
	}
	defer rows.Close()
	var got int64
	if !rows.Next() {
		tb.Fatalf("paired count query returned no rows: %s", query)
	}
	if err = rows.Scan(&got); err != nil {
		tb.Fatalf("paired count query scan failed: %v\nquery: %s", err, query)
	}
	if got != want {
		tb.Fatalf("paired count query returned %d, expected %d\nquery: %s", got, want, query)
	}
	if err = rows.Err(); err != nil {
		tb.Fatalf("paired count query rows failed: %v\nquery: %s", err, query)
	}
}

func explainPairedPostgres(tb testing.TB, ctx context.Context, conn pairedBenchmarkConn, query string) string {
	tb.Helper()
	rows, err := conn.Query(ctx, "EXPLAIN "+query)
	if err != nil {
		tb.Fatalf("paired EXPLAIN failed: %v\nquery: %s", err, query)
	}
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var line string
		if err = rows.Scan(&line); err != nil {
			tb.Fatalf("paired EXPLAIN scan failed: %v\nquery: %s", err, query)
		}
		lines = append(lines, line)
	}
	if err = rows.Err(); err != nil {
		tb.Fatalf("paired EXPLAIN rows failed: %v\nquery: %s", err, query)
	}
	return strings.Join(lines, "\n")
}

func execPairedSQL(tb testing.TB, ctx context.Context, conn pairedBenchmarkConn, query string, args ...any) {
	tb.Helper()
	if _, err := conn.Exec(ctx, query, args...); err != nil {
		tb.Fatalf("paired benchmark SQL failed: %v\nquery: %s", err, query)
	}
}

func benchmarkSkewedJsonbDocument(id int) string {
	skew := "common"
	if id%32 == 0 {
		skew = "rare"
	}
	hotKey := ""
	if id%8 != 0 {
		hotKey = `,"hot":true`
	}
	return fmt.Sprintf(
		`{"tenant":%d,"status":"active","payload":{"category":"cat-%d","region":"r-%d","skew":"%s","score":%d},"tags":["tag-%d","group-%d"]%s}`,
		id%16,
		id%8,
		id%4,
		skew,
		id%100,
		id%64,
		id%7,
		hotKey,
	)
}

func withJsonbGinPostingStorageVersion(tb testing.TB, version string, fn func()) {
	tb.Helper()
	previous, hadPrevious := os.LookupEnv(jsonbGinPostingStorageVersionEnv)
	if version == "" {
		_ = os.Unsetenv(jsonbGinPostingStorageVersionEnv)
	} else {
		_ = os.Setenv(jsonbGinPostingStorageVersionEnv, version)
	}
	defer func() {
		if hadPrevious {
			_ = os.Setenv(jsonbGinPostingStorageVersionEnv, previous)
		} else {
			_ = os.Unsetenv(jsonbGinPostingStorageVersionEnv)
		}
	}()
	fn()
}

func ratio(numerator time.Duration, denominator time.Duration) float64 {
	if denominator == 0 {
		return 0
	}
	return float64(numerator) / float64(denominator)
}

func oneLinePlan(plan string) string {
	return strings.Join(strings.Fields(plan), " ")
}
