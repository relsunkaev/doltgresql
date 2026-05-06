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
	name             string
	doltgresScanSQL  string
	doltgresIndexSQL string
	postgresSQL      string
	want             int64
	planBoundary     bool
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
	setupPairedJsonbGinBenchmark(b, dgCtx, dg, "dg_pair")
	setupPairedJsonbGinBenchmark(b, ctx, pg, "pg_pair")

	for _, benchCase := range pairedIndexBenchmarkCases() {
		benchCase := benchCase
		b.Run(benchCase.name, func(b *testing.B) {
			assertPairedCount(b, dgCtx, dg, benchCase.doltgresScanSQL, benchCase.want)
			assertPairedCount(b, dgCtx, dg, benchCase.doltgresIndexSQL, benchCase.want)
			assertPairedCount(b, ctx, pg, benchCase.postgresSQL, benchCase.want)
			if !benchCase.planBoundary {
				assertBenchmarkPlanShape(b, dgCtx, dgConn, benchCase.doltgresIndexSQL, true)
			} else {
				assertBenchmarkPlanShape(b, dgCtx, dgConn, benchCase.doltgresIndexSQL, false)
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
			b.Logf("paired-index-baseline name=%s iterations=%d dg_scan=%s dg_index=%s pg=%s dg_index_vs_scan=%.2fx dg_index_vs_pg=%.2fx pg_plan=%s",
				benchCase.name, iterations, dgScan, dgIndexed, pgIndexed, ratio(dgIndexed, dgScan), ratio(dgIndexed, pgIndexed), oneLinePlan(pgPlan))
		})
	}
	runPairedIndexBuildBenchmarks(b, dgCtx, dg, ctx, pg)
	runPairedIndexDMLBenchmarks(b, dgCtx, dg, ctx, pg)
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
			name:             "btree/covered_projection",
			doltgresScanSQL:  `SELECT count(*) FROM (SELECT tenant, score FROM dg_pair_btree_scan WHERE tenant = 4 AND score >= 32) covered`,
			doltgresIndexSQL: `SELECT count(*) FROM (SELECT tenant, score FROM dg_pair_btree_idx WHERE tenant = 4 AND score >= 32) covered`,
			postgresSQL:      `SELECT count(*) FROM (SELECT tenant, score FROM pg_pair_btree_idx WHERE tenant = 4 AND score >= 32) covered`,
			want:             btreeBenchmarkRows / 16,
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
			postgresSQL:      `SELECT count(id) FROM pg_pair_jsonb_ops WHERE doc @> '{"tenant":8,"status":"open"}'`,
			want:             32,
		},
		{
			name:             "jsonb_gin/broad_containment",
			doltgresScanSQL:  `SELECT count(*) FROM dg_pair_jsonb_scan WHERE doc @> '{"status":"open"}'`,
			doltgresIndexSQL: `SELECT count(id) FROM dg_pair_jsonb_ops WHERE doc @> '{"status":"open"}'`,
			postgresSQL:      `SELECT count(id) FROM pg_pair_jsonb_ops WHERE doc @> '{"status":"open"}'`,
			want:             256,
		},
		{
			name:             "jsonb_gin/key_exists",
			doltgresScanSQL:  `SELECT count(*) FROM dg_pair_jsonb_scan WHERE doc ? 'vip'`,
			doltgresIndexSQL: `SELECT count(id) FROM dg_pair_jsonb_ops WHERE doc ? 'vip'`,
			postgresSQL:      `SELECT count(id) FROM pg_pair_jsonb_ops WHERE doc ? 'vip'`,
			want:             102,
		},
		{
			name:             "jsonb_gin/key_exists_all",
			doltgresScanSQL:  `SELECT count(*) FROM dg_pair_jsonb_scan WHERE doc ?& ARRAY['tenant','vip']`,
			doltgresIndexSQL: `SELECT count(id) FROM dg_pair_jsonb_ops WHERE doc ?& ARRAY['tenant','vip']`,
			postgresSQL:      `SELECT count(id) FROM pg_pair_jsonb_ops WHERE doc ?& ARRAY['tenant','vip']`,
			want:             102,
			planBoundary:     true,
		},
		{
			name:             "jsonb_gin/path_containment",
			doltgresScanSQL:  `SELECT count(*) FROM dg_pair_jsonb_scan WHERE doc @> '{"payload":{"category":"cat-3"}}'`,
			doltgresIndexSQL: `SELECT count(id) FROM dg_pair_jsonb_path WHERE doc @> '{"payload":{"category":"cat-3"}}'`,
			postgresSQL:      `SELECT count(id) FROM pg_pair_jsonb_path WHERE doc @> '{"payload":{"category":"cat-3"}}'`,
			want:             64,
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
}

func setupPairedJsonbGinBenchmark(tb testing.TB, ctx context.Context, conn pairedBenchmarkConn, prefix string) {
	tb.Helper()
	for _, table := range []string{prefix + "_jsonb_scan", prefix + "_jsonb_ops", prefix + "_jsonb_path"} {
		execPairedSQL(tb, ctx, conn, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
		execPairedSQL(tb, ctx, conn, fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY, doc JSONB NOT NULL)", table))
		insertPairedJsonbRows(tb, ctx, conn, table, jsonbGinBenchmarkRows)
	}
	execPairedSQL(tb, ctx, conn, fmt.Sprintf("CREATE INDEX %s_jsonb_ops_idx ON %s_jsonb_ops USING gin (doc)", prefix, prefix))
	execPairedSQL(tb, ctx, conn, fmt.Sprintf("CREATE INDEX %s_jsonb_path_idx ON %s_jsonb_path USING gin (doc jsonb_path_ops)", prefix, prefix))
}

func runPairedIndexBuildBenchmarks(b *testing.B, dgCtx context.Context, dg pairedBenchmarkConn, pgCtx context.Context, pg pairedBenchmarkConn) {
	for _, target := range []struct {
		name       string
		table      string
		createSQL  string
		dropSQL    string
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
			name:      "jsonb_gin/build",
			table:     "pair_build_jsonb",
			createSQL: "CREATE INDEX %s_idx ON %s USING gin (doc)",
			dropSQL:   "DROP INDEX %s_idx",
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
			dgBuild := measurePairedIndexBuild(b, dgCtx, dg, fmt.Sprintf(target.createSQL, dgTable, dgTable), fmt.Sprintf(target.dropSQL, dgTable), iterations)
			pgBuild := measurePairedIndexBuild(b, pgCtx, pg, fmt.Sprintf(target.createSQL, pgTable, pgTable), fmt.Sprintf(target.dropSQL, pgTable), iterations)
			b.ReportMetric(float64(dgBuild.Microseconds())/float64(iterations), "dg_index_us/op")
			b.ReportMetric(float64(pgBuild.Microseconds())/float64(iterations), "pg_us/op")
			b.ReportMetric(ratio(dgBuild, pgBuild), "dg_index_vs_pg")
			b.Logf("paired-index-baseline name=%s iterations=%d dg_index=%s pg=%s dg_index_vs_pg=%.2fx",
				target.name, iterations, dgBuild, pgBuild, ratio(dgBuild, pgBuild))
		})
	}
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

func ratio(numerator time.Duration, denominator time.Duration) float64 {
	if denominator == 0 {
		return 0
	}
	return float64(numerator) / float64(denominator)
}

func oneLinePlan(plan string) string {
	return strings.Join(strings.Fields(plan), " ")
}
