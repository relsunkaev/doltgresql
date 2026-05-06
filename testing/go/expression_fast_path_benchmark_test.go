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
	"testing"
)

func TestExpressionFastPathSQLSemantics(t *testing.T) {
	ctx, conn, controller := CreateServer(t, "postgres")
	t.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		if err := controller.WaitForStop(); err != nil {
			t.Fatalf("error stopping test server: %v", err)
		}
	})

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE expression_fast_path_semantics (id INTEGER PRIMARY KEY, v INTEGER, label TEXT NOT NULL)")
	execBenchmarkSQL(t, ctx, conn, "INSERT INTO expression_fast_path_semantics VALUES (1, 42, 'Alpha'), (2, 7, 'alpha'), (3, NULL, 'beta')")

	assertCountResult(t, ctx, conn, `SELECT count(*) FROM expression_fast_path_semantics WHERE v = 42`, 1)
	assertCountResult(t, ctx, conn, `SELECT count(*) FROM expression_fast_path_semantics WHERE v = NULL`, 0)
	assertCountResult(t, ctx, conn, `SELECT count(*) FROM expression_fast_path_semantics WHERE v = '42'`, 1)
	assertCountResult(t, ctx, conn, `SELECT count(*) FROM expression_fast_path_semantics WHERE label COLLATE "C" >= 'A' AND label COLLATE "C" < 'b'`, 2)
}

func BenchmarkExpressionFastPathSQL(b *testing.B) {
	ctx, conn := newBenchmarkServer(b)
	setupExpressionFastPathBenchmark(b, ctx, conn)

	b.Run("filter_predicates", func(b *testing.B) {
		benchmarkCountQuery(b, ctx, conn,
			`SELECT count(id) FROM expression_fast_path_filter WHERE tenant = 4 AND score >= 32`,
			btreeBenchmarkRows/16)
	})

	b.Run("join_predicates", func(b *testing.B) {
		benchmarkCountQuery(b, ctx, conn,
			`SELECT count(*) FROM expression_fast_path_join_left l JOIN expression_fast_path_join_right r ON l.tenant = r.tenant AND l.score = r.score WHERE l.id <= 64`,
			64*(btreeBenchmarkRows/64))
	})

	b.Run("dml_check_predicates", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			execBenchmarkSQL(b, ctx, conn,
				"INSERT INTO expression_fast_path_check VALUES ($1, $2, $3)",
				i+1, i%8, i%64)
		}
	})
}

func setupExpressionFastPathBenchmark(tb testing.TB, ctx context.Context, conn *Connection) {
	tb.Helper()

	execBenchmarkSQL(tb, ctx, conn, "CREATE TABLE expression_fast_path_filter (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, score INTEGER NOT NULL, label TEXT NOT NULL)")
	insertBtreePlanRows(tb, ctx, conn, "expression_fast_path_filter", btreeBenchmarkRows)

	for _, table := range []string{"expression_fast_path_join_left", "expression_fast_path_join_right"} {
		execBenchmarkSQL(tb, ctx, conn, fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, score INTEGER NOT NULL, label TEXT NOT NULL)", table))
	}
	insertBtreePlanRows(tb, ctx, conn, "expression_fast_path_join_left", btreeJoinProbeRows)
	insertBtreePlanRows(tb, ctx, conn, "expression_fast_path_join_right", btreeBenchmarkRows)

	execBenchmarkSQL(tb, ctx, conn, `CREATE TABLE expression_fast_path_check (
		id INTEGER PRIMARY KEY,
		tenant INTEGER NOT NULL,
		score INTEGER NOT NULL,
		CHECK (tenant >= 0 AND tenant < 8 AND score >= 0 AND score < 64)
	)`)
}
