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

import "testing"

func TestPreparedPlanCacheSQLInvalidation(t *testing.T) {
	ctx, conn, controller := CreateServer(t, "postgres")
	t.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		if err := controller.WaitForStop(); err != nil {
			t.Fatalf("error stopping test server: %v", err)
		}
	})

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE prepared_plan_cache_invalidation (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL)")
	execBenchmarkSQL(t, ctx, conn, "INSERT INTO prepared_plan_cache_invalidation VALUES (1, 1)")
	execBenchmarkSQL(t, ctx, conn, "PREPARE prepared_plan_cache_count AS SELECT count(*) FROM prepared_plan_cache_invalidation")
	assertCountResult(t, ctx, conn, "EXECUTE prepared_plan_cache_count", 1)

	execBenchmarkSQL(t, ctx, conn, "DROP TABLE prepared_plan_cache_invalidation")
	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE prepared_plan_cache_invalidation (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL)")
	execBenchmarkSQL(t, ctx, conn, "INSERT INTO prepared_plan_cache_invalidation VALUES (1, 1), (2, 2)")
	assertCountResult(t, ctx, conn, "EXECUTE prepared_plan_cache_count", 2)
}

func TestPreparedPlanCacheParameterizedStatementsStayCustom(t *testing.T) {
	ctx, conn, controller := CreateServer(t, "postgres")
	t.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		if err := controller.WaitForStop(); err != nil {
			t.Fatalf("error stopping test server: %v", err)
		}
	})

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE prepared_plan_cache_params (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL)")
	execBenchmarkSQL(t, ctx, conn, "INSERT INTO prepared_plan_cache_params VALUES (1, 1), (2, 1), (3, 2)")
	execBenchmarkSQL(t, ctx, conn, "PREPARE prepared_plan_cache_param(integer) AS SELECT count(*) FROM prepared_plan_cache_params WHERE tenant = $1")
	assertCountResult(t, ctx, conn, "EXECUTE prepared_plan_cache_param(1)", 2)
	assertCountResult(t, ctx, conn, "EXECUTE prepared_plan_cache_param(2)", 1)
}

func BenchmarkPreparedPlanCacheSQL(b *testing.B) {
	ctx, conn := newBenchmarkServer(b)
	execBenchmarkSQL(b, ctx, conn, "CREATE TABLE prepared_plan_cache_bench (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, score INTEGER NOT NULL, label TEXT NOT NULL)")
	insertBtreePlanRows(b, ctx, conn, "prepared_plan_cache_bench", btreeBenchmarkRows)
	execBenchmarkSQL(b, ctx, conn, "PREPARE prepared_plan_cache_bench_count AS SELECT count(id) FROM prepared_plan_cache_bench WHERE tenant = 4 AND score >= 32")

	b.Run("no_parameter_select", func(b *testing.B) {
		benchmarkCountQuery(b, ctx, conn, "EXECUTE prepared_plan_cache_bench_count", btreeBenchmarkRows/16)
	})
}
