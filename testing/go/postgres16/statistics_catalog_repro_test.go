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

// TestAnalyzePopulatesPgStatsRepro reproduces a catalog persistence bug:
// ANALYZE should expose PostgreSQL-compatible column statistics in pg_stats.
func TestAnalyzePopulatesPgStatsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ANALYZE populates pg_stats",
			SetUpScript: []string{
				`CREATE TABLE analyze_catalog_target (
					id INT PRIMARY KEY,
					category TEXT
				);`,
				`INSERT INTO analyze_catalog_target VALUES
					(1, 'a'), (2, 'a'), (3, 'b'), (4, 'b'), (5, 'c');`,
				`ANALYZE analyze_catalog_target;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT attname
						FROM pg_catalog.pg_stats
						WHERE schemaname = 'public'
							AND tablename = 'analyze_catalog_target'
							AND attname = 'category';`, PostgresOracle: ScriptTestPostgresOracle{ID: "statistics-catalog-repro-test-testanalyzepopulatespgstatsrepro-0001-select-attname-from-pg_catalog.pg_stats-where"},
				},
			},
		},
	})
}

// TestCreateStatisticsPopulatesPgStatisticExtRepro reproduces a catalog
// persistence bug: PostgreSQL stores extended statistics metadata in
// pg_statistic_ext after CREATE STATISTICS.
func TestCreateStatisticsPopulatesPgStatisticExtRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE STATISTICS populates pg_statistic_ext",
			SetUpScript: []string{
				`CREATE TABLE extended_stats_target (
					a INT,
					b INT
				);`,
				`INSERT INTO extended_stats_target VALUES
					(1, 1), (1, 2), (2, 2);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE STATISTICS extended_stats_ab_stats (dependencies)
						ON a, b FROM extended_stats_target;`,
				},
				{
					Query: `ANALYZE extended_stats_target;`,
				},
				{
					Query: `SELECT stxname, stxkeys::text, stxkind::text
						FROM pg_catalog.pg_statistic_ext
						WHERE stxname = 'extended_stats_ab_stats';`, PostgresOracle: ScriptTestPostgresOracle{ID: "statistics-catalog-repro-test-testcreatestatisticspopulatespgstatisticextrepro-0001-select-stxname-stxkeys::text-stxkind::text-from"},
				},
			},
		},
	})
}
