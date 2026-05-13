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

func TestMaterializedViewConcurrentUniqueIndexChecklistGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "concurrent refresh accepts multi-column unique index",
			SetUpScript: []string{
				`CREATE TABLE source (a INT, b INT, v TEXT, PRIMARY KEY (a, b));`,
				`INSERT INTO source VALUES (1, 1, 'old'), (1, 2, 'keep');`,
				`CREATE MATERIALIZED VIEW mv_multi AS SELECT a, b, v FROM source;`,
				`CREATE UNIQUE INDEX mv_multi_ab_idx ON mv_multi (a, b);`,
				`UPDATE source SET v = 'new' WHERE a = 1 AND b = 1;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `REFRESH MATERIALIZED VIEW CONCURRENTLY mv_multi;`,
				},
				{
					Query: `SELECT a, b, v FROM mv_multi ORDER BY a, b;`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-checklist-guard-test-testmaterializedviewconcurrentuniqueindexchecklistguard-0001-select-a-b-v-from"},
				},
			},
		},
		{
			Name: "concurrent refresh accepts unique index with include columns",
			SetUpScript: []string{
				`CREATE TABLE source (id INT PRIMARY KEY, v TEXT);`,
				`INSERT INTO source VALUES (1, 'old'), (2, 'keep');`,
				`CREATE MATERIALIZED VIEW mv_include AS SELECT id, v FROM source;`,
				`CREATE UNIQUE INDEX mv_include_id_idx ON mv_include (id) INCLUDE (v);`,
				`UPDATE source SET v = 'new' WHERE id = 1;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `REFRESH MATERIALIZED VIEW CONCURRENTLY mv_include;`,
				},
				{
					Query: `SELECT id, v FROM mv_include ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-checklist-guard-test-testmaterializedviewconcurrentuniqueindexchecklistguard-0002-select-id-v-from-mv_include"},
				},
			},
		},
		{
			Name: "concurrent refresh rejects expression unique index",
			SetUpScript: []string{
				`CREATE TABLE source (id INT PRIMARY KEY, v TEXT);`,
				`INSERT INTO source VALUES (1, 'old'), (2, 'keep');`,
				`CREATE MATERIALIZED VIEW mv_expr AS SELECT id, v FROM source;`,
				`CREATE UNIQUE INDEX mv_expr_lower_idx ON mv_expr ((lower(v)));`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `REFRESH MATERIALIZED VIEW CONCURRENTLY mv_expr;`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-checklist-guard-test-testmaterializedviewconcurrentuniqueindexchecklistguard-0003-refresh-materialized-view-concurrently-mv_expr", Compare: "sqlstate"},
				},
				{
					Query: `SELECT id, v FROM mv_expr ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-checklist-guard-test-testmaterializedviewconcurrentuniqueindexchecklistguard-0004-select-id-v-from-mv_expr"},
				},
			},
		},
	})
}

func TestRefreshMaterializedViewFailedQueryPreservesSnapshotGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "non-concurrent refresh preserves snapshot when stored query errors",
			SetUpScript: []string{
				`CREATE TABLE source (id INT PRIMARY KEY);`,
				`INSERT INTO source VALUES (1), (3);`,
				`CREATE MATERIALIZED VIEW mv_refresh_error AS SELECT id, 10 / (id - 2) AS quotient FROM source;`,
				`INSERT INTO source VALUES (2);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `REFRESH MATERIALIZED VIEW mv_refresh_error;`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-checklist-guard-test-testrefreshmaterializedviewfailedquerypreservessnapshotguard-0001-refresh-materialized-view-mv_refresh_error", Compare: "sqlstate"},
				},
				{
					Query: `SELECT id, quotient FROM mv_refresh_error ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-checklist-guard-test-testrefreshmaterializedviewfailedquerypreservessnapshotguard-0002-select-id-quotient-from-mv_refresh_error"},
				},
			},
		},
		{
			Name: "concurrent refresh preserves snapshot when stored query errors",
			SetUpScript: []string{
				`CREATE TABLE source (id INT PRIMARY KEY);`,
				`INSERT INTO source VALUES (1), (3);`,
				`CREATE MATERIALIZED VIEW mv_concurrent_refresh_error AS SELECT id, 10 / (id - 2) AS quotient FROM source;`,
				`CREATE UNIQUE INDEX mv_concurrent_refresh_error_id_idx ON mv_concurrent_refresh_error (id);`,
				`INSERT INTO source VALUES (2);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `REFRESH MATERIALIZED VIEW CONCURRENTLY mv_concurrent_refresh_error;`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-checklist-guard-test-testrefreshmaterializedviewfailedquerypreservessnapshotguard-0003-refresh-materialized-view-concurrently-mv_concurrent_refresh_error", Compare: "sqlstate"},
				},
				{
					Query: `SELECT id, quotient FROM mv_concurrent_refresh_error ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-checklist-guard-test-testrefreshmaterializedviewfailedquerypreservessnapshotguard-0004-select-id-quotient-from-mv_concurrent_refresh_error"},
				},
			},
		},
	})
}
