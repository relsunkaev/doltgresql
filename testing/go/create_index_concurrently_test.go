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

// TestCreateIndexConcurrently exercises the workload that real ORM
// migration tooling (Drizzle Kit, Prisma migrate, Alembic, Rails
// schema-migrations) hits when emitting `CREATE INDEX CONCURRENTLY`.
// PostgreSQL builds the index without holding a strong table lock;
// doltgres takes the same lock as a regular CREATE INDEX. We accept
// the keyword so the migration does not error.
//
// Coverage:
//
//   - CREATE INDEX CONCURRENTLY on a regular column
//   - CREATE UNIQUE INDEX CONCURRENTLY
//   - CREATE INDEX CONCURRENTLY IF NOT EXISTS
//   - CREATE INDEX CONCURRENTLY on multiple columns
//   - DROP INDEX CONCURRENTLY (single + IF EXISTS + missing)
//   - REINDEX INDEX CONCURRENTLY
//   - REINDEX TABLE CONCURRENTLY
//   - Verify the index is actually built and queryable after CONCURRENTLY
func TestCreateIndexConcurrently(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE INDEX CONCURRENTLY single-column",
			SetUpScript: []string{
				"CREATE TABLE t (id INT PRIMARY KEY, v INT);",
				"INSERT INTO t VALUES (1, 10), (2, 20), (3, 30);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "CREATE INDEX CONCURRENTLY t_v_idx ON t (v);",
				},
				{
					Query: `SELECT indexname FROM pg_catalog.pg_indexes WHERE tablename = 't' ORDER BY indexname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-index-concurrently-test-testcreateindexconcurrently-0001-select-indexname-from-pg_catalog.pg_indexes-where"},
				},
				{
					// Index actually backs lookups, not just metadata.
					Query: "SELECT id FROM t WHERE v = 20;", PostgresOracle: ScriptTestPostgresOracle{ID: "create-index-concurrently-test-testcreateindexconcurrently-0002-select-id-from-t-where"},
				},
			},
		},
		{
			Name: "CREATE UNIQUE INDEX CONCURRENTLY enforces uniqueness",
			SetUpScript: []string{
				"CREATE TABLE u (id INT PRIMARY KEY, code TEXT);",
				"INSERT INTO u VALUES (1, 'a'), (2, 'b');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "CREATE UNIQUE INDEX CONCURRENTLY u_code_uq ON u (code);",
				},
				{
					Query: "INSERT INTO u VALUES (3, 'a');", PostgresOracle: ScriptTestPostgresOracle{ID: "create-index-concurrently-test-testcreateindexconcurrently-0003-insert-into-u-values-3", Compare: "sqlstate"},
				},
				{
					Query: "INSERT INTO u VALUES (3, 'c');",
				},
			},
		},
		{
			Name: "CREATE INDEX CONCURRENTLY IF NOT EXISTS",
			SetUpScript: []string{
				"CREATE TABLE if_not_exists_t (id INT PRIMARY KEY, v INT);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "CREATE INDEX CONCURRENTLY IF NOT EXISTS if_not_exists_idx ON if_not_exists_t (v);",
				},
				{
					// Repeating the same DDL is a no-op (does not error).
					Query: "CREATE INDEX CONCURRENTLY IF NOT EXISTS if_not_exists_idx ON if_not_exists_t (v);",
				},
				{
					// Without IF NOT EXISTS the second CREATE errors.
					Query: "CREATE INDEX CONCURRENTLY if_not_exists_idx ON if_not_exists_t (v);", PostgresOracle: ScriptTestPostgresOracle{ID: "create-index-concurrently-test-testcreateindexconcurrently-0004-create-index-concurrently-if_not_exists_idx-on", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "CREATE INDEX CONCURRENTLY multi-column",
			SetUpScript: []string{
				"CREATE TABLE multi_t (id INT PRIMARY KEY, a INT, b INT);",
				"INSERT INTO multi_t VALUES (1, 10, 100), (2, 20, 200);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "CREATE INDEX CONCURRENTLY multi_idx ON multi_t (a, b);",
				},
				{
					Query: "SELECT id FROM multi_t WHERE a = 10 AND b = 100;", PostgresOracle: ScriptTestPostgresOracle{ID: "create-index-concurrently-test-testcreateindexconcurrently-0005-select-id-from-multi_t-where"},
				},
			},
		},
		{
			Name: "CREATE INDEX CONCURRENTLY metadata-backed btree shapes",
			SetUpScript: []string{
				"CREATE TABLE meta_t (id INT PRIMARY KEY, tenant_id INT, amount INT, active BOOL);",
				"INSERT INTO meta_t VALUES (1, 10, 100, true), (2, 20, 200, false);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "CREATE INDEX CONCURRENTLY meta_t_tenant_cover_idx ON meta_t (tenant_id) INCLUDE (amount);",
				},
				{
					Query: "CREATE INDEX CONCURRENTLY meta_t_active_idx ON meta_t (tenant_id) WHERE active;",
				},
				{
					Query: `SELECT indexname, indexdef
						FROM pg_catalog.pg_indexes
						WHERE tablename = 'meta_t' AND indexname LIKE 'meta_t_%_idx'
						ORDER BY indexname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-index-concurrently-test-testcreateindexconcurrently-0006-select-indexname-indexdef-from-pg_catalog.pg_indexes", ColumnModes: []string{"structural", "schema"}},
				},
				{
					Query: `SELECT c.relname, i.indisready, i.indisvalid
						FROM pg_catalog.pg_index i
						JOIN pg_catalog.pg_class c ON i.indexrelid = c.oid
						WHERE c.relname IN ('meta_t_tenant_cover_idx', 'meta_t_active_idx')
						ORDER BY c.relname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-index-concurrently-test-testcreateindexconcurrently-0007-select-c.relname-i.indisready-i.indisvalid-from"},
				},
				{
					Query: "SELECT id FROM meta_t WHERE tenant_id = 10 AND active ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "create-index-concurrently-test-testcreateindexconcurrently-0008-select-id-from-meta_t-where"},
				},
			},
		},
		{
			Name: "CREATE UNIQUE INDEX CONCURRENTLY partial predicate",
			SetUpScript: []string{
				"CREATE TABLE partial_unique_concurrent_t (id INT PRIMARY KEY, tenant_id INT, active BOOL);",
				"INSERT INTO partial_unique_concurrent_t VALUES (1, 10, true), (2, 10, false);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "CREATE UNIQUE INDEX CONCURRENTLY puc_tenant_active_idx ON partial_unique_concurrent_t (tenant_id) WHERE active;",
				},
				{
					Query: `SELECT indexdef
						FROM pg_catalog.pg_indexes
						WHERE tablename = 'partial_unique_concurrent_t' AND indexname = 'puc_tenant_active_idx';`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-index-concurrently-test-testcreateindexconcurrently-0009-select-indexdef-from-pg_catalog.pg_indexes-where", ColumnModes: []string{"schema"}},
				},
				{
					Query: `SELECT i.indisready, i.indisvalid
						FROM pg_catalog.pg_index i
						JOIN pg_catalog.pg_class c ON i.indexrelid = c.oid
						WHERE c.relname = 'puc_tenant_active_idx';`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-index-concurrently-test-testcreateindexconcurrently-0010-select-i.indisready-i.indisvalid-from-pg_catalog.pg_index"},
				},
				{
					Query: "INSERT INTO partial_unique_concurrent_t VALUES (3, 10, true);", PostgresOracle: ScriptTestPostgresOracle{ID: "create-index-concurrently-test-testcreateindexconcurrently-0011-insert-into-partial_unique_concurrent_t-values-3", Compare: "sqlstate"},
				},
				{
					Query: "INSERT INTO partial_unique_concurrent_t VALUES (3, 10, false);",
				},
			},
		},
		{
			Name: "CREATE INDEX CONCURRENTLY expression",
			SetUpScript: []string{
				"CREATE TABLE expr_concurrent_t (id INT PRIMARY KEY, email TEXT);",
				"INSERT INTO expr_concurrent_t VALUES (1, 'Alice@X'), (2, 'bob@x');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "CREATE INDEX CONCURRENTLY expr_concurrent_lower_email_idx ON expr_concurrent_t ((lower(email)));",
				},
				{
					Query: `SELECT indexdef
						FROM pg_catalog.pg_indexes
						WHERE tablename = 'expr_concurrent_t' AND indexname = 'expr_concurrent_lower_email_idx';`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-index-concurrently-test-testcreateindexconcurrently-0012-select-indexdef-from-pg_catalog.pg_indexes-where", ColumnModes: []string{"schema"}},
				},
				{
					Query: `SELECT i.indisready, i.indisvalid
						FROM pg_catalog.pg_index i
						JOIN pg_catalog.pg_class c ON i.indexrelid = c.oid
						WHERE c.relname = 'expr_concurrent_lower_email_idx';`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-index-concurrently-test-testcreateindexconcurrently-0013-select-i.indisready-i.indisvalid-from-pg_catalog.pg_index"},
				},
				{
					Query: "SELECT id FROM expr_concurrent_t WHERE lower(email) = 'bob@x';", PostgresOracle: ScriptTestPostgresOracle{ID: "create-index-concurrently-test-testcreateindexconcurrently-0014-select-id-from-expr_concurrent_t-where"},
				},
			},
		},
		{
			Name: "CREATE UNIQUE INDEX CONCURRENTLY expression",
			SetUpScript: []string{
				"CREATE TABLE unique_expr_concurrent_t (id INT PRIMARY KEY, email TEXT);",
				"INSERT INTO unique_expr_concurrent_t VALUES (1, 'Alice@X'), (2, 'bob@x');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "CREATE UNIQUE INDEX CONCURRENTLY unique_expr_concurrent_lower_email_idx ON unique_expr_concurrent_t ((lower(email)));",
				},
				{
					Query: `SELECT indexdef
						FROM pg_catalog.pg_indexes
						WHERE tablename = 'unique_expr_concurrent_t' AND indexname = 'unique_expr_concurrent_lower_email_idx';`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-index-concurrently-test-testcreateindexconcurrently-0015-select-indexdef-from-pg_catalog.pg_indexes-where", ColumnModes: []string{"schema"}},
				},
				{
					Query: `SELECT i.indisready, i.indisvalid
						FROM pg_catalog.pg_index i
						JOIN pg_catalog.pg_class c ON i.indexrelid = c.oid
						WHERE c.relname = 'unique_expr_concurrent_lower_email_idx';`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-index-concurrently-test-testcreateindexconcurrently-0016-select-i.indisready-i.indisvalid-from-pg_catalog.pg_index"},
				},
				{
					Query: "INSERT INTO unique_expr_concurrent_t VALUES (3, 'ALICE@x');", PostgresOracle: ScriptTestPostgresOracle{ID: "create-index-concurrently-test-testcreateindexconcurrently-0017-insert-into-unique_expr_concurrent_t-values-3", Compare: "sqlstate"},
				},
				{
					Query: "INSERT INTO unique_expr_concurrent_t VALUES (3, 'carol@x');",
				},
			},
		},
		{
			Name: "CREATE UNIQUE INDEX CONCURRENTLY expression on duplicate data fails cleanly",
			SetUpScript: []string{
				"CREATE TABLE unique_expr_dup_t (id INT PRIMARY KEY, email TEXT);",
				"INSERT INTO unique_expr_dup_t VALUES (1, 'Alice@X'), (2, 'alice@x');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "CREATE UNIQUE INDEX CONCURRENTLY unique_expr_dup_lower_email_idx ON unique_expr_dup_t ((lower(email)));", PostgresOracle: ScriptTestPostgresOracle{ID: "create-index-concurrently-test-testcreateindexconcurrently-0018-create-unique-index-concurrently-unique_expr_dup_lower_email_idx", Compare: "sqlstate"},
				},
				{
					Query: `SELECT COUNT(*) FROM pg_catalog.pg_indexes WHERE indexname = 'unique_expr_dup_lower_email_idx';`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-index-concurrently-test-testcreateindexconcurrently-0019-select-count-*-from-pg_catalog.pg_indexes"},
				},
			},
		},
		{
			Name: "CREATE INDEX CONCURRENTLY JSONB GIN",
			SetUpScript: []string{
				"CREATE TABLE gin_t (id INT PRIMARY KEY, doc JSONB);",
				`INSERT INTO gin_t VALUES (1, '{"kind":"click","tags":["paid"]}'), (2, '{"kind":"view"}');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "CREATE INDEX CONCURRENTLY gin_t_doc_idx ON gin_t USING gin (doc jsonb_path_ops);",
				},
				{
					Query: `SELECT indexdef
						FROM pg_catalog.pg_indexes
						WHERE tablename = 'gin_t' AND indexname = 'gin_t_doc_idx';`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-index-concurrently-test-testcreateindexconcurrently-0020-select-indexdef-from-pg_catalog.pg_indexes-where", ColumnModes: []string{"schema"}},
				},
				{
					Query: `SELECT i.indisready, i.indisvalid
						FROM pg_catalog.pg_index i
						JOIN pg_catalog.pg_class c ON i.indexrelid = c.oid
						WHERE c.relname = 'gin_t_doc_idx';`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-index-concurrently-test-testcreateindexconcurrently-0021-select-i.indisready-i.indisvalid-from-pg_catalog.pg_index"},
				},
				{
					Query: `SELECT id FROM gin_t WHERE doc @> '{"kind":"click"}' ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-index-concurrently-test-testcreateindexconcurrently-0022-select-id-from-gin_t-where"},
				},
			},
		},
		{
			Name: "CREATE INDEX CONCURRENTLY rejects unsupported methods cleanly",
			SetUpScript: []string{
				"CREATE EXTENSION vector;",
				"CREATE TABLE unsupported_concurrent_t (id INT PRIMARY KEY, v INT, embedding vector(3));",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       "CREATE INDEX CONCURRENTLY unsupported_concurrent_gist_idx ON unsupported_concurrent_t USING gist (v);",
					ExpectedErr: "index method gist is not yet supported",
				},
				{
					Query:       "CREATE INDEX CONCURRENTLY unsupported_concurrent_hnsw_idx ON unsupported_concurrent_t USING hnsw (embedding vector_l2_ops);",
					ExpectedErr: "index method hnsw is not yet supported",
				},
				{
					Query: `SELECT indexname
						FROM pg_catalog.pg_indexes
						WHERE tablename = 'unsupported_concurrent_t'
							AND indexname IN ('unsupported_concurrent_gist_idx', 'unsupported_concurrent_hnsw_idx')
						ORDER BY indexname;`,
					Expected: []sql.Row{},
				},
			},
		},
		{
			Name: "DROP INDEX CONCURRENTLY",
			SetUpScript: []string{
				"CREATE TABLE drop_t (id INT PRIMARY KEY, v INT);",
				"CREATE INDEX drop_t_idx ON drop_t (v);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "DROP INDEX CONCURRENTLY drop_t_idx;",
				},
				{
					Query: `SELECT COUNT(*) FROM pg_catalog.pg_indexes WHERE indexname = 'drop_t_idx';`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-index-concurrently-test-testcreateindexconcurrently-0026-select-count-*-from-pg_catalog.pg_indexes"},
				},
				{
					// Missing index without IF EXISTS still errors.
					Query: "DROP INDEX CONCURRENTLY drop_t_idx;", PostgresOracle: ScriptTestPostgresOracle{ID: "create-index-concurrently-test-testcreateindexconcurrently-0027-drop-index-concurrently-drop_t_idx",

						// IF EXISTS suppresses the error.
						Compare: "sqlstate"},
				},
				{

					Query: "DROP INDEX CONCURRENTLY IF EXISTS drop_t_idx;",
				},
			},
		},
		{
			// After the two-phase state machine completes, the index
			// must surface as (indisready=true, indisvalid=true) so
			// migration tooling that polls pg_index post-CONCURRENTLY
			// (drizzle-kit, alembic, knex) sees the index as
			// production-ready, and the planner uses it.
			Name: "CREATE INDEX CONCURRENTLY ends in indisready=true, indisvalid=true",
			SetUpScript: []string{
				"CREATE TABLE state_t (id INT PRIMARY KEY, v INT);",
				"INSERT INTO state_t VALUES (1, 10), (2, 20);",
				"CREATE INDEX CONCURRENTLY state_t_v_idx ON state_t (v);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT i.indisready, i.indisvalid FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class c ON i.indexrelid = c.oid WHERE c.relname = 'state_t_v_idx';`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-index-concurrently-test-testcreateindexconcurrently-0028-select-i.indisready-i.indisvalid-from-pg_catalog.pg_index"},
				},
			},
		},
		{
			// CREATE UNIQUE INDEX CONCURRENTLY against duplicate-key
			// data must still raise. PostgreSQL's two-phase build
			// detects the conflict during the validation step and
			// drops the now-invalid index; doltgres's two-phase
			// build catches it inside Phase 1 (ordinary CreateIndex
			// uniqueness check) and surfaces the same error to the
			// caller. The catalog must end up clean: no orphan
			// pg_index row left behind.
			Name: "CREATE UNIQUE INDEX CONCURRENTLY on duplicate data fails cleanly",
			SetUpScript: []string{
				"CREATE TABLE dup_t (id INT PRIMARY KEY, code TEXT);",
				"INSERT INTO dup_t VALUES (1, 'a'), (2, 'a');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "CREATE UNIQUE INDEX CONCURRENTLY dup_t_idx ON dup_t (code);", PostgresOracle: ScriptTestPostgresOracle{ID: "create-index-concurrently-test-testcreateindexconcurrently-0029-create-unique-index-concurrently-dup_t_idx", Compare: "sqlstate"},
				},
				{
					Query: `SELECT COUNT(*) FROM pg_catalog.pg_indexes WHERE indexname = 'dup_t_idx';`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-index-concurrently-test-testcreateindexconcurrently-0030-select-count-*-from-pg_catalog.pg_indexes"},
				},
			},
		},
		{
			Name: "REINDEX CONCURRENTLY",
			SetUpScript: []string{
				"CREATE TABLE rt (id INT PRIMARY KEY, v INT);",
				"INSERT INTO rt VALUES (1, 10);",
				"CREATE INDEX rt_v_idx ON rt (v);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "REINDEX INDEX CONCURRENTLY rt_v_idx;",
				},
				{
					Query: "REINDEX TABLE CONCURRENTLY rt;",
				},
				{
					// Index still works after both reindexes.
					Query: "SELECT id FROM rt WHERE v = 10;", PostgresOracle: ScriptTestPostgresOracle{ID: "create-index-concurrently-test-testcreateindexconcurrently-0031-select-id-from-rt-where"},
				},
			},
		},
	})
}
