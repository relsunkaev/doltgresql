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
					Query: `SELECT indexname FROM pg_catalog.pg_indexes WHERE tablename = 't' ORDER BY indexname;`,
					Expected: []sql.Row{
						{"t_pkey"},
						{"t_v_idx"},
					},
				},
				{
					// Index actually backs lookups, not just metadata.
					Query: "SELECT id FROM t WHERE v = 20;",
					Expected: []sql.Row{
						{2},
					},
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
					Query:       "INSERT INTO u VALUES (3, 'a');",
					ExpectedErr: "duplicate",
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
					Query:       "CREATE INDEX CONCURRENTLY if_not_exists_idx ON if_not_exists_t (v);",
					ExpectedErr: "Duplicate key name",
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
					Query: "SELECT id FROM multi_t WHERE a = 10 AND b = 100;",
					Expected: []sql.Row{
						{1},
					},
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
						ORDER BY indexname;`,
					Expected: []sql.Row{
						{"meta_t_active_idx", "CREATE INDEX meta_t_active_idx ON public.meta_t USING btree (tenant_id) WHERE active"},
						{"meta_t_tenant_cover_idx", "CREATE INDEX meta_t_tenant_cover_idx ON public.meta_t USING btree (tenant_id) INCLUDE (amount)"},
					},
				},
				{
					Query: `SELECT c.relname, i.indisready, i.indisvalid
						FROM pg_catalog.pg_index i
						JOIN pg_catalog.pg_class c ON i.indexrelid = c.oid
						WHERE c.relname IN ('meta_t_tenant_cover_idx', 'meta_t_active_idx')
						ORDER BY c.relname;`,
					Expected: []sql.Row{
						{"meta_t_active_idx", "t", "t"},
						{"meta_t_tenant_cover_idx", "t", "t"},
					},
				},
				{
					Query: "SELECT id FROM meta_t WHERE tenant_id = 10 AND active ORDER BY id;",
					Expected: []sql.Row{
						{1},
					},
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
						WHERE tablename = 'partial_unique_concurrent_t' AND indexname = 'puc_tenant_active_idx';`,
					Expected: []sql.Row{
						{"CREATE UNIQUE INDEX puc_tenant_active_idx ON public.partial_unique_concurrent_t USING btree (tenant_id) WHERE active"},
					},
				},
				{
					Query: `SELECT i.indisready, i.indisvalid
						FROM pg_catalog.pg_index i
						JOIN pg_catalog.pg_class c ON i.indexrelid = c.oid
						WHERE c.relname = 'puc_tenant_active_idx';`,
					Expected: []sql.Row{
						{"t", "t"},
					},
				},
				{
					Query:       "INSERT INTO partial_unique_concurrent_t VALUES (3, 10, true);",
					ExpectedErr: "duplicate",
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
						WHERE tablename = 'expr_concurrent_t' AND indexname = 'expr_concurrent_lower_email_idx';`,
					Expected: []sql.Row{
						{"CREATE INDEX expr_concurrent_lower_email_idx ON public.expr_concurrent_t USING btree (lower(email))"},
					},
				},
				{
					Query: `SELECT i.indisready, i.indisvalid
						FROM pg_catalog.pg_index i
						JOIN pg_catalog.pg_class c ON i.indexrelid = c.oid
						WHERE c.relname = 'expr_concurrent_lower_email_idx';`,
					Expected: []sql.Row{
						{"t", "t"},
					},
				},
				{
					Query: "SELECT id FROM expr_concurrent_t WHERE lower(email) = 'bob@x';",
					Expected: []sql.Row{
						{2},
					},
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
						WHERE tablename = 'unique_expr_concurrent_t' AND indexname = 'unique_expr_concurrent_lower_email_idx';`,
					Expected: []sql.Row{
						{"CREATE UNIQUE INDEX unique_expr_concurrent_lower_email_idx ON public.unique_expr_concurrent_t USING btree (lower(email))"},
					},
				},
				{
					Query: `SELECT i.indisready, i.indisvalid
						FROM pg_catalog.pg_index i
						JOIN pg_catalog.pg_class c ON i.indexrelid = c.oid
						WHERE c.relname = 'unique_expr_concurrent_lower_email_idx';`,
					Expected: []sql.Row{
						{"t", "t"},
					},
				},
				{
					Query:       "INSERT INTO unique_expr_concurrent_t VALUES (3, 'ALICE@x');",
					ExpectedErr: "duplicate",
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
					Query:       "CREATE UNIQUE INDEX CONCURRENTLY unique_expr_dup_lower_email_idx ON unique_expr_dup_t ((lower(email)));",
					ExpectedErr: "duplicate",
				},
				{
					Query: `SELECT COUNT(*) FROM pg_catalog.pg_indexes WHERE indexname = 'unique_expr_dup_lower_email_idx';`,
					Expected: []sql.Row{
						{0},
					},
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
						WHERE tablename = 'gin_t' AND indexname = 'gin_t_doc_idx';`,
					Expected: []sql.Row{
						{"CREATE INDEX gin_t_doc_idx ON public.gin_t USING gin (doc jsonb_path_ops)"},
					},
				},
				{
					Query: `SELECT i.indisready, i.indisvalid
						FROM pg_catalog.pg_index i
						JOIN pg_catalog.pg_class c ON i.indexrelid = c.oid
						WHERE c.relname = 'gin_t_doc_idx';`,
					Expected: []sql.Row{
						{"t", "t"},
					},
				},
				{
					Query: `SELECT id FROM gin_t WHERE doc @> '{"kind":"click"}' ORDER BY id;`,
					Expected: []sql.Row{
						{1},
					},
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
					Query: `SELECT COUNT(*) FROM pg_catalog.pg_indexes WHERE indexname = 'drop_t_idx';`,
					Expected: []sql.Row{
						{0},
					},
				},
				{
					// Missing index without IF EXISTS still errors.
					Query:       "DROP INDEX CONCURRENTLY drop_t_idx;",
					ExpectedErr: "was not found",
				},
				{
					// IF EXISTS suppresses the error.
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
					Query: `SELECT i.indisready, i.indisvalid FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class c ON i.indexrelid = c.oid WHERE c.relname = 'state_t_v_idx';`,
					Expected: []sql.Row{
						{"t", "t"},
					},
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
					Query:       "CREATE UNIQUE INDEX CONCURRENTLY dup_t_idx ON dup_t (code);",
					ExpectedErr: "duplicate",
				},
				{
					Query: `SELECT COUNT(*) FROM pg_catalog.pg_indexes WHERE indexname = 'dup_t_idx';`,
					Expected: []sql.Row{
						{0},
					},
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
					Query: "SELECT id FROM rt WHERE v = 10;",
					Expected: []sql.Row{
						{1},
					},
				},
			},
		},
	})
}
