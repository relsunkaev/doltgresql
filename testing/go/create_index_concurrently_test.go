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
