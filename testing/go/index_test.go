// Copyright 2024 Dolthub, Inc.
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
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/testing/go/testdata"

	"github.com/dolthub/go-mysql-server/sql"
)

func unsupportedAccessMethodBoundaryScript(displayName string, accessMethod string, handler string) ScriptTest {
	tableName := "access_method_boundary_" + accessMethod
	indexName := tableName + "_idx"
	return ScriptTest{
		Name: "PostgreSQL " + displayName + " access method boundary",
		SetUpScript: []string{
			"CREATE TABLE " + tableName + " (id INTEGER PRIMARY KEY, v TEXT NOT NULL);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT amname, amhandler, amtype
FROM pg_catalog.pg_am
WHERE amname = '` + accessMethod + `';`,
				Expected: []sql.Row{
					{accessMethod, handler, "i"},
				},
			},
			{
				Query: `SELECT COUNT(*)
FROM pg_catalog.pg_opclass opc
JOIN pg_catalog.pg_am am ON am.oid = opc.opcmethod
WHERE am.amname = '` + accessMethod + `';`,
				Expected: []sql.Row{{0}},
			},
			{
				Query: `SELECT COUNT(*)
FROM pg_catalog.pg_opfamily opf
JOIN pg_catalog.pg_am am ON am.oid = opf.opfmethod
WHERE am.amname = '` + accessMethod + `';`,
				Expected: []sql.Row{{0}},
			},
			{
				Query: `SELECT COUNT(*)
FROM pg_catalog.pg_amop amop
JOIN pg_catalog.pg_am am ON am.oid = amop.amopmethod
WHERE am.amname = '` + accessMethod + `';`,
				Expected: []sql.Row{{0}},
			},
			{
				Query: `SELECT COUNT(*)
FROM pg_catalog.pg_amproc amproc
JOIN pg_catalog.pg_opfamily opf ON opf.oid = amproc.amprocfamily
JOIN pg_catalog.pg_am am ON am.oid = opf.opfmethod
WHERE am.amname = '` + accessMethod + `';`,
				Expected: []sql.Row{{0}},
			},
			{
				Query:       "CREATE INDEX " + indexName + " ON " + tableName + " USING " + accessMethod + " (v);",
				ExpectedErr: "index method " + accessMethod + " is not yet supported",
			},
			{
				Query: `SELECT COUNT(*)
FROM pg_catalog.pg_class
WHERE relname = '` + indexName + `';`,
				Expected: []sql.Row{{0}},
			},
		},
	}
}

func TestJsonbGinPostingChunkBuildGate(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "PostgreSQL jsonb gin posting chunk build gate",
			SetUpScript: []string{
				"CREATE TABLE jsonb_gin_build (id INTEGER PRIMARY KEY, doc JSONB NOT NULL);",
				`INSERT INTO jsonb_gin_build VALUES
					(1, '{"tags":["vip"],"status":"open","payload":{"category":"cat-1"}}'),
					(2, '{"tags":["vip","archived"],"status":"open","payload":{"category":"cat-2"}}'),
					(3, '{"tags":["standard"],"status":"closed","payload":{"category":"cat-1"}}');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "CREATE INDEX jsonb_gin_build_doc_idx ON jsonb_gin_build USING gin (doc);",
				},
				{
					Query: `SELECT indexdef
FROM pg_catalog.pg_indexes
WHERE tablename = 'jsonb_gin_build' AND indexname = 'jsonb_gin_build_doc_idx';`,
					Expected: []sql.Row{
						{"CREATE INDEX jsonb_gin_build_doc_idx ON public.jsonb_gin_build USING gin (doc jsonb_ops)"},
					},
				},
				{
					Query: `SELECT COUNT(*) > 0, MIN(format_version), SUM(row_count), COUNT(payload), COUNT(checksum)
FROM dg_gin_jsonb_gin_build_jsonb_gin_build_doc_idx_posting_chunks;`,
					Expected: []sql.Row{
						{"t", 1, float64(22), 11, 11},
					},
				},
			},
		},
		{
			Name: "PostgreSQL jsonb gin numeric primary key direct references",
			SetUpScript: []string{
				"CREATE TABLE jsonb_gin_numeric_pk (id NUMERIC PRIMARY KEY, doc JSONB NOT NULL);",
				`INSERT INTO jsonb_gin_numeric_pk VALUES
					(1.1, '{"tags":["vip"],"status":"open"}'),
					(2.2, '{"tags":["vip","archived"],"status":"open"}'),
					(3.3, '{"tags":["standard"],"status":"closed"}');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "CREATE INDEX jsonb_gin_numeric_pk_doc_idx ON jsonb_gin_numeric_pk USING gin (doc);",
				},
				{
					Query: `SELECT indexdef
FROM pg_catalog.pg_indexes
WHERE tablename = 'jsonb_gin_numeric_pk' AND indexname = 'jsonb_gin_numeric_pk_doc_idx';`,
					Expected: []sql.Row{
						{"CREATE INDEX jsonb_gin_numeric_pk_doc_idx ON public.jsonb_gin_numeric_pk USING gin (doc jsonb_ops)"},
					},
				},
				{
					Query: `SELECT id::text FROM jsonb_gin_numeric_pk
WHERE doc @> '{"tags":["vip"]}'
ORDER BY id;`,
					Expected: []sql.Row{{"1.1"}, {"2.2"}},
				},
				{
					Query: `SELECT COUNT(*) > 0, SUM(row_count) > 0
FROM dg_gin_jsonb_gin_numeric_pk_jsonb_gin_numeric_pk_doc_idx_posting_chunks;`,
					Expected: []sql.Row{{"t", "t"}},
				},
			},
		},
		{
			Name: "PostgreSQL jsonb gin no primary key fallback",
			SetUpScript: []string{
				"CREATE TABLE jsonb_gin_no_pk (id INTEGER NOT NULL, doc JSONB NOT NULL);",
				`INSERT INTO jsonb_gin_no_pk VALUES
					(1, '{"tags":["vip"],"status":"open"}'),
					(2, '{"tags":["standard"],"status":"open"}'),
					(3, '{"tags":["vip"],"status":"closed"}');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "CREATE INDEX jsonb_gin_no_pk_doc_idx ON jsonb_gin_no_pk USING gin (doc);",
				},
				{
					Query: `SELECT indexdef
FROM pg_catalog.pg_indexes
WHERE tablename = 'jsonb_gin_no_pk' AND indexname = 'jsonb_gin_no_pk_doc_idx';`,
					Expected: []sql.Row{
						{"CREATE INDEX jsonb_gin_no_pk_doc_idx ON public.jsonb_gin_no_pk USING gin (doc jsonb_ops)"},
					},
				},
				{
					Query: `SELECT id FROM jsonb_gin_no_pk
WHERE doc @> '{"tags":["vip"]}'
ORDER BY id;`,
					Expected: []sql.Row{{1}, {3}},
				},
			},
		},
		{
			Name: "PostgreSQL jsonb gin failed create index cleanup",
			SetUpScript: []string{
				"CREATE TABLE jsonb_gin_bad_create (id INTEGER PRIMARY KEY, doc TEXT NOT NULL);",
				"INSERT INTO jsonb_gin_bad_create VALUES (1, '{\"tags\":[\"vip\"]}');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       "CREATE INDEX jsonb_gin_bad_create_doc_idx ON jsonb_gin_bad_create USING gin (doc);",
					ExpectedErr: "gin indexes are only supported on jsonb columns",
				},
				{
					Query: `SELECT COUNT(*)
FROM pg_catalog.pg_indexes
WHERE tablename = 'jsonb_gin_bad_create' AND indexname = 'jsonb_gin_bad_create_doc_idx';`,
					Expected: []sql.Row{{0}},
				},
				{
					Query: `SELECT COUNT(*)
FROM pg_catalog.pg_class
WHERE relname = 'dg_gin_jsonb_gin_bad_create_jsonb_gin_bad_create_doc_idx_posting_chunks';`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

func TestJsonbGinPostingChunkLookupGate(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "PostgreSQL jsonb gin indexed lookup and recheck",
			SetUpScript: []string{
				"CREATE TABLE jsonb_gin_lookup (id INTEGER PRIMARY KEY, doc JSONB NOT NULL);",
				`INSERT INTO jsonb_gin_lookup VALUES
						(1, '{"a":1,"b":2,"tags":["x"],"nested":{"a":9}}'),
						(2, '{"a":1,"b":3,"tags":["y"]}'),
						(3, '{"a":2,"b":2,"tags":["x"]}'),
						(4, '{"nested":{"a":1},"tags":["z"]}'),
						(5, '{"a":null,"tags":["x"]}');`,
				"CREATE INDEX jsonb_gin_lookup_idx ON jsonb_gin_lookup USING gin (doc);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `EXPLAIN SELECT id FROM jsonb_gin_lookup
WHERE doc @> '{"a":1}'
ORDER BY id;`,
					Expected: []sql.Row{
						{"Project"},
						{" ├─ columns: [jsonb_gin_lookup.id]"},
						{" └─ Sort(jsonb_gin_lookup.id ASC)"},
						{"     └─ Filter"},
						{`         ├─ jsonb_gin_lookup.doc @> '{"a":1}'`},
						{"         └─ IndexedTableAccess(jsonb_gin_lookup)"},
						{"             ├─ index: [jsonb_gin(doc)]"},
						{"             ├─ filters: [{[jsonb_gin_lookup_idx intersect 2 token(s), jsonb_gin_lookup_idx intersect 2 token(s)]}]"},
						{"             └─ columns: [id doc]"},
					},
				},
				{
					Query: `SELECT id FROM jsonb_gin_lookup
WHERE doc @> '{"a":1}'
ORDER BY id;`,
					Expected: []sql.Row{{1}, {2}},
				},
				{
					Query: `SELECT count(*) FROM jsonb_gin_lookup
WHERE doc @> '{"a":1}';`,
					Expected: []sql.Row{{2}},
				},
				{
					Query: `SELECT id FROM jsonb_gin_lookup
WHERE doc @> '{"nested":{"a":1}}'
ORDER BY id;`,
					Expected: []sql.Row{{4}},
				},
				{
					Query: `SELECT id FROM jsonb_gin_lookup
WHERE doc ? 'a'
ORDER BY id;`,
					Expected: []sql.Row{{1}, {2}, {3}, {5}},
				},
				{
					Query: `SELECT id FROM jsonb_gin_lookup
WHERE doc ?| ARRAY['missing','a']
ORDER BY id;`,
					Expected: []sql.Row{{1}, {2}, {3}, {5}},
				},
				{
					Query: `SELECT id FROM jsonb_gin_lookup
WHERE doc ?& ARRAY['a','tags']
ORDER BY id;`,
					Expected: []sql.Row{{1}, {2}, {3}, {5}},
				},
			},
		},
		{
			Name: "PostgreSQL jsonb gin path ops indexed lookup",
			SetUpScript: []string{
				"CREATE TABLE jsonb_gin_path_lookup (id INTEGER PRIMARY KEY, doc JSONB NOT NULL);",
				`INSERT INTO jsonb_gin_path_lookup VALUES
						(1, '{"a":{"b":1},"tags":["x"]}'),
						(2, '{"a":{"b":2},"tags":["x"]}'),
						(3, '{"a":{"c":1},"tags":["y"]}');`,
				"CREATE INDEX jsonb_gin_path_lookup_idx ON jsonb_gin_path_lookup USING gin (doc jsonb_path_ops);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id FROM jsonb_gin_path_lookup
WHERE doc @> '{"a":{"b":1}}'
ORDER BY id;`,
					Expected: []sql.Row{{1}},
				},
			},
		},
		{
			Name: "PostgreSQL jsonb gin fallback row-reference lookup",
			SetUpScript: []string{
				"CREATE TABLE jsonb_gin_fallback_lookup (id NUMERIC PRIMARY KEY, doc JSONB NOT NULL);",
				`INSERT INTO jsonb_gin_fallback_lookup VALUES
						(1.1, '{"a":1,"tags":["x"]}'),
						(2.2, '{"a":1,"tags":["y"]}'),
						(3.3, '{"a":2,"tags":["x"]}');`,
				"CREATE INDEX jsonb_gin_fallback_lookup_idx ON jsonb_gin_fallback_lookup USING gin (doc);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id::text FROM jsonb_gin_fallback_lookup
WHERE doc @> '{"a":1}'
ORDER BY id;`,
					Expected: []sql.Row{{"1.1"}, {"2.2"}},
				},
			},
		},
	})
}

func TestJsonbGinPostingChunkDMLGate(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "PostgreSQL jsonb gin posting chunk DML maintenance",
			SetUpScript: []string{
				"CREATE TABLE jsonb_gin_dml (id INTEGER PRIMARY KEY, doc JSONB NOT NULL);",
				`INSERT INTO jsonb_gin_dml VALUES
					(1, '{"tags":["vip"],"status":"open"}'),
					(2, '{"tags":["standard"],"status":"open"}');`,
				"CREATE INDEX jsonb_gin_dml_idx ON jsonb_gin_dml USING gin (doc);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO jsonb_gin_dml VALUES
						(3, '{"tags":["vip","archived"],"status":"closed"}');`,
				},
				{
					Query: `SELECT id FROM jsonb_gin_dml
WHERE doc @> '{"tags":["vip"]}'
ORDER BY id;`,
					Expected: []sql.Row{{1}, {3}},
				},
				{
					Query: `UPDATE jsonb_gin_dml
SET doc = '{"tags":["vip"],"status":"open"}'
WHERE id = 2;`,
				},
				{
					Query: `SELECT id FROM jsonb_gin_dml
WHERE doc @> '{"tags":["vip"]}'
ORDER BY id;`,
					Expected: []sql.Row{{1}, {2}, {3}},
				},
				{
					Query: "DELETE FROM jsonb_gin_dml WHERE id = 1;",
				},
				{
					Query: `SELECT id FROM jsonb_gin_dml
WHERE doc @> '{"tags":["vip"]}'
ORDER BY id;`,
					Expected: []sql.Row{{2}, {3}},
				},
				{
					Query:       `INSERT INTO jsonb_gin_dml VALUES (2, '{"tags":["vip"],"status":"duplicate"}');`,
					ExpectedErr: "duplicate",
				},
				{
					Query: `SELECT id FROM jsonb_gin_dml
WHERE doc @> '{"status":"duplicate"}'
ORDER BY id;`,
					Expected: []sql.Row{},
				},
				{
					Query: "BEGIN;",
				},
				{
					Query: `INSERT INTO jsonb_gin_dml VALUES
						(4, '{"tags":["vip"],"status":"rolled-back"}');`,
				},
				{
					Query: `SELECT id FROM jsonb_gin_dml
WHERE doc @> '{"status":"rolled-back"}'
ORDER BY id;`,
					Expected: []sql.Row{{4}},
				},
				{
					Query: "ROLLBACK;",
				},
				{
					Query: `SELECT id FROM jsonb_gin_dml
WHERE doc @> '{"status":"rolled-back"}'
ORDER BY id;`,
					Expected: []sql.Row{},
				},
			},
		},
		{
			Name: "PostgreSQL jsonb gin fallback primary key DML maintenance",
			SetUpScript: []string{
				"CREATE TABLE jsonb_gin_numeric_dml (id NUMERIC PRIMARY KEY, doc JSONB NOT NULL);",
				`INSERT INTO jsonb_gin_numeric_dml VALUES
					(1.1, '{"tags":["vip"],"status":"open"}'),
					(2.2, '{"tags":["standard"],"status":"open"}');`,
				"CREATE INDEX jsonb_gin_numeric_dml_idx ON jsonb_gin_numeric_dml USING gin (doc);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE jsonb_gin_numeric_dml
SET doc = '{"tags":["vip"],"status":"closed"}'
WHERE id = 2.2;`,
				},
				{
					Query: `SELECT id::text FROM jsonb_gin_numeric_dml
WHERE doc @> '{"tags":["vip"]}'
ORDER BY id;`,
					Expected: []sql.Row{{"1.1"}, {"2.2"}},
				},
				{
					Query: "DELETE FROM jsonb_gin_numeric_dml WHERE id = 1.1;",
				},
				{
					Query: `SELECT id::text FROM jsonb_gin_numeric_dml
WHERE doc @> '{"tags":["vip"]}'
ORDER BY id;`,
					Expected: []sql.Row{{"2.2"}},
				},
			},
		},
		{
			Name: "PostgreSQL jsonb gin no primary key DML maintenance",
			SetUpScript: []string{
				"CREATE TABLE jsonb_gin_no_pk_dml (id INTEGER NOT NULL, doc JSONB NOT NULL);",
				`INSERT INTO jsonb_gin_no_pk_dml VALUES
					(1, '{"tags":["vip"],"status":"open"}'),
					(2, '{"tags":["standard"],"status":"open"}');`,
				"CREATE INDEX jsonb_gin_no_pk_dml_idx ON jsonb_gin_no_pk_dml USING gin (doc);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE jsonb_gin_no_pk_dml
SET doc = '{"tags":["vip"],"status":"closed"}'
WHERE id = 2;`,
				},
				{
					Query: `SELECT id FROM jsonb_gin_no_pk_dml
WHERE doc @> '{"tags":["vip"]}'
ORDER BY id;`,
					Expected: []sql.Row{{1}, {2}},
				},
				{
					Query: "DELETE FROM jsonb_gin_no_pk_dml WHERE id = 1;",
				},
				{
					Query: `SELECT id FROM jsonb_gin_no_pk_dml
WHERE doc @> '{"tags":["vip"]}'
ORDER BY id;`,
					Expected: []sql.Row{{2}},
				},
			},
		},
	})
}

func TestJsonbGinPostingChunkRootSemantics(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "PostgreSQL jsonb gin posting chunk Dolt root semantics",
			SetUpScript: []string{
				"CREATE TABLE jsonb_gin_root (id INTEGER PRIMARY KEY, doc JSONB NOT NULL);",
				`INSERT INTO jsonb_gin_root VALUES
					(1, '{"tags":["vip"],"status":"open"}'),
					(2, '{"tags":["standard"],"status":"open"}');`,
				"CREATE INDEX jsonb_gin_root_idx ON jsonb_gin_root USING gin (doc);",
				"SELECT DOLT_COMMIT('-Am', 'initial jsonb gin root');",
				"SELECT DOLT_BRANCH('feature');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id FROM jsonb_gin_root
WHERE doc @> '{"tags":["vip"]}'
ORDER BY id;`,
					Expected: []sql.Row{{1}},
				},
				{
					Query: `SELECT CASE WHEN COUNT(*) > 0 THEN 't' ELSE 'f' END
FROM dg_gin_jsonb_gin_root_jsonb_gin_root_idx_posting_chunks;`,
					Expected: []sql.Row{{"t"}},
				},
				{
					Query:            "SELECT DOLT_CHECKOUT('feature');",
					SkipResultsCheck: true,
				},
				{
					Query: `INSERT INTO jsonb_gin_root VALUES
						(3, '{"tags":["vip","feature"],"status":"feature"}');`,
				},
				{
					Query: `SELECT id FROM jsonb_gin_root
WHERE doc @> '{"tags":["feature"]}'
ORDER BY id;`,
					Expected: []sql.Row{{3}},
				},
				{
					Query:            "SELECT DOLT_COMMIT('-Am', 'feature jsonb gin root');",
					SkipResultsCheck: true,
				},
				{
					Query:            "SELECT DOLT_CHECKOUT('main');",
					SkipResultsCheck: true,
				},
				{
					Query: `SELECT id FROM jsonb_gin_root
WHERE doc @> '{"tags":["feature"]}'
ORDER BY id;`,
					Expected: []sql.Row{},
				},
				{
					Query: `INSERT INTO jsonb_gin_root VALUES
						(4, '{"tags":["main"],"status":"main"}');`,
				},
				{
					Query:            "SELECT DOLT_COMMIT('-Am', 'main jsonb gin root');",
					SkipResultsCheck: true,
				},
				{
					Query:            "SELECT DOLT_MERGE('feature');",
					SkipResultsCheck: true,
				},
				{
					Query: `SELECT id FROM jsonb_gin_root
WHERE doc @> '{"tags":["main"]}' OR doc @> '{"tags":["feature"]}'
ORDER BY id;`,
					Expected: []sql.Row{{3}, {4}},
				},
				{
					Query: "BEGIN;",
				},
				{
					Query: `INSERT INTO jsonb_gin_root VALUES
						(5, '{"tags":["rolled_back"],"status":"temporary"}');`,
				},
				{
					Query: `SELECT id FROM jsonb_gin_root
WHERE doc @> '{"tags":["rolled_back"]}'
ORDER BY id;`,
					Expected: []sql.Row{{5}},
				},
				{
					Query: "ROLLBACK;",
				},
				{
					Query: `SELECT id FROM jsonb_gin_root
WHERE doc @> '{"tags":["rolled_back"]}'
ORDER BY id;`,
					Expected: []sql.Row{},
				},
				{
					Query: `UPDATE jsonb_gin_root
SET doc = '{"tags":["vip","updated"],"status":"updated"}'
WHERE id = 2;`,
				},
				{
					Query: `SELECT id FROM jsonb_gin_root
WHERE doc @> '{"tags":["updated"]}'
ORDER BY id;`,
					Expected: []sql.Row{{2}},
				},
				{
					Query: "DELETE FROM jsonb_gin_root WHERE id = 1;",
				},
				{
					Query: `SELECT id FROM jsonb_gin_root
WHERE doc @> '{"tags":["vip"]}'
ORDER BY id;`,
					Expected: []sql.Row{{2}, {3}},
				},
				{
					Query: "DROP INDEX jsonb_gin_root_idx;",
				},
				{
					Query: `SELECT COUNT(*) FROM pg_catalog.pg_class
WHERE relname = 'dg_gin_jsonb_gin_root_jsonb_gin_root_idx_posting_chunks';`,
					Expected: []sql.Row{{0}},
				},
			},
		},
		{
			Name: "PostgreSQL jsonb gin posting chunk conflicting merge remains table conflict",
			SetUpScript: []string{
				"CREATE TABLE jsonb_gin_root_conflict (id INTEGER PRIMARY KEY, doc JSONB NOT NULL);",
				`INSERT INTO jsonb_gin_root_conflict VALUES
					(1, '{"tags":["vip"],"status":"open"}');`,
				"CREATE INDEX jsonb_gin_root_conflict_idx ON jsonb_gin_root_conflict USING gin (doc);",
				"SELECT DOLT_COMMIT('-Am', 'initial jsonb gin root conflict');",
				"SELECT DOLT_BRANCH('jsonb_gin_conflict');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:            "SELECT DOLT_CHECKOUT('jsonb_gin_conflict');",
					SkipResultsCheck: true,
				},
				{
					Query: `UPDATE jsonb_gin_root_conflict
SET doc = '{"tags":["vip","feature"],"status":"feature"}'
WHERE id = 1;`,
				},
				{
					Query:            "SELECT DOLT_COMMIT('-Am', 'feature jsonb gin root conflict');",
					SkipResultsCheck: true,
				},
				{
					Query:            "SELECT DOLT_CHECKOUT('main');",
					SkipResultsCheck: true,
				},
				{
					Query: `UPDATE jsonb_gin_root_conflict
SET doc = '{"tags":["vip","main"],"status":"main"}'
WHERE id = 1;`,
				},
				{
					Query:            "SELECT DOLT_COMMIT('-Am', 'main jsonb gin root conflict');",
					SkipResultsCheck: true,
				},
				{
					Query:       "SELECT DOLT_MERGE('jsonb_gin_conflict');",
					ExpectedErr: "Merge conflict detected",
				},
			},
		},
	})
}

func TestJsonbGinPostingChunkReopenRootSemantics(t *testing.T) {
	dbDir, err := os.MkdirTemp(os.TempDir(), t.Name())
	if err != nil {
		t.Fatalf("creating temp database directory: %v", err)
	}
	defer os.RemoveAll(dbDir)

	port, err := sql.GetEmptyPort()
	if err != nil {
		t.Fatalf("finding empty port: %v", err)
	}
	ctx, conn, controller := CreateServerLocalInDirWithPort(t, "postgres", dbDir, port)
	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE jsonb_gin_reopen (id INTEGER PRIMARY KEY, doc JSONB NOT NULL);")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO jsonb_gin_reopen VALUES
		(1, '{"tags":["vip"],"status":"open"}'),
		(2, '{"tags":["standard"],"status":"open"}');`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX jsonb_gin_reopen_idx ON jsonb_gin_reopen USING gin (doc);")
	assertBenchmarkPlanShape(t, ctx, conn, `SELECT count(*) FROM jsonb_gin_reopen WHERE doc @> '{"tags":["vip"]}'`, true)
	assertCountResult(t, ctx, conn, `SELECT count(*) FROM jsonb_gin_reopen WHERE doc @> '{"tags":["vip"]}'`, 1)
	if got := queryBenchmarkString(t, ctx, conn, `SELECT CASE WHEN COUNT(*) > 0 THEN 't' ELSE 'f' END
FROM dg_gin_jsonb_gin_reopen_jsonb_gin_reopen_idx_posting_chunks`); got != "t" {
		t.Fatalf("expected posting chunk sidecar rows before restart, got %q", got)
	}
	execBenchmarkSQL(t, ctx, conn, "SELECT DOLT_COMMIT('-Am', 'initial jsonb gin reopen');")
	conn.Close(ctx)
	controller.Stop()
	controller.WaitForStop()

	port, err = sql.GetEmptyPort()
	if err != nil {
		t.Fatalf("finding empty port after restart: %v", err)
	}
	ctx, conn, controller = CreateServerLocalInDirWithPort(t, "postgres", dbDir, port)
	defer func() {
		conn.Close(ctx)
		controller.Stop()
		controller.WaitForStop()
	}()

	assertBenchmarkPlanShape(t, ctx, conn, `SELECT count(*) FROM jsonb_gin_reopen WHERE doc @> '{"tags":["vip"]}'`, true)
	assertCountResult(t, ctx, conn, `SELECT count(*) FROM jsonb_gin_reopen WHERE doc @> '{"tags":["vip"]}'`, 1)
	if got := queryBenchmarkString(t, ctx, conn, `SELECT CASE WHEN COUNT(*) > 0 THEN 't' ELSE 'f' END
FROM dg_gin_jsonb_gin_reopen_jsonb_gin_reopen_idx_posting_chunks`); got != "t" {
		t.Fatalf("expected posting chunk sidecar rows after restart, got %q", got)
	}
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO jsonb_gin_reopen VALUES
		(3, '{"tags":["after_reopen"],"status":"open"}');`)
	assertCountResult(t, ctx, conn, `SELECT count(*) FROM jsonb_gin_reopen WHERE doc @> '{"tags":["after_reopen"]}'`, 1)
}

func TestBasicIndexing(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "Covering Index",
			SetUpScript: []string{
				"CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT);",
				"INSERT INTO test VALUES (13, 3), (11, 1), (15, 5), (12, 2), (14, 4);",
				"CREATE INDEX v1_idx ON test(v1);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT * FROM test WHERE v1 = 2 ORDER BY pk;",
					Expected: []sql.Row{
						{12, 2},
					},
				},
				{
					Query: "explain SELECT * FROM test WHERE v1 = 2 ORDER BY pk;",
					Expected: []sql.Row{
						{"Sort(test.pk ASC)"},
						{" └─ IndexedTableAccess(test)"},
						{"     ├─ index: [test.v1]"},
						{"     ├─ filters: [{[2, 2]}]"},
						{"     └─ columns: [pk v1]"},
					},
				},
				{
					Query: "SELECT * FROM test WHERE v1 > 2 ORDER BY pk;",
					Expected: []sql.Row{
						{13, 3},
						{14, 4},
						{15, 5},
					},
				},
				{
					Query: "explain SELECT * FROM test WHERE v1 > 2 ORDER BY pk;",
					Expected: []sql.Row{
						{"Sort(test.pk ASC)"},
						{" └─ IndexedTableAccess(test)"},
						{"     ├─ index: [test.v1]"},
						{"     ├─ filters: [{(2, ∞)}]"},
						{"     └─ columns: [pk v1]"},
					},
				},
				{
					Query: "SELECT * FROM test WHERE (v1 > 3 OR v1 < 2) AND v1 <> 5 ORDER BY pk;",
					Expected: []sql.Row{
						{11, 1},
						{14, 4}},
				},
				{
					Query: "explain SELECT * FROM test WHERE (v1 > 3 OR v1 < 2) AND v1 <> 5 ORDER BY pk;",
					Expected: []sql.Row{
						{"Sort(test.pk ASC)"},
						{" └─ IndexedTableAccess(test)"},
						{"     ├─ index: [test.v1]"},
						{"     ├─ filters: [{(NULL, 2)}, {(3, 5)}, {(5, ∞)}]"},
						{"     └─ columns: [pk v1]"},
					},
				},
				{
					Query: "SELECT * FROM test WHERE v1 = 2 OR v1 = 4 ORDER BY pk;",
					Expected: []sql.Row{
						{12, 2},
						{14, 4},
					},
				},
				{
					Query: "explain SELECT * FROM test WHERE v1 = 2 OR v1 = 4 ORDER BY pk;",
					Expected: []sql.Row{
						{"Sort(test.pk ASC)"},
						{" └─ IndexedTableAccess(test)"},
						{"     ├─ index: [test.v1]"},
						{"     ├─ filters: [{[2, 2]}, {[4, 4]}]"},
						{"     └─ columns: [pk v1]"},
					},
				},
				{
					Query: "SELECT * FROM test WHERE v1 IN (2, 4) ORDER BY pk;",
					Expected: []sql.Row{
						{12, 2},
						{14, 4},
					},
				},
				{
					Query: "explain SELECT * FROM test WHERE v1 IN (2, 4) ORDER BY pk;",
					Expected: []sql.Row{
						{"Sort(test.pk ASC)"},
						{" └─ IndexedTableAccess(test)"},
						{"     ├─ index: [test.v1]"},
						{"     ├─ filters: [{[2, 2]}, {[4, 4]}]"},
						{"     └─ columns: [pk v1]"},
					},
				},
				{
					Query: "SELECT * FROM test WHERE v1 IN (4, 2, 4, 2) ORDER BY pk;",
					Expected: []sql.Row{
						{12, 2},
						{14, 4},
					},
				},
				{
					Query: "explain SELECT * FROM test WHERE v1 IN (4, 2, 4, 2) ORDER BY pk;",
					Expected: []sql.Row{
						{"Sort(test.pk ASC)"},
						{" └─ IndexedTableAccess(test)"},
						{"     ├─ index: [test.v1]"},
						{"     ├─ filters: [{[2, 2]}, {[4, 4]}]"},
						{"     └─ columns: [pk v1]"},
					},
				},
				{
					Query: "SELECT * FROM test WHERE v1 IN (NULL, 2, 2) ORDER BY pk;",
					Expected: []sql.Row{
						{12, 2},
					},
				},
				{
					Query: "SELECT * FROM test WHERE v1 NOT IN (2, 4) ORDER BY pk;",
					Expected: []sql.Row{
						{11, 1},
						{13, 3},
						{15, 5},
					},
				},
				{
					Query: "explain SELECT * FROM test WHERE v1 NOT IN (2, 4) ORDER BY pk;",
					Expected: []sql.Row{
						{"Sort(test.pk ASC)"},
						{" └─ IndexedTableAccess(test)"},
						{"     ├─ index: [test.v1]"},
						{"     ├─ filters: [{(NULL, 2)}, {(2, 4)}, {(4, ∞)}]"},
						{"     └─ columns: [pk v1]"},
					},
				},
				{
					Query: "SELECT * FROM test WHERE v1 >= 4 ORDER BY pk;",
					Expected: []sql.Row{
						{14, 4},
						{15, 5},
					},
				},
				{
					Query: "explain SELECT * FROM test WHERE v1 >= 4 ORDER BY pk;",
					Expected: []sql.Row{
						{"Sort(test.pk ASC)"},
						{" └─ IndexedTableAccess(test)"},
						{"     ├─ index: [test.v1]"},
						{"     ├─ filters: [{[4, ∞)}]"},
						{"     └─ columns: [pk v1]"},
					},
				},
				{
					Query: "SELECT * FROM test WHERE v1 < 3 ORDER BY pk;",
					Expected: []sql.Row{
						{11, 1},
						{12, 2},
					},
				},
				{
					Query: "SELECT * FROM test WHERE v1 <= 3 ORDER BY pk;",
					Expected: []sql.Row{
						{11, 1},
						{12, 2},
						{13, 3},
					},
				},
			},
		},
		{
			Name: "Covering string Index",
			SetUpScript: []string{
				"CREATE TABLE test (pk bigint PRIMARY KEY, v1 varchar(10));",
				"INSERT INTO test VALUES (13, 'thirteen'), (11, 'eleven'), (15, 'fifteen'), (12, 'twelve'), (14, 'fourteen');",
				"CREATE UNIQUE INDEX v1_idx ON test(v1);",
				"CREATE INDEX v1_pk_idx ON test(v1, pk);",
				"CREATE INDEX pk_v1_idx ON test(pk, v1);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT * FROM test WHERE v1 = 'twelve' ORDER BY pk;",
					Expected: []sql.Row{
						{12, "twelve"},
					},
				},
				{
					Query: "SELECT * FROM test WHERE v1 > 't' OR v1 < 'f' ORDER BY pk;",
					Expected: []sql.Row{
						{11, "eleven"},
						{12, "twelve"},
						{13, "thirteen"},
					},
				},
				{
					Query: "explain SELECT * FROM test WHERE v1 > 't' OR v1 < 'f' ORDER BY pk;",
					Expected: []sql.Row{
						{"Sort(test.pk ASC)"},
						{" └─ IndexedTableAccess(test)"},
						{"     ├─ index: [test.pk,test.v1]"},
						{"     ├─ filters: [{[NULL, ∞), (NULL, f)}, {[NULL, ∞), (t, ∞)}]"},
						{"     └─ columns: [pk v1]"},
					},
				},
				{
					Query:            "DELETE FROM test WHERE v1 = 'twelve'",
					SkipResultsCheck: true,
				},
				{
					Query:    "SELECT * FROM test WHERE v1 = 'twelve' ORDER BY pk;",
					Expected: []sql.Row{},
				},
			},
		},
		{
			Name: "String primary key ordering",
			SetUpScript: []string{
				"create table t (s varchar(5) primary key);",
				"insert into t values ('foo');",
				"insert into t values ('bar');",
				"insert into t values ('baz');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    "select * from t order by s;",
					Expected: []sql.Row{{"bar"}, {"baz"}, {"foo"}},
				},
			},
		},
		{
			Name: "Unique Covering Index",
			SetUpScript: []string{
				"CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT);",
				"INSERT INTO test VALUES (13, 3), (11, 1), (15, 5), (12, 2), (14, 4);",
				"CREATE unique INDEX v1_idx ON test(v1);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT * FROM test WHERE v1 > 2 ORDER BY pk;",
					Expected: []sql.Row{
						{13, 3},
						{14, 4},
						{15, 5},
					},
				},
				{
					Query:       "insert into test values (16, 3);",
					ExpectedErr: "duplicate unique key given",
				},
			},
		},
		{
			Name: "Covering Composite Index",
			SetUpScript: []string{
				"CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT);",
				"INSERT INTO test VALUES (13, 3, 23), (11, 1, 21), (15, 5, 25), (12, 2, 22), (14, 4, 24), (16, 2, 25);",
				"CREATE INDEX v1_v2_idx ON test(v1, v2);",
				"CREATE TABLE jointable (v3 bigint, v4 bigint)",
				"INSERT INTO jointable VALUES (2, 22)",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT * FROM test WHERE v1 = 2 AND v2 = 22 ORDER BY pk;",
					Expected: []sql.Row{
						{12, 2, 22},
					},
				},
				{
					Query: "explain SELECT * FROM test WHERE v1 = 2 AND v2 = 22 ORDER BY pk;",
					Expected: []sql.Row{
						{"Sort(test.pk ASC)"},
						{" └─ IndexedTableAccess(test)"},
						{"     ├─ index: [test.v1,test.v2]"},
						{"     ├─ filters: [{[2, 2], [22, 22]}]"},
						{"     └─ columns: [pk v1 v2]"},
					},
				},
				{
					Query: "select /*+ lookup_join(jointable, test) */ HINT * from test join jointable on test.v1 = jointable.v3 and test.v2 = 22 order by 1",
					Expected: []sql.Row{
						{12, 2, 22, 2, 22},
					},
				},
				{
					Query: "explain select * from test join jointable on test.v1 = jointable.v3 and test.v2 = 22 order by 1",
					Expected: []sql.Row{
						{"InnerJoin"},
						{" ├─ (test.v1 = jointable.v3 AND test.v2 = 22)"},
						{" ├─ IndexedTableAccess(test)"},
						{" │   ├─ index: [test.pk]"},
						{" │   ├─ filters: [{[NULL, ∞)}]"},
						{" │   └─ columns: [pk v1 v2]"},
						{" └─ Table"},
						{"     ├─ name: jointable"},
						{"     └─ columns: [v3 v4]"},
					},
				},
				{
					Query: "select * from test join jointable on test.v1 = jointable.v3 and test.v2 = jointable.v4 order by 1",
					Expected: []sql.Row{
						{12, 2, 22, 2, 22},
					},
				},
				{
					Query: "explain select * from test join jointable on test.v1 = jointable.v3 and test.v2 = jointable.v4 order by 1",
					Expected: []sql.Row{
						{"InnerJoin"},
						{" ├─ (test.v1 = jointable.v3 AND test.v2 = jointable.v4)"},
						{" ├─ IndexedTableAccess(test)"},
						{" │   ├─ index: [test.pk]"},
						{" │   ├─ filters: [{[NULL, ∞)}]"},
						{" │   └─ columns: [pk v1 v2]"},
						{" └─ Table"},
						{"     ├─ name: jointable"},
						{"     └─ columns: [v3 v4]"},
					},
				},
				{
					Query: "SELECT * FROM test WHERE v1 > 2 AND v2 = 24 ORDER BY pk;",
					Expected: []sql.Row{
						{14, 4, 24},
					},
				},
				{
					Query: "SELECT * FROM test WHERE v1 >= 4 AND v2 = 25 ORDER BY pk;",
					Expected: []sql.Row{
						{15, 5, 25},
					},
				},
				{
					Query: "SELECT * FROM test WHERE v1 < 3 AND v2 = 21 ORDER BY pk;",
					Expected: []sql.Row{
						{11, 1, 21},
					},
				},
				{
					Query: "SELECT * FROM test WHERE v1 <= 3 AND v2 = 22 ORDER BY pk;",
					Expected: []sql.Row{
						{12, 2, 22},
					},
				},
				{
					Query: "SELECT * FROM test WHERE v1 = 2 AND v2 < 23 ORDER BY pk;",
					Expected: []sql.Row{
						{12, 2, 22},
					},
				},
				{
					Query:    "SELECT * FROM test WHERE v1 = 2 AND v2 < 22 ORDER BY pk;",
					Expected: []sql.Row{},
				},
				{
					Query: "SELECT * FROM test WHERE v1 > 2 AND v2 < 25 ORDER BY pk;",
					Expected: []sql.Row{
						{13, 3, 23},
						{14, 4, 24},
					},
				},
				{
					Query: "SELECT * FROM test WHERE v1 >= 4 AND v2 <= 24 ORDER BY pk;",
					Expected: []sql.Row{
						{14, 4, 24},
					},
				},
				{
					Query: "SELECT * FROM test WHERE v1 < 3 AND v2 < 22 ORDER BY pk;",
					Expected: []sql.Row{
						{11, 1, 21},
					},
				},
				{
					Query: "SELECT * FROM test WHERE v1 <= 3 AND v2 < 23 ORDER BY pk;",
					Expected: []sql.Row{
						{11, 1, 21},
						{12, 2, 22},
					},
				},
			},
		},
		{
			// TODO: lookups when the join key is specified by a subquery
			Name: "Covering Composite Index join, different types",
			SetUpScript: []string{
				"CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 smallint, v2 smallint);",
				"INSERT INTO test VALUES (13, 3, 23), (11, 1, 21), (15, 5, 25), (12, 2, 22), (14, 4, 24), (16, 2, 25);",
				"CREATE INDEX v1_v2_idx ON test(v1, v2);",
				"CREATE TABLE jointable (v3 bigint, v4 bigint)",
				"INSERT INTO jointable VALUES (2, 22)",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "select /*+ lookup_join(jointable, test) */ HINT * from test join jointable on test.v1 = jointable.v3 and test.v2 = 22 order by 1",
					Expected: []sql.Row{
						{12, 2, 22, 2, 22},
					},
				},
				{
					Query: "explain select /*+ lookup_join(jointable, test) */ HINT * from test join jointable on test.v1 = jointable.v3 and test.v2 = 22 order by 1",
					Expected: []sql.Row{
						{"Project"},
						{" ├─ columns: [test.pk, test.v1, test.v2, jointable.v3, jointable.v4]"},
						{" └─ Sort(test.pk ASC)"},
						{"     └─ LookupJoin"},
						{"         ├─ Table"},
						{"         │   ├─ name: jointable"},
						{"         │   └─ columns: [v3 v4]"},
						{"         └─ IndexedTableAccess(test)"},
						{"             ├─ index: [test.v1,test.v2]"},
						{"             ├─ columns: [pk v1 v2]"},
						{"             └─ keys: jointable.v3, 22"},
					},
				},
				{
					Query: "select /*+ lookup_join(jointable, test) */ HINT * from test join jointable on test.v1 = jointable.v3 and test.v2 = jointable.v4 order by 1",
					Expected: []sql.Row{
						{12, 2, 22, 2, 22},
					},
				},
				{
					Query: "explain select /*+ lookup_join(jointable, test) */ HINT * from test join jointable on test.v1 = jointable.v3 and test.v2 = jointable.v4 order by 1",
					Expected: []sql.Row{
						{"Project"},
						{" ├─ columns: [test.pk, test.v1, test.v2, jointable.v3, jointable.v4]"},
						{" └─ Sort(test.pk ASC)"},
						{"     └─ LookupJoin"},
						{"         ├─ Table"},
						{"         │   ├─ name: jointable"},
						{"         │   └─ columns: [v3 v4]"},
						{"         └─ IndexedTableAccess(test)"},
						{"             ├─ index: [test.v1,test.v2]"},
						{"             ├─ columns: [pk v1 v2]"},
						{"             └─ keys: jointable.v3, jointable.v4"},
					},
				},
			},
		},
		{
			Name: "Covering Composite Index join, different types out of range",
			SetUpScript: []string{
				"CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 smallint, v2 smallint);",
				// The zero value in the last row is important because it catches an error mode in index lookup creation failure
				"INSERT INTO test VALUES (13, 3, 23), (11, 1, 21), (14, 0, 22)",
				"CREATE INDEX v1_v2_idx ON test(v1, v2);",
				"CREATE TABLE jointable (v3 bigint, v4 bigint)",
				"INSERT INTO jointable VALUES (2147483648, 2147483649), (1, 21)",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    "select /*+ lookup_join(jointable, test) */ HINT * from test join jointable on test.v1 = jointable.v3 and test.v2 = 22 order by 1",
					Expected: []sql.Row{},
				},
				{
					Query: "select /*+ lookup_join(jointable, test) */ HINT * from test join jointable on test.v1 = jointable.v3 and test.v2 = 21 order by 1",
					Expected: []sql.Row{
						{11, 1, 21, 1, 21},
					},
				},
				{
					Query: "explain select * from test join jointable on test.v1 = jointable.v3 and test.v2 = 22 order by 1",
					Expected: []sql.Row{
						{"InnerJoin"},
						{" ├─ (test.v1 = jointable.v3 AND test.v2 = 22)"},
						{" ├─ IndexedTableAccess(test)"},
						{" │   ├─ index: [test.pk]"},
						{" │   ├─ filters: [{[NULL, ∞)}]"},
						{" │   └─ columns: [pk v1 v2]"},
						{" └─ Table"},
						{"     ├─ name: jointable"},
						{"     └─ columns: [v3 v4]"},
					},
				},
			},
		},
		{
			Name: "Covering Composite Index join, subquery",
			SetUpScript: []string{
				"CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 smallint, v2 smallint);",
				"INSERT INTO test VALUES (13, 3, 23), (11, 1, 21), (14, 0, 22)",
				"CREATE INDEX v1_v2_idx ON test(v1, v2);",
				"CREATE TABLE jointable (v3 bigint, v4 bigint)",
				"INSERT INTO jointable VALUES (2, 22), (1, 21), (2147483648, 22)",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "select /*+ lookup_join(sq, test) */ HINT * from test join " +
						"(select * from jointable) sq " +
						"on test.v1 = sq.v3 and test.v2 = sq.v4 order by 1",
					Expected: []sql.Row{
						{11, 1, 21, 1, 21},
					},
				},
				{
					Query: "explain select * from test join (select * from jointable) sq on test.v1 = sq.v3 and test.v2 = sq.v4 order by 1",
					Expected: []sql.Row{
						{"InnerJoin"},
						{" ├─ (test.v1 = sq.v3 AND test.v2 = sq.v4)"},
						{" ├─ IndexedTableAccess(test)"},
						{" │   ├─ index: [test.pk]"},
						{" │   ├─ filters: [{[NULL, ∞)}]"},
						{" │   └─ columns: [pk v1 v2]"},
						{" └─ TableAlias(sq)"},
						{"     └─ Table"},
						{"         ├─ name: jointable"},
						{"         └─ columns: [v3 v4]"},
					},
				},
				{
					Query: "explain select /*+ lookup_join(sq, test) */ HINT * from test join (select * from jointable) sq on test.v1 = sq.v3 and test.v2 = sq.v4 order by 1",
					Expected: []sql.Row{
						{"Project"},
						{" ├─ columns: [test.pk, test.v1, test.v2, sq.v3, sq.v4]"},
						{" └─ Sort(test.pk ASC)"},
						{"     └─ LookupJoin"},
						{"         ├─ TableAlias(sq)"},
						{"         │   └─ Table"},
						{"         │       ├─ name: jointable"},
						{"         │       └─ columns: [v3 v4]"},
						{"         └─ IndexedTableAccess(test)"},
						{"             ├─ index: [test.v1,test.v2]"},
						{"             ├─ columns: [pk v1 v2]"},
						{"             └─ keys: sq.v3, sq.v4"},
					},
				},
			},
		},
		{
			Name: "Covering Index Multiple AND",
			SetUpScript: []string{
				"CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT);",
				"INSERT INTO test VALUES (13, 3), (11, 1), (15, 5), (12, 2), (14, 4);",
				"CREATE INDEX v1_idx ON test(v1);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    "SELECT * FROM test WHERE v1 = 2 AND v1 = '3' ORDER BY pk;",
					Expected: []sql.Row{},
				},
				{
					Query: "SELECT * FROM test WHERE v1 > 2 AND v1 > '3' ORDER BY pk;",
					Expected: []sql.Row{
						{14, 4},
						{15, 5},
					},
				},
				{
					Query: "SELECT * FROM test WHERE v1 >= 3 AND v1 <= 4.0 ORDER BY pk;",
					Expected: []sql.Row{
						{13, 3},
						{14, 4},
					},
				},
				{
					Query:    "SELECT * FROM test WHERE v1 < 3 AND v1 > 3::float8 ORDER BY pk;",
					Expected: []sql.Row{},
				},
				{
					Query: "SELECT * FROM test WHERE v1 <= 3 AND v1 = 1 ORDER BY pk;",
					Expected: []sql.Row{
						{11, 1},
					},
				},
			},
		},
		{
			Name: "Covering Index BETWEEN",
			SetUpScript: []string{
				"CREATE TABLE test (pk FLOAT8 PRIMARY KEY, v1 FLOAT8);",
				"INSERT INTO test VALUES (13, 3), (11, 1), (17, 7);",
				"CREATE INDEX v1_idx ON test(v1);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT * FROM test WHERE v1 BETWEEN 1 AND 4 ORDER BY pk;",
					Expected: []sql.Row{
						{float64(11), float64(1)},
						{float64(13), float64(3)},
					},
				},
				{
					Query: "SELECT * FROM test WHERE v1 BETWEEN 2 AND 4 ORDER BY pk;",
					Expected: []sql.Row{
						{float64(13), float64(3)},
					},
				},
				{
					Query:    "SELECT * FROM test WHERE v1 BETWEEN 4 AND 2 ORDER BY pk;",
					Expected: []sql.Row{},
				},
				{
					Query: "SELECT * FROM test WHERE v1 BETWEEN SYMMETRIC 1 AND 4 ORDER BY pk;",
					Expected: []sql.Row{
						{float64(11), float64(1)},
						{float64(13), float64(3)},
					},
				},
				{
					Query: "SELECT * FROM test WHERE v1 BETWEEN SYMMETRIC 2 AND 4 ORDER BY pk;",
					Expected: []sql.Row{
						{float64(13), float64(3)},
					},
				},
				{
					Query: "SELECT * FROM test WHERE v1 BETWEEN SYMMETRIC 4 AND 2 ORDER BY pk;",
					Expected: []sql.Row{
						{float64(13), float64(3)},
					},
				},
				{
					Query: "SELECT * FROM test WHERE v1 NOT BETWEEN 1 AND 4 ORDER BY pk;",
					Expected: []sql.Row{
						{float64(17), float64(7)},
					},
				},
				{
					Query: "SELECT * FROM test WHERE v1 NOT BETWEEN 2 AND 4 ORDER BY pk;",
					Expected: []sql.Row{
						{float64(11), float64(1)},
						{float64(17), float64(7)},
					},
				},
				{
					Query: "SELECT * FROM test WHERE v1 NOT BETWEEN 4 AND 2 ORDER BY pk;",
					Expected: []sql.Row{
						{float64(11), float64(1)},
						{float64(13), float64(3)},
						{float64(17), float64(7)},
					},
				},
				{
					Query: "SELECT * FROM test WHERE v1 NOT BETWEEN SYMMETRIC 1 AND 4 ORDER BY pk;",
					Expected: []sql.Row{
						{float64(17), float64(7)},
					},
				},
				{
					Query: "SELECT * FROM test WHERE v1 NOT BETWEEN SYMMETRIC 2 AND 4 ORDER BY pk;",
					Expected: []sql.Row{
						{float64(11), float64(1)},
						{float64(17), float64(7)},
					},
				},
				{
					Query: "SELECT * FROM test WHERE v1 NOT BETWEEN SYMMETRIC 4 AND 2 ORDER BY pk;",
					Expected: []sql.Row{
						{float64(11), float64(1)},
						{float64(17), float64(7)},
					},
				},
			},
		},
		{
			Name: "Covering Index IN",
			SetUpScript: []string{
				"CREATE TABLE test(pk INT4 PRIMARY KEY, v1 INT4, v2 INT4);",
				"INSERT INTO test VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4), (5, 5, 5);",
				"CREATE INDEX v1_idx ON test(v1);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT * FROM test WHERE v1 IN (2, '3', 4) ORDER BY v1;",
					Expected: []sql.Row{
						{2, 2, 2},
						{3, 3, 3},
						{4, 4, 4},
					},
				},
				{
					Query: "explain SELECT * FROM test WHERE v1 IN (2, '3', 4) ORDER BY v1;",
					Expected: []sql.Row{
						{"IndexedTableAccess(test)"},
						{" ├─ index: [test.v1]"},
						{" ├─ filters: [{[2, 2]}, {[3, 3]}, {[4, 4]}]"},
						{" └─ columns: [pk v1 v2]"},
					},
				},
				{
					Query:    "CREATE INDEX v2_idx ON test(v2);",
					Expected: []sql.Row{},
				},
				{
					Query: "SELECT * FROM test WHERE v2 IN (2, '3', 4) ORDER BY v1;",
					Expected: []sql.Row{
						{2, 2, 2},
						{3, 3, 3},
						{4, 4, 4},
					},
				},
			},
		},
		{
			Name: "Non-Covering Index",
			SetUpScript: []string{
				"CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT);",
				"INSERT INTO test VALUES (13, 3, 23), (11, 1, 21), (15, 5, 25), (12, 2, 22), (14, 4, 24);",
				"CREATE INDEX v1_idx ON test(v1);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT * FROM test WHERE v1 = 2 ORDER BY pk;",
					Expected: []sql.Row{
						{12, 2, 22},
					},
				},
				{
					Query: "SELECT * FROM test WHERE v1 > 2 ORDER BY pk;",
					Expected: []sql.Row{
						{13, 3, 23},
						{14, 4, 24},
						{15, 5, 25},
					},
				},
				{
					Query: "SELECT * FROM test WHERE v1 >= 4 ORDER BY pk;",
					Expected: []sql.Row{
						{14, 4, 24},
						{15, 5, 25},
					},
				},
				{
					Query: "SELECT * FROM test WHERE v1 < 3 ORDER BY pk;",
					Expected: []sql.Row{
						{11, 1, 21},
						{12, 2, 22},
					},
				},
				{
					Query: "SELECT * FROM test WHERE v1 <= 3 ORDER BY pk;",
					Expected: []sql.Row{
						{11, 1, 21},
						{12, 2, 22},
						{13, 3, 23},
					},
				},
			},
		},
		{
			Name: "Unique Non-Covering Index",
			SetUpScript: []string{
				"CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT);",
				"INSERT INTO test VALUES (13, 3, 23), (11, 1, 21), (15, 5, 25), (12, 2, 22), (14, 4, 24);",
				"CREATE UNIQUE INDEX v1_idx ON test(v1);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT * FROM test WHERE v1 > 2 ORDER BY pk;",
					Expected: []sql.Row{
						{13, 3, 23},
						{14, 4, 24},
						{15, 5, 25},
					},
				},
				{
					Query:       "insert into test values (16, 3, 23);",
					ExpectedErr: "duplicate unique key given",
				},
			},
		},
		{
			Name: "Non-Covering Composite Index",
			SetUpScript: []string{
				"CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT, v3 BIGINT);",
				"INSERT INTO test VALUES (13, 3, 23, 33), (11, 1, 21, 31), (15, 5, 25, 35), (12, 2, 22, 32), (14, 4, 24, 34);",
				"CREATE INDEX v1_idx ON test(v1, v2);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT * FROM test WHERE v1 = 2 AND v2 = 22 ORDER BY pk;",
					Expected: []sql.Row{
						{12, 2, 22, 32},
					},
				},
				{
					Query: "SELECT * FROM test WHERE v1 > 2 AND v2 = 24 ORDER BY pk;",
					Expected: []sql.Row{
						{14, 4, 24, 34},
					},
				},
				{
					Query: "SELECT * FROM test WHERE v1 >= 4 AND v2 = 25 ORDER BY pk;",
					Expected: []sql.Row{
						{15, 5, 25, 35},
					},
				},
				{
					Query: "SELECT * FROM test WHERE v1 < 3 AND v2 = 21 ORDER BY pk;",
					Expected: []sql.Row{
						{11, 1, 21, 31},
					},
				},
				{
					Query: "SELECT * FROM test WHERE v1 <= 3 AND v2 = 22 ORDER BY pk;",
					Expected: []sql.Row{
						{12, 2, 22, 32},
					},
				},
				{
					Query: "SELECT * FROM test WHERE v1 = 2 AND v2 < 23 ORDER BY pk;",
					Expected: []sql.Row{
						{12, 2, 22, 32},
					},
				},
				{
					Query:    "SELECT * FROM test WHERE v1 = 2 AND v2 < 22 ORDER BY pk;",
					Expected: []sql.Row{},
				},
				{
					Query: "SELECT * FROM test WHERE v1 > 2 AND v2 < 25 ORDER BY pk;",
					Expected: []sql.Row{
						{13, 3, 23, 33},
						{14, 4, 24, 34},
					},
				},
				{
					Query: "SELECT * FROM test WHERE v1 >= 4 AND v2 <= 24 ORDER BY pk;",
					Expected: []sql.Row{
						{14, 4, 24, 34},
					},
				},
				{
					Query: "SELECT * FROM test WHERE v1 < 3 AND v2 < 22 ORDER BY pk;",
					Expected: []sql.Row{
						{11, 1, 21, 31},
					},
				},
				{
					Query: "SELECT * FROM test WHERE v1 <= 3 AND v2 < 23 ORDER BY pk;",
					Expected: []sql.Row{
						{11, 1, 21, 31},
						{12, 2, 22, 32},
					},
				},
			},
		},
		{
			Name: "Unique Non-Covering Composite Index",
			SetUpScript: []string{
				"CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT, v3 BIGINT);",
				"INSERT INTO test VALUES (13, 3, 23, 33), (11, 1, 21, 31), (15, 5, 25, 35), (12, 2, 22, 32), (14, 4, 24, 34);",
				"CREATE UNIQUE INDEX v1_idx ON test(v1, v2);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT * FROM test WHERE v1 < 3 AND v2 = 21 ORDER BY pk;",
					Expected: []sql.Row{
						{11, 1, 21, 31},
					},
				},
				{
					Query: "SELECT * FROM test WHERE v1 <= 3 AND v2 < 23 ORDER BY pk;",
					Expected: []sql.Row{
						{11, 1, 21, 31},
						{12, 2, 22, 32},
					},
				},
				{
					Query:       "insert into test values (16, 3, 23, 33);",
					ExpectedErr: "duplicate unique key given",
				},
			},
		},
		{
			Name: "Keyless Index",
			SetUpScript: []string{
				"CREATE TABLE test (v0 BIGINT, v1 BIGINT, v2 BIGINT);",
				"INSERT INTO test VALUES (13, 3, 23), (11, 1, 21), (15, 5, 25), (12, 2, 22), (14, 4, 24);",
				"CREATE INDEX v1_idx ON test(v1);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT * FROM test WHERE v1 = 2 ORDER BY v0;",
					Expected: []sql.Row{
						{12, 2, 22},
					},
				},
				{
					Query: "SELECT * FROM test WHERE v1 > 2 ORDER BY v0;",
					Expected: []sql.Row{
						{13, 3, 23},
						{14, 4, 24},
						{15, 5, 25},
					},
				},
				{
					Query: "SELECT * FROM test WHERE v1 >= 4 ORDER BY v0;",
					Expected: []sql.Row{
						{14, 4, 24},
						{15, 5, 25},
					},
				},
				{
					Query: "SELECT * FROM test WHERE v1 < 3 ORDER BY v0;",
					Expected: []sql.Row{
						{11, 1, 21},
						{12, 2, 22},
					},
				},
				{
					Query: "SELECT * FROM test WHERE v1 <= 3 ORDER BY v0;",
					Expected: []sql.Row{
						{11, 1, 21},
						{12, 2, 22},
						{13, 3, 23},
					},
				},
			},
		},
		{
			Name: "Unique Keyless Index",
			SetUpScript: []string{
				"CREATE TABLE test (v0 BIGINT, v1 BIGINT, v2 BIGINT);",
				"INSERT INTO test VALUES (13, 3, 23), (11, 1, 21), (15, 5, 25), (12, 2, 22), (14, 4, 24);",
				"CREATE UNIQUE INDEX v1_idx ON test(v1);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT * FROM test WHERE v1 = 2 ORDER BY v0;",
					Expected: []sql.Row{
						{12, 2, 22},
					},
				},
				{
					Query: "SELECT * FROM test WHERE v1 > 2 ORDER BY v0;",
					Expected: []sql.Row{
						{13, 3, 23},
						{14, 4, 24},
						{15, 5, 25},
					},
				},
				{
					Query:       "INSERT INTO test VALUES (16, 3, 23);",
					ExpectedErr: "duplicate unique key given",
				},
			},
		},
		{
			Name: "Keyless Composite Index",
			SetUpScript: []string{
				"CREATE TABLE test (v0 BIGINT, v1 BIGINT, v2 BIGINT, v3 BIGINT);",
				"INSERT INTO test VALUES (13, 3, 23, 33), (11, 1, 21, 31), (15, 5, 25, 35), (12, 2, 22, 32), (14, 4, 24, 34);",
				"CREATE INDEX v1_idx ON test(v1, v2);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT * FROM test WHERE v1 = 2 AND v2 = 22 ORDER BY v0;",
					Expected: []sql.Row{
						{12, 2, 22, 32},
					},
				},
				{
					Query: "SELECT * FROM test WHERE v1 > 2 AND v2 = 24 ORDER BY v0;",
					Expected: []sql.Row{
						{14, 4, 24, 34},
					},
				},
				{
					Query: "SELECT * FROM test WHERE v1 >= 4 AND v2 = 25 ORDER BY v0;",
					Expected: []sql.Row{
						{15, 5, 25, 35},
					},
				},
				{
					Query: "SELECT * FROM test WHERE v1 < 3 AND v2 = 21 ORDER BY v0;",
					Expected: []sql.Row{
						{11, 1, 21, 31},
					},
				},
				{
					Query: "SELECT * FROM test WHERE v1 <= 3 AND v2 = 22 ORDER BY v0;",
					Expected: []sql.Row{
						{12, 2, 22, 32},
					},
				},
				{
					Query: "SELECT * FROM test WHERE v1 = 2 AND v2 < 23 ORDER BY v0;",
					Expected: []sql.Row{
						{12, 2, 22, 32},
					},
				},
				{
					Query:    "SELECT * FROM test WHERE v1 = 2 AND v2 < 22 ORDER BY v0;",
					Expected: []sql.Row{},
				},
				{
					Query: "SELECT * FROM test WHERE v1 > 2 AND v2 < 25 ORDER BY v0;",
					Expected: []sql.Row{
						{13, 3, 23, 33},
						{14, 4, 24, 34},
					},
				},
				{
					Query: "SELECT * FROM test WHERE v1 >= 4 AND v2 <= 24 ORDER BY v0;",
					Expected: []sql.Row{
						{14, 4, 24, 34},
					},
				},
				{
					Query: "SELECT * FROM test WHERE v1 < 3 AND v2 < 22 ORDER BY v0;",
					Expected: []sql.Row{
						{11, 1, 21, 31},
					},
				},
				{
					Query: "SELECT * FROM test WHERE v1 <= 3 AND v2 < 23 ORDER BY v0;",
					Expected: []sql.Row{
						{11, 1, 21, 31},
						{12, 2, 22, 32},
					},
				},
			},
		},
		{
			Name: "Unique Keyless Composite Index",
			SetUpScript: []string{
				"CREATE TABLE test (v0 BIGINT, v1 BIGINT, v2 BIGINT, v3 BIGINT);",
				"INSERT INTO test VALUES (13, 3, 23, 33), (11, 1, 21, 31), (15, 5, 25, 35), (12, 2, 22, 32), (14, 4, 24, 34);",
				"CREATE UNIQUE INDEX v1_idx ON test(v1, v2);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT * FROM test WHERE v1 = 2 AND v2 < 23 ORDER BY v0;",
					Expected: []sql.Row{
						{12, 2, 22, 32},
					},
				},
				{
					Query: "SELECT * FROM test WHERE v1 <= 3 AND v2 < 23 ORDER BY v0;",
					Expected: []sql.Row{
						{11, 1, 21, 31},
						{12, 2, 22, 32},
					},
				},
				{
					Query:       "insert into test values (16, 3, 23, 33);",
					ExpectedErr: "duplicate unique key given",
				},
			},
		},
		{
			Name: "Indexed Join Covering Indexes",
			SetUpScript: []string{
				"CREATE TABLE test1 (pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT);",
				"CREATE TABLE test2 (pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT);",
				"INSERT INTO test1 VALUES (13, 3, 23), (11, 1, 21), (15, 5, 25), (12, 2, 22), (14, 4, 24);",
				"INSERT INTO test2 VALUES (33, 3, 43), (31, 1, 41), (35, 5, 45), (32, 2, 42), (37, 7, 47);",
				"CREATE INDEX v1_idx ON test1(v1);",
				"CREATE INDEX v2_idx ON test2(v1);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT t1.pk, t2.pk FROM test1 t1 JOIN test2 t2 ON t1.v1 = t2.v1 ORDER BY t1.v1;",
					Expected: []sql.Row{
						{11, 31},
						{12, 32},
						{13, 33},
						{15, 35},
					},
				},
				{
					Query: "SELECT t1.pk, t2.pk FROM test1 t1, test2 t2 WHERE t1.v1 = t2.v1 ORDER BY t1.v1;",
					Expected: []sql.Row{
						{11, 31},
						{12, 32},
						{13, 33},
						{15, 35},
					},
				},
			},
		},
		{
			Name: "Unsupported options",
			SetUpScript: []string{
				"CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 varchar);",
				"CREATE INDEX v1_idx_existing ON test(v1);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       "CREATE INDEX v1_idx ON test(v1 varchar_pattern_ops) WITH (storage_opt1 = foo) TABLESPACE tablespace_name;",
					ExpectedErr: "index storage parameter storage_opt1 is not yet supported",
				},
				{
					Query:       "CREATE INDEX v1_idx2 ON test using hash (v1);",
					ExpectedErr: "index method hash is not yet supported",
				},
				{
					Query:       "CREATE INDEX v1_idx_storage ON test(v1) WITH (definitely_not_supported = 1);",
					ExpectedErr: "index storage parameter definitely_not_supported is not yet supported",
				},
				{
					Query:       "CREATE INDEX v1_idx_tablespace ON test(v1) TABLESPACE definitely_not_supported;",
					ExpectedErr: "TABLESPACE is not yet supported for indexes",
				},
				{
					Query:       "ALTER INDEX v1_idx_storage SET (definitely_not_supported = 1);",
					ExpectedErr: "index storage parameter definitely_not_supported is not yet supported",
				},
				{
					Query:       "ALTER INDEX v1_idx_storage SET TABLESPACE definitely_not_supported;",
					ExpectedErr: "TABLESPACE is not yet supported for indexes",
				},
				{
					Query:       "ALTER INDEX v1_idx_existing ALTER COLUMN 1 SET STATISTICS 100;",
					ExpectedErr: `cannot alter statistics on non-expression column "v1" of index "v1_idx_existing"`,
				},
			},
		},
		unsupportedAccessMethodBoundaryScript("hash", "hash", "hashhandler"),
		unsupportedAccessMethodBoundaryScript("GiST", "gist", "gisthandler"),
		unsupportedAccessMethodBoundaryScript("SP-GiST", "spgist", "spghandler"),
		unsupportedAccessMethodBoundaryScript("BRIN", "brin", "brinhandler"),
		{
			Name: "PostgreSQL btree reloptions metadata",
			SetUpScript: []string{
				"CREATE TABLE btree_reloptions_meta (id INTEGER PRIMARY KEY, v TEXT);",
				"CREATE INDEX btree_reloptions_meta_v_idx ON btree_reloptions_meta (v) WITH (fillfactor = 70);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT
	pg_catalog.pg_get_indexdef(c.oid),
	pg_catalog.pg_get_indexdef(c.oid, 1, false),
	CAST(c.reloptions AS TEXT)
FROM pg_catalog.pg_class c
WHERE c.relname = 'btree_reloptions_meta_v_idx';`,
					Expected: []sql.Row{
						{"CREATE INDEX btree_reloptions_meta_v_idx ON public.btree_reloptions_meta USING btree (v) WITH (fillfactor='70')", "v", "{fillfactor=70}"},
					},
				},
				{
					Query: `SELECT indexdef
FROM pg_catalog.pg_indexes
WHERE tablename = 'btree_reloptions_meta' AND indexname = 'btree_reloptions_meta_v_idx';`,
					Expected: []sql.Row{
						{"CREATE INDEX btree_reloptions_meta_v_idx ON public.btree_reloptions_meta USING btree (v) WITH (fillfactor='70')"},
					},
				},
				{
					Query:       "CREATE INDEX btree_reloptions_bad_name_idx ON btree_reloptions_meta (v) WITH (definitely_not_supported = 1);",
					ExpectedErr: "index storage parameter definitely_not_supported is not yet supported",
				},
				{
					Query:       "CREATE INDEX btree_reloptions_bad_fillfactor_idx ON btree_reloptions_meta (v) WITH (fillfactor = 9);",
					ExpectedErr: "fillfactor must be between 10 and 100",
				},
			},
		},
		{
			Name: "PostgreSQL alter index fillfactor metadata",
			SetUpScript: []string{
				"CREATE TABLE alter_index_fillfactor (id INTEGER PRIMARY KEY, v TEXT, code INTEGER, owned INTEGER UNIQUE);",
				"INSERT INTO alter_index_fillfactor VALUES (1, 'a', 10, 100), (2, 'b', 20, 200), (3, 'c', 30, 300);",
				"CREATE INDEX alter_index_fillfactor_v_idx ON alter_index_fillfactor (v);",
				"CREATE UNIQUE INDEX alter_index_fillfactor_code_idx ON alter_index_fillfactor (code);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "ALTER INDEX alter_index_fillfactor_v_idx SET (fillfactor = 80);",
				},
				{
					Query: "ALTER INDEX alter_index_fillfactor_code_idx SET (fillfactor = 75);",
				},
				{
					Query: `SELECT
	c.relname,
	pg_catalog.pg_get_indexdef(c.oid),
	CAST(c.reloptions AS TEXT)
FROM pg_catalog.pg_class c
WHERE c.relname IN ('alter_index_fillfactor_v_idx', 'alter_index_fillfactor_code_idx')
ORDER BY c.relname;`,
					Expected: []sql.Row{
						{"alter_index_fillfactor_code_idx", "CREATE UNIQUE INDEX alter_index_fillfactor_code_idx ON public.alter_index_fillfactor USING btree (code) WITH (fillfactor='75')", "{fillfactor=75}"},
						{"alter_index_fillfactor_v_idx", "CREATE INDEX alter_index_fillfactor_v_idx ON public.alter_index_fillfactor USING btree (v) WITH (fillfactor='80')", "{fillfactor=80}"},
					},
				},
				{
					Query: "ALTER INDEX alter_index_fillfactor_v_idx RESET (fillfactor);",
				},
				{
					Query: `SELECT
	pg_catalog.pg_get_indexdef(c.oid),
	c.reloptions IS NULL
FROM pg_catalog.pg_class c
WHERE c.relname = 'alter_index_fillfactor_v_idx';`,
					Expected: []sql.Row{
						{"CREATE INDEX alter_index_fillfactor_v_idx ON public.alter_index_fillfactor USING btree (v)", "t"},
					},
				},
				{
					Query:    "SELECT id, v, code, owned FROM alter_index_fillfactor ORDER BY code;",
					Expected: []sql.Row{{1, "a", 10, 100}, {2, "b", 20, 200}, {3, "c", 30, 300}},
				},
				{
					Query:       "ALTER INDEX alter_index_fillfactor_v_idx SET (fillfactor = 9);",
					ExpectedErr: "fillfactor must be between 10 and 100",
				},
				{
					Query:       "ALTER INDEX alter_index_fillfactor_v_idx SET (definitely_not_supported = 1);",
					ExpectedErr: "index storage parameter definitely_not_supported is not yet supported",
				},
				{
					Query:       "ALTER INDEX alter_index_fillfactor_pkey SET (fillfactor = 80);",
					ExpectedErr: "ALTER INDEX storage parameters for constraint-backed indexes are not yet supported",
				},
				{
					Query:       "ALTER INDEX alter_index_fillfactor_owned_key SET (fillfactor = 80);",
					ExpectedErr: "ALTER INDEX storage parameters for constraint-backed indexes are not yet supported",
				},
			},
		},
		{
			Name: "PostgreSQL default tablespace index metadata",
			SetUpScript: []string{
				"CREATE TABLE btree_default_tablespace_meta (id INTEGER PRIMARY KEY, v TEXT);",
				"CREATE INDEX btree_default_tablespace_meta_v_idx ON btree_default_tablespace_meta (v) TABLESPACE pg_default;",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT
	idx.indexdef,
	c.reltablespace,
	idx.tablespace
FROM pg_catalog.pg_indexes idx
JOIN pg_catalog.pg_class c ON c.relname = idx.indexname
WHERE idx.tablename = 'btree_default_tablespace_meta'
  AND idx.indexname = 'btree_default_tablespace_meta_v_idx';`,
					Expected: []sql.Row{
						{"CREATE INDEX btree_default_tablespace_meta_v_idx ON public.btree_default_tablespace_meta USING btree (v)", 0, nil},
					},
				},
				{
					Query:       "CREATE INDEX btree_custom_tablespace_meta_v_idx ON btree_default_tablespace_meta (v) TABLESPACE definitely_not_supported;",
					ExpectedErr: "TABLESPACE is not yet supported for indexes",
				},
			},
		},
		{
			Name: "PostgreSQL alter index default tablespace",
			SetUpScript: []string{
				"CREATE TABLE alter_index_default_tablespace (id INTEGER PRIMARY KEY, v TEXT);",
				"CREATE INDEX alter_index_default_tablespace_v_idx ON alter_index_default_tablespace (v);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "ALTER INDEX alter_index_default_tablespace_v_idx SET TABLESPACE pg_default;",
				},
				{
					Query: "ALTER INDEX alter_index_default_tablespace_pkey SET TABLESPACE pg_default;",
				},
				{
					Query: "ALTER INDEX IF EXISTS alter_index_default_tablespace_missing_idx SET TABLESPACE pg_default;",
				},
				{
					Query: `SELECT
	idx.indexname,
	idx.indexdef,
	c.reltablespace,
	idx.tablespace
FROM pg_catalog.pg_indexes idx
JOIN pg_catalog.pg_class c ON c.relname = idx.indexname
WHERE idx.tablename = 'alter_index_default_tablespace'
ORDER BY idx.indexname;`,
					Expected: []sql.Row{
						{"alter_index_default_tablespace_pkey", "CREATE UNIQUE INDEX alter_index_default_tablespace_pkey ON public.alter_index_default_tablespace USING btree (id)", 0, nil},
						{"alter_index_default_tablespace_v_idx", "CREATE INDEX alter_index_default_tablespace_v_idx ON public.alter_index_default_tablespace USING btree (v)", 0, nil},
					},
				},
				{
					Query:       "ALTER INDEX alter_index_default_tablespace_v_idx SET TABLESPACE definitely_not_supported;",
					ExpectedErr: "TABLESPACE is not yet supported for indexes",
				},
			},
		},
		{
			Name: "PostgreSQL btree partial index metadata",
			SetUpScript: []string{
				"CREATE TABLE btree_partial_meta (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, b TEXT);",
				"INSERT INTO btree_partial_meta VALUES (1, 5, 'x'), (2, 15, 'y'), (3, 25, NULL);",
				"CREATE INDEX btree_partial_meta_a_idx ON btree_partial_meta (a) WHERE a > 10 AND b IS NOT NULL;",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT
	i.indkey,
	i.indpred IS NOT NULL,
	pg_catalog.pg_get_expr(i.indpred, i.indrelid),
	pg_catalog.pg_get_indexdef(i.indexrelid)
FROM pg_catalog.pg_index i
JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
WHERE c.relname = 'btree_partial_meta_a_idx';`,
					Expected: []sql.Row{
						{
							"2",
							"t",
							"(a > 10) AND (b IS NOT NULL)",
							"CREATE INDEX btree_partial_meta_a_idx ON public.btree_partial_meta USING btree (a) WHERE (a > 10) AND (b IS NOT NULL)",
						},
					},
				},
				{
					Query: `SELECT indexdef
FROM pg_catalog.pg_indexes
WHERE tablename = 'btree_partial_meta'
  AND indexname = 'btree_partial_meta_a_idx';`,
					Expected: []sql.Row{
						{"CREATE INDEX btree_partial_meta_a_idx ON public.btree_partial_meta USING btree (a) WHERE (a > 10) AND (b IS NOT NULL)"},
					},
				},
				{
					Query: `SELECT id
FROM btree_partial_meta
WHERE a > 10
ORDER BY id;`,
					Expected: []sql.Row{
						{2},
						{3},
					},
				},
				{
					Query:       "CREATE INDEX btree_partial_missing_idx ON btree_partial_meta (a) WHERE missing > 0;",
					ExpectedErr: "key column 'missing' doesn't exist in table",
				},
				{
					Query:       "CREATE UNIQUE INDEX btree_partial_unique_idx ON btree_partial_meta (a) WHERE b IS NOT NULL;",
					ExpectedErr: "unique partial indexes are not yet supported",
				},
			},
		},
		{
			Name: "PostgreSQL btree INCLUDE index metadata",
			SetUpScript: []string{
				"CREATE TABLE btree_include_meta (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, b TEXT NOT NULL, c TEXT NOT NULL);",
				"INSERT INTO btree_include_meta VALUES (1, 10, 'x', 'cx'), (2, 20, 'y', 'cy'), (3, 10, 'z', 'cz');",
				"CREATE INDEX btree_include_meta_a_idx ON btree_include_meta (a) INCLUDE (b, c);",
				"CREATE TABLE btree_include_unique_meta (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, b TEXT NOT NULL);",
				"CREATE UNIQUE INDEX btree_include_unique_meta_a_idx ON btree_include_unique_meta (a) INCLUDE (b);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT
	i.indnatts,
	i.indnkeyatts,
	i.indkey,
	i.indcollation,
	i.indclass,
	i.indoption,
	pg_catalog.pg_get_indexdef(i.indexrelid),
	pg_catalog.pg_get_indexdef(i.indexrelid, 1, true),
	pg_catalog.pg_get_indexdef(i.indexrelid, 2, true),
	pg_catalog.pg_get_indexdef(i.indexrelid, 3, true)
FROM pg_catalog.pg_index i
JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
WHERE c.relname = 'btree_include_meta_a_idx';`,
					Expected: []sql.Row{
						{
							3,
							1,
							"2 3 4",
							collationOidVector(""),
							opClassOidVector("int4_ops"),
							"0",
							"CREATE INDEX btree_include_meta_a_idx ON public.btree_include_meta USING btree (a) INCLUDE (b, c)",
							"a",
							"b",
							"c",
						},
					},
				},
				{
					Query: `SELECT id
FROM btree_include_meta
WHERE a = 10
ORDER BY id;`,
					Expected: []sql.Row{
						{1},
						{3},
					},
				},
				{
					Query: "INSERT INTO btree_include_unique_meta VALUES (1, 10, 'x');",
				},
				{
					Query:       "INSERT INTO btree_include_unique_meta VALUES (2, 10, 'y');",
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: "INSERT INTO btree_include_unique_meta VALUES (3, 11, 'y');",
				},
				{
					Query:       "CREATE INDEX btree_include_missing_idx ON btree_include_meta (a) INCLUDE (missing);",
					ExpectedErr: "key column 'missing' doesn't exist in table",
				},
				{
					Query:       "CREATE INDEX btree_include_expr_idx ON btree_include_meta (a) INCLUDE ((lower(b)));",
					ExpectedErr: "syntax error",
				},
			},
		},
		{
			Name: "PostgreSQL drop index restrict lifecycle",
			SetUpScript: []string{
				"CREATE TABLE drop_index_restrict (id INTEGER PRIMARY KEY, v INTEGER);",
				"INSERT INTO drop_index_restrict VALUES (1, 10), (2, 20);",
				"CREATE INDEX drop_index_restrict_idx ON drop_index_restrict (v);",
				"CREATE INDEX drop_index_restrict_cascade_idx ON drop_index_restrict (v);",
			},
			Assertions: []ScriptTestAssertion{
				{
					// CONCURRENTLY is silently downgraded to a synchronous build.
					// PostgreSQL builds the index without holding a strong
					// table lock; doltgres takes the same lock as a regular
					// CREATE INDEX. We accept the keyword so that ORM
					// migration tooling that emits CONCURRENTLY (Drizzle,
					// Prisma, Alembic, Rails) does not error.
					Query: "CREATE INDEX CONCURRENTLY drop_index_restrict_concurrent_idx ON drop_index_restrict (v);",
				},
				{
					Query: `SELECT COUNT(*)
FROM pg_catalog.pg_class
WHERE relname = 'drop_index_restrict_concurrent_idx';`,
					Expected: []sql.Row{{1}},
				},
				{
					Query: "DROP INDEX CONCURRENTLY drop_index_restrict_concurrent_idx;",
				},
				{
					Query: `SELECT COUNT(*)
FROM pg_catalog.pg_class
WHERE relname = 'drop_index_restrict_concurrent_idx';`,
					Expected: []sql.Row{{0}},
				},
				{
					Query: "REINDEX INDEX CONCURRENTLY drop_index_restrict_idx;",
				},
				{
					Query: "REINDEX INDEX drop_index_restrict_idx;",
				},
				{
					Query: "REINDEX TABLE drop_index_restrict;",
				},
				{
					Query: `SELECT pg_catalog.pg_get_indexdef(c.oid)
FROM pg_catalog.pg_class c
WHERE c.relname = 'drop_index_restrict_idx';`,
					Expected: []sql.Row{
						{"CREATE INDEX drop_index_restrict_idx ON public.drop_index_restrict USING btree (v)"},
					},
				},
				{
					Query: "SELECT id FROM drop_index_restrict WHERE v = 20;",
					Expected: []sql.Row{
						{2},
					},
				},
				{
					Query:       "REINDEX INDEX drop_index_restrict_missing_idx;",
					ExpectedErr: `index "drop_index_restrict_missing_idx" does not exist`,
				},
				{
					Query:       "REINDEX TABLE drop_index_restrict_missing;",
					ExpectedErr: "table not found: drop_index_restrict_missing",
				},
				{
					Query: "DROP INDEX drop_index_restrict_idx RESTRICT;",
				},
				{
					Query: "DROP INDEX drop_index_restrict_cascade_idx CASCADE;",
				},
				{
					Query: `SELECT indexname
FROM pg_catalog.pg_indexes
WHERE tablename = 'drop_index_restrict'
ORDER BY indexname;`,
					Expected: []sql.Row{
						{"drop_index_restrict_pkey"},
					},
				},
				{
					Query: "DROP INDEX IF EXISTS drop_index_restrict_missing_idx RESTRICT;",
				},
				{
					Query:       "DROP INDEX drop_index_restrict_pkey RESTRICT;",
					ExpectedErr: `because constraint "drop_index_restrict_pkey" on table "drop_index_restrict" requires it`,
				},
				{
					Query:       "DROP INDEX drop_index_restrict_pkey CASCADE;",
					ExpectedErr: `because constraint "drop_index_restrict_pkey" on table "drop_index_restrict" requires it`,
				},
			},
		},
		{
			Name: "PostgreSQL multi drop index lifecycle",
			SetUpScript: []string{
				"CREATE TABLE drop_index_multi (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER);",
				"CREATE INDEX drop_index_multi_a_idx ON drop_index_multi (a);",
				"CREATE INDEX drop_index_multi_b_idx ON drop_index_multi (b);",
				"CREATE INDEX drop_index_multi_c_idx ON drop_index_multi (c);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "DROP INDEX drop_index_multi_a_idx, drop_index_multi_b_idx;",
				},
				{
					Query: `SELECT indexname
FROM pg_catalog.pg_indexes
WHERE tablename = 'drop_index_multi'
ORDER BY indexname;`,
					Expected: []sql.Row{
						{"drop_index_multi_c_idx"},
						{"drop_index_multi_pkey"},
					},
				},
				{
					Query: "DROP INDEX IF EXISTS drop_index_multi_missing_idx, drop_index_multi_c_idx;",
				},
				{
					Query: `SELECT indexname
FROM pg_catalog.pg_indexes
WHERE tablename = 'drop_index_multi'
ORDER BY indexname;`,
					Expected: []sql.Row{
						{"drop_index_multi_pkey"},
					},
				},
			},
		},
		{
			Name: "PostgreSQL drop index respects unique constraint ownership",
			SetUpScript: []string{
				"CREATE TABLE drop_index_unique_constraint (id INTEGER PRIMARY KEY, email TEXT, code TEXT);",
				"CREATE UNIQUE INDEX drop_index_unique_constraint_email_idx ON drop_index_unique_constraint (email);",
				"ALTER TABLE drop_index_unique_constraint ADD CONSTRAINT drop_index_unique_constraint_code_key UNIQUE (code);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       "DROP INDEX drop_index_unique_constraint_code_key RESTRICT;",
					ExpectedErr: `because constraint "drop_index_unique_constraint_code_key" on table "drop_index_unique_constraint" requires it`,
				},
				{
					Query: "DROP INDEX drop_index_unique_constraint_email_idx RESTRICT;",
				},
				{
					Query: `SELECT indexname
FROM pg_catalog.pg_indexes
WHERE tablename = 'drop_index_unique_constraint'
ORDER BY indexname;`,
					Expected: []sql.Row{
						{"drop_index_unique_constraint_code_key"},
						{"drop_index_unique_constraint_pkey"},
					},
				},
			},
		},
		{
			Name: "PostgreSQL unique nulls not distinct",
			SetUpScript: []string{
				"CREATE TABLE unique_nulls_not_distinct (id INTEGER PRIMARY KEY, v INTEGER, w INTEGER);",
				"CREATE TABLE unique_nulls_not_distinct_batch (id INTEGER PRIMARY KEY, v INTEGER);",
				"CREATE TABLE unique_nulls_not_distinct_update_batch (id INTEGER PRIMARY KEY, v INTEGER);",
				"CREATE TABLE unique_nulls_distinct_default (id INTEGER PRIMARY KEY, v INTEGER);",
				"CREATE TABLE unique_nulls_column_constraint (id INTEGER PRIMARY KEY, v INTEGER UNIQUE NULLS NOT DISTINCT);",
				"CREATE TABLE unique_nulls_table_constraint (id INTEGER PRIMARY KEY, v INTEGER, UNIQUE NULLS NOT DISTINCT (v));",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "CREATE UNIQUE INDEX unique_nulls_not_distinct_idx ON unique_nulls_not_distinct (v) NULLS NOT DISTINCT;",
				},
				{
					Query: "CREATE UNIQUE INDEX unique_nulls_not_distinct_multi_idx ON unique_nulls_not_distinct (v, w) NULLS NOT DISTINCT;",
				},
				{
					Query: "CREATE UNIQUE INDEX unique_nulls_not_distinct_batch_idx ON unique_nulls_not_distinct_batch (v) NULLS NOT DISTINCT;",
				},
				{
					Query: "CREATE UNIQUE INDEX unique_nulls_not_distinct_update_batch_idx ON unique_nulls_not_distinct_update_batch (v) NULLS NOT DISTINCT;",
				},
				{
					Query: "CREATE UNIQUE INDEX unique_nulls_distinct_default_idx ON unique_nulls_distinct_default (v);",
				},
				{
					Query: "INSERT INTO unique_nulls_not_distinct VALUES (1, NULL, 10);",
				},
				{
					Query:       "INSERT INTO unique_nulls_not_distinct VALUES (2, NULL, 11);",
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: "INSERT INTO unique_nulls_not_distinct VALUES (3, 20, NULL);",
				},
				{
					Query:       "INSERT INTO unique_nulls_not_distinct VALUES (4, 20, NULL);",
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: "INSERT INTO unique_nulls_not_distinct VALUES (5, 21, NULL);",
				},
				{
					Query:       "UPDATE unique_nulls_not_distinct SET v = NULL WHERE id = 5;",
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query:       "INSERT INTO unique_nulls_not_distinct_batch VALUES (1, NULL), (2, NULL);",
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: "INSERT INTO unique_nulls_not_distinct_update_batch VALUES (1, 100), (2, 101);",
				},
				{
					Query:       "UPDATE unique_nulls_not_distinct_update_batch SET v = NULL;",
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: "INSERT INTO unique_nulls_distinct_default VALUES (1, NULL), (2, NULL);",
				},
				{
					Query: "INSERT INTO unique_nulls_column_constraint VALUES (1, NULL);",
				},
				{
					Query:       "INSERT INTO unique_nulls_column_constraint VALUES (2, NULL);",
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: "INSERT INTO unique_nulls_table_constraint VALUES (1, NULL);",
				},
				{
					Query:       "INSERT INTO unique_nulls_table_constraint VALUES (2, NULL);",
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: `SELECT c.relname, i.indnullsnotdistinct
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid
WHERE c.relname IN (
	'unique_nulls_not_distinct_idx',
	'unique_nulls_not_distinct_multi_idx',
	'unique_nulls_not_distinct_batch_idx',
	'unique_nulls_not_distinct_update_batch_idx',
	'unique_nulls_distinct_default_idx',
	'unique_nulls_column_constraint_v_key',
	'unique_nulls_table_constraint_v_key'
)
ORDER BY c.relname;`,
					Expected: []sql.Row{
						{"unique_nulls_column_constraint_v_key", "t"},
						{"unique_nulls_distinct_default_idx", "f"},
						{"unique_nulls_not_distinct_batch_idx", "t"},
						{"unique_nulls_not_distinct_idx", "t"},
						{"unique_nulls_not_distinct_multi_idx", "t"},
						{"unique_nulls_not_distinct_update_batch_idx", "t"},
						{"unique_nulls_table_constraint_v_key", "t"},
					},
				},
				{
					Query: `SELECT indexname
FROM pg_catalog.pg_indexes
WHERE indexdef LIKE '%NULLS NOT DISTINCT%'
ORDER BY indexname;`,
					Expected: []sql.Row{
						{"unique_nulls_column_constraint_v_key"},
						{"unique_nulls_not_distinct_batch_idx"},
						{"unique_nulls_not_distinct_idx"},
						{"unique_nulls_not_distinct_multi_idx"},
						{"unique_nulls_not_distinct_update_batch_idx"},
						{"unique_nulls_table_constraint_v_key"},
					},
				},
			},
		},
		{
			Name: "PostgreSQL primary key index rename unsupported boundary",
			SetUpScript: []string{
				"CREATE TABLE rename_primary_key_index (id INTEGER PRIMARY KEY, v INTEGER);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       "ALTER INDEX rename_primary_key_index_pkey RENAME TO rename_primary_key_index_id_key;",
					ExpectedErr: "renaming primary key indexes is not yet supported",
				},
				{
					Query:       "DROP INDEX rename_primary_key_index_pkey;",
					ExpectedErr: `because constraint "rename_primary_key_index_pkey" on table "rename_primary_key_index" requires it`,
				},
				{
					Query: "ALTER TABLE rename_primary_key_index DROP CONSTRAINT rename_primary_key_index_pkey;",
				},
				{
					Query: `SELECT con.conname, con.contype
FROM pg_catalog.pg_constraint con
JOIN pg_catalog.pg_class cls ON cls.oid = con.conrelid
WHERE cls.relname = 'rename_primary_key_index';`,
					Expected: []sql.Row{},
				},
			},
		},
		{
			Name: "PostgreSQL custom primary key constraint names",
			SetUpScript: []string{
				`CREATE TABLE primary_key_constraint_table_named (
					id INTEGER,
					name TEXT,
					CONSTRAINT primary_key_constraint_table_custom PRIMARY KEY (id)
				);`,
				`CREATE TABLE primary_key_constraint_column_named (
					id INTEGER CONSTRAINT primary_key_constraint_column_custom PRIMARY KEY,
					name TEXT
				);`,
				"CREATE TABLE primary_key_constraint_alter_named (id INTEGER NOT NULL, name TEXT);",
				"ALTER TABLE primary_key_constraint_alter_named ADD CONSTRAINT primary_key_constraint_alter_custom PRIMARY KEY (id);",
				"CREATE TABLE primary_key_constraint_default (id INTEGER PRIMARY KEY, name TEXT);",
				"CREATE TABLE primary_key_constraint_add_column_named (name TEXT);",
				`ALTER TABLE primary_key_constraint_add_column_named
					ADD COLUMN id INTEGER NOT NULL,
					ADD CONSTRAINT primary_key_constraint_add_column_custom PRIMARY KEY (id);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT cls.relname, con.conname, con.contype
FROM pg_catalog.pg_constraint con
JOIN pg_catalog.pg_class cls ON cls.oid = con.conrelid
WHERE cls.relname IN (
	'primary_key_constraint_table_named',
	'primary_key_constraint_column_named',
	'primary_key_constraint_alter_named',
	'primary_key_constraint_add_column_named',
	'primary_key_constraint_default'
)
ORDER BY cls.relname;`,
					Expected: []sql.Row{
						{"primary_key_constraint_add_column_named", "primary_key_constraint_add_column_custom", "p"},
						{"primary_key_constraint_alter_named", "primary_key_constraint_alter_custom", "p"},
						{"primary_key_constraint_column_named", "primary_key_constraint_column_custom", "p"},
						{"primary_key_constraint_default", "primary_key_constraint_default_pkey", "p"},
						{"primary_key_constraint_table_named", "primary_key_constraint_table_custom", "p"},
					},
				},
				{
					Query: `SELECT tablename, indexname
FROM pg_catalog.pg_indexes
WHERE tablename IN (
	'primary_key_constraint_table_named',
	'primary_key_constraint_column_named',
	'primary_key_constraint_alter_named',
	'primary_key_constraint_add_column_named',
	'primary_key_constraint_default'
)
ORDER BY tablename;`,
					Expected: []sql.Row{
						{"primary_key_constraint_add_column_named", "primary_key_constraint_add_column_custom"},
						{"primary_key_constraint_alter_named", "primary_key_constraint_alter_custom"},
						{"primary_key_constraint_column_named", "primary_key_constraint_column_custom"},
						{"primary_key_constraint_default", "primary_key_constraint_default_pkey"},
						{"primary_key_constraint_table_named", "primary_key_constraint_table_custom"},
					},
				},
				{
					Query: `SELECT cls.relname, idx.indisunique, idx.indisprimary
FROM pg_catalog.pg_class cls
JOIN pg_catalog.pg_index idx ON idx.indexrelid = cls.oid
WHERE cls.relname IN (
	'primary_key_constraint_table_custom',
	'primary_key_constraint_column_custom',
	'primary_key_constraint_alter_custom',
	'primary_key_constraint_add_column_custom'
)
ORDER BY cls.relname;`,
					Expected: []sql.Row{
						{"primary_key_constraint_add_column_custom", "t", "t"},
						{"primary_key_constraint_alter_custom", "t", "t"},
						{"primary_key_constraint_column_custom", "t", "t"},
						{"primary_key_constraint_table_custom", "t", "t"},
					},
				},
				{
					Query: `SELECT c.relname, pg_catalog.pg_get_indexdef(c.oid)
FROM pg_catalog.pg_class c
WHERE c.relname IN (
	'primary_key_constraint_table_custom',
	'primary_key_constraint_column_custom',
	'primary_key_constraint_alter_custom',
	'primary_key_constraint_add_column_custom'
)
ORDER BY c.relname;`,
					Expected: []sql.Row{
						{"primary_key_constraint_add_column_custom", "CREATE UNIQUE INDEX primary_key_constraint_add_column_custom ON public.primary_key_constraint_add_column_named USING btree (id)"},
						{"primary_key_constraint_alter_custom", "CREATE UNIQUE INDEX primary_key_constraint_alter_custom ON public.primary_key_constraint_alter_named USING btree (id)"},
						{"primary_key_constraint_column_custom", "CREATE UNIQUE INDEX primary_key_constraint_column_custom ON public.primary_key_constraint_column_named USING btree (id)"},
						{"primary_key_constraint_table_custom", "CREATE UNIQUE INDEX primary_key_constraint_table_custom ON public.primary_key_constraint_table_named USING btree (id)"},
					},
				},
				{
					Query: `SELECT
	'primary_key_constraint_table_custom'::regclass::text,
	'primary_key_constraint_column_custom'::regclass::text,
	'primary_key_constraint_alter_custom'::regclass::text,
	'primary_key_constraint_add_column_custom'::regclass::text;`,
					Expected: []sql.Row{{
						"primary_key_constraint_table_custom",
						"primary_key_constraint_column_custom",
						"primary_key_constraint_alter_custom",
						"primary_key_constraint_add_column_custom",
					}},
				},
				{
					Query:       "ALTER INDEX primary_key_constraint_table_custom RENAME TO primary_key_constraint_table_other;",
					ExpectedErr: "renaming primary key indexes is not yet supported",
				},
				{
					Query:       "DROP INDEX primary_key_constraint_column_custom;",
					ExpectedErr: `because constraint "primary_key_constraint_column_custom" on table "primary_key_constraint_column_named" requires it`,
				},
				{
					Query:       "ALTER INDEX primary_key_constraint_default_pkey RENAME TO primary_key_constraint_default_other;",
					ExpectedErr: "renaming primary key indexes is not yet supported",
				},
				{
					Query: "ALTER TABLE primary_key_constraint_table_named DROP CONSTRAINT primary_key_constraint_table_custom;",
				},
				{
					Query: "ALTER TABLE primary_key_constraint_column_named DROP CONSTRAINT primary_key_constraint_column_custom;",
				},
				{
					Query: "ALTER TABLE primary_key_constraint_alter_named DROP CONSTRAINT primary_key_constraint_alter_custom;",
				},
				{
					Query: "ALTER TABLE primary_key_constraint_add_column_named DROP CONSTRAINT primary_key_constraint_add_column_custom;",
				},
				{
					Query: `SELECT cls.relname, con.conname
FROM pg_catalog.pg_constraint con
JOIN pg_catalog.pg_class cls ON cls.oid = con.conrelid
WHERE cls.relname IN (
	'primary_key_constraint_table_named',
	'primary_key_constraint_column_named',
	'primary_key_constraint_alter_named',
	'primary_key_constraint_add_column_named'
)
ORDER BY cls.relname;`,
					Expected: []sql.Row{},
				},
				{
					Query: "ALTER TABLE primary_key_constraint_alter_named ADD PRIMARY KEY (id);",
				},
				{
					Query: `SELECT indexname
FROM pg_catalog.pg_indexes
WHERE tablename = 'primary_key_constraint_alter_named';`,
					Expected: []sql.Row{
						{"primary_key_constraint_alter_named_pkey"},
					},
				},
			},
		},
		{
			Name: "PostgreSQL btree sort option metadata",
			SetUpScript: []string{
				"CREATE TABLE index_sort_meta (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER);",
				"CREATE INDEX index_sort_meta_idx ON index_sort_meta (a DESC, b ASC NULLS FIRST);",
				"CREATE INDEX index_sort_nulls_last_idx ON index_sort_meta (a ASC NULLS LAST, b DESC NULLS FIRST, id DESC NULLS LAST);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT
	pg_catalog.pg_get_indexdef(c.oid),
	pg_catalog.pg_get_indexdef(c.oid, 1, false),
	pg_catalog.pg_get_indexdef(c.oid, 2, false)
FROM pg_catalog.pg_class c
WHERE c.relname = 'index_sort_meta_idx';`,
					Expected: []sql.Row{
						{"CREATE INDEX index_sort_meta_idx ON public.index_sort_meta USING btree (a DESC, b NULLS FIRST)", "a DESC", "b NULLS FIRST"},
					},
				},
				{
					Query: `SELECT
	pg_catalog.pg_get_indexdef(c.oid),
	pg_catalog.pg_get_indexdef(c.oid, 1, false),
	pg_catalog.pg_get_indexdef(c.oid, 2, false),
	pg_catalog.pg_get_indexdef(c.oid, 3, false)
FROM pg_catalog.pg_class c
WHERE c.relname = 'index_sort_nulls_last_idx';`,
					Expected: []sql.Row{
						{"CREATE INDEX index_sort_nulls_last_idx ON public.index_sort_meta USING btree (a, b DESC, id DESC NULLS LAST)", "a", "b DESC", "id DESC NULLS LAST"},
					},
				},
				{
					Query: `SELECT unnest(i.indoption)
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid
WHERE c.relname = 'index_sort_meta_idx';`,
					Expected: []sql.Row{
						{3},
						{2},
					},
				},
				{
					Query: `SELECT unnest(i.indoption)
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid
WHERE c.relname = 'index_sort_nulls_last_idx';`,
					Expected: []sql.Row{
						{0},
						{3},
						{1},
					},
				},
				{
					Query: `SELECT c.relname, i.indclass
FROM pg_catalog.pg_index i
JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
WHERE c.relname IN ('index_sort_meta_idx', 'index_sort_meta_pkey', 'index_sort_nulls_last_idx')
ORDER BY c.relname;`,
					Expected: []sql.Row{
						{"index_sort_meta_idx", opClassOidVector("int4_ops", "int4_ops")},
						{"index_sort_meta_pkey", opClassOidVector("int4_ops")},
						{"index_sort_nulls_last_idx", opClassOidVector("int4_ops", "int4_ops", "int4_ops")},
					},
				},
				{
					Query: `SELECT opc.opcname, am.amname, typ.typname, opc.opcdefault
FROM pg_catalog.pg_opclass opc
JOIN pg_catalog.pg_am am ON am.oid = opc.opcmethod
JOIN pg_catalog.pg_type typ ON typ.oid = opc.opcintype
WHERE opc.opcname = 'int4_ops';`,
					Expected: []sql.Row{
						{"int4_ops", "btree", "int4", "t"},
					},
				},
				{
					Query: `SELECT i.indnatts, i.indnkeyatts
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid
WHERE c.relname = 'index_sort_meta_idx';`,
					Expected: []sql.Row{
						{2, 2},
					},
				},
				{
					Query: `SELECT i.indisunique, i.indimmediate
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid
WHERE c.relname = 'index_sort_meta_pkey';`,
					Expected: []sql.Row{
						{"t", "t"},
					},
				},
				{
					Query: `SELECT indexrelname, idx_scan, last_idx_scan, idx_tup_read, idx_tup_fetch
FROM pg_catalog.pg_stat_user_indexes
WHERE relname = 'index_sort_meta'
ORDER BY indexrelname;`,
					Expected: []sql.Row{
						{"index_sort_meta_idx", 0, nil, 0, 0},
						{"index_sort_meta_pkey", 0, nil, 0, 0},
						{"index_sort_nulls_last_idx", 0, nil, 0, 0},
					},
				},
				{
					Query: `SELECT indexrelname, idx_blks_read, idx_blks_hit
FROM pg_catalog.pg_statio_user_indexes
WHERE relname = 'index_sort_meta'
ORDER BY indexrelname;`,
					Expected: []sql.Row{
						{"index_sort_meta_idx", 0, 0},
						{"index_sort_meta_pkey", 0, 0},
						{"index_sort_nulls_last_idx", 0, 0},
					},
				},
			},
		},
		{
			Name: "PostgreSQL standalone unique indexes are not constraints",
			SetUpScript: []string{
				"CREATE TABLE unique_index_constraint_boundary (id INTEGER PRIMARY KEY, email TEXT, code TEXT);",
				"CREATE UNIQUE INDEX unique_index_constraint_boundary_email_idx ON unique_index_constraint_boundary (email);",
				"ALTER TABLE unique_index_constraint_boundary ADD CONSTRAINT unique_index_constraint_boundary_code_key UNIQUE (code);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT con.conname, con.contype
FROM pg_catalog.pg_constraint con
JOIN pg_catalog.pg_class cls ON cls.oid = con.conrelid
WHERE cls.relname = 'unique_index_constraint_boundary'
ORDER BY con.conname;`,
					Expected: []sql.Row{
						{"unique_index_constraint_boundary_code_key", "u"},
						{"unique_index_constraint_boundary_pkey", "p"},
					},
				},
				{
					Query: `SELECT idx.relname, i.indisunique
FROM pg_catalog.pg_index i
JOIN pg_catalog.pg_class idx ON idx.oid = i.indexrelid
JOIN pg_catalog.pg_class tbl ON tbl.oid = i.indrelid
WHERE tbl.relname = 'unique_index_constraint_boundary'
ORDER BY idx.relname;`,
					Expected: []sql.Row{
						{"unique_index_constraint_boundary_code_key", "t"},
						{"unique_index_constraint_boundary_email_idx", "t"},
						{"unique_index_constraint_boundary_pkey", "t"},
					},
				},
				{
					Query:       "ALTER TABLE unique_index_constraint_boundary DROP CONSTRAINT unique_index_constraint_boundary_email_idx;",
					ExpectedErr: `Constraint "unique_index_constraint_boundary_email_idx" does not exist`,
				},
				{
					Query: `SELECT indexname
FROM pg_catalog.pg_indexes
WHERE tablename = 'unique_index_constraint_boundary'
ORDER BY indexname;`,
					Expected: []sql.Row{
						{"unique_index_constraint_boundary_code_key"},
						{"unique_index_constraint_boundary_email_idx"},
						{"unique_index_constraint_boundary_pkey"},
					},
				},
			},
		},
		{
			Name: "PostgreSQL unnamed unique constraints use generated names",
			SetUpScript: []string{
				"CREATE TABLE unique_constraint_default_name (id INTEGER PRIMARY KEY, email TEXT, account_id INTEGER, code TEXT);",
				"ALTER TABLE unique_constraint_default_name ADD UNIQUE (email);",
				"ALTER TABLE unique_constraint_default_name ADD UNIQUE (account_id, code);",
				`CREATE TABLE unique_constraint_create_default_name (
					id INTEGER PRIMARY KEY,
					email TEXT,
					account_id INTEGER,
					code TEXT,
					UNIQUE (email),
					UNIQUE (account_id, code)
				);`,
				`CREATE TABLE unique_constraint_column_default_name (
					id INTEGER PRIMARY KEY,
					email TEXT UNIQUE
				);`,
				`CREATE TABLE unique_constraint_column_named (
					id INTEGER PRIMARY KEY,
					email TEXT CONSTRAINT unique_constraint_column_named_email_custom UNIQUE
				);`,
				"CREATE TABLE unique_constraint_alter_add_column (id INTEGER PRIMARY KEY);",
				"ALTER TABLE unique_constraint_alter_add_column ADD COLUMN email TEXT UNIQUE;",
				"ALTER TABLE unique_constraint_alter_add_column ADD COLUMN code TEXT CONSTRAINT unique_constraint_alter_add_column_code_custom UNIQUE;",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT con.conname, con.contype
FROM pg_catalog.pg_constraint con
JOIN pg_catalog.pg_class cls ON cls.oid = con.conrelid
WHERE cls.relname = 'unique_constraint_default_name'
ORDER BY con.conname;`,
					Expected: []sql.Row{
						{"unique_constraint_default_name_account_id_code_key", "u"},
						{"unique_constraint_default_name_email_key", "u"},
						{"unique_constraint_default_name_pkey", "p"},
					},
				},
				{
					Query: `SELECT indexname
FROM pg_catalog.pg_indexes
WHERE tablename = 'unique_constraint_default_name'
ORDER BY indexname;`,
					Expected: []sql.Row{
						{"unique_constraint_default_name_account_id_code_key"},
						{"unique_constraint_default_name_email_key"},
						{"unique_constraint_default_name_pkey"},
					},
				},
				{
					Query: `SELECT con.conname, con.contype
FROM pg_catalog.pg_constraint con
JOIN pg_catalog.pg_class cls ON cls.oid = con.conrelid
WHERE cls.relname = 'unique_constraint_create_default_name'
ORDER BY con.conname;`,
					Expected: []sql.Row{
						{"unique_constraint_create_default_name_account_id_code_key", "u"},
						{"unique_constraint_create_default_name_email_key", "u"},
						{"unique_constraint_create_default_name_pkey", "p"},
					},
				},
				{
					Query: `SELECT indexname
FROM pg_catalog.pg_indexes
WHERE tablename = 'unique_constraint_create_default_name'
ORDER BY indexname;`,
					Expected: []sql.Row{
						{"unique_constraint_create_default_name_account_id_code_key"},
						{"unique_constraint_create_default_name_email_key"},
						{"unique_constraint_create_default_name_pkey"},
					},
				},
				{
					Query: `SELECT con.conname, con.contype
FROM pg_catalog.pg_constraint con
JOIN pg_catalog.pg_class cls ON cls.oid = con.conrelid
WHERE cls.relname = 'unique_constraint_column_default_name'
ORDER BY con.conname;`,
					Expected: []sql.Row{
						{"unique_constraint_column_default_name_email_key", "u"},
						{"unique_constraint_column_default_name_pkey", "p"},
					},
				},
				{
					Query: `SELECT indexname
FROM pg_catalog.pg_indexes
WHERE tablename = 'unique_constraint_column_default_name'
ORDER BY indexname;`,
					Expected: []sql.Row{
						{"unique_constraint_column_default_name_email_key"},
						{"unique_constraint_column_default_name_pkey"},
					},
				},
				{
					Query: `SELECT con.conname, con.contype
FROM pg_catalog.pg_constraint con
JOIN pg_catalog.pg_class cls ON cls.oid = con.conrelid
WHERE cls.relname = 'unique_constraint_column_named'
ORDER BY con.conname;`,
					Expected: []sql.Row{
						{"unique_constraint_column_named_email_custom", "u"},
						{"unique_constraint_column_named_pkey", "p"},
					},
				},
				{
					Query: `SELECT indexname
FROM pg_catalog.pg_indexes
WHERE tablename = 'unique_constraint_column_named'
ORDER BY indexname;`,
					Expected: []sql.Row{
						{"unique_constraint_column_named_email_custom"},
						{"unique_constraint_column_named_pkey"},
					},
				},
				{
					Query: `SELECT con.conname, con.contype
FROM pg_catalog.pg_constraint con
JOIN pg_catalog.pg_class cls ON cls.oid = con.conrelid
WHERE cls.relname = 'unique_constraint_alter_add_column'
ORDER BY con.conname;`,
					Expected: []sql.Row{
						{"unique_constraint_alter_add_column_code_custom", "u"},
						{"unique_constraint_alter_add_column_email_key", "u"},
						{"unique_constraint_alter_add_column_pkey", "p"},
					},
				},
				{
					Query: `SELECT indexname
FROM pg_catalog.pg_indexes
WHERE tablename = 'unique_constraint_alter_add_column'
ORDER BY indexname;`,
					Expected: []sql.Row{
						{"unique_constraint_alter_add_column_code_custom"},
						{"unique_constraint_alter_add_column_email_key"},
						{"unique_constraint_alter_add_column_pkey"},
					},
				},
				{
					Query: `SELECT cls.relname, idx.indisunique, idx.indisprimary
FROM pg_catalog.pg_class cls
JOIN pg_catalog.pg_index idx ON idx.indexrelid = cls.oid
WHERE cls.relname IN (
	'unique_constraint_alter_add_column_code_custom',
	'unique_constraint_alter_add_column_email_key'
)
ORDER BY cls.relname;`,
					Expected: []sql.Row{
						{"unique_constraint_alter_add_column_code_custom", "t", "f"},
						{"unique_constraint_alter_add_column_email_key", "t", "f"},
					},
				},
				{
					Query: "INSERT INTO unique_constraint_column_default_name VALUES (1, 'hello');",
				},
				{
					Query:       "INSERT INTO unique_constraint_column_default_name VALUES (2, 'hello');",
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: "ALTER TABLE unique_constraint_column_default_name DROP CONSTRAINT unique_constraint_column_default_name_email_key;",
				},
				{
					Query: "INSERT INTO unique_constraint_column_default_name VALUES (2, 'hello');",
				},
				{
					Query: `SELECT indexname
FROM pg_catalog.pg_indexes
WHERE tablename = 'unique_constraint_column_default_name'
ORDER BY indexname;`,
					Expected: []sql.Row{
						{"unique_constraint_column_default_name_pkey"},
					},
				},
				{
					Query: "INSERT INTO unique_constraint_alter_add_column VALUES (1, 'hello', 'code-1');",
				},
				{
					Query:       "INSERT INTO unique_constraint_alter_add_column VALUES (2, 'hello', 'code-2');",
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query:       "INSERT INTO unique_constraint_alter_add_column VALUES (3, 'goodbye', 'code-1');",
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: "ALTER TABLE unique_constraint_alter_add_column DROP CONSTRAINT unique_constraint_alter_add_column_email_key;",
				},
				{
					Query: "INSERT INTO unique_constraint_alter_add_column VALUES (2, 'hello', 'code-2');",
				},
				{
					Query: `SELECT indexname
FROM pg_catalog.pg_indexes
WHERE tablename = 'unique_constraint_alter_add_column'
ORDER BY indexname;`,
					Expected: []sql.Row{
						{"unique_constraint_alter_add_column_code_custom"},
						{"unique_constraint_alter_add_column_pkey"},
					},
				},
			},
		},
		{
			Name: "PostgreSQL btree opclass metadata",
			SetUpScript: []string{
				"CREATE TABLE btree_opclass_meta (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER);",
				"CREATE INDEX btree_opclass_meta_idx ON btree_opclass_meta (a int4_ops, b);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT
	pg_catalog.pg_get_indexdef(c.oid),
	pg_catalog.pg_get_indexdef(c.oid, 1, false),
	pg_catalog.pg_get_indexdef(c.oid, 2, false)
FROM pg_catalog.pg_class c
WHERE c.relname = 'btree_opclass_meta_idx';`,
					Expected: []sql.Row{
						{"CREATE INDEX btree_opclass_meta_idx ON public.btree_opclass_meta USING btree (a int4_ops, b)", "a int4_ops", "b"},
					},
				},
				{
					Query: `SELECT i.indclass
FROM pg_catalog.pg_index i
JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
WHERE c.relname = 'btree_opclass_meta_idx';`,
					Expected: []sql.Row{
						{opClassOidVector("int4_ops", "int4_ops")},
					},
				},
				{
					Query:       "CREATE INDEX btree_opclass_meta_bad_idx ON btree_opclass_meta (a jsonb_path_ops);",
					ExpectedErr: "operator class jsonb_path_ops is not yet supported for btree indexes",
				},
			},
		},
		{
			Name: "PostgreSQL btree opclass type validation",
			SetUpScript: []string{
				"CREATE TABLE btree_opclass_type_validation (id INTEGER PRIMARY KEY, i INTEGER, t TEXT, v VARCHAR, c CHARACTER(12), doc JSONB);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "CREATE INDEX btree_opclass_type_validation_int_idx ON btree_opclass_type_validation (i int4_ops);",
				},
				{
					Query: "CREATE INDEX btree_opclass_type_validation_text_idx ON btree_opclass_type_validation (t text_ops);",
				},
				{
					Query: "CREATE INDEX btree_opclass_type_validation_jsonb_idx ON btree_opclass_type_validation (doc jsonb_ops);",
				},
				{
					Query: "CREATE INDEX btree_opclass_type_validation_varchar_pattern_idx ON btree_opclass_type_validation (v varchar_pattern_ops);",
				},
				{
					Query: "CREATE INDEX btree_opclass_type_validation_bpchar_pattern_idx ON btree_opclass_type_validation (c bpchar_pattern_ops);",
				},
				{
					Query: "CREATE INDEX btree_opclass_type_validation_varchar_bpchar_pattern_idx ON btree_opclass_type_validation (v bpchar_pattern_ops);",
				},
				{
					Query:       "CREATE INDEX btree_opclass_type_validation_text_on_int_bad_idx ON btree_opclass_type_validation (i text_ops);",
					ExpectedErr: `operator class "text_ops" does not accept data type integer`,
				},
				{
					Query:       "CREATE INDEX btree_opclass_type_validation_int_on_text_bad_idx ON btree_opclass_type_validation (t int4_ops);",
					ExpectedErr: `operator class "int4_ops" does not accept data type text`,
				},
				{
					Query:       "CREATE INDEX btree_opclass_type_validation_int_on_jsonb_bad_idx ON btree_opclass_type_validation (doc int4_ops);",
					ExpectedErr: `operator class "int4_ops" does not accept data type jsonb`,
				},
				{
					Query:       "CREATE INDEX btree_opclass_type_validation_varchar_pattern_on_bpchar_bad_idx ON btree_opclass_type_validation (c varchar_pattern_ops);",
					ExpectedErr: `operator class "varchar_pattern_ops" does not accept data type character`,
				},
			},
		},
		{
			Name: "PostgreSQL jsonb btree opclass metadata",
			SetUpScript: []string{
				"CREATE TABLE btree_jsonb_opclass_meta (id INTEGER PRIMARY KEY, doc JSONB NOT NULL);",
				"CREATE INDEX btree_jsonb_default_idx ON btree_jsonb_opclass_meta USING btree (doc);",
				"CREATE INDEX btree_jsonb_explicit_idx ON btree_jsonb_opclass_meta USING btree (doc jsonb_ops);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT
	c.relname,
	pg_catalog.pg_get_indexdef(c.oid),
	pg_catalog.pg_get_indexdef(c.oid, 1, false),
	i.indclass,
	i.indcollation,
	i.indoption
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid
WHERE c.relname IN ('btree_jsonb_default_idx', 'btree_jsonb_explicit_idx')
ORDER BY c.relname;`,
					Expected: []sql.Row{
						{"btree_jsonb_default_idx", "CREATE INDEX btree_jsonb_default_idx ON public.btree_jsonb_opclass_meta USING btree (doc)", "doc", opClassOidVector("jsonb_ops"), collationOidVector(""), "0"},
						{"btree_jsonb_explicit_idx", "CREATE INDEX btree_jsonb_explicit_idx ON public.btree_jsonb_opclass_meta USING btree (doc jsonb_ops)", "doc jsonb_ops", opClassOidVector("jsonb_ops"), collationOidVector(""), "0"},
					},
				},
				{
					Query: `SELECT opc.opcname, am.amname, typ.typname, opc.opcdefault, opc.opckeytype
FROM pg_catalog.pg_opclass opc
JOIN pg_catalog.pg_am am ON am.oid = opc.opcmethod
JOIN pg_catalog.pg_type typ ON typ.oid = opc.opcintype
WHERE opc.opcname = 'jsonb_ops'
ORDER BY am.amname;`,
					Expected: []sql.Row{
						{"jsonb_ops", "btree", "jsonb", "t", 0},
						{"jsonb_ops", "gin", "jsonb", "t", typeOid("text")},
					},
				},
				{
					Query: `SELECT btree_opc.oid <> gin_opc.oid
FROM pg_catalog.pg_opclass btree_opc
JOIN pg_catalog.pg_am btree_am ON btree_am.oid = btree_opc.opcmethod
JOIN pg_catalog.pg_opclass gin_opc ON gin_opc.opcname = btree_opc.opcname
JOIN pg_catalog.pg_am gin_am ON gin_am.oid = gin_opc.opcmethod
WHERE btree_opc.opcname = 'jsonb_ops'
	AND btree_am.amname = 'btree'
	AND gin_am.amname = 'gin';`,
					Expected: []sql.Row{{"t"}},
				},
			},
		},
		{
			Name: "PostgreSQL btree pattern opclass metadata",
			SetUpScript: []string{
				"CREATE TABLE btree_pattern_opclass_meta (id INTEGER PRIMARY KEY, t TEXT, v VARCHAR, c CHARACTER(12));",
				"CREATE INDEX btree_pattern_text_idx ON btree_pattern_opclass_meta (t text_pattern_ops);",
				"CREATE INDEX btree_pattern_varchar_idx ON btree_pattern_opclass_meta (v varchar_pattern_ops);",
				"CREATE INDEX btree_pattern_bpchar_idx ON btree_pattern_opclass_meta (c bpchar_pattern_ops);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT
	c.relname,
	pg_catalog.pg_get_indexdef(c.oid),
	pg_catalog.pg_get_indexdef(c.oid, 1, false),
	i.indclass,
	i.indcollation,
	i.indoption
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid
WHERE c.relname LIKE 'btree_pattern_%_idx'
ORDER BY c.relname;`,
					Expected: []sql.Row{
						{"btree_pattern_bpchar_idx", "CREATE INDEX btree_pattern_bpchar_idx ON public.btree_pattern_opclass_meta USING btree (c bpchar_pattern_ops)", "c bpchar_pattern_ops", opClassOidVector("bpchar_pattern_ops"), collationOidVector("default"), "0"},
						{"btree_pattern_text_idx", "CREATE INDEX btree_pattern_text_idx ON public.btree_pattern_opclass_meta USING btree (t text_pattern_ops)", "t text_pattern_ops", opClassOidVector("text_pattern_ops"), collationOidVector("default"), "0"},
						{"btree_pattern_varchar_idx", "CREATE INDEX btree_pattern_varchar_idx ON public.btree_pattern_opclass_meta USING btree (v varchar_pattern_ops)", "v varchar_pattern_ops", opClassOidVector("varchar_pattern_ops"), collationOidVector("default"), "0"},
					},
				},
				{
					Query: `SELECT opc.opcname, opf.opfname, typ.typname, opc.opcdefault, opc.opckeytype
FROM pg_catalog.pg_opclass opc
JOIN pg_catalog.pg_opfamily opf ON opf.oid = opc.opcfamily
JOIN pg_catalog.pg_type typ ON typ.oid = opc.opcintype
JOIN pg_catalog.pg_am am ON am.oid = opc.opcmethod
WHERE am.amname = 'btree'
	AND opc.opcname IN ('text_pattern_ops', 'varchar_pattern_ops', 'bpchar_pattern_ops')
ORDER BY opc.opcname;`,
					Expected: []sql.Row{
						{"bpchar_pattern_ops", "bpchar_pattern_ops", "bpchar", "f", 0},
						{"text_pattern_ops", "text_pattern_ops", "text", "f", 0},
						{"varchar_pattern_ops", "text_pattern_ops", "text", "f", 0},
					},
				},
			},
		},
		{
			Name: "PostgreSQL scalar btree opclass metadata",
			SetUpScript: []string{
				"CREATE TABLE btree_scalar_opclass_meta (id INTEGER PRIMARY KEY, b BYTEA, o OID, t TIME, tz TIMETZ, i INTERVAL);",
				"CREATE INDEX btree_scalar_opclass_idx ON btree_scalar_opclass_meta (b bytea_ops, o oid_ops, t time_ops, tz timetz_ops, i interval_ops);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT
	pg_catalog.pg_get_indexdef(c.oid),
	pg_catalog.pg_get_indexdef(c.oid, 1, false),
	pg_catalog.pg_get_indexdef(c.oid, 2, false),
	pg_catalog.pg_get_indexdef(c.oid, 3, false),
	pg_catalog.pg_get_indexdef(c.oid, 4, false),
	pg_catalog.pg_get_indexdef(c.oid, 5, false),
	i.indclass,
	i.indcollation,
	i.indoption
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid
WHERE c.relname = 'btree_scalar_opclass_idx';`,
					Expected: []sql.Row{
						{
							"CREATE INDEX btree_scalar_opclass_idx ON public.btree_scalar_opclass_meta USING btree (b bytea_ops, o oid_ops, t time_ops, tz timetz_ops, i interval_ops)",
							"b bytea_ops",
							"o oid_ops",
							"t time_ops",
							"tz timetz_ops",
							"i interval_ops",
							opClassOidVector("bytea_ops", "oid_ops", "time_ops", "timetz_ops", "interval_ops"),
							collationOidVector("", "", "", "", ""),
							"0 0 0 0 0",
						},
					},
				},
			},
		},
		{
			Name: "PostgreSQL system and bitstring btree opclass metadata",
			SetUpScript: []string{
				`CREATE TABLE btree_system_opclass_meta (id INTEGER PRIMARY KEY, b BIT(4), v VARBIT, c "char", ov OIDVECTOR, l PG_LSN);`,
				"CREATE INDEX btree_system_opclass_idx ON btree_system_opclass_meta (b bit_ops, v varbit_ops, c char_ops, ov oidvector_ops, l pg_lsn_ops);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT
	pg_catalog.pg_get_indexdef(c.oid),
	pg_catalog.pg_get_indexdef(c.oid, 1, false),
	pg_catalog.pg_get_indexdef(c.oid, 2, false),
	pg_catalog.pg_get_indexdef(c.oid, 3, false),
	pg_catalog.pg_get_indexdef(c.oid, 4, false),
	pg_catalog.pg_get_indexdef(c.oid, 5, false),
	i.indclass,
	i.indcollation,
	i.indoption
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid
WHERE c.relname = 'btree_system_opclass_idx';`,
					Expected: []sql.Row{
						{
							`CREATE INDEX btree_system_opclass_idx ON public.btree_system_opclass_meta USING btree (b bit_ops, v varbit_ops, c char_ops, ov oidvector_ops, l pg_lsn_ops)`,
							"b bit_ops",
							"v varbit_ops",
							"c char_ops",
							"ov oidvector_ops",
							"l pg_lsn_ops",
							opClassOidVector("bit_ops", "varbit_ops", "char_ops", "oidvector_ops", "pg_lsn_ops"),
							collationOidVector("", "", "", "", ""),
							"0 0 0 0 0",
						},
					},
				},
			},
		},
		{
			Name: "PostgreSQL btree collation metadata",
			SetUpScript: []string{
				"CREATE TABLE btree_collation_meta (id INTEGER PRIMARY KEY, name TEXT, code VARCHAR);",
				"CREATE INDEX btree_collation_meta_idx ON btree_collation_meta (id, name, code);",
				`CREATE INDEX btree_collation_meta_c_idx ON btree_collation_meta (name COLLATE "C", code);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT c.relname, i.indkey, i.indcollation
FROM pg_catalog.pg_index i
JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
WHERE c.relname IN ('btree_collation_meta_idx', 'btree_collation_meta_pkey')
ORDER BY c.relname;`,
					Expected: []sql.Row{
						{"btree_collation_meta_idx", "1 2 3", collationOidVector("", "default", "default")},
						{"btree_collation_meta_pkey", "1", collationOidVector("")},
					},
				},
				{
					Query: `SELECT
	pg_catalog.pg_get_indexdef(c.oid),
	pg_catalog.pg_get_indexdef(c.oid, 1, false),
	pg_catalog.pg_get_indexdef(c.oid, 2, false),
	i.indcollation
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid
WHERE c.relname = 'btree_collation_meta_c_idx';`,
					Expected: []sql.Row{
						{`CREATE INDEX btree_collation_meta_c_idx ON public.btree_collation_meta USING btree (name COLLATE "C", code)`, `name COLLATE "C"`, "code", collationOidVector("C", "default")},
					},
				},
				{
					Query:       `CREATE INDEX btree_collation_meta_bad_idx ON btree_collation_meta (name COLLATE "definitely-not-a-collation");`,
					ExpectedErr: "index collation definitely-not-a-collation is not yet supported",
				},
				{
					Query: `SELECT a.attname, a.attcollation
FROM pg_catalog.pg_attribute a
JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
WHERE c.relname = 'btree_collation_meta' AND a.attname IN ('id', 'name', 'code')
ORDER BY a.attnum;`,
					Expected: []sql.Row{
						{"id", 0},
						{"name", id.Cache().ToOID(id.NewCollation("pg_catalog", "default").AsId())},
						{"code", id.Cache().ToOID(id.NewCollation("pg_catalog", "default").AsId())},
					},
				},
			},
		},
		{
			Name: "PostgreSQL index access method and opclass metadata",
			SetUpScript: []string{
				"CREATE TABLE jsonb_gin_idx (id INTEGER PRIMARY KEY, doc JSONB NOT NULL);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "CREATE INDEX jsonb_gin_ops_idx ON jsonb_gin_idx USING gin (doc);",
				},
				{
					Query: "CREATE INDEX jsonb_gin_path_idx ON jsonb_gin_idx USING gin (doc jsonb_path_ops);",
				},
				{
					Query:       "CREATE INDEX jsonb_gin_bad_idx ON jsonb_gin_idx USING gin (doc jsonb_hash_ops);",
					ExpectedErr: "operator class jsonb_hash_ops is not yet supported for gin indexes",
				},
				{
					Query: `SELECT c.relname, am.amname
	FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_am am ON am.oid = c.relam
WHERE c.relname IN ('jsonb_gin_ops_idx', 'jsonb_gin_path_idx')
ORDER BY c.relname;`,
					Expected: []sql.Row{
						{"jsonb_gin_ops_idx", "gin"},
						{"jsonb_gin_path_idx", "gin"},
					},
				},
				{
					Query: `SELECT indexname, indexdef
FROM pg_catalog.pg_indexes
WHERE tablename = 'jsonb_gin_idx'
ORDER BY indexname;`,
					Expected: []sql.Row{
						{"jsonb_gin_idx_pkey", "CREATE UNIQUE INDEX jsonb_gin_idx_pkey ON public.jsonb_gin_idx USING btree (id)"},
						{"jsonb_gin_ops_idx", "CREATE INDEX jsonb_gin_ops_idx ON public.jsonb_gin_idx USING gin (doc jsonb_ops)"},
						{"jsonb_gin_path_idx", "CREATE INDEX jsonb_gin_path_idx ON public.jsonb_gin_idx USING gin (doc jsonb_path_ops)"},
					},
				},
				{
					Query: `SELECT c.relname,
	pg_catalog.pg_get_indexdef(c.oid),
	pg_catalog.pg_get_indexdef(c.oid, 0, true),
	pg_catalog.pg_get_indexdef(c.oid, 1, false)
FROM pg_catalog.pg_class c
WHERE c.relname IN ('jsonb_gin_idx_pkey', 'jsonb_gin_ops_idx', 'jsonb_gin_path_idx')
ORDER BY c.relname;`,
					Expected: []sql.Row{
						{"jsonb_gin_idx_pkey", "CREATE UNIQUE INDEX jsonb_gin_idx_pkey ON public.jsonb_gin_idx USING btree (id)", "CREATE UNIQUE INDEX jsonb_gin_idx_pkey ON public.jsonb_gin_idx USING btree (id)", "id"},
						{"jsonb_gin_ops_idx", "CREATE INDEX jsonb_gin_ops_idx ON public.jsonb_gin_idx USING gin (doc jsonb_ops)", "CREATE INDEX jsonb_gin_ops_idx ON public.jsonb_gin_idx USING gin (doc jsonb_ops)", "doc jsonb_ops"},
						{"jsonb_gin_path_idx", "CREATE INDEX jsonb_gin_path_idx ON public.jsonb_gin_idx USING gin (doc jsonb_path_ops)", "CREATE INDEX jsonb_gin_path_idx ON public.jsonb_gin_idx USING gin (doc jsonb_path_ops)", "doc jsonb_path_ops"},
					},
				},
				{
					Query: `SELECT c.relname, i.indclass
FROM pg_catalog.pg_index i
JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
WHERE c.relname IN ('jsonb_gin_ops_idx', 'jsonb_gin_path_idx')
ORDER BY c.relname;`,
					Expected: []sql.Row{
						{"jsonb_gin_ops_idx", ginOpClassOidVector("jsonb_ops")},
						{"jsonb_gin_path_idx", ginOpClassOidVector("jsonb_path_ops")},
					},
				},
			},
		},
		{
			Name: "PostgreSQL jsonb gin sidecar backfill",
			SetUpScript: []string{
				"CREATE TABLE jsonb_gin_backfill (id INTEGER PRIMARY KEY, doc JSONB NOT NULL);",
				`INSERT INTO jsonb_gin_backfill VALUES
					(1, '{"a":1,"tags":["x","x"],"empty":{}}'),
					(2, '{"a":2,"tags":["y"],"ok":true}');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "CREATE INDEX jsonb_gin_backfill_idx ON jsonb_gin_backfill USING gin (doc);",
				},
				{
					Query: `SELECT SUM(row_count), COUNT(payload) > 0, COUNT(checksum) > 0
FROM dg_gin_jsonb_gin_backfill_jsonb_gin_backfill_idx_posting_chunks;`,
					Expected: []sql.Row{{float64(12), "t", "t"}},
				},
				{
					Query: `SELECT token, SUM(row_count)
FROM dg_gin_jsonb_gin_backfill_jsonb_gin_backfill_idx_posting_chunks
WHERE token IN ('9:jsonb_ops3:key1:01:a', '9:jsonb_ops3:key1:01:x')
GROUP BY token
ORDER BY token;`,
					Expected: []sql.Row{
						{"9:jsonb_ops3:key1:01:a", float64(2)},
						{"9:jsonb_ops3:key1:01:x", float64(1)},
					},
				},
			},
		},
		{
			Name: "PostgreSQL jsonb gin sidecar DML maintenance",
			SetUpScript: []string{
				"CREATE TABLE jsonb_gin_dml (id INTEGER PRIMARY KEY, doc JSONB NOT NULL);",
				`INSERT INTO jsonb_gin_dml VALUES
					(1, '{"a":1,"tags":["x","x"]}');`,
				"CREATE INDEX jsonb_gin_dml_idx ON jsonb_gin_dml USING gin (doc);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO jsonb_gin_dml VALUES
						(2, '{"a":2,"tags":["y","y"]}');`,
				},
				{
					Query: `SELECT SUM(row_count), COUNT(payload) > 0, COUNT(checksum) > 0
FROM dg_gin_jsonb_gin_dml_jsonb_gin_dml_idx_posting_chunks;`,
					Expected: []sql.Row{{float64(8), "t", "t"}},
				},
				{
					Query: `UPDATE jsonb_gin_dml
SET doc = '{"a":3,"tags":["z"]}'
WHERE id = 1;`,
				},
				{
					Query: `SELECT token, SUM(row_count)
FROM dg_gin_jsonb_gin_dml_jsonb_gin_dml_idx_posting_chunks
WHERE token IN ('9:jsonb_ops3:key1:01:x', '9:jsonb_ops3:key1:01:z')
GROUP BY token
ORDER BY token;`,
					Expected: []sql.Row{
						{"9:jsonb_ops3:key1:01:z", float64(1)},
					},
				},
				{
					Query: "DELETE FROM jsonb_gin_dml WHERE id = 2;",
				},
				{
					Query: `SELECT SUM(row_count), COUNT(payload) > 0, COUNT(checksum) > 0
FROM dg_gin_jsonb_gin_dml_jsonb_gin_dml_idx_posting_chunks;`,
					Expected: []sql.Row{{float64(4), "t", "t"}},
				},
			},
		},
		{
			Name: "PostgreSQL jsonb gin sidecar transaction rollback",
			SetUpScript: []string{
				"CREATE TABLE jsonb_gin_txn (id INTEGER PRIMARY KEY, doc JSONB NOT NULL);",
				`INSERT INTO jsonb_gin_txn VALUES
						(1, '{"a":1,"tags":["x","x"]}');`,
				"CREATE INDEX jsonb_gin_txn_idx ON jsonb_gin_txn USING gin (doc);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "BEGIN;",
				},
				{
					Query: `INSERT INTO jsonb_gin_txn VALUES
							(2, '{"a":2,"tags":["y","y"]}');`,
				},
				{
					Query: `SELECT SUM(row_count), COUNT(payload) > 0, COUNT(checksum) > 0
	FROM dg_gin_jsonb_gin_txn_jsonb_gin_txn_idx_posting_chunks;`,
					Expected: []sql.Row{{float64(8), "t", "t"}},
				},
				{
					Query: "ROLLBACK;",
				},
				{
					Query: `SELECT SUM(row_count), COUNT(payload) > 0, COUNT(checksum) > 0
	FROM dg_gin_jsonb_gin_txn_jsonb_gin_txn_idx_posting_chunks;`,
					Expected: []sql.Row{{float64(4), "t", "t"}},
				},
				{
					Query: "BEGIN;",
				},
				{
					Query: `UPDATE jsonb_gin_txn
	SET doc = '{"a":3,"tags":["z"]}'
	WHERE id = 1;`,
				},
				{
					Query: `SELECT token, SUM(row_count)
	FROM dg_gin_jsonb_gin_txn_jsonb_gin_txn_idx_posting_chunks
	WHERE token IN ('9:jsonb_ops3:key1:01:x', '9:jsonb_ops3:key1:01:z')
	GROUP BY token
	ORDER BY token;`,
					Expected: []sql.Row{
						{"9:jsonb_ops3:key1:01:z", float64(1)},
					},
				},
				{
					Query: "ROLLBACK;",
				},
				{
					Query: `SELECT token, SUM(row_count)
	FROM dg_gin_jsonb_gin_txn_jsonb_gin_txn_idx_posting_chunks
	WHERE token IN ('9:jsonb_ops3:key1:01:x', '9:jsonb_ops3:key1:01:z')
	GROUP BY token
	ORDER BY token;`,
					Expected: []sql.Row{
						{"9:jsonb_ops3:key1:01:x", float64(1)},
					},
				},
				{
					Query: "BEGIN;",
				},
				{
					Query: "DELETE FROM jsonb_gin_txn WHERE id = 1;",
				},
				{
					Query: `SELECT COALESCE(SUM(row_count), 0), COUNT(*)
	FROM dg_gin_jsonb_gin_txn_jsonb_gin_txn_idx_posting_chunks;`,
					Expected: []sql.Row{{float64(0), 0}},
				},
				{
					Query: "ROLLBACK;",
				},
				{
					Query: `SELECT SUM(row_count), COUNT(payload) > 0, COUNT(checksum) > 0
	FROM dg_gin_jsonb_gin_txn_jsonb_gin_txn_idx_posting_chunks;`,
					Expected: []sql.Row{{float64(4), "t", "t"}},
				},
			},
		},
		{
			Name: "PostgreSQL jsonb gin sidecar DDL lifecycle",
			SetUpScript: []string{
				"CREATE TABLE jsonb_gin_lifecycle (id INTEGER PRIMARY KEY, doc JSONB NOT NULL);",
				`INSERT INTO jsonb_gin_lifecycle VALUES
					(1, '{"a":1,"tags":["x","x"]}');`,
				"CREATE INDEX jsonb_gin_lifecycle_idx ON jsonb_gin_lifecycle USING gin (doc);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "ALTER INDEX jsonb_gin_lifecycle_idx RENAME TO jsonb_gin_lifecycle_renamed_idx;",
				},
				{
					Query: `INSERT INTO jsonb_gin_lifecycle VALUES
						(2, '{"a":2,"tags":["y","y"]}');`,
				},
				{
					Query: `SELECT SUM(row_count), COUNT(payload) > 0, COUNT(checksum) > 0
FROM dg_gin_jsonb_gin_lifecycle_jsonb_gin_lifecycle_idx_posting_chunks;`,
					Expected: []sql.Row{{float64(8), "t", "t"}},
				},
				{
					Query: "DROP INDEX jsonb_gin_lifecycle_renamed_idx;",
				},
				{
					Query:       "SELECT COUNT(*) FROM dg_gin_jsonb_gin_lifecycle_jsonb_gin_lifecycle_idx_posting_chunks;",
					ExpectedErr: "table not found",
				},
				{
					Query: "CREATE INDEX jsonb_gin_lifecycle_idx ON jsonb_gin_lifecycle USING gin (doc);",
				},
				{
					Query: `SELECT SUM(row_count), COUNT(payload) > 0, COUNT(checksum) > 0
FROM dg_gin_jsonb_gin_lifecycle_jsonb_gin_lifecycle_idx_posting_chunks;`,
					Expected: []sql.Row{{float64(8), "t", "t"}},
				},
			},
		},
		{
			Name: "PostgreSQL jsonb gin indexed lookup and recheck",
			SetUpScript: []string{
				"CREATE TABLE jsonb_gin_lookup (id INTEGER PRIMARY KEY, doc JSONB NOT NULL);",
				`INSERT INTO jsonb_gin_lookup VALUES
						(1, '{"a":1,"b":2,"tags":["x"],"nested":{"a":9}}'),
						(2, '{"a":1,"b":3,"tags":["y"]}'),
						(3, '{"a":2,"b":2,"tags":["x"]}'),
						(4, '{"nested":{"a":1},"tags":["z"]}'),
						(5, '{"a":null,"tags":["x"]}');`,
				"CREATE INDEX jsonb_gin_lookup_idx ON jsonb_gin_lookup USING gin (doc);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `EXPLAIN SELECT id FROM jsonb_gin_lookup
WHERE doc @> '{"a":1}'
ORDER BY id;`,
					Expected: []sql.Row{
						{"Project"},
						{" ├─ columns: [jsonb_gin_lookup.id]"},
						{" └─ Sort(jsonb_gin_lookup.id ASC)"},
						{"     └─ Filter"},
						{`         ├─ jsonb_gin_lookup.doc @> '{"a":1}'`},
						{"         └─ IndexedTableAccess(jsonb_gin_lookup)"},
						{"             ├─ index: [jsonb_gin(doc)]"},
						{"             ├─ filters: [{[jsonb_gin_lookup_idx intersect 2 token(s), jsonb_gin_lookup_idx intersect 2 token(s)]}]"},
						{"             └─ columns: [id doc]"},
					},
				},
				{
					Query: `SELECT id FROM jsonb_gin_lookup
WHERE doc @> '{"a":1}'
ORDER BY id;`,
					Expected: []sql.Row{{1}, {2}},
				},
				{
					Query: `SELECT count(*) FROM jsonb_gin_lookup
WHERE doc @> '{"a":1}';`,
					Expected: []sql.Row{{2}},
				},
				{
					Query: `SELECT count(id) FROM jsonb_gin_lookup
WHERE doc @> '{"a":1}';`,
					Expected: []sql.Row{{2}},
				},
				{
					Query: `SELECT id FROM jsonb_gin_lookup
	WHERE doc @> '{"a":null}'
	ORDER BY id;`,
					Expected: []sql.Row{{5}},
				},
				{
					Query: `SELECT id FROM jsonb_gin_lookup
	WHERE doc ? 'a'
	ORDER BY id;`,
					Expected: []sql.Row{{1}, {2}, {3}, {5}},
				},
				{
					Query: `SELECT id FROM jsonb_gin_lookup
	WHERE doc ?| ARRAY['missing','a']
	ORDER BY id;`,
					Expected: []sql.Row{{1}, {2}, {3}, {5}},
				},
				{
					Query: `SELECT id FROM jsonb_gin_lookup
	WHERE doc ?& ARRAY['a','tags']
	ORDER BY id;`,
					Expected: []sql.Row{{1}, {2}, {3}, {5}},
				},
			},
		},
		{
			Name: "PostgreSQL jsonb gin path ops indexed lookup",
			SetUpScript: []string{
				"CREATE TABLE jsonb_gin_path_lookup (id INTEGER PRIMARY KEY, doc JSONB NOT NULL);",
				`INSERT INTO jsonb_gin_path_lookup VALUES
						(1, '{"a":{"b":1},"tags":["x"]}'),
						(2, '{"a":{"b":2},"tags":["x"]}'),
						(3, '{"a":{"c":1},"tags":["y"]}');`,
				"CREATE INDEX jsonb_gin_path_lookup_idx ON jsonb_gin_path_lookup USING gin (doc jsonb_path_ops);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `EXPLAIN SELECT id FROM jsonb_gin_path_lookup
	WHERE doc @> '{"a":{"b":1}}'
	ORDER BY id;`,
					Expected: []sql.Row{
						{"Project"},
						{" ├─ columns: [jsonb_gin_path_lookup.id]"},
						{" └─ Sort(jsonb_gin_path_lookup.id ASC)"},
						{"     └─ Filter"},
						{`         ├─ jsonb_gin_path_lookup.doc @> '{"a":{"b":1}}'`},
						{"         └─ IndexedTableAccess(jsonb_gin_path_lookup)"},
						{"             ├─ index: [jsonb_gin(doc)]"},
						{"             ├─ filters: [{[jsonb_gin_path_lookup_idx intersect 1 token(s), jsonb_gin_path_lookup_idx intersect 1 token(s)]}]"},
						{"             └─ columns: [id doc]"},
					},
				},
				{
					Query: `SELECT id FROM jsonb_gin_path_lookup
	WHERE doc @> '{"a":{"b":1}}'
	ORDER BY id;`,
					Expected: []sql.Row{{1}},
				},
			},
		},
		{
			Name: "JSONB expression indexes",
			SetUpScript: []string{
				"CREATE TABLE jsonb_expr_idx (id INTEGER PRIMARY KEY, doc JSONB NOT NULL);",
				`INSERT INTO jsonb_expr_idx VALUES
					(1, '{"key":"alpha","n":1}'),
					(2, '{"key":"beta","n":2}'),
					(3, '{"other":true}');`,
				"CREATE TABLE jsonb_expr_idx_unique (id INTEGER PRIMARY KEY, doc JSONB NOT NULL);",
				`INSERT INTO jsonb_expr_idx_unique VALUES
					(1, '{"key":"alpha","n":1}'),
					(2, '{"key":"beta","n":2}'),
					(3, '{"other":true}');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "CREATE INDEX jsonb_expr_idx_key ON jsonb_expr_idx ((doc->>'key'));",
				},
				{
					Query:    "SELECT id FROM jsonb_expr_idx WHERE doc->>'key' = 'alpha';",
					Expected: []sql.Row{{1}},
				},
				{
					Query:    `INSERT INTO jsonb_expr_idx VALUES (4, '{"key":"gamma","n":4}');`,
					Expected: []sql.Row{},
				},
				{
					Query:    "SELECT id FROM jsonb_expr_idx WHERE doc->>'key' IN ('alpha', 'gamma') ORDER BY id;",
					Expected: []sql.Row{{1}, {4}},
				},
				{
					Query: "CREATE UNIQUE INDEX jsonb_expr_idx_key_unique ON jsonb_expr_idx_unique ((doc->>'key'));",
				},
				{
					Query:       `INSERT INTO jsonb_expr_idx_unique VALUES (4, '{"key":"alpha","n":4}');`,
					ExpectedErr: "duplicate",
				},
				{
					Query:    `INSERT INTO jsonb_expr_idx_unique VALUES (5, '{"key":"gamma","n":5}');`,
					Expected: []sql.Row{},
				},
				{
					Query:    "SELECT id FROM jsonb_expr_idx_unique WHERE doc->>'key' IN ('alpha', 'gamma') ORDER BY id;",
					Expected: []sql.Row{{1}, {5}},
				},
			},
		},
		{
			Name: "multi column int index",
			SetUpScript: []string{
				`CREATE TABLE test (pk INT4 PRIMARY KEY, a int, b int);`,
				`ALTER TABLE test ADD CONSTRAINT uniqIdx UNIQUE (a, b);`,
				`INSERT INTO test VALUES (1, 1, 2);`,
				`insert into test values (2, 1, 3)`,
				`insert into test values (3, 2, 2);`,
				`insert into test values (4, 3, 1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT pk FROM test WHERE a = 2 and b = 2;`,
					Expected: []sql.Row{
						{3},
					},
				},
				{
					Query: `SELECT pk FROM test WHERE a > 1`,
					Expected: []sql.Row{
						{3},
						{4},
					},
				},
				{
					Query: `SELECT pk FROM test WHERE a = 2 and b < 3`,
					Expected: []sql.Row{
						{3},
					},
				},
				{
					Query: `SELECT pk FROM test WHERE a > 2 and b < 3`,
					Expected: []sql.Row{
						{4},
					},
				},
				{
					Query: `SELECT pk FROM test WHERE a > 2 and b < 2`,
					Expected: []sql.Row{
						{4},
					},
				},
				{
					Query:    `SELECT pk FROM test WHERE a > 3 and b < 2`,
					Expected: []sql.Row{},
				},
				{
					Query:    `SELECT pk FROM test WHERE a > 3 and b < 2`,
					Expected: []sql.Row{},
				},
				{
					Query: `SELECT pk FROM test WHERE a > 1 and b > 1`,
					Expected: []sql.Row{
						{3},
					},
				},
				{
					Query: `SELECT pk FROM test WHERE a > 1 and b = 1`,
					Expected: []sql.Row{
						{4},
					},
				},
				{
					Query: `SELECT pk FROM test WHERE a < 3 and b > 0 order by 1`,
					Expected: []sql.Row{
						{1},
						{2},
						{3},
					},
				},
				{
					Query: `SELECT pk FROM test WHERE a > 1 and a < 3 order by 1`,
					Expected: []sql.Row{
						{3},
					},
				},
				{
					Query: `SELECT pk FROM test WHERE a > 1 and a < 3 order by 1`,
					Expected: []sql.Row{
						{3},
					},
				},
				{
					Query: `SELECT pk FROM test WHERE a > 1 and b > 1 order by 1`,
					Expected: []sql.Row{
						{3},
					},
				},
			},
		},
		{
			Name: "multi column int index, part 2",
			SetUpScript: []string{
				`CREATE TABLE test (pk INT4 PRIMARY KEY, a int, b int);`,
				`ALTER TABLE test ADD CONSTRAINT uniqIdx UNIQUE (a, b);`,
				`INSERT INTO test VALUES (1, 1, 2);`,
				`insert into test values (2, 1, 3)`,
				`insert into test values (3, 2, 2);`,
				`insert into test values (4, 2, 3);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT pk FROM test WHERE a = 2 and b = 2;`,
					Expected: []sql.Row{
						{3},
					},
				},
				{
					Query: `SELECT pk FROM test WHERE a = 2 and b = 3;`,
					Expected: []sql.Row{
						{4},
					},
				},
			},
		},
		{
			Name: "multi column int index, reverse traversal",
			SetUpScript: []string{
				`CREATE TABLE test (pk INT4 PRIMARY KEY, a int, b int);`,
				`ALTER TABLE test ADD CONSTRAINT uniqIdx UNIQUE (a, b);`,
				`INSERT INTO test VALUES (1, 1, 1);`,
				`insert into test values (2, 1, 3)`,
				`insert into test values (3, 2, 2);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT pk FROM test WHERE a < 3 and b = 2 order by a desc, b desc;`,
					Expected: []sql.Row{
						{3},
					},
				},
				{
					Query: `SELECT pk FROM test WHERE a < 2 and b = 3 order by a desc, b desc;`,
					Expected: []sql.Row{
						{2},
					},
				},
				{
					Query: `SELECT pk FROM test WHERE a < 2 and b < 10 order by a desc, b desc;`,
					Expected: []sql.Row{
						{2},
						{1},
					},
				},
			},
		},
		{
			Name: "Unique index varchar",
			SetUpScript: []string{
				`CREATE TABLE test (pk INT4 PRIMARY KEY, v1 varchar(100), v2 varchar(100));`,
				`ALTER TABLE test ADD CONSTRAINT uniqIdx UNIQUE (v1, v2);`,
				`INSERT INTO test VALUES (1, 'a', 'b');`,
				`insert into test values (2, 'a', 'u')`,
				`insert into test values (3, 'c', 'c');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT pk FROM test WHERE (v1 = 'c' AND v2 = 'c');`,
					Expected: []sql.Row{
						{3},
					},
				},
			},
		},
		{
			Name: "unique index select",
			SetUpScript: []string{
				`CREATE TABLE "django_content_type" ("id" integer NOT NULL PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY, "name" varchar(100) NOT NULL, "app_label" varchar(100) NOT NULL, "model" varchar(100) NOT NULL);`,
				`ALTER TABLE "django_content_type" ADD CONSTRAINT "django_content_type_app_label_model_76bd3d3b_uniq" UNIQUE ("app_label", "model");`,
				`ALTER TABLE "django_content_type" ALTER COLUMN "name" DROP NOT NULL;`,
				`ALTER TABLE "django_content_type" DROP COLUMN "name" CASCADE;`,
				`INSERT INTO "django_content_type" ("app_label", "model") VALUES ('auth', 'permission'), ('auth', 'group'), ('auth', 'user') RETURNING "django_content_type"."id";`,
				`INSERT INTO "django_content_type" ("app_label", "model") VALUES ('contenttypes', 'contenttype') RETURNING "django_content_type"."id";`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT "django_content_type"."id", "django_content_type"."app_label", "django_content_type"."model" FROM "django_content_type" WHERE ("django_content_type"."app_label" = 'auth' AND "django_content_type"."model" = 'permission') LIMIT 21;`,
					Expected: []sql.Row{
						{1, "auth", "permission"},
					},
				},
				{
					Query: `SELECT "django_content_type"."id", "django_content_type"."app_label", "django_content_type"."model" FROM "django_content_type" WHERE ("django_content_type"."app_label" = 'auth' AND "django_content_type"."model" = 'group') LIMIT 21;`,
					Expected: []sql.Row{
						{2, "auth", "group"},
					},
				},
				{
					Query: `SELECT "django_content_type"."id", "django_content_type"."app_label", "django_content_type"."model" FROM "django_content_type" WHERE ("django_content_type"."app_label" = 'auth' AND "django_content_type"."model" = 'user') LIMIT 21;`,
					Expected: []sql.Row{
						{3, "auth", "user"},
					},
				},
				{
					Query: `SELECT "django_content_type"."id", "django_content_type"."app_label", "django_content_type"."model" FROM "django_content_type" WHERE ("django_content_type"."app_label" = 'contenttypes' AND "django_content_type"."model" = 'contenttype') LIMIT 21;`,
					Expected: []sql.Row{
						{4, "contenttypes", "contenttype"},
					},
				},
			},
		},
		{
			Name: "Proper range AND + OR handling",
			SetUpScript: []string{
				"CREATE TABLE test(pk INTEGER PRIMARY KEY, v1 INTEGER);",
				"INSERT INTO test VALUES (1, 1),  (2, 3),  (3, 5),  (4, 7),  (5, 9);",
				"CREATE INDEX v1_idx ON test(v1);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT * FROM test WHERE v1 BETWEEN 3 AND 5 OR v1 BETWEEN 7 AND 9;",
					Expected: []sql.Row{
						{2, 3},
						{3, 5},
						{4, 7},
						{5, 9},
					},
				},
				{
					Query: "explain SELECT * FROM test WHERE v1 BETWEEN 3 AND 5 OR v1 BETWEEN 7 AND 9 order by 1;",
					Expected: []sql.Row{
						{"Sort(test.pk ASC)"},
						{" └─ IndexedTableAccess(test)"},
						{"     ├─ index: [test.v1]"},
						{"     ├─ filters: [{[3, 5]}, {[7, 9]}]"},
						{"     └─ columns: [pk v1]"},
					},
				},
			},
		},
		{
			Name: "Performance Regression Test #1",
			SetUpScript: []string{
				"CREATE TABLE sbtest1(id SERIAL, k INTEGER DEFAULT '0' NOT NULL, c CHAR(120) DEFAULT '' NOT NULL, pad CHAR(60) DEFAULT '' NOT NULL, PRIMARY KEY (id))",
				testdata.INDEX_PERFORMANCE_REGRESSION_INSERTS,
				"CREATE INDEX k_1 ON sbtest1(k)",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT id, k FROM sbtest1 WHERE k BETWEEN 3708 AND 3713 OR k BETWEEN 5041 AND 5046;",
					Expected: []sql.Row{
						{2, 5041},
						{18, 5041},
						{57, 5046},
						{58, 5044},
						{79, 5045},
						{80, 5041},
						{81, 5045},
						{107, 5041},
						{113, 5044},
						{153, 5043},
						{167, 5043},
						{187, 5044},
						{210, 5046},
						{213, 5046},
						{216, 5041},
						{222, 5045},
						{238, 5043},
						{265, 5042},
						{269, 5046},
						{279, 5045},
						{295, 5042},
						{298, 5045},
						{309, 5044},
						{324, 3710},
						{348, 5042},
						{353, 5045},
						{374, 5045},
						{390, 5042},
						{400, 5045},
						{430, 5045},
						{445, 5044},
						{476, 5046},
						{496, 5045},
						{554, 5042},
						{565, 5043},
						{566, 5045},
						{571, 5046},
						{573, 5046},
						{582, 5043},
					},
				},
			},
		},
		{ // https://github.com/dolthub/doltgresql/issues/2206
			Name: "Index attributes",
			SetUpScript: []string{
				`CREATE TABLE IF NOT EXISTS items (id SERIAL PRIMARY KEY, title VARCHAR(100) NOT NULL, metadata JSON, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    "CREATE UNIQUE INDEX IF NOT EXISTS idx_items_title_lower ON items(lower(title));",
					Expected: []sql.Row{},
				},
				{
					Query:    `CREATE INDEX idx_items_title_updated_include ON items(title COLLATE "C", updated_at) INCLUDE (metadata);`,
					Expected: []sql.Row{},
				},
				{
					Query: `SELECT i.indkey,
	i.indexprs,
	pg_catalog.pg_get_expr(i.indexprs, i.indrelid),
	pg_catalog.pg_get_indexdef(i.indexrelid),
	pg_catalog.pg_get_indexdef(i.indexrelid, 1, true)
FROM pg_catalog.pg_index i
JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
WHERE c.relname = 'idx_items_title_lower';`,
					Expected: []sql.Row{
						{"0", "lower(title)", "lower(title)", "CREATE UNIQUE INDEX idx_items_title_lower ON public.items USING btree (lower(title))", "lower(title)"},
					},
				},
				{
					Query: `SELECT a.attname, a.attnum
FROM pg_catalog.pg_attribute a
JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
WHERE c.relname = 'items' AND a.attnum > 0
ORDER BY a.attnum;`,
					Expected: []sql.Row{
						{"id", 1},
						{"title", 2},
						{"metadata", 3},
						{"updated_at", 4},
					},
				},
				{
					Query: `SELECT
	c.relname,
	a.attname,
	a.attnum,
	a.atttypid,
	a.attcollation,
	a.attstattarget
FROM pg_catalog.pg_attribute a
JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
WHERE c.relname IN ('items_pkey', 'idx_items_title_lower', 'idx_items_title_updated_include')
  AND a.attnum > 0
ORDER BY c.relname, a.attnum;`,
					Expected: []sql.Row{
						{"idx_items_title_lower", "lower", int16(1), typeOid("text"), collationOid("default"), int16(-1)},
						{"idx_items_title_updated_include", "title", int16(1), typeOid("varchar"), collationOid("C"), int16(-1)},
						{"idx_items_title_updated_include", "updated_at", int16(2), typeOid("timestamp"), uint32(0), int16(-1)},
						{"idx_items_title_updated_include", "metadata", int16(3), typeOid("json"), uint32(0), int16(-1)},
						{"items_pkey", "id", int16(1), typeOid("int4"), uint32(0), int16(-1)},
					},
				},
				{
					Query:    "INSERT INTO items (title, metadata, updated_at) VALUES ('ABC', '{}', '2026-10-10 01:02:03');",
					Expected: []sql.Row{},
				},
				{
					Query:       "INSERT INTO items (title, metadata, updated_at) VALUES ('abc', '{}', '2026-11-12 03:04:05');",
					ExpectedErr: "duplicate unique key given",
				},
			},
		},
		{
			Name: "PostgreSQL alter expression index statistics target",
			SetUpScript: []string{
				`CREATE TABLE alter_index_stats_meta (
					id INTEGER PRIMARY KEY,
					title TEXT NOT NULL,
					code INTEGER NOT NULL
				);`,
				"CREATE INDEX alter_index_stats_lower_idx ON alter_index_stats_meta (lower(title));",
				"CREATE INDEX alter_index_stats_mixed_idx ON alter_index_stats_meta (lower(title), code);",
				"CREATE INDEX alter_index_stats_code_idx ON alter_index_stats_meta (code);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "ALTER INDEX alter_index_stats_lower_idx ALTER COLUMN 1 SET STATISTICS 100;",
				},
				{
					Query: "ALTER INDEX alter_index_stats_mixed_idx ALTER COLUMN 1 SET STATISTICS 200;",
				},
				{
					Query: `SELECT pg_catalog.pg_get_indexdef(c.oid)
FROM pg_catalog.pg_class c
WHERE c.relname = 'alter_index_stats_mixed_idx';`,
					Expected: []sql.Row{
						{"CREATE INDEX alter_index_stats_mixed_idx ON public.alter_index_stats_meta USING btree (lower(title), code)"},
					},
				},
				{
					Query: `SELECT
	c.relname,
	a.attname,
	a.attnum,
	a.attstattarget
FROM pg_catalog.pg_attribute a
JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
WHERE c.relname IN ('alter_index_stats_lower_idx', 'alter_index_stats_mixed_idx')
  AND a.attnum > 0
ORDER BY c.relname, a.attnum;`,
					Expected: []sql.Row{
						{"alter_index_stats_lower_idx", "lower", int16(1), int16(100)},
						{"alter_index_stats_mixed_idx", "lower", int16(1), int16(200)},
						{"alter_index_stats_mixed_idx", "code", int16(2), int16(-1)},
					},
				},
				{
					Query: "ALTER INDEX alter_index_stats_lower_idx ALTER COLUMN 1 SET STATISTICS 0;",
				},
				{
					Query: "ALTER INDEX alter_index_stats_mixed_idx ALTER COLUMN 1 SET STATISTICS 10001;",
				},
				{
					Query: `SELECT c.relname, a.attname, a.attstattarget
FROM pg_catalog.pg_attribute a
JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
WHERE c.relname IN ('alter_index_stats_lower_idx', 'alter_index_stats_mixed_idx')
  AND a.attnum = 1
ORDER BY c.relname;`,
					Expected: []sql.Row{
						{"alter_index_stats_lower_idx", "lower", int16(0)},
						{"alter_index_stats_mixed_idx", "lower", int16(10000)},
					},
				},
				{
					Query:       "ALTER INDEX alter_index_stats_code_idx ALTER COLUMN 1 SET STATISTICS 100;",
					ExpectedErr: `cannot alter statistics on non-expression column "code" of index "alter_index_stats_code_idx"`,
				},
				{
					Query:       "ALTER INDEX alter_index_stats_mixed_idx ALTER COLUMN 2 SET STATISTICS 100;",
					ExpectedErr: `cannot alter statistics on non-expression column "code" of index "alter_index_stats_mixed_idx"`,
				},
				{
					Query:       "ALTER INDEX alter_index_stats_lower_idx ALTER COLUMN 2 SET STATISTICS 100;",
					ExpectedErr: `column number 2 of relation "alter_index_stats_lower_idx" does not exist`,
				},
				{
					Query:       "ALTER INDEX alter_index_stats_meta_pkey ALTER COLUMN 1 SET STATISTICS 100;",
					ExpectedErr: "ALTER INDEX statistics targets for constraint-backed indexes are not yet supported",
				},
			},
		},
		{
			Name: "PostgreSQL mixed expression index metadata",
			SetUpScript: []string{
				`CREATE TABLE mixed_expression_index_meta (
					id INTEGER PRIMARY KEY,
					title TEXT NOT NULL,
					code TEXT NOT NULL
				);`,
				`INSERT INTO mixed_expression_index_meta VALUES
					(1, 'Alpha', 'a1'),
					(2, 'Beta', 'b2'),
					(3, 'Alpha', 'a3');`,
				"CREATE INDEX mixed_expression_index_meta_idx ON mixed_expression_index_meta (lower(title), code, (upper(code)));",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT
	i.indkey,
	i.indexprs,
	pg_catalog.pg_get_expr(i.indexprs, i.indrelid),
	pg_catalog.pg_get_indexdef(i.indexrelid),
	pg_catalog.pg_get_indexdef(i.indexrelid, 1, true),
	pg_catalog.pg_get_indexdef(i.indexrelid, 2, true),
	pg_catalog.pg_get_indexdef(i.indexrelid, 3, true)
FROM pg_catalog.pg_index i
JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
WHERE c.relname = 'mixed_expression_index_meta_idx';`,
					Expected: []sql.Row{
						{
							"0 3 0",
							"lower(title), upper(code)",
							"lower(title), upper(code)",
							"CREATE INDEX mixed_expression_index_meta_idx ON public.mixed_expression_index_meta USING btree (lower(title), code, upper(code))",
							"lower(title)",
							"code",
							"upper(code)",
						},
					},
				},
				{
					Query: `SELECT id
FROM mixed_expression_index_meta
WHERE lower(title) = 'alpha' AND code = 'a1'
ORDER BY id;`,
					Expected: []sql.Row{{1}},
				},
			},
		},
	})
}

func opClassOidVector(names ...string) string {
	return opClassOidVectorForMethod("btree", names...)
}

func ginOpClassOidVector(names ...string) string {
	return opClassOidVectorForMethod("gin", names...)
}

func opClassOidVectorForMethod(method string, names ...string) string {
	oids := make([]string, len(names))
	for i, name := range names {
		oid := id.Cache().ToOID(id.NewId(id.Section_OperatorClass, method, name))
		oids[i] = strconv.FormatUint(uint64(oid), 10)
	}
	return strings.Join(oids, " ")
}

func typeOid(name string) uint32 {
	return id.Cache().ToOID(id.NewType("pg_catalog", name).AsId())
}

func collationOid(name string) uint32 {
	return id.Cache().ToOID(id.NewCollation("pg_catalog", name).AsId())
}

func collationOidVector(names ...string) string {
	oids := make([]string, len(names))
	for i, name := range names {
		if name == "" {
			oids[i] = "0"
			continue
		}
		oid := id.Cache().ToOID(id.NewCollation("pg_catalog", name).AsId())
		oids[i] = strconv.FormatUint(uint64(oid), 10)
	}
	return strings.Join(oids, " ")
}
