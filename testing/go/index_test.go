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

func unsupportedAccessMethodBoundaryScript(displayName string, accessMethod string, handler string, createIndexSupported bool, opclassCount int64, opfamilyCount int64, amopCount int64, amprocCount int64) ScriptTest {
	tableName := "access_method_boundary_" + accessMethod
	indexName := tableName + "_idx"
	expectedIndexCount := int64(0)
	if createIndexSupported {
		expectedIndexCount = 1
	}
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
				Expected: []sql.Row{{opclassCount}},
			},
			{
				Query: `SELECT COUNT(*)
FROM pg_catalog.pg_opfamily opf
JOIN pg_catalog.pg_am am ON am.oid = opf.opfmethod
WHERE am.amname = '` + accessMethod + `';`,
				Expected: []sql.Row{{opfamilyCount}},
			},
			{
				Query: `SELECT COUNT(*)
FROM pg_catalog.pg_amop amop
JOIN pg_catalog.pg_am am ON am.oid = amop.amopmethod
WHERE am.amname = '` + accessMethod + `';`,
				Expected: []sql.Row{{amopCount}},
			},
			{
				Query: `SELECT COUNT(*)
FROM pg_catalog.pg_amproc amproc
JOIN pg_catalog.pg_opfamily opf ON opf.oid = amproc.amprocfamily
JOIN pg_catalog.pg_am am ON am.oid = opf.opfmethod
WHERE am.amname = '` + accessMethod + `';`,
				Expected: []sql.Row{{amprocCount}},
			},
			{
				Query:       "CREATE INDEX " + indexName + " ON " + tableName + " USING " + accessMethod + " (v);",
				ExpectedErr: map[bool]string{false: "index method " + accessMethod + " is not yet supported"}[createIndexSupported],
				ExpectedTag: map[bool]string{true: "CREATE INDEX"}[createIndexSupported],
			},
			{
				Query: `SELECT COUNT(*)
FROM pg_catalog.pg_class
WHERE relname = '` + indexName + `';`,
				Expected: []sql.Row{{expectedIndexCount}},
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
WHERE tablename = 'jsonb_gin_build' AND indexname = 'jsonb_gin_build_doc_idx';`, PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testjsonbginpostingchunkbuildgate-0001-select-indexdef-from-pg_catalog.pg_indexes-where", ColumnModes: []string{"schema"}},
				},
				{
					Query: `SELECT COUNT(*) > 0, MIN(format_version), SUM(row_count), COUNT(payload), COUNT(checksum)
FROM dg_gin_jsonb_gin_build_jsonb_gin_build_doc_idx_posting_chunks;`,
					Expected: []sql.Row{
						{"t", 1, 22, 11, 11},
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
WHERE tablename = 'jsonb_gin_numeric_pk' AND indexname = 'jsonb_gin_numeric_pk_doc_idx';`, PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testjsonbginpostingchunkbuildgate-0003-select-indexdef-from-pg_catalog.pg_indexes-where", ColumnModes: []string{"schema"}},
				},
				{
					Query: `SELECT id::text FROM jsonb_gin_numeric_pk
WHERE doc @> '{"tags":["vip"]}'
ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testjsonbginpostingchunkbuildgate-0004-select-id::text-from-jsonb_gin_numeric_pk-where"},
				},
				{
					Query: `SELECT COUNT(*) > 0, SUM(row_count) > 0
FROM dg_gin_jsonb_gin_numeric_pk_jsonb_gin_numeric_pk_doc_idx_posting_chunks;`,
					Expected: []sql.Row{{"t", "t"}},
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
					Query: "CREATE INDEX jsonb_gin_bad_create_doc_idx ON jsonb_gin_bad_create USING gin (doc);", PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testjsonbginpostingchunkbuildgate-0008-create-index-jsonb_gin_bad_create_doc_idx-on-jsonb_gin_bad_create", Compare: "sqlstate"},
				},
				{
					Query: `SELECT COUNT(*)
FROM pg_catalog.pg_indexes
WHERE tablename = 'jsonb_gin_bad_create' AND indexname = 'jsonb_gin_bad_create_doc_idx';`, PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testjsonbginpostingchunkbuildgate-0009-select-count-*-from-pg_catalog.pg_indexes"},
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
ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testjsonbginpostingchunklookupgate-0002-select-id-from-jsonb_gin_lookup-where"},
				},
				{
					Query: `SELECT count(*) FROM jsonb_gin_lookup
WHERE doc @> '{"a":1}';`, PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testjsonbginpostingchunklookupgate-0003-select-count-*-from-jsonb_gin_lookup"},
				},
				{
					Query: `SELECT id FROM jsonb_gin_lookup
WHERE doc @> '{"nested":{"a":1}}'
ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testjsonbginpostingchunklookupgate-0004-select-id-from-jsonb_gin_lookup-where"},
				},
				{
					Query: `SELECT id FROM jsonb_gin_lookup
WHERE doc ? 'a'
ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testjsonbginpostingchunklookupgate-0005-select-id-from-jsonb_gin_lookup-where"},
				},
				{
					Query: `SELECT id FROM jsonb_gin_lookup
WHERE doc ?| ARRAY['missing','a']
ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testjsonbginpostingchunklookupgate-0006-select-id-from-jsonb_gin_lookup-where"},
				},
				{
					Query: `SELECT id FROM jsonb_gin_lookup
WHERE doc ?& ARRAY['a','tags']
ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testjsonbginpostingchunklookupgate-0007-select-id-from-jsonb_gin_lookup-where"},
				},
			},
		},
	})
}

func TestJsonbGinPostingChunkDMLGate(t *testing.T) {
	RunScripts(t, []ScriptTest{})
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
					Query: "SELECT * FROM test WHERE v1 = 2 ORDER BY pk;", PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testbasicindexing-0001-select-*-from-test-where"},
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
					Query: "SELECT * FROM test WHERE v1 > 2 ORDER BY pk;", PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testbasicindexing-0003-select-*-from-test-where"},
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
					Query: "SELECT * FROM test WHERE (v1 > 3 OR v1 < 2) AND v1 <> 5 ORDER BY pk;", PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testbasicindexing-0005-select-*-from-test-where"},
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
					Query: "SELECT * FROM test WHERE v1 = 2 OR v1 = 4 ORDER BY pk;", PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testbasicindexing-0007-select-*-from-test-where"},
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
					Query: "SELECT * FROM test WHERE v1 IN (2, 4) ORDER BY pk;", PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testbasicindexing-0009-select-*-from-test-where"},
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
					Query: "SELECT * FROM test WHERE v1 IN (4, 2, 4, 2) ORDER BY pk;", PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testbasicindexing-0011-select-*-from-test-where"},
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
					Query: "SELECT * FROM test WHERE v1 IN (NULL, 2, 2) ORDER BY pk;", PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testbasicindexing-0013-select-*-from-test-where"},
				},
				{
					Query: "SELECT * FROM test WHERE v1 NOT IN (2, 4) ORDER BY pk;", PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testbasicindexing-0014-select-*-from-test-where"},
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
					Query: "SELECT * FROM test WHERE v1 >= 4 ORDER BY pk;", PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testbasicindexing-0016-select-*-from-test-where"},
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
					Query: "SELECT * FROM test WHERE v1 < 3 ORDER BY pk;", PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testbasicindexing-0018-select-*-from-test-where"},
				},
				{
					Query: "SELECT * FROM test WHERE v1 <= 3 ORDER BY pk;", PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testbasicindexing-0019-select-*-from-test-where"},
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
					Query: "SELECT * FROM test WHERE v1 = 'twelve' ORDER BY pk;", PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testbasicindexing-0020-select-*-from-test-where"},
				},
				{
					Query: "SELECT * FROM test WHERE v1 > 't' OR v1 < 'f' ORDER BY pk;", PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testbasicindexing-0021-select-*-from-test-where"},
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
					Query: "SELECT * FROM test WHERE v1 = 'twelve' ORDER BY pk;", PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testbasicindexing-0023-select-*-from-test-where"},
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
					Query: "SELECT * FROM test WHERE v1 = 2 AND v2 = 22 ORDER BY pk;", PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testbasicindexing-0027-select-*-from-test-where"},
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
					Query: "select * from test join jointable on test.v1 = jointable.v3 and test.v2 = jointable.v4 order by 1", PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testbasicindexing-0031-select-*-from-test-join"},
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
					Query: "SELECT * FROM test WHERE v1 > 2 AND v2 = 24 ORDER BY pk;", PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testbasicindexing-0033-select-*-from-test-where"},
				},
				{
					Query: "SELECT * FROM test WHERE v1 >= 4 AND v2 = 25 ORDER BY pk;", PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testbasicindexing-0034-select-*-from-test-where"},
				},
				{
					Query: "SELECT * FROM test WHERE v1 < 3 AND v2 = 21 ORDER BY pk;", PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testbasicindexing-0035-select-*-from-test-where"},
				},
				{
					Query: "SELECT * FROM test WHERE v1 <= 3 AND v2 = 22 ORDER BY pk;", PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testbasicindexing-0036-select-*-from-test-where"},
				},
				{
					Query: "SELECT * FROM test WHERE v1 = 2 AND v2 < 23 ORDER BY pk;", PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testbasicindexing-0037-select-*-from-test-where"},
				},
				{
					Query: "SELECT * FROM test WHERE v1 = 2 AND v2 < 22 ORDER BY pk;", PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testbasicindexing-0038-select-*-from-test-where"},
				},
				{
					Query: "SELECT * FROM test WHERE v1 > 2 AND v2 < 25 ORDER BY pk;", PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testbasicindexing-0039-select-*-from-test-where"},
				},
				{
					Query: "SELECT * FROM test WHERE v1 >= 4 AND v2 <= 24 ORDER BY pk;", PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testbasicindexing-0040-select-*-from-test-where"},
				},
				{
					Query: "SELECT * FROM test WHERE v1 < 3 AND v2 < 22 ORDER BY pk;", PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testbasicindexing-0041-select-*-from-test-where"},
				},
				{
					Query: "SELECT * FROM test WHERE v1 <= 3 AND v2 < 23 ORDER BY pk;", PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testbasicindexing-0042-select-*-from-test-where"},
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
			Name: "Covering Index IN",
			SetUpScript: []string{
				"CREATE TABLE test(pk INT4 PRIMARY KEY, v1 INT4, v2 INT4);",
				"INSERT INTO test VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4), (5, 5, 5);",
				"CREATE INDEX v1_idx ON test(v1);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT * FROM test WHERE v1 IN (2, '3', 4) ORDER BY v1;", PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testbasicindexing-0070-select-*-from-test-where"},
				},
				{
					Query: "explain SELECT * FROM test WHERE v1 IN (2, '3', 4) ORDER BY v1;",
					Expected: []sql.Row{
						{"Sort(test.v1 IS NULL ASC, test.v1 ASC)"},
						{" └─ IndexedTableAccess(test)"},
						{"     ├─ index: [test.v1]"},
						{"     ├─ filters: [{[2, 2]}, {[3, 3]}, {[4, 4]}]"},
						{"     └─ columns: [pk v1 v2]"},
					},
				},
				{
					Query: "CREATE INDEX v2_idx ON test(v2);", PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testbasicindexing-0072-create-index-v2_idx-on-test"},
				},
				{
					Query: "SELECT * FROM test WHERE v2 IN (2, '3', 4) ORDER BY v1;", PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testbasicindexing-0073-select-*-from-test-where"},
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
					ExpectedErr: `cannot alter statistics on non-expression column "v1"`,
				},
			},
		},
		unsupportedAccessMethodBoundaryScript("hash", "hash", "hashhandler", false, 2, 2, 2, 4),
		unsupportedAccessMethodBoundaryScript("GiST", "gist", "gisthandler", true, 0, 0, 0, 0),
		unsupportedAccessMethodBoundaryScript("SP-GiST", "spgist", "spghandler", false, 0, 0, 0, 0),
		unsupportedAccessMethodBoundaryScript("BRIN", "brin", "brinhandler", false, 0, 0, 0, 0),
		{
			Name: "PostgreSQL primary key index rename unsupported boundary",
			SetUpScript: []string{
				"CREATE TABLE rename_primary_key_index (id INTEGER PRIMARY KEY, v INTEGER);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "ALTER INDEX rename_primary_key_index_pkey RENAME TO rename_primary_key_index_id_key;", PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testbasicindexing-0173-alter-index-rename_primary_key_index_pkey-rename-to", Compare: "sqlstate"},
				},
				{
					Query: "DROP INDEX rename_primary_key_index_pkey;", PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testbasicindexing-0174-drop-index-rename_primary_key_index_pkey", Compare: "sqlstate"},
				},
				{
					Query: "ALTER TABLE rename_primary_key_index DROP CONSTRAINT rename_primary_key_index_pkey;",
				},
				{
					Query: `SELECT con.conname, con.contype
FROM pg_catalog.pg_constraint con
JOIN pg_catalog.pg_class cls ON cls.oid = con.conrelid
WHERE cls.relname = 'rename_primary_key_index';`, PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testbasicindexing-0175-select-con.conname-con.contype-from-pg_catalog.pg_constraint"},
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
ORDER BY cls.relname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testbasicindexing-0176-select-cls.relname-con.conname-con.contype-from"},
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
ORDER BY tablename;`, PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testbasicindexing-0177-select-tablename-indexname-from-pg_catalog.pg_indexes"},
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
ORDER BY cls.relname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testbasicindexing-0178-select-cls.relname-idx.indisunique-idx.indisprimary-from"},
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
ORDER BY c.relname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testbasicindexing-0179-select-c.relname-pg_catalog.pg_get_indexdef-c.oid-from", ColumnModes: []string{"structural", "schema"}},
				},
				{
					Query: `SELECT
	'primary_key_constraint_table_custom'::regclass::text,
	'primary_key_constraint_column_custom'::regclass::text,
	'primary_key_constraint_alter_custom'::regclass::text,
	'primary_key_constraint_add_column_custom'::regclass::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testbasicindexing-0180-select-primary_key_constraint_table_custom-::regclass::text-primary_key_constraint_column_custom-::regclass::text"},
				},
				{
					Query: "ALTER INDEX primary_key_constraint_table_custom RENAME TO primary_key_constraint_table_other;", PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testbasicindexing-0181-alter-index-primary_key_constraint_table_custom-rename-to", Compare: "sqlstate"},
				},
				{
					Query: "DROP INDEX primary_key_constraint_column_custom;", PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testbasicindexing-0182-drop-index-primary_key_constraint_column_custom", Compare: "sqlstate"},
				},
				{
					Query: "ALTER INDEX primary_key_constraint_default_pkey RENAME TO primary_key_constraint_default_other;", PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testbasicindexing-0183-alter-index-primary_key_constraint_default_pkey-rename-to", Compare: "sqlstate"},
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
ORDER BY cls.relname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testbasicindexing-0184-select-cls.relname-con.conname-from-pg_catalog.pg_constraint"},
				},
				{
					Query: "ALTER TABLE primary_key_constraint_alter_named ADD PRIMARY KEY (id);",
				},
				{
					Query: `SELECT indexname
FROM pg_catalog.pg_indexes
WHERE tablename = 'primary_key_constraint_alter_named';`, PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testbasicindexing-0185-select-indexname-from-pg_catalog.pg_indexes-where"},
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
					Expected: []sql.Row{{int64(12), "t", "t"}},
				},
				{
					Query: `SELECT token, SUM(row_count)
FROM dg_gin_jsonb_gin_backfill_jsonb_gin_backfill_idx_posting_chunks
WHERE token IN ('9:jsonb_ops3:key1:01:a', '9:jsonb_ops3:key1:01:x')
GROUP BY token
ORDER BY token;`,
					Expected: []sql.Row{
						{"9:jsonb_ops3:key1:01:a", int64(2)},
						{"9:jsonb_ops3:key1:01:x", int64(1)},
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
					Expected: []sql.Row{{int64(8), "t", "t"}},
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
						{"9:jsonb_ops3:key1:01:z", int64(1)},
					},
				},
				{
					Query: "DELETE FROM jsonb_gin_dml WHERE id = 2;",
				},
				{
					Query: `SELECT SUM(row_count), COUNT(payload) > 0, COUNT(checksum) > 0
FROM dg_gin_jsonb_gin_dml_jsonb_gin_dml_idx_posting_chunks;`,
					Expected: []sql.Row{{int64(4), "t", "t"}},
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
					Expected: []sql.Row{{int64(8), "t", "t"}},
				},
				{
					Query: "ROLLBACK;",
				},
				{
					Query: `SELECT SUM(row_count), COUNT(payload) > 0, COUNT(checksum) > 0
	FROM dg_gin_jsonb_gin_txn_jsonb_gin_txn_idx_posting_chunks;`,
					Expected: []sql.Row{{int64(4), "t", "t"}},
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
						{"9:jsonb_ops3:key1:01:z", int64(1)},
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
						{"9:jsonb_ops3:key1:01:x", int64(1)},
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
					Expected: []sql.Row{{int64(0), int64(0)}},
				},
				{
					Query: "ROLLBACK;",
				},
				{
					Query: `SELECT SUM(row_count), COUNT(payload) > 0, COUNT(checksum) > 0
	FROM dg_gin_jsonb_gin_txn_jsonb_gin_txn_idx_posting_chunks;`,
					Expected: []sql.Row{{int64(4), "t", "t"}},
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
					Expected: []sql.Row{{int64(8), "t", "t"}},
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
					Expected: []sql.Row{{int64(8), "t", "t"}},
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
ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testbasicindexing-0282-select-id-from-jsonb_gin_lookup-where"},
				},
				{
					Query: `SELECT count(*) FROM jsonb_gin_lookup
WHERE doc @> '{"a":1}';`, PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testbasicindexing-0283-select-count-*-from-jsonb_gin_lookup"},
				},
				{
					Query: `SELECT count(id) FROM jsonb_gin_lookup
WHERE doc @> '{"a":1}';`, PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testbasicindexing-0284-select-count-id-from-jsonb_gin_lookup"},
				},
				{
					Query: `SELECT id FROM jsonb_gin_lookup
	WHERE doc @> '{"a":null}'
	ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testbasicindexing-0285-select-id-from-jsonb_gin_lookup-where"},
				},
				{
					Query: `SELECT id FROM jsonb_gin_lookup
	WHERE doc ? 'a'
	ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testbasicindexing-0286-select-id-from-jsonb_gin_lookup-where"},
				},
				{
					Query: `SELECT id FROM jsonb_gin_lookup
	WHERE doc ?| ARRAY['missing','a']
	ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testbasicindexing-0287-select-id-from-jsonb_gin_lookup-where"},
				},
				{
					Query: `SELECT id FROM jsonb_gin_lookup
	WHERE doc ?& ARRAY['a','tags']
	ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testbasicindexing-0288-select-id-from-jsonb_gin_lookup-where"},
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
	ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testbasicindexing-0290-select-id-from-jsonb_gin_path_lookup-where"},
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
					Query: "SELECT * FROM test WHERE v1 BETWEEN 3 AND 5 OR v1 BETWEEN 7 AND 9;", PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testbasicindexing-0320-select-*-from-test-where"},
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
					Query: "CREATE UNIQUE INDEX IF NOT EXISTS idx_items_title_lower ON items(lower(title));", PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testbasicindexing-0323-create-unique-index-if-not"},
				},
				{
					Query: `CREATE INDEX idx_items_title_updated_include ON items(title COLLATE "C", updated_at) INCLUDE (metadata);`, PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testbasicindexing-0324-create-index-idx_items_title_updated_include-on-items"},
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
					Expected: []sql.Row{{
						"0",
						"lower(title)",
						"lower(title)",
						"CREATE UNIQUE INDEX idx_items_title_lower ON public.items USING btree (lower(title))",
						"lower(title)",
					}},
				},
				{
					Query: `SELECT a.attname, a.attnum
FROM pg_catalog.pg_attribute a
JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
WHERE c.relname = 'items' AND a.attnum > 0
ORDER BY a.attnum;`,
					Expected: []sql.Row{
						{"id", int64(1)},
						{"title", int64(2)},
						{"metadata", int64(3)},
						{"updated_at", int64(4)},
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
						{"idx_items_title_lower", "lower", int64(1), int64(25), int64(100), int64(-1)},
						{"idx_items_title_updated_include", "title", int64(1), int64(1043), int64(950), int64(-1)},
						{"idx_items_title_updated_include", "updated_at", int64(2), int64(1114), int64(0), int64(-1)},
						{"idx_items_title_updated_include", "metadata", int64(3), int64(114), int64(0), int64(-1)},
						{"items_pkey", "id", int64(1), int64(23), int64(0), int64(-1)},
					},
				},
				{
					Query: "INSERT INTO items (title, metadata, updated_at) VALUES ('ABC', '{}', '2026-10-10 01:02:03');", PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testbasicindexing-0328-insert-into-items-title-metadata"},
				},
				{
					Query: "INSERT INTO items (title, metadata, updated_at) VALUES ('abc', '{}', '2026-11-12 03:04:05');", PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testbasicindexing-0329-insert-into-items-title-metadata", Compare: "sqlstate"},
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
					Expected: []sql.Row{{
						"0 3 0",
						"lower(title), upper(code)",
						"lower(title), upper(code)",
						"CREATE INDEX mixed_expression_index_meta_idx ON public.mixed_expression_index_meta USING btree (lower(title), code, upper(code))",
						"lower(title)",
						"code",
						"upper(code)",
					}},
				},
				{
					Query: `SELECT id
FROM mixed_expression_index_meta
WHERE lower(title) = 'alpha' AND code = 'a1'
ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "index-test-testbasicindexing-0338-select-id-from-mixed_expression_index_meta-where"},
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
