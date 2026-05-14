// Copyright 2023 Dolthub, Inc.
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

func TestSmokeTests(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "Simple statements",
			SetUpScript: []string{
				"CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "CREATE TABLE test2 (pk BIGINT PRIMARY KEY, v1 BIGINT);", PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0001-create-table-test2-pk-bigint"},
				},
				{
					Query: "INSERT INTO test VALUES (1, 1), (2, 2);", PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0002-insert-into-test-values-1"},
				},
				{
					Query: "INSERT INTO test2 VALUES (3, 3), (4, 4);", PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0003-insert-into-test2-values-3"},
				},
				{
					Query: "SELECT * FROM test;", PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0004-select-*-from-test"},
				},
				{
					Query: "SELECT * FROM test2;", PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0005-select-*-from-test2"},
				},
				{
					Query: "SELECT test2.pk FROM test2;", PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0006-select-test2.pk-from-test2"},
				},
				{
					Query: "SELECT * FROM test ORDER BY 1 LIMIT 1 OFFSET 1;", PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0007-select-*-from-test-order"},
				},
				{
					Query: "SELECT NULL = NULL", PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0008-select-null-=-null"},
				},
				{
					Query: ";", PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0009", Compare: "tag"},
				},
				{
					Query: " ; ", PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0010", Compare: "tag"},
				},
				{
					Query: "-- this is only a comment", PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0011-this-is-only-a-comment", Compare: "tag"},
				},
			},
		},
		{
			Name: "Insert statements",
			SetUpScript: []string{
				"CREATE TABLE test (pk INT8 PRIMARY KEY, v1 INT4, v2 INT2);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "INSERT INTO test VALUES (1, 2, 3);", PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0012-insert-into-test-values-1"},
				},
				{
					Query: "INSERT INTO test (v1, pk) VALUES (5, 4);", PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0013-insert-into-test-v1-pk"},
				},
				{
					Query: "INSERT INTO test (pk, v2) SELECT pk + 5, v2 + 10 FROM test WHERE v2 IS NOT NULL;", PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0014-insert-into-test-pk-v2"},
				},
				{
					Query: "SELECT * FROM test;", PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0015-select-*-from-test"},
				},
			},
		},
		{
			Name: "Update statements",
			SetUpScript: []string{
				"CREATE TABLE test (pk INT8 PRIMARY KEY, v1 INT4, v2 INT2);",
				"INSERT INTO test VALUES (1, 2, 3), (4, 5, 6), (7, 8, 9);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "UPDATE test SET v2 = 10;", PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0016-update-test-set-v2-="},
				},
				{
					Query: "UPDATE test SET v1 = pk + v2;", PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0017-update-test-set-v1-="},
				},
				{
					Query: "SELECT * FROM test;", PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0018-select-*-from-test"},
				},
				{
					Query: "UPDATE test SET pk = subquery.val FROM (SELECT 22 as val) AS subquery WHERE pk >= 7;",
					Skip:  true, PostgresOracle: // FROM not yet supported
					ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0019-update-test-set-pk-="},
				},
				{
					Query: "SELECT * FROM test;",
					Skip:  true, PostgresOracle: // Above query doesn't run yet
					ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0020-select-*-from-test"},
				},
			},
		},
		{
			Name: "Delete statements",
			SetUpScript: []string{
				"CREATE TABLE test (pk INT8 PRIMARY KEY, v1 INT4, v2 INT2);",
				"INSERT INTO test VALUES (1, 1, 1), (2, 3, 4), (5, 7, 9);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "DELETE FROM test WHERE v2 = 9;", PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0021-delete-from-test-where-v2"},
				},
				{
					Query: "DELETE FROM test WHERE v1 = pk;", PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0022-delete-from-test-where-v1"},
				},
				{
					Query: "SELECT * FROM test;", PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0023-select-*-from-test"},
				},
			},
		},
		{
			Name: "USE statements",
			SetUpScript: []string{
				"CREATE DATABASE test",
				"USE test",
				"CREATE TABLE t1 (pk BIGINT PRIMARY KEY, v1 BIGINT);",
				"INSERT INTO t1 VALUES (1, 1), (2, 2);",
				"select dolt_commit('-Am', 'initial commit');",
				"select dolt_branch('b1');",
				"select dolt_checkout('b1');",
				"INSERT INTO t1 VALUES (3, 3), (4, 4);",
				"select dolt_commit('-Am', 'commit b1');",
				"select dolt_tag('tag1')",
				"INSERT INTO t1 VALUES (5, 5), (6, 6);",
				"select dolt_checkout('main');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "select * from t1 order by 1;",
					Expected: []sql.Row{
						{1, 1},
						{2, 2},
					},
				},
				{
					Query:            "USE test/b1",
					SkipResultsCheck: true,
				},
				{
					Query: "select * from t1 order by 1;",
					Expected: []sql.Row{
						{1, 1},
						{2, 2},
						{3, 3},
						{4, 4},
						{5, 5},
						{6, 6},
					},
				},
				{
					Query:            "USE \"test/main\"",
					SkipResultsCheck: true,
				},
				{
					Query: "select * from t1 order by 1;",
					Expected: []sql.Row{
						{1, 1},
						{2, 2},
					},
				},
				{
					Query:            "USE 'test/tag1'",
					SkipResultsCheck: true,
				},
				{
					Query: "select * from t1 order by 1;",
					Expected: []sql.Row{
						{1, 1},
						{2, 2},
						{3, 3},
						{4, 4},
					},
				},
			},
		},
		{
			Name: "Boolean results",
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT 1 IN (2);", PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0028-select-1-in-2"},
				},
				{
					Query: "SELECT 2 IN (2);", PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0029-select-2-in-2"},
				},
			},
		},
		{
			Name: "Commit and diff across branches",
			SetUpScript: []string{
				"CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT);",
				"INSERT INTO test VALUES (1, 1), (2, 2);",
				"SELECT DOLT_ADD('-A');",
				"SELECT DOLT_COMMIT('-m', 'initial commit');",
				"SELECT DOLT_BRANCH('other');",
				"UPDATE test SET v1 = 3;",
				"SELECT DOLT_ADD('-A');",
				"SELECT DOLT_COMMIT('-m', 'commit main');",
				"SELECT DOLT_CHECKOUT('other');",
				"UPDATE test SET v1 = 4 WHERE pk = 2;",
				"SELECT DOLT_ADD('-A');",
				"SELECT DOLT_COMMIT('-m', 'commit other');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    "SELECT DOLT_CHECKOUT('main');",
					Expected: []sql.Row{{"{0,\"Switched to branch 'main'\"}"}},
				},
				{
					Query: "SELECT * FROM test;",
					Expected: []sql.Row{
						{1, 3},
						{2, 3},
					},
				},
				{
					Query:    "SELECT DOLT_CHECKOUT('other');",
					Expected: []sql.Row{{"{0,\"Switched to branch 'other'\"}"}},
				},
				{
					Query: "SELECT * FROM test;",
					Expected: []sql.Row{
						{1, 1},
						{2, 4},
					},
				},
				{
					Query: "SELECT from_pk, to_pk, from_v1, to_v1 FROM dolt_diff_test;",
					Expected: []sql.Row{
						{2, 2, 2, 4},
						{nil, 1, nil, 1},
						{nil, 2, nil, 2},
					},
				},
			},
		},
		{
			Name: "ARRAY expression",
			SetUpScript: []string{
				"CREATE TABLE test1 (id INTEGER primary key, v1 BOOLEAN);",
				"INSERT INTO test1 VALUES (1, 'true'), (2, 'false');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT ARRAY[v1]::boolean[] FROM test1 ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0035-select-array[v1]::boolean[]-from-test1-order"},
				},
				{
					Query: "SELECT ARRAY[v1] FROM test1 ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0036-select-array[v1]-from-test1-order"},
				},
				{
					Query: "SELECT ARRAY[v1, true, v1] FROM test1 ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0037-select-array[v1-true-v1]-from"},
				},
				{
					Query: "SELECT ARRAY[1::float8, 2::numeric];", PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0038-select-array[1::float8-2::numeric]"},
				},
				{
					Query: "SELECT ARRAY[1::float8, NULL];", PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0039-select-array[1::float8-null]"},
				},
				{
					Query: "SELECT ARRAY[1::int2, 2::int4, 3::int8]::varchar[];", PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0040-select-array[1::int2-2::int4-3::int8]::varchar[]"},
				},
				{
					Query: "SELECT ARRAY[1::int8]::int;", PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0041-select-array[1::int8]::int", Compare: "sqlstate"},
				},
				{
					Query: "SELECT ARRAY[1::int8, 2::varchar];", PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0042-select-array[1::int8-2::varchar]", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "Array casting",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT '{true,false,true}'::boolean[];`, PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0043-select-{true-false-true}-::boolean[]"},
				},
				{
					Skip:  true, // TODO: result differs from Postgres
					Query: `SELECT '{"\x68656c6c6f", "\x776f726c64", "\x6578616d706c65"}'::bytea[]::text[];`, PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0044-select-{-\\x68656c6c6f-\\x776f726c64-\\x6578616d706c65"},
				},
				{
					Skip:  true, // TODO: result differs from Postgres
					Query: `SELECT '{"\\x68656c6c6f", "\\x776f726c64", "\\x6578616d706c65"}'::bytea[]::text[];`, PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0045-select-{-\\\\x68656c6c6f-\\\\x776f726c64-\\\\x6578616d706c65"},
				},
				{
					Query: `SELECT '{"abcd", "efgh", "ijkl"}'::char(3)[];`, PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0046-select-{-abcd-efgh-ijkl"},
				},
				{
					Query: `SELECT '{"2020-02-03", "2020-04-05", "2020-06-06"}'::date[];`, PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0047-select-{-2020-02-03-2020-04-05-2020-06-06"},
				},
				{
					Query: `SELECT '{1.25,2.5,3.75}'::float4[];`, PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0048-select-{1.25-2.5-3.75}-::float4[]"},
				},
				{
					Query: `SELECT '{4.25,5.5,6.75}'::float8[];`, PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0049-select-{4.25-5.5-6.75}-::float8[]"},
				},
				{
					Query: `SELECT '{1,2,3}'::int2[];`, PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0050-select-{1-2-3}-::int2[]"},
				},
				{
					Query: `SELECT '{4,5,6}'::int4[];`, PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0051-select-{4-5-6}-::int4[]"},
				},
				{
					Query: `SELECT '{7,8,9}'::int8[];`, PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0052-select-{7-8-9}-::int8[]"},
				},
				{
					Query: `SELECT '{"{\"a\":\"val1\"}", "{\"b\":\"value2\"}", "{\"c\": \"object_value3\"}"}'::json[];`, PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0053-select-{-{\\-a\\-:\\"},
				},
				{
					Query: `SELECT '{"{\"d\":\"val1\"}", "{\"e\":\"value2\"}", "{\"f\": \"object_value3\"}"}'::jsonb[];`, PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0054-select-{-{\\-d\\-:\\"},
				},
				{
					Query: `SELECT '{"the", "legendary", "formula"}'::name[];`, PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0055-select-{-the-legendary-formula"},
				},
				{
					Query: `SELECT '{10.01,20.02,30.03}'::numeric[];`, PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0056-select-{10.01-20.02-30.03}-::numeric[]"},
				},
				{
					Query: `SELECT '{1,10,100}'::oid[];`, PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0057-select-{1-10-100}-::oid[]"},
				},
				{
					Query: `SELECT '{"this", "is", "some", "text"}'::text[], '{text,without,quotes}'::text[], '{null,NULL,"NULL","quoted"}'::text[];`, PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0058-select-{-this-is-some"},
				},
				{
					Query: `SELECT '{"12:12:13", "14:14:15", "16:16:17"}'::time[];`, PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0059-select-{-12:12:13-14:14:15-16:16:17"},
				},
				{
					Query: `SELECT '{"2020-02-03 12:13:14", "2020-04-05 15:16:17", "2020-06-06 18:19:20"}'::timestamp[];`, PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0060-select-{-2020-02-03-12:13:14-2020-04-05"},
				},
				{
					Query: `SELECT '{"3920fd79-7b53-437c-b647-d450b58b4532", "a594c217-4c63-4669-96ec-40eed180b7cf", "4367b70d-8d8b-4969-a1aa-bf59536455fb"}'::uuid[];`, PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0061-select-{-3920fd79-7b53-437c-b647-d450b58b4532-a594c217-4c63-4669-96ec-40eed180b7cf-4367b70d-8d8b-4969-a1aa-bf59536455fb"},
				},
				{
					Query: `SELECT '{"somewhere", "over", "the", "rainbow"}'::varchar(5)[];`, PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0062-select-{-somewhere-over-the"},
				},
				{
					Query: `SELECT '{1,2,3}'::xid[];`, PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0063-select-{1-2-3}-::xid[]"},
				},
				{
					Query: `SELECT '{"abc""","def"}'::text[];`, PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0064-select-{-abc-def-}", Compare: "sqlstate"},
				},
				{
					Query: `SELECT '{a,b,c'::text[];`, PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0065-select-{a-b-c-::text[]", Compare: "sqlstate"},
				},
				{
					Query: `SELECT 'a,b,c}'::text[];`, PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0066-select-a-b-c}-::text[]", Compare: "sqlstate"},
				},
				{
					Query: `SELECT '{"a,b,c}'::text[];`, PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0067-select-{-a-b-c}", Compare: "sqlstate"},
				},
				{
					Query: `SELECT '{a",b,c}'::text[];`, PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0068-select-{a-b-c}-::text[]", Compare: "sqlstate"},
				},
				{
					Query: `SELECT '{a,b,"c}'::text[];`, PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0069-select-{a-b-c}-::text[]", Compare: "sqlstate"},
				},
				{
					Query: `SELECT '{a,b,c"}'::text[];`, PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0070-select-{a-b-c-}", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "BETWEEN",
			SetUpScript: []string{
				"CREATE TABLE test (v1 FLOAT8);",
				"INSERT INTO test VALUES (1), (3), (7);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT * FROM test WHERE v1 BETWEEN 1 AND 4 ORDER BY v1;", PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0071-select-*-from-test-where"},
				},
				{
					Query: "SELECT * FROM test WHERE v1 BETWEEN 2 AND 4 ORDER BY v1;", PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0072-select-*-from-test-where"},
				},
				{
					Query: "SELECT * FROM test WHERE v1 BETWEEN 4 AND 2 ORDER BY v1;", PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0073-select-*-from-test-where"},
				},
				{
					Query: "SELECT * FROM test WHERE v1 BETWEEN SYMMETRIC 1 AND 4 ORDER BY v1;", PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0074-select-*-from-test-where"},
				},
				{
					Query: "SELECT * FROM test WHERE v1 BETWEEN SYMMETRIC 2 AND 4 ORDER BY v1;", PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0075-select-*-from-test-where"},
				},
				{
					Query: "SELECT * FROM test WHERE v1 BETWEEN SYMMETRIC 4 AND 2 ORDER BY v1;", PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0076-select-*-from-test-where"},
				},
				{
					Query: "SELECT * FROM test WHERE v1 NOT BETWEEN 1 AND 4 ORDER BY v1;", PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0077-select-*-from-test-where"},
				},
				{
					Query: "SELECT * FROM test WHERE v1 NOT BETWEEN 2 AND 4 ORDER BY v1;", PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0078-select-*-from-test-where"},
				},
				{
					Query: "SELECT * FROM test WHERE v1 NOT BETWEEN 4 AND 2 ORDER BY v1;", PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0079-select-*-from-test-where"},
				},
				{
					Query: "SELECT * FROM test WHERE v1 NOT BETWEEN SYMMETRIC 1 AND 4 ORDER BY v1;", PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0080-select-*-from-test-where"},
				},
				{
					Query: "SELECT * FROM test WHERE v1 NOT BETWEEN SYMMETRIC 2 AND 4 ORDER BY v1;", PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0081-select-*-from-test-where"},
				},
				{
					Query: "SELECT * FROM test WHERE v1 NOT BETWEEN SYMMETRIC 4 AND 2 ORDER BY v1;", PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0082-select-*-from-test-where"},
				},
			},
		},
		{
			Name: "IN",
			SetUpScript: []string{
				"CREATE TABLE test(v1 INT4, v2 INT4);",
				"INSERT INTO test VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT * FROM test WHERE v1 IN (2, '3', 4) ORDER BY v1;", PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0083-select-*-from-test-where"},
				},
				{
					Query: "CREATE INDEX v2_idx ON test(v2);", PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0084-create-index-v2_idx-on-test"},
				},
				{
					Query: "SELECT * FROM test WHERE v2 IN (2, '3', 4) ORDER BY v1;", PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0085-select-*-from-test-where"},
				},
			},
		},
		{
			Name: "SUM",
			SetUpScript: []string{
				"CREATE TABLE test(pk SERIAL PRIMARY KEY, v1 INT4);",
				"INSERT INTO test (v1) VALUES (1), (2), (3), (4), (5);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT SUM(v1) FROM test WHERE v1 BETWEEN 3 AND 5;", PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0086-select-sum-v1-from-test"},
				},
				{
					Query: "SELECT pg_typeof(SUM(v1)) FROM test WHERE v1 BETWEEN 3 AND 5;", PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0087-select-pg_typeof-sum-v1-from"},
				},
				{
					Query: "CREATE INDEX v1_idx ON test(v1);", PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0088-create-index-v1_idx-on-test"},
				},
				{
					Query: "SELECT SUM(v1) FROM test WHERE v1 BETWEEN 3 AND 5;", PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0089-select-sum-v1-from-test"},
				},
			},
		},
		{
			Name: "Empty statement",
			Assertions: []ScriptTestAssertion{
				{
					Query: ";", PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0090", Compare: "tag"},
				},
			},
		},
		{
			Name: "Unsupported MySQL statements",
			Assertions: []ScriptTestAssertion{
				{
					Query: "SHOW CREATE TABLE;", PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0091-show-create-table", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "querying tables with same name as pg_catalog tables",
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT attname FROM pg_catalog.pg_attribute ORDER BY attname LIMIT 3;", PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0092-select-attname-from-pg_catalog.pg_attribute-order"},
				},
				{
					Query: "SELECT attname FROM pg_attribute ORDER BY attname LIMIT 3;", PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0093-select-attname-from-pg_attribute-order"},
				},
				{
					Query: "CREATE TABLE pg_attribute (id INT);", PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0094-create-table-pg_attribute-id-int"},
				},
				{
					Query: "insert into pg_attribute values (1);", PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0095-insert-into-pg_attribute-values-1"},
				},
				{
					Query:    "insert into public.pg_attribute values (1);",
					Expected: []sql.Row{},
				},
				{
					Query: "SELECT attname FROM pg_attribute ORDER BY attname LIMIT 3;",
					Expected: []sql.Row{
						{"ACTION_CONDITION"},
						{"ACTION_ORDER"},
						{"ACTION_ORIENTATION"},
					},
				},
				{
					Query:    "SELECT * FROM public.pg_attribute;",
					Expected: []sql.Row{{1}},
				},
				{
					Query:       "drop table pg_attribute;",
					ExpectedErr: "tables cannot be dropped on database pg_catalog",
				},
				{
					Query:    "drop table public.pg_attribute;",
					Expected: []sql.Row{},
				},
				{
					Query:       "SELECT * FROM public.pg_attribute;",
					ExpectedErr: "table not found: pg_attribute",
				},
			},
		},
		{
			Name: "200 Row Test",
			SetUpScript: []string{
				"CREATE TABLE test (pk INT8 PRIMARY KEY);",
				"INSERT INTO test VALUES " +
					"(1),   (2),   (3),   (4),   (5),   (6),   (7),   (8),   (9),   (10)," +
					"(11),  (12),  (13),  (14),  (15),  (16),  (17),  (18),  (19),  (20)," +
					"(21),  (22),  (23),  (24),  (25),  (26),  (27),  (28),  (29),  (30)," +
					"(31),  (32),  (33),  (34),  (35),  (36),  (37),  (38),  (39),  (40)," +
					"(41),  (42),  (43),  (44),  (45),  (46),  (47),  (48),  (49),  (50)," +
					"(51),  (52),  (53),  (54),  (55),  (56),  (57),  (58),  (59),  (60)," +
					"(61),  (62),  (63),  (64),  (65),  (66),  (67),  (68),  (69),  (70)," +
					"(71),  (72),  (73),  (74),  (75),  (76),  (77),  (78),  (79),  (80)," +
					"(81),  (82),  (83),  (84),  (85),  (86),  (87),  (88),  (89),  (90)," +
					"(91),  (92),  (93),  (94),  (95),  (96),  (97),  (98),  (99),  (100)," +
					"(101), (102), (103), (104), (105), (106), (107), (108), (109), (110)," +
					"(111), (112), (113), (114), (115), (116), (117), (118), (119), (120)," +
					"(121), (122), (123), (124), (125), (126), (127), (128), (129), (130)," +
					"(131), (132), (133), (134), (135), (136), (137), (138), (139), (140)," +
					"(141), (142), (143), (144), (145), (146), (147), (148), (149), (150)," +
					"(151), (152), (153), (154), (155), (156), (157), (158), (159), (160)," +
					"(161), (162), (163), (164), (165), (166), (167), (168), (169), (170)," +
					"(171), (172), (173), (174), (175), (176), (177), (178), (179), (180)," +
					"(181), (182), (183), (184), (185), (186), (187), (188), (189), (190)," +
					"(191), (192), (193), (194), (195), (196), (197), (198), (199), (200);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT * FROM test ORDER BY pk;",
					Expected: []sql.Row{
						{1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10},
						{11}, {12}, {13}, {14}, {15}, {16}, {17}, {18}, {19}, {20},
						{21}, {22}, {23}, {24}, {25}, {26}, {27}, {28}, {29}, {30},
						{31}, {32}, {33}, {34}, {35}, {36}, {37}, {38}, {39}, {40},
						{41}, {42}, {43}, {44}, {45}, {46}, {47}, {48}, {49}, {50},
						{51}, {52}, {53}, {54}, {55}, {56}, {57}, {58}, {59}, {60},
						{61}, {62}, {63}, {64}, {65}, {66}, {67}, {68}, {69}, {70},
						{71}, {72}, {73}, {74}, {75}, {76}, {77}, {78}, {79}, {80},
						{81}, {82}, {83}, {84}, {85}, {86}, {87}, {88}, {89}, {90},
						{91}, {92}, {93}, {94}, {95}, {96}, {97}, {98}, {99}, {100},
						{101}, {102}, {103}, {104}, {105}, {106}, {107}, {108}, {109}, {110},
						{111}, {112}, {113}, {114}, {115}, {116}, {117}, {118}, {119}, {120},
						{121}, {122}, {123}, {124}, {125}, {126}, {127}, {128}, {129}, {130},
						{131}, {132}, {133}, {134}, {135}, {136}, {137}, {138}, {139}, {140},
						{141}, {142}, {143}, {144}, {145}, {146}, {147}, {148}, {149}, {150},
						{151}, {152}, {153}, {154}, {155}, {156}, {157}, {158}, {159}, {160},
						{161}, {162}, {163}, {164}, {165}, {166}, {167}, {168}, {169}, {170},
						{171}, {172}, {173}, {174}, {175}, {176}, {177}, {178}, {179}, {180},
						{181}, {182}, {183}, {184}, {185}, {186}, {187}, {188}, {189}, {190},
						{191}, {192}, {193}, {194}, {195}, {196}, {197}, {198}, {199}, {200},
					},
				},
			},
		},
		{
			Name: "INDEX as column name",
			SetUpScript: []string{
				`CREATE TABLE test1 (index INT4, CONSTRAINT index_constraint1 CHECK ((index >= 0)));`,
				`CREATE TABLE test2 ("IndeX" INT4, CONSTRAINT index_constraint2 CHECK (("IndeX" >= 0)));`,
				`INSERT INTO test1 VALUES (1);`,
				`INSERT INTO test2 VALUES (2);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM test1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0103-select-*-from-test1"},
				},
				{
					Query: `SELECT * FROM test2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0104-select-*-from-test2"},
				},
				{
					Query: `INSERT INTO test1 VALUES (-1);`, PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0105-insert-into-test1-values-1", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO test2 VALUES (-1);`, PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testsmoketests-0106-insert-into-test2-values-1", Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestEmptyQuery(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			// TODO: we want to be able to assert that the empty query returns a specific postgres backend message,
			//  EmptyQueryResponse. The pg library automatically converts this response to an empty-string CommandTag,
			//  which we can't tell apart from other empty CommandTag responses. We do assert that the command tag is empty,
			//  but it would nice to be able to assert a particular message type.
			Name: "Empty query test",
			Assertions: []ScriptTestAssertion{
				{
					Query: ";", PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testemptyquery-0001", Compare: "tag"},
				},
				{
					Query: " ", PostgresOracle: ScriptTestPostgresOracle{ID: "smoke-test-testemptyquery-0002", Compare: "tag"},
				},
			},
		},
	})
}
