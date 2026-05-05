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
	"strconv"
	"strings"
	"testing"

	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/testing/go/testdata"

	"github.com/dolthub/go-mysql-server/sql"
)

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
			},
			Assertions: []ScriptTestAssertion{
				{
					// Unsupported btree opclasses are rejected instead of being hidden by other ignored options.
					Query:       "CREATE INDEX v1_idx ON test(v1 varchar_pattern_ops) WITH (storage_opt1 = foo) TABLESPACE tablespace_name;",
					ExpectedErr: "operator class varchar_pattern_ops is not yet supported for btree indexes",
				},
				{
					Query:       "CREATE INDEX v1_idx2 ON test using hash (v1);",
					ExpectedErr: "not yet supported",
				},
				{
					Query:       "CREATE INDEX v1_idx2 ON test(v1) WHERE v1 > 100;",
					ExpectedErr: "not yet supported",
				},
				{
					Query:       "CREATE INDEX v1_idx2 ON test(v1) INCLUDE (pk);",
					ExpectedErr: "not yet supported",
				},
				{
					Query:       "CREATE INDEX v1_idx_storage ON test(v1) WITH (fillfactor = 70);",
					ExpectedErr: "storage parameters are not yet supported for indexes",
				},
				{
					Query:       "CREATE INDEX v1_idx_tablespace ON test(v1) TABLESPACE pg_default;",
					ExpectedErr: "TABLESPACE is not yet supported for indexes",
				},
			},
		},
		{
			Name: "PostgreSQL drop index restrict lifecycle",
			SetUpScript: []string{
				"CREATE TABLE drop_index_restrict (id INTEGER PRIMARY KEY, v INTEGER);",
				"CREATE INDEX drop_index_restrict_idx ON drop_index_restrict (v);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "DROP INDEX drop_index_restrict_idx RESTRICT;",
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
					Query:       "DROP INDEX drop_index_restrict_pkey CASCADE;",
					ExpectedErr: "CASCADE is not yet supported",
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
			Name: "PostgreSQL unique nulls not distinct unsupported boundary",
			SetUpScript: []string{
				"CREATE TABLE unique_nulls_not_distinct (id INTEGER PRIMARY KEY, v INTEGER);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       "CREATE UNIQUE INDEX unique_nulls_not_distinct_idx ON unique_nulls_not_distinct (v) NULLS NOT DISTINCT;",
					ExpectedErr: "NULLS NOT DISTINCT is not yet supported",
				},
				{
					Query: `SELECT indexname
FROM pg_catalog.pg_indexes
WHERE tablename = 'unique_nulls_not_distinct'
ORDER BY indexname;`,
					Expected: []sql.Row{
						{"unique_nulls_not_distinct_pkey"},
					},
				},
			},
		},
		{
			Name: "PostgreSQL btree sort option metadata",
			SetUpScript: []string{
				"CREATE TABLE index_sort_meta (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER);",
				"CREATE INDEX index_sort_meta_idx ON index_sort_meta (a DESC, b ASC NULLS FIRST);",
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
					Query: `SELECT c.relname, i.indclass
FROM pg_catalog.pg_index i
JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
WHERE c.relname IN ('index_sort_meta_idx', 'index_sort_meta_pkey')
ORDER BY c.relname;`,
					Expected: []sql.Row{
						{"index_sort_meta_idx", opClassOidVector("int4_ops", "int4_ops")},
						{"index_sort_meta_pkey", opClassOidVector("int4_ops")},
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
					Query:       "CREATE INDEX btree_opclass_meta_bad_idx ON btree_opclass_meta (a jsonb_ops);",
					ExpectedErr: "operator class jsonb_ops is not yet supported for btree indexes",
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
						{"jsonb_gin_ops_idx", opClassOidVector("jsonb_ops")},
						{"jsonb_gin_path_idx", opClassOidVector("jsonb_path_ops")},
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
					Query: `SELECT COUNT(*), COUNT(DISTINCT row_id)
FROM dg_gin_jsonb_gin_backfill_jsonb_gin_backfill_idx_postings;`,
					Expected: []sql.Row{{12, 2}},
				},
				{
					Query: `SELECT token, COUNT(*)
FROM dg_gin_jsonb_gin_backfill_jsonb_gin_backfill_idx_postings
WHERE token IN ('9:jsonb_ops3:key1:01:a', '9:jsonb_ops3:key1:01:x')
GROUP BY token
ORDER BY token;`,
					Expected: []sql.Row{
						{"9:jsonb_ops3:key1:01:a", 2},
						{"9:jsonb_ops3:key1:01:x", 1},
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
					Query: `SELECT COUNT(*), COUNT(DISTINCT row_id)
FROM dg_gin_jsonb_gin_dml_jsonb_gin_dml_idx_postings;`,
					Expected: []sql.Row{{8, 2}},
				},
				{
					Query: `UPDATE jsonb_gin_dml
SET doc = '{"a":3,"tags":["z"]}'
WHERE id = 1;`,
				},
				{
					Query: `SELECT token, COUNT(*)
FROM dg_gin_jsonb_gin_dml_jsonb_gin_dml_idx_postings
WHERE token IN ('9:jsonb_ops3:key1:01:x', '9:jsonb_ops3:key1:01:z')
GROUP BY token
ORDER BY token;`,
					Expected: []sql.Row{
						{"9:jsonb_ops3:key1:01:z", 1},
					},
				},
				{
					Query: "DELETE FROM jsonb_gin_dml WHERE id = 2;",
				},
				{
					Query: `SELECT COUNT(*), COUNT(DISTINCT row_id)
FROM dg_gin_jsonb_gin_dml_jsonb_gin_dml_idx_postings;`,
					Expected: []sql.Row{{4, 1}},
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
					Query: `SELECT COUNT(*), COUNT(DISTINCT row_id)
	FROM dg_gin_jsonb_gin_txn_jsonb_gin_txn_idx_postings;`,
					Expected: []sql.Row{{8, 2}},
				},
				{
					Query: "ROLLBACK;",
				},
				{
					Query: `SELECT COUNT(*), COUNT(DISTINCT row_id)
	FROM dg_gin_jsonb_gin_txn_jsonb_gin_txn_idx_postings;`,
					Expected: []sql.Row{{4, 1}},
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
					Query: `SELECT token, COUNT(*)
	FROM dg_gin_jsonb_gin_txn_jsonb_gin_txn_idx_postings
	WHERE token IN ('9:jsonb_ops3:key1:01:x', '9:jsonb_ops3:key1:01:z')
	GROUP BY token
	ORDER BY token;`,
					Expected: []sql.Row{
						{"9:jsonb_ops3:key1:01:z", 1},
					},
				},
				{
					Query: "ROLLBACK;",
				},
				{
					Query: `SELECT token, COUNT(*)
	FROM dg_gin_jsonb_gin_txn_jsonb_gin_txn_idx_postings
	WHERE token IN ('9:jsonb_ops3:key1:01:x', '9:jsonb_ops3:key1:01:z')
	GROUP BY token
	ORDER BY token;`,
					Expected: []sql.Row{
						{"9:jsonb_ops3:key1:01:x", 1},
					},
				},
				{
					Query: "BEGIN;",
				},
				{
					Query: "DELETE FROM jsonb_gin_txn WHERE id = 1;",
				},
				{
					Query: `SELECT COUNT(*), COUNT(DISTINCT row_id)
	FROM dg_gin_jsonb_gin_txn_jsonb_gin_txn_idx_postings;`,
					Expected: []sql.Row{{0, 0}},
				},
				{
					Query: "ROLLBACK;",
				},
				{
					Query: `SELECT COUNT(*), COUNT(DISTINCT row_id)
	FROM dg_gin_jsonb_gin_txn_jsonb_gin_txn_idx_postings;`,
					Expected: []sql.Row{{4, 1}},
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
					Query: `SELECT COUNT(*), COUNT(DISTINCT row_id)
FROM dg_gin_jsonb_gin_lifecycle_jsonb_gin_lifecycle_idx_postings;`,
					Expected: []sql.Row{{8, 2}},
				},
				{
					Query: "DROP INDEX jsonb_gin_lifecycle_renamed_idx;",
				},
				{
					Query:       "SELECT COUNT(*) FROM dg_gin_jsonb_gin_lifecycle_jsonb_gin_lifecycle_idx_postings;",
					ExpectedErr: "table not found",
				},
				{
					Query: "CREATE INDEX jsonb_gin_lifecycle_idx ON jsonb_gin_lifecycle USING gin (doc);",
				},
				{
					Query: `SELECT COUNT(*), COUNT(DISTINCT row_id)
FROM dg_gin_jsonb_gin_lifecycle_jsonb_gin_lifecycle_idx_postings;`,
					Expected: []sql.Row{{8, 2}},
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
						{"             └─ filters: [{[jsonb_gin_lookup_idx intersect 2 token(s), jsonb_gin_lookup_idx intersect 2 token(s)]}]"},
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
						{"             └─ filters: [{[jsonb_gin_path_lookup_idx intersect 1 token(s), jsonb_gin_path_lookup_idx intersect 1 token(s)]}]"},
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
					Query:    "INSERT INTO items (title, metadata, updated_at) VALUES ('ABC', '{}', '2026-10-10 01:02:03');",
					Expected: []sql.Row{},
				},
				{
					Query:       "INSERT INTO items (title, metadata, updated_at) VALUES ('abc', '{}', '2026-11-12 03:04:05');",
					ExpectedErr: "duplicate unique key given",
				},
			},
		},
	})
}

func opClassOidVector(names ...string) string {
	oids := make([]string, len(names))
	for i, name := range names {
		oid := id.Cache().ToOID(id.NewId(id.Section_OperatorClass, name))
		oids[i] = strconv.FormatUint(uint64(oid), 10)
	}
	return strings.Join(oids, " ")
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
