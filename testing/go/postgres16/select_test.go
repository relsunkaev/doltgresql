// Copyright 2025 Dolthub, Inc.
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

// TestSelect covers SELECT syntax not covered by our MySQL select tests
func TestSelect(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "SELECT empty",
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT;", PostgresOracle: ScriptTestPostgresOracle{ID: "select-test-testselect-0001-select"},
				},
			},
		},
		{
			Name: "SELECT DISTINCT ON",
			SetUpScript: []string{
				"CREATE TABLE test (v1 INT4, v2 INT4);",
				"INSERT INTO test VALUES (1, 3), (1, 4), (2, 3), (2, 4);",
				"CREATE TABLE test2 (v1 INT4, v2 INT4, v3 INT4);",
				"INSERT INTO test2 VALUES (1, 3, 5), (2, 3, 5), (1, 4, 5), (2, 4, 5);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT * FROM test ORDER BY v1, v2;", PostgresOracle: ScriptTestPostgresOracle{ID: "select-test-testselect-0002-select-*-from-test-order"},
				},
				{
					Query: "SELECT DISTINCT * FROM test ORDER BY v1, v2;", PostgresOracle: ScriptTestPostgresOracle{ID: "select-test-testselect-0003-select-distinct-*-from-test"},
				},
				{
					Query: "SELECT DISTINCT ON(v1) * FROM test ORDER BY v1, v2;", PostgresOracle: ScriptTestPostgresOracle{ID: "select-test-testselect-0004-select-distinct-on-v1-*"},
				},
				{
					Query: "SELECT DISTINCT ON(v2) * FROM test ORDER BY v2, v1;", PostgresOracle: ScriptTestPostgresOracle{ID: "select-test-testselect-0005-select-distinct-on-v2-*"},
				},
				{
					Query: "SELECT DISTINCT ON(v1) * FROM test ORDER BY v2, v1;", PostgresOracle: ScriptTestPostgresOracle{ID: "select-test-testselect-0006-select-distinct-on-v1-*", Compare: "sqlstate"},
				},
				{
					Query: "SELECT DISTINCT ON(v2) * FROM test ORDER BY v2 DESC, v1 DESC;", PostgresOracle: ScriptTestPostgresOracle{ID: "select-test-testselect-0007-select-distinct-on-v2-*"},
				},
				{
					Query: "SELECT DISTINCT ON(v2, v1) * FROM test2 ORDER BY v1, v2;", PostgresOracle: ScriptTestPostgresOracle{ID: "select-test-testselect-0008-select-distinct-on-v2-v1"},
				},
				{
					Query: "SELECT DISTINCT ON(v2, v1) * FROM test2 ORDER BY v1, v2 DESC;", PostgresOracle: ScriptTestPostgresOracle{ID: "select-test-testselect-0009-select-distinct-on-v2-v1"},
				},
				{
					Query: "SELECT DISTINCT ON(v2, v1) * FROM test2 ORDER BY v1, v2 LIMIT 1;", PostgresOracle: ScriptTestPostgresOracle{ID: "select-test-testselect-0010-select-distinct-on-v2-v1"},
				},
				{
					Query: "SELECT DISTINCT ON(v2, v1) * FROM test2 ORDER BY v1, v2 DESC LIMIT 1;", PostgresOracle: ScriptTestPostgresOracle{ID: "select-test-testselect-0011-select-distinct-on-v2-v1"},
				},
				{
					Query: "SELECT DISTINCT ON(v1, v2, v3) * FROM test2 ORDER BY v1, v2;", PostgresOracle: ScriptTestPostgresOracle{ID: "select-test-testselect-0012-select-distinct-on-v1-v2"},
				},
				{
					Query: "SELECT DISTINCT ON(v3) v1 FROM test2;", PostgresOracle: ScriptTestPostgresOracle{ID: "select-test-testselect-0013-select-distinct-on-v3-v1"},
				},
				{
					Query: "SELECT DISTINCT ON(v1, v3) * FROM test2 ORDER BY v1, v2;", PostgresOracle: ScriptTestPostgresOracle{ID: "select-test-testselect-0014-select-distinct-on-v1-v3", Compare: "sqlstate"},
				},
				{
					Query: "SELECT DISTINCT ON(v2) * FROM test2 ORDER BY v1, v2;", PostgresOracle: ScriptTestPostgresOracle{ID: "select-test-testselect-0015-select-distinct-on-v2-*", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "select values",
			Assertions: []ScriptTestAssertion{
				{
					Query: "select * from (values(1,'峰哥',18),(2,'王哥',20),(3,'张哥',22));", PostgresOracle: ScriptTestPostgresOracle{ID: "select-test-testselect-0016-select-*-from-values-1"},
				},
				{
					Query: "select * from (values(1,'峰哥',18),(2,'王哥',20),(3,'张哥',22)) x(id,name,age);", PostgresOracle: ScriptTestPostgresOracle{ID: "select-test-testselect-0017-select-*-from-values-1"},
				},
				{
					Query:          "select * from (values(1,'峰哥',18),(2,'王哥',20),(3,'张哥',22)) x(id,name,age) limit $1;",
					BindVars:       []any{2}, // forcing this to use prepared statements
					PostgresOracle: ScriptTestPostgresOracle{ID: "select-test-testselect-0018-select-*-from-values-1"},
				},
			},
		},
		{
			Name: "SELECT with no select expressions",
			SetUpScript: []string{
				"CREATE TABLE mytable (pk int primary key);",
				"INSERT INTO mytable VALUES (1), (2);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "select from mytable;", PostgresOracle: ScriptTestPostgresOracle{ID: "select-test-testselect-0019-select-from-mytable"},
				},
				{
					// https://github.com/dolthub/doltgresql/issues/1470
					Query: "SELECT EXISTS (SELECT FROM mytable where pk > 0);", PostgresOracle: ScriptTestPostgresOracle{ID: "select-test-testselect-0020-select-exists-select-from-mytable"},
				},
			},
		},
	})
}
