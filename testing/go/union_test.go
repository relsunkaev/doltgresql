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
	"testing"
)

func TestUnion(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "union tests",
			SetUpScript: []string{
				`CREATE TABLE t1 (i INT PRIMARY KEY);`,
				`CREATE TABLE t2 (j INT PRIMARY KEY);`,
				`INSERT INTO t1 VALUES (1), (2), (3);`,
				`INSERT INTO t2 VALUES (2), (3), (4);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM t1 UNION SELECT * FROM t2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "union-test-testunion-0001-select-*-from-t1-union"},
				},
				{
					Query: `SELECT 123 UNION SELECT 456;`, PostgresOracle: ScriptTestPostgresOracle{ID: "union-test-testunion-0002-select-123-union-select-456"},
				},
				{
					Query: `SELECT * FROM (VALUES (123), (456)) a UNION SELECT * FROM (VALUES (456), (789)) b;`, PostgresOracle: ScriptTestPostgresOracle{ID: "union-test-testunion-0003-select-*-from-values-123"},
				},
			},
		},
	})
}

func TestIntersect(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "intersect tests",
			SetUpScript: []string{
				`CREATE TABLE t1 (i INT PRIMARY KEY);`,
				`CREATE TABLE t2 (j INT PRIMARY KEY);`,
				`INSERT INTO t1 VALUES (1), (2), (3);`,
				`INSERT INTO t2 VALUES (2), (3), (4);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM t1 INTERSECT SELECT * FROM t2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "union-test-testintersect-0001-select-*-from-t1-intersect"},
				},
				{
					Query: `SELECT 123 INTERSECT SELECT 456;`, PostgresOracle: ScriptTestPostgresOracle{ID: "union-test-testintersect-0002-select-123-intersect-select-456"},
				},
				{
					Query: `SELECT * FROM (VALUES (123), (456)) a INTERSECT SELECT * FROM (VALUES (456), (789)) b;`, PostgresOracle: ScriptTestPostgresOracle{ID: "union-test-testintersect-0003-select-*-from-values-123"},
				},
			},
		},
	})
}

func TestExcept(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "except tests",
			SetUpScript: []string{
				`CREATE TABLE t1 (i INT PRIMARY KEY);`,
				`CREATE TABLE t2 (j INT PRIMARY KEY);`,
				`INSERT INTO t1 VALUES (1), (2), (3);`,
				`INSERT INTO t2 VALUES (2), (3), (4);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM t1 EXCEPT SELECT * FROM t2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "union-test-testexcept-0001-select-*-from-t1-except"},
				},
				{
					Query: `SELECT 123 EXCEPT SELECT 456;`, PostgresOracle: ScriptTestPostgresOracle{ID: "union-test-testexcept-0002-select-123-except-select-456"},
				},
				{
					Query: `SELECT * FROM (VALUES (123), (456)) a EXCEPT SELECT * FROM (VALUES (456), (789)) b;`, PostgresOracle: ScriptTestPostgresOracle{ID: "union-test-testexcept-0003-select-*-from-values-123"},
				},
			},
		},
	})
}
