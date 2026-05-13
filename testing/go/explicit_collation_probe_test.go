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

// TestExplicitCollationProbe pins how runtime `COLLATE` references
// behave in queries and DDL today. dump output emits explicit
// COLLATE references when columns are declared with non-default
// collations; ORM tools surface the same shapes for sort-stable
// query plans. Per the Schema/DDL TODO in
// docs/app-compatibility-checklist.md.
func TestExplicitCollationProbe(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "COLLATE \"C\" in ORDER BY runs",
			SetUpScript: []string{
				`CREATE TABLE letters (id INT PRIMARY KEY, c TEXT);`,
				`INSERT INTO letters VALUES (1, 'b'), (2, 'A'), (3, 'a'), (4, 'B');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					// COLLATE "C" sorts ASCII-byte order:
					// upper-case before lower-case.
					Query: `SELECT c FROM letters ORDER BY c COLLATE "C", id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "explicit-collation-probe-test-testexplicitcollationprobe-0001-select-c-from-letters-order"},
				},
			},
		},
		{
			Name: "COLLATE \"POSIX\" in ORDER BY runs",
			SetUpScript: []string{
				`CREATE TABLE words (id INT PRIMARY KEY, w TEXT);`,
				`INSERT INTO words VALUES (1, 'Z'), (2, 'a'), (3, 'A');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT w FROM words ORDER BY w COLLATE "POSIX", id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "explicit-collation-probe-test-testexplicitcollationprobe-0002-select-w-from-words-order"},
				},
			},
		},
		{
			Name: "CREATE TABLE column with explicit COLLATE \"C\" round-trips",
			SetUpScript: []string{
				`CREATE TABLE labels (id INT PRIMARY KEY, name TEXT COLLATE "C");`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT collation_name
						FROM information_schema.columns
						WHERE table_name = 'labels' AND column_name = 'name';`, PostgresOracle: ScriptTestPostgresOracle{ID: "explicit-collation-probe-test-testexplicitcollationprobe-0003-select-collation_name-from-information_schema.columns-where"},
				},
			},
		},
	})
}
