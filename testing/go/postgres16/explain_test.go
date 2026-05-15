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

package postgres16

import (
	. "github.com/dolthub/doltgresql/testing/go"

	"testing"
)

func TestExplain(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "basic explain tests",
			SetUpScript: []string{
				`CREATE TABLE t (i INT PRIMARY KEY)`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Skip:  true, // Our explain output is very different
					Query: `EXPLAIN SELECT * FROM T;`, PostgresOracle: ScriptTestPostgresOracle{ID: "explain-test-testexplain-0001-explain-select-*-from-t", ColumnModes: []string{"explain"}},
				},
				{
					Skip: true, // Need to properly support explain options
					Query: `
EXPLAIN 
(
	ANALYZE, 
	VERBOSE, 
	COSTS, 
	SETTINGS,
	BUFFERS,
	WAL,
	TIMING,
	SUMMARY,
	FORMAT TEXT
) 
	SELECT * FROM t;
`, PostgresOracle: ScriptTestPostgresOracle{ID: "explain-test-testexplain-0002-explain-analyze-verbose-costs-settings", ColumnModes: []string{"explain"}},
				},
				{
					Skip: true, // Need to properly support explain options
					Query: `
EXPLAIN 
(
	ANALYZE ON, 
	VERBOSE OFF, 
	COSTS TRUE, 
	SETTINGS FALSE,
	BUFFERS,
	WAL,
	TIMING,
	SUMMARY,
	FORMAT TEXT
) 
	SELECT * FROM t;
`, PostgresOracle: ScriptTestPostgresOracle{ID: "explain-test-testexplain-0003-explain-analyze-on-verbose-off", ColumnModes: []string{"explain"}},
				},
				{
					Skip: true, // Need to properly support explain options
					Query: `
EXPLAIN 
(
	NOTAVALIDOPTION
) 
	SELECT * FROM t;
`, PostgresOracle: ScriptTestPostgresOracle{ID: "explain-test-testexplain-0004-explain-notavalidoption-select-*-from", Compare: "sqlstate"},
				},
			},
		},
	})
}
