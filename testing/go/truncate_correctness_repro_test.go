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

// TestTruncateMultipleTablesRepro reproduces a PostgreSQL correctness bug:
// TRUNCATE accepts a relation list and should truncate all named tables in one
// statement.
func TestTruncateMultipleTablesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "TRUNCATE multiple tables",
			SetUpScript: []string{
				`CREATE TABLE truncate_multi_a (id INT PRIMARY KEY, label TEXT);`,
				`CREATE TABLE truncate_multi_b (id INT PRIMARY KEY, label TEXT);`,
				`INSERT INTO truncate_multi_a VALUES (1, 'a');`,
				`INSERT INTO truncate_multi_b VALUES (2, 'b');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `TRUNCATE truncate_multi_a, truncate_multi_b;`,
				},
				{
					Query: `SELECT
						(SELECT count(*) FROM truncate_multi_a),
						(SELECT count(*) FROM truncate_multi_b);`, PostgresOracle: ScriptTestPostgresOracle{ID: "truncate-correctness-repro-test-testtruncatemultipletablesrepro-0001-select-select-count-*-from"},
				},
			},
		},
	})
}
