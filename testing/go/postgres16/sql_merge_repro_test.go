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

package postgres16

import (
	. "github.com/dolthub/doltgresql/testing/go"

	"testing"
)

// TestSqlMergeUpdateAndInsertRepro reproduces a PostgreSQL 15 compatibility
// gap: SQL MERGE can update matched target rows and insert unmatched source
// rows in one statement.
func TestSqlMergeUpdateAndInsertRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "SQL MERGE updates matched rows and inserts unmatched rows",
			SetUpScript: []string{
				`CREATE TABLE sql_merge_target (
					id INT PRIMARY KEY,
					label TEXT NOT NULL
				);`,
				`CREATE TABLE sql_merge_source (
					id INT PRIMARY KEY,
					label TEXT NOT NULL
				);`,
				`INSERT INTO sql_merge_target VALUES (1, 'old'), (2, 'keep');`,
				`INSERT INTO sql_merge_source VALUES (1, 'updated'), (3, 'inserted');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `MERGE INTO sql_merge_target AS target
						USING sql_merge_source AS source
						ON target.id = source.id
						WHEN MATCHED THEN
							UPDATE SET label = source.label
						WHEN NOT MATCHED THEN
							INSERT (id, label) VALUES (source.id, source.label);`,
				},
				{
					Query: `SELECT id, label
						FROM sql_merge_target
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "sql-merge-repro-test-testsqlmergeupdateandinsertrepro-0001-select-id-label-from-sql_merge_target"},
				},
			},
		},
	})
}
