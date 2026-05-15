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

// TestDmlReturningCanProjectTableoidRepro reproduces a DML RETURNING
// correctness gap: PostgreSQL allows RETURNING clauses to project system
// columns from the affected base-table rows.
func TestDmlReturningCanProjectTableoidRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DML RETURNING can project tableoid",
			SetUpScript: []string{
				`CREATE TABLE returning_tableoid_items (
					id INT PRIMARY KEY,
					v INT
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO returning_tableoid_items VALUES (1, 10)
						RETURNING tableoid::regclass::text, id, v;`, PostgresOracle: ScriptTestPostgresOracle{ID: "dml-returning-correctness-repro-test-testdmlreturningcanprojecttableoidrepro-0001-insert-into-returning_tableoid_items-values-1"},
				},
				{
					Query: `UPDATE returning_tableoid_items
						SET v = 20
						WHERE id = 1
						RETURNING tableoid::regclass::text, id, v;`, PostgresOracle: ScriptTestPostgresOracle{ID: "dml-returning-correctness-repro-test-testdmlreturningcanprojecttableoidrepro-0002-update-returning_tableoid_items-set-v-="},
				},
				{
					Query: `DELETE FROM returning_tableoid_items
						WHERE id = 1
						RETURNING tableoid::regclass::text, id, v;`, PostgresOracle: ScriptTestPostgresOracle{ID: "dml-returning-correctness-repro-test-testdmlreturningcanprojecttableoidrepro-0003-delete-from-returning_tableoid_items-where-id"},
				},
			},
		},
	})
}
