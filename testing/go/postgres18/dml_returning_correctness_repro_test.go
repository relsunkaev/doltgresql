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

package postgres18

import (
	. "github.com/dolthub/doltgresql/testing/go"

	"testing"
)

// TestPostgres18InsertDeleteReturningOldNewAliasesRepro reproduces PostgreSQL
// 18 compatibility gaps: INSERT and DELETE RETURNING can explicitly project
// old and new row aliases.
func TestPostgres18InsertDeleteReturningOldNewAliasesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "INSERT and DELETE RETURNING expose old and new row aliases",
			SetUpScript: []string{
				`CREATE TABLE returning_insert_delete_old_new_items (
					id INT PRIMARY KEY,
					v INT NOT NULL
				);`,
				`INSERT INTO returning_insert_delete_old_new_items VALUES (1, 10);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO returning_insert_delete_old_new_items VALUES (2, 20)
						RETURNING old.v IS NULL, new.v;`, PostgresOracle: ScriptTestPostgresOracle{ID: "postgres18/dml-returning-correctness-repro-test-testpostgres18insertdeletereturningoldnewaliasesrepro-0001-insert-into-returning_insert_delete_old_new_items-values-2"},
				},
				{
					Query: `DELETE FROM returning_insert_delete_old_new_items
						WHERE id = 1
						RETURNING old.v, new.v IS NULL;`, PostgresOracle: ScriptTestPostgresOracle{ID: "postgres18/dml-returning-correctness-repro-test-testpostgres18insertdeletereturningoldnewaliasesrepro-0002-delete-from-returning_insert_delete_old_new_items-where-id"},
				},
			},
		},
	})
}

// TestPostgres18UpdateReturningOldNewAliasesRepro reproduces a PostgreSQL 18
// compatibility gap: UPDATE RETURNING can project the pre-update row through
// old and the post-update row through new.
func TestPostgres18UpdateReturningOldNewAliasesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "UPDATE RETURNING exposes old and new row aliases",
			SetUpScript: []string{
				`CREATE TABLE returning_old_new_items (
					id INT PRIMARY KEY,
					v INT NOT NULL
				);`,
				`INSERT INTO returning_old_new_items VALUES (1, 10);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE returning_old_new_items
						SET v = v + 5
						WHERE id = 1
						RETURNING old.v, new.v;`, PostgresOracle: ScriptTestPostgresOracle{ID: "postgres18/dml-returning-correctness-repro-test-testpostgres18updatereturningoldnewaliasesrepro-0001-update-returning_old_new_items-set-v-="},
				},
			},
		},
	})
}
