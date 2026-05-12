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

	"github.com/dolthub/go-mysql-server/sql"
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
						RETURNING tableoid::regclass::text, id, v;`,
					Expected: []sql.Row{{"returning_tableoid_items", 1, 10}},
				},
				{
					Query: `UPDATE returning_tableoid_items
						SET v = 20
						WHERE id = 1
						RETURNING tableoid::regclass::text, id, v;`,
					Expected: []sql.Row{{"returning_tableoid_items", 1, 20}},
				},
				{
					Query: `DELETE FROM returning_tableoid_items
						WHERE id = 1
						RETURNING tableoid::regclass::text, id, v;`,
					Expected: []sql.Row{{"returning_tableoid_items", 1, 20}},
				},
			},
		},
	})
}

// TestUpdateReturningOldNewAliasesRepro reproduces a PostgreSQL compatibility
// gap: UPDATE RETURNING can project the pre-update row through old and the
// post-update row through new.
func TestUpdateReturningOldNewAliasesRepro(t *testing.T) {
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
						RETURNING old.v, new.v;`,
					Expected: []sql.Row{{10, 15}},
				},
			},
		},
	})
}

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
						RETURNING old.v IS NULL, new.v;`,
					Expected: []sql.Row{{true, 20}},
				},
				{
					Query: `DELETE FROM returning_insert_delete_old_new_items
						WHERE id = 1
						RETURNING old.v, new.v IS NULL;`,
					Expected: []sql.Row{{10, true}},
				},
			},
		},
	})
}
