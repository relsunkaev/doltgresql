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

// PostgreSQL supports renaming table constraints and persists the new
// constraint name in pg_constraint and future violation diagnostics.
func TestAlterTableRenameConstraintRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TABLE RENAME CONSTRAINT updates check name",
			SetUpScript: []string{
				`CREATE TABLE rename_constraint_items (
					id INT,
					amount INT CONSTRAINT amount_positive CHECK (amount > 0)
				);`,
				`ALTER TABLE rename_constraint_items
					RENAME CONSTRAINT amount_positive TO amount_above_zero;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT conname
						FROM pg_catalog.pg_constraint
						WHERE conrelid = 'rename_constraint_items'::regclass
						ORDER BY conname;`,
					Expected: []sql.Row{{"amount_above_zero"}},
				},
				{
					Query:       `INSERT INTO rename_constraint_items VALUES (1, -1);`,
					ExpectedErr: `amount_above_zero`,
				},
			},
		},
		{
			Name: "ALTER TABLE RENAME CONSTRAINT preserves comments",
			SetUpScript: []string{
				`CREATE TABLE rename_constraint_comment_items (
					id INT,
					amount INT CONSTRAINT amount_positive_comment CHECK (amount > 0)
				);`,
				`COMMENT ON CONSTRAINT amount_positive_comment
					ON rename_constraint_comment_items IS 'kept constraint comment';`,
				`ALTER TABLE rename_constraint_comment_items
					RENAME CONSTRAINT amount_positive_comment TO amount_above_zero_comment;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT obj_description(oid, 'pg_constraint')
						FROM pg_catalog.pg_constraint
						WHERE conrelid = 'rename_constraint_comment_items'::regclass
							AND conname = 'amount_above_zero_comment';`,
					Expected: []sql.Row{{"kept constraint comment"}},
				},
			},
		},
	})
}
