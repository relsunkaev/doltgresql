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

// TestDropColumnClearsCommentRepro reproduces a metadata persistence bug:
// dropping a column does not clear COMMENT ON COLUMN metadata, so a later
// column that reuses the same table/ordinal inherits the dropped column's
// comment.
func TestDropColumnClearsCommentRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP COLUMN clears column comment before re-add",
			SetUpScript: []string{
				`CREATE TABLE drop_recreate_comment_column_table (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`COMMENT ON COLUMN drop_recreate_comment_column_table.label
					IS 'old private column';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE drop_recreate_comment_column_table
						DROP COLUMN label;`,
				},
				{
					Query: `ALTER TABLE drop_recreate_comment_column_table
						ADD COLUMN label TEXT;`,
				},
				{
					Query: `SELECT col_description(
						'drop_recreate_comment_column_table'::regclass,
						2);`, PostgresOracle: ScriptTestPostgresOracle{ID: "column-comment-drop-repro-test-testdropcolumnclearscommentrepro-0001-select-col_description-drop_recreate_comment_column_table-::regclass-2"},
				},
			},
		},
	})
}
