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

// TestDropTableClearsCommentRepro reproduces a metadata persistence bug:
// dropping a table does not clear COMMENT ON TABLE metadata, so a later table
// with the same name inherits the dropped table's comment.
func TestDropTableClearsCommentRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP TABLE clears relation comment before recreate",
			SetUpScript: []string{
				`CREATE TABLE drop_recreate_comment_target (id INT PRIMARY KEY);`,
				`COMMENT ON TABLE drop_recreate_comment_target IS 'old private table';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP TABLE drop_recreate_comment_target;`,
				},
				{
					Query: `CREATE TABLE drop_recreate_comment_target (id INT PRIMARY KEY);`,
				},
				{
					Query: `SELECT obj_description('drop_recreate_comment_target'::regclass);`, PostgresOracle: ScriptTestPostgresOracle{ID: "relation-comment-drop-repro-test-testdroptableclearscommentrepro-0001-select-obj_description-drop_recreate_comment_target-::regclass"},
				},
			},
		},
	})
}
