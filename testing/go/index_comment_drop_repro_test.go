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

// TestDropIndexClearsCommentRepro reproduces a metadata persistence bug:
// dropping an index does not clear COMMENT ON INDEX metadata, so a later index
// with the same name inherits the dropped index's comment.
func TestDropIndexClearsCommentRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP INDEX clears index comment before recreate",
			SetUpScript: []string{
				`CREATE TABLE drop_recreate_comment_index_table (
					id INT PRIMARY KEY,
					v INT
				);`,
				`CREATE INDEX drop_recreate_comment_index_idx
					ON drop_recreate_comment_index_table (v);`,
				`COMMENT ON INDEX drop_recreate_comment_index_idx
					IS 'old private index';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP INDEX drop_recreate_comment_index_idx;`,
				},
				{
					Query: `CREATE INDEX drop_recreate_comment_index_idx
						ON drop_recreate_comment_index_table (v);`,
				},
				{
					Query: `SELECT obj_description(
						'drop_recreate_comment_index_idx'::regclass,
						'pg_class');`, PostgresOracle: ScriptTestPostgresOracle{ID: "index-comment-drop-repro-test-testdropindexclearscommentrepro-0001-select-obj_description-drop_recreate_comment_index_idx-::regclass-pg_class"},
				},
			},
		},
	})
}
