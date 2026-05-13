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

// TestDropViewClearsCommentRepro reproduces a metadata persistence bug:
// dropping a view does not clear COMMENT ON VIEW metadata, so a later view with
// the same name inherits the dropped view's comment.
func TestDropViewClearsCommentRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP VIEW clears view comment before recreate",
			SetUpScript: []string{
				`CREATE VIEW drop_recreate_comment_view AS SELECT 1 AS id;`,
				`COMMENT ON VIEW drop_recreate_comment_view
					IS 'old private view';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP VIEW drop_recreate_comment_view;`,
				},
				{
					Query: `CREATE VIEW drop_recreate_comment_view AS SELECT 2 AS id;`,
				},
				{
					Query: `SELECT obj_description(
						'drop_recreate_comment_view'::regclass,
						'pg_class');`, PostgresOracle: ScriptTestPostgresOracle{ID: "view-comment-drop-repro-test-testdropviewclearscommentrepro-0001-select-obj_description-drop_recreate_comment_view-::regclass-pg_class"},
				},
			},
		},
	})
}
