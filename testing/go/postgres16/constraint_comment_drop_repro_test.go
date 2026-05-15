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

// TestDropConstraintClearsCommentRepro reproduces a metadata persistence bug:
// dropping a constraint does not clear COMMENT ON CONSTRAINT metadata, so a
// later constraint with the same name inherits the dropped constraint's comment.
func TestDropConstraintClearsCommentRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP CONSTRAINT clears constraint comment before recreate",
			SetUpScript: []string{
				`CREATE TABLE drop_recreate_comment_constraint_table (
					id INT PRIMARY KEY,
					v INT,
					CONSTRAINT drop_recreate_comment_constraint CHECK (v > 0)
				);`,
				`COMMENT ON CONSTRAINT drop_recreate_comment_constraint
					ON drop_recreate_comment_constraint_table
					IS 'old private constraint';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE drop_recreate_comment_constraint_table
						DROP CONSTRAINT drop_recreate_comment_constraint;`,
				},
				{
					Query: `ALTER TABLE drop_recreate_comment_constraint_table
						ADD CONSTRAINT drop_recreate_comment_constraint CHECK (v >= 0);`,
				},
				{
					Query: `SELECT obj_description(
						(SELECT oid FROM pg_constraint
						 WHERE conname = 'drop_recreate_comment_constraint'
						   AND conrelid = 'drop_recreate_comment_constraint_table'::regclass),
						'pg_constraint');`, PostgresOracle: ScriptTestPostgresOracle{ID: "constraint-comment-drop-repro-test-testdropconstraintclearscommentrepro-0001-select-obj_description-select-oid-from"},
				},
			},
		},
	})
}
