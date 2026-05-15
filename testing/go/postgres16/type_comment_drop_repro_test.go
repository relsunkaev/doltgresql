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

// TestDropTypeClearsCommentRepro reproduces a metadata persistence bug:
// dropping a type does not clear COMMENT ON TYPE metadata, so a later type with
// the same name inherits the dropped type's comment.
func TestDropTypeClearsCommentRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP TYPE clears type comment before recreate",
			SetUpScript: []string{
				`CREATE TYPE drop_recreate_comment_type AS ENUM ('old');`,
				`COMMENT ON TYPE drop_recreate_comment_type IS 'old private type';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP TYPE drop_recreate_comment_type;`,
				},
				{
					Query: `CREATE TYPE drop_recreate_comment_type AS ENUM ('new');`,
				},
				{
					Query: `SELECT obj_description(
						(SELECT oid FROM pg_type WHERE typname = 'drop_recreate_comment_type'),
						'pg_type');`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-comment-drop-repro-test-testdroptypeclearscommentrepro-0001-select-obj_description-select-oid-from"},
				},
			},
		},
	})
}
