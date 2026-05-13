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

// TestDropSequenceClearsCommentRepro reproduces a metadata persistence bug:
// dropping a sequence does not clear COMMENT ON SEQUENCE metadata, so a later
// sequence with the same name inherits the dropped sequence's comment.
func TestDropSequenceClearsCommentRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP SEQUENCE clears sequence comment before recreate",
			SetUpScript: []string{
				`CREATE SEQUENCE drop_recreate_comment_sequence;`,
				`COMMENT ON SEQUENCE drop_recreate_comment_sequence
					IS 'old private sequence';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP SEQUENCE drop_recreate_comment_sequence;`,
				},
				{
					Query: `CREATE SEQUENCE drop_recreate_comment_sequence;`,
				},
				{
					Query: `SELECT obj_description(
						'drop_recreate_comment_sequence'::regclass,
						'pg_class');`,
					Expected: []sql.Row{{nil}},
				},
			},
		},
	})
}
