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

// TestDropRoleClearsCommentRepro reproduces a shared-metadata persistence bug:
// dropping a role does not clear COMMENT ON ROLE metadata, so a later role with
// the same name inherits the dropped role's comment.
func TestDropRoleClearsCommentRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP ROLE clears role comment before recreate",
			SetUpScript: []string{
				`CREATE ROLE drop_recreate_comment_role;`,
				`COMMENT ON ROLE drop_recreate_comment_role
					IS 'old private role';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP ROLE drop_recreate_comment_role;`,
				},
				{
					Query: `CREATE ROLE drop_recreate_comment_role;`,
				},
				{
					Query: `SELECT shobj_description(
						(SELECT oid FROM pg_authid
						 WHERE rolname = 'drop_recreate_comment_role'),
						'pg_authid');`,
					Expected: []sql.Row{{nil}},
				},
			},
		},
	})
}
