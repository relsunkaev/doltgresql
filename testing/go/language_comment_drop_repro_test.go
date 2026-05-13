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

// TestDropLanguageClearsCommentRepro reproduces a metadata persistence bug:
// dropping a language does not clear COMMENT ON LANGUAGE metadata, so a later
// language with the same name inherits the dropped language's comment.
func TestDropLanguageClearsCommentRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP LANGUAGE clears language comment before recreate",
			SetUpScript: []string{
				`CREATE LANGUAGE drop_recreate_comment_language
					HANDLER plpgsql_call_handler;`,
				`COMMENT ON LANGUAGE drop_recreate_comment_language
					IS 'old private language';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP LANGUAGE drop_recreate_comment_language;`,
				},
				{
					Query: `CREATE LANGUAGE drop_recreate_comment_language
						HANDLER plpgsql_call_handler;`,
				},
				{
					Query: `SELECT obj_description(
						(SELECT oid FROM pg_language
						 WHERE lanname = 'drop_recreate_comment_language'),
						'pg_language');`,
					Expected: []sql.Row{{nil}},
				},
			},
		},
	})
}
