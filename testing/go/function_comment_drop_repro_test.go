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

// TestDropFunctionClearsCommentRepro reproduces a metadata persistence bug:
// dropping a function does not clear COMMENT ON FUNCTION metadata, so a later
// function with the same signature inherits the dropped function's comment.
func TestDropFunctionClearsCommentRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP FUNCTION clears function comment before recreate",
			SetUpScript: []string{
				`CREATE FUNCTION drop_recreate_comment_func() RETURNS INT
					LANGUAGE SQL AS $$ SELECT 1 $$;`,
				`COMMENT ON FUNCTION drop_recreate_comment_func()
					IS 'old private function';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP FUNCTION drop_recreate_comment_func();`,
				},
				{
					Query: `CREATE FUNCTION drop_recreate_comment_func() RETURNS INT
						LANGUAGE SQL AS $$ SELECT 2 $$;`,
				},
				{
					Query: `SELECT obj_description(
						(SELECT oid FROM pg_proc WHERE proname = 'drop_recreate_comment_func'),
						'pg_proc');`,
					Expected: []sql.Row{{nil}},
				},
			},
		},
	})
}
