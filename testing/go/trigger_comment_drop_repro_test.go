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

// TestDropTriggerClearsCommentRepro reproduces a metadata persistence bug:
// dropping a trigger does not clear COMMENT ON TRIGGER metadata, so a later
// trigger with the same table/name inherits the dropped trigger's comment.
func TestDropTriggerClearsCommentRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP TRIGGER clears trigger comment before recreate",
			SetUpScript: []string{
				`CREATE TABLE drop_recreate_comment_trigger_table (
					id INT PRIMARY KEY,
					v INT
				);`,
				`CREATE FUNCTION drop_recreate_comment_trigger_func()
					RETURNS TRIGGER AS $$
					BEGIN
						RETURN NEW;
					END;
					$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER drop_recreate_comment_trigger
					BEFORE INSERT ON drop_recreate_comment_trigger_table
					FOR EACH ROW EXECUTE FUNCTION drop_recreate_comment_trigger_func();`,
				`COMMENT ON TRIGGER drop_recreate_comment_trigger
					ON drop_recreate_comment_trigger_table
					IS 'old private trigger';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP TRIGGER drop_recreate_comment_trigger
						ON drop_recreate_comment_trigger_table;`,
				},
				{
					Query: `CREATE TRIGGER drop_recreate_comment_trigger
						BEFORE INSERT ON drop_recreate_comment_trigger_table
						FOR EACH ROW EXECUTE FUNCTION drop_recreate_comment_trigger_func();`,
				},
				{
					Query: `SELECT obj_description(
						(SELECT oid FROM pg_trigger
						 WHERE tgname = 'drop_recreate_comment_trigger'
						   AND tgrelid = 'drop_recreate_comment_trigger_table'::regclass),
						'pg_trigger');`,
					Expected: []sql.Row{{nil}},
				},
			},
		},
	})
}
