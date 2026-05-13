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

// TestDropDatabaseClearsCommentRepro reproduces a shared-metadata persistence
// bug: dropping a database does not clear COMMENT ON DATABASE metadata, so a
// later database with the same name inherits the dropped database's comment.
func TestDropDatabaseClearsCommentRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP DATABASE clears database comment before recreate",
			SetUpScript: []string{
				`CREATE DATABASE drop_recreate_comment_database;`,
				`COMMENT ON DATABASE drop_recreate_comment_database
					IS 'old private database';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP DATABASE drop_recreate_comment_database;`,
				},
				{
					Query: `CREATE DATABASE drop_recreate_comment_database;`,
				},
				{
					Query: `SELECT shobj_description(
						(SELECT oid FROM pg_database
						 WHERE datname = 'drop_recreate_comment_database'),
						'pg_database');`,
					Expected: []sql.Row{{nil}},
				},
			},
		},
	})
}
