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

// TestDropExtensionClearsCommentRepro reproduces a metadata persistence bug:
// dropping an extension does not clear COMMENT ON EXTENSION metadata, so a
// later extension with the same name inherits the dropped extension's comment.
func TestDropExtensionClearsCommentRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP EXTENSION clears extension comment before recreate",
			SetUpScript: []string{
				`CREATE EXTENSION hstore WITH SCHEMA public;`,
				`COMMENT ON EXTENSION hstore IS 'old private extension';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP EXTENSION hstore;`,
				},
				{
					Query: `CREATE EXTENSION hstore WITH SCHEMA public;`,
				},
				{
					Query: `SELECT obj_description(
						(SELECT oid FROM pg_extension WHERE extname = 'hstore'),
						'pg_extension');`,
					Expected: []sql.Row{{nil}},
				},
			},
		},
	})
}
