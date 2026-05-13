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

import "testing"

// TestDropSchemaClearsPrivilegesRepro reproduces an ACL persistence bug:
// dropping a schema does not clear privileges granted on it, so a later schema
// with the same name inherits CREATE access granted to the dropped schema.
func TestDropSchemaClearsPrivilegesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP SCHEMA clears CREATE privilege before recreate",
			SetUpScript: []string{
				`CREATE USER drop_recreate_schema_user PASSWORD 'schema';`,
				`CREATE SCHEMA drop_recreate_schema_acl;`,
				`REVOKE ALL ON SCHEMA drop_recreate_schema_acl FROM PUBLIC;`,
				`GRANT CREATE, USAGE ON SCHEMA drop_recreate_schema_acl
					TO drop_recreate_schema_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `CREATE TABLE drop_recreate_schema_acl.before_drop (pk INT);`,
					Username: `drop_recreate_schema_user`,
					Password: `schema`,
				},
				{
					Query:    `DROP TABLE drop_recreate_schema_acl.before_drop;`,
					Username: `drop_recreate_schema_user`,
					Password: `schema`,
				},
				{
					Query: `DROP SCHEMA drop_recreate_schema_acl;`,
				},
				{
					Query: `CREATE SCHEMA drop_recreate_schema_acl;`,
				},
				{
					Query: `REVOKE ALL ON SCHEMA drop_recreate_schema_acl FROM PUBLIC;`,
				},
				{
					Query:       `CREATE TABLE drop_recreate_schema_acl.after_drop (pk INT);`,
					ExpectedErr: `permission denied`,
					Username:    `drop_recreate_schema_user`,
					Password:    `schema`,
				},
			},
		},
	})
}
