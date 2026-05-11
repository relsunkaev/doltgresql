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

// TestCreateTemporaryTableRequiresDatabaseTemporaryPrivilegeRepro reproduces a
// database privilege bug: PostgreSQL-compatible TEMPORARY database privileges
// are required before a role can create temporary tables.
func TestCreateTemporaryTableRequiresDatabaseTemporaryPrivilegeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TEMPORARY TABLE requires database TEMPORARY privilege",
			SetUpScript: []string{
				`CREATE USER temp_table_user PASSWORD 'temp';`,
				`REVOKE TEMPORARY ON DATABASE postgres FROM PUBLIC;`,
				`GRANT USAGE, CREATE ON SCHEMA public TO temp_table_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `CREATE TEMPORARY TABLE temp_privilege_denied (id INT);`,
					ExpectedErr: `permission denied`,
					Username:    `temp_table_user`,
					Password:    `temp`,
				},
			},
		},
	})
}

// TestCreateTemporaryTableAsRequiresDatabaseTemporaryPrivilegeRepro reproduces
// the same database privilege bug through the CTAS temporary-table path.
func TestCreateTemporaryTableAsRequiresDatabaseTemporaryPrivilegeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TEMPORARY TABLE AS requires database TEMPORARY privilege",
			SetUpScript: []string{
				`CREATE USER temp_table_as_user PASSWORD 'temp';`,
				`REVOKE TEMPORARY ON DATABASE postgres FROM PUBLIC;`,
				`GRANT USAGE, CREATE ON SCHEMA public TO temp_table_as_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `CREATE TEMPORARY TABLE temp_ctas_privilege_denied AS SELECT 1 AS id;`,
					ExpectedErr: `permission denied`,
					Username:    `temp_table_as_user`,
					Password:    `temp`,
				},
			},
		},
	})
}
