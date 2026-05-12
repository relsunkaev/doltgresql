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

// TestCreateDatabaseRequiresCreatedbPrivilegeRepro reproduces a security bug:
// a normal login role without CREATEDB can create a database.
func TestCreateDatabaseRequiresCreatedbPrivilegeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE DATABASE requires CREATEDB privilege",
			SetUpScript: []string{
				`CREATE USER db_creator PASSWORD 'creator';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `CREATE DATABASE unauthorized_db;`,
					ExpectedErr: `permission denied`,
					Username:    `db_creator`,
					Password:    `creator`,
				},
			},
		},
	})
}

// TestDropDatabaseRequiresOwnershipRepro reproduces a security bug: a normal
// login role can drop a database owned by another role.
func TestDropDatabaseRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP DATABASE requires database ownership",
			SetUpScript: []string{
				`CREATE USER db_dropper PASSWORD 'dropper';`,
				`CREATE DATABASE protected_db;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DROP DATABASE protected_db;`,
					ExpectedErr: `permission denied`,
					Username:    `db_dropper`,
					Password:    `dropper`,
				},
			},
		},
	})
}

// TestAlterDatabaseOwnerToRequiresOwnershipRepro reproduces a PostgreSQL
// privilege incompatibility: a normal login role can run ALTER DATABASE OWNER
// TO against a database owned by another role.
func TestAlterDatabaseOwnerToRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER DATABASE OWNER TO requires database ownership",
			SetUpScript: []string{
				`CREATE USER db_owner_hijacker PASSWORD 'hijacker';`,
				`CREATE DATABASE owner_to_database_private;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `ALTER DATABASE owner_to_database_private OWNER TO db_owner_hijacker;`,
					ExpectedErr: `must be owner`,
					Username:    `db_owner_hijacker`,
					Password:    `hijacker`,
				},
			},
		},
	})
}
