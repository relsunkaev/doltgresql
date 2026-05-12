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

// TestAlterDefaultPrivilegesForRoleRequiresOwnershipRepro reproduces a
// privilege-escalation bug: an unprivileged role can alter another role's
// default privileges and grant itself access to objects that role creates later.
func TestAlterDefaultPrivilegesForRoleRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "unprivileged role cannot alter another role's default privileges",
			SetUpScript: []string{
				`CREATE USER default_priv_hijacker PASSWORD 'pw';`,
				`GRANT USAGE ON SCHEMA public TO default_priv_hijacker;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public
						GRANT SELECT ON TABLES TO default_priv_hijacker;`,
					ExpectedErr: `permission denied`,
					Username:    `default_priv_hijacker`,
					Password:    `pw`,
				},
				{
					Query: `CREATE TABLE default_priv_hijack_private (
						id INT PRIMARY KEY,
						secret TEXT
					);`,
				},
				{
					Query: `INSERT INTO default_priv_hijack_private VALUES (1, 'private');`,
				},
				{
					Query:       `SELECT id, secret FROM default_priv_hijack_private;`,
					ExpectedErr: `permission denied`,
					Username:    `default_priv_hijacker`,
					Password:    `pw`,
				},
			},
		},
		{
			Name: "owner can alter its own default privileges",
			SetUpScript: []string{
				`CREATE USER default_priv_owner PASSWORD 'pw';`,
				`CREATE USER default_priv_owner_reader PASSWORD 'reader';`,
				`GRANT USAGE, CREATE ON SCHEMA public TO default_priv_owner;`,
				`GRANT USAGE ON SCHEMA public TO default_priv_owner_reader;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER DEFAULT PRIVILEGES IN SCHEMA public
						GRANT SELECT ON TABLES TO default_priv_owner_reader;`,
					Username: `default_priv_owner`,
					Password: `pw`,
				},
				{
					Query: `CREATE TABLE default_priv_owner_items (
						id INT PRIMARY KEY,
						label TEXT
					);`,
					Username: `default_priv_owner`,
					Password: `pw`,
				},
				{
					Query:    `INSERT INTO default_priv_owner_items VALUES (1, 'visible');`,
					Username: `default_priv_owner`,
					Password: `pw`,
				},
				{
					Query:    `SELECT id, label FROM default_priv_owner_items;`,
					Expected: []sql.Row{{1, "visible"}},
					Username: `default_priv_owner_reader`,
					Password: `reader`,
				},
			},
		},
	})
}
