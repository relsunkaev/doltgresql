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

// TestDropTableClearsTablePrivilegesRepro reproduces an ACL persistence bug:
// dropping a table does not clear its privileges, so a later table with the
// same name inherits access granted to the dropped table.
func TestDropTableClearsTablePrivilegesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP TABLE clears table privileges before recreate",
			SetUpScript: []string{
				`CREATE USER drop_recreate_acl_reader PASSWORD 'reader';`,
				`CREATE TABLE drop_recreate_acl_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO drop_recreate_acl_items VALUES (1, 'old visible');`,
				`GRANT USAGE ON SCHEMA public TO drop_recreate_acl_reader;`,
				`GRANT SELECT ON drop_recreate_acl_items TO drop_recreate_acl_reader;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, label
						FROM drop_recreate_acl_items;`,
					Expected: []sql.Row{{1, "old visible"}},
					Username: `drop_recreate_acl_reader`,
					Password: `reader`,
				},
				{
					Query: `DROP TABLE drop_recreate_acl_items;`,
				},
				{
					Query: `CREATE TABLE drop_recreate_acl_items (
						id INT PRIMARY KEY,
						label TEXT
					);`,
				},
				{
					Query: `INSERT INTO drop_recreate_acl_items VALUES (1, 'new sensitive');`,
				},
				{
					Query:       `SELECT id, label FROM drop_recreate_acl_items;`,
					ExpectedErr: `permission denied`,
					Username:    `drop_recreate_acl_reader`,
					Password:    `reader`,
				},
			},
		},
	})
}
