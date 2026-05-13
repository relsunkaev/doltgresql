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

// TestDropSchemaClearsDefaultPrivilegesRepro reproduces an ACL persistence bug:
// schema-scoped ALTER DEFAULT PRIVILEGES entries survive DROP SCHEMA, so tables
// in a later schema with the same name inherit grants from the dropped schema.
func TestDropSchemaClearsDefaultPrivilegesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP SCHEMA clears schema-scoped default table privileges",
			SetUpScript: []string{
				`CREATE USER drop_schema_default_reader PASSWORD 'default';`,
				`CREATE SCHEMA drop_schema_default_acl;`,
				`GRANT USAGE ON SCHEMA drop_schema_default_acl TO drop_schema_default_reader;`,
				`ALTER DEFAULT PRIVILEGES IN SCHEMA drop_schema_default_acl
					GRANT SELECT ON TABLES TO drop_schema_default_reader;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE drop_schema_default_acl.before_drop (
						id INT PRIMARY KEY,
						label TEXT
					);`,
				},
				{
					Query: `INSERT INTO drop_schema_default_acl.before_drop VALUES (1, 'old visible');`,
				},
				{
					Query:    `SELECT label FROM drop_schema_default_acl.before_drop;`,
					Expected: []sql.Row{{"old visible"}},
					Username: `drop_schema_default_reader`,
					Password: `default`,
				},
				{
					Query: `DROP TABLE drop_schema_default_acl.before_drop;`,
				},
				{
					Query: `DROP SCHEMA drop_schema_default_acl;`,
				},
				{
					Query: `CREATE SCHEMA drop_schema_default_acl;`,
				},
				{
					Query: `GRANT USAGE ON SCHEMA drop_schema_default_acl TO drop_schema_default_reader;`,
				},
				{
					Query: `CREATE TABLE drop_schema_default_acl.after_drop (
						id INT PRIMARY KEY,
						label TEXT
					);`,
				},
				{
					Query: `INSERT INTO drop_schema_default_acl.after_drop VALUES (1, 'new sensitive');`,
				},
				{
					Query:       `SELECT label FROM drop_schema_default_acl.after_drop;`,
					ExpectedErr: `permission denied`,
					Username:    `drop_schema_default_reader`,
					Password:    `default`,
				},
			},
		},
	})
}
