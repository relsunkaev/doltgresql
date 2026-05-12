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

func TestRollbackRevertsAlterDefaultPrivileges(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK reverts default table privileges",
			SetUpScript: []string{
				`CREATE USER rollback_default_reader PASSWORD 'reader';`,
				`GRANT USAGE ON SCHEMA public TO rollback_default_reader;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `ALTER DEFAULT PRIVILEGES IN SCHEMA public
						GRANT SELECT ON TABLES TO rollback_default_reader;`,
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_default_acl
						WHERE defaclnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')
							AND defaclobjtype = 'r';`,
					Expected: []sql.Row{{int64(0)}},
				},
				{
					Query: `CREATE TABLE rollback_default_private (
						id INT PRIMARY KEY,
						secret TEXT
					);`,
				},
				{
					Query: `INSERT INTO rollback_default_private VALUES (1, 'hidden');`,
				},
				{
					Query:       `SELECT id, secret FROM rollback_default_private;`,
					ExpectedErr: `permission denied`,
					Username:    `rollback_default_reader`,
					Password:    `reader`,
				},
			},
		},
	})
}
