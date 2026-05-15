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

// TestDropDatabaseWithForceIfExistsNoopsMissingDatabase guards that combining
// WITH (FORCE) with IF EXISTS on a non-existent target succeeds silently, the
// same way PostgreSQL does.
func TestDropDatabaseWithForceIfExistsNoopsMissingDatabase(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP DATABASE IF EXISTS WITH FORCE no-ops on missing database",
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP DATABASE IF EXISTS force_drop_database_missing WITH (FORCE);`,
				},
			},
		},
	})
}

// TestDropCurrentDatabaseRejectedRepro reproduces a database DDL persistence
// boundary: PostgreSQL rejects dropping the database currently used by the
// session, leaving it present in pg_database.
func TestDropCurrentDatabaseRejectedRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP DATABASE rejects current database",
			SetUpScript: []string{
				`CREATE DATABASE current_drop_database_repro;`,
				`USE current_drop_database_repro;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DROP DATABASE current_drop_database_repro;`,
					ExpectedErr: `cannot drop the currently open database`,
				},
				{
					Query: `USE postgres;`,
				},
				{
					Query: `SELECT datname
						FROM pg_database
						WHERE datname = 'current_drop_database_repro';`,
					Expected: []sql.Row{{"current_drop_database_repro"}},
				},
			},
		},
	})
}
