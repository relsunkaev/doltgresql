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

// TestDropDatabaseWithForceDropsIdleDatabase guards that DROP DATABASE accepts
// the WITH (FORCE) option and removes an idle target database.
func TestDropDatabaseWithForceDropsIdleDatabase(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP DATABASE WITH FORCE drops an idle database",
			SetUpScript: []string{
				`CREATE DATABASE force_drop_database_idle;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP DATABASE force_drop_database_idle WITH (FORCE);`,
				},
				{
					Query: `SELECT datname
						FROM pg_database
						WHERE datname = 'force_drop_database_idle';`,
					Expected: []sql.Row{},
				},
			},
		},
	})
}

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
