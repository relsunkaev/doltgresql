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

// TestRollbackRevertsAlterDatabaseSetRepro reproduces a transaction
// consistency bug: ALTER DATABASE ... SET writes pg_db_role_setting outside
// the surrounding transaction and survives ROLLBACK.
func TestRollbackRevertsAlterDatabaseSetRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK reverts ALTER DATABASE SET",
			SetUpScript: []string{
				`CREATE DATABASE rollback_database_setting_catalog;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `ALTER DATABASE rollback_database_setting_catalog SET work_mem = '64kB';`,
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `SELECT COUNT(*)
						FROM pg_catalog.pg_db_role_setting
						WHERE setdatabase = 'rollback_database_setting_catalog'::regdatabase;`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}
