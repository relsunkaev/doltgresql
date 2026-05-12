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

func TestCommitKeepsAlterDatabaseSet(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "COMMIT keeps ALTER DATABASE SET",
			SetUpScript: []string{
				`CREATE DATABASE commit_database_setting_catalog;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `ALTER DATABASE commit_database_setting_catalog SET work_mem = '64kB';`,
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query: `SELECT array_to_string(setconfig, ',')
						FROM pg_catalog.pg_db_role_setting
						WHERE setdatabase = 'commit_database_setting_catalog'::regdatabase;`,
					Expected: []sql.Row{{"work_mem=64kB"}},
				},
			},
		},
	})
}

func TestRollbackRestoresPreviousAlterDatabaseSet(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK restores previous ALTER DATABASE SET",
			SetUpScript: []string{
				`CREATE DATABASE rollback_restore_database_setting;`,
				`ALTER DATABASE rollback_restore_database_setting SET work_mem = '64kB';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `ALTER DATABASE rollback_restore_database_setting SET work_mem = '128kB';`,
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `SELECT array_to_string(setconfig, ',')
						FROM pg_catalog.pg_db_role_setting
						WHERE setdatabase = 'rollback_restore_database_setting'::regdatabase;`,
					Expected: []sql.Row{{"work_mem=64kB"}},
				},
			},
		},
	})
}

// TestRollbackRevertsAlterDatabaseCatalogOptionsRepro reproduces a transaction
// consistency bug: ALTER DATABASE ... WITH writes pg_database metadata outside
// the surrounding transaction and survives ROLLBACK.
func TestRollbackRevertsAlterDatabaseCatalogOptionsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK reverts ALTER DATABASE catalog options",
			SetUpScript: []string{
				`CREATE DATABASE rollback_database_options_catalog;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `ALTER DATABASE rollback_database_options_catalog
						WITH CONNECTION LIMIT 0;`,
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `SELECT datconnlimit
						FROM pg_catalog.pg_database
						WHERE datname = 'rollback_database_options_catalog';`,
					Expected: []sql.Row{{int64(-1)}},
				},
			},
		},
	})
}

// TestRollbackRevertsAlterRoleSetRepro reproduces a transaction consistency
// bug: ALTER ROLE ... SET writes pg_db_role_setting outside the surrounding
// transaction and survives ROLLBACK.
func TestRollbackRevertsAlterRoleSetRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK reverts ALTER ROLE SET",
			SetUpScript: []string{
				`CREATE ROLE rollback_role_setting_catalog;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `ALTER ROLE rollback_role_setting_catalog SET work_mem = '64kB';`,
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `SELECT COUNT(*)
						FROM pg_catalog.pg_db_role_setting
						WHERE setrole = 'rollback_role_setting_catalog'::regrole;`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestRollbackRevertsAlterRoleOptionsRepro reproduces a transaction
// consistency bug: ALTER ROLE option changes write pg_roles metadata outside
// the surrounding transaction and survive ROLLBACK.
func TestRollbackRevertsAlterRoleOptionsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK reverts ALTER ROLE options",
			SetUpScript: []string{
				`CREATE ROLE rollback_role_options_catalog;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `ALTER ROLE rollback_role_options_catalog CONNECTION LIMIT 0;`,
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `SELECT rolconnlimit
						FROM pg_catalog.pg_roles
						WHERE rolname = 'rollback_role_options_catalog';`,
					Expected: []sql.Row{{int32(-1)}},
				},
			},
		},
	})
}

// TestRollbackDropsCreatedRoleRepro reproduces a transaction consistency bug:
// CREATE ROLE writes auth metadata outside the surrounding transaction and
// survives ROLLBACK.
func TestRollbackDropsCreatedRoleRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK drops created role",
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `CREATE ROLE rollback_created_role;`,
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `SELECT COUNT(*)
						FROM pg_catalog.pg_roles
						WHERE rolname = 'rollback_created_role';`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestRollbackRestoresDroppedRoleRepro reproduces a transaction consistency bug:
// DROP ROLE deletes auth metadata outside the surrounding transaction and
// survives ROLLBACK.
func TestRollbackRestoresDroppedRoleRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK restores dropped role",
			SetUpScript: []string{
				`CREATE ROLE rollback_dropped_role;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `DROP ROLE rollback_dropped_role;`,
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `SELECT COUNT(*)
						FROM pg_catalog.pg_roles
						WHERE rolname = 'rollback_dropped_role';`,
					Expected: []sql.Row{{1}},
				},
			},
		},
	})
}
