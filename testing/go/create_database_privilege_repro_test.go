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

// TestDropDatabaseRequiresOwnershipDespiteAllPrivilegesRepro reproduces a
// PostgreSQL authorization bug: GRANT ALL PRIVILEGES ON DATABASE does not
// transfer ownership and should not allow the grantee to DROP the database.
func TestDropDatabaseRequiresOwnershipDespiteAllPrivilegesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP DATABASE requires ownership despite ALL PRIVILEGES",
			SetUpScript: []string{
				`CREATE USER drop_database_intruder PASSWORD 'dropper';`,
				`CREATE DATABASE protected_all_db;`,
				`GRANT ALL PRIVILEGES ON DATABASE protected_all_db TO drop_database_intruder;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DROP DATABASE protected_all_db;`,
					ExpectedErr: `must be owner`,
					Username:    `drop_database_intruder`,
					Password:    `dropper`,
				},
				{
					Query:    `SELECT datname FROM pg_database WHERE datname = 'protected_all_db';`,
					Expected: []sql.Row{{"protected_all_db"}},
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

// TestAlterDatabaseRenameToRequiresOwnershipRepro reproduces a security bug:
// Doltgres allows a role that does not own a database to rename it.
func TestAlterDatabaseRenameToRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER DATABASE RENAME TO requires database ownership",
			SetUpScript: []string{
				`CREATE USER db_renamer PASSWORD 'renamer';`,
				`CREATE DATABASE rename_to_database_private;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER DATABASE rename_to_database_private
						RENAME TO renamed_by_non_owner;`,
					ExpectedErr: `must be owner`,
					Username:    `db_renamer`,
					Password:    `renamer`,
				},
				{
					Query: `SELECT datname
						FROM pg_catalog.pg_database
						WHERE datname IN ('rename_to_database_private', 'renamed_by_non_owner')
						ORDER BY datname;`,
					Expected: []sql.Row{{"rename_to_database_private"}},
				},
			},
		},
	})
}

// TestAlterDatabaseSetRequiresOwnershipRepro reproduces a security bug:
// Doltgres allows a role that does not own a database to change its persisted
// database-level settings.
func TestAlterDatabaseSetRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER DATABASE SET requires database ownership",
			SetUpScript: []string{
				`CREATE USER db_setting_intruder PASSWORD 'setter';`,
				`CREATE DATABASE setting_database_private;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `ALTER DATABASE setting_database_private SET work_mem = '64kB';`,
					ExpectedErr: `must be owner`,
					Username:    `db_setting_intruder`,
					Password:    `setter`,
				},
				{
					Query: `SELECT COUNT(*)
						FROM pg_catalog.pg_db_role_setting
						WHERE setdatabase = 'setting_database_private'::regdatabase;`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestAlterDatabaseCatalogOptionsRequireOwnershipRepro reproduces a security
// bug: Doltgres allows a role that does not own a database to change persisted
// pg_database catalog options.
func TestAlterDatabaseCatalogOptionsRequireOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER DATABASE catalog options require database ownership",
			SetUpScript: []string{
				`CREATE USER db_metadata_intruder PASSWORD 'metadata';`,
				`CREATE DATABASE metadata_allow_database_private;`,
				`CREATE DATABASE metadata_connection_database_private;`,
				`CREATE DATABASE metadata_template_database_private;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER DATABASE metadata_allow_database_private
						WITH ALLOW_CONNECTIONS false;`,
					ExpectedErr: `must be owner`,
					Username:    `db_metadata_intruder`,
					Password:    `metadata`,
				},
				{
					Query: `ALTER DATABASE metadata_connection_database_private
						WITH CONNECTION LIMIT 0;`,
					ExpectedErr: `must be owner`,
					Username:    `db_metadata_intruder`,
					Password:    `metadata`,
				},
				{
					Query: `ALTER DATABASE metadata_template_database_private
						WITH IS_TEMPLATE true;`,
					ExpectedErr: `must be owner`,
					Username:    `db_metadata_intruder`,
					Password:    `metadata`,
				},
				{
					Query: `SELECT datname, datallowconn, datconnlimit, datistemplate
						FROM pg_catalog.pg_database
						WHERE datname IN (
							'metadata_allow_database_private',
							'metadata_connection_database_private',
							'metadata_template_database_private'
						)
						ORDER BY datname;`,
					Expected: []sql.Row{
						{"metadata_allow_database_private", "t", int64(-1), "f"},
						{"metadata_connection_database_private", "t", int64(-1), "f"},
						{"metadata_template_database_private", "t", int64(-1), "f"},
					},
				},
			},
		},
	})
}
