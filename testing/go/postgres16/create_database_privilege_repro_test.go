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

package postgres16

import (
	. "github.com/dolthub/doltgresql/testing/go"

	"testing"
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
					Query: `CREATE DATABASE unauthorized_db;`,

					Username: `db_creator`,
					Password: `creator`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-database-privilege-repro-test-testcreatedatabaserequirescreatedbprivilegerepro-0001-create-database-unauthorized_db", Compare: "sqlstate"},

					// TestDropDatabaseRequiresOwnershipRepro reproduces a security bug: a normal
					// login role can drop a database owned by another role.

				},
			},
		},
	})
}

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
					Query: `DROP DATABASE protected_db;`,

					Username: `db_dropper`,
					Password: `dropper`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-database-privilege-repro-test-testdropdatabaserequiresownershiprepro-0001-drop-database-protected_db", Compare: "sqlstate"},

					// TestDropDatabaseRequiresOwnershipDespiteAllPrivilegesRepro reproduces a
					// PostgreSQL authorization bug: GRANT ALL PRIVILEGES ON DATABASE does not
					// transfer ownership and should not allow the grantee to DROP the database.

				},
			},
		},
	})
}

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
					Query: `DROP DATABASE protected_all_db;`,

					Username: `drop_database_intruder`,
					Password: `dropper`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-database-privilege-repro-test-testdropdatabaserequiresownershipdespiteallprivilegesrepro-0001-drop-database-protected_all_db", Compare: "sqlstate"},

					// TestAlterDatabaseOwnerToRequiresOwnershipRepro reproduces a PostgreSQL
					// privilege incompatibility: a normal login role can run ALTER DATABASE OWNER
					// TO against a database owned by another role.

				},
				{
					Query: `SELECT datname FROM pg_database WHERE datname = 'protected_all_db';`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-database-privilege-repro-test-testdropdatabaserequiresownershipdespiteallprivilegesrepro-0002-select-datname-from-pg_database-where"},
				},
			},
		},
	})
}

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
					Query: `ALTER DATABASE owner_to_database_private OWNER TO db_owner_hijacker;`,

					Username: `db_owner_hijacker`,
					Password: `hijacker`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-database-privilege-repro-test-testalterdatabaseownertorequiresownershiprepro-0001-alter-database-owner_to_database_private-owner-to", Compare: "sqlstate"},

					// TestAlterDatabaseRenameToRequiresOwnershipRepro reproduces a security bug:
					// Doltgres allows a role that does not own a database to rename it.

				},
			},
		},
	})
}

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

					Username: `db_renamer`,
					Password: `renamer`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-database-privilege-repro-test-testalterdatabaserenametorequiresownershiprepro-0001-alter-database-rename_to_database_private-rename-to", Compare: "sqlstate"},
				},
				{
					Query: `SELECT datname
						FROM pg_catalog.pg_database
						WHERE datname IN ('rename_to_database_private', 'renamed_by_non_owner')
						ORDER BY datname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-database-privilege-repro-test-testalterdatabaserenametorequiresownershiprepro-0002-select-datname-from-pg_catalog.pg_database-where"},
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
					Query: `ALTER DATABASE setting_database_private SET work_mem = '64kB';`,

					Username: `db_setting_intruder`,
					Password: `setter`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-database-privilege-repro-test-testalterdatabasesetrequiresownershiprepro-0001-alter-database-setting_database_private-set-work_mem", Compare: "sqlstate"},
				},
				{
					Query: `SELECT COUNT(*)
						FROM pg_catalog.pg_db_role_setting
						WHERE setdatabase = 'setting_database_private'::regdatabase;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-database-privilege-repro-test-testalterdatabasesetrequiresownershiprepro-0002-select-count-*-from-pg_catalog.pg_db_role_setting", Compare: "sqlstate"},

					// TestAlterDatabaseCatalogOptionsRequireOwnershipRepro reproduces a security
					// bug: Doltgres allows a role that does not own a database to change persisted
					// pg_database catalog options.

				},
			},
		},
	})
}

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

					Username: `db_metadata_intruder`,
					Password: `metadata`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-database-privilege-repro-test-testalterdatabasecatalogoptionsrequireownershiprepro-0001-alter-database-metadata_allow_database_private-with-allow_connections", Compare: "sqlstate"},
				},
				{
					Query: `ALTER DATABASE metadata_connection_database_private
						WITH CONNECTION LIMIT 0;`,

					Username: `db_metadata_intruder`,
					Password: `metadata`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-database-privilege-repro-test-testalterdatabasecatalogoptionsrequireownershiprepro-0002-alter-database-metadata_connection_database_private-with-connection", Compare: "sqlstate"},
				},
				{
					Query: `ALTER DATABASE metadata_template_database_private
						WITH IS_TEMPLATE true;`,

					Username: `db_metadata_intruder`,
					Password: `metadata`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-database-privilege-repro-test-testalterdatabasecatalogoptionsrequireownershiprepro-0003-alter-database-metadata_template_database_private-with-is_template", Compare: "sqlstate"},
				},
				{
					Query: `SELECT datname, datallowconn, datconnlimit, datistemplate
						FROM pg_catalog.pg_database
						WHERE datname IN (
							'metadata_allow_database_private',
							'metadata_connection_database_private',
							'metadata_template_database_private'
						)
						ORDER BY datname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-database-privilege-repro-test-testalterdatabasecatalogoptionsrequireownershiprepro-0004-select-datname-datallowconn-datconnlimit-datistemplate"},
				},
			},
		},
	})
}

// TestAlterDatabaseResetRequiresOwnershipRepro reproduces a security bug:
// Doltgres allows a role that does not own a database to delete persisted
// database-level settings.
func TestAlterDatabaseResetRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER DATABASE RESET requires database ownership",
			SetUpScript: []string{
				`CREATE USER db_reset_intruder PASSWORD 'resetter';`,
				`CREATE DATABASE reset_setting_database_private;`,
				`CREATE DATABASE reset_all_database_private;`,
				`ALTER DATABASE reset_setting_database_private SET work_mem = '64kB';`,
				`ALTER DATABASE reset_all_database_private SET work_mem = '64kB';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER DATABASE reset_setting_database_private RESET work_mem;`,

					Username: `db_reset_intruder`,
					Password: `resetter`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-database-privilege-repro-test-testalterdatabaseresetrequiresownershiprepro-0001-alter-database-reset_setting_database_private-reset-work_mem", Compare: "sqlstate"},
				},
				{
					Query: `ALTER DATABASE reset_all_database_private RESET ALL;`,

					Username: `db_reset_intruder`,
					Password: `resetter`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-database-privilege-repro-test-testalterdatabaseresetrequiresownershiprepro-0002-alter-database-reset_all_database_private-reset-all", Compare: "sqlstate"},
				},
				{
					Query: `SELECT setdatabase::regdatabase::text, array_to_string(setconfig, ',')
						FROM pg_catalog.pg_db_role_setting
						WHERE setdatabase IN (
							'reset_setting_database_private'::regdatabase,
							'reset_all_database_private'::regdatabase
						)
						ORDER BY setdatabase::regdatabase::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-database-privilege-repro-test-testalterdatabaseresetrequiresownershiprepro-0003-select-setdatabase::regdatabase::text-array_to_string-setconfig-from", Compare: "sqlstate"},
				},
			},
		},
	})
}
