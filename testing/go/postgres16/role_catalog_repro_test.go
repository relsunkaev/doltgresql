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

// TestPgDatabaseOwnerRoleExistsRepro reproduces a PostgreSQL 15 catalog and
// security-default bug: pg_database_owner is a predefined role that owns the
// public schema in dumps and models database-owner privileges.
func TestPgDatabaseOwnerRoleExistsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_database_owner predefined role exists",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT rolname, rolcanlogin, rolsuper
						FROM pg_catalog.pg_roles
						WHERE rolname = 'pg_database_owner';`, PostgresOracle: ScriptTestPostgresOracle{ID: "role-catalog-repro-test-testpgdatabaseownerroleexistsrepro-0001-select-rolname-rolcanlogin-rolsuper-from"},
				},
			},
		},
	})
}

// TestPublicSchemaOwnedByPgDatabaseOwnerRepro reproduces a PostgreSQL 15
// security-default bug: the public schema should be owned by the predefined
// pg_database_owner role, not by the bootstrap superuser.
func TestPublicSchemaOwnedByPgDatabaseOwnerRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "public schema is owned by pg_database_owner",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT pg_get_userbyid(nspowner)
						FROM pg_catalog.pg_namespace
						WHERE nspname = 'public';`, PostgresOracle: ScriptTestPostgresOracle{ID: "role-catalog-repro-test-testpublicschemaownedbypgdatabaseownerrepro-0001-select-pg_get_userbyid-nspowner-from-pg_catalog.pg_namespace"},
				},
			},
		},
	})
}

// TestServerFilePredefinedRolesExistRepro reproduces a security-default bug:
// PostgreSQL exposes predefined roles for controlled server-file and
// server-program access.
func TestServerFilePredefinedRolesExistRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "server-file predefined roles exist",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT rolname, rolcanlogin, rolsuper
						FROM pg_catalog.pg_roles
						WHERE rolname IN (
							'pg_read_server_files',
							'pg_write_server_files',
							'pg_execute_server_program'
						)
						ORDER BY rolname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "role-catalog-repro-test-testserverfilepredefinedrolesexistrepro-0001-select-rolname-rolcanlogin-rolsuper-from"},
				},
			},
		},
	})
}

// TestDataAndMonitoringPredefinedRolesExistRepro reproduces a catalog and
// privilege-model correctness bug: PostgreSQL exposes predefined roles for
// cluster-wide data access and monitoring permissions.
func TestDataAndMonitoringPredefinedRolesExistRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "data and monitoring predefined roles exist",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT rolname, rolcanlogin, rolsuper
						FROM pg_catalog.pg_roles
						WHERE rolname IN (
							'pg_read_all_data',
							'pg_write_all_data',
							'pg_monitor',
							'pg_read_all_settings',
							'pg_read_all_stats',
							'pg_stat_scan_tables',
							'pg_signal_backend'
						)
						ORDER BY rolname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "role-catalog-repro-test-testdataandmonitoringpredefinedrolesexistrepro-0001-select-rolname-rolcanlogin-rolsuper-from"},
				},
			},
		},
	})
}

// TestPgMonitorInheritsMonitoringRolesRepro reproduces a predefined-role
// privilege bug: pg_monitor should inherit PostgreSQL's read-all-settings,
// read-all-stats, and stat-scan-table memberships.
func TestPgMonitorInheritsMonitoringRolesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_monitor inherits monitoring memberships",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT pg_get_userbyid(roleid), pg_get_userbyid(member)
						FROM pg_catalog.pg_auth_members
						WHERE pg_get_userbyid(member) = 'pg_monitor'
						ORDER BY pg_get_userbyid(roleid);`, PostgresOracle: ScriptTestPostgresOracle{ID: "role-catalog-repro-test-testpgmonitorinheritsmonitoringrolesrepro-0001-select-pg_get_userbyid-roleid-pg_get_userbyid-member"},
				},
			},
		},
	})
}

// TestPgReadAllDataRoleAllowsTableReadsRepro reproduces a predefined-role
// privilege bug: membership in pg_read_all_data should allow reading tables
// without per-table SELECT grants.
func TestPgReadAllDataRoleAllowsTableReadsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_read_all_data allows table reads",
			SetUpScript: []string{
				`CREATE USER read_all_data_user PASSWORD 'pw';`,
				`CREATE TABLE read_all_data_private (
					id INT PRIMARY KEY,
					secret TEXT
				);`,
				`INSERT INTO read_all_data_private VALUES (1, 'visible through predefined role');`,
				`GRANT USAGE ON SCHEMA public TO read_all_data_user;`,
				`GRANT pg_read_all_data TO read_all_data_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, secret
						FROM read_all_data_private;`,

					Username: `read_all_data_user`,
					Password: `pw`, PostgresOracle: ScriptTestPostgresOracle{

						// TestPgWriteAllDataRoleAllowsTableWritesRepro reproduces a predefined-role
						// privilege bug: membership in pg_write_all_data should allow writing tables
						// without per-table INSERT grants.
						ID: "role-catalog-repro-test-testpgreadalldataroleallowstablereadsrepro-0001-select-id-secret-from-read_all_data_private"},
				},
			},
		},
	})
}

func TestPgWriteAllDataRoleAllowsTableWritesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_write_all_data allows table writes",
			SetUpScript: []string{
				`CREATE USER write_all_data_user PASSWORD 'pw';`,
				`CREATE TABLE write_all_data_private (
					id INT PRIMARY KEY,
					secret TEXT
				);`,
				`GRANT USAGE ON SCHEMA public TO write_all_data_user;`,
				`GRANT pg_write_all_data TO write_all_data_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO write_all_data_private
						VALUES (1, 'written through predefined role');`,
					Username: `write_all_data_user`,
					Password: `pw`,
				},
			},
		},
	})
}

// TestMaintenancePredefinedRolesExistRepro reproduces a privilege-model
// correctness bug: PostgreSQL exposes predefined maintenance roles that can be
// granted instead of broad superuser access.
func TestMaintenancePredefinedRolesExistRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "maintenance predefined roles exist",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT rolname, rolcanlogin, rolsuper
						FROM pg_catalog.pg_roles
						WHERE rolname IN (
							'pg_maintain',
							'pg_signal_autovacuum_worker'
						)
						ORDER BY rolname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "role-catalog-repro-test-testmaintenancepredefinedrolesexistrepro-0001-select-rolname-rolcanlogin-rolsuper-from"},
				},
			},
		},
	})
}

// TestCheckpointPredefinedRoleExistsRepro reproduces a predefined-role catalog
// gap: PostgreSQL exposes pg_checkpoint for delegating CHECKPOINT execution
// without broad superuser access.
func TestCheckpointPredefinedRoleExistsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "checkpoint predefined role exists",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT rolname, rolcanlogin, rolsuper
						FROM pg_catalog.pg_roles
						WHERE rolname = 'pg_checkpoint';`, PostgresOracle: ScriptTestPostgresOracle{ID: "role-catalog-repro-test-testcheckpointpredefinedroleexistsrepro-0001-select-rolname-rolcanlogin-rolsuper-from"},
				},
			},
		},
	})
}

// TestCreateSubscriptionPredefinedRoleExistsRepro reproduces a predefined-role
// catalog gap: PostgreSQL exposes pg_create_subscription for delegating
// subscription creation without broad superuser access.
func TestCreateSubscriptionPredefinedRoleExistsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "subscription predefined role exists",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT rolname, rolcanlogin, rolsuper
						FROM pg_catalog.pg_roles
						WHERE rolname = 'pg_create_subscription';`, PostgresOracle: ScriptTestPostgresOracle{ID: "role-catalog-repro-test-testcreatesubscriptionpredefinedroleexistsrepro-0001-select-rolname-rolcanlogin-rolsuper-from"},
				},
			},
		},
	})
}

// TestUseReservedConnectionsPredefinedRoleExistsRepro reproduces a predefined
// role catalog gap: PostgreSQL exposes pg_use_reserved_connections for
// delegating use of reserved connection slots without superuser access.
func TestUseReservedConnectionsPredefinedRoleExistsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "reserved-connections predefined role exists",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT rolname, rolcanlogin, rolsuper
						FROM pg_catalog.pg_roles
						WHERE rolname = 'pg_use_reserved_connections';`, PostgresOracle: ScriptTestPostgresOracle{ID: "role-catalog-repro-test-testusereservedconnectionspredefinedroleexistsrepro-0001-select-rolname-rolcanlogin-rolsuper-from"},
				},
			},
		},
	})
}

// TestCreateUserPopulatesPgShadowRepro reproduces a catalog correctness bug:
// pg_shadow should expose login roles, but Doltgres returns an empty stub.
func TestCreateUserPopulatesPgShadowRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE USER populates pg_shadow",
			SetUpScript: []string{
				`CREATE USER shadow_catalog_user PASSWORD 'pw';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT usename, usesuper, usecreatedb
						FROM pg_catalog.pg_shadow
						WHERE usename = 'shadow_catalog_user';`, PostgresOracle: ScriptTestPostgresOracle{ID: "role-catalog-repro-test-testcreateuserpopulatespgshadowrepro-0001-select-usename-usesuper-usecreatedb-from"},
				},
			},
		},
	})
}

func TestCreateRoleRejectsPgPrefixRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE ROLE rejects reserved pg prefix",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE ROLE pg_reserved_role;`, PostgresOracle: ScriptTestPostgresOracle{
						ID: "role-catalog-repro-test-testcreaterolerejectspgprefixrepro-0001-create-role-pg_reserved_role", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "CREATE USER rejects reserved pg prefix",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE USER pg_reserved_user;`, PostgresOracle: ScriptTestPostgresOracle{
						ID: "role-catalog-repro-test-testcreaterolerejectspgprefixrepro-0002-create-user-pg_reserved_user", Compare: "sqlstate"},
				},
			},
		},
	})
}

// TestCreateUserPopulatesPgAuthidPasswordRepro reproduces a catalog
// persistence bug: pg_authid should expose the stored password hash to
// superusers, but Doltgres leaves rolpassword null.
func TestCreateUserPopulatesPgAuthidPasswordRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE USER populates pg_authid rolpassword",
			SetUpScript: []string{
				`CREATE USER authid_password_user PASSWORD 'pw';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT rolname, rolpassword IS NOT NULL
						FROM pg_catalog.pg_authid
						WHERE rolname = 'authid_password_user';`, PostgresOracle: ScriptTestPostgresOracle{ID: "role-catalog-repro-test-testcreateuserpopulatespgauthidpasswordrepro-0001-select-rolname-rolpassword-is-not"},
				},
			},
		},
	})
}

// TestCreateUserPopulatesPgUserPasswordMaskRepro reproduces a catalog
// correctness bug: pg_user should expose a masked password marker for roles
// with passwords, but Doltgres leaves passwd null.
func TestCreateUserPopulatesPgUserPasswordMaskRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE USER populates pg_user password mask",
			SetUpScript: []string{
				`CREATE USER pguser_password_user PASSWORD 'pw';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT usename, passwd
						FROM pg_catalog.pg_user
						WHERE usename = 'pguser_password_user';`, PostgresOracle: ScriptTestPostgresOracle{ID: "role-catalog-repro-test-testcreateuserpopulatespguserpasswordmaskrepro-0001-select-usename-passwd-from-pg_catalog.pg_user"},
				},
			},
		},
	})
}

// TestQuotedRoleNamesAreCaseSensitive guards role identity: quoted role names
// preserve case, so "CaseRole" and caserole are distinct PostgreSQL principals.
func TestQuotedRoleNamesAreCaseSensitive(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "quoted role names remain distinct from folded unquoted names",
			SetUpScript: []string{
				`CREATE ROLE "CaseRole" LOGIN PASSWORD 'quoted';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE ROLE caserole LOGIN PASSWORD 'folded';`, PostgresOracle: ScriptTestPostgresOracle{ID: "role-catalog-repro-test-testquotedrolenamesarecasesensitive-0001-create-role-caserole-login-password"},
				},
				{
					Query: `SELECT rolname
						FROM pg_catalog.pg_roles
						WHERE rolname IN ('CaseRole', 'caserole')
						ORDER BY rolname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "role-catalog-repro-test-testquotedrolenamesarecasesensitive-0002-select-rolname-from-pg_catalog.pg_roles-where"},
				},
			},
		},
	})
}

// TestPgAuthidRequiresCatalogPrivilegeGuard guards that ordinary users cannot
// read pg_authid directly.
func TestPgAuthidRequiresCatalogPrivilegeGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_authid requires catalog privilege",
			SetUpScript: []string{
				`CREATE USER authid_reader PASSWORD 'pw';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT rolname FROM pg_catalog.pg_authid LIMIT 1;`,

					Username: `authid_reader`,
					Password: `pw`, PostgresOracle: ScriptTestPostgresOracle{ID: "role-catalog-repro-test-testpgauthidrequirescatalogprivilegeguard-0001-select-rolname-from-pg_catalog.pg_authid-limit", Compare: "sqlstate"},
				},
			},
		},
	})
}
