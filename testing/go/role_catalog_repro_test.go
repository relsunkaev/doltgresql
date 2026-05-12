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
						WHERE rolname = 'pg_database_owner';`,
					Expected: []sql.Row{{"pg_database_owner", "f", "f"}},
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
						WHERE nspname = 'public';`,
					Expected: []sql.Row{{"pg_database_owner"}},
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
						ORDER BY rolname;`,
					Expected: []sql.Row{
						{"pg_execute_server_program", "f", "f"},
						{"pg_read_server_files", "f", "f"},
						{"pg_write_server_files", "f", "f"},
					},
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
						ORDER BY rolname;`,
					Expected: []sql.Row{
						{"pg_monitor", "f", "f"},
						{"pg_read_all_data", "f", "f"},
						{"pg_read_all_settings", "f", "f"},
						{"pg_read_all_stats", "f", "f"},
						{"pg_signal_backend", "f", "f"},
						{"pg_stat_scan_tables", "f", "f"},
						{"pg_write_all_data", "f", "f"},
					},
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
						ORDER BY pg_get_userbyid(roleid);`,
					Expected: []sql.Row{
						{"pg_read_all_settings", "pg_monitor"},
						{"pg_read_all_stats", "pg_monitor"},
						{"pg_stat_scan_tables", "pg_monitor"},
					},
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
					Expected: []sql.Row{{1, "visible through predefined role"}},
					Username: `read_all_data_user`,
					Password: `pw`,
				},
			},
		},
	})
}

// TestPgWriteAllDataRoleAllowsTableWritesRepro reproduces a predefined-role
// privilege bug: membership in pg_write_all_data should allow writing tables
// without per-table INSERT grants.
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
						ORDER BY rolname;`,
					Expected: []sql.Row{
						{"pg_maintain", "f", "f"},
						{"pg_signal_autovacuum_worker", "f", "f"},
					},
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
						WHERE rolname = 'pg_checkpoint';`,
					Expected: []sql.Row{{"pg_checkpoint", "f", "f"}},
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
						WHERE rolname = 'pg_create_subscription';`,
					Expected: []sql.Row{{"pg_create_subscription", "f", "f"}},
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
						WHERE rolname = 'pg_use_reserved_connections';`,
					Expected: []sql.Row{{"pg_use_reserved_connections", "f", "f"}},
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
						WHERE usename = 'shadow_catalog_user';`,
					Expected: []sql.Row{{"shadow_catalog_user", "f", "f"}},
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
						WHERE rolname = 'authid_password_user';`,
					Expected: []sql.Row{{"authid_password_user", "t"}},
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
						WHERE usename = 'pguser_password_user';`,
					Expected: []sql.Row{{"pguser_password_user", "********"}},
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
					Query:    `CREATE ROLE caserole LOGIN PASSWORD 'folded';`,
					Expected: []sql.Row{},
				},
				{
					Query: `SELECT rolname
						FROM pg_catalog.pg_roles
						WHERE rolname IN ('CaseRole', 'caserole')
						ORDER BY rolname;`,
					Expected: []sql.Row{{"CaseRole"}, {"caserole"}},
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
					Query:       `SELECT rolname FROM pg_catalog.pg_authid LIMIT 1;`,
					ExpectedErr: `permission denied for table pg_authid`,
					Username:    `authid_reader`,
					Password:    `pw`,
				},
			},
		},
	})
}
