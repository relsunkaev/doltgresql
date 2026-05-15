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

// TestAlterRoleSelfCreatedbPrivilegeEscalationRepro reproduces a security bug:
// a normal role can grant itself CREATEDB.
func TestAlterRoleSelfCreatedbPrivilegeEscalationRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "normal role cannot grant itself CREATEDB",
			SetUpScript: []string{
				`CREATE USER self_createdb PASSWORD 'pw';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER ROLE self_createdb CREATEDB;`,

					Username: `self_createdb`,
					Password: `pw`, PostgresOracle: ScriptTestPostgresOracle{

						// TestAlterRoleSelfCreateRolePrivilegeEscalationRepro reproduces a security
						// bug: a normal role can grant itself CREATEROLE.
						ID: "role-privilege-escalation-repro-test-testalterroleselfcreatedbprivilegeescalationrepro-0001-alter-role-self_createdb-createdb", Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestAlterRoleSelfCreateRolePrivilegeEscalationRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "normal role cannot grant itself CREATEROLE",
			SetUpScript: []string{
				`CREATE USER self_createrole PASSWORD 'pw';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER ROLE self_createrole CREATEROLE;`,

					Username: `self_createrole`,
					Password: `pw`, PostgresOracle: ScriptTestPostgresOracle{

						// TestAlterRoleSelfLoginAttributeRepro reproduces a role-authorization bug: a
						// normal role cannot change its own LOGIN attribute.
						ID: "role-privilege-escalation-repro-test-testalterroleselfcreateroleprivilegeescalationrepro-0001-alter-role-self_createrole-createrole", Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestAlterRoleSelfLoginAttributeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "normal role cannot change its own LOGIN attribute",
			SetUpScript: []string{
				`CREATE USER self_login_attr PASSWORD 'pw';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER ROLE self_login_attr NOLOGIN;`,

					Username: `self_login_attr`,
					Password: `pw`, PostgresOracle: ScriptTestPostgresOracle{

						// TestAlterRoleSelfInheritAttributeRepro reproduces a role-authorization bug:
						// a normal role cannot change its own INHERIT attribute.
						ID: "role-privilege-escalation-repro-test-testalterroleselfloginattributerepro-0001-alter-role-self_login_attr-nologin", Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestAlterRoleSelfInheritAttributeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "normal role cannot change its own INHERIT attribute",
			SetUpScript: []string{
				`CREATE USER self_inherit_attr PASSWORD 'pw';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER ROLE self_inherit_attr NOINHERIT;`,

					Username: `self_inherit_attr`,
					Password: `pw`, PostgresOracle: ScriptTestPostgresOracle{

						// TestAlterRoleSelfSuperuserPrivilegeEscalationGuard guards that a normal role
						// cannot grant itself SUPERUSER.
						ID: "role-privilege-escalation-repro-test-testalterroleselfinheritattributerepro-0001-alter-role-self_inherit_attr-noinherit", Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestAlterRoleSelfSuperuserPrivilegeEscalationGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "normal role cannot grant itself SUPERUSER",
			SetUpScript: []string{
				`CREATE USER self_superuser PASSWORD 'pw';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER ROLE self_superuser SUPERUSER;`,

					Username: `self_superuser`,
					Password: `pw`, PostgresOracle: ScriptTestPostgresOracle{

						// TestCreateRoleSuperuserRequiresSuperuserGuard guards that a non-superuser
						// role with CREATEROLE cannot create a SUPERUSER role.
						ID: "role-privilege-escalation-repro-test-testalterroleselfsuperuserprivilegeescalationguard-0001-alter-role-self_superuser-superuser", Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestCreateRoleSuperuserRequiresSuperuserGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE ROLE SUPERUSER requires superuser",
			SetUpScript: []string{
				`CREATE USER superuser_role_creator PASSWORD 'pw' CREATEROLE;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE ROLE superuser_created SUPERUSER;`,

					Username: `superuser_role_creator`,
					Password: `pw`, PostgresOracle: ScriptTestPostgresOracle{

						// TestCreateRoleBypassRLSRequiresSuperuserRepro reproduces a security bug:
						// a non-superuser role with CREATEROLE can create a BYPASSRLS role.
						ID: "role-privilege-escalation-repro-test-testcreaterolesuperuserrequiressuperuserguard-0001-create-role-superuser_created-superuser", Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestCreateRoleBypassRLSRequiresSuperuserRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE ROLE BYPASSRLS requires superuser",
			SetUpScript: []string{
				`CREATE USER bypass_role_creator PASSWORD 'pw' CREATEROLE;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE ROLE bypass_created BYPASSRLS;`,

					Username: `bypass_role_creator`,
					Password: `pw`, PostgresOracle: ScriptTestPostgresOracle{

						// TestCreateRoleReplicationRequiresSuperuserRepro reproduces a security bug:
						// a non-superuser role with CREATEROLE can create a REPLICATION role.
						ID: "role-privilege-escalation-repro-test-testcreaterolebypassrlsrequiressuperuserrepro-0001-create-role-bypass_created-bypassrls", Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestCreateRoleReplicationRequiresSuperuserRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE ROLE REPLICATION requires superuser",
			SetUpScript: []string{
				`CREATE USER replication_role_creator PASSWORD 'pw' CREATEROLE;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE ROLE replication_created REPLICATION;`,

					Username: `replication_role_creator`,
					Password: `pw`, PostgresOracle: ScriptTestPostgresOracle{

						// TestCreateRoleCreatedbRequiresCreatedbRepro reproduces a security bug:
						// CREATEROLE alone does not allow creating roles with CREATEDB.
						ID: "role-privilege-escalation-repro-test-testcreaterolereplicationrequiressuperuserrepro-0001-create-role-replication_created-replication", Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestCreateRoleCreatedbRequiresCreatedbRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE ROLE CREATEDB requires CREATEDB",
			SetUpScript: []string{
				`CREATE USER createdb_role_creator PASSWORD 'pw' CREATEROLE;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE ROLE createdb_created CREATEDB;`,

					Username: `createdb_role_creator`,
					Password: `pw`, PostgresOracle: ScriptTestPostgresOracle{

						// TestAlterRoleBypassRLSRequiresSuperuserGuard guards that a non-superuser role
						// with CREATEROLE cannot grant BYPASSRLS to another role.
						ID: "role-privilege-escalation-repro-test-testcreaterolecreatedbrequirescreatedbrepro-0001-create-role-createdb_created-createdb", Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestAlterRoleBypassRLSRequiresSuperuserGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER ROLE BYPASSRLS requires superuser",
			SetUpScript: []string{
				`CREATE USER bypass_role_alterer PASSWORD 'pw' CREATEROLE;`,
				`CREATE ROLE bypass_altered;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER ROLE bypass_altered BYPASSRLS;`,

					Username: `bypass_role_alterer`,
					Password: `pw`, PostgresOracle: ScriptTestPostgresOracle{

						// TestAlterRoleReplicationRequiresSuperuserGuard guards that a non-superuser
						// role with CREATEROLE cannot grant REPLICATION to another role.
						ID: "role-privilege-escalation-repro-test-testalterrolebypassrlsrequiressuperuserguard-0001-alter-role-bypass_altered-bypassrls", Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestAlterRoleReplicationRequiresSuperuserGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER ROLE REPLICATION requires superuser",
			SetUpScript: []string{
				`CREATE USER replication_role_alterer PASSWORD 'pw' CREATEROLE;`,
				`CREATE ROLE replication_altered;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER ROLE replication_altered REPLICATION;`,

					Username: `replication_role_alterer`,
					Password: `pw`, PostgresOracle: ScriptTestPostgresOracle{

						// TestAlterRoleCreatedbRequiresCreatedbRepro reproduces a security bug:
						// CREATEROLE alone does not allow granting CREATEDB to another role.
						ID: "role-privilege-escalation-repro-test-testalterrolereplicationrequiressuperuserguard-0001-alter-role-replication_altered-replication", Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestAlterRoleCreatedbRequiresCreatedbRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER ROLE CREATEDB requires CREATEDB",
			SetUpScript: []string{
				`CREATE USER createdb_role_alterer PASSWORD 'pw' CREATEROLE;`,
				`CREATE ROLE createdb_altered;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER ROLE createdb_altered CREATEDB;`,

					Username: `createdb_role_alterer`,
					Password: `pw`, PostgresOracle: ScriptTestPostgresOracle{

						// TestAlterRoleSuperuserRequiresSuperuserGuard guards that a non-superuser role
						// with CREATEROLE cannot grant SUPERUSER to another role.
						ID: "role-privilege-escalation-repro-test-testalterrolecreatedbrequirescreatedbrepro-0001-alter-role-createdb_altered-createdb", Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestAlterRoleSuperuserRequiresSuperuserGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER ROLE SUPERUSER requires superuser",
			SetUpScript: []string{
				`CREATE USER superuser_role_alterer PASSWORD 'pw' CREATEROLE;`,
				`CREATE ROLE superuser_altered;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER ROLE superuser_altered SUPERUSER;`,

					Username: `superuser_role_alterer`,
					Password: `pw`, PostgresOracle: ScriptTestPostgresOracle{ID: "role-privilege-escalation-repro-test-testalterrolesuperuserrequiressuperuserguard-0001-alter-role-superuser_altered-superuser", Compare: "sqlstate"},
				},
			},
		},
	})
}
