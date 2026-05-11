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

import "testing"

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
					Query:       `ALTER ROLE self_createdb CREATEDB;`,
					ExpectedErr: `permission denied`,
					Username:    `self_createdb`,
					Password:    `pw`,
				},
			},
		},
	})
}

// TestAlterRoleSelfCreateRolePrivilegeEscalationRepro reproduces a security
// bug: a normal role can grant itself CREATEROLE.
func TestAlterRoleSelfCreateRolePrivilegeEscalationRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "normal role cannot grant itself CREATEROLE",
			SetUpScript: []string{
				`CREATE USER self_createrole PASSWORD 'pw';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `ALTER ROLE self_createrole CREATEROLE;`,
					ExpectedErr: `permission denied`,
					Username:    `self_createrole`,
					Password:    `pw`,
				},
			},
		},
	})
}

// TestAlterRoleSelfSuperuserPrivilegeEscalationGuard guards that a normal role
// cannot grant itself SUPERUSER.
func TestAlterRoleSelfSuperuserPrivilegeEscalationGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "normal role cannot grant itself SUPERUSER",
			SetUpScript: []string{
				`CREATE USER self_superuser PASSWORD 'pw';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `ALTER ROLE self_superuser SUPERUSER;`,
					ExpectedErr: `does not have permission`,
					Username:    `self_superuser`,
					Password:    `pw`,
				},
			},
		},
	})
}

// TestCreateRoleSuperuserRequiresSuperuserGuard guards that a non-superuser
// role with CREATEROLE cannot create a SUPERUSER role.
func TestCreateRoleSuperuserRequiresSuperuserGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE ROLE SUPERUSER requires superuser",
			SetUpScript: []string{
				`CREATE USER superuser_role_creator PASSWORD 'pw' CREATEROLE;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `CREATE ROLE superuser_created SUPERUSER;`,
					ExpectedErr: `does not have permission`,
					Username:    `superuser_role_creator`,
					Password:    `pw`,
				},
			},
		},
	})
}

// TestCreateRoleBypassRLSRequiresSuperuserRepro reproduces a security bug:
// a non-superuser role with CREATEROLE can create a BYPASSRLS role.
func TestCreateRoleBypassRLSRequiresSuperuserRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE ROLE BYPASSRLS requires superuser",
			SetUpScript: []string{
				`CREATE USER bypass_role_creator PASSWORD 'pw' CREATEROLE;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `CREATE ROLE bypass_created BYPASSRLS;`,
					ExpectedErr: `permission denied`,
					Username:    `bypass_role_creator`,
					Password:    `pw`,
				},
			},
		},
	})
}

// TestCreateRoleReplicationRequiresSuperuserRepro reproduces a security bug:
// a non-superuser role with CREATEROLE can create a REPLICATION role.
func TestCreateRoleReplicationRequiresSuperuserRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE ROLE REPLICATION requires superuser",
			SetUpScript: []string{
				`CREATE USER replication_role_creator PASSWORD 'pw' CREATEROLE;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `CREATE ROLE replication_created REPLICATION;`,
					ExpectedErr: `permission denied`,
					Username:    `replication_role_creator`,
					Password:    `pw`,
				},
			},
		},
	})
}

// TestCreateRoleCreatedbRequiresCreatedbRepro reproduces a security bug:
// CREATEROLE alone does not allow creating roles with CREATEDB.
func TestCreateRoleCreatedbRequiresCreatedbRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE ROLE CREATEDB requires CREATEDB",
			SetUpScript: []string{
				`CREATE USER createdb_role_creator PASSWORD 'pw' CREATEROLE;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `CREATE ROLE createdb_created CREATEDB;`,
					ExpectedErr: `permission denied`,
					Username:    `createdb_role_creator`,
					Password:    `pw`,
				},
			},
		},
	})
}

// TestAlterRoleBypassRLSRequiresSuperuserGuard guards that a non-superuser role
// with CREATEROLE cannot grant BYPASSRLS to another role.
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
					Query:       `ALTER ROLE bypass_altered BYPASSRLS;`,
					ExpectedErr: `does not have permission`,
					Username:    `bypass_role_alterer`,
					Password:    `pw`,
				},
			},
		},
	})
}

// TestAlterRoleReplicationRequiresSuperuserGuard guards that a non-superuser
// role with CREATEROLE cannot grant REPLICATION to another role.
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
					Query:       `ALTER ROLE replication_altered REPLICATION;`,
					ExpectedErr: `does not have permission`,
					Username:    `replication_role_alterer`,
					Password:    `pw`,
				},
			},
		},
	})
}

// TestAlterRoleCreatedbRequiresCreatedbRepro reproduces a security bug:
// CREATEROLE alone does not allow granting CREATEDB to another role.
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
					Query:       `ALTER ROLE createdb_altered CREATEDB;`,
					ExpectedErr: `permission denied`,
					Username:    `createdb_role_alterer`,
					Password:    `pw`,
				},
			},
		},
	})
}

// TestAlterRoleSuperuserRequiresSuperuserGuard guards that a non-superuser role
// with CREATEROLE cannot grant SUPERUSER to another role.
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
					Query:       `ALTER ROLE superuser_altered SUPERUSER;`,
					ExpectedErr: `does not have permission`,
					Username:    `superuser_role_alterer`,
					Password:    `pw`,
				},
			},
		},
	})
}
