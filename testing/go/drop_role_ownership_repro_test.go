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

// TestDropRoleOwningTableRepro reproduces a catalog integrity bug: Doltgres
// allows DROP ROLE even when the role owns a table.
func TestDropRoleOwningTableRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP ROLE rejects roles that own tables",
			SetUpScript: []string{
				`CREATE USER doomed_owner PASSWORD 'owner';`,
				`GRANT ALL PRIVILEGES ON SCHEMA public TO doomed_owner;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `CREATE TABLE doomed_owned_table (id INT PRIMARY KEY);`,
					Username: `doomed_owner`,
					Password: `owner`,
				},
				{
					Query:       `DROP ROLE doomed_owner;`,
					ExpectedErr: `cannot be dropped`,
				},
			},
		},
	})
}

// TestDropRoleWithGrantedTablePrivilegesRepro reproduces a catalog integrity
// bug: Doltgres allows DROP ROLE even while explicit table ACL entries still
// depend on that role.
func TestDropRoleWithGrantedTablePrivilegesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP ROLE rejects roles with granted table privileges",
			SetUpScript: []string{
				`CREATE ROLE doomed_acl_role;`,
				`CREATE TABLE doomed_acl_items (id INT PRIMARY KEY);`,
				`GRANT SELECT ON doomed_acl_items TO doomed_acl_role;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DROP ROLE doomed_acl_role;`,
					ExpectedErr: `cannot be dropped`,
				},
			},
		},
	})
}

// TestDropRoleWithGrantedSchemaPrivilegesRepro reproduces a catalog integrity
// bug: Doltgres allows DROP ROLE even while explicit schema ACL entries still
// depend on that role.
func TestDropRoleWithGrantedSchemaPrivilegesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP ROLE rejects roles with granted schema privileges",
			SetUpScript: []string{
				`CREATE ROLE doomed_schema_acl_role;`,
				`CREATE SCHEMA doomed_schema_acl_target;`,
				`GRANT USAGE ON SCHEMA doomed_schema_acl_target TO doomed_schema_acl_role;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DROP ROLE doomed_schema_acl_role;`,
					ExpectedErr: `cannot be dropped`,
				},
			},
		},
	})
}

// TestDropRoleWithGrantedSequencePrivilegesRepro reproduces a catalog integrity
// bug: Doltgres allows DROP ROLE even while explicit sequence ACL entries still
// depend on that role.
func TestDropRoleWithGrantedSequencePrivilegesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP ROLE rejects roles with granted sequence privileges",
			SetUpScript: []string{
				`CREATE ROLE doomed_sequence_acl_role;`,
				`CREATE SEQUENCE doomed_sequence_acl_seq;`,
				`GRANT USAGE ON SEQUENCE doomed_sequence_acl_seq TO doomed_sequence_acl_role;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DROP ROLE doomed_sequence_acl_role;`,
					ExpectedErr: `cannot be dropped`,
				},
			},
		},
	})
}

// TestDropRoleWithGrantedRoutinePrivilegesRepro reproduces a catalog integrity
// bug: Doltgres allows DROP ROLE even while explicit routine ACL entries still
// depend on that role.
func TestDropRoleWithGrantedRoutinePrivilegesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP ROLE rejects roles with granted routine privileges",
			SetUpScript: []string{
				`CREATE ROLE doomed_routine_acl_role;`,
				`CREATE FUNCTION doomed_routine_acl_func(input INT) RETURNS INT
					LANGUAGE SQL
					AS $$ SELECT input + 1 $$;`,
				`GRANT EXECUTE ON FUNCTION doomed_routine_acl_func(INT) TO doomed_routine_acl_role;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DROP ROLE doomed_routine_acl_role;`,
					ExpectedErr: `cannot be dropped`,
				},
			},
		},
	})
}

// TestDropRoleWithGrantedDatabasePrivilegesRepro reproduces a catalog integrity
// bug: Doltgres allows DROP ROLE even while explicit database ACL entries still
// depend on that role.
func TestDropRoleWithGrantedDatabasePrivilegesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP ROLE rejects roles with granted database privileges",
			SetUpScript: []string{
				`CREATE ROLE doomed_database_acl_role;`,
				`GRANT CONNECT ON DATABASE postgres TO doomed_database_acl_role;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DROP ROLE doomed_database_acl_role;`,
					ExpectedErr: `cannot be dropped`,
				},
			},
		},
	})
}

// TestDropRoleWithGrantedRoleMembershipRepro reproduces a catalog integrity bug:
// Doltgres allows DROP ROLE even while that role is the grantor of an active
// role membership.
func TestDropRoleWithGrantedRoleMembershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP ROLE rejects roles that granted role memberships",
			SetUpScript: []string{
				`CREATE ROLE doomed_membership_group;`,
				`CREATE USER doomed_membership_grantor PASSWORD 'grantor';`,
				`CREATE ROLE doomed_membership_member;`,
				`GRANT doomed_membership_group
						TO doomed_membership_grantor WITH ADMIN OPTION;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `GRANT doomed_membership_group
						TO doomed_membership_member;`,
					Username: `doomed_membership_grantor`,
					Password: `grantor`,
				},
				{
					Query:       `DROP ROLE doomed_membership_grantor;`,
					ExpectedErr: `cannot be dropped`,
				},
			},
		},
	})
}
