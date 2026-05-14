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
)

// TestGrantRoleRejectsCircularMembershipRepro reproduces a security/availability
// bug: Doltgres does not return a normal SQL error for circular role membership.
func TestGrantRoleRejectsCircularMembershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "GRANT role rejects circular membership",
			SetUpScript: []string{
				`CREATE ROLE circular_role_a;`,
				`CREATE ROLE circular_role_b;`,
				`GRANT circular_role_a TO circular_role_b;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `GRANT circular_role_b TO circular_role_a;`, PostgresOracle: ScriptTestPostgresOracle{ID: "role-membership-repro-test-testgrantrolerejectscircularmembershiprepro-0001-grant-circular_role_b-to-circular_role_a",

						// TestGrantSuperuserRoleDoesNotTripCircularMembershipRepro reproduces a role
						// membership bug: the superuser's virtual membership in every role should not
						// make GRANTing the superuser role look circular.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestGrantSuperuserRoleDoesNotTripCircularMembershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "GRANT superuser role does not trip circular membership guard",
			SetUpScript: []string{
				`CREATE USER grant_superuser_member PASSWORD 'member';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `GRANT postgres TO grant_superuser_member;`,
				},
				{
					Query: `SELECT pg_get_userbyid(roleid), pg_get_userbyid(member)
						FROM pg_catalog.pg_auth_members
						WHERE pg_get_userbyid(roleid) = 'postgres'
							AND pg_get_userbyid(member) = 'grant_superuser_member';`, PostgresOracle: ScriptTestPostgresOracle{ID: "role-membership-repro-test-testgrantsuperuserroledoesnottripcircularmembershiprepro-0001-select-pg_get_userbyid-roleid-pg_get_userbyid-member"},
				},
			},
		},
	})
}

// TestGrantRoleRejectsSelfMembershipRepro reproduces a security/availability
// bug: Doltgres does not return a normal SQL error when a role is granted to
// itself.
func TestGrantRoleRejectsSelfMembershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "GRANT role rejects self membership",
			SetUpScript: []string{
				`CREATE ROLE self_member_role;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `GRANT self_member_role TO self_member_role;`, PostgresOracle: ScriptTestPostgresOracle{ID: "role-membership-repro-test-testgrantrolerejectsselfmembershiprepro-0001-grant-self_member_role-to-self_member_role",

						// TestGrantRolePopulatesPgAuthMembersRepro reproduces a catalog persistence
						// bug: Doltgres accepts role membership grants but pg_auth_members does not
						// expose the membership row.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestGrantRolePopulatesPgAuthMembersRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "GRANT role populates pg_auth_members",
			SetUpScript: []string{
				`CREATE ROLE catalog_parent_role;`,
				`CREATE ROLE catalog_child_role;`,
				`GRANT catalog_parent_role TO catalog_child_role;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT pg_get_userbyid(roleid), pg_get_userbyid(member), admin_option
						FROM pg_catalog.pg_auth_members
						WHERE pg_get_userbyid(roleid) = 'catalog_parent_role'
							AND pg_get_userbyid(member) = 'catalog_child_role';`, PostgresOracle: ScriptTestPostgresOracle{ID: "role-membership-repro-test-testgrantrolepopulatespgauthmembersrepro-0001-select-pg_get_userbyid-roleid-pg_get_userbyid-member"},
				},
			},
		},
	})
}

// TestGrantRolePopulatesPgGroupRepro reproduces a catalog persistence bug:
// role membership grants should be visible through the legacy pg_group view.
func TestGrantRolePopulatesPgGroupRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "GRANT role populates pg_group",
			SetUpScript: []string{
				`CREATE ROLE group_catalog_role;`,
				`CREATE ROLE group_catalog_member;`,
				`GRANT group_catalog_role TO group_catalog_member;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT groname, grolist IS NOT NULL
						FROM pg_catalog.pg_group
						WHERE groname = 'group_catalog_role';`, PostgresOracle: ScriptTestPostgresOracle{ID: "role-membership-repro-test-testgrantrolepopulatespggrouprepro-0001-select-groname-grolist-is-not"},
				},
			},
		},
	})
}

// TestCreateRoleInRoleGrantsMembershipRepro reproduces a role-membership DDL
// correctness bug: CREATE ROLE supports granting the new role membership in
// existing roles through IN ROLE.
func TestCreateRoleInRoleGrantsMembershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE ROLE IN ROLE grants membership",
			SetUpScript: []string{
				`CREATE ROLE create_in_role_parent;`,
				`CREATE ROLE create_in_role_member IN ROLE create_in_role_parent;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT pg_has_role(
							'create_in_role_member',
							'create_in_role_parent',
							'member'
						);`, PostgresOracle: ScriptTestPostgresOracle{ID: "role-membership-repro-test-testcreateroleinrolegrantsmembershiprepro-0001-select-pg_has_role-create_in_role_member-create_in_role_parent-member"},
				},
			},
		},
	})
}

// TestCreateRoleRoleOptionAddsMembersRepro reproduces a role-membership DDL
// correctness bug: CREATE ROLE ... ROLE should add existing roles as members of
// the newly-created role.
func TestCreateRoleRoleOptionAddsMembersRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE ROLE ROLE option adds members",
			SetUpScript: []string{
				`CREATE ROLE create_role_option_member;`,
				`CREATE ROLE create_role_option_group ROLE create_role_option_member;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT pg_has_role(
							'create_role_option_member',
							'create_role_option_group',
							'member'
						);`, PostgresOracle: ScriptTestPostgresOracle{ID: "role-membership-repro-test-testcreateroleroleoptionaddsmembersrepro-0001-select-pg_has_role-create_role_option_member-create_role_option_group-member"},
				},
			},
		},
	})
}

// TestCreateRoleAdminOptionGrantsAdminMembershipRepro reproduces a
// role-membership DDL correctness bug: CREATE ROLE ... ADMIN should add members
// with the admin option, allowing those members to delegate the new role.
func TestCreateRoleAdminOptionGrantsAdminMembershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE ROLE ADMIN option grants delegable membership",
			SetUpScript: []string{
				`CREATE USER create_admin_option_member PASSWORD 'pw';`,
				`CREATE ROLE create_admin_option_target;`,
				`CREATE ROLE create_admin_option_group ADMIN create_admin_option_member;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `GRANT create_admin_option_group TO create_admin_option_target;`,
					Username: `create_admin_option_member`,
					Password: `pw`,
				},
				{
					Query: `SELECT pg_has_role(
							'create_admin_option_target',
							'create_admin_option_group',
							'member'
						);`, PostgresOracle: ScriptTestPostgresOracle{ID: "role-membership-repro-test-testcreateroleadminoptiongrantsadminmembershiprepro-0001-select-pg_has_role-create_admin_option_target-create_admin_option_group-member"},
				},
			},
		},
	})
}

// TestGrantRoleRequiresAdminOptionGuard guards that ordinary membership in a
// role does not allow granting that role onward.
func TestGrantRoleRequiresAdminOptionGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "GRANT role requires admin option",
			SetUpScript: []string{
				`CREATE ROLE delegated_role;`,
				`CREATE USER delegated_member PASSWORD 'pw';`,
				`CREATE USER delegated_target PASSWORD 'pw';`,
				`GRANT delegated_role TO delegated_member;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `GRANT delegated_role TO delegated_target;`,

					Username: `delegated_member`,
					Password: `pw`, PostgresOracle: ScriptTestPostgresOracle{

						// TestGrantRoleAdminOptionAllowsDelegationGuard guards that WITH ADMIN OPTION
						// allows a role member to grant that role onward.
						ID: "role-membership-repro-test-testgrantrolerequiresadminoptionguard-0001-grant-delegated_role-to-delegated_target", Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestGrantRoleAdminOptionAllowsDelegationGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "GRANT role with admin option allows delegation",
			SetUpScript: []string{
				`CREATE ROLE admin_delegated_role;`,
				`CREATE USER admin_delegate PASSWORD 'pw';`,
				`CREATE USER admin_target PASSWORD 'pw';`,
				`GRANT admin_delegated_role TO admin_delegate WITH ADMIN OPTION;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `GRANT admin_delegated_role TO admin_target;`,
					Username: `admin_delegate`,
					Password: `pw`,
				},
			},
		},
	})
}

// TestRevokeRoleOnlyRemovesNamedGrantorMembershipRepro reproduces a role
// membership persistence bug: two grantors can independently grant the same
// role membership, and revoking one grantor's edge should leave the other.
func TestRevokeRoleOnlyRemovesNamedGrantorMembershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "REVOKE role keeps other grantors' memberships",
			SetUpScript: []string{
				`CREATE ROLE role_multi_group;`,
				`CREATE USER role_multi_admin_one PASSWORD 'one';`,
				`CREATE USER role_multi_admin_two PASSWORD 'two';`,
				`CREATE USER role_multi_member PASSWORD 'member';`,
				`GRANT role_multi_group
						TO role_multi_admin_one WITH ADMIN OPTION;`,
				`GRANT role_multi_group
						TO role_multi_admin_two WITH ADMIN OPTION;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `GRANT role_multi_group
						TO role_multi_member;`,
					Username: `role_multi_admin_one`,
					Password: `one`,
				},
				{
					Query: `GRANT role_multi_group
						TO role_multi_member;`,
					Username: `role_multi_admin_two`,
					Password: `two`,
				},
				{
					Query: `REVOKE role_multi_group
						FROM role_multi_member;`,
					Username: `role_multi_admin_one`,
					Password: `one`,
				},
				{
					Query: `SELECT pg_has_role(
							'role_multi_member',
							oid,
							'member'
						)
						FROM pg_catalog.pg_roles
						WHERE rolname = 'role_multi_group';`, PostgresOracle: ScriptTestPostgresOracle{ID: "role-membership-repro-test-testrevokeroleonlyremovesnamedgrantormembershiprepro-0001-select-pg_has_role-role_multi_member-oid-member"},
				},
			},
		},
	})
}

// TestRevokeAdminOptionOnlyRemovesNamedGrantorMembershipRepro reproduces a role
// membership persistence bug: revoking one grantor's admin option should leave
// another grantor's admin-option edge available for delegation.
func TestRevokeAdminOptionOnlyRemovesNamedGrantorMembershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "REVOKE ADMIN OPTION keeps other grantors' memberships",
			SetUpScript: []string{
				`CREATE ROLE role_multi_option_group;`,
				`CREATE USER role_multi_option_admin_one PASSWORD 'one';`,
				`CREATE USER role_multi_option_admin_two PASSWORD 'two';`,
				`CREATE USER role_multi_option_member PASSWORD 'member';`,
				`CREATE USER role_multi_option_delegate PASSWORD 'delegate';`,
				`GRANT role_multi_option_group
						TO role_multi_option_admin_one WITH ADMIN OPTION;`,
				`GRANT role_multi_option_group
						TO role_multi_option_admin_two WITH ADMIN OPTION;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `GRANT role_multi_option_group
						TO role_multi_option_member WITH ADMIN OPTION;`,
					Username: `role_multi_option_admin_one`,
					Password: `one`,
				},
				{
					Query: `GRANT role_multi_option_group
						TO role_multi_option_member WITH ADMIN OPTION;`,
					Username: `role_multi_option_admin_two`,
					Password: `two`,
				},
				{
					Query: `REVOKE ADMIN OPTION FOR role_multi_option_group
						FROM role_multi_option_member;`,
					Username: `role_multi_option_admin_one`,
					Password: `one`,
				},
				{
					Query: `GRANT role_multi_option_group
						TO role_multi_option_delegate;`,
					Username: `role_multi_option_member`,
					Password: `member`,
				},
				{
					Query: `SELECT pg_has_role(
							'role_multi_option_delegate',
							oid,
							'member'
						)
						FROM pg_catalog.pg_roles
						WHERE rolname = 'role_multi_option_group';`, PostgresOracle: ScriptTestPostgresOracle{ID: "role-membership-repro-test-testrevokeadminoptiononlyremovesnamedgrantormembershiprepro-0001-select-pg_has_role-role_multi_option_delegate-oid-member"},
				},
			},
		},
	})
}

// TestRevokeAdminOptionRestrictRejectsDependentRoleGrantRepro reproduces a role
// membership dependency bug: REVOKE ADMIN OPTION ... RESTRICT should reject
// revoking an admin option that has dependent role grants.
func TestRevokeAdminOptionRestrictRejectsDependentRoleGrantRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "REVOKE ADMIN OPTION RESTRICT rejects dependent role grant",
			SetUpScript: []string{
				`CREATE ROLE revoke_role_restrict_group;`,
				`CREATE USER revoke_role_restrict_admin PASSWORD 'admin';`,
				`CREATE USER revoke_role_restrict_middle PASSWORD 'middle';`,
				`CREATE USER revoke_role_restrict_leaf PASSWORD 'leaf';`,
				`CREATE USER revoke_role_restrict_delegate PASSWORD 'delegate';`,
				`GRANT revoke_role_restrict_group
						TO revoke_role_restrict_admin WITH ADMIN OPTION;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `GRANT revoke_role_restrict_group
						TO revoke_role_restrict_middle WITH ADMIN OPTION;`,
					Username: `revoke_role_restrict_admin`,
					Password: `admin`,
				},
				{
					Query: `GRANT revoke_role_restrict_group
						TO revoke_role_restrict_leaf;`,
					Username: `revoke_role_restrict_middle`,
					Password: `middle`,
				},
				{
					Query: `REVOKE ADMIN OPTION FOR revoke_role_restrict_group
						FROM revoke_role_restrict_middle RESTRICT;`,

					Username: `revoke_role_restrict_admin`,
					Password: `admin`, PostgresOracle: ScriptTestPostgresOracle{ID: "role-membership-repro-test-testrevokeadminoptionrestrictrejectsdependentrolegrantrepro-0001-revoke-admin-option-for-revoke_role_restrict_group"},
				},
				{
					Query: `GRANT revoke_role_restrict_group
						TO revoke_role_restrict_delegate;`,
					Username: `revoke_role_restrict_middle`,
					Password: `middle`,
				},
				{
					Query: `SELECT pg_has_role(
							'revoke_role_restrict_delegate',
							oid,
							'member'
						)
						FROM pg_catalog.pg_roles
						WHERE rolname = 'revoke_role_restrict_group';`, PostgresOracle: ScriptTestPostgresOracle{ID: "role-membership-repro-test-testrevokeadminoptionrestrictrejectsdependentrolegrantrepro-0002-select-pg_has_role-revoke_role_restrict_delegate-oid-member"},
				},
			},
		},
	})
}

// TestRevokeAdminOptionCascadeRemovesDependentRoleGrantRepro reproduces a role
// membership dependency bug: REVOKE ADMIN OPTION ... CASCADE should remove
// dependent role grants made through the revoked admin option.
func TestRevokeAdminOptionCascadeRemovesDependentRoleGrantRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "REVOKE ADMIN OPTION CASCADE removes dependent role grant",
			SetUpScript: []string{
				`CREATE ROLE revoke_role_cascade_group;`,
				`CREATE USER revoke_role_cascade_admin PASSWORD 'admin';`,
				`CREATE USER revoke_role_cascade_middle PASSWORD 'middle';`,
				`CREATE USER revoke_role_cascade_leaf PASSWORD 'leaf';`,
				`CREATE USER revoke_role_cascade_delegate PASSWORD 'delegate';`,
				`GRANT revoke_role_cascade_group
						TO revoke_role_cascade_admin WITH ADMIN OPTION;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `GRANT revoke_role_cascade_group
						TO revoke_role_cascade_middle WITH ADMIN OPTION;`,
					Username: `revoke_role_cascade_admin`,
					Password: `admin`,
				},
				{
					Query: `GRANT revoke_role_cascade_group
						TO revoke_role_cascade_leaf;`,
					Username: `revoke_role_cascade_middle`,
					Password: `middle`,
				},
				{
					Query: `REVOKE ADMIN OPTION FOR revoke_role_cascade_group
						FROM revoke_role_cascade_middle CASCADE;`,
					Username: `revoke_role_cascade_admin`,
					Password: `admin`,
				},
				{
					Query: `SELECT pg_has_role(
							'revoke_role_cascade_leaf',
							oid,
							'member'
						)
						FROM pg_catalog.pg_roles
						WHERE rolname = 'revoke_role_cascade_group';`, PostgresOracle: ScriptTestPostgresOracle{ID: "role-membership-repro-test-testrevokeadminoptioncascaderemovesdependentrolegrantrepro-0001-select-pg_has_role-revoke_role_cascade_leaf-oid-member"},
				},
				{
					Query: `SELECT pg_has_role(
							'revoke_role_cascade_middle',
							oid,
							'member'
						)
						FROM pg_catalog.pg_roles
						WHERE rolname = 'revoke_role_cascade_group';`, PostgresOracle: ScriptTestPostgresOracle{ID: "role-membership-repro-test-testrevokeadminoptioncascaderemovesdependentrolegrantrepro-0002-select-pg_has_role-revoke_role_cascade_middle-oid-member"},
				},
				{
					Query: `GRANT revoke_role_cascade_group
						TO revoke_role_cascade_delegate;`,

					Username: `revoke_role_cascade_middle`,
					Password: `middle`, PostgresOracle: ScriptTestPostgresOracle{

						// TestRevokeRoleRestrictRejectsDependentRoleGrantRepro reproduces a role
						// membership dependency bug: REVOKE role ... RESTRICT should reject removing a
						// membership that has dependent role grants.
						ID: "role-membership-repro-test-testrevokeadminoptioncascaderemovesdependentrolegrantrepro-0003-grant-revoke_role_cascade_group-to-revoke_role_cascade_delegate", Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestRevokeRoleRestrictRejectsDependentRoleGrantRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "REVOKE role RESTRICT rejects dependent role grant",
			SetUpScript: []string{
				`CREATE ROLE revoke_role_full_restrict_group;`,
				`CREATE USER revoke_role_full_restrict_admin PASSWORD 'admin';`,
				`CREATE USER revoke_role_full_restrict_middle PASSWORD 'middle';`,
				`CREATE USER revoke_role_full_restrict_leaf PASSWORD 'leaf';`,
				`CREATE USER revoke_role_full_restrict_delegate PASSWORD 'delegate';`,
				`GRANT revoke_role_full_restrict_group
						TO revoke_role_full_restrict_admin WITH ADMIN OPTION;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `GRANT revoke_role_full_restrict_group
						TO revoke_role_full_restrict_middle WITH ADMIN OPTION;`,
					Username: `revoke_role_full_restrict_admin`,
					Password: `admin`,
				},
				{
					Query: `GRANT revoke_role_full_restrict_group
						TO revoke_role_full_restrict_leaf;`,
					Username: `revoke_role_full_restrict_middle`,
					Password: `middle`,
				},
				{
					Query: `REVOKE revoke_role_full_restrict_group
						FROM revoke_role_full_restrict_middle RESTRICT;`,

					Username: `revoke_role_full_restrict_admin`,
					Password: `admin`, PostgresOracle: ScriptTestPostgresOracle{ID: "role-membership-repro-test-testrevokerolerestrictrejectsdependentrolegrantrepro-0001-revoke-revoke_role_full_restrict_group-from-revoke_role_full_restrict_middle-restrict"},
				},
				{
					Query: `GRANT revoke_role_full_restrict_group
						TO revoke_role_full_restrict_delegate;`,
					Username: `revoke_role_full_restrict_middle`,
					Password: `middle`,
				},
				{
					Query: `SELECT pg_has_role(
							'revoke_role_full_restrict_middle',
							oid,
							'member'
						)
						FROM pg_catalog.pg_roles
						WHERE rolname = 'revoke_role_full_restrict_group';`, PostgresOracle: ScriptTestPostgresOracle{ID: "role-membership-repro-test-testrevokerolerestrictrejectsdependentrolegrantrepro-0002-select-pg_has_role-revoke_role_full_restrict_middle-oid-member"},
				},
				{
					Query: `SELECT pg_has_role(
							'revoke_role_full_restrict_delegate',
							oid,
							'member'
						)
						FROM pg_catalog.pg_roles
						WHERE rolname = 'revoke_role_full_restrict_group';`, PostgresOracle: ScriptTestPostgresOracle{ID: "role-membership-repro-test-testrevokerolerestrictrejectsdependentrolegrantrepro-0003-select-pg_has_role-revoke_role_full_restrict_delegate-oid-member"},
				},
			},
		},
	})
}

// TestRevokeRoleCascadeRemovesDependentRoleGrantRepro reproduces a role
// membership dependency bug: REVOKE role ... CASCADE should remove dependent
// role grants made through the revoked membership.
func TestRevokeRoleCascadeRemovesDependentRoleGrantRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "REVOKE role CASCADE removes dependent role grant",
			SetUpScript: []string{
				`CREATE ROLE revoke_role_full_cascade_group;`,
				`CREATE USER revoke_role_full_cascade_admin PASSWORD 'admin';`,
				`CREATE USER revoke_role_full_cascade_middle PASSWORD 'middle';`,
				`CREATE USER revoke_role_full_cascade_leaf PASSWORD 'leaf';`,
				`CREATE USER revoke_role_full_cascade_delegate PASSWORD 'delegate';`,
				`GRANT revoke_role_full_cascade_group
						TO revoke_role_full_cascade_admin WITH ADMIN OPTION;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `GRANT revoke_role_full_cascade_group
						TO revoke_role_full_cascade_middle WITH ADMIN OPTION;`,
					Username: `revoke_role_full_cascade_admin`,
					Password: `admin`,
				},
				{
					Query: `GRANT revoke_role_full_cascade_group
						TO revoke_role_full_cascade_leaf;`,
					Username: `revoke_role_full_cascade_middle`,
					Password: `middle`,
				},
				{
					Query: `REVOKE revoke_role_full_cascade_group
						FROM revoke_role_full_cascade_middle CASCADE;`,
					Username: `revoke_role_full_cascade_admin`,
					Password: `admin`,
				},
				{
					Query: `SELECT pg_has_role(
							'revoke_role_full_cascade_middle',
							oid,
							'member'
						)
						FROM pg_catalog.pg_roles
						WHERE rolname = 'revoke_role_full_cascade_group';`, PostgresOracle: ScriptTestPostgresOracle{ID: "role-membership-repro-test-testrevokerolecascaderemovesdependentrolegrantrepro-0001-select-pg_has_role-revoke_role_full_cascade_middle-oid-member"},
				},
				{
					Query: `SELECT pg_has_role(
							'revoke_role_full_cascade_leaf',
							oid,
							'member'
						)
						FROM pg_catalog.pg_roles
						WHERE rolname = 'revoke_role_full_cascade_group';`, PostgresOracle: ScriptTestPostgresOracle{ID: "role-membership-repro-test-testrevokerolecascaderemovesdependentrolegrantrepro-0002-select-pg_has_role-revoke_role_full_cascade_leaf-oid-member"},
				},
				{
					Query: `GRANT revoke_role_full_cascade_group
						TO revoke_role_full_cascade_delegate;`,

					Username: `revoke_role_full_cascade_middle`,
					Password: `middle`, PostgresOracle: ScriptTestPostgresOracle{

						// TestInheritedRolePrivilegesApplyGuard guards that login roles inherit
						// member-role privileges by default.
						ID: "role-membership-repro-test-testrevokerolecascaderemovesdependentrolegrantrepro-0003-grant-revoke_role_full_cascade_group-to-revoke_role_full_cascade_delegate", Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestInheritedRolePrivilegesApplyGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "inherited role privileges apply by default",
			SetUpScript: []string{
				`CREATE ROLE inherited_role_reader;`,
				`CREATE USER inherited_role_user PASSWORD 'pw';`,
				`CREATE TABLE inherited_role_private_items (
					id INT PRIMARY KEY,
					secret TEXT
				);`,
				`INSERT INTO inherited_role_private_items VALUES (1, 'visible through inherited role');`,
				`GRANT USAGE ON SCHEMA public TO inherited_role_reader, inherited_role_user;`,
				`GRANT SELECT ON inherited_role_private_items TO inherited_role_reader;`,
				`GRANT inherited_role_reader TO inherited_role_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, secret
						FROM inherited_role_private_items;`,

					Username: `inherited_role_user`,
					Password: `pw`, PostgresOracle: ScriptTestPostgresOracle{

						// TestGrantRoleWithInheritFalseDoesNotApplyPrivilegesRepro reproduces a role
						// membership security gap: PostgreSQL lets a membership grant explicitly disable
						// privilege inheritance even when the member role itself has INHERIT enabled.
						ID: "role-membership-repro-test-testinheritedroleprivilegesapplyguard-0001-select-id-secret-from-inherited_role_private_items"},
				},
			},
		},
	})
}

func TestGrantRoleWithInheritFalseDoesNotApplyPrivilegesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "GRANT role WITH INHERIT FALSE blocks inherited privileges",
			SetUpScript: []string{
				`CREATE ROLE grant_inherit_false_reader;`,
				`CREATE USER grant_inherit_false_user PASSWORD 'pw';`,
				`CREATE TABLE grant_inherit_false_private_items (
					id INT PRIMARY KEY,
					secret TEXT
				);`,
				`INSERT INTO grant_inherit_false_private_items VALUES (1, 'hidden without set role');`,
				`GRANT USAGE ON SCHEMA public TO grant_inherit_false_reader, grant_inherit_false_user;`,
				`GRANT SELECT ON grant_inherit_false_private_items TO grant_inherit_false_reader;`,
				`GRANT grant_inherit_false_reader TO grant_inherit_false_user WITH INHERIT FALSE;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, secret
						FROM grant_inherit_false_private_items;`,

					Username: `grant_inherit_false_user`,
					Password: `pw`, PostgresOracle: ScriptTestPostgresOracle{

						// TestGrantRoleWithInheritFalsePopulatesPgAuthMembersRepro reproduces a role
						// membership catalog gap: pg_auth_members should record per-membership inherit
						// and set options independently from the member role's default attributes.
						ID: "role-membership-repro-test-testgrantrolewithinheritfalsedoesnotapplyprivilegesrepro-0001-select-id-secret-from-grant_inherit_false_private_items", Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestGrantRoleWithInheritFalsePopulatesPgAuthMembersRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "GRANT role WITH INHERIT FALSE records membership options",
			SetUpScript: []string{
				`CREATE ROLE grant_option_catalog_parent;`,
				`CREATE ROLE grant_option_catalog_child;`,
				`GRANT grant_option_catalog_parent
					TO grant_option_catalog_child WITH INHERIT FALSE;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT inherit_option, set_option
						FROM pg_catalog.pg_auth_members
						WHERE pg_get_userbyid(roleid) = 'grant_option_catalog_parent'
							AND pg_get_userbyid(member) = 'grant_option_catalog_child';`, PostgresOracle: ScriptTestPostgresOracle{ID: "role-membership-repro-test-testgrantrolewithinheritfalsepopulatespgauthmembersrepro-0001-select-inherit_option-set_option-from-pg_catalog.pg_auth_members"},
				},
			},
		},
	})
}

// TestGrantRoleWithSetFalsePopulatesPgAuthMembersRepro reproduces a role
// membership catalog gap: pg_auth_members should record when a membership
// explicitly denies SET ROLE to the granted role.
func TestGrantRoleWithSetFalsePopulatesPgAuthMembersRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "GRANT role WITH SET FALSE records membership options",
			SetUpScript: []string{
				`CREATE ROLE grant_set_option_catalog_parent;`,
				`CREATE ROLE grant_set_option_catalog_child;`,
				`GRANT grant_set_option_catalog_parent
					TO grant_set_option_catalog_child WITH SET FALSE;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT inherit_option, set_option
						FROM pg_catalog.pg_auth_members
						WHERE pg_get_userbyid(roleid) = 'grant_set_option_catalog_parent'
							AND pg_get_userbyid(member) = 'grant_set_option_catalog_child';`, PostgresOracle: ScriptTestPostgresOracle{ID: "role-membership-repro-test-testgrantrolewithsetfalsepopulatespgauthmembersrepro-0001-select-inherit_option-set_option-from-pg_catalog.pg_auth_members"},
				},
			},
		},
	})
}

// TestNoInheritRolePrivilegesDoNotApplyGuard guards that NOINHERIT login roles
// do not automatically use privileges from member roles.
func TestNoInheritRolePrivilegesDoNotApplyGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "NOINHERIT role privileges do not apply by default",
			SetUpScript: []string{
				`CREATE ROLE noinherit_role_reader;`,
				`CREATE USER noinherit_role_user PASSWORD 'pw' NOINHERIT;`,
				`CREATE TABLE noinherit_role_private_items (
					id INT PRIMARY KEY,
					secret TEXT
				);`,
				`INSERT INTO noinherit_role_private_items VALUES (1, 'not visible without set role');`,
				`GRANT USAGE ON SCHEMA public TO noinherit_role_reader, noinherit_role_user;`,
				`GRANT SELECT ON noinherit_role_private_items TO noinherit_role_reader;`,
				`GRANT noinherit_role_reader TO noinherit_role_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, secret
						FROM noinherit_role_private_items;`,

					Username: `noinherit_role_user`,
					Password: `pw`, PostgresOracle: ScriptTestPostgresOracle{

						// TestPgHasRoleSupportsRoleNameArgumentsRepro reproduces a role-introspection
						// correctness bug: PostgreSQL supports pg_has_role role-name and regrole
						// argument forms, but Doltgres only registers the text/oid/text variant.
						ID: "role-membership-repro-test-testnoinheritroleprivilegesdonotapplyguard-0001-select-id-secret-from-noinherit_role_private_items", Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestPgHasRoleSupportsRoleNameArgumentsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_has_role supports role-name arguments",
			SetUpScript: []string{
				`CREATE ROLE has_role_catalog_group;`,
				`CREATE ROLE has_role_catalog_member;`,
				`GRANT has_role_catalog_group TO has_role_catalog_member;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT pg_has_role(
							'has_role_catalog_member',
							'has_role_catalog_group',
							'member'
						);`, PostgresOracle: ScriptTestPostgresOracle{ID: "role-membership-repro-test-testpghasrolesupportsrolenameargumentsrepro-0001-select-pg_has_role-has_role_catalog_member-has_role_catalog_group-member"},
				},
				{
					Query: `SELECT pg_has_role(
							'has_role_catalog_member'::regrole,
							'has_role_catalog_group'::regrole,
							'member'
						);`, PostgresOracle: ScriptTestPostgresOracle{ID: "role-membership-repro-test-testpghasrolesupportsrolenameargumentsrepro-0002-select-pg_has_role-has_role_catalog_member-::regrole-has_role_catalog_group"},
				},
				{
					Query: `SELECT pg_has_role('has_role_catalog_group', 'member');`, PostgresOracle: ScriptTestPostgresOracle{ID: "role-membership-repro-test-testpghasrolesupportsrolenameargumentsrepro-0003-select-pg_has_role-has_role_catalog_group-member"},
				},
			},
		},
	})
}

// TestPgHasRoleUsageHonorsIntermediateNoInheritRepro reproduces a
// role-introspection correctness bug: pg_has_role(..., 'usage') should not
// treat privileges as inheritable through a NOINHERIT intermediate role.
func TestPgHasRoleUsageHonorsIntermediateNoInheritRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_has_role usage honors intermediate NOINHERIT roles",
			SetUpScript: []string{
				`CREATE ROLE usage_chain_top;`,
				`CREATE ROLE usage_chain_middle NOINHERIT;`,
				`CREATE ROLE usage_chain_leaf;`,
				`GRANT usage_chain_top TO usage_chain_middle;`,
				`GRANT usage_chain_middle TO usage_chain_leaf;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT pg_has_role('usage_chain_leaf', oid, 'member')
						FROM pg_catalog.pg_roles
						WHERE rolname = 'usage_chain_top';`, PostgresOracle: ScriptTestPostgresOracle{ID: "role-membership-repro-test-testpghasroleusagehonorsintermediatenoinheritrepro-0001-select-pg_has_role-usage_chain_leaf-oid-member"},
				},
				{
					Query: `SELECT pg_has_role('usage_chain_leaf', oid, 'usage')
						FROM pg_catalog.pg_roles
						WHERE rolname = 'usage_chain_top';`, PostgresOracle: ScriptTestPostgresOracle{ID: "role-membership-repro-test-testpghasroleusagehonorsintermediatenoinheritrepro-0002-select-pg_has_role-usage_chain_leaf-oid-usage"},
				},
			},
		},
	})
}

// TestSetRoleUsesGrantedRolePrivilegesRepro reproduces a role correctness bug:
// PostgreSQL lets a session SET ROLE to a granted role and use that role's
// privileges.
func TestSetRoleUsesGrantedRolePrivilegesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "SET ROLE uses granted role privileges",
			SetUpScript: []string{
				`CREATE ROLE set_role_reader;`,
				`CREATE USER set_role_switcher PASSWORD 'pw' NOINHERIT;`,
				`CREATE TABLE set_role_private_items (
					id INT PRIMARY KEY,
					secret TEXT
				);`,
				`INSERT INTO set_role_private_items VALUES (1, 'visible through granted role');`,
				`GRANT USAGE ON SCHEMA public TO set_role_reader, set_role_switcher;`,
				`GRANT SELECT ON set_role_private_items TO set_role_reader;`,
				`GRANT set_role_reader TO set_role_switcher;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SET ROLE set_role_reader;`,
					Username: `set_role_switcher`,
					Password: `pw`,
				},
				{
					Query: `SELECT id, secret
						FROM set_role_private_items;`,

					Username: `set_role_switcher`,
					Password: `pw`, PostgresOracle: ScriptTestPostgresOracle{ID: "role-membership-repro-test-testsetroleusesgrantedroleprivilegesrepro-0001-select-id-secret-from-set_role_private_items", Compare: "sqlstate"},
				},
			},
		},
	})
}
