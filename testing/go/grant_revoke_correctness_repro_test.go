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

// TestRevokeGrantedByOnlyRemovesNamedGrantorRepro reproduces an ACL
// persistence bug: REVOKE ... GRANTED BY should remove only the ACL entry from
// the named grantor when another grantor also granted the same privilege.
func TestRevokeGrantedByOnlyRemovesNamedGrantorRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "REVOKE GRANTED BY keeps other grantors' privileges",
			SetUpScript: []string{
				`CREATE USER revoke_grantor_one PASSWORD 'one';`,
				`CREATE USER revoke_grantor_two PASSWORD 'two';`,
				`CREATE USER revoke_grantee PASSWORD 'grantee';`,
				`GRANT USAGE ON SCHEMA public TO revoke_grantor_one, revoke_grantor_two, revoke_grantee;`,
				`CREATE TABLE revoke_granted_by_items (
						id INT PRIMARY KEY
					);`,
				`INSERT INTO revoke_granted_by_items VALUES (7);`,
				`GRANT SELECT ON revoke_granted_by_items
						TO revoke_grantor_one WITH GRANT OPTION;`,
				`GRANT SELECT ON revoke_granted_by_items
						TO revoke_grantor_two WITH GRANT OPTION;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `GRANT SELECT ON revoke_granted_by_items
						TO revoke_grantee;`,
					Username: `revoke_grantor_one`,
					Password: `one`,
				},
				{
					Query: `GRANT SELECT ON revoke_granted_by_items
						TO revoke_grantee;`,
					Username: `revoke_grantor_two`,
					Password: `two`,
				},
				{
					Query: `REVOKE SELECT ON revoke_granted_by_items
						FROM revoke_grantee
						GRANTED BY revoke_grantor_one;`,
					Username: `revoke_grantor_one`,
					Password: `one`,
				},
				{
					Query: `SELECT id
						FROM revoke_granted_by_items;`,
					Expected: []sql.Row{{7}},
					Username: `revoke_grantee`,
					Password: `grantee`,
				},
			},
		},
	})
}

// TestRevokeGrantOptionForGrantedByOnlyRemovesNamedGrantorRepro reproduces an
// ACL persistence bug: REVOKE GRANT OPTION FOR ... GRANTED BY should remove
// only the grant option entry from the named grantor when another grantor also
// granted the same privilege with grant option.
func TestRevokeGrantOptionForGrantedByOnlyRemovesNamedGrantorRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "REVOKE GRANT OPTION GRANTED BY keeps other grantors' grant option",
			SetUpScript: []string{
				`CREATE USER revoke_option_grantor_one PASSWORD 'one';`,
				`CREATE USER revoke_option_grantor_two PASSWORD 'two';`,
				`CREATE USER revoke_option_grantee PASSWORD 'grantee';`,
				`CREATE USER revoke_option_delegate PASSWORD 'delegate';`,
				`GRANT USAGE ON SCHEMA public TO revoke_option_grantor_one, revoke_option_grantor_two, revoke_option_grantee, revoke_option_delegate;`,
				`CREATE TABLE revoke_option_items (
						id INT PRIMARY KEY
					);`,
				`INSERT INTO revoke_option_items VALUES (9);`,
				`GRANT SELECT ON revoke_option_items
						TO revoke_option_grantor_one WITH GRANT OPTION;`,
				`GRANT SELECT ON revoke_option_items
						TO revoke_option_grantor_two WITH GRANT OPTION;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `GRANT SELECT ON revoke_option_items
						TO revoke_option_grantee WITH GRANT OPTION;`,
					Username: `revoke_option_grantor_one`,
					Password: `one`,
				},
				{
					Query: `GRANT SELECT ON revoke_option_items
						TO revoke_option_grantee WITH GRANT OPTION;`,
					Username: `revoke_option_grantor_two`,
					Password: `two`,
				},
				{
					Query: `REVOKE GRANT OPTION FOR SELECT ON revoke_option_items
						FROM revoke_option_grantee
						GRANTED BY revoke_option_grantor_one;`,
					Username: `revoke_option_grantor_one`,
					Password: `one`,
				},
				{
					Query: `GRANT SELECT ON revoke_option_items
						TO revoke_option_delegate;`,
					Username: `revoke_option_grantee`,
					Password: `grantee`,
				},
				{
					Query: `SELECT id
						FROM revoke_option_items;`,
					Expected: []sql.Row{{9}},
					Username: `revoke_option_delegate`,
					Password: `delegate`,
				},
			},
		},
	})
}

// TestRevokeGrantedByRequiresCurrentUserRepro reproduces an ACL security bug:
// PostgreSQL requires the GRANTED BY role on REVOKE to be the current user, not
// merely a role inherited by the current user.
func TestRevokeGrantedByRequiresCurrentUserRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "REVOKE GRANTED BY requires current user",
			SetUpScript: []string{
				`CREATE USER revoke_by_group PASSWORD 'group';`,
				`CREATE USER revoke_by_member PASSWORD 'member';`,
				`CREATE USER revoke_by_grantee PASSWORD 'grantee';`,
				`CREATE TABLE revoke_by_group_items (
						id INT PRIMARY KEY
					);`,
				`INSERT INTO revoke_by_group_items VALUES (11);`,
				`GRANT USAGE ON SCHEMA public TO revoke_by_group, revoke_by_member, revoke_by_grantee;`,
				`GRANT SELECT ON revoke_by_group_items
						TO revoke_by_group WITH GRANT OPTION;`,
				`GRANT revoke_by_group TO revoke_by_member;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `GRANT SELECT ON revoke_by_group_items
						TO revoke_by_grantee;`,
					Username: `revoke_by_group`,
					Password: `group`,
				},
				{
					Query: `REVOKE SELECT ON revoke_by_group_items
						FROM revoke_by_grantee
						GRANTED BY revoke_by_group;`,

					Username: `revoke_by_member`,
					Password: `member`, PostgresOracle: ScriptTestPostgresOracle{ID: "grant-revoke-correctness-repro-test-testrevokegrantedbyrequirescurrentuserrepro-0001-revoke-select-on-revoke_by_group_items-from", Compare: "sqlstate"},
				},
				{
					Query: `SELECT id
						FROM revoke_by_group_items;`,

					Username: `revoke_by_grantee`,
					Password: `grantee`, PostgresOracle: ScriptTestPostgresOracle{

						// TestRevokeSchemaGrantedByOnlyRemovesNamedGrantorRepro reproduces an ACL
						// persistence bug: REVOKE ... GRANTED BY should remove only the schema ACL
						// entry from the named grantor when another grantor also granted the same
						// privilege.
						ID: "grant-revoke-correctness-repro-test-testrevokegrantedbyrequirescurrentuserrepro-0002-select-id-from-revoke_by_group_items"},
				},
			},
		},
	})
}

func TestRevokeSchemaGrantedByOnlyRemovesNamedGrantorRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "schema REVOKE GRANTED BY keeps other grantors' privileges",
			SetUpScript: []string{
				`CREATE USER revoke_schema_grantor_one PASSWORD 'one';`,
				`CREATE USER revoke_schema_grantor_two PASSWORD 'two';`,
				`CREATE USER revoke_schema_grantee PASSWORD 'grantee';`,
				`CREATE SCHEMA revoke_schema_target;`,
				`GRANT USAGE ON SCHEMA revoke_schema_target TO revoke_schema_grantee;`,
				`GRANT CREATE ON SCHEMA revoke_schema_target
						TO revoke_schema_grantor_one WITH GRANT OPTION;`,
				`GRANT CREATE ON SCHEMA revoke_schema_target
						TO revoke_schema_grantor_two WITH GRANT OPTION;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `GRANT CREATE ON SCHEMA revoke_schema_target
						TO revoke_schema_grantee;`,
					Username: `revoke_schema_grantor_one`,
					Password: `one`,
				},
				{
					Query: `GRANT CREATE ON SCHEMA revoke_schema_target
						TO revoke_schema_grantee;`,
					Username: `revoke_schema_grantor_two`,
					Password: `two`,
				},
				{
					Query: `REVOKE CREATE ON SCHEMA revoke_schema_target
						FROM revoke_schema_grantee
						GRANTED BY revoke_schema_grantor_one;`,
					Username: `revoke_schema_grantor_one`,
					Password: `one`,
				},
				{
					Query: `CREATE TABLE revoke_schema_target.grantee_created_items (
						id INT PRIMARY KEY
					);`,
					Username: `revoke_schema_grantee`,
					Password: `grantee`,
				},
			},
		},
	})
}

// TestRevokeSequenceGrantedByOnlyRemovesNamedGrantorRepro reproduces an ACL
// persistence bug: REVOKE ... GRANTED BY should remove only the sequence ACL
// entry from the named grantor when another grantor also granted the same
// privilege.
func TestRevokeSequenceGrantedByOnlyRemovesNamedGrantorRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "sequence REVOKE GRANTED BY keeps other grantors' privileges",
			SetUpScript: []string{
				`CREATE USER revoke_sequence_grantor_one PASSWORD 'one';`,
				`CREATE USER revoke_sequence_grantor_two PASSWORD 'two';`,
				`CREATE USER revoke_sequence_grantee PASSWORD 'grantee';`,
				`CREATE SEQUENCE revoke_sequence_target;`,
				`GRANT USAGE ON SCHEMA public TO revoke_sequence_grantor_one, revoke_sequence_grantor_two, revoke_sequence_grantee;`,
				`GRANT USAGE ON SEQUENCE revoke_sequence_target
						TO revoke_sequence_grantor_one WITH GRANT OPTION;`,
				`GRANT USAGE ON SEQUENCE revoke_sequence_target
						TO revoke_sequence_grantor_two WITH GRANT OPTION;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `GRANT USAGE ON SEQUENCE revoke_sequence_target
						TO revoke_sequence_grantee;`,
					Username: `revoke_sequence_grantor_one`,
					Password: `one`,
				},
				{
					Query: `GRANT USAGE ON SEQUENCE revoke_sequence_target
						TO revoke_sequence_grantee;`,
					Username: `revoke_sequence_grantor_two`,
					Password: `two`,
				},
				{
					Query: `REVOKE USAGE ON SEQUENCE revoke_sequence_target
						FROM revoke_sequence_grantee
						GRANTED BY revoke_sequence_grantor_one;`,
					Username: `revoke_sequence_grantor_one`,
					Password: `one`,
				},
				{
					Query:    `SELECT nextval('revoke_sequence_target');`,
					Expected: []sql.Row{{1}},
					Username: `revoke_sequence_grantee`,
					Password: `grantee`,
				},
			},
		},
	})
}

// TestRevokeRoutineGrantedByOnlyRemovesNamedGrantorRepro reproduces an ACL
// persistence bug: REVOKE ... GRANTED BY should remove only the routine ACL
// entry from the named grantor when another grantor also granted the same
// privilege.
func TestRevokeRoutineGrantedByOnlyRemovesNamedGrantorRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "routine REVOKE GRANTED BY keeps other grantors' privileges",
			SetUpScript: []string{
				`CREATE USER revoke_routine_grantor_one PASSWORD 'one';`,
				`CREATE USER revoke_routine_grantor_two PASSWORD 'two';`,
				`CREATE USER revoke_routine_grantee PASSWORD 'grantee';`,
				`CREATE FUNCTION revoke_routine_target(input INT) RETURNS INT
					LANGUAGE SQL
					AS $$ SELECT input + 1 $$;`,
				`REVOKE EXECUTE ON FUNCTION revoke_routine_target(INT) FROM PUBLIC;`,
				`GRANT USAGE ON SCHEMA public TO revoke_routine_grantor_one, revoke_routine_grantor_two, revoke_routine_grantee;`,
				`GRANT EXECUTE ON FUNCTION revoke_routine_target(INT)
						TO revoke_routine_grantor_one WITH GRANT OPTION;`,
				`GRANT EXECUTE ON FUNCTION revoke_routine_target(INT)
						TO revoke_routine_grantor_two WITH GRANT OPTION;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `GRANT EXECUTE ON FUNCTION revoke_routine_target(INT)
						TO revoke_routine_grantee;`,
					Username: `revoke_routine_grantor_one`,
					Password: `one`,
				},
				{
					Query: `GRANT EXECUTE ON FUNCTION revoke_routine_target(INT)
						TO revoke_routine_grantee;`,
					Username: `revoke_routine_grantor_two`,
					Password: `two`,
				},
				{
					Query: `REVOKE EXECUTE ON FUNCTION revoke_routine_target(INT)
						FROM revoke_routine_grantee
						GRANTED BY revoke_routine_grantor_one;`,
					Username: `revoke_routine_grantor_one`,
					Password: `one`,
				},
				{
					Query:    `SELECT revoke_routine_target(4);`,
					Expected: []sql.Row{{5}},
					Username: `revoke_routine_grantee`,
					Password: `grantee`,
				},
			},
		},
	})
}

// TestRevokeGrantOptionCascadeRemovesDependentTableGrantRepro reproduces an
// ACL persistence bug: REVOKE GRANT OPTION ... CASCADE should remove dependent
// grants made through the revoked grant option.
func TestRevokeGrantOptionCascadeRemovesDependentTableGrantRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "REVOKE GRANT OPTION CASCADE removes dependent table grant",
			SetUpScript: []string{
				`CREATE USER cascade_grantor PASSWORD 'grantor';`,
				`CREATE USER cascade_middle PASSWORD 'middle';`,
				`CREATE USER cascade_leaf PASSWORD 'leaf';`,
				`CREATE TABLE cascade_items (
						id INT PRIMARY KEY
					);`,
				`INSERT INTO cascade_items VALUES (13);`,
				`GRANT USAGE ON SCHEMA public TO cascade_grantor, cascade_middle, cascade_leaf;`,
				`GRANT SELECT ON cascade_items
						TO cascade_grantor WITH GRANT OPTION;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `GRANT SELECT ON cascade_items
						TO cascade_middle WITH GRANT OPTION;`,
					Username: `cascade_grantor`,
					Password: `grantor`,
				},
				{
					Query: `GRANT SELECT ON cascade_items
						TO cascade_leaf;`,
					Username: `cascade_middle`,
					Password: `middle`,
				},
				{
					Query: `REVOKE GRANT OPTION FOR SELECT ON cascade_items
						FROM cascade_middle CASCADE;`,
					Username: `cascade_grantor`,
					Password: `grantor`,
				},
				{
					Query: `SELECT id FROM cascade_items;`,

					Username: `cascade_leaf`,
					Password: `leaf`, PostgresOracle: ScriptTestPostgresOracle{

						// TestRevokeCascadeRemovesDependentTableGrantRepro reproduces an ACL
						// persistence bug: REVOKE ... CASCADE should remove dependent grants made
						// through the revoked privilege.
						ID: "grant-revoke-correctness-repro-test-testrevokegrantoptioncascaderemovesdependenttablegrantrepro-0001-select-id-from-cascade_items"},
				},
			},
		},
	})
}

func TestRevokeCascadeRemovesDependentTableGrantRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "REVOKE CASCADE removes dependent table grant",
			SetUpScript: []string{
				`CREATE USER full_cascade_grantor PASSWORD 'grantor';`,
				`CREATE USER full_cascade_middle PASSWORD 'middle';`,
				`CREATE USER full_cascade_leaf PASSWORD 'leaf';`,
				`CREATE TABLE full_cascade_items (
						id INT PRIMARY KEY
					);`,
				`INSERT INTO full_cascade_items VALUES (31);`,
				`GRANT USAGE ON SCHEMA public TO full_cascade_grantor, full_cascade_middle, full_cascade_leaf;`,
				`GRANT SELECT ON full_cascade_items
						TO full_cascade_grantor WITH GRANT OPTION;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `GRANT SELECT ON full_cascade_items
						TO full_cascade_middle WITH GRANT OPTION;`,
					Username: `full_cascade_grantor`,
					Password: `grantor`,
				},
				{
					Query: `GRANT SELECT ON full_cascade_items
						TO full_cascade_leaf;`,
					Username: `full_cascade_middle`,
					Password: `middle`,
				},
				{
					Query: `REVOKE SELECT ON full_cascade_items
						FROM full_cascade_middle CASCADE;`,
					Username: `full_cascade_grantor`,
					Password: `grantor`,
				},
				{
					Query: `SELECT id FROM full_cascade_items;`,

					Username: `full_cascade_middle`,
					Password: `middle`, PostgresOracle: ScriptTestPostgresOracle{ID: "grant-revoke-correctness-repro-test-testrevokecascaderemovesdependenttablegrantrepro-0001-select-id-from-full_cascade_items", Compare: "sqlstate"},
				},
				{
					Query: `SELECT id FROM full_cascade_items;`,

					Username: `full_cascade_leaf`,
					Password: `leaf`, PostgresOracle: ScriptTestPostgresOracle{

						// TestRevokeRestrictRejectsDependentTableGrantRepro reproduces an ACL
						// dependency bug: REVOKE ... RESTRICT should reject revoking a privilege that
						// has dependent grants.
						ID: "grant-revoke-correctness-repro-test-testrevokecascaderemovesdependenttablegrantrepro-0002-select-id-from-full_cascade_items"},
				},
			},
		},
	})
}

func TestRevokeRestrictRejectsDependentTableGrantRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "REVOKE RESTRICT rejects dependent table grant",
			SetUpScript: []string{
				`CREATE USER restrict_grantor PASSWORD 'grantor';`,
				`CREATE USER restrict_middle PASSWORD 'middle';`,
				`CREATE USER restrict_leaf PASSWORD 'leaf';`,
				`CREATE TABLE restrict_items (
						id INT PRIMARY KEY
					);`,
				`INSERT INTO restrict_items VALUES (17);`,
				`GRANT USAGE ON SCHEMA public TO restrict_grantor, restrict_middle, restrict_leaf;`,
				`GRANT SELECT ON restrict_items
						TO restrict_grantor WITH GRANT OPTION;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `GRANT SELECT ON restrict_items
						TO restrict_middle WITH GRANT OPTION;`,
					Username: `restrict_grantor`,
					Password: `grantor`,
				},
				{
					Query: `GRANT SELECT ON restrict_items
						TO restrict_leaf;`,
					Username: `restrict_middle`,
					Password: `middle`,
				},
				{
					Query: `REVOKE SELECT ON restrict_items
						FROM restrict_middle RESTRICT;`,

					Username: `restrict_grantor`,
					Password: `grantor`, PostgresOracle: ScriptTestPostgresOracle{ID: "grant-revoke-correctness-repro-test-testrevokerestrictrejectsdependenttablegrantrepro-0001-revoke-select-on-restrict_items-from"},
				},
				{
					Query: `SELECT id FROM restrict_items;`,

					Username: `restrict_middle`,
					Password: `middle`, PostgresOracle: ScriptTestPostgresOracle{ID: "grant-revoke-correctness-repro-test-testrevokerestrictrejectsdependenttablegrantrepro-0002-select-id-from-restrict_items"},
				},
				{
					Query: `SELECT id FROM restrict_items;`,

					Username: `restrict_leaf`,
					Password: `leaf`, PostgresOracle: ScriptTestPostgresOracle{

						// TestRevokeGrantOptionRestrictRejectsDependentTableGrantRepro reproduces an
						// ACL dependency bug: REVOKE GRANT OPTION ... RESTRICT should reject revoking a
						// grant option that has dependent grants.
						ID: "grant-revoke-correctness-repro-test-testrevokerestrictrejectsdependenttablegrantrepro-0003-select-id-from-restrict_items"},
				},
			},
		},
	})
}

func TestRevokeGrantOptionRestrictRejectsDependentTableGrantRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "REVOKE GRANT OPTION RESTRICT rejects dependent table grant",
			SetUpScript: []string{
				`CREATE USER restrict_option_grantor PASSWORD 'grantor';`,
				`CREATE USER restrict_option_middle PASSWORD 'middle';`,
				`CREATE USER restrict_option_leaf PASSWORD 'leaf';`,
				`CREATE USER restrict_option_delegate PASSWORD 'delegate';`,
				`CREATE TABLE restrict_option_items (
						id INT PRIMARY KEY
					);`,
				`INSERT INTO restrict_option_items VALUES (19);`,
				`GRANT USAGE ON SCHEMA public TO restrict_option_grantor, restrict_option_middle, restrict_option_leaf, restrict_option_delegate;`,
				`GRANT SELECT ON restrict_option_items
						TO restrict_option_grantor WITH GRANT OPTION;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `GRANT SELECT ON restrict_option_items
						TO restrict_option_middle WITH GRANT OPTION;`,
					Username: `restrict_option_grantor`,
					Password: `grantor`,
				},
				{
					Query: `GRANT SELECT ON restrict_option_items
						TO restrict_option_leaf;`,
					Username: `restrict_option_middle`,
					Password: `middle`,
				},
				{
					Query: `REVOKE GRANT OPTION FOR SELECT ON restrict_option_items
						FROM restrict_option_middle RESTRICT;`,

					Username: `restrict_option_grantor`,
					Password: `grantor`, PostgresOracle: ScriptTestPostgresOracle{ID: "grant-revoke-correctness-repro-test-testrevokegrantoptionrestrictrejectsdependenttablegrantrepro-0001-revoke-grant-option-for-select"},
				},
				{
					Query: `GRANT SELECT ON restrict_option_items
						TO restrict_option_delegate;`,
					Username: `restrict_option_middle`,
					Password: `middle`,
				},
				{
					Query: `SELECT id FROM restrict_option_items;`,

					Username: `restrict_option_delegate`,
					Password: `delegate`, PostgresOracle: ScriptTestPostgresOracle{ID: "grant-revoke-correctness-repro-test-testrevokegrantoptionrestrictrejectsdependenttablegrantrepro-0002-select-id-from-restrict_option_items"},
				},
			},
		},
	})
}
