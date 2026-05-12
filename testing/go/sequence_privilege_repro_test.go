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

// TestSetvalRequiresUpdatePrivilegeRepro reproduces a sequence security bug:
// setval requires UPDATE on the sequence, but Doltgres allows it with only
// USAGE.
func TestSetvalRequiresUpdatePrivilegeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "setval requires sequence UPDATE privilege",
			SetUpScript: []string{
				`CREATE USER setval_sequence_user PASSWORD 'sequence';`,
				`CREATE SEQUENCE setval_private_seq;`,
				`GRANT USAGE ON SCHEMA public TO setval_sequence_user;`,
				`GRANT USAGE ON SEQUENCE setval_private_seq TO setval_sequence_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `SELECT setval('setval_private_seq', 100);`,
					ExpectedErr: `permission denied`,
					Username:    `setval_sequence_user`,
					Password:    `sequence`,
				},
			},
		},
	})
}

// TestSetvalWithSelectOnlyRequiresUpdatePrivilegeRepro guards that sequence
// SELECT alone is not enough to mutate sequence state with setval.
func TestSetvalWithSelectOnlyRequiresUpdatePrivilegeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "setval with sequence SELECT still requires UPDATE privilege",
			SetUpScript: []string{
				`CREATE USER setval_select_only_user PASSWORD 'sequence';`,
				`CREATE SEQUENCE setval_select_only_seq;`,
				`GRANT USAGE ON SCHEMA public TO setval_select_only_user;`,
				`GRANT SELECT ON SEQUENCE setval_select_only_seq TO setval_select_only_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `SELECT setval('setval_select_only_seq', 100);`,
					ExpectedErr: `permission denied`,
					Username:    `setval_select_only_user`,
					Password:    `sequence`,
				},
			},
		},
	})
}

// TestNextvalRequiresUsageOrUpdatePrivilegeRepro guards PostgreSQL sequence
// ACL semantics: SELECT lets a role read sequence state, but nextval requires
// USAGE or UPDATE because it advances the sequence.
func TestNextvalRequiresUsageOrUpdatePrivilegeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "nextval with sequence SELECT requires USAGE or UPDATE privilege",
			SetUpScript: []string{
				`CREATE USER nextval_select_only_user PASSWORD 'sequence';`,
				`CREATE SEQUENCE nextval_select_only_seq;`,
				`GRANT USAGE ON SCHEMA public TO nextval_select_only_user;`,
				`GRANT SELECT ON SEQUENCE nextval_select_only_seq TO nextval_select_only_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `SELECT nextval('nextval_select_only_seq');`,
					ExpectedErr: `permission denied`,
					Username:    `nextval_select_only_user`,
					Password:    `sequence`,
				},
			},
		},
	})
}

// TestNextvalAllowsUpdatePrivilegeRepro reproduces a sequence ACL correctness
// bug: UPDATE privilege can advance a sequence even without USAGE, but
// Doltgres denies the call.
func TestNextvalAllowsUpdatePrivilegeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "nextval allows sequence UPDATE privilege",
			SetUpScript: []string{
				`CREATE USER nextval_update_user PASSWORD 'sequence';`,
				`CREATE SEQUENCE nextval_update_seq;`,
				`GRANT USAGE ON SCHEMA public TO nextval_update_user;`,
				`GRANT UPDATE ON SEQUENCE nextval_update_seq TO nextval_update_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT nextval('nextval_update_seq');`,
					Expected: []sql.Row{{1}},
					Username: `nextval_update_user`,
					Password: `sequence`,
				},
			},
		},
	})
}

// TestSequenceRelationSelectRequiresSelectPrivilegeRepro guards sequence
// relation reads: a role with only schema USAGE cannot read sequence state.
func TestSequenceRelationSelectRequiresSelectPrivilegeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "sequence relation SELECT requires sequence SELECT privilege",
			SetUpScript: []string{
				`CREATE USER sequence_relation_reader PASSWORD 'reader';`,
				`CREATE SEQUENCE relation_private_seq;`,
				`SELECT nextval('relation_private_seq');`,
				`GRANT USAGE ON SCHEMA public TO sequence_relation_reader;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `SELECT last_value FROM relation_private_seq;`,
					ExpectedErr: `permission denied`,
					Username:    `sequence_relation_reader`,
					Password:    `reader`,
				},
			},
		},
	})
}

// TestSequenceRelationSelectAllowsSelectPrivilegeRepro reproduces a sequence
// ACL correctness bug: explicit sequence SELECT should allow reading the
// sequence relation, but Doltgres still denies the read.
func TestSequenceRelationSelectAllowsSelectPrivilegeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "sequence relation SELECT allows sequence SELECT privilege",
			SetUpScript: []string{
				`CREATE USER sequence_relation_select_user PASSWORD 'reader';`,
				`CREATE SEQUENCE relation_select_seq;`,
				`SELECT nextval('relation_select_seq');`,
				`GRANT USAGE ON SCHEMA public TO sequence_relation_select_user;`,
				`GRANT SELECT ON SEQUENCE relation_select_seq TO sequence_relation_select_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT last_value FROM relation_select_seq;`,
					Expected: []sql.Row{{1}},
					Username: `sequence_relation_select_user`,
					Password: `reader`,
				},
			},
		},
	})
}

// TestSequenceOwnerCanUseCreatedSequenceRepro reproduces a sequence privilege
// bug: a role that creates a sequence owns it and has implicit privileges to
// use it.
func TestSequenceOwnerCanUseCreatedSequenceRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "sequence owner can use created sequence",
			SetUpScript: []string{
				authTestCreateSuperUser,
				`CREATE USER sequence_owner_user PASSWORD 'sequence';`,
				`GRANT USAGE ON SCHEMA public TO sequence_owner_user;`,
				`GRANT CREATE ON SCHEMA public TO sequence_owner_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `CREATE SEQUENCE owner_created_seq;`,
					Expected: []sql.Row{},
					Username: `sequence_owner_user`,
					Password: `sequence`,
				},
				{
					Query:    `SELECT nextval('owner_created_seq');`,
					Expected: []sql.Row{{1}},
					Username: `sequence_owner_user`,
					Password: `sequence`,
				},
			},
		},
	})
}

// TestCurrvalAllowsSequenceUsagePrivilegeRepro reproduces a sequence privilege
// bug: currval should be callable by a role with USAGE on the target sequence
// after that same session has called nextval.
func TestCurrvalAllowsSequenceUsagePrivilegeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "currval allows sequence USAGE privilege",
			SetUpScript: []string{
				`CREATE USER currval_usage_user PASSWORD 'sequence';`,
				`CREATE SEQUENCE currval_usage_seq;`,
				`GRANT USAGE ON SCHEMA public TO currval_usage_user;`,
				`GRANT USAGE ON SEQUENCE currval_usage_seq TO currval_usage_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT nextval('currval_usage_seq');`,
					Expected: []sql.Row{{1}},
					Username: `currval_usage_user`,
					Password: `sequence`,
				},
				{
					Query:    `SELECT currval('currval_usage_seq');`,
					Expected: []sql.Row{{1}},
					Username: `currval_usage_user`,
					Password: `sequence`,
				},
			},
		},
	})
}
