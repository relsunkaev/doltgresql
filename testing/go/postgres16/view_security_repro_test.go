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

// TestSecurityInvokerViewRequiresBaseTablePrivilegesGuard covers a sensitive
// privilege boundary: a role must not read through a SECURITY INVOKER view
// without SELECT on the underlying table.
func TestSecurityInvokerViewRequiresBaseTablePrivilegesGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "security invoker view checks invoker permissions on base table",
			SetUpScript: []string{
				`CREATE USER view_reader PASSWORD 'reader';`,
				`CREATE TABLE private_base (id INT PRIMARY KEY, secret TEXT);`,
				`INSERT INTO private_base VALUES (1, 'alpha'), (2, 'beta');`,
				`CREATE VIEW invoker_view WITH (security_invoker = true) AS SELECT id, secret FROM private_base;`,
				`GRANT USAGE ON SCHEMA public TO view_reader;`,
				`GRANT SELECT ON invoker_view TO view_reader;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, secret FROM invoker_view ORDER BY id;`,

					Username: `view_reader`,
					Password: `reader`, PostgresOracle: ScriptTestPostgresOracle{

						// TestAlterViewSecurityInvokerRequiresBaseTablePrivilegesRepro verifies that
						// ALTER VIEW SET (security_invoker = true) changes the view privilege boundary
						// for future reads.
						ID: "view-security-repro-test-testsecurityinvokerviewrequiresbasetableprivilegesguard-0001-select-id-secret-from-invoker_view", Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestAlterViewSecurityInvokerRequiresBaseTablePrivilegesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER VIEW security_invoker checks invoker permissions",
			SetUpScript: []string{
				`CREATE USER alter_view_reader PASSWORD 'reader';`,
				`CREATE TABLE alter_invoker_private_base (
					id INT PRIMARY KEY,
					secret TEXT
				);`,
				`INSERT INTO alter_invoker_private_base VALUES (1, 'alpha'), (2, 'beta');`,
				`CREATE VIEW alter_invoker_view AS
					SELECT id, secret FROM alter_invoker_private_base;`,
				`GRANT USAGE ON SCHEMA public TO alter_view_reader;`,
				`GRANT SELECT ON alter_invoker_view TO alter_view_reader;`,
				`ALTER VIEW alter_invoker_view SET (security_invoker = true);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, secret FROM alter_invoker_view ORDER BY id;`,

					Username: `alter_view_reader`,
					Password: `reader`, PostgresOracle: ScriptTestPostgresOracle{

						// TestDefaultViewGrantUsesViewOwnerPrivilegesRepro reproduces a view privilege
						// bug: PostgreSQL's default security-definer view boundary lets a grantee read
						// through the view without direct SELECT on the base table.
						ID: "view-security-repro-test-testalterviewsecurityinvokerrequiresbasetableprivilegesrepro-0001-select-id-secret-from-alter_invoker_view", Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestDefaultViewGrantUsesViewOwnerPrivilegesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "default view grant uses view owner privileges",
			SetUpScript: []string{
				`CREATE USER default_view_reader PASSWORD 'reader';`,
				`CREATE TABLE default_view_grant_base (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO default_view_grant_base VALUES (1, 'visible');`,
				`CREATE VIEW default_view_grant_reader AS
					SELECT id, label FROM default_view_grant_base;`,
				`GRANT USAGE ON SCHEMA public TO default_view_reader;`,
				`GRANT SELECT ON default_view_grant_reader TO default_view_reader;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, label
						FROM default_view_grant_reader;`,

					Username: `default_view_reader`,
					Password: `reader`, PostgresOracle: ScriptTestPostgresOracle{

						// TestCreateOrReplaceViewPreservesGrantedPrivilegesGuard covers view ACL
						// persistence: PostgreSQL keeps existing view grants when CREATE OR REPLACE
						// VIEW updates a compatible view definition.
						ID: "view-security-repro-test-testdefaultviewgrantusesviewownerprivilegesrepro-0001-select-id-label-from-default_view_grant_reader"},
				},
			},
		},
	})
}

func TestCreateOrReplaceViewPreservesGrantedPrivilegesGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE OR REPLACE VIEW preserves granted privileges",
			SetUpScript: []string{
				`CREATE USER replace_view_acl_user PASSWORD 'reader';`,
				`CREATE TABLE replace_view_acl_base (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO replace_view_acl_base VALUES (1, 'old'), (2, 'new');`,
				`CREATE VIEW replace_view_acl_reader AS
					SELECT id, label FROM replace_view_acl_base WHERE id = 1;`,
				`GRANT USAGE ON SCHEMA public TO replace_view_acl_user;`,
				`GRANT SELECT ON replace_view_acl_base TO replace_view_acl_user;`,
				`GRANT SELECT ON replace_view_acl_reader TO replace_view_acl_user;`,
				`CREATE OR REPLACE VIEW replace_view_acl_reader AS
					SELECT id, label FROM replace_view_acl_base WHERE id = 2;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, label
						FROM replace_view_acl_reader;`,

					Username: `replace_view_acl_user`,
					Password: `reader`, PostgresOracle: ScriptTestPostgresOracle{ID: "view-security-repro-test-testcreateorreplaceviewpreservesgrantedprivilegesguard-0001-select-id-label-from-replace_view_acl_reader"},
				},
			},
		},
	})
}
