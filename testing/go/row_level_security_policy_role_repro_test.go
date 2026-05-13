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

// TestRowLevelSecurityPolicyRoleListRestrictsPolicyRepro reproduces a security
// bug: CREATE POLICY records the predicate but ignores the TO role list, so an
// unlisted role can use a policy that PostgreSQL does not apply to that role.
func TestRowLevelSecurityPolicyRoleListRestrictsPolicyRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "RLS policy role list restricts policy applicability",
			SetUpScript: []string{
				`CREATE USER rls_policy_list_allowed PASSWORD 'allowed';`,
				`CREATE USER rls_policy_list_unlisted PASSWORD 'unlisted';`,
				`CREATE TABLE rls_policy_list_docs (
					id INT PRIMARY KEY,
					owner_name TEXT,
					label TEXT
				);`,
				`INSERT INTO rls_policy_list_docs VALUES
					(1, 'rls_policy_list_allowed', 'allowed row'),
					(2, 'rls_policy_list_unlisted', 'unlisted row');`,
				`GRANT USAGE ON SCHEMA public TO rls_policy_list_allowed;`,
				`GRANT USAGE ON SCHEMA public TO rls_policy_list_unlisted;`,
				`GRANT SELECT ON rls_policy_list_docs
					TO rls_policy_list_allowed, rls_policy_list_unlisted;`,
				`CREATE POLICY rls_policy_list_docs_owner_select
					ON rls_policy_list_docs
					FOR SELECT
					TO rls_policy_list_allowed
					USING (owner_name = current_user);`,
				`ALTER TABLE rls_policy_list_docs ENABLE ROW LEVEL SECURITY;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, label
						FROM rls_policy_list_docs
						ORDER BY id;`,
					Expected: []sql.Row{{1, "allowed row"}},
					Username: `rls_policy_list_allowed`,
					Password: `allowed`,
					PostgresOracle: ScriptTestPostgresOracle{
						ID:          "rls-policy-role-list-allows-listed-role",
						Compare:     "structural",
						ColumnModes: []string{"structural", "structural"},
						Cleanup: []string{
							"RESET ROLE",
							"DROP TABLE IF EXISTS rls_policy_list_docs",
							"DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rls_policy_list_allowed') THEN REVOKE USAGE ON SCHEMA public FROM rls_policy_list_allowed; END IF; IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rls_policy_list_unlisted') THEN REVOKE USAGE ON SCHEMA public FROM rls_policy_list_unlisted; END IF; END $$",
							"DROP ROLE IF EXISTS rls_policy_list_allowed",
							"DROP ROLE IF EXISTS rls_policy_list_unlisted",
						},
					},
				},
				{
					Query: `SELECT id, label
						FROM rls_policy_list_docs
						ORDER BY id;`,
					Expected: []sql.Row{},
					Username: `rls_policy_list_unlisted`,
					Password: `unlisted`,
					PostgresOracle: ScriptTestPostgresOracle{
						ID:          "rls-policy-role-list-denies-unlisted-role",
						Compare:     "structural",
						ColumnModes: []string{"structural", "structural"},
						Cleanup: []string{
							"RESET ROLE",
							"DROP TABLE IF EXISTS rls_policy_list_docs",
							"DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rls_policy_list_allowed') THEN REVOKE USAGE ON SCHEMA public FROM rls_policy_list_allowed; END IF; IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rls_policy_list_unlisted') THEN REVOKE USAGE ON SCHEMA public FROM rls_policy_list_unlisted; END IF; END $$",
							"DROP ROLE IF EXISTS rls_policy_list_allowed",
							"DROP ROLE IF EXISTS rls_policy_list_unlisted",
						},
					},
				},
			},
		},
	})
}

// TestRowLevelSecurityPolicyRoleListRestrictsInsertPolicyRepro reproduces the
// same role-list applicability bug for write policies: an unlisted role can use
// an INSERT policy that PostgreSQL only applies to the listed role.
func TestRowLevelSecurityPolicyRoleListRestrictsInsertPolicyRepro(t *testing.T) {
	cleanup := []string{
		"RESET ROLE",
		"DROP TABLE IF EXISTS rls_insert_policy_list_docs",
		"DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rls_insert_policy_list_allowed') THEN REVOKE USAGE ON SCHEMA public FROM rls_insert_policy_list_allowed; END IF; IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rls_insert_policy_list_unlisted') THEN REVOKE USAGE ON SCHEMA public FROM rls_insert_policy_list_unlisted; END IF; END $$",
		"DROP ROLE IF EXISTS rls_insert_policy_list_allowed",
		"DROP ROLE IF EXISTS rls_insert_policy_list_unlisted",
	}
	RunScripts(t, []ScriptTest{
		{
			Name: "RLS policy role list restricts insert policy applicability",
			SetUpScript: []string{
				`CREATE USER rls_insert_policy_list_allowed PASSWORD 'allowed';`,
				`CREATE USER rls_insert_policy_list_unlisted PASSWORD 'unlisted';`,
				`CREATE TABLE rls_insert_policy_list_docs (
					id INT PRIMARY KEY,
					owner_name TEXT,
					label TEXT
				);`,
				`GRANT USAGE ON SCHEMA public TO rls_insert_policy_list_allowed;`,
				`GRANT USAGE ON SCHEMA public TO rls_insert_policy_list_unlisted;`,
				`GRANT INSERT, SELECT ON rls_insert_policy_list_docs
					TO rls_insert_policy_list_allowed, rls_insert_policy_list_unlisted;`,
				`CREATE POLICY rls_insert_policy_list_docs_owner_insert
					ON rls_insert_policy_list_docs
					FOR INSERT
					TO rls_insert_policy_list_allowed
					WITH CHECK (owner_name = current_user);`,
				`CREATE POLICY rls_insert_policy_list_docs_owner_select
					ON rls_insert_policy_list_docs
					FOR SELECT
					TO rls_insert_policy_list_allowed
					USING (owner_name = current_user);`,
				`ALTER TABLE rls_insert_policy_list_docs ENABLE ROW LEVEL SECURITY;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO rls_insert_policy_list_docs
						VALUES (1, 'rls_insert_policy_list_allowed', 'allowed insert')
						RETURNING id, label;`,
					Expected: []sql.Row{{1, "allowed insert"}},
					Username: `rls_insert_policy_list_allowed`,
					Password: `allowed`,
					PostgresOracle: ScriptTestPostgresOracle{
						ID:          "rls-policy-role-list-allows-listed-insert",
						Compare:     "structural",
						ColumnModes: []string{"structural", "structural"},
						Cleanup:     cleanup,
					},
				},
				{
					Query: `INSERT INTO rls_insert_policy_list_docs
						VALUES (2, 'rls_insert_policy_list_unlisted', 'unlisted insert')
						RETURNING id, label;`,
					ExpectedErr: `violates row-level security policy`,
					Username:    `rls_insert_policy_list_unlisted`,
					Password:    `unlisted`,
					PostgresOracle: ScriptTestPostgresOracle{
						ID:                    "rls-policy-role-list-denies-unlisted-insert",
						Compare:               "sqlstate",
						ExpectedSQLState:      "42501",
						ExpectedErrorSeverity: "ERROR",
						Cleanup:               cleanup,
					},
				},
			},
		},
	})
}

// TestRowLevelSecurityPolicyRoleListRestrictsUpdatePolicyRepro reproduces the
// role-list applicability bug for UPDATE policies: PostgreSQL applies the
// policy only to the listed role, but Doltgres lets an unlisted role update rows.
func TestRowLevelSecurityPolicyRoleListRestrictsUpdatePolicyRepro(t *testing.T) {
	cleanup := []string{
		"RESET ROLE",
		"DROP TABLE IF EXISTS rls_update_policy_list_docs",
		"DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rls_update_policy_list_allowed') THEN REVOKE USAGE ON SCHEMA public FROM rls_update_policy_list_allowed; END IF; IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rls_update_policy_list_unlisted') THEN REVOKE USAGE ON SCHEMA public FROM rls_update_policy_list_unlisted; END IF; END $$",
		"DROP ROLE IF EXISTS rls_update_policy_list_allowed",
		"DROP ROLE IF EXISTS rls_update_policy_list_unlisted",
	}
	RunScripts(t, []ScriptTest{
		{
			Name: "RLS policy role list restricts update policy applicability",
			SetUpScript: []string{
				`CREATE USER rls_update_policy_list_allowed PASSWORD 'allowed';`,
				`CREATE USER rls_update_policy_list_unlisted PASSWORD 'unlisted';`,
				`CREATE TABLE rls_update_policy_list_docs (
					id INT PRIMARY KEY,
					owner_name TEXT,
					label TEXT
				);`,
				`INSERT INTO rls_update_policy_list_docs VALUES
					(1, 'rls_update_policy_list_allowed', 'allowed row'),
					(2, 'rls_update_policy_list_unlisted', 'unlisted row');`,
				`GRANT USAGE ON SCHEMA public TO rls_update_policy_list_allowed;`,
				`GRANT USAGE ON SCHEMA public TO rls_update_policy_list_unlisted;`,
				`GRANT UPDATE, SELECT ON rls_update_policy_list_docs
					TO rls_update_policy_list_allowed, rls_update_policy_list_unlisted;`,
				`CREATE POLICY rls_update_policy_list_docs_owner_update
					ON rls_update_policy_list_docs
					FOR UPDATE
					TO rls_update_policy_list_allowed
					USING (owner_name = current_user)
					WITH CHECK (owner_name = current_user);`,
				`CREATE POLICY rls_update_policy_list_docs_owner_select
					ON rls_update_policy_list_docs
					FOR SELECT
					TO rls_update_policy_list_allowed
					USING (owner_name = current_user);`,
				`ALTER TABLE rls_update_policy_list_docs ENABLE ROW LEVEL SECURITY;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE rls_update_policy_list_docs
						SET label = 'allowed updated'
						WHERE owner_name = current_user
						RETURNING id, label;`,
					Expected: []sql.Row{{1, "allowed updated"}},
					Username: `rls_update_policy_list_allowed`,
					Password: `allowed`,
					PostgresOracle: ScriptTestPostgresOracle{
						ID:          "rls-policy-role-list-allows-listed-update",
						Compare:     "structural",
						ColumnModes: []string{"structural", "structural"},
						Cleanup:     cleanup,
					},
				},
				{
					Query: `UPDATE rls_update_policy_list_docs
						SET label = 'unlisted updated'
						WHERE owner_name = current_user
						RETURNING id, label;`,
					Expected: []sql.Row{},
					Username: `rls_update_policy_list_unlisted`,
					Password: `unlisted`,
					PostgresOracle: ScriptTestPostgresOracle{
						ID:          "rls-policy-role-list-denies-unlisted-update",
						Compare:     "structural",
						ColumnModes: []string{"structural", "structural"},
						Cleanup:     cleanup,
					},
				},
			},
		},
	})
}

// TestRowLevelSecurityPolicyRoleListRestrictsDeletePolicyRepro reproduces the
// role-list applicability bug for DELETE policies: PostgreSQL applies the
// policy only to the listed role, but Doltgres lets an unlisted role delete rows.
func TestRowLevelSecurityPolicyRoleListRestrictsDeletePolicyRepro(t *testing.T) {
	cleanup := []string{
		"RESET ROLE",
		"DROP TABLE IF EXISTS rls_delete_policy_list_docs",
		"DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rls_delete_policy_list_allowed') THEN REVOKE USAGE ON SCHEMA public FROM rls_delete_policy_list_allowed; END IF; IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rls_delete_policy_list_unlisted') THEN REVOKE USAGE ON SCHEMA public FROM rls_delete_policy_list_unlisted; END IF; END $$",
		"DROP ROLE IF EXISTS rls_delete_policy_list_allowed",
		"DROP ROLE IF EXISTS rls_delete_policy_list_unlisted",
	}
	RunScripts(t, []ScriptTest{
		{
			Name: "RLS policy role list restricts delete policy applicability",
			SetUpScript: []string{
				`CREATE USER rls_delete_policy_list_allowed PASSWORD 'allowed';`,
				`CREATE USER rls_delete_policy_list_unlisted PASSWORD 'unlisted';`,
				`CREATE TABLE rls_delete_policy_list_docs (
					id INT PRIMARY KEY,
					owner_name TEXT,
					label TEXT
				);`,
				`INSERT INTO rls_delete_policy_list_docs VALUES
					(1, 'rls_delete_policy_list_allowed', 'allowed row'),
					(2, 'rls_delete_policy_list_unlisted', 'unlisted row');`,
				`GRANT USAGE ON SCHEMA public TO rls_delete_policy_list_allowed;`,
				`GRANT USAGE ON SCHEMA public TO rls_delete_policy_list_unlisted;`,
				`GRANT DELETE, SELECT ON rls_delete_policy_list_docs
					TO rls_delete_policy_list_allowed, rls_delete_policy_list_unlisted;`,
				`CREATE POLICY rls_delete_policy_list_docs_owner_delete
					ON rls_delete_policy_list_docs
					FOR DELETE
					TO rls_delete_policy_list_allowed
					USING (owner_name = current_user);`,
				`CREATE POLICY rls_delete_policy_list_docs_owner_select
					ON rls_delete_policy_list_docs
					FOR SELECT
					TO rls_delete_policy_list_allowed
					USING (owner_name = current_user);`,
				`ALTER TABLE rls_delete_policy_list_docs ENABLE ROW LEVEL SECURITY;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DELETE FROM rls_delete_policy_list_docs
						WHERE owner_name = current_user
						RETURNING id, label;`,
					Expected: []sql.Row{{1, "allowed row"}},
					Username: `rls_delete_policy_list_allowed`,
					Password: `allowed`,
					PostgresOracle: ScriptTestPostgresOracle{
						ID:          "rls-policy-role-list-allows-listed-delete",
						Compare:     "structural",
						ColumnModes: []string{"structural", "structural"},
						Cleanup:     cleanup,
					},
				},
				{
					Query: `DELETE FROM rls_delete_policy_list_docs
						WHERE owner_name = current_user
						RETURNING id, label;`,
					Expected: []sql.Row{},
					Username: `rls_delete_policy_list_unlisted`,
					Password: `unlisted`,
					PostgresOracle: ScriptTestPostgresOracle{
						ID:          "rls-policy-role-list-denies-unlisted-delete",
						Compare:     "structural",
						ColumnModes: []string{"structural", "structural"},
						Cleanup:     cleanup,
					},
				},
			},
		},
	})
}
