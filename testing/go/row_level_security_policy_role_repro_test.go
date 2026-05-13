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
				},
				{
					Query: `SELECT id, label
						FROM rls_policy_list_docs
						ORDER BY id;`,
					Expected: []sql.Row{},
					Username: `rls_policy_list_unlisted`,
					Password: `unlisted`,
				},
			},
		},
	})
}
