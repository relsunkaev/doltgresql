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

// TestDropPolicyRemovesRowSecurityPolicyRepro reproduces a security/correctness
// bug: DROP POLICY IF EXISTS is accepted but leaves the active policy in place.
func TestDropPolicyRemovesRowSecurityPolicyRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP POLICY removes active policy",
			SetUpScript: []string{
				`CREATE USER drop_policy_reader PASSWORD 'reader';`,
				`CREATE TABLE drop_policy_docs (
					id INT PRIMARY KEY,
					owner_name TEXT,
					label TEXT
				);`,
				`INSERT INTO drop_policy_docs VALUES
					(1, 'drop_policy_reader', 'visible'),
					(2, 'other_user', 'hidden');`,
				`GRANT USAGE ON SCHEMA public TO drop_policy_reader;`,
				`GRANT SELECT ON drop_policy_docs TO drop_policy_reader;`,
				`CREATE POLICY drop_policy_docs_owner_select
					ON drop_policy_docs
					FOR SELECT
					USING (owner_name = current_user);`,
				`ALTER TABLE drop_policy_docs ENABLE ROW LEVEL SECURITY;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, label
						FROM drop_policy_docs
						ORDER BY id;`,

					Username: `drop_policy_reader`,
					Password: `reader`, PostgresOracle: ScriptTestPostgresOracle{ID: "row-level-security-drop-policy-repro-test-testdroppolicyremovesrowsecuritypolicyrepro-0001-select-id-label-from-drop_policy_docs"},
				},
				{
					Query: `DROP POLICY IF EXISTS drop_policy_docs_owner_select
						ON drop_policy_docs;`,
				},
				{
					Query: `SELECT id, label
						FROM drop_policy_docs
						ORDER BY id;`,

					Username: `drop_policy_reader`,
					Password: `reader`, PostgresOracle: ScriptTestPostgresOracle{ID: "row-level-security-drop-policy-repro-test-testdroppolicyremovesrowsecuritypolicyrepro-0002-select-id-label-from-drop_policy_docs"},
				},
			},
		},
	})
}
