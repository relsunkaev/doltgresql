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

// TestCreatePolicyRejectsDuplicatePolicyNameRepro reproduces a PostgreSQL
// compatibility bug: CREATE POLICY must reject a duplicate policy name for the
// same table, but Doltgres replaces the existing policy in memory.
func TestCreatePolicyRejectsDuplicatePolicyNameRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE POLICY rejects duplicate policy name",
			SetUpScript: []string{
				`CREATE TABLE rls_duplicate_policy_docs (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`CREATE POLICY rls_duplicate_policy_docs_select
					ON rls_duplicate_policy_docs
					FOR SELECT
					USING (true);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE POLICY rls_duplicate_policy_docs_select
						ON rls_duplicate_policy_docs
						FOR SELECT
						USING (false);`, PostgresOracle: ScriptTestPostgresOracle{ID: "rls-create-policy-rejects-duplicate-name", Compare: "sqlstate", Cleanup: []string{"DROP TABLE IF EXISTS rls_duplicate_policy_docs"}},
				},
			},
		},
	})
}
