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

// TestRollbackRevertsCreatePolicyRepro reproduces a transaction consistency
// bug: CREATE POLICY mutates row-level security metadata outside the
// surrounding transaction and survives ROLLBACK.
func TestRollbackRevertsCreatePolicyRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK reverts CREATE POLICY",
			SetUpScript: []string{
				`CREATE USER rollback_policy_reader PASSWORD 'reader';`,
				`CREATE TABLE rollback_policy_docs (
					id INT PRIMARY KEY,
					owner_name TEXT,
					label TEXT
				);`,
				`INSERT INTO rollback_policy_docs VALUES
					(1, 'rollback_policy_reader', 'visible'),
					(2, 'other_user', 'hidden');`,
				`GRANT USAGE ON SCHEMA public TO rollback_policy_reader;`,
				`GRANT SELECT ON rollback_policy_docs TO rollback_policy_reader;`,
				`ALTER TABLE rollback_policy_docs ENABLE ROW LEVEL SECURITY;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `CREATE POLICY rollback_policy_docs_owner_select
						ON rollback_policy_docs
						FOR SELECT
						USING (owner_name = current_user);`,
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `SELECT id, label
						FROM rollback_policy_docs
						ORDER BY id;`,

					Username: `rollback_policy_reader`,
					Password: `reader`, PostgresOracle: ScriptTestPostgresOracle{ID: "row-level-security-policy-transaction-repro-test-testrollbackrevertscreatepolicyrepro-0001-select-id-label-from-rollback_policy_docs"},
				},
			},
		},
	})
}
