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

// TestRowLevelSecurityMultipleSelectPoliciesCombineRepro reproduces a data
// consistency bug: PostgreSQL combines multiple permissive SELECT policies with
// OR semantics, but Doltgres only applies the first matching policy.
func TestRowLevelSecurityMultipleSelectPoliciesCombineRepro(t *testing.T) {
	cleanup := []string{
		"RESET ROLE",
		"DROP TABLE IF EXISTS rls_multi_policy_docs",
		"DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rls_multi_policy_reader') THEN REVOKE USAGE ON SCHEMA public FROM rls_multi_policy_reader; END IF; END $$",
		"DROP ROLE IF EXISTS rls_multi_policy_reader",
	}
	RunScripts(t, []ScriptTest{
		{
			Name: "multiple permissive SELECT policies combine with OR",
			SetUpScript: []string{
				`CREATE USER rls_multi_policy_reader PASSWORD 'reader';`,
				`CREATE TABLE rls_multi_policy_docs (
					id INT PRIMARY KEY,
					owner_name TEXT,
					shared_with TEXT,
					label TEXT
				);`,
				`INSERT INTO rls_multi_policy_docs VALUES
					(1, 'rls_multi_policy_reader', 'nobody', 'owned'),
					(2, 'other_user', 'rls_multi_policy_reader', 'shared'),
					(3, 'other_user', 'nobody', 'hidden');`,
				`GRANT USAGE ON SCHEMA public TO rls_multi_policy_reader;`,
				`GRANT SELECT ON rls_multi_policy_docs TO rls_multi_policy_reader;`,
				`CREATE POLICY rls_multi_policy_docs_owner_select
					ON rls_multi_policy_docs
					FOR SELECT
					USING (owner_name = current_user);`,
				`CREATE POLICY rls_multi_policy_docs_shared_select
					ON rls_multi_policy_docs
					FOR SELECT
					USING (shared_with = current_user);`,
				`ALTER TABLE rls_multi_policy_docs ENABLE ROW LEVEL SECURITY;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, label
						FROM rls_multi_policy_docs
						ORDER BY id;`,
					Expected: []sql.Row{
						{1, "owned"},
						{2, "shared"},
					},
					Username: `rls_multi_policy_reader`,
					Password: `reader`,
					PostgresOracle: ScriptTestPostgresOracle{
						ID:          "rls-multiple-select-policies-combine",
						Compare:     "structural",
						ColumnModes: []string{"structural", "structural"},
						Cleanup:     cleanup,
					},
				},
			},
		},
	})
}

// TestRowLevelSecuritySelectPolicyUsingTrueRepro reproduces a data consistency
// bug: PostgreSQL treats USING (true) as an allow-all permissive policy, but
// Doltgres rewrites unsupported policy expressions as false.
func TestRowLevelSecuritySelectPolicyUsingTrueRepro(t *testing.T) {
	cleanup := []string{
		"RESET ROLE",
		"DROP TABLE IF EXISTS rls_true_policy_docs",
		"DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rls_true_policy_reader') THEN REVOKE USAGE ON SCHEMA public FROM rls_true_policy_reader; END IF; END $$",
		"DROP ROLE IF EXISTS rls_true_policy_reader",
	}
	RunScripts(t, []ScriptTest{
		{
			Name: "SELECT policy USING true allows all rows",
			SetUpScript: []string{
				`CREATE USER rls_true_policy_reader PASSWORD 'reader';`,
				`CREATE TABLE rls_true_policy_docs (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO rls_true_policy_docs VALUES
					(1, 'visible'),
					(2, 'also visible');`,
				`GRANT USAGE ON SCHEMA public TO rls_true_policy_reader;`,
				`GRANT SELECT ON rls_true_policy_docs TO rls_true_policy_reader;`,
				`CREATE POLICY rls_true_policy_docs_select
					ON rls_true_policy_docs
					FOR SELECT
					USING (true);`,
				`ALTER TABLE rls_true_policy_docs ENABLE ROW LEVEL SECURITY;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, label
						FROM rls_true_policy_docs
						ORDER BY id;`,
					Expected: []sql.Row{
						{1, "visible"},
						{2, "also visible"},
					},
					Username: `rls_true_policy_reader`,
					Password: `reader`,
					PostgresOracle: ScriptTestPostgresOracle{
						ID:          "rls-select-policy-using-true-allows-all",
						Compare:     "structural",
						ColumnModes: []string{"structural", "structural"},
						Cleanup:     cleanup,
					},
				},
			},
		},
	})
}

// TestRowLevelSecurityInsertPolicyWithCheckTrueRepro reproduces the same
// unsupported-expression data consistency bug for INSERT policies: PostgreSQL
// treats WITH CHECK (true) as allowing every inserted row.
func TestRowLevelSecurityInsertPolicyWithCheckTrueRepro(t *testing.T) {
	cleanup := []string{
		"RESET ROLE",
		"DROP TABLE IF EXISTS rls_true_insert_policy_docs",
		"DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rls_true_insert_policy_writer') THEN REVOKE USAGE ON SCHEMA public FROM rls_true_insert_policy_writer; END IF; END $$",
		"DROP ROLE IF EXISTS rls_true_insert_policy_writer",
	}
	RunScripts(t, []ScriptTest{
		{
			Name: "INSERT policy WITH CHECK true allows all rows",
			SetUpScript: []string{
				`CREATE USER rls_true_insert_policy_writer PASSWORD 'writer';`,
				`CREATE TABLE rls_true_insert_policy_docs (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`GRANT USAGE ON SCHEMA public TO rls_true_insert_policy_writer;`,
				`GRANT INSERT, SELECT ON rls_true_insert_policy_docs TO rls_true_insert_policy_writer;`,
				`CREATE POLICY rls_true_insert_policy_docs_insert
					ON rls_true_insert_policy_docs
					FOR INSERT
					WITH CHECK (true);`,
				`CREATE POLICY rls_true_insert_policy_docs_select
					ON rls_true_insert_policy_docs
					FOR SELECT
					USING (true);`,
				`ALTER TABLE rls_true_insert_policy_docs ENABLE ROW LEVEL SECURITY;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO rls_true_insert_policy_docs
						VALUES (1, 'inserted')
						RETURNING id, label;`,
					Expected: []sql.Row{{1, "inserted"}},
					Username: `rls_true_insert_policy_writer`,
					Password: `writer`,
					PostgresOracle: ScriptTestPostgresOracle{
						ID:          "rls-insert-policy-check-true-allows-all",
						Compare:     "structural",
						ColumnModes: []string{"structural", "structural"},
						Cleanup:     cleanup,
					},
				},
			},
		},
	})
}
