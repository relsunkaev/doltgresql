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
				},
			},
		},
	})
}

// TestRowLevelSecuritySelectPolicyUsingTrueRepro reproduces a data consistency
// bug: PostgreSQL treats USING (true) as an allow-all permissive policy, but
// Doltgres rewrites unsupported policy expressions as false.
func TestRowLevelSecuritySelectPolicyUsingTrueRepro(t *testing.T) {
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
				},
			},
		},
	})
}

// TestRowLevelSecurityInsertPolicyWithCheckTrueRepro reproduces the same
// unsupported-expression data consistency bug for INSERT policies: PostgreSQL
// treats WITH CHECK (true) as allowing every inserted row.
func TestRowLevelSecurityInsertPolicyWithCheckTrueRepro(t *testing.T) {
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
				},
			},
		},
	})
}

// TestRowLevelSecurityUpdatePolicyUsingTrueRepro reproduces the unsupported
// true-expression bug for UPDATE policies: PostgreSQL allows the update, while
// Doltgres filters every row out.
func TestRowLevelSecurityUpdatePolicyUsingTrueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "UPDATE policy USING true allows all rows",
			SetUpScript: []string{
				`CREATE USER rls_true_update_policy_writer PASSWORD 'writer';`,
				`CREATE TABLE rls_true_update_policy_docs (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO rls_true_update_policy_docs VALUES (1, 'original');`,
				`GRANT USAGE ON SCHEMA public TO rls_true_update_policy_writer;`,
				`GRANT SELECT, UPDATE ON rls_true_update_policy_docs TO rls_true_update_policy_writer;`,
				`CREATE POLICY rls_true_update_policy_docs_update
					ON rls_true_update_policy_docs
					FOR UPDATE
					USING (true)
					WITH CHECK (true);`,
				`CREATE POLICY rls_true_update_policy_docs_select
					ON rls_true_update_policy_docs
					FOR SELECT
					USING (true);`,
				`ALTER TABLE rls_true_update_policy_docs ENABLE ROW LEVEL SECURITY;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE rls_true_update_policy_docs
						SET label = 'updated'
						WHERE id = 1
						RETURNING id, label;`,
					Expected: []sql.Row{{1, "updated"}},
					Username: `rls_true_update_policy_writer`,
					Password: `writer`,
				},
			},
		},
	})
}

// TestRowLevelSecurityDeletePolicyUsingTrueRepro reproduces the unsupported
// true-expression bug for DELETE policies: PostgreSQL allows the delete, while
// Doltgres filters every row out.
func TestRowLevelSecurityDeletePolicyUsingTrueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DELETE policy USING true allows all rows",
			SetUpScript: []string{
				`CREATE USER rls_true_delete_policy_writer PASSWORD 'writer';`,
				`CREATE TABLE rls_true_delete_policy_docs (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO rls_true_delete_policy_docs VALUES (1, 'delete me');`,
				`GRANT USAGE ON SCHEMA public TO rls_true_delete_policy_writer;`,
				`GRANT SELECT, DELETE ON rls_true_delete_policy_docs TO rls_true_delete_policy_writer;`,
				`CREATE POLICY rls_true_delete_policy_docs_delete
					ON rls_true_delete_policy_docs
					FOR DELETE
					USING (true);`,
				`CREATE POLICY rls_true_delete_policy_docs_select
					ON rls_true_delete_policy_docs
					FOR SELECT
					USING (true);`,
				`ALTER TABLE rls_true_delete_policy_docs ENABLE ROW LEVEL SECURITY;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DELETE FROM rls_true_delete_policy_docs
						WHERE id = 1
						RETURNING id, label;`,
					Expected: []sql.Row{{1, "delete me"}},
					Username: `rls_true_delete_policy_writer`,
					Password: `writer`,
				},
			},
		},
	})
}

// TestRowLevelSecuritySelectPolicyReversedCurrentUserRepro reproduces an RLS
// expression-parsing bug: PostgreSQL treats current_user = owner_name the same
// as owner_name = current_user, but Doltgres only recognizes the latter form.
func TestRowLevelSecuritySelectPolicyReversedCurrentUserRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "SELECT policy current_user equals owner column allows matching rows",
			SetUpScript: []string{
				`CREATE USER rls_reversed_policy_reader PASSWORD 'reader';`,
				`CREATE TABLE rls_reversed_policy_docs (
					id INT PRIMARY KEY,
					owner_name TEXT,
					label TEXT
				);`,
				`INSERT INTO rls_reversed_policy_docs VALUES
					(1, 'rls_reversed_policy_reader', 'visible'),
					(2, 'other_user', 'hidden');`,
				`GRANT USAGE ON SCHEMA public TO rls_reversed_policy_reader;`,
				`GRANT SELECT ON rls_reversed_policy_docs TO rls_reversed_policy_reader;`,
				`CREATE POLICY rls_reversed_policy_docs_select
					ON rls_reversed_policy_docs
					FOR SELECT
					USING (current_user = owner_name);`,
				`ALTER TABLE rls_reversed_policy_docs ENABLE ROW LEVEL SECURITY;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, label
						FROM rls_reversed_policy_docs
						ORDER BY id;`,
					Expected: []sql.Row{{1, "visible"}},
					Username: `rls_reversed_policy_reader`,
					Password: `reader`,
				},
			},
		},
	})
}
