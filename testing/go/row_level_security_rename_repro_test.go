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

// TestRenameTablePreservesRowLevelSecurityStateRepro reproduces a row-level
// security metadata bug: ALTER TABLE RENAME leaves the policy state keyed under
// the old table name, so the renamed table loses RLS filtering.
func TestRenameTablePreservesRowLevelSecurityStateRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TABLE RENAME preserves row-level security state",
			SetUpScript: []string{
				`CREATE USER rename_rls_reader PASSWORD 'reader';`,
				`CREATE TABLE rename_rls_docs (
					id INT PRIMARY KEY,
					owner_name TEXT,
					label TEXT
				);`,
				`INSERT INTO rename_rls_docs VALUES
					(1, 'rename_rls_reader', 'visible'),
					(2, 'other_user', 'hidden');`,
				`GRANT USAGE ON SCHEMA public TO rename_rls_reader;`,
				`GRANT SELECT ON rename_rls_docs TO rename_rls_reader;`,
				`CREATE POLICY rename_rls_docs_owner_select
					ON rename_rls_docs
					FOR SELECT
					USING (owner_name = current_user);`,
				`ALTER TABLE rename_rls_docs ENABLE ROW LEVEL SECURITY;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, label
						FROM rename_rls_docs
						ORDER BY id;`,

					Username: `rename_rls_reader`,
					Password: `reader`, PostgresOracle: ScriptTestPostgresOracle{ID: "row-level-security-rename-repro-test-testrenametablepreservesrowlevelsecuritystaterepro-0001-select-id-label-from-rename_rls_docs"},
				},
				{
					Query: `ALTER TABLE rename_rls_docs
						RENAME TO rename_rls_docs_renamed;`,
				},
				{
					Query: `GRANT SELECT ON rename_rls_docs_renamed
						TO rename_rls_reader;`,
				},
				{
					Query: `SELECT relrowsecurity
						FROM pg_catalog.pg_class
						WHERE oid = 'rename_rls_docs_renamed'::regclass;`, PostgresOracle: ScriptTestPostgresOracle{ID: "row-level-security-rename-repro-test-testrenametablepreservesrowlevelsecuritystaterepro-0002-select-relrowsecurity-from-pg_catalog.pg_class-where"},
				},
				{
					Query: `SELECT id, label
						FROM rename_rls_docs_renamed
						ORDER BY id;`,

					Username: `rename_rls_reader`,
					Password: `reader`, PostgresOracle: ScriptTestPostgresOracle{ID: "row-level-security-rename-repro-test-testrenametablepreservesrowlevelsecuritystaterepro-0003-select-id-label-from-rename_rls_docs_renamed"},
				},
			},
		},
	})
}
