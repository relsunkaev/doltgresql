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

// TestDropTableClearsRowLevelSecurityStateRepro reproduces a row-level security
// persistence bug: dropping a table does not clear its in-memory RLS metadata,
// so a later table with the same name inherits the old policy and filters rows.
func TestDropTableClearsRowLevelSecurityStateRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP TABLE clears row-level security state before recreate",
			SetUpScript: []string{
				`CREATE USER drop_recreate_rls_reader PASSWORD 'reader';`,
				`CREATE TABLE drop_recreate_rls_docs (
					id INT PRIMARY KEY,
					owner_name TEXT,
					label TEXT
				);`,
				`INSERT INTO drop_recreate_rls_docs VALUES
					(1, 'drop_recreate_rls_reader', 'old visible'),
					(2, 'other_user', 'old hidden');`,
				`GRANT USAGE ON SCHEMA public TO drop_recreate_rls_reader;`,
				`GRANT SELECT ON drop_recreate_rls_docs TO drop_recreate_rls_reader;`,
				`CREATE POLICY drop_recreate_rls_docs_owner_select
					ON drop_recreate_rls_docs
					FOR SELECT
					USING (owner_name = current_user);`,
				`ALTER TABLE drop_recreate_rls_docs ENABLE ROW LEVEL SECURITY;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, label
						FROM drop_recreate_rls_docs
						ORDER BY id;`,

					Username: `drop_recreate_rls_reader`,
					Password: `reader`, PostgresOracle: ScriptTestPostgresOracle{ID: "row-level-security-drop-recreate-repro-test-testdroptableclearsrowlevelsecuritystaterepro-0001-select-id-label-from-drop_recreate_rls_docs"},
				},
				{
					Query: `DROP TABLE drop_recreate_rls_docs;`,
				},
				{
					Query: `CREATE TABLE drop_recreate_rls_docs (
						id INT PRIMARY KEY,
						owner_name TEXT,
						label TEXT
					);`,
				},
				{
					Query: `INSERT INTO drop_recreate_rls_docs VALUES
						(1, 'drop_recreate_rls_reader', 'new visible'),
						(2, 'other_user', 'new unrestricted');`,
				},
				{
					Query: `GRANT SELECT ON drop_recreate_rls_docs
						TO drop_recreate_rls_reader;`,
				},
				{
					Query: `SELECT relrowsecurity
						FROM pg_catalog.pg_class
						WHERE oid = 'drop_recreate_rls_docs'::regclass;`, PostgresOracle: ScriptTestPostgresOracle{ID: "row-level-security-drop-recreate-repro-test-testdroptableclearsrowlevelsecuritystaterepro-0002-select-relrowsecurity-from-pg_catalog.pg_class-where"},
				},
				{
					Query: `SELECT id, label
						FROM drop_recreate_rls_docs
						ORDER BY id;`,

					Username: `drop_recreate_rls_reader`,
					Password: `reader`, PostgresOracle: ScriptTestPostgresOracle{ID: "row-level-security-drop-recreate-repro-test-testdroptableclearsrowlevelsecuritystaterepro-0003-select-id-label-from-drop_recreate_rls_docs"},
				},
			},
		},
	})
}
