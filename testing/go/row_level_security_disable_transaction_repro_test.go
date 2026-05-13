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

func TestRollbackRevertsDisableRowLevelSecurityRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK reverts DISABLE ROW LEVEL SECURITY",
			SetUpScript: []string{
				`CREATE USER rollback_disable_rls_reader PASSWORD 'reader';`,
				`CREATE TABLE rollback_disable_rls_docs (
					id INT PRIMARY KEY,
					owner_name TEXT,
					label TEXT
				);`,
				`INSERT INTO rollback_disable_rls_docs VALUES
					(1, 'rollback_disable_rls_reader', 'visible'),
					(2, 'other_user', 'hidden');`,
				`GRANT USAGE ON SCHEMA public TO rollback_disable_rls_reader;`,
				`GRANT SELECT ON rollback_disable_rls_docs TO rollback_disable_rls_reader;`,
				`CREATE POLICY rollback_disable_rls_docs_owner_select
					ON rollback_disable_rls_docs
					FOR SELECT
					USING (owner_name = current_user);`,
				`ALTER TABLE rollback_disable_rls_docs ENABLE ROW LEVEL SECURITY;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, label
						FROM rollback_disable_rls_docs
						ORDER BY id;`,

					Username: `rollback_disable_rls_reader`,
					Password: `reader`, PostgresOracle: ScriptTestPostgresOracle{ID: "row-level-security-disable-transaction-repro-test-testrollbackrevertsdisablerowlevelsecurityrepro-0001-select-id-label-from-rollback_disable_rls_docs"},
				},
				{
					Query: `BEGIN;`,
				},
				{
					Query: `ALTER TABLE rollback_disable_rls_docs DISABLE ROW LEVEL SECURITY;`,
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `SELECT relrowsecurity
						FROM pg_catalog.pg_class
						WHERE oid = 'rollback_disable_rls_docs'::regclass;`, PostgresOracle: ScriptTestPostgresOracle{ID: "row-level-security-disable-transaction-repro-test-testrollbackrevertsdisablerowlevelsecurityrepro-0002-select-relrowsecurity-from-pg_catalog.pg_class-where"},
				},
				{
					Query: `SELECT id, label
						FROM rollback_disable_rls_docs
						ORDER BY id;`,

					Username: `rollback_disable_rls_reader`,
					Password: `reader`, PostgresOracle: ScriptTestPostgresOracle{ID: "row-level-security-disable-transaction-repro-test-testrollbackrevertsdisablerowlevelsecurityrepro-0003-select-id-label-from-rollback_disable_rls_docs"},
				},
			},
		},
	})
}
