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

// TestRollbackRevertsRowLevelSecurityModeRepro reproduces a transaction
// consistency bug: ALTER TABLE ... ENABLE ROW LEVEL SECURITY mutates RLS
// metadata outside the surrounding transaction and survives ROLLBACK.
func TestRollbackRevertsRowLevelSecurityModeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK reverts ENABLE ROW LEVEL SECURITY",
			SetUpScript: []string{
				`CREATE USER rollback_rls_reader PASSWORD 'reader';`,
				`CREATE TABLE rollback_rls_docs (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO rollback_rls_docs VALUES (1, 'visible');`,
				`GRANT USAGE ON SCHEMA public TO rollback_rls_reader;`,
				`GRANT SELECT ON rollback_rls_docs TO rollback_rls_reader;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `ALTER TABLE rollback_rls_docs ENABLE ROW LEVEL SECURITY;`,
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `SELECT relrowsecurity
						FROM pg_catalog.pg_class
						WHERE oid = 'rollback_rls_docs'::regclass;`,
					Expected: []sql.Row{{"f"}},
				},
				{
					Query:    `SELECT id, label FROM rollback_rls_docs;`,
					Expected: []sql.Row{{1, "visible"}},
					Username: `rollback_rls_reader`,
					Password: `reader`,
				},
			},
		},
	})
}
