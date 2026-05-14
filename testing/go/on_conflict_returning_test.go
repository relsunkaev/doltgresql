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

// TestInsertOnConflictReturning pins the INSERT ... ON CONFLICT ... RETURNING
// shapes ORM upsert helpers depend on. SQLAlchemy
// `Session.execute(stmt).rowcount` and Drizzle `.returning()` use the
// returned row set + affected-row-count to drive optimistic-concurrency
// checks and "did we actually insert this?" branches. Per the Runtime
// SQL TODO in docs/app-compatibility-checklist.md.
func TestInsertOnConflictReturning(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			// Plain INSERT ... RETURNING (no ON CONFLICT) is the
			// baseline ORMs depend on.
			Name: "INSERT RETURNING (no conflict clause)",
			SetUpScript: []string{
				`CREATE TABLE kv (
					k INT PRIMARY KEY,
					v INT,
					updated_at INT
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO kv VALUES (1, 10, 100) RETURNING k, v, updated_at;`, PostgresOracle: ScriptTestPostgresOracle{ID: "on-conflict-returning-test-testinsertonconflictreturning-0001-insert-into-kv-values-1"},
				},
				{
					Query: `INSERT INTO kv VALUES (2, 20, 200), (3, 30, 300) RETURNING k, v;`, PostgresOracle: ScriptTestPostgresOracle{ID: "on-conflict-returning-test-testinsertonconflictreturning-0002-insert-into-kv-values-2"},
				},
			},
		},
		{
			// ON CONFLICT DO NOTHING RETURNING returns zero rows when
			// the existing row is preserved, and returns the inserted row
			// when no conflict occurs.
			Name: "ON CONFLICT DO NOTHING RETURNING",
			SetUpScript: []string{
				`CREATE TABLE kv (k INT PRIMARY KEY, v INT);`,
				`INSERT INTO kv VALUES (1, 10);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO kv VALUES (1, 99)
						ON CONFLICT (k) DO NOTHING
						RETURNING k, v;`, PostgresOracle: ScriptTestPostgresOracle{ID: "on-conflict-returning-test-testinsertonconflictreturning-0003-insert-into-kv-values-1"},
				},
				{
					// Confirm DO NOTHING preserved the original row.
					Query: `SELECT k, v FROM kv WHERE k = 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "on-conflict-returning-test-testinsertonconflictreturning-0004-select-k-v-from-kv"},
				},
				{
					Query: `INSERT INTO kv VALUES (2, 20)
						ON CONFLICT (k) DO NOTHING
						RETURNING k, v;`, PostgresOracle: ScriptTestPostgresOracle{ID: "on-conflict-returning-test-testinsertonconflictreturning-0005-insert-into-kv-values-2"},
				},
				{
					Query: "SELECT k, v FROM kv ORDER BY k;", PostgresOracle: ScriptTestPostgresOracle{ID: "on-conflict-returning-test-testinsertonconflictreturning-0006-select-k-v-from-kv"},
				},
			},
		},
		{
			Name: "ON CONFLICT DO UPDATE RETURNING returns updated row",
			SetUpScript: []string{
				`CREATE TABLE kv (k INT PRIMARY KEY, v INT);`,
				`INSERT INTO kv VALUES (1, 10);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO kv VALUES (1, 99)
						ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v
						RETURNING k, v;`, PostgresOracle: ScriptTestPostgresOracle{ID: "on-conflict-returning-test-testinsertonconflictreturning-0007-insert-into-kv-values-1"},
				},
				{
					Query: `SELECT k, v FROM kv WHERE k = 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "on-conflict-returning-test-testinsertonconflictreturning-0008-select-k-v-from-kv"},
				},
			},
		},
		{
			Name: "ON CONFLICT DO UPDATE RETURNING mixed insert and update rows",
			SetUpScript: []string{
				`CREATE TABLE kv (k INT PRIMARY KEY, v INT);`,
				`INSERT INTO kv VALUES (1, 10);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO kv VALUES (1, 11), (2, 22)
						ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v
						RETURNING k, v;`, PostgresOracle: ScriptTestPostgresOracle{ID: "on-conflict-returning-test-testinsertonconflictreturning-0009-insert-into-kv-values-1"},
				},
				{
					Query: "SELECT k, v FROM kv ORDER BY k;", PostgresOracle: ScriptTestPostgresOracle{ID: "on-conflict-returning-test-testinsertonconflictreturning-0010-select-k-v-from-kv"},
				},
			},
		},
		{
			Name: "ON CONFLICT RETURNING command tags count affected rows",
			SetUpScript: []string{
				`CREATE TABLE kv (k INT PRIMARY KEY, v INT);`,
				`INSERT INTO kv VALUES (1, 10);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO kv VALUES (1, 99)
						ON CONFLICT (k) DO NOTHING
						RETURNING k, v;`, PostgresOracle: ScriptTestPostgresOracle{ID: "on-conflict-returning-test-testinsertonconflictreturning-0011-insert-into-kv-values-1", Compare: "tag"},
				},
				{
					Query: `INSERT INTO kv VALUES (2, 20)
						ON CONFLICT (k) DO NOTHING
						RETURNING k, v;`, PostgresOracle: ScriptTestPostgresOracle{ID: "on-conflict-returning-test-testinsertonconflictreturning-0012-insert-into-kv-values-2", Compare: "tag"},
				},
				{
					Query: `INSERT INTO kv VALUES (1, 11)
						ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v
						RETURNING k, v;`, PostgresOracle: ScriptTestPostgresOracle{ID: "on-conflict-returning-test-testinsertonconflictreturning-0013-insert-into-kv-values-1", Compare: "tag"},
				},
				{
					Query: `INSERT INTO kv VALUES (1, 12), (3, 30)
						ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v
						RETURNING k, v;`, PostgresOracle: ScriptTestPostgresOracle{ID: "on-conflict-returning-test-testinsertonconflictreturning-0014-insert-into-kv-values-1", Compare: "tag"},
				},
				{
					Query: "SELECT k, v FROM kv ORDER BY k;", PostgresOracle: ScriptTestPostgresOracle{ID: "on-conflict-returning-test-testinsertonconflictreturning-0015-select-k-v-from-kv"},
				},
			},
		},
	})
}
