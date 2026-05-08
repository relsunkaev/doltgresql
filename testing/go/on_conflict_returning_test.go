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
					Query:    `INSERT INTO kv VALUES (1, 10, 100) RETURNING k, v, updated_at;`,
					Expected: []sql.Row{{int32(1), int32(10), int32(100)}},
				},
				{
					Query: `INSERT INTO kv VALUES (2, 20, 200), (3, 30, 300) RETURNING k, v;`,
					Expected: []sql.Row{
						{int32(2), int32(20)},
						{int32(3), int32(30)},
					},
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
						RETURNING k, v;`,
					Expected: []sql.Row{},
				},
				{
					// Confirm DO NOTHING preserved the original row.
					Query:    `SELECT k, v FROM kv WHERE k = 1;`,
					Expected: []sql.Row{{int32(1), int32(10)}},
				},
				{
					Query: `INSERT INTO kv VALUES (2, 20)
						ON CONFLICT (k) DO NOTHING
						RETURNING k, v;`,
					Expected: []sql.Row{{int32(2), int32(20)}},
				},
				{
					Query: "SELECT k, v FROM kv ORDER BY k;",
					Expected: []sql.Row{
						{int32(1), int32(10)},
						{int32(2), int32(20)},
					},
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
						RETURNING k, v;`,
					Expected: []sql.Row{{int32(1), int32(99)}},
				},
				{
					Query:    `SELECT k, v FROM kv WHERE k = 1;`,
					Expected: []sql.Row{{int32(1), int32(99)}},
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
						RETURNING k, v;`,
					Expected: []sql.Row{
						{int32(1), int32(11)},
						{int32(2), int32(22)},
					},
				},
				{
					Query: "SELECT k, v FROM kv ORDER BY k;",
					Expected: []sql.Row{
						{int32(1), int32(11)},
						{int32(2), int32(22)},
					},
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
						RETURNING k, v;`,
					ExpectedTag: `INSERT 0 0`,
				},
				{
					Query: `INSERT INTO kv VALUES (2, 20)
						ON CONFLICT (k) DO NOTHING
						RETURNING k, v;`,
					ExpectedTag: `INSERT 0 1`,
				},
				{
					Query: `INSERT INTO kv VALUES (1, 11)
						ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v
						RETURNING k, v;`,
					ExpectedTag: `INSERT 0 1`,
				},
				{
					Query: `INSERT INTO kv VALUES (1, 12), (3, 30)
						ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v
						RETURNING k, v;`,
					ExpectedTag: `INSERT 0 2`,
				},
				{
					Query: "SELECT k, v FROM kv ORDER BY k;",
					Expected: []sql.Row{
						{int32(1), int32(12)},
						{int32(2), int32(20)},
						{int32(3), int32(30)},
					},
				},
			},
		},
	})
}
