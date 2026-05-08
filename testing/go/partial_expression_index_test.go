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

// TestPartialAndExpressionIndexes probes how far partial-index and
// expression-index DDL goes today. The index planner gap (partial
// unique indexes don't drive ON CONFLICT arbiter selection) is tracked
// separately; this test pins what *does* work at the DDL level so
// migration tools that emit these forms don't blow up. Per the
// Index/planner TODO in docs/app-compatibility-checklist.md.
func TestPartialAndExpressionIndexes(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "partial index with WHERE IS NOT NULL",
			SetUpScript: []string{
				`CREATE TABLE users (id INT PRIMARY KEY, email TEXT, deleted_at TIMESTAMP);`,
				`INSERT INTO users VALUES
					(1, 'a@x', NULL),
					(2, 'b@x', NULL),
					(3, 'c@x', '2026-01-01');`,
				`CREATE INDEX users_active_email_idx
					ON users (email)
					WHERE deleted_at IS NULL;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					// Index existence and indpred should round-trip
					// through pg_index (the catalog row, not the
					// planner choice).
					Query:    `SELECT count(*)::text FROM pg_indexes WHERE tablename = 'users' AND indexname = 'users_active_email_idx';`,
					Expected: []sql.Row{{"1"}},
				},
				{
					// Reading through the partial-index predicate
					// must still return the right rows.
					Query: `SELECT id, email FROM users WHERE deleted_at IS NULL ORDER BY id;`,
					Expected: []sql.Row{
						{int32(1), "a@x"},
						{int32(2), "b@x"},
					},
				},
			},
		},
		{
			Name: "partial index with boolean active flag",
			SetUpScript: []string{
				`CREATE TABLE jobs (id INT PRIMARY KEY, active BOOL, payload TEXT);`,
				`INSERT INTO jobs VALUES (1, true, 'p1'), (2, false, 'p2'), (3, true, 'p3');`,
				`CREATE INDEX jobs_active_idx ON jobs (id) WHERE active = true;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT count(*)::text FROM pg_indexes WHERE tablename = 'jobs' AND indexname = 'jobs_active_idx';`,
					Expected: []sql.Row{{"1"}},
				},
				{
					Query: `SELECT id, payload FROM jobs WHERE active = true ORDER BY id;`,
					Expected: []sql.Row{
						{int32(1), "p1"},
						{int32(3), "p3"},
					},
				},
			},
		},
		{
			// Partial UNIQUE indexes are explicitly rejected with a
			// clear error today. This is the deeper gap behind the
			// Index/planner TODO entry on partial indexes (also
			// implicates `ON CONFLICT (col) WHERE arbiter_pred`).
			// Pin the rejection so the gap stays visible.
			Name: "partial UNIQUE index DDL is rejected with a clear error",
			SetUpScript: []string{
				`CREATE TABLE memberships (id INT PRIMARY KEY, user_id INT, status TEXT);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE UNIQUE INDEX memberships_one_active_idx
						ON memberships (user_id)
						WHERE status = 'active';`,
					ExpectedErr: "unique partial indexes are not yet supported",
				},
			},
		},
		{
			Name: "expression index over lower(text)",
			SetUpScript: []string{
				`CREATE TABLE accounts (id INT PRIMARY KEY, email TEXT);`,
				`INSERT INTO accounts VALUES (1, 'Alice@X'), (2, 'BOB@X'), (3, 'carol@x');`,
				`CREATE INDEX accounts_lower_email_idx ON accounts ((lower(email)));`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT count(*)::text FROM pg_indexes WHERE tablename = 'accounts' AND indexname = 'accounts_lower_email_idx';`,
					Expected: []sql.Row{{"1"}},
				},
				{
					Query:    `SELECT id FROM accounts WHERE lower(email) = 'bob@x';`,
					Expected: []sql.Row{{int32(2)}},
				},
			},
		},
	})
}
