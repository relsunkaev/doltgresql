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
			Name: "partial UNIQUE index enforces predicate scoped duplicates",
			SetUpScript: []string{
				`CREATE TABLE memberships (id INT PRIMARY KEY, user_id INT, status TEXT);`,
				`CREATE UNIQUE INDEX memberships_one_active_idx
					ON memberships (user_id)
					WHERE status = 'active';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO memberships VALUES
						(1, 10, 'inactive'),
						(2, 10, 'inactive'),
						(3, 10, 'active');`,
				},
				{
					Query:       `INSERT INTO memberships VALUES (4, 10, 'active');`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query:       `UPDATE memberships SET status = 'active' WHERE id = 2;`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: `UPDATE memberships SET status = 'inactive' WHERE id = 3;`,
				},
				{
					Query: `INSERT INTO memberships VALUES (4, 10, 'active');`,
				},
				{
					Query: `INSERT INTO memberships VALUES (5, 10, 'active') ON CONFLICT DO NOTHING;`,
				},
				{
					Query:    `SELECT count(*)::text FROM memberships WHERE user_id = 10 AND status = 'active';`,
					Expected: []sql.Row{{"1"}},
				},
				{
					Query: `SELECT c.relname, i.indisunique, pg_catalog.pg_get_expr(i.indpred, i.indrelid)
FROM pg_catalog.pg_index i
JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
WHERE c.relname = 'memberships_one_active_idx';`,
					Expected: []sql.Row{
						{"memberships_one_active_idx", "t", "status = 'active'"},
					},
				},
				{
					Query: `SELECT indexdef
FROM pg_catalog.pg_indexes
WHERE tablename = 'memberships'
  AND indexname = 'memberships_one_active_idx';`,
					Expected: []sql.Row{
						{"CREATE UNIQUE INDEX memberships_one_active_idx ON public.memberships USING btree (user_id) WHERE status = 'active'"},
					},
				},
			},
		},
		{
			Name: "partial UNIQUE index supports boolean predicates",
			SetUpScript: []string{
				`CREATE TABLE inventory (id INT PRIMARY KEY, vmid INT, at_service BOOL);`,
				`CREATE UNIQUE INDEX inventory_vmid_not_service_idx ON inventory (vmid) WHERE NOT at_service;`,
				`CREATE UNIQUE INDEX inventory_vmid_service_idx ON inventory (vmid) WHERE at_service;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO inventory VALUES (1, 42, false), (2, 42, true);`,
				},
				{
					Query:       `INSERT INTO inventory VALUES (3, 42, false);`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query:       `INSERT INTO inventory VALUES (4, 42, true);`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: `INSERT INTO inventory VALUES (5, 42, NULL), (6, 42, NULL);`,
				},
			},
		},
		{
			Name: "partial UNIQUE index supports BETWEEN predicates",
			SetUpScript: []string{
				`CREATE TABLE quota_windows (id INT PRIMARY KEY, user_id INT, score INT);`,
				`CREATE UNIQUE INDEX quota_windows_user_score_idx ON quota_windows (user_id) WHERE score BETWEEN 1 AND 100;`,
				`CREATE TABLE quota_not_windows (id INT PRIMARY KEY, user_id INT, score INT);`,
				`CREATE UNIQUE INDEX quota_not_windows_user_score_idx ON quota_not_windows (user_id) WHERE score NOT BETWEEN 1 AND 100;`,
				`CREATE TABLE quota_symmetric_windows (id INT PRIMARY KEY, user_id INT, score INT);`,
				`CREATE UNIQUE INDEX quota_symmetric_windows_user_score_idx ON quota_symmetric_windows (user_id) WHERE score BETWEEN SYMMETRIC 100 AND 1;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO quota_windows VALUES (1, 10, 50), (2, 10, 0), (3, 10, 101);`,
				},
				{
					Query:       `INSERT INTO quota_windows VALUES (4, 10, 60);`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query:       `UPDATE quota_windows SET score = 75 WHERE id = 2;`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: `UPDATE quota_windows SET score = 101 WHERE id = 1;`,
				},
				{
					Query: `INSERT INTO quota_windows VALUES (4, 10, 60);`,
				},
				{
					Query:    `SELECT id, score FROM quota_windows WHERE user_id = 10 ORDER BY id;`,
					Expected: []sql.Row{{int32(1), int32(101)}, {int32(2), int32(0)}, {int32(3), int32(101)}, {int32(4), int32(60)}},
				},
				{
					Query: `INSERT INTO quota_not_windows VALUES (1, 20, 0), (2, 20, 50);`,
				},
				{
					Query:       `INSERT INTO quota_not_windows VALUES (3, 20, 101);`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: `INSERT INTO quota_not_windows VALUES (4, 20, 60);`,
				},
				{
					Query: `INSERT INTO quota_symmetric_windows VALUES (1, 30, 50), (2, 30, 0), (3, 30, 101);`,
				},
				{
					Query:       `INSERT INTO quota_symmetric_windows VALUES (4, 30, 60);`,
					ExpectedErr: "duplicate unique key given",
				},
			},
		},
		{
			Name: "partial UNIQUE index supports IN predicates",
			SetUpScript: []string{
				`CREATE TABLE workflow_states (id INT PRIMARY KEY, user_id INT, status TEXT);`,
				`CREATE UNIQUE INDEX workflow_states_open_idx ON workflow_states (user_id) WHERE status IN ('active', 'pending');`,
				`CREATE TABLE workflow_not_states (id INT PRIMARY KEY, user_id INT, status TEXT);`,
				`CREATE UNIQUE INDEX workflow_not_states_open_idx ON workflow_not_states (user_id) WHERE status NOT IN ('archived', 'deleted');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO workflow_states VALUES (1, 10, 'active'), (2, 10, 'archived');`,
				},
				{
					Query:       `INSERT INTO workflow_states VALUES (3, 10, 'pending');`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query:       `UPDATE workflow_states SET status = 'pending' WHERE id = 2;`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: `UPDATE workflow_states SET status = 'archived' WHERE id = 1;`,
				},
				{
					Query: `INSERT INTO workflow_states VALUES (3, 10, 'pending');`,
				},
				{
					Query: `INSERT INTO workflow_not_states VALUES (1, 20, 'active'), (2, 20, 'archived');`,
				},
				{
					Query:       `INSERT INTO workflow_not_states VALUES (3, 20, 'pending');`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: `INSERT INTO workflow_not_states VALUES (4, 20, 'deleted');`,
				},
			},
		},
		{
			Name: "partial UNIQUE index supports lower text predicate",
			SetUpScript: []string{
				`CREATE TABLE case_folded_accounts (id INT PRIMARY KEY, email TEXT);`,
				`CREATE UNIQUE INDEX case_folded_accounts_active_idx
					ON case_folded_accounts (email)
					WHERE lower(email) IN ('active@example.com', 'admin@example.com');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO case_folded_accounts VALUES
						(1, 'Active@Example.com'),
						(2, 'other@example.com');`,
				},
				{
					Query:       `INSERT INTO case_folded_accounts VALUES (3, 'Active@Example.com');`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: `INSERT INTO case_folded_accounts VALUES (4, 'Other@Example.com');`,
				},
				{
					Query:       `UPDATE case_folded_accounts SET email = 'Active@Example.com' WHERE id = 2;`,
					ExpectedErr: "duplicate unique key given",
				},
			},
		},
		{
			Name: "partial UNIQUE index validates existing rows",
			SetUpScript: []string{
				`CREATE TABLE duplicate_memberships (id INT PRIMARY KEY, user_id INT, status TEXT);`,
				`INSERT INTO duplicate_memberships VALUES (1, 10, 'active'), (2, 10, 'active'), (3, 10, 'inactive');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE UNIQUE INDEX duplicate_memberships_active_idx
						ON duplicate_memberships (user_id)
						WHERE status = 'active';`,
					ExpectedErr: "duplicate unique key given",
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
