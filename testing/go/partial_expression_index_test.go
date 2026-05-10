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
			Name: "partial UNIQUE index supports abs numeric predicate",
			SetUpScript: []string{
				`CREATE TABLE absolute_scores (id INT PRIMARY KEY, user_id INT, delta BIGINT);`,
				`CREATE UNIQUE INDEX absolute_scores_user_delta_idx
					ON absolute_scores (user_id)
					WHERE abs(delta) = 10;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO absolute_scores VALUES
						(1, 10, -10),
						(2, 10, 5);`,
				},
				{
					Query:       `INSERT INTO absolute_scores VALUES (3, 10, 10);`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: `INSERT INTO absolute_scores VALUES (4, 10, -5);`,
				},
				{
					Query:       `UPDATE absolute_scores SET delta = 10 WHERE id = 2;`,
					ExpectedErr: "duplicate unique key given",
				},
			},
		},
		{
			Name: "partial UNIQUE index supports octet_length bytea predicate",
			SetUpScript: []string{
				`CREATE TABLE byte_payloads (id INT PRIMARY KEY, user_id INT, payload BYTEA);`,
				`CREATE UNIQUE INDEX byte_payloads_user_payload_idx
					ON byte_payloads (user_id)
					WHERE octet_length(payload) = 3;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO byte_payloads VALUES
						(1, 10, '\x010203'),
						(2, 10, '\x0102');`,
				},
				{
					Query:       `INSERT INTO byte_payloads VALUES (3, 10, '\xAABBCC');`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: `INSERT INTO byte_payloads VALUES (4, 10, '\xAABB');`,
				},
				{
					Query:       `UPDATE byte_payloads SET payload = '\xAABBCC' WHERE id = 2;`,
					ExpectedErr: "duplicate unique key given",
				},
			},
		},
		{
			Name: "partial UNIQUE index supports bit_length bytea predicate",
			SetUpScript: []string{
				`CREATE TABLE bit_payloads (id INT PRIMARY KEY, user_id INT, payload BYTEA);`,
				`CREATE UNIQUE INDEX bit_payloads_user_payload_idx
					ON bit_payloads (user_id)
					WHERE bit_length(payload) = 24;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO bit_payloads VALUES
						(1, 10, '\x010203'),
						(2, 10, '\x0102');`,
				},
				{
					Query:       `INSERT INTO bit_payloads VALUES (3, 10, '\xAABBCC');`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: `INSERT INTO bit_payloads VALUES (4, 10, '\xAABB');`,
				},
				{
					Query:       `UPDATE bit_payloads SET payload = '\xAABBCC' WHERE id = 2;`,
					ExpectedErr: "duplicate unique key given",
				},
			},
		},
		{
			Name: "partial UNIQUE index supports strpos predicate",
			SetUpScript: []string{
				`CREATE TABLE prefixed_codes (id INT PRIMARY KEY, user_id INT, code TEXT);`,
				`CREATE UNIQUE INDEX prefixed_codes_user_code_idx
					ON prefixed_codes (user_id)
					WHERE strpos(code, 'active') = 1;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO prefixed_codes VALUES
						(1, 10, 'active-a'),
						(2, 10, 'pending');`,
				},
				{
					Query:       `INSERT INTO prefixed_codes VALUES (3, 10, 'active-b');`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: `INSERT INTO prefixed_codes VALUES (4, 10, 'inactive');`,
				},
				{
					Query:       `UPDATE prefixed_codes SET code = 'active-c' WHERE id = 2;`,
					ExpectedErr: "duplicate unique key given",
				},
			},
		},
		{
			Name: "partial UNIQUE index supports starts_with predicate",
			SetUpScript: []string{
				`CREATE TABLE prefix_codes (id INT PRIMARY KEY, user_id INT, code TEXT);`,
				`CREATE UNIQUE INDEX prefix_codes_user_code_idx
					ON prefix_codes (user_id)
					WHERE starts_with(code, 'active');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO prefix_codes VALUES
						(1, 10, 'active-a'),
						(2, 10, 'pending');`,
				},
				{
					Query:       `INSERT INTO prefix_codes VALUES (3, 10, 'active-b');`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: `INSERT INTO prefix_codes VALUES (4, 10, 'inactive');`,
				},
				{
					Query:       `UPDATE prefix_codes SET code = 'active-c' WHERE id = 2;`,
					ExpectedErr: "duplicate unique key given",
				},
			},
		},
		{
			Name: "partial UNIQUE index supports prefix LIKE predicate",
			SetUpScript: []string{
				`CREATE TABLE like_prefix_codes (id INT PRIMARY KEY, user_id INT, code TEXT);`,
				`CREATE UNIQUE INDEX like_prefix_codes_user_code_idx
					ON like_prefix_codes (user_id)
					WHERE code LIKE 'active%';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO like_prefix_codes VALUES
						(1, 10, 'active-a'),
						(2, 10, 'pending');`,
				},
				{
					Query:       `INSERT INTO like_prefix_codes VALUES (3, 10, 'active-b');`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: `INSERT INTO like_prefix_codes VALUES (4, 10, 'inactive');`,
				},
				{
					Query:       `UPDATE like_prefix_codes SET code = 'active-c' WHERE id = 2;`,
					ExpectedErr: "duplicate unique key given",
				},
			},
		},
		{
			Name: "partial UNIQUE index supports left and right predicates",
			SetUpScript: []string{
				`CREATE TABLE left_codes (id INT PRIMARY KEY, user_id INT, code TEXT);`,
				`CREATE UNIQUE INDEX left_codes_user_code_idx
					ON left_codes (user_id)
					WHERE left(code, 2) = 'åc';`,
				`CREATE TABLE right_codes (id INT PRIMARY KEY, user_id INT, code TEXT);`,
				`CREATE UNIQUE INDEX right_codes_user_code_idx
					ON right_codes (user_id)
					WHERE right(code, -1) = 'ctive';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO left_codes VALUES
						(1, 10, 'åctive'),
						(2, 10, 'pending');`,
				},
				{
					Query:       `INSERT INTO left_codes VALUES (3, 10, 'åctor');`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: `INSERT INTO left_codes VALUES (4, 10, 'archive');`,
				},
				{
					Query:       `UPDATE left_codes SET code = 'åction' WHERE id = 2;`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: `INSERT INTO right_codes VALUES
						(1, 20, 'åctive'),
						(2, 20, 'pending');`,
				},
				{
					Query:       `INSERT INTO right_codes VALUES (3, 20, 'bctive');`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: `INSERT INTO right_codes VALUES (4, 20, 'inactive');`,
				},
				{
					Query:       `UPDATE right_codes SET code = 'cctive' WHERE id = 2;`,
					ExpectedErr: "duplicate unique key given",
				},
			},
		},
		{
			Name: "partial UNIQUE index supports replace predicate",
			SetUpScript: []string{
				`CREATE TABLE normalized_codes (id INT PRIMARY KEY, user_id INT, code TEXT);`,
				`CREATE UNIQUE INDEX normalized_codes_user_code_idx
					ON normalized_codes (user_id)
					WHERE replace(code, '-', '') = 'activea';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO normalized_codes VALUES
						(1, 10, 'active-a'),
						(2, 10, 'pending');`,
				},
				{
					Query:       `INSERT INTO normalized_codes VALUES (3, 10, 'active--a');`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: `INSERT INTO normalized_codes VALUES (4, 10, 'active_a');`,
				},
				{
					Query:       `UPDATE normalized_codes SET code = 'active-a' WHERE id = 2;`,
					ExpectedErr: "duplicate unique key given",
				},
			},
		},
		{
			Name: "partial UNIQUE index supports translate predicate",
			SetUpScript: []string{
				`CREATE TABLE translated_codes (id INT PRIMARY KEY, user_id INT, code TEXT);`,
				`CREATE UNIQUE INDEX translated_codes_user_code_idx
					ON translated_codes (user_id)
					WHERE translate(code, '-_', '') = 'activea';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO translated_codes VALUES
						(1, 10, 'active-a'),
						(2, 10, 'pending');`,
				},
				{
					Query:       `INSERT INTO translated_codes VALUES (3, 10, 'active__a');`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: `INSERT INTO translated_codes VALUES (4, 10, 'active.a');`,
				},
				{
					Query:       `UPDATE translated_codes SET code = 'active_a' WHERE id = 2;`,
					ExpectedErr: "duplicate unique key given",
				},
			},
		},
		{
			Name: "partial UNIQUE index supports md5 predicate",
			SetUpScript: []string{
				`CREATE TABLE hashed_codes (id INT PRIMARY KEY, user_id INT, code TEXT);`,
				`CREATE UNIQUE INDEX hashed_codes_user_code_idx
					ON hashed_codes (user_id)
					WHERE md5(code) = 'c76a5e84e4bdee527e274ea30c680d79';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO hashed_codes VALUES
						(1, 10, 'active'),
						(2, 10, 'pending');`,
				},
				{
					Query:       `INSERT INTO hashed_codes VALUES (3, 10, 'active');`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: `INSERT INTO hashed_codes VALUES (4, 10, 'ACTIVE');`,
				},
				{
					Query:       `UPDATE hashed_codes SET code = 'active' WHERE id = 2;`,
					ExpectedErr: "duplicate unique key given",
				},
			},
		},
		{
			Name: "partial UNIQUE index supports split_part predicate",
			SetUpScript: []string{
				`CREATE TABLE email_domains (id INT PRIMARY KEY, user_id INT, email TEXT);`,
				`CREATE UNIQUE INDEX email_domains_user_domain_idx
					ON email_domains (user_id)
					WHERE split_part(email, '@', 2) = 'example.com';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO email_domains VALUES
						(1, 10, 'first@example.com'),
						(2, 10, 'second@example.org');`,
				},
				{
					Query:       `INSERT INTO email_domains VALUES (3, 10, 'other@example.com');`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: `INSERT INTO email_domains VALUES (4, 10, 'missing-domain');`,
				},
				{
					Query:       `UPDATE email_domains SET email = 'third@example.com' WHERE id = 2;`,
					ExpectedErr: "duplicate unique key given",
				},
			},
		},
		{
			Name: "partial UNIQUE index supports ascii predicate",
			SetUpScript: []string{
				`CREATE TABLE ascii_codes (id INT PRIMARY KEY, user_id INT, code TEXT);`,
				`CREATE UNIQUE INDEX ascii_codes_user_code_idx
					ON ascii_codes (user_id)
					WHERE ascii(code) = 65;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO ascii_codes VALUES
						(1, 10, 'Alpha'),
						(2, 10, 'beta');`,
				},
				{
					Query:       `INSERT INTO ascii_codes VALUES (3, 10, 'Admin');`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: `INSERT INTO ascii_codes VALUES (4, 10, 'alpha');`,
				},
				{
					Query:       `UPDATE ascii_codes SET code = 'April' WHERE id = 2;`,
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

func TestPartialIndexPlannerImplication(t *testing.T) {
	ctx, conn, controller := CreateServer(t, "postgres")
	t.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		if err := controller.WaitForStop(); err != nil {
			t.Fatalf("error stopping test server: %v", err)
		}
	})

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE partial_planner_scores (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, score INTEGER NOT NULL)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO partial_planner_scores VALUES
		(1, 1, 50),
		(2, 1, -1),
		(3, 1, 5),
		(4, 1, 0),
		(5, 2, 20),
		(6, 2, -3)`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX partial_planner_scores_tenant_idx ON partial_planner_scores (tenant) WHERE score > 0")

	impliedQuery := `SELECT count(id) FROM partial_planner_scores WHERE tenant = 1 AND score > 10`
	assertCountResult(t, ctx, conn, impliedQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, impliedQuery, true)

	nonImpliedQuery := `SELECT count(id) FROM partial_planner_scores WHERE tenant = 1 AND score >= 0`
	assertCountResult(t, ctx, conn, nonImpliedQuery, 3)
	assertBenchmarkPlanShape(t, ctx, conn, nonImpliedQuery, false)

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE partial_planner_contacts (id INTEGER PRIMARY KEY, email TEXT, active BOOL)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO partial_planner_contacts VALUES
		(1, 'ada@example.com', true),
		(2, 'grace@example.com', false),
		(3, NULL, true)`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX partial_planner_contacts_email_idx ON partial_planner_contacts (email) WHERE email IS NOT NULL")

	notNullImpliedQuery := `SELECT id FROM partial_planner_contacts WHERE email = 'ada@example.com'`
	assertBenchmarkPlanShape(t, ctx, conn, notNullImpliedQuery, true)
	if got := queryBenchmarkString(t, ctx, conn, `SELECT id::text FROM partial_planner_contacts WHERE email = 'ada@example.com'`); got != "1" {
		t.Fatalf("unexpected email lookup result: %s", got)
	}

	nullQuery := `SELECT count(id) FROM partial_planner_contacts WHERE email IS NULL`
	assertCountResult(t, ctx, conn, nullQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, nullQuery, false)

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE partial_planner_statuses (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, status TEXT)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO partial_planner_statuses VALUES
		(1, 1, 'active'),
		(2, 1, 'pending'),
		(3, 1, 'archived'),
		(4, 2, 'deleted'),
		(5, 2, 'active'),
		(6, 2, NULL)`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX partial_planner_statuses_tenant_idx ON partial_planner_statuses (tenant) WHERE status NOT IN ('archived', 'deleted')")

	exclusionImpliedQuery := `SELECT count(id) FROM partial_planner_statuses WHERE tenant = 1 AND status = 'active'`
	assertCountResult(t, ctx, conn, exclusionImpliedQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, exclusionImpliedQuery, true)

	exclusionInListQuery := `SELECT count(id) FROM partial_planner_statuses WHERE tenant = 1 AND status IN ('active', 'pending')`
	assertCountResult(t, ctx, conn, exclusionInListQuery, 2)
	assertBenchmarkPlanShape(t, ctx, conn, exclusionInListQuery, true)

	exclusionNotInSubsetQuery := `SELECT count(id) FROM partial_planner_statuses WHERE tenant = 1 AND status NOT IN ('archived', 'deleted', 'blocked')`
	assertCountResult(t, ctx, conn, exclusionNotInSubsetQuery, 2)
	assertBenchmarkPlanShape(t, ctx, conn, exclusionNotInSubsetQuery, true)

	exclusionNonImpliedQuery := `SELECT count(id) FROM partial_planner_statuses WHERE tenant = 1 AND status IN ('active', 'archived')`
	assertCountResult(t, ctx, conn, exclusionNonImpliedQuery, 2)
	assertBenchmarkPlanShape(t, ctx, conn, exclusionNonImpliedQuery, false)

	exclusionIncompleteQuery := `SELECT count(id) FROM partial_planner_statuses WHERE tenant = 1 AND status != 'archived'`
	assertCountResult(t, ctx, conn, exclusionIncompleteQuery, 2)
	assertBenchmarkPlanShape(t, ctx, conn, exclusionIncompleteQuery, false)

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE partial_planner_status_ne (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, status TEXT)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO partial_planner_status_ne VALUES
		(1, 1, 'active'),
		(2, 1, 'archived'),
		(3, 2, 'pending'),
		(4, 2, NULL)`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX partial_planner_status_ne_tenant_idx ON partial_planner_status_ne (tenant) WHERE status != 'archived'")

	notEqualImpliedQuery := `SELECT count(id) FROM partial_planner_status_ne WHERE tenant = 1 AND status = 'active'`
	assertCountResult(t, ctx, conn, notEqualImpliedQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, notEqualImpliedQuery, true)

	notEqualNonImpliedQuery := `SELECT count(id) FROM partial_planner_status_ne WHERE tenant = 1 AND status = 'archived'`
	assertCountResult(t, ctx, conn, notEqualNonImpliedQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, notEqualNonImpliedQuery, false)

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE partial_planner_pattern (id INTEGER PRIMARY KEY, name TEXT NOT NULL, active BOOL NOT NULL)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO partial_planner_pattern VALUES
		(1, 'alpha', true),
		(2, 'alphabet', true),
		(3, 'alpine', false),
		(4, 'beta', true)`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX partial_planner_pattern_name_idx ON partial_planner_pattern (name text_pattern_ops) WHERE active")

	patternImpliedQuery := `SELECT count(id) FROM partial_planner_pattern WHERE active = true AND name LIKE 'alph%'`
	assertCountResult(t, ctx, conn, patternImpliedQuery, 2)
	assertBenchmarkPlanShape(t, ctx, conn, patternImpliedQuery, true)

	patternNonImpliedQuery := `SELECT count(id) FROM partial_planner_pattern WHERE active = false AND name LIKE 'alp%'`
	assertCountResult(t, ctx, conn, patternNonImpliedQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, patternNonImpliedQuery, false)

	patternUnsafeQuery := `SELECT count(id) FROM partial_planner_pattern WHERE active = true AND name LIKE '%lph%'`
	assertCountResult(t, ctx, conn, patternUnsafeQuery, 2)
	assertBenchmarkPlanShape(t, ctx, conn, patternUnsafeQuery, false)

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE partial_planner_nullsafe (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, deleted_at TEXT)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO partial_planner_nullsafe VALUES
		(1, 1, NULL),
		(2, 1, '2026-01-01'),
		(3, 2, NULL)`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX partial_planner_nullsafe_tenant_idx ON partial_planner_nullsafe (tenant) WHERE deleted_at IS NULL")

	nullSafeNullQuery := `SELECT count(id) FROM partial_planner_nullsafe WHERE tenant = 1 AND deleted_at IS NOT DISTINCT FROM NULL`
	assertCountResult(t, ctx, conn, nullSafeNullQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, nullSafeNullQuery, true)

	nullSafeNonNullQuery := `SELECT count(id) FROM partial_planner_nullsafe WHERE tenant = 1 AND deleted_at IS NOT DISTINCT FROM '2026-01-01'`
	assertCountResult(t, ctx, conn, nullSafeNonNullQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, nullSafeNonNullQuery, false)

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE partial_planner_distinct (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, status TEXT)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO partial_planner_distinct VALUES
		(1, 1, 'active'),
		(2, 1, 'archived'),
		(3, 1, NULL),
		(4, 2, 'pending'),
		(5, 2, 'archived')`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX partial_planner_distinct_tenant_idx ON partial_planner_distinct (tenant) WHERE status IS DISTINCT FROM 'archived'")

	distinctExactQuery := `SELECT count(id) FROM partial_planner_distinct WHERE tenant = 1 AND status IS DISTINCT FROM 'archived'`
	assertCountResult(t, ctx, conn, distinctExactQuery, 2)
	assertBenchmarkPlanShape(t, ctx, conn, distinctExactQuery, true)

	distinctValueQuery := `SELECT count(id) FROM partial_planner_distinct WHERE tenant = 1 AND status = 'active'`
	assertCountResult(t, ctx, conn, distinctValueQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, distinctValueQuery, true)

	distinctNullQuery := `SELECT count(id) FROM partial_planner_distinct WHERE tenant = 1 AND status IS NULL`
	assertCountResult(t, ctx, conn, distinctNullQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, distinctNullQuery, true)

	distinctExcludedQuery := `SELECT count(id) FROM partial_planner_distinct WHERE tenant = 1 AND status IS NOT DISTINCT FROM 'archived'`
	assertCountResult(t, ctx, conn, distinctExcludedQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, distinctExcludedQuery, false)

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE partial_planner_cross (id INTEGER PRIMARY KEY, tenant INTEGER, owner_tenant INTEGER)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO partial_planner_cross VALUES
		(1, 1, 1),
		(2, 1, 2),
		(3, 2, 2),
		(4, NULL, NULL)`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX partial_planner_cross_tenant_idx ON partial_planner_cross (tenant) WHERE tenant = owner_tenant")

	crossColumnImpliedQuery := `SELECT count(id) FROM partial_planner_cross WHERE tenant = 1 AND owner_tenant = 1`
	assertCountResult(t, ctx, conn, crossColumnImpliedQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, crossColumnImpliedQuery, true)

	crossColumnMismatchQuery := `SELECT count(id) FROM partial_planner_cross WHERE tenant = 1 AND owner_tenant = 2`
	assertCountResult(t, ctx, conn, crossColumnMismatchQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, crossColumnMismatchQuery, false)

	crossColumnNullableEqualityQuery := `SELECT count(id) FROM partial_planner_cross WHERE tenant IS NULL AND owner_tenant IS NULL`
	assertCountResult(t, ctx, conn, crossColumnNullableEqualityQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, crossColumnNullableEqualityQuery, false)

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE partial_planner_cross_nullsafe (id INTEGER PRIMARY KEY, label TEXT NOT NULL, tenant INTEGER, owner_tenant INTEGER)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO partial_planner_cross_nullsafe VALUES
		(1, 'same-value', 1, 1),
		(2, 'mismatch', 1, 2),
		(3, 'same-null', NULL, NULL)`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX partial_planner_cross_nullsafe_label_idx ON partial_planner_cross_nullsafe (label) WHERE tenant IS NOT DISTINCT FROM owner_tenant")

	crossColumnNullSafeValueQuery := `SELECT count(id) FROM partial_planner_cross_nullsafe WHERE label = 'same-value' AND tenant = 1 AND owner_tenant = 1`
	assertCountResult(t, ctx, conn, crossColumnNullSafeValueQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, crossColumnNullSafeValueQuery, true)

	crossColumnNullSafeNullQuery := `SELECT count(id) FROM partial_planner_cross_nullsafe WHERE label = 'same-null' AND tenant IS NULL AND owner_tenant IS NULL`
	assertCountResult(t, ctx, conn, crossColumnNullSafeNullQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, crossColumnNullSafeNullQuery, true)

	crossColumnNullSafeMismatchQuery := `SELECT count(id) FROM partial_planner_cross_nullsafe WHERE label = 'mismatch' AND tenant = 1 AND owner_tenant = 2`
	assertCountResult(t, ctx, conn, crossColumnNullSafeMismatchQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, crossColumnNullSafeMismatchQuery, false)

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE partial_planner_cross_chain (id INTEGER PRIMARY KEY, tenant INTEGER, workspace_tenant INTEGER, owner_tenant INTEGER)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO partial_planner_cross_chain VALUES
		(1, 1, 1, 1),
		(2, 1, 1, 2),
		(3, NULL, NULL, NULL)`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX partial_planner_cross_chain_tenant_idx ON partial_planner_cross_chain (tenant) WHERE tenant = owner_tenant")

	crossColumnChainQuery := `SELECT count(id) FROM partial_planner_cross_chain WHERE tenant = 1 AND tenant = workspace_tenant AND workspace_tenant = owner_tenant`
	assertCountResult(t, ctx, conn, crossColumnChainQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, crossColumnChainQuery, true)

	crossColumnNullSafeChainQuery := `SELECT count(id) FROM partial_planner_cross_chain WHERE tenant IS NULL AND tenant IS NOT DISTINCT FROM workspace_tenant AND workspace_tenant IS NOT DISTINCT FROM owner_tenant`
	assertCountResult(t, ctx, conn, crossColumnNullSafeChainQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, crossColumnNullSafeChainQuery, false)

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE partial_planner_cross_chain_nullsafe (id INTEGER PRIMARY KEY, label TEXT NOT NULL, tenant INTEGER, workspace_tenant INTEGER, owner_tenant INTEGER)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO partial_planner_cross_chain_nullsafe VALUES
		(1, 'same-value', 1, 1, 1),
		(2, 'mismatch', 1, 1, 2),
		(3, 'same-null', NULL, NULL, NULL)`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX partial_planner_cross_chain_nullsafe_label_idx ON partial_planner_cross_chain_nullsafe (label) WHERE tenant IS NOT DISTINCT FROM owner_tenant")

	crossColumnNullSafeChainImpliedQuery := `SELECT count(id) FROM partial_planner_cross_chain_nullsafe WHERE label = 'same-null' AND tenant IS NOT DISTINCT FROM workspace_tenant AND workspace_tenant IS NOT DISTINCT FROM owner_tenant`
	assertCountResult(t, ctx, conn, crossColumnNullSafeChainImpliedQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, crossColumnNullSafeChainImpliedQuery, true)

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE partial_planner_trim (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, code TEXT)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO partial_planner_trim VALUES
		(1, 1, ' active'),
		(2, 1, 'active '),
		(3, 1, 'archived'),
		(4, 2, ' active')`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX partial_planner_trim_tenant_idx ON partial_planner_trim (tenant) WHERE ltrim(code) = 'active'")

	trimImpliedQuery := `SELECT count(id) FROM partial_planner_trim WHERE tenant = 1 AND ltrim(code) = 'active'`
	assertCountResult(t, ctx, conn, trimImpliedQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, trimImpliedQuery, true)

	trimNonEquivalentQuery := `SELECT count(id) FROM partial_planner_trim WHERE tenant = 1 AND rtrim(code) = 'active'`
	assertCountResult(t, ctx, conn, trimNonEquivalentQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, trimNonEquivalentQuery, false)

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE partial_planner_btrim (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, code TEXT)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO partial_planner_btrim VALUES
		(1, 1, ' active '),
		(2, 1, 'archived'),
		(3, 2, ' active ')`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX partial_planner_btrim_tenant_idx ON partial_planner_btrim (tenant) WHERE btrim(code) = 'active'")

	btrimImpliedQuery := `SELECT count(id) FROM partial_planner_btrim WHERE tenant = 1 AND btrim(code) = 'active'`
	assertCountResult(t, ctx, conn, btrimImpliedQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, btrimImpliedQuery, true)

	btrimNonMatchingQuery := `SELECT count(id) FROM partial_planner_btrim WHERE tenant = 1 AND btrim(code) = 'archived'`
	assertCountResult(t, ctx, conn, btrimNonMatchingQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, btrimNonMatchingQuery, false)

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE partial_planner_length (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, code TEXT)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO partial_planner_length VALUES
		(1, 1, 'active'),
		(2, 1, 'archived'),
		(3, 2, 'common')`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX partial_planner_length_tenant_idx ON partial_planner_length (tenant) WHERE length(code) = 6")

	lengthAliasImpliedQuery := `SELECT count(id) FROM partial_planner_length WHERE tenant = 1 AND char_length(code) = 6`
	assertCountResult(t, ctx, conn, lengthAliasImpliedQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, lengthAliasImpliedQuery, true)

	lengthNonMatchingQuery := `SELECT count(id) FROM partial_planner_length WHERE tenant = 1 AND length(code) = 8`
	assertCountResult(t, ctx, conn, lengthNonMatchingQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, lengthNonMatchingQuery, false)

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE partial_planner_octet (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, code TEXT)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO partial_planner_octet VALUES
		(1, 1, 'abc'),
		(2, 1, 'de'),
		(3, 2, 'xyz')`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX partial_planner_octet_tenant_idx ON partial_planner_octet (tenant) WHERE octet_length(code) = 3")

	octetImpliedQuery := `SELECT count(id) FROM partial_planner_octet WHERE tenant = 1 AND octet_length(code) = 3`
	assertCountResult(t, ctx, conn, octetImpliedQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, octetImpliedQuery, true)

	octetLengthNonEquivalentQuery := `SELECT count(id) FROM partial_planner_octet WHERE tenant = 1 AND length(code) = 3`
	assertCountResult(t, ctx, conn, octetLengthNonEquivalentQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, octetLengthNonEquivalentQuery, false)

	octetNonMatchingQuery := `SELECT count(id) FROM partial_planner_octet WHERE tenant = 1 AND octet_length(code) = 2`
	assertCountResult(t, ctx, conn, octetNonMatchingQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, octetNonMatchingQuery, false)

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE partial_planner_bit (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, code TEXT)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO partial_planner_bit VALUES
		(1, 1, 'abc'),
		(2, 1, 'de'),
		(3, 2, 'xyz')`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX partial_planner_bit_tenant_idx ON partial_planner_bit (tenant) WHERE bit_length(code) = 24")

	bitImpliedQuery := `SELECT count(id) FROM partial_planner_bit WHERE tenant = 1 AND bit_length(code) = 24`
	assertCountResult(t, ctx, conn, bitImpliedQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, bitImpliedQuery, true)

	bitOctetNonEquivalentQuery := `SELECT count(id) FROM partial_planner_bit WHERE tenant = 1 AND octet_length(code) = 3`
	assertCountResult(t, ctx, conn, bitOctetNonEquivalentQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, bitOctetNonEquivalentQuery, false)

	bitNonMatchingQuery := `SELECT count(id) FROM partial_planner_bit WHERE tenant = 1 AND bit_length(code) = 16`
	assertCountResult(t, ctx, conn, bitNonMatchingQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, bitNonMatchingQuery, false)

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE partial_planner_strpos (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, code TEXT)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO partial_planner_strpos VALUES
		(1, 1, 'active-a'),
		(2, 1, 'inactive'),
		(3, 1, 'pending'),
		(4, 2, 'active-b')`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX partial_planner_strpos_tenant_idx ON partial_planner_strpos (tenant) WHERE strpos(code, 'active') = 1")

	strposImpliedQuery := `SELECT count(id) FROM partial_planner_strpos WHERE tenant = 1 AND strpos(code, 'active') = 1`
	assertCountResult(t, ctx, conn, strposImpliedQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, strposImpliedQuery, true)

	strposNonMatchingQuery := `SELECT count(id) FROM partial_planner_strpos WHERE tenant = 1 AND strpos(code, 'active') = 3`
	assertCountResult(t, ctx, conn, strposNonMatchingQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, strposNonMatchingQuery, false)

	strposWrongNeedleQuery := `SELECT count(id) FROM partial_planner_strpos WHERE tenant = 1 AND strpos(code, 'pending') = 1`
	assertCountResult(t, ctx, conn, strposWrongNeedleQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, strposWrongNeedleQuery, false)

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE partial_planner_starts_with (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, code TEXT)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO partial_planner_starts_with VALUES
		(1, 1, 'active-a'),
		(2, 1, 'inactive'),
		(3, 1, 'pending'),
		(4, 2, 'active-b')`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX partial_planner_starts_with_tenant_idx ON partial_planner_starts_with (tenant) WHERE starts_with(code, 'active')")

	startsWithImpliedQuery := `SELECT count(id) FROM partial_planner_starts_with WHERE tenant = 1 AND starts_with(code, 'active') = true`
	assertCountResult(t, ctx, conn, startsWithImpliedQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, startsWithImpliedQuery, true)

	startsWithWrongPrefixQuery := `SELECT count(id) FROM partial_planner_starts_with WHERE tenant = 1 AND starts_with(code, 'pending')`
	assertCountResult(t, ctx, conn, startsWithWrongPrefixQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, startsWithWrongPrefixQuery, false)

	startsWithFalseQuery := `SELECT count(id) FROM partial_planner_starts_with WHERE tenant = 1 AND NOT starts_with(code, 'active')`
	assertCountResult(t, ctx, conn, startsWithFalseQuery, 2)
	assertBenchmarkPlanShape(t, ctx, conn, startsWithFalseQuery, false)

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE partial_planner_like_prefix (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, code TEXT)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO partial_planner_like_prefix VALUES
		(1, 1, 'active-a'),
		(2, 1, 'inactive'),
		(3, 1, 'pending'),
		(4, 2, 'active-b')`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX partial_planner_like_prefix_tenant_idx ON partial_planner_like_prefix (tenant) WHERE code LIKE 'active%'")

	likePrefixImpliedQuery := `SELECT count(id) FROM partial_planner_like_prefix WHERE tenant = 1 AND code LIKE 'active-a%'`
	assertCountResult(t, ctx, conn, likePrefixImpliedQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, likePrefixImpliedQuery, true)

	likePrefixEqualityQuery := `SELECT count(id) FROM partial_planner_like_prefix WHERE tenant = 1 AND code = 'active-a'`
	assertCountResult(t, ctx, conn, likePrefixEqualityQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, likePrefixEqualityQuery, true)

	likePrefixWrongPrefixQuery := `SELECT count(id) FROM partial_planner_like_prefix WHERE tenant = 1 AND code LIKE 'pending%'`
	assertCountResult(t, ctx, conn, likePrefixWrongPrefixQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, likePrefixWrongPrefixQuery, false)

	likePrefixUnsafeQuery := `SELECT count(id) FROM partial_planner_like_prefix WHERE tenant = 1 AND code LIKE 'act_ve%'`
	assertCountResult(t, ctx, conn, likePrefixUnsafeQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, likePrefixUnsafeQuery, false)

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE partial_planner_left (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, code TEXT)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO partial_planner_left VALUES
		(1, 1, 'åctive-a'),
		(2, 1, 'åctor'),
		(3, 1, 'archive'),
		(4, 2, 'åctive-b')`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX partial_planner_left_tenant_idx ON partial_planner_left (tenant) WHERE left(code, 2) = 'åc'")

	leftImpliedQuery := `SELECT count(id) FROM partial_planner_left WHERE tenant = 1 AND left(code, 2) = 'åc'`
	assertCountResult(t, ctx, conn, leftImpliedQuery, 2)
	assertBenchmarkPlanShape(t, ctx, conn, leftImpliedQuery, true)

	leftWrongLengthQuery := `SELECT count(id) FROM partial_planner_left WHERE tenant = 1 AND left(code, 4) = 'åcti'`
	assertCountResult(t, ctx, conn, leftWrongLengthQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, leftWrongLengthQuery, false)

	leftSemanticQuery := `SELECT count(id) FROM partial_planner_left WHERE tenant = 1 AND code = 'åctive-a'`
	assertCountResult(t, ctx, conn, leftSemanticQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, leftSemanticQuery, false)

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE partial_planner_right (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, code TEXT)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO partial_planner_right VALUES
		(1, 1, 'active'),
		(2, 1, 'bctive'),
		(3, 1, 'inactive'),
		(4, 1, 'åctive'),
		(5, 2, 'cctive')`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX partial_planner_right_tenant_idx ON partial_planner_right (tenant) WHERE right(code, -1) = 'ctive'")

	rightImpliedQuery := `SELECT count(id) FROM partial_planner_right WHERE tenant = 1 AND right(code, -1) = 'ctive'`
	assertCountResult(t, ctx, conn, rightImpliedQuery, 3)
	assertBenchmarkPlanShape(t, ctx, conn, rightImpliedQuery, true)

	rightWrongLengthQuery := `SELECT count(id) FROM partial_planner_right WHERE tenant = 1 AND right(code, 2) = 've'`
	assertCountResult(t, ctx, conn, rightWrongLengthQuery, 4)
	assertBenchmarkPlanShape(t, ctx, conn, rightWrongLengthQuery, false)

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE partial_planner_replace (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, code TEXT)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO partial_planner_replace VALUES
		(1, 1, 'active-a'),
		(2, 1, 'active--a'),
		(3, 1, 'active_b'),
		(4, 2, 'active-a')`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX partial_planner_replace_tenant_idx ON partial_planner_replace (tenant) WHERE replace(code, '-', '') = 'activea'")

	replaceImpliedQuery := `SELECT count(id) FROM partial_planner_replace WHERE tenant = 1 AND replace(code, '-', '') = 'activea'`
	assertCountResult(t, ctx, conn, replaceImpliedQuery, 2)
	assertBenchmarkPlanShape(t, ctx, conn, replaceImpliedQuery, true)

	replaceWrongArgumentQuery := `SELECT count(id) FROM partial_planner_replace WHERE tenant = 1 AND replace(code, '_', '') = 'active-a'`
	assertCountResult(t, ctx, conn, replaceWrongArgumentQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, replaceWrongArgumentQuery, false)

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE partial_planner_translate (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, code TEXT)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO partial_planner_translate VALUES
		(1, 1, 'active-a'),
		(2, 1, 'active__a'),
		(3, 1, 'active.a'),
		(4, 2, 'active-a')`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX partial_planner_translate_tenant_idx ON partial_planner_translate (tenant) WHERE translate(code, '-_', '') = 'activea'")

	translateImpliedQuery := `SELECT count(id) FROM partial_planner_translate WHERE tenant = 1 AND translate(code, '-_', '') = 'activea'`
	assertCountResult(t, ctx, conn, translateImpliedQuery, 2)
	assertBenchmarkPlanShape(t, ctx, conn, translateImpliedQuery, true)

	translateWrongArgumentQuery := `SELECT count(id) FROM partial_planner_translate WHERE tenant = 1 AND translate(code, '-.', '') = 'activea'`
	assertCountResult(t, ctx, conn, translateWrongArgumentQuery, 2)
	assertBenchmarkPlanShape(t, ctx, conn, translateWrongArgumentQuery, false)

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE partial_planner_md5 (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, code TEXT)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO partial_planner_md5 VALUES
		(1, 1, 'active'),
		(2, 1, 'pending'),
		(3, 1, 'ACTIVE'),
		(4, 2, 'active')`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX partial_planner_md5_tenant_idx ON partial_planner_md5 (tenant) WHERE md5(code) = 'c76a5e84e4bdee527e274ea30c680d79'")

	md5ImpliedQuery := `SELECT count(id) FROM partial_planner_md5 WHERE tenant = 1 AND md5(code) = 'c76a5e84e4bdee527e274ea30c680d79'`
	assertCountResult(t, ctx, conn, md5ImpliedQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, md5ImpliedQuery, true)

	md5RawSourceQuery := `SELECT count(id) FROM partial_planner_md5 WHERE tenant = 1 AND code = 'active'`
	assertCountResult(t, ctx, conn, md5RawSourceQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, md5RawSourceQuery, false)

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE partial_planner_split_part (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, email TEXT)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO partial_planner_split_part VALUES
		(1, 1, 'first@example.com'),
		(2, 1, 'second@example.org'),
		(3, 1, 'missing-domain'),
		(4, 2, 'other@example.com')`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX partial_planner_split_part_tenant_idx ON partial_planner_split_part (tenant) WHERE split_part(email, '@', 2) = 'example.com'")

	splitPartImpliedQuery := `SELECT count(id) FROM partial_planner_split_part WHERE tenant = 1 AND split_part(email, '@', 2) = 'example.com'`
	assertCountResult(t, ctx, conn, splitPartImpliedQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, splitPartImpliedQuery, true)

	splitPartWrongArgumentQuery := `SELECT count(id) FROM partial_planner_split_part WHERE tenant = 1 AND split_part(email, '.', 2) = 'com'`
	assertCountResult(t, ctx, conn, splitPartWrongArgumentQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, splitPartWrongArgumentQuery, false)

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE partial_planner_ascii (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, code TEXT)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO partial_planner_ascii VALUES
		(1, 1, 'Alpha'),
		(2, 1, 'beta'),
		(3, 1, 'Admin'),
		(4, 2, 'Active')`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX partial_planner_ascii_tenant_idx ON partial_planner_ascii (tenant) WHERE ascii(code) = 65")

	asciiImpliedQuery := `SELECT count(id) FROM partial_planner_ascii WHERE tenant = 1 AND ascii(code) = 65`
	assertCountResult(t, ctx, conn, asciiImpliedQuery, 2)
	assertBenchmarkPlanShape(t, ctx, conn, asciiImpliedQuery, true)

	asciiRawSourceQuery := `SELECT count(id) FROM partial_planner_ascii WHERE tenant = 1 AND code = 'Alpha'`
	assertCountResult(t, ctx, conn, asciiRawSourceQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, asciiRawSourceQuery, false)

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE partial_planner_coalesce (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, status TEXT)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO partial_planner_coalesce VALUES
		(1, 1, 'active'),
		(2, 1, NULL),
		(3, 2, 'active')`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX partial_planner_coalesce_tenant_idx ON partial_planner_coalesce (tenant) WHERE coalesce(status, 'inactive') = 'active'")

	coalesceImpliedQuery := `SELECT count(id) FROM partial_planner_coalesce WHERE tenant = 1 AND coalesce(status, 'inactive') = 'active'`
	assertCountResult(t, ctx, conn, coalesceImpliedQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, coalesceImpliedQuery, true)

	coalesceSemanticQuery := `SELECT count(id) FROM partial_planner_coalesce WHERE tenant = 1 AND status = 'active'`
	assertCountResult(t, ctx, conn, coalesceSemanticQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, coalesceSemanticQuery, false)

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE partial_planner_abs (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, delta BIGINT)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO partial_planner_abs VALUES
		(1, 1, -10),
		(2, 1, 10),
		(3, 1, 5),
		(4, 2, -10)`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX partial_planner_abs_tenant_idx ON partial_planner_abs (tenant) WHERE abs(delta) = 10")

	absImpliedQuery := `SELECT count(id) FROM partial_planner_abs WHERE tenant = 1 AND abs(delta) = 10`
	assertCountResult(t, ctx, conn, absImpliedQuery, 2)
	assertBenchmarkPlanShape(t, ctx, conn, absImpliedQuery, true)

	absSignSensitiveQuery := `SELECT count(id) FROM partial_planner_abs WHERE tenant = 1 AND delta = 10`
	assertCountResult(t, ctx, conn, absSignSensitiveQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, absSignSensitiveQuery, false)

	absNonMatchingQuery := `SELECT count(id) FROM partial_planner_abs WHERE tenant = 1 AND abs(delta) = 5`
	assertCountResult(t, ctx, conn, absNonMatchingQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, absNonMatchingQuery, false)
}
