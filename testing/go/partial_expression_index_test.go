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
			Name: "partial UNIQUE index supports NULLIF predicate",
			SetUpScript: []string{
				`CREATE TABLE nullif_codes (id INT PRIMARY KEY, user_id INT, code TEXT);`,
				`CREATE UNIQUE INDEX nullif_codes_user_idx
					ON nullif_codes (user_id)
					WHERE nullif(code, '') = 'active';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO nullif_codes VALUES
						(1, 10, 'active'),
						(2, 10, ''),
						(3, 10, '');`,
				},
				{
					Query:       `INSERT INTO nullif_codes VALUES (4, 10, 'active');`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query:       `UPDATE nullif_codes SET code = 'active' WHERE id = 2;`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: `INSERT INTO nullif_codes VALUES (5, 10, 'pending');`,
				},
			},
		},
		{
			Name: "partial UNIQUE index supports arithmetic expression predicates",
			SetUpScript: []string{
				`CREATE TABLE arithmetic_plus_scores (id INT PRIMARY KEY, user_id INT, score INT);`,
				`CREATE UNIQUE INDEX arithmetic_plus_scores_user_idx
					ON arithmetic_plus_scores (user_id)
					WHERE score + 1 = 8;`,
				`CREATE TABLE arithmetic_minus_scores (id INT PRIMARY KEY, user_id INT, score INT);`,
				`CREATE UNIQUE INDEX arithmetic_minus_scores_user_idx
					ON arithmetic_minus_scores (user_id)
					WHERE score - 1 = 6;`,
				`CREATE TABLE arithmetic_mult_scores (id INT PRIMARY KEY, user_id INT, score INT);`,
				`CREATE UNIQUE INDEX arithmetic_mult_scores_user_idx
					ON arithmetic_mult_scores (user_id)
					WHERE score * 2 = 14;`,
				`CREATE TABLE arithmetic_commuted_plus_scores (id INT PRIMARY KEY, user_id INT, score INT);`,
				`CREATE UNIQUE INDEX arithmetic_commuted_plus_scores_user_idx
					ON arithmetic_commuted_plus_scores (user_id)
					WHERE 1 + score = 8;`,
				`CREATE TABLE arithmetic_commuted_mult_scores (id INT PRIMARY KEY, user_id INT, score INT);`,
				`CREATE UNIQUE INDEX arithmetic_commuted_mult_scores_user_idx
					ON arithmetic_commuted_mult_scores (user_id)
					WHERE 2 * score = 14;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO arithmetic_plus_scores VALUES
						(1, 10, 7),
						(2, 10, 8),
						(3, 10, NULL);`,
				},
				{
					Query:       `INSERT INTO arithmetic_plus_scores VALUES (4, 10, 7);`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query:       `UPDATE arithmetic_plus_scores SET score = 7 WHERE id = 2;`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: `INSERT INTO arithmetic_plus_scores VALUES (5, 10, 9);`,
				},
				{
					Query: `INSERT INTO arithmetic_minus_scores VALUES
						(1, 20, 7),
						(2, 20, 8);`,
				},
				{
					Query:       `INSERT INTO arithmetic_minus_scores VALUES (3, 20, 7);`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: `INSERT INTO arithmetic_mult_scores VALUES
						(1, 30, 7),
						(2, 30, 8);`,
				},
				{
					Query:       `INSERT INTO arithmetic_mult_scores VALUES (3, 30, 7);`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: `INSERT INTO arithmetic_commuted_plus_scores VALUES
						(1, 40, 7),
						(2, 40, 8);`,
				},
				{
					Query:       `INSERT INTO arithmetic_commuted_plus_scores VALUES (3, 40, 7);`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: `INSERT INTO arithmetic_commuted_mult_scores VALUES
						(1, 50, 7),
						(2, 50, 8);`,
				},
				{
					Query:       `INSERT INTO arithmetic_commuted_mult_scores VALUES (3, 50, 7);`,
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
			Name: "partial UNIQUE index supports custom trim predicates",
			SetUpScript: []string{
				`CREATE TABLE custom_ltrim_codes (id INT PRIMARY KEY, user_id INT, code TEXT);`,
				`CREATE UNIQUE INDEX custom_ltrim_codes_user_code_idx
					ON custom_ltrim_codes (user_id)
					WHERE ltrim(code, '0_') = 'active';`,
				`CREATE TABLE custom_rtrim_codes (id INT PRIMARY KEY, user_id INT, code TEXT);`,
				`CREATE UNIQUE INDEX custom_rtrim_codes_user_code_idx
					ON custom_rtrim_codes (user_id)
					WHERE rtrim(code, '_') = 'active';`,
				`CREATE TABLE custom_btrim_codes (id INT PRIMARY KEY, user_id INT, code TEXT);`,
				`CREATE UNIQUE INDEX custom_btrim_codes_user_code_idx
					ON custom_btrim_codes (user_id)
					WHERE btrim(code, 'x_') = 'active';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO custom_ltrim_codes VALUES
						(1, 10, '_0active'),
						(2, 10, 'pending');`,
				},
				{
					Query:       `INSERT INTO custom_ltrim_codes VALUES (3, 10, '00active');`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: `INSERT INTO custom_ltrim_codes VALUES (4, 10, '-active');`,
				},
				{
					Query:       `UPDATE custom_ltrim_codes SET code = '0_active' WHERE id = 2;`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: `INSERT INTO custom_rtrim_codes VALUES
						(1, 20, 'active__'),
						(2, 20, 'pending');`,
				},
				{
					Query:       `INSERT INTO custom_rtrim_codes VALUES (3, 20, 'active_');`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: `INSERT INTO custom_rtrim_codes VALUES (4, 20, 'active-');`,
				},
				{
					Query:       `UPDATE custom_rtrim_codes SET code = 'active_' WHERE id = 2;`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: `INSERT INTO custom_btrim_codes VALUES
						(1, 30, 'x_active_'),
						(2, 30, 'pending');`,
				},
				{
					Query:       `INSERT INTO custom_btrim_codes VALUES (3, 30, '_activex');`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: `INSERT INTO custom_btrim_codes VALUES (4, 30, 'yactive');`,
				},
				{
					Query:       `UPDATE custom_btrim_codes SET code = 'xactivex' WHERE id = 2;`,
					ExpectedErr: "duplicate unique key given",
				},
			},
		},
		{
			Name: "partial UNIQUE index supports repeat predicate",
			SetUpScript: []string{
				`CREATE TABLE repeat_codes (id INT PRIMARY KEY, user_id INT, code TEXT);`,
				`CREATE UNIQUE INDEX repeat_codes_user_code_idx
					ON repeat_codes (user_id)
					WHERE repeat(code, 2) = 'activeactive';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO repeat_codes VALUES
						(1, 10, 'active'),
						(2, 10, 'pending');`,
				},
				{
					Query:       `INSERT INTO repeat_codes VALUES (3, 10, 'active');`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: `INSERT INTO repeat_codes VALUES (4, 10, 'activeactive');`,
				},
				{
					Query:       `UPDATE repeat_codes SET code = 'active' WHERE id = 2;`,
					ExpectedErr: "duplicate unique key given",
				},
			},
		},
		{
			Name: "partial UNIQUE index supports concat predicate",
			SetUpScript: []string{
				`CREATE TABLE concat_codes (id INT PRIMARY KEY, user_id INT, prefix TEXT, code TEXT);`,
				`CREATE UNIQUE INDEX concat_codes_user_code_idx
					ON concat_codes (user_id)
					WHERE concat(prefix, '-', code) = 'acct-active';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO concat_codes VALUES
						(1, 10, 'acct', 'active'),
						(2, 10, 'acct', 'pending'),
						(3, 10, NULL, 'active');`,
				},
				{
					Query:       `INSERT INTO concat_codes VALUES (4, 10, 'acct', 'active');`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: `INSERT INTO concat_codes VALUES (5, 10, 'acctactive', '');`,
				},
				{
					Query:       `UPDATE concat_codes SET code = 'active' WHERE id = 2;`,
					ExpectedErr: "duplicate unique key given",
				},
			},
		},
		{
			Name: "partial UNIQUE index supports sign predicate",
			SetUpScript: []string{
				`CREATE TABLE signed_scores (id INT PRIMARY KEY, user_id INT, delta BIGINT);`,
				`CREATE UNIQUE INDEX signed_scores_user_delta_idx
					ON signed_scores (user_id)
					WHERE sign(delta) = 1;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO signed_scores VALUES
						(1, 10, 5),
						(2, 10, -5);`,
				},
				{
					Query:       `INSERT INTO signed_scores VALUES (3, 10, 20);`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: `INSERT INTO signed_scores VALUES (4, 10, 0);`,
				},
				{
					Query:       `UPDATE signed_scores SET delta = 1 WHERE id = 2;`,
					ExpectedErr: "duplicate unique key given",
				},
			},
		},
		{
			Name: "partial UNIQUE index supports gcd predicate",
			SetUpScript: []string{
				`CREATE TABLE gcd_scores (id INT PRIMARY KEY, user_id INT, width BIGINT, height BIGINT);`,
				`CREATE UNIQUE INDEX gcd_scores_user_dims_idx
					ON gcd_scores (user_id)
					WHERE gcd(width, height) = 4;`,
				`CREATE TABLE gcd_commuted_scores (id INT PRIMARY KEY, user_id INT, width BIGINT, height BIGINT);`,
				`CREATE UNIQUE INDEX gcd_commuted_scores_user_dims_idx
					ON gcd_commuted_scores (user_id)
					WHERE gcd(height, width) = 4;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO gcd_scores VALUES
						(1, 10, 8, 12),
						(2, 10, 9, 6);`,
				},
				{
					Query:       `INSERT INTO gcd_scores VALUES (3, 10, 16, 20);`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: `INSERT INTO gcd_scores VALUES (4, 10, 6, 10);`,
				},
				{
					Query:       `UPDATE gcd_scores SET width = 12, height = 16 WHERE id = 2;`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: `INSERT INTO gcd_commuted_scores VALUES
						(1, 20, 8, 12),
						(2, 20, 9, 6);`,
				},
				{
					Query:       `INSERT INTO gcd_commuted_scores VALUES (3, 20, 16, 20);`,
					ExpectedErr: "duplicate unique key given",
				},
			},
		},
		{
			Name: "partial UNIQUE index supports lcm predicate",
			SetUpScript: []string{
				`CREATE TABLE lcm_scores (id INT PRIMARY KEY, user_id INT, width BIGINT, height BIGINT);`,
				`CREATE UNIQUE INDEX lcm_scores_user_dims_idx
					ON lcm_scores (user_id)
					WHERE lcm(width, height) = 12;`,
				`CREATE TABLE lcm_commuted_scores (id INT PRIMARY KEY, user_id INT, width BIGINT, height BIGINT);`,
				`CREATE UNIQUE INDEX lcm_commuted_scores_user_dims_idx
					ON lcm_commuted_scores (user_id)
					WHERE lcm(height, width) = 12;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO lcm_scores VALUES
						(1, 10, 3, 4),
						(2, 10, 5, 6);`,
				},
				{
					Query:       `INSERT INTO lcm_scores VALUES (3, 10, 4, 6);`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: `INSERT INTO lcm_scores VALUES (4, 10, 5, 10);`,
				},
				{
					Query:       `UPDATE lcm_scores SET width = 6, height = 12 WHERE id = 2;`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query:       `INSERT INTO lcm_scores VALUES (5, 11, 9223372036854775807, 9223372036854775806);`,
					ExpectedErr: "bigint out of range",
				},
				{
					Query: `INSERT INTO lcm_commuted_scores VALUES
						(1, 20, 3, 4),
						(2, 20, 5, 6);`,
				},
				{
					Query:       `INSERT INTO lcm_commuted_scores VALUES (3, 20, 4, 6);`,
					ExpectedErr: "duplicate unique key given",
				},
			},
		},
		{
			Name: "partial UNIQUE index supports mod predicate",
			SetUpScript: []string{
				`CREATE TABLE mod_scores (id INT PRIMARY KEY, user_id INT, account_id BIGINT, shard_count BIGINT);`,
				`CREATE UNIQUE INDEX mod_scores_user_shard_idx
					ON mod_scores (user_id)
					WHERE mod(account_id, shard_count) = 1;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO mod_scores VALUES
						(1, 10, 7, 3),
						(2, 10, 8, 3);`,
				},
				{
					Query:       `INSERT INTO mod_scores VALUES (3, 10, 10, 3);`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: `INSERT INTO mod_scores VALUES (4, 10, 11, 3);`,
				},
				{
					Query:       `UPDATE mod_scores SET account_id = 10 WHERE id = 2;`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query:       `INSERT INTO mod_scores VALUES (5, 11, 3, 0);`,
					ExpectedErr: "division by zero",
				},
			},
		},
		{
			Name: "partial UNIQUE index supports chr predicate",
			SetUpScript: []string{
				`CREATE TABLE chr_codes (id INT PRIMARY KEY, user_id INT, codepoint INT);`,
				`CREATE UNIQUE INDEX chr_codes_user_code_idx
					ON chr_codes (user_id)
					WHERE chr(codepoint) = 'A';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO chr_codes VALUES
						(1, 10, 65),
						(2, 10, 66);`,
				},
				{
					Query:       `INSERT INTO chr_codes VALUES (3, 10, 65);`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: `INSERT INTO chr_codes VALUES (4, 10, 67);`,
				},
				{
					Query:       `UPDATE chr_codes SET codepoint = 65 WHERE id = 2;`,
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
			Name: "partial UNIQUE index supports hashtext predicate",
			SetUpScript: []string{
				`CREATE TABLE hashtext_codes (id INT PRIMARY KEY, user_id INT, code TEXT);`,
				`CREATE UNIQUE INDEX hashtext_codes_user_code_idx
					ON hashtext_codes (user_id)
					WHERE hashtext(code) = -785388649;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO hashtext_codes VALUES
						(1, 10, 'abc'),
						(2, 10, 'pending');`,
				},
				{
					Query:       `INSERT INTO hashtext_codes VALUES (3, 10, 'abc');`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: `INSERT INTO hashtext_codes VALUES (4, 10, 'ABC');`,
				},
				{
					Query:       `UPDATE hashtext_codes SET code = 'abc' WHERE id = 2;`,
					ExpectedErr: "duplicate unique key given",
				},
			},
		},
		{
			Name: "partial UNIQUE index supports floor and ceiling predicates",
			SetUpScript: []string{
				`CREATE TABLE rounded_floor_scores (id INT PRIMARY KEY, user_id INT, score INT);`,
				`CREATE UNIQUE INDEX rounded_floor_scores_user_idx
					ON rounded_floor_scores (user_id)
					WHERE floor(score) = 7;`,
				`CREATE TABLE rounded_ceiling_scores (id INT PRIMARY KEY, user_id INT, score INT);`,
				`CREATE UNIQUE INDEX rounded_ceiling_scores_user_idx
					ON rounded_ceiling_scores (user_id)
					WHERE ceiling(score) = 9;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO rounded_floor_scores VALUES
						(1, 10, 7),
						(2, 10, 8);`,
				},
				{
					Query:       `INSERT INTO rounded_floor_scores VALUES (3, 10, 7);`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query:       `UPDATE rounded_floor_scores SET score = 7 WHERE id = 2;`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: `INSERT INTO rounded_ceiling_scores VALUES
						(1, 20, 9),
						(2, 20, 10);`,
				},
				{
					Query:       `INSERT INTO rounded_ceiling_scores VALUES (3, 20, 9);`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query:       `UPDATE rounded_ceiling_scores SET score = 9 WHERE id = 2;`,
					ExpectedErr: "duplicate unique key given",
				},
			},
		},
		{
			Name: "partial UNIQUE index supports round and trunc predicates",
			SetUpScript: []string{
				`CREATE TABLE rounded_scores (id INT PRIMARY KEY, user_id INT, score INT);`,
				`CREATE UNIQUE INDEX rounded_scores_user_idx
					ON rounded_scores (user_id)
					WHERE round(score) = 7;`,
				`CREATE TABLE truncated_scores (id INT PRIMARY KEY, user_id INT, score INT);`,
				`CREATE UNIQUE INDEX truncated_scores_user_idx
					ON truncated_scores (user_id)
					WHERE trunc(score) = 9;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO rounded_scores VALUES
						(1, 10, 7),
						(2, 10, 8);`,
				},
				{
					Query:       `INSERT INTO rounded_scores VALUES (3, 10, 7);`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query:       `UPDATE rounded_scores SET score = 7 WHERE id = 2;`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: `INSERT INTO truncated_scores VALUES
						(1, 20, 9),
						(2, 20, 10);`,
				},
				{
					Query:       `INSERT INTO truncated_scores VALUES (3, 20, 9);`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query:       `UPDATE truncated_scores SET score = 9 WHERE id = 2;`,
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
			Name: "partial UNIQUE index supports substring predicate",
			SetUpScript: []string{
				`CREATE TABLE substring_codes (id INT PRIMARY KEY, user_id INT, code TEXT);`,
				`CREATE UNIQUE INDEX substring_codes_user_prefix_idx
					ON substring_codes (user_id)
					WHERE substring(code, 1, 3) = 'Adm';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO substring_codes VALUES
						(1, 10, 'Admin'),
						(2, 10, 'Alpha');`,
				},
				{
					Query:       `INSERT INTO substring_codes VALUES (3, 10, 'Admiral');`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: `INSERT INTO substring_codes VALUES (4, 10, 'admin');`,
				},
				{
					Query:       `UPDATE substring_codes SET code = 'Admire' WHERE id = 2;`,
					ExpectedErr: "duplicate unique key given",
				},
			},
		},
		{
			Name: "partial UNIQUE index supports lpad predicate",
			SetUpScript: []string{
				`CREATE TABLE lpad_codes (id INT PRIMARY KEY, user_id INT, code TEXT);`,
				`CREATE UNIQUE INDEX lpad_codes_user_code_idx
					ON lpad_codes (user_id)
					WHERE lpad(code, 6, '0') = '00ABCD';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO lpad_codes VALUES
						(1, 10, 'ABCD'),
						(2, 10, 'XYZ');`,
				},
				{
					Query:       `INSERT INTO lpad_codes VALUES (3, 10, 'ABCD');`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: `INSERT INTO lpad_codes VALUES (4, 10, 'AXYZ');`,
				},
				{
					Query:       `UPDATE lpad_codes SET code = 'ABCD' WHERE id = 2;`,
					ExpectedErr: "duplicate unique key given",
				},
			},
		},
		{
			Name: "partial UNIQUE index supports rpad predicate",
			SetUpScript: []string{
				`CREATE TABLE rpad_codes (id INT PRIMARY KEY, user_id INT, code TEXT);`,
				`CREATE UNIQUE INDEX rpad_codes_user_code_idx
					ON rpad_codes (user_id)
					WHERE rpad(code, 6, '_') = 'ABCD__';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO rpad_codes VALUES
						(1, 10, 'ABCD'),
						(2, 10, 'XYZ');`,
				},
				{
					Query:       `INSERT INTO rpad_codes VALUES (3, 10, 'ABCD');`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: `INSERT INTO rpad_codes VALUES (4, 10, 'AXYZ');`,
				},
				{
					Query:       `UPDATE rpad_codes SET code = 'ABCD' WHERE id = 2;`,
					ExpectedErr: "duplicate unique key given",
				},
			},
		},
		{
			Name: "partial UNIQUE index supports reverse predicate",
			SetUpScript: []string{
				`CREATE TABLE reverse_codes (id INT PRIMARY KEY, user_id INT, code TEXT);`,
				`CREATE UNIQUE INDEX reverse_codes_user_code_idx
					ON reverse_codes (user_id)
					WHERE reverse(code) = 'nimdA';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO reverse_codes VALUES
						(1, 10, 'Admin'),
						(2, 10, 'Alpha');`,
				},
				{
					Query:       `INSERT INTO reverse_codes VALUES (3, 10, 'Admin');`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: `INSERT INTO reverse_codes VALUES (4, 10, 'admin');`,
				},
				{
					Query:       `UPDATE reverse_codes SET code = 'Admin' WHERE id = 2;`,
					ExpectedErr: "duplicate unique key given",
				},
			},
		},
		{
			Name: "partial UNIQUE index supports to_hex predicate",
			SetUpScript: []string{
				`CREATE TABLE hex_codes (id INT PRIMARY KEY, user_id INT, account_id INT);`,
				`CREATE UNIQUE INDEX hex_codes_user_account_idx
					ON hex_codes (user_id)
					WHERE to_hex(account_id) = 'a';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO hex_codes VALUES
						(1, 10, 10),
						(2, 10, 11);`,
				},
				{
					Query:       `INSERT INTO hex_codes VALUES (3, 10, 10);`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: `INSERT INTO hex_codes VALUES (4, 10, 12);`,
				},
				{
					Query:       `UPDATE hex_codes SET account_id = 10 WHERE id = 2;`,
					ExpectedErr: "duplicate unique key given",
				},
			},
		},
		{
			Name: "partial UNIQUE index supports initcap predicate",
			SetUpScript: []string{
				`CREATE TABLE initcap_roles (id INT PRIMARY KEY, user_id INT, role TEXT);`,
				`CREATE UNIQUE INDEX initcap_roles_user_role_idx
					ON initcap_roles (user_id)
					WHERE initcap(role) = 'Admin User';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO initcap_roles VALUES
						(1, 10, 'admin user'),
						(2, 10, 'regular user');`,
				},
				{
					Query:       `INSERT INTO initcap_roles VALUES (3, 10, 'admin user');`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: `INSERT INTO initcap_roles VALUES (4, 10, 'billing user');`,
				},
				{
					Query:       `UPDATE initcap_roles SET role = 'admin user' WHERE id = 2;`,
					ExpectedErr: "duplicate unique key given",
				},
			},
		},
		{
			Name: "partial UNIQUE index supports quote_literal predicate",
			SetUpScript: []string{
				`CREATE TABLE quote_literal_roles (id INT PRIMARY KEY, user_id INT, role TEXT);`,
				`CREATE UNIQUE INDEX quote_literal_roles_user_role_idx
					ON quote_literal_roles (user_id)
					WHERE quote_literal(role) = '''admin user''';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO quote_literal_roles VALUES
						(1, 10, 'admin user'),
						(2, 10, 'regular user');`,
				},
				{
					Query:       `INSERT INTO quote_literal_roles VALUES (3, 10, 'admin user');`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: `INSERT INTO quote_literal_roles VALUES (4, 10, 'billing user');`,
				},
				{
					Query:       `UPDATE quote_literal_roles SET role = 'admin user' WHERE id = 2;`,
					ExpectedErr: "duplicate unique key given",
				},
			},
		},
		{
			Name: "partial UNIQUE index supports quote_ident predicate",
			SetUpScript: []string{
				`CREATE TABLE quote_ident_roles (id INT PRIMARY KEY, user_id INT, role TEXT);`,
				`CREATE UNIQUE INDEX quote_ident_roles_user_role_idx
					ON quote_ident_roles (user_id)
					WHERE quote_ident(role) = '"admin user"';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO quote_ident_roles VALUES
						(1, 10, 'admin user'),
						(2, 10, 'regular user');`,
				},
				{
					Query:       `INSERT INTO quote_ident_roles VALUES (3, 10, 'admin user');`,
					ExpectedErr: "duplicate unique key given",
				},
				{
					Query: `INSERT INTO quote_ident_roles VALUES (4, 10, 'billing user');`,
				},
				{
					Query:       `UPDATE quote_ident_roles SET role = 'admin user' WHERE id = 2;`,
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

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE partial_planner_lower (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, email TEXT)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO partial_planner_lower VALUES
		(1, 1, 'Active@Example.com'),
		(2, 1, 'ADMIN@EXAMPLE.COM'),
		(3, 1, 'other@example.com'),
		(4, 2, 'Active@Example.com')`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX partial_planner_lower_tenant_idx ON partial_planner_lower (tenant) WHERE lower(email) IN ('active@example.com', 'admin@example.com')")

	lowerRawEqualQuery := `SELECT count(id) FROM partial_planner_lower WHERE tenant = 1 AND email = 'Active@Example.com'`
	assertCountResult(t, ctx, conn, lowerRawEqualQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, lowerRawEqualQuery, true)

	lowerRawInQuery := `SELECT count(id) FROM partial_planner_lower WHERE tenant = 1 AND email IN ('Active@Example.com', 'ADMIN@EXAMPLE.COM')`
	assertCountResult(t, ctx, conn, lowerRawInQuery, 2)
	assertBenchmarkPlanShape(t, ctx, conn, lowerRawInQuery, true)

	lowerRawNonMatchingQuery := `SELECT count(id) FROM partial_planner_lower WHERE tenant = 1 AND email = 'other@example.com'`
	assertCountResult(t, ctx, conn, lowerRawNonMatchingQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, lowerRawNonMatchingQuery, false)

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE partial_planner_upper (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, code TEXT)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO partial_planner_upper VALUES
		(1, 1, 'active'),
		(2, 1, 'Admin'),
		(3, 1, 'pending'),
		(4, 2, 'active')`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX partial_planner_upper_tenant_idx ON partial_planner_upper (tenant) WHERE upper(code) IN ('ACTIVE', 'ADMIN')")

	upperRawEqualQuery := `SELECT count(id) FROM partial_planner_upper WHERE tenant = 1 AND code = 'active'`
	assertCountResult(t, ctx, conn, upperRawEqualQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, upperRawEqualQuery, true)

	upperRawInQuery := `SELECT count(id) FROM partial_planner_upper WHERE tenant = 1 AND code IN ('active', 'Admin')`
	assertCountResult(t, ctx, conn, upperRawInQuery, 2)
	assertBenchmarkPlanShape(t, ctx, conn, upperRawInQuery, true)

	upperRawNonMatchingQuery := `SELECT count(id) FROM partial_planner_upper WHERE tenant = 1 AND code = 'pending'`
	assertCountResult(t, ctx, conn, upperRawNonMatchingQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, upperRawNonMatchingQuery, false)

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

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE partial_planner_custom_trim (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, code TEXT)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO partial_planner_custom_trim VALUES
		(1, 1, '_0active'),
		(2, 1, 'active--'),
		(3, 1, 'x_active_'),
		(4, 1, 'pending'),
		(5, 2, '_active_')`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX partial_planner_custom_trim_tenant_idx ON partial_planner_custom_trim (tenant) WHERE btrim(code, 'x_') = 'active'")

	customTrimImpliedQuery := `SELECT count(id) FROM partial_planner_custom_trim WHERE tenant = 1 AND btrim(code, 'x_') = 'active'`
	assertCountResult(t, ctx, conn, customTrimImpliedQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, customTrimImpliedQuery, true)

	customTrimWrongCharsQuery := `SELECT count(id) FROM partial_planner_custom_trim WHERE tenant = 1 AND btrim(code, '_') = 'x_active'`
	assertCountResult(t, ctx, conn, customTrimWrongCharsQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, customTrimWrongCharsQuery, false)

	customLtrimQuery := `SELECT count(id) FROM partial_planner_custom_trim WHERE tenant = 1 AND ltrim(code, '0_') = 'active'`
	assertCountResult(t, ctx, conn, customLtrimQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, customLtrimQuery, false)

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE partial_planner_repeat (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, code TEXT)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO partial_planner_repeat VALUES
		(1, 1, 'active'),
		(2, 1, 'pending'),
		(3, 1, 'activeactive'),
		(4, 2, 'active')`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX partial_planner_repeat_tenant_idx ON partial_planner_repeat (tenant) WHERE repeat(code, 2) = 'activeactive'")

	repeatImpliedQuery := `SELECT count(id) FROM partial_planner_repeat WHERE tenant = 1 AND repeat(code, 2) = 'activeactive'`
	assertCountResult(t, ctx, conn, repeatImpliedQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, repeatImpliedQuery, true)

	repeatWrongCountQuery := `SELECT count(id) FROM partial_planner_repeat WHERE tenant = 1 AND repeat(code, 3) = 'activeactiveactive'`
	assertCountResult(t, ctx, conn, repeatWrongCountQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, repeatWrongCountQuery, false)

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE partial_planner_concat (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, prefix TEXT, code TEXT)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO partial_planner_concat VALUES
		(1, 1, 'acct', 'active'),
		(2, 1, 'acct', 'pending'),
		(3, 1, 'acctactive', ''),
		(4, 2, 'acct', 'active')`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX partial_planner_concat_tenant_idx ON partial_planner_concat (tenant) WHERE concat(prefix, '-', code) = 'acct-active'")

	concatImpliedQuery := `SELECT count(id) FROM partial_planner_concat WHERE tenant = 1 AND concat(prefix, '-', code) = 'acct-active'`
	assertCountResult(t, ctx, conn, concatImpliedQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, concatImpliedQuery, true)

	concatWrongResultQuery := `SELECT count(id) FROM partial_planner_concat WHERE tenant = 1 AND concat(prefix, '-', code) = 'acct-pending'`
	assertCountResult(t, ctx, conn, concatWrongResultQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, concatWrongResultQuery, false)

	concatRawSemanticQuery := `SELECT count(id) FROM partial_planner_concat WHERE tenant = 1 AND prefix = 'acct' AND code = 'active'`
	assertCountResult(t, ctx, conn, concatRawSemanticQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, concatRawSemanticQuery, false)

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

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE partial_planner_hashtext (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, code TEXT)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO partial_planner_hashtext VALUES
		(1, 1, 'abc'),
		(2, 1, 'pending'),
		(3, 1, 'ABC'),
		(4, 2, 'abc')`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX partial_planner_hashtext_tenant_idx ON partial_planner_hashtext (tenant) WHERE hashtext(code) = -785388649")

	hashtextImpliedQuery := `SELECT count(id) FROM partial_planner_hashtext WHERE tenant = 1 AND hashtext(code) = -785388649`
	assertCountResult(t, ctx, conn, hashtextImpliedQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, hashtextImpliedQuery, true)

	hashtextRawSourceQuery := `SELECT count(id) FROM partial_planner_hashtext WHERE tenant = 1 AND code = 'abc'`
	assertCountResult(t, ctx, conn, hashtextRawSourceQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, hashtextRawSourceQuery, false)

	hashtextNonMatchingQuery := `SELECT count(id) FROM partial_planner_hashtext WHERE tenant = 1 AND hashtext(code) = 1425101999`
	assertCountResult(t, ctx, conn, hashtextNonMatchingQuery, 0)
	assertBenchmarkPlanShape(t, ctx, conn, hashtextNonMatchingQuery, false)

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE partial_planner_floor (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, score INTEGER)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO partial_planner_floor VALUES
		(1, 1, 7),
		(2, 1, 8),
		(3, 2, 7)`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX partial_planner_floor_tenant_idx ON partial_planner_floor (tenant) WHERE floor(score) = 7")

	floorImpliedQuery := `SELECT count(id) FROM partial_planner_floor WHERE tenant = 1 AND floor(score) = 7`
	assertCountResult(t, ctx, conn, floorImpliedQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, floorImpliedQuery, true)

	floorRawSourceQuery := `SELECT count(id) FROM partial_planner_floor WHERE tenant = 1 AND score = 7`
	assertCountResult(t, ctx, conn, floorRawSourceQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, floorRawSourceQuery, false)

	floorNonMatchingQuery := `SELECT count(id) FROM partial_planner_floor WHERE tenant = 1 AND floor(score) = 8`
	assertCountResult(t, ctx, conn, floorNonMatchingQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, floorNonMatchingQuery, false)

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE partial_planner_ceil (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, score INTEGER)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO partial_planner_ceil VALUES
		(1, 1, 7),
		(2, 1, 8),
		(3, 2, 7)`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX partial_planner_ceil_tenant_idx ON partial_planner_ceil (tenant) WHERE ceiling(score) = 7")

	ceilImpliedQuery := `SELECT count(id) FROM partial_planner_ceil WHERE tenant = 1 AND ceil(score) = 7`
	assertCountResult(t, ctx, conn, ceilImpliedQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, ceilImpliedQuery, true)

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE partial_planner_round (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, score INTEGER)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO partial_planner_round VALUES
		(1, 1, 7),
		(2, 1, 8),
		(3, 2, 7)`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX partial_planner_round_tenant_idx ON partial_planner_round (tenant) WHERE round(score) = 7")

	roundImpliedQuery := `SELECT count(id) FROM partial_planner_round WHERE tenant = 1 AND round(score) = 7`
	assertCountResult(t, ctx, conn, roundImpliedQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, roundImpliedQuery, true)

	roundRawSourceQuery := `SELECT count(id) FROM partial_planner_round WHERE tenant = 1 AND score = 7`
	assertCountResult(t, ctx, conn, roundRawSourceQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, roundRawSourceQuery, false)

	roundNonMatchingQuery := `SELECT count(id) FROM partial_planner_round WHERE tenant = 1 AND round(score) = 8`
	assertCountResult(t, ctx, conn, roundNonMatchingQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, roundNonMatchingQuery, false)

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE partial_planner_trunc (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, score INTEGER)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO partial_planner_trunc VALUES
		(1, 1, 7),
		(2, 1, 8),
		(3, 2, 7)`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX partial_planner_trunc_tenant_idx ON partial_planner_trunc (tenant) WHERE trunc(score) = 7")

	truncImpliedQuery := `SELECT count(id) FROM partial_planner_trunc WHERE tenant = 1 AND trunc(score) = 7`
	assertCountResult(t, ctx, conn, truncImpliedQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, truncImpliedQuery, true)

	truncRawSourceQuery := `SELECT count(id) FROM partial_planner_trunc WHERE tenant = 1 AND score = 7`
	assertCountResult(t, ctx, conn, truncRawSourceQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, truncRawSourceQuery, false)

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

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE partial_planner_substring (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, code TEXT)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO partial_planner_substring VALUES
		(1, 1, 'Admin'),
		(2, 1, 'Alpha'),
		(3, 1, 'Admiral'),
		(4, 2, 'Admire')`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX partial_planner_substring_tenant_idx ON partial_planner_substring (tenant) WHERE substring(code, 1, 3) = 'Adm'")

	substringImpliedQuery := `SELECT count(id) FROM partial_planner_substring WHERE tenant = 1 AND substr(code, 1, 3) = 'Adm'`
	assertCountResult(t, ctx, conn, substringImpliedQuery, 2)
	assertBenchmarkPlanShape(t, ctx, conn, substringImpliedQuery, true)

	substringRawSourceQuery := `SELECT count(id) FROM partial_planner_substring WHERE tenant = 1 AND code = 'Admin'`
	assertCountResult(t, ctx, conn, substringRawSourceQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, substringRawSourceQuery, false)

	substringWrongCountQuery := `SELECT count(id) FROM partial_planner_substring WHERE tenant = 1 AND substring(code, 1, 2) = 'Ad'`
	assertCountResult(t, ctx, conn, substringWrongCountQuery, 2)
	assertBenchmarkPlanShape(t, ctx, conn, substringWrongCountQuery, false)

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE partial_planner_lpad (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, code TEXT)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO partial_planner_lpad VALUES
		(1, 1, 'ABCD'),
		(2, 1, 'XYZ'),
		(3, 1, 'ABXY'),
		(4, 2, 'ABCD')`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX partial_planner_lpad_tenant_idx ON partial_planner_lpad (tenant) WHERE lpad(code, 6, '0') = '00ABCD'")

	lpadImpliedQuery := `SELECT count(id) FROM partial_planner_lpad WHERE tenant = 1 AND lpad(code, 6, '0') = '00ABCD'`
	assertCountResult(t, ctx, conn, lpadImpliedQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, lpadImpliedQuery, true)

	lpadWrongFillQuery := `SELECT count(id) FROM partial_planner_lpad WHERE tenant = 1 AND lpad(code, 6, '_') = '__ABCD'`
	assertCountResult(t, ctx, conn, lpadWrongFillQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, lpadWrongFillQuery, false)

	lpadRawSourceQuery := `SELECT count(id) FROM partial_planner_lpad WHERE tenant = 1 AND code = 'ABCD'`
	assertCountResult(t, ctx, conn, lpadRawSourceQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, lpadRawSourceQuery, false)

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE partial_planner_rpad (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, code TEXT)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO partial_planner_rpad VALUES
		(1, 1, 'ABCD'),
		(2, 1, 'XYZ'),
		(3, 1, 'ABXY'),
		(4, 2, 'ABCD')`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX partial_planner_rpad_tenant_idx ON partial_planner_rpad (tenant) WHERE rpad(code, 6, '_') = 'ABCD__'")

	rpadImpliedQuery := `SELECT count(id) FROM partial_planner_rpad WHERE tenant = 1 AND rpad(code, 6, '_') = 'ABCD__'`
	assertCountResult(t, ctx, conn, rpadImpliedQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, rpadImpliedQuery, true)

	rpadWrongFillQuery := `SELECT count(id) FROM partial_planner_rpad WHERE tenant = 1 AND rpad(code, 6, '-') = 'ABCD--'`
	assertCountResult(t, ctx, conn, rpadWrongFillQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, rpadWrongFillQuery, false)

	rpadRawSourceQuery := `SELECT count(id) FROM partial_planner_rpad WHERE tenant = 1 AND code = 'ABCD'`
	assertCountResult(t, ctx, conn, rpadRawSourceQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, rpadRawSourceQuery, false)

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE partial_planner_reverse (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, code TEXT)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO partial_planner_reverse VALUES
		(1, 1, 'Admin'),
		(2, 1, 'Alpha'),
		(3, 1, 'Admiral'),
		(4, 2, 'Admin')`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX partial_planner_reverse_tenant_idx ON partial_planner_reverse (tenant) WHERE reverse(code) = 'nimdA'")

	reverseImpliedQuery := `SELECT count(id) FROM partial_planner_reverse WHERE tenant = 1 AND reverse(code) = 'nimdA'`
	assertCountResult(t, ctx, conn, reverseImpliedQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, reverseImpliedQuery, true)

	reverseRawSourceQuery := `SELECT count(id) FROM partial_planner_reverse WHERE tenant = 1 AND code = 'Admin'`
	assertCountResult(t, ctx, conn, reverseRawSourceQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, reverseRawSourceQuery, false)

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE partial_planner_to_hex (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, account_id INTEGER)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO partial_planner_to_hex VALUES
		(1, 1, 10),
		(2, 1, 11),
		(3, 1, 12),
		(4, 2, 10)`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX partial_planner_to_hex_tenant_idx ON partial_planner_to_hex (tenant) WHERE to_hex(account_id) = 'a'")

	toHexImpliedQuery := `SELECT count(id) FROM partial_planner_to_hex WHERE tenant = 1 AND to_hex(account_id) = 'a'`
	assertCountResult(t, ctx, conn, toHexImpliedQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, toHexImpliedQuery, true)

	toHexRawSourceQuery := `SELECT count(id) FROM partial_planner_to_hex WHERE tenant = 1 AND account_id = 10`
	assertCountResult(t, ctx, conn, toHexRawSourceQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, toHexRawSourceQuery, false)

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE partial_planner_initcap (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, role TEXT)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO partial_planner_initcap VALUES
		(1, 1, 'admin user'),
		(2, 1, 'regular user'),
		(3, 1, 'billing user'),
		(4, 2, 'admin user')`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX partial_planner_initcap_tenant_idx ON partial_planner_initcap (tenant) WHERE initcap(role) = 'Admin User'")

	initcapImpliedQuery := `SELECT count(id) FROM partial_planner_initcap WHERE tenant = 1 AND initcap(role) = 'Admin User'`
	assertCountResult(t, ctx, conn, initcapImpliedQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, initcapImpliedQuery, true)

	initcapRawSourceQuery := `SELECT count(id) FROM partial_planner_initcap WHERE tenant = 1 AND role = 'admin user'`
	assertCountResult(t, ctx, conn, initcapRawSourceQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, initcapRawSourceQuery, false)

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE partial_planner_quote_literal (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, role TEXT)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO partial_planner_quote_literal VALUES
		(1, 1, 'admin user'),
		(2, 1, 'regular user'),
		(3, 1, 'billing user'),
		(4, 2, 'admin user')`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX partial_planner_quote_literal_tenant_idx ON partial_planner_quote_literal (tenant) WHERE quote_literal(role) = '''admin user'''")

	quoteLiteralImpliedQuery := `SELECT count(id) FROM partial_planner_quote_literal WHERE tenant = 1 AND quote_literal(role) = '''admin user'''`
	assertCountResult(t, ctx, conn, quoteLiteralImpliedQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, quoteLiteralImpliedQuery, true)

	quoteLiteralRawSourceQuery := `SELECT count(id) FROM partial_planner_quote_literal WHERE tenant = 1 AND role = 'admin user'`
	assertCountResult(t, ctx, conn, quoteLiteralRawSourceQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, quoteLiteralRawSourceQuery, false)

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE partial_planner_quote_ident (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, role TEXT)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO partial_planner_quote_ident VALUES
		(1, 1, 'admin user'),
		(2, 1, 'regular user'),
		(3, 1, 'billing user'),
		(4, 2, 'admin user')`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX partial_planner_quote_ident_tenant_idx ON partial_planner_quote_ident (tenant) WHERE quote_ident(role) = '\"admin user\"'")

	quoteIdentImpliedQuery := `SELECT count(id) FROM partial_planner_quote_ident WHERE tenant = 1 AND quote_ident(role) = '"admin user"'`
	assertCountResult(t, ctx, conn, quoteIdentImpliedQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, quoteIdentImpliedQuery, true)

	quoteIdentRawSourceQuery := `SELECT count(id) FROM partial_planner_quote_ident WHERE tenant = 1 AND role = 'admin user'`
	assertCountResult(t, ctx, conn, quoteIdentRawSourceQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, quoteIdentRawSourceQuery, false)

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

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE partial_planner_nullif (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, status TEXT)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO partial_planner_nullif VALUES
		(1, 1, 'active'),
		(2, 1, ''),
		(3, 1, 'pending'),
		(4, 2, 'active')`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX partial_planner_nullif_tenant_idx ON partial_planner_nullif (tenant) WHERE nullif(status, '') = 'active'")

	nullifImpliedQuery := `SELECT count(id) FROM partial_planner_nullif WHERE tenant = 1 AND nullif(status, '') = 'active'`
	assertCountResult(t, ctx, conn, nullifImpliedQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, nullifImpliedQuery, true)

	nullifRawSourceQuery := `SELECT count(id) FROM partial_planner_nullif WHERE tenant = 1 AND status = 'active'`
	assertCountResult(t, ctx, conn, nullifRawSourceQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, nullifRawSourceQuery, false)

	nullifWrongArgumentQuery := `SELECT count(id) FROM partial_planner_nullif WHERE tenant = 1 AND nullif(status, 'inactive') = 'active'`
	assertCountResult(t, ctx, conn, nullifWrongArgumentQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, nullifWrongArgumentQuery, false)

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE partial_planner_arithmetic_plus (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, score INTEGER)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO partial_planner_arithmetic_plus VALUES
		(1, 1, 7),
		(2, 1, 8),
		(3, 2, 7)`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX partial_planner_arithmetic_plus_tenant_idx ON partial_planner_arithmetic_plus (tenant) WHERE score + 1 = 8")

	arithmeticPlusImpliedQuery := `SELECT count(id) FROM partial_planner_arithmetic_plus WHERE tenant = 1 AND score + 1 = 8`
	assertCountResult(t, ctx, conn, arithmeticPlusImpliedQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, arithmeticPlusImpliedQuery, true)

	arithmeticPlusCommutedQuery := `SELECT count(id) FROM partial_planner_arithmetic_plus WHERE tenant = 1 AND 1 + score = 8`
	assertCountResult(t, ctx, conn, arithmeticPlusCommutedQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, arithmeticPlusCommutedQuery, true)

	arithmeticPlusRawSourceQuery := `SELECT count(id) FROM partial_planner_arithmetic_plus WHERE tenant = 1 AND score = 7`
	assertCountResult(t, ctx, conn, arithmeticPlusRawSourceQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, arithmeticPlusRawSourceQuery, false)

	arithmeticPlusWrongExpressionQuery := `SELECT count(id) FROM partial_planner_arithmetic_plus WHERE tenant = 1 AND score + 2 = 8`
	assertCountResult(t, ctx, conn, arithmeticPlusWrongExpressionQuery, 0)
	assertBenchmarkPlanShape(t, ctx, conn, arithmeticPlusWrongExpressionQuery, false)

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE partial_planner_arithmetic_minus (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, score INTEGER)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO partial_planner_arithmetic_minus VALUES
		(1, 1, 7),
		(2, 1, 8),
		(3, 2, 7)`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX partial_planner_arithmetic_minus_tenant_idx ON partial_planner_arithmetic_minus (tenant) WHERE score - 1 = 6")

	arithmeticMinusImpliedQuery := `SELECT count(id) FROM partial_planner_arithmetic_minus WHERE tenant = 1 AND score - 1 = 6`
	assertCountResult(t, ctx, conn, arithmeticMinusImpliedQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, arithmeticMinusImpliedQuery, true)

	arithmeticMinusReversedQuery := `SELECT count(id) FROM partial_planner_arithmetic_minus WHERE tenant = 1 AND 1 - score = 6`
	assertCountResult(t, ctx, conn, arithmeticMinusReversedQuery, 0)
	assertBenchmarkPlanShape(t, ctx, conn, arithmeticMinusReversedQuery, false)

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE partial_planner_arithmetic_mult (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, score INTEGER)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO partial_planner_arithmetic_mult VALUES
		(1, 1, 7),
		(2, 1, 8),
		(3, 2, 7)`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX partial_planner_arithmetic_mult_tenant_idx ON partial_planner_arithmetic_mult (tenant) WHERE score * 2 = 14")

	arithmeticMultImpliedQuery := `SELECT count(id) FROM partial_planner_arithmetic_mult WHERE tenant = 1 AND score * 2 = 14`
	assertCountResult(t, ctx, conn, arithmeticMultImpliedQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, arithmeticMultImpliedQuery, true)

	arithmeticMultCommutedQuery := `SELECT count(id) FROM partial_planner_arithmetic_mult WHERE tenant = 1 AND 2 * score = 14`
	assertCountResult(t, ctx, conn, arithmeticMultCommutedQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, arithmeticMultCommutedQuery, true)

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

	absRawPositiveQuery := `SELECT count(id) FROM partial_planner_abs WHERE tenant = 1 AND delta = 10`
	assertCountResult(t, ctx, conn, absRawPositiveQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, absRawPositiveQuery, true)

	absRawNegativeQuery := `SELECT count(id) FROM partial_planner_abs WHERE tenant = 1 AND delta = -10`
	assertCountResult(t, ctx, conn, absRawNegativeQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, absRawNegativeQuery, true)

	absRawInQuery := `SELECT count(id) FROM partial_planner_abs WHERE tenant = 1 AND delta IN (-10, 10)`
	assertCountResult(t, ctx, conn, absRawInQuery, 2)
	assertBenchmarkPlanShape(t, ctx, conn, absRawInQuery, true)

	absRawNonMatchingQuery := `SELECT count(id) FROM partial_planner_abs WHERE tenant = 1 AND delta = 11`
	assertCountResult(t, ctx, conn, absRawNonMatchingQuery, 0)
	assertBenchmarkPlanShape(t, ctx, conn, absRawNonMatchingQuery, false)

	absNonMatchingQuery := `SELECT count(id) FROM partial_planner_abs WHERE tenant = 1 AND abs(delta) = 5`
	assertCountResult(t, ctx, conn, absNonMatchingQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, absNonMatchingQuery, false)

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE partial_planner_sign (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, delta BIGINT)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO partial_planner_sign VALUES
		(1, 1, 5),
		(2, 1, -5),
		(3, 1, 0),
		(4, 2, 10)`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX partial_planner_sign_tenant_idx ON partial_planner_sign (tenant) WHERE sign(delta) = 1")

	signImpliedQuery := `SELECT count(id) FROM partial_planner_sign WHERE tenant = 1 AND sign(delta) = 1`
	assertCountResult(t, ctx, conn, signImpliedQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, signImpliedQuery, true)

	signRawSemanticQuery := `SELECT count(id) FROM partial_planner_sign WHERE tenant = 1 AND delta > 0`
	assertCountResult(t, ctx, conn, signRawSemanticQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, signRawSemanticQuery, true)

	signRawValueQuery := `SELECT count(id) FROM partial_planner_sign WHERE tenant = 1 AND delta = 5`
	assertCountResult(t, ctx, conn, signRawValueQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, signRawValueQuery, true)

	signRawInQuery := `SELECT count(id) FROM partial_planner_sign WHERE tenant = 1 AND delta IN (5, 10)`
	assertCountResult(t, ctx, conn, signRawInQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, signRawInQuery, true)

	signRawCrossingQuery := `SELECT count(id) FROM partial_planner_sign WHERE tenant = 1 AND delta >= 0`
	assertCountResult(t, ctx, conn, signRawCrossingQuery, 2)
	assertBenchmarkPlanShape(t, ctx, conn, signRawCrossingQuery, false)

	signNonMatchingQuery := `SELECT count(id) FROM partial_planner_sign WHERE tenant = 1 AND sign(delta) = -1`
	assertCountResult(t, ctx, conn, signNonMatchingQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, signNonMatchingQuery, false)

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE partial_planner_sign_negative (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, delta BIGINT)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO partial_planner_sign_negative VALUES
		(1, 1, -5),
		(2, 1, 5),
		(3, 1, 0),
		(4, 2, -10)`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX partial_planner_sign_negative_tenant_idx ON partial_planner_sign_negative (tenant) WHERE sign(delta) = -1")

	signNegativeRawRangeQuery := `SELECT count(id) FROM partial_planner_sign_negative WHERE tenant = 1 AND delta < 0`
	assertCountResult(t, ctx, conn, signNegativeRawRangeQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, signNegativeRawRangeQuery, true)

	signNegativeCrossingQuery := `SELECT count(id) FROM partial_planner_sign_negative WHERE tenant = 1 AND delta <= 0`
	assertCountResult(t, ctx, conn, signNegativeCrossingQuery, 2)
	assertBenchmarkPlanShape(t, ctx, conn, signNegativeCrossingQuery, false)

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE partial_planner_sign_zero (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, delta BIGINT)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO partial_planner_sign_zero VALUES
		(1, 1, 0),
		(2, 1, 5),
		(3, 1, -5),
		(4, 2, 0)`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX partial_planner_sign_zero_tenant_idx ON partial_planner_sign_zero (tenant) WHERE sign(delta) = 0")

	signZeroRawValueQuery := `SELECT count(id) FROM partial_planner_sign_zero WHERE tenant = 1 AND delta = 0`
	assertCountResult(t, ctx, conn, signZeroRawValueQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, signZeroRawValueQuery, true)

	signZeroCrossingQuery := `SELECT count(id) FROM partial_planner_sign_zero WHERE tenant = 1 AND delta BETWEEN -1 AND 1`
	assertCountResult(t, ctx, conn, signZeroCrossingQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, signZeroCrossingQuery, false)

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE partial_planner_gcd (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, width BIGINT, height BIGINT)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO partial_planner_gcd VALUES
		(1, 1, 8, 12),
		(2, 1, 9, 6),
		(3, 1, 12, 16),
		(4, 2, 8, 12)`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX partial_planner_gcd_tenant_idx ON partial_planner_gcd (tenant) WHERE gcd(width, height) = 4")

	gcdImpliedQuery := `SELECT count(id) FROM partial_planner_gcd WHERE tenant = 1 AND gcd(width, height) = 4`
	assertCountResult(t, ctx, conn, gcdImpliedQuery, 2)
	assertBenchmarkPlanShape(t, ctx, conn, gcdImpliedQuery, true)

	gcdCommutedQuery := `SELECT count(id) FROM partial_planner_gcd WHERE tenant = 1 AND gcd(height, width) = 4`
	assertCountResult(t, ctx, conn, gcdCommutedQuery, 2)
	assertBenchmarkPlanShape(t, ctx, conn, gcdCommutedQuery, true)

	gcdRawSemanticQuery := `SELECT count(id) FROM partial_planner_gcd WHERE tenant = 1 AND width = 8 AND height = 12`
	assertCountResult(t, ctx, conn, gcdRawSemanticQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, gcdRawSemanticQuery, false)

	gcdNonMatchingQuery := `SELECT count(id) FROM partial_planner_gcd WHERE tenant = 1 AND gcd(width, height) = 3`
	assertCountResult(t, ctx, conn, gcdNonMatchingQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, gcdNonMatchingQuery, false)

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE partial_planner_lcm (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, width BIGINT, height BIGINT)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO partial_planner_lcm VALUES
		(1, 1, 3, 4),
		(2, 1, 5, 6),
		(3, 1, 4, 6),
		(4, 2, 3, 4)`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX partial_planner_lcm_tenant_idx ON partial_planner_lcm (tenant) WHERE lcm(width, height) = 12")

	lcmImpliedQuery := `SELECT count(id) FROM partial_planner_lcm WHERE tenant = 1 AND lcm(width, height) = 12`
	assertCountResult(t, ctx, conn, lcmImpliedQuery, 2)
	assertBenchmarkPlanShape(t, ctx, conn, lcmImpliedQuery, true)

	lcmCommutedQuery := `SELECT count(id) FROM partial_planner_lcm WHERE tenant = 1 AND lcm(height, width) = 12`
	assertCountResult(t, ctx, conn, lcmCommutedQuery, 2)
	assertBenchmarkPlanShape(t, ctx, conn, lcmCommutedQuery, true)

	lcmRawSemanticQuery := `SELECT count(id) FROM partial_planner_lcm WHERE tenant = 1 AND width = 3 AND height = 4`
	assertCountResult(t, ctx, conn, lcmRawSemanticQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, lcmRawSemanticQuery, false)

	lcmNonMatchingQuery := `SELECT count(id) FROM partial_planner_lcm WHERE tenant = 1 AND lcm(width, height) = 30`
	assertCountResult(t, ctx, conn, lcmNonMatchingQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, lcmNonMatchingQuery, false)

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE partial_planner_mod (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, account_id BIGINT, shard_count BIGINT)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO partial_planner_mod VALUES
		(1, 1, 7, 3),
		(2, 1, 8, 3),
		(3, 1, 10, 3),
		(4, 2, 7, 3)`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX partial_planner_mod_tenant_idx ON partial_planner_mod (tenant) WHERE mod(account_id, shard_count) = 1")

	modImpliedQuery := `SELECT count(id) FROM partial_planner_mod WHERE tenant = 1 AND mod(account_id, shard_count) = 1`
	assertCountResult(t, ctx, conn, modImpliedQuery, 2)
	assertBenchmarkPlanShape(t, ctx, conn, modImpliedQuery, true)

	modRawSemanticQuery := `SELECT count(id) FROM partial_planner_mod WHERE tenant = 1 AND account_id = 7 AND shard_count = 3`
	assertCountResult(t, ctx, conn, modRawSemanticQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, modRawSemanticQuery, false)

	modNonMatchingQuery := `SELECT count(id) FROM partial_planner_mod WHERE tenant = 1 AND mod(account_id, shard_count) = 2`
	assertCountResult(t, ctx, conn, modNonMatchingQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, modNonMatchingQuery, false)

	execBenchmarkSQL(t, ctx, conn, "CREATE TABLE partial_planner_chr (id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, codepoint INTEGER)")
	execBenchmarkSQL(t, ctx, conn, `INSERT INTO partial_planner_chr VALUES
		(1, 1, 65),
		(2, 1, 66),
		(3, 1, 67),
		(4, 2, 65)`)
	execBenchmarkSQL(t, ctx, conn, "CREATE INDEX partial_planner_chr_tenant_idx ON partial_planner_chr (tenant) WHERE chr(codepoint) = 'A'")

	chrImpliedQuery := `SELECT count(id) FROM partial_planner_chr WHERE tenant = 1 AND chr(codepoint) = 'A'`
	assertCountResult(t, ctx, conn, chrImpliedQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, chrImpliedQuery, true)

	chrRawSemanticQuery := `SELECT count(id) FROM partial_planner_chr WHERE tenant = 1 AND codepoint = 65`
	assertCountResult(t, ctx, conn, chrRawSemanticQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, chrRawSemanticQuery, false)

	chrNonMatchingQuery := `SELECT count(id) FROM partial_planner_chr WHERE tenant = 1 AND chr(codepoint) = 'B'`
	assertCountResult(t, ctx, conn, chrNonMatchingQuery, 1)
	assertBenchmarkPlanShape(t, ctx, conn, chrNonMatchingQuery, false)
}
