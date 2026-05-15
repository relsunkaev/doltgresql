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

package postgres16

import (
	. "github.com/dolthub/doltgresql/testing/go"

	"testing"
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
					Query: `SELECT count(*)::text FROM pg_indexes WHERE tablename = 'users' AND indexname = 'users_active_email_idx';`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0001-select-count-*-::text-from"},
				},
				{
					// Reading through the partial-index predicate
					// must still return the right rows.
					Query: `SELECT id, email FROM users WHERE deleted_at IS NULL ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0002-select-id-email-from-users"},
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
					Query: `SELECT count(*)::text FROM pg_indexes WHERE tablename = 'jobs' AND indexname = 'jobs_active_idx';`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0003-select-count-*-::text-from"},
				},
				{
					Query: `SELECT id, payload FROM jobs WHERE active = true ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0004-select-id-payload-from-jobs"},
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
					Query: `INSERT INTO memberships VALUES (4, 10, 'active');`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0005-insert-into-memberships-values-4", Compare: "sqlstate"},
				},
				{
					Query: `UPDATE memberships SET status = 'active' WHERE id = 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0006-update-memberships-set-status-=", Compare: "sqlstate"},
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
					Query: `SELECT count(*)::text FROM memberships WHERE user_id = 10 AND status = 'active';`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0007-select-count-*-::text-from"},
				},
				{
					Query: `SELECT c.relname, i.indisunique, pg_catalog.pg_get_expr(i.indpred, i.indrelid)
FROM pg_catalog.pg_index i
JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
WHERE c.relname = 'memberships_one_active_idx';`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0008-select-c.relname-i.indisunique-pg_catalog.pg_get_expr-i.indpred"},
				},
				{
					Query: `SELECT indexdef
FROM pg_catalog.pg_indexes
WHERE tablename = 'memberships'
  AND indexname = 'memberships_one_active_idx';`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0009-select-indexdef-from-pg_catalog.pg_indexes-where", ColumnModes: []string{"schema"}},
				},
			},
		},
		{
			Name: "partial UNIQUE index permits duplicate NULL keys",
			SetUpScript: []string{
				`CREATE TABLE nullable_memberships (
					id INT PRIMARY KEY,
					user_id INT,
					locale TEXT,
					active BOOL NOT NULL
				);`,
				`CREATE UNIQUE INDEX nullable_memberships_user_idx
					ON nullable_memberships (user_id)
					WHERE active;`,
				`CREATE TABLE nullable_membership_pairs (
					id INT PRIMARY KEY,
					user_id INT,
					locale TEXT,
					active BOOL NOT NULL
				);`,
				`CREATE UNIQUE INDEX nullable_membership_pairs_user_locale_idx
					ON nullable_membership_pairs (user_id, locale)
					WHERE active;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO nullable_memberships VALUES
						(1, NULL, 'en', true),
						(2, NULL, 'en', true),
						(3, 10, NULL, false),
						(4, 10, NULL, false),
						(5, 10, NULL, true);`,
				},
				{
					Query: `INSERT INTO nullable_memberships VALUES (6, 10, NULL, true);`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0010-insert-into-nullable_memberships-values-6", Compare: "sqlstate"},
				},
				{
					Query: `SELECT count(*)::text FROM nullable_memberships;`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0011-select-count-*-::text-from"},
				},
				{
					Query: `INSERT INTO nullable_membership_pairs VALUES
						(1, 20, NULL, true),
						(2, 20, NULL, true),
						(3, NULL, 'en', true),
						(4, NULL, 'en', true),
						(5, 30, 'en', false),
						(6, 30, 'en', false),
						(7, 30, 'en', true);`,
				},
				{
					Query: `INSERT INTO nullable_membership_pairs VALUES (8, 30, 'en', true);`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0012-insert-into-nullable_membership_pairs-values-8", Compare: "sqlstate"},
				},
				{
					Query: `SELECT count(*)::text FROM nullable_membership_pairs;`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0013-select-count-*-::text-from"},
				},
			},
		},
		{
			Name: "partial UNIQUE index duplicate multi-row insert is atomic",
			SetUpScript: []string{
				`CREATE TABLE partial_unique_atomic_memberships (
					id INT PRIMARY KEY,
					user_id INT NOT NULL,
					active BOOL NOT NULL
				);`,
				`CREATE UNIQUE INDEX partial_unique_atomic_memberships_user_idx
					ON partial_unique_atomic_memberships (user_id)
					WHERE active;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO partial_unique_atomic_memberships VALUES
						(1, 10, false),
						(2, 10, true),
						(3, 10, true);`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0014-insert-into-partial_unique_atomic_memberships-values-1", Compare: "sqlstate"},
				},
				{
					Query: `SELECT count(*)::text FROM partial_unique_atomic_memberships;`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0015-select-count-*-::text-from"},
				},
			},
		},
		{
			Name: "partial UNIQUE index duplicate multi-row update is atomic",
			SetUpScript: []string{
				`CREATE TABLE partial_unique_update_atomic_memberships (
					id INT PRIMARY KEY,
					user_id INT NOT NULL,
					active BOOL NOT NULL
				);`,
				`CREATE UNIQUE INDEX partial_unique_update_atomic_memberships_user_idx
					ON partial_unique_update_atomic_memberships (user_id)
					WHERE active;`,
				`INSERT INTO partial_unique_update_atomic_memberships VALUES
					(1, 10, false),
					(2, 10, false),
					(3, 20, true);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE partial_unique_update_atomic_memberships SET active = true WHERE user_id = 10;`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0016-update-partial_unique_update_atomic_memberships-set-active-=", Compare: "sqlstate"},
				},
				{
					Query: `SELECT id, active
						FROM partial_unique_update_atomic_memberships
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0017-select-id-active-from-partial_unique_update_atomic_memberships"},
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
					Query: `INSERT INTO inventory VALUES (3, 42, false);`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0018-insert-into-inventory-values-3", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO inventory VALUES (4, 42, true);`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0019-insert-into-inventory-values-4", Compare: "sqlstate"},
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
					Query: `INSERT INTO quota_windows VALUES (4, 10, 60);`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0020-insert-into-quota_windows-values-4", Compare: "sqlstate"},
				},
				{
					Query: `UPDATE quota_windows SET score = 75 WHERE id = 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0021-update-quota_windows-set-score-=", Compare: "sqlstate"},
				},
				{
					Query: `UPDATE quota_windows SET score = 101 WHERE id = 1;`,
				},
				{
					Query: `INSERT INTO quota_windows VALUES (4, 10, 60);`,
				},
				{
					Query: `SELECT id, score FROM quota_windows WHERE user_id = 10 ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0022-select-id-score-from-quota_windows"},
				},
				{
					Query: `INSERT INTO quota_not_windows VALUES (1, 20, 0), (2, 20, 50);`,
				},
				{
					Query: `INSERT INTO quota_not_windows VALUES (3, 20, 101);`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0023-insert-into-quota_not_windows-values-3", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO quota_not_windows VALUES (4, 20, 60);`,
				},
				{
					Query: `INSERT INTO quota_symmetric_windows VALUES (1, 30, 50), (2, 30, 0), (3, 30, 101);`,
				},
				{
					Query: `INSERT INTO quota_symmetric_windows VALUES (4, 30, 60);`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0024-insert-into-quota_symmetric_windows-values-4", Compare: "sqlstate"},
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
					Query: `INSERT INTO workflow_states VALUES (3, 10, 'pending');`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0025-insert-into-workflow_states-values-3", Compare: "sqlstate"},
				},
				{
					Query: `UPDATE workflow_states SET status = 'pending' WHERE id = 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0026-update-workflow_states-set-status-=", Compare: "sqlstate"},
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
					Query: `INSERT INTO workflow_not_states VALUES (3, 20, 'pending');`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0027-insert-into-workflow_not_states-values-3", Compare: "sqlstate"},
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
					Query: `INSERT INTO case_folded_accounts VALUES (3, 'Active@Example.com');`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0028-insert-into-case_folded_accounts-values-3", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO case_folded_accounts VALUES (4, 'Other@Example.com');`,
				},
				{
					Query: `UPDATE case_folded_accounts SET email = 'Active@Example.com' WHERE id = 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0029-update-case_folded_accounts-set-email-=", Compare: "sqlstate"},
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
					Query: `INSERT INTO nullif_codes VALUES (4, 10, 'active');`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0030-insert-into-nullif_codes-values-4", Compare: "sqlstate"},
				},
				{
					Query: `UPDATE nullif_codes SET code = 'active' WHERE id = 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0031-update-nullif_codes-set-code-=", Compare: "sqlstate"},
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
					Query: `INSERT INTO arithmetic_plus_scores VALUES (4, 10, 7);`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0032-insert-into-arithmetic_plus_scores-values-4", Compare: "sqlstate"},
				},
				{
					Query: `UPDATE arithmetic_plus_scores SET score = 7 WHERE id = 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0033-update-arithmetic_plus_scores-set-score-=", Compare: "sqlstate"},
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
					Query: `INSERT INTO arithmetic_minus_scores VALUES (3, 20, 7);`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0034-insert-into-arithmetic_minus_scores-values-3", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO arithmetic_mult_scores VALUES
						(1, 30, 7),
						(2, 30, 8);`,
				},
				{
					Query: `INSERT INTO arithmetic_mult_scores VALUES (3, 30, 7);`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0035-insert-into-arithmetic_mult_scores-values-3", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO arithmetic_commuted_plus_scores VALUES
						(1, 40, 7),
						(2, 40, 8);`,
				},
				{
					Query: `INSERT INTO arithmetic_commuted_plus_scores VALUES (3, 40, 7);`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0036-insert-into-arithmetic_commuted_plus_scores-values-3", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO arithmetic_commuted_mult_scores VALUES
						(1, 50, 7),
						(2, 50, 8);`,
				},
				{
					Query: `INSERT INTO arithmetic_commuted_mult_scores VALUES (3, 50, 7);`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0037-insert-into-arithmetic_commuted_mult_scores-values-3", Compare: "sqlstate"},
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
					Query: `INSERT INTO absolute_scores VALUES (3, 10, 10);`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0038-insert-into-absolute_scores-values-3", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO absolute_scores VALUES (4, 10, -5);`,
				},
				{
					Query: `UPDATE absolute_scores SET delta = 10 WHERE id = 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0039-update-absolute_scores-set-delta-=", Compare: "sqlstate"},
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
					Query: `INSERT INTO byte_payloads VALUES (3, 10, '\xAABBCC');`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0040-insert-into-byte_payloads-values-3", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO byte_payloads VALUES (4, 10, '\xAABB');`,
				},
				{
					Query: `UPDATE byte_payloads SET payload = '\xAABBCC' WHERE id = 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0041-update-byte_payloads-set-payload-=", Compare: "sqlstate"},
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
					Query: `INSERT INTO bit_payloads VALUES (3, 10, '\xAABBCC');`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0042-insert-into-bit_payloads-values-3", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO bit_payloads VALUES (4, 10, '\xAABB');`,
				},
				{
					Query: `UPDATE bit_payloads SET payload = '\xAABBCC' WHERE id = 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0043-update-bit_payloads-set-payload-=", Compare: "sqlstate"},
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
					Query: `INSERT INTO prefixed_codes VALUES (3, 10, 'active-b');`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0044-insert-into-prefixed_codes-values-3", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO prefixed_codes VALUES (4, 10, 'inactive');`,
				},
				{
					Query: `UPDATE prefixed_codes SET code = 'active-c' WHERE id = 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0045-update-prefixed_codes-set-code-=", Compare: "sqlstate"},
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
					Query: `INSERT INTO prefix_codes VALUES (3, 10, 'active-b');`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0046-insert-into-prefix_codes-values-3", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO prefix_codes VALUES (4, 10, 'inactive');`,
				},
				{
					Query: `UPDATE prefix_codes SET code = 'active-c' WHERE id = 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0047-update-prefix_codes-set-code-=", Compare: "sqlstate"},
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
					Query: `INSERT INTO like_prefix_codes VALUES (3, 10, 'active-b');`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0048-insert-into-like_prefix_codes-values-3", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO like_prefix_codes VALUES (4, 10, 'inactive');`,
				},
				{
					Query: `UPDATE like_prefix_codes SET code = 'active-c' WHERE id = 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0049-update-like_prefix_codes-set-code-=", Compare: "sqlstate"},
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
					Query: `INSERT INTO custom_ltrim_codes VALUES (3, 10, '00active');`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0050-insert-into-custom_ltrim_codes-values-3", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO custom_ltrim_codes VALUES (4, 10, '-active');`,
				},
				{
					Query: `UPDATE custom_ltrim_codes SET code = '0_active' WHERE id = 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0051-update-custom_ltrim_codes-set-code-=", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO custom_rtrim_codes VALUES
						(1, 20, 'active__'),
						(2, 20, 'pending');`,
				},
				{
					Query: `INSERT INTO custom_rtrim_codes VALUES (3, 20, 'active_');`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0052-insert-into-custom_rtrim_codes-values-3", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO custom_rtrim_codes VALUES (4, 20, 'active-');`,
				},
				{
					Query: `UPDATE custom_rtrim_codes SET code = 'active_' WHERE id = 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0053-update-custom_rtrim_codes-set-code-=", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO custom_btrim_codes VALUES
						(1, 30, 'x_active_'),
						(2, 30, 'pending');`,
				},
				{
					Query: `INSERT INTO custom_btrim_codes VALUES (3, 30, '_activex');`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0054-insert-into-custom_btrim_codes-values-3", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO custom_btrim_codes VALUES (4, 30, 'yactive');`,
				},
				{
					Query: `UPDATE custom_btrim_codes SET code = 'xactivex' WHERE id = 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0055-update-custom_btrim_codes-set-code-=", Compare: "sqlstate"},
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
					Query: `INSERT INTO repeat_codes VALUES (3, 10, 'active');`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0056-insert-into-repeat_codes-values-3", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO repeat_codes VALUES (4, 10, 'activeactive');`,
				},
				{
					Query: `UPDATE repeat_codes SET code = 'active' WHERE id = 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0057-update-repeat_codes-set-code-=", Compare: "sqlstate"},
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
					Query: `INSERT INTO signed_scores VALUES (3, 10, 20);`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0060-insert-into-signed_scores-values-3", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO signed_scores VALUES (4, 10, 0);`,
				},
				{
					Query: `UPDATE signed_scores SET delta = 1 WHERE id = 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0061-update-signed_scores-set-delta-=", Compare: "sqlstate"},
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
					Query: `INSERT INTO gcd_scores VALUES (3, 10, 16, 20);`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0062-insert-into-gcd_scores-values-3", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO gcd_scores VALUES (4, 10, 6, 10);`,
				},
				{
					Query: `UPDATE gcd_scores SET width = 12, height = 16 WHERE id = 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0063-update-gcd_scores-set-width-=", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO gcd_commuted_scores VALUES
						(1, 20, 8, 12),
						(2, 20, 9, 6);`,
				},
				{
					Query: `INSERT INTO gcd_commuted_scores VALUES (3, 20, 16, 20);`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0064-insert-into-gcd_commuted_scores-values-3", Compare: "sqlstate"},
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
					Query: `INSERT INTO lcm_scores VALUES (3, 10, 4, 6);`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0065-insert-into-lcm_scores-values-3", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO lcm_scores VALUES (4, 10, 5, 10);`,
				},
				{
					Query: `UPDATE lcm_scores SET width = 6, height = 12 WHERE id = 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0066-update-lcm_scores-set-width-=", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO lcm_scores VALUES (5, 11, 9223372036854775807, 9223372036854775806);`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0067-insert-into-lcm_scores-values-5", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO lcm_commuted_scores VALUES
						(1, 20, 3, 4),
						(2, 20, 5, 6);`,
				},
				{
					Query: `INSERT INTO lcm_commuted_scores VALUES (3, 20, 4, 6);`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0068-insert-into-lcm_commuted_scores-values-3", Compare: "sqlstate"},
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
					Query: `INSERT INTO mod_scores VALUES (3, 10, 10, 3);`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0069-insert-into-mod_scores-values-3", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO mod_scores VALUES (4, 10, 11, 3);`,
				},
				{
					Query: `UPDATE mod_scores SET account_id = 10 WHERE id = 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0070-update-mod_scores-set-account_id-=", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO mod_scores VALUES (5, 11, 3, 0);`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0071-insert-into-mod_scores-values-5", Compare: "sqlstate"},
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
					Query: `INSERT INTO chr_codes VALUES (3, 10, 65);`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0072-insert-into-chr_codes-values-3", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO chr_codes VALUES (4, 10, 67);`,
				},
				{
					Query: `UPDATE chr_codes SET codepoint = 65 WHERE id = 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0073-update-chr_codes-set-codepoint-=", Compare: "sqlstate"},
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
					Query: `INSERT INTO left_codes VALUES (3, 10, 'åctor');`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0074-insert-into-left_codes-values-3", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO left_codes VALUES (4, 10, 'archive');`,
				},
				{
					Query: `UPDATE left_codes SET code = 'åction' WHERE id = 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0075-update-left_codes-set-code-=", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO right_codes VALUES
						(1, 20, 'åctive'),
						(2, 20, 'pending');`,
				},
				{
					Query: `INSERT INTO right_codes VALUES (3, 20, 'bctive');`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0076-insert-into-right_codes-values-3", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO right_codes VALUES (4, 20, 'inactive');`,
				},
				{
					Query: `UPDATE right_codes SET code = 'cctive' WHERE id = 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0077-update-right_codes-set-code-=", Compare: "sqlstate"},
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
					Query: `INSERT INTO normalized_codes VALUES (3, 10, 'active--a');`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0078-insert-into-normalized_codes-values-3", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO normalized_codes VALUES (4, 10, 'active_a');`,
				},
				{
					Query: `UPDATE normalized_codes SET code = 'active-a' WHERE id = 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0079-update-normalized_codes-set-code-=", Compare: "sqlstate"},
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
					Query: `INSERT INTO translated_codes VALUES (3, 10, 'active__a');`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0080-insert-into-translated_codes-values-3", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO translated_codes VALUES (4, 10, 'active.a');`,
				},
				{
					Query: `UPDATE translated_codes SET code = 'active_a' WHERE id = 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0081-update-translated_codes-set-code-=", Compare: "sqlstate"},
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
					Query: `INSERT INTO hashed_codes VALUES (3, 10, 'active');`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0082-insert-into-hashed_codes-values-3", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO hashed_codes VALUES (4, 10, 'ACTIVE');`,
				},
				{
					Query: `UPDATE hashed_codes SET code = 'active' WHERE id = 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0083-update-hashed_codes-set-code-=", Compare: "sqlstate"},
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
					Query: `INSERT INTO hashtext_codes VALUES (3, 10, 'abc');`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0084-insert-into-hashtext_codes-values-3", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO hashtext_codes VALUES (4, 10, 'ABC');`,
				},
				{
					Query: `UPDATE hashtext_codes SET code = 'abc' WHERE id = 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0085-update-hashtext_codes-set-code-=", Compare: "sqlstate"},
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
					Query: `INSERT INTO rounded_floor_scores VALUES (3, 10, 7);`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0086-insert-into-rounded_floor_scores-values-3", Compare: "sqlstate"},
				},
				{
					Query: `UPDATE rounded_floor_scores SET score = 7 WHERE id = 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0087-update-rounded_floor_scores-set-score-=", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO rounded_ceiling_scores VALUES
						(1, 20, 9),
						(2, 20, 10);`,
				},
				{
					Query: `INSERT INTO rounded_ceiling_scores VALUES (3, 20, 9);`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0088-insert-into-rounded_ceiling_scores-values-3", Compare: "sqlstate"},
				},
				{
					Query: `UPDATE rounded_ceiling_scores SET score = 9 WHERE id = 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0089-update-rounded_ceiling_scores-set-score-=", Compare: "sqlstate"},
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
					Query: `INSERT INTO rounded_scores VALUES (3, 10, 7);`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0090-insert-into-rounded_scores-values-3", Compare: "sqlstate"},
				},
				{
					Query: `UPDATE rounded_scores SET score = 7 WHERE id = 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0091-update-rounded_scores-set-score-=", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO truncated_scores VALUES
						(1, 20, 9),
						(2, 20, 10);`,
				},
				{
					Query: `INSERT INTO truncated_scores VALUES (3, 20, 9);`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0092-insert-into-truncated_scores-values-3", Compare: "sqlstate"},
				},
				{
					Query: `UPDATE truncated_scores SET score = 9 WHERE id = 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0093-update-truncated_scores-set-score-=", Compare: "sqlstate"},
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
					Query: `INSERT INTO email_domains VALUES (3, 10, 'other@example.com');`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0094-insert-into-email_domains-values-3", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO email_domains VALUES (4, 10, 'missing-domain');`,
				},
				{
					Query: `UPDATE email_domains SET email = 'third@example.com' WHERE id = 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0095-update-email_domains-set-email-=", Compare: "sqlstate"},
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
					Query: `INSERT INTO ascii_codes VALUES (3, 10, 'Admin');`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0096-insert-into-ascii_codes-values-3", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO ascii_codes VALUES (4, 10, 'alpha');`,
				},
				{
					Query: `UPDATE ascii_codes SET code = 'April' WHERE id = 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0097-update-ascii_codes-set-code-=", Compare: "sqlstate"},
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
					Query: `INSERT INTO substring_codes VALUES (3, 10, 'Admiral');`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0098-insert-into-substring_codes-values-3", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO substring_codes VALUES (4, 10, 'admin');`,
				},
				{
					Query: `UPDATE substring_codes SET code = 'Admire' WHERE id = 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0099-update-substring_codes-set-code-=", Compare: "sqlstate"},
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
					Query: `INSERT INTO lpad_codes VALUES (3, 10, 'ABCD');`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0100-insert-into-lpad_codes-values-3", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO lpad_codes VALUES (4, 10, 'AXYZ');`,
				},
				{
					Query: `UPDATE lpad_codes SET code = 'ABCD' WHERE id = 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0101-update-lpad_codes-set-code-=", Compare: "sqlstate"},
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
					Query: `INSERT INTO rpad_codes VALUES (3, 10, 'ABCD');`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0102-insert-into-rpad_codes-values-3", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO rpad_codes VALUES (4, 10, 'AXYZ');`,
				},
				{
					Query: `UPDATE rpad_codes SET code = 'ABCD' WHERE id = 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0103-update-rpad_codes-set-code-=", Compare: "sqlstate"},
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
					Query: `INSERT INTO reverse_codes VALUES (3, 10, 'Admin');`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0104-insert-into-reverse_codes-values-3", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO reverse_codes VALUES (4, 10, 'admin');`,
				},
				{
					Query: `UPDATE reverse_codes SET code = 'Admin' WHERE id = 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0105-update-reverse_codes-set-code-=", Compare: "sqlstate"},
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
					Query: `INSERT INTO hex_codes VALUES (3, 10, 10);`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0106-insert-into-hex_codes-values-3", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO hex_codes VALUES (4, 10, 12);`,
				},
				{
					Query: `UPDATE hex_codes SET account_id = 10 WHERE id = 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0107-update-hex_codes-set-account_id-=", Compare: "sqlstate"},
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
					Query: `INSERT INTO initcap_roles VALUES (3, 10, 'admin user');`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0108-insert-into-initcap_roles-values-3", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO initcap_roles VALUES (4, 10, 'billing user');`,
				},
				{
					Query: `UPDATE initcap_roles SET role = 'admin user' WHERE id = 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0109-update-initcap_roles-set-role-=", Compare: "sqlstate"},
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
					Query: `INSERT INTO quote_literal_roles VALUES (3, 10, 'admin user');`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0110-insert-into-quote_literal_roles-values-3", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO quote_literal_roles VALUES (4, 10, 'billing user');`,
				},
				{
					Query: `UPDATE quote_literal_roles SET role = 'admin user' WHERE id = 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0111-update-quote_literal_roles-set-role-=", Compare: "sqlstate"},
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
					Query: `INSERT INTO quote_ident_roles VALUES (3, 10, 'admin user');`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0112-insert-into-quote_ident_roles-values-3", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO quote_ident_roles VALUES (4, 10, 'billing user');`,
				},
				{
					Query: `UPDATE quote_ident_roles SET role = 'admin user' WHERE id = 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0113-update-quote_ident_roles-set-role-=", Compare: "sqlstate"},
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
						WHERE status = 'active';`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0114-create-unique-index-duplicate_memberships_active_idx-on", Compare: "sqlstate"},
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
					Query: `SELECT count(*)::text FROM pg_indexes WHERE tablename = 'accounts' AND indexname = 'accounts_lower_email_idx';`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0115-select-count-*-::text-from"},
				},
				{
					Query: `SELECT id FROM accounts WHERE lower(email) = 'bob@x';`, PostgresOracle: ScriptTestPostgresOracle{ID: "partial-expression-index-test-testpartialandexpressionindexes-0116-select-id-from-accounts-where"},
				},
			},
		},
	})
}
