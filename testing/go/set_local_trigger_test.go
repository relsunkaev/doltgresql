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

// TestSetLocalReadFromTrigger is the audit-context workload harness
// covering three checklist items end-to-end:
//
//   - SET LOCAL — the SQL form
//   - set_config(..., is_local=true) — the function form
//   - current_setting(..., true) reads from triggers
//
// The pattern this exercises is the canonical one real applications use
// to record "who did this" without threading a user id through every
// statement: a BEFORE INSERT trigger written in PL/pgSQL reads
// current_setting('app.actor') and writes it into an audit row. The
// caller sets app.actor with SET LOCAL inside the same transaction. If
// the GUC propagates correctly into the trigger body, the audit row
// reflects the actor; if the rollback is honored, the audit row exists
// only when the surrounding transaction committed.
func TestSetLocalReadFromTrigger(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "trigger reads current_setting set via SET LOCAL within the same transaction",
			SetUpScript: []string{
				`CREATE TABLE accounts (id INT PRIMARY KEY, balance NUMERIC);`,
				`CREATE TABLE audit_log (id SERIAL PRIMARY KEY, op TEXT, account_id INT, actor TEXT);`,
				`CREATE FUNCTION audit_actor_fn() RETURNS TRIGGER AS $$
                 BEGIN
                     INSERT INTO audit_log (op, account_id, actor)
                     VALUES ('INSERT', NEW.id, current_setting('app.actor', true));
                     RETURN NEW;
                 END;
                 $$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER audit_actor
                 BEFORE INSERT ON accounts
                 FOR EACH ROW EXECUTE FUNCTION audit_actor_fn();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`, PostgresOracle: ScriptTestPostgresOracle{ID: "set-local-trigger-test-testsetlocalreadfromtrigger-0001-begin"},
				},
				{
					// SET LOCAL the actor for this transaction only.
					Query:            `SET LOCAL app.actor = 'alice';`,
					SkipResultsCheck: true,
				},
				{
					Query: `INSERT INTO accounts VALUES (1, 100);`, PostgresOracle: ScriptTestPostgresOracle{ID: "set-local-trigger-test-testsetlocalreadfromtrigger-0002-insert-into-accounts-values-1"},
				},
				{
					// Inside the txn the trigger has already fired and
					// the audit row reflects the SET LOCAL value.
					Query: `SELECT actor FROM audit_log WHERE account_id = 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "set-local-trigger-test-testsetlocalreadfromtrigger-0003-select-actor-from-audit_log-where"},
				},
				{
					Query: `COMMIT;`, PostgresOracle: ScriptTestPostgresOracle{ID: "set-local-trigger-test-testsetlocalreadfromtrigger-0004-commit"},
				},
				{
					// After COMMIT, audit row persists with the right
					// actor and the SET LOCAL value is gone from the
					// session.
					Query: `SELECT actor FROM audit_log WHERE account_id = 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "set-local-trigger-test-testsetlocalreadfromtrigger-0005-select-actor-from-audit_log-where"},
				},
				{
					Query: `SELECT current_setting('app.actor', true);`, PostgresOracle: ScriptTestPostgresOracle{ID: "set-local-trigger-test-testsetlocalreadfromtrigger-0006-select-current_setting-app.actor-true"},
				},
			},
		},
		{
			Name: "trigger reads value set via set_config(name, value, true)",
			SetUpScript: []string{
				`CREATE TABLE accounts (id INT PRIMARY KEY, balance NUMERIC);`,
				`CREATE TABLE audit_log (id SERIAL PRIMARY KEY, op TEXT, account_id INT, actor TEXT);`,
				`CREATE FUNCTION audit_actor_fn() RETURNS TRIGGER AS $$
                 BEGIN
                     INSERT INTO audit_log (op, account_id, actor)
                     VALUES ('INSERT', NEW.id, current_setting('app.actor', true));
                     RETURN NEW;
                 END;
                 $$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER audit_actor
                 BEFORE INSERT ON accounts
                 FOR EACH ROW EXECUTE FUNCTION audit_actor_fn();`,
			},
			Assertions: []ScriptTestAssertion{
				{Query: `BEGIN;`, PostgresOracle: ScriptTestPostgresOracle{

					// set_config returns the new value — not skipped.
					ID: "set-local-trigger-test-testsetlocalreadfromtrigger-0007-begin"}},
				{

					Query: `SELECT set_config('app.actor', 'bob', true);`, PostgresOracle: ScriptTestPostgresOracle{ID: "set-local-trigger-test-testsetlocalreadfromtrigger-0008-select-set_config-app.actor-bob-true"},
				},
				{
					Query: `INSERT INTO accounts VALUES (2, 250);`, PostgresOracle: ScriptTestPostgresOracle{ID: "set-local-trigger-test-testsetlocalreadfromtrigger-0009-insert-into-accounts-values-2"},
				},
				{
					Query: `SELECT actor FROM audit_log WHERE account_id = 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "set-local-trigger-test-testsetlocalreadfromtrigger-0010-select-actor-from-audit_log-where"},
				},
				{Query: `COMMIT;`, PostgresOracle: ScriptTestPostgresOracle{ID: "set-local-trigger-test-testsetlocalreadfromtrigger-0011-commit"}},
				{
					Query: `SELECT current_setting('app.actor', true);`, PostgresOracle: ScriptTestPostgresOracle{ID: "set-local-trigger-test-testsetlocalreadfromtrigger-0012-select-current_setting-app.actor-true"},
				},
			},
		},
		{
			Name: "rollback discards the trigger-written audit row and reverts the GUC",
			SetUpScript: []string{
				`CREATE TABLE accounts (id INT PRIMARY KEY, balance NUMERIC);`,
				`CREATE TABLE audit_log (id SERIAL PRIMARY KEY, op TEXT, account_id INT, actor TEXT);`,
				`CREATE FUNCTION audit_actor_fn() RETURNS TRIGGER AS $$
                 BEGIN
                     INSERT INTO audit_log (op, account_id, actor)
                     VALUES ('INSERT', NEW.id, current_setting('app.actor', true));
                     RETURN NEW;
                 END;
                 $$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER audit_actor
                 BEFORE INSERT ON accounts
                 FOR EACH ROW EXECUTE FUNCTION audit_actor_fn();`,
			},
			Assertions: []ScriptTestAssertion{
				{Query: `BEGIN;`, PostgresOracle: ScriptTestPostgresOracle{ID: "set-local-trigger-test-testsetlocalreadfromtrigger-0013-begin"}},
				{
					Query:            `SET LOCAL app.actor = 'carol';`,
					SkipResultsCheck: true,
				},
				{
					Query: `INSERT INTO accounts VALUES (3, 17);`, PostgresOracle: ScriptTestPostgresOracle{ID: "set-local-trigger-test-testsetlocalreadfromtrigger-0014-insert-into-accounts-values-3"},
				},
				{
					Query: `SELECT actor FROM audit_log WHERE account_id = 3;`, PostgresOracle: ScriptTestPostgresOracle{ID: "set-local-trigger-test-testsetlocalreadfromtrigger-0015-select-actor-from-audit_log-where"},
				},
				{Query: `ROLLBACK;`, PostgresOracle: ScriptTestPostgresOracle{

					// The audit row was rolled back along with the
					// account row, so it must be gone.
					ID: "set-local-trigger-test-testsetlocalreadfromtrigger-0016-rollback"}},
				{

					Query: `SELECT actor FROM audit_log WHERE account_id = 3;`, PostgresOracle: ScriptTestPostgresOracle{ID: "set-local-trigger-test-testsetlocalreadfromtrigger-0017-select-actor-from-audit_log-where"},
				},
				{
					// And the SET LOCAL value is reverted.
					Query: `SELECT current_setting('app.actor', true);`, PostgresOracle: ScriptTestPostgresOracle{ID: "set-local-trigger-test-testsetlocalreadfromtrigger-0018-select-current_setting-app.actor-true"},
				},
			},
		},
		{
			// Workload pattern: a session-scope app.actor (via
			// set_config(..., false)) is overridden inside a transaction
			// by SET LOCAL, then must be restored on COMMIT.
			Name: "SET LOCAL inside a transaction restores the session-scope actor on COMMIT",
			SetUpScript: []string{
				`CREATE TABLE accounts (id INT PRIMARY KEY, balance NUMERIC);`,
				`CREATE TABLE audit_log (id SERIAL PRIMARY KEY, op TEXT, account_id INT, actor TEXT);`,
				`CREATE FUNCTION audit_actor_fn() RETURNS TRIGGER AS $$
                 BEGIN
                     INSERT INTO audit_log (op, account_id, actor)
                     VALUES ('INSERT', NEW.id, current_setting('app.actor', true));
                     RETURN NEW;
                 END;
                 $$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER audit_actor
                 BEFORE INSERT ON accounts
                 FOR EACH ROW EXECUTE FUNCTION audit_actor_fn();`,
				// Establish a session-wide default actor.
				`SELECT set_config('app.actor', 'service-account', false);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					// Outside the transaction, the trigger sees the
					// session-scope actor.
					Query: `INSERT INTO accounts VALUES (10, 1);`, PostgresOracle: ScriptTestPostgresOracle{ID: "set-local-trigger-test-testsetlocalreadfromtrigger-0019-insert-into-accounts-values-10"},
				},
				{
					Query: `SELECT actor FROM audit_log WHERE account_id = 10;`, PostgresOracle: ScriptTestPostgresOracle{ID: "set-local-trigger-test-testsetlocalreadfromtrigger-0020-select-actor-from-audit_log-where"},
				},
				{Query: `BEGIN;`, PostgresOracle: ScriptTestPostgresOracle{ID: "set-local-trigger-test-testsetlocalreadfromtrigger-0021-begin"}},
				{
					Query:            `SET LOCAL app.actor = 'on-call';`,
					SkipResultsCheck: true,
				},
				{
					// Inside the transaction the trigger uses the
					// SET LOCAL override.
					Query: `INSERT INTO accounts VALUES (11, 1);`, PostgresOracle: ScriptTestPostgresOracle{ID: "set-local-trigger-test-testsetlocalreadfromtrigger-0022-insert-into-accounts-values-11"},
				},
				{
					Query: `SELECT actor FROM audit_log WHERE account_id = 11;`, PostgresOracle: ScriptTestPostgresOracle{ID: "set-local-trigger-test-testsetlocalreadfromtrigger-0023-select-actor-from-audit_log-where"},
				},
				{Query: `COMMIT;`, PostgresOracle: ScriptTestPostgresOracle{

					// After COMMIT, the session-scope actor is back.
					ID: "set-local-trigger-test-testsetlocalreadfromtrigger-0024-commit"}},
				{

					Query: `INSERT INTO accounts VALUES (12, 1);`, PostgresOracle: ScriptTestPostgresOracle{ID: "set-local-trigger-test-testsetlocalreadfromtrigger-0025-insert-into-accounts-values-12"},
				},
				{
					Query: `SELECT actor FROM audit_log WHERE account_id = 12;`, PostgresOracle: ScriptTestPostgresOracle{ID: "set-local-trigger-test-testsetlocalreadfromtrigger-0026-select-actor-from-audit_log-where"},
				},
			},
		},
	})
}
