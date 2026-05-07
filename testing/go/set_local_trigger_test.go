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
					Query:    `BEGIN;`,
					Expected: []sql.Row{},
				},
				{
					// SET LOCAL the actor for this transaction only.
					Query:            `SET LOCAL app.actor = 'alice';`,
					SkipResultsCheck: true,
				},
				{
					Query:    `INSERT INTO accounts VALUES (1, 100);`,
					Expected: []sql.Row{},
				},
				{
					// Inside the txn the trigger has already fired and
					// the audit row reflects the SET LOCAL value.
					Query:    `SELECT actor FROM audit_log WHERE account_id = 1;`,
					Expected: []sql.Row{{"alice"}},
				},
				{
					Query:    `COMMIT;`,
					Expected: []sql.Row{},
				},
				{
					// After COMMIT, audit row persists with the right
					// actor and the SET LOCAL value is gone from the
					// session.
					Query:    `SELECT actor FROM audit_log WHERE account_id = 1;`,
					Expected: []sql.Row{{"alice"}},
				},
				{
					Query:    `SELECT current_setting('app.actor', true);`,
					Expected: []sql.Row{{nil}},
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
				{Query: `BEGIN;`, Expected: []sql.Row{}},
				{
					// set_config returns the new value — not skipped.
					Query:    `SELECT set_config('app.actor', 'bob', true);`,
					Expected: []sql.Row{{"bob"}},
				},
				{
					Query:    `INSERT INTO accounts VALUES (2, 250);`,
					Expected: []sql.Row{},
				},
				{
					Query:    `SELECT actor FROM audit_log WHERE account_id = 2;`,
					Expected: []sql.Row{{"bob"}},
				},
				{Query: `COMMIT;`, Expected: []sql.Row{}},
				{
					Query:    `SELECT current_setting('app.actor', true);`,
					Expected: []sql.Row{{nil}},
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
				{Query: `BEGIN;`, Expected: []sql.Row{}},
				{
					Query:            `SET LOCAL app.actor = 'carol';`,
					SkipResultsCheck: true,
				},
				{
					Query:    `INSERT INTO accounts VALUES (3, 17);`,
					Expected: []sql.Row{},
				},
				{
					Query:    `SELECT actor FROM audit_log WHERE account_id = 3;`,
					Expected: []sql.Row{{"carol"}},
				},
				{Query: `ROLLBACK;`, Expected: []sql.Row{}},
				{
					// The audit row was rolled back along with the
					// account row, so it must be gone.
					Query:    `SELECT actor FROM audit_log WHERE account_id = 3;`,
					Expected: []sql.Row{},
				},
				{
					// And the SET LOCAL value is reverted.
					Query:    `SELECT current_setting('app.actor', true);`,
					Expected: []sql.Row{{nil}},
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
					Query:    `INSERT INTO accounts VALUES (10, 1);`,
					Expected: []sql.Row{},
				},
				{
					Query:    `SELECT actor FROM audit_log WHERE account_id = 10;`,
					Expected: []sql.Row{{"service-account"}},
				},
				{Query: `BEGIN;`, Expected: []sql.Row{}},
				{
					Query:            `SET LOCAL app.actor = 'on-call';`,
					SkipResultsCheck: true,
				},
				{
					// Inside the transaction the trigger uses the
					// SET LOCAL override.
					Query:    `INSERT INTO accounts VALUES (11, 1);`,
					Expected: []sql.Row{},
				},
				{
					Query:    `SELECT actor FROM audit_log WHERE account_id = 11;`,
					Expected: []sql.Row{{"on-call"}},
				},
				{Query: `COMMIT;`, Expected: []sql.Row{}},
				{
					// After COMMIT, the session-scope actor is back.
					Query:    `INSERT INTO accounts VALUES (12, 1);`,
					Expected: []sql.Row{},
				},
				{
					Query:    `SELECT actor FROM audit_log WHERE account_id = 12;`,
					Expected: []sql.Row{{"service-account"}},
				},
			},
		},
	})
}
