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

// TestPlpgsqlTriggerFunctionProbe pins the canonical PL/pgSQL trigger
// function shape dumps emit: `CREATE FUNCTION ... RETURNS trigger AS
// $$ ... $$ LANGUAGE plpgsql;` plus a `CREATE TRIGGER` that wires it
// up. Per the Schema/DDL TODO in
// docs/app-compatibility-checklist.md.
func TestPlpgsqlTriggerFunctionProbe(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			// AFTER INSERT trigger function that writes to a side
			// audit table — the audit-context pattern that already
			// works end-to-end via testing/go/set_local_trigger_test.go.
			// This pins the standalone shape without the SET LOCAL
			// dependency.
			Name: "AFTER INSERT trigger function writes to audit table",
			SetUpScript: []string{
				`CREATE TABLE main (id INT PRIMARY KEY, v INT);`,
				`CREATE TABLE audit_log (id SERIAL PRIMARY KEY, main_id INT, marked TEXT);`,
				`CREATE FUNCTION log_main_insert() RETURNS trigger AS $$
					BEGIN
						INSERT INTO audit_log (main_id, marked)
						VALUES (NEW.id, 'inserted');
						RETURN NEW;
					END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER log_after_insert
					AFTER INSERT ON main
					FOR EACH ROW EXECUTE FUNCTION log_main_insert();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO main (id, v) VALUES (1, 100), (2, 200);`,
				},
				{
					Query: `SELECT main_id, marked FROM audit_log ORDER BY main_id;`,
					Expected: []sql.Row{
						{int32(1), "inserted"},
						{int32(2), "inserted"},
					},
				},
			},
		},
		{
			// BEFORE INSERT triggers that *assign* to a NEW field
			// (e.g. `NEW.marked := upper(NEW.label)`) panic the
			// server today with `index out of range [2] with
			// length 2` in plpgsql.InterpreterStack.GetVariable —
			// see server/plpgsql/interpreter_stack.go:133. Pin the
			// shape so the gap is visible. PG-correct semantics
			// would compute and store the upper-cased value.
			Name: "BEFORE INSERT NEW-field assignment panics (residual gap)",
			SetUpScript: []string{
				`CREATE TABLE will_panic (id INT PRIMARY KEY, label TEXT, marked TEXT);`,
				`CREATE FUNCTION mark_label() RETURNS trigger AS $$
					BEGIN
						NEW.marked := upper(NEW.label);
						RETURN NEW;
					END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER mark_before_insert
					BEFORE INSERT ON will_panic
					FOR EACH ROW EXECUTE FUNCTION mark_label();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `INSERT INTO will_panic (id, label) VALUES (1, 'hello');`,
					ExpectedErr: "index out of range",
				},
			},
		},
	})
}
