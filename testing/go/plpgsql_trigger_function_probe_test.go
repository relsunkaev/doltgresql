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
			// BEFORE INSERT trigger that assigns to a NEW field via
			// `NEW.marked := upper(NEW.label)`. This used to panic
			// with `index out of range [2] with length 2` in
			// plpgsql.InterpreterStack.GetVariable when the INSERT
			// did not specify every column (NEW row was shorter
			// than the schema). Fixed by padding NEW/OLD rows to
			// schema length in InterpreterStack.NewRecord.
			//
			// The full-column-list form below is what works
			// end-to-end: the trigger fires, NEW.marked is assigned
			// to upper(NEW.label), and the modified value is
			// persisted. The partial-column-list form
			// (`INSERT (id, label) VALUES (...)` — i.e. omitting the
			// trigger-target column from the INSERT) no longer
			// panics, but the trigger's modification does not yet
			// flow back into the inserted row; that's a separate
			// gap pinned in the partial-column subtest below.
			Name: "BEFORE INSERT NEW-field assignment with full column list",
			SetUpScript: []string{
				`CREATE TABLE rows (id INT PRIMARY KEY, label TEXT, marked TEXT);`,
				`CREATE FUNCTION mark_label() RETURNS trigger AS $$
					BEGIN
						NEW.marked := upper(NEW.label);
						RETURN NEW;
					END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER mark_before_insert
					BEFORE INSERT ON rows
					FOR EACH ROW EXECUTE FUNCTION mark_label();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO rows (id, label, marked) VALUES (1, 'hello', NULL), (2, 'world', NULL);`,
				},
				{
					Query: `SELECT id, marked FROM rows ORDER BY id;`,
					Expected: []sql.Row{
						{int32(1), "HELLO"},
						{int32(2), "WORLD"},
					},
				},
			},
		},
		{
			// Partial-column INSERT: trigger no longer panics
			// (NewRecord now pads the NEW row to schema length), but
			// the BEFORE trigger's modification of NEW does not yet
			// propagate back into the inserted row. The row is
			// inserted with the trigger-target column NULL (or the
			// column default), not the upper-cased value the
			// trigger computed. That's a separate gap in the row-
			// flow plumbing — fixing it needs the inserter to use
			// the trigger-returned row positions for columns the
			// INSERT statement omitted.
			Name: "BEFORE INSERT partial-column INSERT does not yet propagate NEW changes",
			SetUpScript: []string{
				`CREATE TABLE partial_rows (id INT PRIMARY KEY, label TEXT, marked TEXT);`,
				`CREATE FUNCTION mark_label_partial() RETURNS trigger AS $$
					BEGIN
						NEW.marked := upper(NEW.label);
						RETURN NEW;
					END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER mark_before_insert_partial
					BEFORE INSERT ON partial_rows
					FOR EACH ROW EXECUTE FUNCTION mark_label_partial();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO partial_rows (id, label) VALUES (1, 'hello');`,
				},
				{
					// PG-correct: marked='HELLO'. Today: marked=NULL.
					// Pin current behaviour so the gap is visible.
					Query:    `SELECT marked IS NULL FROM partial_rows WHERE id = 1;`,
					Expected: []sql.Row{{"t"}},
				},
			},
		},
	})
}
