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

// TestBeforeInsertTriggerReturningNullSkipsRowRepro guards that a row-level
// BEFORE INSERT trigger returning NULL skips inserting that row.
func TestBeforeInsertTriggerReturningNullSkipsRowRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "BEFORE INSERT trigger returning NULL skips row",
			SetUpScript: []string{
				`CREATE TABLE trigger_skip_target (pk INT PRIMARY KEY, v1 TEXT);`,
				`CREATE TABLE trigger_skip_audit (pk INT PRIMARY KEY, v1 TEXT);`,
				`CREATE FUNCTION trigger_skip_func() RETURNS TRIGGER AS $$
				BEGIN
					NEW.v1 := NEW.v1 || '_' || NEW.pk::text;
					INSERT INTO trigger_skip_audit VALUES (NEW.pk, NEW.v1);
					RETURN NULL;
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER trigger_skip_before_insert
					BEFORE INSERT ON trigger_skip_target
					FOR EACH ROW EXECUTE FUNCTION trigger_skip_func();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO trigger_skip_target VALUES (1, 'hi'), (2, 'there');`,
				},
				{
					Query:    `SELECT * FROM trigger_skip_target ORDER BY pk;`,
					Expected: []sql.Row{},
				},
				{
					Query: `SELECT * FROM trigger_skip_audit ORDER BY pk;`,
					Expected: []sql.Row{
						{1, "hi_1"},
						{2, "there_2"},
					},
				},
			},
		},
	})
}

// TestTriggerArgumentsPopulateTgArgvRepro reproduces a trigger correctness bug:
// trigger arguments should be available through TG_ARGV inside PL/pgSQL
// trigger functions.
func TestTriggerArgumentsPopulateTgArgvRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "trigger arguments populate TG_ARGV",
			SetUpScript: []string{
				`CREATE TABLE trigger_argument_target (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`CREATE TABLE trigger_argument_audit (
					id INT PRIMARY KEY,
					arg_count INT,
					first_arg TEXT,
					second_arg TEXT
				);`,
				`CREATE FUNCTION trigger_argument_func() RETURNS TRIGGER AS $$
				BEGIN
					INSERT INTO trigger_argument_audit
					VALUES (NEW.id, TG_NARGS, TG_ARGV[0], TG_ARGV[1]);
					RETURN NEW;
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER trigger_argument_after_insert
					AFTER INSERT ON trigger_argument_target
					FOR EACH ROW
					EXECUTE FUNCTION trigger_argument_func('alpha', 'beta');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO trigger_argument_target VALUES (1, 'payload');`,
				},
				{
					Query: `SELECT id, arg_count, first_arg, second_arg
						FROM trigger_argument_audit;`,
					Expected: []sql.Row{
						{int64(1), int64(2), "alpha", "beta"},
					},
				},
			},
		},
	})
}

// TestBeforeInsertTriggerMutatedRowChecksConstraintsRepro guards that table
// constraints are checked against the row after BEFORE INSERT triggers mutate
// NEW, not only against the original input tuple.
func TestBeforeInsertTriggerMutatedRowChecksConstraintsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "BEFORE INSERT trigger-mutated row checks constraints",
			SetUpScript: []string{
				`CREATE TABLE trigger_check_target (
					id INT PRIMARY KEY,
					qty INT NOT NULL CHECK (qty > 0)
				);`,
				`CREATE FUNCTION make_trigger_check_invalid() RETURNS TRIGGER AS $$
				BEGIN
					NEW.qty := -NEW.qty;
					RETURN NEW;
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER trigger_check_before_insert
					BEFORE INSERT ON trigger_check_target
					FOR EACH ROW EXECUTE FUNCTION make_trigger_check_invalid();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `INSERT INTO trigger_check_target VALUES (1, 5);`,
					ExpectedErr: `Check constraint`,
				},
				{
					Query:    `SELECT count(*) FROM trigger_check_target;`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
	})
}

// TestBeforeInsertTriggerReturningSeesMutatedRowRepro guards that INSERT
// RETURNING reports the row after BEFORE INSERT triggers mutate NEW.
func TestBeforeInsertTriggerReturningSeesMutatedRowRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "INSERT RETURNING sees BEFORE INSERT trigger-mutated row",
			SetUpScript: []string{
				`CREATE TABLE trigger_returning_target (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`CREATE FUNCTION mutate_trigger_returning_label() RETURNS TRIGGER AS $$
				BEGIN
					NEW.label := upper(NEW.label) || '_triggered';
					RETURN NEW;
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER trigger_returning_before_insert
					BEFORE INSERT ON trigger_returning_target
					FOR EACH ROW EXECUTE FUNCTION mutate_trigger_returning_label();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `INSERT INTO trigger_returning_target VALUES (1, 'alpha') RETURNING label;`,
					Expected: []sql.Row{{"ALPHA_triggered"}},
				},
				{
					Query:    `SELECT label FROM trigger_returning_target WHERE id = 1;`,
					Expected: []sql.Row{{"ALPHA_triggered"}},
				},
			},
		},
	})
}

// TestBeforeUpdateTriggerMutatedRowChecksConstraintsRepro guards that UPDATE
// constraints are checked after BEFORE UPDATE triggers mutate NEW.
func TestBeforeUpdateTriggerMutatedRowChecksConstraintsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "BEFORE UPDATE trigger-mutated row checks constraints",
			SetUpScript: []string{
				`CREATE TABLE trigger_update_check_target (
					id INT PRIMARY KEY,
					qty INT NOT NULL CHECK (qty > 0)
				);`,
				`INSERT INTO trigger_update_check_target VALUES (1, 5);`,
				`CREATE FUNCTION make_trigger_update_check_invalid() RETURNS TRIGGER AS $$
				BEGIN
					NEW.qty := -NEW.qty;
					RETURN NEW;
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER trigger_update_check_before_update
					BEFORE UPDATE ON trigger_update_check_target
					FOR EACH ROW EXECUTE FUNCTION make_trigger_update_check_invalid();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `UPDATE trigger_update_check_target SET qty = 6 WHERE id = 1;`,
					ExpectedErr: `Check constraint`,
				},
				{
					Query:    `SELECT qty FROM trigger_update_check_target WHERE id = 1;`,
					Expected: []sql.Row{{5}},
				},
			},
		},
	})
}

// TestOnConflictUpdateWhereFalseDoesNotFireUpdateTriggersRepro reproduces an
// UPSERT/trigger correctness bug: PostgreSQL does not fire row-level UPDATE
// triggers when the ON CONFLICT DO UPDATE WHERE predicate rejects the
// conflicting row.
func TestOnConflictUpdateWhereFalseDoesNotFireUpdateTriggersRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ON CONFLICT DO UPDATE WHERE false does not fire UPDATE triggers",
			SetUpScript: []string{
				`CREATE TABLE upsert_where_false_trigger_target (
					id INT PRIMARY KEY,
					v TEXT NOT NULL
				);`,
				`CREATE TABLE upsert_where_false_trigger_audit (
					id INT,
					old_v TEXT,
					new_v TEXT
				);`,
				`CREATE FUNCTION audit_upsert_where_false_update() RETURNS TRIGGER AS $$
				BEGIN
					INSERT INTO upsert_where_false_trigger_audit VALUES (NEW.id, OLD.v, NEW.v);
					RETURN NEW;
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER upsert_where_false_after_update
					AFTER UPDATE ON upsert_where_false_trigger_target
					FOR EACH ROW EXECUTE FUNCTION audit_upsert_where_false_update();`,
				`INSERT INTO upsert_where_false_trigger_target VALUES (1, 'old');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO upsert_where_false_trigger_target VALUES (1, 'new')
						ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v
						WHERE false;`,
				},
				{
					Query:    `SELECT count(*) FROM upsert_where_false_trigger_audit;`,
					Expected: []sql.Row{{int64(0)}},
				},
				{
					Query:    `SELECT id, v FROM upsert_where_false_trigger_target;`,
					Expected: []sql.Row{{1, "old"}},
				},
			},
		},
	})
}

// TestBeforeInsertTriggerSeesEarlierRowsInSameStatementRepro reproduces a
// trigger correctness bug: row-level triggers in a multi-row INSERT should see
// rows already inserted earlier in the same statement.
func TestBeforeInsertTriggerSeesEarlierRowsInSameStatementRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "BEFORE INSERT trigger sees earlier rows in same statement",
			SetUpScript: []string{
				`CREATE TABLE trigger_visibility_items (pk INT PRIMARY KEY, v1 TEXT);`,
				`CREATE FUNCTION trigger_visibility_insert_func() RETURNS TRIGGER AS $$
				BEGIN
					UPDATE trigger_visibility_items SET v1 = v1 || NEW.pk::text;
					RETURN NEW;
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE FUNCTION trigger_visibility_update_func() RETURNS TRIGGER AS $$
				BEGIN
					NEW.v1 := NEW.v1 || '_u';
					RETURN NEW;
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER trigger_visibility_before_insert
					BEFORE INSERT ON trigger_visibility_items
					FOR EACH ROW EXECUTE FUNCTION trigger_visibility_insert_func();`,
				`CREATE TRIGGER trigger_visibility_before_update
					BEFORE UPDATE ON trigger_visibility_items
					FOR EACH ROW EXECUTE FUNCTION trigger_visibility_update_func();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO trigger_visibility_items VALUES (1, 'hi'), (2, 'there');`,
				},
				{
					Query: `SELECT * FROM trigger_visibility_items ORDER BY pk;`,
					Expected: []sql.Row{
						{1, "hi2_u"},
						{2, "there"},
					},
				},
			},
		},
	})
}

// TestUpdateFromFiresRowTriggersRepro reproduces a trigger correctness bug:
// UPDATE ... FROM should fire row-level UPDATE triggers for changed target rows.
func TestUpdateFromFiresRowTriggersRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "UPDATE FROM fires row-level update triggers",
			SetUpScript: []string{
				`CREATE TABLE update_from_trigger_departments (
					id INT PRIMARY KEY,
					bonus INT
				);`,
				`CREATE TABLE update_from_trigger_employees (
					id INT PRIMARY KEY,
					department_id INT REFERENCES update_from_trigger_departments(id),
					salary INT
				);`,
				`CREATE TABLE update_from_trigger_log (
					employee_id INT,
					old_salary INT,
					new_salary INT
				);`,
				`INSERT INTO update_from_trigger_departments VALUES (1, 1000), (2, 500);`,
				`INSERT INTO update_from_trigger_employees VALUES (1, 1, 50000), (2, 2, 45000);`,
				`CREATE FUNCTION log_update_from_salary_change() RETURNS TRIGGER AS $$
				BEGIN
					IF NEW.salary <> OLD.salary THEN
						INSERT INTO update_from_trigger_log VALUES (OLD.id, OLD.salary, NEW.salary);
					END IF;
					RETURN NEW;
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER update_from_salary_change
					AFTER UPDATE ON update_from_trigger_employees
					FOR EACH ROW EXECUTE FUNCTION log_update_from_salary_change();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE update_from_trigger_employees AS e
						SET salary = salary + d.bonus
						FROM update_from_trigger_departments AS d
						WHERE e.department_id = d.id;`,
				},
				{
					Query: `SELECT employee_id, old_salary, new_salary
						FROM update_from_trigger_log
						ORDER BY employee_id;`,
					Expected: []sql.Row{
						{1, 50000, 51000},
						{2, 45000, 45500},
					},
				},
			},
		},
	})
}

// TestBeforeInsertTriggerTableTypedRecordAssignmentRepro reproduces a PL/pgSQL
// trigger correctness bug: table-typed trigger variables should expose row
// fields and assignments back to NEW should affect the inserted row.
func TestBeforeInsertTriggerTableTypedRecordAssignmentRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "BEFORE INSERT trigger table-typed record assignment persists",
			SetUpScript: []string{
				`CREATE TABLE trigger_record_items (
					id INT4 PRIMARY KEY,
					name TEXT NOT NULL,
					qty INT4 NOT NULL,
					price REAL NOT NULL
				);`,
				`CREATE FUNCTION normalize_trigger_record_qty() RETURNS TRIGGER AS $$
				DECLARE
					rec trigger_record_items;
				BEGIN
					rec := NEW;
					IF rec.qty < 0 THEN
						rec.qty := -rec.qty;
					END IF;
					NEW := rec;
					RETURN NEW;
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER normalize_record_before_insert
					BEFORE INSERT ON trigger_record_items
					FOR EACH ROW EXECUTE FUNCTION normalize_trigger_record_qty();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO trigger_record_items VALUES
						(1, 'apple', 3, 2.5),
						(2, 'banana', -5, -1.2);`,
				},
				{
					Query: `SELECT id, name, qty, price
						FROM trigger_record_items
						ORDER BY id;`,
					Expected: []sql.Row{
						{1, "apple", 3, 2.5},
						{2, "banana", 5, -1.2},
					},
				},
			},
		},
	})
}

// TestInsteadOfInsertTriggerOnViewRepro reproduces a view-trigger correctness
// bug: PostgreSQL allows INSTEAD OF triggers on views to define explicit write
// behavior for otherwise non-updatable view shapes.
func TestInsteadOfInsertTriggerOnViewRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "INSTEAD OF INSERT trigger on view routes writes to base table",
			SetUpScript: []string{
				`CREATE TABLE instead_view_trigger_base (
					id INT PRIMARY KEY,
					label TEXT NOT NULL
				);`,
				`CREATE VIEW instead_view_trigger_items AS
					SELECT id, label FROM instead_view_trigger_base;`,
				`CREATE FUNCTION route_instead_view_insert() RETURNS TRIGGER AS $$
				BEGIN
					INSERT INTO instead_view_trigger_base
					VALUES (NEW.id, upper(NEW.label));
					RETURN NEW;
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER instead_view_trigger_insert
					INSTEAD OF INSERT ON instead_view_trigger_items
					FOR EACH ROW EXECUTE FUNCTION route_instead_view_insert();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO instead_view_trigger_items VALUES (1, 'alpha');`,
				},
				{
					Query: `SELECT id, label
						FROM instead_view_trigger_base;`,
					Expected: []sql.Row{{1, "ALPHA"}},
				},
			},
		},
	})
}

// TestAlterTableDisableEnableTriggerRepro reproduces a trigger correctness bug:
// PostgreSQL supports ALTER TABLE ... DISABLE/ENABLE TRIGGER to control whether
// an existing user trigger fires.
func TestAlterTableDisableEnableTriggerRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TABLE disables and re-enables a user trigger",
			SetUpScript: []string{
				`CREATE TABLE trigger_toggle_target (
					id INT PRIMARY KEY,
					label TEXT NOT NULL
				);`,
				`CREATE TABLE trigger_toggle_log (
					id INT PRIMARY KEY,
					label TEXT NOT NULL
				);`,
				`CREATE FUNCTION log_trigger_toggle_insert() RETURNS TRIGGER AS $$
				BEGIN
					INSERT INTO trigger_toggle_log VALUES (NEW.id, NEW.label);
					RETURN NEW;
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER trigger_toggle_after_insert
					AFTER INSERT ON trigger_toggle_target
					FOR EACH ROW EXECUTE FUNCTION log_trigger_toggle_insert();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE trigger_toggle_target DISABLE TRIGGER trigger_toggle_after_insert;`,
				},
				{
					Query: `INSERT INTO trigger_toggle_target VALUES (1, 'disabled');`,
				},
				{
					Query:    `SELECT * FROM trigger_toggle_log ORDER BY id;`,
					Expected: []sql.Row{},
				},
				{
					Query: `ALTER TABLE trigger_toggle_target ENABLE TRIGGER trigger_toggle_after_insert;`,
				},
				{
					Query: `INSERT INTO trigger_toggle_target VALUES (2, 'enabled');`,
				},
				{
					Query: `SELECT * FROM trigger_toggle_log ORDER BY id;`,
					Expected: []sql.Row{
						{2, "enabled"},
					},
				},
			},
		},
	})
}

// TestAlterTriggerRenameRepro reproduces a trigger DDL correctness bug:
// PostgreSQL supports renaming existing triggers and persists the new name in
// pg_trigger.
func TestAlterTriggerRenameRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TRIGGER renames a user trigger",
			SetUpScript: []string{
				`CREATE TABLE trigger_rename_target (
					id INT PRIMARY KEY,
					label TEXT NOT NULL
				);`,
				`CREATE FUNCTION trigger_rename_func() RETURNS TRIGGER AS $$
				BEGIN
					NEW.label := NEW.label || ':triggered';
					RETURN NEW;
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER trigger_rename_before_insert
					BEFORE INSERT ON trigger_rename_target
					FOR EACH ROW EXECUTE FUNCTION trigger_rename_func();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TRIGGER trigger_rename_before_insert
						ON trigger_rename_target
						RENAME TO trigger_renamed_before_insert;`,
				},
				{
					Query: `SELECT tgname
						FROM pg_catalog.pg_trigger
						WHERE tgrelid = 'trigger_rename_target'::regclass
							AND NOT tgisinternal
						ORDER BY tgname;`,
					Expected: []sql.Row{{"trigger_renamed_before_insert"}},
				},
				{
					Query: `INSERT INTO trigger_rename_target VALUES (1, 'row');`,
				},
				{
					Query:    `SELECT label FROM trigger_rename_target;`,
					Expected: []sql.Row{{"row:triggered"}},
				},
			},
		},
	})
}

// TestTriggerFunctionCannotBeCalledDirectlyRepro guards PostgreSQL's trigger
// execution contract: trigger functions require trigger context and cannot be
// invoked as ordinary scalar functions.
func TestTriggerFunctionCannotBeCalledDirectlyRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "trigger function cannot be called directly",
			SetUpScript: []string{
				`CREATE FUNCTION trigger_direct_call_func() RETURNS TRIGGER AS $$
				BEGIN
					RETURN NEW;
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `SELECT trigger_direct_call_func();`,
					ExpectedErr: "trigger functions can only be called as triggers",
				},
			},
		},
	})
}

// TestDeferrableConstraintTriggerFiresAtCommitRepro reproduces a trigger
// correctness bug: PostgreSQL supports deferrable constraint triggers that run
// at transaction end rather than immediately after the statement.
func TestDeferrableConstraintTriggerFiresAtCommitRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "deferrable constraint trigger fires at commit",
			SetUpScript: []string{
				`CREATE TABLE constraint_trigger_target (
					id INT PRIMARY KEY,
					v INT NOT NULL
				);`,
				`CREATE TABLE constraint_trigger_audit (
					id INT PRIMARY KEY,
					v INT NOT NULL
				);`,
				`CREATE FUNCTION audit_constraint_trigger_insert() RETURNS TRIGGER AS $$
				BEGIN
					INSERT INTO constraint_trigger_audit VALUES (NEW.id, NEW.v);
					RETURN NEW;
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE CONSTRAINT TRIGGER audit_constraint_trigger
					AFTER INSERT ON constraint_trigger_target
					DEFERRABLE INITIALLY DEFERRED
					FOR EACH ROW EXECUTE FUNCTION audit_constraint_trigger_insert();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `INSERT INTO constraint_trigger_target VALUES (1, 10);`,
				},
				{
					Query:    `SELECT count(*) FROM constraint_trigger_audit;`,
					Expected: []sql.Row{{int64(0)}},
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query:    `SELECT id, v FROM constraint_trigger_audit;`,
					Expected: []sql.Row{{1, 10}},
				},
			},
		},
	})
}

// TestUpdateTriggerWhenOldNewDistinctRepro guards that trigger WHEN
// predicates may compare OLD and NEW values to suppress unchanged-row audits.
func TestUpdateTriggerWhenOldNewDistinctRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "UPDATE trigger WHEN compares OLD and NEW",
			SetUpScript: []string{
				`CREATE TABLE trigger_when_update_target (
					id INT PRIMARY KEY,
					v INT NOT NULL,
					note TEXT NOT NULL
				);`,
				`CREATE TABLE trigger_when_update_audit (
					id INT PRIMARY KEY,
					old_v INT NOT NULL,
					new_v INT NOT NULL
				);`,
				`INSERT INTO trigger_when_update_target VALUES (1, 10, 'original');`,
				`CREATE FUNCTION log_trigger_when_update() RETURNS TRIGGER AS $$
				BEGIN
					INSERT INTO trigger_when_update_audit VALUES (NEW.id, OLD.v, NEW.v);
					RETURN NULL;
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER trigger_when_update_changed
					AFTER UPDATE ON trigger_when_update_target
					FOR EACH ROW
					WHEN (OLD.v IS DISTINCT FROM NEW.v)
					EXECUTE FUNCTION log_trigger_when_update();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE trigger_when_update_target SET note = 'same-value-change' WHERE id = 1;`,
				},
				{
					Query:    `SELECT count(*) FROM trigger_when_update_audit;`,
					Expected: []sql.Row{{int64(0)}},
				},
				{
					Query: `UPDATE trigger_when_update_target SET v = 11 WHERE id = 1;`,
				},
				{
					Query:    `SELECT id, old_v, new_v FROM trigger_when_update_audit;`,
					Expected: []sql.Row{{1, 10, 11}},
				},
			},
		},
	})
}

// TestUpdateTriggerWhenWholeRowDistinctRepro guards whole-row trigger WHEN
// predicates used by generic audit triggers.
func TestUpdateTriggerWhenWholeRowDistinctRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "UPDATE trigger WHEN compares whole OLD and NEW rows",
			SetUpScript: []string{
				`CREATE TABLE trigger_when_whole_row_target (
					id INT PRIMARY KEY,
					v INT NOT NULL,
					note TEXT NOT NULL
				);`,
				`CREATE TABLE trigger_when_whole_row_audit (
					id INT PRIMARY KEY,
					old_note TEXT NOT NULL,
					new_note TEXT NOT NULL
				);`,
				`INSERT INTO trigger_when_whole_row_target VALUES (1, 10, 'original');`,
				`CREATE FUNCTION log_trigger_when_whole_row() RETURNS TRIGGER AS $$
				BEGIN
					INSERT INTO trigger_when_whole_row_audit VALUES (NEW.id, OLD.note, NEW.note);
					RETURN NULL;
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER trigger_when_whole_row_changed
					AFTER UPDATE ON trigger_when_whole_row_target
					FOR EACH ROW
					WHEN (OLD.* IS DISTINCT FROM NEW.*)
					EXECUTE FUNCTION log_trigger_when_whole_row();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE trigger_when_whole_row_target SET note = note WHERE id = 1;`,
				},
				{
					Query:    `SELECT count(*) FROM trigger_when_whole_row_audit;`,
					Expected: []sql.Row{{int64(0)}},
				},
				{
					Query: `UPDATE trigger_when_whole_row_target SET note = 'changed' WHERE id = 1;`,
				},
				{
					Query:    `SELECT id, old_note, new_note FROM trigger_when_whole_row_audit;`,
					Expected: []sql.Row{{1, "original", "changed"}},
				},
			},
		},
	})
}

// TestAfterInsertTriggerErrorRollsBackStatementRepro guards PostgreSQL trigger
// atomicity: if an AFTER trigger raises an exception, the base-row write and
// trigger side effects from the failed statement are both rolled back.
func TestAfterInsertTriggerErrorRollsBackStatementRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "AFTER INSERT trigger error rolls back statement",
			SetUpScript: []string{
				`CREATE TABLE trigger_error_target (
					id INT PRIMARY KEY,
					label TEXT NOT NULL
				);`,
				`CREATE TABLE trigger_error_audit (
					id INT PRIMARY KEY,
					label TEXT NOT NULL
				);`,
				`CREATE FUNCTION reject_trigger_error_insert() RETURNS TRIGGER AS $$
				BEGIN
					INSERT INTO trigger_error_audit VALUES (NEW.id, NEW.label);
					RAISE EXCEPTION 'reject trigger insert';
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER trigger_error_after_insert
					AFTER INSERT ON trigger_error_target
					FOR EACH ROW EXECUTE FUNCTION reject_trigger_error_insert();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `INSERT INTO trigger_error_target VALUES (1, 'bad');`,
					ExpectedErr: `reject trigger insert`,
				},
				{
					Query:    `SELECT count(*) FROM trigger_error_target;`,
					Expected: []sql.Row{{int64(0)}},
				},
				{
					Query:    `SELECT count(*) FROM trigger_error_audit;`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
	})
}

// TestAfterUpdateTriggerErrorRollsBackStatementRepro reproduces a data
// consistency bug: if an AFTER UPDATE trigger raises an exception, the base-row
// update and trigger side effects from the failed statement are both rolled
// back.
func TestAfterUpdateTriggerErrorRollsBackStatementRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "AFTER UPDATE trigger error rolls back statement",
			SetUpScript: []string{
				`CREATE TABLE after_update_trigger_error_target (
					id INT PRIMARY KEY,
					label TEXT NOT NULL
				);`,
				`INSERT INTO after_update_trigger_error_target VALUES (1, 'old');`,
				`CREATE TABLE after_update_trigger_error_audit (
					id INT PRIMARY KEY,
					label TEXT NOT NULL
				);`,
				`CREATE FUNCTION reject_after_update_trigger_error() RETURNS TRIGGER AS $$
				BEGIN
					INSERT INTO after_update_trigger_error_audit VALUES (NEW.id, NEW.label);
					RAISE EXCEPTION 'reject after update';
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER after_update_trigger_error_after_update
					AFTER UPDATE ON after_update_trigger_error_target
					FOR EACH ROW EXECUTE FUNCTION reject_after_update_trigger_error();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `UPDATE after_update_trigger_error_target SET label = 'new' WHERE id = 1;`,
					ExpectedErr: `reject after update`,
				},
				{
					Query:    `SELECT label FROM after_update_trigger_error_target WHERE id = 1;`,
					Expected: []sql.Row{{"old"}},
				},
				{
					Query:    `SELECT count(*) FROM after_update_trigger_error_audit;`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
	})
}

// TestAfterDeleteTriggerErrorRollsBackStatementRepro reproduces a data
// consistency bug: if an AFTER DELETE trigger raises an exception, the base-row
// delete and trigger side effects from the failed statement are both rolled
// back.
func TestAfterDeleteTriggerErrorRollsBackStatementRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "AFTER DELETE trigger error rolls back statement",
			SetUpScript: []string{
				`CREATE TABLE after_delete_trigger_error_target (
					id INT PRIMARY KEY,
					label TEXT NOT NULL
				);`,
				`INSERT INTO after_delete_trigger_error_target VALUES (1, 'old');`,
				`CREATE TABLE after_delete_trigger_error_audit (
					id INT PRIMARY KEY,
					label TEXT NOT NULL
				);`,
				`CREATE FUNCTION reject_after_delete_trigger_error() RETURNS TRIGGER AS $$
				BEGIN
					INSERT INTO after_delete_trigger_error_audit VALUES (OLD.id, OLD.label);
					RAISE EXCEPTION 'reject after delete';
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER after_delete_trigger_error_after_delete
					AFTER DELETE ON after_delete_trigger_error_target
					FOR EACH ROW EXECUTE FUNCTION reject_after_delete_trigger_error();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DELETE FROM after_delete_trigger_error_target WHERE id = 1;`,
					ExpectedErr: `reject after delete`,
				},
				{
					Query:    `SELECT id, label FROM after_delete_trigger_error_target;`,
					Expected: []sql.Row{{1, "old"}},
				},
				{
					Query:    `SELECT count(*) FROM after_delete_trigger_error_audit;`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
	})
}

// TestBeforeInsertTriggerErrorRollsBackSideEffectsRepro reproduces a data
// consistency bug: if a BEFORE trigger writes side effects and then raises an
// exception, those side effects are rolled back with the failed statement.
func TestBeforeInsertTriggerErrorRollsBackSideEffectsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "BEFORE INSERT trigger error rolls back side effects",
			SetUpScript: []string{
				`CREATE TABLE before_trigger_error_target (
					id INT PRIMARY KEY,
					label TEXT NOT NULL
				);`,
				`CREATE TABLE before_trigger_error_audit (
					id INT PRIMARY KEY,
					label TEXT NOT NULL
				);`,
				`CREATE FUNCTION reject_before_trigger_error_insert() RETURNS TRIGGER AS $$
				BEGIN
					INSERT INTO before_trigger_error_audit VALUES (NEW.id, NEW.label);
					RAISE EXCEPTION 'reject before trigger insert';
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER before_trigger_error_before_insert
					BEFORE INSERT ON before_trigger_error_target
					FOR EACH ROW EXECUTE FUNCTION reject_before_trigger_error_insert();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `INSERT INTO before_trigger_error_target VALUES (1, 'bad');`,
					ExpectedErr: `reject before trigger insert`,
				},
				{
					Query:    `SELECT count(*) FROM before_trigger_error_target;`,
					Expected: []sql.Row{{int64(0)}},
				},
				{
					Query:    `SELECT count(*) FROM before_trigger_error_audit;`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
	})
}

// TestBeforeUpdateTriggerErrorRollsBackSideEffectsRepro reproduces a data
// consistency bug: if a BEFORE UPDATE trigger writes side effects and then
// raises an exception, those side effects are rolled back with the failed
// statement.
func TestBeforeUpdateTriggerErrorRollsBackSideEffectsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "BEFORE UPDATE trigger error rolls back side effects",
			SetUpScript: []string{
				`CREATE TABLE before_update_trigger_error_target (
					id INT PRIMARY KEY,
					label TEXT NOT NULL
				);`,
				`INSERT INTO before_update_trigger_error_target VALUES (1, 'old');`,
				`CREATE TABLE before_update_trigger_error_audit (
					id INT PRIMARY KEY,
					label TEXT NOT NULL
				);`,
				`CREATE FUNCTION reject_before_update_trigger_error() RETURNS TRIGGER AS $$
				BEGIN
					INSERT INTO before_update_trigger_error_audit VALUES (NEW.id, NEW.label);
					RAISE EXCEPTION 'reject before update';
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER before_update_trigger_error_before_update
					BEFORE UPDATE ON before_update_trigger_error_target
					FOR EACH ROW EXECUTE FUNCTION reject_before_update_trigger_error();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `UPDATE before_update_trigger_error_target SET label = 'new' WHERE id = 1;`,
					ExpectedErr: `reject before update`,
				},
				{
					Query:    `SELECT label FROM before_update_trigger_error_target WHERE id = 1;`,
					Expected: []sql.Row{{"old"}},
				},
				{
					Query:    `SELECT count(*) FROM before_update_trigger_error_audit;`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
	})
}

// TestBeforeDeleteTriggerErrorRollsBackSideEffectsRepro reproduces a data
// consistency bug: if a BEFORE DELETE trigger writes side effects and then
// raises an exception, those side effects are rolled back with the failed
// statement.
func TestBeforeDeleteTriggerErrorRollsBackSideEffectsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "BEFORE DELETE trigger error rolls back side effects",
			SetUpScript: []string{
				`CREATE TABLE before_delete_trigger_error_target (
					id INT PRIMARY KEY,
					label TEXT NOT NULL
				);`,
				`INSERT INTO before_delete_trigger_error_target VALUES (1, 'old');`,
				`CREATE TABLE before_delete_trigger_error_audit (
					id INT PRIMARY KEY,
					label TEXT NOT NULL
				);`,
				`CREATE FUNCTION reject_before_delete_trigger_error() RETURNS TRIGGER AS $$
				BEGIN
					INSERT INTO before_delete_trigger_error_audit VALUES (OLD.id, OLD.label);
					RAISE EXCEPTION 'reject before delete';
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER before_delete_trigger_error_before_delete
					BEFORE DELETE ON before_delete_trigger_error_target
					FOR EACH ROW EXECUTE FUNCTION reject_before_delete_trigger_error();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DELETE FROM before_delete_trigger_error_target WHERE id = 1;`,
					ExpectedErr: `reject before delete`,
				},
				{
					Query:    `SELECT id, label FROM before_delete_trigger_error_target;`,
					Expected: []sql.Row{{1, "old"}},
				},
				{
					Query:    `SELECT count(*) FROM before_delete_trigger_error_audit;`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
	})
}

// TestBeforeInsertTriggerSideEffectsRollBackOnConstraintErrorRepro reproduces a
// data consistency bug: side effects written by a BEFORE trigger roll back if
// the target row later fails a table constraint.
func TestBeforeInsertTriggerSideEffectsRollBackOnConstraintErrorRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "BEFORE INSERT trigger side effects roll back on constraint error",
			SetUpScript: []string{
				`CREATE TABLE before_trigger_check_target (
					id INT PRIMARY KEY,
					qty INT NOT NULL CHECK (qty > 0)
				);`,
				`CREATE TABLE before_trigger_check_audit (
					id INT PRIMARY KEY,
					qty INT NOT NULL
				);`,
				`CREATE FUNCTION audit_before_trigger_check_insert() RETURNS TRIGGER AS $$
				BEGIN
					INSERT INTO before_trigger_check_audit VALUES (NEW.id, NEW.qty);
					RETURN NEW;
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER before_trigger_check_before_insert
					BEFORE INSERT ON before_trigger_check_target
					FOR EACH ROW EXECUTE FUNCTION audit_before_trigger_check_insert();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `INSERT INTO before_trigger_check_target VALUES (1, -1);`,
					ExpectedErr: `Check constraint`,
				},
				{
					Query:    `SELECT count(*) FROM before_trigger_check_target;`,
					Expected: []sql.Row{{int64(0)}},
				},
				{
					Query:    `SELECT count(*) FROM before_trigger_check_audit;`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
	})
}

// TestBeforeUpdateTriggerSideEffectsRollBackOnConstraintErrorRepro reproduces a
// data consistency bug: side effects written by a BEFORE UPDATE trigger roll
// back if the updated row later fails a table constraint.
func TestBeforeUpdateTriggerSideEffectsRollBackOnConstraintErrorRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "BEFORE UPDATE trigger side effects roll back on constraint error",
			SetUpScript: []string{
				`CREATE TABLE before_update_trigger_check_target (
					id INT PRIMARY KEY,
					qty INT NOT NULL CHECK (qty > 0)
				);`,
				`INSERT INTO before_update_trigger_check_target VALUES (1, 5);`,
				`CREATE TABLE before_update_trigger_check_audit (
					id INT PRIMARY KEY,
					qty INT NOT NULL
				);`,
				`CREATE FUNCTION audit_before_update_trigger_check() RETURNS TRIGGER AS $$
				BEGIN
					INSERT INTO before_update_trigger_check_audit VALUES (NEW.id, NEW.qty);
					RETURN NEW;
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER before_update_trigger_check_before_update
					BEFORE UPDATE ON before_update_trigger_check_target
					FOR EACH ROW EXECUTE FUNCTION audit_before_update_trigger_check();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `UPDATE before_update_trigger_check_target SET qty = -1 WHERE id = 1;`,
					ExpectedErr: `Check constraint`,
				},
				{
					Query:    `SELECT qty FROM before_update_trigger_check_target WHERE id = 1;`,
					Expected: []sql.Row{{5}},
				},
				{
					Query:    `SELECT count(*) FROM before_update_trigger_check_audit;`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
	})
}

// TestStatementTriggerSideEffectsRollBackOnConstraintErrorRepro reproduces a
// data consistency bug: side effects written by a statement-level trigger roll
// back if the triggering statement later fails a table constraint.
func TestStatementTriggerSideEffectsRollBackOnConstraintErrorRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "statement trigger side effects roll back on constraint error",
			SetUpScript: []string{
				`CREATE TABLE statement_trigger_check_target (
					id INT PRIMARY KEY,
					qty INT NOT NULL CHECK (qty > 0)
				);`,
				`CREATE TABLE statement_trigger_check_audit (
					label TEXT PRIMARY KEY
				);`,
				`CREATE FUNCTION audit_statement_trigger_check_insert() RETURNS TRIGGER AS $$
				BEGIN
					INSERT INTO statement_trigger_check_audit VALUES ('before statement');
					RETURN NULL;
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER statement_trigger_check_before_insert
					BEFORE INSERT ON statement_trigger_check_target
					FOR EACH STATEMENT EXECUTE FUNCTION audit_statement_trigger_check_insert();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `INSERT INTO statement_trigger_check_target VALUES (1, -1);`,
					ExpectedErr: `Check constraint`,
				},
				{
					Query:    `SELECT count(*) FROM statement_trigger_check_target;`,
					Expected: []sql.Row{{int64(0)}},
				},
				{
					Query:    `SELECT count(*) FROM statement_trigger_check_audit;`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
	})
}

// TestStatementUpdateTriggerSideEffectsRollBackOnConstraintErrorRepro
// reproduces a data consistency bug: side effects written by a statement-level
// UPDATE trigger roll back if the triggering statement later fails a table
// constraint.
func TestStatementUpdateTriggerSideEffectsRollBackOnConstraintErrorRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "statement UPDATE trigger side effects roll back on constraint error",
			SetUpScript: []string{
				`CREATE TABLE statement_update_trigger_check_target (
					id INT PRIMARY KEY,
					qty INT NOT NULL CHECK (qty > 0)
				);`,
				`INSERT INTO statement_update_trigger_check_target VALUES (1, 5);`,
				`CREATE TABLE statement_update_trigger_check_audit (
					label TEXT PRIMARY KEY
				);`,
				`CREATE FUNCTION audit_statement_update_trigger_check() RETURNS TRIGGER AS $$
				BEGIN
					INSERT INTO statement_update_trigger_check_audit VALUES ('before update statement');
					RETURN NULL;
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER statement_update_trigger_check_before_update
					BEFORE UPDATE ON statement_update_trigger_check_target
					FOR EACH STATEMENT EXECUTE FUNCTION audit_statement_update_trigger_check();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `UPDATE statement_update_trigger_check_target SET qty = -1 WHERE id = 1;`,
					ExpectedErr: `Check constraint`,
				},
				{
					Query:    `SELECT qty FROM statement_update_trigger_check_target WHERE id = 1;`,
					Expected: []sql.Row{{5}},
				},
				{
					Query:    `SELECT count(*) FROM statement_update_trigger_check_audit;`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
	})
}

// TestStatementDeleteTriggerSideEffectsRollBackOnForeignKeyErrorRepro
// reproduces a data consistency bug: side effects written by a statement-level
// DELETE trigger roll back if the triggering statement later fails a foreign-key
// constraint.
func TestStatementDeleteTriggerSideEffectsRollBackOnForeignKeyErrorRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "statement DELETE trigger side effects roll back on foreign-key error",
			SetUpScript: []string{
				`CREATE TABLE statement_delete_trigger_parent (
					id INT PRIMARY KEY
				);`,
				`CREATE TABLE statement_delete_trigger_child (
					id INT PRIMARY KEY,
					parent_id INT REFERENCES statement_delete_trigger_parent(id)
				);`,
				`INSERT INTO statement_delete_trigger_parent VALUES (1);`,
				`INSERT INTO statement_delete_trigger_child VALUES (10, 1);`,
				`CREATE TABLE statement_delete_trigger_audit (
					label TEXT PRIMARY KEY
				);`,
				`CREATE FUNCTION audit_statement_delete_trigger() RETURNS TRIGGER AS $$
				BEGIN
					INSERT INTO statement_delete_trigger_audit VALUES ('before delete statement');
					RETURN NULL;
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER statement_delete_trigger_before_delete
					BEFORE DELETE ON statement_delete_trigger_parent
					FOR EACH STATEMENT EXECUTE FUNCTION audit_statement_delete_trigger();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DELETE FROM statement_delete_trigger_parent WHERE id = 1;`,
					ExpectedErr: `Foreign key violation`,
				},
				{
					Query:    `SELECT count(*) FROM statement_delete_trigger_parent;`,
					Expected: []sql.Row{{int64(1)}},
				},
				{
					Query:    `SELECT count(*) FROM statement_delete_trigger_audit;`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
	})
}

// TestStatementInsertTriggerErrorRollsBackSideEffectsRepro reproduces a data
// consistency bug: side effects written by a statement-level BEFORE INSERT
// trigger roll back if that trigger later raises an exception.
func TestStatementInsertTriggerErrorRollsBackSideEffectsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "statement INSERT trigger exception rolls back trigger side effects",
			SetUpScript: []string{
				`CREATE TABLE stmt_insert_exception_target (
					id INT PRIMARY KEY,
					label TEXT NOT NULL
				);`,
				`CREATE TABLE stmt_insert_exception_audit (
					label TEXT PRIMARY KEY
				);`,
				`CREATE FUNCTION audit_stmt_insert_exception() RETURNS TRIGGER AS $$
				BEGIN
					INSERT INTO stmt_insert_exception_audit VALUES ('before insert statement');
					RAISE EXCEPTION 'reject statement insert';
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER stmt_insert_exception_before_insert
					BEFORE INSERT ON stmt_insert_exception_target
					FOR EACH STATEMENT EXECUTE FUNCTION audit_stmt_insert_exception();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `INSERT INTO stmt_insert_exception_target VALUES (1, 'new');`,
					ExpectedErr: `reject statement insert`,
				},
				{
					Query:    `SELECT count(*) FROM stmt_insert_exception_target;`,
					Expected: []sql.Row{{int64(0)}},
				},
				{
					Query:    `SELECT count(*) FROM stmt_insert_exception_audit;`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
	})
}

// TestStatementUpdateTriggerErrorRollsBackSideEffectsRepro reproduces a data
// consistency bug: side effects written by a statement-level BEFORE UPDATE
// trigger roll back if that trigger later raises an exception.
func TestStatementUpdateTriggerErrorRollsBackSideEffectsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "statement UPDATE trigger exception rolls back trigger side effects",
			SetUpScript: []string{
				`CREATE TABLE stmt_update_exception_target (
					id INT PRIMARY KEY,
					label TEXT NOT NULL
				);`,
				`INSERT INTO stmt_update_exception_target VALUES (1, 'old');`,
				`CREATE TABLE stmt_update_exception_audit (
					label TEXT PRIMARY KEY
				);`,
				`CREATE FUNCTION audit_stmt_update_exception() RETURNS TRIGGER AS $$
				BEGIN
					INSERT INTO stmt_update_exception_audit VALUES ('before update statement');
					RAISE EXCEPTION 'reject statement update';
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER stmt_update_exception_before_update
					BEFORE UPDATE ON stmt_update_exception_target
					FOR EACH STATEMENT EXECUTE FUNCTION audit_stmt_update_exception();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `UPDATE stmt_update_exception_target SET label = 'new' WHERE id = 1;`,
					ExpectedErr: `reject statement update`,
				},
				{
					Query:    `SELECT label FROM stmt_update_exception_target WHERE id = 1;`,
					Expected: []sql.Row{{"old"}},
				},
				{
					Query:    `SELECT count(*) FROM stmt_update_exception_audit;`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
	})
}

// TestStatementDeleteTriggerErrorRollsBackSideEffectsRepro reproduces a data
// consistency bug: side effects written by a statement-level BEFORE DELETE
// trigger roll back if that trigger later raises an exception.
func TestStatementDeleteTriggerErrorRollsBackSideEffectsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "statement DELETE trigger exception rolls back trigger side effects",
			SetUpScript: []string{
				`CREATE TABLE stmt_delete_exception_target (
					id INT PRIMARY KEY,
					label TEXT NOT NULL
				);`,
				`INSERT INTO stmt_delete_exception_target VALUES (1, 'old');`,
				`CREATE TABLE stmt_delete_exception_audit (
					label TEXT PRIMARY KEY
				);`,
				`CREATE FUNCTION audit_stmt_delete_exception() RETURNS TRIGGER AS $$
				BEGIN
					INSERT INTO stmt_delete_exception_audit VALUES ('before delete statement');
					RAISE EXCEPTION 'reject statement delete';
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER stmt_delete_exception_before_delete
					BEFORE DELETE ON stmt_delete_exception_target
					FOR EACH STATEMENT EXECUTE FUNCTION audit_stmt_delete_exception();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DELETE FROM stmt_delete_exception_target WHERE id = 1;`,
					ExpectedErr: `reject statement delete`,
				},
				{
					Query:    `SELECT count(*) FROM stmt_delete_exception_target;`,
					Expected: []sql.Row{{int64(1)}},
				},
				{
					Query:    `SELECT count(*) FROM stmt_delete_exception_audit;`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
	})
}

// TestAfterStatementInsertTriggerErrorRollsBackStatementRepro reproduces a data
// consistency bug: statement-level AFTER INSERT trigger exceptions roll back
// both the base INSERT and trigger side effects.
func TestAfterStatementInsertTriggerErrorRollsBackStatementRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "AFTER INSERT statement trigger exception rolls back statement",
			SetUpScript: []string{
				`CREATE TABLE after_stmt_insert_exception_target (
					id INT PRIMARY KEY,
					label TEXT NOT NULL
				);`,
				`CREATE TABLE after_stmt_insert_exception_audit (
					label TEXT PRIMARY KEY
				);`,
				`CREATE FUNCTION audit_after_stmt_insert_exception() RETURNS TRIGGER AS $$
				BEGIN
					INSERT INTO after_stmt_insert_exception_audit VALUES ('after insert statement');
					RAISE EXCEPTION 'reject after statement insert';
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER after_stmt_insert_exception_after_insert
					AFTER INSERT ON after_stmt_insert_exception_target
					FOR EACH STATEMENT EXECUTE FUNCTION audit_after_stmt_insert_exception();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `INSERT INTO after_stmt_insert_exception_target VALUES (1, 'new');`,
					ExpectedErr: `reject after statement insert`,
				},
				{
					Query:    `SELECT count(*) FROM after_stmt_insert_exception_target;`,
					Expected: []sql.Row{{int64(0)}},
				},
				{
					Query:    `SELECT count(*) FROM after_stmt_insert_exception_audit;`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
	})
}

// TestAfterStatementUpdateTriggerErrorRollsBackStatementRepro reproduces a data
// consistency bug: statement-level AFTER UPDATE trigger exceptions roll back
// both the base UPDATE and trigger side effects.
func TestAfterStatementUpdateTriggerErrorRollsBackStatementRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "AFTER UPDATE statement trigger exception rolls back statement",
			SetUpScript: []string{
				`CREATE TABLE after_stmt_update_exception_target (
					id INT PRIMARY KEY,
					label TEXT NOT NULL
				);`,
				`INSERT INTO after_stmt_update_exception_target VALUES (1, 'old');`,
				`CREATE TABLE after_stmt_update_exception_audit (
					label TEXT PRIMARY KEY
				);`,
				`CREATE FUNCTION audit_after_stmt_update_exception() RETURNS TRIGGER AS $$
				BEGIN
					INSERT INTO after_stmt_update_exception_audit VALUES ('after update statement');
					RAISE EXCEPTION 'reject after statement update';
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER after_stmt_update_exception_after_update
					AFTER UPDATE ON after_stmt_update_exception_target
					FOR EACH STATEMENT EXECUTE FUNCTION audit_after_stmt_update_exception();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `UPDATE after_stmt_update_exception_target SET label = 'new' WHERE id = 1;`,
					ExpectedErr: `reject after statement update`,
				},
				{
					Query:    `SELECT label FROM after_stmt_update_exception_target WHERE id = 1;`,
					Expected: []sql.Row{{"old"}},
				},
				{
					Query:    `SELECT count(*) FROM after_stmt_update_exception_audit;`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
	})
}

// TestAfterStatementDeleteTriggerErrorRollsBackStatementRepro reproduces a data
// consistency bug: statement-level AFTER DELETE trigger exceptions roll back
// both the base DELETE and trigger side effects.
func TestAfterStatementDeleteTriggerErrorRollsBackStatementRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "AFTER DELETE statement trigger exception rolls back statement",
			SetUpScript: []string{
				`CREATE TABLE after_stmt_delete_exception_target (
					id INT PRIMARY KEY,
					label TEXT NOT NULL
				);`,
				`INSERT INTO after_stmt_delete_exception_target VALUES (1, 'old');`,
				`CREATE TABLE after_stmt_delete_exception_audit (
					label TEXT PRIMARY KEY
				);`,
				`CREATE FUNCTION audit_after_stmt_delete_exception() RETURNS TRIGGER AS $$
				BEGIN
					INSERT INTO after_stmt_delete_exception_audit VALUES ('after delete statement');
					RAISE EXCEPTION 'reject after statement delete';
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER after_stmt_delete_exception_after_delete
					AFTER DELETE ON after_stmt_delete_exception_target
					FOR EACH STATEMENT EXECUTE FUNCTION audit_after_stmt_delete_exception();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DELETE FROM after_stmt_delete_exception_target WHERE id = 1;`,
					ExpectedErr: `reject after statement delete`,
				},
				{
					Query:    `SELECT count(*) FROM after_stmt_delete_exception_target;`,
					Expected: []sql.Row{{int64(1)}},
				},
				{
					Query:    `SELECT count(*) FROM after_stmt_delete_exception_audit;`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
	})
}

// TestBeforeTruncateTriggerErrorRollsBackSideEffectsRepro reproduces a data
// consistency bug: side effects written by a BEFORE TRUNCATE trigger roll back
// if that trigger later raises an exception.
func TestBeforeTruncateTriggerErrorRollsBackSideEffectsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "BEFORE TRUNCATE trigger exception rolls back trigger side effects",
			SetUpScript: []string{
				`CREATE TABLE before_truncate_exception_target (
					id INT PRIMARY KEY,
					label TEXT NOT NULL
				);`,
				`INSERT INTO before_truncate_exception_target VALUES (1, 'kept');`,
				`CREATE TABLE before_truncate_exception_audit (
					label TEXT PRIMARY KEY
				);`,
				`CREATE FUNCTION audit_before_truncate_exception() RETURNS TRIGGER AS $$
				BEGIN
					INSERT INTO before_truncate_exception_audit VALUES ('before truncate');
					RAISE EXCEPTION 'reject before truncate';
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER before_truncate_exception_trigger
					BEFORE TRUNCATE ON before_truncate_exception_target
					FOR EACH STATEMENT EXECUTE FUNCTION audit_before_truncate_exception();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `TRUNCATE before_truncate_exception_target;`,
					ExpectedErr: `reject before truncate`,
				},
				{
					Query:    `SELECT count(*) FROM before_truncate_exception_target;`,
					Expected: []sql.Row{{int64(1)}},
				},
				{
					Query:    `SELECT count(*) FROM before_truncate_exception_audit;`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
	})
}

// TestAfterTruncateTriggerErrorRollsBackStatementRepro reproduces a data
// consistency bug: AFTER TRUNCATE trigger exceptions roll back both the
// truncate and trigger side effects.
func TestAfterTruncateTriggerErrorRollsBackStatementRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "AFTER TRUNCATE trigger exception rolls back statement",
			SetUpScript: []string{
				`CREATE TABLE after_truncate_exception_target (
					id INT PRIMARY KEY,
					label TEXT NOT NULL
				);`,
				`INSERT INTO after_truncate_exception_target VALUES (1, 'kept');`,
				`CREATE TABLE after_truncate_exception_audit (
					label TEXT PRIMARY KEY
				);`,
				`CREATE FUNCTION audit_after_truncate_exception() RETURNS TRIGGER AS $$
				BEGIN
					INSERT INTO after_truncate_exception_audit VALUES ('after truncate');
					RAISE EXCEPTION 'reject after truncate';
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER after_truncate_exception_trigger
					AFTER TRUNCATE ON after_truncate_exception_target
					FOR EACH STATEMENT EXECUTE FUNCTION audit_after_truncate_exception();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `TRUNCATE after_truncate_exception_target;`,
					ExpectedErr: `reject after truncate`,
				},
				{
					Query:    `SELECT count(*) FROM after_truncate_exception_target;`,
					Expected: []sql.Row{{int64(1)}},
				},
				{
					Query:    `SELECT count(*) FROM after_truncate_exception_audit;`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
	})
}

// TestBeforeDeleteRowTriggerSideEffectsRollBackOnForeignKeyErrorRepro
// reproduces a data consistency bug: side effects written by a row-level BEFORE
// DELETE trigger roll back if the delete later fails a foreign-key constraint.
func TestBeforeDeleteRowTriggerSideEffectsRollBackOnForeignKeyErrorRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "BEFORE DELETE row trigger side effects roll back on foreign-key error",
			SetUpScript: []string{
				`CREATE TABLE before_delete_row_trigger_parent (
					id INT PRIMARY KEY
				);`,
				`CREATE TABLE before_delete_row_trigger_child (
					id INT PRIMARY KEY,
					parent_id INT REFERENCES before_delete_row_trigger_parent(id)
				);`,
				`INSERT INTO before_delete_row_trigger_parent VALUES (1);`,
				`INSERT INTO before_delete_row_trigger_child VALUES (10, 1);`,
				`CREATE TABLE before_delete_row_trigger_audit (
					id INT PRIMARY KEY
				);`,
				`CREATE FUNCTION audit_before_delete_row_trigger() RETURNS TRIGGER AS $$
				BEGIN
					INSERT INTO before_delete_row_trigger_audit VALUES (OLD.id);
					RETURN OLD;
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER before_delete_row_trigger_before_delete
					BEFORE DELETE ON before_delete_row_trigger_parent
					FOR EACH ROW EXECUTE FUNCTION audit_before_delete_row_trigger();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DELETE FROM before_delete_row_trigger_parent WHERE id = 1;`,
					ExpectedErr: `Foreign key violation`,
				},
				{
					Query:    `SELECT count(*) FROM before_delete_row_trigger_parent;`,
					Expected: []sql.Row{{int64(1)}},
				},
				{
					Query:    `SELECT count(*) FROM before_delete_row_trigger_audit;`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
	})
}

// TestBeforeUpdateRowTriggerSideEffectsRollBackOnForeignKeyErrorRepro
// reproduces a data consistency bug: side effects written by a row-level BEFORE
// UPDATE trigger roll back if the update later fails a foreign-key constraint.
func TestBeforeUpdateRowTriggerSideEffectsRollBackOnForeignKeyErrorRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "BEFORE UPDATE row trigger side effects roll back on foreign-key error",
			SetUpScript: []string{
				`CREATE TABLE before_update_row_trigger_parent (
					id INT PRIMARY KEY
				);`,
				`CREATE TABLE before_update_row_trigger_child (
					id INT PRIMARY KEY,
					parent_id INT REFERENCES before_update_row_trigger_parent(id)
				);`,
				`INSERT INTO before_update_row_trigger_parent VALUES (1);`,
				`INSERT INTO before_update_row_trigger_child VALUES (10, 1);`,
				`CREATE TABLE before_update_row_trigger_audit (
					old_id INT PRIMARY KEY,
					new_id INT NOT NULL
				);`,
				`CREATE FUNCTION audit_before_update_row_trigger() RETURNS TRIGGER AS $$
				BEGIN
					INSERT INTO before_update_row_trigger_audit VALUES (OLD.id, NEW.id);
					RETURN NEW;
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER before_update_row_trigger_before_update
					BEFORE UPDATE ON before_update_row_trigger_parent
					FOR EACH ROW EXECUTE FUNCTION audit_before_update_row_trigger();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `UPDATE before_update_row_trigger_parent SET id = 2 WHERE id = 1;`,
					ExpectedErr: `Foreign key violation`,
				},
				{
					Query:    `SELECT id FROM before_update_row_trigger_parent;`,
					Expected: []sql.Row{{1}},
				},
				{
					Query:    `SELECT count(*) FROM before_update_row_trigger_audit;`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
	})
}

// TestEventTriggerAuditsDdlCommandRepro reproduces a DDL audit correctness bug:
// PostgreSQL event triggers can run at ddl_command_end and inspect the DDL tag.
func TestEventTriggerAuditsDdlCommandRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "event trigger audits CREATE TABLE",
			SetUpScript: []string{
				`CREATE TABLE event_trigger_audit (
					tag TEXT NOT NULL
				);`,
				`CREATE FUNCTION audit_ddl_command() RETURNS event_trigger AS $$
				BEGIN
					INSERT INTO event_trigger_audit VALUES (TG_TAG);
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE EVENT TRIGGER audit_create_table
					ON ddl_command_end
					WHEN TAG IN ('CREATE TABLE')
					EXECUTE FUNCTION audit_ddl_command();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE event_trigger_created_table (id INT PRIMARY KEY);`,
				},
				{
					Query:    `SELECT tag FROM event_trigger_audit;`,
					Expected: []sql.Row{{"CREATE TABLE"}},
				},
			},
		},
	})
}
