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

// TestMultiRowInsertDuplicateKeyIsStatementAtomicRepro guards PostgreSQL
// statement atomicity: if a multi-row INSERT hits a duplicate key, none of
// that statement's rows should persist.
func TestMultiRowInsertDuplicateKeyIsStatementAtomicRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "multi-row insert duplicate key is statement atomic",
			SetUpScript: []string{
				`CREATE TABLE atomic_insert_items (
					id INT PRIMARY KEY,
					v TEXT
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO atomic_insert_items VALUES
						(1, 'first'),
						(1, 'duplicate');`,
					ExpectedErr: `duplicate`,
				},
				{
					Query:    `SELECT count(*) FROM atomic_insert_items;`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
	})
}

// TestMultiRowInsertCheckConstraintIsStatementAtomicRepro guards PostgreSQL
// statement atomicity: if a later VALUES row violates a CHECK constraint, no
// earlier rows from that INSERT statement should persist.
func TestMultiRowInsertCheckConstraintIsStatementAtomicRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "multi-row insert check constraint is statement atomic",
			SetUpScript: []string{
				`CREATE TABLE atomic_insert_check_items (
					id INT PRIMARY KEY,
					qty INT CHECK (qty > 0)
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO atomic_insert_check_items VALUES
						(1, 1),
						(2, -1);`,
					ExpectedErr: `Check constraint`,
				},
				{
					Query:    `SELECT count(*) FROM atomic_insert_check_items;`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
	})
}

// TestInsertSelectDuplicateKeyIsStatementAtomicRepro guards PostgreSQL
// statement atomicity for INSERT ... SELECT: if a later selected row violates a
// key, earlier selected rows from the same statement should not persist.
func TestInsertSelectDuplicateKeyIsStatementAtomicRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "insert select duplicate key is statement atomic",
			SetUpScript: []string{
				`CREATE TABLE atomic_insert_select_target (
					id INT PRIMARY KEY,
					note TEXT
				);`,
				`CREATE TABLE atomic_insert_select_source (
					id INT,
					note TEXT
				);`,
				`INSERT INTO atomic_insert_select_source VALUES
					(1, 'first'),
					(1, 'duplicate');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO atomic_insert_select_target
						SELECT id, note FROM atomic_insert_select_source ORDER BY note;`,
					ExpectedErr: `duplicate`,
				},
				{
					Query:    `SELECT count(*) FROM atomic_insert_select_target;`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
	})
}

// TestInsertSelectCheckConstraintIsStatementAtomicRepro guards PostgreSQL
// statement atomicity for INSERT ... SELECT: if a later selected row violates a
// CHECK constraint, earlier selected rows from the same statement should not
// persist.
func TestInsertSelectCheckConstraintIsStatementAtomicRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "insert select check constraint is statement atomic",
			SetUpScript: []string{
				`CREATE TABLE atomic_insert_select_check_target (
					id INT PRIMARY KEY,
					qty INT CHECK (qty > 0)
				);`,
				`CREATE TABLE atomic_insert_select_check_source (
					id INT,
					qty INT
				);`,
				`INSERT INTO atomic_insert_select_check_source VALUES
					(1, 1),
					(2, -1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO atomic_insert_select_check_target
						SELECT id, qty FROM atomic_insert_select_check_source ORDER BY id;`,
					ExpectedErr: `Check constraint`,
				},
				{
					Query:    `SELECT count(*) FROM atomic_insert_select_check_target;`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
	})
}

// TestInsertSelectFunctionSideEffectsRollBackOnConstraintErrorRepro reproduces
// a data consistency bug: side effects from a function evaluated by INSERT ...
// SELECT must roll back if a later selected row fails a target constraint.
func TestInsertSelectFunctionSideEffectsRollBackOnConstraintErrorRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "INSERT SELECT function side effects roll back on constraint error",
			SetUpScript: []string{
				`CREATE TABLE insert_select_function_check_target (
					id INT PRIMARY KEY,
					qty INT NOT NULL CHECK (qty > 0)
				);`,
				`CREATE TABLE insert_select_function_check_source (
					id INT PRIMARY KEY,
					qty INT NOT NULL
				);`,
				`INSERT INTO insert_select_function_check_source VALUES
					(1, 10),
					(2, -1);`,
				`CREATE TABLE insert_select_function_check_audit (
					id INT PRIMARY KEY
				);`,
				`CREATE FUNCTION audit_and_return_insert_select_qty(id_arg INT, qty_arg INT) RETURNS INT AS $$
				BEGIN
					INSERT INTO insert_select_function_check_audit VALUES (id_arg);
					RETURN qty_arg;
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO insert_select_function_check_target
						SELECT id, audit_and_return_insert_select_qty(id, qty)
						FROM insert_select_function_check_source
						ORDER BY id;`,
					ExpectedErr: `Check constraint`,
				},
				{
					Query: `SELECT
						(SELECT count(*) FROM insert_select_function_check_target),
						(SELECT count(*) FROM insert_select_function_check_audit);`,
					Expected: []sql.Row{{int64(0), int64(0)}},
				},
			},
		},
	})
}

// TestInsertSelectFunctionSideEffectsRollBackOnDuplicateKeyErrorRepro
// reproduces a data consistency bug: side effects from a function evaluated by
// INSERT ... SELECT must roll back if a later selected row hits a duplicate
// target key.
func TestInsertSelectFunctionSideEffectsRollBackOnDuplicateKeyErrorRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "INSERT SELECT function side effects roll back on duplicate key error",
			SetUpScript: []string{
				`CREATE TABLE insert_select_function_duplicate_target (
					id INT PRIMARY KEY,
					label TEXT NOT NULL
				);`,
				`CREATE TABLE insert_select_function_duplicate_source (
					seq INT PRIMARY KEY,
					id INT NOT NULL,
					label TEXT NOT NULL
				);`,
				`INSERT INTO insert_select_function_duplicate_source VALUES
					(1, 1, 'first'),
					(2, 1, 'duplicate');`,
				`CREATE TABLE insert_select_function_duplicate_audit (
					seq INT PRIMARY KEY
				);`,
				`CREATE FUNCTION audit_and_return_insert_select_label(seq_arg INT, label_arg TEXT) RETURNS TEXT AS $$
				BEGIN
					INSERT INTO insert_select_function_duplicate_audit VALUES (seq_arg);
					RETURN label_arg;
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO insert_select_function_duplicate_target
						SELECT id, audit_and_return_insert_select_label(seq, label)
						FROM insert_select_function_duplicate_source
						ORDER BY seq;`,
					ExpectedErr: `duplicate`,
				},
				{
					Query: `SELECT
						(SELECT count(*) FROM insert_select_function_duplicate_target),
						(SELECT count(*) FROM insert_select_function_duplicate_audit);`,
					Expected: []sql.Row{{int64(0), int64(0)}},
				},
			},
		},
	})
}

// TestOnConflictDoNothingNonTargetUniqueViolationIsStatementAtomicRepro guards
// PostgreSQL statement atomicity: if an INSERT ... ON CONFLICT DO NOTHING hits a
// non-target unique violation after inserting an earlier row, none of the
// statement's rows should persist.
func TestOnConflictDoNothingNonTargetUniqueViolationIsStatementAtomicRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ON CONFLICT DO NOTHING non-target unique violation is statement atomic",
			SetUpScript: []string{
				`CREATE TABLE atomic_on_conflict_do_nothing_items (
					id INT PRIMARY KEY,
					code INT UNIQUE
				);`,
				`INSERT INTO atomic_on_conflict_do_nothing_items VALUES (1, 10);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO atomic_on_conflict_do_nothing_items VALUES
							(2, 20),
							(3, 10)
						ON CONFLICT (id) DO NOTHING;`,
					ExpectedErr: `duplicate`,
				},
				{
					Query: `SELECT id, code
						FROM atomic_on_conflict_do_nothing_items
						ORDER BY id;`,
					Expected: []sql.Row{{1, 10}},
				},
			},
		},
	})
}

// TestOnConflictDoNothingFunctionSideEffectsRollBackOnNonTargetUniqueErrorRepro
// reproduces a data consistency bug: side effects from INSERT expressions must
// roll back when ON CONFLICT DO NOTHING later fails a non-arbiter unique
// constraint.
func TestOnConflictDoNothingFunctionSideEffectsRollBackOnNonTargetUniqueErrorRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ON CONFLICT DO NOTHING function side effects roll back on non-target unique error",
			SetUpScript: []string{
				`CREATE TABLE on_conflict_do_nothing_function_target (
					id INT PRIMARY KEY,
					code INT UNIQUE
				);`,
				`INSERT INTO on_conflict_do_nothing_function_target VALUES (1, 10);`,
				`CREATE TABLE on_conflict_do_nothing_function_audit (
					seq INT PRIMARY KEY
				);`,
				`CREATE FUNCTION audit_and_return_on_conflict_code(seq_arg INT, code_arg INT) RETURNS INT AS $$
				BEGIN
					INSERT INTO on_conflict_do_nothing_function_audit VALUES (seq_arg);
					RETURN code_arg;
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO on_conflict_do_nothing_function_target VALUES
							(2, audit_and_return_on_conflict_code(1, 20)),
							(3, audit_and_return_on_conflict_code(2, 10))
						ON CONFLICT (id) DO NOTHING;`,
					ExpectedErr: `duplicate`,
				},
				{
					Query: `SELECT
						(SELECT count(*) FROM on_conflict_do_nothing_function_target),
						(SELECT count(*) FROM on_conflict_do_nothing_function_audit);`,
					Expected: []sql.Row{{int64(1), int64(0)}},
				},
				{
					Query: `SELECT id, code
						FROM on_conflict_do_nothing_function_target
						ORDER BY id;`,
					Expected: []sql.Row{{1, 10}},
				},
			},
		},
	})
}

// TestOnConflictDoNothingSkipsDuplicateInputRowsGuard guards PostgreSQL
// conflict handling: duplicate proposed rows for the arbiter key are handled
// by inserting the first row and skipping later conflicting rows.
func TestOnConflictDoNothingSkipsDuplicateInputRowsGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ON CONFLICT DO NOTHING skips duplicate input rows",
			SetUpScript: []string{
				`CREATE TABLE on_conflict_duplicate_input_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO on_conflict_duplicate_input_items VALUES
							(1, 'first'),
							(1, 'second')
						ON CONFLICT (id) DO NOTHING;`,
				},
				{
					Query: `SELECT id, label
						FROM on_conflict_duplicate_input_items;`,
					Expected: []sql.Row{{1, "first"}},
				},
			},
		},
	})
}

// TestOnConflictDoUpdateCheckViolationIsStatementAtomicRepro guards PostgreSQL
// statement atomicity: if an INSERT ... ON CONFLICT DO UPDATE violates a CHECK
// constraint after inserting an earlier row, none of the statement's writes
// should persist.
func TestOnConflictDoUpdateCheckViolationIsStatementAtomicRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ON CONFLICT DO UPDATE check violation is statement atomic",
			SetUpScript: []string{
				`CREATE TABLE atomic_on_conflict_update_check_items (
					id INT PRIMARY KEY,
					qty INT CHECK (qty > 0)
				);`,
				`INSERT INTO atomic_on_conflict_update_check_items VALUES (1, 1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO atomic_on_conflict_update_check_items VALUES
							(2, 2),
							(1, -1)
						ON CONFLICT (id) DO UPDATE SET qty = EXCLUDED.qty;`,
					ExpectedErr: `Check constraint`,
				},
				{
					Query: `SELECT id, qty
						FROM atomic_on_conflict_update_check_items
						ORDER BY id;`,
					Expected: []sql.Row{{1, 1}},
				},
			},
		},
	})
}

// TestOnConflictDoUpdateFunctionSideEffectsRollBackOnConstraintErrorRepro
// reproduces a data consistency bug: function side effects from the
// conflict-update assignment must roll back when the updated row later violates
// a CHECK constraint.
func TestOnConflictDoUpdateFunctionSideEffectsRollBackOnConstraintErrorRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ON CONFLICT DO UPDATE function side effects roll back on constraint error",
			SetUpScript: []string{
				`CREATE TABLE upsert_function_side_effect_target (
					id INT PRIMARY KEY,
					qty INT NOT NULL CHECK (qty > 0)
				);`,
				`INSERT INTO upsert_function_side_effect_target VALUES (1, 1);`,
				`CREATE TABLE upsert_function_side_effect_audit (
					id INT PRIMARY KEY,
					qty INT NOT NULL
				);`,
				`CREATE FUNCTION audit_and_return_bad_upsert(id_arg INT) RETURNS INT AS $$
				BEGIN
					INSERT INTO upsert_function_side_effect_audit VALUES (id_arg, -1);
					RETURN -1;
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO upsert_function_side_effect_target VALUES (1, 2)
						ON CONFLICT (id) DO UPDATE
						SET qty = audit_and_return_bad_upsert(upsert_function_side_effect_target.id);`,
					ExpectedErr: `Check constraint`,
				},
				{
					Query: `SELECT id, qty
						FROM upsert_function_side_effect_target
						ORDER BY id;`,
					Expected: []sql.Row{{1, 1}},
				},
				{
					Query:    `SELECT count(*) FROM upsert_function_side_effect_audit;`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
	})
}

// TestOnConflictDoUpdateInsertFunctionSideEffectsRollBackOnConstraintErrorRepro
// reproduces a data consistency bug: side effects from functions evaluated in
// the INSERT side of an ON CONFLICT DO UPDATE statement must roll back when the
// conflict update later fails a target constraint.
func TestOnConflictDoUpdateInsertFunctionSideEffectsRollBackOnConstraintErrorRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ON CONFLICT DO UPDATE insert function side effects roll back on constraint error",
			SetUpScript: []string{
				`CREATE TABLE upsert_insert_function_side_effect_target (
					id INT PRIMARY KEY,
					qty INT NOT NULL CHECK (qty > 0)
				);`,
				`INSERT INTO upsert_insert_function_side_effect_target VALUES (1, 1);`,
				`CREATE TABLE upsert_insert_function_side_effect_audit (
					id INT PRIMARY KEY
				);`,
				`CREATE FUNCTION audit_and_return_bad_upsert_insert(id_arg INT, qty_arg INT) RETURNS INT AS $$
				BEGIN
					INSERT INTO upsert_insert_function_side_effect_audit VALUES (id_arg);
					RETURN qty_arg;
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO upsert_insert_function_side_effect_target VALUES
							(1, audit_and_return_bad_upsert_insert(1, -1))
						ON CONFLICT (id) DO UPDATE SET qty = EXCLUDED.qty;`,
					ExpectedErr: `Check constraint`,
				},
				{
					Query: `SELECT
						(SELECT qty FROM upsert_insert_function_side_effect_target WHERE id = 1),
						(SELECT count(*) FROM upsert_insert_function_side_effect_audit);`,
					Expected: []sql.Row{{1, int64(0)}},
				},
			},
		},
	})
}

// TestOnConflictDoUpdateReturningExpressionErrorRollsBackUpdateRepro guards ON
// CONFLICT DO UPDATE RETURNING atomicity: if a plain RETURNING expression
// errors, PostgreSQL rolls back the conflict update.
func TestOnConflictDoUpdateReturningExpressionErrorRollsBackUpdateRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ON CONFLICT DO UPDATE RETURNING expression error rolls back update",
			SetUpScript: []string{
				`CREATE TABLE upsert_returning_error_target (
					id INT PRIMARY KEY,
					qty INT NOT NULL
				);`,
				`INSERT INTO upsert_returning_error_target VALUES (1, 1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO upsert_returning_error_target VALUES (1, 2)
						ON CONFLICT (id) DO UPDATE SET qty = EXCLUDED.qty
						RETURNING 1 / 0;`,
					ExpectedErr: `division by zero`,
				},
				{
					Query:    `SELECT id, qty FROM upsert_returning_error_target;`,
					Expected: []sql.Row{{1, 1}},
				},
			},
		},
	})
}

// TestOnConflictDoUpdateReturningFunctionSucceedsAfterUpdateRepro guards that
// PostgreSQL permits PL/pgSQL functions in the RETURNING list of a conflict
// update, and the successful statement commits both the row update and the
// function's side effect.
func TestOnConflictDoUpdateReturningFunctionSucceedsAfterUpdateRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ON CONFLICT DO UPDATE RETURNING function succeeds after update",
			SetUpScript: []string{
				`CREATE TABLE upsert_returning_function_target (
					id INT PRIMARY KEY,
					qty INT NOT NULL
				);`,
				`INSERT INTO upsert_returning_function_target VALUES (1, 1);`,
				`CREATE TABLE upsert_returning_function_audit (
					id INT PRIMARY KEY
				);`,
				`CREATE FUNCTION audit_upsert_returning(id_arg INT) RETURNS INT AS $$
				BEGIN
					INSERT INTO upsert_returning_function_audit VALUES (id_arg);
					RETURN id_arg;
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO upsert_returning_function_target VALUES (1, 2)
						ON CONFLICT (id) DO UPDATE SET qty = EXCLUDED.qty
						RETURNING audit_upsert_returning(id);`,
					Expected: []sql.Row{{1}},
				},
				{
					Query:    `SELECT id, qty FROM upsert_returning_function_target;`,
					Expected: []sql.Row{{1, 2}},
				},
				{
					Query:    `SELECT count(*) FROM upsert_returning_function_audit;`,
					Expected: []sql.Row{{int64(1)}},
				},
			},
		},
	})
}

// TestUpdateDuplicateKeyIsStatementAtomicRepro guards PostgreSQL statement
// atomicity for UPDATE: if an UPDATE hits a duplicate key, none of that
// statement's row changes should persist.
func TestUpdateDuplicateKeyIsStatementAtomicRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "update duplicate key is statement atomic",
			SetUpScript: []string{
				`CREATE TABLE atomic_update_items (
					id INT PRIMARY KEY,
					code INT UNIQUE,
					v TEXT
				);`,
				`INSERT INTO atomic_update_items VALUES
					(1, 10, 'one'),
					(2, 20, 'two');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `UPDATE atomic_update_items SET code = 10, v = 'changed';`,
					ExpectedErr: `duplicate`,
				},
				{
					Query: `SELECT id, code, v
						FROM atomic_update_items
						ORDER BY id;`,
					Expected: []sql.Row{
						{1, 10, "one"},
						{2, 20, "two"},
					},
				},
			},
		},
	})
}

// TestUpdateFromDuplicateKeyIsStatementAtomicGuard guards PostgreSQL statement
// atomicity for UPDATE ... FROM: if a joined update hits a duplicate key, none
// of that statement's row changes should persist.
func TestUpdateFromDuplicateKeyIsStatementAtomicGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "UPDATE FROM duplicate key is statement atomic",
			SetUpScript: []string{
				`CREATE TABLE atomic_update_from_unique_items (
					id INT PRIMARY KEY,
					code INT UNIQUE,
					v TEXT
				);`,
				`CREATE TABLE atomic_update_from_unique_source (
					id INT PRIMARY KEY,
					new_code INT,
					new_v TEXT
				);`,
				`INSERT INTO atomic_update_from_unique_items VALUES
					(1, 10, 'one'),
					(2, 20, 'two');`,
				`INSERT INTO atomic_update_from_unique_source VALUES
					(1, 30, 'changed-one'),
					(2, 30, 'changed-two');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE atomic_update_from_unique_items AS t
						SET code = s.new_code, v = s.new_v
						FROM atomic_update_from_unique_source AS s
						WHERE t.id = s.id;`,
					ExpectedErr: `duplicate`,
				},
				{
					Query: `SELECT id, code, v
						FROM atomic_update_from_unique_items
						ORDER BY id;`,
					Expected: []sql.Row{
						{1, 10, "one"},
						{2, 20, "two"},
					},
				},
			},
		},
	})
}

// TestUpdateFromCheckConstraintIsStatementAtomicRepro reproduces a data
// consistency bug: UPDATE ... FROM must enforce target-table CHECK constraints
// and leave no partial writes behind when a joined update row violates one.
func TestUpdateFromCheckConstraintIsStatementAtomicRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "UPDATE FROM check constraint is statement atomic",
			SetUpScript: []string{
				`CREATE TABLE atomic_update_from_items (
					id INT PRIMARY KEY,
					qty INT CHECK (qty > 0)
				);`,
				`CREATE TABLE atomic_update_from_source (
					id INT PRIMARY KEY,
					new_qty INT
				);`,
				`INSERT INTO atomic_update_from_items VALUES (1, 1), (2, 2);`,
				`INSERT INTO atomic_update_from_source VALUES (1, 10), (2, -1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE atomic_update_from_items AS t
						SET qty = s.new_qty
						FROM atomic_update_from_source AS s
						WHERE t.id = s.id;`,
					ExpectedErr: `Check constraint`,
				},
				{
					Query: `SELECT id, qty
						FROM atomic_update_from_items
						ORDER BY id;`,
					Expected: []sql.Row{
						{1, 1},
						{2, 2},
					},
				},
			},
		},
	})
}

// TestUpdateFromNotNullConstraintIsStatementAtomicGuard guards that UPDATE ...
// FROM enforces target-table NOT NULL constraints and rolls back the statement
// if any joined row violates one.
func TestUpdateFromNotNullConstraintIsStatementAtomicGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "UPDATE FROM not null constraint is statement atomic",
			SetUpScript: []string{
				`CREATE TABLE atomic_update_from_not_null_items (
					id INT PRIMARY KEY,
					label TEXT NOT NULL
				);`,
				`CREATE TABLE atomic_update_from_not_null_source (
					id INT PRIMARY KEY,
					new_label TEXT
				);`,
				`INSERT INTO atomic_update_from_not_null_items VALUES
					(1, 'one'),
					(2, 'two');`,
				`INSERT INTO atomic_update_from_not_null_source VALUES
					(1, 'updated'),
					(2, NULL);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE atomic_update_from_not_null_items AS t
						SET label = s.new_label
						FROM atomic_update_from_not_null_source AS s
						WHERE t.id = s.id;`,
					ExpectedErr: `non-nullable`,
				},
				{
					Query: `SELECT id, label
						FROM atomic_update_from_not_null_items
						ORDER BY id;`,
					Expected: []sql.Row{
						{1, "one"},
						{2, "two"},
					},
				},
			},
		},
	})
}

// TestUpdateFromForeignKeyConstraintIsStatementAtomicGuard guards PostgreSQL
// statement atomicity for UPDATE ... FROM: foreign-key violations from joined
// updates must reject the statement and leave prior target rows unchanged.
func TestUpdateFromForeignKeyConstraintIsStatementAtomicGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "UPDATE FROM foreign key constraint is statement atomic",
			SetUpScript: []string{
				`CREATE TABLE atomic_update_from_fk_parents (
					id INT PRIMARY KEY
				);`,
				`CREATE TABLE atomic_update_from_fk_children (
					id INT PRIMARY KEY,
					parent_id INT REFERENCES atomic_update_from_fk_parents(id)
				);`,
				`CREATE TABLE atomic_update_from_fk_source (
					id INT PRIMARY KEY,
					new_parent_id INT
				);`,
				`INSERT INTO atomic_update_from_fk_parents VALUES (1), (2);`,
				`INSERT INTO atomic_update_from_fk_children VALUES (1, 1), (2, 2);`,
				`INSERT INTO atomic_update_from_fk_source VALUES (1, 2), (2, 999);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE atomic_update_from_fk_children AS c
						SET parent_id = s.new_parent_id
						FROM atomic_update_from_fk_source AS s
						WHERE c.id = s.id;`,
					ExpectedErr: `Foreign key violation`,
				},
				{
					Query: `SELECT id, parent_id
						FROM atomic_update_from_fk_children
						ORDER BY id;`,
					Expected: []sql.Row{
						{1, 1},
						{2, 2},
					},
				},
			},
		},
	})
}

// TestDeleteForeignKeyRestrictIsStatementAtomicRepro guards PostgreSQL
// statement atomicity for DELETE: if a later row is blocked by a referencing
// foreign key, earlier deletes from the same statement should not persist.
func TestDeleteForeignKeyRestrictIsStatementAtomicRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "delete foreign key restrict is statement atomic",
			SetUpScript: []string{
				`CREATE TABLE atomic_delete_parent (
					id INT PRIMARY KEY
				);`,
				`CREATE TABLE atomic_delete_child (
					id INT PRIMARY KEY,
					parent_id INT REFERENCES atomic_delete_parent(id)
				);`,
				`INSERT INTO atomic_delete_parent VALUES (1), (2);`,
				`INSERT INTO atomic_delete_child VALUES (10, 2);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DELETE FROM atomic_delete_parent WHERE id IN (1, 2);`,
					ExpectedErr: `Foreign key`,
				},
				{
					Query: `SELECT id
						FROM atomic_delete_parent
						ORDER BY id;`,
					Expected: []sql.Row{{1}, {2}},
				},
			},
		},
	})
}

// TestPlpgsqlExceptionBlockRollsBackInnerWritesRepro reproduces a PostgreSQL
// correctness bug: a PL/pgSQL block with an EXCEPTION handler runs the protected
// block as a subtransaction, so writes before the caught exception are rolled
// back while handler writes persist.
func TestPlpgsqlExceptionBlockRollsBackInnerWritesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "PL/pgSQL exception block rolls back inner writes",
			SetUpScript: []string{
				`CREATE TABLE plpgsql_exception_atomic_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DO $$
						BEGIN
							BEGIN
								INSERT INTO plpgsql_exception_atomic_items VALUES (1, 'before exception');
								RAISE EXCEPTION 'rollback protected block';
							EXCEPTION
								WHEN OTHERS THEN
									INSERT INTO plpgsql_exception_atomic_items VALUES (2, 'handler');
							END;
						END;
					$$;`,
				},
				{
					Query: `SELECT id, label
						FROM plpgsql_exception_atomic_items
						ORDER BY id;`,
					Expected: []sql.Row{{2, "handler"}},
				},
			},
		},
	})
}

// TestPlpgsqlUnhandledExceptionRollsBackFunctionWritesRepro guards PostgreSQL
// function-call atomicity: writes performed by a PL/pgSQL function must roll
// back when the function raises an unhandled exception.
func TestPlpgsqlUnhandledExceptionRollsBackFunctionWritesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "PL/pgSQL unhandled exception rolls back function writes",
			SetUpScript: []string{
				`CREATE TABLE plpgsql_unhandled_exception_items (
					id INT PRIMARY KEY,
					label TEXT NOT NULL
				);`,
				`CREATE FUNCTION insert_then_raise_plpgsql_exception() RETURNS VOID AS $$
				BEGIN
					INSERT INTO plpgsql_unhandled_exception_items VALUES (1, 'before exception');
					RAISE EXCEPTION 'rollback function body';
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `SELECT insert_then_raise_plpgsql_exception();`,
					ExpectedErr: `rollback function body`,
				},
				{
					Query:    `SELECT count(*) FROM plpgsql_unhandled_exception_items;`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
	})
}

// TestDoBlockUnhandledExceptionRollsBackWritesRepro guards PostgreSQL
// anonymous-block atomicity: writes performed by a PL/pgSQL DO block must roll
// back when the block raises an unhandled exception.
func TestDoBlockUnhandledExceptionRollsBackWritesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DO block unhandled exception rolls back writes",
			SetUpScript: []string{
				`CREATE TABLE do_unhandled_exception_items (
					id INT PRIMARY KEY,
					label TEXT NOT NULL
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DO $$
					BEGIN
						INSERT INTO do_unhandled_exception_items VALUES (1, 'before exception');
						RAISE EXCEPTION 'rollback do block';
					END;
					$$;`,
					ExpectedErr: `rollback do block`,
				},
				{
					Query:    `SELECT count(*) FROM do_unhandled_exception_items;`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
	})
}

// TestSqlFunctionErrorRollsBackFunctionWritesRepro reproduces a data
// consistency bug: writes performed by a SQL-language function must roll back
// when a later statement in the same function fails.
func TestSqlFunctionErrorRollsBackFunctionWritesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "SQL function error rolls back function writes",
			SetUpScript: []string{
				`CREATE TABLE sql_function_atomic_items (
					id INT PRIMARY KEY,
					label TEXT NOT NULL
				);`,
				`CREATE FUNCTION insert_then_duplicate_sql() RETURNS VOID AS $$
					INSERT INTO sql_function_atomic_items VALUES (1, 'first');
					INSERT INTO sql_function_atomic_items VALUES (1, 'duplicate');
				$$ LANGUAGE SQL;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `SELECT insert_then_duplicate_sql();`,
					ExpectedErr: `duplicate`,
				},
				{
					Query:    `SELECT count(*) FROM sql_function_atomic_items;`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
	})
}

// TestFunctionSideEffectsRollBackOnOuterStatementErrorRepro reproduces a data
// consistency bug: side effects from a function called by a DML expression must
// roll back if the outer statement later fails a table constraint.
func TestFunctionSideEffectsRollBackOnOuterStatementErrorRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "function side effects roll back on outer statement error",
			SetUpScript: []string{
				`CREATE TABLE function_side_effect_target (
					id INT PRIMARY KEY,
					qty INT NOT NULL CHECK (qty > 0)
				);`,
				`CREATE TABLE function_side_effect_audit (
					id INT PRIMARY KEY,
					qty INT NOT NULL
				);`,
				`CREATE FUNCTION audit_and_return_bad_qty(id_arg INT) RETURNS INT AS $$
				BEGIN
					INSERT INTO function_side_effect_audit VALUES (id_arg, -1);
					RETURN -1;
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `INSERT INTO function_side_effect_target VALUES (1, audit_and_return_bad_qty(1));`,
					ExpectedErr: `Check constraint`,
				},
				{
					Query:    `SELECT count(*) FROM function_side_effect_target;`,
					Expected: []sql.Row{{int64(0)}},
				},
				{
					Query:    `SELECT count(*) FROM function_side_effect_audit;`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
	})
}

// TestInsertFunctionSideEffectsRollBackOnForeignKeyErrorRepro reproduces a
// data consistency bug: side effects from a function called by an INSERT
// expression must roll back if the inserted row later fails a foreign key.
func TestInsertFunctionSideEffectsRollBackOnForeignKeyErrorRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "INSERT function side effects roll back on foreign-key error",
			SetUpScript: []string{
				`CREATE TABLE insert_function_fk_parent (
					id INT PRIMARY KEY
				);`,
				`CREATE TABLE insert_function_fk_child (
					id INT PRIMARY KEY,
					parent_id INT REFERENCES insert_function_fk_parent(id)
				);`,
				`CREATE TABLE insert_function_fk_audit (
					id INT PRIMARY KEY
				);`,
				`CREATE FUNCTION audit_and_return_missing_insert_parent(id_arg INT) RETURNS INT AS $$
				BEGIN
					INSERT INTO insert_function_fk_audit VALUES (id_arg);
					RETURN 999;
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `INSERT INTO insert_function_fk_child VALUES (1, audit_and_return_missing_insert_parent(1));`,
					ExpectedErr: `Foreign key violation`,
				},
				{
					Query:    `SELECT count(*) FROM insert_function_fk_child;`,
					Expected: []sql.Row{{int64(0)}},
				},
				{
					Query:    `SELECT count(*) FROM insert_function_fk_audit;`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
	})
}

// TestUpdateFunctionForeignKeyViolationReportsForeignKeyErrorRepro reproduces a
// correctness bug: an UPDATE expression that calls a PL/pgSQL function and then
// violates a foreign key should report the foreign-key violation, not a
// recovered internal panic.
func TestUpdateFunctionForeignKeyViolationReportsForeignKeyErrorRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "UPDATE function foreign-key violation reports foreign-key error",
			SetUpScript: []string{
				`CREATE TABLE update_function_fk_parent (
					id INT PRIMARY KEY
				);`,
				`CREATE TABLE update_function_fk_child (
					id INT PRIMARY KEY,
					parent_id INT REFERENCES update_function_fk_parent(id)
				);`,
				`INSERT INTO update_function_fk_parent VALUES (1);`,
				`INSERT INTO update_function_fk_child VALUES (10, 1);`,
				`CREATE TABLE update_function_fk_audit (
					id INT PRIMARY KEY
				);`,
				`CREATE FUNCTION audit_and_return_missing_update_parent(id_arg INT) RETURNS INT AS $$
				BEGIN
					INSERT INTO update_function_fk_audit VALUES (id_arg);
					RETURN 999;
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE update_function_fk_child
						SET parent_id = audit_and_return_missing_update_parent(id)
						WHERE id = 10;`,
					ExpectedErr: `Foreign key violation`,
				},
				{
					Query:    `SELECT parent_id FROM update_function_fk_child WHERE id = 10;`,
					Expected: []sql.Row{{1}},
				},
				{
					Query:    `SELECT count(*) FROM update_function_fk_audit;`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
	})
}

// TestDeleteFunctionPredicateForeignKeyViolationReportsForeignKeyErrorRepro
// reproduces a correctness bug: a DELETE predicate that calls a PL/pgSQL
// function and then violates a foreign key should report the foreign-key
// violation, not a recovered internal panic.
func TestDeleteFunctionPredicateForeignKeyViolationReportsForeignKeyErrorRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DELETE function predicate foreign-key violation reports foreign-key error",
			SetUpScript: []string{
				`CREATE TABLE delete_function_fk_parent (
					id INT PRIMARY KEY
				);`,
				`CREATE TABLE delete_function_fk_child (
					id INT PRIMARY KEY,
					parent_id INT REFERENCES delete_function_fk_parent(id)
				);`,
				`INSERT INTO delete_function_fk_parent VALUES (1);`,
				`INSERT INTO delete_function_fk_child VALUES (10, 1);`,
				`CREATE TABLE delete_function_fk_audit (
					id INT PRIMARY KEY
				);`,
				`CREATE FUNCTION audit_and_return_delete_parent(id_arg INT) RETURNS INT AS $$
				BEGIN
					INSERT INTO delete_function_fk_audit VALUES (id_arg);
					RETURN id_arg;
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DELETE FROM delete_function_fk_parent WHERE id = audit_and_return_delete_parent(1);`,
					ExpectedErr: `Foreign key`,
				},
				{
					Query:    `SELECT count(*) FROM delete_function_fk_parent;`,
					Expected: []sql.Row{{int64(1)}},
				},
				{
					Query:    `SELECT count(*) FROM delete_function_fk_audit;`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
	})
}

// TestSelectFunctionSideEffectsRollBackOnExpressionErrorRepro reproduces a data
// consistency bug: side effects from a function called by a SELECT expression
// must roll back if another expression in the same SELECT statement errors.
func TestSelectFunctionSideEffectsRollBackOnExpressionErrorRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "SELECT function side effects roll back on expression error",
			SetUpScript: []string{
				`CREATE TABLE select_function_side_effect_audit (
					id INT PRIMARY KEY
				);`,
				`CREATE FUNCTION audit_and_return_bad_select(id_arg INT) RETURNS INT AS $$
				BEGIN
					INSERT INTO select_function_side_effect_audit VALUES (id_arg);
					RETURN id_arg;
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `SELECT audit_and_return_bad_select(1), 1 / 0;`,
					ExpectedErr: `division by zero`,
				},
				{
					Query:    `SELECT count(*) FROM select_function_side_effect_audit;`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
	})
}

// TestReturningFunctionSideEffectsRollBackOnExpressionErrorRepro reproduces a
// data consistency bug: if a RETURNING expression errors, PostgreSQL rolls back
// the base write and any function side effects from the RETURNING list.
func TestReturningFunctionSideEffectsRollBackOnExpressionErrorRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "RETURNING function side effects roll back on expression error",
			SetUpScript: []string{
				`CREATE TABLE returning_function_target (
					id INT PRIMARY KEY
				);`,
				`CREATE TABLE returning_function_audit (
					id INT PRIMARY KEY
				);`,
				`CREATE FUNCTION audit_and_return_bad_returning(id_arg INT) RETURNS INT AS $$
				BEGIN
					INSERT INTO returning_function_audit VALUES (id_arg);
					RETURN id_arg;
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO returning_function_target VALUES (1)
						RETURNING audit_and_return_bad_returning(id), 1 / 0;`,
					ExpectedErr: `division by zero`,
				},
				{
					Query:    `SELECT count(*) FROM returning_function_target;`,
					Expected: []sql.Row{{int64(0)}},
				},
				{
					Query:    `SELECT count(*) FROM returning_function_audit;`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
	})
}

// TestUpdateReturningExpressionErrorRollsBackRowChangeRepro guards UPDATE
// RETURNING atomicity: if a plain RETURNING expression errors, PostgreSQL rolls
// back the row change from the failed statement.
func TestUpdateReturningExpressionErrorRollsBackRowChangeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "UPDATE RETURNING expression error rolls back row change",
			SetUpScript: []string{
				`CREATE TABLE update_returning_error_target (
					id INT PRIMARY KEY,
					qty INT NOT NULL
				);`,
				`INSERT INTO update_returning_error_target VALUES (1, 1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE update_returning_error_target SET qty = 2
						RETURNING 1 / 0;`,
					ExpectedErr: `division by zero`,
				},
				{
					Query:    `SELECT id, qty FROM update_returning_error_target;`,
					Expected: []sql.Row{{1, 1}},
				},
			},
		},
	})
}

// TestUpdateReturningFunctionSucceedsAfterRowChangeRepro reproduces a
// correctness bug: PostgreSQL permits PL/pgSQL functions in UPDATE RETURNING,
// and the successful statement commits both the row change and the function's
// side effect.
func TestUpdateReturningFunctionSucceedsAfterRowChangeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "UPDATE RETURNING function succeeds after row change",
			SetUpScript: []string{
				`CREATE TABLE update_returning_function_target (
					id INT PRIMARY KEY,
					qty INT NOT NULL
				);`,
				`INSERT INTO update_returning_function_target VALUES (1, 1);`,
				`CREATE TABLE update_returning_function_audit (
					id INT PRIMARY KEY
				);`,
				`CREATE FUNCTION audit_update_returning(id_arg INT) RETURNS INT AS $$
				BEGIN
					INSERT INTO update_returning_function_audit VALUES (id_arg);
					RETURN id_arg;
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE update_returning_function_target SET qty = 2
						RETURNING audit_update_returning(id);`,
					Expected: []sql.Row{{1}},
				},
				{
					Query:    `SELECT id, qty FROM update_returning_function_target;`,
					Expected: []sql.Row{{1, 2}},
				},
				{
					Query:    `SELECT count(*) FROM update_returning_function_audit;`,
					Expected: []sql.Row{{int64(1)}},
				},
			},
		},
	})
}

// TestDeleteReturningExpressionErrorRollsBackDeleteRepro guards DELETE
// RETURNING atomicity: if a plain RETURNING expression errors, PostgreSQL rolls
// back the delete from the failed statement.
func TestDeleteReturningExpressionErrorRollsBackDeleteRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DELETE RETURNING expression error rolls back delete",
			SetUpScript: []string{
				`CREATE TABLE delete_returning_error_target (
					id INT PRIMARY KEY,
					qty INT NOT NULL
				);`,
				`INSERT INTO delete_returning_error_target VALUES (1, 1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DELETE FROM delete_returning_error_target
						WHERE id = 1
						RETURNING 1 / 0;`,
					ExpectedErr: `division by zero`,
				},
				{
					Query:    `SELECT id, qty FROM delete_returning_error_target;`,
					Expected: []sql.Row{{1, 1}},
				},
			},
		},
	})
}

// TestDeleteReturningFunctionSucceedsAfterDeleteRepro reproduces a correctness
// bug: PostgreSQL permits PL/pgSQL functions in DELETE RETURNING, and the
// successful statement commits both the delete and the function's side effect.
func TestDeleteReturningFunctionSucceedsAfterDeleteRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DELETE RETURNING function succeeds after delete",
			SetUpScript: []string{
				`CREATE TABLE delete_returning_function_target (
					id INT PRIMARY KEY,
					qty INT NOT NULL
				);`,
				`INSERT INTO delete_returning_function_target VALUES (1, 1);`,
				`CREATE TABLE delete_returning_function_audit (
					id INT PRIMARY KEY
				);`,
				`CREATE FUNCTION audit_delete_returning(id_arg INT) RETURNS INT AS $$
				BEGIN
					INSERT INTO delete_returning_function_audit VALUES (id_arg);
					RETURN id_arg;
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DELETE FROM delete_returning_function_target
						WHERE id = 1
						RETURNING audit_delete_returning(id);`,
					Expected: []sql.Row{{1}},
				},
				{
					Query:    `SELECT count(*) FROM delete_returning_function_target;`,
					Expected: []sql.Row{{int64(0)}},
				},
				{
					Query:    `SELECT count(*) FROM delete_returning_function_audit;`,
					Expected: []sql.Row{{int64(1)}},
				},
			},
		},
	})
}

// TestUpdateFunctionSideEffectsRollBackOnConstraintErrorRepro guards UPDATE
// atomicity: side effects from a function called by an UPDATE expression must
// roll back if the updated row later fails a table constraint.
func TestUpdateFunctionSideEffectsRollBackOnConstraintErrorRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "UPDATE function side effects roll back on constraint error",
			SetUpScript: []string{
				`CREATE TABLE update_function_side_effect_target (
					id INT PRIMARY KEY,
					qty INT NOT NULL CHECK (qty > 0)
				);`,
				`INSERT INTO update_function_side_effect_target VALUES (1, 1);`,
				`CREATE TABLE update_function_side_effect_audit (
					id INT PRIMARY KEY,
					qty INT NOT NULL
				);`,
				`CREATE FUNCTION audit_and_return_bad_update(id_arg INT) RETURNS INT AS $$
				BEGIN
					INSERT INTO update_function_side_effect_audit VALUES (id_arg, -1);
					RETURN -1;
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE update_function_side_effect_target
						SET qty = audit_and_return_bad_update(id)
						WHERE id = 1;`,
					ExpectedErr: `ERROR`,
				},
				{
					Query:    `SELECT qty FROM update_function_side_effect_target WHERE id = 1;`,
					Expected: []sql.Row{{1}},
				},
				{
					Query:    `SELECT count(*) FROM update_function_side_effect_audit;`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
	})
}

// TestUpdateFunctionConstraintViolationReportsCheckErrorRepro reproduces a
// correctness bug: an UPDATE expression that calls a PL/pgSQL function and then
// violates a CHECK constraint should report the constraint violation, not a
// recovered internal panic.
func TestUpdateFunctionConstraintViolationReportsCheckErrorRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "UPDATE function result violating CHECK reports check error",
			SetUpScript: []string{
				`CREATE TABLE update_function_error_target (
					id INT PRIMARY KEY,
					qty INT NOT NULL CHECK (qty > 0)
				);`,
				`INSERT INTO update_function_error_target VALUES (1, 1);`,
				`CREATE TABLE update_function_error_audit (
					id INT PRIMARY KEY,
					qty INT NOT NULL
				);`,
				`CREATE FUNCTION audit_and_return_bad_update_error(id_arg INT) RETURNS INT AS $$
				BEGIN
					INSERT INTO update_function_error_audit VALUES (id_arg, -1);
					RETURN -1;
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE update_function_error_target
						SET qty = audit_and_return_bad_update_error(id)
						WHERE id = 1;`,
					ExpectedErr: `Check constraint`,
				},
			},
		},
	})
}

// TestCreateTableAsRollsBackFunctionSideEffectsOnQueryErrorRepro reproduces a
// persistence bug: CREATE TABLE AS is atomic in PostgreSQL, so an error while
// evaluating the source query must roll back function side effects and leave no
// durable target relation.
func TestCreateTableAsRollsBackFunctionSideEffectsOnQueryErrorRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE AS rolls back function side effects on query error",
			SetUpScript: []string{
				`CREATE TABLE ctas_side_effect_audit (
					id INT PRIMARY KEY
				);`,
				`CREATE FUNCTION audit_and_return_bad_ctas(id_arg INT) RETURNS INT AS $$
				BEGIN
					INSERT INTO ctas_side_effect_audit VALUES (id_arg);
					RETURN id_arg;
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE ctas_side_effect_target AS
						SELECT audit_and_return_bad_ctas(1) AS id, 1 / 0 AS boom;`,
					ExpectedErr: `division by zero`,
				},
				{
					Query:    `SELECT to_regclass('ctas_side_effect_target') IS NULL;`,
					Expected: []sql.Row{{"t"}},
				},
				{
					Query:    `SELECT count(*) FROM ctas_side_effect_audit;`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
	})
}

// TestCreateMaterializedViewRollsBackFunctionSideEffectsOnQueryErrorRepro
// reproduces a persistence bug: CREATE MATERIALIZED VIEW is atomic in
// PostgreSQL, so an error while evaluating the source query must roll back
// function side effects and leave no durable materialized-view relation.
func TestCreateMaterializedViewRollsBackFunctionSideEffectsOnQueryErrorRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE MATERIALIZED VIEW rolls back function side effects on query error",
			SetUpScript: []string{
				`CREATE TABLE matview_side_effect_audit (
					id INT PRIMARY KEY
				);`,
				`CREATE FUNCTION audit_and_return_bad_matview(id_arg INT) RETURNS INT AS $$
				BEGIN
					INSERT INTO matview_side_effect_audit VALUES (id_arg);
					RETURN id_arg;
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE MATERIALIZED VIEW matview_side_effect_target AS
						SELECT audit_and_return_bad_matview(1) AS id, 1 / 0 AS boom;`,
					ExpectedErr: `division by zero`,
				},
				{
					Query:    `SELECT to_regclass('matview_side_effect_target') IS NULL;`,
					Expected: []sql.Row{{"t"}},
				},
				{
					Query:    `SELECT count(*) FROM matview_side_effect_audit;`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
	})
}

// TestRefreshMaterializedViewRollsBackFunctionSideEffectsOnQueryErrorRepro
// guards REFRESH MATERIALIZED VIEW atomicity: an error while evaluating the
// refresh query must keep the old snapshot and roll back function side effects.
func TestRefreshMaterializedViewRollsBackFunctionSideEffectsOnQueryErrorRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "REFRESH MATERIALIZED VIEW rolls back function side effects on query error",
			SetUpScript: []string{
				`CREATE TABLE refresh_mv_source (
					id INT PRIMARY KEY,
					fail BOOL NOT NULL
				);`,
				`INSERT INTO refresh_mv_source VALUES (1, false);`,
				`CREATE TABLE refresh_mv_side_effect_audit (
					id INT PRIMARY KEY
				);`,
				`CREATE FUNCTION audit_then_maybe_fail_mv(id_arg INT, fail_arg BOOL) RETURNS INT AS $$
				BEGIN
					INSERT INTO refresh_mv_side_effect_audit VALUES (id_arg) ON CONFLICT DO NOTHING;
					IF fail_arg THEN
						RAISE EXCEPTION 'refresh failure';
					END IF;
					RETURN id_arg;
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE MATERIALIZED VIEW refresh_mv_side_effect_target AS
					SELECT audit_then_maybe_fail_mv(id, fail) AS id
					FROM refresh_mv_source;`,
				`TRUNCATE refresh_mv_side_effect_audit;`,
				`UPDATE refresh_mv_source SET fail = true WHERE id = 1;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `REFRESH MATERIALIZED VIEW refresh_mv_side_effect_target;`,
					ExpectedErr: `refresh failure`,
				},
				{
					Query:    `SELECT count(*) FROM refresh_mv_side_effect_audit;`,
					Expected: []sql.Row{{int64(0)}},
				},
				{
					Query:    `SELECT id FROM refresh_mv_side_effect_target;`,
					Expected: []sql.Row{{1}},
				},
			},
		},
	})
}

// TestAlterTableAddColumnRollsBackDefaultSideEffectsOnCheckErrorRepro guards
// ALTER TABLE atomicity: adding a column with a default expression is atomic in
// PostgreSQL, so a CHECK failure while backfilling existing rows must roll back
// both the schema change and default-function side effects.
func TestAlterTableAddColumnRollsBackDefaultSideEffectsOnCheckErrorRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TABLE ADD COLUMN rolls back default side effects on check error",
			SetUpScript: []string{
				`CREATE TABLE alter_default_side_effect_items (
					id INT PRIMARY KEY
				);`,
				`INSERT INTO alter_default_side_effect_items VALUES (1);`,
				`CREATE TABLE alter_default_side_effect_audit (
					id INT PRIMARY KEY
				);`,
				`CREATE FUNCTION audit_and_return_bad_default() RETURNS INT AS $$
				BEGIN
					INSERT INTO alter_default_side_effect_audit VALUES (1);
					RETURN -1;
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE alter_default_side_effect_items
						ADD COLUMN qty INT DEFAULT audit_and_return_bad_default() CHECK (qty > 0);`,
					ExpectedErr: `Check constraint`,
				},
				{
					Query: `SELECT count(*)
						FROM information_schema.columns
						WHERE table_schema = 'public'
							AND table_name = 'alter_default_side_effect_items'
							AND column_name = 'qty';`,
					Expected: []sql.Row{{int64(0)}},
				},
				{
					Query:    `SELECT count(*) FROM alter_default_side_effect_audit;`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
	})
}
