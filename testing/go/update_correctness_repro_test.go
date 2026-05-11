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

// TestUpdateSetImplicitNullDefaultRepro reproduces an UPDATE correctness bug:
// columns without an explicit default should update to NULL when assigned
// DEFAULT, but Doltgres currently rejects the update.
func TestUpdateSetImplicitNullDefaultRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "UPDATE SET DEFAULT uses implicit NULL default",
			SetUpScript: []string{
				`CREATE TABLE update_default_items (i INT);`,
				`INSERT INTO update_default_items VALUES (1), (2), (3);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE update_default_items SET i = DEFAULT;`,
				},
				{
					Query: `SELECT i FROM update_default_items;`,
					Expected: []sql.Row{
						{nil},
						{nil},
						{nil},
					},
				},
			},
		},
	})
}

// TestUpdateAssignmentsUseOriginalRowValuesRepro guards PostgreSQL's
// simultaneous UPDATE assignment semantics: right-hand expressions read the
// original row values, not values assigned earlier in the same SET list.
func TestUpdateAssignmentsUseOriginalRowValuesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "UPDATE assignments read original row values",
			SetUpScript: []string{
				`CREATE TABLE update_assignment_items (id INT PRIMARY KEY, a INT, b INT);`,
				`INSERT INTO update_assignment_items VALUES (1, 10, 20);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE update_assignment_items SET a = b, b = a WHERE id = 1;`,
				},
				{
					Query:    `SELECT a, b FROM update_assignment_items WHERE id = 1;`,
					Expected: []sql.Row{{20, 10}},
				},
			},
		},
	})
}

// TestUpdateMultiAssignmentFromSubqueryRepro guards PostgreSQL's row-valued
// UPDATE assignment syntax, where a scalar subquery can populate multiple
// target columns in one simultaneous assignment.
func TestUpdateMultiAssignmentFromSubqueryRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "UPDATE multi-assignment reads values from subquery",
			SetUpScript: []string{
				`CREATE TABLE update_multi_assignment_items (
					id INT PRIMARY KEY,
					a INT NOT NULL,
					b INT NOT NULL
				);`,
				`INSERT INTO update_multi_assignment_items VALUES (1, 10, 20), (2, 30, 40);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE update_multi_assignment_items AS t
						SET (a, b) = (
							SELECT s.b, s.a
							FROM update_multi_assignment_items AS s
							WHERE s.id = t.id
						)
						WHERE t.id = 1;`,
				},
				{
					Query: `SELECT id, a, b FROM update_multi_assignment_items ORDER BY id;`,
					Expected: []sql.Row{
						{1, 20, 10},
						{2, 30, 40},
					},
				},
			},
		},
	})
}

// TestUpdateSelfReferentialSubqueryUsesStatementSnapshotGuard guards
// PostgreSQL's statement-snapshot semantics: a subquery reading the target
// table must see the pre-update rows, not rows already rewritten earlier in the
// same statement.
func TestUpdateSelfReferentialSubqueryUsesStatementSnapshotGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "UPDATE self-referential subquery uses statement snapshot",
			SetUpScript: []string{
				`CREATE TABLE update_self_subquery_items (
					id INT PRIMARY KEY,
					amount INT NOT NULL
				);`,
				`INSERT INTO update_self_subquery_items VALUES (1, 10), (2, 20), (3, 30);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE update_self_subquery_items
						SET amount = (
							SELECT max(amount) FROM update_self_subquery_items
						);`,
				},
				{
					Query: `SELECT id, amount
						FROM update_self_subquery_items
						ORDER BY id;`,
					Expected: []sql.Row{
						{1, 30},
						{2, 30},
						{3, 30},
					},
				},
			},
		},
	})
}

// TestOnConflictUpdateAssignmentsUseOriginalRowValuesRepro reproduces a
// PostgreSQL correctness bug: ON CONFLICT DO UPDATE SET expressions should
// read original target-row values unless they explicitly reference EXCLUDED,
// but Doltgres applies earlier assignments from the same SET list.
func TestOnConflictUpdateAssignmentsUseOriginalRowValuesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ON CONFLICT UPDATE assignments read original target row values",
			SetUpScript: []string{
				`CREATE TABLE on_conflict_assignment_items (
					id INT PRIMARY KEY,
					c1 TEXT,
					c2 TEXT
				);`,
				`INSERT INTO on_conflict_assignment_items VALUES (1, 'old-c1', 'old-c2');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO on_conflict_assignment_items VALUES (1, 'excluded-c1', 'excluded-c2')
						ON CONFLICT (id) DO UPDATE SET c1 = 'new-c1', c2 = c1;`,
				},
				{
					Query:    `SELECT c1, c2 FROM on_conflict_assignment_items WHERE id = 1;`,
					Expected: []sql.Row{{"new-c1", "old-c1"}},
				},
			},
		},
	})
}

// TestOnConflictUpdateFunctionArgumentCanReferenceExcludedRepro reproduces a
// PostgreSQL correctness bug: EXCLUDED columns remain visible when used as
// arguments to functions in the DO UPDATE action.
func TestOnConflictUpdateFunctionArgumentCanReferenceExcludedRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ON CONFLICT UPDATE function argument can reference EXCLUDED",
			SetUpScript: []string{
				`CREATE TABLE on_conflict_excluded_function_items (
					id INT PRIMARY KEY,
					label TEXT NOT NULL
				);`,
				`INSERT INTO on_conflict_excluded_function_items VALUES (1, 'old');`,
				`CREATE FUNCTION add_excluded_suffix(label_arg TEXT) RETURNS TEXT AS $$
				BEGIN
					RETURN label_arg || '-fn';
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO on_conflict_excluded_function_items VALUES (1, 'new')
						ON CONFLICT (id) DO UPDATE
						SET label = add_excluded_suffix(EXCLUDED.label);`,
				},
				{
					Query:    `SELECT id, label FROM on_conflict_excluded_function_items;`,
					Expected: []sql.Row{{1, "new-fn"}},
				},
			},
		},
	})
}

// TestOnConflictUpdateMultiAssignmentFromSubqueryRepro reproduces a PostgreSQL
// correctness bug: ON CONFLICT DO UPDATE supports row-valued assignment from a
// scalar subquery.
func TestOnConflictUpdateMultiAssignmentFromSubqueryRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ON CONFLICT UPDATE multi-assignment reads values from subquery",
			SetUpScript: []string{
				`CREATE TABLE on_conflict_multi_assignment_items (
					id INT PRIMARY KEY,
					a INT NOT NULL,
					b INT NOT NULL
				);`,
				`CREATE TABLE on_conflict_multi_assignment_source (
					id INT PRIMARY KEY,
					new_a INT NOT NULL,
					new_b INT NOT NULL
				);`,
				`INSERT INTO on_conflict_multi_assignment_items VALUES (1, 10, 20);`,
				`INSERT INTO on_conflict_multi_assignment_source VALUES (1, 100, 200);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO on_conflict_multi_assignment_items VALUES (1, 30, 40)
						ON CONFLICT (id) DO UPDATE
						SET (a, b) = (
							SELECT s.new_a, s.new_b
							FROM on_conflict_multi_assignment_source AS s
							WHERE s.id = EXCLUDED.id
						);`,
				},
				{
					Query:    `SELECT a, b FROM on_conflict_multi_assignment_items WHERE id = 1;`,
					Expected: []sql.Row{{100, 200}},
				},
			},
		},
	})
}

// TestOnConflictUpdateRejectsDuplicateTargetRowsRepro reproduces a PostgreSQL
// correctness bug: a single INSERT ... ON CONFLICT DO UPDATE statement may not
// update the same target row more than once.
func TestOnConflictUpdateRejectsDuplicateTargetRowsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ON CONFLICT DO UPDATE rejects duplicate target rows",
			SetUpScript: []string{
				`CREATE TABLE on_conflict_duplicate_items (
					id INT PRIMARY KEY,
					v TEXT
				);`,
				`INSERT INTO on_conflict_duplicate_items VALUES (1, 'original');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO on_conflict_duplicate_items VALUES
							(1, 'first'),
							(1, 'second')
						ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v;`,
					ExpectedErr: `ON CONFLICT DO UPDATE command cannot affect row a second time`,
				},
				{
					Query:    `SELECT v FROM on_conflict_duplicate_items WHERE id = 1;`,
					Expected: []sql.Row{{"original"}},
				},
			},
		},
	})
}

// TestOnConflictUpdateEnforcesCheckConstraintsGuard guards that DO UPDATE
// assignments still validate ordinary table CHECK constraints.
func TestOnConflictUpdateEnforcesCheckConstraintsGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ON CONFLICT DO UPDATE enforces CHECK constraints",
			SetUpScript: []string{
				`CREATE TABLE on_conflict_check_items (
					id INT PRIMARY KEY,
					amount INT CONSTRAINT on_conflict_amount_positive CHECK (amount > 0)
				);`,
				`INSERT INTO on_conflict_check_items VALUES (1, 1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO on_conflict_check_items VALUES (1, 2)
						ON CONFLICT (id) DO UPDATE SET amount = -1;`,
					ExpectedErr: `on_conflict_amount_positive`,
				},
				{
					Query:    `SELECT id, amount FROM on_conflict_check_items;`,
					Expected: []sql.Row{{1, 1}},
				},
			},
		},
	})
}

// TestUpdateFromReturningSourceColumnsGuard guards PostgreSQL's UPDATE FROM
// RETURNING semantics: the RETURNING list may reference joined source tables.
func TestUpdateFromReturningSourceColumnsGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "UPDATE FROM RETURNING can reference source columns",
			SetUpScript: []string{
				`CREATE TABLE update_returning_departments (
					id INT PRIMARY KEY,
					bonus INT NOT NULL
				);`,
				`CREATE TABLE update_returning_employees (
					id INT PRIMARY KEY,
					department_id INT REFERENCES update_returning_departments(id),
					salary INT NOT NULL
				);`,
				`INSERT INTO update_returning_departments VALUES (1, 1000), (2, 500);`,
				`INSERT INTO update_returning_employees VALUES (1, 1, 50000), (2, 2, 45000);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE update_returning_employees AS e
						SET salary = salary + d.bonus
						FROM update_returning_departments AS d
						WHERE e.department_id = d.id
						RETURNING e.id, e.salary, d.bonus;`,
					Expected: []sql.Row{
						{1, 51000, 1000},
						{2, 45500, 500},
					},
				},
			},
		},
	})
}

// TestOnConflictReturningCannotReferenceExcludedGuard guards PostgreSQL's
// namespace boundary: EXCLUDED is available to the DO UPDATE action, but not
// to the statement RETURNING list.
func TestOnConflictReturningCannotReferenceExcludedGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ON CONFLICT RETURNING cannot reference excluded",
			SetUpScript: []string{
				`CREATE TABLE on_conflict_returning_excluded_items (
					id INT PRIMARY KEY,
					v TEXT NOT NULL
				);`,
				`INSERT INTO on_conflict_returning_excluded_items VALUES (1, 'old');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO on_conflict_returning_excluded_items VALUES (1, 'new')
						ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v
						RETURNING EXCLUDED.v;`,
					ExpectedErr: `excluded`,
				},
			},
		},
	})
}
