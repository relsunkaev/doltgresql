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
	"strings"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
)

// TestGeneratedColumnRejectsVolatileFunctionsRepro reproduces a generated
// column correctness bug: Doltgres accepts a volatile expression for a stored
// generated column.
func TestGeneratedColumnRejectsVolatileFunctionsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "generated columns reject volatile expressions",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE generated_volatile_items (
						id INT PRIMARY KEY,
						random_value DOUBLE PRECISION GENERATED ALWAYS AS (random()) STORED
					);`,
					ExpectedErr: `generation expression is not immutable`,
				},
			},
		},
	})
}

// TestGeneratedColumnRejectsSelfReferenceRepro reproduces a generated-column
// correctness bug: Doltgres accepts a stored generated column that references
// itself.
func TestGeneratedColumnRejectsSelfReferenceRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "generated columns reject self references",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE generated_self_ref_items (
						id INT PRIMARY KEY,
						doubled INT GENERATED ALWAYS AS (doubled * 2) STORED
					);`,
					ExpectedErr: `cannot use generated column "doubled" in column generation expression`,
				},
			},
		},
	})
}

// TestGeneratedColumnRejectsGeneratedColumnReferenceRepro reproduces a
// generated-column correctness bug: Doltgres accepts a stored generated column
// that references another generated column.
func TestGeneratedColumnRejectsGeneratedColumnReferenceRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "generated columns reject references to generated columns",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE generated_generated_ref_items (
						id INT PRIMARY KEY,
						base_value INT,
						doubled INT GENERATED ALWAYS AS (base_value * 2) STORED,
						quadrupled INT GENERATED ALWAYS AS (doubled * 2) STORED
					);`,
					ExpectedErr: `cannot use generated column "doubled" in column generation expression`,
				},
			},
		},
	})
}

// TestGeneratedColumnRejectsMalformedReferencesRepro reproduces a
// generated-column correctness bug: Doltgres accepts a duplicate generation
// clause that PostgreSQL rejects before persisting the table.
func TestGeneratedColumnRejectsMalformedReferencesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "generated columns reject duplicate generation clauses",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE generated_duplicate_clause_items (
						id INT PRIMARY KEY,
						doubled INT GENERATED ALWAYS AS (id * 2) STORED GENERATED ALWAYS AS (id * 3) STORED
					);`,
					ExpectedErr: `generation`,
				},
			},
		},
		{
			Name: "generated columns reject missing column references",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE generated_missing_ref_items (
						id INT PRIMARY KEY,
						doubled INT GENERATED ALWAYS AS (missing_value * 2) STORED
					);`,
					ExpectedErr: `missing_value`,
				},
			},
		},
		{
			Name: "generated columns reject whole-row references",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE generated_whole_row_items (
						id INT PRIMARY KEY,
						null_count INT GENERATED ALWAYS AS (num_nulls(generated_whole_row_items)) STORED
					);`,
					ExpectedErr: `ERROR`,
				},
			},
		},
	})
}

// TestGeneratedColumnRejectsAggregateExpressionsRepro reproduces a
// generated-column correctness bug: Doltgres accepts an aggregate expression
// for a stored generated column.
func TestGeneratedColumnRejectsAggregateExpressionsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "generated columns reject aggregate expressions",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE generated_aggregate_items (
						id INT PRIMARY KEY,
						aggregate_value INT GENERATED ALWAYS AS (avg(id)) STORED
					);`,
					ExpectedErr: `aggregate functions are not allowed in column generation expressions`,
				},
			},
		},
	})
}

// TestGeneratedColumnRejectsWindowExpressionsRepro reproduces a
// generated-column correctness bug: Doltgres accepts a window expression for a
// stored generated column.
func TestGeneratedColumnRejectsWindowExpressionsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "generated columns reject window expressions",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE generated_window_items (
						id INT PRIMARY KEY,
						ranked INT GENERATED ALWAYS AS (row_number() OVER (ORDER BY id)) STORED
					);`,
					ExpectedErr: `window functions are not allowed in column generation expressions`,
				},
			},
		},
	})
}

// TestGeneratedColumnRejectsSetReturningExpressionsRepro reproduces a
// generated-column correctness bug: Doltgres accepts a set-returning expression
// for a stored generated column.
func TestGeneratedColumnRejectsSetReturningExpressionsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "generated columns reject set-returning expressions",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE generated_srf_items (
						id INT PRIMARY KEY,
						from_series INT GENERATED ALWAYS AS (generate_series(1, id)) STORED
					);`,
					ExpectedErr: `set-returning functions are not allowed in column generation expressions`,
				},
			},
		},
	})
}

// TestGeneratedColumnRejectsSubqueryExpressionsRepro reproduces a generated
// column correctness bug: PostgreSQL rejects subqueries in stored generated
// column expressions.
func TestGeneratedColumnRejectsSubqueryExpressionsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "generated columns reject subquery expressions",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE generated_subquery_items (
						id INT PRIMARY KEY,
						from_subquery INT GENERATED ALWAYS AS ((SELECT id)) STORED
					);`,
					ExpectedErr: `subqueries`,
				},
			},
		},
	})
}

// TestGeneratedColumnRejectsSystemColumnReferencesRepro reproduces a generated
// column correctness bug: system columns other than tableoid cannot be used in
// stored generated column expressions.
func TestGeneratedColumnRejectsSystemColumnReferencesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "generated columns reject unsupported system columns",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE generated_system_column_items (
						id INT PRIMARY KEY,
						xid_seen BOOL GENERATED ALWAYS AS (xmin <> 37) STORED
					);`,
					ExpectedErr: `xmin`,
				},
			},
		},
	})
}

// TestGeneratedColumnAllowsTableoidReferenceRepro reproduces a generated-column
// correctness bug: PostgreSQL permits tableoid in stored generated expressions
// even though other system columns are rejected.
func TestGeneratedColumnAllowsTableoidReferenceRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "generated columns allow tableoid references",
			SetUpScript: []string{
				`CREATE TABLE generated_tableoid_items (
					id INT PRIMARY KEY,
					is_self BOOL GENERATED ALWAYS AS (tableoid = 'generated_tableoid_items'::regclass) STORED
				);`,
				`INSERT INTO generated_tableoid_items (id) VALUES (1);`,
				`ALTER TABLE generated_tableoid_items
					ADD COLUMN rel REGCLASS GENERATED ALWAYS AS (tableoid) STORED;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, is_self, rel::text
						FROM generated_tableoid_items;`,
					Expected: []sql.Row{{1, true, "generated_tableoid_items"}},
				},
			},
		},
	})
}

// TestGeneratedColumnRejectsConflictingGenerationClausesRepro reproduces
// schema correctness bugs: stored generated columns cannot also declare a
// regular default or identity generation, and generated expressions must use
// GENERATED ALWAYS rather than GENERATED BY DEFAULT.
func TestGeneratedColumnRejectsConflictingGenerationClausesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "generated columns reject regular defaults",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE generated_conflicting_default_items (
						id INT PRIMARY KEY,
						doubled INT DEFAULT 5 GENERATED ALWAYS AS (id * 2) STORED
					);`,
					ExpectedErr: `ERROR`,
				},
			},
		},
		{
			Name: "generated columns reject identity generation",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE generated_conflicting_identity_items (
						id INT PRIMARY KEY,
						doubled INT GENERATED ALWAYS AS IDENTITY GENERATED ALWAYS AS (id * 2) STORED
					);`,
					ExpectedErr: `both identity and generation expression specified`,
				},
			},
		},
		{
			Name: "stored generated columns require GENERATED ALWAYS",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE generated_by_default_expression_items (
						id INT PRIMARY KEY,
						doubled INT GENERATED BY DEFAULT AS (id * 2) STORED
					);`,
					ExpectedErr: `ERROR`,
				},
			},
		},
	})
}

// TestGeneratedColumnRejectsExplicitInsertGuard guards that callers cannot
// provide stored generated-column values directly.
func TestGeneratedColumnRejectsExplicitInsertGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "generated columns reject explicit insert values",
			SetUpScript: []string{
				`CREATE TABLE generated_insert_items (
					id INT PRIMARY KEY,
					base_value INT,
					doubled INT GENERATED ALWAYS AS (base_value * 2) STORED
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO generated_insert_items (id, base_value, doubled)
						VALUES (1, 5, 999);`,
					ExpectedErr: `generated column`,
				},
			},
		},
	})
}

// TestGeneratedColumnRejectsExplicitUpdateGuard guards that callers cannot
// update stored generated-column values directly.
func TestGeneratedColumnRejectsExplicitUpdateGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "generated columns reject explicit update values",
			SetUpScript: []string{
				`CREATE TABLE generated_update_items (
					id INT PRIMARY KEY,
					base_value INT,
					doubled INT GENERATED ALWAYS AS (base_value * 2) STORED
				);`,
				`INSERT INTO generated_update_items (id, base_value) VALUES (1, 5);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `UPDATE generated_update_items SET doubled = 999 WHERE id = 1;`,
					ExpectedErr: `generated column`,
				},
			},
		},
	})
}

// TestCopyFromGeneratedColumnErrorKeepsSessionUsableRepro reproduces a COPY
// error-handling correctness bug: rejecting an explicit stored generated-column
// value should not poison the session.
func TestCopyFromGeneratedColumnErrorKeepsSessionUsableRepro(t *testing.T) {
	ctx, connection, controller := CreateServer(t, "postgres")
	defer func() {
		connection.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	_, err := connection.Exec(ctx, `CREATE TABLE generated_copy_items (
		id INT PRIMARY KEY,
		base_value INT,
		doubled INT GENERATED ALWAYS AS (base_value * 2) STORED
	);`)
	require.NoError(t, err)

	tag, err := connection.Default.PgConn().CopyFrom(
		ctx,
		strings.NewReader("1\t5\t999\n"),
		`COPY generated_copy_items (id, base_value, doubled) FROM STDIN;`,
	)
	require.Error(t, err, "COPY FROM should reject explicit generated-column values; tag=%s", tag.String())

	var count int64
	require.NoError(t, connection.Default.QueryRow(
		ctx,
		`SELECT count(*) FROM generated_copy_items;`,
	).Scan(&count))
	require.Equal(t, int64(0), count)
}

// TestGeneratedColumnAcceptsDefaultKeywordGuard guards that callers can use the
// PostgreSQL-supported DEFAULT keyword for stored generated columns.
func TestGeneratedColumnAcceptsDefaultKeywordGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "generated columns accept DEFAULT keyword",
			SetUpScript: []string{
				`CREATE TABLE generated_default_items (
					id INT PRIMARY KEY,
					base_value INT,
					doubled INT GENERATED ALWAYS AS (base_value * 2) STORED
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO generated_default_items (id, base_value, doubled)
						VALUES (1, 5, DEFAULT);`,
				},
				{
					Query: `UPDATE generated_default_items
						SET base_value = 8, doubled = DEFAULT
						WHERE id = 1;`,
				},
				{
					Query:    `SELECT base_value, doubled FROM generated_default_items;`,
					Expected: []sql.Row{{8, 16}},
				},
			},
		},
	})
}

// TestGeneratedColumnAddedToExistingRowsBackfillsValuesGuard guards that adding
// a stored generated column backfills existing rows.
func TestGeneratedColumnAddedToExistingRowsBackfillsValuesGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TABLE ADD generated column backfills existing rows",
			SetUpScript: []string{
				`CREATE TABLE generated_add_items (
					id INT PRIMARY KEY,
					base_value INT
				);`,
				`INSERT INTO generated_add_items VALUES (1, 5), (2, 7);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE generated_add_items
						ADD COLUMN doubled INT GENERATED ALWAYS AS (base_value * 2) STORED;`,
				},
				{
					Query: `SELECT id, base_value, doubled
						FROM generated_add_items
						ORDER BY id;`,
					Expected: []sql.Row{
						{1, 5, 10},
						{2, 7, 14},
					},
				},
			},
		},
	})
}

// TestGeneratedColumnCheckConstraintUsesGeneratedValueGuard verifies generated
// column values participate in ordinary CHECK constraints.
func TestGeneratedColumnCheckConstraintUsesGeneratedValueGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CHECK constraints validate generated column values",
			SetUpScript: []string{
				`CREATE TABLE generated_check_items (
					id INT PRIMARY KEY,
					base_value INT,
					doubled INT GENERATED ALWAYS AS (base_value * 2) STORED CHECK (doubled < 50)
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `INSERT INTO generated_check_items (id, base_value) VALUES (1, 30);`,
					ExpectedErr: `Check constraint`,
				},
			},
		},
	})
}

// TestGeneratedColumnUniqueConstraintUsesGeneratedValueGuard verifies generated
// column values participate in ordinary UNIQUE constraints.
func TestGeneratedColumnUniqueConstraintUsesGeneratedValueGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "UNIQUE constraints validate generated column values",
			SetUpScript: []string{
				`CREATE TABLE generated_unique_items (
					id INT PRIMARY KEY,
					base_value INT,
					doubled INT GENERATED ALWAYS AS (base_value * 2) STORED UNIQUE
				);`,
				`INSERT INTO generated_unique_items (id, base_value) VALUES (1, 5);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `INSERT INTO generated_unique_items (id, base_value) VALUES (2, 5);`,
					ExpectedErr: `duplicate unique key`,
				},
			},
		},
	})
}

// TestGeneratedColumnUniqueConstraintUsesUpdatedGeneratedValueGuard guards that
// generated column values participate in UNIQUE constraints after base-column
// updates.
func TestGeneratedColumnUniqueConstraintUsesUpdatedGeneratedValueGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "UNIQUE constraints validate updated generated column values",
			SetUpScript: []string{
				`CREATE TABLE generated_unique_update_items (
					id INT PRIMARY KEY,
					base_value INT,
					doubled INT GENERATED ALWAYS AS (base_value * 2) STORED UNIQUE
				);`,
				`INSERT INTO generated_unique_update_items (id, base_value)
					VALUES (1, 5), (2, 7);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `UPDATE generated_unique_update_items SET base_value = 5 WHERE id = 2;`,
					ExpectedErr: `duplicate unique key`,
				},
				{
					Query: `SELECT id, base_value, doubled
						FROM generated_unique_update_items
						ORDER BY id;`,
					Expected: []sql.Row{{1, 5, 10}, {2, 7, 14}},
				},
			},
		},
	})
}

// TestGeneratedColumnCheckConstraintUsesUpdatedGeneratedValueGuard guards that
// generated column values participate in CHECK constraints after base-column
// updates.
func TestGeneratedColumnCheckConstraintUsesUpdatedGeneratedValueGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CHECK constraints validate updated generated column values",
			SetUpScript: []string{
				`CREATE TABLE generated_check_update_items (
					id INT PRIMARY KEY,
					base_value INT,
					doubled INT GENERATED ALWAYS AS (base_value * 2) STORED CHECK (doubled < 50)
				);`,
				`INSERT INTO generated_check_update_items (id, base_value) VALUES (1, 10);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `UPDATE generated_check_update_items SET base_value = 30 WHERE id = 1;`,
					ExpectedErr: `Check constraint`,
				},
				{
					Query:    `SELECT id, base_value, doubled FROM generated_check_update_items;`,
					Expected: []sql.Row{{1, 10, 20}},
				},
			},
		},
	})
}

// TestGeneratedColumnNotNullConstraintUsesGeneratedValueGuard guards that
// generated column values participate in ordinary NOT NULL constraints.
func TestGeneratedColumnNotNullConstraintUsesGeneratedValueGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "NOT NULL constraints validate generated column values",
			SetUpScript: []string{
				`CREATE TABLE generated_not_null_items (
					id INT PRIMARY KEY,
					base_value INT,
					nonzero INT GENERATED ALWAYS AS (nullif(base_value, 0)) STORED NOT NULL
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO generated_not_null_items (id, base_value) VALUES (1, 5);`,
				},
				{
					Query:       `INSERT INTO generated_not_null_items (id, base_value) VALUES (2, 0);`,
					ExpectedErr: `non-nullable`,
				},
				{
					Query:    `SELECT id, base_value, nonzero FROM generated_not_null_items;`,
					Expected: []sql.Row{{1, 5, 5}},
				},
			},
		},
	})
}

// TestAlterGeneratedColumnSetNotNullEnforcesGeneratedValueRepro guards that
// ALTER COLUMN ... SET NOT NULL applies to generated column values.
func TestAlterGeneratedColumnSetNotNullEnforcesGeneratedValueRepro(t *testing.T) {
	ctx, conn, controller := CreateServer(t, "postgres")
	t.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	for _, stmt := range []string{
		`CREATE TABLE generated_alter_not_null_items (
			id INT PRIMARY KEY,
			base_value INT,
			nonzero INT GENERATED ALWAYS AS (nullif(base_value, 0)) STORED
		);`,
		`ALTER TABLE generated_alter_not_null_items ALTER COLUMN nonzero SET NOT NULL;`,
	} {
		_, err := conn.Current.Exec(ctx, stmt)
		require.NoError(t, err, stmt)
	}

	_, err := conn.Current.Exec(ctx, `INSERT INTO generated_alter_not_null_items (id, base_value) VALUES (1, 5);`)
	require.NoError(t, err)

	_, err = conn.Current.Exec(ctx, `INSERT INTO generated_alter_not_null_items (id, base_value) VALUES (2, 0);`)
	require.ErrorContains(t, err, `non-nullable`)

	rows, err := conn.Current.Query(ctx, `SELECT id, base_value, nonzero
		FROM generated_alter_not_null_items
		ORDER BY id;`)
	require.NoError(t, err)
	actual, _, err := ReadRows(rows, true)
	require.NoError(t, err)
	require.Equal(t, []sql.Row{{int64(1), int64(5), int64(5)}}, actual)
}

// TestAlterGeneratedColumnDropNotNullPreservesGeneratedValueRepro guards that
// ALTER COLUMN ... DROP NOT NULL preserves generated column expressions.
func TestAlterGeneratedColumnDropNotNullPreservesGeneratedValueRepro(t *testing.T) {
	ctx, conn, controller := CreateServer(t, "postgres")
	t.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	for _, stmt := range []string{
		`CREATE TABLE generated_drop_not_null_items (
			id INT PRIMARY KEY,
			base_value INT,
			nonzero INT GENERATED ALWAYS AS (nullif(base_value, 0)) STORED NOT NULL
		);`,
		`ALTER TABLE generated_drop_not_null_items ALTER COLUMN nonzero DROP NOT NULL;`,
	} {
		_, err := conn.Current.Exec(ctx, stmt)
		require.NoError(t, err, stmt)
	}

	_, err := conn.Current.Exec(ctx, `INSERT INTO generated_drop_not_null_items (id, base_value) VALUES (1, 5), (2, 0);`)
	require.NoError(t, err)

	rows, err := conn.Current.Query(ctx, `SELECT id, base_value, nonzero
		FROM generated_drop_not_null_items
		ORDER BY id;`)
	require.NoError(t, err)
	actual, _, err := ReadRows(rows, true)
	require.NoError(t, err)
	require.Equal(t, []sql.Row{{int64(1), int64(5), int64(5)}, {int64(2), int64(0), nil}}, actual)
}

// TestGeneratedColumnRecomputesAfterBaseUpdateGuard guards that stored
// generated columns are recalculated when their base columns change.
func TestGeneratedColumnRecomputesAfterBaseUpdateGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "generated column recomputes after base column update",
			SetUpScript: []string{
				`CREATE TABLE generated_update_recompute_items (
					id INT PRIMARY KEY,
					base_value INT,
					doubled INT GENERATED ALWAYS AS (base_value * 2) STORED
				);`,
				`INSERT INTO generated_update_recompute_items (id, base_value) VALUES (1, 3);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE generated_update_recompute_items SET base_value = 5 WHERE id = 1;`,
				},
				{
					Query:    `SELECT base_value, doubled FROM generated_update_recompute_items;`,
					Expected: []sql.Row{{5, 10}},
				},
			},
		},
	})
}

// TestGeneratedColumnRecomputesAfterUpdateFromGuard guards that stored
// generated columns are recalculated when UPDATE ... FROM changes their base
// columns through a joined source relation.
func TestGeneratedColumnRecomputesAfterUpdateFromGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "generated column recomputes after UPDATE FROM",
			SetUpScript: []string{
				`CREATE TABLE generated_update_from_recompute_source (
					id INT PRIMARY KEY,
					new_base_value INT
				);`,
				`CREATE TABLE generated_update_from_recompute_items (
					id INT PRIMARY KEY,
					base_value INT,
					doubled INT GENERATED ALWAYS AS (base_value * 2) STORED
				);`,
				`INSERT INTO generated_update_from_recompute_items (id, base_value)
					VALUES (1, 3), (2, 4);`,
				`INSERT INTO generated_update_from_recompute_source VALUES (1, 5), (2, 7);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE generated_update_from_recompute_items AS t
						SET base_value = s.new_base_value
						FROM generated_update_from_recompute_source AS s
						WHERE t.id = s.id;`,
				},
				{
					Query: `SELECT id, base_value, doubled
						FROM generated_update_from_recompute_items
						ORDER BY id;`,
					Expected: []sql.Row{
						{1, 5, 10},
						{2, 7, 14},
					},
				},
			},
		},
	})
}

// TestGeneratedColumnRecomputesAfterOnConflictUpdateGuard guards that stored
// generated columns are recalculated when ON CONFLICT DO UPDATE changes their
// base columns.
func TestGeneratedColumnRecomputesAfterOnConflictUpdateGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "generated column recomputes after ON CONFLICT DO UPDATE",
			SetUpScript: []string{
				`CREATE TABLE generated_on_conflict_recompute_items (
					id INT PRIMARY KEY,
					base_value INT,
					doubled INT GENERATED ALWAYS AS (base_value * 2) STORED
				);`,
				`INSERT INTO generated_on_conflict_recompute_items (id, base_value)
					VALUES (1, 3);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO generated_on_conflict_recompute_items (id, base_value)
						VALUES (1, 5)
						ON CONFLICT (id) DO UPDATE SET base_value = EXCLUDED.base_value;`,
				},
				{
					Query: `SELECT id, base_value, doubled
						FROM generated_on_conflict_recompute_items;`,
					Expected: []sql.Row{{1, 5, 10}},
				},
			},
		},
	})
}

// TestBeforeTriggerGeneratedColumnAssignmentIsIgnoredRepro guards that stored
// generated columns are recomputed after BEFORE triggers run; assigning NEW for
// a generated column inside the trigger must not persist that trigger-supplied
// value.
func TestBeforeTriggerGeneratedColumnAssignmentIsIgnoredRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "BEFORE trigger generated column assignment is ignored",
			SetUpScript: []string{
				`CREATE TABLE generated_trigger_assignment_items (
					id INT PRIMARY KEY,
					base_value INT,
					doubled INT GENERATED ALWAYS AS (base_value * 2) STORED
				);`,
				`INSERT INTO generated_trigger_assignment_items (id, base_value) VALUES (1, 3);`,
				`CREATE FUNCTION rewrite_generated_trigger_assignment() RETURNS TRIGGER AS $$
				BEGIN
					NEW.base_value := 10;
					NEW.doubled := 999;
					RETURN NEW;
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER generated_trigger_assignment_before_update
					BEFORE UPDATE ON generated_trigger_assignment_items
					FOR EACH ROW EXECUTE FUNCTION rewrite_generated_trigger_assignment();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE generated_trigger_assignment_items SET base_value = 7 WHERE id = 1;`,
				},
				{
					Query:    `SELECT base_value, doubled FROM generated_trigger_assignment_items;`,
					Expected: []sql.Row{{10, 20}},
				},
			},
		},
	})
}

// TestBeforeTriggerWhenCannotReferenceNewGeneratedColumnRepro guards
// PostgreSQL's trigger timing rule: a BEFORE trigger's WHEN condition cannot
// reference a NEW generated column because the generated value is computed
// after BEFORE triggers run.
func TestBeforeTriggerWhenCannotReferenceNewGeneratedColumnRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "BEFORE trigger WHEN cannot reference NEW generated column",
			SetUpScript: []string{
				`CREATE TABLE generated_trigger_when_items (
					id INT PRIMARY KEY,
					base_value INT,
					doubled INT GENERATED ALWAYS AS (base_value * 2) STORED
				);`,
				`CREATE FUNCTION noop_generated_trigger_when() RETURNS TRIGGER AS $$
				BEGIN
					RETURN NEW;
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TRIGGER generated_trigger_when_before_insert
						BEFORE INSERT ON generated_trigger_when_items
						FOR EACH ROW
						WHEN (NEW.doubled > 10)
						EXECUTE FUNCTION noop_generated_trigger_when();`,
					ExpectedErr: `generated column`,
				},
			},
		},
	})
}

// TestUpdateOfGeneratedColumnFiresForBaseColumnChangeRepro guards PostgreSQL's
// trigger rule that UPDATE OF a stored generated column fires when one of its
// base columns changes.
func TestUpdateOfGeneratedColumnFiresForBaseColumnChangeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "UPDATE OF generated column fires for base column change",
			SetUpScript: []string{
				`CREATE TABLE generated_update_of_items (
					id INT PRIMARY KEY,
					base_value INT,
					doubled INT GENERATED ALWAYS AS (base_value * 2) STORED
				);`,
				`CREATE TABLE generated_update_of_audit (
					id INT PRIMARY KEY,
					old_doubled INT,
					new_doubled INT
				);`,
				`INSERT INTO generated_update_of_items (id, base_value) VALUES (1, 3);`,
				`CREATE FUNCTION audit_generated_update_of() RETURNS TRIGGER AS $$
				BEGIN
					INSERT INTO generated_update_of_audit VALUES (NEW.id, OLD.doubled, NEW.doubled);
					RETURN NEW;
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER generated_update_of_doubled
					AFTER UPDATE OF doubled ON generated_update_of_items
					FOR EACH ROW EXECUTE FUNCTION audit_generated_update_of();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE generated_update_of_items SET base_value = 4 WHERE id = 1;`,
				},
				{
					Query:    `SELECT id, old_doubled, new_doubled FROM generated_update_of_audit;`,
					Expected: []sql.Row{{1, 6, 8}},
				},
			},
		},
	})
}

// TestRenameColumnUsedByGeneratedColumnKeepsGeneratedColumnUsableGuard guards
// generated-column dependency rewrites after base-column renames.
func TestRenameColumnUsedByGeneratedColumnKeepsGeneratedColumnUsableGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "RENAME COLUMN rewrites generated-column dependencies",
			SetUpScript: []string{
				`CREATE TABLE generated_column_rename_items (
					id INT PRIMARY KEY,
					base_value INT,
					doubled INT GENERATED ALWAYS AS (base_value * 2) STORED
				);`,
				`INSERT INTO generated_column_rename_items (id, base_value) VALUES (1, 5);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE generated_column_rename_items RENAME COLUMN base_value TO amount;`,
				},
				{
					Query: `INSERT INTO generated_column_rename_items (id, amount) VALUES (2, 7);`,
				},
				{
					Query: `SELECT id, amount, doubled
						FROM generated_column_rename_items
						ORDER BY id;`,
					Expected: []sql.Row{{1, 5, 10}, {2, 7, 14}},
				},
			},
		},
	})
}

// TestDropColumnUsedByGeneratedColumnRequiresCascadeRepro reproduces a
// generated-column dependency bug: PostgreSQL rejects dropping a base column
// used by a stored generated column unless CASCADE is requested.
func TestDropColumnUsedByGeneratedColumnRequiresCascadeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP COLUMN rejects generated-column dependencies",
			SetUpScript: []string{
				`CREATE TABLE generated_column_dependency_items (
					id INT PRIMARY KEY,
					base_value INT,
					doubled INT GENERATED ALWAYS AS (base_value * 2) STORED
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `ALTER TABLE generated_column_dependency_items DROP COLUMN base_value;`,
					ExpectedErr: `because other objects depend on it`,
				},
			},
		},
	})
}

// TestGeneratedColumnRejectsOnUpdateCascadeReferenceRepro reproduces a
// generated-column correctness bug: Doltgres accepts ON UPDATE CASCADE on a
// foreign key whose referencing column is generated.
func TestGeneratedColumnRejectsOnUpdateCascadeReferenceRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "generated columns reject ON UPDATE CASCADE references",
			SetUpScript: []string{
				`CREATE TABLE generated_fk_parent (id INT PRIMARY KEY);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE generated_fk_update_child (
						id INT PRIMARY KEY,
						parent_id INT GENERATED ALWAYS AS (id * 2) STORED
							REFERENCES generated_fk_parent(id) ON UPDATE CASCADE
					);`,
					ExpectedErr: `generated column`,
				},
			},
		},
	})
}

// TestGeneratedColumnRejectsOnDeleteSetNullReferenceRepro reproduces a
// generated-column correctness bug: Doltgres accepts ON DELETE SET NULL on a
// foreign key whose referencing column is generated.
func TestGeneratedColumnRejectsOnDeleteSetNullReferenceRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "generated columns reject ON DELETE SET NULL references",
			SetUpScript: []string{
				`CREATE TABLE generated_fk_delete_parent (id INT PRIMARY KEY);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE generated_fk_delete_child (
						id INT PRIMARY KEY,
						parent_id INT GENERATED ALWAYS AS (id * 2) STORED
							REFERENCES generated_fk_delete_parent(id) ON DELETE SET NULL
					);`,
					ExpectedErr: `generated column`,
				},
			},
		},
	})
}
