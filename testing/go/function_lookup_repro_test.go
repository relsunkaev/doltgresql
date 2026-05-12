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

// TestAlterFunctionRenameRepro reproduces a routine DDL correctness bug:
// PostgreSQL supports ALTER FUNCTION ... RENAME TO and resolves the function
// under its new name afterward.
func TestAlterFunctionRenameRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER FUNCTION RENAME TO updates function lookup",
			SetUpScript: []string{
				`CREATE FUNCTION rename_function_old()
					RETURNS INT LANGUAGE SQL AS $$ SELECT 7 $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER FUNCTION rename_function_old()
						RENAME TO rename_function_new;`,
				},
				{
					Query:    `SELECT rename_function_new();`,
					Expected: []sql.Row{{7}},
				},
				{
					Query:       `SELECT rename_function_old();`,
					ExpectedErr: `not found`,
				},
			},
		},
	})
}

// TestAlterRoutineRenameFunctionRepro reproduces a routine DDL correctness bug:
// PostgreSQL supports the generic ALTER ROUTINE ... RENAME TO syntax for
// functions.
func TestAlterRoutineRenameFunctionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER ROUTINE RENAME TO updates function lookup",
			SetUpScript: []string{
				`CREATE FUNCTION rename_routine_old()
					RETURNS INT LANGUAGE SQL AS $$ SELECT 7 $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER ROUTINE rename_routine_old()
						RENAME TO rename_routine_new;`,
				},
				{
					Query:    `SELECT rename_routine_new();`,
					Expected: []sql.Row{{7}},
				},
				{
					Query:       `SELECT rename_routine_old();`,
					ExpectedErr: `not found`,
				},
			},
		},
	})
}

// TestSchemaQualifiedFunctionLookupUsesExplicitSchemaRepro reproduces a
// function lookup correctness bug: schema-qualified calls should use the named
// schema instead of resolving the same-name function from the current schema.
func TestSchemaQualifiedFunctionLookupUsesExplicitSchemaRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "schema-qualified function lookup uses explicit schema",
			SetUpScript: []string{
				`CREATE SCHEMA dg_fn_lookup_a;`,
				`CREATE SCHEMA dg_fn_lookup_b;`,
				`CREATE FUNCTION dg_fn_lookup_a.dg_lookup_probe() RETURNS TEXT AS $$
					SELECT 'a'::text
				$$ LANGUAGE sql;`,
				`CREATE FUNCTION dg_fn_lookup_b.dg_lookup_probe() RETURNS TEXT AS $$
					SELECT 'b'::text
				$$ LANGUAGE sql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SET search_path = dg_fn_lookup_a, public;`,
				},
				{
					Query:    `SELECT dg_lookup_probe();`,
					Expected: []sql.Row{{"a"}},
				},
				{
					Query:    `SELECT dg_fn_lookup_b.dg_lookup_probe();`,
					Expected: []sql.Row{{"b"}},
				},
			},
		},
	})
}

// TestSchemaQualifiedFunctionSideEffectsUseExplicitSchemaRepro reproduces a
// data consistency bug: schema-qualified calls should execute the named
// function's side effects, not a same-name function from the current schema.
func TestSchemaQualifiedFunctionSideEffectsUseExplicitSchemaRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "schema-qualified function side effects use explicit schema",
			SetUpScript: []string{
				`CREATE TABLE dg_fn_effect_audit (
					label TEXT PRIMARY KEY
				);`,
				`CREATE SCHEMA dg_fn_effect_a;`,
				`CREATE SCHEMA dg_fn_effect_b;`,
				`CREATE FUNCTION dg_fn_effect_a.lookup_mutator() RETURNS INT AS $$
				BEGIN
					INSERT INTO dg_fn_effect_audit VALUES ('a');
					RETURN 1;
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE FUNCTION dg_fn_effect_b.lookup_mutator() RETURNS INT AS $$
				BEGIN
					INSERT INTO dg_fn_effect_audit VALUES ('b');
					RETURN 1;
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SET search_path = dg_fn_effect_a, public;`,
				},
				{
					Query:    `SELECT dg_fn_effect_b.lookup_mutator();`,
					Expected: []sql.Row{{1}},
				},
				{
					Query:    `SELECT label FROM dg_fn_effect_audit;`,
					Expected: []sql.Row{{"b"}},
				},
			},
		},
	})
}

// TestSchemaQualifiedDefaultFunctionUsesExplicitSchemaRepro reproduces a data
// corruption bug: stored defaults that explicitly call a schema-qualified
// function should persist the named function's result, not the result from a
// same-name function in the current search path.
func TestSchemaQualifiedDefaultFunctionUsesExplicitSchemaRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "schema-qualified default function uses explicit schema",
			SetUpScript: []string{
				`CREATE SCHEMA dg_default_fn_a;`,
				`CREATE SCHEMA dg_default_fn_b;`,
				`CREATE FUNCTION dg_default_fn_a.lookup_default() RETURNS INT AS $$
					SELECT 1
				$$ LANGUAGE sql;`,
				`CREATE FUNCTION dg_default_fn_b.lookup_default() RETURNS INT AS $$
					SELECT 2
				$$ LANGUAGE sql;`,
				`SET search_path = dg_default_fn_a, public;`,
				`CREATE TABLE dg_default_fn_items (
					id INT PRIMARY KEY,
					value INT DEFAULT dg_default_fn_b.lookup_default()
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO dg_default_fn_items (id) VALUES (1);`,
				},
				{
					Query:    `SELECT value FROM dg_default_fn_items;`,
					Expected: []sql.Row{{2}},
				},
			},
		},
	})
}

// TestUnqualifiedDefaultFunctionBindsAtCreateTimeRepro reproduces a data
// corruption bug: unqualified functions in stored defaults should bind to the
// function visible when the default is created, not re-resolve through the
// insert-time search_path.
func TestUnqualifiedDefaultFunctionBindsAtCreateTimeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "unqualified default function binds at create time",
			SetUpScript: []string{
				`CREATE SCHEMA dg_default_bind_a;`,
				`CREATE SCHEMA dg_default_bind_b;`,
				`CREATE FUNCTION dg_default_bind_a.bind_default() RETURNS INT AS $$
					SELECT 1
				$$ LANGUAGE sql;`,
				`CREATE FUNCTION dg_default_bind_b.bind_default() RETURNS INT AS $$
					SELECT 2
				$$ LANGUAGE sql;`,
				`SET search_path = dg_default_bind_a, public;`,
				`CREATE TABLE dg_default_bind_items (
					id INT PRIMARY KEY,
					value INT DEFAULT bind_default()
				);`,
				`SET search_path = dg_default_bind_b, public;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO dg_default_bind_a.dg_default_bind_items (id) VALUES (1);`,
				},
				{
					Query:    `SELECT value FROM dg_default_bind_a.dg_default_bind_items;`,
					Expected: []sql.Row{{1}},
				},
			},
		},
	})
}

// TestCheckConstraintAllowsSchemaQualifiedFunctionRepro reproduces a correctness
// bug: PostgreSQL accepts user-defined functions in CHECK constraints, including
// schema-qualified calls.
func TestCheckConstraintAllowsSchemaQualifiedFunctionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CHECK constraint allows schema-qualified function",
			SetUpScript: []string{
				`CREATE SCHEMA dg_check_fn_a;`,
				`CREATE SCHEMA dg_check_fn_b;`,
				`CREATE FUNCTION dg_check_fn_a.is_valid(input INT) RETURNS BOOL AS $$
					SELECT true
				$$ LANGUAGE sql;`,
				`CREATE FUNCTION dg_check_fn_b.is_valid(input INT) RETURNS BOOL AS $$
					SELECT false
				$$ LANGUAGE sql;`,
				`SET search_path = dg_check_fn_a, public;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE dg_check_fn_items (
						id INT PRIMARY KEY,
						value INT,
						CHECK (dg_check_fn_b.is_valid(value))
					);`,
				},
			},
		},
	})
}

// TestCheckConstraintAllowsUnqualifiedFunctionRepro reproduces a correctness
// bug: PostgreSQL also allows unqualified user-defined functions in CHECK
// constraints when the function is visible through search_path.
func TestCheckConstraintAllowsUnqualifiedFunctionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CHECK constraint allows unqualified function",
			SetUpScript: []string{
				`CREATE FUNCTION dg_unqualified_check_is_valid(input INT) RETURNS BOOL AS $$
					SELECT false
				$$ LANGUAGE sql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE dg_check_fn_items (
							id INT PRIMARY KEY,
							value INT,
							CHECK (dg_unqualified_check_is_valid(value))
						);`,
				},
			},
		},
	})
}

// TestSchemaQualifiedUniqueExpressionIndexUsesExplicitSchemaRepro reproduces a
// data integrity bug: a unique expression index should compute keys with the
// explicitly schema-qualified function in the index expression.
func TestSchemaQualifiedUniqueExpressionIndexUsesExplicitSchemaRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "schema-qualified unique expression index uses explicit schema",
			SetUpScript: []string{
				`CREATE SCHEMA dg_unique_fn_a;`,
				`CREATE SCHEMA dg_unique_fn_b;`,
				`CREATE FUNCTION dg_unique_fn_a.unique_key(input INT) RETURNS INT
					LANGUAGE SQL IMMUTABLE AS $$ SELECT input $$;`,
				`CREATE FUNCTION dg_unique_fn_b.unique_key(input INT) RETURNS INT
					LANGUAGE SQL IMMUTABLE AS $$ SELECT input % 10 $$;`,
				`SET search_path = dg_unique_fn_a, public;`,
				`CREATE TABLE dg_unique_fn_items (
					id INT PRIMARY KEY,
					value INT
				);`,
				`CREATE UNIQUE INDEX dg_unique_fn_expr_idx
					ON dg_unique_fn_items ((dg_unique_fn_b.unique_key(value)));`,
				`INSERT INTO dg_unique_fn_items VALUES (1, 1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `INSERT INTO dg_unique_fn_items VALUES (2, 11);`,
					ExpectedErr: `duplicate`,
				},
				{
					Query:    `SELECT count(*) FROM dg_unique_fn_items;`,
					Expected: []sql.Row{{int64(1)}},
				},
			},
		},
	})
}

// TestUnqualifiedUniqueExpressionIndexBindsAtCreateTimeRepro reproduces a data
// integrity bug: unqualified functions in expression indexes should bind at
// CREATE INDEX time, not re-resolve through the INSERT-time search_path.
func TestUnqualifiedUniqueExpressionIndexBindsAtCreateTimeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "unqualified unique expression index binds at create time",
			SetUpScript: []string{
				`CREATE SCHEMA dg_unique_bind_a;`,
				`CREATE SCHEMA dg_unique_bind_b;`,
				`CREATE FUNCTION dg_unique_bind_a.unique_bind_key(input INT) RETURNS INT
					LANGUAGE SQL IMMUTABLE AS $$ SELECT input $$;`,
				`CREATE FUNCTION dg_unique_bind_b.unique_bind_key(input INT) RETURNS INT
					LANGUAGE SQL IMMUTABLE AS $$ SELECT input % 10 $$;`,
				`SET search_path = dg_unique_bind_a, public;`,
				`CREATE TABLE dg_unique_bind_items (
					id INT PRIMARY KEY,
					value INT
				);`,
				`CREATE UNIQUE INDEX dg_unique_bind_expr_idx
					ON dg_unique_bind_items ((unique_bind_key(value)));`,
				`INSERT INTO dg_unique_bind_items VALUES (1, 1);`,
				`SET search_path = dg_unique_bind_b, public;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO dg_unique_bind_a.dg_unique_bind_items VALUES (2, 11);`,
				},
				{
					Query:    `SELECT count(*) FROM dg_unique_bind_a.dg_unique_bind_items;`,
					Expected: []sql.Row{{int64(2)}},
				},
			},
		},
	})
}

// TestPartialUniqueIndexPredicateFunctionEvaluatesOnInsertRepro reproduces a
// correctness bug: PostgreSQL evaluates user-defined functions in partial index
// predicates during INSERT.
func TestPartialUniqueIndexPredicateFunctionEvaluatesOnInsertRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "partial unique index predicate function evaluates on insert",
			SetUpScript: []string{
				`CREATE SCHEMA dg_partial_fn_a;`,
				`CREATE SCHEMA dg_partial_fn_b;`,
				`CREATE FUNCTION dg_partial_fn_a.include_row(input BIGINT) RETURNS BOOL
					LANGUAGE SQL IMMUTABLE AS $$ SELECT false $$;`,
				`CREATE FUNCTION dg_partial_fn_b.include_row(input BIGINT) RETURNS BOOL
					LANGUAGE SQL IMMUTABLE AS $$ SELECT true $$;`,
				`SET search_path = dg_partial_fn_a, public;`,
				`CREATE TABLE dg_partial_fn_items (
					id INT,
					value BIGINT
				);`,
				`CREATE UNIQUE INDEX dg_partial_fn_value_idx
					ON dg_partial_fn_items (value)
					WHERE dg_partial_fn_b.include_row(value);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO dg_partial_fn_items VALUES (1, 7);`,
				},
			},
		},
	})
}

// TestUnqualifiedTriggerFunctionBindsAtCreateTimeRepro guards that
// unqualified trigger functions bind when the trigger is created, not
// re-resolve through the DML-time search_path.
func TestUnqualifiedTriggerFunctionBindsAtCreateTimeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "unqualified trigger function binds at create time",
			SetUpScript: []string{
				`CREATE SCHEMA dg_trigger_bind_a;`,
				`CREATE SCHEMA dg_trigger_bind_b;`,
				`CREATE TABLE dg_trigger_bind_target (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`CREATE TABLE dg_trigger_bind_log (
					label TEXT PRIMARY KEY
				);`,
				`CREATE FUNCTION dg_trigger_bind_a.trigger_bind_func() RETURNS TRIGGER AS $$
				BEGIN
					INSERT INTO public.dg_trigger_bind_log VALUES ('a');
					RETURN NEW;
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE FUNCTION dg_trigger_bind_b.trigger_bind_func() RETURNS TRIGGER AS $$
				BEGIN
					INSERT INTO public.dg_trigger_bind_log VALUES ('b');
					RETURN NEW;
				END;
				$$ LANGUAGE plpgsql;`,
				`SET search_path = dg_trigger_bind_a, public;`,
				`CREATE TRIGGER dg_trigger_bind_before_insert
					BEFORE INSERT ON public.dg_trigger_bind_target
					FOR EACH ROW EXECUTE FUNCTION trigger_bind_func();`,
				`SET search_path = dg_trigger_bind_b, public;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO public.dg_trigger_bind_target VALUES (1, 'row');`,
				},
				{
					Query:    `SELECT label FROM public.dg_trigger_bind_log;`,
					Expected: []sql.Row{{"a"}},
				},
			},
		},
	})
}

// TestSchemaQualifiedGeneratedColumnFunctionUsesExplicitSchemaRepro reproduces
// a data corruption bug: a stored generated column should persist the result of
// the explicitly schema-qualified function in its expression.
func TestSchemaQualifiedGeneratedColumnFunctionUsesExplicitSchemaRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "schema-qualified generated column function uses explicit schema",
			SetUpScript: []string{
				`CREATE SCHEMA dg_generated_fn_a;`,
				`CREATE SCHEMA dg_generated_fn_b;`,
				`CREATE FUNCTION dg_generated_fn_a.generated_value(input INT) RETURNS INT
					LANGUAGE SQL IMMUTABLE AS $$ SELECT input + 100 $$;`,
				`CREATE FUNCTION dg_generated_fn_b.generated_value(input INT) RETURNS INT
					LANGUAGE SQL IMMUTABLE AS $$ SELECT input + 200 $$;`,
				`SET search_path = dg_generated_fn_a, public;`,
				`CREATE TABLE dg_generated_fn_items (
					id INT PRIMARY KEY,
					value INT,
					derived INT GENERATED ALWAYS AS (dg_generated_fn_b.generated_value(value)) STORED
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO dg_generated_fn_items (id, value) VALUES (1, 5);`,
				},
				{
					Query:    `SELECT derived FROM dg_generated_fn_items;`,
					Expected: []sql.Row{{205}},
				},
			},
		},
	})
}

// TestUnqualifiedGeneratedColumnFunctionBindsAtCreateTimeRepro reproduces a
// data corruption bug: unqualified functions in stored generated columns should
// bind to the function visible when the table is created, not re-resolve
// through the insert-time search_path.
func TestUnqualifiedGeneratedColumnFunctionBindsAtCreateTimeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "unqualified generated column function binds at create time",
			SetUpScript: []string{
				`CREATE SCHEMA dg_generated_bind_a;`,
				`CREATE SCHEMA dg_generated_bind_b;`,
				`CREATE FUNCTION dg_generated_bind_a.generated_bind_value(input INT) RETURNS INT
					LANGUAGE SQL IMMUTABLE AS $$ SELECT input + 100 $$;`,
				`CREATE FUNCTION dg_generated_bind_b.generated_bind_value(input INT) RETURNS INT
					LANGUAGE SQL IMMUTABLE AS $$ SELECT input + 200 $$;`,
				`SET search_path = dg_generated_bind_a, public;`,
				`CREATE TABLE dg_generated_bind_items (
					id INT PRIMARY KEY,
					value INT,
					derived INT GENERATED ALWAYS AS (generated_bind_value(value)) STORED
				);`,
				`SET search_path = dg_generated_bind_b, public;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO dg_generated_bind_a.dg_generated_bind_items (id, value)
						VALUES (1, 5);`,
				},
				{
					Query:    `SELECT derived FROM dg_generated_bind_a.dg_generated_bind_items;`,
					Expected: []sql.Row{{105}},
				},
			},
		},
	})
}

// TestSchemaQualifiedViewFunctionUsesExplicitSchemaRepro reproduces a
// correctness bug: view expressions should execute schema-qualified functions
// from the schema named in the view definition.
func TestSchemaQualifiedViewFunctionUsesExplicitSchemaRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "schema-qualified view function uses explicit schema",
			SetUpScript: []string{
				`CREATE SCHEMA dg_view_fn_a;`,
				`CREATE SCHEMA dg_view_fn_b;`,
				`CREATE FUNCTION dg_view_fn_a.view_value(input INT) RETURNS INT AS $$
					SELECT input + 100
				$$ LANGUAGE sql;`,
				`CREATE FUNCTION dg_view_fn_b.view_value(input INT) RETURNS INT AS $$
					SELECT input + 200
				$$ LANGUAGE sql;`,
				`CREATE TABLE dg_view_fn_items (
					id INT PRIMARY KEY,
					value INT
				);`,
				`INSERT INTO dg_view_fn_items VALUES (1, 5);`,
				`SET search_path = dg_view_fn_a, public;`,
				`CREATE VIEW dg_view_fn_view AS
					SELECT dg_view_fn_b.view_value(value) AS resolved
					FROM dg_view_fn_items;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT resolved FROM dg_view_fn_view;`,
					Expected: []sql.Row{{205}},
				},
			},
		},
	})
}

// TestUnqualifiedViewFunctionBindsAtCreateTimeRepro reproduces a view
// correctness bug: unqualified functions in stored view definitions should bind
// to the function visible when the view is created, not re-resolve through the
// query-time search_path.
func TestUnqualifiedViewFunctionBindsAtCreateTimeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "unqualified view function binds at create time",
			SetUpScript: []string{
				`CREATE SCHEMA dg_view_bind_a;`,
				`CREATE SCHEMA dg_view_bind_b;`,
				`CREATE FUNCTION dg_view_bind_a.view_bind_value(input INT) RETURNS INT AS $$
					SELECT input + 100
				$$ LANGUAGE sql;`,
				`CREATE FUNCTION dg_view_bind_b.view_bind_value(input INT) RETURNS INT AS $$
					SELECT input + 200
				$$ LANGUAGE sql;`,
				`CREATE TABLE dg_view_bind_items (
					id INT PRIMARY KEY,
					value INT
				);`,
				`INSERT INTO dg_view_bind_items VALUES (1, 5);`,
				`SET search_path = dg_view_bind_a, public;`,
				`CREATE VIEW dg_view_bind_a.bind_view AS
					SELECT view_bind_value(value) AS resolved
					FROM public.dg_view_bind_items;`,
				`SET search_path = dg_view_bind_b, public;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT resolved FROM dg_view_bind_a.bind_view;`,
					Expected: []sql.Row{{105}},
				},
			},
		},
	})
}

// TestSchemaQualifiedMaterializedViewFunctionUsesExplicitSchemaRepro reproduces
// a persistence bug: materialized view data should be computed with the
// explicitly schema-qualified function in the view query.
func TestSchemaQualifiedMaterializedViewFunctionUsesExplicitSchemaRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "schema-qualified materialized view function uses explicit schema",
			SetUpScript: []string{
				`CREATE SCHEMA dg_matview_fn_a;`,
				`CREATE SCHEMA dg_matview_fn_b;`,
				`CREATE FUNCTION dg_matview_fn_a.mat_value(input INT) RETURNS INT AS $$
					SELECT input + 100
				$$ LANGUAGE sql;`,
				`CREATE FUNCTION dg_matview_fn_b.mat_value(input INT) RETURNS INT AS $$
					SELECT input + 200
				$$ LANGUAGE sql;`,
				`CREATE TABLE dg_matview_fn_items (
					id INT PRIMARY KEY,
					value INT
				);`,
				`INSERT INTO dg_matview_fn_items VALUES (1, 5);`,
				`SET search_path = dg_matview_fn_a, public;`,
				`CREATE MATERIALIZED VIEW dg_matview_fn_view AS
					SELECT dg_matview_fn_b.mat_value(value) AS resolved
					FROM dg_matview_fn_items;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT resolved FROM dg_matview_fn_view;`,
					Expected: []sql.Row{{205}},
				},
			},
		},
	})
}

// TestUnqualifiedMaterializedViewFunctionBindsAtCreateTimeRepro reproduces a
// materialized-view persistence bug: REFRESH MATERIALIZED VIEW should reuse the
// function binding captured by the materialized-view definition, not re-resolve
// unqualified functions through the refresh-time search_path.
func TestUnqualifiedMaterializedViewFunctionBindsAtCreateTimeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "unqualified materialized view function binds at create time",
			SetUpScript: []string{
				`CREATE SCHEMA dg_matview_bind_a;`,
				`CREATE SCHEMA dg_matview_bind_b;`,
				`CREATE FUNCTION dg_matview_bind_a.mat_bind_value(input INT) RETURNS INT AS $$
					SELECT input + 100
				$$ LANGUAGE sql;`,
				`CREATE FUNCTION dg_matview_bind_b.mat_bind_value(input INT) RETURNS INT AS $$
					SELECT input + 200
				$$ LANGUAGE sql;`,
				`CREATE TABLE dg_matview_bind_items (
					id INT PRIMARY KEY,
					value INT
				);`,
				`INSERT INTO dg_matview_bind_items VALUES (1, 5);`,
				`SET search_path = dg_matview_bind_a, public;`,
				`CREATE MATERIALIZED VIEW dg_matview_bind_a.bind_matview AS
					SELECT mat_bind_value(value) AS resolved
					FROM public.dg_matview_bind_items;`,
				`UPDATE public.dg_matview_bind_items SET value = 6 WHERE id = 1;`,
				`SET search_path = dg_matview_bind_b, public;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `REFRESH MATERIALIZED VIEW dg_matview_bind_a.bind_matview;`,
				},
				{
					Query:    `SELECT resolved FROM dg_matview_bind_a.bind_matview;`,
					Expected: []sql.Row{{106}},
				},
			},
		},
	})
}

// TestFunctionSetSearchPathOptionAppliesDuringExecutionRepro reproduces a
// function execution correctness bug: a function-level SET search_path option
// should apply while the function body runs, regardless of the caller's
// search_path.
func TestFunctionSetSearchPathOptionAppliesDuringExecutionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "function SET search_path controls unqualified lookup",
			SetUpScript: []string{
				`CREATE SCHEMA dg_fn_set_safe;`,
				`CREATE SCHEMA dg_fn_set_attacker;`,
				`CREATE TABLE dg_fn_set_safe.lookup_items (
					id INT PRIMARY KEY
				);`,
				`CREATE TABLE dg_fn_set_attacker.lookup_items (
					id INT PRIMARY KEY
				);`,
				`INSERT INTO dg_fn_set_safe.lookup_items VALUES (1);`,
				`INSERT INTO dg_fn_set_attacker.lookup_items VALUES (10), (11);`,
				`SET search_path = dg_fn_set_attacker, public;`,
				`CREATE FUNCTION function_set_path_count()
				RETURNS INT
				LANGUAGE SQL
				SET search_path = dg_fn_set_safe, public
				AS $$ SELECT count(*)::INT FROM lookup_items $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SET search_path = dg_fn_set_attacker, public;`,
				},
				{
					Query:    `SELECT function_set_path_count();`,
					Expected: []sql.Row{{1}},
				},
				{
					Query:    `SELECT current_setting('search_path');`,
					Expected: []sql.Row{{"dg_fn_set_attacker, public"}},
				},
			},
		},
	})
}
