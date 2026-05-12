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

// TestFunctionDefaultArgumentCatalogRepro reproduces a routine catalog metadata
// gap: PostgreSQL records defaulted callable arguments in pg_proc.
func TestFunctionDefaultArgumentCatalogRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "function default arguments populate pg_proc metadata",
			SetUpScript: []string{
				`CREATE FUNCTION default_arg_function(
					input_value INT,
					multiplier INT DEFAULT 2
				)
				RETURNS INT
				LANGUAGE SQL
				AS $$ SELECT input_value * multiplier $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT pronargs::text, pronargdefaults::text, proargdefaults IS NOT NULL
						FROM pg_catalog.pg_proc
						WHERE proname = 'default_arg_function';`,
					Expected: []sql.Row{{"2", "1", true}},
				},
			},
		},
	})
}

// TestProcedureDefaultArgumentCatalogRepro reproduces the same pg_proc metadata
// gap for procedures with defaulted callable arguments.
func TestProcedureDefaultArgumentCatalogRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "procedure default arguments populate pg_proc metadata",
			SetUpScript: []string{
				`CREATE PROCEDURE default_arg_procedure(
					input_value INT,
					multiplier INT DEFAULT 2
				)
				LANGUAGE SQL
				AS $$ SELECT input_value * multiplier $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT pronargs::text, pronargdefaults::text, proargdefaults IS NOT NULL
						FROM pg_catalog.pg_proc
						WHERE proname = 'default_arg_procedure';`,
					Expected: []sql.Row{{"2", "1", true}},
				},
			},
		},
	})
}

// TestFunctionDefaultArgumentIntrospectionRepro reproduces a catalog helper
// compatibility gap: PostgreSQL exposes default expressions through
// pg_get_function_arg_default.
func TestFunctionDefaultArgumentIntrospectionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_get_function_arg_default renders function defaults",
			SetUpScript: []string{
				`CREATE FUNCTION introspect_default_arg_function(
					input_value INT,
					multiplier INT DEFAULT 2
				)
				RETURNS INT
				LANGUAGE SQL
				AS $$ SELECT input_value * multiplier $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT pg_get_function_arg_default(p.oid, 1)
						FROM pg_catalog.pg_proc p
						WHERE p.proname = 'introspect_default_arg_function';`,
					Expected: []sql.Row{{"2"}},
				},
			},
		},
	})
}

// TestFunctionDefaultArgumentAmbiguousOverloadRepro reproduces a function call
// resolution gap: PostgreSQL reports ambiguity when multiple overloads can
// satisfy an omitted defaulted argument.
func TestFunctionDefaultArgumentAmbiguousOverloadRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "defaulted overload call reports ambiguity",
			SetUpScript: []string{
				`CREATE FUNCTION default_ambiguous_overload(input_value INT, multiplier INT DEFAULT 2)
					RETURNS INT
					LANGUAGE SQL
					AS $$ SELECT input_value * multiplier $$;`,
				`CREATE FUNCTION default_ambiguous_overload(input_value INT, label TEXT DEFAULT 'x')
					RETURNS INT
					LANGUAGE SQL
					AS $$ SELECT input_value $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `SELECT default_ambiguous_overload(5);`,
					ExpectedErr: `not unique`,
				},
			},
		},
	})
}

// TestProcedureDefaultArgumentAmbiguousOverloadRepro reproduces the same
// default-argument overload ambiguity gap for CALL.
func TestProcedureDefaultArgumentAmbiguousOverloadRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "defaulted procedure overload call reports ambiguity",
			SetUpScript: []string{
				`CREATE PROCEDURE default_ambiguous_proc(input_value INT, multiplier INT DEFAULT 2)
					LANGUAGE SQL
					AS $$ SELECT input_value * multiplier $$;`,
				`CREATE PROCEDURE default_ambiguous_proc(input_value INT, label TEXT DEFAULT 'x')
					LANGUAGE SQL
					AS $$ SELECT input_value $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `CALL default_ambiguous_proc(5);`,
					ExpectedErr: `not unique`,
				},
			},
		},
	})
}

// TestRoutineDefaultArgumentValidationRepro reproduces routine definition
// correctness gaps: PostgreSQL rejects default values on output parameters and
// rejects required input parameters after defaulted inputs.
func TestRoutineDefaultArgumentValidationRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "routine default arguments are validated",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE FUNCTION default_before_required_function(
						first_value INT DEFAULT 1,
						second_value INT
					)
					RETURNS INT
					LANGUAGE SQL
					AS $$ SELECT first_value + second_value $$;`,
					ExpectedErr: `default`,
				},
				{
					Query: `CREATE FUNCTION out_default_function(
						OUT result_value INT DEFAULT 1
					)
					LANGUAGE SQL
					AS $$ SELECT 1 $$;`,
					ExpectedErr: `default`,
				},
				{
					Query: `CREATE PROCEDURE out_default_procedure(
						OUT result_value INT DEFAULT 1
					)
					LANGUAGE SQL
					AS $$ SELECT 1 $$;`,
					ExpectedErr: `default`,
				},
				{
					Query: `CREATE FUNCTION variadic_before_required_function(
						VARIADIC values_in INT[],
						trailing_value INT
					)
					RETURNS INT
					LANGUAGE SQL
					AS $$ SELECT trailing_value $$;`,
					ExpectedErr: `VARIADIC`,
				},
				{
					Query: `CREATE PROCEDURE variadic_before_out_procedure(
						VARIADIC values_in INT[],
						OUT result_value INT
					)
					LANGUAGE SQL
					AS $$ SELECT 1 $$;`,
					ExpectedErr: `VARIADIC`,
				},
			},
		},
	})
}

// TestFunctionInoutArgumentCatalogRepro reproduces a routine catalog metadata
// gap: PostgreSQL records INOUT function arguments with mode "b".
func TestFunctionInoutArgumentCatalogRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "function INOUT arguments populate pg_proc metadata",
			SetUpScript: []string{
				`CREATE FUNCTION catalog_inout_argument_function(INOUT value_seen INT)
					LANGUAGE SQL
					AS $$ SELECT value_seen + 1 $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT
							pronargs::text,
							array_to_string(proargmodes, ','),
							array_to_string(proargnames, ',')
						FROM pg_catalog.pg_proc
						WHERE proname = 'catalog_inout_argument_function';`,
					Expected: []sql.Row{{"1", "b", "value_seen"}},
				},
				{
					Query:    `SELECT catalog_inout_argument_function(4);`,
					Expected: []sql.Row{{5}},
				},
			},
		},
	})
}

// TestProcedureInoutArgumentCatalogRepro reproduces the same routine catalog
// metadata gap for procedures with INOUT parameters.
func TestProcedureInoutArgumentCatalogRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "procedure INOUT arguments populate pg_proc metadata",
			SetUpScript: []string{
				`CREATE PROCEDURE catalog_inout_argument_procedure(INOUT value_seen INT)
					LANGUAGE SQL
					AS $$ SELECT value_seen + 1 $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT
							pronargs::text,
							array_to_string(proargmodes, ','),
							array_to_string(proargnames, ',')
						FROM pg_catalog.pg_proc
						WHERE proname = 'catalog_inout_argument_procedure';`,
					Expected: []sql.Row{{"1", "b", "value_seen"}},
				},
			},
		},
	})
}

// TestVariadicSqlFunctionCallRepro reproduces a routine argument compatibility
// gap: PostgreSQL supports user-defined VARIADIC SQL functions and both
// expanded and explicit VARIADIC array calls.
func TestVariadicSqlFunctionCallRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "VARIADIC SQL function accepts expanded and array calls",
			SetUpScript: []string{
				`CREATE FUNCTION variadic_sql_count(VARIADIC values_in INT[])
					RETURNS INT
					LANGUAGE SQL
					AS $$ SELECT array_length(values_in, 1) $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT provariadic <> 0, array_to_string(proargmodes, ',')
						FROM pg_catalog.pg_proc
						WHERE proname = 'variadic_sql_count';`,
					Expected: []sql.Row{{true, "v"}},
				},
				{
					Query:    `SELECT variadic_sql_count(1, 2, 3);`,
					Expected: []sql.Row{{3}},
				},
				{
					Query:    `SELECT variadic_sql_count(VARIADIC ARRAY[4, 5]);`,
					Expected: []sql.Row{{2}},
				},
			},
		},
	})
}

// TestVariadicSqlProcedureCallRepro reproduces the same routine argument
// compatibility gap for CALL resolution of VARIADIC SQL procedures.
func TestVariadicSqlProcedureCallRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "VARIADIC SQL procedure accepts expanded and array calls",
			SetUpScript: []string{
				`CREATE TABLE variadic_proc_audit (
					value_seen INT
				);`,
				`CREATE PROCEDURE variadic_sql_proc(VARIADIC values_in INT[])
					LANGUAGE SQL
					AS $$ INSERT INTO variadic_proc_audit VALUES (array_length(values_in, 1)) $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT provariadic <> 0, array_to_string(proargmodes, ',')
						FROM pg_catalog.pg_proc
						WHERE proname = 'variadic_sql_proc';`,
					Expected: []sql.Row{{true, "v"}},
				},
				{
					Query: `CALL variadic_sql_proc(1, 2, 3);`,
				},
				{
					Query: `CALL variadic_sql_proc(VARIADIC ARRAY[4, 5]);`,
				},
				{
					Query:    `SELECT value_seen FROM variadic_proc_audit ORDER BY value_seen;`,
					Expected: []sql.Row{{2}, {3}},
				},
			},
		},
	})
}
