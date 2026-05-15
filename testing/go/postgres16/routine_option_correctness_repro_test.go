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

package postgres16

import (
	. "github.com/dolthub/doltgresql/testing/go"

	"testing"
)

// TestFunctionDefinitionsRejectInvalidPlannerOptionsRepro reproduces
// correctness bugs: Doltgres accepts function planner options that PostgreSQL
// rejects before creating the function.
func TestFunctionDefinitionsRejectInvalidPlannerOptionsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "function definitions reject invalid planner options",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE FUNCTION routine_scalar_rows_option()
						RETURNS INT
						LANGUAGE SQL
						ROWS 10
						AS $$ SELECT 1 $$;`, PostgresOracle: ScriptTestPostgresOracle{ID: "routine-option-correctness-repro-test-testfunctiondefinitionsrejectinvalidplanneroptionsrepro-0001-create-function-routine_scalar_rows_option-returns-int", Compare: "sqlstate"},
				},
				{
					Query: `CREATE FUNCTION routine_zero_rows_option()
						RETURNS SETOF INT
						LANGUAGE SQL
						ROWS 0
						AS $$ SELECT 1 $$;`, PostgresOracle: ScriptTestPostgresOracle{ID: "routine-option-correctness-repro-test-testfunctiondefinitionsrejectinvalidplanneroptionsrepro-0002-create-function-routine_zero_rows_option-returns-setof", Compare: "sqlstate"},
				},
				{
					Query: `CREATE FUNCTION routine_zero_cost_option()
						RETURNS INT
						LANGUAGE SQL
						COST 0
						AS $$ SELECT 1 $$;`, PostgresOracle: ScriptTestPostgresOracle{ID: "routine-option-correctness-repro-test-testfunctiondefinitionsrejectinvalidplanneroptionsrepro-0003-create-function-routine_zero_cost_option-returns-int", Compare: "sqlstate"},
				},
				{
					Query: `CREATE FUNCTION routine_negative_cost_option()
						RETURNS INT
						LANGUAGE SQL
						COST -1
						AS $$ SELECT 1 $$;`, PostgresOracle: ScriptTestPostgresOracle{ID: "routine-option-correctness-repro-test-testfunctiondefinitionsrejectinvalidplanneroptionsrepro-0004-create-function-routine_negative_cost_option-returns-int",

						// TestCreateFunctionNullInputOptionsGuard keeps coverage for create-time
						// null-input routine options.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestCreateFunctionNullInputOptionsGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE FUNCTION null-input options control strict execution",
			SetUpScript: []string{
				`CREATE FUNCTION strict_null_input_value(input INT)
					RETURNS INT
					LANGUAGE SQL
					STRICT
					AS $$ SELECT 7 $$;`,
				`CREATE FUNCTION called_null_input_value(input INT)
					RETURNS INT
					LANGUAGE SQL
					CALLED ON NULL INPUT
					AS $$ SELECT 7 $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT strict_null_input_value(NULL::INT) IS NULL;`, PostgresOracle: ScriptTestPostgresOracle{ID: "routine-option-correctness-repro-test-testcreatefunctionnullinputoptionsguard-0001-select-strict_null_input_value-null::int-is-null"},
				},
				{
					Query: `SELECT called_null_input_value(NULL::INT);`, PostgresOracle: ScriptTestPostgresOracle{ID: "routine-option-correctness-repro-test-testcreatefunctionnullinputoptionsguard-0002-select-called_null_input_value-null::int"},
				},
			},
		},
	})
}

// TestCreateOrReplaceFunctionNullInputOptionsGuard keeps coverage for
// replacement definitions that change null-input routine options.
func TestCreateOrReplaceFunctionNullInputOptionsGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE OR REPLACE FUNCTION updates null-input behavior",
			SetUpScript: []string{
				`CREATE FUNCTION replace_strict_option_value(input INT)
					RETURNS INT
					LANGUAGE SQL
					CALLED ON NULL INPUT
					AS $$ SELECT 7 $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT replace_strict_option_value(NULL::INT);`, PostgresOracle: ScriptTestPostgresOracle{ID: "routine-option-correctness-repro-test-testcreateorreplacefunctionnullinputoptionsguard-0001-select-replace_strict_option_value-null::int"},
				},
				{
					Query: `CREATE OR REPLACE FUNCTION replace_strict_option_value(input INT)
						RETURNS INT
						LANGUAGE SQL
						STRICT
						AS $$ SELECT 7 $$;`, PostgresOracle: ScriptTestPostgresOracle{ID: "routine-option-correctness-repro-test-testcreateorreplacefunctionnullinputoptionsguard-0002-create-or-replace-function-replace_strict_option_value"},
				},
				{
					Query: `SELECT replace_strict_option_value(NULL::INT) IS NULL;`, PostgresOracle: ScriptTestPostgresOracle{ID: "routine-option-correctness-repro-test-testcreateorreplacefunctionnullinputoptionsguard-0003-select-replace_strict_option_value-null::int-is-null"},
				},
			},
		},
	})
}

// TestCreateFunctionSecurityDefinerCatalogRepro reproduces a security metadata
// persistence bug: pg_proc.prosecdef should persist SECURITY DEFINER status.
func TestCreateFunctionSecurityDefinerCatalogRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE FUNCTION security option persists in pg_proc",
			SetUpScript: []string{
				`CREATE FUNCTION catalog_security_definer_value()
					RETURNS INT
					LANGUAGE SQL
					SECURITY DEFINER
					AS $$ SELECT 1 $$;`,
				`CREATE FUNCTION catalog_security_invoker_value()
					RETURNS INT
					LANGUAGE SQL
					SECURITY INVOKER
					AS $$ SELECT 1 $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT proname, prosecdef
						FROM pg_catalog.pg_proc
						WHERE proname IN (
							'catalog_security_definer_value',
							'catalog_security_invoker_value'
						)
						ORDER BY proname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "routine-option-correctness-repro-test-testcreatefunctionsecuritydefinercatalogrepro-0001-select-proname-prosecdef-from-pg_catalog.pg_proc"},
				},
			},
		},
	})
}

// TestCreateProcedureSecurityDefinerCatalogRepro reproduces a security metadata
// persistence gap: pg_proc.prosecdef should persist SECURITY DEFINER status for
// procedures as well as functions.
func TestCreateProcedureSecurityDefinerCatalogRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE PROCEDURE security option persists in pg_proc",
			SetUpScript: []string{
				`CREATE PROCEDURE catalog_security_definer_proc()
					LANGUAGE SQL
					SECURITY DEFINER
					AS $$ SELECT 1 $$;`,
				`CREATE PROCEDURE catalog_security_invoker_proc()
					LANGUAGE SQL
					SECURITY INVOKER
					AS $$ SELECT 1 $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT proname, prosecdef
						FROM pg_catalog.pg_proc
						WHERE proname IN (
							'catalog_security_definer_proc',
							'catalog_security_invoker_proc'
						)
						ORDER BY proname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "routine-option-correctness-repro-test-testcreateproceduresecuritydefinercatalogrepro-0001-select-proname-prosecdef-from-pg_catalog.pg_proc"},
				},
			},
		},
	})
}

// TestCreateFunctionOutArgumentCatalogRepro reproduces a routine catalog
// metadata gap: pg_proc records OUT argument modes and names separately from
// the callable input-argument list.
func TestCreateFunctionOutArgumentCatalogRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE FUNCTION OUT arguments populate pg_proc metadata",
			SetUpScript: []string{
				`CREATE FUNCTION catalog_out_argument_value(
					input_value INT,
					OUT doubled INT,
					OUT tripled INT
				)
				LANGUAGE SQL
				AS $$ SELECT input_value * 2, input_value * 3 $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT
							pronargs::text,
							array_to_string(proargmodes, ','),
							array_to_string(proargnames, ',')
						FROM pg_catalog.pg_proc
						WHERE proname = 'catalog_out_argument_value';`, PostgresOracle: ScriptTestPostgresOracle{ID: "routine-option-correctness-repro-test-testcreatefunctionoutargumentcatalogrepro-0001-select-pronargs::text-array_to_string-proargmodes-array_to_string"},
				},
			},
		},
	})
}

// TestCreateProcedureOutArgumentCatalogRepro reproduces the same routine
// catalog metadata gap for procedures with OUT parameters.
func TestCreateProcedureOutArgumentCatalogRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE PROCEDURE OUT arguments populate pg_proc metadata",
			SetUpScript: []string{
				`CREATE PROCEDURE catalog_out_argument_proc(
					input_value INT,
					OUT doubled INT
				)
				LANGUAGE SQL
				AS $$ SELECT input_value * 2 $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT
							pronargs::text,
							array_to_string(proargmodes, ','),
							array_to_string(proargnames, ',')
						FROM pg_catalog.pg_proc
						WHERE proname = 'catalog_out_argument_proc';`, PostgresOracle: ScriptTestPostgresOracle{ID: "routine-option-correctness-repro-test-testcreateprocedureoutargumentcatalogrepro-0001-select-pronargs::text-array_to_string-proargmodes-array_to_string"},
				},
			},
		},
	})
}

// TestCreateFunctionVolatilityCatalogRepro reproduces a routine metadata
// persistence bug: pg_proc.provolatile should reflect IMMUTABLE/STABLE/VOLATILE.
func TestCreateFunctionVolatilityCatalogRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE FUNCTION volatility option persists in pg_proc",
			SetUpScript: []string{
				`CREATE FUNCTION catalog_volatile_default_value()
					RETURNS INT
					LANGUAGE SQL
					AS $$ SELECT 1 $$;`,
				`CREATE FUNCTION catalog_immutable_value()
					RETURNS INT
					LANGUAGE SQL
					IMMUTABLE
					AS $$ SELECT 1 $$;`,
				`CREATE FUNCTION catalog_stable_value()
					RETURNS INT
					LANGUAGE SQL
					STABLE
					AS $$ SELECT 1 $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT proname, provolatile
						FROM pg_catalog.pg_proc
						WHERE proname IN (
							'catalog_volatile_default_value',
							'catalog_immutable_value',
							'catalog_stable_value'
						)
						ORDER BY proname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "routine-option-correctness-repro-test-testcreatefunctionvolatilitycatalogrepro-0001-select-proname-provolatile-from-pg_catalog.pg_proc"},
				},
			},
		},
	})
}

// TestCreateFunctionLeakproofCatalogRepro reproduces a security metadata
// persistence bug: pg_proc.proleakproof should reflect LEAKPROOF status.
func TestCreateFunctionLeakproofCatalogRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE FUNCTION leakproof option persists in pg_proc",
			SetUpScript: []string{
				`CREATE FUNCTION catalog_not_leakproof_value()
					RETURNS BOOL
					LANGUAGE SQL
					AS $$ SELECT true $$;`,
				`CREATE FUNCTION catalog_leakproof_value()
					RETURNS BOOL
					LANGUAGE SQL
					LEAKPROOF
					AS $$ SELECT true $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT proname, proleakproof
						FROM pg_catalog.pg_proc
						WHERE proname IN (
							'catalog_not_leakproof_value',
							'catalog_leakproof_value'
						)
						ORDER BY proname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "routine-option-correctness-repro-test-testcreatefunctionleakproofcatalogrepro-0001-select-proname-proleakproof-from-pg_catalog.pg_proc"},
				},
			},
		},
	})
}

// TestCreateFunctionParallelCatalogRepro reproduces a routine metadata
// persistence bug: pg_proc.proparallel should reflect PARALLEL safety options.
func TestCreateFunctionParallelCatalogRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE FUNCTION parallel option persists in pg_proc",
			SetUpScript: []string{
				`CREATE FUNCTION catalog_parallel_default_value()
					RETURNS INT
					LANGUAGE SQL
					AS $$ SELECT 1 $$;`,
				`CREATE FUNCTION catalog_parallel_safe_value()
					RETURNS INT
					LANGUAGE SQL
					PARALLEL SAFE
					AS $$ SELECT 1 $$;`,
				`CREATE FUNCTION catalog_parallel_restricted_value()
					RETURNS INT
					LANGUAGE SQL
					PARALLEL RESTRICTED
					AS $$ SELECT 1 $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT proname, proparallel
						FROM pg_catalog.pg_proc
						WHERE proname IN (
							'catalog_parallel_default_value',
							'catalog_parallel_safe_value',
							'catalog_parallel_restricted_value'
						)
						ORDER BY proname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "routine-option-correctness-repro-test-testcreatefunctionparallelcatalogrepro-0001-select-proname-proparallel-from-pg_catalog.pg_proc"},
				},
			},
		},
	})
}

// TestCreateFunctionCostRowsCatalogRepro reproduces routine metadata
// persistence bugs: pg_proc.procost/prorows should reflect COST/ROWS options,
// and scalar functions should store prorows as zero.
func TestCreateFunctionCostRowsCatalogRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE FUNCTION cost and rows options persist in pg_proc",
			SetUpScript: []string{
				`CREATE FUNCTION catalog_scalar_cost_value()
					RETURNS INT
					LANGUAGE SQL
					COST 7
					AS $$ SELECT 1 $$;`,
				`CREATE FUNCTION catalog_setof_cost_rows_value()
					RETURNS SETOF INT
					LANGUAGE SQL
					COST 3
					ROWS 5
					AS $$ SELECT 1 $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT proname, procost::text, prorows::text
						FROM pg_catalog.pg_proc
						WHERE proname IN (
							'catalog_scalar_cost_value',
							'catalog_setof_cost_rows_value'
						)
						ORDER BY proname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "routine-option-correctness-repro-test-testcreatefunctioncostrowscatalogrepro-0001-select-proname-procost::text-prorows::text-from"},
				},
			},
		},
	})
}

// TestAlterFunctionVolatilityOptionRepro reproduces a routine DDL compatibility
// gap: PostgreSQL lets ALTER FUNCTION change volatility metadata.
func TestAlterFunctionVolatilityOptionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER FUNCTION volatility changes pg_proc",
			SetUpScript: []string{
				`CREATE FUNCTION alter_catalog_volatility_value()
					RETURNS INT
					LANGUAGE SQL
					VOLATILE
					AS $$ SELECT 1 $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER FUNCTION alter_catalog_volatility_value() IMMUTABLE;`, PostgresOracle: ScriptTestPostgresOracle{ID: "routine-option-correctness-repro-test-testalterfunctionvolatilityoptionrepro-0001-alter-function-alter_catalog_volatility_value-immutable"},
				},
				{
					Query: `SELECT provolatile
						FROM pg_catalog.pg_proc
						WHERE proname = 'alter_catalog_volatility_value';`, PostgresOracle: ScriptTestPostgresOracle{ID: "routine-option-correctness-repro-test-testalterfunctionvolatilityoptionrepro-0002-select-provolatile-from-pg_catalog.pg_proc-where"},
				},
			},
		},
	})
}

// TestAlterFunctionSecurityDefinerOptionRepro reproduces a routine DDL security
// metadata gap: ALTER FUNCTION can switch SECURITY DEFINER/INVOKER.
func TestAlterFunctionSecurityDefinerOptionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER FUNCTION security option changes pg_proc",
			SetUpScript: []string{
				`CREATE FUNCTION alter_catalog_security_value()
					RETURNS INT
					LANGUAGE SQL
					SECURITY INVOKER
					AS $$ SELECT 1 $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER FUNCTION alter_catalog_security_value() SECURITY DEFINER;`, PostgresOracle: ScriptTestPostgresOracle{ID: "routine-option-correctness-repro-test-testalterfunctionsecuritydefineroptionrepro-0001-alter-function-alter_catalog_security_value-security-definer"},
				},
				{
					Query: `SELECT prosecdef
						FROM pg_catalog.pg_proc
						WHERE proname = 'alter_catalog_security_value';`, PostgresOracle: ScriptTestPostgresOracle{ID: "routine-option-correctness-repro-test-testalterfunctionsecuritydefineroptionrepro-0002-select-prosecdef-from-pg_catalog.pg_proc-where"},
				},
			},
		},
	})
}

// TestAlterFunctionLeakproofOptionRepro reproduces a routine DDL security
// metadata gap: ALTER FUNCTION can set LEAKPROOF.
func TestAlterFunctionLeakproofOptionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER FUNCTION leakproof option changes pg_proc",
			SetUpScript: []string{
				`CREATE FUNCTION alter_catalog_leakproof_value()
					RETURNS BOOL
					LANGUAGE SQL
					AS $$ SELECT true $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER FUNCTION alter_catalog_leakproof_value() LEAKPROOF;`, PostgresOracle: ScriptTestPostgresOracle{ID: "routine-option-correctness-repro-test-testalterfunctionleakproofoptionrepro-0001-alter-function-alter_catalog_leakproof_value-leakproof"},
				},
				{
					Query: `SELECT proleakproof
						FROM pg_catalog.pg_proc
						WHERE proname = 'alter_catalog_leakproof_value';`, PostgresOracle: ScriptTestPostgresOracle{ID: "routine-option-correctness-repro-test-testalterfunctionleakproofoptionrepro-0002-select-proleakproof-from-pg_catalog.pg_proc-where"},
				},
			},
		},
	})
}

// TestAlterFunctionCostRowsOptionRepro reproduces routine DDL metadata gaps:
// ALTER FUNCTION can change COST and ROWS for set-returning functions.
func TestAlterFunctionCostRowsOptionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER FUNCTION cost and rows options change pg_proc",
			SetUpScript: []string{
				`CREATE FUNCTION alter_catalog_cost_rows_value()
					RETURNS SETOF INT
					LANGUAGE SQL
					AS $$ SELECT 1 $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER FUNCTION alter_catalog_cost_rows_value() COST 9 ROWS 11;`, PostgresOracle: ScriptTestPostgresOracle{ID: "routine-option-correctness-repro-test-testalterfunctioncostrowsoptionrepro-0001-alter-function-alter_catalog_cost_rows_value-cost-9"},
				},
				{
					Query: `SELECT procost::text, prorows::text
						FROM pg_catalog.pg_proc
						WHERE proname = 'alter_catalog_cost_rows_value';`, PostgresOracle: ScriptTestPostgresOracle{ID: "routine-option-correctness-repro-test-testalterfunctioncostrowsoptionrepro-0002-select-procost::text-prorows::text-from-pg_catalog.pg_proc"},
				},
			},
		},
	})
}

// TestAlterFunctionSetConfigOptionRepro reproduces a routine catalog
// persistence gap: ALTER FUNCTION ... SET should store function-local GUC
// options in pg_proc.proconfig.
func TestAlterFunctionSetConfigOptionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER FUNCTION SET updates pg_proc proconfig",
			SetUpScript: []string{
				`CREATE FUNCTION alter_config_option_value()
					RETURNS INT
					LANGUAGE SQL
					AS $$ SELECT 1 $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER FUNCTION alter_config_option_value()
						SET work_mem = '64kB';`,
				},
				{
					Query: `SELECT array_to_string(proconfig, ',')
						FROM pg_catalog.pg_proc
						WHERE proname = 'alter_config_option_value';`, PostgresOracle: ScriptTestPostgresOracle{ID: "routine-option-correctness-repro-test-testalterfunctionsetconfigoptionrepro-0001-select-array_to_string-proconfig-from-pg_catalog.pg_proc"},
				},
			},
		},
	})
}

// TestAlterProcedureSetConfigOptionRepro reproduces a routine catalog
// persistence gap: ALTER PROCEDURE ... SET should store procedure-local GUC
// options in pg_proc.proconfig.
func TestAlterProcedureSetConfigOptionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER PROCEDURE SET updates pg_proc proconfig",
			SetUpScript: []string{
				`CREATE TABLE alter_config_proc_audit (
					value_seen INT
				);`,
				`CREATE PROCEDURE alter_config_option_proc()
					LANGUAGE SQL
					AS $$ INSERT INTO alter_config_proc_audit VALUES (1) $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER PROCEDURE alter_config_option_proc()
						SET work_mem = '64kB';`,
				},
				{
					Query: `SELECT array_to_string(proconfig, ',')
						FROM pg_catalog.pg_proc
						WHERE proname = 'alter_config_option_proc';`, PostgresOracle: ScriptTestPostgresOracle{ID: "routine-option-correctness-repro-test-testalterproceduresetconfigoptionrepro-0001-select-array_to_string-proconfig-from-pg_catalog.pg_proc"},
				},
			},
		},
	})
}

// TestCreateProcedurePgProcCatalogRowRepro reproduces a routine catalog
// persistence bug: PostgreSQL stores procedures in pg_proc with prokind = 'p'.
func TestCreateProcedurePgProcCatalogRowRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE PROCEDURE creates pg_proc row",
			SetUpScript: []string{
				`CREATE PROCEDURE catalog_proc_row_value(input_value INT)
					LANGUAGE plpgsql
					AS $$
					BEGIN
						NULL;
					END;
					$$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT proname, prokind
						FROM pg_catalog.pg_proc
						WHERE proname = 'catalog_proc_row_value';`, PostgresOracle: ScriptTestPostgresOracle{ID: "routine-option-correctness-repro-test-testcreateprocedurepgproccatalogrowrepro-0001-select-proname-prokind-from-pg_catalog.pg_proc"},
				},
			},
		},
	})
}

// TestAlterFunctionNullInputOptionRepro reproduces a routine DDL correctness
// bug: PostgreSQL lets ALTER FUNCTION switch between CALLED ON NULL INPUT and
// STRICT / RETURNS NULL ON NULL INPUT behavior.
func TestAlterFunctionNullInputOptionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER FUNCTION STRICT changes null-input behavior",
			SetUpScript: []string{
				`CREATE FUNCTION alter_strict_null_input_value(input INT)
					RETURNS INT
					LANGUAGE SQL
					CALLED ON NULL INPUT
					AS $$ SELECT 7 $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT alter_strict_null_input_value(NULL::INT);`, PostgresOracle: ScriptTestPostgresOracle{ID: "routine-option-correctness-repro-test-testalterfunctionnullinputoptionrepro-0001-select-alter_strict_null_input_value-null::int"},
				},
				{
					Query: `ALTER FUNCTION alter_strict_null_input_value(INT) STRICT;`, PostgresOracle: ScriptTestPostgresOracle{ID: "routine-option-correctness-repro-test-testalterfunctionnullinputoptionrepro-0002-alter-function-alter_strict_null_input_value-int-strict"},
				},
				{
					Query: `SELECT alter_strict_null_input_value(NULL::INT) IS NULL;`, PostgresOracle: ScriptTestPostgresOracle{ID: "routine-option-correctness-repro-test-testalterfunctionnullinputoptionrepro-0003-select-alter_strict_null_input_value-null::int-is-null"},
				},
			},
		},
	})
}
