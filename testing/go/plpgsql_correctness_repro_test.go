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

// TestPlpgsqlCaseWithoutElseRaisesCaseNotFoundRepro reproduces a PL/pgSQL
// correctness bug: CASE statements without ELSE must raise case_not_found when
// no WHEN branch matches.
func TestPlpgsqlCaseWithoutElseRaisesCaseNotFoundRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "PL/pgSQL CASE without ELSE raises case_not_found",
			SetUpScript: []string{
				`CREATE FUNCTION plpgsql_case_without_else(input_value INT4)
				RETURNS TEXT AS $$
				DECLARE
					msg TEXT;
				BEGIN
					CASE input_value
						WHEN 1 THEN
							msg := 'one';
					END CASE;
					RETURN msg;
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT plpgsql_case_without_else(1);`, PostgresOracle: ScriptTestPostgresOracle{ID: "plpgsql-correctness-repro-test-testplpgsqlcasewithoutelseraisescasenotfoundrepro-0001-select-plpgsql_case_without_else-1"},
				},
				{
					Query: `SELECT plpgsql_case_without_else(2);`, PostgresOracle: ScriptTestPostgresOracle{ID: "plpgsql-correctness-repro-test-testplpgsqlcasewithoutelseraisescasenotfoundrepro-0002-select-plpgsql_case_without_else-2",

						// TestPlpgsqlRaiseRejectsDuplicateMessageOptionRepro reproduces a PL/pgSQL
						// correctness bug: a RAISE statement cannot specify the MESSAGE option both via
						// the format string and the USING clause.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestPlpgsqlRaiseRejectsDuplicateMessageOptionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "PL/pgSQL RAISE rejects duplicate MESSAGE option",
			SetUpScript: []string{
				`CREATE FUNCTION plpgsql_raise_duplicate_message()
				RETURNS VOID AS $$
				BEGIN
					RAISE DEBUG 'DebugTest1' USING MESSAGE = 'DebugMessage';
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT plpgsql_raise_duplicate_message();`, PostgresOracle: ScriptTestPostgresOracle{ID: "plpgsql-correctness-repro-test-testplpgsqlraiserejectsduplicatemessageoptionrepro-0001-select-plpgsql_raise_duplicate_message",

						// TestPlpgsqlRaiseRejectsDuplicateDetailOptionRepro reproduces a PL/pgSQL
						// correctness bug: a RAISE statement cannot specify the same USING option more
						// than once.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestPlpgsqlRaiseRejectsDuplicateDetailOptionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "PL/pgSQL RAISE rejects duplicate DETAIL option",
			SetUpScript: []string{
				`CREATE FUNCTION plpgsql_raise_duplicate_detail()
				RETURNS VOID AS $$
				BEGIN
					RAISE EXCEPTION USING MESSAGE = 'raise message', DETAIL = 'first detail', DETAIL = 'second detail';
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT plpgsql_raise_duplicate_detail();`, PostgresOracle: ScriptTestPostgresOracle{ID: "plpgsql-correctness-repro-test-testplpgsqlraiserejectsduplicatedetailoptionrepro-0001-select-plpgsql_raise_duplicate_detail",

						// TestPlpgsqlExceptionDiagnosticsRollbackRepro reproduces a PL/pgSQL
						// compatibility gap: exception blocks can catch errors, inspect stacked
						// diagnostics, and roll back only the failed block's side effects.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestPlpgsqlExceptionDiagnosticsRollbackRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "PL/pgSQL exception block exposes diagnostics and rolls back block",
			SetUpScript: []string{
				`CREATE TABLE plpgsql_exception_diag_items (id INT PRIMARY KEY);`,
				`CREATE FUNCTION plpgsql_exception_diag()
				RETURNS TEXT AS $$
				DECLARE
					state_text TEXT;
					message_text TEXT;
				BEGIN
					BEGIN
						INSERT INTO plpgsql_exception_diag_items VALUES (1);
						RAISE EXCEPTION 'broken %', 'thing'
							USING ERRCODE = '22012', DETAIL = 'detail text';
					EXCEPTION WHEN OTHERS THEN
						GET STACKED DIAGNOSTICS
							state_text = RETURNED_SQLSTATE,
							message_text = MESSAGE_TEXT;
						INSERT INTO plpgsql_exception_diag_items VALUES (2);
						RETURN state_text || ':' || message_text;
					END;
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT plpgsql_exception_diag();`, PostgresOracle: ScriptTestPostgresOracle{ID: "plpgsql-correctness-repro-test-testplpgsqlexceptiondiagnosticsrollbackrepro-0001-select-plpgsql_exception_diag"},
				},
				{
					Query: `SELECT id FROM plpgsql_exception_diag_items ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "plpgsql-correctness-repro-test-testplpgsqlexceptiondiagnosticsrollbackrepro-0002-select-id-from-plpgsql_exception_diag_items-order"},
				},
			},
		},
	})
}

// TestPlpgsqlExceptionSqlstateSqlerrmVariablesRepro reproduces a PL/pgSQL
// compatibility gap: exception handlers expose implicit SQLSTATE and SQLERRM
// variables for the caught error.
func TestPlpgsqlExceptionSqlstateSqlerrmVariablesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "PL/pgSQL exception handler exposes SQLSTATE and SQLERRM",
			SetUpScript: []string{
				`CREATE FUNCTION plpgsql_exception_sqlstate_sqlerrm()
				RETURNS TEXT AS $$
				BEGIN
					RAISE EXCEPTION 'special failure' USING ERRCODE = '22012';
				EXCEPTION WHEN OTHERS THEN
					RETURN SQLSTATE || ':' || SQLERRM;
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT plpgsql_exception_sqlstate_sqlerrm();`, PostgresOracle: ScriptTestPostgresOracle{ID: "plpgsql-correctness-repro-test-testplpgsqlexceptionsqlstatesqlerrmvariablesrepro-0001-select-plpgsql_exception_sqlstate_sqlerrm"},
				},
			},
		},
	})
}

// TestPlpgsqlBareRaiseRethrowsCurrentExceptionRepro reproduces a PL/pgSQL
// compatibility gap: bare RAISE inside an exception handler should rethrow the
// current exception with its original SQLSTATE and message.
func TestPlpgsqlBareRaiseRethrowsCurrentExceptionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "PL/pgSQL bare RAISE rethrows current exception",
			SetUpScript: []string{
				`CREATE FUNCTION plpgsql_bare_raise_rethrow()
				RETURNS TEXT AS $$
				DECLARE
					message_text TEXT;
				BEGIN
					BEGIN
						RAISE EXCEPTION 'reraised failure' USING ERRCODE = '22012';
					EXCEPTION WHEN OTHERS THEN
						RAISE;
					END;
				EXCEPTION WHEN SQLSTATE '22012' THEN
					GET STACKED DIAGNOSTICS message_text = MESSAGE_TEXT;
					RETURN message_text;
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT plpgsql_bare_raise_rethrow();`, PostgresOracle: ScriptTestPostgresOracle{ID: "plpgsql-correctness-repro-test-testplpgsqlbareraiserethrowscurrentexceptionrepro-0001-select-plpgsql_bare_raise_rethrow"},
				},
			},
		},
	})
}

// TestPlpgsqlDynamicExecuteDoesNotChangeFoundRepro reproduces a PL/pgSQL
// correctness bug: EXECUTE updates ROW_COUNT but must not change FOUND.
func TestPlpgsqlDynamicExecuteDoesNotChangeFoundRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "dynamic EXECUTE INTO does not change FOUND",
			SetUpScript: []string{
				`CREATE TABLE plpgsql_execute_found_source (id INT PRIMARY KEY);`,
				`INSERT INTO plpgsql_execute_found_source VALUES (1);`,
				`CREATE TABLE plpgsql_execute_found_seen (marker TEXT PRIMARY KEY, found_value TEXT);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DO $$
						DECLARE
							got_id INT;
						BEGIN
							PERFORM 1;
							EXECUTE 'SELECT id FROM plpgsql_execute_found_source WHERE id = 999' INTO got_id;
							INSERT INTO plpgsql_execute_found_seen VALUES ('execute_into', FOUND::text);
						END;
					$$;`,
				},
				{
					Query: `SELECT found_value FROM plpgsql_execute_found_seen WHERE marker = 'execute_into';`, PostgresOracle: ScriptTestPostgresOracle{ID: "plpgsql-correctness-repro-test-testplpgsqldynamicexecutedoesnotchangefoundrepro-0001-select-found_value-from-plpgsql_execute_found_seen-where"},
				},
			},
		},
		{
			Name: "dynamic EXECUTE DML does not change FOUND",
			SetUpScript: []string{
				`CREATE TABLE plpgsql_execute_dml_found_source (
					id INT PRIMARY KEY,
					touched BOOL NOT NULL DEFAULT false
				);`,
				`INSERT INTO plpgsql_execute_dml_found_source VALUES (1, false);`,
				`CREATE TABLE plpgsql_execute_dml_found_seen (
					marker TEXT PRIMARY KEY,
					found_value TEXT,
					affected INT
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DO $$
						DECLARE
							affected INT;
						BEGIN
							PERFORM 1;
							EXECUTE 'UPDATE plpgsql_execute_dml_found_source SET touched = true WHERE id = 999';
							GET DIAGNOSTICS affected = ROW_COUNT;
							INSERT INTO plpgsql_execute_dml_found_seen VALUES ('execute_dml', FOUND::text, affected);
						END;
					$$;`,
				},
				{
					Query: `SELECT found_value, affected FROM plpgsql_execute_dml_found_seen WHERE marker = 'execute_dml';`, PostgresOracle: ScriptTestPostgresOracle{ID: "plpgsql-correctness-repro-test-testplpgsqldynamicexecutedoesnotchangefoundrepro-0002-select-found_value-affected-from-plpgsql_execute_dml_found_seen"},
				},
			},
		},
	})
}

// TestPlpgsqlDynamicExecuteIntoRecordRepro reproduces a PL/pgSQL compatibility
// gap: EXECUTE ... INTO can populate a RECORD target whose fields are then
// accessible by name.
func TestPlpgsqlDynamicExecuteIntoRecordRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "dynamic EXECUTE INTO populates RECORD fields",
			SetUpScript: []string{
				`CREATE FUNCTION plpgsql_execute_into_record()
				RETURNS TEXT AS $$
				DECLARE
					got_row RECORD;
				BEGIN
					EXECUTE 'SELECT 10 AS id, ''dynamic'' AS label' INTO got_row;
					RETURN got_row.id::TEXT || ':' || got_row.label;
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT plpgsql_execute_into_record();`, PostgresOracle: ScriptTestPostgresOracle{ID: "plpgsql-correctness-repro-test-testplpgsqldynamicexecuteintorecordrepro-0001-select-plpgsql_execute_into_record"},
				},
			},
		},
	})
}

// TestPlpgsqlNonVoidFunctionRequiresReturnValueRepro reproduces a PL/pgSQL
// correctness bug: reaching the end of a non-void function without RETURN must
// raise an error.
func TestPlpgsqlNonVoidFunctionRequiresReturnValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "PL/pgSQL non-void function requires return value",
			SetUpScript: []string{
				`CREATE FUNCTION plpgsql_missing_return_value()
				RETURNS INT AS $$
				BEGIN
					PERFORM 1;
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT plpgsql_missing_return_value();`, PostgresOracle: ScriptTestPostgresOracle{ID: "plpgsql-correctness-repro-test-testplpgsqlnonvoidfunctionrequiresreturnvaluerepro-0001-select-plpgsql_missing_return_value",

						// TestPlpgsqlReturnStatementValidationRepro reproduces PL/pgSQL compatibility
						// gaps: RETURN syntax is validated against the function's declared result type.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestPlpgsqlReturnStatementValidationRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "PL/pgSQL RETURN statements match result type",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE FUNCTION plpgsql_nonvoid_bare_return()
						RETURNS INT AS $$
						BEGIN
							RETURN;
						END;
						$$ LANGUAGE plpgsql;`, PostgresOracle: ScriptTestPostgresOracle{ID: "plpgsql-correctness-repro-test-testplpgsqlreturnstatementvalidationrepro-0001-create-function-plpgsql_nonvoid_bare_return-returns-int", Compare: "sqlstate"},
				},
				{
					Query: `CREATE FUNCTION plpgsql_void_return_expression()
						RETURNS VOID AS $$
						BEGIN
							RETURN 5;
						END;
						$$ LANGUAGE plpgsql;`, PostgresOracle: ScriptTestPostgresOracle{ID: "plpgsql-correctness-repro-test-testplpgsqlreturnstatementvalidationrepro-0002-create-function-plpgsql_void_return_expression-returns-void",

						// TestPlpgsqlSelectIntoStrictCardinalityRepro reproduces a PL/pgSQL
						// compatibility gap: SELECT ... INTO STRICT must require exactly one row,
						// returning the row when present and raising errors for zero or multiple rows.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestPlpgsqlSelectIntoStrictCardinalityRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "PL/pgSQL SELECT INTO STRICT enforces cardinality",
			SetUpScript: []string{
				`CREATE TABLE plpgsql_strict_items (
					id INT PRIMARY KEY,
					label TEXT NOT NULL
				);`,
				`INSERT INTO plpgsql_strict_items VALUES
					(1, 'one'),
					(2, 'two');`,
				`CREATE FUNCTION plpgsql_strict_label(input_id INT)
				RETURNS TEXT AS $$
				DECLARE
					got_label TEXT;
				BEGIN
					SELECT label INTO STRICT got_label
					FROM plpgsql_strict_items
					WHERE id = input_id;
					RETURN got_label;
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE FUNCTION plpgsql_strict_any_label()
				RETURNS TEXT AS $$
				DECLARE
					got_label TEXT;
				BEGIN
					SELECT label INTO STRICT got_label
					FROM plpgsql_strict_items
					ORDER BY id;
					RETURN got_label;
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT plpgsql_strict_label(1);`, PostgresOracle: ScriptTestPostgresOracle{ID: "plpgsql-correctness-repro-test-testplpgsqlselectintostrictcardinalityrepro-0001-select-plpgsql_strict_label-1"},
				},
				{
					Query: `SELECT plpgsql_strict_label(999);`, PostgresOracle: ScriptTestPostgresOracle{ID: "plpgsql-correctness-repro-test-testplpgsqlselectintostrictcardinalityrepro-0002-select-plpgsql_strict_label-999", Compare: "sqlstate"},
				},
				{
					Query: `SELECT plpgsql_strict_any_label();`, PostgresOracle: ScriptTestPostgresOracle{ID: "plpgsql-correctness-repro-test-testplpgsqlselectintostrictcardinalityrepro-0003-select-plpgsql_strict_any_label",

						// TestPlpgsqlAliasVariablesResolveRepro reproduces a PL/pgSQL correctness bug:
						// ALIAS variables should be assignable names for local variables and function
						// arguments.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestPlpgsqlAliasVariablesResolveRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "PL/pgSQL ALIAS variables resolve",
			SetUpScript: []string{
				`CREATE FUNCTION plpgsql_alias_echo(input_value TEXT)
				RETURNS TEXT AS $$
				DECLARE
					base_value TEXT;
					base_alias ALIAS FOR base_value;
					nested_alias ALIAS FOR base_alias;
					input_alias ALIAS FOR input_value;
				BEGIN
					nested_alias := input_alias;
					RETURN base_value;
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT plpgsql_alias_echo('aliased value');`, PostgresOracle: ScriptTestPostgresOracle{ID: "plpgsql-correctness-repro-test-testplpgsqlaliasvariablesresolverepro-0001-select-plpgsql_alias_echo-aliased-value"},
				},
			},
		},
	})
}

// TestPlpgsqlReturnsTableCompositeVariableRepro reproduces a PL/pgSQL
// correctness bug: PostgreSQL lets a function declare a variable using a
// table row type, SELECT a row into it, and return that composite value.
func TestPlpgsqlReturnsTableCompositeVariableRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "PL/pgSQL returns table-typed composite variable",
			SetUpScript: []string{
				`CREATE TABLE plpgsql_composite_return_items (
					id INT PRIMARY KEY,
					name TEXT NOT NULL,
					qty INT NOT NULL,
					price REAL NOT NULL
				);`,
				`INSERT INTO plpgsql_composite_return_items VALUES
					(1, 'apple', 3, 2.5),
					(2, 'banana', 5, 1.2);`,
				`CREATE FUNCTION plpgsql_composite_single_return()
				RETURNS plpgsql_composite_return_items AS $$
				DECLARE
					result plpgsql_composite_return_items;
				BEGIN
					SELECT * INTO result
					FROM plpgsql_composite_return_items
					WHERE id = 1;
					RETURN result;
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT plpgsql_composite_single_return()::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "plpgsql-correctness-repro-test-testplpgsqlreturnstablecompositevariablerepro-0001-select-plpgsql_composite_single_return-::text"},
				},
			},
		},
	})
}

// TestPlpgsqlTableStarCompositeArgumentRepro reproduces a PL/pgSQL
// compatibility gap: PostgreSQL allows alias.* to pass a table row into a
// composite-typed PL/pgSQL function argument.
func TestPlpgsqlTableStarCompositeArgumentRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "PL/pgSQL accepts table star composite argument",
			SetUpScript: []string{
				`CREATE TABLE plpgsql_table_star_items (
					id INT PRIMARY KEY,
					name TEXT NOT NULL,
					qty INT NOT NULL,
					price REAL NOT NULL
				);`,
				`INSERT INTO plpgsql_table_star_items VALUES
					(1, 'apple', 3, 2.5),
					(2, 'banana', 5, 1.2);`,
				`CREATE FUNCTION plpgsql_table_star_total(item plpgsql_table_star_items)
				RETURNS REAL AS $$
				BEGIN
					RETURN item.qty * item.price;
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT plpgsql_table_star_total(item.*)
						FROM plpgsql_table_star_items AS item
						ORDER BY item.id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "plpgsql-correctness-repro-test-testplpgsqltablestarcompositeargumentrepro-0001-select-plpgsql_table_star_total-item.*-from-plpgsql_table_star_items"},
				},
			},
		},
	})
}

// TestPlpgsqlReturnNextSetofScalarRepro reproduces a PL/pgSQL compatibility
// gap: set-returning functions can emit scalar rows with RETURN NEXT.
func TestPlpgsqlReturnNextSetofScalarRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "PL/pgSQL RETURN NEXT emits scalar SETOF rows",
			SetUpScript: []string{
				`CREATE FUNCTION plpgsql_return_next_scalar(start_value INT)
				RETURNS SETOF INT AS $$
				BEGIN
					RETURN NEXT start_value;
					RETURN NEXT start_value + 1;
					RETURN;
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM plpgsql_return_next_scalar(7);`, PostgresOracle: ScriptTestPostgresOracle{ID: "plpgsql-correctness-repro-test-testplpgsqlreturnnextsetofscalarrepro-0001-select-*-from-plpgsql_return_next_scalar-7"},
				},
			},
		},
	})
}

// TestPlpgsqlReturnNextRecordVariableRepro reproduces a PL/pgSQL compatibility
// gap: set-returning functions can emit composite rows with RETURN NEXT from a
// RECORD variable.
func TestPlpgsqlReturnNextRecordVariableRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "PL/pgSQL RETURN NEXT emits RECORD rows",
			SetUpScript: []string{
				`CREATE TABLE plpgsql_return_next_record_items (
					id INT PRIMARY KEY,
					label TEXT NOT NULL
				);`,
				`INSERT INTO plpgsql_return_next_record_items VALUES
					(1, 'one'),
					(2, 'two');`,
				`CREATE FUNCTION plpgsql_return_next_record_rows()
				RETURNS SETOF plpgsql_return_next_record_items AS $$
				DECLARE
					item RECORD;
				BEGIN
					FOR item IN SELECT * FROM plpgsql_return_next_record_items ORDER BY id LOOP
						RETURN NEXT item;
					END LOOP;
					RETURN;
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, label FROM plpgsql_return_next_record_rows();`, PostgresOracle: ScriptTestPostgresOracle{ID: "plpgsql-correctness-repro-test-testplpgsqlreturnnextrecordvariablerepro-0001-select-id-label-from-plpgsql_return_next_record_rows"},
				},
			},
		},
	})
}

// TestPlpgsqlReturnQueryExecuteRepro reproduces a PL/pgSQL compatibility gap:
// set-returning functions can append rows from dynamic RETURN QUERY EXECUTE.
func TestPlpgsqlReturnQueryExecuteRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "PL/pgSQL RETURN QUERY EXECUTE emits scalar SETOF rows",
			SetUpScript: []string{
				`CREATE FUNCTION plpgsql_return_query_execute()
				RETURNS SETOF INT AS $$
				BEGIN
					RETURN QUERY EXECUTE 'SELECT * FROM (VALUES (10), (20)) AS v(x)';
					RETURN QUERY EXECUTE 'SELECT * FROM (VALUES ($1), ($2)) AS v(x)' USING 40, 50;
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM plpgsql_return_query_execute();`, PostgresOracle: ScriptTestPostgresOracle{ID: "plpgsql-correctness-repro-test-testplpgsqlreturnqueryexecuterepro-0001-select-*-from-plpgsql_return_query_execute"},
				},
			},
		},
	})
}

// TestPlpgsqlForQueryLoopUpdatesFoundRepro reproduces a PL/pgSQL runtime
// correctness bug: FOR query loops should set FOUND after the loop exits based
// on whether at least one row was iterated.
func TestPlpgsqlForQueryLoopUpdatesFoundRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "PL/pgSQL FOR query loop updates FOUND",
			SetUpScript: []string{
				`CREATE FUNCTION plpgsql_for_query_found(include_rows BOOL)
				RETURNS TEXT AS $$
				DECLARE
					value_seen RECORD;
				BEGIN
					PERFORM 1 WHERE false;
					FOR value_seen IN
						SELECT x FROM (VALUES (1), (2)) AS v(x) WHERE include_rows
					LOOP
						NULL;
					END LOOP;
					RETURN FOUND::TEXT;
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT plpgsql_for_query_found(true);`, PostgresOracle: ScriptTestPostgresOracle{ID: "plpgsql-correctness-repro-test-testplpgsqlforqueryloopupdatesfoundrepro-0001-select-plpgsql_for_query_found-true"},
				},
				{
					Query: `SELECT plpgsql_for_query_found(false);`, PostgresOracle: ScriptTestPostgresOracle{ID: "plpgsql-correctness-repro-test-testplpgsqlforqueryloopupdatesfoundrepro-0002-select-plpgsql_for_query_found-false"},
				},
			},
		},
	})
}

// TestPlpgsqlForIntegerLoopUpdatesFoundRepro reproduces a PL/pgSQL runtime
// correctness bug: integer FOR loops should set FOUND after the loop exits
// based on whether at least one iteration ran.
func TestPlpgsqlForIntegerLoopUpdatesFoundRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "PL/pgSQL integer FOR loop updates FOUND",
			SetUpScript: []string{
				`CREATE FUNCTION plpgsql_for_integer_found(start_value INT, end_value INT)
				RETURNS TEXT AS $$
				DECLARE
					i INT;
				BEGIN
					PERFORM 1 WHERE false;
					FOR i IN start_value..end_value LOOP
						NULL;
					END LOOP;
					RETURN FOUND::TEXT;
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT plpgsql_for_integer_found(1, 2);`, PostgresOracle: ScriptTestPostgresOracle{ID: "plpgsql-correctness-repro-test-testplpgsqlforintegerloopupdatesfoundrepro-0001-select-plpgsql_for_integer_found-1-2"},
				},
				{
					Query: `SELECT plpgsql_for_integer_found(2, 1);`, PostgresOracle: ScriptTestPostgresOracle{ID: "plpgsql-correctness-repro-test-testplpgsqlforintegerloopupdatesfoundrepro-0002-select-plpgsql_for_integer_found-2-1"},
				},
			},
		},
	})
}

// TestPlpgsqlForInExecuteLoopRepro reproduces a PL/pgSQL compatibility gap:
// FOR target IN EXECUTE should iterate rows from a dynamic query.
func TestPlpgsqlForInExecuteLoopRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "PL/pgSQL FOR IN EXECUTE iterates dynamic query rows",
			SetUpScript: []string{
				`CREATE FUNCTION plpgsql_for_execute_sum()
				RETURNS INT AS $$
				DECLARE
					total INT := 0;
					value_seen INT;
				BEGIN
					FOR value_seen IN EXECUTE 'SELECT x FROM (VALUES (1), (2), (3)) AS v(x)' LOOP
						total := total + value_seen;
					END LOOP;
					RETURN total;
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT plpgsql_for_execute_sum();`, PostgresOracle: ScriptTestPostgresOracle{ID: "plpgsql-correctness-repro-test-testplpgsqlforinexecutelooprepro-0001-select-plpgsql_for_execute_sum"},
				},
			},
		},
	})
}

// TestPlpgsqlDmlReturningIntoRejectsMultipleRowsRepro reproduces a PL/pgSQL
// consistency bug: DML RETURNING INTO must reject multiple returned rows and
// leave the failed statement's writes rolled back.
func TestPlpgsqlDmlReturningIntoRejectsMultipleRowsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "PL/pgSQL DML RETURNING INTO rejects multiple rows atomically",
			SetUpScript: []string{
				`CREATE TABLE plpgsql_returning_multi_items (id INT PRIMARY KEY);`,
				`CREATE FUNCTION plpgsql_returning_multi()
				RETURNS INT AS $$
				DECLARE
					got_id INT;
				BEGIN
					INSERT INTO plpgsql_returning_multi_items VALUES (1), (2)
						RETURNING id INTO got_id;
					RETURN got_id;
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT plpgsql_returning_multi();`, PostgresOracle: ScriptTestPostgresOracle{ID: "plpgsql-correctness-repro-test-testplpgsqldmlreturningintorejectsmultiplerowsrepro-0001-select-plpgsql_returning_multi", Compare: "sqlstate"},
				},
				{
					Query: `SELECT COUNT(*) FROM plpgsql_returning_multi_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "plpgsql-correctness-repro-test-testplpgsqldmlreturningintorejectsmultiplerowsrepro-0002-select-count-*-from-plpgsql_returning_multi_items"},
				},
			},
		},
	})
}

// TestPlpgsqlUpdateReturningIntoRejectsMultipleRowsRepro reproduces a PL/pgSQL
// consistency bug: UPDATE RETURNING INTO must reject multiple returned rows and
// leave the failed update rolled back.
func TestPlpgsqlUpdateReturningIntoRejectsMultipleRowsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "PL/pgSQL UPDATE RETURNING INTO rejects multiple rows atomically",
			SetUpScript: []string{
				`CREATE TABLE plpgsql_update_returning_multi_items (
					id INT PRIMARY KEY,
					touched BOOL NOT NULL DEFAULT false
				);`,
				`INSERT INTO plpgsql_update_returning_multi_items VALUES
					(1, false),
					(2, false);`,
				`CREATE FUNCTION plpgsql_update_returning_multi()
				RETURNS INT AS $$
				DECLARE
					got_id INT;
				BEGIN
					UPDATE plpgsql_update_returning_multi_items
					SET touched = true
					RETURNING id INTO got_id;
					RETURN got_id;
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT plpgsql_update_returning_multi();`, PostgresOracle: ScriptTestPostgresOracle{ID: "plpgsql-correctness-repro-test-testplpgsqlupdatereturningintorejectsmultiplerowsrepro-0001-select-plpgsql_update_returning_multi", Compare: "sqlstate"},
				},
				{
					Query: `SELECT COUNT(*) FROM plpgsql_update_returning_multi_items WHERE touched;`, PostgresOracle: ScriptTestPostgresOracle{ID: "plpgsql-correctness-repro-test-testplpgsqlupdatereturningintorejectsmultiplerowsrepro-0002-select-count-*-from-plpgsql_update_returning_multi_items"},
				},
			},
		},
	})
}

// TestPlpgsqlDeleteReturningIntoRejectsMultipleRowsRepro reproduces a PL/pgSQL
// consistency bug: DELETE RETURNING INTO must reject multiple returned rows and
// leave the failed delete rolled back.
func TestPlpgsqlDeleteReturningIntoRejectsMultipleRowsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "PL/pgSQL DELETE RETURNING INTO rejects multiple rows atomically",
			SetUpScript: []string{
				`CREATE TABLE plpgsql_delete_returning_multi_items (id INT PRIMARY KEY);`,
				`INSERT INTO plpgsql_delete_returning_multi_items VALUES (1), (2);`,
				`CREATE FUNCTION plpgsql_delete_returning_multi()
				RETURNS INT AS $$
				DECLARE
					got_id INT;
				BEGIN
					DELETE FROM plpgsql_delete_returning_multi_items
					RETURNING id INTO got_id;
					RETURN got_id;
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT plpgsql_delete_returning_multi();`, PostgresOracle: ScriptTestPostgresOracle{ID: "plpgsql-correctness-repro-test-testplpgsqldeletereturningintorejectsmultiplerowsrepro-0001-select-plpgsql_delete_returning_multi", Compare: "sqlstate"},
				},
				{
					Query: `SELECT COUNT(*) FROM plpgsql_delete_returning_multi_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "plpgsql-correctness-repro-test-testplpgsqldeletereturningintorejectsmultiplerowsrepro-0002-select-count-*-from-plpgsql_delete_returning_multi_items"},
				},
			},
		},
	})
}

// TestPlpgsqlProcedureCommitPersistsPriorWorkRepro reproduces a PL/pgSQL
// persistence gap: transaction control inside a top-level CALL can commit work
// before a later procedure error.
func TestPlpgsqlProcedureCommitPersistsPriorWorkRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "PL/pgSQL procedure COMMIT persists prior work before later error",
			SetUpScript: []string{
				`CREATE TABLE plpgsql_proc_commit_items (id INT PRIMARY KEY);`,
				`CREATE PROCEDURE plpgsql_proc_commit_then_fail()
				LANGUAGE plpgsql
				AS $$
				BEGIN
					INSERT INTO plpgsql_proc_commit_items VALUES (1);
					COMMIT;
					INSERT INTO plpgsql_proc_commit_items VALUES (2);
					RAISE EXCEPTION 'fail after commit';
				END;
				$$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CALL plpgsql_proc_commit_then_fail();`, PostgresOracle: ScriptTestPostgresOracle{ID: "plpgsql-correctness-repro-test-testplpgsqlprocedurecommitpersistspriorworkrepro-0001-call-plpgsql_proc_commit_then_fail", Compare: "sqlstate"},
				},
				{
					Query: `SELECT id FROM plpgsql_proc_commit_items ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "plpgsql-correctness-repro-test-testplpgsqlprocedurecommitpersistspriorworkrepro-0002-select-id-from-plpgsql_proc_commit_items-order"},
				},
			},
		},
	})
}

// TestPlpgsqlProcedureRollbackDiscardsPriorWorkRepro reproduces a PL/pgSQL
// persistence gap: transaction control inside a top-level CALL can roll back
// earlier procedure work and continue in a new transaction.
func TestPlpgsqlProcedureRollbackDiscardsPriorWorkRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "PL/pgSQL procedure ROLLBACK discards prior work and continues",
			SetUpScript: []string{
				`CREATE TABLE plpgsql_proc_rollback_items (id INT PRIMARY KEY);`,
				`CREATE PROCEDURE plpgsql_proc_rollback_then_insert()
				LANGUAGE plpgsql
				AS $$
				BEGIN
					INSERT INTO plpgsql_proc_rollback_items VALUES (1);
					ROLLBACK;
					INSERT INTO plpgsql_proc_rollback_items VALUES (2);
				END;
				$$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CALL plpgsql_proc_rollback_then_insert();`,
				},
				{
					Query: `SELECT id FROM plpgsql_proc_rollback_items ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "plpgsql-correctness-repro-test-testplpgsqlprocedurerollbackdiscardspriorworkrepro-0001-select-id-from-plpgsql_proc_rollback_items-order"},
				},
			},
		},
	})
}

// TestPlpgsqlForeachArrayLoopRepro reproduces a PL/pgSQL compatibility gap:
// FOREACH loops should iterate over array elements.
func TestPlpgsqlForeachArrayLoopRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "PL/pgSQL FOREACH iterates array values",
			SetUpScript: []string{
				`CREATE FUNCTION plpgsql_foreach_array_sum(values_in INT[])
				RETURNS INT AS $$
				DECLARE
					value_seen INT;
					total INT := 0;
				BEGIN
					FOREACH value_seen IN ARRAY values_in LOOP
						total := total + value_seen;
					END LOOP;
					RETURN total;
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT plpgsql_foreach_array_sum(ARRAY[1, 2, 3, 4]);`, PostgresOracle: ScriptTestPostgresOracle{ID: "plpgsql-correctness-repro-test-testplpgsqlforeacharraylooprepro-0001-select-plpgsql_foreach_array_sum-array[1-2-3"},
				},
			},
		},
	})
}

// TestPlpgsqlForeachSliceArrayLoopRepro reproduces a PL/pgSQL compatibility
// gap: FOREACH ... SLICE should iterate array slices from multidimensional
// arrays.
func TestPlpgsqlForeachSliceArrayLoopRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "PL/pgSQL FOREACH SLICE iterates multidimensional array rows",
			SetUpScript: []string{
				`CREATE FUNCTION plpgsql_foreach_slice_sum(values_in INT[])
				RETURNS INT AS $$
				DECLARE
					row_slice INT[];
					value_seen INT;
					total INT := 0;
				BEGIN
					FOREACH row_slice SLICE 1 IN ARRAY values_in LOOP
						FOREACH value_seen IN ARRAY row_slice LOOP
							total := total + value_seen;
						END LOOP;
					END LOOP;
					RETURN total;
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT plpgsql_foreach_slice_sum(ARRAY[[1, 2], [3, 4]]);`, PostgresOracle: ScriptTestPostgresOracle{ID: "plpgsql-correctness-repro-test-testplpgsqlforeachslicearraylooprepro-0001-select-plpgsql_foreach_slice_sum-array[[1-2]-[3"},
				},
			},
		},
	})
}

// TestPlpgsqlColumnTypeDeclarationRepro reproduces a PL/pgSQL compatibility
// gap: variables can use table.column%TYPE declarations.
func TestPlpgsqlColumnTypeDeclarationRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "PL/pgSQL variable uses table column percent TYPE",
			SetUpScript: []string{
				`CREATE TABLE plpgsql_type_source (
					id INT PRIMARY KEY,
					label VARCHAR(8)
				);`,
				`CREATE FUNCTION plpgsql_column_type_echo(input_label TEXT)
				RETURNS TEXT AS $$
				DECLARE
					typed_label plpgsql_type_source.label%TYPE;
				BEGIN
					typed_label := input_label;
					RETURN typed_label;
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT plpgsql_column_type_echo('sample');`, PostgresOracle: ScriptTestPostgresOracle{ID: "plpgsql-correctness-repro-test-testplpgsqlcolumntypedeclarationrepro-0001-select-plpgsql_column_type_echo-sample"},
				},
			},
		},
	})
}

// TestPlpgsqlRowTypeDeclarationRepro reproduces a PL/pgSQL compatibility gap:
// variables can use table%ROWTYPE declarations and access row fields.
func TestPlpgsqlRowTypeDeclarationRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "PL/pgSQL variable uses table percent ROWTYPE",
			SetUpScript: []string{
				`CREATE TABLE plpgsql_rowtype_source (
					id INT PRIMARY KEY,
					label TEXT NOT NULL
				);`,
				`INSERT INTO plpgsql_rowtype_source VALUES (1, 'first');`,
				`CREATE FUNCTION plpgsql_rowtype_label(input_id INT)
				RETURNS TEXT AS $$
				DECLARE
					row_value plpgsql_rowtype_source%ROWTYPE;
				BEGIN
					SELECT * INTO row_value
					FROM plpgsql_rowtype_source
					WHERE id = input_id;
					RETURN row_value.label;
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT plpgsql_rowtype_label(1);`, PostgresOracle: ScriptTestPostgresOracle{ID: "plpgsql-correctness-repro-test-testplpgsqlrowtypedeclarationrepro-0001-select-plpgsql_rowtype_label-1"},
				},
			},
		},
	})
}

// TestPlpgsqlDomainVariableAssignmentChecksConstraintRepro reproduces a data
// consistency bug: assignments to PL/pgSQL variables declared as domain types
// must enforce the domain's check constraints.
func TestPlpgsqlDomainVariableAssignmentChecksConstraintRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "PL/pgSQL domain variable assignment checks constraint",
			SetUpScript: []string{
				`CREATE DOMAIN plpgsql_var_positive_domain AS INT
					CHECK (VALUE > 0);`,
				`CREATE FUNCTION plpgsql_domain_assignment_bad()
				RETURNS INT AS $$
				DECLARE
					value_seen plpgsql_var_positive_domain;
				BEGIN
					value_seen := -1;
					RETURN value_seen;
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT plpgsql_domain_assignment_bad();`, PostgresOracle: ScriptTestPostgresOracle{ID: "plpgsql-correctness-repro-test-testplpgsqldomainvariableassignmentchecksconstraintrepro-0001-select-plpgsql_domain_assignment_bad",

						// TestPlpgsqlNotNullVariableRejectsNullAssignmentRepro reproduces a PL/pgSQL
						// data-integrity bug: variables declared NOT NULL must reject NULL assignment.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestPlpgsqlNotNullVariableRejectsNullAssignmentRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "PL/pgSQL NOT NULL variable rejects NULL assignment",
			SetUpScript: []string{
				`CREATE FUNCTION plpgsql_not_null_assignment()
				RETURNS INT AS $$
				DECLARE
					value_seen INT NOT NULL := 1;
				BEGIN
					value_seen := NULL;
					RETURN value_seen;
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT plpgsql_not_null_assignment();`, PostgresOracle: ScriptTestPostgresOracle{ID: "plpgsql-correctness-repro-test-testplpgsqlnotnullvariablerejectsnullassignmentrepro-0001-select-plpgsql_not_null_assignment",

						// TestPlpgsqlAssertStatementRepro reproduces a PL/pgSQL compatibility gap:
						// ASSERT should raise an exception when its condition is false.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestPlpgsqlAssertStatementRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "PL/pgSQL ASSERT checks conditions",
			SetUpScript: []string{
				`CREATE FUNCTION plpgsql_assert_positive(input_value INT)
				RETURNS INT AS $$
				BEGIN
					ASSERT input_value > 0, 'input must be positive';
					RETURN input_value;
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT plpgsql_assert_positive(5);`, PostgresOracle: ScriptTestPostgresOracle{ID: "plpgsql-correctness-repro-test-testplpgsqlassertstatementrepro-0001-select-plpgsql_assert_positive-5"},
				},
				{
					Query: `SELECT plpgsql_assert_positive(0);`, PostgresOracle: ScriptTestPostgresOracle{ID: "plpgsql-correctness-repro-test-testplpgsqlassertstatementrepro-0002-select-plpgsql_assert_positive-0",

						// TestPlpgsqlExplicitCursorFetchLoopRepro reproduces a PL/pgSQL compatibility
						// gap: explicit cursor variables should support OPEN, FETCH, and CLOSE.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestPlpgsqlExplicitCursorFetchLoopRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "PL/pgSQL explicit cursor fetch loop sums rows",
			SetUpScript: []string{
				`CREATE FUNCTION plpgsql_cursor_fetch_sum()
				RETURNS INT AS $$
				DECLARE
					item_cursor CURSOR FOR
						SELECT x FROM (VALUES (1), (2), (3)) AS v(x) ORDER BY x;
					row_value INT;
					total INT := 0;
				BEGIN
					OPEN item_cursor;
					LOOP
						FETCH item_cursor INTO row_value;
						EXIT WHEN NOT FOUND;
						total := total + row_value;
					END LOOP;
					CLOSE item_cursor;
					RETURN total;
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT plpgsql_cursor_fetch_sum();`, PostgresOracle: ScriptTestPostgresOracle{ID: "plpgsql-correctness-repro-test-testplpgsqlexplicitcursorfetchlooprepro-0001-select-plpgsql_cursor_fetch_sum"},
				},
			},
		},
	})
}

// TestPlpgsqlCursorParametersRepro reproduces a PL/pgSQL compatibility gap:
// explicit cursor declarations can accept parameters that are bound by OPEN.
func TestPlpgsqlCursorParametersRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "PL/pgSQL cursor parameters filter rows",
			SetUpScript: []string{
				`CREATE TABLE plpgsql_cursor_param_items (id INT PRIMARY KEY);`,
				`INSERT INTO plpgsql_cursor_param_items VALUES (1), (2), (3), (4);`,
				`CREATE FUNCTION plpgsql_cursor_param_sum(low_id INT, high_id INT)
				RETURNS INT AS $$
				DECLARE
					item_cursor CURSOR (min_id INT, max_id INT) FOR
						SELECT id FROM plpgsql_cursor_param_items
						WHERE id BETWEEN min_id AND max_id
						ORDER BY id;
					row_value INT;
					total INT := 0;
				BEGIN
					OPEN item_cursor(low_id, high_id);
					LOOP
						FETCH item_cursor INTO row_value;
						EXIT WHEN NOT FOUND;
						total := total + row_value;
					END LOOP;
					CLOSE item_cursor;
					RETURN total;
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT plpgsql_cursor_param_sum(2, 3);`, PostgresOracle: ScriptTestPostgresOracle{ID: "plpgsql-correctness-repro-test-testplpgsqlcursorparametersrepro-0001-select-plpgsql_cursor_param_sum-2-3"},
				},
			},
		},
	})
}

// TestPlpgsqlFunctionReturnsRefcursorRepro reproduces a PL/pgSQL compatibility
// gap: PL/pgSQL functions can open and return a named refcursor that callers
// fetch from in the surrounding transaction.
func TestPlpgsqlFunctionReturnsRefcursorRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "PL/pgSQL function returns fetchable refcursor",
			SetUpScript: []string{
				`CREATE TABLE plpgsql_refcursor_items (id INT PRIMARY KEY);`,
				`INSERT INTO plpgsql_refcursor_items VALUES (1), (2), (3);`,
				`CREATE FUNCTION plpgsql_open_refcursor(cursor_name REFCURSOR)
				RETURNS REFCURSOR AS $$
				BEGIN
					OPEN cursor_name FOR
						SELECT id FROM plpgsql_refcursor_items ORDER BY id;
					RETURN cursor_name;
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:            `BEGIN;`,
					SkipResultsCheck: true,
				},
				{
					Query: `SELECT plpgsql_open_refcursor('plpgsql_item_cursor');`, PostgresOracle: ScriptTestPostgresOracle{ID: "plpgsql-correctness-repro-test-testplpgsqlfunctionreturnsrefcursorrepro-0001-select-plpgsql_open_refcursor-plpgsql_item_cursor"},
				},
				{
					Query:    `FETCH ALL FROM plpgsql_item_cursor;`,
					Expected: []sql.Row{{1}, {2}, {3}},
				},
				{
					Query:            `COMMIT;`,
					SkipResultsCheck: true,
				},
			},
		},
	})
}

// TestPlpgsqlFunctionOutParametersReturnRowsRepro reproduces a PL/pgSQL
// compatibility gap: OUT parameters are excluded from the callable argument
// list, and their assigned values form the function result row.
func TestPlpgsqlFunctionOutParametersReturnRowsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "PL/pgSQL function OUT parameters are callable by input args",
			SetUpScript: []string{
				`CREATE FUNCTION plpgsql_out_parameter_values(
					input_value INT,
					OUT doubled INT,
					OUT tripled INT
				)
				AS $$
				BEGIN
					doubled := input_value * 2;
					tripled := input_value * 3;
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT * FROM plpgsql_out_parameter_values(4);`,
					Expected: []sql.Row{{8, 12}},
				},
			},
		},
	})
}
