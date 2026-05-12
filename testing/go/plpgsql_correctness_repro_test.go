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
					Query:    `SELECT plpgsql_case_without_else(1);`,
					Expected: []sql.Row{{"one"}},
				},
				{
					Query:       `SELECT plpgsql_case_without_else(2);`,
					ExpectedErr: `case not found`,
				},
			},
		},
	})
}

// TestPlpgsqlRaiseRejectsDuplicateMessageOptionRepro reproduces a PL/pgSQL
// correctness bug: a RAISE statement cannot specify the MESSAGE option both via
// the format string and the USING clause.
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
					Query:       `SELECT plpgsql_raise_duplicate_message();`,
					ExpectedErr: `RAISE option already specified: MESSAGE`,
				},
			},
		},
	})
}

// TestPlpgsqlRaiseRejectsDuplicateDetailOptionRepro reproduces a PL/pgSQL
// correctness bug: a RAISE statement cannot specify the same USING option more
// than once.
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
					Query:       `SELECT plpgsql_raise_duplicate_detail();`,
					ExpectedErr: `RAISE option already specified: DETAIL`,
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
					Query:    `SELECT found_value FROM plpgsql_execute_found_seen WHERE marker = 'execute_into';`,
					Expected: []sql.Row{{"true"}},
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
					Query:    `SELECT found_value, affected FROM plpgsql_execute_dml_found_seen WHERE marker = 'execute_dml';`,
					Expected: []sql.Row{{"true", 0}},
				},
			},
		},
	})
}

// TestPlpgsqlAliasVariablesResolveRepro reproduces a PL/pgSQL correctness bug:
// ALIAS variables should be assignable names for local variables and function
// arguments.
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
					Query:    `SELECT plpgsql_alias_echo('aliased value');`,
					Expected: []sql.Row{{"aliased value"}},
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
					Query:    `SELECT plpgsql_composite_single_return()::text;`,
					Expected: []sql.Row{{"(1,apple,3,2.5)"}},
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
					Query:    `SELECT * FROM plpgsql_return_next_scalar(7);`,
					Expected: []sql.Row{{7}, {8}},
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
					Query:    `SELECT * FROM plpgsql_return_query_execute();`,
					Expected: []sql.Row{{10}, {20}, {40}, {50}},
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
					Query:    `SELECT plpgsql_for_execute_sum();`,
					Expected: []sql.Row{{6}},
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
					Query:       `SELECT plpgsql_returning_multi();`,
					ExpectedErr: `query returned more than one row`,
				},
				{
					Query:    `SELECT COUNT(*) FROM plpgsql_returning_multi_items;`,
					Expected: []sql.Row{{0}},
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
					Query:       `SELECT plpgsql_update_returning_multi();`,
					ExpectedErr: `query returned more than one row`,
				},
				{
					Query:    `SELECT COUNT(*) FROM plpgsql_update_returning_multi_items WHERE touched;`,
					Expected: []sql.Row{{0}},
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
					Query:       `SELECT plpgsql_delete_returning_multi();`,
					ExpectedErr: `query returned more than one row`,
				},
				{
					Query:    `SELECT COUNT(*) FROM plpgsql_delete_returning_multi_items;`,
					Expected: []sql.Row{{2}},
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
					Query:       `CALL plpgsql_proc_commit_then_fail();`,
					ExpectedErr: `fail after commit`,
				},
				{
					Query:    `SELECT id FROM plpgsql_proc_commit_items ORDER BY id;`,
					Expected: []sql.Row{{1}},
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
					Query:    `SELECT id FROM plpgsql_proc_rollback_items ORDER BY id;`,
					Expected: []sql.Row{{2}},
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
					Query:    `SELECT plpgsql_foreach_array_sum(ARRAY[1, 2, 3, 4]);`,
					Expected: []sql.Row{{10}},
				},
			},
		},
	})
}

// TestPlpgsqlAssertStatementRepro reproduces a PL/pgSQL compatibility gap:
// ASSERT should raise an exception when its condition is false.
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
					Query:    `SELECT plpgsql_assert_positive(5);`,
					Expected: []sql.Row{{5}},
				},
				{
					Query:       `SELECT plpgsql_assert_positive(0);`,
					ExpectedErr: `input must be positive`,
				},
			},
		},
	})
}
