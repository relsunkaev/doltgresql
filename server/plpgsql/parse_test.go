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

package plpgsql

import (
	"strings"
	"testing"
)

func TestParseDeclareAliasesArraysAndRecords(t *testing.T) {
	ops, err := Parse(`CREATE FUNCTION test_block() RETURNS void AS $$
		DECLARE
			labels TEXT[] := '{alpha,beta,gamma}';
			target_id INT;
			required_id INT NOT NULL := 1;
			target_alias ALIAS FOR target_id;
			item RECORD;
		BEGIN
			PERFORM target_id;
			target_alias := 1;
			FOR item IN SELECT target_alias AS id LOOP
				PERFORM item.id;
			END LOOP;
		END;
	$$ LANGUAGE plpgsql;`)
	if err != nil {
		t.Fatal(err)
	}

	declares := map[string]string{}
	for _, op := range ops {
		if op.OpCode == OpCode_Declare {
			declares[op.Target] = op.PrimaryData
		}
	}
	for target, typ := range map[string]string{
		"labels":      "text[] ",
		"target_id":   "int",
		"required_id": "int ",
		"item":        "record",
	} {
		if declares[target] != typ {
			t.Fatalf("declare %s = %q, expected %q; all declares: %#v", target, declares[target], typ, declares)
		}
	}

	for raw, expectedType := range map[string]string{
		"text[] ": "_text",
		"int":     "int4",
		"record":  "record",
	} {
		schema, typ := normalizeDeclareTypeName(raw)
		if schema != "pg_catalog" || typ != expectedType {
			t.Fatalf("normalize %q = %s.%s, expected pg_catalog.%s", raw, schema, typ, expectedType)
		}
	}

	var foundNotNull bool
	for _, op := range ops {
		if op.OpCode == OpCode_Declare && op.Target == "required_id" && op.Options[notNullVariableOption] == "true" {
			foundNotNull = true
			break
		}
	}
	if !foundNotNull {
		t.Fatalf("NOT NULL declaration option was not preserved; ops: %#v", ops)
	}

	var foundAlias bool
	for _, op := range ops {
		if op.OpCode == OpCode_Alias && op.Target == "target_alias" && op.PrimaryData == "target_id" {
			foundAlias = true
			break
		}
	}
	if !foundAlias {
		t.Fatalf("expected target_alias alias operation; ops: %#v", ops)
	}
}

func TestParseRaiseDuplicateOptionValidation(t *testing.T) {
	ops, err := Parse(`CREATE FUNCTION plpgsql_raise_duplicate_detail()
		RETURNS void AS $$
		BEGIN
			RAISE EXCEPTION USING MESSAGE = 'raise message', DETAIL = 'first detail', DETAIL = 'second detail';
		END;
		$$ LANGUAGE plpgsql;`)
	if err != nil {
		t.Fatal(err)
	}

	for _, op := range ops {
		if op.OpCode == OpCode_Raise {
			if op.Options[raiseValidationErrorOption] != "RAISE option already specified: DETAIL" {
				t.Fatalf("validation error = %q, expected duplicate DETAIL", op.Options[raiseValidationErrorOption])
			}
			return
		}
	}
	t.Fatalf("expected raise operation; ops: %#v", ops)
}

func TestParseCaseWithoutElseRaisesCaseNotFound(t *testing.T) {
	ops, err := Parse(`CREATE FUNCTION plpgsql_case_without_else(v INT)
		RETURNS void AS $$
		BEGIN
			CASE v
				WHEN 1 THEN
					RETURN;
			END CASE;
		END;
		$$ LANGUAGE plpgsql;`)
	if err != nil {
		t.Fatal(err)
	}

	for _, op := range ops {
		if op.OpCode == OpCode_Raise {
			if op.Options["0"] != "'case_not_found'" {
				t.Fatalf("raise errcode = %q, expected case_not_found; op: %#v", op.Options["0"], op)
			}
			if op.Options["3"] != "'CASE statement is missing ELSE part.'" {
				t.Fatalf("raise hint = %q, expected missing ELSE hint; op: %#v", op.Options["3"], op)
			}
			return
		}
	}
	t.Fatalf("expected implicit case-not-found raise operation; ops: %#v", ops)
}

func TestParseReturnQueryOperations(t *testing.T) {
	ops, err := Parse(`CREATE OR REPLACE FUNCTION func2(n INT) RETURNS TABLE (c_id INT, c_name TEXT, c_total_spent INT)
		LANGUAGE plpgsql
		AS $$
		BEGIN
			RETURN QUERY
			SELECT c.id,
			       c.name,
			       SUM(o.amount) AS total_spent
			FROM customers c
			JOIN orders o ON o.customer_id = c.id
			GROUP BY c.id, c.name
			HAVING SUM(o.amount) >= n
			;
		END;
		$$;`)
	if err != nil {
		t.Fatal(err)
	}
	if len(ops) != 4 {
		t.Fatalf("expected 4 operations, found %d: %#v", len(ops), ops)
	}
	for i, expected := range []OpCode{OpCode_ScopeBegin, OpCode_ReturnQuery, OpCode_Return, OpCode_ScopeEnd} {
		if ops[i].OpCode != expected {
			t.Fatalf("operation %d opcode = %d, expected %d; all ops: %#v", i, ops[i].OpCode, expected, ops)
		}
	}
	if len(ops[1].SecondaryData) != 1 || ops[1].SecondaryData[0] != "n" {
		t.Fatalf("RETURN QUERY bindings = %#v, expected n", ops[1].SecondaryData)
	}
	if !strings.Contains(ops[1].PrimaryData, ">= $1") {
		t.Fatalf("RETURN QUERY text = %q, expected >= $1", ops[1].PrimaryData)
	}
}

func TestParseReturnQueryExecuteOperations(t *testing.T) {
	ops, err := Parse(`CREATE FUNCTION func2() RETURNS SETOF INT
		LANGUAGE plpgsql
		AS $$
		BEGIN
			RETURN QUERY EXECUTE 'SELECT * FROM (VALUES ($1), ($2)) AS v(x)' USING 40, 50;
		END;
		$$;`)
	if err != nil {
		t.Fatal(err)
	}
	if len(ops) != 4 {
		t.Fatalf("expected 4 operations, found %d: %#v", len(ops), ops)
	}
	if ops[1].OpCode != OpCode_ReturnQuery {
		t.Fatalf("operation 1 opcode = %d, expected %d; all ops: %#v", ops[1].OpCode, OpCode_ReturnQuery, ops)
	}
	if ops[1].Options["dynamic"] != "true" {
		t.Fatalf("dynamic option = %q, expected true; op: %#v", ops[1].Options["dynamic"], ops[1])
	}
	if ops[1].Options["queryBindingCount"] != "0" {
		t.Fatalf("queryBindingCount = %q, expected 0; op: %#v", ops[1].Options["queryBindingCount"], ops[1])
	}
	if len(ops[1].SecondaryData) != 2 || ops[1].SecondaryData[0] != "40" || ops[1].SecondaryData[1] != "50" {
		t.Fatalf("RETURN QUERY EXECUTE bindings = %#v, expected USING params", ops[1].SecondaryData)
	}
}

func TestParseReturnNextOperations(t *testing.T) {
	ops, err := Parse(`CREATE FUNCTION func2(n INT) RETURNS SETOF INT
		LANGUAGE plpgsql
		AS $$
		BEGIN
			RETURN NEXT n;
			RETURN NEXT n + 1;
			RETURN;
		END;
		$$;`)
	if err != nil {
		t.Fatal(err)
	}
	if len(ops) != 5 {
		t.Fatalf("expected 5 operations, found %d: %#v", len(ops), ops)
	}
	for i, expected := range []OpCode{OpCode_ScopeBegin, OpCode_ReturnNext, OpCode_ReturnNext, OpCode_Return, OpCode_ScopeEnd} {
		if ops[i].OpCode != expected {
			t.Fatalf("operation %d opcode = %d, expected %d; all ops: %#v", i, ops[i].OpCode, expected, ops)
		}
	}
	for i := 1; i <= 2; i++ {
		if len(ops[i].SecondaryData) != 1 || ops[i].SecondaryData[0] != "n" {
			t.Fatalf("RETURN NEXT operation %d bindings = %#v, expected n", i, ops[i].SecondaryData)
		}
	}
}

func TestParseReturnExpressionFallback(t *testing.T) {
	ops, err := Parse(`CREATE FUNCTION func2() RETURNS INT
		LANGUAGE plpgsql
		AS $$
		BEGIN
			RETURN 5;
		END;
		$$;`)
	if err != nil {
		t.Fatal(err)
	}
	for _, op := range ops {
		if op.OpCode == OpCode_Return {
			if strings.ReplaceAll(op.PrimaryData, " ", "") != "SELECT5;" {
				t.Fatalf("RETURN primary data = %q, expected SELECT 5; op: %#v", op.PrimaryData, op)
			}
			return
		}
	}
	t.Fatalf("expected return operation; ops: %#v", ops)
}

func TestParseDynamicExecuteExpressionBindings(t *testing.T) {
	ops, err := Parse(`CREATE FUNCTION test_block() RETURNS void AS $$
		DECLARE
			target_table TEXT := 'do_dynamic_target';
			target_id INT := 7;
			new_label TEXT := 'made by execute';
		BEGIN
			EXECUTE format('UPDATE %I SET label = $2 WHERE id = $1 OR id = $1', target_table)
				USING target_id, new_label;
		END;
	$$ LANGUAGE plpgsql;`)
	if err != nil {
		t.Fatal(err)
	}

	var executeOp *InterpreterOperation
	for i := range ops {
		if ops[i].OpCode == OpCode_Execute {
			executeOp = &ops[i]
			break
		}
	}
	if executeOp == nil {
		t.Fatalf("expected dynamic execute operation, found %#v", ops)
	}
	if executeOp.Options["dynamic"] != "true" {
		t.Fatalf("dynamic option = %q, expected true; op: %#v", executeOp.Options["dynamic"], executeOp)
	}
	if executeOp.Options["queryBindingCount"] != "1" {
		t.Fatalf("queryBindingCount = %q, expected 1; op: %#v", executeOp.Options["queryBindingCount"], executeOp)
	}
	if executeOp.PrimaryData != "format ( 'UPDATE %I SET label = $2 WHERE id = $1 OR id = $1' , $1 ) " {
		t.Fatalf("primary data = %q", executeOp.PrimaryData)
	}
	if len(executeOp.SecondaryData) != 3 || executeOp.SecondaryData[0] != "target_table" || executeOp.SecondaryData[1] != "target_id" || executeOp.SecondaryData[2] != "new_label" {
		t.Fatalf("secondary data = %#v", executeOp.SecondaryData)
	}
}

func TestParseDynamicForExecuteExpressionBindings(t *testing.T) {
	ops, err := Parse(`CREATE FUNCTION test_block() RETURNS void AS $$
		DECLARE
			query_text TEXT := 'SELECT $1::int AS id';
			target_id INT := 7;
			value_seen INT;
		BEGIN
			FOR value_seen IN EXECUTE query_text USING target_id LOOP
				PERFORM value_seen;
			END LOOP;
		END;
	$$ LANGUAGE plpgsql;`)
	if err != nil {
		t.Fatal(err)
	}

	var initOp *InterpreterOperation
	for i := range ops {
		if ops[i].OpCode == OpCode_ForQueryInit {
			initOp = &ops[i]
			break
		}
	}
	if initOp == nil {
		t.Fatalf("expected dynamic FOR query init operation, found %#v", ops)
	}
	if initOp.Options["dynamic"] != "true" {
		t.Fatalf("dynamic option = %q, expected true; op: %#v", initOp.Options["dynamic"], initOp)
	}
	if initOp.Options["queryBindingCount"] != "1" {
		t.Fatalf("queryBindingCount = %q, expected 1; op: %#v", initOp.Options["queryBindingCount"], initOp)
	}
	if initOp.PrimaryData != "$1 " {
		t.Fatalf("primary data = %q", initOp.PrimaryData)
	}
	if len(initOp.SecondaryData) != 2 || initOp.SecondaryData[0] != "query_text" || initOp.SecondaryData[1] != "target_id" {
		t.Fatalf("secondary data = %#v", initOp.SecondaryData)
	}
}

func TestParseDynamicExecuteIntoStrict(t *testing.T) {
	ops, err := Parse(`CREATE FUNCTION test_block() RETURNS void AS $$
		DECLARE
			got_id INT;
		BEGIN
			EXECUTE 'SELECT 1' INTO STRICT got_id;
		END;
	$$ LANGUAGE plpgsql;`)
	if err != nil {
		t.Fatal(err)
	}

	var executeOp *InterpreterOperation
	for i := range ops {
		if ops[i].OpCode == OpCode_Execute {
			executeOp = &ops[i]
			break
		}
	}
	if executeOp == nil {
		t.Fatalf("expected dynamic execute operation, found %#v", ops)
	}
	if executeOp.Target != "got_id" {
		t.Fatalf("target = %q", executeOp.Target)
	}
	if executeOp.Options["strict"] != "true" {
		t.Fatalf("strict option = %q, expected true; op: %#v", executeOp.Options["strict"], executeOp)
	}
}

func TestParseSelectIntoStrict(t *testing.T) {
	ops, err := Parse(`CREATE FUNCTION test_block() RETURNS void AS $$
		DECLARE
			got_id INT;
		BEGIN
			SELECT id INTO STRICT got_id FROM items WHERE id = 1;
		END;
	$$ LANGUAGE plpgsql;`)
	if err != nil {
		t.Fatal(err)
	}

	var executeOp *InterpreterOperation
	for i := range ops {
		if ops[i].OpCode == OpCode_Execute {
			executeOp = &ops[i]
			break
		}
	}
	if executeOp == nil {
		t.Fatalf("expected execute operation, found %#v", ops)
	}
	if executeOp.Target != "got_id" {
		t.Fatalf("target = %q", executeOp.Target)
	}
	if executeOp.Options["strict"] != "true" {
		t.Fatalf("strict option = %q, expected true; op: %#v", executeOp.Options["strict"], executeOp)
	}
}

func TestParseDmlReturningIntoMarksMultiRowCheck(t *testing.T) {
	ops, err := Parse(`CREATE FUNCTION test_block() RETURNS void AS $$
		DECLARE
			got_id INT;
		BEGIN
			INSERT INTO items VALUES (1), (2) RETURNING id INTO got_id;
		END;
	$$ LANGUAGE plpgsql;`)
	if err != nil {
		t.Fatal(err)
	}

	var executeOp *InterpreterOperation
	for i := range ops {
		if ops[i].OpCode == OpCode_Execute {
			executeOp = &ops[i]
			break
		}
	}
	if executeOp == nil {
		t.Fatalf("expected execute operation, found %#v", ops)
	}
	if executeOp.Target != "got_id" {
		t.Fatalf("target = %q", executeOp.Target)
	}
	if executeOp.Options[dmlReturningIntoOption] != "true" {
		t.Fatalf("dmlReturningInto option = %q, expected true; op: %#v", executeOp.Options[dmlReturningIntoOption], executeOp)
	}
}

func TestParseProcedureTransactionControl(t *testing.T) {
	ops, err := Parse(`CREATE PROCEDURE test_proc()
		LANGUAGE plpgsql
		AS $$
		BEGIN
			COMMIT;
			ROLLBACK;
		END;
		$$;`)
	if err != nil {
		t.Fatal(err)
	}

	var statements []string
	var commitOp *InterpreterOperation
	for i := range ops {
		if ops[i].OpCode == OpCode_Execute {
			if strings.TrimSpace(ops[i].PrimaryData) == "COMMIT" {
				commitOp = &ops[i]
			}
			statements = append(statements, strings.TrimSpace(ops[i].PrimaryData))
		}
	}
	if len(statements) != 2 || statements[0] != "COMMIT" || statements[1] != "ROLLBACK" {
		t.Fatalf("transaction statements = %#v; ops: %#v", statements, ops)
	}
	if commitOp == nil || commitOp.Options[transactionControlNoop] != "true" {
		t.Fatalf("expected COMMIT to be marked as a no-op; op: %#v", commitOp)
	}
}

func TestParseForeachArrayLoop(t *testing.T) {
	ops, err := Parse(`CREATE FUNCTION test_block(values_in INT[]) RETURNS void AS $$
		DECLARE
			value_seen INT;
		BEGIN
			FOREACH value_seen IN ARRAY values_in LOOP
				PERFORM value_seen;
			END LOOP;
		END;
	$$ LANGUAGE plpgsql;`)
	if err != nil {
		t.Fatal(err)
	}

	var initOp *InterpreterOperation
	for i := range ops {
		if ops[i].OpCode == OpCode_ForQueryInit {
			initOp = &ops[i]
			break
		}
	}
	if initOp == nil {
		t.Fatalf("expected FOREACH to lower to query-loop init; ops: %#v", ops)
	}
	if initOp.PrimaryData != "SELECT unnest ( $1 ) " {
		t.Fatalf("FOREACH query = %q; op: %#v", initOp.PrimaryData, initOp)
	}
	if len(initOp.SecondaryData) != 1 || initOp.SecondaryData[0] != "values_in" {
		t.Fatalf("FOREACH bindings = %#v; op: %#v", initOp.SecondaryData, initOp)
	}
}

func TestParseForeachSliceArrayLoop(t *testing.T) {
	ops, err := Parse(`CREATE FUNCTION test_block(values_in INT[]) RETURNS void AS $$
		DECLARE
			row_slice INT[];
		BEGIN
			FOREACH row_slice SLICE 1 IN ARRAY values_in LOOP
				PERFORM row_slice;
			END LOOP;
		END;
	$$ LANGUAGE plpgsql;`)
	if err != nil {
		t.Fatal(err)
	}

	var initOp *InterpreterOperation
	for i := range ops {
		if ops[i].OpCode == OpCode_ForEachInit {
			initOp = &ops[i]
			break
		}
	}
	if initOp == nil {
		t.Fatalf("expected FOREACH SLICE to lower to foreach init; ops: %#v", ops)
	}
	if strings.TrimSpace(initOp.PrimaryData) != "$1" {
		t.Fatalf("FOREACH SLICE expression = %q; op: %#v", initOp.PrimaryData, initOp)
	}
	if len(initOp.SecondaryData) != 1 || initOp.SecondaryData[0] != "values_in" {
		t.Fatalf("FOREACH SLICE bindings = %#v; op: %#v", initOp.SecondaryData, initOp)
	}
	if initOp.Options["slice"] != "1" || initOp.Options["target"] != "row_slice" {
		t.Fatalf("FOREACH SLICE options = %#v; op: %#v", initOp.Options, initOp)
	}
}

func TestParseDynamicExecuteIntoRecord(t *testing.T) {
	ops, err := Parse(`CREATE FUNCTION test_block() RETURNS text AS $$
		DECLARE
			got_row RECORD;
		BEGIN
			EXECUTE 'SELECT 10 AS id, ''dynamic'' AS label' INTO got_row;
			RETURN got_row.id::TEXT || ':' || got_row.label;
		END;
	$$ LANGUAGE plpgsql;`)
	if err != nil {
		t.Fatal(err)
	}

	var executeOp *InterpreterOperation
	for i := range ops {
		if ops[i].OpCode == OpCode_Execute {
			executeOp = &ops[i]
			break
		}
	}
	if executeOp == nil {
		t.Fatalf("expected dynamic execute operation, found %#v", ops)
	}
	if executeOp.Target != "got_row" {
		t.Fatalf("target = %q", executeOp.Target)
	}
	if executeOp.Options["dynamic"] != "true" {
		t.Fatalf("dynamic option = %q, expected true; op: %#v", executeOp.Options["dynamic"], executeOp)
	}
}

func TestParseGetDiagnosticsRowCount(t *testing.T) {
	ops, err := Parse(`CREATE FUNCTION test_block() RETURNS void AS $$
		DECLARE
			affected INT;
		BEGIN
			GET DIAGNOSTICS affected = ROW_COUNT;
		END;
	$$ LANGUAGE plpgsql;`)
	if err != nil {
		t.Fatal(err)
	}

	var getOp *InterpreterOperation
	for i := range ops {
		if ops[i].OpCode == OpCode_Get {
			getOp = &ops[i]
			break
		}
	}
	if getOp == nil {
		t.Fatalf("expected GET DIAGNOSTICS operation, found %#v", ops)
	}
	if getOp.Target != "affected" {
		t.Fatalf("target = %q, expected affected; op: %#v", getOp.Target, getOp)
	}
	if getOp.PrimaryData != "ROW_COUNT" {
		t.Fatalf("diagnostic item = %q, expected ROW_COUNT; op: %#v", getOp.PrimaryData, getOp)
	}
}

func TestParseGetDiagnosticsPgContext(t *testing.T) {
	ops, err := Parse(`CREATE FUNCTION test_block() RETURNS void AS $$
		DECLARE
			context TEXT;
		BEGIN
			GET DIAGNOSTICS context = PG_CONTEXT;
		END;
	$$ LANGUAGE plpgsql;`)
	if err != nil {
		t.Fatal(err)
	}

	var getOp *InterpreterOperation
	for i := range ops {
		if ops[i].OpCode == OpCode_Get {
			getOp = &ops[i]
			break
		}
	}
	if getOp == nil {
		t.Fatalf("expected GET DIAGNOSTICS operation, found %#v", ops)
	}
	if getOp.Target != "context" {
		t.Fatalf("target = %q, expected context; op: %#v", getOp.Target, getOp)
	}
	if getOp.PrimaryData != "PG_CONTEXT" {
		t.Fatalf("diagnostic item = %q, expected PG_CONTEXT; op: %#v", getOp.PrimaryData, getOp)
	}
	if getOp.Options["lineNumber"] == "" {
		t.Fatalf("expected lineNumber option on PG_CONTEXT operation; op: %#v", getOp)
	}
}

func TestParseGetDiagnosticsPgRoutineOid(t *testing.T) {
	ops, err := Parse(`CREATE FUNCTION test_block() RETURNS void AS $$
		DECLARE
			routine_oid oid;
		BEGIN
			GET DIAGNOSTICS routine_oid = PG_ROUTINE_OID;
		END;
	$$ LANGUAGE plpgsql;`)
	if err != nil {
		t.Fatal(err)
	}

	var getOp *InterpreterOperation
	for i := range ops {
		if ops[i].OpCode == OpCode_Get {
			getOp = &ops[i]
			break
		}
	}
	if getOp == nil {
		t.Fatalf("expected GET DIAGNOSTICS operation, found %#v", ops)
	}
	if getOp.Target != "routine_oid" {
		t.Fatalf("target = %q, expected routine_oid; op: %#v", getOp.Target, getOp)
	}
	if getOp.PrimaryData != "PG_ROUTINE_OID" {
		t.Fatalf("diagnostic item = %q, expected PG_ROUTINE_OID; op: %#v", getOp.PrimaryData, getOp)
	}
	if getOp.Options["lineNumber"] == "" {
		t.Fatalf("expected lineNumber option on PG_ROUTINE_OID operation; op: %#v", getOp)
	}
}

func TestParseExceptionBlockStackedDiagnostics(t *testing.T) {
	ops, err := Parse(`CREATE FUNCTION test_block() RETURNS void AS $$
		DECLARE
			message TEXT;
			detail TEXT;
		BEGIN
			RAISE EXCEPTION 'custom exception'
				USING DETAIL = 'some detail';
		EXCEPTION WHEN OTHERS THEN
			GET STACKED DIAGNOSTICS
				message = MESSAGE_TEXT,
				detail = PG_EXCEPTION_DETAIL;
		END;
	$$ LANGUAGE plpgsql;`)
	if err != nil {
		t.Fatal(err)
	}

	var exceptionOp *InterpreterOperation
	stackedItems := map[string]bool{}
	for i := range ops {
		if ops[i].OpCode == OpCode_Exception {
			exceptionOp = &ops[i]
		}
		if ops[i].OpCode == OpCode_Get && ops[i].Options["stacked"] == "true" {
			stackedItems[ops[i].PrimaryData] = true
		}
	}
	if exceptionOp == nil {
		t.Fatalf("expected exception operation, found %#v", ops)
	}
	if exceptionOp.Options["handlerConditions"] != "others" {
		t.Fatalf("handler conditions = %q, expected others; op: %#v", exceptionOp.Options["handlerConditions"], exceptionOp)
	}
	for _, item := range []string{"MESSAGE_TEXT", "PG_EXCEPTION_DETAIL"} {
		if !stackedItems[item] {
			t.Fatalf("missing stacked diagnostic item %s; ops: %#v", item, ops)
		}
	}
}

func TestParseExceptionBlockSpecialVariables(t *testing.T) {
	ops, err := Parse(`CREATE FUNCTION test_block() RETURNS text AS $$
		BEGIN
			RAISE EXCEPTION 'custom exception' USING ERRCODE = '22012';
		EXCEPTION WHEN OTHERS THEN
			RETURN SQLSTATE || ':' || SQLERRM;
		END;
	$$ LANGUAGE plpgsql;`)
	if err != nil {
		t.Fatal(err)
	}

	var returnOp *InterpreterOperation
	for i := range ops {
		if ops[i].OpCode == OpCode_Return {
			returnOp = &ops[i]
			break
		}
	}
	if returnOp == nil {
		t.Fatalf("expected return operation, found %#v", ops)
	}
	expectedVars := []string{"SQLSTATE", "SQLERRM"}
	if len(returnOp.SecondaryData) != len(expectedVars) {
		t.Fatalf("referenced variables = %#v, expected %#v; op: %#v", returnOp.SecondaryData, expectedVars, returnOp)
	}
	for i, expectedVar := range expectedVars {
		if returnOp.SecondaryData[i] != expectedVar {
			t.Fatalf("referenced variable %d = %q, expected %q; op: %#v", i, returnOp.SecondaryData[i], expectedVar, returnOp)
		}
	}
}

func TestParseExceptionBlockMultipleHandlers(t *testing.T) {
	ops, err := Parse(`CREATE FUNCTION test_block() RETURNS void AS $$
		DECLARE
			message TEXT;
		BEGIN
			RAISE EXCEPTION 'custom exception'
				USING ERRCODE = 'unique_violation';
		EXCEPTION
			WHEN division_by_zero THEN
				message := 'wrong handler';
			WHEN unique_violation THEN
				GET STACKED DIAGNOSTICS message = MESSAGE_TEXT;
			WHEN OTHERS THEN
				message := 'fallback handler';
		END;
	$$ LANGUAGE plpgsql;`)
	if err != nil {
		t.Fatal(err)
	}

	var exceptionOp *InterpreterOperation
	for i := range ops {
		if ops[i].OpCode == OpCode_Exception {
			exceptionOp = &ops[i]
			break
		}
	}
	if exceptionOp == nil {
		t.Fatalf("expected exception operation, found %#v", ops)
	}
	if exceptionOp.Options["handlerCount"] != "3" {
		t.Fatalf("handler count = %q, expected 3; op: %#v", exceptionOp.Options["handlerCount"], exceptionOp)
	}
	for _, key := range []string{"handlerConditions.0", "handlerConditions.1", "handlerConditions.2"} {
		if exceptionOp.Options[key] == "" {
			t.Fatalf("missing %s; op: %#v", key, exceptionOp)
		}
	}
	if exceptionOp.Options["handlerConditions.0"] != "division_by_zero" {
		t.Fatalf("first handler conditions = %q, expected division_by_zero", exceptionOp.Options["handlerConditions.0"])
	}
	if exceptionOp.Options["handlerConditions.1"] != "unique_violation" {
		t.Fatalf("second handler conditions = %q, expected unique_violation", exceptionOp.Options["handlerConditions.1"])
	}
	if exceptionOp.Options["handlerConditions.2"] != "others" {
		t.Fatalf("third handler conditions = %q, expected others", exceptionOp.Options["handlerConditions.2"])
	}
}

func TestParseExceptionBlockStackedObjectDiagnostics(t *testing.T) {
	ops, err := Parse(`CREATE FUNCTION test_block() RETURNS void AS $$
		DECLARE
			column_name_text TEXT;
			constraint_name_text TEXT;
			datatype_name_text TEXT;
			table_name_text TEXT;
			schema_name_text TEXT;
		BEGIN
			RAISE EXCEPTION 'object metadata'
				USING COLUMN = 'amount',
					CONSTRAINT = 'amount_positive',
					DATATYPE = 'numeric',
					TABLE = 'invoice_lines',
					SCHEMA = 'public';
		EXCEPTION WHEN OTHERS THEN
			GET STACKED DIAGNOSTICS
				column_name_text = COLUMN_NAME,
				constraint_name_text = CONSTRAINT_NAME,
				datatype_name_text = PG_DATATYPE_NAME,
				table_name_text = TABLE_NAME,
				schema_name_text = SCHEMA_NAME;
		END;
	$$ LANGUAGE plpgsql;`)
	if err != nil {
		t.Fatal(err)
	}

	stackedItems := map[string]bool{}
	for i := range ops {
		if ops[i].OpCode == OpCode_Get && ops[i].Options["stacked"] == "true" {
			stackedItems[ops[i].PrimaryData] = true
		}
	}
	for _, item := range []string{"COLUMN_NAME", "CONSTRAINT_NAME", "PG_DATATYPE_NAME", "TABLE_NAME", "SCHEMA_NAME"} {
		if !stackedItems[item] {
			t.Fatalf("missing stacked object diagnostic item %s; ops: %#v", item, ops)
		}
	}
}
