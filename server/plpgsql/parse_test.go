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

import "testing"

func TestParseDeclareAliasesArraysAndRecords(t *testing.T) {
	ops, err := Parse(`CREATE FUNCTION test_block() RETURNS void AS $$
		DECLARE
			labels TEXT[] := '{alpha,beta,gamma}';
			target_id INT;
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
		"labels":    "text[] ",
		"target_id": "int",
		"item":      "record",
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
