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
			item RECORD;
		BEGIN
			FOR item IN SELECT 1 AS id LOOP
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
