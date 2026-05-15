// Copyright 2025 Dolthub, Inc.
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

func TestCreateProcedureLanguagePlpgsql(t *testing.T) {
	RunScripts(
		t,
		[]ScriptTest{
			{
				Name: "Simple example",
				SetUpScript: []string{
					`CREATE TABLE test (v1 INT8);`,
					`CREATE PROCEDURE example(input INT8) AS $$
							BEGIN
								INSERT INTO test VALUES (input);
							END;
							$$ LANGUAGE 'plpgsql';`,
				},
				Assertions: []ScriptTestAssertion{
					{
						Query: "CALL example(1);", PostgresOracle: ScriptTestPostgresOracle{ID: "create-procedure-plpgsql-test-testcreateprocedurelanguageplpgsql-0001-call-example-1"},
					},
					{
						Query: "CALL example('2');", PostgresOracle: ScriptTestPostgresOracle{ID: "create-procedure-plpgsql-test-testcreateprocedurelanguageplpgsql-0002-call-example-2"},
					},
					{
						Query: "SELECT * FROM test;", PostgresOracle: ScriptTestPostgresOracle{ID: "create-procedure-plpgsql-test-testcreateprocedurelanguageplpgsql-0003-select-*-from-test"},
					},
				},
			},
			{
				Name: "WHILE Label",
				SetUpScript: []string{
					`CREATE TABLE test (v1 INT8);`,
					`CREATE PROCEDURE interpreted_while_label(input INT4) AS $$
			DECLARE
				counter INT4;
			BEGIN
				<<while_label>>
				WHILE input < 1000 LOOP
					input := input + 1;
					counter := counter + 1;
					IF counter >= 10 THEN
						EXIT while_label;
					END IF;
				END LOOP;
				INSERT INTO test VALUES (input);
			END;
			$$ LANGUAGE plpgsql;`,
				},
				Assertions: []ScriptTestAssertion{
					{
						Query: "CALL interpreted_while_label(42);", PostgresOracle: ScriptTestPostgresOracle{ID: "create-procedure-plpgsql-test-testcreateprocedurelanguageplpgsql-0004-call-interpreted_while_label-42"},
					},
					{
						Query: "SELECT * FROM test;", PostgresOracle: ScriptTestPostgresOracle{ID: "create-procedure-plpgsql-test-testcreateprocedurelanguageplpgsql-0005-select-*-from-test"},
					},
				},
			},
			{
				Name: "Overloading",
				SetUpScript: []string{
					`CREATE TABLE test (v1 TEXT);`,
					`CREATE PROCEDURE interpreted_overload(input TEXT) AS $$
			DECLARE
				var1 TEXT;
			BEGIN
				IF length(input) > 3 THEN
					var1 := input || '_long';
				ELSE
					var1 := input;
				END IF;
				INSERT INTO test VALUES (var1);
			END;
			$$ LANGUAGE plpgsql;`,
					`CREATE PROCEDURE interpreted_overload(input INT4) AS $$
			DECLARE
				var1 INT4;
			BEGIN
				IF input > 3 THEN
					var1 := -input;
				ELSE
					var1 := input;
				END IF;
				INSERT INTO test VALUES (var1::text);
			END;
			$$ LANGUAGE plpgsql;`},
				Assertions: []ScriptTestAssertion{
					{
						Query: "CALL interpreted_overload('abc');", PostgresOracle: ScriptTestPostgresOracle{ID: "create-procedure-plpgsql-test-testcreateprocedurelanguageplpgsql-0006-call-interpreted_overload-abc"},
					},
					{
						Query: "CALL interpreted_overload('abcd');", PostgresOracle: ScriptTestPostgresOracle{ID: "create-procedure-plpgsql-test-testcreateprocedurelanguageplpgsql-0007-call-interpreted_overload-abcd"},
					},
					{
						Query: "CALL interpreted_overload(3);", PostgresOracle: ScriptTestPostgresOracle{ID: "create-procedure-plpgsql-test-testcreateprocedurelanguageplpgsql-0008-call-interpreted_overload-3"},
					},
					{
						Query: "CALL interpreted_overload(4);", PostgresOracle: ScriptTestPostgresOracle{ID: "create-procedure-plpgsql-test-testcreateprocedurelanguageplpgsql-0009-call-interpreted_overload-4"},
					},
					{
						Query: "SELECT * FROM test;", PostgresOracle: ScriptTestPostgresOracle{ID: "create-procedure-plpgsql-test-testcreateprocedurelanguageplpgsql-0010-select-*-from-test"},
					},
				},
			},
			{
				Name: "DECLARE variable with default value of literal value or parameter reference",
				SetUpScript: []string{
					`CREATE TABLE t (a int, b text);`,
				},
				Assertions: []ScriptTestAssertion{
					{
						Query: `CREATE OR REPLACE PROCEDURE m (x text, y text, e int) LANGUAGE plpgsql AS $$
			declare
			  xx text := x;
			  yy text := y;
			  zz int := e;
			begin
			  insert into t values (zz, xx || yy);
			end
			$$;`,
					},
					{
						Query: "CALL m('a', 'B', 1);", PostgresOracle: ScriptTestPostgresOracle{ID: "create-procedure-plpgsql-test-testcreateprocedurelanguageplpgsql-0065-call-m-a-b-1"},
					},
					{
						Query: "SELECT * FROM t", PostgresOracle: ScriptTestPostgresOracle{ID: "create-procedure-plpgsql-test-testcreateprocedurelanguageplpgsql-0066-select-*-from-t"},
					},
				},
			},
			{
				Name: "use nested block statements and call statement in procedure body",
				SetUpScript: []string{
					`CREATE TABLE tbl (a int, b text);`,
					`CREATE PROCEDURE add_value(IN a int, IN b text)
			            LANGUAGE plpgsql
			            AS $$
			        BEGIN
			            INSERT INTO tbl VALUES (a, b);
			        END;
					$$;`,
				},
				Assertions: []ScriptTestAssertion{
					{
						Query: `CREATE PROCEDURE check_and_add(IN i int, IN t text)
			            LANGUAGE plpgsql
			            AS $$
					DECLARE d text := t;
			        BEGIN
			            IF LENGTH(t) < 6 THEN
			                d = t || ' is too short';
			            END IF;

			            BEGIN
			                CALL add_value(i, d);
			            END;
			        END;
			        $$;`,
					},
					{
						Query: "CALL check_and_add(1, 'hi');", PostgresOracle: ScriptTestPostgresOracle{ID: "create-procedure-plpgsql-test-testcreateprocedurelanguageplpgsql-0067-call-check_and_add-1-hi"},
					},
					{
						Query: "CALL check_and_add(3, 'hellooo');", PostgresOracle: ScriptTestPostgresOracle{ID: "create-procedure-plpgsql-test-testcreateprocedurelanguageplpgsql-0068-call-check_and_add-3-hellooo"},
					},
					{
						Query: "SELECT * FROM tbl", PostgresOracle: ScriptTestPostgresOracle{ID: "create-procedure-plpgsql-test-testcreateprocedurelanguageplpgsql-0069-select-*-from-tbl"},
					},
				},
			},
		},
	)
}

func TestCreateProcedureLanguagePlpgsqlPostgresOraclePrefix(t *testing.T) {
	RunScripts(
		t,
		[]ScriptTest{
			{
				Name: "Branching",
				SetUpScript: []string{
					`CREATE TABLE test(v1 INT4, v2 INT4);`,
				},
				Assertions: []ScriptTestAssertion{
					{
						Query: `CREATE PROCEDURE interpreted_branch(input INT4) AS $$
					BEGIN
						DELETE FROM test WHERE v1 = 1;
						INSERT INTO test VALUES (1, input + 100);
					END;
					$$ LANGUAGE plpgsql;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-procedure-plpgsql-test-testcreateprocedurelanguageplpgsql-0011-create-procedure-interpreted_branch-input-int4"},
					},
					{
						Query: "CALL interpreted_branch(4);", PostgresOracle: ScriptTestPostgresOracle{ID: "create-procedure-plpgsql-test-testcreateprocedurelanguageplpgsql-0012-call-interpreted_branch-4"},
					},
					{
						Query: "SELECT * FROM test;", PostgresOracle: ScriptTestPostgresOracle{ID: "create-procedure-plpgsql-test-testcreateprocedurelanguageplpgsql-0013-select-*-from-test"},
					},
					{
						Query: "DELETE FROM test WHERE v1 = 1;", PostgresOracle: ScriptTestPostgresOracle{ID: "create-procedure-plpgsql-test-testcreateprocedurelanguageplpgsql-0014-delete-from-test-where-v1"},
					},
				},
			},
		},
	)
}
