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

// TestDoBlockProbe pins where PG `DO $$ ... $$` anonymous code blocks
// stand in doltgresql today. pg_dump emits these for matview/state
// repair, Alembic upgrade scripts wrap conditional DDL in them, and
// many ORM init scripts use the IF-NOT-EXISTS pattern through DO. Per
// the Schema/DDL TODO in
// docs/app-compatibility-checklist.md.
func TestDoBlockProbe(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "DO block runs conditional CREATE",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DO $$
						BEGIN
							IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'created_by_do') THEN
								CREATE TABLE created_by_do (id INT PRIMARY KEY);
							END IF;
						END;
					$$;`,
				},
				{
					Query: `INSERT INTO created_by_do VALUES (1);`,
				},
				{
					Query:    `SELECT count(*)::text FROM created_by_do;`,
					Expected: []sql.Row{{"1"}},
				},
			},
		},
		{
			Name:        "DO block defaults to plpgsql and can raise notice",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DO $$
						BEGIN
							RAISE NOTICE 'hello from DO block';
						END;
					$$;`,
				},
			},
		},
		{
			Name:        "DO block rejects unsupported language",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DO LANGUAGE sql $$
						SELECT 1;
					$$;`,
					ExpectedErr: `DO only supports LANGUAGE plpgsql`,
				},
			},
		},
	})
}

func TestDoBlockPlpgsqlInterpreterCoverage(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DO block runs declarations loops and DML",
			SetUpScript: []string{
				`CREATE TABLE do_loop_log (id INT PRIMARY KEY, label TEXT NOT NULL);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DO $$
						DECLARE
							labels TEXT[] := '{alpha,beta,gamma}';
							i INT;
						BEGIN
							FOR i IN 1..3 LOOP
								INSERT INTO do_loop_log VALUES (i, labels[i]);
							END LOOP;
						END;
					$$;`,
				},
				{
					Query: `SELECT array_to_string(array_agg(label ORDER BY id), ',') FROM do_loop_log;`,
					Expected: []sql.Row{
						{"alpha,beta,gamma"},
					},
				},
			},
		},
		{
			Name: "DO block runs SELECT INTO FOUND query loops and PERFORM",
			SetUpScript: []string{
				`CREATE SEQUENCE do_perform_seq;`,
				`CREATE TABLE do_items (id INT PRIMARY KEY, label TEXT NOT NULL, touched BOOL NOT NULL DEFAULT false);`,
				`INSERT INTO do_items VALUES (1, 'one', false), (2, 'two', false), (3, 'three', false);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DO $$
						DECLARE
							target_id INT;
							item RECORD;
						BEGIN
							SELECT id INTO target_id FROM do_items WHERE label = 'two';
							IF FOUND THEN
								UPDATE do_items SET touched = true WHERE id = target_id;
							END IF;

							FOR item IN SELECT id FROM do_items WHERE touched = false ORDER BY id LOOP
								UPDATE do_items SET label = label || '-seen' WHERE id = item.id;
							END LOOP;

							PERFORM nextval('do_perform_seq');
						END;
					$$;`,
				},
				{
					Query: `SELECT id, label, touched FROM do_items ORDER BY id;`,
					Expected: []sql.Row{
						{1, "one-seen", "f"},
						{2, "two", "t"},
						{3, "three-seen", "f"},
					},
				},
				{
					Query:    `SELECT nextval('do_perform_seq');`,
					Expected: []sql.Row{{2}},
				},
			},
		},
		{
			Name: "DO block runs dynamic EXECUTE format with USING parameters",
			SetUpScript: []string{
				`CREATE TABLE do_dynamic_target (id INT PRIMARY KEY, label TEXT);`,
				`INSERT INTO do_dynamic_target VALUES (7, 'before execute');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DO $$
						DECLARE
							target_table TEXT := 'do_dynamic_target';
							target_id INT := 7;
							new_label TEXT := 'made by execute';
						BEGIN
							EXECUTE format('UPDATE %I SET label = $2 WHERE id = $1 OR id = $1', target_table)
								USING target_id, new_label;
						END;
					$$;`,
				},
				{
					Query:    `SELECT label FROM do_dynamic_target WHERE id = 7;`,
					Expected: []sql.Row{{"made by execute"}},
				},
			},
		},
		{
			Name: "DO block evaluates dynamic EXECUTE USING expressions",
			SetUpScript: []string{
				`CREATE TABLE do_dynamic_expr_target (id INT PRIMARY KEY, label TEXT);`,
				`INSERT INTO do_dynamic_expr_target VALUES (7, 'before execute');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DO $$
						DECLARE
							target_table TEXT := 'do_dynamic_expr_target';
						BEGIN
							EXECUTE format('UPDATE %I SET label = $2 WHERE id = $1', target_table)
								USING 6 + 1, lower('MADE BY LITERAL');
						END;
					$$;`,
				},
				{
					Query:    `SELECT label FROM do_dynamic_expr_target WHERE id = 7;`,
					Expected: []sql.Row{{"made by literal"}},
				},
			},
		},
		{
			Name:        "DO block runs dynamic EXECUTE format DDL",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DO $$
						DECLARE
							target_table TEXT := 'do_dynamic_ddl_target';
						BEGIN
							EXECUTE format('CREATE TABLE %I (id INT PRIMARY KEY, label TEXT)', target_table);
						END;
					$$;`,
				},
				{
					Query: `INSERT INTO do_dynamic_ddl_target VALUES (1, 'created by dynamic ddl');`,
				},
				{
					Query:    `SELECT label FROM do_dynamic_ddl_target WHERE id = 1;`,
					Expected: []sql.Row{{"created by dynamic ddl"}},
				},
			},
		},
		{
			Name: "DO block assigns dynamic EXECUTE INTO results",
			SetUpScript: []string{
				`CREATE TABLE do_dynamic_into_source (id INT PRIMARY KEY, label TEXT);`,
				`CREATE TABLE do_dynamic_into_seen (id INT PRIMARY KEY, label TEXT);`,
				`INSERT INTO do_dynamic_into_source VALUES (7, 'from execute into');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DO $$
						DECLARE
							target_table TEXT := 'do_dynamic_into_source';
							got_id INT;
							got_label TEXT;
						BEGIN
							EXECUTE format('SELECT id, label FROM %I WHERE id = $1', target_table)
								INTO got_id, got_label
								USING 7;
							INSERT INTO do_dynamic_into_seen VALUES (got_id, got_label);
						END;
					$$;`,
				},
				{
					Query:    `SELECT label FROM do_dynamic_into_seen WHERE id = 7;`,
					Expected: []sql.Row{{"from execute into"}},
				},
			},
		},
		{
			Name: "DO block handles dynamic EXECUTE INTO row count semantics",
			SetUpScript: []string{
				`CREATE TABLE do_dynamic_into_many (id INT PRIMARY KEY, label TEXT);`,
				`CREATE TABLE do_dynamic_into_many_seen (id INT PRIMARY KEY, label TEXT);`,
				`INSERT INTO do_dynamic_into_many VALUES (1, 'first row'), (2, 'second row');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DO $$
						DECLARE
							got_id INT;
							got_label TEXT;
						BEGIN
							EXECUTE 'SELECT id, label FROM do_dynamic_into_many ORDER BY id'
								INTO got_id, got_label;
							INSERT INTO do_dynamic_into_many_seen VALUES (got_id, got_label);
						END;
					$$;`,
				},
				{
					Query:    `SELECT label FROM do_dynamic_into_many_seen WHERE id = 1;`,
					Expected: []sql.Row{{"first row"}},
				},
				{
					Query: `DO $$
						DECLARE
							got_id INT;
						BEGIN
							EXECUTE 'SELECT id FROM do_dynamic_into_many WHERE id = 99'
								INTO STRICT got_id;
						END;
					$$;`,
					ExpectedErr: `query returned no rows`,
				},
				{
					Query: `DO $$
						DECLARE
							got_id INT;
						BEGIN
							EXECUTE 'SELECT id FROM do_dynamic_into_many ORDER BY id'
								INTO STRICT got_id;
						END;
					$$;`,
					ExpectedErr: `query returned more than one row`,
				},
			},
		},
		{
			Name: "DO block assigns GET DIAGNOSTICS ROW_COUNT",
			SetUpScript: []string{
				`CREATE TABLE do_diag_items (id INT PRIMARY KEY, touched BOOL NOT NULL DEFAULT false);`,
				`CREATE TABLE do_diag_seen (seq INT PRIMARY KEY, affected INT NOT NULL);`,
				`INSERT INTO do_diag_items VALUES (1, false), (2, false), (3, false);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DO $$
						DECLARE
							affected INT;
							affected_again INT;
						BEGIN
							UPDATE do_diag_items SET touched = true WHERE id <= 2;
							GET DIAGNOSTICS affected = ROW_COUNT;
							INSERT INTO do_diag_seen VALUES (1, affected);

							EXECUTE 'DELETE FROM do_diag_items WHERE id = 3';
							GET DIAGNOSTICS affected = ROW_COUNT;
							INSERT INTO do_diag_seen VALUES (2, affected);

							UPDATE do_diag_items SET touched = false WHERE id = 99;
							GET DIAGNOSTICS affected = ROW_COUNT, affected_again = ROW_COUNT;
							INSERT INTO do_diag_seen VALUES (3, affected + affected_again);

							PERFORM 1 FROM do_diag_items WHERE touched = true ORDER BY id;
							GET DIAGNOSTICS affected = ROW_COUNT;
							INSERT INTO do_diag_seen VALUES (4, affected);
						END;
					$$;`,
				},
				{
					Query: `SELECT seq, affected FROM do_diag_seen ORDER BY seq;`,
					Expected: []sql.Row{
						{1, 2},
						{2, 1},
						{3, 0},
						{4, 2},
					},
				},
			},
		},
		{
			Name: "DO block assigns GET DIAGNOSTICS PG_CONTEXT",
			SetUpScript: []string{
				`CREATE TABLE do_diag_context_items (id INT PRIMARY KEY, touched BOOL NOT NULL DEFAULT false);`,
				`CREATE TABLE do_diag_context_seen (affected INT NOT NULL, context TEXT NOT NULL);`,
				`CREATE TABLE do_diag_context_stack_seen (kind TEXT NOT NULL, context TEXT NOT NULL);`,
				`INSERT INTO do_diag_context_items VALUES (1, false), (2, false);`,
				`CREATE FUNCTION diag_inner_context() RETURNS TEXT AS $$
					DECLARE
						context TEXT;
					BEGIN
						GET DIAGNOSTICS context = PG_CONTEXT;
						RETURN context;
					END;
				$$ LANGUAGE plpgsql;`,
				`CREATE FUNCTION diag_outer_context() RETURNS VOID AS $$
					DECLARE
						context TEXT;
					BEGIN
						context := diag_inner_context();
						INSERT INTO do_diag_context_stack_seen VALUES ('assignment', context);
					END;
				$$ LANGUAGE plpgsql;`,
				`CREATE FUNCTION diag_sql_context() RETURNS VOID AS $$
					BEGIN
						INSERT INTO do_diag_context_stack_seen(kind, context)
						SELECT 'sql_statement', diag_inner_context();
					END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DO $$
						DECLARE
							context TEXT;
							affected INT;
						BEGIN
							UPDATE do_diag_context_items SET touched = true WHERE id = 1;
							GET DIAGNOSTICS affected = ROW_COUNT, context = PG_CONTEXT;
							INSERT INTO do_diag_context_seen VALUES (affected, context);
						END;
					$$;`,
				},
				{
					Query: `SELECT affected,
							(context LIKE 'PL/pgSQL function inline_code_block line % at GET DIAGNOSTICS')::text
						FROM do_diag_context_seen;`,
					Expected: []sql.Row{{1, "true"}},
				},
				{
					Query: `DO $$
						BEGIN
							PERFORM diag_outer_context();
							PERFORM diag_sql_context();
						END;
					$$;`,
				},
				{
					Query: `SELECT
							(context LIKE 'PL/pgSQL function diag_inner_context() line % at GET DIAGNOSTICS%')::text,
							(position('PL/pgSQL function diag_outer_context() line ' in context) > 0)::text,
							(position('PL/pgSQL function diag_outer_context() line 0' in context) = 0)::text,
							(position(' at assignment' in context) > 0)::text,
							(position('PL/pgSQL function inline_code_block line ' in context) > 0)::text,
							(position('PL/pgSQL function inline_code_block line 0' in context) = 0)::text,
							(position(' at PERFORM' in context) > 0)::text
						FROM do_diag_context_stack_seen
						WHERE kind = 'assignment';`,
					Expected: []sql.Row{{"true", "true", "true", "true", "true", "true", "true"}},
				},
				{
					Query: `SELECT
							(context LIKE 'PL/pgSQL function diag_inner_context() line % at GET DIAGNOSTICS%')::text,
							(position('PL/pgSQL function diag_sql_context() line ' in context) > 0)::text,
							(position('PL/pgSQL function diag_sql_context() line 0' in context) = 0)::text,
							(position(' at SQL statement' in context) > 0)::text,
							(context LIKE '%SQL statement "%diag_inner_context()%')::text,
							(position('PL/pgSQL function inline_code_block line ' in context) > 0)::text,
							(position(' at PERFORM' in context) > 0)::text
						FROM do_diag_context_stack_seen
						WHERE kind = 'sql_statement';`,
					Expected: []sql.Row{{"true", "true", "true", "true", "true", "true", "true"}},
				},
			},
		},
		{
			Name: "PL/pgSQL function assigns GET DIAGNOSTICS PG_ROUTINE_OID",
			SetUpScript: []string{
				`CREATE TABLE do_diag_routine_oid_seen (routine_oid_text TEXT NOT NULL);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DO $$
						DECLARE
							routine_oid oid;
						BEGIN
							GET DIAGNOSTICS routine_oid = PG_ROUTINE_OID;
							INSERT INTO do_diag_routine_oid_seen VALUES (routine_oid::text);
						END;
					$$;`,
				},
				{
					Query:    `SELECT routine_oid_text FROM do_diag_routine_oid_seen;`,
					Expected: []sql.Row{{"0"}},
				},
				{
					Query: `CREATE FUNCTION diag_routine_oid() RETURNS oid AS $$
						DECLARE
							routine_oid oid;
						BEGIN
							GET DIAGNOSTICS routine_oid = PG_ROUTINE_OID;
							RETURN routine_oid;
						END;
					$$ LANGUAGE plpgsql;`,
				},
				{
					Query:    `SELECT (diag_routine_oid()::text <> '0')::text;`,
					Expected: []sql.Row{{"true"}},
				},
			},
		},
		{
			Name: "DO block catches raised exception with stacked diagnostics",
			SetUpScript: []string{
				`CREATE TABLE do_stacked_diag_seen (
					kind TEXT NOT NULL,
					message TEXT NOT NULL,
					sqlstate TEXT NOT NULL,
					detail TEXT NOT NULL,
					hint TEXT NOT NULL,
					has_context TEXT NOT NULL
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DO $$
						DECLARE
							message TEXT;
							returned_state TEXT;
							detail TEXT;
							hint TEXT;
							context TEXT;
						BEGIN
							BEGIN
								RAISE EXCEPTION 'custom exception %', 7
									USING DETAIL = 'some detail', HINT = 'some hint';
							EXCEPTION WHEN OTHERS THEN
								GET STACKED DIAGNOSTICS
									message = MESSAGE_TEXT,
									returned_state = RETURNED_SQLSTATE,
									detail = PG_EXCEPTION_DETAIL,
									hint = PG_EXCEPTION_HINT,
									context = PG_EXCEPTION_CONTEXT;
								INSERT INTO do_stacked_diag_seen VALUES (
									'do',
									message,
									returned_state,
									detail,
									hint,
									(length(context) > 0)::text
								);
							END;
						END;
					$$;`,
				},
				{
					Query:    `SELECT kind, message, sqlstate, detail, hint, has_context FROM do_stacked_diag_seen;`,
					Expected: []sql.Row{{"do", "custom exception 7", "P0001", "some detail", "some hint", "true"}},
				},
			},
		},
		{
			Name: "PL/pgSQL function catches raised exception with stacked diagnostics",
			SetUpScript: []string{
				`CREATE FUNCTION diag_catch_raise() RETURNS TEXT AS $$
					DECLARE
						message TEXT;
					BEGIN
						BEGIN
							RAISE EXCEPTION 'function failed';
						EXCEPTION WHEN OTHERS THEN
							GET STACKED DIAGNOSTICS message = MESSAGE_TEXT;
							RETURN message;
						END;
					END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT diag_catch_raise();`,
					Expected: []sql.Row{{"function failed"}},
				},
			},
		},
		{
			Name:        "GET STACKED DIAGNOSTICS requires exception handler",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DO $$
						DECLARE
							message TEXT;
						BEGIN
							GET STACKED DIAGNOSTICS message = MESSAGE_TEXT;
						END;
					$$;`,
					ExpectedErr: `GET STACKED DIAGNOSTICS cannot be used outside an exception handler`,
				},
			},
		},
		{
			Name: "DO block chooses first matching exception handler",
			SetUpScript: []string{
				`CREATE TABLE do_multi_handler_seen (
					marker TEXT NOT NULL,
					message TEXT NOT NULL,
					sqlstate TEXT NOT NULL
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DO $$
						DECLARE
							message TEXT;
							returned_state TEXT;
						BEGIN
							BEGIN
								RAISE EXCEPTION 'duplicate key branch'
									USING ERRCODE = 'unique_violation';
							EXCEPTION
								WHEN division_by_zero THEN
									INSERT INTO do_multi_handler_seen VALUES ('wrong', 'division', '22012');
								WHEN unique_violation THEN
									GET STACKED DIAGNOSTICS
										message = MESSAGE_TEXT,
										returned_state = RETURNED_SQLSTATE;
									INSERT INTO do_multi_handler_seen VALUES ('matched', message, returned_state);
								WHEN OTHERS THEN
									INSERT INTO do_multi_handler_seen VALUES ('wrong', 'others', 'XX000');
							END;
						END;
					$$;`,
				},
				{
					Query:    `SELECT marker, message, sqlstate FROM do_multi_handler_seen;`,
					Expected: []sql.Row{{"matched", "duplicate key branch", "23505"}},
				},
			},
		},
		{
			Name: "DO block matches SQLSTATE exception handler",
			SetUpScript: []string{
				`CREATE TABLE do_sqlstate_handler_seen (
					marker TEXT NOT NULL,
					sqlstate TEXT NOT NULL
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DO $$
						DECLARE
							returned_state TEXT;
						BEGIN
							BEGIN
								RAISE EXCEPTION 'sqlstate branch'
									USING ERRCODE = 'P0001';
							EXCEPTION
								WHEN SQLSTATE '23505' THEN
									INSERT INTO do_sqlstate_handler_seen VALUES ('wrong', '23505');
								WHEN SQLSTATE 'P0001' THEN
									GET STACKED DIAGNOSTICS returned_state = RETURNED_SQLSTATE;
									INSERT INTO do_sqlstate_handler_seen VALUES ('matched', returned_state);
							END;
						END;
					$$;`,
				},
				{
					Query:    `SELECT marker, sqlstate FROM do_sqlstate_handler_seen;`,
					Expected: []sql.Row{{"matched", "P0001"}},
				},
			},
		},
		{
			Name: "DO block reads stacked object-name diagnostics",
			SetUpScript: []string{
				`CREATE TABLE do_stacked_object_diag_seen (
					column_name TEXT NOT NULL,
					constraint_name TEXT NOT NULL,
					datatype_name TEXT NOT NULL,
					table_name TEXT NOT NULL,
					schema_name TEXT NOT NULL
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DO $$
						DECLARE
							column_name_text TEXT;
							constraint_name_text TEXT;
							datatype_name_text TEXT;
							table_name_text TEXT;
							schema_name_text TEXT;
						BEGIN
							BEGIN
								RAISE EXCEPTION 'object metadata'
									USING ERRCODE = 'check_violation',
										COLUMN = 'amount',
										CONSTRAINT = 'amount_positive',
										DATATYPE = 'numeric',
										TABLE = 'invoice_lines',
										SCHEMA = 'public';
							EXCEPTION
								WHEN check_violation THEN
									GET STACKED DIAGNOSTICS
										column_name_text = COLUMN_NAME,
										constraint_name_text = CONSTRAINT_NAME,
										datatype_name_text = PG_DATATYPE_NAME,
										table_name_text = TABLE_NAME,
										schema_name_text = SCHEMA_NAME;
									INSERT INTO do_stacked_object_diag_seen VALUES (
										column_name_text,
										constraint_name_text,
										datatype_name_text,
										table_name_text,
										schema_name_text
									);
							END;
						END;
					$$;`,
				},
				{
					Query: `SELECT column_name, constraint_name, datatype_name, table_name, schema_name
						FROM do_stacked_object_diag_seen;`,
					Expected: []sql.Row{{"amount", "amount_positive", "numeric", "invoice_lines", "public"}},
				},
			},
		},
		{
			Name: "DO block catches native SQL unique violation by condition name",
			SetUpScript: []string{
				`CREATE TABLE do_native_sqlstate_items (
					id INT PRIMARY KEY,
					label TEXT UNIQUE
				);`,
				`CREATE TABLE do_native_sqlstate_seen (
					sqlstate TEXT NOT NULL,
					has_message TEXT NOT NULL
				);`,
				`INSERT INTO do_native_sqlstate_items VALUES (1, 'existing');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DO $$
						DECLARE
							returned_state TEXT;
							message TEXT;
						BEGIN
							BEGIN
								INSERT INTO do_native_sqlstate_items VALUES (2, 'existing');
							EXCEPTION
								WHEN unique_violation THEN
									GET STACKED DIAGNOSTICS
										returned_state = RETURNED_SQLSTATE,
										message = MESSAGE_TEXT;
									INSERT INTO do_native_sqlstate_seen VALUES (
										returned_state,
										(length(message) > 0)::text
									);
							END;
						END;
					$$;`,
				},
				{
					Query:    `SELECT sqlstate, has_message FROM do_native_sqlstate_seen;`,
					Expected: []sql.Row{{"23505", "true"}},
				},
			},
		},
		{
			Name: "DO block reports native SQL exception context",
			SetUpScript: []string{
				`CREATE TABLE do_native_context_items (
					id INT PRIMARY KEY,
					label TEXT UNIQUE
				);`,
				`CREATE TABLE do_native_context_seen (
					has_statement TEXT NOT NULL,
					has_plpgsql_frame TEXT NOT NULL
				);`,
				`INSERT INTO do_native_context_items VALUES (1, 'existing');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DO $$
						DECLARE
							context_text TEXT;
						BEGIN
							BEGIN
								INSERT INTO do_native_context_items VALUES (2, 'existing');
							EXCEPTION
								WHEN unique_violation THEN
									GET STACKED DIAGNOSTICS context_text = PG_EXCEPTION_CONTEXT;
									INSERT INTO do_native_context_seen VALUES (
										(position('SQL statement "INSERT INTO do_native_context_items' in context_text) > 0)::text,
										(context_text LIKE '%PL/pgSQL function inline_code_block line % at SQL statement%')::text
									);
							END;
						END;
					$$;`,
				},
				{
					Query:    `SELECT has_statement, has_plpgsql_frame FROM do_native_context_seen;`,
					Expected: []sql.Row{{"true", "true"}},
				},
			},
		},
		{
			Name: "DO block propagates unmatched native SQL unique violation",
			SetUpScript: []string{
				`CREATE TABLE do_native_unmatched_items (
					id INT PRIMARY KEY,
					label TEXT UNIQUE
				);`,
				`INSERT INTO do_native_unmatched_items VALUES (1, 'existing');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DO $$
						BEGIN
							BEGIN
								INSERT INTO do_native_unmatched_items VALUES (2, 'existing');
							EXCEPTION
								WHEN division_by_zero THEN
									RAISE NOTICE 'wrong handler';
							END;
						END;
					$$;`,
					ExpectedErr: "duplicate unique key given",
				},
			},
		},
		{
			Name:        "DO block multiple exception handlers propagate when unmatched",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DO $$
						BEGIN
							BEGIN
								RAISE EXCEPTION 'uncaught branch';
							EXCEPTION
								WHEN division_by_zero THEN
									RAISE NOTICE 'wrong handler';
								WHEN unique_violation THEN
									RAISE NOTICE 'wrong handler';
							END;
						END;
					$$;`,
					ExpectedErr: "uncaught branch",
				},
			},
		},
		{
			Name:        "DO block propagates raised exception",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DO $$
						BEGIN
							RAISE EXCEPTION 'do block failed: %', 42;
						END;
					$$;`,
					ExpectedErr: `do block failed: 42`,
				},
			},
		},
	})
}
