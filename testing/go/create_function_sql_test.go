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

package _go

import (
	"testing"
)

func TestCreateFunctionsLanguageSQL(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "unnamed parameter",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE FUNCTION alt_func1(int) RETURNS int LANGUAGE sql AS 'SELECT $1 + 1';`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-function-sql-test-testcreatefunctionslanguagesql-0001-create-function-alt_func1-int-returns", Cleanup: []string{"DROP FUNCTION IF EXISTS alt_func1(int)"}},
				},
				{
					Query: `SELECT alt_func1(3);`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-function-sql-test-testcreatefunctionslanguagesql-0002-select-alt_func1-3"},
				},
			},
		},
		{
			Name:        "named parameter",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE FUNCTION alt_func1(x int) RETURNS int LANGUAGE sql AS 'SELECT x + 1';`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-function-sql-test-testcreatefunctionslanguagesql-0003-create-function-alt_func1-x-int", Cleanup: []string{"DROP FUNCTION IF EXISTS alt_func1(int)"}},
				},
				{
					Query: `SELECT alt_func1(3);`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-function-sql-test-testcreatefunctionslanguagesql-0004-select-alt_func1-3"},
				},
				{
					Query: `CREATE FUNCTION sub_numbers(x int, y int) RETURNS int LANGUAGE sql AS 'SELECT y - x';`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-function-sql-test-testcreatefunctionslanguagesql-0005-create-function-sub_numbers-x-int"},
				},
				{
					Query: `SELECT sub_numbers(1, 2);`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-function-sql-test-testcreatefunctionslanguagesql-0006-select-sub_numbers-1-2"},
				},
			},
		},
		{
			Name:        "unknown functions",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE FUNCTION get_grade_description(score INT)
							RETURNS TEXT
							LANGUAGE SQL
							AS $$
								SELECT
									CASE
										WHEN score >= 90 THEN 'Excellent'
										WHEN score >= 75 THEN 'Good'
										WHEN score >= 50 THEN 'Average'
									ELSE 'Fail'
									END;
							$$;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-function-sql-test-testcreatefunctionslanguagesql-0007-create-function-get_grade_description-score-int", Cleanup: []string{"DROP FUNCTION IF EXISTS get_grade_description(int)"}},
				},
				{
					Query: `SELECT get_grade_description(92);`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-function-sql-test-testcreatefunctionslanguagesql-0008-select-get_grade_description-92"},
				},
				{
					Query: `SELECT get_grade_description(65);`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-function-sql-test-testcreatefunctionslanguagesql-0009-select-get_grade_description-65"},
				},
			},
		},
		{
			Name:        "nested functions",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE FUNCTION calculate_double_sum(x INT, y INT)
							RETURNS INT
							LANGUAGE SQL
							AS $$
								SELECT add_numbers(x, y) * 2;
							$$;`, PostgresOracle:
					// TODO: error message should be:  function add_numbers(integer, integer) does not exist
					ScriptTestPostgresOracle{ID: "create-function-sql-test-testcreatefunctionslanguagesql-0010-create-function-calculate_double_sum-x-int", Compare: "sqlstate"},
				},
				{
					Query: `CREATE FUNCTION add_numbers(int, int) RETURNS int LANGUAGE sql AS 'SELECT $1 + $2';`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-function-sql-test-testcreatefunctionslanguagesql-0011-create-function-add_numbers-int-int", Cleanup: []string{"DROP FUNCTION IF EXISTS add_numbers(int, int)"}},
				},
				{
					Query: `CREATE FUNCTION calculate_double_sum(x INT, y INT)
							RETURNS INT
							LANGUAGE SQL
							AS $$
								SELECT add_numbers(x, y) * 2;
							$$;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-function-sql-test-testcreatefunctionslanguagesql-0012-create-function-calculate_double_sum-x-int"},
				},
				{
					Query: `SELECT calculate_double_sum(1, 2);`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-function-sql-test-testcreatefunctionslanguagesql-0013-select-calculate_double_sum-1-2"},
				},
			},
		},
		{
			Name: "function returning multiple rows",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE FUNCTION gen(a int) RETURNS SETOF INT LANGUAGE SQL AS $$ SELECT generate_series(1, a) $$ STABLE;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-function-sql-test-testcreatefunctionslanguagesql-0014-create-function-gen-a-int", Cleanup: []string{"DROP FUNCTION IF EXISTS gen(int)"}},
				},
				{
					Query: `SELECT * FROM gen(3);`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-function-sql-test-testcreatefunctionslanguagesql-0015-select-*-from-gen-3"},
				},
			},
		},
		{
			Name: "function with create or replace view",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE FUNCTION public.sp_build_view_bathymetry_layer() RETURNS void
							LANGUAGE sql
							AS $$
								CREATE OR REPLACE VIEW public.view_bathymetry_layer AS
								SELECT 1;
							$$;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-function-sql-test-testcreatefunctionslanguagesql-0016-create-function-public.sp_build_view_bathymetry_layer-returns-void", Cleanup: []string{"DROP FUNCTION IF EXISTS public.sp_build_view_bathymetry_layer()"}},
				},
				{
					Query: `SELECT public.sp_build_view_bathymetry_layer()`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-function-sql-test-testcreatefunctionslanguagesql-0017-select-public.sp_build_view_bathymetry_layer", Cleanup: []string{"DROP VIEW IF EXISTS public.view_bathymetry_layer", "DROP FUNCTION IF EXISTS public.sp_build_view_bathymetry_layer()"}},
				},
				{
					Query: `SELECT * from view_bathymetry_layer`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-function-sql-test-testcreatefunctionslanguagesql-0018-select-*-from-view_bathymetry_layer", Cleanup: []string{"DROP VIEW IF EXISTS public.view_bathymetry_layer", "DROP FUNCTION IF EXISTS public.sp_build_view_bathymetry_layer()"}},
				},
				{
					Query: `SELECT public.sp_build_view_bathymetry_layer()`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-function-sql-test-testcreatefunctionslanguagesql-0019-select-public.sp_build_view_bathymetry_layer"},
				},
				{
					Query: `SELECT * from view_bathymetry_layer`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-function-sql-test-testcreatefunctionslanguagesql-0020-select-*-from-view_bathymetry_layer"},
				},
			},
		},
		{
			Name: "function with update ... returning",
			SetUpScript: []string{
				`CREATE TYPE public.tax_job_state AS ENUM (
					'sched',
					'busy',
					'final',
					'help'
				);`,
				`CREATE TABLE public.tax_job (
					id bigint NOT NULL,
					state public.tax_job_state NOT NULL,
					created timestamp NOT NULL,
					modified timestamp NOT NULL,
					scheduled timestamp,
					worker text,
					processor text,
					ext_id text,
					data jsonb,
					gross integer,
					notes text[],
					ops jsonb,
					CONSTRAINT tax_job_check CHECK ((NOT ((state = 'sched'::public.tax_job_state) AND (scheduled IS NULL)))),
					CONSTRAINT tax_job_check1 CHECK ((NOT ((state = 'busy'::public.tax_job_state) AND (worker IS NULL))))
				);`,
				`INSERT INTO tax_job (id, state, created, modified, scheduled, worker, processor, ext_id, data) VALUES (1, 'sched', '2025-05-05 05:05:05', '2025-05-05 05:05:05', '2025-05-05 05:05:05', 'worker', 'processor', 'ext_id', NULL)`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE FUNCTION public.tax_job_take(arg_worker text) RETURNS SETOF public.tax_job
								LANGUAGE sql
								AS '
								UPDATE
									tax_job
								SET
									state = ''busy'',
									worker = arg_worker
								WHERE
									state = ''sched''
									AND scheduled <= CURRENT_TIMESTAMP
								RETURNING
									*;
							';`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-function-sql-test-testcreatefunctionslanguagesql-0021-create-function-public.tax_job_take-arg_worker-text"},
				},
				{
					Query: `SELECT public.tax_job_take('worker')`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-function-sql-test-testcreatefunctionslanguagesql-0022-select-public.tax_job_take-worker"},
				},
				{
					Query: `INSERT INTO tax_job (id, state, created, modified, scheduled, worker, processor, ext_id, data) VALUES (2, 'sched', '2025-05-05 05:05:06', '2025-05-05 05:05:06', '2025-05-05 05:05:06', 'worker', 'processor', 'ext_id', NULL), (3, 'sched', '2025-05-05 05:05:07', '2025-05-05 05:05:07', '2025-05-05 05:05:07', 'worker', 'processor', 'ext_id', NULL)`,
				},
				{
					Query: `SELECT public.tax_job_take('worker')`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-function-sql-test-testcreatefunctionslanguagesql-0023-select-public.tax_job_take-worker"},
				},
			},
		},
		{
			Name: "function with delete",
			SetUpScript: []string{
				`CREATE TABLE test (id bigint NOT NULL, state text NOT NULL);`,
				`INSERT INTO test VALUES (1, 'sched'), (2, 'busy');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE FUNCTION d(w text) RETURNS bigint
								LANGUAGE sql
								AS '
								DELETE FROM test
								WHERE
									state = w
								RETURNING
									id;
							';`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-function-sql-test-testcreatefunctionslanguagesql-0024-create-function-d-w-text"},
				},
				{
					Query: `SELECT * FROM test;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-function-sql-test-testcreatefunctionslanguagesql-0025-select-*-from-test"},
				},
				{
					Query: `SELECT d('sched');`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-function-sql-test-testcreatefunctionslanguagesql-0026-select-d-sched"},
				},
				{
					Query: `SELECT * FROM test;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-function-sql-test-testcreatefunctionslanguagesql-0027-select-*-from-test"},
				},
			},
		},
		{
			Name: "multiple statements in function",
			SetUpScript: []string{
				`CREATE TABLE test (id int);`,
				`INSERT INTO test VALUES (1), (2), (3);`,
				`CREATE VIEW test1 AS SELECT * FROM test WHERE id = 1;`,
				`CREATE VIEW test2 AS SELECT * FROM test WHERE id = 2;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE FUNCTION drop_views() RETURNS void
								LANGUAGE sql
								AS $$
							DROP VIEW test1;
							DROP VIEW test2;
							$$;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-function-sql-test-testcreatefunctionslanguagesql-0028-create-function-drop_views-returns-void"},
				},
				{
					Query: `SELECT * FROM test1`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-function-sql-test-testcreatefunctionslanguagesql-0029-select-*-from-test1"},
				},
				{
					Query: `SELECT * FROM test2`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-function-sql-test-testcreatefunctionslanguagesql-0030-select-*-from-test2"},
				},
				{
					Query: `SELECT drop_views();`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-function-sql-test-testcreatefunctionslanguagesql-0031-select-drop_views"},
				},
				{
					Query: `SELECT * FROM test1`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-function-sql-test-testcreatefunctionslanguagesql-0032-select-*-from-test1", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM test2`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-function-sql-test-testcreatefunctionslanguagesql-0033-select-*-from-test2", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "function with default expression in parameter",
			SetUpScript: []string{
				`CREATE TABLE cp_test (a int, b text);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE OR REPLACE FUNCTION dfunc(e int, d text, f int default 100)
							 RETURNS int LANGUAGE SQL
							AS $$
								INSERT INTO cp_test VALUES(e+f, d);
								SELECT a FROM cp_test WHERE b = d;
							$$;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-function-sql-test-testcreatefunctionslanguagesql-0034-create-or-replace-function-dfunc"},
				},
				{
					Query: `CREATE OR REPLACE FUNCTION dfunc(e int, f int default 100)
							 RETURNS int LANGUAGE SQL
							AS $$
								INSERT INTO cp_test VALUES(e+f, 'seconddfunc');
								SELECT a FROM cp_test WHERE b = 'seconddfunc';
							$$;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-function-sql-test-testcreatefunctionslanguagesql-0035-create-or-replace-function-dfunc"},
				},
				{
					Query: `SELECT * FROM dfunc(10, 'Hello', 20);`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-function-sql-test-testcreatefunctionslanguagesql-0036-select-*-from-dfunc-10"},
				},
				{
					Query: `SELECT * FROM cp_test`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-function-sql-test-testcreatefunctionslanguagesql-0037-select-*-from-cp_test"},
				},
				{
					Query: `SELECT * FROM dfunc(50, 'Bye');`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-function-sql-test-testcreatefunctionslanguagesql-0038-select-*-from-dfunc-50"},
				},
				{
					Query: `SELECT * FROM cp_test`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-function-sql-test-testcreatefunctionslanguagesql-0039-select-*-from-cp_test"},
				},
				{
					Query: `SELECT dfunc(2, 'After');`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-function-sql-test-testcreatefunctionslanguagesql-0040-select-dfunc-2-after"},
				},
				{
					Query: `SELECT * FROM cp_test`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-function-sql-test-testcreatefunctionslanguagesql-0041-select-*-from-cp_test"},
				},
				{
					Query: `CREATE OR REPLACE FUNCTION dfunc(e int, f text default '100')
							 RETURNS int LANGUAGE SQL
							AS $$
								INSERT INTO cp_test VALUES(e, f);
								SELECT a FROM cp_test WHERE b = f;
							$$;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-function-sql-test-testcreatefunctionslanguagesql-0042-create-or-replace-function-dfunc"},
				},
				{
					Query: `SELECT dfunc(50);`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-function-sql-test-testcreatefunctionslanguagesql-0043-select-dfunc-50", Compare: "sqlstate"},
				},
			},
		},
		{
			Name:        "use sql statements in BEGIN ATOMIC ... END in sql_body",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE FUNCTION match_default() RETURNS jsonb
            LANGUAGE sql
            BEGIN ATOMIC 
				SELECT jsonb_build_object('k', 6, 'm', 2048, 'include_original', true, 'tokenizer', json_build_object('kind', 'ngram', 'token_length', 3), 'token_filters', json_build_array(json_build_object('kind', 'downcase'))) AS jsonb_build_object; 
			END;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-function-sql-test-testcreatefunctionslanguagesql-0044-create-function-match_default-returns-jsonb"},
				},
				{
					Skip:  true, // TODO support json_build_object() function
					Query: `SELECT public.match_default();`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-function-sql-test-testcreatefunctionslanguagesql-0045-select-public.match_default"},
				},
				{
					Query: `CREATE FUNCTION select1() RETURNS int
            LANGUAGE sql
            BEGIN ATOMIC 
				SELECT 1; 
			END;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-function-sql-test-testcreatefunctionslanguagesql-0046-create-function-select1-returns-int"},
				},
				{
					Query: `SELECT select1();`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-function-sql-test-testcreatefunctionslanguagesql-0047-select-select1"},
				},
			},
		},
		{
			Name:        "use RETURN in BEGIN ATOMIC ... END in sql_body",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE FUNCTION return1() RETURNS text
            LANGUAGE sql
            BEGIN ATOMIC 
				RETURN 1::text || 'one'; 
			END;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-function-sql-test-testcreatefunctionslanguagesql-0048-create-function-return1-returns-text"},
				},
				{
					Query: `SELECT return1();`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-function-sql-test-testcreatefunctionslanguagesql-0049-select-return1"},
				},
			},
		},
	})
}
