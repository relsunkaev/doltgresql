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
)

// TestSqlProcedureInsertReturningExecutesRepro reproduces a SQL procedure
// execution bug: CALL should run an INSERT ... RETURNING body without panicking
// and persist the inserted row.
func TestSqlProcedureInsertReturningExecutesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "SQL procedure INSERT RETURNING executes",
			SetUpScript: []string{
				`CREATE TABLE sql_proc_returning_items (
					id SERIAL PRIMARY KEY,
					label TEXT
				);`,
				`CREATE PROCEDURE sql_proc_insert_returning(input_label TEXT)
					LANGUAGE SQL
					AS $$
						INSERT INTO sql_proc_returning_items (label)
						VALUES (input_label)
						RETURNING id
					$$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:            `CALL sql_proc_insert_returning('first');`,
					SkipResultsCheck: true,
				},
				{
					Query: `SELECT label
						FROM sql_proc_returning_items
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-procedure-sql-test-testsqlprocedureinsertreturningexecutesrepro-0001-select-label-from-sql_proc_returning_items-order"},
				},
			},
		},
	})
}

// TestSqlProcedureBeginAtomicBodyRepro reproduces a SQL-standard routine body
// compatibility gap: PostgreSQL accepts BEGIN ATOMIC ... END procedure bodies.
func TestSqlProcedureBeginAtomicBodyRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "SQL procedure BEGIN ATOMIC body executes",
			SetUpScript: []string{
				`CREATE TABLE sql_proc_atomic_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE PROCEDURE sql_proc_atomic_insert(input_id INT, input_label TEXT)
						LANGUAGE SQL
						BEGIN ATOMIC
							INSERT INTO sql_proc_atomic_items VALUES (input_id, input_label);
						END;`,
				},
				{
					Query: `CALL sql_proc_atomic_insert(1, 'first');`,
				},
				{
					Query: `SELECT id, label FROM sql_proc_atomic_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-procedure-sql-test-testsqlprocedurebeginatomicbodyrepro-0001-select-id-label-from-sql_proc_atomic_items"},
				},
			},
		},
	})
}

func TestCreateProcedureLanguageSql(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "procedure with insert returning",
			SetUpScript: []string{
				`CREATE TABLE public.games (
    id bigint NOT NULL,
    game_id character varying(4) NOT NULL,
    host_connection_id character varying(50) NOT NULL
);`,
				`CREATE SEQUENCE public.games_id_seq START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;`,
				`ALTER SEQUENCE public.games_id_seq OWNED BY public.games.id;`,
				`ALTER TABLE ONLY public.games ALTER COLUMN id SET DEFAULT nextval('public.games_id_seq'::regclass);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE PROCEDURE public.add(INOUT new_host_connection_id character varying)
    LANGUAGE sql
    AS $$
	INSERT INTO public.games (
		game_id,
		host_connection_id
	)
	VALUES (2222, new_host_connection_id)
	RETURNING game_id;
$$;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-procedure-sql-test-testcreateprocedurelanguagesql-0001-create-procedure-public.add-inout-new_host_connection_id"},
				},
				{
					SkipResultsCheck: true, // TODO: need fix for returning results
					Query:            `CALL add('f')`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-procedure-sql-test-testcreateprocedurelanguagesql-0002-call-add-f"},
				},
				{
					Query: `SELECT id, game_id, host_connection_id FROM games`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-procedure-sql-test-testcreateprocedurelanguagesql-0003-select-id-game_id-host_connection_id-from"},
				},
				{
					Query: `CREATE PROCEDURE public.create_game(INOUT new_host_connection_id character varying)
    LANGUAGE sql
    AS $$
	WITH new_game_id_holder (new_game_id) AS (
		SELECT n.random_number
		FROM (
			SELECT LPAD(FLOOR(random() * 10000)::varchar, 4, '0') AS random_number
			FROM generate_series(1, (SELECT COUNT(*) FROM public.games) + 10)
		) AS n
		LEFT OUTER JOIN 
			public.games AS g on g.game_id = n.random_number
		WHERE g.id IS NULL
		LIMIT 1
	)
	INSERT INTO public.games (
		game_id,
		host_connection_id
	)
	VALUES ( 
		(SELECT new_game_id FROM new_game_id_holder),
		new_host_connection_id
	)
	RETURNING game_id;
$$;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-procedure-sql-test-testcreateprocedurelanguagesql-0004-create-procedure-public.create_game-inout-new_host_connection_id"},
				},
				{
					SkipResultsCheck: true,
					Query:            `CALL create_game('d')`,
				},
				{
					Query: `SELECT id, host_connection_id FROM games`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-procedure-sql-test-testcreateprocedurelanguagesql-0005-select-id-host_connection_id-from-games"},
				},
			},
		},
		{
			Name: "procedure with default expression in parameter",
			SetUpScript: []string{
				`CREATE TABLE cp_test (a int, b text);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE OR REPLACE PROCEDURE ptest5(a int, b text, c int default 100)
							LANGUAGE SQL
							AS $$
								INSERT INTO cp_test VALUES(a, b);
								INSERT INTO cp_test VALUES(c, b);
							$$;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-procedure-sql-test-testcreateprocedurelanguagesql-0006-create-or-replace-procedure-ptest5"},
				},
				{
					Query: `CALL ptest5(10, 'Hello', 20);`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-procedure-sql-test-testcreateprocedurelanguagesql-0007-call-ptest5-10-hello-20"},
				},
				{
					Query: `SELECT * FROM cp_test`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-procedure-sql-test-testcreateprocedurelanguagesql-0008-select-*-from-cp_test"},
				},
				{
					Query: `CALL ptest5(50, 'Bye');`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-procedure-sql-test-testcreateprocedurelanguagesql-0009-call-ptest5-50-bye"},
				},
				{
					Query: `SELECT * FROM cp_test`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-procedure-sql-test-testcreateprocedurelanguagesql-0010-select-*-from-cp_test"},
				},
			},
		},
	})
}
