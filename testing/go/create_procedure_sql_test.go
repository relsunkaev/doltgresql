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
						ORDER BY id;`,
					Expected: []sql.Row{{"first"}},
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
					Query:    `SELECT id, label FROM sql_proc_atomic_items;`,
					Expected: []sql.Row{{1, "first"}},
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
$$;`,
					Expected: []sql.Row{},
				},
				{
					SkipResultsCheck: true, // TODO: need fix for returning results
					Query:            `CALL add('f')`,
					Expected:         []sql.Row{{"2222"}},
				},
				{
					Query:    `SELECT id, game_id, host_connection_id FROM games`,
					Expected: []sql.Row{{1, "2222", "f"}},
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
$$;`,
					Expected: []sql.Row{},
				},
				{
					SkipResultsCheck: true,
					Query:            `CALL create_game('d')`,
				},
				{
					Query:    `SELECT id, host_connection_id FROM games`,
					Expected: []sql.Row{{1, "f"}, {2, "d"}},
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
							$$;`,
					Expected: []sql.Row{},
				},
				{
					Query:    `CALL ptest5(10, 'Hello', 20);`,
					Expected: []sql.Row{},
				},
				{
					Query:    `SELECT * FROM cp_test`,
					Expected: []sql.Row{{10, "Hello"}, {20, "Hello"}},
				},
				{
					Query:    `CALL ptest5(50, 'Bye');`,
					Expected: []sql.Row{},
				},
				{
					Query:    `SELECT * FROM cp_test`,
					Expected: []sql.Row{{10, "Hello"}, {20, "Hello"}, {50, "Bye"}, {100, "Bye"}},
				},
			},
		},
	})
}
