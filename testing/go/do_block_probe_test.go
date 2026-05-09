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
