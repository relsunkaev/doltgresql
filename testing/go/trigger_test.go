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

	"github.com/dolthub/go-mysql-server/sql"
)

func TestCreateTrigger(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "BEFORE INSERT",
			SetUpScript: []string{
				"CREATE TABLE test (pk INT PRIMARY KEY, v1 TEXT);",
				`CREATE FUNCTION trigger_func() RETURNS TRIGGER AS $$
				BEGIN
					NEW.v1 := NEW.v1 || '_' || NEW.pk::text;
					RETURN NEW;
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER test_trigger BEFORE INSERT ON test FOR EACH ROW EXECUTE FUNCTION trigger_func();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    "INSERT INTO test VALUES (1, 'hi'), (2, 'there');",
					Expected: []sql.Row{},
				},
				{
					Query: "SELECT * FROM test;",
					Expected: []sql.Row{
						{1, "hi_1"},
						{2, "there_2"},
					},
				},
			},
		},
		{
			Name: "BEFORE UPDATE",
			SetUpScript: []string{
				"CREATE TABLE test (pk INT PRIMARY KEY, v1 TEXT);",
				"INSERT INTO test VALUES (1, 'hi'), (2, 'there');",
				`CREATE FUNCTION trigger_func() RETURNS TRIGGER AS $$
			BEGIN
				NEW.v1 := NEW.v1 || '_' || NEW.pk::text;
				RETURN NEW;
			END;
			$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER test_trigger BEFORE UPDATE ON test FOR EACH ROW EXECUTE FUNCTION trigger_func();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    "UPDATE test SET v1 = v1 || '|' WHERE pk IN (1, 2);",
					Expected: []sql.Row{},
				},
				{
					Query: "SELECT * FROM test;",
					Expected: []sql.Row{
						{1, "hi|_1"},
						{2, "there|_2"},
					},
				},
			},
		},
		{
			Name: "BEFORE DELETE",
			SetUpScript: []string{
				"CREATE TABLE test (pk INT PRIMARY KEY, v1 TEXT);",
				"CREATE TABLE test2 (pk INT PRIMARY KEY, v1 TEXT);",
				"INSERT INTO test VALUES (1, 'hi'), (2, 'there');",
				`CREATE FUNCTION trigger_func() RETURNS TRIGGER AS $$
			BEGIN
				INSERT INTO test2 VALUES (OLD.pk, OLD.v1);
				RETURN OLD;
			END;
			$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER test_trigger BEFORE DELETE ON test FOR EACH ROW EXECUTE FUNCTION trigger_func();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT * FROM test;",
					Expected: []sql.Row{
						{1, "hi"},
						{2, "there"},
					},
				},
				{
					Query:    "SELECT * FROM test2;",
					Expected: []sql.Row{},
				},
				{
					Query:    "DELETE FROM test WHERE pk = 1;",
					Expected: []sql.Row{},
				},
				{
					Query: "SELECT * FROM test;",
					Expected: []sql.Row{
						{2, "there"},
					},
				},
				{
					Query: "SELECT * FROM test2;",
					Expected: []sql.Row{
						{1, "hi"},
					},
				},
			},
		},
		{
			Name: "BEFORE INSERT returning NULL",
			Skip: true, // TODO: returning a NULL-filled row isn't quite valid for this
			SetUpScript: []string{
				"CREATE TABLE test (pk INT PRIMARY KEY, v1 TEXT);",
				"CREATE TABLE test2 (pk INT PRIMARY KEY, v1 TEXT);",
				`CREATE FUNCTION trigger_func() RETURNS TRIGGER AS $$
				BEGIN
					NEW.v1 := NEW.v1 || '_' || NEW.pk::text;
					INSERT INTO test2 VALUES (NEW.pk, NEW.v1);
					RETURN NULL;
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER test_trigger BEFORE INSERT ON test FOR EACH ROW EXECUTE FUNCTION trigger_func();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    "INSERT INTO test VALUES (1, 'hi'), (2, 'there');",
					Expected: []sql.Row{},
				},
				{
					Query: "SELECT * FROM test;",
					Expected: []sql.Row{
						{1, "hi"},
						{2, "there"},
					},
				},
				{
					Query: "SELECT * FROM test2;",
					Expected: []sql.Row{
						{1, "hi_1"},
						{2, "there_2"},
					},
				},
			},
		},
		{
			Name: "BEFORE UPDATE returning NULL",
			SetUpScript: []string{
				"CREATE TABLE test (pk INT PRIMARY KEY, v1 TEXT);",
				"CREATE TABLE test2 (pk INT PRIMARY KEY, v1 TEXT);",
				"INSERT INTO test VALUES (1, 'hi'), (2, 'there');",
				`CREATE FUNCTION trigger_func() RETURNS TRIGGER AS $$
			BEGIN
				NEW.v1 := NEW.v1 || '_' || NEW.pk::text;
				INSERT INTO test2 VALUES (NEW.pk, NEW.v1);
				RETURN NULL;
			END;
			$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER test_trigger BEFORE UPDATE ON test FOR EACH ROW EXECUTE FUNCTION trigger_func();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    "UPDATE test SET v1 = v1 || '|' WHERE pk IN (1, 2);",
					Expected: []sql.Row{},
				},
				{
					Query: "SELECT * FROM test;",
					Expected: []sql.Row{
						{1, "hi"},
						{2, "there"},
					},
				},
				{
					Query: "SELECT * FROM test2;",
					Expected: []sql.Row{
						{1, "hi|_1"},
						{2, "there|_2"},
					},
				},
			},
		},
		{
			Name: "BEFORE DELETE returning NULL",
			SetUpScript: []string{
				"CREATE TABLE test (pk INT PRIMARY KEY, v1 TEXT);",
				"CREATE TABLE test2 (pk INT PRIMARY KEY, v1 TEXT);",
				"INSERT INTO test VALUES (1, 'hi'), (2, 'there');",
				`CREATE FUNCTION trigger_func() RETURNS TRIGGER AS $$
			BEGIN
				INSERT INTO test2 VALUES (OLD.pk, OLD.v1);
				RETURN NULL;
			END;
			$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER test_trigger BEFORE DELETE ON test FOR EACH ROW EXECUTE FUNCTION trigger_func();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT * FROM test;",
					Expected: []sql.Row{
						{1, "hi"},
						{2, "there"},
					},
				},
				{
					Query:    "SELECT * FROM test2;",
					Expected: []sql.Row{},
				},
				{
					Query:    "DELETE FROM test WHERE pk = 1;",
					Expected: []sql.Row{},
				},
				{
					Query: "SELECT * FROM test;",
					Expected: []sql.Row{
						{1, "hi"},
						{2, "there"},
					},
				},
				{
					Query: "SELECT * FROM test2;",
					Expected: []sql.Row{
						{1, "hi"},
					},
				},
			},
		},
		{
			Name: "BEFORE UPDATE with DELETE DML",
			SetUpScript: []string{
				"CREATE TABLE test (pk INT PRIMARY KEY, v1 TEXT);",
				"CREATE TABLE test2 (pk INT PRIMARY KEY, v1 TEXT);",
				"INSERT INTO test VALUES (1, 'hi'), (2, 'there');",
				`CREATE FUNCTION trigger_func() RETURNS TRIGGER AS $$
			BEGIN
				INSERT INTO test2 VALUES (OLD.pk, OLD.v1);
				RETURN OLD;
			END;
			$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER test_trigger BEFORE UPDATE ON test FOR EACH ROW EXECUTE FUNCTION trigger_func();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT * FROM test;",
					Expected: []sql.Row{
						{1, "hi"},
						{2, "there"},
					},
				},
				{
					Query:    "SELECT * FROM test2;",
					Expected: []sql.Row{},
				},
				{
					Query:    "DELETE FROM test WHERE pk = 1;",
					Expected: []sql.Row{},
				},
				{
					Query: "SELECT * FROM test;",
					Expected: []sql.Row{
						{2, "there"},
					},
				},
				{
					Query:    "SELECT * FROM test2;",
					Expected: []sql.Row{},
				},
			},
		},
		{
			Name: "AFTER INSERT",
			SetUpScript: []string{
				"CREATE TABLE test (pk INT PRIMARY KEY, v1 TEXT);",
				"CREATE TABLE test2 (pk INT PRIMARY KEY, v1 TEXT);",
				`CREATE FUNCTION trigger_func() RETURNS TRIGGER AS $$
				BEGIN
					NEW.v1 := NEW.v1 || '_' || NEW.pk::text;
					INSERT INTO test2 VALUES (NEW.pk, NEW.v1);
					RETURN NEW;
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER test_trigger AFTER INSERT ON test FOR EACH ROW EXECUTE FUNCTION trigger_func();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    "INSERT INTO test VALUES (1, 'hi'), (2, 'there');",
					Expected: []sql.Row{},
				},
				{
					Query: "SELECT * FROM test;",
					Expected: []sql.Row{
						{1, "hi"},
						{2, "there"},
					},
				},
				{
					Query: "SELECT * FROM test2;",
					Expected: []sql.Row{
						{1, "hi_1"},
						{2, "there_2"},
					},
				},
			},
		},
		{
			Name: "AFTER UPDATE",
			SetUpScript: []string{
				"CREATE TABLE test (pk INT PRIMARY KEY, v1 TEXT);",
				"CREATE TABLE test2 (pk INT PRIMARY KEY, v1 TEXT);",
				"INSERT INTO test VALUES (1, 'hi'), (2, 'there');",
				`CREATE FUNCTION trigger_func() RETURNS TRIGGER AS $$
			BEGIN
				NEW.v1 := NEW.v1 || '_' || NEW.pk::text;
				INSERT INTO test2 VALUES (NEW.pk, NEW.v1);
				RETURN NEW;
			END;
			$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER test_trigger AFTER UPDATE ON test FOR EACH ROW EXECUTE FUNCTION trigger_func();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    "UPDATE test SET v1 = v1 || '|' WHERE pk IN (1, 2);",
					Expected: []sql.Row{},
				},
				{
					Query: "SELECT * FROM test;",
					Expected: []sql.Row{
						{1, "hi|"},
						{2, "there|"},
					},
				},
				{
					Query: "SELECT * FROM test2;",
					Expected: []sql.Row{
						{1, "hi|_1"},
						{2, "there|_2"},
					},
				},
			},
		},
		{
			Name: "AFTER DELETE returning NULL",
			SetUpScript: []string{
				"CREATE TABLE test (pk INT PRIMARY KEY, v1 TEXT);",
				"CREATE TABLE test2 (pk INT PRIMARY KEY, v1 TEXT);",
				"INSERT INTO test VALUES (1, 'hi'), (2, 'there');",
				`CREATE FUNCTION trigger_func() RETURNS TRIGGER AS $$
			BEGIN
				INSERT INTO test2 VALUES (OLD.pk, OLD.v1);
				RETURN NULL;
			END;
			$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER test_trigger AFTER DELETE ON test FOR EACH ROW EXECUTE FUNCTION trigger_func();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT * FROM test;",
					Expected: []sql.Row{
						{1, "hi"},
						{2, "there"},
					},
				},
				{
					Query:    "SELECT * FROM test2;",
					Expected: []sql.Row{},
				},
				{
					Query:    "DELETE FROM test WHERE pk = 1;",
					Expected: []sql.Row{},
				},
				{
					Query: "SELECT * FROM test;",
					Expected: []sql.Row{
						{2, "there"},
					},
				},
				{
					Query: "SELECT * FROM test2;",
					Expected: []sql.Row{
						{1, "hi"},
					},
				},
			},
		},
		{
			Name: "Cascading DELETE into INSERT, different tables",
			SetUpScript: []string{
				"CREATE TABLE test (pk INT PRIMARY KEY, v1 TEXT);",
				"CREATE TABLE test2 (pk INT PRIMARY KEY, v1 TEXT);",
				"INSERT INTO test VALUES (1, 'hi'), (2, 'there');",
				`CREATE FUNCTION trigger_func() RETURNS TRIGGER AS $$
BEGIN
	INSERT INTO test2 VALUES (OLD.pk, OLD.v1);
	RETURN OLD;
END;
$$ LANGUAGE plpgsql;`,
				`CREATE FUNCTION trigger_func2() RETURNS TRIGGER AS $$
BEGIN
	NEW.pk := NEW.pk + 100;
	RETURN NEW;
END;
$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER test_trigger BEFORE DELETE ON test FOR EACH ROW EXECUTE FUNCTION trigger_func();`,
				`CREATE TRIGGER test_trigger BEFORE INSERT ON test2 FOR EACH ROW EXECUTE FUNCTION trigger_func2();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT * FROM test;",
					Expected: []sql.Row{
						{1, "hi"},
						{2, "there"},
					},
				},
				{
					Query:    "SELECT * FROM test2;",
					Expected: []sql.Row{},
				},
				{
					Query:    "DELETE FROM test WHERE pk = 1;",
					Expected: []sql.Row{},
				},
				{
					Query: "SELECT * FROM test;",
					Expected: []sql.Row{
						{2, "there"},
					},
				},
				{
					Query: "SELECT * FROM test2;",
					Expected: []sql.Row{
						{101, "hi"},
					},
				},
			},
		},
		{
			Name: "Cascading INSERT into UPDATE, same table",
			SetUpScript: []string{
				"CREATE TABLE test (pk INT PRIMARY KEY, v1 TEXT);",
				`CREATE FUNCTION trigger_func() RETURNS TRIGGER AS $$
BEGIN
	UPDATE test SET v1 = v1 || NEW.pk::text;
	RETURN NEW;
END;
$$ LANGUAGE plpgsql;`,
				`CREATE FUNCTION trigger_func2() RETURNS TRIGGER AS $$
BEGIN
	NEW.v1 := NEW.v1 || '_u';
	RETURN NEW;
END;
$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER test_trigger BEFORE INSERT ON test FOR EACH ROW EXECUTE FUNCTION trigger_func();`,
				`CREATE TRIGGER test_trigger2 BEFORE UPDATE ON test FOR EACH ROW EXECUTE FUNCTION trigger_func2();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    "INSERT INTO test VALUES (1, 'hi'), (2, 'there');",
					Expected: []sql.Row{},
				},
				{
					Query: "SELECT * FROM test ORDER BY pk;",
					Skip:  true, // TODO: the UPDATE cannot see the table's contents until the INSERT has completely finished
					Expected: []sql.Row{
						{1, "hi2_u"},
						{2, "there"},
					},
				},
			},
		},
		{
			Name: "Multiple triggers on same table",
			SetUpScript: []string{
				"CREATE TABLE test (pk INT PRIMARY KEY, v1 TEXT);",
				`CREATE FUNCTION trigger_func_a() RETURNS TRIGGER AS $$
BEGIN
	NEW.v1 := NEW.v1 || 'a';
	RETURN NEW;
END;
$$ LANGUAGE plpgsql;`,
				`CREATE FUNCTION trigger_func_c() RETURNS TRIGGER AS $$
BEGIN
	NEW.v1 := NEW.v1 || 'c';
	RETURN NEW;
END;
$$ LANGUAGE plpgsql;`,
				`CREATE FUNCTION trigger_func_b() RETURNS TRIGGER AS $$
BEGIN
	NEW.v1 := NEW.v1 || 'b';
	RETURN NEW;
END;
$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER test_trigger_b BEFORE INSERT ON test FOR EACH ROW EXECUTE FUNCTION trigger_func_b();`,
				`CREATE TRIGGER test_trigger_a BEFORE INSERT ON test FOR EACH ROW EXECUTE FUNCTION trigger_func_a();`,
				`CREATE TRIGGER test_trigger_c BEFORE INSERT ON test FOR EACH ROW EXECUTE FUNCTION trigger_func_c();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    "INSERT INTO test VALUES (1, 'hi'), (2, 'there');",
					Expected: []sql.Row{},
				},
				{
					Query: "SELECT * FROM test ORDER BY pk;",
					Expected: []sql.Row{
						{1, "hiabc"},
						{2, "thereabc"},
					},
				},
			},
		},
		{
			Name: "Stack depth limit exceeded",
			SetUpScript: []string{
				"CREATE TABLE test (pk INT PRIMARY KEY, v1 TEXT);",
				"CREATE TABLE test2 (pk INT PRIMARY KEY, v1 TEXT);",
				`CREATE FUNCTION trigger_func() RETURNS TRIGGER AS $$
BEGIN
	INSERT INTO test2 VALUES (NEW.pk+2, NEW.v1 || '_');
	RETURN NEW;
END;
$$ LANGUAGE plpgsql;`,
				`CREATE FUNCTION trigger_func2() RETURNS TRIGGER AS $$
BEGIN
	INSERT INTO test VALUES (NEW.pk+4, NEW.v1 || '|');
	RETURN NEW;
END;
$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER test_trigger BEFORE INSERT ON test FOR EACH ROW EXECUTE FUNCTION trigger_func();`,
				`CREATE TRIGGER test_trigger BEFORE INSERT ON test2 FOR EACH ROW EXECUTE FUNCTION trigger_func2();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       "INSERT INTO test VALUES (1, 'hi'), (2, 'there');",
					Skip:        true, // TODO: currently we'll just run until we run out of memory, need to abort before that
					ExpectedErr: "stack depth limit exceeded",
				},
			},
		},
		{
			Name: "DELETE TABLE deletes attached triggers",
			SetUpScript: []string{
				"CREATE TABLE test (pk INT PRIMARY KEY, v1 TEXT);",
				`CREATE FUNCTION trigger_func() RETURNS TRIGGER AS $$
BEGIN
	NEW.v1 := NEW.v1 || '_';
	RETURN NEW;
END;
$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER test_trigger BEFORE INSERT ON test FOR EACH ROW EXECUTE FUNCTION trigger_func();`,
				`CREATE TRIGGER test_trigger2 BEFORE UPDATE ON test FOR EACH ROW EXECUTE FUNCTION trigger_func();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       "CREATE TRIGGER test_trigger BEFORE INSERT ON test FOR EACH ROW EXECUTE FUNCTION trigger_func();",
					ExpectedErr: "already exists",
				},
				{
					Query:       "CREATE TRIGGER test_trigger2 BEFORE UPDATE ON test FOR EACH ROW EXECUTE FUNCTION trigger_func();",
					ExpectedErr: "already exists",
				},
				{
					Query:    "DROP TABLE test;",
					Expected: []sql.Row{},
				},
				{
					Query:    "CREATE TABLE test (pk INT PRIMARY KEY, v1 TEXT);",
					Expected: []sql.Row{},
				},
				{
					Query:    "CREATE TRIGGER test_trigger BEFORE INSERT ON test FOR EACH ROW EXECUTE FUNCTION trigger_func();",
					Expected: []sql.Row{},
				},
				{
					Query:    "CREATE TRIGGER test_trigger2 BEFORE UPDATE ON test FOR EACH ROW EXECUTE FUNCTION trigger_func();",
					Expected: []sql.Row{},
				},
			},
		},
		{
			Name: "WHEN on BEFORE INSERT",
			SetUpScript: []string{
				"CREATE TABLE test (pk INT PRIMARY KEY, v1 TEXT);",
				`CREATE FUNCTION trigger_func1() RETURNS TRIGGER AS $$
BEGIN
	NEW.v1 := NEW.pk::text || '_' || NEW.v1;
	RETURN NEW;
END;
$$ LANGUAGE plpgsql;`,
				`CREATE FUNCTION trigger_func2() RETURNS TRIGGER AS $$
BEGIN
	NEW.v1 := NEW.v1 || '_' || NEW.pk::text;
	RETURN NEW;
END;
$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER test_trigger1 BEFORE INSERT ON test FOR EACH ROW WHEN (NEW.pk < 1) EXECUTE FUNCTION trigger_func1();`,
				`CREATE TRIGGER test_trigger2 BEFORE INSERT ON test FOR EACH ROW WHEN (NEW.pk > 1) EXECUTE FUNCTION trigger_func2();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    "INSERT INTO test VALUES (0, 'hi'), (1, 'there'), (2, 'dude');",
					Expected: []sql.Row{},
				},
				{
					Query: "SELECT * FROM test;",
					Expected: []sql.Row{
						{0, "0_hi"},
						{1, "there"},
						{2, "dude_2"},
					},
				},
			},
		},
		{
			Name: "WHEN with non-boolean expression",
			SetUpScript: []string{
				"CREATE TABLE test (pk INT PRIMARY KEY, v1 TEXT);",
				`CREATE FUNCTION trigger_func() RETURNS TRIGGER AS $$
BEGIN
	NEW.v1 := NEW.pk::text || '_' || NEW.v1;
	RETURN NEW;
END;
$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER test_trigger BEFORE INSERT ON test FOR EACH ROW WHEN (NEW.pk + 1) EXECUTE FUNCTION trigger_func();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       "INSERT INTO test VALUES (1, 'hi'), (2, 'there');",
					ExpectedErr: "argument of WHEN must be type boolean",
				},
			},
		},
		{
			Name: "Table as type",
			Skip: true, // TODO: figure out why this is not recognizing rec.qty as valid
			SetUpScript: []string{
				`CREATE TABLE test (id INT4 PRIMARY KEY, name TEXT NOT NULL, qty INT4 NOT NULL, price REAL NOT NULL);`,
				`CREATE FUNCTION trigger_func() RETURNS trigger AS $$
DECLARE
	rec test;
BEGIN
	rec := NEW;
	IF rec.qty < 0 THEN
		rec.qty := -rec.qty;
	END IF;
	NEW := rec;
	RETURN NEW;
END; $$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER test_trigger BEFORE INSERT ON test FOR EACH ROW EXECUTE FUNCTION trigger_func();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `INSERT INTO test VALUES (1, 'apple', 3, 2.5), (2, 'banana', -5, -1.2);`,
					Expected: []sql.Row{},
				}, {
					Query: `SELECT * FROM test;`,
					Expected: []sql.Row{
						{1, "apple", 3, 2.5},
						{2, "banana", 5, -1.2},
					},
				},
			},
		},
		{
			Name: "trigger to call procedure that updates another table using dynamic execute",
			SetUpScript: []string{
				`create table public."Collections"(
				 id uuid PRIMARY KEY NOT NULL,
				 name text not null,
				 username varchar(28) not null,
				 total_tracks integer DEFAULT 0);`,
				`INSERT INTO public."Collections" (id, name, username, total_tracks) VALUES ('550e8400-e29b-41d4-a716-446655440000', 'My Custom Playlist', 'user_alpha', 10);`,
				`create table public."CollectionItems"(
			collection_id uuid not null,
			track_id integer not null);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE OR REPLACE FUNCTION update_collections()
  RETURNS trigger AS $$
  DECLARE
    BEGIN
    IF TG_OP = 'INSERT' THEN
      EXECUTE 'update public."Collections" set total_tracks=total_tracks+1 where id = $1;'
      USING NEW.collection_id;
    END IF;

    IF TG_OP = 'DELETE' THEN 
      EXECUTE 'update public."Collections" set total_tracks=total_tracks-1 where id = $1;'
      USING OLD.collection_id;
    END IF;
    
    RETURN NEW;
    END;
$$ LANGUAGE plpgsql;`,
					Expected: []sql.Row{},
				},
				{
					Query: `CREATE TRIGGER update_collection
				AFTER INSERT OR DELETE ON public."CollectionItems"
				FOR EACH ROW EXECUTE PROCEDURE update_collections();`,
					Expected: []sql.Row{},
				},
				{
					Query:    `INSERT INTO public."CollectionItems" (collection_id, track_id) VALUES ('550e8400-e29b-41d4-a716-446655440000', 101);`,
					Expected: []sql.Row{},
				},
				{
					Query:    `SELECT total_tracks FROM public."Collections"`,
					Expected: []sql.Row{{11}},
				},
			},
		},
		{
			Name: "DROP TRIGGER",
			SetUpScript: []string{
				`CREATE TABLE test (pk INT PRIMARY KEY, v1 TEXT);`,
				`CREATE FUNCTION trigger_func() RETURNS TRIGGER AS $$
				BEGIN
					NEW.v1 := NEW.v1 || '_' || NEW.pk::text;
					RETURN NEW;
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER test_trigger BEFORE INSERT ON test FOR EACH ROW EXECUTE FUNCTION trigger_func();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    "DROP TRIGGER test_trigger ON test;",
					Expected: []sql.Row{},
				},
				{
					Query:    "DROP TRIGGER IF EXISTS test_trigger ON test;",
					Expected: []sql.Row{},
				},
			},
		},
	})
}

func TestStatementTriggerTransitionTables(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "plain statement triggers fire once",
			SetUpScript: []string{
				`CREATE TABLE plain_target (id INT PRIMARY KEY);`,
				`CREATE TABLE plain_audit (seq SERIAL PRIMARY KEY, phase TEXT, seen_count BIGINT);`,
				`CREATE FUNCTION audit_plain_statement() RETURNS trigger AS $$
					BEGIN
						INSERT INTO plain_audit (phase, seen_count)
						VALUES (
							TG_WHEN || ':' || TG_LEVEL || ':' || TG_OP,
							1
						);
						RETURN NULL;
					END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER plain_before
					BEFORE INSERT ON plain_target
					FOR EACH STATEMENT EXECUTE FUNCTION audit_plain_statement();`,
				`CREATE TRIGGER plain_after
					AFTER INSERT ON plain_target
					EXECUTE FUNCTION audit_plain_statement();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO plain_target VALUES (1), (2);`,
				},
				{
					Query: `SELECT phase, seen_count FROM plain_audit ORDER BY seq;`,
					Expected: []sql.Row{
						{"BEFORE:STATEMENT:INSERT", int64(1)},
						{"AFTER:STATEMENT:INSERT", int64(1)},
					},
				},
				{
					Query:    `SELECT count(*) FROM plain_target;`,
					Expected: []sql.Row{{int64(2)}},
				},
			},
		},
		{
			Name: "AFTER statement triggers see transition tables",
			SetUpScript: []string{
				`CREATE TABLE target (id INT PRIMARY KEY, v INT);`,
				`CREATE TABLE statement_audit (
					seq SERIAL PRIMARY KEY,
					tg_name TEXT,
					tg_when TEXT,
					tg_level TEXT,
					tg_op TEXT,
					old_count BIGINT,
					new_count BIGINT,
					old_sum BIGINT,
					new_sum BIGINT
				);`,
				`CREATE FUNCTION audit_insert_statement() RETURNS trigger AS $$
					BEGIN
						INSERT INTO statement_audit (
							tg_name, tg_when, tg_level, tg_op,
							old_count, new_count, old_sum, new_sum
						) VALUES (
							TG_NAME, TG_WHEN, TG_LEVEL, TG_OP,
							0, (SELECT count(*) FROM new_rows),
							0, (SELECT coalesce(sum(v), 0) FROM new_rows)
						);
						RETURN NULL;
					END;
				$$ LANGUAGE plpgsql;`,
				`CREATE FUNCTION audit_update_statement() RETURNS trigger AS $$
					BEGIN
						INSERT INTO statement_audit (
							tg_name, tg_when, tg_level, tg_op,
							old_count, new_count, old_sum, new_sum
						) VALUES (
							TG_NAME, TG_WHEN, TG_LEVEL, TG_OP,
							(SELECT count(*) FROM old_rows),
							(SELECT count(*) FROM new_rows),
							(SELECT coalesce(sum(v), 0) FROM old_rows),
							(SELECT coalesce(sum(v), 0) FROM new_rows)
						);
						RETURN NULL;
					END;
				$$ LANGUAGE plpgsql;`,
				`CREATE FUNCTION audit_delete_statement() RETURNS trigger AS $$
					BEGIN
						INSERT INTO statement_audit (
							tg_name, tg_when, tg_level, tg_op,
							old_count, new_count, old_sum, new_sum
						) VALUES (
							TG_NAME, TG_WHEN, TG_LEVEL, TG_OP,
							(SELECT count(*) FROM old_rows), 0,
							(SELECT coalesce(sum(v), 0) FROM old_rows), 0
						);
						RETURN NULL;
					END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER audit_insert
					AFTER INSERT ON target
					REFERENCING NEW TABLE AS new_rows
					FOR EACH STATEMENT EXECUTE FUNCTION audit_insert_statement();`,
				`CREATE TRIGGER audit_update
					AFTER UPDATE ON target
					REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
					FOR EACH STATEMENT EXECUTE FUNCTION audit_update_statement();`,
				`CREATE TRIGGER audit_delete
					AFTER DELETE ON target
					REFERENCING OLD TABLE AS old_rows
					FOR EACH STATEMENT EXECUTE FUNCTION audit_delete_statement();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO target VALUES (1, 10), (2, 20), (3, 30);`,
				},
				{
					Query: `UPDATE target SET v = v + 1 WHERE id IN (1, 2);`,
				},
				{
					Query: `UPDATE target SET v = v + 100 WHERE id = 999;`,
				},
				{
					Query: `DELETE FROM target WHERE id = 3;`,
				},
				{
					Query: `SELECT tg_name, tg_when, tg_level, tg_op, old_count, new_count, old_sum, new_sum
						FROM statement_audit ORDER BY seq;`,
					Expected: []sql.Row{
						{"audit_insert", "AFTER", "STATEMENT", "INSERT", int64(0), int64(3), int64(0), int64(60)},
						{"audit_update", "AFTER", "STATEMENT", "UPDATE", int64(2), int64(2), int64(30), int64(32)},
						{"audit_update", "AFTER", "STATEMENT", "UPDATE", int64(0), int64(0), int64(0), int64(0)},
						{"audit_delete", "AFTER", "STATEMENT", "DELETE", int64(1), int64(0), int64(30), int64(0)},
					},
				},
			},
		},
	})
}
