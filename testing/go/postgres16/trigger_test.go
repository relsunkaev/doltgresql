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
					Query: "INSERT INTO test VALUES (1, 'hi'), (2, 'there');", PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0001-insert-into-test-values-1"},
				},
				{
					Query: "SELECT * FROM test;", PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0002-select-*-from-test"},
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
					Query: "UPDATE test SET v1 = v1 || '|' WHERE pk IN (1, 2);", PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0003-update-test-set-v1-="},
				},
				{
					Query: "SELECT * FROM test;", PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0004-select-*-from-test"},
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
					Query: "SELECT * FROM test;", PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0005-select-*-from-test"},
				},
				{
					Query: "SELECT * FROM test2;", PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0006-select-*-from-test2"},
				},
				{
					Query: "DELETE FROM test WHERE pk = 1;", PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0007-delete-from-test-where-pk"},
				},
				{
					Query: "SELECT * FROM test;", PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0008-select-*-from-test"},
				},
				{
					Query: "SELECT * FROM test2;", PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0009-select-*-from-test2"},
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
					Query: "INSERT INTO test VALUES (1, 'hi'), (2, 'there');", PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0010-insert-into-test-values-1"},
				},
				{
					Query: "SELECT * FROM test;", PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0011-select-*-from-test"},
				},
				{
					Query: "SELECT * FROM test2;", PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0012-select-*-from-test2"},
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
					Query: "UPDATE test SET v1 = v1 || '|' WHERE pk IN (1, 2);", PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0013-update-test-set-v1-="},
				},
				{
					Query: "SELECT * FROM test;", PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0014-select-*-from-test"},
				},
				{
					Query: "SELECT * FROM test2;", PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0015-select-*-from-test2"},
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
					Query: "SELECT * FROM test;", PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0016-select-*-from-test"},
				},
				{
					Query: "SELECT * FROM test2;", PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0017-select-*-from-test2"},
				},
				{
					Query: "DELETE FROM test WHERE pk = 1;", PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0018-delete-from-test-where-pk"},
				},
				{
					Query: "SELECT * FROM test;", PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0019-select-*-from-test"},
				},
				{
					Query: "SELECT * FROM test2;", PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0020-select-*-from-test2"},
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
					Query: "SELECT * FROM test;", PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0021-select-*-from-test"},
				},
				{
					Query: "SELECT * FROM test2;", PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0022-select-*-from-test2"},
				},
				{
					Query: "DELETE FROM test WHERE pk = 1;", PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0023-delete-from-test-where-pk"},
				},
				{
					Query: "SELECT * FROM test;", PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0024-select-*-from-test"},
				},
				{
					Query: "SELECT * FROM test2;", PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0025-select-*-from-test2"},
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
					Query: "INSERT INTO test VALUES (1, 'hi'), (2, 'there');", PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0026-insert-into-test-values-1"},
				},
				{
					Query: "SELECT * FROM test;", PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0027-select-*-from-test"},
				},
				{
					Query: "SELECT * FROM test2;", PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0028-select-*-from-test2"},
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
					Query: "UPDATE test SET v1 = v1 || '|' WHERE pk IN (1, 2);", PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0029-update-test-set-v1-="},
				},
				{
					Query: "SELECT * FROM test;", PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0030-select-*-from-test"},
				},
				{
					Query: "SELECT * FROM test2;", PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0031-select-*-from-test2"},
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
					Query: "SELECT * FROM test;", PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0032-select-*-from-test"},
				},
				{
					Query: "SELECT * FROM test2;", PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0033-select-*-from-test2"},
				},
				{
					Query: "DELETE FROM test WHERE pk = 1;", PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0034-delete-from-test-where-pk"},
				},
				{
					Query: "SELECT * FROM test;", PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0035-select-*-from-test"},
				},
				{
					Query: "SELECT * FROM test2;", PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0036-select-*-from-test2"},
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
					Query: "SELECT * FROM test;", PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0037-select-*-from-test"},
				},
				{
					Query: "SELECT * FROM test2;", PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0038-select-*-from-test2"},
				},
				{
					Query: "DELETE FROM test WHERE pk = 1;", PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0039-delete-from-test-where-pk"},
				},
				{
					Query: "SELECT * FROM test;", PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0040-select-*-from-test"},
				},
				{
					Query: "SELECT * FROM test2;", PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0041-select-*-from-test2"},
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
					Query: "INSERT INTO test VALUES (1, 'hi'), (2, 'there');", PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0042-insert-into-test-values-1"},
				},
				{
					Query: "SELECT * FROM test ORDER BY pk;",
					Skip:  true, PostgresOracle: // TODO: the UPDATE cannot see the table's contents until the INSERT has completely finished
					ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0043-select-*-from-test-order"},
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
					Query: "INSERT INTO test VALUES (1, 'hi'), (2, 'there');", PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0044-insert-into-test-values-1"},
				},
				{
					Query: "SELECT * FROM test ORDER BY pk;", PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0045-select-*-from-test-order"},
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
					Query: "INSERT INTO test VALUES (1, 'hi'), (2, 'there');",
					Skip:  true, PostgresOracle: // TODO: currently we'll just run until we run out of memory, need to abort before that
					ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0046-insert-into-test-values-1", Compare: "sqlstate"},
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
					Query: "CREATE TRIGGER test_trigger BEFORE INSERT ON test FOR EACH ROW EXECUTE FUNCTION trigger_func();", PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0047-create-trigger-test_trigger-before-insert", Compare: "sqlstate"},
				},
				{
					Query: "CREATE TRIGGER test_trigger2 BEFORE UPDATE ON test FOR EACH ROW EXECUTE FUNCTION trigger_func();", PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0048-create-trigger-test_trigger2-before-update", Compare: "sqlstate"},
				},
				{
					Query: "DROP TABLE test;", PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0049-drop-table-test"},
				},
				{
					Query: "CREATE TABLE test (pk INT PRIMARY KEY, v1 TEXT);", PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0050-create-table-test-pk-int"},
				},
				{
					Query: "CREATE TRIGGER test_trigger BEFORE INSERT ON test FOR EACH ROW EXECUTE FUNCTION trigger_func();", PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0051-create-trigger-test_trigger-before-insert"},
				},
				{
					Query: "CREATE TRIGGER test_trigger2 BEFORE UPDATE ON test FOR EACH ROW EXECUTE FUNCTION trigger_func();", PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0052-create-trigger-test_trigger2-before-update"},
				},
			},
		},
		{
			Name: "TRUNCATE statement triggers",
			SetUpScript: []string{
				"CREATE TABLE test (pk INT PRIMARY KEY, v1 TEXT);",
				"CREATE TABLE trigger_log (seq INT, entry TEXT, op TEXT, table_name TEXT);",
				"INSERT INTO test VALUES (1, 'hi'), (2, 'there');",
				`CREATE FUNCTION truncate_trigger_func() RETURNS TRIGGER AS $$
BEGIN
	INSERT INTO trigger_log VALUES ((SELECT count(*) + 1 FROM trigger_log), TG_WHEN || ' ' || TG_LEVEL, TG_OP, TG_TABLE_NAME);
	RETURN NULL;
END;
$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER before_truncate BEFORE TRUNCATE ON test FOR EACH STATEMENT EXECUTE FUNCTION truncate_trigger_func();`,
				`CREATE TRIGGER after_truncate AFTER TRUNCATE ON test FOR EACH STATEMENT EXECUTE FUNCTION truncate_trigger_func();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TRIGGER row_truncate BEFORE TRUNCATE ON test FOR EACH ROW EXECUTE FUNCTION truncate_trigger_func();`, PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0053-create-trigger-row_truncate-before-truncate", Compare: "sqlstate"},
				},
				{
					Query: `SELECT tgname, tgtype::int
						FROM pg_trigger
						WHERE tgname IN ('before_truncate', 'after_truncate')
						ORDER BY tgname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0054-select-tgname-tgtype::int-from-pg_trigger"},
				},
				{
					Query: `SELECT event_manipulation, action_timing, action_orientation
						FROM information_schema.triggers
						WHERE trigger_name IN ('before_truncate', 'after_truncate')
						ORDER BY trigger_name;`, PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0055-select-event_manipulation-action_timing-action_orientation-from"},
				},
				{
					Query: `TRUNCATE test;`,
				},
				{
					Query: `SELECT * FROM test;`, PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0056-select-*-from-test"},
				},
				{
					Query: `SELECT seq, entry, op, table_name FROM trigger_log ORDER BY seq;`, PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0057-select-seq-entry-op-table_name"},
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
					Query: "INSERT INTO test VALUES (0, 'hi'), (1, 'there'), (2, 'dude');", PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0058-insert-into-test-values-0"},
				},
				{
					Query: "SELECT * FROM test;", PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0059-select-*-from-test"},
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
					Query: `INSERT INTO test VALUES (1, 'apple', 3, 2.5), (2, 'banana', -5, -1.2);`, PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0061-insert-into-test-values-1"},
				}, {
					Query: `SELECT * FROM test;`, PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0062-select-*-from-test"},
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
				FOR EACH ROW EXECUTE PROCEDURE update_collections();`, PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0064-create-trigger-update_collection-after-insert"},
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
					Query: "DROP TRIGGER test_trigger ON test;", PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0067-drop-trigger-test_trigger-on-test"},
				},
				{
					Query: "DROP TRIGGER IF EXISTS test_trigger ON test;", PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-testcreatetrigger-0068-drop-trigger-if-exists-test_trigger"},
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
					Query: `SELECT phase, seen_count FROM plain_audit ORDER BY seq;`, PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-teststatementtriggertransitiontables-0001-select-phase-seen_count-from-plain_audit"},
				},
				{
					Query: `SELECT count(*) FROM plain_target;`, PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-teststatementtriggertransitiontables-0002-select-count-*-from-plain_target"},
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
						FROM statement_audit ORDER BY seq;`, PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-teststatementtriggertransitiontables-0003-select-tg_name-tg_when-tg_level-tg_op"},
				},
			},
		},
		{
			Name: "AFTER statement trigger self-query sees post-statement target state",
			SetUpScript: []string{
				`CREATE TABLE self_query_target (id INT PRIMARY KEY, v INT);`,
				`CREATE TABLE self_query_audit (
					seq SERIAL PRIMARY KEY,
					op TEXT,
					row_count BIGINT,
					value_sum BIGINT
				);`,
				`CREATE FUNCTION audit_self_query_statement() RETURNS trigger AS $$
					BEGIN
						INSERT INTO self_query_audit (op, row_count, value_sum)
						VALUES (
							TG_OP,
							(SELECT count(*) FROM self_query_target),
							(SELECT coalesce(sum(v), 0) FROM self_query_target)
						);
						RETURN NULL;
					END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER self_query_after_insert
					AFTER INSERT ON self_query_target
					FOR EACH STATEMENT EXECUTE FUNCTION audit_self_query_statement();`,
				`CREATE TRIGGER self_query_after_update
					AFTER UPDATE ON self_query_target
					FOR EACH STATEMENT EXECUTE FUNCTION audit_self_query_statement();`,
				`CREATE TRIGGER self_query_after_delete
					AFTER DELETE ON self_query_target
					FOR EACH STATEMENT EXECUTE FUNCTION audit_self_query_statement();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO self_query_target VALUES (1, 10), (2, 20), (3, 30);`,
				},
				{
					Query: `UPDATE self_query_target SET v = v + 1 WHERE id IN (1, 2);`,
				},
				{
					Query: `DELETE FROM self_query_target WHERE id = 3;`,
				},
				{
					Query: `SELECT op, row_count, value_sum FROM self_query_audit ORDER BY seq;`, PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-teststatementtriggertransitiontables-0004-select-op-row_count-value_sum-from"},
				},
			},
		},
		{
			Name: "AFTER row triggers see statement transition tables",
			SetUpScript: []string{
				`CREATE TABLE row_transition_target (id INT PRIMARY KEY, v INT);`,
				`CREATE TABLE row_transition_audit (
					seq SERIAL PRIMARY KEY,
					op TEXT,
					row_id INT,
					old_count BIGINT,
					new_count BIGINT,
					old_sum BIGINT,
					new_sum BIGINT
				);`,
				`CREATE FUNCTION audit_insert_row_transition() RETURNS trigger AS $$
					BEGIN
						INSERT INTO row_transition_audit (
							op, row_id, old_count, new_count, old_sum, new_sum
						) VALUES (
							TG_OP, NEW.id,
							0, (SELECT count(*) FROM new_rows),
							0, (SELECT coalesce(sum(v), 0) FROM new_rows)
						);
						RETURN NULL;
					END;
				$$ LANGUAGE plpgsql;`,
				`CREATE FUNCTION audit_update_row_transition() RETURNS trigger AS $$
					BEGIN
						INSERT INTO row_transition_audit (
							op, row_id, old_count, new_count, old_sum, new_sum
						) VALUES (
							TG_OP, NEW.id,
							(SELECT count(*) FROM old_rows),
							(SELECT count(*) FROM new_rows),
							(SELECT coalesce(sum(v), 0) FROM old_rows),
							(SELECT coalesce(sum(v), 0) FROM new_rows)
						);
						RETURN NULL;
					END;
				$$ LANGUAGE plpgsql;`,
				`CREATE FUNCTION audit_delete_row_transition() RETURNS trigger AS $$
					BEGIN
						INSERT INTO row_transition_audit (
							op, row_id, old_count, new_count, old_sum, new_sum
						) VALUES (
							TG_OP, OLD.id,
							(SELECT count(*) FROM old_rows), 0,
							(SELECT coalesce(sum(v), 0) FROM old_rows), 0
						);
						RETURN NULL;
					END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER row_audit_insert
					AFTER INSERT ON row_transition_target
					REFERENCING NEW TABLE AS new_rows
					FOR EACH ROW EXECUTE FUNCTION audit_insert_row_transition();`,
				`CREATE TRIGGER row_audit_update
					AFTER UPDATE ON row_transition_target
					REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
					FOR EACH ROW EXECUTE FUNCTION audit_update_row_transition();`,
				`CREATE TRIGGER row_audit_delete
					AFTER DELETE ON row_transition_target
					REFERENCING OLD TABLE AS old_rows
					FOR EACH ROW EXECUTE FUNCTION audit_delete_row_transition();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO row_transition_target VALUES (1, 10), (2, 20), (3, 30);`,
				},
				{
					Query: `UPDATE row_transition_target SET v = v + 1 WHERE id IN (1, 2);`,
				},
				{
					Query: `DELETE FROM row_transition_target WHERE id = 3;`,
				},
				{
					Query: `SELECT op, row_id, old_count, new_count, old_sum, new_sum
						FROM row_transition_audit ORDER BY seq;`, PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-test-teststatementtriggertransitiontables-0005-select-op-row_id-old_count-new_count"},
				},
			},
		},
	})
}
