package postgres16

import (
	. "github.com/dolthub/doltgresql/testing/go"

	"testing"
)

func TestSubqueries(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "Subselect",
			SetUpScript: []string{
				`CREATE TABLE test (id INT);`,
				`INSERT INTO test VALUES (1), (3), (2);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM test WHERE id = (SELECT 2);`, PostgresOracle: ScriptTestPostgresOracle{ID: "subqueries-test-testsubqueries-0001-select-*-from-test-where"},
				},
				{
					Query: `SELECT *, (SELECT id from test where id = 2) FROM test order by id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "subqueries-test-testsubqueries-0002-select-*-select-id-from", Compare: "sqlstate"},
				},
				{
					Query: `SELECT *, (SELECT id from test t2 where t2.id = test.id) FROM test order by id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "subqueries-test-testsubqueries-0003-select-*-select-id-from", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "IN",
			SetUpScript: []string{
				`CREATE TABLE test (id INT);`,
				`INSERT INTO test VALUES (1), (3), (2);`,

				`CREATE TABLE test2 (id INT, test_id INT, txt text);`,
				`INSERT INTO test2 VALUES (1, 1, 'foo'), (2, 10, 'bar'), (3, 2, 'baz');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM test WHERE id IN (SELECT * FROM test WHERE id = 2);`, PostgresOracle: ScriptTestPostgresOracle{ID: "subqueries-test-testsubqueries-0004-select-*-from-test-where"},
				},
				{
					Query: `SELECT * FROM test WHERE id IN (SELECT id FROM test WHERE id = 3);`, PostgresOracle: ScriptTestPostgresOracle{ID: "subqueries-test-testsubqueries-0005-select-*-from-test-where"},
				},
				{
					Query: `SELECT * FROM test WHERE id IN (SELECT * FROM test WHERE id > 0);`, PostgresOracle: ScriptTestPostgresOracle{ID: "subqueries-test-testsubqueries-0006-select-*-from-test-where"},
				},
				{
					Query: `SELECT * FROM test2 WHERE test_id IN (SELECT * FROM test WHERE id = 2);`, PostgresOracle: ScriptTestPostgresOracle{ID: "subqueries-test-testsubqueries-0007-select-*-from-test2-where"},
				},
				{
					Query: `SELECT * FROM test2 WHERE test_id IN (SELECT * FROM test WHERE id > 0);`, PostgresOracle: ScriptTestPostgresOracle{ID: "subqueries-test-testsubqueries-0008-select-*-from-test2-where"},
				},
				{
					Query: `SELECT id FROM test2 WHERE (2, 10) IN (SELECT id, test_id FROM test2 WHERE id > 0);`,
					Skip:  true, PostgresOracle: // won't pass until we have a doltgres tuple type to match against for equality funcs
					ScriptTestPostgresOracle{ID: "subqueries-test-testsubqueries-0009-select-id-from-test2-where"},
				},
				{
					Query: `SELECT id FROM test2 WHERE (id, test_id) IN (SELECT id, test_id FROM test2 WHERE id > 0);`,
					Skip:  true, PostgresOracle: // won't pass until we have a doltgres tuple type to match against for equality funcs
					ScriptTestPostgresOracle{ID: "subqueries-test-testsubqueries-0010-select-id-from-test2-where"},
				},
			},
		},
		{
			Name: "subquery equality",
			SetUpScript: []string{
				`CREATE TABLE test (id INT, c varchar);`,
				`INSERT INTO test VALUES (1, 'a'), (2, 'b'), (3, 'b');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM test WHERE id = (SELECT id from test where id = 2);`, PostgresOracle: ScriptTestPostgresOracle{ID: "subqueries-test-testsubqueries-0011-select-*-from-test-where"},
				},
				{
					Skip:  true, // panic in equality func
					Query: `SELECT (SELECT id from test where id = 2) = (SELECT id from test where id = 2);`, PostgresOracle: ScriptTestPostgresOracle{ID: "subqueries-test-testsubqueries-0012-select-select-id-from-test"},
				},
				{
					Skip:  true, // panic in equality func
					Query: `SELECT (SELECT c from test where id = 2) = (SELECT c from test where id = 3);`, PostgresOracle: ScriptTestPostgresOracle{ID: "subqueries-test-testsubqueries-0013-select-select-c-from-test"},
				},
				{
					Skip:  true, // panic in equality func
					Query: `SELECT (SELECT c from test where id = 1) = (SELECT c from test where id = 2);`, PostgresOracle: ScriptTestPostgresOracle{ID: "subqueries-test-testsubqueries-0014-select-select-c-from-test"},
				},
			},
		},
		{
			Name: "array flatten",
			SetUpScript: []string{
				`CREATE TABLE test (id INT, c varchar);`,
				`INSERT INTO test VALUES (1, 'a'), (2, 'b'), (3, 'c');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT ARRAY(SELECT id FROM test order by 1);`, PostgresOracle: ScriptTestPostgresOracle{ID: "subqueries-test-testsubqueries-0015-select-array-select-id-from"},
				},
				{
					Query: `SELECT ARRAY(SELECT c FROM test order by id limit 1);`, PostgresOracle: ScriptTestPostgresOracle{ID: "subqueries-test-testsubqueries-0016-select-array-select-c-from"},
				},
				{
					Query: `SELECT ARRAY(SELECT c FROM test order by id desc);`, PostgresOracle: ScriptTestPostgresOracle{ID: "subqueries-test-testsubqueries-0017-select-array-select-c-from"},
				},
				{
					Query: `SELECT ARRAY(SELECT id, id FROM test order by 1);`, PostgresOracle: ScriptTestPostgresOracle{ID: "subqueries-test-testsubqueries-0018-select-array-select-id-id", Compare: "sqlstate"},
				},
				{
					Query: `SELECT array_to_string(ARRAY(SELECT id FROM test order by 1), ',')`, PostgresOracle: ScriptTestPostgresOracle{ID: "subqueries-test-testsubqueries-0019-select-array_to_string-array-select-id"},
				},
				{
					Query: `WITH flattened AS (SELECT ARRAY(SELECT id FROM test ORDER BY 1) AS ids)
SELECT array_to_string(ids, ',') FROM flattened;`, PostgresOracle: ScriptTestPostgresOracle{ID: "subqueries-test-testsubqueries-0020-with-flattened-as-select-array"},
				},
			},
		},
	})
}

func TestSubqueryJoins(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "subquery join",
			SetUpScript: []string{
				"CREATE TABLE t1 (a int primary key);",
				"CREATE TABLE t2 (b int primary key);",
				"INSERT INTO t1 VALUES (1), (2), (3);",
				"INSERT INTO t2 VALUES (2), (3), (4);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT
s1.a FROM (SELECT a from t1) s1
INNER JOIN t2 q1
ON q1.b = s1.a
ORDER BY 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "subqueries-test-testsubqueryjoins-0001-select-s1.a-from-select-a"},
				},
			},
		},
		{
			Name: "subquery join with aliased column",
			SetUpScript: []string{
				"CREATE TABLE t1 (a int primary key);",
				"CREATE TABLE t2 (b int primary key);",
				"INSERT INTO t1 VALUES (1), (2), (3);",
				"INSERT INTO t2 VALUES (2), (3), (4);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT
s1.c FROM (SELECT a as c from t1) s1
INNER JOIN t2 q1
ON q1.b = s1.c
ORDER BY 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "subqueries-test-testsubqueryjoins-0002-select-s1.c-from-select-a"},
				},
			},
		},
		{
			Name: "subquery join with column renames",
			SetUpScript: []string{
				"CREATE TABLE t1 (a int primary key, b int);",
				"CREATE TABLE t2 (c int primary key);",
				"INSERT INTO t1 VALUES (1,10), (2,20), (3,30);",
				"INSERT INTO t2 VALUES (2), (3), (4);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT
s1.d FROM (SELECT b as f, a as g from t1) s1(d,e)
INNER JOIN t2 q1
ON q1.c = s1.e
ORDER BY 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "subqueries-test-testsubqueryjoins-0003-select-s1.d-from-select-b"},
				},
			},
		},
	})
}

func TestExistSubquery(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "basic case",
			SetUpScript: []string{
				`CREATE TABLE test (id INT PRIMARY KEY);`,
				`INSERT INTO test VALUES (1), (3), (2);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM test WHERE EXISTS (SELECT 123);`, PostgresOracle: ScriptTestPostgresOracle{ID: "subqueries-test-testexistsubquery-0001-select-*-from-test-where"},
				},
				{
					Query: `SELECT * FROM test WHERE NOT EXISTS (SELECT 123);`, PostgresOracle: ScriptTestPostgresOracle{ID: "subqueries-test-testexistsubquery-0002-select-*-from-test-where"},
				},
				{
					Query: `SELECT 123 WHERE EXISTS (SELECT * FROM test);`, PostgresOracle: ScriptTestPostgresOracle{ID: "subqueries-test-testexistsubquery-0003-select-123-where-exists-select"},
				},
				{
					Query: `SELECT 123 WHERE EXISTS (SELECT * FROM test WHERE id > 10);`, PostgresOracle: ScriptTestPostgresOracle{ID: "subqueries-test-testexistsubquery-0004-select-123-where-exists-select"},
				},
				{
					Query: `SELECT 123 WHERE NOT EXISTS (SELECT * FROM test);`, PostgresOracle: ScriptTestPostgresOracle{ID: "subqueries-test-testexistsubquery-0005-select-123-where-not-exists"},
				},
				{
					Query: `SELECT 123 WHERE NOT EXISTS (SELECT * FROM test WHERE id > 10);`, PostgresOracle: ScriptTestPostgresOracle{ID: "subqueries-test-testexistsubquery-0006-select-123-where-not-exists"},
				},
			},
		},
	})
}
