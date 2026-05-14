// Copyright 2023 Dolthub, Inc.
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

func TestAggregateFunctions(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "bool_and",
			SetUpScript: []string{
				`CREATE TABLE t1 (pk INT primary key, v1 BOOLEAN, v2 BOOLEAN);`,
				`INSERT INTO t1 VALUES (1, true, false), (2, true, true), (3, true, true);`,
				`CREATE TABLE t2 (v1 BOOLEAN);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT bool_and(v1), bool_and(v2) FROM t1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testaggregatefunctions-0001-select-bool_and-v1-bool_and-v2"},
				},
				{
					Query: `SELECT bool_and(v1 and v2) FROM t1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testaggregatefunctions-0002-select-bool_and-v1-and-v2"},
				},
				{
					Query: `SELECT bool_and(v1 and v2) FROM t1 where v1 and v2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testaggregatefunctions-0003-select-bool_and-v1-and-v2"},
				},
				{
					Query: `SELECT bool_and(v1) FROM t1 where pk > 10;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testaggregatefunctions-0004-select-bool_and-v1-from-t1"},
				},
				{
					Skip:  true, // building a values-derived table's type fails here, postgres is more permissive
					Query: `SELECT bool_and(a) FROM (VALUES(true),(false),(null)) r(a);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testaggregatefunctions-0005-select-bool_and-a-from-values"},
				},
				{
					Query: `SELECT bool_and(a) FROM (VALUES(true),(false),(null::bool)) r(a);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testaggregatefunctions-0006-select-bool_and-a-from-values"},
				},
				{
					Query: `SELECT bool_and(a) FROM (VALUES(null::bool),(true),(null::bool)) r(a);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testaggregatefunctions-0007-select-bool_and-a-from-values"},
				},
				{
					Query: `SELECT bool_and(v1) FROM t2`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testaggregatefunctions-0008-select-bool_and-v1-from-t2"},
				},
			},
		},
		{
			Name: "bool_or",
			SetUpScript: []string{
				`CREATE TABLE t1 (pk INT primary key, v1 BOOLEAN, v2 BOOLEAN);`,
				`INSERT INTO t1 VALUES (1, false, false), (2, true, true), (3, true, false);`,
				`CREATE TABLE t2 (v1 BOOLEAN);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT bool_or(v1), bool_or(v2) FROM t1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testaggregatefunctions-0009-select-bool_or-v1-bool_or-v2"},
				},
				{
					Query: `SELECT bool_or(v1), bool_or(v2) FROM t1 where pk <> 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testaggregatefunctions-0010-select-bool_or-v1-bool_or-v2"},
				},
				{
					Query: `SELECT bool_or(v1 and v2) FROM t1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testaggregatefunctions-0011-select-bool_or-v1-and-v2"},
				},
				{
					Query: `SELECT bool_or(v1) FROM t1 where pk > 10;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testaggregatefunctions-0012-select-bool_or-v1-from-t1"},
				},
				{
					Query: `SELECT bool_or(a) FROM (VALUES(true),(false),(null::bool)) r(a);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testaggregatefunctions-0013-select-bool_or-a-from-values"},
				},
				{
					Query: `SELECT bool_or(a) FROM (VALUES(null::bool),(false),(null::bool)) r(a);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testaggregatefunctions-0014-select-bool_or-a-from-values"},
				},
				{
					Query: `SELECT bool_or(v1) FROM t2`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testaggregatefunctions-0015-select-bool_or-v1-from-t2"},
				},
			},
		},
		{
			Name: "array_agg",
			SetUpScript: []string{
				`CREATE TABLE t1 (pk INT primary key, t timestamp, v varchar, f float[]);`,
				`INSERT INTO t1 VALUES 
                   (1, '2023-01-01 00:00:00', 'a', '{1.0, 2.0}'),
                   (2, '2023-01-02 00:00:00', 'b', '{3.0, 4.0}'),
                   (3, '2023-01-03 00:00:00', 'c', '{5.0, 6.0}');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT array_agg(pk) FROM t1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testaggregatefunctions-0016-select-array_agg-pk-from-t1"},
				},
				{
					Query: `SELECT array_agg(t) FROM t1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testaggregatefunctions-0017-select-array_agg-t-from-t1"},
				},
				{
					Query: `SELECT array_agg(v) FROM t1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testaggregatefunctions-0018-select-array_agg-v-from-t1"},
				},
				{
					Query: `SELECT array_agg(f) FROM t1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testaggregatefunctions-0019-select-array_agg-f-from-t1"},
				},
			},
		},
		{
			Name: "array_agg with order by",
			SetUpScript: []string{
				`CREATE TABLE test_data (
					id INT PRIMARY KEY, 
					name VARCHAR(50), 
					age INT, 
					score FLOAT, 
					created_at TIMESTAMP, 
					category CHAR(1),
					nullable_field VARCHAR(20)
				);`,
				`INSERT INTO test_data VALUES 
					(1, 'Alice', 25, 95.5, '2023-01-03 10:00:00', 'A', 'value1'),
					(2, 'Bob', 30, 87.2, '2023-01-01 09:30:00', 'B', NULL),
					(3, 'Charlie', 22, 92.8, '2023-01-02 11:15:00', 'A', 'value2'),
					(4, 'Diana', 28, 88.9, '2023-01-04 08:45:00', 'C', NULL),
					(5, 'Eve', 35, 94.1, '2023-01-05 14:20:00', 'B', 'value3'),
					(6, 'Frank', 26, 89.3, '2023-01-06 16:30:00', 'A', 'value4');`,
			},
			Assertions: []ScriptTestAssertion{
				// Basic ORDER BY ASC
				{
					Query: `SELECT array_agg(name ORDER BY age ASC) FROM test_data;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testaggregatefunctions-0020-select-array_agg-name-order-by"},
				},
				// Basic ORDER BY DESC
				{
					Query: `SELECT array_agg(name ORDER BY age DESC) FROM test_data;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testaggregatefunctions-0021-select-array_agg-name-order-by"},
				},
				// ORDER BY with integers
				{
					Query: `SELECT array_agg(id ORDER BY age) FROM test_data;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testaggregatefunctions-0022-select-array_agg-id-order-by"},
				},
				// ORDER BY with floats
				{
					Query: `SELECT array_agg(name ORDER BY score DESC) FROM test_data;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testaggregatefunctions-0023-select-array_agg-name-order-by"},
				},
				// ORDER BY with timestamps
				{
					Query: `SELECT array_agg(name ORDER BY created_at ASC) FROM test_data;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testaggregatefunctions-0024-select-array_agg-name-order-by"},
				},
				// ORDER BY with VARCHAR/CHAR
				{
					Query: `SELECT array_agg(age ORDER BY name) FROM test_data;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testaggregatefunctions-0025-select-array_agg-age-order-by"},
				},
				// Multiple columns in ORDER BY
				{
					Query: `SELECT array_agg(name ORDER BY category ASC, age DESC) FROM test_data;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testaggregatefunctions-0026-select-array_agg-name-order-by"},
				},
				// ORDER BY with mixed ASC/DESC
				{
					Query: `SELECT array_agg(id ORDER BY category ASC, score DESC) FROM test_data;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testaggregatefunctions-0027-select-array_agg-id-order-by"},
				},
				// ORDER BY with expression
				{
					Query: `SELECT array_agg(name ORDER BY age * 2) FROM test_data;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testaggregatefunctions-0028-select-array_agg-name-order-by"},
				},
				// ORDER BY with string concatenation
				{
					Query: `SELECT array_agg(age ORDER BY category || name) FROM test_data;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testaggregatefunctions-0029-select-array_agg-age-order-by"},
				},
				// ORDER BY with CASE expression
				{
					Query: `SELECT array_agg(name ORDER BY CASE WHEN age > 27 THEN 1 ELSE 0 END, age) FROM test_data;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testaggregatefunctions-0030-select-array_agg-name-order-by"},
				},
				// ORDER BY with NULL values (PostgreSQL default ASC NULLS LAST behavior)
				{
					Query: `SELECT array_agg(name ORDER BY nullable_field) FROM test_data;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testaggregatefunctions-0031-select-array_agg-name-order-by"},
				},
				// ORDER BY with GROUP BY
				{
					Query: `SELECT category, array_agg(name ORDER BY age) FROM test_data GROUP BY category ORDER BY category;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testaggregatefunctions-0032-select-category-array_agg-name-order"},
				},
				// ORDER BY with subquery correlation
				{
					Query: `SELECT category, array_agg(name ORDER BY (SELECT COUNT(*) FROM test_data t2 WHERE t2.category = test_data.category AND t2.age < test_data.age)) FROM test_data GROUP BY category ORDER BY category;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testaggregatefunctions-0033-select-category-array_agg-name-order"},
				},
				// ORDER BY with COALESCE for NULL handling
				{
					Query: `SELECT array_agg(name ORDER BY COALESCE(nullable_field, 'zzz')) FROM test_data;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testaggregatefunctions-0034-select-array_agg-name-order-by"},
				},
				// Complex ORDER BY with multiple expressions
				{
					Query: `SELECT array_agg(name ORDER BY LENGTH(name) DESC, name ASC) FROM test_data;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testaggregatefunctions-0035-select-array_agg-name-order-by"},
				},
				// ORDER BY with aggregated values in grouped context
				{
					Query: `SELECT category, array_agg(name ORDER BY score - (SELECT AVG(score) FROM test_data t2 WHERE t2.category = test_data.category)) FROM test_data GROUP BY category ORDER BY category;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testaggregatefunctions-0036-select-category-array_agg-name-order"},
				},
				// ORDER BY with date functions
				{
					Query: `SELECT array_agg(name ORDER BY EXTRACT(hour FROM created_at)) FROM test_data;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testaggregatefunctions-0037-select-array_agg-name-order-by"},
				},
				// Empty result set
				{
					Query: `SELECT array_agg(name ORDER BY age) FROM test_data WHERE age > 100;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testaggregatefunctions-0038-select-array_agg-name-order-by"},
				},
				// ORDER BY with boolean expression
				{
					Query: `SELECT array_agg(name ORDER BY age > 27, age) FROM test_data;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testaggregatefunctions-0039-select-array_agg-name-order-by"},
				},
			},
		},
		{
			Name: "array agg with case statement",
			SetUpScript: []string{
				"CREATE TABLE t1 (pk INT primary key, v1 INT, v2 INT);",
				"INSERT INTO t1 VALUES (1, 10, 20), (2, 30, 40), (3, 50, 60);",
				"CREATE TABLE t2 (pk INT primary key, v1 INT, v2 TEXT);",
				"INSERT INTO t2 VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 30, 'c');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT array_agg(CASE WHEN v1 > 20 THEN v1 ELSE NULL END) FROM t1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testaggregatefunctions-0040-select-array_agg-case-when-v1"},
				},
				{
					Query: `SELECT array_agg(CASE WHEN v1 >= 20 THEN v2 ELSE NULL END) FROM t2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testaggregatefunctions-0041-select-array_agg-case-when-v1"},
				},
				{
					Query: `SELECT array_agg(CASE WHEN v1 > 20 THEN v1::text ELSE v2 END) FROM t2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testaggregatefunctions-0042-select-array_agg-case-when-v1"},
				},
				{
					// Panic on type mixing, the logic for mixed types is hard-coded in GMS plan builder, needs
					// to be configurable. Postgres rejects this plan because of the type differences
					Skip:  true,
					Query: `SELECT array_agg(CASE WHEN v1 > 20 THEN v1 ELSE v2 END) FROM t2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testaggregatefunctions-0043-select-array_agg-case-when-v1", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "json aggregates",
			SetUpScript: []string{
				`CREATE TABLE json_items (pk INT primary key, k text, v int, j jsonb);`,
				`INSERT INTO json_items VALUES (1, 'b', 2, '{"x":2}'::jsonb), (2, 'a', 1, '{"x":1}'::jsonb), (3, 'b', NULL, 'null'::jsonb);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT json_agg(v ORDER BY v DESC) FROM json_items WHERE v IS NOT NULL;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testaggregatefunctions-0044-select-json_agg-v-order-by"},
				},
				{
					Query: `SELECT json_agg(DISTINCT v) FROM (VALUES (1), (1), (2)) AS vals(v);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testaggregatefunctions-0045-select-json_agg-distinct-v-from"},
				},
				{
					Query: `SELECT jsonb_agg(j ORDER BY pk) FROM json_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testaggregatefunctions-0046-select-jsonb_agg-j-order-by"},
				},
				{
					Query: `SELECT jsonb_agg(DISTINCT v) FROM (VALUES (1), (1), (2)) AS vals(v);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testaggregatefunctions-0047-select-jsonb_agg-distinct-v-from"},
				},
				{
					Query: `SELECT json_object_agg(k, v ORDER BY pk) FROM json_items WHERE v IS NOT NULL;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testaggregatefunctions-0048-select-json_object_agg-k-v-order"},
				},
				{
					Query: `SELECT jsonb_object_agg(k, v ORDER BY pk) FROM json_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testaggregatefunctions-0049-select-jsonb_object_agg-k-v-order"},
				},
				{
					Query: `SELECT json_agg(v) FROM json_items WHERE false;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testaggregatefunctions-0050-select-json_agg-v-from-json_items"},
				},
				{
					Query: `SELECT json_object_agg(NULL::text, v) FROM json_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testaggregatefunctions-0051-select-json_object_agg-null::text-v-from",

						// https://www.postgresql.org/docs/15/functions-math.html
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestFunctionsMath(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "cbrt",
			SetUpScript: []string{
				`CREATE TABLE test (pk INT primary key, v1 INT, v2 FLOAT4, v3 FLOAT8, v4 VARCHAR(255));`,
				`INSERT INTO test VALUES (1, -1, -2, -3, '-5'), (2, 7, 11, 13, '17'), (3, 19, -23, 29, '-31');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT cbrt(v1), cbrt(v2), cbrt(v3) FROM test ORDER BY pk;`,
					Skip:  true, PostgresOracle: // Our values are slightly different
					ScriptTestPostgresOracle{ID: "functions-test-testfunctionsmath-0001-select-cbrt-v1-cbrt-v2"},
				},
				{
					Query: `SELECT round(cbrt(v1)::numeric, 10), round(cbrt(v2)::numeric, 10), round(cbrt(v3)::numeric, 10) FROM test ORDER BY pk;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testfunctionsmath-0002-select-round-cbrt-v1-::numeric"},
				},
				{
					Query: `SELECT cbrt(v4) FROM test ORDER BY pk;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testfunctionsmath-0003-select-cbrt-v4-from-test", Compare: "sqlstate"},
				},
				{
					Query: `SELECT cbrt('64');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testfunctionsmath-0004-select-cbrt-64"},
				},
				{
					Query: `SELECT round(cbrt('64'));`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testfunctionsmath-0005-select-round-cbrt-64"},
				},
			},
		},
		{
			Name: "gcd",
			SetUpScript: []string{
				`CREATE TABLE test (pk INT primary key, v1 INT4, v2 INT8, v3 FLOAT8, v4 VARCHAR(255));`,
				`INSERT INTO test VALUES (1, -2, -4, -6, '-8'), (2, 10, 12, 14.14, '16.16'), (3, 18, -20, 22.22, '-24.24');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT gcd(v1, 10), gcd(v2, 20) FROM test ORDER BY pk;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testfunctionsmath-0006-select-gcd-v1-10-gcd"},
				},
				{
					Query: `SELECT gcd(v3, 10) FROM test ORDER BY pk;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testfunctionsmath-0007-select-gcd-v3-10-from", Compare: "sqlstate"},
				},
				{
					Query: `SELECT gcd(v4, 10) FROM test ORDER BY pk;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testfunctionsmath-0008-select-gcd-v4-10-from", Compare: "sqlstate"},
				},
				{
					Query: `SELECT gcd(36, '48');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testfunctionsmath-0009-select-gcd-36-48"},
				},
				{
					Query: `SELECT gcd('36', 48);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testfunctionsmath-0010-select-gcd-36-48"},
				},
				{
					Query: `SELECT gcd(1, 0), gcd(0, 1), gcd(0, 0);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testfunctionsmath-0011-select-gcd-1-0-gcd"},
				},
			},
		},
		{
			Name: "lcm",
			SetUpScript: []string{
				`CREATE TABLE test (pk INT primary key, v1 INT4, v2 INT8, v3 FLOAT8, v4 VARCHAR(255));`,
				`INSERT INTO test VALUES (1, -2, -4, -6, '-8'), (2, 10, 12, 14.14, '16.16'), (3, 18, -20, 22.22, '-24.24');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT lcm(v1, 10), lcm(v2, 20) FROM test ORDER BY pk;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testfunctionsmath-0012-select-lcm-v1-10-lcm"},
				},
				{
					Query: `SELECT lcm(v3, 10) FROM test ORDER BY pk;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testfunctionsmath-0013-select-lcm-v3-10-from", Compare: "sqlstate"},
				},
				{
					Query: `SELECT lcm(v4, 10) FROM test ORDER BY pk;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testfunctionsmath-0014-select-lcm-v4-10-from", Compare: "sqlstate"},
				},
				{
					Query: `SELECT lcm(36, '48');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testfunctionsmath-0015-select-lcm-36-48"},
				},
				{
					Query: `SELECT lcm('36', 48);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testfunctionsmath-0016-select-lcm-36-48"},
				},
				{
					Query: `SELECT lcm(1, 0), lcm(0, 1), lcm(0, 0);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testfunctionsmath-0017-select-lcm-1-0-lcm"},
				},
			},
		},
		{
			Name:        "to_hex",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT to_hex(10::int4), to_hex(255::int8);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testfunctionsmath-0018-select-to_hex-10::int4-to_hex-255::int8"},
				},
			},
		},
		{
			Name: "power",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT power(1::float8, 1::float8);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testfunctionsmath-0019-select-power-1::float8-1::float8"},
				},
				{
					Query: `SELECT power(2::float8, 0.5::float8);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testfunctionsmath-0020-select-power-2::float8-0.5::float8"},
				},
				{
					Query: `SELECT power(0::float8, 0::float8);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testfunctionsmath-0021-select-power-0::float8-0::float8"},
				},
				{
					Query: `SELECT power(4::float8, -1::float8);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testfunctionsmath-0022-select-power-4::float8-1::float8"},
				},
				{
					Query: `SELECT power(-2::float8, -1::float8);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testfunctionsmath-0023-select-power-2::float8-1::float8"},
				},
				{
					Query: `SELECT power(0::float8, -1::float8);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testfunctionsmath-0024-select-power-0::float8-1::float8", Compare: "sqlstate"},
				},
				{
					Query: `SELECT power(1::numeric, 1::numeric)::float8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testfunctionsmath-0025-select-power-1::numeric-1::numeric-::float8"},
				},
				{
					Query: `SELECT power(2::numeric, 0.5::numeric)::float8;`,
					Skip:  true, PostgresOracle: // TODO: we don't handle non-integer exponents properly
					ScriptTestPostgresOracle{ID: "functions-test-testfunctionsmath-0026-select-power-2::numeric-0.5::numeric-::float8"},
				},
				{
					Query: `SELECT power(0::numeric, 0::numeric)::float8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testfunctionsmath-0027-select-power-0::numeric-0::numeric-::float8"},
				},
				{
					Query: `SELECT power(4::numeric, -1::numeric)::float8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testfunctionsmath-0028-select-power-4::numeric-1::numeric-::float8"},
				},
				{
					Query: `SELECT power(-2::numeric, -1::numeric)::float8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testfunctionsmath-0029-select-power-2::numeric-1::numeric-::float8"},
				},
				{
					Query: `SELECT power(0::numeric, -1::numeric);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testfunctionsmath-0030-select-power-0::numeric-1::numeric", Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestFunctionsOID(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "to_regclass",
			SetUpScript: []string{
				`CREATE TABLE testing (pk INT primary key, v1 INT UNIQUE);`,
				`CREATE TABLE "Testing2" (pk INT primary key, v1 INT);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT to_regclass('testing');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testfunctionsoid-0001-select-to_regclass-testing"},
				},
				{
					Query: `SELECT to_regclass('Testing2');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testfunctionsoid-0002-select-to_regclass-testing2"},
				},
				{
					Query: `SELECT to_regclass('"Testing2"');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testfunctionsoid-0003-select-to_regclass-testing2"},
				},
				{
					Query: `SELECT to_regclass(('testing'::regclass)::text);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testfunctionsoid-0004-select-to_regclass-testing-::regclass-::text"},
				},
				{
					Query: `SELECT to_regclass((('testing'::regclass)::oid)::text);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testfunctionsoid-0005-select-to_regclass-testing-::regclass-::oid"},
				},
				{
					// When the relation is from a schema on the search path, it is not qualified with the schema name
					Query: `SELECT to_regclass(('public.testing'::regclass)::text);`,
					Expected: []sql.Row{
						{"testing"},
					},
				},
				{
					// Clear out the current search_path setting to test fully qualified relation names
					Query:    `SET search_path = '';`,
					Expected: []sql.Row{},
				},
				{
					Query: `SELECT to_regclass(('public.testing'::regclass)::text);`,
					Expected: []sql.Row{
						{"public.testing"},
					},
				},
			},
		},
		{
			Name: "to_regproc",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT to_regproc('acos');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testfunctionsoid-0009-select-to_regproc-acos"},
				},
				{
					Query: `SELECT to_regproc('acos"');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testfunctionsoid-0010-select-to_regproc-acos"},
				},
				{
					Query: `SELECT to_regproc(('acos'::regproc)::text);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testfunctionsoid-0011-select-to_regproc-acos-::regproc-::text"},
				},
				{
					Query: `SELECT to_regproc((('acos'::regproc)::oid)::text);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testfunctionsoid-0012-select-to_regproc-acos-::regproc-::oid"},
				},
			},
		},
		{
			Name: "to_regtype",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT to_regtype('integer');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testfunctionsoid-0013-select-to_regtype-integer"},
				},
				{
					Query: `SELECT to_regtype('integer[]');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testfunctionsoid-0014-select-to_regtype-integer[]"},
				},
				{
					Query: `SELECT to_regtype('int4');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testfunctionsoid-0015-select-to_regtype-int4"},
				},
				{
					Query: `SELECT to_regtype('varchar');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testfunctionsoid-0016-select-to_regtype-varchar"},
				},
				{
					Query: `SELECT to_regtype('pg_catalog.varchar');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testfunctionsoid-0017-select-to_regtype-pg_catalog.varchar"},
				},
				{
					Query: `SELECT to_regtype('varchar(10)');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testfunctionsoid-0018-select-to_regtype-varchar-10"},
				},
				{
					Query: `SELECT to_regtype('char');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testfunctionsoid-0019-select-to_regtype-char"},
				},
				{
					Query: `SELECT to_regtype('pg_catalog.char');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testfunctionsoid-0020-select-to_regtype-pg_catalog.char"},
				},
				{
					Query: `SELECT to_regtype('char(10)');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testfunctionsoid-0021-select-to_regtype-char-10"},
				},
				{
					Query: `SELECT to_regtype('"char"');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testfunctionsoid-0022-select-to_regtype-char"},
				},
				{
					Query: `SELECT to_regtype('pg_catalog."char"');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testfunctionsoid-0023-select-to_regtype-pg_catalog.-char"},
				},
				{
					Query: `SELECT to_regtype('otherschema.char');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testfunctionsoid-0024-select-to_regtype-otherschema.char"},
				},
				{
					Query: `SELECT to_regtype('timestamp');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testfunctionsoid-0025-select-to_regtype-timestamp"},
				},
				{
					Query: `SELECT to_regtype('timestamp without time zone');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testfunctionsoid-0026-select-to_regtype-timestamp-without-time"},
				},
				{
					Query: `SELECT to_regtype('integer"');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testfunctionsoid-0027-select-to_regtype-integer", Compare: "sqlstate"},
				},
				{
					Query: `SELECT to_regtype(('integer'::regtype)::text);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testfunctionsoid-0028-select-to_regtype-integer-::regtype-::text"},
				},
				{
					Query: `SELECT to_regtype(('int'::regtype)::text);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testfunctionsoid-0029-select-to_regtype-int-::regtype-::text"},
				},
				{
					Query: `SELECT to_regtype((('integer'::regtype)::oid)::text);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testfunctionsoid-0030-select-to_regtype-integer-::regtype-::oid"},
				},
			},
		},
	})
}

func TestSystemInformationFunctions(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:     "current_database",
			Database: "test",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT current_database();`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0001-select-current_database"},
				},
				{
					Query: `SELECT current_database;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0002-select-current_database",

						// TODO: Implement table function for current_database
						Compare: "sqlstate"},
				},

				{
					Query: `SELECT * FROM current_database();`,
					Skip:  true, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0003-select-*-from-current_database"},
				},
				{
					Query: `SELECT * FROM current_database;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0004-select-*-from-current_database", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "backend pid and text hash",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT pg_backend_pid() > 0, pg_backend_pid() = pg_backend_pid();`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0005-select-pg_backend_pid->-0-pg_backend_pid"},
				},
				{
					Query: `SELECT hashtext(''), hashtext('electric_slot_default'), hashtext('abc'), hashtext('ümlaut'), hashtext(NULL::text);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0006-select-hashtext-hashtext-electric_slot_default-hashtext"},
				},
				{
					Query: `SELECT hashtextextended('', 0), hashtextextended('electric_slot_default', 0), hashtextextended('abc', 0), hashtextextended('ümlaut', 0), hashtextextended(NULL::text, 0), hashtextextended(NULL::text, 1);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0007-select-hashtextextended-0-hashtextextended-electric_slot_default"},
				},
				{
					// When seed=0, the low 32 bits of hashtextextended must equal hashtext (as PG documents).
					Query: `SELECT v, (hashtext(v)::int8 & 4294967295) = (hashtextextended(v, 0) & 4294967295) AS matches
                            FROM (VALUES (''::text), ('PostgreSQL'), ('eIpUEtqmY89'), ('AXKEJBTK'), ('muop28x03'), ('yi3nm0d73'), ('ümlaut'), ('abc')) x(v);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0008-select-v-hashtext-v-::int8"},
				},
				{
					// With non-zero seed, hashtextextended must produce a different result than the unseeded form.
					Query: `SELECT hashtextextended('PostgreSQL', 1), hashtextextended('PostgreSQL', 1) = hashtextextended('PostgreSQL', 0) AS same;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0009-select-hashtextextended-postgresql-1-hashtextextended"},
				},
				{
					// Large positive and negative seeds must produce values that match real PostgreSQL.
					Query: `SELECT hashtextextended('abc', 1234567890123), hashtextextended('abc', -1);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0010-select-hashtextextended-abc-1234567890123-hashtextextended"},
				},
				{
					// Length boundary cases for the 12-byte chunk loop in the hash mixer.
					Query: `SELECT hashtextextended('a', 0), hashtextextended('ab', 0), hashtextextended('abcd', 0), hashtextextended('abcdefghijkl', 0), hashtextextended('abcdefghijklm', 0);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0011-select-hashtextextended-a-0-hashtextextended"},
				},
				{
					// Long input plus extreme bigint seeds.
					Query: `SELECT hashtextextended('the quick brown fox jumps over the lazy dog', 0),
                                   hashtextextended('the quick brown fox jumps over the lazy dog', 9223372036854775807),
                                   hashtextextended('the quick brown fox jumps over the lazy dog', -9223372036854775808);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0012-select-hashtextextended-the-quick-brown"},
				},
				{
					// Result must be deterministic — same input/seed yields same output.
					Query: `SELECT hashtextextended('PostgreSQL', 42) = hashtextextended('PostgreSQL', 42);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0013-select-hashtextextended-postgresql-42-="},
				},
			},
		},
		{
			Name: "type introspection",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT pg_typeof(1), pg_typeof('x'::text), pg_typeof(NULL::integer);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0014-select-pg_typeof-1-pg_typeof-x"},
				},
				{
					Query: `SELECT pg_typeof('x'::char(3)) = 'character'::regtype, pg_typeof('x'::text) = 'character'::regtype;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0015-select-pg_typeof-x-::char-3"},
				},
			},
		},
		{
			Name: "pg_column_size",
			SetUpScript: []string{
				`CREATE TABLE colsize_items (id int primary key, label text, active bool);`,
				`INSERT INTO colsize_items VALUES (1, 'abc', true), (2, NULL, false);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT pg_column_size(NULL::text), pg_column_size('abc'::text), pg_column_size(1::int2), pg_column_size(1::int4), pg_column_size(1::int8), pg_column_size(true);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0016-select-pg_column_size-null::text-pg_column_size-abc"},
				},
				{
					Query: `SELECT COALESCE(SUM(COALESCE(pg_column_size(id), 0)) + SUM(COALESCE(pg_column_size(label), 0)) + SUM(COALESCE(pg_column_size(active), 0)), 0)::bigint FROM colsize_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0017-select-coalesce-sum-coalesce-pg_column_size"},
				},
			},
		},
		{
			Name: "current user sql value functions",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT current_user, current_role, session_user;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0018-select-current_user-current_role-session_user"},
				},
				{
					Query: `SELECT pg_get_userbyid(10) = current_role;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0019-select-pg_get_userbyid-10-=-current_role"},
				},
			},
		},
		{
			Name: "current snapshot",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT pg_current_snapshot();`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0020-select-pg_current_snapshot"},
				},
				{
					Query: `SELECT pg_snapshot_xmin('1:4:2,3'::pg_snapshot)::text, pg_snapshot_xmax('1:4:2,3'::pg_snapshot)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0021-select-pg_snapshot_xmin-1:4:2-3-::pg_snapshot"},
				},
				{
					Query: `SELECT pg_snapshot_xip('1:4:2,3'::pg_snapshot)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0022-select-pg_snapshot_xip-1:4:2-3-::pg_snapshot"},
				},
				{
					Query: `SELECT pg_snapshot_send(pg_current_snapshot())::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0023-select-pg_snapshot_send-pg_current_snapshot-::text"},
				},
				{
					Query: `SELECT pg_visible_in_snapshot('1'::xid8, '1:4:2,3'::pg_snapshot), pg_visible_in_snapshot('2'::xid8, '1:4:2,3'::pg_snapshot), pg_visible_in_snapshot('4'::xid8, '1:4:2,3'::pg_snapshot);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0024-select-pg_visible_in_snapshot-1-::xid8-1:4:2"},
				},
			},
		},
		{
			Name:     "current_catalog",
			Database: "test",
			Assertions: []ScriptTestAssertion{
				{
					Skip:  true, // TODO: current_catalog currently returns current_database column name
					Query: `SELECT current_catalog;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0025-select-current_catalog"},
				},
				{
					Query: `SELECT current_catalog;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0026-select-current_catalog"},
				},
				{
					Query: `SELECT current_catalog();`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0027-select-current_catalog",

						// // TODO: Implement table function for current_catalog
						Compare: "sqlstate"},
				},

				{
					Query: `SELECT * FROM current_catalog;`,
					Skip:  true, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0028-select-*-from-current_catalog"},
				},
				{
					Query: `SELECT * FROM current_catalog();`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0029-select-*-from-current_catalog", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "current_schema",
			Assertions: []ScriptTestAssertion{
				{
					Skip:  true, // TODO: current_schema currently returns column name in quotes
					Query: `SELECT current_schema();`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0030-select-current_schema"},
				},
				{
					Query: `SELECT current_schema();`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0031-select-current_schema", ColumnModes: []string{"schema"}},
				},
				{
					Query: "CREATE SCHEMA test_schema;", PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0032-create-schema-test_schema"},
				},
				{
					Query: `SET SEARCH_PATH TO test_schema;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0033-set-search_path-to-test_schema"},
				},
				{
					Query: `SELECT current_schema();`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0034-select-current_schema"},
				},
				{
					Query:    `SET SEARCH_PATH TO public, test_schema;`,
					Expected: []sql.Row{},
				},
				{
					Query: `SELECT current_schema();`,
					Expected: []sql.Row{
						{"public"},
					},
				},
				{
					Query: `SELECT current_schema;`,
					Expected: []sql.Row{
						{"public"},
					},
				},
				{
					Query:    `SET SEARCH_PATH TO test_schema, public;`,
					Expected: []sql.Row{},
				},
				{
					Query: `SELECT current_schema();`,
					Expected: []sql.Row{
						{"test_schema"},
					},
				},
				// TODO: Implement table function for current_schema
				{
					Query: `SELECT * FROM current_schema();`,
					Skip:  true,
					Expected: []sql.Row{
						{"public"},
					},
				},
				{
					Query: `SELECT * FROM current_schema;`,
					Skip:  true,
					Expected: []sql.Row{
						{"public"},
					},
				},
			},
		},
		{
			Name: "current_schemas",
			Assertions: []ScriptTestAssertion{
				{ // TODO: Not sure why Postgres does not display "$user", which is postgres here
					Query: `SELECT current_schemas(true);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0042-select-current_schemas-true"},
				},
				{ // TODO: Not sure why Postgres does not display "$user" here
					Query: `SELECT current_schemas(false);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0043-select-current_schemas-false", ColumnModes: []string{"schema"}},
				},
				{
					Query: "CREATE SCHEMA test_schema;", PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0044-create-schema-test_schema"},
				},
				{
					Query: `SET SEARCH_PATH TO test_schema;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0045-set-search_path-to-test_schema"},
				},
				{
					Query: `SELECT current_schemas(true);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0046-select-current_schemas-true"},
				},
				{
					Query: `SELECT current_schemas(false);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0047-select-current_schemas-false"},
				},
				{
					Query:    `SET SEARCH_PATH TO public, test_schema;`,
					Expected: []sql.Row{},
				},
				{
					Query: `SELECT current_schemas(true);`,
					Expected: []sql.Row{
						{"{pg_catalog,public,test_schema}"},
					},
				},
				{
					Query: `SELECT current_schemas(false);`,
					Expected: []sql.Row{
						{"{public,test_schema}"},
					},
				},
			},
		},
		{
			Name: "version",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT version();`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0051-select-version"},
				},
			},
		},
		{
			Name: "col_description",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT col_description(100, 1);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0052-select-col_description-100-1"},
				},
				{
					Query: `SELECT col_description('not_a_table'::regclass, 1);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0053-select-col_description-not_a_table-::regclass-1", Compare: "sqlstate"},
				},
				{
					Query: `CREATE TABLE test_table (id INT);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0054-create-table-test_table-id-int"},
				},
				{
					Skip:  true, // TODO: Implement column comments
					Query: `COMMENT ON COLUMN test_table.id IS 'This is col id';`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0055-comment-on-column-test_table.id-is"},
				},
				{
					Skip:  true, // TODO: Implement column object comments
					Query: `SELECT col_description('test_table'::regclass, 1);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0056-select-col_description-test_table-::regclass-1"},
				},
			},
		},
		{
			Name: "obj_description",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT obj_description(1003);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0057-select-obj_description-1003"},
				},
				{
					Query: `SELECT obj_description(100, 'pg_class');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0058-select-obj_description-100-pg_class"},
				},
				{
					Query: `SELECT obj_description('does-not-exist'::regproc, 'pg_class');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0059-select-obj_description-does-not-exist-::regproc-pg_class",

						// TODO: Implement database object comments
						Compare: "sqlstate"},
				},
				{
					Skip:  true,
					Query: `SELECT obj_description('sinh'::regproc, 'pg_proc');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0060-select-obj_description-sinh-::regproc-pg_proc"},
				},
			},
		},
		{
			Name: "shobj_description",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT shobj_description(100, 'pg_class');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0061-select-shobj_description-100-pg_class"},
				},
				{
					Query: `SELECT shobj_description('does-not-exist'::regproc, 'pg_class');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0062-select-shobj_description-does-not-exist-::regproc-pg_class",

						// TODO: Implement tablespaces
						Compare: "sqlstate"},
				},
				{
					Skip:  true,
					Query: `CREATE TABLESPACE tblspc_2 LOCATION '/';`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0063-create-tablespace-tblspc_2-location-/",

						// TODO: Implement shared database object comments
						Compare: "sqlstate"},
				},
				{
					Skip:  true,
					Query: `COMMENT ON TABLESPACE tblspc_2 IS 'Store a few of the things';`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0064-comment-on-tablespace-tblspc_2-is",

						// TODO: Implement shared database object comments
						Compare: "sqlstate"},
				},
				{
					Skip: true,
					Query: `SELECT shobj_description(
                 (SELECT oid FROM pg_tablespace WHERE spcname = 'tblspc_2'),
                 'pg_tablespace');`,
					ExpectedColNames: []string{"shobj_description"},
					Expected: []sql.Row{
						{"Store a few of the things"},
					},
				},
			},
		},
		{
			Name: "format_type",
			Assertions: []ScriptTestAssertion{
				// Without typemod
				{
					Query: `SELECT format_type('integer'::regtype, null);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0066-select-format_type-integer-::regtype-null"},
				},
				{
					Query: `SELECT format_type('character varying'::regtype, null);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0067-select-format_type-character-varying-::regtype"},
				},
				{
					Query: `SELECT format_type('varchar'::regtype, null);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0068-select-format_type-varchar-::regtype-null"},
				},
				{
					Query: `SELECT format_type('date'::regtype, null);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0069-select-format_type-date-::regtype-null"},
				},
				{
					Query: `SELECT format_type('timestamptz'::regtype, null);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0070-select-format_type-timestamptz-::regtype-null"},
				},
				{
					Query: `SELECT format_type('bool'::regtype, null);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0071-select-format_type-bool-::regtype-null"},
				},
				{
					Query: `SELECT format_type(1007, null);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0072-select-format_type-1007-null"},
				},
				{
					Query: `SELECT format_type('"char"'::regtype, null);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0073-select-format_type-char-::regtype-null"},
				},
				{
					Query: `SELECT format_type('"char"[]'::regtype, null);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0074-select-format_type-char-[]-::regtype"},
				},
				{
					Query: `SELECT format_type(1002, null);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0075-select-format_type-1002-null"},
				},
				{
					Query: `SELECT format_type('real[]'::regtype, null);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0076-select-format_type-real[]-::regtype-null"},
				},
				// With typemod
				{
					Query: `SELECT format_type('character varying'::regtype, 100);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0077-select-format_type-character-varying-::regtype"},
				},
				{
					Query: `SELECT format_type('text'::regtype, 0);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0078-select-format_type-text-::regtype-0"},
				},
				{
					Query: `SELECT format_type('text'::regtype, 4);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0079-select-format_type-text-::regtype-4"},
				},
				{
					Query: `SELECT format_type('text'::regtype, -1);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0080-select-format_type-text-::regtype-1"},
				},
				{
					Query: `SELECT format_type('name'::regtype, 0);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0081-select-format_type-name-::regtype-0"},
				},
				{
					Query: `SELECT format_type('bpchar'::regtype, -1);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0082-select-format_type-bpchar-::regtype-1"},
				},
				{
					Query: `SELECT format_type('bpchar'::regtype, 10);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0083-select-format_type-bpchar-::regtype-10"},
				},
				{
					Query: `SELECT format_type('bpchar'::regtype, 10);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0084-select-format_type-bpchar-::regtype-10"},
				},
				{
					Query: `SELECT format_type('character'::regtype, 4);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0085-select-format_type-character-::regtype-4"},
				},
				{
					Query: `SELECT format_type('varchar'::regtype, 0);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0086-select-format_type-varchar-::regtype-0"},
				},
				{
					Query: `SELECT format_type('"char"'::regtype, 0);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0087-select-format_type-char-::regtype-0"},
				},
				{
					Query: `SELECT format_type('numeric'::regtype, 12);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0088-select-format_type-numeric-::regtype-12"},
				},
				// OID does not exist
				{
					Query: `SELECT format_type(874938247, 20);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0089-select-format_type-874938247-20"},
				},
				{
					Query: `SELECT format_type(874938247, null);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0090-select-format_type-874938247-null"},
				},
			},
		},
		{
			Name: "pg_get_constraintdef",
			SetUpScript: []string{
				`CREATE TABLE testing (pk INT primary key, v1 INT UNIQUE);`,
				`CREATE TABLE testing2 (pk INT primary key, pktesting INT REFERENCES testing(pk), v1 TEXT);`,
				`CREATE TABLE testing3 (pk1 INT, pk2 INT, PRIMARY KEY (pk1, pk2));`,
				// TODO: Uncomment when check constraints supported
				// `ALTER TABLE testing2 ADD CONSTRAINT v1_check CHECK (v1 != '');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT pg_get_constraintdef(845743985);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0091-select-pg_get_constraintdef-845743985"},
				},
				{
					Query: `SELECT pg_get_constraintdef(oid) FROM pg_catalog.pg_constraint WHERE conrelid='testing'::regclass;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0092-select-pg_get_constraintdef-oid-from-pg_catalog.pg_constraint"},
				},
				{
					Query: `SELECT pg_get_constraintdef(oid) FROM pg_catalog.pg_constraint WHERE conrelid='testing2'::regclass;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0093-select-pg_get_constraintdef-oid-from-pg_catalog.pg_constraint"},
				},
				{
					Query: `SELECT pg_get_constraintdef(oid) FROM pg_catalog.pg_constraint WHERE conrelid='testing3'::regclass;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0094-select-pg_get_constraintdef-oid-from-pg_catalog.pg_constraint"},
				},
				{
					Query: `SELECT pg_get_constraintdef(oid, true) FROM pg_catalog.pg_constraint WHERE conrelid='testing3'::regclass;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0095-select-pg_get_constraintdef-oid-true-from"},
				},
				{
					Query: `SELECT pg_get_constraintdef(oid, false) FROM pg_catalog.pg_constraint WHERE conrelid='testing3'::regclass;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0096-select-pg_get_constraintdef-oid-false-from"},
				},
			},
		},
		{
			Name: "pg_get_ruledef",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT pg_get_ruledef(845743985);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0097-select-pg_get_ruledef-845743985"},
				},
				{
					Query: `SELECT pg_get_ruledef(845743985, true);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0098-select-pg_get_ruledef-845743985-true"},
				},
				{
					Query: `SELECT pg_get_ruledef(845743985, false);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0099-select-pg_get_ruledef-845743985-false"},
				},
			},
		},
		{
			Name: "pg_get_expr",
			SetUpScript: []string{
				`CREATE TABLE testing (id INT primary key);`,
				`CREATE TABLE temperature (celsius SMALLINT NOT NULL, fahrenheit SMALLINT NOT NULL GENERATED ALWAYS AS ((celsius * 9/5) + 32) STORED);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Skip:  true, // TODO: pg_attrdef.adbin not implemented
					Query: `SELECT pg_get_expr(adbin, adrelid) FROM pg_catalog.pg_attrdef WHERE adrelid = 'temperature'::regclass;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0100-select-pg_get_expr-adbin-adrelid-from"},
				},
				{
					Query: `SELECT indexrelid, pg_get_expr(indpred, indrelid) FROM pg_catalog.pg_index WHERE indrelid='testing'::regclass;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0101-select-indexrelid-pg_get_expr-indpred-indrelid"},
				},
				{
					Query: `SELECT indexrelid, pg_get_expr(indpred, indrelid, true) FROM pg_catalog.pg_index WHERE indrelid='testing'::regclass;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0102-select-indexrelid-pg_get_expr-indpred-indrelid"},
				},
				{
					Query: `SELECT indexrelid, pg_get_expr(indpred, indrelid, NULL) FROM pg_catalog.pg_index WHERE indrelid='testing'::regclass;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0103-select-indexrelid-pg_get_expr-indpred-indrelid"},
				},
			},
		},
		{
			Name: "pg_get_serial_sequence",
			SetUpScript: []string{
				`create table t0 (id INTEGER NOT NULL PRIMARY KEY);`,
				`create table t1 (id SERIAL PRIMARY KEY);`,
				`create sequence t2_id_seq START 1 INCREMENT 3;`,
				`create table t2 (id INTEGER NOT NULL DEFAULT nextval('t2_id_seq'));`,
				// TODO: ALTER SEQUENCE OWNED BY is not supported yet. When the sequence is created
				//       explicitly, separate from the column, the owner must be updated before
				//       pg_get_serial_sequence() will identify it.
				// `ALTER SEQUENCE t2_id_seq OWNED BY t2.id;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT pg_get_serial_sequence('doesnotexist.t1', 'id');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0104-select-pg_get_serial_sequence-doesnotexist.t1-id", Compare: "sqlstate"},
				},
				{
					Query: `SELECT pg_get_serial_sequence('doesnotexist', 'id');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0105-select-pg_get_serial_sequence-doesnotexist-id", Compare: "sqlstate"},
				},
				{
					Query: `SELECT pg_get_serial_sequence('t0', 'doesnotexist');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0106-select-pg_get_serial_sequence-t0-doesnotexist",

						// No sequence for column returns null
						Compare: "sqlstate"},
				},
				{

					Query: `SELECT pg_get_serial_sequence('t0', 'id');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0107-select-pg_get_serial_sequence-t0-id"},
				},
				{
					Query:            `SELECT pg_get_serial_sequence('public.t1', 'id');`,
					ExpectedColNames: []string{"pg_get_serial_sequence"},
					Expected:         []sql.Row{{"public.t1_id_seq"}},
				},
				{
					Query:            `SELECT pg_get_serial_sequence('"public"."t1"', 'id');`,
					ExpectedColNames: []string{"pg_get_serial_sequence"},
					Expected:         []sql.Row{{"public.t1_id_seq"}},
				},
				{
					// Test with no schema specified
					Query:            `SELECT pg_get_serial_sequence('t1', 'id');`,
					ExpectedColNames: []string{"pg_get_serial_sequence"},
					Expected:         []sql.Row{{"public.t1_id_seq"}},
				},
				{
					// TODO: This test shouldn't pass until we're able to use
					//       ALTER SEQUENCE OWNED BY to set the owning column.
					Skip:             true,
					Query:            `SELECT pg_get_serial_sequence('t2', 'id');`,
					ExpectedColNames: []string{"pg_get_serial_sequence"},
					Expected:         []sql.Row{{"public.t2_id_seq"}},
				},
			},
		},
		{
			Name: "current_setting function",
			Assertions: []ScriptTestAssertion{
				{
					Query: "SET timezone TO '+00:00';", PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0112-set-timezone-to-+00:00"},
				},
				{
					Query: "SELECT current_setting('timezone')", PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0113-select-current_setting-timezone"},
				},
				{
					Query: "SELECT current_setting('wrong_input')", PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0114-select-current_setting-wrong_input", Compare: "sqlstate"},
				},
				{
					Query: "SELECT current_setting('wrong_input', true)", PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0115-select-current_setting-wrong_input-true"},
				},
				{
					Query: "SELECT current_setting('wrong_input', false)", PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsysteminformationfunctions-0116-select-current_setting-wrong_input-false", Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestJsonFunctions(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "json_build_array",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT json_build_array(1, 2, 3);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0001-select-json_build_array-1-2-3"},
				},
				{
					Query: `SELECT json_build_array(1, '2', 3);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0002-select-json_build_array-1-2-3"},
				},
				{
					Query: `SELECT json_build_array();`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0003-select-json_build_array"},
				},
				{
					Query: `SELECT json_build_array('a', '{"x": 3}'::json, true);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0004-select-json_build_array-a-{-x"},
				},
				{
					Query: `SELECT json_build_array('a' || chr(10) || 'b', 'c' || chr(92) || 'd', 'e' || chr(9) || 'f');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0005-select-json_build_array-a-||-chr"},
				},
			},
		},
		{
			Name: "json_build_object",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT json_build_object('a', 2, 'b', 4);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0006-select-json_build_object-a-2-b"},
				},
				{
					Query: `SELECT json_build_object('a', 2, 'b');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0007-select-json_build_object-a-2-b", Compare: "sqlstate"},
				},
				{
					Query: `SELECT json_build_object(1, 2, 'b', 3);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0008-select-json_build_object-1-2-b"},
				},
				{
					Query: `SELECT json_build_object('payload', '{"x": 3}'::json);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0009-select-json_build_object-payload-{-x"},
				},
				{
					Query: `SELECT json_build_object(k, v) FROM (SELECT 'a' || chr(10) || 'b' AS k, 'c' || chr(92) || 'd' AS v) q;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0010-select-json_build_object-k-v-from"},
				},
			},
		},
		{
			Name: "jsonb_build_array",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT jsonb_build_array(1, 2, 3);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0011-select-jsonb_build_array-1-2-3"},
				},
				{
					Query: `SELECT jsonb_build_array(1, '2', 3);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0012-select-jsonb_build_array-1-2-3"},
				},
				{
					Query: `SELECT jsonb_build_array();`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0013-select-jsonb_build_array"},
				},
				{
					Query: `SELECT jsonb_build_array('a', '{"x": 3}'::jsonb, true);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0014-select-jsonb_build_array-a-{-x"},
				},
			},
		},
		{
			Name: "jsonb_build_object",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT jsonb_build_object('a', 2, 'b', 4);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0015-select-jsonb_build_object-a-2-b"},
				},
				{
					Query: `SELECT jsonb_build_object('a', 2, 'b');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0016-select-jsonb_build_object-a-2-b", Compare: "sqlstate"},
				},
				{
					Query: `SELECT jsonb_build_object(1, 2, 'b', 3);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0017-select-jsonb_build_object-1-2-b"},
				},
				{
					Query: `SELECT jsonb_build_object('payload', '{"x": 3}'::jsonb);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0018-select-jsonb_build_object-payload-{-x"},
				},
			},
		},
		{
			Name: "json helper functions",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT json_typeof('{"a":1}'::json), json_typeof('[1]'::json), json_typeof('null'::json), json_array_length('[1,2,3]'::json);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0019-select-json_typeof-{-a-:1}"},
				},
				{
					Query: `SELECT json_object_keys('{"a":1,"b":2}'::json);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0020-select-json_object_keys-{-a-:1"},
				},
				{
					Query: `SELECT array_to_string(ARRAY(SELECT json_object_keys('{"a":1,"b":2}'::json)), ',');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0021-select-array_to_string-array-select-json_object_keys"},
				},
				{
					Query: `SELECT * FROM json_each('{"a":1,"b":"x"}'::json) ORDER BY 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0022-select-*-from-json_each-{"},
				},
				{
					Query: `SELECT json_each('{"a":1}'::json);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0023-select-json_each-{-a-:1}"},
				},
				{
					Query: `SELECT * FROM json_each_text('{"a":1,"b":"x","c":null}'::json) ORDER BY 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0024-select-*-from-json_each_text-{"},
				},
				{
					Query: `SELECT json_each_text('{"a":"x\ny"}'::json);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0025-select-json_each_text-{-a-:"},
				},
				{
					Query: `SELECT json_array_elements_text('["a\nb","c\\d"]'::json);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0026-select-json_array_elements_text-[-a\\nb-c\\\\d"},
				},
				{
					Query: `SELECT json_extract_path('{"a":{"b":"x"},"arr":[10,null]}'::json, 'a', 'b');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0027-select-json_extract_path-{-a-:{"},
				},
				{
					Query: `SELECT json_extract_path_text('{"a":{"b":"x"},"arr":[10,null]}'::json, 'a', 'b');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0028-select-json_extract_path_text-{-a-:{"},
				},
				{
					Query: `SELECT json_extract_path_text('{"a":{"b":"x\ny"}}'::json, 'a', 'b');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0029-select-json_extract_path_text-{-a-:{"},
				},
				{
					Query: `SELECT json_extract_path('{"a":{"b":"x"},"arr":[10,null]}'::json, 'arr', '1') IS NULL, json_extract_path_text('{"a":{"b":"x"},"arr":[10,null]}'::json, 'arr', '1') IS NULL;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0030-select-json_extract_path-{-a-:{"},
				},
				{
					Query: `SELECT json_extract_path('{"a":{"b":"x"},"arr":[10,null]}'::json, 'missing') IS NULL, json_extract_path_text('{"a":{"b":"x"},"arr":[10,null]}'::json, 'missing') IS NULL;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0031-select-json_extract_path-{-a-:{"},
				},
				{
					Query: `SELECT '{"a":{"b":"x"},"arr":[10,null]}'::json #> ARRAY['a','b'], '{"a":{"b":"x"},"arr":[10,null]}'::json #>> ARRAY['a','b'];`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0032-select-{-a-:{-b"},
				},
				{
					Query: `SELECT json_strip_nulls('{"a":1,"b":null,"c":[2,null,{"d":null,"e":3}],"f":{"g":null,"h":4}}');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0033-select-json_strip_nulls-{-a-:1"},
				},
				{
					Query: `SELECT json_strip_nulls('[1,null,{"a":null,"b":2}]');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0034-select-json_strip_nulls-[1-null-{"},
				},
				{
					Query: `SELECT json_strip_nulls('{"a":{"b":null,"c":null},"d":{}}');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0035-select-json_strip_nulls-{-a-:{"},
				},
				{
					Query: `SELECT json_object('{a,1,b,"two words",c,NULL}'::text[]);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0036-select-json_object-{a-1-b"},
				},
				{
					Query: `SELECT json_object('{a,b,c}'::text[], '{1,"two words",NULL}'::text[]);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0037-select-json_object-{a-b-c}"},
				},
				{
					Query: `SELECT json_object('{a,b,c}'::text[]);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0038-select-json_object-{a-b-c}", Compare: "sqlstate"},
				},
				{
					Query: `SELECT json_object(ARRAY['a', NULL]::text[], ARRAY['1', '2']::text[]);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0039-select-json_object-array[-a-null]::text[]", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM json_to_record('{"a":1,"b":"foo","j":{"x":2}}'::json) AS r(a int, b text, j json);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0040-select-*-from-json_to_record-{"},
				},
				{
					Query: `SELECT row_to_json(row(1,'foo'));`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0041-select-row_to_json-row-1-foo"},
				},
				{
					Query: `SELECT row_to_json(r) FROM (SELECT 1 AS id, 'foo' AS label, NULL::text AS note) AS r;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0042-select-row_to_json-r-from-select"},
				},
				{
					Query: `SELECT row_to_json(row(1,NULL), false);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0043-select-row_to_json-row-1-null"},
				},
				{
					Query: `SELECT to_json('plain'::text)::text, to_json(42)::text, to_json(true)::text, to_json(NULL::int)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0044-select-to_json-plain-::text-::text"},
				},
				{
					Query: `SELECT to_json('"public"."electric_smoke_items"/"1"'::text)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0045-select-to_json-public-.-electric_smoke_items"},
				},
			},
		},
		{
			Name: "IS JSON predicate",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT '123' IS JSON, '"abc"' IS JSON SCALAR, '{"a":"b"}' IS JSON OBJECT, '[1,2]' IS JSON ARRAY, 'abc' IS JSON;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0046-select-123-is-json-abc"},
				},
				{
					Query: `SELECT '123' IS JSON VALUE, '"abc"' IS JSON VALUE, '{"a":1}' IS JSON VALUE, '[1]' IS JSON VALUE;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0047-select-123-is-json-value"},
				},
				{
					Query: `SELECT '{}' IS NOT JSON ARRAY, '[]' IS NOT JSON OBJECT, 'abc' IS NOT JSON, NULL::text IS JSON, NULL::text IS NOT JSON;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0048-select-{}-is-not-json"},
				},
				{
					Query: `SELECT '{"a":1,"a":2}' IS JSON WITH UNIQUE KEYS, '{"a":1,"a":2}' IS JSON WITHOUT UNIQUE KEYS;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0049-select-{-a-:1-a"},
				},
				{
					Query: `SELECT '[{"a":"1"},{"b":"2","b":"3"}]' IS JSON ARRAY WITH UNIQUE KEYS, '[{"a":"1"},{"b":"2","b":"3"}]' IS JSON ARRAY WITHOUT UNIQUE KEYS;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0050-select-[{-a-:-1"},
				},
				{
					Query: `SELECT '{"a":1,"a":2}'::json IS JSON WITH UNIQUE KEYS, '{"a":1,"a":2}'::jsonb IS JSON WITH UNIQUE KEYS;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0051-select-{-a-:1-a"},
				},
				{
					Query: `SELECT '{}{}' IS JSON, '{"a":1' IS JSON;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0052-select-{}{}-is-json-{"},
				},
				{
					Query: `SELECT 123 IS JSON;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0053-select-123-is-json", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "jsonb helper functions",
			SetUpScript: []string{
				`CREATE TABLE jsonb_record_items (id INT PRIMARY KEY, label TEXT);`,
				`INSERT INTO jsonb_record_items VALUES (1, 'one');`,
				`CREATE TYPE jsonb_populate_subrow AS (d INT, e TEXT);`,
				`CREATE TYPE jsonb_populate_row AS (a INT, b TEXT[], c jsonb_populate_subrow, j JSONB);`,
				`CREATE TYPE jsonb_populate_base AS (a INT, b TEXT, c BOOL);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT jsonb_typeof('{"a":1}'::jsonb), jsonb_typeof('[1]'::jsonb), jsonb_typeof('null'::jsonb), jsonb_array_length('[1,2,3]'::jsonb);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0054-select-jsonb_typeof-{-a-:1}"},
				},
				{
					Query: `SELECT to_jsonb(r)->>'id', to_jsonb(r)->>'label' FROM jsonb_record_items AS r;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0055-select-to_jsonb-r->>-id"},
				},
				{
					Query: `SELECT * FROM jsonb_populate_record(NULL::jsonb_populate_row, '{"a":1,"b":["2","a b"],"c":{"d":4,"e":"a b c"},"j":{"x":2},"ignored":"field"}'::jsonb);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0056-select-*-from-jsonb_populate_record-null::jsonb_populate_row"},
				},
				{
					Query: `SELECT * FROM jsonb_populate_record(ROW(10, 'base', true)::jsonb_populate_base, '{"a":5,"b":null,"ignored":"field"}'::jsonb);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0057-select-*-from-jsonb_populate_record-row"},
				},
				{
					Query: `SELECT (jsonb_populate_record(NULL::jsonb_populate_base, '{"a":7,"b":"scalar","c":false}'::jsonb)).b;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0058-select-jsonb_populate_record-null::jsonb_populate_base-{-a"},
				},
				{
					Query: `SELECT * FROM jsonb_to_record('{"a":1,"b":"foo","c":null,"j":{"x":2}}'::jsonb) AS r(a int, b text, c bool, j jsonb);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0059-select-*-from-jsonb_to_record-{"},
				},
				{
					Query: `SELECT r.b FROM jsonb_to_record('{"a":1,"b":"foo"}'::jsonb) AS r(a int, b text);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0060-select-r.b-from-jsonb_to_record-{"},
				},
				{
					Query: `SELECT * FROM jsonb_to_recordset('[{"a":1,"b":"foo","c":true},{"a":2,"b":"bar"}]'::jsonb) AS r(a int, b text, c bool) ORDER BY a;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0061-select-*-from-jsonb_to_recordset-[{"},
				},
				{
					Query: `SELECT jsonb_object_keys('{"a":1,"b":2}'::jsonb);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0062-select-jsonb_object_keys-{-a-:1"},
				},
				{
					Query: `SELECT * FROM jsonb_each('{"a":1,"b":"x"}'::jsonb) ORDER BY 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0063-select-*-from-jsonb_each-{"},
				},
				{
					Query: `SELECT jsonb_each('{"a":1}'::jsonb);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0064-select-jsonb_each-{-a-:1}"},
				},
				{
					Query: `SELECT * FROM jsonb_each_text('{"a":1,"b":"x","c":null}'::jsonb) ORDER BY 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0065-select-*-from-jsonb_each_text-{"},
				},
				{
					Query: `SELECT jsonb_each_text('{"a":"x\ny"}'::jsonb);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0066-select-jsonb_each_text-{-a-:"},
				},
				{
					Query: `SELECT jsonb_array_elements_text('["a\nb","c\\d"]'::jsonb);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0067-select-jsonb_array_elements_text-[-a\\nb-c\\\\d"},
				},
				{
					Query: `SELECT jsonb_path_exists('{"a":{"b":2},"items":[{"v":1},{"v":2}]}'::jsonb, '$.a.b'), jsonb_path_exists('{"a":{"b":2},"items":[{"v":1},{"v":2}]}'::jsonb, '$.missing');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0068-select-jsonb_path_exists-{-a-:{"},
				},
				{
					Query: `SELECT jsonb_path_query('{"items":[{"v":1},{"v":2}]}'::jsonb, '$.items[*].v');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0069-select-jsonb_path_query-{-items-:[{"},
				},
				{
					Query: `SELECT jsonb_path_query_array('{"items":[{"v":1},{"v":2}]}'::jsonb, '$.items[*].v');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0070-select-jsonb_path_query_array-{-items-:[{"},
				},
				{
					Query: `SELECT jsonb_path_match('{"a":2}'::jsonb, '$.a == 2'), jsonb_path_match('{"a":2}'::jsonb, '$.a > 3');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0071-select-jsonb_path_match-{-a-:2}"},
				},
				{
					Query: `SELECT '{"items":[{"v":1},{"v":2}]}'::jsonb @? '$.items[*].v', '{"items":[{"v":1},{"v":2}]}'::jsonb @? '$.missing';`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0072-select-{-items-:[{-v"},
				},
				{
					Query: `SELECT '{"a":2}'::jsonb @@ '$.a == 2', '{"a":2}'::jsonb @@ '$.a > 3';`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0073-select-{-a-:2}-::jsonb"},
				},
				{
					Query: `SELECT jsonb_extract_path('{"a":{"b":"x"},"arr":[10,null]}'::jsonb, 'a', 'b');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0074-select-jsonb_extract_path-{-a-:{"},
				},
				{
					Query: `SELECT jsonb_extract_path_text('{"a":{"b":"x"},"arr":[10,null]}'::jsonb, 'a', 'b');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0075-select-jsonb_extract_path_text-{-a-:{"},
				},
				{
					Query: `SELECT jsonb_extract_path_text('{"a":{"b":"x\ny"}}'::jsonb, 'a', 'b');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0076-select-jsonb_extract_path_text-{-a-:{"},
				},
				{
					Query: `SELECT jsonb_extract_path('{"a":{"b":"x"},"arr":[10,null]}'::jsonb, 'arr', '1') IS NULL, jsonb_extract_path_text('{"a":{"b":"x"},"arr":[10,null]}'::jsonb, 'arr', '1') IS NULL;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0077-select-jsonb_extract_path-{-a-:{"},
				},
				{
					Query: `SELECT jsonb_extract_path('{"a":{"b":"x"},"arr":[10,null]}'::jsonb, 'missing') IS NULL, jsonb_extract_path_text('{"a":{"b":"x"},"arr":[10,null]}'::jsonb, 'missing') IS NULL;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0078-select-jsonb_extract_path-{-a-:{"},
				},
				{
					Query: `SELECT '{"a":{"b":"x"},"arr":[10,null]}'::jsonb #> ARRAY['a','b'], '{"a":{"b":"x"},"arr":[10,null]}'::jsonb #>> ARRAY['a','b'];`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0079-select-{-a-:{-b"},
				},
				{
					Query: `SELECT jsonb_strip_nulls('{"a":1,"b":null,"c":[2,null,{"d":null,"e":3}],"f":{"g":null,"h":4}}');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0080-select-jsonb_strip_nulls-{-a-:1"},
				},
				{
					Query: `SELECT jsonb_strip_nulls('[1,null,{"a":null,"b":2}]');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0081-select-jsonb_strip_nulls-[1-null-{"},
				},
				{
					Query: `SELECT jsonb_strip_nulls('{"a":{"b":null,"c":null},"d":{}}');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0082-select-jsonb_strip_nulls-{-a-:{"},
				},
				{
					Query: `SELECT jsonb_object('{a,1,b,"two words",c,NULL}'::text[]);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0083-select-jsonb_object-{a-1-b"},
				},
				{
					Query: `SELECT jsonb_object('{a,b,c}'::text[], '{1,"two words",NULL}'::text[]);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0084-select-jsonb_object-{a-b-c}"},
				},
				{
					Query: `SELECT jsonb_object('{a,b,c}'::text[]);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0085-select-jsonb_object-{a-b-c}", Compare: "sqlstate"},
				},
				{
					Query: `SELECT jsonb_object(ARRAY['a', NULL]::text[], ARRAY['1', '2']::text[]);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0086-select-jsonb_object-array[-a-null]::text[]", Compare: "sqlstate"},
				},
				{
					Query: `SELECT '{"a":1,"b":[2,3]}'::jsonb @> '{"b":[2]}'::jsonb, '{"a":1}'::jsonb <@ '{"a":1,"b":2}'::jsonb;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0087-select-{-a-:1-b"},
				},
				{
					Query: `SELECT '[[1,2]]'::jsonb @> '[1]'::jsonb, '[[1,2]]'::jsonb @> '[[1]]'::jsonb;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0088-select-[[1-2]]-::jsonb-@>"},
				},
				{
					Query: `SELECT '{"a":[1,2]}'::jsonb @> '{"a":1}'::jsonb, '{"a":[1,2]}'::jsonb @> '{"a":[1]}'::jsonb;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0089-select-{-a-:[1-2]}"},
				},
				{
					Query: `SELECT jsonb_set('{"a":1,"b":[1,2]}'::jsonb, '{a}', '2'::jsonb);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0090-select-jsonb_set-{-a-:1"},
				},
				{
					Query: `SELECT jsonb_set('{"a":1}'::jsonb, '{}', '2'::jsonb);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0091-select-jsonb_set-{-a-:1}"},
				},
				{
					Query: `SELECT jsonb_set('{"a":1}'::jsonb, '{b,c}', '2'::jsonb);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0092-select-jsonb_set-{-a-:1}"},
				},
				{
					Query: `SELECT jsonb_set('{"a":1}'::jsonb, ARRAY[NULL]::text[], '2'::jsonb);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0093-select-jsonb_set-{-a-:1}", Compare: "sqlstate"},
				},
				{
					Query: `SELECT jsonb_set_lax('{"a":1,"b":2}'::jsonb, '{b}', '5'::jsonb), jsonb_set_lax('{"a":1,"b":2}'::jsonb, '{d}', '6'::jsonb, true);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0094-select-jsonb_set_lax-{-a-:1"},
				},
				{
					Query: `SELECT jsonb_set_lax('{"a":1,"b":2}'::jsonb, '{b}', NULL::jsonb), jsonb_set_lax('{"a":1,"b":2}'::jsonb, '{d}', NULL::jsonb, true);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0095-select-jsonb_set_lax-{-a-:1"},
				},
				{
					Query: `SELECT jsonb_set_lax('{"a":1,"b":2}'::jsonb, '{b}', NULL::jsonb, true, 'return_target'), jsonb_set_lax('{"a":1,"b":2}'::jsonb, '{b}', NULL::jsonb, true, 'delete_key'), jsonb_set_lax('{"a":1,"b":2}'::jsonb, '{b}', NULL::jsonb, true, 'use_json_null');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0096-select-jsonb_set_lax-{-a-:1"},
				},
				{
					Query: `SELECT jsonb_set_lax(NULL::jsonb, '{b}', NULL::jsonb) IS NULL, jsonb_set_lax('{"a":1}'::jsonb, NULL::text[], NULL::jsonb) IS NULL, jsonb_set_lax('{"a":1}'::jsonb, '{a}', NULL::jsonb, NULL::boolean) IS NULL;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0097-select-jsonb_set_lax-null::jsonb-{b}-null::jsonb"},
				},
				{
					Query: `SELECT jsonb_set_lax('{"a":1,"b":2}'::jsonb, '{b}', NULL::jsonb, true, 'raise_exception');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0098-select-jsonb_set_lax-{-a-:1", Compare: "sqlstate"},
				},
				{
					Query: `SELECT jsonb_set_lax('{"a":1,"b":2}'::jsonb, '{b}', NULL::jsonb, true, NULL::text);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0099-select-jsonb_set_lax-{-a-:1", Compare: "sqlstate"},
				},
				{
					Query: `SELECT jsonb_set_lax('{"a":1,"b":2}'::jsonb, '{b}', NULL::jsonb, true, 'no_such_treatment');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0100-select-jsonb_set_lax-{-a-:1", Compare: "sqlstate"},
				},
				{
					Query: `SELECT jsonb_delete('{"a":1, "b":2, "c":3}'::jsonb, 'a');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0101-select-jsonb_delete-{-a-:1"},
				},
				{
					Query: `SELECT jsonb_delete('{"a":1, "b":2, "c":3}'::jsonb, ARRAY['a','c']::text[]);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0102-select-jsonb_delete-{-a-:1", Compare: "sqlstate"},
				},
				{
					Query: `SELECT jsonb_delete_path('{"n":null, "a":1, "b":[1,2], "d":{"1":[2,3]}}', '{n}');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0103-select-jsonb_delete_path-{-n-:null"},
				},
				{
					Query: `SELECT jsonb_delete_path('{"n":null, "a":1, "b":[1,2], "d":{"1":[2,3]}}', '{b,-1}');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0104-select-jsonb_delete_path-{-n-:null"},
				},
				{
					Query: `SELECT jsonb_delete_path('{"a":1}'::jsonb, '{}'::text[]);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0105-select-jsonb_delete_path-{-a-:1}"},
				},
				{
					Query: `SELECT jsonb_delete_path('{"a":1}'::jsonb, ARRAY[NULL]::text[]);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0106-select-jsonb_delete_path-{-a-:1}", Compare: "sqlstate"},
				},
				{
					Query: `SELECT jsonb_delete_path('"a"'::jsonb, '{a}'::text[]);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0107-select-jsonb_delete_path-a-::jsonb-{a}", Compare: "sqlstate"},
				},
				{
					Query: `SELECT jsonb_insert('{"a":[0,1,2]}', '{a,1}', '"new_value"');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0108-select-jsonb_insert-{-a-:[0"},
				},
				{
					Query: `SELECT jsonb_insert('{"a":[0,1,2]}', '{a,1}', '"new_value"', true);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0109-select-jsonb_insert-{-a-:[0"},
				},
				{
					Query: `SELECT jsonb_insert('{"a":{"b":{"c":[0,1,"test1","test2"]}}}', '{a,b,c,2}', '"new_value"');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0110-select-jsonb_insert-{-a-:{"},
				},
				{
					Query: `SELECT jsonb_insert('{"a":[0,1,2]}', '{a,10}', '"new_value"'), jsonb_insert('{"a":[0,1,2]}', '{a,-10}', '"new_value"');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0111-select-jsonb_insert-{-a-:[0"},
				},
				{
					Query: `SELECT jsonb_insert('{"a":{"b":"value"}}', '{a,c}', '"new_value"'), jsonb_insert('{"a":{"b":"value"}}', '{a,b}', '"new_value"');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0112-select-jsonb_insert-{-a-:{", Compare: "sqlstate"},
				},
				{
					Query: `SELECT jsonb_insert('{"a":[0,1,2]}', '{missing,0}', '"new_value"');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0113-select-jsonb_insert-{-a-:[0"},
				},
				{
					Query: `SELECT jsonb_insert('{"a":[0,1,2]}', '{a,not_an_int}', '"new_value"');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0114-select-jsonb_insert-{-a-:[0", Compare: "sqlstate"},
				},
				{
					Query: `SELECT jsonb_insert('{"a":[0,1,2]}', ARRAY['a',NULL]::text[], '"new_value"');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0115-select-jsonb_insert-{-a-:[0", Compare: "sqlstate"},
				},
				{
					Query: `SELECT jsonb_pretty('{"a":1,"b":[2]}'::jsonb);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0116-select-jsonb_pretty-{-a-:1"},
				},
			},
		},
		{
			Name: "json mixed representation coverage",
			SetUpScript: []string{
				`CREATE TABLE json_shapes (pk INT primary key, doc jsonb, raw json);`,
				`INSERT INTO json_shapes VALUES (1, '{"a":1,"items":[1,2]}'::jsonb, '{"a":1,"items":[1,2]}'::json), (2, jsonb_build_object('a', 2, 'items', jsonb_build_array(2, 3), 'payload', repeat('x', 512)), json_build_object('a', 2, 'items', json_build_array(2, 3), 'payload', repeat('x', 512)));`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT pk, jsonb_array_length(doc->'items'), json_array_length(raw->'items') FROM json_shapes ORDER BY pk;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0117-select-pk-jsonb_array_length-doc->-items"},
				},
				{
					Query: `SELECT s.pk, e.value FROM json_shapes s JOIN LATERAL (SELECT * FROM jsonb_each(s.doc)) AS e ON true WHERE e.key = 'a' ORDER BY s.pk;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0118-select-s.pk-e.value-from-json_shapes"},
				},
				{
					Query: `SELECT s.pk, e.value FROM json_shapes s JOIN LATERAL jsonb_each(s.doc) AS e ON true WHERE e.key = 'a' ORDER BY s.pk;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0119-select-s.pk-e.value-from-json_shapes"},
				},
				{
					Query: `SELECT s.pk, key FROM json_shapes s JOIN LATERAL jsonb_object_keys(s.doc) AS key ON true ORDER BY s.pk, key;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0120-select-s.pk-key-from-json_shapes"},
				},
				{
					Query: `SELECT s.pk, key FROM json_shapes s, jsonb_object_keys(s.doc) AS key ORDER BY s.pk, key;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0121-select-s.pk-key-from-json_shapes"},
				},
				{
					Query: `SELECT s.pk, e.jsonb_array_elements FROM json_shapes s JOIN LATERAL (SELECT * FROM jsonb_array_elements(s.doc->'items')) AS e ON true ORDER BY s.pk, e.jsonb_array_elements;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0122-select-s.pk-e.jsonb_array_elements-from-json_shapes", Compare: "sqlstate"},
				},
				{
					Query: `SELECT s.pk, e.jsonb_array_elements FROM json_shapes s JOIN LATERAL jsonb_array_elements(s.doc->'items') AS e ON true ORDER BY s.pk, e.jsonb_array_elements;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testjsonfunctions-0123-select-s.pk-e.jsonb_array_elements-from-json_shapes", Compare: "sqlstate"},
				},
				{
					Query:    `SELECT s.pk, elem FROM json_shapes s JOIN LATERAL jsonb_array_elements(s.doc->'items') AS elem ON true ORDER BY s.pk, elem;`,
					Expected: []sql.Row{{1, "1"}, {1, "2"}, {2, "2"}, {2, "3"}},
				},
				{
					Query:    `SELECT DISTINCT elem FROM json_shapes s JOIN LATERAL jsonb_array_elements(s.doc->'items') AS elem ON true ORDER BY elem;`,
					Expected: []sql.Row{{"1"}, {"2"}, {"3"}},
				},
				{
					Query:    `SELECT DISTINCT jsonb_array_elements(doc->'items') FROM json_shapes ORDER BY jsonb_array_elements;`,
					Expected: []sql.Row{{"1"}, {"2"}, {"3"}},
				},
				{
					Query:    `SELECT s.pk, e.json_array_elements FROM json_shapes s JOIN LATERAL (SELECT * FROM json_array_elements(s.raw->'items')) AS e ON true ORDER BY s.pk, e.json_array_elements;`,
					Expected: []sql.Row{{1, "1"}, {1, "2"}, {2, "2"}, {2, "3"}},
				},
				{
					Query:    `SELECT s.pk, e.json_array_elements FROM json_shapes s JOIN LATERAL json_array_elements(s.raw->'items') AS e ON true ORDER BY s.pk, e.json_array_elements;`,
					Expected: []sql.Row{{1, "1"}, {1, "2"}, {2, "2"}, {2, "3"}},
				},
				{
					Query:    `SELECT DISTINCT elem FROM json_shapes s JOIN LATERAL json_array_elements(s.raw->'items') AS elem ON true ORDER BY elem;`,
					Expected: []sql.Row{{"1"}, {"2"}, {"3"}},
				},
				{
					Query:    `SELECT DISTINCT elem FROM json_shapes s JOIN LATERAL jsonb_array_elements_text(s.doc->'items') AS elem ON true ORDER BY elem;`,
					Expected: []sql.Row{{"1"}, {"2"}, {"3"}},
				},
				{
					Query:    `SELECT length(doc->>'payload') FROM json_shapes WHERE pk = 2;`,
					Expected: []sql.Row{{512}},
				},
				{
					Query:    `UPDATE json_shapes SET doc = jsonb_set(doc, '{items,1}', '9'::jsonb) WHERE pk = 1;`,
					Expected: []sql.Row{},
				},
				{
					Query:    `SELECT doc->'items' FROM json_shapes WHERE pk = 1;`,
					Expected: []sql.Row{{`[1, 9]`}},
				},
			},
		},
	})
}

func TestArrayFunctions(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "unnest",
			SetUpScript: []string{
				`CREATE TABLE testing (id INT primary key, val1 smallint[]);`,
				`INSERT INTO testing VALUES (1, '{}'), (2, '{1}'), (3, '{1, 2}');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT unnest(val1) FROM testing WHERE id=1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testarrayfunctions-0001-select-unnest-val1-from-testing"},
				},
				{
					Query: `SELECT unnest(val1) FROM testing WHERE id=2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testarrayfunctions-0002-select-unnest-val1-from-testing"},
				},
				{
					Query: `SELECT unnest(val1) FROM testing WHERE id=3;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testarrayfunctions-0003-select-unnest-val1-from-testing"},
				},
				{
					Skip:  true, // TODO: fix for this in gms breaks regression test
					Query: `select * from unnest(array[1,2,3]);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testarrayfunctions-0004-select-*-from-unnest-array[1"},
				},
			},
		},
		{
			Name:        "array_to_string",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT array_to_string(ARRAY[1, 2, 3, NULL, 5], ',', '*')`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testarrayfunctions-0005-select-array_to_string-array[1-2-3"},
				},
				{
					Query: `SELECT array_to_string(ARRAY[1, 2, 3, NULL, 5], ',')`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testarrayfunctions-0006-select-array_to_string-array[1-2-3"},
				},
				{
					Query: `SELECT array_to_string(ARRAY[37.89, 1.2], '_');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testarrayfunctions-0007-select-array_to_string-array[37.89-1.2]"},
				},
				{
					Skip:  true, // TODO: we currently return "37_1"
					Query: `SELECT array_to_string(ARRAY[37.89::int4, 1.2::int4], '_');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testarrayfunctions-0008-select-array_to_string-array[37.89::int4-1.2::int4]"},
				},
			},
		},
		{
			Name: "array_upper",
			Assertions: []ScriptTestAssertion{
				{
					Query: `select array_upper(ARRAY[1,2,3,4], 1);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testarrayfunctions-0009-select-array_upper-array[1-2-3"},
				},
				{
					Skip:  true, // TODO: multi-dimensional is not supported yet
					Query: `select array_upper(ARRAY[1,2,3,4], 2);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testarrayfunctions-0010-select-array_upper-array[1-2-3"},
				},
			},
		},
		{
			Name: "array_cat",
			Assertions: []ScriptTestAssertion{
				{
					Query: `select array_cat(ARRAY[1,2,3], ARRAY[4,5]);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testarrayfunctions-0011-select-array_cat-array[1-2-3]"},
				},
				{
					Query: `select array_cat(NULL, ARRAY[4,5]);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testarrayfunctions-0012-select-array_cat-null-array[4-5]"},
				},
				{
					Query: `select array_cat(ARRAY[1,2,3], NULL);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testarrayfunctions-0013-select-array_cat-array[1-2-3]"},
				},
				{
					Query: `select array_cat(NULL, NULL);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testarrayfunctions-0014-select-array_cat-null-null"},
				},
			},
		},
		{
			Name: "array_length",
			Assertions: []ScriptTestAssertion{
				{
					Query: `select array_length(ARRAY[1,2,3,4,5], 1);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testarrayfunctions-0015-select-array_length-array[1-2-3"},
				},
			},
		},
		{
			Name: "array_position and array_positions",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT array_position(ARRAY[1,2,3,4,5], 4);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testarrayfunctions-0016-select-array_position-array[1-2-3"},
				},
				{
					Query: `SELECT array_position(ARRAY[1,4,2,3,4,5,4], 4, 3);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testarrayfunctions-0017-select-array_position-array[1-4-2"},
				},
				{
					Query: `SELECT array_position(int2vectorin('1 3 5'), 3::int2);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testarrayfunctions-0018-select-array_position-int2vectorin-1-3"},
				},
				{
					Query: `SELECT array_position(int2vectorin('1 3 5'), 2::int2);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testarrayfunctions-0019-select-array_position-int2vectorin-1-3"},
				},
				{
					Query: `SELECT array_position(NULL::int2vector, 1::int2);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testarrayfunctions-0020-select-array_position-null::int2vector-1::int2"},
				},
				{
					Query: `select array_position(NULL, 1);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testarrayfunctions-0021-select-array_position-null-1"},
				},
				{
					Query: `select array_position(ARRAY[1,4,2,3,4,5,4], NULL);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testarrayfunctions-0022-select-array_position-array[1-4-2"},
				},
				{
					Query: `select array_position(NULL, NULL);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testarrayfunctions-0023-select-array_position-null-null"},
				},
				{
					Query: `SELECT array_positions(ARRAY[1,2,3,4,5,6,1,2,3,4,5,6], 4);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testarrayfunctions-0024-select-array_positions-array[1-2-3"},
				},
				{
					Query: `select array_positions(NULL, 1);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testarrayfunctions-0025-select-array_positions-null-1"},
				},
				{
					Query: `select array_positions(ARRAY[1,4,2,3,4,5,4], NULL);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testarrayfunctions-0026-select-array_positions-array[1-4-2"},
				},
				{
					Query: `select array_positions(NULL, NULL);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testarrayfunctions-0027-select-array_positions-null-null"},
				},
			},
		},
		{
			Name: "array_prepend",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT array_prepend(NULL, NULL);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testarrayfunctions-0028-select-array_prepend-null-null"},
				},
				{
					Query: `SELECT array_prepend(NULL, ARRAY[6]);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testarrayfunctions-0029-select-array_prepend-null-array[6]"},
				},
				{
					Query: `SELECT array_prepend(5, NULL);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testarrayfunctions-0030-select-array_prepend-5-null"},
				},
				{
					Query: `SELECT array_prepend(5, ARRAY[6]);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testarrayfunctions-0031-select-array_prepend-5-array[6]"},
				},
			},
		},
	})
}

func TestSchemaVisibilityInquiryFunctions(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Skip:        true, // TODO: not supported
			Name:        "pg_function_is_visible",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT pg_function_is_visible(1342177280);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testschemavisibilityinquiryfunctions-0001-select-pg_function_is_visible-1342177280"},
				},
				{
					Query: `SELECT pg_function_is_visible(22);`, PostgresOracle: // invalid
					ScriptTestPostgresOracle{ID: "functions-test-testschemavisibilityinquiryfunctions-0002-select-pg_function_is_visible-22"},
				},
			},
		},
		{
			Name: "pg_table_is_visible",
			SetUpScript: []string{
				"CREATE SCHEMA myschema;",
				"SET search_path TO myschema;",
				"CREATE TABLE mytable (id int, name text);",
				"INSERT INTO mytable VALUES (1,'desk'), (2,'chair');",
				"CREATE VIEW myview AS SELECT name FROM mytable;",
				"CREATE SCHEMA testschema;",
				"SET search_path TO testschema;",
				`CREATE TABLE test_table (pk INT primary key, v1 INT UNIQUE);`,
				"INSERT INTO test_table VALUES (1,5), (2,7);",
				"CREATE INDEX test_index ON test_table(v1);",
				"CREATE SEQUENCE test_seq START 39;",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT c.oid, c.relname AS table_name, n.nspname AS table_schema FROM pg_catalog.pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE (n.nspname='myschema' OR n.nspname='testschema') AND left(relname, 5) <> 'dolt_' order by relname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testschemavisibilityinquiryfunctions-0003-select-c.oid-c.relname-as-table_name"},
				},
				{
					Query: `SHOW search_path;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testschemavisibilityinquiryfunctions-0004-show-search_path"},
				},
				{
					Query: `select pg_table_is_visible(3057657334);`, PostgresOracle: // index from testschema
					ScriptTestPostgresOracle{ID: "functions-test-testschemavisibilityinquiryfunctions-0005-select-pg_table_is_visible-3057657334"},
				},
				{
					Query: `select pg_table_is_visible(1952237395);`, PostgresOracle: // table from testschema
					ScriptTestPostgresOracle{ID: "functions-test-testschemavisibilityinquiryfunctions-0006-select-pg_table_is_visible-1952237395"},
				},
				{
					Query: `select pg_table_is_visible(1539973141);`, PostgresOracle: // sequence from testschema
					ScriptTestPostgresOracle{ID: "functions-test-testschemavisibilityinquiryfunctions-0007-select-pg_table_is_visible-1539973141"},
				},
				{
					Query: `select pg_table_is_visible(3983475213);`, PostgresOracle: // view from myschema
					ScriptTestPostgresOracle{ID: "functions-test-testschemavisibilityinquiryfunctions-0008-select-pg_table_is_visible-3983475213"},
				},
				{
					Query: `SET search_path = 'myschema';`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testschemavisibilityinquiryfunctions-0009-set-search_path-=-myschema"},
				},
				{
					Query: `SHOW search_path;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testschemavisibilityinquiryfunctions-0010-show-search_path"},
				},
				{
					Query: `select pg_table_is_visible(3983475213);`, PostgresOracle: // view from myschema
					ScriptTestPostgresOracle{ID: "functions-test-testschemavisibilityinquiryfunctions-0011-select-pg_table_is_visible-3983475213"},
				},
				{
					Query: `select pg_table_is_visible(3905781870);`, PostgresOracle: // table from myschema
					ScriptTestPostgresOracle{ID: "functions-test-testschemavisibilityinquiryfunctions-0012-select-pg_table_is_visible-3905781870"},
				},
			},
		},
		{
			Name: "pg_type_is_visible",
			SetUpScript: []string{
				"CREATE SCHEMA myschema;",
				"SET search_path TO myschema;",
				"CREATE DOMAIN mydomain AS text;",
				"CREATE TYPE myenum AS ENUM ('a', 'b', 'c');",
				"CREATE SCHEMA testschema;",
				"SET search_path TO testschema;",
				"CREATE DOMAIN test_domain AS int;",
				"CREATE TYPE test_enum AS ENUM ('x', 'y', 'z');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT t.oid, t.typname, n.nspname FROM pg_catalog.pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace WHERE n.nspname='myschema' OR n.nspname='testschema' ORDER BY t.typname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testschemavisibilityinquiryfunctions-0013-select-t.oid-t.typname-n.nspname-from"},
				},
				{
					Query: `SHOW search_path;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testschemavisibilityinquiryfunctions-0014-show-search_path"},
				},
				{
					Query: `SELECT pg_type_is_visible(2272253470);`, PostgresOracle: // test_domain from testschema
					ScriptTestPostgresOracle{ID: "functions-test-testschemavisibilityinquiryfunctions-0015-select-pg_type_is_visible-2272253470"},
				},
				{
					Query: `SELECT pg_type_is_visible(1117094145);`, PostgresOracle: // test_enum from testschema
					ScriptTestPostgresOracle{ID: "functions-test-testschemavisibilityinquiryfunctions-0016-select-pg_type_is_visible-1117094145"},
				},
				{
					Query: `SELECT pg_type_is_visible(340132571);`, PostgresOracle: // mydomain from myschema
					ScriptTestPostgresOracle{ID: "functions-test-testschemavisibilityinquiryfunctions-0017-select-pg_type_is_visible-340132571"},
				},
				{
					Query: `SELECT pg_type_is_visible(1684884017);`, PostgresOracle: // myenum from myschema
					ScriptTestPostgresOracle{ID: "functions-test-testschemavisibilityinquiryfunctions-0018-select-pg_type_is_visible-1684884017"},
				},
				{
					Query: `SET search_path = 'myschema';`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testschemavisibilityinquiryfunctions-0019-set-search_path-=-myschema"},
				},
				{
					Query: `SHOW search_path;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testschemavisibilityinquiryfunctions-0020-show-search_path"},
				},
				{
					Query: `SELECT pg_type_is_visible(340132571);`, PostgresOracle: // mydomain from myschema
					ScriptTestPostgresOracle{ID: "functions-test-testschemavisibilityinquiryfunctions-0021-select-pg_type_is_visible-340132571"},
				},
				{
					Query: `SELECT pg_type_is_visible(1684884017);`, PostgresOracle: // myenum from myschema
					ScriptTestPostgresOracle{ID: "functions-test-testschemavisibilityinquiryfunctions-0022-select-pg_type_is_visible-1684884017"},
				},
				{
					Query: `SELECT pg_type_is_visible(2272253470);`, PostgresOracle: // test_domain from testschema
					ScriptTestPostgresOracle{ID: "functions-test-testschemavisibilityinquiryfunctions-0023-select-pg_type_is_visible-2272253470"},
				},
				{
					Query: `SELECT pg_type_is_visible(1117094145);`, PostgresOracle: // test_enum from testschema
					ScriptTestPostgresOracle{ID: "functions-test-testschemavisibilityinquiryfunctions-0024-select-pg_type_is_visible-1117094145"},
				},
				{
					Query: `SELECT pg_type_is_visible(999999);`, PostgresOracle: // non-existent type oid
					ScriptTestPostgresOracle{ID: "functions-test-testschemavisibilityinquiryfunctions-0025-select-pg_type_is_visible-999999"},
				},
				{
					Query: `SET search_path = 'pg_catalog';`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testschemavisibilityinquiryfunctions-0026-set-search_path-=-pg_catalog"},
				},
				{
					Query: `SELECT pg_type_is_visible('text'::regtype::oid);`, PostgresOracle: // built-in type should be visible in pg_catalog
					ScriptTestPostgresOracle{ID: "functions-test-testschemavisibilityinquiryfunctions-0027-select-pg_type_is_visible-text-::regtype::oid"},
				},
				{
					Query: `SELECT pg_type_is_visible('int4'::regtype::oid);`, PostgresOracle: // built-in type should be visible in pg_catalog
					ScriptTestPostgresOracle{ID: "functions-test-testschemavisibilityinquiryfunctions-0028-select-pg_type_is_visible-int4-::regtype::oid"},
				},
			},
		},
	})
}

func TestSystemCatalogInformationFunctions(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "pg_encoding_to_char",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT pg_encoding_to_char(encoding) FROM pg_database WHERE datname = 'postgres';`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsystemcataloginformationfunctions-0001-select-pg_encoding_to_char-encoding-from-pg_database"},
				},
			},
		},
		{
			Name:        "pg_get_functiondef",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					// TODO: not supported yet
					Query: `SELECT pg_get_functiondef(22)`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsystemcataloginformationfunctions-0002-select-pg_get_functiondef-22"},
				},
			},
		},
		{
			Name:        "pg_get_function_result",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					// TODO: not supported yet
					Query: `SELECT pg_get_function_result(22)`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsystemcataloginformationfunctions-0003-select-pg_get_function_result-22"},
				},
			},
		},
		{
			Name:        "pg_get_triggerdef",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					// TODO: triggers are not supported yet
					Query: `SELECT pg_get_triggerdef(22)`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsystemcataloginformationfunctions-0004-select-pg_get_triggerdef-22"},
				},
			},
		},
		{
			Name: "pg_get_userbyid",
			SetUpScript: []string{
				`CREATE USER catalog_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT pg_get_userbyid(22)`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsystemcataloginformationfunctions-0005-select-pg_get_userbyid-22"},
				},
				{
					Query: `SELECT pg_get_userbyid(oid) FROM pg_roles WHERE rolname = 'catalog_user'`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsystemcataloginformationfunctions-0006-select-pg_get_userbyid-oid-from-pg_roles"},
				},
			},
		},
		{
			Name: "pg_get_viewdef",
			SetUpScript: []string{
				"CREATE TABLE test (id int, name text)",
				"INSERT INTO test VALUES (1,'desk'), (2,'chair')",
				"CREATE VIEW test_view AS SELECT name FROM test",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT c.oid, c.relname AS table_name, n.nspname AS table_schema FROM pg_catalog.pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE (n.nspname='myschema' OR n.nspname='public') and left(relname, 5) <> 'dolt_';`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsystemcataloginformationfunctions-0007-select-c.oid-c.relname-as-table_name"},
				},
				{
					Query: `select pg_get_viewdef(2707638987);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsystemcataloginformationfunctions-0008-select-pg_get_viewdef-2707638987"},
				},
			},
		},
	})
}

func TestDateAndTimeFunction(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "extract from date",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT EXTRACT(CENTURY FROM DATE '2022-02-02');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0001-select-extract-century-from-date"},
				},
				{
					Query: `SELECT EXTRACT(CENTURY FROM DATE '0002-12-31 BC');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0002-select-extract-century-from-date"},
				},
				{
					Query: `SELECT EXTRACT(DAY FROM DATE '2022-02-02');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0003-select-extract-day-from-date"},
				},
				{
					Query: `SELECT EXTRACT(DECADE FROM DATE '2022-02-02');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0004-select-extract-decade-from-date"},
				},
				{
					Query: `SELECT EXTRACT(DOW FROM DATE '2022-02-02');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0005-select-extract-dow-from-date"},
				},
				{
					Query: `SELECT EXTRACT(DOY FROM DATE '2022-02-02');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0006-select-extract-doy-from-date"},
				},
				{
					Query: `SELECT EXTRACT(EPOCH FROM DATE '2022-02-02');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0007-select-extract-epoch-from-date"},
				},
				{
					Query: `SELECT EXTRACT(HOUR FROM DATE '2022-02-02');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0008-select-extract-hour-from-date", Compare: "sqlstate"},
				},
				{
					Query: `SELECT EXTRACT(ISODOW FROM DATE '2022-02-02');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0009-select-extract-isodow-from-date"},
				},
				{
					Query: `SELECT EXTRACT(ISOYEAR FROM DATE '2006-01-01');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0010-select-extract-isoyear-from-date"},
				},
				{
					Query: `SELECT EXTRACT(ISOYEAR FROM DATE '2006-01-02');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0011-select-extract-isoyear-from-date"},
				},
				{
					Query: `SELECT extract(julian from date '2021-06-23');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0012-select-extract-julian-from-date"},
				},
				{
					Query: `SELECT EXTRACT(MICROSECONDS FROM DATE '2022-02-02');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0013-select-extract-microseconds-from-date", Compare: "sqlstate"},
				},
				{
					Query: `SELECT EXTRACT(MILLENNIUM FROM DATE '2022-02-02');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0014-select-extract-millennium-from-date"},
				},
				{
					Query: `SELECT EXTRACT(MILLISECONDS FROM DATE '2022-02-02');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0015-select-extract-milliseconds-from-date", Compare: "sqlstate"},
				},
				{
					Query: `SELECT EXTRACT(MINUTE FROM DATE '2022-02-02');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0016-select-extract-minute-from-date", Compare: "sqlstate"},
				},
				{
					Query: `SELECT EXTRACT(MONTH FROM DATE '2022-02-02');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0017-select-extract-month-from-date"},
				},
				{
					Query: `SELECT EXTRACT(QUARTER FROM DATE '2022-02-02');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0018-select-extract-quarter-from-date"},
				},
				{
					Query: `SELECT EXTRACT(SECOND FROM DATE '2022-02-02');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0019-select-extract-second-from-date", Compare: "sqlstate"},
				},
				{
					Query: `SELECT EXTRACT(TIMEZONE FROM DATE '2022-02-02');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0020-select-extract-timezone-from-date", Compare: "sqlstate"},
				},
				{
					Query: `SELECT EXTRACT(TIMEZONE_HOUR FROM DATE '2022-02-02');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0021-select-extract-timezone_hour-from-date", Compare: "sqlstate"},
				},
				{
					Query: `SELECT EXTRACT(TIMEZONE_MINUTE FROM DATE '2022-02-02');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0022-select-extract-timezone_minute-from-date", Compare: "sqlstate"},
				},
				{
					Query: `SELECT EXTRACT(WEEK FROM DATE '2022-02-02');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0023-select-extract-week-from-date"},
				},
				{
					Query: `SELECT EXTRACT(YEAR FROM DATE '2022-02-02');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0024-select-extract-year-from-date"},
				},
			},
		},
		{
			Name:        "extract from time without time zone",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT EXTRACT(CENTURY FROM TIME '17:12:28.5');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0025-select-extract-century-from-time", Compare: "sqlstate"},
				},
				{
					Query: `SELECT EXTRACT(DAY FROM TIME '17:12:28.5');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0026-select-extract-day-from-time", Compare: "sqlstate"},
				},
				{
					Query: `SELECT EXTRACT(DECADE FROM TIME '17:12:28.5');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0027-select-extract-decade-from-time", Compare: "sqlstate"},
				},
				{
					Query: `SELECT EXTRACT(DOW FROM TIME '17:12:28.5');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0028-select-extract-dow-from-time", Compare: "sqlstate"},
				},
				{
					Query: `SELECT EXTRACT(DOY FROM TIME '17:12:28.5');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0029-select-extract-doy-from-time", Compare: "sqlstate"},
				},
				{
					Query: `SELECT EXTRACT(EPOCH FROM TIME '17:12:28.5');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0030-select-extract-epoch-from-time"},
				},
				{
					Query: `SELECT EXTRACT(HOUR FROM TIME '17:12:28.5');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0031-select-extract-hour-from-time"},
				},
				{
					Query: `SELECT EXTRACT(ISODOW FROM TIME '17:12:28.5');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0032-select-extract-isodow-from-time", Compare: "sqlstate"},
				},
				{
					Query: `SELECT EXTRACT(ISOYEAR FROM TIME '17:12:28.5');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0033-select-extract-isoyear-from-time", Compare: "sqlstate"},
				},
				{
					Query: `SELECT EXTRACT(JULIAN FROM TIME '17:12:28.5');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0034-select-extract-julian-from-time", Compare: "sqlstate"},
				},
				{
					Query: `SELECT EXTRACT(MICROSECONDS FROM TIME '17:12:28.5');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0035-select-extract-microseconds-from-time"},
				},
				{
					Query: `SELECT EXTRACT(MILLENNIUM FROM TIME '17:12:28.5');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0036-select-extract-millennium-from-time", Compare: "sqlstate"},
				},
				{
					Query: `SELECT EXTRACT(MILLISECONDS FROM TIME '17:12:28.5');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0037-select-extract-milliseconds-from-time"},
				},
				{
					Query: `SELECT EXTRACT(MINUTE FROM TIME '17:12:28.5');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0038-select-extract-minute-from-time"},
				},
				{
					Query: `SELECT EXTRACT(MONTH FROM TIME '17:12:28.5');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0039-select-extract-month-from-time", Compare: "sqlstate"},
				},
				{
					Query: `SELECT EXTRACT(QUARTER FROM TIME '17:12:28.5');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0040-select-extract-quarter-from-time", Compare: "sqlstate"},
				},
				{
					Query: `SELECT EXTRACT(SECOND FROM TIME '17:12:28.5');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0041-select-extract-second-from-time"},
				},
				{
					Query: `SELECT EXTRACT(TIMEZONE FROM TIME '17:12:28.5');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0042-select-extract-timezone-from-time", Compare: "sqlstate"},
				},
				{
					Query: `SELECT EXTRACT(TIMEZONE_HOUR FROM TIME '17:12:28.5');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0043-select-extract-timezone_hour-from-time", Compare: "sqlstate"},
				},
				{
					Query: `SELECT EXTRACT(TIMEZONE_MINUTE FROM TIME '17:12:28.5');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0044-select-extract-timezone_minute-from-time", Compare: "sqlstate"},
				},
				{
					Query: `SELECT EXTRACT(WEEK FROM TIME '17:12:28.5');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0045-select-extract-week-from-time", Compare: "sqlstate"},
				},
				{
					Query: `SELECT EXTRACT(YEAR FROM TIME '17:12:28.5');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0046-select-extract-year-from-time", Compare: "sqlstate"},
				},
			},
		},
		{
			Name:        "extract from time with time zone",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT EXTRACT(CENTURY FROM TIME WITH TIME ZONE '17:12:28.5-03');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0047-select-extract-century-from-time", Compare: "sqlstate"},
				},
				{
					Query: `SELECT EXTRACT(DAY FROM TIME WITH TIME ZONE '17:12:28.5-03');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0048-select-extract-day-from-time", Compare: "sqlstate"},
				},
				{
					Query: `SELECT EXTRACT(DECADE FROM TIME WITH TIME ZONE '17:12:28.5-03');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0049-select-extract-decade-from-time", Compare: "sqlstate"},
				},
				{
					Query: `SELECT EXTRACT(DOW FROM TIME WITH TIME ZONE '17:12:28.5-03');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0050-select-extract-dow-from-time", Compare: "sqlstate"},
				},
				{
					Query: `SELECT EXTRACT(DOY FROM TIME WITH TIME ZONE '17:12:28.5-03');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0051-select-extract-doy-from-time", Compare: "sqlstate"},
				},
				{
					Query: `SELECT EXTRACT(EPOCH FROM TIME WITH TIME ZONE '17:12:28.5-03');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0052-select-extract-epoch-from-time"},
				},
				{
					Query: `SELECT EXTRACT(HOUR FROM TIME WITH TIME ZONE '17:12:28.5-03');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0053-select-extract-hour-from-time"},
				},
				{
					Query: `SELECT EXTRACT(ISODOW FROM TIME WITH TIME ZONE '17:12:28.5-03');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0054-select-extract-isodow-from-time", Compare: "sqlstate"},
				},
				{
					Query: `SELECT EXTRACT(ISOYEAR FROM TIME WITH TIME ZONE '17:12:28.5-03');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0055-select-extract-isoyear-from-time", Compare: "sqlstate"},
				},
				{
					Query: `SELECT EXTRACT(JULIAN FROM TIME WITH TIME ZONE '17:12:28.5-03');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0056-select-extract-julian-from-time", Compare: "sqlstate"},
				},
				{
					Query: `SELECT EXTRACT(MICROSECONDS FROM TIME WITH TIME ZONE '17:12:28.5-03');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0057-select-extract-microseconds-from-time"},
				},
				{
					Query: `SELECT EXTRACT(MILLENNIUM FROM TIME WITH TIME ZONE '17:12:28.5-03');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0058-select-extract-millennium-from-time", Compare: "sqlstate"},
				},
				{
					Query: `SELECT EXTRACT(MILLISECONDS FROM TIME WITH TIME ZONE '17:12:28.5-03');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0059-select-extract-milliseconds-from-time"},
				},
				{
					Query: `SELECT EXTRACT(MINUTE FROM TIME WITH TIME ZONE '17:12:28.5-03');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0060-select-extract-minute-from-time"},
				},
				{
					Query: `SELECT EXTRACT(MONTH FROM TIME WITH TIME ZONE '17:12:28.5-03');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0061-select-extract-month-from-time", Compare: "sqlstate"},
				},
				{
					Query: `SELECT EXTRACT(QUARTER FROM TIME WITH TIME ZONE '17:12:28.5-03');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0062-select-extract-quarter-from-time", Compare: "sqlstate"},
				},
				{
					Query: `SELECT EXTRACT(SECOND FROM TIME WITH TIME ZONE '17:12:28.5-03');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0063-select-extract-second-from-time"},
				},
				{
					Query: `SELECT EXTRACT(TIMEZONE FROM TIME WITH TIME ZONE '17:12:28.5+03');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0064-select-extract-timezone-from-time"},
				},
				{
					Query: `SELECT EXTRACT(TIMEZONE_HOUR FROM TIME WITH TIME ZONE '17:12:28.5-03');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0065-select-extract-timezone_hour-from-time"},
				},
				{
					Query: `SELECT EXTRACT(TIMEZONE_MINUTE FROM TIME WITH TIME ZONE '17:12:28.5-03:45');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0066-select-extract-timezone_minute-from-time"},
				},
				{
					Query: `SELECT EXTRACT(WEEK FROM TIME WITH TIME ZONE '17:12:28.5-03');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0067-select-extract-week-from-time", Compare: "sqlstate"},
				},
				{
					Query: `SELECT EXTRACT(YEAR FROM TIME WITH TIME ZONE '17:12:28.5-03');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0068-select-extract-year-from-time", Compare: "sqlstate"},
				},
			},
		},
		{
			Name:        "extract from timestamp without time zone",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT EXTRACT(CENTURY FROM TIMESTAMP '2000-12-16 12:21:13');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0069-select-extract-century-from-timestamp"},
				},
				{
					Query: `SELECT EXTRACT(CENTURY FROM TIMESTAMP '2001-02-16 20:38:40');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0070-select-extract-century-from-timestamp"},
				},
				{
					Query: `SELECT EXTRACT(DAY FROM TIMESTAMP '2001-02-16 20:38:40');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0071-select-extract-day-from-timestamp"},
				},
				{
					Query: `SELECT EXTRACT(DECADE FROM TIMESTAMP '2001-02-16 20:38:40');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0072-select-extract-decade-from-timestamp"},
				},
				{
					Query: `SELECT EXTRACT(DOW FROM TIMESTAMP '2001-02-16 20:38:40');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0073-select-extract-dow-from-timestamp"},
				},
				{
					Query: `SELECT EXTRACT(DOY FROM TIMESTAMP '2001-02-16 20:38:40');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0074-select-extract-doy-from-timestamp"},
				},
				{
					Query: `SELECT EXTRACT(EPOCH FROM TIMESTAMP '2001-02-16 20:38:40.12');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0075-select-extract-epoch-from-timestamp"},
				},
				{
					Query: `SELECT EXTRACT(HOUR FROM TIMESTAMP '2001-02-16 20:38:40');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0076-select-extract-hour-from-timestamp"},
				},
				{
					Query: `SELECT EXTRACT(ISODOW FROM TIMESTAMP '2001-02-18 20:38:40');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0077-select-extract-isodow-from-timestamp"},
				},
				{
					Query: `SELECT EXTRACT(ISOYEAR FROM TIMESTAMP '2001-02-18 20:38:40');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0078-select-extract-isoyear-from-timestamp"},
				},
				{
					Skip:  true, // TODO: not supported yet
					Query: `SELECT EXTRACT(JULIAN FROM TIMESTAMP '2001-02-18 20:38:40');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0079-select-extract-julian-from-timestamp"},
				},
				{
					Query: `SELECT EXTRACT(MICROSECONDS FROM TIMESTAMP '2001-02-18 20:38:40');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0080-select-extract-microseconds-from-timestamp"},
				},
				{
					Query: `SELECT EXTRACT(MILLENNIUM FROM TIMESTAMP '2001-02-16 20:38:40');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0081-select-extract-millennium-from-timestamp"},
				},
				{
					Query: `SELECT EXTRACT(MILLENNIUM FROM TIMESTAMP '2000-02-16 20:38:40');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0082-select-extract-millennium-from-timestamp"},
				},
				{
					Query: `SELECT EXTRACT(MILLISECONDS FROM TIMESTAMP '2000-02-16 20:38:40');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0083-select-extract-milliseconds-from-timestamp"},
				},
				{
					Query: `SELECT EXTRACT(MINUTE FROM TIMESTAMP '2001-02-16 20:38:40');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0084-select-extract-minute-from-timestamp"},
				},
				{
					Query: `SELECT EXTRACT(MONTH FROM TIMESTAMP '2001-02-16 20:38:40');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0085-select-extract-month-from-timestamp"},
				},
				{
					Query: `SELECT EXTRACT(QUARTER FROM TIMESTAMP '2001-02-16 20:38:40');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0086-select-extract-quarter-from-timestamp"},
				},
				{
					Query: `SELECT EXTRACT(SECOND FROM TIMESTAMP '2001-02-16 20:38:40');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0087-select-extract-second-from-timestamp"},
				},
				{
					Query: `SELECT EXTRACT(TIMEZONE FROM TIMESTAMP '2001-02-16 20:38:40');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0088-select-extract-timezone-from-timestamp", Compare: "sqlstate"},
				},
				{
					Query: `SELECT EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2001-02-16 20:38:40');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0089-select-extract-timezone_hour-from-timestamp", Compare: "sqlstate"},
				},
				{
					Query: `SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2001-02-16 20:38:40');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0090-select-extract-timezone_minute-from-timestamp", Compare: "sqlstate"},
				},
				{
					Query: `SELECT EXTRACT(WEEK FROM TIMESTAMP '2001-02-16 20:38:40');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0091-select-extract-week-from-timestamp"},
				},
				{
					Query: `SELECT EXTRACT(YEAR FROM TIMESTAMP '2001-02-16 20:38:40');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0092-select-extract-year-from-timestamp"},
				},
			},
		},
		{
			// The TIMESTAMPTZ value gets converted to Local timezone / server timezone,
			// so set the server timezone to UTC. GitHub CI runs on UTC time zone.
			Name:        "extract from timestamp with time zone",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SET TIMEZONE TO 'UTC';`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0093-set-timezone-to-utc"},
				},
				{
					Query: `SELECT EXTRACT(CENTURY FROM TIMESTAMP WITH TIME ZONE '2001-02-16 12:38:40.12-05');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0094-select-extract-century-from-timestamp"},
				},
				{
					Query: `SELECT EXTRACT(DAY FROM TIMESTAMP WITH TIME ZONE '2001-02-16 12:38:40.12-05');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0095-select-extract-day-from-timestamp"},
				},
				{
					Query: `SELECT EXTRACT(DECADE FROM TIMESTAMP WITH TIME ZONE '2001-02-16 12:38:40.12-05');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0096-select-extract-decade-from-timestamp"},
				},
				{
					Query: `SELECT EXTRACT(DOW FROM TIMESTAMP WITH TIME ZONE '2001-02-16 12:38:40.12-05');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0097-select-extract-dow-from-timestamp"},
				},
				{
					Query: `SELECT EXTRACT(DOY FROM TIMESTAMP WITH TIME ZONE '2001-02-16 12:38:40.12-05');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0098-select-extract-doy-from-timestamp"},
				},
				{
					Query: `SELECT EXTRACT(EPOCH FROM TIMESTAMP WITH TIME ZONE '2001-02-16 12:38:40.12-05');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0099-select-extract-epoch-from-timestamp"},
				},
				{
					Query: `SELECT EXTRACT(HOUR FROM TIMESTAMP WITH TIME ZONE '2001-02-16 12:38:40.12-05');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0100-select-extract-hour-from-timestamp"},
				},
				{
					Query: `SELECT EXTRACT(ISODOW FROM TIMESTAMP WITH TIME ZONE '2001-02-16 12:38:40.12-05');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0101-select-extract-isodow-from-timestamp"},
				},
				{
					Query: `SELECT EXTRACT(ISOYEAR FROM TIMESTAMP WITH TIME ZONE '2001-02-16 12:38:40.12-05');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0102-select-extract-isoyear-from-timestamp"},
				},
				{
					Skip:  true, // TODO: not supported yet
					Query: `SELECT EXTRACT(JULIAN FROM TIMESTAMP WITH TIME ZONE '2001-02-16 12:38:40.12-05');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0103-select-extract-julian-from-timestamp"},
				},
				{
					Skip:  true, // TODO: not supported yet
					Query: `SELECT extract(julian from '2021-06-23 7:00:00-04'::timestamptz at time zone 'UTC+12');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0104-select-extract-julian-from-2021-06-23"},
				},
				{
					Skip:  true, // TODO: not supported yet
					Query: `SELECT extract(julian from '2021-06-23 8:00:00-04'::timestamptz at time zone 'UTC+12');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0105-select-extract-julian-from-2021-06-23"},
				},
				{
					Query: `SELECT EXTRACT(MICROSECONDS FROM TIMESTAMP WITH TIME ZONE '2001-02-16 12:38:40.12-05');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0106-select-extract-microseconds-from-timestamp"},
				},
				{
					Query: `SELECT EXTRACT(MILLENNIUM FROM TIMESTAMP WITH TIME ZONE '2001-02-16 12:38:40.12-05');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0107-select-extract-millennium-from-timestamp"},
				},
				{
					Query: `SELECT EXTRACT(MILLISECONDS FROM TIMESTAMP WITH TIME ZONE '2001-02-16 12:38:40.12-05');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0108-select-extract-milliseconds-from-timestamp"},
				},
				{
					Query: `SELECT EXTRACT(MINUTE FROM TIMESTAMP WITH TIME ZONE '2001-02-16 12:38:40.12-05');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0109-select-extract-minute-from-timestamp"},
				},
				{
					Query: `SELECT EXTRACT(MONTH FROM TIMESTAMP WITH TIME ZONE '2001-02-16 12:38:40.12-05');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0110-select-extract-month-from-timestamp"},
				},
				{
					Query: `SELECT EXTRACT(QUARTER FROM TIMESTAMP WITH TIME ZONE '2001-02-16 12:38:40.12-05');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0111-select-extract-quarter-from-timestamp"},
				},
				{
					Query: `SELECT EXTRACT(SECOND FROM TIMESTAMP WITH TIME ZONE '2001-02-16 12:38:40.12-05');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0112-select-extract-second-from-timestamp"},
				},
				{
					Query: `SELECT EXTRACT(TIMEZONE FROM TIMESTAMP WITH TIME ZONE '2001-02-16 12:38:40.12-05');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0113-select-extract-timezone-from-timestamp"},
				},
				{
					Query: `SELECT EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP WITH TIME ZONE '2001-02-16 12:38:40.12-05');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0114-select-extract-timezone_hour-from-timestamp"},
				},
				{
					Query: `SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP WITH TIME ZONE '2001-02-16 12:38:40.12-05:45');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0115-select-extract-timezone_minute-from-timestamp"},
				},
				{
					Query: `SELECT EXTRACT(WEEK FROM TIMESTAMP WITH TIME ZONE '2001-02-16 12:38:40.12-05');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0116-select-extract-week-from-timestamp"},
				},
				{
					Query: `SELECT EXTRACT(YEAR FROM TIMESTAMP WITH TIME ZONE '2001-02-16 12:38:40.12-05');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0117-select-extract-year-from-timestamp"},
				},
				{
					Query: `SET TIMEZONE TO DEFAULT;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0118-set-timezone-to-default"},
				},
			},
		},
		{
			Name:        "extract from interval",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT EXTRACT(CENTURY FROM INTERVAL '2001 years');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0119-select-extract-century-from-interval"},
				},
				{
					Query: `SELECT EXTRACT(DAY FROM INTERVAL '40 days 1 minute');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0120-select-extract-day-from-interval"},
				},
				{
					Query: `select extract(decades from interval '1000 months');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0121-select-extract-decades-from-interval"},
				},
				{
					Query: `SELECT EXTRACT(EPOCH FROM INTERVAL '5 days 3 hours');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0122-select-extract-epoch-from-interval"},
				},
				{
					Query: `select extract(epoch from interval '10 months 10 seconds');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0123-select-extract-epoch-from-interval"},
				},
				{
					Query: `select extract(hours from interval '10 months 65 minutes 10 seconds');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0124-select-extract-hours-from-interval"},
				},
				{
					Query: `select extract(microsecond from interval '10 months 65 minutes 10 seconds');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0125-select-extract-microsecond-from-interval"},
				},
				{
					Query: `SELECT EXTRACT(MILLENNIUM FROM INTERVAL '2001 years');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0126-select-extract-millennium-from-interval"},
				},
				{
					Query: `select extract(millenniums from interval '3000 years 65 minutes 10 seconds');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0127-select-extract-millenniums-from-interval"},
				},
				{
					Query: `select extract(millisecond from interval '10 months 65 minutes 10 seconds');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0128-select-extract-millisecond-from-interval"},
				},
				{
					Query: `select extract(minutes from interval '10 months 65 minutes 10 seconds');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0129-select-extract-minutes-from-interval"},
				},
				{
					Query: `SELECT EXTRACT(MONTH FROM INTERVAL '2 years 3 months');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0130-select-extract-month-from-interval"},
				},
				{
					Query: `SELECT EXTRACT(MONTH FROM INTERVAL '2 years 13 months');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0131-select-extract-month-from-interval"},
				},
				{
					Query: `select extract(months from interval '20 months 65 minutes 10 seconds');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0132-select-extract-months-from-interval"},
				},
				{
					Query: `select extract(quarter from interval '20 months 65 minutes 10 seconds');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0133-select-extract-quarter-from-interval"},
				},
				{
					Query: `select extract(seconds from interval '65 minutes 10 seconds 5 millisecond');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0134-select-extract-seconds-from-interval"},
				},
				{
					Query: `select extract(years from interval '20 months 65 minutes 10 seconds');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0135-select-extract-years-from-interval"},
				},
			},
		},
		{
			Name:        "age",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT age(timestamp '2001-04-10', timestamp '1957-06-13');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0136-select-age-timestamp-2001-04-10-timestamp"},
				},
				{
					Query: `SELECT age(timestamp '1957-06-13', timestamp '2001-04-10');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0137-select-age-timestamp-1957-06-13-timestamp"},
				},
				{
					Query: `SELECT age(timestamp '2001-06-13', timestamp '2001-04-10');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0138-select-age-timestamp-2001-06-13-timestamp"},
				},
				{
					Query: `SELECT age(timestamp '2001-04-10', timestamp '2001-06-13');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0139-select-age-timestamp-2001-04-10-timestamp"},
				},
				{
					Query: `SELECT age(timestamp '2001-04-10 12:23:33', timestamp '1957-06-13 13:23:34.4');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0140-select-age-timestamp-2001-04-10-12:23:33"},
				},
				{
					Query: `SELECT age(timestamp '1957-06-13 13:23:34.4', timestamp '2001-04-10 12:23:33');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0141-select-age-timestamp-1957-06-13-13:23:34.4"},
				},
				{
					Query: `SELECT age(current_date);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0142-select-age-current_date"},
				},
				{
					Query: `SELECT age(current_date::timestamp);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0143-select-age-current_date::timestamp"},
				},
			},
		},
		{
			Name:        "timezone",
			SetUpScript: []string{`SET timezone = '+06:30'`},
			Assertions: []ScriptTestAssertion{
				{
					Query: `select timezone(interval '2 minutes', timestamp with time zone '2001-02-16 20:38:40.12-05');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0144-select-timezone-interval-2-minutes"},
				},
				{
					Query: `select timezone('UTC', timestamp with time zone '2001-02-16 20:38:40.12-05');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0145-select-timezone-utc-timestamp-with"},
				},
				{
					Query: `select timezone('-04:45', time with time zone '20:38:40.12-05');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0146-select-timezone-04:45-time-with"},
				},
				{
					Query: `select timezone(interval '2 hours 2 minutes', time with time zone '20:38:40.12-05');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0147-select-timezone-interval-2-hours"},
				},
				{
					Query: `select timezone('-04:45', timestamp '2001-02-16 20:38:40.12');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0148-select-timezone-04:45-timestamp-2001-02-16"},
				},
				{
					Query: `select timezone('-04:45:44', timestamp '2001-02-16 20:38:40.12');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0149-select-timezone-04:45:44-timestamp-2001-02-16"},
				},
				{
					Query: `select '2001-02-16 20:38:40.12'::timestamp at time zone '-04:45:44';`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0150-select-2001-02-16-20:38:40.12-::timestamp-at"},
				},
				{
					Query: `select timezone(interval '2 hours 2 minutes', timestamp '2001-02-16 20:38:40.12');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0151-select-timezone-interval-2-hours"},
				},
				{
					Query: `select '2024-08-22 14:47:57 -07' at time zone 'utc';`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0152-select-2024-08-22-14:47:57-07-at"},
				},
				{
					Query: `select round(extract(epoch from '2024-08-22 13:47:57-07' at time zone 'UTC')) as startup_time;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0153-select-round-extract-epoch-from"},
				},
				{
					Query: `select timestamptz '2024-08-22 13:47:57-07' at time zone 'utc';`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0154-select-timestamptz-2024-08-22-13:47:57-07-at"},
				},
				{
					Query: `select timestamp '2024-08-22 13:47:57-07';`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0155-select-timestamp-2024-08-22-13:47:57-07"},
				},
				{
					Query: `select timestamp '2024-08-22 13:47:57-07' at time zone 'utc';`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0156-select-timestamp-2024-08-22-13:47:57-07-at"},
				},
				{
					Query: `select '2011-03-27 02:00:00'::timestamp at time zone '+01:00';`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0157-select-2011-03-27-02:00:00-::timestamp-at"},
				},
				{
					Query: `select '2011-03-27 02:00:00'::timestamp at time zone 'UTC';`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0158-select-2011-03-27-02:00:00-::timestamp-at"},
				},
				{
					Query: `select timezone('MSK', timestamp '2011-03-27 02:00:00');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0159-select-timezone-msk-timestamp-2011-03-27"},
				},
				{
					Query: `select '2011-03-27 02:00:00'::timestamp at time zone 'MSK';`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0160-select-2011-03-27-02:00:00-::timestamp-at"},
				},
			},
		},
		{
			Name: "date_part",
			Assertions: []ScriptTestAssertion{
				{
					Query: `select date_part('month', date '2001-02-16');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0161-select-date_part-month-date-2001-02-16"},
				},
				{
					Query: `select date_part('minute', time without time zone '20:38:40');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0162-select-date_part-minute-time-without"},
				},
				{
					Query: `select date_part('second', time with time zone '20:38:40 UTC');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0163-select-date_part-second-time-with"},
				},
				{
					Query: `select date_part('year', timestamp without time zone '2001-02-16 20:38:40');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0164-select-date_part-year-timestamp-without"},
				},
				{
					Query: `select date_part('day', timestamp with time zone '2001-02-16 20:38:40 UTC');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0165-select-date_part-day-timestamp-with"},
				},
				{
					Query: `select date_part('month', interval '2 years 3 months');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0166-select-date_part-month-interval-2"},
				},
			},
		},
		{
			Name: "date_trunc",
			Assertions: []ScriptTestAssertion{
				{
					Query: `select date_trunc('hour', timestamp '2001-02-16 20:38:40');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0167-select-date_trunc-hour-timestamp-2001-02-16"},
				},
				{
					Query: `SET timezone to '+06:30';`,
				},
				{
					Query: `select date_trunc('day', timestamp with time zone '2001-02-16 20:38:40 UTC');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0168-select-date_trunc-day-timestamp-with"},
				},
				{
					Query: `select date_trunc('day', timestamp with time zone '2001-02-16 20:38:40 UTC', '-07:00');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0169-select-date_trunc-day-timestamp-with"},
				},
				{
					Query: `SET timezone to '+06:30';`,
				},
				{
					Query: `select date_trunc('hour', interval '2 days 10 hours 30 minutes');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0170-select-date_trunc-hour-interval-2"},
				},
			},
		},
		{
			Name: "to_date",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT to_date('1 4 1902', 'Q MM YYYY');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0171-select-to_date-1-4-1902"},
				},
				{
					Query: `SELECT to_date('3 4 21 01', 'W MM CC YY');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0172-select-to_date-3-4-21"},
				},
				{
					Query: `SELECT to_date('2458872', 'J');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0173-select-to_date-2458872-j"},
				},
				{
					Query: `SELECT to_date('44-02-01 BC','YYYY-MM-DD BC');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0174-select-to_date-44-02-01-bc-yyyy-mm-dd"},
				},
				{
					Query: `SELECT to_date('-44-02-01','YYYY-MM-DD');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0175-select-to_date-44-02-01-yyyy-mm-dd"},
				},
				{
					Query: `SELECT to_date('2011x 12x 18', 'YYYYxMMxDD');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0176-select-to_date-2011x-12x-18"},
				},
				{
					Query: `SELECT to_date('2015 365', 'YYYY DDD');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0177-select-to_date-2015-365-yyyy"},
				},
			},
		},
		{
			Name: "to_timestamp",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SET timezone to '+06:30';`,
				},
				{
					Query: `SELECT to_timestamp('2011-12-18 23:38:15', 'YYYY-MM-DD HH24:MI:SS');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0178-select-to_timestamp-2011-12-18-23:38:15-yyyy-mm-dd"},
				},
				{
					Query: `SELECT to_timestamp('2000-01-01 12:30:45', 'YYYY-MM-DD HH24:MI:SS');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0179-select-to_timestamp-2000-01-01-12:30:45-yyyy-mm-dd"},
				},
				{
					Query: `SELECT to_timestamp('0097/Feb/16 --> 08:14:30', 'YYYY/Mon/DD --> HH:MI:SS');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0180-select-to_timestamp-0097/feb/16->-08:14:30"},
				},
				{
					Query: `SELECT to_timestamp('97/2/16 8:14:30', 'FMYYYY/FMMM/FMDD FMHH:FMMI:FMSS');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0181-select-to_timestamp-97/2/16-8:14:30-fmyyyy/fmmm/fmdd"},
				},
				{
					Query: `SELECT to_timestamp('2011$03!18 23_38_15', 'YYYY-MM-DD HH24:MI:SS');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0182-select-to_timestamp-2011$03!18-23_38_15-yyyy-mm-dd"},
				},
				{
					Query: `SELECT to_timestamp('1985 January 12', 'YYYY FMMonth DD');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0183-select-to_timestamp-1985-january-12"},
				},
				{
					Query: `SELECT to_timestamp('1985 FMMonth 12', 'YYYY "FMMonth" DD');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0184-select-to_timestamp-1985-fmmonth-12"},
				},
				{
					Query: `SELECT to_timestamp('1985 \\ 12', 'YYYY \\\\ DD');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0185-select-to_timestamp-1985-\\\\-12"},
				},
				{
					Query: `SELECT to_timestamp('My birthday-> Year: 1976, Month: May, Day: 16', '"My birthday-> Year:" YYYY, "Month:" FMMonth, "Day:" DD');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0186-select-to_timestamp-my-birthday->-year:"},
				},
				{
					Query: `SELECT to_timestamp('1,582nd VIII 21', 'Y,YYYth FMRM DD');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0187-select-to_timestamp-1-582nd-viii"},
				},
				{
					Query: `SELECT to_timestamp('15 "text between quote marks" 98 54 45',
				  E'HH24 "\\"text between quote marks\\"" YY MI SS');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0188-select-to_timestamp-15-text-between"},
				},
				{
					Query: `SELECT to_timestamp('05121445482000', 'MMDDHH24MISSYYYY');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0189-select-to_timestamp-05121445482000-mmddhh24missyyyy"},
				},
				{
					Query: `SELECT to_timestamp('2000January09Sunday', 'YYYYFMMonthDDFMDay');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0190-select-to_timestamp-2000january09sunday-yyyyfmmonthddfmday"},
				},
				{
					Query: `SELECT to_timestamp('97/Feb/16', 'YYMonDD');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0191-select-to_timestamp-97/feb/16-yymondd", Compare: "sqlstate"},
				},
				{
					Query: `SELECT to_timestamp('97/Feb/16', 'YY:Mon:DD');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0192-select-to_timestamp-97/feb/16-yy:mon:dd"},
				},
				{
					Query: `SELECT to_timestamp('97/Feb/16', 'FXYY:Mon:DD');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0193-select-to_timestamp-97/feb/16-fxyy:mon:dd"},
				},
				{
					Query: `SELECT to_timestamp('97/Feb/16', 'FXYY/Mon/DD');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0194-select-to_timestamp-97/feb/16-fxyy/mon/dd"},
				},
				{
					Query: `SELECT to_timestamp('19971116', 'YYYYMMDD');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0195-select-to_timestamp-19971116-yyyymmdd"},
				},
				{
					// TODO: this test passes but time library parsing does not allow year length to be more than 4
					//  the using pgx library for tests relies on it.
					// https://github.com/jackc/pgx/blob/master/pgtype/timestamptz.go#L312
					Skip:     true,
					Query:    `SELECT to_timestamp('20000-1116', 'FXYYYY-MMDD');`,
					Expected: []sql.Row{{"20000-11-16 00:00:00-06:30"}},
				},
				{
					Query: `SELECT to_timestamp('1997 AD 11 16', 'YYYY BC MM DD');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0197-select-to_timestamp-1997-ad-11"},
				},
				{
					Query: `SELECT to_timestamp('1997 BC 11 16', 'YYYY BC MM DD');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0198-select-to_timestamp-1997-bc-11"},
				},
				{
					Query: `SELECT to_timestamp('1997 A.D. 11 16', 'YYYY B.C. MM DD');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0199-select-to_timestamp-1997-a.d.-11"},
				},
				{
					Query: `SELECT to_timestamp('1997 B.C. 11 16', 'YYYY B.C. MM DD');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0200-select-to_timestamp-1997-b.c.-11"},
				},
				{
					Query: `SELECT to_timestamp('9-1116', 'Y-MMDD');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0201-select-to_timestamp-9-1116-y-mmdd"},
				},
				{
					Query: `SELECT to_timestamp('95-1116', 'YY-MMDD');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0202-select-to_timestamp-95-1116-yy-mmdd"},
				},
				{
					Query: `SELECT to_timestamp('995-1116', 'YYY-MMDD');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0203-select-to_timestamp-995-1116-yyy-mmdd"},
				},
				{
					Query: `SELECT to_timestamp('2005426', 'YYYYWWD');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0204-select-to_timestamp-2005426-yyyywwd"},
				},
				{
					Query: `SELECT to_timestamp('2005300', 'YYYYDDD');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0205-select-to_timestamp-2005300-yyyyddd"},
				},
				{
					Query: `SELECT to_timestamp('2005527', 'IYYYIWID');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0206-select-to_timestamp-2005527-iyyyiwid"},
				},
				{
					Query: `SELECT to_timestamp('005527', 'IYYIWID');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0207-select-to_timestamp-005527-iyyiwid"},
				},
				{
					Query: `SELECT to_timestamp('05527', 'IYIWID');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0208-select-to_timestamp-05527-iyiwid"},
				},
				{
					Query: `SELECT to_timestamp('5527', 'IIWID');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0209-select-to_timestamp-5527-iiwid"},
				},
				{
					Query: `SELECT to_timestamp('2005364', 'IYYYIDDD');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0210-select-to_timestamp-2005364-iyyyiddd"},
				},
				{
					Query: `SELECT to_timestamp('20050302', 'YYYYMMDD');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0211-select-to_timestamp-20050302-yyyymmdd"},
				},
				{
					Query: `SELECT to_timestamp('2005 03 02', 'YYYYMMDD');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0212-select-to_timestamp-2005-03-02"},
				},
				{
					Query: `SELECT to_timestamp(' 2005 03 02', 'YYYYMMDD');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0213-select-to_timestamp-2005-03-02"},
				},
				{
					Query: `SELECT to_timestamp('  20050302', 'YYYYMMDD');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0214-select-to_timestamp-20050302-yyyymmdd"},
				},
				{
					Query: `SELECT to_timestamp('2011-12-18 11:38 AM', 'YYYY-MM-DD HH12:MI PM');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0215-select-to_timestamp-2011-12-18-11:38-am"},
				},
				{
					Query: `SELECT to_timestamp('2011-12-18 11:38 PM', 'YYYY-MM-DD HH12:MI PM');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0216-select-to_timestamp-2011-12-18-11:38-pm"},
				},
				{
					Query: `SELECT to_timestamp('2011-12-18 11:38 A.M.', 'YYYY-MM-DD HH12:MI P.M.');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0217-select-to_timestamp-2011-12-18-11:38-a.m."},
				},
				{
					Query: `SELECT to_timestamp('2011-12-18 11:38 P.M.', 'YYYY-MM-DD HH12:MI P.M.');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0218-select-to_timestamp-2011-12-18-11:38-p.m."},
				},
				{
					Query: `SELECT to_timestamp('2011-12-18 11:38 +05', 'YYYY-MM-DD HH12:MI TZH');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0219-select-to_timestamp-2011-12-18-11:38-+05"},
				},
				{
					Query: `SELECT to_timestamp('2011-12-18 11:38 -05', 'YYYY-MM-DD HH12:MI TZH');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0220-select-to_timestamp-2011-12-18-11:38-05"},
				},
				{
					Query: `SELECT to_timestamp('2011-12-18 11:38 +05:20', 'YYYY-MM-DD HH12:MI TZH:TZM');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0221-select-to_timestamp-2011-12-18-11:38-+05:20"},
				},
				{
					Query: `SELECT to_timestamp('2011-12-18 11:38 -05:20', 'YYYY-MM-DD HH12:MI TZH:TZM');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0222-select-to_timestamp-2011-12-18-11:38-05:20"},
				},
				{
					Query: `SELECT to_timestamp('2011-12-18 11:38 20', 'YYYY-MM-DD HH12:MI TZM');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0223-select-to_timestamp-2011-12-18-11:38-20"},
				},
				{
					Skip:  true, // TODO: support formatting TZ
					Query: `SELECT to_timestamp('2011-12-18 11:38 PST', 'YYYY-MM-DD HH12:MI TZ');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0224-select-to_timestamp-2011-12-18-11:38-pst", Compare: "sqlstate"},
				},
				{
					Query: `SELECT to_timestamp('2018-11-02 12:34:56.025', 'YYYY-MM-DD HH24:MI:SS.MS');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0225-select-to_timestamp-2018-11-02-12:34:56.025-yyyy-mm-dd"},
				},
				{
					Query: `SELECT to_timestamp('44-02-01 11:12:13 BC','YYYY-MM-DD HH24:MI:SS BC');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0226-select-to_timestamp-44-02-01-11:12:13-bc"},
				},
				{
					Query: `SELECT to_timestamp('-44-02-01 11:12:13','YYYY-MM-DD HH24:MI:SS');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0227-select-to_timestamp-44-02-01-11:12:13-yyyy-mm-dd"},
				},
				{
					Query: `SELECT to_timestamp('-44-02-01 11:12:13 BC','YYYY-MM-DD HH24:MI:SS BC');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0228-select-to_timestamp-44-02-01-11:12:13-bc"},
				},
				{
					Query: `SELECT to_timestamp('2011-12-18 23:38:15', 'YYYY-MM-DD  HH24:MI:SS');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0229-select-to_timestamp-2011-12-18-23:38:15-yyyy-mm-dd"},
				},
				{
					Query: `SELECT to_timestamp('2011-12-18  23:38:15', 'YYYY-MM-DD  HH24:MI:SS');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0230-select-to_timestamp-2011-12-18-23:38:15-yyyy-mm-dd"},
				},
				{
					Query: `SELECT to_timestamp('2011-12-18   23:38:15', 'YYYY-MM-DD  HH24:MI:SS');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0231-select-to_timestamp-2011-12-18-23:38:15-yyyy-mm-dd"},
				},
				{
					Query: `SELECT to_timestamp('2011-12-18  23:38:15', 'YYYY-MM-DD HH24:MI:SS');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0232-select-to_timestamp-2011-12-18-23:38:15-yyyy-mm-dd"},
				},
				{
					Query: `SELECT to_timestamp('2011-12-18  23:38:15', 'YYYY-MM-DD  HH24:MI:SS');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0233-select-to_timestamp-2011-12-18-23:38:15-yyyy-mm-dd"},
				},
				{
					Query: `SELECT to_timestamp('2011-12-18  23:38:15', 'YYYY-MM-DD   HH24:MI:SS');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0234-select-to_timestamp-2011-12-18-23:38:15-yyyy-mm-dd"},
				},
				{
					Query: `SELECT to_timestamp('2000+   JUN', 'YYYY/MON');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0235-select-to_timestamp-2000+-jun-yyyy/mon"},
				},
				{
					Query: `SELECT to_timestamp('  2000 +JUN', 'YYYY/MON');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0236-select-to_timestamp-2000-+jun-yyyy/mon"},
				},
				{
					Query: `SELECT to_timestamp(' 2000 +JUN', 'YYYY//MON');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0237-select-to_timestamp-2000-+jun-yyyy//mon"},
				},
				{
					Query: `SELECT to_timestamp('2000  +JUN', 'YYYY//MON');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0238-select-to_timestamp-2000-+jun-yyyy//mon"},
				},
				{
					Query: `SELECT to_timestamp('2000 + JUN', 'YYYY MON');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0239-select-to_timestamp-2000-+-jun"},
				},
				{
					Query: `SELECT to_timestamp('2000 ++ JUN', 'YYYY  MON');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0240-select-to_timestamp-2000-++-jun"},
				},
				{
					Query: `SELECT to_timestamp('2000 + - JUN', 'YYYY  MON');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0241-select-to_timestamp-2000-+-jun", Compare: "sqlstate"},
				},
				{
					Query: `SELECT to_timestamp('2000 + + JUN', 'YYYY   MON');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0242-select-to_timestamp-2000-+-+"},
				},
				{
					Query: `SELECT to_timestamp('2000 -10', 'YYYY TZH');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0243-select-to_timestamp-2000-10-yyyy"},
				},
				{
					Query: `SELECT to_timestamp('2000 -10', 'YYYY  TZH');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0244-select-to_timestamp-2000-10-yyyy"},
				},
				{
					Query: `SELECT to_timestamp('2005527', 'YYYYIWID');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0245-select-to_timestamp-2005527-yyyyiwid", Compare: "sqlstate"},
				},
				{
					Query: `SELECT to_timestamp('19971', 'YYYYMMDD');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0246-select-to_timestamp-19971-yyyymmdd", Compare: "sqlstate"},
				},
				{
					Query: `SELECT to_timestamp('19971)24', 'YYYYMMDD');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0247-select-to_timestamp-19971-24-yyyymmdd", Compare: "sqlstate"},
				},
				{
					Query: `SELECT to_timestamp('Friday 1-January-1999', 'DY DD MON YYYY');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0248-select-to_timestamp-friday-1-january-1999-dy", Compare: "sqlstate"},
				},
				{
					Query: `SELECT to_timestamp('Fri 1-January-1999', 'DY DD MON YYYY');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0249-select-to_timestamp-fri-1-january-1999-dy", Compare: "sqlstate"},
				},
				{
					Query: `SELECT to_timestamp('Fri 1-Jan-1999', 'DY DD MON YYYY');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0250-select-to_timestamp-fri-1-jan-1999-dy"},
				},
				{
					Query: `SELECT to_timestamp('1997-11-Jan-16', 'YYYY-MM-Mon-DD');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0251-select-to_timestamp-1997-11-jan-16-yyyy-mm-mon-dd", Compare: "sqlstate"},
				},
				{
					Query: `SELECT to_timestamp('199711xy', 'YYYYMMDD');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0252-select-to_timestamp-199711xy-yyyymmdd", Compare: "sqlstate"},
				},
				{
					Query: `SELECT to_timestamp('10000000000', 'FMYYYY');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0253-select-to_timestamp-10000000000-fmyyyy", Compare: "sqlstate"},
				},
				{
					Query: `SELECT to_timestamp('2016-06-13 25:00:00', 'YYYY-MM-DD HH24:MI:SS');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0254-select-to_timestamp-2016-06-13-25:00:00-yyyy-mm-dd", Compare: "sqlstate"},
				},
				{
					Query: `SELECT to_timestamp('2016-06-13 15:60:00', 'YYYY-MM-DD HH24:MI:SS');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0255-select-to_timestamp-2016-06-13-15:60:00-yyyy-mm-dd", Compare: "sqlstate"},
				},
				{
					Query: `SELECT to_timestamp('2016-06-13 15:50:60', 'YYYY-MM-DD HH24:MI:SS');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0256-select-to_timestamp-2016-06-13-15:50:60-yyyy-mm-dd", Compare: "sqlstate"},
				},
				{
					Query: `SELECT to_timestamp('2016-06-13 15:50:55', 'YYYY-MM-DD HH24:MI:SS');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0257-select-to_timestamp-2016-06-13-15:50:55-yyyy-mm-dd"},
				},
				{
					Query: `SELECT to_timestamp('2016-06-13 15:50:55', 'YYYY-MM-DD HH:MI:SS');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0258-select-to_timestamp-2016-06-13-15:50:55-yyyy-mm-dd", Compare: "sqlstate"},
				},
				{
					Query: `SELECT to_timestamp('2016-13-01 15:50:55', 'YYYY-MM-DD HH24:MI:SS');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0259-select-to_timestamp-2016-13-01-15:50:55-yyyy-mm-dd", Compare: "sqlstate"},
				},
				{
					Query: `SELECT to_timestamp('2016-02-30 15:50:55', 'YYYY-MM-DD HH24:MI:SS');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0260-select-to_timestamp-2016-02-30-15:50:55-yyyy-mm-dd", Compare: "sqlstate"},
				},
				{
					Query: `SELECT to_timestamp('2016-02-29 15:50:55', 'YYYY-MM-DD HH24:MI:SS');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0261-select-to_timestamp-2016-02-29-15:50:55-yyyy-mm-dd"},
				},
				{
					Query: `SELECT to_timestamp('2015-02-29 15:50:55', 'YYYY-MM-DD HH24:MI:SS');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0262-select-to_timestamp-2015-02-29-15:50:55-yyyy-mm-dd", Compare: "sqlstate"},
				},
				{
					Query: `SELECT to_timestamp('2015-02-11 86000', 'YYYY-MM-DD SSSS');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0263-select-to_timestamp-2015-02-11-86000-yyyy-mm-dd"},
				},
				{
					Query: `SELECT to_timestamp('2015-02-11 86400', 'YYYY-MM-DD SSSS');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0264-select-to_timestamp-2015-02-11-86400-yyyy-mm-dd", Compare: "sqlstate"},
				},
				{
					Query: `SELECT to_timestamp('2015-02-11 86000', 'YYYY-MM-DD SSSSS');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0265-select-to_timestamp-2015-02-11-86000-yyyy-mm-dd"},
				},
				{
					Query: `SELECT to_timestamp('2015-02-11 86400', 'YYYY-MM-DD SSSSS');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0266-select-to_timestamp-2015-02-11-86400-yyyy-mm-dd", Compare: "sqlstate"},
				},
				{
					Query: `SET timezone to '+06:30';`,
				},
			},
		},
		{
			Name: "current_time and now functions",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT now()::timetz::text = current_time()::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0267-select-now-::timetz::text-=-current_time",

						// TODO: support precision
						Compare: "sqlstate"},
				},
				{
					Skip:  true,
					Query: `SELECT now()::timetz(4)::text = current_time(5)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0268-select-now-::timetz-4-::text"},
				},
				{
					Query:    `SELECT length(to_char(current_date + 'now'::timetz, 'HH24:MI:SS.USTZH:TZM'));`,
					Expected: []sql.Row{{int32(21)}},
				},
				{
					Query: `SELECT length('now'::timetz::text) > length('now'::time::text);`, PostgresOracle:
					// Direct ::text casts trim trailing zeros from microseconds, making the length variable.
					ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0270-select-length-now-::timetz::text->"},
				},
				{
					Query:    `SELECT length(to_char('now'::time, 'HH24:MI:SS.US'));`,
					Expected: []sql.Row{{int32(15)}},
				},
			},
		},
		{
			Name: "make_timestamp",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT make_timestamp(2014, 12, 28, 6, 30, 45.887);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0272-select-make_timestamp-2014-12-28"},
				},
				{
					Query: `SELECT make_timestamp(-44, 3, 15, 12, 30, 15);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0273-select-make_timestamp-44-3-15"},
				},
				{
					Query: `SELECT make_timestamp(-1, 3, 15, 12, 30, 15);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0274-select-make_timestamp-1-3-15"},
				},
				{
					Query: `select make_timestamp(0, 7, 15, 12, 30, 15);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0275-select-make_timestamp-0-7-15", Compare: "sqlstate"},
				},
				{
					Query: `select make_timestamp(2000, 0, 15, 12, 30, 15);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0276-select-make_timestamp-2000-0-15", Compare: "sqlstate"},
				},
				{
					Query: `select make_timestamp(2000, 7, 32, 12, 30, 15);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0277-select-make_timestamp-2000-7-32", Compare: "sqlstate"},
				},
				{
					Query: `select make_timestamp(2000, 7, 15, 25, 30, 15);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0278-select-make_timestamp-2000-7-15", Compare: "sqlstate"},
				},
				{
					Query: `select make_timestamp(2000, 7, 15, 2, 61, 15);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0279-select-make_timestamp-2000-7-15", Compare: "sqlstate"},
				},
				{
					Query: `select make_timestamp(2000, 7, 15, 25, 30, 61);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0280-select-make_timestamp-2000-7-15", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "make_timestamptz",
			SetUpScript: []string{
				`SET timezone = '+06:30'`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT make_timestamptz(2014, 12, 28, 6, 30, 45.887);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0281-select-make_timestamptz-2014-12-28"},
				},
				{
					Query: `SELECT make_timestamptz(-44, 3, 15, 12, 30, 15);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0282-select-make_timestamptz-44-3-15"},
				},
				{
					Query: `SELECT make_timestamptz(-1, 3, 15, 12, 30, 15);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0283-select-make_timestamptz-1-3-15"},
				},
				{
					Query: `select make_timestamptz(0, 7, 15, 12, 30, 15);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0284-select-make_timestamptz-0-7-15", Compare: "sqlstate"},
				},
				{
					Query: `SELECT make_timestamptz(1910, 12, 24, 0, 0, 0, 'Nehwon/Lankhmar');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0285-select-make_timestamptz-1910-12-24", Compare: "sqlstate"},
				},
				{
					Query: `SELECT make_timestamptz(1881, 12, 10, 0, 0, 0, 'Europe/Paris') AT TIME ZONE 'UTC';`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0286-select-make_timestamptz-1881-12-10"},
				},
				{
					Query: `SELECT make_timestamptz(2008, 12, 10, 10, 10, 10, 'EST');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0287-select-make_timestamptz-2008-12-10"},
				},
			},
		},
		{
			Name: "date_bin",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT date_bin('5 min'::interval, timestamp '2020-02-01 01:01:01', timestamp '2020-02-01 00:02:30');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0288-select-date_bin-5-min-::interval"},
				},
				{
					Query: `SELECT date_bin('5 months'::interval, timestamp '2020-02-01 01:01:01', timestamp '2001-01-01');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0289-select-date_bin-5-months-::interval", Compare: "sqlstate"},
				},
				{
					Query: `SELECT date_bin('0 days'::interval, timestamp '1970-01-01 01:00:00' , timestamp '1970-01-01 00:00:00');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testdateandtimefunction-0290-select-date_bin-0-days-::interval", Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestStringFunction(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "use name type for text type input",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT ascii('name'::name)`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0001-select-ascii-name-::name"},
				},
				{
					Query: `SELECT ascii(''::text), ascii('åctive'::text);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0002-select-ascii-::text-ascii-åctive"},
				},
				{
					Query: "SELECT bit_length('name'::name);", PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0003-select-bit_length-name-::name"},
				},
				{
					Query: "SELECT btrim(' name  '::name);", PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0004-select-btrim-name-::name"},
				},
				{
					Query: "SELECT initcap('name'::name);", PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0005-select-initcap-name-::name"},
				},
				{
					Query: "SELECT initcap('admin user'::text);", PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0006-select-initcap-admin-user-::text"},
				},
				{
					Query: "SELECT left('name'::name, 2);", PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0007-select-left-name-::name-2"},
				},
				{
					Query: "SELECT left('åctive'::text, 1), left('åctive'::text, -5), right('åctive'::text, 5), right('åctive'::text, -1);", PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0008-select-left-åctive-::text-1"},
				},
				{
					Query: "SELECT length('name'::name);", PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0009-select-length-name-::name"},
				},
				{
					Query: "SELECT lower('naMe'::name);", PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0010-select-lower-name-::name"},
				},
				{
					Query: "SELECT lpad('name'::name, 7, '*');", PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0011-select-lpad-name-::name-7"},
				},
				{
					Query: "SELECT repeat('Pg'::text, 4), repeat('Pg'::text, 0), repeat('Pg'::text, -4);", PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0012-select-repeat-pg-::text-4"},
				},
				{
					Query: "SELECT starts_with('profile', 'p'), starts_with('profile', 'x'), starts_with('profile', ''), starts_with(NULL::text, 'p');", PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0013-select-starts_with-profile-p-starts_with"},
				},
			},
		},
		{
			Name:        "quote_ident",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `select quote_ident('hi"bye');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0014-select-quote_ident-hi-bye"},
				},
				{
					Query: `select quote_ident('hi""bye');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0015-select-quote_ident-hi-bye"},
				},
				{
					Query: `select quote_ident('hi"""bye');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0016-select-quote_ident-hi-bye"},
				},
				{
					Query: `select quote_ident('hi"b"ye');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0017-select-quote_ident-hi-b-ye"},
				},
				{
					Query: `select quote_ident('admin user'), quote_ident('select');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0018-select-quote_ident-admin-user-quote_ident"},
				},
			},
		},
		{
			Name:        "quote_literal",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `select quote_literal('O''Reilly');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0019-select-quote_literal-o-reilly"},
				},
				{
					Query: `select quote_literal('admin user');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0020-select-quote_literal-admin-user"},
				},
			},
		},
		{
			Name:        "format",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT format('Hello %s, %1$s', 'World');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0021-select-format-hello-%s-%1$s"},
				},
				{
					Query: `SELECT format('Testing %s, %s, %s, %%', 'one', 'two', 'three');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0022-select-format-testing-%s-%s"},
				},
				{
					Query: `SELECT format('INSERT INTO %I VALUES(%L)', 'Foo bar', 'O''Reilly');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0023-select-format-insert-into-%i"},
				},
				{
					Query: `SELECT format('ALTER TABLE %I ADD COLUMN %I text', 'accounts', 'display name');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0024-select-format-alter-table-%i"},
				},
				{
					Query: `SELECT format('%3$s, %2$s, %s', 'one', 'two', 'three');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0025-select-format-%3$s-%2$s-%s"},
				},
				{
					Query: `SELECT format('|%10s|%-10s|', 'left', 'right');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0026-select-format-|%10s|%-10s|-left-right"},
				},
				{
					Query: `SELECT format('%s/%I/%L', NULL::text, 'needs space', NULL::text);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0027-select-format-%s/%i/%l-null::text-needs"},
				},
			},
		},
		{
			Name:        "translate",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `select translate('12345', '143', 'ax');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0028-select-translate-12345-143-ax"},
				},
				{
					Query: `select translate('12345', '143', 'axs');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0029-select-translate-12345-143-axs"},
				},
				{
					Query: `select translate('12345', '143', 'axsl');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0030-select-translate-12345-143-axsl"},
				},
				{
					Query: `select translate('こんにちは', 'ん', 'a');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0031-select-translate-こんにちは-ん-a"},
				},
			},
		},
		{
			Name:        "reverse",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT reverse('Admin'), reverse('åctive');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0032-select-reverse-admin-reverse-åctive"},
				},
			},
		},
		{
			Name:        "split_part",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT split_part('first@example.com', '@', 2);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0033-select-split_part-first@example.com-@-2"},
				},
				{
					Query: `SELECT split_part('a/b/c', '/', -1), split_part('a/b/c', '/', 4);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0034-select-split_part-a/b/c-/-1"},
				},
				{
					Query: `SELECT split_part('abc', '', 1), split_part('abc', '', -1);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0035-select-split_part-abc-1-split_part"},
				},
				{
					Query: `SELECT split_part('a/b/c', '/', 0);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0036-select-split_part-a/b/c-/-0", Compare: "sqlstate"},
				},
			},
		},
		{
			Name:        "substring with integer arg",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT substr('hello', 2)`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0037-select-substr-hello-2"},
				},
				{
					Query: `SELECT substring('hello', 2)`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0038-select-substring-hello-2"},
				},
			},
		},
		{
			Name:        "substring with integer args",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT substr('hello', 2, 3)`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0039-select-substr-hello-2-3"},
				},
				{
					Query: `SELECT substring('hello', 2, 3)`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0040-select-substring-hello-2-3"},
				},
				{
					Query: `SELECT substring('åctive', 1, 1), substr('åctive', 2, 3);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0041-select-substring-åctive-1-1"},
				},
			},
		},
		{
			Name:        "substring with integer args, expanded form",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT substr('hello' from 2 for 3)`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0042-select-substr-hello-from-2", Compare: "sqlstate"},
				},
				{
					Query: `SELECT substring('hello' from 2 for 3)`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0043-select-substring-hello-from-2"},
				},
				{
					Query: `SELECT substr('hello' from 2)`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0044-select-substr-hello-from-2", Compare: "sqlstate"},
				},
				{
					Query: `SELECT substring('hello' from 2)`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0045-select-substring-hello-from-2"},
				},
				{
					Query: `SELECT substr('hello' for 3)`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0046-select-substr-hello-for-3", Compare: "sqlstate"},
				},
				{
					Query: `SELECT substring('hello' for 3)`,
					Skip:  true, PostgresOracle: // ERROR: function substring(unknown, bigint, integer) does not exist
					ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0047-select-substring-hello-for-3"},
				},
			},
		},
		{
			Name:        "substring with regex",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT substring('hello', 'l+')", PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0048-select-substring-hello-l+"},
				},
				{
					Query: "SELECT substring('hello' FROM 'l+')", PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0049-select-substring-hello-from-l+"},
				},
				{
					Query: `SELECT substring('hello.' similar 'hello#.' escape '#')`,
					Skip:  true, PostgresOracle: // syntax error
					ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0050-select-substring-hello.-similar-hello#."},
				},
				{
					Query: `SELECT substring('Thomas' similar '%#"o_a#"_' escape '#')`,
					Skip:  true, PostgresOracle: // syntax error
					ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0051-select-substring-thomas-similar-%#"},
				},
			},
		},
		{
			Name: "string_agg",
			SetUpScript: []string{
				"CREATE TABLE test (pk INT primary key, v1 INT, v2 TEXT);",
				"INSERT INTO test VALUES (1, 1, 'a'), (2, 2, 'b'), (3, 3, 'c'), (4, 4, 'd'), (5, 5, 'e');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT string_agg(v1::text, ',') FROM test;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0052-select-string_agg-v1::text-from-test"},
				},
				{
					Query: `SELECT string_agg(v2, '|') FROM test;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0053-select-string_agg-v2-|-from"},
				},
				{
					Query: `SELECT STRING_AGG(concat(v1::text, v2), ' * ') FROM test;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0054-select-string_agg-concat-v1::text-v2"},
				},
				{
					Skip:  true, // can't use expressions for separator because GROUP_CONCAT can't at the moment
					Query: `SELECT STRING_agg(concat(v1::text, v2), CONCAT(' *', ' ') ORDER BY V1 DESC) FROM test;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0055-select-string_agg-concat-v1::text-v2"},
				},
				{
					Query: `SELECT STRING_AGG(v2, '*', v1) FROM test;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0056-select-string_agg-v2-*-v1", Compare: "sqlstate"},
				},
				{
					Query: `SELECT STRING_AGG(v2) FROM test;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0057-select-string_agg-v2-from-test", Compare: "sqlstate"},
				},
				{
					Query: `SELECT STRING_AGG(concat(v1::text, v2), ' * '::text) FROM test;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0058-select-string_agg-concat-v1::text-v2"},
				},
				{
					Query: `SELECT STRING_AGG(concat(v1::text, v2), 8::text) FROM test;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0059-select-string_agg-concat-v1::text-v2"},
				},
			},
		},
		{
			Name: "concat",
			Assertions: []ScriptTestAssertion{
				{ // https://github.com/dolthub/doltgresql/issues/2547
					Query: `SELECT LENGTH(CONCAT('', NULL, ''));`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0060-select-length-concat-null"},
				},
				{
					Query: `SELECT CONCAT('a', NULL, 'b');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0061-select-concat-a-null-b"},
				},
				{
					Query: `SELECT CONCAT(1, 2, true);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0062-select-concat-1-2-true"},
				},
				{
					Query: `SELECT CONCAT(1::int2, 2::int4, true::bool, false::text);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0063-select-concat-1::int2-2::int4-true::bool"},
				},
				{
					Query: `SELECT CONCAT('b');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0064-select-concat-b"},
				},
				{
					Query: `SELECT CONCAT(NULL);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-teststringfunction-0065-select-concat-null"},
				},
			},
		},
	})
}

func TestFormatFunctions(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "test to_char",
			SetUpScript: []string{
				`CREATE TABLE TIMESTAMP_TBL (d1 timestamp(2) without time zone);`,
				`INSERT INTO TIMESTAMP_TBL VALUES ('1997-02-10 17:32:01-0800');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT to_char(timestamp '2021-09-15 21:43:56.123456789', 'YYYY-MM-DD HH24:MI:SS.MS');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testformatfunctions-0001-select-to_char-timestamp-2021-09-15-21:43:56.123456789"},
				},
				{
					Query: `SELECT to_char(timestamp '2021-09-15 21:43:56.123456789', 'HH HH12 HH24 hh hh12 hh24 H h hH Hh');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testformatfunctions-0002-select-to_char-timestamp-2021-09-15-21:43:56.123456789"},
				},
				{
					Query: `SELECT to_char(timestamp '2021-09-15 21:43:56.123456789', 'MI mi M m');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testformatfunctions-0003-select-to_char-timestamp-2021-09-15-21:43:56.123456789"},
				},
				{
					Query: `SELECT to_char(timestamp '2021-09-15 21:43:56.123456789', 'SS ss S s MS ms Ms mS US us Us uS');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testformatfunctions-0004-select-to_char-timestamp-2021-09-15-21:43:56.123456789"},
				},
				{
					Query: `SELECT to_char(timestamp '2021-09-15 21:43:56.123456789', 'Y,YYY y,yyy YYYY yyyy YYY yyy YY yy Y y');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testformatfunctions-0005-select-to_char-timestamp-2021-09-15-21:43:56.123456789"},
				},
				{
					Query: `SELECT to_char(timestamp '2021-09-15 21:43:56.123456789', 'MONTH Month month MON Mon mon MM mm Mm mM');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testformatfunctions-0006-select-to_char-timestamp-2021-09-15-21:43:56.123456789"},
				},
				{
					Query: `SELECT to_char(timestamp '2021-09-15 21:43:56.123456789', 'DAY Day day DDD ddd DY Dy dy DD dd D d');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testformatfunctions-0007-select-to_char-timestamp-2021-09-15-21:43:56.123456789"},
				},
				{
					Query: `SELECT to_char(timestamp '2021-09-15 21:43:56.123456789', 'DAY Day day DDD ddd DY Dy dy DD dd D d');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testformatfunctions-0008-select-to_char-timestamp-2021-09-15-21:43:56.123456789"},
				},
				{
					Query: `SELECT to_char(timestamp '2021-09-15 21:43:56.123456789', 'IW iw');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testformatfunctions-0009-select-to_char-timestamp-2021-09-15-21:43:56.123456789"},
				},
				{
					Query: `SELECT to_char(timestamp '2021-09-15 21:43:56.123456789', 'AM PM am pm A.M. P.M. a.m. p.m.');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testformatfunctions-0010-select-to_char-timestamp-2021-09-15-21:43:56.123456789"},
				},
				{
					Query: `SELECT to_char(timestamp '2021-09-15 21:43:56.123456789', 'Q q');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testformatfunctions-0011-select-to_char-timestamp-2021-09-15-21:43:56.123456789"},
				},
				{
					Query: `SELECT to_char('2012-12-12 12:00'::timestamptz, 'YYYY-MM-DD SSSS');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testformatfunctions-0012-select-to_char-2012-12-12-12:00-::timestamptz"},
				},
				{
					Query: `SET timezone = '-06:30';`,
				},
				{
					Query: `SELECT to_char('2012-12-12 12:00'::timestamptz, 'YYYY-MM-DD HH:MI:SS TZH:TZM');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testformatfunctions-0013-select-to_char-2012-12-12-12:00-::timestamptz"},
				},
				{
					Query: `SELECT to_char('2012-12-12 12:00 -02:00'::timestamptz, 'YYYY-MM-DD HH:MI:SS TZH:TZM');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testformatfunctions-0014-select-to_char-2012-12-12-12:00-02:00"},
				},
				{
					Query: `SELECT to_char('2012-12-12 12:00 -02:00'::timestamptz, 'TZ');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testformatfunctions-0015-select-to_char-2012-12-12-12:00-02:00"},
				},
				{
					Query: `SET timezone = 'UTC';`,
				},
				{
					Query: `SELECT to_char('2012-12-12 12:00 -02:00'::timestamptz, 'TZ tz');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testformatfunctions-0016-select-to_char-2012-12-12-12:00-02:00"},
				},
				{
					Query: `SELECT to_char(d1, 'Y,YYY YYYY YYY YY Y CC Q MM WW DDD DD D J') FROM TIMESTAMP_TBL;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testformatfunctions-0017-select-to_char-d1-y-yyy"},
				},
				{
					Query: `SELECT to_char(d1, 'DAY Day day DY Dy dy MONTH Month month RM MON Mon mon') FROM TIMESTAMP_TBL;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testformatfunctions-0018-select-to_char-d1-day-day"},
				},
			},
		},
	})
}

func TestUnknownFunctions(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "unknown functions",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT unknown_func();`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testunknownfunctions-0001-select-unknown_func", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "Unsupported group_concat syntax",
			SetUpScript: []string{
				"CREATE TABLE x (pk int)",
				"INSERT INTO x VALUES (1),(2),(3),(4),(NULL)",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT group_concat(pk ORDER BY pk) FROM x;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testunknownfunctions-0002-select-group_concat-pk-order-by",
						// error message is kind of nonsensical, we just want to make sure there isn't a panic
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestSelectFromFunctions(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "select * FROM functions",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT array_to_string(ARRAY[1, 2, 3, NULL, 5], ',', '*')`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testselectfromfunctions-0001-select-array_to_string-array[1-2-3"},
				},
				{
					Query: `SELECT * FROM array_to_string(ARRAY[1, 2, 3, NULL, 5], ',', '*')`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testselectfromfunctions-0002-select-*-from-array_to_string-array[1"},
				},
				{
					Query: `SELECT * FROM array_to_string(ARRAY[37.89, 1.2], '_');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testselectfromfunctions-0003-select-*-from-array_to_string-array[37.89"},
				},
				{
					Query: `SELECT * FROM format_type('text'::regtype, 4);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testselectfromfunctions-0004-select-*-from-format_type-text"},
				},
				{
					Query: `SELECT * from format_type(874938247, 20);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testselectfromfunctions-0005-select-*-from-format_type-874938247"},
				},
				{
					Query: `SELECT * FROM to_char(timestamp '2021-09-15 21:43:56.123456789', 'IW iw');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testselectfromfunctions-0006-select-*-from-to_char-timestamp"},
				},
				{
					Query: `SELECT * from format_type('text'::regtype, -1);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testselectfromfunctions-0007-select-*-from-format_type-text"},
				},
				{
					Query: `SELECT "left" FROM left('name'::name, 2);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testselectfromfunctions-0008-select-left-from-left-name"},
				},
				{
					Query: "SELECT length FROM length('name'::name);", PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testselectfromfunctions-0009-select-length-from-length-name"},
				},
				{
					Query: "SELECT lower FROM lower('naMe'::name);", PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testselectfromfunctions-0010-select-lower-from-lower-name"},
				},
				{
					Query: "SELECT * FROM lpad('name'::name, 7, '*');", PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testselectfromfunctions-0011-select-*-from-lpad-name"},
				},
			},
		},
		{
			Name: "test select  from dolt_ functions",
			Skip: true, // need a way for single-row functions to declare a schema like table functions do, maybe just by modeling them as table functions in the first place
			SetUpScript: []string{
				"CREATE TABLE test (pk INT primary key, v1 INT, v2 TEXT);",
				"INSERT INTO test VALUES (1, 1, 'a'), (2, 2, 'b'), (3, 3, 'c'), (4, 4, 'd'), (5, 5, 'e');",
				"call dolt_commit('-Am', 'first table');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `select * from dolt_branch('newBranch')`,
					Expected: []sql.Row{{0}},
				},
				{
					Query:    `select status from dolt_checkout('newBranch')`,
					Expected: []sql.Row{{0}},
				},
				{
					Query: `insert into test values (6, 6, 'f')`,
				},
				{
					Query:    `select length(commit_hash) > 0 from (select commit_hash from dolt_commit('-Am', 'added f') as result)`,
					Expected: []sql.Row{{"t"}},
				},
				{
					Query:    "select dolt_checkout('main')",
					Expected: []sql.Row{{0}},
				},
				{
					Query:    `select fast_forward, conflicts from dolt_merge('newBranch')`,
					Expected: []sql.Row{{"t", 0}},
				},
			},
		},
	})
}

func TestSetReturningFunctions(t *testing.T) {
	RunScripts(
		t,
		[]ScriptTest{
			{
				Name: "generate_series",
				Assertions: []ScriptTestAssertion{
					{
						Query: `SELECT generate_series(1,3)`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsetreturningfunctions-0001-select-generate_series-1-3"},
					},
					{
						Query: `SELECT generate_series(1,6,2)`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsetreturningfunctions-0002-select-generate_series-1-6-2"},
					},
					{
						Query: `SELECT generate_series(1::int4,6::int4,0::int4)`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsetreturningfunctions-0003-select-generate_series-1::int4-6::int4-0::int4", Compare: "sqlstate"},
					},
					{
						Query: `SELECT generate_series(6,1,-2)`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsetreturningfunctions-0004-select-generate_series-6-1-2"},
					},
					{
						Query: `SELECT generate_series(1.5,6,2)`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsetreturningfunctions-0005-select-generate_series-1.5-6-2"},
					},
					{
						Query: `SELECT generate_series(1::int8,6::int8,0::int8)`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsetreturningfunctions-0006-select-generate_series-1::int8-6::int8-0::int8", Compare: "sqlstate"},
					},
					{
						Query: `SELECT generate_series(6,2.2,-2)`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsetreturningfunctions-0007-select-generate_series-6-2.2-2"},
					},
					{
						Query: `SELECT generate_series('2008-03-01 00:00'::timestamp,'2008-03-02 12:00', '10 hours');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsetreturningfunctions-0008-select-generate_series-2008-03-01-00:00-::timestamp"},
					},
					{
						Query: `SELECT generate_series('2008-03-02 12:00'::timestamp,'2008-03-01 00:00'::timestamp, '-10 hours');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsetreturningfunctions-0009-select-generate_series-2008-03-02-12:00-::timestamp"},
					},
				},
			},
			{
				Name: "generate_series as table function",
				Assertions: []ScriptTestAssertion{
					{
						Query: `SELECT * FROM generate_series(1,3)`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsetreturningfunctions-0010-select-*-from-generate_series-1"},
					},
					{
						Query: `SELECT * FROM generate_series(1,6,2)`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsetreturningfunctions-0011-select-*-from-generate_series-1"},
					},
					{
						Query: `SELECT * FROM generate_series(-100::numeric, 100::numeric, 0::numeric);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsetreturningfunctions-0012-select-*-from-generate_series-100::numeric", Compare: "sqlstate"},
					},
					{
						Query: `SELECT * FROM generate_series('2008-03-02 12:00'::timestamp,'2008-03-01 00:00'::timestamp, '-10 hours'::interval);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsetreturningfunctions-0013-select-*-from-generate_series-2008-03-02"},
					},
					{
						Query: `select * from generate_series('2020-01-01 00:00'::timestamp, '2020-01-02 03:00'::timestamp, '0 hour'::interval);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsetreturningfunctions-0014-select-*-from-generate_series-2020-01-01",

							// TODO: cannot cast unknown to interval, but this should work
							Compare: "sqlstate"},
					},
					{
						Skip:  true,
						Query: `SELECT * FROM generate_series('2008-03-02 12:00'::timestamp,'2008-03-01 00:00'::timestamp, '-10 hours');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsetreturningfunctions-0015-select-*-from-generate_series-2008-03-02"},
					},
				},
			},
			{
				Name: "nested generate_series",
				// Nested SRF expressions cause an infinite loop, skipped in regression tests.
				// Challenging to fix with the current expression eval architecture and very marginal as a use case.
				Skip: true,
				Assertions: []ScriptTestAssertion{
					{
						Query: `SELECT generate_series(1, generate_series(1, 3))`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsetreturningfunctions-0016-select-generate_series-1-generate_series-1"},
					},
				},
			},
			{
				Name: "limit, offset, sort",
				Assertions: []ScriptTestAssertion{
					{
						Query: `SELECT a, generate_series(1,2) FROM (VALUES(1),(2),(3)) r(a) LIMIT 2 OFFSET 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsetreturningfunctions-0017-select-a-generate_series-1-2"},
					},
					{
						Query: `SELECT a, generate_series(1,2) FROM (VALUES(1),(2),(3)) r(a) ORDER BY 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsetreturningfunctions-0018-select-a-generate_series-1-2"},
					},
				},
			},
			{
				Name: "generate_series with table",
				SetUpScript: []string{
					"CREATE TABLE t1 (pk INT primary key, v1 INT);",
					"INSERT INTO t1 VALUES (1, 1), (2, 2), (3, 3);",
				},
				Assertions: []ScriptTestAssertion{
					{
						Query: `SELECT generate_series(1,3), pk from t1`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsetreturningfunctions-0019-select-generate_series-1-3-pk"},
					},
					{
						Query: `SELECT generate_series(1,3) + pk, pk from t1`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsetreturningfunctions-0020-select-generate_series-1-3-+"},
					},
				},
			},
			{
				Name: "set returning function as table function: generate_series",
				Skip: true, // select * from functions does not work yet
				Assertions: []ScriptTestAssertion{
					{
						Query: `select * from generate_series(1,3)`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsetreturningfunctions-0021-select-*-from-generate_series-1"},
					},
					{
						Query: `select sum(null::int4) from generate_series(1,3);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsetreturningfunctions-0022-select-sum-null::int4-from-generate_series"},
					},
					{
						Query: `SELECT * from generate_series('2008-03-01 00:00'::timestamp,'2008-03-02 12:00', '10 hours');`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsetreturningfunctions-0023-select-*-from-generate_series-2008-03-01"},
					},
				},
			},
			{
				Name: "generate_subscripts",
				SetUpScript: []string{
					"CREATE TABLE t1 (pk INT primary key, v1 INT[]);",
					"INSERT INTO t1 VALUES (1, ARRAY[1, 2, 3]), (2, ARRAY[4, 5]), (3, NULL);",
				},
				Assertions: []ScriptTestAssertion{
					{
						Query: "select generate_subscripts(v1, 1) from t1 where pk = 1", PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsetreturningfunctions-0024-select-generate_subscripts-v1-1-from"},
					},
					{
						Query: "select generate_subscripts(v1, 1) + 100 from t1 where pk = 1", PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsetreturningfunctions-0025-select-generate_subscripts-v1-1-+"},
					},
					{
						Query: "select generate_subscripts(v1, 1) from t1 where pk = 3", PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsetreturningfunctions-0026-select-generate_subscripts-v1-1-from"},
					},
					{
						Query: "select generate_subscripts(v1, 1), v1 from t1", PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsetreturningfunctions-0027-select-generate_subscripts-v1-1-v1"},
					},
				},
			},
			{
				Name: "generate_subscripts with join",
				SetUpScript: []string{
					"CREATE TABLE t1 (a INT[]);",
					"CREATE TABLE t2 (b int[]);",
					"INSERT INTO t1 VALUES (ARRAY[1]), (ARRAY[1, 2, 3])",
					"INSERT INTO t2 VALUES (ARRAY[9,10])",
				},
				Assertions: []ScriptTestAssertion{
					{
						Query: "select generate_subscripts(a, 1), a, generate_subscripts(b, 1), b from t1, t2;", PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsetreturningfunctions-0028-select-generate_subscripts-a-1-a"},
					},
				},
			},
			{
				Name: "generate_subscripts and generate_series combined",
				SetUpScript: []string{
					"CREATE TABLE t1 (a INT[]);",
					"INSERT INTO t1 VALUES (ARRAY[1, 2, 3]), (ARRAY[4, 5]);",
				},
				Assertions: []ScriptTestAssertion{
					{
						Query: "select generate_subscripts(a, 1), a, generate_series(1,4) from t1;", PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsetreturningfunctions-0029-select-generate_subscripts-a-1-a"},
					},
				},
			},
			{
				Name: "generate_subscripts on 0-indexed array types",
				Assertions: []ScriptTestAssertion{
					{
						Query: "SELECT generate_subscripts('1 2 3'::int2vector, 1);", PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsetreturningfunctions-0030-select-generate_subscripts-1-2-3"},
					},
					{
						Query: "SELECT generate_subscripts('1 2 3'::oidvector, 1);", PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsetreturningfunctions-0031-select-generate_subscripts-1-2-3"},
					},
				},
			},
			{
				Name: "set generation with other func calls",
				SetUpScript: []string{
					"CREATE sequence test_seq START WITH 1 INCREMENT BY 3;",
				},
				Assertions: []ScriptTestAssertion{
					{
						Query: `SELECT generate_series(1, 5), nextval('test_seq')`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsetreturningfunctions-0032-select-generate_series-1-5-nextval"},
					},
				},
			},
			{
				Name: "generate_series as table function and projection",
				Assertions: []ScriptTestAssertion{
					{
						Query: `SELECT *, unnest(ARRAY['cat', 'dog', 'bird']) AS animal FROM generate_series(1, 3);`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsetreturningfunctions-0033-select-*-unnest-array[-cat"},
					},
				},
			},
			{
				Name: "insert with set returning function",
				SetUpScript: []string{
					"create table hash_parted (a int, b int, c int);",
				},
				Assertions: []ScriptTestAssertion{
					{
						Query: `insert into hash_parted values(0, generate_series(1,3), generate_series(5,8));`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsetreturningfunctions-0034-insert-into-hash_parted-values-0"},
					},
					{
						Query: `insert into hash_parted values(0, generate_series(11,12), generate_series(51,54)), (1, generate_series(1,3), generate_series(5,8));`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsetreturningfunctions-0035-insert-into-hash_parted-values-0", Compare: "sqlstate"},
					},
					{
						Query: `select * from hash_parted;`, PostgresOracle: ScriptTestPostgresOracle{ID: "functions-test-testsetreturningfunctions-0036-select-*-from-hash_parted"},
					},
				},
			},
		},
	)
}
