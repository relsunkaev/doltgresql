// Copyright 2024 Dolthub, Inc.
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

func TestValuesStatement(t *testing.T) {
	RunScripts(t, ValuesStatementTests)
}

var ValuesStatementTests = []ScriptTest{
	{
		Name: "basic values statements",
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT * FROM (VALUES (1), (2), (3)) sqa;`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0001-select-*-from-values-1"},
			},
			{
				Query: `SELECT * FROM (VALUES (1, 2), (3, 4)) sqa;`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0002-select-*-from-values-1"},
			},
			{
				Query: `SELECT i * 10, j * 100 FROM (VALUES (1, 2), (3, 4)) sqa(i, j);`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0003-select-i-*-10-j"},
			},
		},
	},
	{
		Name: "VALUES with mixed int and decimal",
		Assertions: []ScriptTestAssertion{
			{
				// Integer first, then decimal - should resolve to numeric
				Query: `SELECT * FROM (VALUES(1),(2.01),(3)) v(n);`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0004-select-*-from-values-1"},
			},
			{
				// Decimal first, then integers - should resolve to numeric
				Query: `SELECT * FROM (VALUES(1.01),(2),(3)) v(n);`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0005-select-*-from-values-1.01"},
			},
			{
				// SUM should work directly now that VALUES has correct type
				Query: `SELECT SUM(n) FROM (VALUES(1),(2.01),(3)) v(n);`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0006-select-sum-n-from-values"},
			},
			{
				// Exact repro from issue #1648: integer first, explicit cast to numeric
				Query: `SELECT SUM(n::numeric) FROM (VALUES(1),(2.01),(3)) v(n);`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0007-select-sum-n::numeric-from-values"},
			},
			{
				// Exact repro from issue #1648: decimal first, explicit cast to numeric
				Query: `SELECT SUM(n::numeric) FROM (VALUES(1.01),(2),(3)) v(n);`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0008-select-sum-n::numeric-from-values"},
			},
		},
	},
	{
		Name: "VALUES with multiple columns mixed types",
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT * FROM (VALUES(1, 'a'), (2.5, 'b')) v(num, str);`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0009-select-*-from-values-1"},
			},
		},
	},
	{
		Name: "VALUES with GROUP BY",
		Assertions: []ScriptTestAssertion{
			{
				// GROUP BY on mixed type VALUES - tests that GetField types are updated correctly
				Query: `SELECT n, COUNT(*) FROM (VALUES(1),(2.5),(1),(3.5),(2.5)) v(n) GROUP BY n ORDER BY n;`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0010-select-n-count-*-from"},
			},
			{
				// SUM with GROUP BY
				Query: `SELECT category, SUM(amount) FROM (VALUES('a', 1),('b', 2.5),('a', 3),('b', 4.5)) v(category, amount) GROUP BY category ORDER BY category;`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0011-select-category-sum-amount-from"},
			},
		},
	},
	{
		Name: "VALUES with DISTINCT",
		Assertions: []ScriptTestAssertion{
			{
				// DISTINCT on mixed type VALUES
				Query: `SELECT DISTINCT n FROM (VALUES(1),(2.5),(1),(2.5),(3)) v(n) ORDER BY n;`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0012-select-distinct-n-from-values"},
			},
		},
	},
	{
		Name: "VALUES with LIMIT and OFFSET",
		Assertions: []ScriptTestAssertion{
			{
				// LIMIT on mixed type VALUES
				Query: `SELECT * FROM (VALUES(1),(2.5),(3),(4.5),(5)) v(n) LIMIT 3;`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0013-select-*-from-values-1"},
			},
			{
				// LIMIT with OFFSET
				Query: `SELECT * FROM (VALUES(1),(2.5),(3),(4.5),(5)) v(n) LIMIT 2 OFFSET 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0014-select-*-from-values-1"},
			},
		},
	},
	{
		Name: "VALUES with ORDER BY",
		Assertions: []ScriptTestAssertion{
			{
				// ORDER BY on mixed type VALUES - ascending
				Query: `SELECT * FROM (VALUES(3),(1.5),(2),(4.5)) v(n) ORDER BY n;`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0015-select-*-from-values-3"},
			},
			{
				// ORDER BY descending
				Query: `SELECT * FROM (VALUES(3),(1.5),(2),(4.5)) v(n) ORDER BY n DESC;`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0016-select-*-from-values-3"},
			},
		},
	},
	{
		Name: "VALUES in subquery",
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT * FROM (SELECT n * 2 AS doubled FROM (VALUES(1),(2.5),(3)) v(n)) sub;`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0017-select-*-from-select-n"},
			},
			{
				Query: `SELECT * FROM (SELECT * FROM (VALUES(1),(2.5),(3),(4.5)) v(n) LIMIT 2) sub;`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0018-select-*-from-select-*"},
			},
			{
				Query: `SELECT * FROM (SELECT * FROM (VALUES(3),(1.5),(2)) v(n) ORDER BY n) sub;`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0019-select-*-from-select-*"},
			},
		},
	},
	{
		Name: "VALUES with WHERE clause (Filter node)",
		Assertions: []ScriptTestAssertion{
			{
				// Filter on mixed type VALUES
				Query: `SELECT * FROM (VALUES(1),(2.5),(3),(4.5),(5)) v(n) WHERE n > 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0020-select-*-from-values-1"},
			},
			{
				// Filter with multiple conditions
				Query: `SELECT * FROM (VALUES(1),(2.5),(3),(4.5),(5)) v(n) WHERE n > 1 AND n < 4.5;`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0021-select-*-from-values-1"},
			},
		},
	},
	{
		Name: "VALUES with aggregate functions",
		Assertions: []ScriptTestAssertion{
			{
				// AVG on mixed types
				Query: `SELECT AVG(n) FROM (VALUES(1),(2),(3),(4)) v(n);`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0022-select-avg-n-from-values"},
			},
			{
				Query: `SELECT pg_typeof(AVG(n)) FROM (VALUES(1),(2),(3),(4)) v(n);`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0023-select-pg_typeof-avg-n-from"},
			},
			{
				// MIN/MAX on mixed types
				Query: `SELECT MIN(n), MAX(n) FROM (VALUES(1),(2.5),(3),(0.5)) v(n);`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0024-select-min-n-max-n"},
			},
		},
	},
	{
		Name: "VALUES combined operations",
		Assertions: []ScriptTestAssertion{
			{
				// GROUP BY + ORDER BY + LIMIT
				Query: `SELECT n, COUNT(*) as cnt FROM (VALUES(1),(2.5),(1),(2.5),(3),(1)) v(n) GROUP BY n ORDER BY cnt DESC LIMIT 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0025-select-n-count-*-as"},
			},
			{
				// DISTINCT + ORDER BY + LIMIT
				Query: `SELECT DISTINCT n FROM (VALUES(1),(2.5),(1),(3),(2.5),(4)) v(n) ORDER BY n DESC LIMIT 3;`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0026-select-distinct-n-from-values"},
			},
			{
				// WHERE + ORDER BY + LIMIT
				Query: `SELECT * FROM (VALUES(1),(2.5),(3),(4.5),(5)) v(n) WHERE n > 1 ORDER BY n DESC LIMIT 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0027-select-*-from-values-1"},
			},
		},
	},
	{
		Name: "VALUES with single row (no type unification needed)",
		Assertions: []ScriptTestAssertion{
			{
				// Single row should pass through unchanged
				Query: `SELECT * FROM (VALUES(42)) v(n);`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0028-select-*-from-values-42"},
			},
			{
				// Single row with decimal
				Query: `SELECT * FROM (VALUES(3.14)) v(n);`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0029-select-*-from-values-3.14"},
			},
		},
	},
	{
		Name: "VALUES with NULL values",
		Assertions: []ScriptTestAssertion{
			{
				// NULL mixed with integers - should resolve to integer, NULL stays NULL
				Query: `SELECT * FROM (VALUES(1),(NULL),(3)) v(n);`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0030-select-*-from-values-1"},
			},
			{
				// NULL mixed with decimals - should resolve to numeric
				Query: `SELECT * FROM (VALUES(1.5),(NULL),(3.5)) v(n);`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0031-select-*-from-values-1.5"},
			},
			{
				// NULL mixed with int and decimal - should resolve to numeric
				Query: `SELECT * FROM (VALUES(1),(NULL),(2.5)) v(n);`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0032-select-*-from-values-1"},
			},
			{
				// All NULLs - should resolve to text (PostgreSQL behavior)
				Query: `SELECT * FROM (VALUES(NULL),(NULL)) v(n);`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0033-select-*-from-values-null"},
			},
		},
	},
	{
		Name: "VALUES type mismatch errors",
		Assertions: []ScriptTestAssertion{
			{
				// Integer and unknown('text'): FindCommonType resolves to int4 (the non-unknown type),
				// then the I/O cast from 'text' to int4 fails at execution time. This matches PostgreSQL behavior:
				// psql returns "invalid input syntax for type integer: "text""
				Query: `SELECT * FROM (VALUES(1),('text'),(3)) v(n);`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0034-select-*-from-values-1",

					// Boolean and integer cannot be matched
					Compare: "sqlstate"},
			},
			{

				Query: `SELECT * FROM (VALUES(true),(1),(false)) v(n);`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0035-select-*-from-values-true", Compare: "sqlstate"},
			},
		},
	},
	{
		Name: "VALUES with all unknown types (string literals)",
		Assertions: []ScriptTestAssertion{
			{
				// All string literals should resolve to text
				Query: `SELECT * FROM (VALUES('a'),('b'),('c')) v(n);`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0036-select-*-from-values-a"},
			},
			{
				// String literals with operations
				Query: `SELECT n || '!' FROM (VALUES('hello'),('world')) v(n);`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0037-select-n-||-!-from"},
			},
		},
	},
	{
		Name: "VALUES with array types",
		Assertions: []ScriptTestAssertion{
			{
				// Integer arrays: doltgresql returns arrays in text format over the wire
				Query: `SELECT * FROM (VALUES(ARRAY[1,2]),(ARRAY[3,4])) v(arr);`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0038-select-*-from-values-array[1"},
			},
			{
				// Text arrays: doltgresql returns arrays in text format over the wire
				Query: `SELECT * FROM (VALUES(ARRAY['a','b']),(ARRAY['c','d'])) v(arr);`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0039-select-*-from-values-array["},
			},
		},
	},
	{
		Name: "VALUES with all same type multi-row (no casts needed)",
		Assertions: []ScriptTestAssertion{
			{
				// All integers
				Query: `SELECT * FROM (VALUES(1),(2),(3)) v(n);`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0040-select-*-from-values-1"},
			},
			{
				// All decimals
				Query: `SELECT * FROM (VALUES(1.5),(2.5),(3.5)) v(n);`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0041-select-*-from-values-1.5"},
			},
			{
				// All text
				Query: `SELECT * FROM (VALUES('x'),('y'),('z')) v(n);`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0042-select-*-from-values-x"},
			},
		},
	},
	{
		Name: "VALUES with multi-column partial cast",
		Assertions: []ScriptTestAssertion{
			{
				// Only first column needs cast
				Query: `SELECT * FROM (VALUES(1, 'a'),(2.5, 'b'),(3, 'c')) v(num, str);`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0043-select-*-from-values-1"},
			},
			{
				// Only second column needs cast
				Query: `SELECT * FROM (VALUES(1, 10),(2, 20.5),(3, 30)) v(a, b);`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0044-select-*-from-values-1"},
			},
		},
	},
	{
		Name: "VALUES in CTE (WITH clause)",
		Assertions: []ScriptTestAssertion{
			{
				// Mixed types via CTE
				Query: `WITH nums AS (SELECT * FROM (VALUES(1),(2.5),(3)) v(n)) SELECT * FROM nums;`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0045-with-nums-as-select-*"},
			},
			{
				// SUM over CTE
				Query: `WITH nums AS (SELECT * FROM (VALUES(1),(2.5),(3)) v(n)) SELECT SUM(n) FROM nums;`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0046-with-nums-as-select-*"},
			},
		},
	},
	{
		Name: "VALUES with JOIN",
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT a.n, b.label FROM (VALUES(1),(2),(3)) a(n) JOIN (VALUES(1, 'one'),(2, 'two'),(3, 'three')) b(id, label) ON a.n = b.id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0047-select-a.n-b.label-from-values"},
			},
			{
				// Mixed types in one of the joined VALUES
				Query: `SELECT a.n, b.label FROM (VALUES(1),(2.5),(3)) a(n) JOIN (VALUES(1, 'one'),(3, 'three')) b(id, label) ON a.n = b.id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0048-select-a.n-b.label-from-values"},
			},
		},
	},
	{
		Name: "VALUES with same-type booleans",
		Assertions: []ScriptTestAssertion{
			{
				// All booleans, returned as "t"/"f" over the wire
				Query: `SELECT * FROM (VALUES(true),(false),(true)) v(b);`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0049-select-*-from-values-true"},
			},
			{
				// Boolean WHERE filter
				Query: `SELECT * FROM (VALUES(true),(false),(true),(false)) v(b) WHERE b = true;`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0050-select-*-from-values-true"},
			},
		},
	},
	{
		Name: "VALUES with case-sensitive quoted column names",
		Assertions: []ScriptTestAssertion{
			{
				// Column names w/ quotes preserve case; unquoted are lowercased by the parser
				Query: `SELECT "ColA", "colb" FROM (VALUES(1, 2),(3.5, 4.5)) v("ColA", "colb");`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0051-select-cola-colb-from-values"},
			},
			{
				// Mixed case: one quoted (preserved), one unquoted (lowered)
				Query: `SELECT "MixedCase", plain FROM (VALUES(1, 'a'),(2.5, 'b')) v("MixedCase", plain);`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0052-select-mixedcase-plain-from-values"},
			},
			{
				// SUM with quoted column name
				Query: `SELECT SUM("Val") FROM (VALUES(1),(2.5),(3)) v("Val");`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0053-select-sum-val-from-values"},
			},
		},
	},
	{
		Name: "VALUES with case-differing quoted columns and aggregates",
		Assertions: []ScriptTestAssertion{
			{
				// TODO: GMS is case-insensitive for identifiers, but
				// Postgres requires case-sensitivity for quoted identifiers.
				// Columns "Val" and "val" both become "val" after
				// strings.ToLower, so their aggregates match the wrong column.
				Skip:  true,
				Query: `SELECT SUM("Val"), SUM("val") FROM (VALUES(1, 10),(2.5, 20)) v("Val", "val");`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0054-select-sum-val-sum-val"},
			},
		},
	},
	{
		Name: "values inside subquery preserves projections",
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT * FROM (SELECT n * 2 AS doubled FROM (VALUES (1), (2), (3)) v(n)) sub;`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0055-select-*-from-select-n"},
			},
			{
				Query: `SELECT * FROM (SELECT n * 2 AS doubled FROM (VALUES (1), (2.5), (3)) v(n)) sub;`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0056-select-*-from-select-n"},
			},
		},
	},
	{
		Name: "values inside subquery preserves LIMIT",
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT * FROM (SELECT * FROM (VALUES (1), (2), (3), (4)) v(n) LIMIT 2) sub;`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0057-select-*-from-select-*"},
			},
			{
				Query: `SELECT * FROM (SELECT * FROM (VALUES (1), (2.5), (3), (4.5)) v(n) LIMIT 2) sub;`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0058-select-*-from-select-*"},
			},
		},
	},
	{
		Name: "values inside subquery preserves ORDER BY",
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT * FROM (SELECT * FROM (VALUES (3), (1), (2)) v(n) ORDER BY n) sub;`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0059-select-*-from-select-*"},
			},
			{
				Query: `SELECT * FROM (SELECT * FROM (VALUES (3), (1.5), (2)) v(n) ORDER BY n) sub;`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0060-select-*-from-select-*"},
			},
		},
	},
	{
		Name: "values inside subquery preserves DISTINCT",
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT * FROM (SELECT DISTINCT * FROM (VALUES (1), (1), (2), (2), (3)) v(n)) sub;`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0061-select-*-from-select-distinct"},
			},
			{
				Query: `SELECT * FROM (SELECT DISTINCT * FROM (VALUES (1), (1), (2.5), (2.5), (3)) v(n)) sub;`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0062-select-*-from-select-distinct"},
			},
		},
	},
	{
		Name: "values inside subquery preserves WHERE",
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT * FROM (SELECT * FROM (VALUES (1), (2), (3), (4), (5)) v(n) WHERE n > 3) sub;`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0063-select-*-from-select-*"},
			},
			{
				Query: `SELECT * FROM (SELECT * FROM (VALUES (1), (2.5), (3), (4.5), (5)) v(n) WHERE n > 3) sub;`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0064-select-*-from-select-*"},
			},
		},
	},
	{
		Name: "values inside subquery preserves OFFSET",
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT * FROM (SELECT * FROM (VALUES (10), (20), (30)) v(n) OFFSET 1) sub;`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0065-select-*-from-select-*"},
			},
			{
				Query: `SELECT * FROM (SELECT * FROM (VALUES (10), (20), (30), (40), (50)) v(n) LIMIT 2 OFFSET 1) sub;`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0066-select-*-from-select-*"},
			},
			{
				Query: `SELECT * FROM (SELECT * FROM (VALUES (10), (20.5), (30)) v(n) OFFSET 1) sub;`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0067-select-*-from-select-*"},
			},
			{
				Query: `SELECT * FROM (SELECT * FROM (VALUES (10), (20.5), (30), (40.5), (50)) v(n) LIMIT 2 OFFSET 1) sub;`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0068-select-*-from-select-*"},
			},
		},
	},
	{
		Name: "values inside subquery preserves ORDER BY with LIMIT",
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT * FROM (SELECT * FROM (VALUES (5), (3), (1), (4), (2)) v(n) ORDER BY n LIMIT 3) sub;`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0069-select-*-from-select-*"},
			},
			{
				Query: `SELECT * FROM (SELECT * FROM (VALUES (5), (3.5), (1), (4), (2.5)) v(n) ORDER BY n LIMIT 3) sub;`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0070-select-*-from-select-*"},
			},
		},
	},
	{
		Name: "values inside subquery preserves GROUP BY with aggregate",
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT * FROM (SELECT n, count(*) AS cnt FROM (VALUES (1), (1), (2), (2), (2), (3)) v(n) GROUP BY n ORDER BY n) sub;`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0071-select-*-from-select-n"},
			},
			{
				Query: `SELECT * FROM (SELECT n, count(*) AS cnt FROM (VALUES (1), (1), (2.5), (2.5), (2.5), (3)) v(n) GROUP BY n ORDER BY n) sub;`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0072-select-*-from-select-n"},
			},
		},
	},
	{
		Name: "values inside subquery preserves HAVING",
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT * FROM (SELECT n, count(*) AS cnt FROM (VALUES (1), (1), (2), (2), (2), (3)) v(n) GROUP BY n HAVING count(*) > 1 ORDER BY n) sub;`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0073-select-*-from-select-n"},
			},
			{
				Query: `SELECT * FROM (SELECT n, count(*) AS cnt FROM (VALUES (1), (1), (2.5), (2.5), (2.5), (3)) v(n) GROUP BY n HAVING count(*) > 1 ORDER BY n) sub;`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0074-select-*-from-select-n"},
			},
		},
	},
	{
		Name: "values inside subquery preserves column aliasing",
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT * FROM (SELECT n AS val, n * 10 AS tenfold FROM (VALUES (1), (2), (3)) v(n)) sub;`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0075-select-*-from-select-n"},
			},
			{
				Query: `SELECT * FROM (SELECT n AS val, n * 10 AS tenfold FROM (VALUES (1), (2.5), (3)) v(n)) sub;`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0076-select-*-from-select-n"},
			},
		},
	},
	{
		Name: "values inside subquery preserves column subset selection",
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT * FROM (SELECT a FROM (VALUES (1, 10), (2, 20), (3, 30)) v(a, b)) sub;`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0077-select-*-from-select-a"},
			},
		},
	},
	{
		Name: "values subquery trivial SELECT * still unwraps correctly",
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT * FROM (SELECT * FROM (VALUES (1), (2), (3)) v(n)) sub;`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0078-select-*-from-select-*"},
			},
		},
	},
	{
		Name: "values inside subquery with multiple combined clauses",
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT * FROM (SELECT DISTINCT n FROM (VALUES (3), (1), (1), (2), (2), (3)) v(n) ORDER BY n LIMIT 2) sub;`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0079-select-*-from-select-distinct"},
			},
			{
				Query: `SELECT * FROM (SELECT DISTINCT n FROM (VALUES (3), (1.5), (1.5), (2), (2), (3)) v(n) ORDER BY n LIMIT 2) sub;`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0080-select-*-from-select-distinct"},
			},
		},
	},
	{
		Name: "values in JOIN preserves inner subquery semantics",
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT a.n, b.m FROM (VALUES (1), (2)) a(n) JOIN (SELECT m * 10 AS m FROM (VALUES (1), (2)) v(m)) b ON a.n = b.m / 10;`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0081-select-a.n-b.m-from-values"},
			},
			{
				Query: `SELECT a.n, b.m FROM (VALUES (1), (2.5)) a(n) JOIN (SELECT m * 10 AS m FROM (VALUES (1), (2.5)) v(m)) b ON a.n = b.m / 10;`, PostgresOracle: ScriptTestPostgresOracle{ID: "values-statement-test-testvaluesstatement-0082-select-a.n-b.m-from-values"},
			},
		},
	},
}
