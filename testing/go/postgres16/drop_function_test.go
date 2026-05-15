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

func TestDropFunction(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "Function does not exist",
			Assertions: []ScriptTestAssertion{
				{
					Query: "DROP FUNCTION doesnotexist;", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-function-test-testdropfunction-0001-drop-function-doesnotexist", Compare: "sqlstate"},
				},
				{
					Query: "DROP FUNCTION IF EXISTS doesnotexist;", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-function-test-testdropfunction-0002-drop-function-if-exists-doesnotexist"},
				},
			},
		},
		{
			Name: "Basic cases",
			SetUpScript: []string{`
CREATE FUNCTION func1() RETURNS TEXT AS $$
BEGIN RETURN 'func1'; END;
$$ LANGUAGE plpgsql;`, `
CREATE FUNCTION func2(input INT) RETURNS TEXT AS $$
BEGIN RETURN 'func2(INT)'; END;
$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT func1(), func2(99);", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-function-test-testdropfunction-0003-select-func1-func2-99"},
				},
				{
					Query: "DROP FUNCTION func1;", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-function-test-testdropfunction-0004-drop-function-func1"},
				},
				{
					Query: "DROP FUNCTION func2(INT);", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-function-test-testdropfunction-0005-drop-function-func2-int"},
				},
				{
					Query: "SELECT func1();", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-function-test-testdropfunction-0006-select-func1", Compare: "sqlstate"},
				},
				{
					Query: "SELECT func2(99);", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-function-test-testdropfunction-0007-select-func2-99", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "Optional type information",
			SetUpScript: []string{`
CREATE FUNCTION func1() RETURNS TEXT AS $$
BEGIN RETURN 'func1'; END;
$$ LANGUAGE plpgsql;`, `
CREATE FUNCTION func2() RETURNS TEXT AS $$
BEGIN RETURN 'func2'; END;
$$ LANGUAGE plpgsql;`, `
CREATE FUNCTION func3(input INT) RETURNS TEXT AS $$
BEGIN RETURN 'func3(INT)'; END;
$$ LANGUAGE plpgsql;`, `
CREATE FUNCTION func4(input INT) RETURNS TEXT AS $$
BEGIN RETURN 'func4(INT)'; END;
$$ LANGUAGE plpgsql;`, `
CREATE FUNCTION func5(input INT, foo TEXT) RETURNS TEXT AS $$
BEGIN RETURN 'func5(INT, TEXT)'; END;
$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT func1(), func2(), func3(1), func4(2);", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-function-test-testdropfunction-0008-select-func1-func2-func3-1"},
				},
				{
					Query: "DROP FUNCTION func1(OUT TEXT);", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-function-test-testdropfunction-0009-drop-function-func1-out-text"},
				},
				{
					Query: "DROP FUNCTION func2(OUT paramname TEXT);", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-function-test-testdropfunction-0010-drop-function-func2-out-paramname"},
				},
				{
					Query: "DROP FUNCTION func3(paramname INT);", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-function-test-testdropfunction-0011-drop-function-func3-paramname-int"},
				},
				{
					Query: "DROP FUNCTION func4(IN paramname INT);", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-function-test-testdropfunction-0012-drop-function-func4-in-paramname"},
				},
				{
					Query: "DROP FUNCTION func5(IN paramname INT, IN paramname TEXT);", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-function-test-testdropfunction-0013-drop-function-func5-in-paramname"},
				},
			},
		},
		{
			Name: "Qualified names",
			SetUpScript: []string{`
CREATE FUNCTION func1() RETURNS TEXT AS $$
BEGIN RETURN 'func1'; END;
$$ LANGUAGE plpgsql;`, `
CREATE FUNCTION func2(input TEXT) RETURNS TEXT AS $$
BEGIN RETURN 'func2(TEXT)'; END;
$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT current_schema(), current_database();", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-function-test-testdropfunction-0014-select-current_schema-current_database", ColumnModes: []string{"schema"}},
				},
				{
					Query: "SELECT func1(), func2('foo');", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-function-test-testdropfunction-0015-select-func1-func2-foo"},
				},
				{
					Query:    "DROP FUNCTION public.func1;",
					Expected: []sql.Row{},
				},
				{
					Query:       "SELECT func1();",
					ExpectedErr: "not found",
				},
				{
					Query:    "DROP FUNCTION postgres.public.func2(TEXT);",
					Expected: []sql.Row{},
				},
				{
					Query:       "SELECT func2('w00t');",
					ExpectedErr: "not found",
				},
			},
		},
		{
			// When there is only one function with a name, the parameter types are not required,
			// but if the name is not unique, an error is returned.
			Name: "Unspecified parameter types",
			SetUpScript: []string{`
CREATE FUNCTION func1(input1 TEXT, input2 TEXT) RETURNS int AS $$
BEGIN RETURN 42; END;
$$ LANGUAGE plpgsql;`, `
CREATE FUNCTION func2(input1 TEXT) RETURNS int AS $$
BEGIN RETURN 42; END;
$$ LANGUAGE plpgsql;`, `
CREATE FUNCTION func2(input1 TEXT, input2 TEXT) RETURNS int AS $$
BEGIN RETURN 42; END;
$$ LANGUAGE plpgsql;`, `
CREATE FUNCTION func3(input1 TEXT, input2 TEXT) RETURNS int AS $$
BEGIN RETURN 42; END;
$$ LANGUAGE plpgsql;`, `
CREATE FUNCTION func3() RETURNS int AS $$
BEGIN RETURN 42; END;
$$ LANGUAGE plpgsql;`},
			Assertions: []ScriptTestAssertion{
				{
					Query: "DROP FUNCTION func1;", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-function-test-testdropfunction-0020-drop-function-func1"},
				},
				{
					Query: "DROP FUNCTION func2;", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-function-test-testdropfunction-0021-drop-function-func2", Compare: "sqlstate"},
				},
				{
					Query: "DROP FUNCTION func3;", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-function-test-testdropfunction-0022-drop-function-func3",

						// TODO: Postgres supports specifying multiple functions to drop, but our
						//       parser doesn't seem to support parsing multiple functions yet.
						Compare: "sqlstate"},
				},
			},
		},
		{

			Skip: true,
			Name: "Multiple functions",
			SetUpScript: []string{`
CREATE FUNCTION func1() RETURNS TEXT AS $$
BEGIN
	RETURN 'func1';
END;
$$ LANGUAGE plpgsql;`, `
CREATE FUNCTION func2(input TEXT) RETURNS TEXT AS $$
BEGIN
	RETURN 'func2(TEXT)';
END;
$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT func1(), func2('foo');", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-function-test-testdropfunction-0023-select-func1-func2-foo"},
				},
				{
					Query: "DELETE FUNCTION func1, func2(TExT);", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-function-test-testdropfunction-0024-delete-function-func1-func2-text", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "Overloaded functions",
			SetUpScript: []string{`
CREATE FUNCTION func2(input TEXT) RETURNS TEXT AS $$
BEGIN
	RETURN 'func2(TEXT)';
END;
$$ LANGUAGE plpgsql;`, `
CREATE FUNCTION func2(input INT) RETURNS TEXT AS $$
BEGIN
	RETURN 'func2(INT)';
END;
$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT func2('foo'), func2(42);", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-function-test-testdropfunction-0025-select-func2-foo-func2-42"},
				},
				{
					Query: "DROP FUNCTION func2(TEXT);", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-function-test-testdropfunction-0026-drop-function-func2-text"},
				},
				{
					Query: "SELECT func2('foo'::text);", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-function-test-testdropfunction-0027-select-func2-foo-::text", Compare: "sqlstate"},
				},
				{
					Query: "SELECT func2(42);", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-function-test-testdropfunction-0028-select-func2-42"},
				},
				{
					Query: "DROP FUNCTION func2(INT);", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-function-test-testdropfunction-0029-drop-function-func2-int"},
				},
				{
					Query: "SELECT func2(42);", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-function-test-testdropfunction-0030-select-func2-42", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "drop function with empty search_path",
			SetUpScript: []string{
				`SELECT pg_catalog.set_config('search_path', '', false);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP FUNCTION IF EXISTS public.vmstate(s integer);`, PostgresOracle: ScriptTestPostgresOracle{ID: "drop-function-test-testdropfunction-0031-drop-function-if-exists-public.vmstate"},
				},
			},
		},
		{
			Name: "user defined type used as parameter",
			SetUpScript: []string{
				`CREATE TABLE public.trans (vmid integer NOT NULL);`,
				`CREATE FUNCTION public.tax_job_trans(t public.trans) RETURNS public.trans
    LANGUAGE plpgsql
    AS '
BEGIN
    SELECT * FROM public.trans;
END;
';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP FUNCTION IF EXISTS public.tax_job_trans(t public.trans);`, PostgresOracle: ScriptTestPostgresOracle{ID: "drop-function-test-testdropfunction-0032-drop-function-if-exists-public.tax_job_trans"},
				},
			},
		},
		{
			Name: "drop non existing function with non existing type",
			Assertions: []ScriptTestAssertion{
				{
					Query: "DROP FUNCTION IF EXISTS public.tax_job_trans(t public.trans);", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-function-test-testdropfunction-0033-drop-function-if-exists-public.tax_job_trans"},
				},
			},
		},
	})
}
