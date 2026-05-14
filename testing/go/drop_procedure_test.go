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

func TestDropProcedure(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "Procedure does not exist",
			Assertions: []ScriptTestAssertion{
				{
					Query: "DROP PROCEDURE doesnotexist;", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-procedure-test-testdropprocedure-0001-drop-procedure-doesnotexist", Compare: "sqlstate"},
				},
				{
					Query: "DROP PROCEDURE IF EXISTS doesnotexist;", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-procedure-test-testdropprocedure-0002-drop-procedure-if-exists-doesnotexist"},
				},
			},
		},
		{
			Name: "Basic cases",
			SetUpScript: []string{
				`CREATE PROCEDURE proc1() AS $$ BEGIN NULL; END; $$ LANGUAGE plpgsql;`,
				`CREATE PROCEDURE proc2(input INT) AS $$ BEGIN NULL; END; $$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "CALL proc1();", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-procedure-test-testdropprocedure-0003-call-proc1"},
				},
				{
					Query: "CALL proc2(99);", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-procedure-test-testdropprocedure-0004-call-proc2-99"},
				},
				{
					Query: "DROP PROCEDURE proc1;", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-procedure-test-testdropprocedure-0005-drop-procedure-proc1"},
				},
				{
					Query: "DROP PROCEDURE proc2(INT);", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-procedure-test-testdropprocedure-0006-drop-procedure-proc2-int"},
				},
				{
					Query: "CALL proc1();", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-procedure-test-testdropprocedure-0007-call-proc1", Compare: "sqlstate"},
				},
				{
					Query: "CALL proc2(99);", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-procedure-test-testdropprocedure-0008-call-proc2-99", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "Optional type information",
			SetUpScript: []string{
				`CREATE PROCEDURE proc1() AS $$ BEGIN NULL; END; $$ LANGUAGE plpgsql;`,
				`CREATE PROCEDURE proc2() AS $$ BEGIN NULL; END; $$ LANGUAGE plpgsql;`,
				`CREATE PROCEDURE proc3(input INT) AS $$ BEGIN NULL; END; $$ LANGUAGE plpgsql;`,
				`CREATE PROCEDURE proc4(input INT) AS $$ BEGIN NULL; END; $$ LANGUAGE plpgsql;`,
				`CREATE PROCEDURE proc5(input INT, foo TEXT) AS $$ BEGIN NULL; END; $$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "CALL proc1();", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-procedure-test-testdropprocedure-0009-call-proc1"},
				},
				{
					Query: "CALL proc2();", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-procedure-test-testdropprocedure-0010-call-proc2"},
				},
				{
					Query: "CALL proc3(1);", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-procedure-test-testdropprocedure-0011-call-proc3-1"},
				},
				{
					Query: "CALL proc4(2);", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-procedure-test-testdropprocedure-0012-call-proc4-2"},
				},
				{
					Query: "CALL proc5(3, 'abc');", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-procedure-test-testdropprocedure-0013-call-proc5-3-abc"},
				},
				{
					Query: "DROP PROCEDURE proc1(OUT TEXT);", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-procedure-test-testdropprocedure-0014-drop-procedure-proc1-out-text"},
				},
				{
					Query: "DROP PROCEDURE proc2(OUT paramname TEXT);", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-procedure-test-testdropprocedure-0015-drop-procedure-proc2-out-paramname"},
				},
				{
					Query: "DROP PROCEDURE proc3(paramname INT);", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-procedure-test-testdropprocedure-0016-drop-procedure-proc3-paramname-int"},
				},
				{
					Query: "DROP PROCEDURE proc4(IN paramname INT);", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-procedure-test-testdropprocedure-0017-drop-procedure-proc4-in-paramname"},
				},
				{
					Query: "DROP PROCEDURE proc5(IN paramname INT, IN paramname TEXT);", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-procedure-test-testdropprocedure-0018-drop-procedure-proc5-in-paramname"},
				},
				{
					Query: "CALL proc1();", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-procedure-test-testdropprocedure-0019-call-proc1", Compare: "sqlstate"},
				},
				{
					Query: "CALL proc2();", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-procedure-test-testdropprocedure-0020-call-proc2", Compare: "sqlstate"},
				},
				{
					Query: "CALL proc3(1);", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-procedure-test-testdropprocedure-0021-call-proc3-1", Compare: "sqlstate"},
				},
				{
					Query: "CALL proc4(2);", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-procedure-test-testdropprocedure-0022-call-proc4-2", Compare: "sqlstate"},
				},
				{
					Query: "CALL proc5(3, 'abc');", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-procedure-test-testdropprocedure-0023-call-proc5-3-abc", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "Qualified names",
			SetUpScript: []string{
				`CREATE PROCEDURE proc1() AS $$ BEGIN NULL; END; $$ LANGUAGE plpgsql;`,
				`CREATE PROCEDURE proc2(input TEXT) AS $$ BEGIN NULL; END; $$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT current_schema(), current_database();", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-procedure-test-testdropprocedure-0024-select-current_schema-current_database", ColumnModes: []string{"schema"}},
				},
				{
					Query: "CALL proc1();", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-procedure-test-testdropprocedure-0025-call-proc1"},
				},
				{
					Query: "CALL proc2('foo');", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-procedure-test-testdropprocedure-0026-call-proc2-foo"},
				},
				{
					Query: "DROP PROCEDURE public.proc1;", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-procedure-test-testdropprocedure-0027-drop-procedure-public.proc1", Compare: "sqlstate"},
				},
				{
					Query: "CALL proc1();", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-procedure-test-testdropprocedure-0028-call-proc1"},
				},
				{
					Query: "DROP PROCEDURE postgres.public.proc2(TEXT);", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-procedure-test-testdropprocedure-0029-drop-procedure-postgres.public.proc2-text", Compare: "sqlstate"},
				},
				{
					Query:       "CALL proc2('bar');",
					ExpectedErr: "does not exist",
				},
			},
		},
		{
			Name: "Unspecified parameter types",
			SetUpScript: []string{
				`CREATE PROCEDURE proc1(input1 TEXT, input2 TEXT) AS $$ BEGIN NULL; END; $$ LANGUAGE plpgsql;`,
				`CREATE PROCEDURE proc2(input1 TEXT) AS $$ BEGIN NULL; END; $$ LANGUAGE plpgsql;`,
				`CREATE PROCEDURE proc2(input1 TEXT, input2 TEXT) AS $$ BEGIN NULL; END; $$ LANGUAGE plpgsql;`,
				`CREATE PROCEDURE proc3(input1 TEXT, input2 TEXT) AS $$ BEGIN NULL; END; $$ LANGUAGE plpgsql;`,
				`CREATE PROCEDURE proc3() AS $$ BEGIN NULL; END; $$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "DROP PROCEDURE proc1;", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-procedure-test-testdropprocedure-0031-drop-procedure-proc1"},
				},
				{
					Query: "DROP PROCEDURE proc2;", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-procedure-test-testdropprocedure-0032-drop-procedure-proc2", Compare: "sqlstate"},
				},
				{
					Query: "DROP PROCEDURE proc3;", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-procedure-test-testdropprocedure-0033-drop-procedure-proc3", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "Overloaded procedures",
			SetUpScript: []string{
				`CREATE PROCEDURE proc2(input TEXT) AS $$ BEGIN NULL; END; $$ LANGUAGE plpgsql;`,
				`CREATE PROCEDURE proc2(input INT) AS $$ BEGIN NULL; END; $$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "CALL proc2('foo');", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-procedure-test-testdropprocedure-0034-call-proc2-foo"},
				},
				{
					Query: "CALL proc2(42);", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-procedure-test-testdropprocedure-0035-call-proc2-42"},
				},
				{
					Query: "DROP PROCEDURE proc2(TEXT);", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-procedure-test-testdropprocedure-0036-drop-procedure-proc2-text"},
				},
				{
					Query: "CALL proc2('foo'::text);", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-procedure-test-testdropprocedure-0037-call-proc2-foo-::text", Compare: "sqlstate"},
				},
				{
					Query: "CALL proc2(42);", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-procedure-test-testdropprocedure-0038-call-proc2-42"},
				},
				{
					Query: "DROP PROCEDURE proc2(INT);", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-procedure-test-testdropprocedure-0039-drop-procedure-proc2-int"},
				},
				{
					Query: "CALL proc2(42);", PostgresOracle: ScriptTestPostgresOracle{ID: "drop-procedure-test-testdropprocedure-0040-call-proc2-42", Compare: "sqlstate"},
				},
			},
		},
	})
}
