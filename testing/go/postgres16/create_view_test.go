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

	"github.com/dolthub/go-mysql-server/sql"
)

func TestCreateViewStatements(t *testing.T) {
	RunScripts(t, createViewStmts)
}

var createViewStmts = []ScriptTest{
	{
		Name: "basic create view statements",
		SetUpScript: []string{
			"create table t1 (pk int);",
			"insert into t1 values (1), (2), (3), (1);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "create view v as select * from t1 order by pk;", PostgresOracle: ScriptTestPostgresOracle{ID: "create-view-test-testcreateviewstatements-0001-create-view-v-as-select"},
			},
			{
				Query: "select * from v order by pk;", PostgresOracle: ScriptTestPostgresOracle{ID: "create-view-test-testcreateviewstatements-0002-select-*-from-v-order"},
			},
		},
	},
	{
		Name: "views on different schemas",
		SetUpScript: []string{
			"CREATE SCHEMA testschema;",
			"SET search_path TO testschema;",
			"CREATE TABLE testing (pk INT primary key, v2 TEXT);",
			"INSERT INTO testing VALUES (1,'a'), (2,'b'), (3,'c');",
			"CREATE VIEW testview AS SELECT * FROM testing;",
			"CREATE SCHEMA myschema;",
			"SET search_path TO myschema;",
			"CREATE TABLE mytable (pk INT primary key, v1 INT);",
			"INSERT INTO mytable VALUES (1,4), (2,5), (3,6);",
			"CREATE VIEW myview AS SELECT * FROM mytable;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW search_path;", PostgresOracle: ScriptTestPostgresOracle{ID: "create-view-test-testcreateviewstatements-0003-show-search_path"},
			},
			{
				Query: "select v1 from myview order by pk;", PostgresOracle: ScriptTestPostgresOracle{ID: "create-view-test-testcreateviewstatements-0004-select-v1-from-myview-order"},
			},
			{
				Query: "select v2 from testview order by pk;", PostgresOracle: ScriptTestPostgresOracle{ID: "create-view-test-testcreateviewstatements-0005-select-v2-from-testview-order", Compare: "sqlstate"},
			},
			{
				Query: "select v2 from testschema.testview order by pk;", PostgresOracle: ScriptTestPostgresOracle{ID: "create-view-test-testcreateviewstatements-0006-select-v2-from-testschema.testview-order"},
			},
			{
				Query:    "select name from dolt_schemas;",
				Expected: []sql.Row{{"myview"}},
			},
			{
				Query: "SET search_path = 'testschema';",
			},
			{
				Query: "SHOW search_path;", PostgresOracle: ScriptTestPostgresOracle{ID: "create-view-test-testcreateviewstatements-0008-show-search_path"},
			},
			{
				Query: "select * from myview order by pk; /* err */", PostgresOracle: ScriptTestPostgresOracle{ID: "create-view-test-testcreateviewstatements-0009-select-*-from-myview-order", Compare: "sqlstate"},
			},
			{
				Query: "select v1 from myschema.myview order by pk;", PostgresOracle: ScriptTestPostgresOracle{ID: "create-view-test-testcreateviewstatements-0010-select-v1-from-myschema.myview-order"},
			},
			{
				Query: "select v2 from testview order by pk;", PostgresOracle: ScriptTestPostgresOracle{ID: "create-view-test-testcreateviewstatements-0011-select-v2-from-testview-order"},
			},
			{
				Query:    "select name from dolt_schemas;",
				Expected: []sql.Row{{"testview"}},
			},
			{
				Query:    "SET search_path = testschema, myschema;",
				Expected: []sql.Row{},
			},
			{
				Query:    "SHOW search_path;",
				Expected: []sql.Row{{"testschema, myschema"}},
			},
			{
				Skip:     true, // TODO: Should be able to resolve views from all schema in search_path
				Query:    "select v1 from myview order by pk;",
				Expected: []sql.Row{{4}, {5}, {6}},
			},
			{
				Query:    "select v2 from testview order by pk;",
				Expected: []sql.Row{{"a"}, {"b"}, {"c"}},
			},
			{
				Skip:     true, // TODO: Should be able to resolve views from all schema in search_path
				Query:    "select name from dolt_schemas;",
				Expected: []sql.Row{{"testview"}, {"myview"}},
			},
		},
	},
	{
		Name: "create view from view",
		SetUpScript: []string{
			"create table t1 (pk int);",
			"insert into t1 values (1), (2), (3), (1);",
			"create view v as select * from t1 where pk > 1;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "create view v1 as select * from v order by pk;", PostgresOracle: ScriptTestPostgresOracle{ID: "create-view-test-testcreateviewstatements-0018-create-view-v1-as-select"},
			},
			{
				Query: "select * from v1 order by pk;", PostgresOracle: ScriptTestPostgresOracle{ID: "create-view-test-testcreateviewstatements-0019-select-*-from-v1-order"},
			},
		},
	},
	{
		Name: "view with expression name",
		SetUpScript: []string{
			"create view v as select 2+2",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * from v;", PostgresOracle: ScriptTestPostgresOracle{ID: "create-view-test-testcreateviewstatements-0020-select-*-from-v"},
			},
		},
	},
	{
		Name: "view with column names",
		SetUpScript: []string{
			`CREATE TABLE xy (x int primary key, y int);`,
			`insert into xy values (1, 4), (4, 9)`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "create view v_today(today) as select 2", PostgresOracle: ScriptTestPostgresOracle{ID: "create-view-test-testcreateviewstatements-0021-create-view-v_today-today-as"},
			},
			{
				Query: "CREATE VIEW xyv (u,v) AS SELECT * from xy", PostgresOracle: ScriptTestPostgresOracle{ID: "create-view-test-testcreateviewstatements-0022-create-view-xyv-u-v"},
			},
			{
				Query: "SELECT v from xyv;", PostgresOracle: ScriptTestPostgresOracle{ID: "create-view-test-testcreateviewstatements-0023-select-v-from-xyv"},
			},
			{
				Query: "SELECT today from v_today;", PostgresOracle: ScriptTestPostgresOracle{ID: "create-view-test-testcreateviewstatements-0024-select-today-from-v_today"},
			},
		},
	},
	{
		Skip: true, // TODO: getting subquery alias not supported error
		Name: "nested view",
		SetUpScript: []string{
			"create table t1 (pk int);",
			"insert into t1 values (1), (2), (3), (4);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "create view unionView as (select * from t1 order by pk desc limit 1) union all (select * from t1 order by pk limit 1)", PostgresOracle: ScriptTestPostgresOracle{ID: "create-view-test-testcreateviewstatements-0025-create-view-unionview-as-select"},
			},
			{
				Query: "select * from unionView order by pk;", PostgresOracle: ScriptTestPostgresOracle{ID: "create-view-test-testcreateviewstatements-0026-select-*-from-unionview-order"},
			},
		},
	},
	{
		Name: "cast (postgres-specific syntax)",
		SetUpScript: []string{
			"create table t1 (pk int);",
			"insert into t1 values (1), (2), (3), (4);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "CREATE VIEW v AS SELECT pk::INT2 FROM t1 ORDER BY pk;", PostgresOracle: ScriptTestPostgresOracle{ID: "create-view-test-testcreateviewstatements-0027-create-view-v-as-select"},
			},
			{
				Query: "select * from v order by pk;", PostgresOracle: ScriptTestPostgresOracle{ID: "create-view-test-testcreateviewstatements-0028-select-*-from-v-order"},
			},
			{
				Query: "CREATE VIEW v_text AS SELECT pk::int2, (pk)::text AS pk_text FROM t1;", PostgresOracle: ScriptTestPostgresOracle{ID: "create-view-test-testcreateviewstatements-0029-create-view-v_text-as-select"},
			},
			{
				Query: "select pk_text from v_text order by pk;", PostgresOracle: ScriptTestPostgresOracle{ID: "create-view-test-testcreateviewstatements-0030-select-pk_text-from-v_text-order"},
			},
		},
	},
	{
		Name: "not yet supported create view queries",
		Assertions: []ScriptTestAssertion{
			{
				Query: "CREATE TEMPORARY VIEW v AS SELECT 1;",
				Skip:  true,
			},
			{
				Query: "CREATE RECURSIVE VIEW v AS SELECT 1;",
				Skip:  true,
			},
			{
				Query: "CREATE VIEW v AS SELECT 1 WITH LOCAL CHECK OPTION;",
				Skip:  true,
			},
			{
				Query: "CREATE VIEW v WITH (check_option = 'local') AS SELECT 1;",
				Skip:  true,
			},
			{
				Query: "CREATE VIEW v WITH (security_barrier = true) AS SELECT 1;",
				Skip:  true,
			},
		},
	},
	{
		Name: "create view with CTE",
		SetUpScript: []string{
			"CREATE TABLE public.t1 (id integer NOT NULL);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `create view public.v1 as with table1 as (select * from t1) select id from table1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-view-test-testcreateviewstatements-0031-create-view-public.v1-as-with", Cleanup: []string{"DROP TABLE IF EXISTS public.t1 CASCADE"}},
			},
		},
	},
	{
		Name: "create view with custom type in its select statement",
		SetUpScript: []string{
			"CREATE TYPE e AS ENUM ('sched', 'busy', 'final', 'help');",
			"CREATE TABLE t (id integer NOT NULL, t e);",
			"INSERT INTO t VALUES (1, 'busy'), (2, 'final'), (3, 'busy'), (4, 'help');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `create view v as select * from t where (t = 'busy'::e);`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-view-test-testcreateviewstatements-0032-create-view-v-as-select"},
			},
			{
				Query: `select * from v;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-view-test-testcreateviewstatements-0033-select-*-from-v"},
			},
		},
	},
}
