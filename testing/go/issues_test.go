/// Copyright 2023 Dolthub, Inc.
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
	"github.com/jackc/pgx/v5/pgproto3"
)

func TestIssues(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "Issue #25",
			SetUpScript: []string{
				"create table tbl (pk int);",
				"insert into tbl values (1);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `select dolt_add(".");`,
					ExpectedErr: "could not be found in any table in scope",
				},
				{
					Query:    `select dolt_add('.');`,
					Expected: []sql.Row{{"{0}"}},
				},
				{
					Query:       `select dolt_commit("-m", "look ma");`,
					ExpectedErr: "could not be found in any table in scope",
				},
				{
					Query:    `select length(dolt_commit('-m', 'look ma')::text);`,
					Expected: []sql.Row{{34}},
				},
				{
					Query:       `select dolt_branch("br1");`,
					ExpectedErr: "could not be found in any table in scope",
				},
				{
					Query:    `select dolt_branch('br1');`,
					Expected: []sql.Row{{"{0}"}},
				},
			},
		},
		{
			Name: "Issue #2030",
			SetUpScript: []string{
				`CREATE TABLE sub_entities (
  project_id VARCHAR(256) NOT NULL,
  entity_id  VARCHAR(256) NOT NULL,
  id         VARCHAR(256) NOT NULL,
  name       VARCHAR(256) NOT NULL,
  PRIMARY KEY (project_id, entity_id, id)
);
`,
				`
CREATE TABLE entities (
  project_id              VARCHAR(256) NOT NULL,
  id                      VARCHAR(256) NOT NULL,
  name                    VARCHAR(256) NOT NULL,
  default_sub_entity_id   VARCHAR(256),
  PRIMARY KEY (project_id, id)
);
`,
				`
CREATE TABLE conversations (
  id                 VARCHAR(256) NOT NULL,
  tenant_id          VARCHAR(256) NOT NULL,
  project_id         VARCHAR(256) NOT NULL,
  active_sub_agent_id VARCHAR(256) NOT NULL,
  PRIMARY KEY (tenant_id, project_id, id)
);
`,
				`INSERT INTO sub_entities (project_id, entity_id, id, name) VALUES
  ('projectA', 'entityA', 'subA1', 'Sub-Entity A1'),
  ('projectA', 'entityB', 'subB1', 'Sub-Entity B1');
`,
				`INSERT INTO entities (project_id, id, name, default_sub_entity_id) VALUES
  ('projectA', 'entityA', 'Entity A', 'subA1'),
  ('projectA', 'entityB', 'Entity B', 'subB1');
`,
				`INSERT INTO conversations (tenant_id, project_id, id, active_sub_agent_id) VALUES
  ('tenant1', 'projectA', 'conv1', 'subA1'),
  ('tenant1', 'projectA', 'conv2', 'subB1');
`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `select
  "entities"."project_id",
  "entities"."id",
  "entities"."name",
  "entities"."default_sub_entity_id",
  "entities_defaultSubEntity"."data" as "defaultSubEntity"
from "entities" "entities"
left join lateral (
  select json_build_array(
           "entities_defaultSubEntity"."project_id",
           "entities_defaultSubEntity"."entity_id",
           "entities_defaultSubEntity"."id",
           "entities_defaultSubEntity"."name"
         ) as "data"
  from (
    select * from "sub_entities" "entities_defaultSubEntity"
    where "entities_defaultSubEntity"."id" = "entities"."default_sub_entity_id"
    limit $1
  ) "entities_defaultSubEntity"
) "entities_defaultSubEntity" on true
where ("entities"."project_id" = $2 and "entities"."id" = $3)
limit $4`,
					BindVars: []any{
						int64(1),
						"projectA",
						"entityA",
						int64(1),
					},
					Expected: []sql.Row{
						{
							"projectA",
							"entityA",
							"Entity A",
							"subA1",
							`["projectA", "entityA", "subA1", "Sub-Entity A1"]`,
						},
					},
				},
				{
					Query: `select
  "entities"."project_id",
  "entities"."id",
  "entities"."name",
  "entities"."default_sub_entity_id",
  "entities_defaultSubEntity"."data" as "defaultSubEntity"
from "entities" "entities"
left join lateral (
  select json_build_array(
           "entities_defaultSubEntity"."project_id",
           "entities_defaultSubEntity"."entity_id",
           "entities_defaultSubEntity"."id",
           "entities_defaultSubEntity"."name"
         ) as "data"
  from (
    select * from "sub_entities" "entities_defaultSubEntity"
    where "entities_defaultSubEntity"."id" = "entities"."default_sub_entity_id"
    limit 1
  ) "entities_defaultSubEntity"
) "entities_defaultSubEntity" on true
where ("entities"."project_id" = 'projectA' and "entities"."id" = 'entityA')
limit 1`, PostgresOracle: ScriptTestPostgresOracle{ID: "issues-test-testissues-0008-select-entities-.-project_id-entities"},
				},
			},
		},
		{
			Name: "Issue #2049",
			SetUpScript: []string{
				`CREATE TABLE jsonb_test (id VARCHAR(256) NOT NULL PRIMARY KEY, "jsonbColumn" JSONB);`,
				`INSERT INTO jsonb_test VALUES ('test', '{"test": "value\n"}');`,
				`INSERT INTO jsonb_test VALUES ('test2', '{"test": "value\t"}');`,
				`INSERT INTO jsonb_test VALUES ('test3', '{"test": "value\r"}');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT * FROM jsonb_test;",
					// The pgx library incorrectly reinterprets our JSON value by replacing the individual newline
					// characters (ASCII 92,110) with the actual newline character (ASCII 10), which is incorrect for us.
					// Therefore, we have to use the raw returned values. To make it more clear, we aren't using a raw
					// string literal and instead escaping the characters in the byte slice. We also test other escape
					// characters that are replaced.
					ExpectedRaw: [][][]byte{
						{[]byte("test"), []byte("{\"test\": \"value\\n\"}")},
						{[]byte("test2"), []byte("{\"test\": \"value\\t\"}")},
						{[]byte("test3"), []byte("{\"test\": \"value\\r\"}")},
					},
				},
			},
		},
		{
			Name: "Issue #2197 Part 1",
			SetUpScript: []string{
				`CREATE TABLE t1 (a INT, b VARCHAR(3));`,
				`CREATE TABLE t2(id SERIAL, t1 t1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO t2(t1) VALUES (ROW(1, 'abc'));`, PostgresOracle: ScriptTestPostgresOracle{ID: "issues-test-testissues-0010-insert-into-t2-t1-values"},
				},
				{
					Query: `SELECT * FROM t2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "issues-test-testissues-0011-select-*-from-t2"},
				},
				{
					Query: `INSERT INTO t2(t1) VALUES (ROW('a', 'def'));`, PostgresOracle: ScriptTestPostgresOracle{ID: "issues-test-testissues-0012-insert-into-t2-t1-values", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO t2(t1) VALUES (ROW(true, 'def'));`, PostgresOracle: ScriptTestPostgresOracle{ID: "issues-test-testissues-0013-insert-into-t2-t1-values", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO t2(t1) VALUES (ROW(2, 'def', 'ghi'));`, PostgresOracle: ScriptTestPostgresOracle{ID: "issues-test-testissues-0014-insert-into-t2-t1-values", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO t2(t1) VALUES (ROW(2));`, PostgresOracle: ScriptTestPostgresOracle{ID: "issues-test-testissues-0015-insert-into-t2-t1-values", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "Issue #2197 Part 2",
			SetUpScript: []string{
				`CREATE TABLE t1a (a INT4, b VARCHAR(3));`,
				`CREATE TABLE t1b (a INT4 NOT NULL, b VARCHAR(3) NOT NULL);`,
				`CREATE TABLE t2 (id SERIAL, t1a t1a, t1b t1b);`,
				`INSERT INTO t2 (t1a) VALUES (ROW(1, 'abc'));`,
				`INSERT INTO t2 (t1b) VALUES (ROW(1, 'abc'));`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM t2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "issues-test-testissues-0016-select-*-from-t2"},
				},
				{
					Query: `ALTER TABLE t1a ADD COLUMN c VARCHAR(10);`, PostgresOracle: ScriptTestPostgresOracle{ID: "issues-test-testissues-0017-alter-table-t1a-add-column"},
				},
				{
					Query: `ALTER TABLE t1b ADD COLUMN c VARCHAR(10) NOT NULL;`, PostgresOracle: ScriptTestPostgresOracle{ID: "issues-test-testissues-0018-alter-table-t1b-add-column"},
				},
				{
					Query: `SELECT * FROM t2 ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "issues-test-testissues-0019-select-*-from-t2-order"},
				},
				{
					Query: `ALTER TABLE t1a DROP COLUMN b;`, PostgresOracle: ScriptTestPostgresOracle{ID: "issues-test-testissues-0020-alter-table-t1a-drop-column"},
				},
				{
					Query: `ALTER TABLE t1b DROP COLUMN b;`, PostgresOracle: ScriptTestPostgresOracle{ID: "issues-test-testissues-0021-alter-table-t1b-drop-column"},
				},
				{
					Query: `SELECT * FROM t2 ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "issues-test-testissues-0022-select-*-from-t2-order"},
				},
				{
					Query: `INSERT INTO t1a VALUES (2, 'def');`, PostgresOracle: ScriptTestPostgresOracle{ID: "issues-test-testissues-0023-insert-into-t1a-values-2"},
				},
				{
					Query: `INSERT INTO t1b VALUES (3, 'xyzzy');`, PostgresOracle: ScriptTestPostgresOracle{ID: "issues-test-testissues-0024-insert-into-t1b-values-3"},
				},
				{
					Query: `INSERT INTO t2 (t1a) SELECT ROW(a,c)::t1a FROM t1a;`, PostgresOracle: ScriptTestPostgresOracle{ID: "issues-test-testissues-0025-insert-into-t2-t1a-select"},
				},
				{
					Query: `INSERT INTO t2 (t1b) SELECT ROW(a,c)::t1b FROM t1b;`, PostgresOracle: ScriptTestPostgresOracle{ID: "issues-test-testissues-0026-insert-into-t2-t1b-select"},
				},
				{
					Query: `SELECT * FROM t2 ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "issues-test-testissues-0027-select-*-from-t2-order"},
				},
				{
					Query: `SELECT ((t1a).@1), ((t1b).@2) FROM t2 ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "issues-test-testissues-0028-select-t1a-.@1-t1b-.@2", Compare: "sqlstate"},
				},
				{
					Query:    `UPDATE t2 SET t1a=ROW((t1a).a+100, (t1a).c)::t1a WHERE length(t1a::text) > 0;`,
					Expected: []sql.Row{},
				},
				{
					Query:    `UPDATE t2 SET t1b=ROW((t1b).@1+100, (t1b).@2)::t1b WHERE length(t1b::text) > 0;`,
					Expected: []sql.Row{},
				},
				{
					Query: `SELECT * FROM t2 ORDER BY id;`,
					Expected: []sql.Row{
						{1, "(101,)", nil},
						{2, nil, "(101,)"},
						{3, "(102,def)", nil},
						{4, nil, "(103,xyzzy)"},
					},
				},
				{
					Query:       `SELECT (id).a FROM t2;`,
					ExpectedErr: "column notation .a applied to type",
				},
				{
					Query:       `SELECT (t1a).g FROM t2;`,
					ExpectedErr: `column "g" not found in data type`,
				},
				{
					Query:       `SELECT (t1a).@0 FROM t2;`,
					ExpectedErr: "out of bounds",
				},
				{
					Query:       `SELECT (t1a).@3 FROM t2;`,
					ExpectedErr: "out of bounds",
				},
				{
					Query:       `ALTER TABLE t1a ADD COLUMN d VARCHAR(10) DEFAULT 'abc';`,
					ExpectedErr: `cannot alter table "t1a" because column "t2.t1a" uses its row type`,
				},
				{
					Query:    `ALTER TABLE t1a ADD COLUMN d VARCHAR(10);`,
					Expected: []sql.Row{},
				},
				{
					Query:    `ALTER TABLE t1a DROP COLUMN c;`,
					Expected: []sql.Row{},
				},
				{
					Query: `SELECT * FROM t2 ORDER BY id;`,
					Expected: []sql.Row{
						{1, "(101,)", nil},
						{2, nil, "(101,)"},
						{3, "(102,)", nil},
						{4, nil, "(103,xyzzy)"},
					},
				},
			},
		},
		{
			Name: "Issue #2299",
			SetUpScript: []string{
				"CREATE TYPE team_role AS ENUM ('admin', 'editor', 'member');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE users (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), role team_role NOT NULL DEFAULT 'member');`, PostgresOracle: ScriptTestPostgresOracle{ID: "issues-test-testissues-0040-create-table-users-id-uuid"},
				},
				{
					Query: `INSERT INTO users (role) VALUES (DEFAULT);`, PostgresOracle: ScriptTestPostgresOracle{ID: "issues-test-testissues-0041-insert-into-users-role-values"},
				},
				{
					Query: `SELECT role FROM users;`, PostgresOracle: ScriptTestPostgresOracle{ID: "issues-test-testissues-0042-select-role-from-users"},
				},
			},
		},
		{
			Name: "Issue #2307",
			SetUpScript: []string{
				"CREATE TABLE test (pk INT4);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_tables WHERE tablename = 'test');`, PostgresOracle: ScriptTestPostgresOracle{ID: "issues-test-testissues-0043-select-exists-select-1-from"},
				},
				{
					Query: `SELECT NOT EXISTS(SELECT 1 FROM pg_catalog.pg_tables WHERE tablename = 'test');`, PostgresOracle: ScriptTestPostgresOracle{ID: "issues-test-testissues-0044-select-not-exists-select-1"},
				},
			},
		},
		{
			Name: "Issue #2548",
			SetUpScript: []string{
				"CREATE TABLE test (pk INT4 PRIMARY KEY, v1 TIMESTAMP WITH TIME ZONE);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SET TimeZone = 'UTC-01:00';`, PostgresOracle: ScriptTestPostgresOracle{ID: "issues-test-testissues-0045-set-timezone-=-utc-01:00"},
				},
				{
					Query: `INSERT INTO test VALUES (1, '2026-04-15 10:11:12');`, PostgresOracle: ScriptTestPostgresOracle{ID: "issues-test-testissues-0046-insert-into-test-values-1"},
				},
				{
					Query: `SET TimeZone = 'UTC-03:00';`, PostgresOracle: ScriptTestPostgresOracle{ID: "issues-test-testissues-0047-set-timezone-=-utc-03:00"},
				},
				{
					Query: `INSERT INTO test VALUES (2, '2026-04-15 10:11:12');`, PostgresOracle: ScriptTestPostgresOracle{ID: "issues-test-testissues-0048-insert-into-test-values-2"},
				},
				{
					Query: `SELECT (SELECT v1 FROM test WHERE pk = 2) - (SELECT v1 FROM test WHERE pk = 1);`, PostgresOracle: ScriptTestPostgresOracle{ID: "issues-test-testissues-0049-select-select-v1-from-test"},
				},
			},
		},
		{
			Name: "Issue #2604",
			SetUpScript: []string{
				"CREATE TABLE t (id INT PRIMARY KEY, a TEXT, b TEXT DEFAULT 'x');",
				"CREATE UNIQUE INDEX idx_t_a ON t(a);",
				"SELECT dolt_add('-A');",
				"SELECT dolt_commit('-m', 'schema');",
				"SELECT dolt_branch('f', 'main');",
				"SELECT dolt_checkout('f');",
				"INSERT INTO t (id, a) VALUES (1, 'feat');",
				"SELECT dolt_add('-A');",
				"SELECT dolt_commit('-m', 'feat');",
				"SELECT dolt_checkout('main');",
				"INSERT INTO t (id, a) VALUES (2, 'main');",
				"SELECT dolt_add('-A');",
				"SELECT dolt_commit('-m', 'main');",
				"SELECT dolt_checkout('f');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    "SELECT length(dolt_merge('main')::text) = 57;",
					Expected: []sql.Row{{"t"}},
				},
			},
		},
	})
}

func TestIssuesWire(t *testing.T) {
	RunWireScripts(t, []WireScriptTest{
		{
			Name: "Issue #2546",
			Assertions: []WireScriptTestAssertion{
				{
					Send: []pgproto3.FrontendMessage{
						&pgproto3.Query{String: "SELECT 'foo';"},
					},
					Receive: []pgproto3.BackendMessage{
						&pgproto3.RowDescription{
							Fields: []pgproto3.FieldDescription{
								{
									Name:                 []byte("?column?"),
									TableOID:             0,
									TableAttributeNumber: 0,
									DataTypeOID:          25,
									DataTypeSize:         -1,
									TypeModifier:         -1,
									Format:               0,
								},
							},
						},
						&pgproto3.DataRow{Values: [][]byte{[]byte("foo")}},
						&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")},
						&pgproto3.ReadyForQuery{TxStatus: 'I'},
					},
				},
			},
		},
		{
			Name: "Issue #2557",
			Assertions: []WireScriptTestAssertion{
				{
					Send: []pgproto3.FrontendMessage{
						&pgproto3.Parse{
							Name:  "stmt_name1",
							Query: `SELECT '{"v":"a\\nb"}'::jsonb;`,
						},
						&pgproto3.Sync{},
					},
					Receive: []pgproto3.BackendMessage{
						&pgproto3.ParseComplete{},
						&pgproto3.ReadyForQuery{TxStatus: 'I'},
					},
				},
				{
					Send: []pgproto3.FrontendMessage{
						&pgproto3.Bind{
							PreparedStatement: "stmt_name1",
							ResultFormatCodes: []int16{0},
						},
						&pgproto3.Execute{},
						&pgproto3.Close{
							ObjectType: 'P',
						},
						&pgproto3.Sync{},
					},
					Receive: []pgproto3.BackendMessage{
						&pgproto3.BindComplete{},
						&pgproto3.DataRow{
							Values: [][]byte{
								[]byte(`{"v": "a\\nb"}`),
							},
						},
						&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")},
						&pgproto3.CloseComplete{},
						&pgproto3.ReadyForQuery{TxStatus: 'I'},
					},
				},
				{
					Send: []pgproto3.FrontendMessage{
						&pgproto3.Parse{
							Name:  "stmt_name2",
							Query: `SELECT $${"v":"a\\nb"}$$::jsonb;`,
						},
						&pgproto3.Sync{},
					},
					Receive: []pgproto3.BackendMessage{
						&pgproto3.ParseComplete{},
						&pgproto3.ReadyForQuery{TxStatus: 'I'},
					},
				},
				{
					Send: []pgproto3.FrontendMessage{
						&pgproto3.Bind{
							PreparedStatement: "stmt_name2",
							ResultFormatCodes: []int16{0},
						},
						&pgproto3.Execute{},
						&pgproto3.Close{
							ObjectType: 'P',
						},
						&pgproto3.Sync{},
					},
					Receive: []pgproto3.BackendMessage{
						&pgproto3.BindComplete{},
						&pgproto3.DataRow{
							Values: [][]byte{
								[]byte(`{"v": "a\\nb"}`),
							},
						},
						&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")},
						&pgproto3.CloseComplete{},
						&pgproto3.ReadyForQuery{TxStatus: 'I'},
					},
				},
				{
					Send: []pgproto3.FrontendMessage{
						&pgproto3.Parse{
							Name:  "stmt_name3",
							Query: `SELECT $${"v":"a\\\nb"}$$::jsonb;`,
						},
						&pgproto3.Sync{},
					},
					Receive: []pgproto3.BackendMessage{
						&pgproto3.ParseComplete{},
						&pgproto3.ReadyForQuery{TxStatus: 'I'},
					},
				},
				{
					Send: []pgproto3.FrontendMessage{
						&pgproto3.Bind{
							PreparedStatement: "stmt_name3",
							ResultFormatCodes: []int16{0},
						},
						&pgproto3.Execute{},
						&pgproto3.Close{
							ObjectType: 'P',
						},
						&pgproto3.Sync{},
					},
					Receive: []pgproto3.BackendMessage{
						&pgproto3.BindComplete{},
						&pgproto3.DataRow{
							Values: [][]byte{
								[]byte(`{"v": "a\\\nb"}`),
							},
						},
						&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")},
						&pgproto3.CloseComplete{},
						&pgproto3.ReadyForQuery{TxStatus: 'I'},
					},
				},
				{
					Send: []pgproto3.FrontendMessage{
						&pgproto3.Parse{
							Name:  "stmt_name4",
							Query: `select json '{ "a":  "dollar \\u0024 character" }' ->> 'a' as not_an_escape;`,
						},
						&pgproto3.Sync{},
					},
					Receive: []pgproto3.BackendMessage{
						&pgproto3.ParseComplete{},
						&pgproto3.ReadyForQuery{TxStatus: 'I'},
					},
				},
				{
					Send: []pgproto3.FrontendMessage{
						&pgproto3.Bind{
							PreparedStatement: "stmt_name4",
							ResultFormatCodes: []int16{0},
						},
						&pgproto3.Execute{},
						&pgproto3.Close{
							ObjectType: 'P',
						},
						&pgproto3.Sync{},
					},
					Receive: []pgproto3.BackendMessage{
						&pgproto3.BindComplete{},
						&pgproto3.DataRow{
							Values: [][]byte{
								[]byte(`dollar $ character`),
							},
						},
						&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")},
						&pgproto3.CloseComplete{},
						&pgproto3.ReadyForQuery{TxStatus: 'I'},
					},
				},
			},
		},
	})
}
