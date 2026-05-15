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

package postgres16

import (
	. "github.com/dolthub/doltgresql/testing/go"
	"testing"
)

func TestIssues(t *testing.T) {
	RunScripts(
		t,
		[]ScriptTest{
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
						}, PostgresOracle: ScriptTestPostgresOracle{ID: "issues-test-testissues-0007-select-entities-.-project_id-entities"},
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
						Query: `UPDATE t2 SET t1a=ROW((t1a).a+100, (t1a).c)::t1a WHERE length(t1a::text) > 0;`, PostgresOracle: ScriptTestPostgresOracle{ID: "issues-test-testissues-0029-update-t2-set-t1a=row-t1a"},
					},
					{
						Query: `UPDATE t2 SET t1b=ROW((t1b).@1+100, (t1b).@2)::t1b WHERE length(t1b::text) > 0;`, PostgresOracle: ScriptTestPostgresOracle{ID: "issues-test-testissues-0030-update-t2-set-t1b=row-t1b", Compare: "sqlstate"},
					},
					{
						Query: `SELECT * FROM t2 ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "issues-test-testissues-0031-select-*-from-t2-order"},
					},
					{
						Query: `SELECT (id).a FROM t2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "issues-test-testissues-0032-select-id-.a-from-t2", Compare: "sqlstate"},
					},
					{
						Query: `SELECT (t1a).g FROM t2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "issues-test-testissues-0033-select-t1a-.g-from-t2", Compare: "sqlstate"},
					},
					{
						Query: `SELECT (t1a).@0 FROM t2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "issues-test-testissues-0034-select-t1a-.@0-from-t2", Compare: "sqlstate"},
					},
					{
						Query: `SELECT (t1a).@3 FROM t2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "issues-test-testissues-0035-select-t1a-.@3-from-t2", Compare: "sqlstate"},
					},
					{
						Query: `ALTER TABLE t1a ADD COLUMN d VARCHAR(10) DEFAULT 'abc';`, PostgresOracle: ScriptTestPostgresOracle{ID: "issues-test-testissues-0036-alter-table-t1a-add-column", Compare: "sqlstate"},
					},
					{
						Query: `ALTER TABLE t1a ADD COLUMN d VARCHAR(10);`, PostgresOracle: ScriptTestPostgresOracle{ID: "issues-test-testissues-0037-alter-table-t1a-add-column"},
					},
					{
						Query: `ALTER TABLE t1a DROP COLUMN c;`, PostgresOracle: ScriptTestPostgresOracle{ID: "issues-test-testissues-0038-alter-table-t1a-drop-column"},
					},
					{
						Query: `SELECT * FROM t2 ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "issues-test-testissues-0039-select-*-from-t2-order"},
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
		},
	)
}
