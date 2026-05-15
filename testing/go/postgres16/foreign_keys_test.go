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

func TestForeignKeys(t *testing.T) {
	RunScripts(
		t,
		[]ScriptTest{
			{
				Name: "simple foreign key",
				SetUpScript: []string{
					`CREATE TABLE parent (a INT PRIMARY KEY, b int)`,
					`CREATE TABLE child (a INT PRIMARY KEY, b INT, FOREIGN KEY (b) REFERENCES parent(a))`,
					`INSERT INTO parent VALUES (1, 1)`,
				},
				Assertions: []ScriptTestAssertion{
					{
						Query: "INSERT INTO child VALUES (1, 1)",
					},
					{
						Query: "INSERT INTO child VALUES (2, 1)",
					},
					{
						Query: "INSERT INTO child VALUES (2, 2)", PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-keys-test-testforeignkeys-0001-insert-into-child-values-2", Compare: "sqlstate"},
					},
				},
			},
			{
				Name: "named constraint",
				SetUpScript: []string{
					`CREATE TABLE parent (a INT PRIMARY KEY, b int)`,
					`CREATE TABLE child (a INT PRIMARY KEY, b INT)`,
					`INSERT INTO parent VALUES (1, 1)`,
					`ALTER TABLE child ADD CONSTRAINT fk123 FOREIGN KEY (b) REFERENCES parent(a)`,
				},
				Assertions: []ScriptTestAssertion{
					{
						Query: "INSERT INTO child VALUES (1, 1)",
					},
					{
						Query: "INSERT INTO child VALUES (2, 1)",
					},
					{
						Query: "INSERT INTO child VALUES (2, 2)", PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-keys-test-testforeignkeys-0002-insert-into-child-values-2", Compare: "sqlstate"},
					},
				},
			},
			{
				Name: "unnamed constraint",
				SetUpScript: []string{
					`CREATE TABLE parent (a INT PRIMARY KEY, b int)`,
					`CREATE TABLE child (a INT PRIMARY KEY, b INT)`,
					`INSERT INTO parent VALUES (1, 1)`,
					`ALTER TABLE child ADD FOREIGN KEY (b) REFERENCES parent(a)`,
				},
				Assertions: []ScriptTestAssertion{
					{
						Query: "INSERT INTO child VALUES (1, 1)",
					},
					{
						Query: "INSERT INTO child VALUES (2, 1)",
					},
					{
						Query: "INSERT INTO child VALUES (2, 2)", PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-keys-test-testforeignkeys-0003-insert-into-child-values-2", Compare: "sqlstate"},
					},
				},
			},
			{
				Name: "text foreign key",
				SetUpScript: []string{
					`CREATE TABLE parent (a text PRIMARY KEY, b int)`,
					`CREATE TABLE child (a INT PRIMARY KEY, b text, FOREIGN KEY (b) REFERENCES parent(a))`,
					`INSERT INTO parent VALUES ('a', 1)`,
				},
				Assertions: []ScriptTestAssertion{
					{
						Query: "INSERT INTO child VALUES (1, 'a')",
					},
					{
						Query: "INSERT INTO child VALUES (2, 'a')",
					},
					{
						Query: "INSERT INTO child VALUES (3, 'b')", PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-keys-test-testforeignkeys-0004-insert-into-child-values-3", Compare: "sqlstate"},
					},
				},
			},
			{
				Name: "type compatibility",
				SetUpScript: []string{
					`create table parent (i2 int2, i4 int4, i8 int8, f float, d double precision, v varchar, vl varchar(100), t text, j json, ts timestamp);`,
					"alter table parent add constraint u1 unique (i2);",
					"alter table parent add constraint u2 unique (i4);",
					"alter table parent add constraint u3 unique (i8);",
					"alter table parent add constraint u4 unique (d);",
					"alter table parent add constraint u5 unique (f);",
					"alter table parent add constraint u6 unique (v);",
					"alter table parent add constraint u7 unique (vl);",
					"alter table parent add constraint u8 unique (t);",
					"alter table parent add constraint u9 unique (ts);",
					`create table child (i2 int2, i4 int4, i8 int8, f float, d double precision, v varchar, vl varchar(100), t text, j json, ts timestamp);`,
					"insert into parent values (1, 1, 1, 1.0, 1.0, 'a', 'a', 'a', '{\"a\": 1}', '2021-01-01 00:00:00');",
				},
				Assertions: []ScriptTestAssertion{
					{
						Query: "alter table child add constraint fi2i2 foreign key (i2) references parent(i2)",
					},
					{
						Query: "alter table child add constraint fi2i4 foreign key (i2) references parent(i4)",
					},
					{
						Query: "alter table child add constraint fi2i8 foreign key (i2) references parent(i8);",
					},
					{
						Query: "alter table child add constraint fi2f foreign key (i2) references parent(f);",
					},
					{
						Query: "alter table child add constraint fi2d foreign key (i2) references parent(d);",
					},
					{
						Query: "alter table child add constraint fi2v foreign key (i2) references parent(v);", PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-keys-test-testforeignkeys-0005-alter-table-child-add-constraint", Compare: "sqlstate"},
					},
					{
						Query: "alter table child add constraint fi2vl foreign key (i2) references parent(vl);", PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-keys-test-testforeignkeys-0006-alter-table-child-add-constraint", Compare: "sqlstate"},
					},
					{
						Query: "alter table child add constraint fi2t foreign key (i2) references parent(t);", PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-keys-test-testforeignkeys-0007-alter-table-child-add-constraint", Compare: "sqlstate"},
					},
					{
						Query: "alter table child add constraint fi2ts foreign key (i2) references parent(ts);", PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-keys-test-testforeignkeys-0008-alter-table-child-add-constraint", Compare: "sqlstate"},
					},
					{
						Query: "alter table child add constraint fi4i2 foreign key (i4) references parent(i2);",
					},
					{
						Query: "alter table child add constraint fi4i4 foreign key (i4) references parent(i4);",
					},
					{
						Query: "alter table child add constraint fi4i8 foreign key (i4) references parent(i8);",
					},
					{
						Query: "alter table child add constraint fi4f foreign key (i4) references parent(f);",
					},
					{
						Query: "alter table child add constraint fi8i2 foreign key (i8) references parent(i2);",
					},
					{
						Query: "alter table child add constraint fi8i4 foreign key (i8) references parent(i4);",
					},
					{
						Query: "alter table child add constraint fi8d foreign key (i8) references parent(d);",
					},
					{
						Query: "alter table child add constraint fi8t foreign key (i8) references parent(t);", PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-keys-test-testforeignkeys-0009-alter-table-child-add-constraint",

							// this isn't allowed in postgres, but works with our constraints currently
							Compare: "sqlstate"},
					},
					{
						Skip:  true,
						Query: "alter table child add constraint ffi2 foreign key (f) references parent(i2);", PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-keys-test-testforeignkeys-0010-alter-table-child-add-constraint",

							// this isn't allowed in postgres, but works with our constraints currently
							Compare: "sqlstate"},
					},
					{
						Skip:  true,
						Query: "alter table child add constraint ffi4 foreign key (f) references parent(i4);", PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-keys-test-testforeignkeys-0011-alter-table-child-add-constraint",

							// this isn't allowed in postgres, but works with our constraints currently
							Compare: "sqlstate"},
					},
					{
						Skip:  true,
						Query: "alter table child add constraint ffi8 foreign key (f) references parent(i8);", PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-keys-test-testforeignkeys-0012-alter-table-child-add-constraint", Compare: "sqlstate"},
					},
					{
						Query: "alter table child add constraint ffd foreign key (f) references parent(d);",
					},
					{
						Query: "alter table child add constraint fdf foreign key (d) references parent(f);",
					},
					{
						Query: "alter table child add constraint fft foreign key (f) references parent(t);", PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-keys-test-testforeignkeys-0013-alter-table-child-add-constraint", Compare: "sqlstate"},
					},
					{
						Query: "alter table child add constraint ffv foreign key (f) references parent(v);", PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-keys-test-testforeignkeys-0014-alter-table-child-add-constraint", Compare: "sqlstate"},
					},
					{
						Query: "alter table child add constraint fvv foreign key (v) references parent(v);",
					},
					{
						Query: "alter table child add constraint fvvl foreign key (v) references parent(vl);",
					},
					{
						Query: "alter table child add constraint fvi8 foreign key (v) references parent(i8);", PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-keys-test-testforeignkeys-0015-alter-table-child-add-constraint", Compare: "sqlstate"},
					},
					{
						Query: "alter table child add constraint fvf foreign key (v) references parent(f);", PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-keys-test-testforeignkeys-0016-alter-table-child-add-constraint", Compare: "sqlstate"},
					},
					{
						Query: "alter table child add constraint fvts foreign key (v) references parent(ts);", PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-keys-test-testforeignkeys-0017-alter-table-child-add-constraint", Compare: "sqlstate"},
					},
					{
						Query: "alter table child add constraint fvj foreign key (v) references parent(j);", PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-keys-test-testforeignkeys-0018-alter-table-child-add-constraint", Compare: "sqlstate"},
					},
					{
						Query: "alter table child add constraint fvt foreign key (v) references parent(t);",
					},
					{
						Query: "alter table child add constraint fvllv foreign key (vl) references parent(vl);",
					},
					{
						Query: "alter table child add constraint fvlv foreign key (vl) references parent(v);",
					},
					{
						Query: "alter table child add constraint fvlt foreign key (vl) references parent(t);",
					},
					{
						Query: "alter table child add constraint ftt foreign key (t) references parent(t);",
					},
					{
						Query: "alter table child add constraint ftv foreign key (t) references parent(v);",
					},
					{
						Query: "alter table child add constraint ftvl foreign key (t) references parent(vl);",
					},
					{
						Query: "alter table child add constraint fti8 foreign key (t) references parent(i8);", PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-keys-test-testforeignkeys-0019-alter-table-child-add-constraint", Compare: "sqlstate"},
					},
					{
						Query: "alter table child add constraint ftsts foreign key (ts) references parent(ts);",
					},
					{
						Query: "alter table child add constraint ftst foreign key (ts) references parent(t);", PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-keys-test-testforeignkeys-0020-alter-table-child-add-constraint", Compare: "sqlstate"},
					},
					{
						Query: "alter table child add constraint ftsi8 foreign key (ts) references parent(i8);", PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-keys-test-testforeignkeys-0021-alter-table-child-add-constraint", Compare: "sqlstate"},
					},
					{
						Query: "insert into child values (1, 1, 1, 1.0, 1.0, 'a', 'a', 'a', '{\"a\": 1}', '2021-01-01 00:00:00');",
					},
					{
						Query: "insert into child values (1, 2, 1, 1.0, 1.0, 'a', 'a', 'a', '{\"a\": 1}', '2021-01-01 00:00:00');", PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-keys-test-testforeignkeys-0022-insert-into-child-values-1", Compare: "sqlstate"},
					},
					{
						Query: "insert into child values (1, 1, 1, 2.0, 1.0, 'a', 'a', 'a', '{\"a\": 1}', '2021-01-01 00:00:00');", PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-keys-test-testforeignkeys-0023-insert-into-child-values-1", Compare: "sqlstate"},
					},
					{
						Query: "insert into child values (1, 1, 1, 1.0, 1.0, 'a', 'a', 'b', '{\"a\": 1}', '2021-01-01 00:00:00');", PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-keys-test-testforeignkeys-0024-insert-into-child-values-1", Compare: "sqlstate"},
					},
					{
						Query: "insert into child values (1, 1, 1, 1.0, 1.0, 'a', 'a', 'a', '{\"a\": 1}', '2021-01-01 00:00:01');", PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-keys-test-testforeignkeys-0025-insert-into-child-values-1", Compare: "sqlstate"},
					},
				},
			},
			{
				Name: "type conversion: text to varchar",
				SetUpScript: []string{
					`CREATE TABLE parent (a INT PRIMARY KEY, b varchar(100))`,
					`CREATE TABLE child (c INT PRIMARY KEY, d text)`,
					`INSERT INTO parent VALUES (1, 'abc'), (2, 'def')`,
					`alter table parent add constraint ub unique (b)`,
				},
				Assertions: []ScriptTestAssertion{
					{
						Query: "alter table child add constraint fk foreign key (d) references parent(b)",
					},
					{
						Query: "insert into child values (1, 'abc')",
					},
					{
						Query: "insert into child values (2, 'xyz')", PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-keys-test-testforeignkeys-0026-insert-into-child-values-2", Compare: "sqlstate"},
					},
					{
						Query: "delete from parent where b = 'def'",
					},
					{
						Query: "delete from parent where b = 'abc'", PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-keys-test-testforeignkeys-0027-delete-from-parent-where-b", Compare: "sqlstate"},
					},
				},
			},
			{
				Name: "type conversion: integer to double",
				SetUpScript: []string{
					`CREATE TABLE parent (a INT PRIMARY KEY, b double precision)`,
					`CREATE TABLE child (c INT PRIMARY KEY, d int)`,
					`INSERT INTO parent VALUES (1, 1), (3, 3)`,
					`alter table parent add constraint ub unique (b)`,
				},
				Assertions: []ScriptTestAssertion{
					{
						Query: "alter table child add constraint fk foreign key (d) references parent(b)",
					},
					{
						Query: "select * from parent where b = 1.0", PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-keys-test-testforeignkeys-0028-select-*-from-parent-where"},
					},
					{
						Query: "insert into child values (1, 1)",
					},
					{
						Query: "insert into child values (2, 1)",
					},
					{
						Query: "insert into child values (2, 2)", PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-keys-test-testforeignkeys-0029-insert-into-child-values-2", Compare: "sqlstate"},
					},
					{
						Query: "delete from parent where b = 3.0",
					},
					{
						Query: "delete from parent where b = 1.0", PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-keys-test-testforeignkeys-0030-delete-from-parent-where-b", Compare: "sqlstate"},
					},
				},
			},
			{
				Name: "type conversion: value out of bounds, child larger",
				SetUpScript: []string{
					`CREATE TABLE parent (a INT PRIMARY KEY, b int2)`,
					`CREATE TABLE child (c INT PRIMARY KEY, d int8)`,
					`INSERT INTO parent VALUES (1, 1), (3, 3)`,
					`alter table parent add constraint ub unique (b)`,
				},
				Assertions: []ScriptTestAssertion{
					{
						Query: "alter table child add constraint fk foreign key (d) references parent(b)",
					},
					{
						Query: "insert into child values (1, 1)",
					},
					{
						Query: "insert into child values (2, 2)", PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-keys-test-testforeignkeys-0031-insert-into-child-values-2",

							// above maximum int2
							Compare: "sqlstate"},
					},
					{
						Query: "insert into child values (2, 65536)", PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-keys-test-testforeignkeys-0032-insert-into-child-values-2", Compare: "sqlstate"},
					},
					{
						Query: "delete from parent where b = 3",
					},
					{
						Query: "delete from parent where b = 1", PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-keys-test-testforeignkeys-0033-delete-from-parent-where-b", Compare: "sqlstate"},
					},
				},
			},
			{
				Name: "type conversion: value out of bound, parent larger",
				SetUpScript: []string{
					`CREATE TABLE parent (a INT PRIMARY KEY, b int8)`,
					`CREATE TABLE child (c INT PRIMARY KEY, d int2)`,
					`INSERT INTO parent VALUES (1, 1), (65536, 65536)`, // above maximum int2
					`alter table parent add constraint ub unique (b)`,
				},
				Assertions: []ScriptTestAssertion{
					{
						Query: "alter table child add constraint fk foreign key (d) references parent(b)",
					},
					{
						Query: "insert into child values (1, 1)",
					},
					{
						Query: "insert into child values (2, 2)", PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-keys-test-testforeignkeys-0034-insert-into-child-values-2", Compare: "sqlstate"},
					},
					{
						Query: "insert into child values (2, 65536)", PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-keys-test-testforeignkeys-0035-insert-into-child-values-2", Compare: "sqlstate"},
					},
					{
						Query: "delete from parent where b = 65536",
					},
					{
						Query: "delete from parent where b = 1", PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-keys-test-testforeignkeys-0036-delete-from-parent-where-b", Compare: "sqlstate"},
					},
				},
			},
			{
				Name: "foreign key default naming",
				SetUpScript: []string{
					"CREATE TABLE webhooks (id varchar not null, id2 int8, primary key (id));",
					"CREATE UNIQUE INDEX idx1 on webhooks(id, id2);",
					"CREATE TABLE t33 (id varchar not null, webhook_id_fk varchar not null, webhook_id2_fk int8, foreign key (webhook_id_fk) references webhooks(id), foreign key (webhook_id_fk, webhook_id2_fk) references webhooks(id, id2), primary key (id));",
				},
				Assertions: []ScriptTestAssertion{
					{
						Query: "SELECT conname AS constraint_name FROM pg_constraint WHERE conrelid = 't33'::regclass  AND contype = 'f';", PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-keys-test-testforeignkeys-0080-select-conname-as-constraint_name-from"},
					},
					{
						Query: "ALTER TABLE t33 DROP CONSTRAINT t33_webhook_id_fk_fkey;", PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-keys-test-testforeignkeys-0081-alter-table-t33-drop-constraint"},
					},
				},
			},
			{
				Name: "foreign key default naming, name collision ",
				SetUpScript: []string{
					"CREATE TABLE parent (id varchar not null primary key);",
					"CREATE TABLE child (id varchar primary key, constraint t33_webhook_id_fk_fkey foreign key (id) references parent(id));",
					"CREATE TABLE webhooks (id varchar not null, id2 int8, primary key (id));",
					"CREATE TABLE t33 (id varchar not null, webhook_id_fk varchar not null, foreign key (webhook_id_fk) references webhooks(id), primary key (id));",
				},
				Assertions: []ScriptTestAssertion{
					{
						Query: "SELECT conname AS constraint_name FROM pg_constraint WHERE conrelid = 't33'::regclass  AND contype = 'f';", PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-keys-test-testforeignkeys-0082-select-conname-as-constraint_name-from"},
					},
					{
						Query: "ALTER TABLE t33 DROP CONSTRAINT t33_webhook_id_fk_fkey1;", PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-keys-test-testforeignkeys-0083-alter-table-t33-drop-constraint"},
					},
				},
			},
			{
				Name: "foreign key default naming, in column definition",
				SetUpScript: []string{
					"CREATE TABLE webhooks (id varchar not null, primary key (id));",
					"CREATE TABLE t33 (id varchar not null, webhook_id_fk varchar not null references webhooks(id), primary key (id));",
				},
				Assertions: []ScriptTestAssertion{
					{
						Query: "SELECT conname AS constraint_name FROM pg_constraint WHERE conrelid = 't33'::regclass  AND contype = 'f';", PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-keys-test-testforeignkeys-0084-select-conname-as-constraint_name-from"},
					},
					{
						Query: "ALTER TABLE t33 DROP CONSTRAINT t33_webhook_id_fk_fkey;", PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-keys-test-testforeignkeys-0085-alter-table-t33-drop-constraint"},
					},
				},
			},
			{
				Name: "foreign key custom naming",
				SetUpScript: []string{
					"CREATE TABLE webhooks (id VARCHAR NOT NULL, PRIMARY KEY (id));",
					"CREATE TABLE t33 (id VARCHAR NOT NULL, webhook_id_fk VARCHAR NOT NULL, CONSTRAINT foo1 FOREIGN KEY (webhook_id_fk) REFERENCES webhooks(id), PRIMARY KEY (id));",
				},
				Assertions: []ScriptTestAssertion{
					{
						Query: "SELECT conname AS constraint_name FROM pg_constraint WHERE conrelid = 't33'::regclass AND contype = 'f';", PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-keys-test-testforeignkeys-0086-select-conname-as-constraint_name-from"},
					},
					{
						Query: "ALTER TABLE t33 DROP CONSTRAINT foo1;", PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-keys-test-testforeignkeys-0087-alter-table-t33-drop-constraint"},
					},
				},
			},
			{
				Name: "foreign key default naming, added through alter table",
				SetUpScript: []string{
					"CREATE TABLE webhooks (id varchar not null, primary key (id));",
					"CREATE TABLE t33 (id varchar not null, webhook_id_fk varchar not null, primary key (id));",
					"ALTER TABLE t33 ADD FOREIGN KEY (webhook_id_fk) REFERENCES webhooks(id);",
				},
				Assertions: []ScriptTestAssertion{
					{
						Query: "SELECT conname AS constraint_name FROM pg_constraint WHERE conrelid = 't33'::regclass  AND contype = 'f';", PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-keys-test-testforeignkeys-0088-select-conname-as-constraint_name-from"},
					},
					{
						Query: "ALTER TABLE t33 DROP CONSTRAINT t33_webhook_id_fk_fkey;", PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-keys-test-testforeignkeys-0089-alter-table-t33-drop-constraint"},
					},
				},
			},
		},
	)
}
