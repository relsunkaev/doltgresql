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

package _go

import (
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
)

func TestInsert(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "simple insert",
			SetUpScript: []string{
				"CREATE TABLE mytable (id INT PRIMARY KEY, name TEXT)",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:            "INSERT INTO mytable (id, name) VALUES (1, 'hello')",
					SkipResultsCheck: true,
				},
				{
					Query:            "INSERT INTO mytable (ID, naME) VALUES (2, 'world')",
					SkipResultsCheck: true,
				},
				{
					Query: "SELECT * FROM mytable order by id", PostgresOracle: ScriptTestPostgresOracle{ID: "insert-test-testinsert-0001-select-*-from-mytable-order"},
				},
			},
		},
		{
			Name: "keyless insert",
			SetUpScript: []string{
				"CREATE TABLE mytable (id INT, name TEXT)",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:            "INSERT INTO mytable (id, name) VALUES (1, 'hello')",
					SkipResultsCheck: true,
				},
				{
					Query:            "INSERT INTO mytable (ID, naME) VALUES (2, 'world')",
					SkipResultsCheck: true,
				},
				{
					Query: "SELECT * FROM mytable order by id", PostgresOracle: ScriptTestPostgresOracle{ID: "insert-test-testinsert-0002-select-*-from-mytable-order"},
				},
			},
		},
		{
			Name: "on conflict clause",
			SetUpScript: []string{
				"CREATE TABLE mytable (id INT primary key, name TEXT)",
				"create table t2 (id int primary key, c1 text, c2 text)",
				"CREATE TABLE conflict_arbiters (id INT PRIMARY KEY, email TEXT UNIQUE, name TEXT)",
				"INSERT INTO conflict_arbiters VALUES (1, 'a@example.com', 'first'), (2, 'b@example.com', 'second')",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:            "INSERT INTO mytable (id, name) VALUES (1, 'hello')",
					SkipResultsCheck: true,
				},
				{
					Query:            "INSERT INTO mytable (ID, naME) VALUES (2, 'world')",
					SkipResultsCheck: true,
				},
				{
					Query:            "INSERT INTO mytable (ID, naME) VALUES (1, 'world') ON CONFLICT (id) DO UPDATE SET name = 'world'",
					SkipResultsCheck: true,
				},
				{
					Query:            "INSERT INTO mytable (ID, naME) VALUES (2, 'hello') ON CONFLICT (id) DO UPDATE SET name = 'conflict'",
					SkipResultsCheck: true,
				},
				{
					Query: "INSERT INTO mytable (ID, naME) VALUES (1, 'not inserted') ON CONFLICT (id) DO NOTHING",
				},
				{
					Query: "SELECT * FROM mytable order by id", PostgresOracle: ScriptTestPostgresOracle{ID: "insert-test-testinsert-0003-select-*-from-mytable-order"},
				},
				{
					Query: "INSERT INTO mytable (ID, naME) VALUES (1, 'hello') ON CONFLICT (id) DO UPDATE set name = concat('new', name)",
				},
				{
					Query: "SELECT * FROM mytable order by id",
					Expected: []sql.Row{
						{1, "newworld"},
						{2, "conflict"},
					},
				},
				{
					Query:            "INSERT INTO t2 (id, c1, c2) VALUES (1, 'hello', 'world'), (2, 'world', 'hello')",
					SkipResultsCheck: true,
				},
				{
					Query:            "INSERT INTO t2 (id, c1, c2) VALUES (1, 'hello', 'world') ON CONFLICT (id) DO UPDATE SET c1 = 'conflict', c2 = c1",
					SkipResultsCheck: true,
				},
				{
					Query:            "INSERT INTO t2 (id, c1, c2) VALUES (2, 'hello', 'world') ON CONFLICT (id) DO UPDATE SET c2 = c1",
					SkipResultsCheck: true,
				},
				{
					Query: "SELECT * FROM t2 order by id",
					Expected: []sql.Row{
						{1, "conflict", "conflict"},
						{2, "world", "world"},
					},
				},
				{
					Query: `INSERT INTO t2 (id, c1, c2) 
VALUES ($1, $2, $3)
ON CONFLICT (id) do update set c1 = $4`,
					BindVars: []any{1, "x", "y", "no conflict expected"},
				},
				{
					Query:       "INSERT INTO mytable (id, name) VALUES (3, 'bad target') ON CONFLICT (name) DO UPDATE SET name = 'not applied'",
					ExpectedErr: "there is no unique or exclusion constraint matching the ON CONFLICT specification",
				},
				{
					// New row id=3 with email='a@example.com' targets ON
					// CONFLICT (id), but email='a@example.com' already
					// belongs to id=1. The non-target unique conflict
					// raises rather than silently firing the update.
					Query:       "INSERT INTO conflict_arbiters VALUES (3, 'a@example.com', 'wrong update') ON CONFLICT (id) DO UPDATE SET name = 'wrong update'",
					ExpectedErr: "duplicate key value violates unique constraint",
				},
				{
					// New id=3, email='a@example.com' conflicts on the
					// non-target email index — pre-check raises the
					// duplicate-key error rather than letting INSERT
					// IGNORE silently swallow it.
					Query:       "INSERT INTO conflict_arbiters VALUES (3, 'a@example.com', 'wrong ignore') ON CONFLICT (id) DO NOTHING",
					ExpectedErr: "duplicate key value violates unique constraint",
				},
				{
					// Email is the targeted unique here; the existing
					// email='a@example.com' row is updated.
					Query: "INSERT INTO conflict_arbiters VALUES (3, 'a@example.com', 'email update') ON CONFLICT (email) DO UPDATE SET name = 'email update'",
				},
				{
					// Email is the targeted unique. The existing row
					// matches email='a@example.com' so the conflict
					// is on the target → DO NOTHING swallows it. The
					// non-target id index sees no collision (id=3 is
					// new), so the pre-check passes through.
					Query: "INSERT INTO conflict_arbiters VALUES (3, 'a@example.com', 'email ignore') ON CONFLICT (email) DO NOTHING",
				},
				{
					// Arbiter predicate is accepted: doltgres has no
					// partial unique indexes to discriminate against,
					// so the predicate is benign and the upsert
					// proceeds via the existing target-by-columns
					// resolver.
					Query: "INSERT INTO mytable (id, name) VALUES (1, 'predicate target') ON CONFLICT (id) WHERE id > 0 DO UPDATE SET name = 'predicate target'",
				},
				{
					// DO UPDATE WHERE predicate is supported by
					// rewriting `col = expr` to a CASE expression that
					// preserves the existing value when the predicate
					// is false. Predicate is true (mytable.id=1's name
					// is 'newworld'), so the update applies.
					Query: "INSERT INTO mytable (id, name) VALUES (1, 'conditional update') ON CONFLICT (id) DO UPDATE SET name = 'conditional update' WHERE mytable.name = 'newworld'",
				},
				{
					// id=1's name has been updated by the targeted
					// email upsert above; id=2 stays as seeded.
					Query: "SELECT * FROM conflict_arbiters ORDER BY id",
					Expected: []sql.Row{
						{1, "a@example.com", "email update"},
						{2, "b@example.com", "second"},
					},
				},
			},
		},
		{
			Name: "null and unspecified default values",
			SetUpScript: []string{
				"CREATE TABLE t (i INT DEFAULT NULL, j INT)",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:            "INSERT INTO t VALUES (default, default)",
					SkipResultsCheck: true,
				},
				{
					Query: "SELECT * FROM t", PostgresOracle: ScriptTestPostgresOracle{ID: "insert-test-testinsert-0010-select-*-from-t"},
				},
			},
		},
		{
			Name: "implicit default values",
			SetUpScript: []string{
				"CREATE TABLE t (i INT DEFAULT 123, j INT default 456);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:            "INSERT INTO t DEFAULT VALUES;",
					SkipResultsCheck: true,
				},
				{
					Query: "SELECT * FROM t", PostgresOracle: ScriptTestPostgresOracle{ID: "insert-test-testinsert-0011-select-*-from-t"},
				},
			},
		},
		{
			Name: "types",
			SetUpScript: []string{
				`create table child (i2 int2, i4 int4, i8 int8, f float, d double precision, v varchar, vl varchar(100), t text, j json, ts timestamp);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `insert into child values (1, 2, 3, 4.5, 6.7, 'hello', 'world', 'text', '{"a": 1}', '2021-01-01 00:00:00');`,
				},
				{
					Query: `select * from child;`, PostgresOracle: ScriptTestPostgresOracle{ID: "insert-test-testinsert-0012-select-*-from-child"},
				},
			},
		},
		{
			Name: "insert returning",
			SetUpScript: []string{
				"CREATE TABLE t (i serial, j INT)",
				"CREATE TABLE u (u uuid DEFAULT 'ac1f3e2d-1e4b-4d3e-8b1f-2b7f1e7f0e3d', j INT)",
				"CREATE TABLE s (v1 varchar DEFAULT 'hello', v2 varchar DEFAULT 'world')",
				"CREATE SCHEMA ts",
				"CREATE TABLE ts.t (i serial, j INT)",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "INSERT INTO t (j) VALUES (5), (6), (7) RETURNING i", PostgresOracle: ScriptTestPostgresOracle{ID: "insert-test-testinsert-0013-insert-into-t-j-values"},
				},
				{
					Query: "INSERT INTO t (j) VALUES (5), (6), (7) RETURNING i+3", PostgresOracle: ScriptTestPostgresOracle{ID: "insert-test-testinsert-0014-insert-into-t-j-values"},
				},
				{
					Query: "INSERT INTO t (j) VALUES (5), (6), (7) RETURNING i+j, j-3*i", PostgresOracle: ScriptTestPostgresOracle{ID: "insert-test-testinsert-0015-insert-into-t-j-values"},
				},
				{
					Query: "INSERT INTO u (j) VALUES (5), (6), (7) RETURNING u", PostgresOracle: ScriptTestPostgresOracle{ID: "insert-test-testinsert-0016-insert-into-u-j-values"},
				},
				{
					Query: "INSERT INTO s (v2) VALUES (' a') RETURNING concat(v1, v2)", PostgresOracle: ScriptTestPostgresOracle{ID: "insert-test-testinsert-0017-insert-into-s-v2-values"},
				},
				{
					Query: "INSERT INTO s (v1) VALUES ('sup ') RETURNING concat(v1, v2)", PostgresOracle: ScriptTestPostgresOracle{ID: "insert-test-testinsert-0018-insert-into-s-v1-values"},
				},
				{
					Query: "INSERT INTO s (v2, v1) VALUES ('def', 'abc'), ('xyz', 'uvw') RETURNING concat(v1, v2), concat(v2, v1), 100", PostgresOracle: ScriptTestPostgresOracle{ID: "insert-test-testinsert-0019-insert-into-s-v2-v1"},
				},
				{
					Query: "INSERT INTO t (j) VALUES (5), (6), (7) RETURNING i, doesnotexist", PostgresOracle: ScriptTestPostgresOracle{ID: "insert-test-testinsert-0020-insert-into-t-j-values", Compare: "sqlstate"},
				},
				{
					Query: "INSERT INTO t (j) VALUES (5), (6), (7) RETURNING i, doesnotexist(j)", PostgresOracle: ScriptTestPostgresOracle{ID: "insert-test-testinsert-0021-insert-into-t-j-values", Compare: "sqlstate"},
				},
				{
					Query: "INSERT INTO public.t (j) VALUES (8) RETURNING t.j", PostgresOracle: ScriptTestPostgresOracle{ID: "insert-test-testinsert-0022-insert-into-public.t-j-values", Cleanup: []string{"DROP TABLE IF EXISTS t CASCADE", "DROP TABLE IF EXISTS u CASCADE", "DROP TABLE IF EXISTS s CASCADE", "DROP TABLE IF EXISTS ts.t CASCADE", "DROP SCHEMA IF EXISTS ts CASCADE"}},
				},
				{
					Query: "INSERT INTO public.t (j) VALUES (9) RETURNING public.t.j", PostgresOracle: ScriptTestPostgresOracle{ID: "insert-test-testinsert-0023-insert-into-public.t-j-values", Cleanup: []string{"DROP TABLE IF EXISTS t CASCADE", "DROP TABLE IF EXISTS u CASCADE", "DROP TABLE IF EXISTS s CASCADE", "DROP TABLE IF EXISTS ts.t CASCADE", "DROP SCHEMA IF EXISTS ts CASCADE"}},
				},
				{
					Query: "INSERT INTO ts.t (j) VALUES (10) RETURNING ts.t.j", PostgresOracle: ScriptTestPostgresOracle{ID: "insert-test-testinsert-0024-insert-into-ts.t-j-values", Cleanup: []string{"DROP TABLE IF EXISTS t CASCADE", "DROP TABLE IF EXISTS u CASCADE", "DROP TABLE IF EXISTS s CASCADE", "DROP TABLE IF EXISTS ts.t CASCADE", "DROP SCHEMA IF EXISTS ts CASCADE"}},
				},
				{
					Query:    "INSERT INTO public.t (j) VALUES ($1) RETURNING j;",
					BindVars: []any{11}, PostgresOracle: ScriptTestPostgresOracle{ID: "insert-test-testinsert-0025-insert-into-public.t-j-values", Cleanup: []string{"DROP TABLE IF EXISTS t CASCADE", "DROP TABLE IF EXISTS u CASCADE", "DROP TABLE IF EXISTS s CASCADE", "DROP TABLE IF EXISTS ts.t CASCADE", "DROP SCHEMA IF EXISTS ts CASCADE"}},
				},
				{
					Query:    "INSERT INTO public.t (j) VALUES ($1) RETURNING t.j;",
					BindVars: []any{12}, PostgresOracle: ScriptTestPostgresOracle{ID: "insert-test-testinsert-0026-insert-into-public.t-j-values", Cleanup: []string{"DROP TABLE IF EXISTS t CASCADE", "DROP TABLE IF EXISTS u CASCADE", "DROP TABLE IF EXISTS s CASCADE", "DROP TABLE IF EXISTS ts.t CASCADE", "DROP SCHEMA IF EXISTS ts CASCADE"}},
				},
				{
					Query:    "INSERT INTO public.t (j) VALUES ($1) RETURNING public.t.j;",
					BindVars: []any{13}, PostgresOracle: ScriptTestPostgresOracle{ID: "insert-test-testinsert-0027-insert-into-public.t-j-values", Cleanup: []string{"DROP TABLE IF EXISTS t CASCADE", "DROP TABLE IF EXISTS u CASCADE", "DROP TABLE IF EXISTS s CASCADE", "DROP TABLE IF EXISTS ts.t CASCADE", "DROP SCHEMA IF EXISTS ts CASCADE"}},
				},
			},
		},
		{
			Name: "insert iso8601 timestamptz literal",
			SetUpScript: []string{
				"CREATE TABLE django_migrations (id serial primary key, app varchar, name varchar, applied timestamptz)",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO "django_migrations" ("app", "name", "applied") VALUES ('contenttypes', '0001_initial', '2025-03-24T19:21:59.690479+00:00'::timestamptz) RETURNING "django_migrations"."id"`, PostgresOracle: ScriptTestPostgresOracle{ID: "insert-test-testinsert-0028-insert-into-django_migrations-app-name"},
				},
			},
		},
	})
}
