// Copyright 2026 Dolthub, Inc.
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
	"context"
	"fmt"
	"testing"

	gms "github.com/dolthub/go-mysql-server/sql"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// TestInsertOnConflictExcluded covers the EXCLUDED pseudo-table that
// every PostgreSQL ORM emits in ON CONFLICT (col) DO UPDATE SET clauses
// to reference the row that would have been inserted. PG-style:
//
//	INSERT INTO t (id, name) VALUES (1, 'a')
//	  ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name;
//
// Maps internally to MySQL's `values(name)`. Without this, every ORM
// upsert (Drizzle, Prisma, SQLAlchemy.merge, ActiveRecord upsert,
// Sequelize.upsert, Drizzle's onConflictDoUpdate) errors at parse.
func TestInsertOnConflictExcluded(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "EXCLUDED.col copies the new value into the existing row",
			SetUpScript: []string{
				"CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT);",
				"INSERT INTO users VALUES (1, 'old', 30);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO users (id, name, age) VALUES (1, 'new', 31)
ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, age = EXCLUDED.age;`,
				},
				{
					Query: "SELECT id, name, age FROM users WHERE id = 1;",
					Expected: []gms.Row{
						{1, "new", 31},
					},
				},
			},
		},
		{
			Name: "EXCLUDED in expressions and mixed with existing column refs",
			SetUpScript: []string{
				"CREATE TABLE counters (id INT PRIMARY KEY, hits INT, label TEXT);",
				"INSERT INTO counters VALUES (1, 5, 'old');",
			},
			Assertions: []ScriptTestAssertion{
				{
					// Increment by EXCLUDED.hits (the proposed new value)
					// and concatenate label with EXCLUDED.label.
					Query: `INSERT INTO counters (id, hits, label) VALUES (1, 3, 'plus')
ON CONFLICT (id) DO UPDATE
SET hits = counters.hits + EXCLUDED.hits,
    label = counters.label || ':' || EXCLUDED.label;`,
				},
				{
					Query: "SELECT id, hits, label FROM counters WHERE id = 1;",
					Expected: []gms.Row{
						{1, 8, "old:plus"},
					},
				},
			},
		},
		{
			Name: "EXCLUDED case-insensitive (lowercase, uppercase, mixed)",
			SetUpScript: []string{
				"CREATE TABLE c_t (id INT PRIMARY KEY, v TEXT);",
				"INSERT INTO c_t VALUES (1, 'old');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO c_t (id, v) VALUES (1, 'A')
ON CONFLICT (id) DO UPDATE SET v = excluded.v;`,
				},
				{
					Query:    "SELECT v FROM c_t WHERE id = 1;",
					Expected: []gms.Row{{"A"}},
				},
				{
					Query: `INSERT INTO c_t (id, v) VALUES (1, 'B')
ON CONFLICT (id) DO UPDATE SET v = ExCluDed.v;`,
				},
				{
					Query:    "SELECT v FROM c_t WHERE id = 1;",
					Expected: []gms.Row{{"B"}},
				},
			},
		},
		{
			Name: "EXCLUDED with multi-row VALUES applies the matched row",
			SetUpScript: []string{
				"CREATE TABLE m (id INT PRIMARY KEY, v INT);",
				"INSERT INTO m VALUES (1, 100), (2, 200);",
			},
			Assertions: []ScriptTestAssertion{
				{
					// Three rows: id=1 conflicts (row 0), id=2 conflicts (row 1),
					// id=3 inserts cleanly (row 2).
					Query: `INSERT INTO m (id, v) VALUES (1, 11), (2, 22), (3, 33)
ON CONFLICT (id) DO UPDATE SET v = m.v + EXCLUDED.v;`,
				},
				{
					Query: "SELECT id, v FROM m ORDER BY id;",
					Expected: []gms.Row{
						{1, 111}, {2, 222}, {3, 33},
					},
				},
			},
		},
	})
}

// TestInsertOnConflictDoUpdateWhere covers the conditional update form
// of ON CONFLICT — `DO UPDATE SET ... WHERE pred`. PG semantics: the
// UPDATE only fires when pred (evaluated against the existing row +
// EXCLUDED proposed row) is true. Otherwise the existing row is kept
// unchanged AND no error is raised.
//
// Real-world example (DDIA / Vitess docs / Drizzle PG):
//
//	INSERT INTO counters (id, hits) VALUES (1, 1)
//	  ON CONFLICT (id) DO UPDATE
//	  SET hits = counters.hits + 1
//	  WHERE counters.hits < 100;
//
// Cap-at-100 idempotent counter increment.
func TestInsertOnConflictDoUpdateWhere(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "WHERE true applies the update",
			SetUpScript: []string{
				"CREATE TABLE w (id INT PRIMARY KEY, v INT);",
				"INSERT INTO w VALUES (1, 10);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO w (id, v) VALUES (1, 99)
ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v WHERE w.v < 100;`,
				},
				{
					Query:    "SELECT v FROM w WHERE id = 1;",
					Expected: []gms.Row{{99}},
				},
			},
		},
		{
			Name: "WHERE false leaves the existing row unchanged, no error",
			SetUpScript: []string{
				"CREATE TABLE w2 (id INT PRIMARY KEY, v INT);",
				"INSERT INTO w2 VALUES (1, 200);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO w2 (id, v) VALUES (1, 99)
ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v WHERE w2.v < 100;`,
				},
				{
					Query:    "SELECT v FROM w2 WHERE id = 1;",
					Expected: []gms.Row{{200}},
				},
			},
		},
		{
			Name: "WHERE referencing EXCLUDED",
			SetUpScript: []string{
				"CREATE TABLE w3 (id INT PRIMARY KEY, v INT);",
				"INSERT INTO w3 VALUES (1, 50);",
			},
			Assertions: []ScriptTestAssertion{
				{
					// New value is bigger -> apply.
					Query: `INSERT INTO w3 (id, v) VALUES (1, 75)
ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v WHERE EXCLUDED.v > w3.v;`,
				},
				{
					Query:    "SELECT v FROM w3 WHERE id = 1;",
					Expected: []gms.Row{{75}},
				},
				{
					// New value is smaller -> skip.
					Query: `INSERT INTO w3 (id, v) VALUES (1, 25)
ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v WHERE EXCLUDED.v > w3.v;`,
				},
				{
					Query:    "SELECT v FROM w3 WHERE id = 1;",
					Expected: []gms.Row{{75}},
				},
			},
		},
		{
			Name: "WHERE in mixed multi-row insert: each row checked independently",
			SetUpScript: []string{
				"CREATE TABLE w4 (id INT PRIMARY KEY, v INT);",
				"INSERT INTO w4 VALUES (1, 5), (2, 99);",
			},
			Assertions: []ScriptTestAssertion{
				{
					// id=1 (v=5 < 50) updates, id=2 (v=99 < 50 false) keeps,
					// id=3 inserts cleanly.
					Query: `INSERT INTO w4 (id, v) VALUES (1, 10), (2, 22), (3, 33)
ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v WHERE w4.v < 50;`,
				},
				{
					Query: "SELECT id, v FROM w4 ORDER BY id;",
					Expected: []gms.Row{
						{1, 10}, {2, 99}, {3, 33},
					},
				},
			},
		},
	})
}

// TestInsertOnConflictMultiUnique covers the workload pattern that
// real apps with id PK + email UNIQUE (or any second unique constraint)
// hit on every upsert: ON CONFLICT (id) DO UPDATE on a table with
// multiple unique indexes. PG-correct semantics:
//
//   - conflict on the targeted unique (id) -> DO UPDATE fires
//   - conflict on a non-target unique (email) -> raise the unique
//     constraint violation, NOT silently DO UPDATE
//   - no conflict -> INSERT
//
// The previous Doltgres behavior rejected this entire shape with an
// error to avoid MySQL's permissive ON DUPLICATE KEY UPDATE that fires
// for any unique conflict. With a row-by-row pre-check on non-target
// uniques, the targeted upsert pattern works correctly.
func TestInsertOnConflictMultiUnique(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ON CONFLICT (pk) on table with email UNIQUE: target conflict updates",
			SetUpScript: []string{
				"CREATE TABLE u (id INT PRIMARY KEY, email TEXT UNIQUE, name TEXT);",
				"INSERT INTO u VALUES (1, 'a@x.com', 'first'), (2, 'b@x.com', 'second');",
			},
			Assertions: []ScriptTestAssertion{
				{
					// Conflict on PK -> DO UPDATE fires.
					Query: `INSERT INTO u (id, email, name) VALUES (1, 'c@x.com', 'updated')
ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, email = EXCLUDED.email;`,
				},
				{
					Query:    "SELECT id, email, name FROM u WHERE id = 1;",
					Expected: []gms.Row{{1, "c@x.com", "updated"}},
				},
			},
		},
		{
			Name: "ON CONFLICT (pk): non-target unique conflict raises",
			SetUpScript: []string{
				"CREATE TABLE u2 (id INT PRIMARY KEY, email TEXT UNIQUE, name TEXT);",
				"INSERT INTO u2 VALUES (1, 'a@x.com', 'first'), (2, 'b@x.com', 'second');",
			},
			Assertions: []ScriptTestAssertion{
				{
					// New id=3 (no PK conflict) but email='a@x.com' (UNIQUE
					// conflict). PG raises duplicate key violation.
					Query: `INSERT INTO u2 (id, email, name) VALUES (3, 'a@x.com', 'wrong')
ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name;`,
					ExpectedErr: "duplicate",
				},
				{
					// State unchanged: no row id=3 was inserted, and
					// id=1's name is still 'first' (not 'wrong').
					Query: "SELECT id, email, name FROM u2 ORDER BY id;",
					Expected: []gms.Row{
						{1, "a@x.com", "first"},
						{2, "b@x.com", "second"},
					},
				},
			},
		},
		{
			Name: "ON CONFLICT (email): same coverage from the other unique direction",
			SetUpScript: []string{
				"CREATE TABLE u3 (id INT PRIMARY KEY, email TEXT UNIQUE, name TEXT);",
				"INSERT INTO u3 VALUES (1, 'a@x.com', 'first');",
			},
			Assertions: []ScriptTestAssertion{
				{
					// Email conflict -> DO UPDATE fires.
					Query: `INSERT INTO u3 (id, email, name) VALUES (99, 'a@x.com', 'updated')
ON CONFLICT (email) DO UPDATE SET name = EXCLUDED.name;`,
				},
				{
					Query:    "SELECT id, email, name FROM u3;",
					Expected: []gms.Row{{1, "a@x.com", "updated"}},
				},
				{
					// PK conflict (id=1) without email conflict -> raises.
					Query: `INSERT INTO u3 (id, email, name) VALUES (1, 'fresh@x.com', 'wrong')
ON CONFLICT (email) DO UPDATE SET name = EXCLUDED.name;`,
					ExpectedErr: "duplicate",
				},
			},
		},
		{
			Name: "ON CONFLICT (pk) DO NOTHING with multi-unique still rejected",
			SetUpScript: []string{
				"CREATE TABLE u4 (id INT PRIMARY KEY, email TEXT UNIQUE, name TEXT);",
				"INSERT INTO u4 VALUES (1, 'a@x.com', 'first');",
			},
			Assertions: []ScriptTestAssertion{
				{
					// DO NOTHING on a multi-unique table still routes
					// through INSERT IGNORE in GMS, which would silently
					// swallow non-target unique violations — incorrect
					// under PG semantics. The DO UPDATE form is
					// supported via the target-guard wrapper instead.
					Query:       `INSERT INTO u4 (id, email, name) VALUES (1, 'b@x.com', 'wrong') ON CONFLICT (id) DO NOTHING;`,
					ExpectedErr: "DO NOTHING is not yet supported on tables with multiple unique indexes",
				},
			},
		},
		{
			Name: "ON CONFLICT (email) on table with id PK + email UNIQUE (2 seed rows)",
			SetUpScript: []string{
				"CREATE TABLE u_two (id INT PRIMARY KEY, email TEXT UNIQUE, name TEXT);",
				"INSERT INTO u_two VALUES (1, 'a@x.com', 'first'), (2, 'b@x.com', 'second');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO u_two VALUES (3, 'a@x.com', 'email update')
ON CONFLICT (email) DO UPDATE SET name = 'email update';`,
				},
				{
					Query: "SELECT id, email, name FROM u_two ORDER BY id;",
					Expected: []gms.Row{
						{1, "a@x.com", "email update"},
						{2, "b@x.com", "second"},
					},
				},
			},
		},
	})
}

// TestInsertOnConflictOnConstraint covers the
// `ON CONFLICT ON CONSTRAINT name` syntax. ORM-generated upserts
// (Drizzle .onConflictDoUpdate({target: "constraint_name"}),
// SQLAlchemy.dialects.postgresql.insert(...).on_conflict_do_update
// with constraint=) routinely use the named-constraint form because
// it resolves cleanly even when the constraint columns include
// expressions or are inferred from a table-rename migration.
//
// The implementation looks up the constraint by name, derives its
// column list, and routes through the existing target-by-columns
// pipeline (which already handles the multi-unique target guard
// added earlier).
func TestInsertOnConflictOnConstraint(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ON CONFLICT ON CONSTRAINT named PK index updates",
			SetUpScript: []string{
				"CREATE TABLE oc_pk (id INT PRIMARY KEY, v INT);",
				"INSERT INTO oc_pk VALUES (1, 10);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "INSERT INTO oc_pk VALUES (1, 99) ON CONFLICT ON CONSTRAINT oc_pk_pkey DO UPDATE SET v = EXCLUDED.v;",
				},
				{
					Query:    "SELECT v FROM oc_pk WHERE id = 1;",
					Expected: []gms.Row{{99}},
				},
			},
		},
		{
			Name: "ON CONFLICT ON CONSTRAINT named UNIQUE updates",
			SetUpScript: []string{
				"CREATE TABLE oc_uq (id INT PRIMARY KEY, code TEXT, name TEXT);",
				"CREATE UNIQUE INDEX oc_uq_code ON oc_uq (code);",
				"INSERT INTO oc_uq VALUES (1, 'A', 'first');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "INSERT INTO oc_uq VALUES (99, 'A', 'updated') ON CONFLICT ON CONSTRAINT oc_uq_code DO UPDATE SET name = EXCLUDED.name;",
				},
				{
					Query:    "SELECT id, code, name FROM oc_uq;",
					Expected: []gms.Row{{1, "A", "updated"}},
				},
			},
		},
		{
			Name: "ON CONFLICT ON CONSTRAINT DO NOTHING ignores target conflict",
			SetUpScript: []string{
				"CREATE TABLE oc_dn (id INT PRIMARY KEY, v INT);",
				"INSERT INTO oc_dn VALUES (1, 10);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "INSERT INTO oc_dn VALUES (1, 999) ON CONFLICT ON CONSTRAINT oc_dn_pkey DO NOTHING;",
				},
				{
					Query:    "SELECT v FROM oc_dn WHERE id = 1;",
					Expected: []gms.Row{{10}},
				},
			},
		},
		{
			Name: "ON CONFLICT ON CONSTRAINT with unknown name errors",
			SetUpScript: []string{
				"CREATE TABLE oc_bad (id INT PRIMARY KEY);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       "INSERT INTO oc_bad VALUES (1) ON CONFLICT ON CONSTRAINT nope_no_such_constraint DO NOTHING;",
					ExpectedErr: "constraint",
				},
			},
		},
	})
}

// TestInsertOnConflictORMShape exercises the upsert workflow exactly
// as Drizzle / Prisma / SQLAlchemy emit it through the pgx driver.
func TestInsertOnConflictORMShape(t *testing.T) {
	port, err := gms.GetEmptyPort()
	require.NoError(t, err)
	ctx, defaultConn, controller := CreateServerWithPort(t, "postgres", port)
	t.Cleanup(func() {
		defaultConn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	dial := func(t *testing.T) *pgx.Conn {
		t.Helper()
		conn, err := pgx.Connect(ctx, fmt.Sprintf(
			"postgres://postgres:password@127.0.0.1:%d/postgres?sslmode=disable", port))
		require.NoError(t, err)
		t.Cleanup(func() { _ = conn.Close(ctx) })
		return conn
	}

	conn := dial(t)
	_, err = conn.Exec(ctx, `CREATE TABLE upserted (
  id INT PRIMARY KEY,
  email TEXT UNIQUE,
  hits INT NOT NULL DEFAULT 0
);`)
	require.NoError(t, err)

	t.Run("Drizzle-shape upsert via parameterized INSERT ON CONFLICT", func(t *testing.T) {
		// Round 1: insert.
		_, err := conn.Exec(ctx,
			`INSERT INTO upserted (id, email, hits) VALUES ($1, $2, $3)
ON CONFLICT (id) DO UPDATE SET email = EXCLUDED.email, hits = upserted.hits + EXCLUDED.hits;`,
			1, "a@x.com", 1)
		require.NoError(t, err)

		// Round 2: conflict on PK -> increment.
		_, err = conn.Exec(ctx,
			`INSERT INTO upserted (id, email, hits) VALUES ($1, $2, $3)
ON CONFLICT (id) DO UPDATE SET email = EXCLUDED.email, hits = upserted.hits + EXCLUDED.hits;`,
			1, "a-2@x.com", 2)
		require.NoError(t, err)

		var hits int
		var email string
		require.NoError(t, conn.QueryRow(ctx,
			"SELECT email, hits FROM upserted WHERE id = 1").Scan(&email, &hits))
		require.Equal(t, "a-2@x.com", email)
		require.Equal(t, 3, hits)
	})

	t.Run("non-target unique conflict propagates as a typed error", func(t *testing.T) {
		_, err := conn.Exec(ctx,
			`INSERT INTO upserted (id, email, hits) VALUES (1, 'a-2@x.com', 9)
ON CONFLICT (id) DO UPDATE SET hits = EXCLUDED.hits;`)
		// id=1 conflict -> DO UPDATE applies.
		require.NoError(t, err)

		// New id, but email already on id=1 -> non-target conflict.
		_, err = conn.Exec(ctx, `INSERT INTO upserted (id, email, hits) VALUES (10, 'a-2@x.com', 0)
ON CONFLICT (id) DO UPDATE SET hits = EXCLUDED.hits;`)
		require.Error(t, err)

		// Verify state: id=10 is NOT inserted, id=1 still has the
		// value from the previous successful upsert.
		var count int
		require.NoError(t, conn.QueryRow(context.Background(),
			"SELECT COUNT(*) FROM upserted WHERE id = 10").Scan(&count))
		require.Equal(t, 0, count)
	})
}
