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

// TestSelectForUpdate exercises the row-locking clauses on SELECT —
// FOR UPDATE / FOR SHARE / FOR NO KEY UPDATE / FOR KEY SHARE, with
// optional NOWAIT / SKIP LOCKED and FOR UPDATE OF table-list. Real
// applications use these for the read-then-write pattern that ORMs
// (SQLAlchemy `with_for_update`, ActiveRecord `lock`, Django
// `select_for_update`, Drizzle `.for("update")`) wrap.
//
// Doltgres does not yet take true row-level pessimistic locks; under
// MVCC + serializable isolation the locking clause is largely
// advisory. The goal here is workload compatibility: the keyword
// must parse, the query must return the expected rows, and the
// downstream UPDATE / DELETE must succeed.
func TestSelectForUpdate(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "FOR UPDATE returns rows; UPDATE that follows succeeds",
			SetUpScript: []string{
				"CREATE TABLE t (id INT PRIMARY KEY, v INT);",
				"INSERT INTO t VALUES (1, 100), (2, 200), (3, 300);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT id, v FROM t WHERE id = 2 FOR UPDATE;",
					Expected: []gms.Row{
						{2, 200},
					},
				},
				{
					Query: "BEGIN;",
				},
				{
					Query: "SELECT id, v FROM t WHERE id = 2 FOR UPDATE;",
					Expected: []gms.Row{
						{2, 200},
					},
				},
				{
					Query: "UPDATE t SET v = 222 WHERE id = 2;",
				},
				{
					Query: "COMMIT;",
				},
				{
					Query: "SELECT v FROM t WHERE id = 2;",
					Expected: []gms.Row{
						{222},
					},
				},
			},
		},
		{
			Name: "FOR SHARE / FOR NO KEY UPDATE / FOR KEY SHARE all parse and run",
			SetUpScript: []string{
				"CREATE TABLE share_t (id INT PRIMARY KEY, v INT);",
				"INSERT INTO share_t VALUES (1, 10), (2, 20);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT id FROM share_t WHERE id = 1 FOR SHARE;",
					Expected: []gms.Row{
						{1},
					},
				},
				{
					Query: "SELECT id FROM share_t WHERE id = 1 FOR NO KEY UPDATE;",
					Expected: []gms.Row{
						{1},
					},
				},
				{
					Query: "SELECT id FROM share_t WHERE id = 1 FOR KEY SHARE;",
					Expected: []gms.Row{
						{1},
					},
				},
			},
		},
		{
			Name: "NOWAIT and SKIP LOCKED parse and run uncontended",
			SetUpScript: []string{
				"CREATE TABLE wait_t (id INT PRIMARY KEY, v INT);",
				"INSERT INTO wait_t VALUES (1, 1), (2, 2);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT id FROM wait_t ORDER BY id FOR UPDATE NOWAIT;",
					Expected: []gms.Row{
						{1}, {2},
					},
				},
				{
					Query: "SELECT id FROM wait_t ORDER BY id FOR UPDATE SKIP LOCKED;",
					Expected: []gms.Row{
						{1}, {2},
					},
				},
				{
					Query: "SELECT id FROM wait_t ORDER BY id FOR SHARE NOWAIT;",
					Expected: []gms.Row{
						{1}, {2},
					},
				},
				{
					Query: "SELECT id FROM wait_t ORDER BY id FOR SHARE SKIP LOCKED;",
					Expected: []gms.Row{
						{1}, {2},
					},
				},
			},
		},
		{
			Name: "FOR UPDATE OF restricts to listed tables",
			SetUpScript: []string{
				"CREATE TABLE parent (id INT PRIMARY KEY, name TEXT);",
				"CREATE TABLE child (id INT PRIMARY KEY, parent_id INT, v INT);",
				"INSERT INTO parent VALUES (1, 'a'), (2, 'b');",
				"INSERT INTO child VALUES (10, 1, 100), (20, 2, 200);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT child.id, child.v
FROM child
JOIN parent ON child.parent_id = parent.id
WHERE parent.id = 1
FOR UPDATE OF child;`,
					Expected: []gms.Row{
						{10, 100},
					},
				},
				{
					Query: `SELECT child.id
FROM child
JOIN parent ON child.parent_id = parent.id
ORDER BY child.id
FOR UPDATE OF parent, child;`,
					Expected: []gms.Row{
						{10}, {20},
					},
				},
			},
		},
		{
			Name: "FOR UPDATE OF with unknown table errors",
			SetUpScript: []string{
				"CREATE TABLE for_of_t (id INT PRIMARY KEY);",
				"INSERT INTO for_of_t VALUES (1);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       "SELECT id FROM for_of_t FOR UPDATE OF nope;",
					ExpectedErr: "nope",
				},
			},
		},
		{
			Name: "FOR UPDATE inside a CTE",
			SetUpScript: []string{
				"CREATE TABLE cte_t (id INT PRIMARY KEY, v INT);",
				"INSERT INTO cte_t VALUES (1, 10), (2, 20);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `WITH locked AS (
  SELECT id, v FROM cte_t WHERE v >= 10 FOR UPDATE
)
SELECT id, v FROM locked ORDER BY id;`,
					Expected: []gms.Row{
						{1, 10}, {2, 20},
					},
				},
			},
		},
		{
			Name: "multiple locking items on same SELECT",
			SetUpScript: []string{
				"CREATE TABLE multi_lock_a (id INT PRIMARY KEY);",
				"CREATE TABLE multi_lock_b (id INT PRIMARY KEY);",
				"INSERT INTO multi_lock_a VALUES (1);",
				"INSERT INTO multi_lock_b VALUES (2);",
			},
			Assertions: []ScriptTestAssertion{
				{
					// PostgreSQL allows chained locking clauses; the
					// strongest applicable mode wins per row.
					Query: `SELECT a.id, b.id
FROM multi_lock_a a, multi_lock_b b
FOR UPDATE OF a
FOR SHARE OF b;`,
					Expected: []gms.Row{
						{1, 2},
					},
				},
			},
		},
	})
}

// TestSelectForUpdateORMShape covers the read-modify-write pattern
// every ORM uses: in a single transaction, SELECT FOR UPDATE the
// target row, decide based on its value, then UPDATE.
func TestSelectForUpdateORMShape(t *testing.T) {
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
	_, err = conn.Exec(ctx, "CREATE TABLE accounts (id INT PRIMARY KEY, balance INT);")
	require.NoError(t, err)
	_, err = conn.Exec(ctx, "INSERT INTO accounts VALUES (1, 100);")
	require.NoError(t, err)

	t.Run("read-modify-write inside a transaction", func(t *testing.T) {
		tx, err := conn.Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { _ = tx.Rollback(context.Background()) })

		var balance int
		require.NoError(t, tx.QueryRow(ctx,
			"SELECT balance FROM accounts WHERE id = 1 FOR UPDATE;").Scan(&balance))
		require.Equal(t, 100, balance)

		_, err = tx.Exec(ctx, "UPDATE accounts SET balance = balance + 50 WHERE id = 1;")
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))

		require.NoError(t, conn.QueryRow(ctx,
			"SELECT balance FROM accounts WHERE id = 1;").Scan(&balance))
		require.Equal(t, 150, balance)
	})
}
