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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// TestSetTransactionIsolationAfterQueryRejectedRepro reproduces a transaction
// mode bug: PostgreSQL requires SET TRANSACTION ISOLATION LEVEL before any
// query in the transaction.
func TestSetTransactionIsolationAfterQueryRejectedRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "SET TRANSACTION ISOLATION rejects after query",
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query:    `SELECT 1;`,
					Expected: []sql.Row{{1}},
				},
				{
					Query:       `SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;`,
					ExpectedErr: `must be called before any query`,
				},
				{
					Query: `ROLLBACK;`,
				},
			},
		},
	})
}

// TestSetTransactionSnapshotValidationRepro reproduces snapshot-import
// validation bugs around SET TRANSACTION SNAPSHOT.
func TestSetTransactionSnapshotValidationRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "SET TRANSACTION SNAPSHOT rejects invalid identifier",
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN ISOLATION LEVEL REPEATABLE READ;`,
				},
				{
					Query:       `SET TRANSACTION SNAPSHOT 'not-a-snapshot';`,
					ExpectedErr: `invalid snapshot identifier`,
				},
				{
					Query: `ROLLBACK;`,
				},
			},
		},
		{
			Name: "SET TRANSACTION SNAPSHOT requires repeatable read or serializable",
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query:       `SET TRANSACTION SNAPSHOT 'not-a-snapshot';`,
					ExpectedErr: `must have isolation level SERIALIZABLE or REPEATABLE READ`,
				},
				{
					Query: `ROLLBACK;`,
				},
			},
		},
	})
}

// TestTxidCurrentReportsNonzeroTransactionIdRepro reproduces a transaction
// correctness bug: PostgreSQL txid_current() returns the current nonzero
// transaction ID, stable within the surrounding transaction.
func TestTxidCurrentReportsNonzeroTransactionIdRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "txid_current reports a nonzero current transaction ID",
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `SELECT
							txid_current() = txid_current(),
							txid_current() > 0;`,
					Expected: []sql.Row{{"t", "t"}},
				},
				{
					Query: `ROLLBACK;`,
				},
			},
		},
	})
}

// TestRepeatableReadUsesStableSnapshotRepro reproduces a transaction isolation
// bug: PostgreSQL REPEATABLE READ transactions keep a stable snapshot even
// after other transactions commit new rows.
func TestRepeatableReadUsesStableSnapshotRepro(t *testing.T) {
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)
	ctx, defaultConn, controller := CreateServerWithPort(t, "postgres", port)
	t.Cleanup(func() {
		defaultConn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	for _, stmt := range []string{
		`CREATE TABLE repeatable_read_items (id INT PRIMARY KEY);`,
		`INSERT INTO repeatable_read_items VALUES (1);`,
	} {
		_, err = defaultConn.Exec(ctx, stmt)
		require.NoError(t, err, stmt)
	}

	dial := func(t *testing.T) *pgx.Conn {
		t.Helper()
		conn, err := pgx.Connect(ctx, fmt.Sprintf(
			"postgres://postgres:password@127.0.0.1:%d/postgres?sslmode=disable",
			port,
		))
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = conn.Close(context.Background())
		})
		return conn
	}

	reader := dial(t)
	writer := dial(t)

	_, err = reader.Exec(ctx, `BEGIN ISOLATION LEVEL REPEATABLE READ;`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = reader.Exec(context.Background(), `ROLLBACK;`)
	})

	var count int
	require.NoError(t, reader.QueryRow(ctx, `SELECT count(*) FROM repeatable_read_items;`).Scan(&count))
	require.Equal(t, 1, count)

	_, err = writer.Exec(ctx, `INSERT INTO repeatable_read_items VALUES (2);`)
	require.NoError(t, err)

	require.NoError(t, reader.QueryRow(ctx, `SELECT count(*) FROM repeatable_read_items;`).Scan(&count))
	require.Equal(t, 1, count)
	_, err = reader.Exec(ctx, `COMMIT;`)
	require.NoError(t, err)
}

// TestSerializableRejectsWriteSkewRepro reproduces a serializable-isolation
// bug: PostgreSQL aborts one of two concurrent transactions whose reads and
// writes would otherwise violate a cross-row invariant.
func TestSerializableRejectsWriteSkewRepro(t *testing.T) {
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)
	ctx, defaultConn, controller := CreateServerWithPort(t, "postgres", port)
	t.Cleanup(func() {
		defaultConn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	for _, stmt := range []string{
		`CREATE TABLE serializable_on_call (
			id INT PRIMARY KEY,
			on_call BOOL NOT NULL
		);`,
		`INSERT INTO serializable_on_call VALUES (1, true), (2, true);`,
	} {
		_, err = defaultConn.Exec(ctx, stmt)
		require.NoError(t, err, stmt)
	}

	dial := func(t *testing.T) *pgx.Conn {
		t.Helper()
		conn, err := pgx.Connect(ctx, fmt.Sprintf(
			"postgres://postgres:password@127.0.0.1:%d/postgres?sslmode=disable",
			port,
		))
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = conn.Close(context.Background())
		})
		return conn
	}

	a := dial(t)
	b := dial(t)

	for _, conn := range []*pgx.Conn{a, b} {
		_, err = conn.Exec(ctx, `BEGIN ISOLATION LEVEL SERIALIZABLE;`)
		require.NoError(t, err)
	}
	t.Cleanup(func() {
		_, _ = a.Exec(context.Background(), `ROLLBACK;`)
		_, _ = b.Exec(context.Background(), `ROLLBACK;`)
	})

	var aCount, bCount int
	require.NoError(t, a.QueryRow(ctx, `SELECT count(*) FROM serializable_on_call WHERE on_call;`).Scan(&aCount))
	require.NoError(t, b.QueryRow(ctx, `SELECT count(*) FROM serializable_on_call WHERE on_call;`).Scan(&bCount))
	require.Equal(t, 2, aCount)
	require.Equal(t, 2, bCount)

	_, err = a.Exec(ctx, `UPDATE serializable_on_call SET on_call = false WHERE id = 1;`)
	require.NoError(t, err)
	_, err = b.Exec(ctx, `UPDATE serializable_on_call SET on_call = false WHERE id = 2;`)
	require.NoError(t, err)

	_, aErr := a.Exec(ctx, `COMMIT;`)
	_, bErr := b.Exec(ctx, `COMMIT;`)

	var onCall int
	require.NoError(t, defaultConn.Default.QueryRow(ctx, `SELECT count(*) FROM serializable_on_call WHERE on_call;`).Scan(&onCall))
	require.Equal(t, 1, onCall)
	require.True(t, (aErr == nil) != (bErr == nil), "exactly one serializable transaction should commit; aErr=%v bErr=%v", aErr, bErr)
}

// TestSerializableRejectsStaleReadModifyWriteRepro reproduces a serializable
// isolation bug: PostgreSQL aborts one of two transactions that both read the
// same row and then write a value derived from the stale snapshot.
func TestSerializableRejectsStaleReadModifyWriteRepro(t *testing.T) {
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)
	ctx, defaultConn, controller := CreateServerWithPort(t, "postgres", port)
	t.Cleanup(func() {
		defaultConn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	for _, stmt := range []string{
		`CREATE TABLE serializable_lost_update (
			id INT PRIMARY KEY,
			balance INT NOT NULL
		);`,
		`INSERT INTO serializable_lost_update VALUES (1, 0);`,
	} {
		_, err = defaultConn.Exec(ctx, stmt)
		require.NoError(t, err, stmt)
	}

	dial := func(t *testing.T) *pgx.Conn {
		t.Helper()
		conn, err := pgx.Connect(ctx, fmt.Sprintf(
			"postgres://postgres:password@127.0.0.1:%d/postgres?sslmode=disable",
			port,
		))
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = conn.Close(context.Background())
		})
		return conn
	}

	a := dial(t)
	b := dial(t)

	for _, conn := range []*pgx.Conn{a, b} {
		_, err = conn.Exec(ctx, `BEGIN ISOLATION LEVEL SERIALIZABLE;`)
		require.NoError(t, err)
	}
	t.Cleanup(func() {
		_, _ = a.Exec(context.Background(), `ROLLBACK;`)
		_, _ = b.Exec(context.Background(), `ROLLBACK;`)
	})

	var aBalance, bBalance int
	require.NoError(t, a.QueryRow(ctx, `SELECT balance FROM serializable_lost_update WHERE id = 1;`).Scan(&aBalance))
	require.NoError(t, b.QueryRow(ctx, `SELECT balance FROM serializable_lost_update WHERE id = 1;`).Scan(&bBalance))
	require.Equal(t, 0, aBalance)
	require.Equal(t, 0, bBalance)

	_, err = a.Exec(ctx, `UPDATE serializable_lost_update SET balance = $1 WHERE id = 1;`, aBalance+1)
	require.NoError(t, err)

	bUpdateErrCh := make(chan error, 1)
	go func() {
		_, updateErr := b.Exec(context.Background(), `UPDATE serializable_lost_update SET balance = $1 WHERE id = 1;`, bBalance+1)
		bUpdateErrCh <- updateErr
	}()

	_, aCommitErr := a.Exec(ctx, `COMMIT;`)
	bUpdateErr := <-bUpdateErrCh
	_, bCommitErr := b.Exec(ctx, `COMMIT;`)

	require.NoError(t, aCommitErr)
	var finalBalance int
	require.NoError(t, defaultConn.Default.QueryRow(ctx, `SELECT balance FROM serializable_lost_update WHERE id = 1;`).Scan(&finalBalance))
	require.True(t, bUpdateErr != nil || bCommitErr != nil, "stale serializable transaction should abort; updateErr=%v commitErr=%v finalBalance=%d", bUpdateErr, bCommitErr, finalBalance)
}

// TestReadUncommittedDoesNotExposeDirtyRowsRepro guards PostgreSQL's behavior
// that READ UNCOMMITTED is implemented no dirtier than READ COMMITTED.
func TestReadUncommittedDoesNotExposeDirtyRowsRepro(t *testing.T) {
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)
	ctx, defaultConn, controller := CreateServerWithPort(t, "postgres", port)
	t.Cleanup(func() {
		defaultConn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	_, err = defaultConn.Exec(ctx, `CREATE TABLE read_uncommitted_items (id INT PRIMARY KEY);`)
	require.NoError(t, err)

	dial := func(t *testing.T) *pgx.Conn {
		t.Helper()
		conn, err := pgx.Connect(ctx, fmt.Sprintf(
			"postgres://postgres:password@127.0.0.1:%d/postgres?sslmode=disable",
			port,
		))
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = conn.Close(context.Background())
		})
		return conn
	}

	writer := dial(t)
	reader := dial(t)

	_, err = writer.Exec(ctx, `BEGIN; INSERT INTO read_uncommitted_items VALUES (1);`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = writer.Exec(context.Background(), `ROLLBACK;`)
	})

	_, err = reader.Exec(ctx, `BEGIN ISOLATION LEVEL READ UNCOMMITTED;`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = reader.Exec(context.Background(), `ROLLBACK;`)
	})

	var count int
	require.NoError(t, reader.QueryRow(ctx, `SELECT count(*) FROM read_uncommitted_items;`).Scan(&count))
	require.Equal(t, 0, count)
}
