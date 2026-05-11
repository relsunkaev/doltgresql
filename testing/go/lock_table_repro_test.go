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
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// TestLockTableAccessExclusiveBlocksReadersRepro reproduces a data consistency
// bug: Doltgres accepts LOCK TABLE IN ACCESS EXCLUSIVE MODE, but readers do not
// block behind the held relation lock.
func TestLockTableAccessExclusiveBlocksReadersRepro(t *testing.T) {
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)
	ctx, defaultConn, controller := CreateServerWithPort(t, "postgres", port)
	t.Cleanup(func() {
		defaultConn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	for _, stmt := range []string{
		`CREATE TABLE lock_target (id INT PRIMARY KEY, label TEXT);`,
		`INSERT INTO lock_target VALUES (1, 'alpha'), (2, 'beta');`,
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

	locker := dial(t)
	reader := dial(t)

	lockTx, err := locker.Begin(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = lockTx.Rollback(context.Background())
	})
	_, err = lockTx.Exec(ctx, `LOCK TABLE lock_target IN ACCESS EXCLUSIVE MODE;`)
	require.NoError(t, err)

	readDone := make(chan error, 1)
	go func() {
		var count int
		err := reader.QueryRow(ctx, `SELECT count(*) FROM lock_target;`).Scan(&count)
		if err == nil && count != 2 {
			err = fmt.Errorf("expected count 2 after lock release, got %d", count)
		}
		readDone <- err
	}()

	select {
	case err := <-readDone:
		require.NoError(t, err)
		t.Fatalf("reader completed while ACCESS EXCLUSIVE lock was still held")
	case <-time.After(50 * time.Millisecond):
	}

	require.NoError(t, lockTx.Rollback(ctx))
	select {
	case err := <-readDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("reader did not complete after releasing ACCESS EXCLUSIVE lock")
	}
}

// TestLockTableAccessExclusiveBlocksWritersRepro reproduces a data consistency
// bug: Doltgres accepts LOCK TABLE IN ACCESS EXCLUSIVE MODE, but writers do not
// block behind the held relation lock.
func TestLockTableAccessExclusiveBlocksWritersRepro(t *testing.T) {
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)
	ctx, defaultConn, controller := CreateServerWithPort(t, "postgres", port)
	t.Cleanup(func() {
		defaultConn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	for _, stmt := range []string{
		`CREATE TABLE lock_write_target (id INT PRIMARY KEY, label TEXT);`,
		`INSERT INTO lock_write_target VALUES (1, 'alpha');`,
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

	locker := dial(t)
	writer := dial(t)

	lockTx, err := locker.Begin(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = lockTx.Rollback(context.Background())
	})
	_, err = lockTx.Exec(ctx, `LOCK TABLE lock_write_target IN ACCESS EXCLUSIVE MODE;`)
	require.NoError(t, err)

	writeDone := make(chan error, 1)
	go func() {
		_, err := writer.Exec(ctx, `INSERT INTO lock_write_target VALUES (2, 'beta');`)
		writeDone <- err
	}()

	select {
	case err := <-writeDone:
		require.NoError(t, err)
		t.Fatalf("writer completed while ACCESS EXCLUSIVE lock was still held")
	case <-time.After(50 * time.Millisecond):
	}

	require.NoError(t, lockTx.Rollback(ctx))
	select {
	case err := <-writeDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("writer did not complete after releasing ACCESS EXCLUSIVE lock")
	}
}

// TestLockTableShareModeBlocksWritersRepro reproduces a data consistency bug:
// Doltgres accepts LOCK TABLE IN SHARE MODE, but writers do not block behind
// the held relation lock.
func TestLockTableShareModeBlocksWritersRepro(t *testing.T) {
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)
	ctx, defaultConn, controller := CreateServerWithPort(t, "postgres", port)
	t.Cleanup(func() {
		defaultConn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	for _, stmt := range []string{
		`CREATE TABLE lock_share_write_target (id INT PRIMARY KEY, label TEXT);`,
		`INSERT INTO lock_share_write_target VALUES (1, 'alpha');`,
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

	locker := dial(t)
	writer := dial(t)

	lockTx, err := locker.Begin(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = lockTx.Rollback(context.Background())
	})
	_, err = lockTx.Exec(ctx, `LOCK TABLE lock_share_write_target IN SHARE MODE;`)
	require.NoError(t, err)

	writeDone := make(chan error, 1)
	go func() {
		_, err := writer.Exec(ctx, `INSERT INTO lock_share_write_target VALUES (2, 'beta');`)
		writeDone <- err
	}()

	select {
	case err := <-writeDone:
		require.NoError(t, err)
		t.Fatalf("writer completed while SHARE lock was still held")
	case <-time.After(50 * time.Millisecond):
	}

	require.NoError(t, lockTx.Rollback(ctx))
	select {
	case err := <-writeDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("writer did not complete after releasing SHARE lock")
	}
}

// TestLockTableRequiresTransactionBlockRepro reproduces a transaction-boundary
// bug: PostgreSQL rejects LOCK TABLE outside an explicit transaction block.
func TestLockTableRequiresTransactionBlockRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "LOCK TABLE requires explicit transaction block",
			SetUpScript: []string{
				`CREATE TABLE lock_transaction_target (id INT PRIMARY KEY);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `LOCK TABLE lock_transaction_target IN ACCESS EXCLUSIVE MODE;`,
					ExpectedErr: `can only be used in transaction blocks`,
				},
			},
		},
	})
}

// TestLockTableNowaitRejectsConflictingLocksRepro reproduces a data consistency
// bug: PostgreSQL rejects conflicting LOCK TABLE ... NOWAIT attempts
// immediately while another transaction holds an incompatible table lock.
func TestLockTableNowaitRejectsConflictingLocksRepro(t *testing.T) {
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)
	ctx, defaultConn, controller := CreateServerWithPort(t, "postgres", port)
	t.Cleanup(func() {
		defaultConn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	_, err = defaultConn.Exec(ctx, `CREATE TABLE lock_nowait_target (id INT PRIMARY KEY);`)
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

	locker := dial(t)
	contender := dial(t)

	lockTx, err := locker.Begin(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = lockTx.Rollback(context.Background())
	})
	_, err = lockTx.Exec(ctx, `LOCK TABLE lock_nowait_target IN ACCESS EXCLUSIVE MODE;`)
	require.NoError(t, err)

	contenderCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	started := time.Now()
	_, err = contender.Exec(contenderCtx, `BEGIN; LOCK TABLE lock_nowait_target IN ACCESS SHARE MODE NOWAIT; ROLLBACK;`)
	elapsed := time.Since(started)

	require.Error(t, err)
	require.Contains(t, err.Error(), "could not obtain lock")
	require.Less(t, elapsed, 500*time.Millisecond, "NOWAIT should fail immediately")
	require.NoError(t, lockTx.Rollback(ctx))
}

// TestLockTableRequiresTablePrivilegeRepro guards authorization for explicit
// LOCK TABLE statements.
func TestLockTableRequiresTablePrivilegeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "LOCK TABLE requires privileges on locked relation",
			SetUpScript: []string{
				`CREATE USER lock_table_user PASSWORD 'lock';`,
				`CREATE TABLE lock_table_private (id INT PRIMARY KEY);`,
				`GRANT USAGE ON SCHEMA public TO lock_table_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `BEGIN;`,
					Username: `lock_table_user`,
					Password: `lock`,
				},
				{
					Query:       `LOCK TABLE lock_table_private IN ACCESS EXCLUSIVE MODE;`,
					ExpectedErr: `permission denied`,
					Username:    `lock_table_user`,
					Password:    `lock`,
				},
				{
					Query:    `ROLLBACK;`,
					Username: `lock_table_user`,
					Password: `lock`,
				},
			},
		},
	})
}
