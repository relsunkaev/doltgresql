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
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

// TestPgNotifyProbe pins PG NOTIFY / pg_notify compatibility. Real apps use
// this for cache-invalidation fanout and listener queues; this test keeps the
// simple statement/function call shapes covered while TestPgNotifyDelivery
// exercises asynchronous delivery.
func TestPgNotifyProbe(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "NOTIFY statement is accepted",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `NOTIFY my_channel, 'payload';`,
				},
				{
					Query: `NOTIFY my_channel;`,
				},
			},
		},
		{
			Name:        "pg_notify returns void",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT pg_notify('my_channel', 'payload');`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-notify-probe-test-testpgnotifyprobe-0001-select-pg_notify-my_channel-payload"},
				},
			},
		},
	})
}

func TestPgNotifyDelivery(t *testing.T) {
	port, err := sql.GetEmptyPort()
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
			"postgres://postgres:password@127.0.0.1:%d/postgres?sslmode=disable",
			port,
		))
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, conn.Close(context.Background()))
		})
		return conn
	}

	listener := dial(t)
	notifier := dial(t)

	_, err = listener.Exec(ctx, "LISTEN my_channel")
	require.NoError(t, err)

	_, err = notifier.Exec(ctx, "NOTIFY my_channel, 'payload one'")
	require.NoError(t, err)
	notification := waitForPgNotification(t, listener)
	require.Equal(t, "my_channel", notification.Channel)
	require.Equal(t, "payload one", notification.Payload)

	_, err = notifier.Exec(ctx, "SELECT pg_notify('my_channel', 'payload two')")
	require.NoError(t, err)
	notification = waitForPgNotification(t, listener)
	require.Equal(t, "my_channel", notification.Channel)
	require.Equal(t, "payload two", notification.Payload)

	_, err = listener.Exec(ctx, "UNLISTEN my_channel")
	require.NoError(t, err)
	_, err = notifier.Exec(ctx, "NOTIFY my_channel, 'payload three'")
	require.NoError(t, err)
	requireNoPgNotification(t, listener)

	_, err = listener.Exec(ctx, "LISTEN my_channel")
	require.NoError(t, err)
	_, err = listener.Exec(ctx, "LISTEN other_channel")
	require.NoError(t, err)
	_, err = listener.Exec(ctx, "UNLISTEN *")
	require.NoError(t, err)
	_, err = notifier.Exec(ctx, "NOTIFY my_channel, 'payload four'")
	require.NoError(t, err)
	requireNoPgNotification(t, listener)
}

func TestPgNotifyTransactionSemantics(t *testing.T) {
	port, err := sql.GetEmptyPort()
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
			"postgres://postgres:password@127.0.0.1:%d/postgres?sslmode=disable",
			port,
		))
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, conn.Close(context.Background()))
		})
		return conn
	}

	listener := dial(t)
	notifier := dial(t)

	_, err = listener.Exec(ctx, "LISTEN tx_channel")
	require.NoError(t, err)

	_, err = notifier.Exec(ctx, "BEGIN")
	require.NoError(t, err)
	_, err = notifier.Exec(ctx, "NOTIFY tx_channel, 'before commit'")
	require.NoError(t, err)
	_, err = notifier.Exec(ctx, "NOTIFY tx_channel, 'before commit'")
	require.NoError(t, err)
	requireNoPgNotification(t, listener)
	_, err = notifier.Exec(ctx, "COMMIT")
	require.NoError(t, err)
	notification := waitForPgNotification(t, listener)
	require.Equal(t, "tx_channel", notification.Channel)
	require.Equal(t, "before commit", notification.Payload)
	requireNoPgNotification(t, listener)

	_, err = notifier.Exec(ctx, "BEGIN")
	require.NoError(t, err)
	_, err = notifier.Exec(ctx, "SELECT pg_notify('tx_channel', 'rolled back')")
	require.NoError(t, err)
	_, err = notifier.Exec(ctx, "ROLLBACK")
	require.NoError(t, err)
	requireNoPgNotification(t, listener)
}

func TestPgNotifyListenUnlistenTransactionSemantics(t *testing.T) {
	port, err := sql.GetEmptyPort()
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
			"postgres://postgres:password@127.0.0.1:%d/postgres?sslmode=disable",
			port,
		))
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, conn.Close(context.Background()))
		})
		return conn
	}

	listener := dial(t)
	notifier := dial(t)

	_, err = listener.Exec(ctx, "BEGIN")
	require.NoError(t, err)
	_, err = listener.Exec(ctx, "LISTEN delayed_channel")
	require.NoError(t, err)
	_, err = notifier.Exec(ctx, "NOTIFY delayed_channel, 'before listen commit'")
	require.NoError(t, err)
	requireNoPgNotification(t, listener)
	_, err = listener.Exec(ctx, "COMMIT")
	require.NoError(t, err)
	requireNoPgNotification(t, listener)

	_, err = notifier.Exec(ctx, "NOTIFY delayed_channel, 'after listen commit'")
	require.NoError(t, err)
	notification := waitForPgNotification(t, listener)
	require.Equal(t, "delayed_channel", notification.Channel)
	require.Equal(t, "after listen commit", notification.Payload)

	_, err = listener.Exec(ctx, "BEGIN")
	require.NoError(t, err)
	_, err = listener.Exec(ctx, "UNLISTEN delayed_channel")
	require.NoError(t, err)
	_, err = notifier.Exec(ctx, "NOTIFY delayed_channel, 'before unlisten commit'")
	require.NoError(t, err)
	notification = waitForPgNotification(t, listener)
	require.Equal(t, "delayed_channel", notification.Channel)
	require.Equal(t, "before unlisten commit", notification.Payload)
	_, err = listener.Exec(ctx, "COMMIT")
	require.NoError(t, err)

	_, err = notifier.Exec(ctx, "NOTIFY delayed_channel, 'after unlisten commit'")
	require.NoError(t, err)
	requireNoPgNotification(t, listener)

	_, err = listener.Exec(ctx, "BEGIN")
	require.NoError(t, err)
	_, err = listener.Exec(ctx, "LISTEN rolled_back_channel")
	require.NoError(t, err)
	_, err = listener.Exec(ctx, "ROLLBACK")
	require.NoError(t, err)
	_, err = notifier.Exec(ctx, "NOTIFY rolled_back_channel, 'after rollback'")
	require.NoError(t, err)
	requireNoPgNotification(t, listener)
}

func waitForPgNotification(t *testing.T, conn *pgx.Conn) *pgconn.Notification {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	notification, err := conn.WaitForNotification(ctx)
	require.NoError(t, err)
	return notification
}

func requireNoPgNotification(t *testing.T, conn *pgx.Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := conn.WaitForNotification(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}
