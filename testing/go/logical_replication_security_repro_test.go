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
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/doltgresql/server/replsource"
)

// TestPgTerminateBackendRequiresPrivilegeForReplicationSenderRepro reproduces
// a security bug: PostgreSQL restricts terminating another user's backend to
// superusers or roles with pg_signal_backend privileges.
func TestPgTerminateBackendRequiresPrivilegeForReplicationSenderRepro(t *testing.T) {
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	t.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	slotName := "terminate_privilege_slot"
	for _, stmt := range []string{
		`CREATE USER terminate_attacker PASSWORD 'pw';`,
		`CREATE TABLE terminate_privilege_items (id INT PRIMARY KEY);`,
		`CREATE PUBLICATION terminate_privilege_pub FOR TABLE terminate_privilege_items;`,
	} {
		_, err = conn.Current.Exec(ctx, stmt)
		require.NoError(t, err, stmt)
	}

	replConn := connectReplicationConn(t, ctx, port)
	t.Cleanup(func() {
		_ = replConn.Close(context.Background())
	})
	_, err = pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication,
	})
	require.NoError(t, err)
	require.NoError(t, pglogrepl.StartReplication(ctx, replConn, slotName, 0, pglogrepl.StartReplicationOptions{
		Mode: pglogrepl.LogicalReplication,
		PluginArgs: []string{
			`"proto_version" '1'`,
			`"publication_names" 'terminate_privilege_pub'`,
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	var activePID int32
	require.NoError(t, conn.Current.QueryRow(ctx, `
		SELECT active_pid
		FROM pg_catalog.pg_replication_slots
		WHERE slot_name = $1`, slotName).Scan(&activePID))
	require.Greater(t, activePID, int32(0))

	attacker, err := pgx.Connect(ctx, fmt.Sprintf(
		"postgres://terminate_attacker:pw@127.0.0.1:%d/postgres?sslmode=disable",
		port,
	))
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = attacker.Close(context.Background())
	})

	var terminated bool
	err = attacker.QueryRow(ctx, `SELECT pg_terminate_backend($1::int4);`, activePID).Scan(&terminated)
	require.Error(t, err, "non-privileged role should not terminate another user's replication backend; terminated=%v", terminated)
}

// TestPgSignalBackendCannotTerminateSuperuserReplicationSenderRepro reproduces
// a security bug: pg_signal_backend permits signaling ordinary backends, but
// PostgreSQL still prevents non-superusers from terminating superuser-owned
// backends.
func TestPgSignalBackendCannotTerminateSuperuserReplicationSenderRepro(t *testing.T) {
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	t.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	slotName := "terminate_superuser_sender_slot"
	for _, stmt := range []string{
		`CREATE USER terminate_signaler PASSWORD 'pw';`,
		`GRANT pg_signal_backend TO terminate_signaler;`,
		`CREATE TABLE terminate_superuser_sender_items (id INT PRIMARY KEY);`,
		`CREATE PUBLICATION terminate_superuser_sender_pub FOR TABLE terminate_superuser_sender_items;`,
	} {
		_, err = conn.Current.Exec(ctx, stmt)
		require.NoError(t, err, stmt)
	}

	replConn := connectReplicationConn(t, ctx, port)
	t.Cleanup(func() {
		_ = replConn.Close(context.Background())
	})
	_, err = pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication,
	})
	require.NoError(t, err)
	require.NoError(t, pglogrepl.StartReplication(ctx, replConn, slotName, 0, pglogrepl.StartReplicationOptions{
		Mode: pglogrepl.LogicalReplication,
		PluginArgs: []string{
			`"proto_version" '1'`,
			`"publication_names" 'terminate_superuser_sender_pub'`,
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	var activePID int32
	require.NoError(t, conn.Current.QueryRow(ctx, `
		SELECT active_pid
		FROM pg_catalog.pg_replication_slots
		WHERE slot_name = $1`, slotName).Scan(&activePID))
	require.Greater(t, activePID, int32(0))

	signaler, err := pgx.Connect(ctx, fmt.Sprintf(
		"postgres://terminate_signaler:pw@127.0.0.1:%d/postgres?sslmode=disable",
		port,
	))
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = signaler.Close(context.Background())
	})

	var terminated bool
	err = signaler.QueryRow(ctx, `SELECT pg_terminate_backend($1::int4);`, activePID).Scan(&terminated)
	require.Error(t, err, "pg_signal_backend role should not terminate a superuser-owned replication backend; terminated=%v", terminated)
}
