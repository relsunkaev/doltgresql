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
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/doltgresql/server/replsource"
)

func TestLogicalReplicationSourceProtocolAndCatalogs(t *testing.T) {
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	slotName := "dg_logical_source_slot"
	replConn := connectReplicationConn(t, ctx, port)
	defer replConn.Close(context.Background())

	system, err := pglogrepl.IdentifySystem(ctx, replConn)
	require.NoError(t, err)
	require.Equal(t, int32(1), system.Timeline)
	require.Equal(t, "postgres", system.DBName)
	require.Equal(t, pglogrepl.LSN(0), system.XLogPos)

	slot, err := pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication,
	})
	require.NoError(t, err)
	require.Equal(t, slotName, slot.SlotName)
	require.Equal(t, "pgoutput", slot.OutputPlugin)

	var plugin, slotType, confirmedFlush string
	var active bool
	require.NoError(t, conn.Current.QueryRow(ctx, `
		SELECT plugin, slot_type, active, confirmed_flush_lsn::text
		FROM pg_catalog.pg_replication_slots
		WHERE slot_name = $1`, slotName).Scan(&plugin, &slotType, &active, &confirmedFlush))
	require.Equal(t, "pgoutput", plugin)
	require.Equal(t, "logical", slotType)
	require.False(t, active)
	require.Equal(t, "0/0", confirmedFlush)

	var totalTxns int64
	require.NoError(t, conn.Current.QueryRow(ctx, `
		SELECT total_txns
		FROM pg_catalog.pg_stat_replication_slots
		WHERE slot_name = $1`, slotName).Scan(&totalTxns))
	require.Equal(t, int64(0), totalTxns)

	require.NoError(t, pglogrepl.StartReplication(ctx, replConn, slotName, 0, pglogrepl.StartReplicationOptions{
		Mode: pglogrepl.LogicalReplication,
		PluginArgs: []string{
			`"proto_version" '1'`,
			`"publication_names" 'dg_logical_source_pub'`,
		},
	}))

	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])
	parsedKeepalive, err := pglogrepl.ParsePrimaryKeepaliveMessage(keepalive.Data[1:])
	require.NoError(t, err)
	require.Equal(t, pglogrepl.LSN(0), parsedKeepalive.ServerWALEnd)

	require.NoError(t, pglogrepl.SendStandbyStatusUpdate(ctx, replConn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: pglogrepl.LSN(0x16),
		WALFlushPosition: pglogrepl.LSN(0x16),
		WALApplyPosition: pglogrepl.LSN(0x16),
		ReplyRequested:   true,
	}))
	reply := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), reply.Data[0])

	waitForReplicationState(t, ctx, conn, slotName)

	_, err = pglogrepl.SendStandbyCopyDone(ctx, replConn)
	require.NoError(t, err)
	require.NoError(t, replConn.Close(ctx))
	replConn = nil

	waitForInactiveSlot(t, ctx, conn, slotName)

	dropConn := connectReplicationConn(t, ctx, port)
	defer dropConn.Close(context.Background())
	require.NoError(t, pglogrepl.DropReplicationSlot(ctx, dropConn, slotName, pglogrepl.DropReplicationSlotOptions{}))

	var slotCount int
	require.NoError(t, conn.Current.QueryRow(ctx, `
		SELECT count(*) FROM pg_catalog.pg_replication_slots WHERE slot_name = $1`, slotName).Scan(&slotCount))
	require.Equal(t, 0, slotCount)
}

func connectReplicationConn(t *testing.T, ctx context.Context, port int) *pgconn.PgConn {
	t.Helper()
	conn, err := pgconn.Connect(ctx, fmt.Sprintf("postgres://postgres:password@127.0.0.1:%d/postgres?sslmode=disable&replication=database&application_name=dg-logical-source-test", port))
	require.NoError(t, err)
	return conn
}

func receiveReplicationCopyData(t *testing.T, conn *pgconn.PgConn) *pgproto3.CopyData {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	msg, err := conn.ReceiveMessage(ctx)
	require.NoError(t, err)
	copyData, ok := msg.(*pgproto3.CopyData)
	require.Truef(t, ok, "expected CopyData, got %T", msg)
	require.NotEmpty(t, copyData.Data)
	return copyData
}

func waitForReplicationState(t *testing.T, ctx context.Context, conn *Connection, slotName string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		var state, sentLSN, writeLSN, flushLSN, replayLSN, syncState, lag string
		var replyTimeSet bool
		lastErr = conn.Current.QueryRow(ctx, `
			SELECT state, sent_lsn::text, write_lsn::text, flush_lsn::text, replay_lsn::text, sync_state,
			       reply_time IS NOT NULL, pg_wal_lsn_diff(write_lsn, sent_lsn)::text
			FROM pg_catalog.pg_stat_replication
			WHERE application_name = 'dg-logical-source-test'`).Scan(
			&state, &sentLSN, &writeLSN, &flushLSN, &replayLSN, &syncState, &replyTimeSet, &lag)
		if lastErr == nil && state == "streaming" && sentLSN == "0/16" && writeLSN == "0/16" &&
			flushLSN == "0/16" && replayLSN == "0/16" && syncState == "async" && replyTimeSet && lag == "0" {
			var active bool
			var activePIDSet bool
			var confirmedFlush string
			require.NoError(t, conn.Current.QueryRow(ctx, `
				SELECT active, active_pid IS NOT NULL, confirmed_flush_lsn::text
				FROM pg_catalog.pg_replication_slots
				WHERE slot_name = $1`, slotName).Scan(&active, &activePIDSet, &confirmedFlush))
			require.True(t, active)
			require.True(t, activePIDSet)
			require.Equal(t, "0/16", confirmedFlush)
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	require.NoError(t, lastErr)
	require.FailNow(t, "replication sender state did not become visible in pg_stat_replication")
}

func waitForInactiveSlot(t *testing.T, ctx context.Context, conn *Connection, slotName string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		var active bool
		var activePIDSet bool
		lastErr = conn.Current.QueryRow(ctx, `
			SELECT active, active_pid IS NOT NULL
			FROM pg_catalog.pg_replication_slots
			WHERE slot_name = $1`, slotName).Scan(&active, &activePIDSet)
		if lastErr == nil && !active && !activePIDSet {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	require.NoError(t, lastErr)
	require.FailNow(t, "replication slot did not become inactive")
}
