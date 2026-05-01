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
	"os"
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
	_, err = conn.Current.Exec(ctx, "CREATE TABLE dg_rep_items (tenant_id BIGINT PRIMARY KEY, label TEXT);")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "CREATE PUBLICATION dg_logical_source_pub FOR TABLE dg_rep_items;")
	require.NoError(t, err)

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

	_, err = conn.Current.Exec(ctx, "INSERT INTO dg_rep_items VALUES (42, 'forty-two');")
	require.NoError(t, err)
	relation, insert, commit := receiveInsertChange(t, replConn)
	requireInsertChange(t, relation, insert, "dg_rep_items", "42", "forty-two")
	require.Greater(t, commit.CommitLSN, pglogrepl.LSN(0))

	var currentLSN string
	require.NoError(t, conn.Current.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text;").Scan(&currentLSN))
	require.Equal(t, commit.CommitLSN.String(), currentLSN)

	_, err = conn.Current.Exec(ctx, "PREPARE dg_rep_insert(bigint, text) AS INSERT INTO dg_rep_items VALUES ($1, $2);")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "EXECUTE dg_rep_insert(43, 'forty-three');")
	require.NoError(t, err)
	relation, insert, commit = receiveInsertChange(t, replConn)
	requireInsertChange(t, relation, insert, "dg_rep_items", "43", "forty-three")
	require.Greater(t, commit.CommitLSN, pglogrepl.LSN(0))

	_, err = conn.Current.Exec(ctx, "INSERT INTO dg_rep_items VALUES ($1, $2);", int64(44), "forty-four")
	require.NoError(t, err)
	relation, insert, commit = receiveInsertChange(t, replConn)
	requireInsertChange(t, relation, insert, "dg_rep_items", "44", "forty-four")
	require.Greater(t, commit.CommitLSN, pglogrepl.LSN(0))

	var returnedID int64
	require.NoError(t, conn.Current.QueryRow(ctx,
		"INSERT INTO dg_rep_items VALUES (45, 'forty-five') RETURNING tenant_id;").Scan(&returnedID))
	require.Equal(t, int64(45), returnedID)
	relation, insert, commit = receiveInsertChange(t, replConn)
	requireInsertChange(t, relation, insert, "dg_rep_items", "45", "forty-five")
	require.Greater(t, commit.CommitLSN, pglogrepl.LSN(0))

	_, err = conn.Current.Exec(ctx, "UPDATE dg_rep_items SET label = 'forty-five-updated' WHERE tenant_id = 45;")
	require.NoError(t, err)
	relation, update, commit := receiveUpdateChange(t, replConn)
	requireUpdateChange(t, relation, update, "dg_rep_items", "45", "forty-five-updated")
	require.Greater(t, commit.CommitLSN, pglogrepl.LSN(0))

	_, err = conn.Current.Exec(ctx, "DELETE FROM dg_rep_items WHERE tenant_id = 45;")
	require.NoError(t, err)
	relation, deleteMessage, commit := receiveDeleteChange(t, replConn)
	requireDeleteChange(t, relation, deleteMessage, "dg_rep_items", "45", "forty-five-updated")
	require.Greater(t, commit.CommitLSN, pglogrepl.LSN(0))

	_, err = conn.Current.Exec(ctx, "BEGIN;")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "INSERT INTO dg_rep_items VALUES (46, 'rolled-back');")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "ROLLBACK;")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "INSERT INTO dg_rep_items VALUES (47, 'after-rollback');")
	require.NoError(t, err)
	relation, insert, commit = receiveInsertChange(t, replConn)
	requireInsertChange(t, relation, insert, "dg_rep_items", "47", "after-rollback")
	require.Greater(t, commit.CommitLSN, pglogrepl.LSN(0))

	require.NoError(t, pglogrepl.SendStandbyStatusUpdate(ctx, replConn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: commit.CommitLSN,
		WALFlushPosition: commit.CommitLSN,
		WALApplyPosition: commit.CommitLSN,
		ReplyRequested:   true,
	}))
	reply := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), reply.Data[0])

	waitForReplicationState(t, ctx, conn, slotName, commit.CommitLSN.String())

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

func TestLogicalReplicationSourceFiltersPublicationAndIgnoresClientLSNFeedback(t *testing.T) {
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	slotName := "dg_logical_filter_slot"
	_, err = conn.Current.Exec(ctx, "CREATE TABLE dg_pub_a_items (tenant_id BIGINT PRIMARY KEY, label TEXT);")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "CREATE TABLE dg_pub_b_items (tenant_id BIGINT PRIMARY KEY, label TEXT);")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "CREATE PUBLICATION dg_pub_a FOR TABLE dg_pub_a_items;")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "CREATE PUBLICATION dg_pub_b FOR TABLE dg_pub_b_items;")
	require.NoError(t, err)

	replConn := connectReplicationConn(t, ctx, port)
	defer replConn.Close(context.Background())
	_, err = pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication,
	})
	require.NoError(t, err)
	require.NoError(t, pglogrepl.StartReplication(ctx, replConn, slotName, 0, pglogrepl.StartReplicationOptions{
		Mode: pglogrepl.LogicalReplication,
		PluginArgs: []string{
			`"proto_version" '1'`,
			`"publication_names" 'dg_pub_a'`,
		},
	}))

	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])
	require.NoError(t, pglogrepl.SendStandbyStatusUpdate(ctx, replConn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: pglogrepl.LSN(0x70000000),
		WALFlushPosition: pglogrepl.LSN(0x70000000),
		WALApplyPosition: pglogrepl.LSN(0x70000000),
		ReplyRequested:   true,
	}))
	reply := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), reply.Data[0])

	var currentLSN string
	require.NoError(t, conn.Current.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text;").Scan(&currentLSN))
	require.Equal(t, "0/0", currentLSN)

	_, err = conn.Current.Exec(ctx, "INSERT INTO dg_pub_b_items VALUES (1, 'wrong-publication');")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "INSERT INTO dg_pub_a_items VALUES (2, 'right-publication');")
	require.NoError(t, err)
	relation, insert, _ := receiveInsertChange(t, replConn)
	requireInsertChange(t, relation, insert, "dg_pub_a_items", "2", "right-publication")
}

func TestLogicalReplicationSourceAdvancesLocalLSNWithoutActiveSender(t *testing.T) {
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	_, err = conn.Current.Exec(ctx, "CREATE TABLE dg_lsn_items (tenant_id BIGINT PRIMARY KEY, label TEXT);")
	require.NoError(t, err)
	var before string
	require.NoError(t, conn.Current.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text;").Scan(&before))
	require.Equal(t, "0/0", before)

	_, err = conn.Current.Exec(ctx, "INSERT INTO dg_lsn_items VALUES (1, 'one');")
	require.NoError(t, err)
	var after string
	require.NoError(t, conn.Current.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text;").Scan(&after))
	require.NotEqual(t, before, after)
	require.Equal(t, "0/10", after)

	_, err = conn.Current.Exec(ctx, "UPDATE dg_lsn_items SET label = 'missing' WHERE tenant_id = 999;")
	require.NoError(t, err)
	var afterNoop string
	require.NoError(t, conn.Current.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text;").Scan(&afterNoop))
	require.Equal(t, after, afterNoop)

	_, err = conn.Current.Exec(ctx, "BEGIN;")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "INSERT INTO dg_lsn_items VALUES (2, 'rolled-back');")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "ROLLBACK;")
	require.NoError(t, err)
	var afterRollback string
	require.NoError(t, conn.Current.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text;").Scan(&afterRollback))
	require.Equal(t, after, afterRollback)

	_, err = conn.Current.Exec(ctx, "BEGIN;")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "INSERT INTO dg_lsn_items VALUES (2, 'committed');")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "COMMIT;")
	require.NoError(t, err)
	var afterCommit string
	require.NoError(t, conn.Current.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text;").Scan(&afterCommit))
	require.Equal(t, "0/20", afterCommit)
}

func TestLogicalReplicationSourceReplaysInactiveSlotChangesAfterRestart(t *testing.T) {
	replsource.ResetForTests()
	dbDir, err := os.MkdirTemp(os.TempDir(), t.Name())
	require.NoError(t, err)

	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerLocalInDirWithPort(t, "postgres", dbDir, port)
	slotName := "dg_replay_restart_slot"
	_, err = conn.Current.Exec(ctx, "CREATE TABLE dg_replay_items (tenant_id BIGINT PRIMARY KEY, label TEXT);")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "CREATE PUBLICATION dg_replay_pub FOR TABLE dg_replay_items;")
	require.NoError(t, err)

	replConn := connectReplicationConn(t, ctx, port)
	_, err = pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication,
	})
	require.NoError(t, err)
	require.NoError(t, replConn.Close(ctx))

	_, err = conn.Current.Exec(ctx, "INSERT INTO dg_replay_items VALUES (1, 'one');")
	require.NoError(t, err)

	var beforeRestartLSN string
	require.NoError(t, conn.Current.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text;").Scan(&beforeRestartLSN))
	require.NotEqual(t, "0/0", beforeRestartLSN)

	conn.Close(ctx)
	controller.Stop()
	require.NoError(t, controller.WaitForStop())

	replsource.ResetForTests()
	ctx, conn, controller = CreateServerLocalInDirWithPort(t, "postgres", dbDir, port)
	defer func() {
		conn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	var active bool
	var confirmedFlush string
	require.NoError(t, conn.Current.QueryRow(ctx, `
		SELECT active, confirmed_flush_lsn::text
		FROM pg_catalog.pg_replication_slots
		WHERE slot_name = $1`, slotName).Scan(&active, &confirmedFlush))
	require.False(t, active)
	require.Equal(t, "0/0", confirmedFlush)

	replConn = connectReplicationConn(t, ctx, port)
	defer func() {
		if replConn != nil {
			replConn.Close(context.Background())
		}
	}()
	require.NoError(t, pglogrepl.StartReplication(ctx, replConn, slotName, 0, pglogrepl.StartReplicationOptions{
		Mode: pglogrepl.LogicalReplication,
		PluginArgs: []string{
			`"proto_version" '1'`,
			`"publication_names" 'dg_replay_pub'`,
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	relation, insert, commit := receiveInsertChange(t, replConn)
	requireInsertChange(t, relation, insert, "dg_replay_items", "1", "one")
	require.Equal(t, beforeRestartLSN, commit.CommitLSN.String())

	require.NoError(t, pglogrepl.SendStandbyStatusUpdate(ctx, replConn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: commit.CommitLSN,
		WALFlushPosition: commit.CommitLSN,
		WALApplyPosition: commit.CommitLSN,
		ReplyRequested:   true,
	}))
	reply := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), reply.Data[0])
	waitForReplicationState(t, ctx, conn, slotName, commit.CommitLSN.String())

	_, err = pglogrepl.SendStandbyCopyDone(ctx, replConn)
	require.NoError(t, err)
	require.NoError(t, replConn.Close(ctx))
	replConn = nil

	conn.Close(ctx)
	controller.Stop()
	require.NoError(t, controller.WaitForStop())

	replsource.ResetForTests()
	ctx, conn, controller = CreateServerLocalInDirWithPort(t, "postgres", dbDir, port)
	require.NoError(t, conn.Current.QueryRow(ctx, `
		SELECT active, confirmed_flush_lsn::text
		FROM pg_catalog.pg_replication_slots
		WHERE slot_name = $1`, slotName).Scan(&active, &confirmedFlush))
	require.False(t, active)
	require.Equal(t, commit.CommitLSN.String(), confirmedFlush)
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

func receiveInsertChange(t *testing.T, conn *pgconn.PgConn) (*pglogrepl.RelationMessageV2, *pglogrepl.InsertMessageV2, *pglogrepl.CommitMessage) {
	t.Helper()
	var relation *pglogrepl.RelationMessageV2
	var insert *pglogrepl.InsertMessageV2
	var commit *pglogrepl.CommitMessage
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		copyData := receiveReplicationCopyData(t, conn)
		if copyData.Data[0] != pglogrepl.XLogDataByteID {
			continue
		}
		xld, err := pglogrepl.ParseXLogData(copyData.Data[1:])
		require.NoError(t, err)
		msg, err := pglogrepl.ParseV2(xld.WALData, false)
		require.NoError(t, err)
		switch typed := msg.(type) {
		case *pglogrepl.RelationMessageV2:
			relation = typed
		case *pglogrepl.InsertMessageV2:
			insert = typed
		case *pglogrepl.CommitMessage:
			commit = typed
		}
		if relation != nil && insert != nil && commit != nil {
			return relation, insert, commit
		}
	}
	require.FailNow(t, "timed out waiting for relation, insert, and commit logical replication messages")
	return nil, nil, nil
}

func receiveUpdateChange(t *testing.T, conn *pgconn.PgConn) (*pglogrepl.RelationMessageV2, *pglogrepl.UpdateMessageV2, *pglogrepl.CommitMessage) {
	t.Helper()
	var relation *pglogrepl.RelationMessageV2
	var update *pglogrepl.UpdateMessageV2
	var commit *pglogrepl.CommitMessage
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		copyData := receiveReplicationCopyData(t, conn)
		if copyData.Data[0] != pglogrepl.XLogDataByteID {
			continue
		}
		xld, err := pglogrepl.ParseXLogData(copyData.Data[1:])
		require.NoError(t, err)
		msg, err := pglogrepl.ParseV2(xld.WALData, false)
		require.NoError(t, err)
		switch typed := msg.(type) {
		case *pglogrepl.RelationMessageV2:
			relation = typed
		case *pglogrepl.UpdateMessageV2:
			update = typed
		case *pglogrepl.CommitMessage:
			commit = typed
		}
		if relation != nil && update != nil && commit != nil {
			return relation, update, commit
		}
	}
	require.FailNow(t, "timed out waiting for relation, update, and commit logical replication messages")
	return nil, nil, nil
}

func receiveDeleteChange(t *testing.T, conn *pgconn.PgConn) (*pglogrepl.RelationMessageV2, *pglogrepl.DeleteMessageV2, *pglogrepl.CommitMessage) {
	t.Helper()
	var relation *pglogrepl.RelationMessageV2
	var deleteMessage *pglogrepl.DeleteMessageV2
	var commit *pglogrepl.CommitMessage
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		copyData := receiveReplicationCopyData(t, conn)
		if copyData.Data[0] != pglogrepl.XLogDataByteID {
			continue
		}
		xld, err := pglogrepl.ParseXLogData(copyData.Data[1:])
		require.NoError(t, err)
		msg, err := pglogrepl.ParseV2(xld.WALData, false)
		require.NoError(t, err)
		switch typed := msg.(type) {
		case *pglogrepl.RelationMessageV2:
			relation = typed
		case *pglogrepl.DeleteMessageV2:
			deleteMessage = typed
		case *pglogrepl.CommitMessage:
			commit = typed
		}
		if relation != nil && deleteMessage != nil && commit != nil {
			return relation, deleteMessage, commit
		}
	}
	require.FailNow(t, "timed out waiting for relation, delete, and commit logical replication messages")
	return nil, nil, nil
}

func requireInsertChange(t *testing.T, relation *pglogrepl.RelationMessageV2, insert *pglogrepl.InsertMessageV2, table string, tenantID string, label string) {
	t.Helper()
	require.Equal(t, "public", relation.Namespace)
	require.Equal(t, table, relation.RelationName)
	require.Equal(t, uint16(2), relation.ColumnNum)
	require.Equal(t, "tenant_id", relation.Columns[0].Name)
	require.Equal(t, uint8(1), relation.Columns[0].Flags)
	require.Equal(t, "label", relation.Columns[1].Name)
	require.Equal(t, relation.RelationID, insert.RelationID)
	require.Len(t, insert.Tuple.Columns, 2)
	require.Equal(t, tenantID, string(insert.Tuple.Columns[0].Data))
	require.Equal(t, label, string(insert.Tuple.Columns[1].Data))
}

func requireUpdateChange(t *testing.T, relation *pglogrepl.RelationMessageV2, update *pglogrepl.UpdateMessageV2, table string, tenantID string, label string) {
	t.Helper()
	require.Equal(t, "public", relation.Namespace)
	require.Equal(t, table, relation.RelationName)
	require.Equal(t, relation.RelationID, update.RelationID)
	require.NotNil(t, update.NewTuple)
	require.Len(t, update.NewTuple.Columns, 2)
	require.Equal(t, tenantID, string(update.NewTuple.Columns[0].Data))
	require.Equal(t, label, string(update.NewTuple.Columns[1].Data))
}

func requireDeleteChange(t *testing.T, relation *pglogrepl.RelationMessageV2, deleteMessage *pglogrepl.DeleteMessageV2, table string, tenantID string, label string) {
	t.Helper()
	require.Equal(t, "public", relation.Namespace)
	require.Equal(t, table, relation.RelationName)
	require.Equal(t, relation.RelationID, deleteMessage.RelationID)
	require.Equal(t, uint8(pglogrepl.DeleteMessageTupleTypeOld), deleteMessage.OldTupleType)
	require.NotNil(t, deleteMessage.OldTuple)
	require.Len(t, deleteMessage.OldTuple.Columns, 2)
	require.Equal(t, tenantID, string(deleteMessage.OldTuple.Columns[0].Data))
	require.Equal(t, label, string(deleteMessage.OldTuple.Columns[1].Data))
}

func waitForReplicationState(t *testing.T, ctx context.Context, conn *Connection, slotName string, expectedLSN string) {
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
		if lastErr == nil && state == "streaming" && sentLSN == expectedLSN && writeLSN == expectedLSN &&
			flushLSN == expectedLSN && replayLSN == expectedLSN && syncState == "async" && replyTimeSet && lag == "0" {
			var active bool
			var activePIDSet bool
			var confirmedFlush string
			require.NoError(t, conn.Current.QueryRow(ctx, `
				SELECT active, active_pid IS NOT NULL, confirmed_flush_lsn::text
				FROM pg_catalog.pg_replication_slots
				WHERE slot_name = $1`, slotName).Scan(&active, &activePIDSet, &confirmedFlush))
			require.True(t, active)
			require.True(t, activePIDSet)
			require.Equal(t, expectedLSN, confirmedFlush)
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
