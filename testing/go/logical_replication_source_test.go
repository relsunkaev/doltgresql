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
	"github.com/dolthub/doltgresql/server/sessionstate"
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

	var plugin, slotType, restartLSN, confirmedFlush string
	var active bool
	require.NoError(t, conn.Current.QueryRow(ctx, `
		SELECT plugin, slot_type, active, restart_lsn::text, confirmed_flush_lsn::text
		FROM pg_catalog.pg_replication_slots
		WHERE slot_name = $1`, slotName).Scan(&plugin, &slotType, &active, &restartLSN, &confirmedFlush))
	require.Equal(t, "pgoutput", plugin)
	require.Equal(t, "logical", slotType)
	require.False(t, active)
	require.Equal(t, "0/0", restartLSN)
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

	var totalBytes int64
	require.NoError(t, conn.Current.QueryRow(ctx, `
		SELECT total_txns, total_bytes
		FROM pg_catalog.pg_stat_replication_slots
		WHERE slot_name = $1`, slotName).Scan(&totalTxns, &totalBytes))
	require.Equal(t, int64(7), totalTxns)
	require.Greater(t, totalBytes, int64(0))

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

func TestLogicalReplicationSourceUpdateIncludesOldTupleForReplicaIdentityFull(t *testing.T) {
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	slotName := "dg_full_update_slot"
	_, err = conn.Current.Exec(ctx, "CREATE TABLE dg_full_update_items (id INT PRIMARY KEY, label TEXT);")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "INSERT INTO dg_full_update_items VALUES (1, 'old-label');")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "CREATE PUBLICATION dg_full_update_pub FOR TABLE dg_full_update_items;")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "ALTER TABLE dg_full_update_items REPLICA IDENTITY FULL;")
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
			`"publication_names" 'dg_full_update_pub'`,
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, "UPDATE dg_full_update_items SET label = 'new-label' WHERE id = 1;")
	require.NoError(t, err)
	relation, update, _ := receiveUpdateChange(t, replConn)

	require.Equal(t, "public", relation.Namespace)
	require.Equal(t, "dg_full_update_items", relation.RelationName)
	require.Equal(t, relation.RelationID, update.RelationID)
	require.Equal(t, uint8(pglogrepl.UpdateMessageTupleTypeOld), update.OldTupleType)
	require.NotNil(t, update.OldTuple)
	require.Len(t, update.OldTuple.Columns, 2)
	require.Equal(t, "1", string(update.OldTuple.Columns[0].Data))
	require.Equal(t, "old-label", string(update.OldTuple.Columns[1].Data))
	require.NotNil(t, update.NewTuple)
	require.Len(t, update.NewTuple.Columns, 2)
	require.Equal(t, "1", string(update.NewTuple.Columns[0].Data))
	require.Equal(t, "new-label", string(update.NewTuple.Columns[1].Data))
}

func TestLogicalReplicationSourceCreateExistingInactiveSlotReturnsExisting(t *testing.T) {
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	slotName := "dg_existing_inactive_slot"
	replConn := connectReplicationConn(t, ctx, port)
	first, err := pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication,
	})
	require.NoError(t, err)
	require.NoError(t, replConn.Close(ctx))

	replConn = connectReplicationConn(t, ctx, port)
	defer replConn.Close(context.Background())
	second, err := pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication,
	})
	require.NoError(t, err)
	require.Equal(t, first.SlotName, second.SlotName)
	require.Equal(t, first.ConsistentPoint, second.ConsistentPoint)
	require.Equal(t, first.OutputPlugin, second.OutputPlugin)

	var slotCount int
	require.NoError(t, conn.Current.QueryRow(ctx, `
		SELECT count(*) FROM pg_catalog.pg_replication_slots WHERE slot_name = $1`, slotName).Scan(&slotCount))
	require.Equal(t, 1, slotCount)

	_, err = conn.Current.Exec(ctx, "SELECT pg_drop_replication_slot($1);", slotName)
	require.NoError(t, err)
	require.NoError(t, conn.Current.QueryRow(ctx, `
		SELECT count(*) FROM pg_catalog.pg_replication_slots WHERE slot_name = $1`, slotName).Scan(&slotCount))
	require.Equal(t, 0, slotCount)
}

func TestLogicalReplicationSourceTerminateBackendDeactivatesSlot(t *testing.T) {
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	slotName := "dg_terminate_backend_slot"
	_, err = conn.Current.Exec(ctx, "CREATE TABLE dg_terminate_items (id INT PRIMARY KEY);")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "CREATE PUBLICATION dg_terminate_pub FOR TABLE dg_terminate_items;")
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
			`"publication_names" 'dg_terminate_pub'`,
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

	dropConn := connectReplicationConn(t, ctx, port)
	err = pglogrepl.DropReplicationSlot(ctx, dropConn, slotName, pglogrepl.DropReplicationSlotOptions{})
	require.ErrorContains(t, err, `replication slot "dg_terminate_backend_slot" is active`)
	require.NoError(t, dropConn.Close(ctx))

	var terminated bool
	require.NoError(t, conn.Current.QueryRow(ctx, "SELECT pg_terminate_backend($1::int4);", activePID).Scan(&terminated))
	require.True(t, terminated)
	waitForInactiveSlot(t, ctx, conn, slotName)

	var missing bool
	require.NoError(t, conn.Current.QueryRow(ctx, "SELECT pg_terminate_backend(2147483647::int4);").Scan(&missing))
	require.False(t, missing)
}

func TestLogicalReplicationSourceRejectsUnsupportedSlotModesAndPlugins(t *testing.T) {
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	replConn := connectReplicationConn(t, ctx, port)
	defer replConn.Close(context.Background())

	_, err = pglogrepl.CreateReplicationSlot(ctx, replConn, "dg_physical_slot", "", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.PhysicalReplication,
	})
	require.ErrorContains(t, err, "only logical replication slots are supported")

	var slotCount int
	require.NoError(t, conn.Current.QueryRow(ctx, "SELECT count(*) FROM pg_catalog.pg_replication_slots;").Scan(&slotCount))
	require.Equal(t, 0, slotCount)

	_, err = pglogrepl.CreateReplicationSlot(ctx, replConn, "dg_test_decoding_slot", "test_decoding", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication,
	})
	require.ErrorContains(t, err, `logical decoding output plugin "test_decoding" is not supported`)

	require.NoError(t, conn.Current.QueryRow(ctx, "SELECT count(*) FROM pg_catalog.pg_replication_slots;").Scan(&slotCount))
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

func TestLogicalReplicationSourceHonorsPublicationRowFilterAndColumnList(t *testing.T) {
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	slotName := "dg_publication_filter_slot"
	_, err = conn.Current.Exec(ctx, `
		CREATE TABLE dg_customer_items (
			item_id BIGINT PRIMARY KEY,
			customer_id BIGINT NOT NULL,
			label TEXT,
			internal_note TEXT
		);`)
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, `
		CREATE PUBLICATION dg_customer_pub
		FOR TABLE dg_customer_items (customer_id, label)
		WHERE (customer_id = 42);`)
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
			`"publication_names" 'dg_customer_pub'`,
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, "INSERT INTO dg_customer_items VALUES (1, 7, 'wrong-customer', 'hidden-7');")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "INSERT INTO dg_customer_items VALUES (2, 42, 'right-customer', 'hidden-42');")
	require.NoError(t, err)

	relation, insert, _ := receiveInsertChange(t, replConn)
	require.Equal(t, "public", relation.Namespace)
	require.Equal(t, "dg_customer_items", relation.RelationName)
	require.Equal(t, uint16(2), relation.ColumnNum)
	require.Equal(t, "customer_id", relation.Columns[0].Name)
	require.Equal(t, "label", relation.Columns[1].Name)
	require.Equal(t, relation.RelationID, insert.RelationID)
	require.Len(t, insert.Tuple.Columns, 2)
	require.Equal(t, "42", string(insert.Tuple.Columns[0].Data))
	require.Equal(t, "right-customer", string(insert.Tuple.Columns[1].Data))
}

func TestLogicalReplicationSourceHonorsPublicationUpdateDeleteFiltersAndColumnLists(t *testing.T) {
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	slotName := "dg_publication_update_delete_filter_slot"
	_, err = conn.Current.Exec(ctx, `
		CREATE TABLE dg_customer_update_delete_items (
			item_id BIGINT PRIMARY KEY,
			customer_id BIGINT NOT NULL,
			label TEXT,
			internal_note TEXT
		);`)
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, `
		INSERT INTO dg_customer_update_delete_items VALUES
			(1, 7, 'wrong-customer', 'hidden-7'),
			(2, 42, 'right-customer', 'hidden-42');`)
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, `
		CREATE PUBLICATION dg_customer_update_delete_pub
		FOR TABLE dg_customer_update_delete_items (customer_id, label)
		WHERE (customer_id = 42)
		WITH (publish = 'update, delete');`)
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
			`"publication_names" 'dg_customer_update_delete_pub'`,
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, "UPDATE dg_customer_update_delete_items SET label = 'wrong-updated' WHERE customer_id = 7;")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "UPDATE dg_customer_update_delete_items SET label = 'right-updated' WHERE customer_id = 42;")
	require.NoError(t, err)
	relation, update, _ := receiveUpdateChange(t, replConn)
	require.Equal(t, "dg_customer_update_delete_items", relation.RelationName)
	require.Equal(t, uint16(2), relation.ColumnNum)
	require.Equal(t, "customer_id", relation.Columns[0].Name)
	require.Equal(t, "label", relation.Columns[1].Name)
	require.Len(t, update.NewTuple.Columns, 2)
	require.Equal(t, "42", string(update.NewTuple.Columns[0].Data))
	require.Equal(t, "right-updated", string(update.NewTuple.Columns[1].Data))

	_, err = conn.Current.Exec(ctx, "DELETE FROM dg_customer_update_delete_items WHERE customer_id = 7;")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "DELETE FROM dg_customer_update_delete_items WHERE customer_id = 42;")
	require.NoError(t, err)
	relation, deleteMessage, _ := receiveDeleteChange(t, replConn)
	require.Equal(t, "dg_customer_update_delete_items", relation.RelationName)
	require.Equal(t, uint16(2), relation.ColumnNum)
	require.Equal(t, "customer_id", relation.Columns[0].Name)
	require.Equal(t, "label", relation.Columns[1].Name)
	require.NotNil(t, deleteMessage.OldTuple)
	require.Len(t, deleteMessage.OldTuple.Columns, 2)
	require.Equal(t, "42", string(deleteMessage.OldTuple.Columns[0].Data))
	require.Equal(t, "right-updated", string(deleteMessage.OldTuple.Columns[1].Data))
}

func TestLogicalReplicationSourceHonorsPublicationActionFlags(t *testing.T) {
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	slotName := "dg_publication_action_slot"
	_, err = conn.Current.Exec(ctx, "CREATE TABLE dg_publication_action_items (tenant_id BIGINT PRIMARY KEY, label TEXT);")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "CREATE PUBLICATION dg_publication_action_pub FOR TABLE dg_publication_action_items WITH (publish = 'insert');")
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
			`"publication_names" 'dg_publication_action_pub'`,
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, "INSERT INTO dg_publication_action_items VALUES (1, 'one');")
	require.NoError(t, err)
	relation, insert, _ := receiveInsertChange(t, replConn)
	requireInsertChange(t, relation, insert, "dg_publication_action_items", "1", "one")

	_, err = conn.Current.Exec(ctx, "UPDATE dg_publication_action_items SET label = 'one-updated' WHERE tenant_id = 1;")
	require.NoError(t, err)
	requireNoReplicationCopyData(t, replConn, 250*time.Millisecond)
	_, err = conn.Current.Exec(ctx, "DELETE FROM dg_publication_action_items WHERE tenant_id = 1;")
	require.NoError(t, err)
	requireNoReplicationCopyData(t, replConn, 250*time.Millisecond)

	_, err = conn.Current.Exec(ctx, "TRUNCATE dg_publication_action_items;")
	require.NoError(t, err)
	requireNoReplicationCopyData(t, replConn, 250*time.Millisecond)
}

func TestLogicalReplicationSourcePublishesTruncate(t *testing.T) {
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	slotName := "dg_truncate_slot"
	_, err = conn.Current.Exec(ctx, "CREATE TABLE dg_truncate_items (tenant_id BIGINT PRIMARY KEY, label TEXT);")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "INSERT INTO dg_truncate_items VALUES (1, 'one'), (2, 'two');")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "CREATE PUBLICATION dg_truncate_pub FOR TABLE dg_truncate_items WITH (publish = 'truncate');")
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
			`"publication_names" 'dg_truncate_pub'`,
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, "TRUNCATE dg_truncate_items;")
	require.NoError(t, err)
	relation, truncate, commit := receiveTruncateChange(t, replConn)
	require.Equal(t, "public", relation.Namespace)
	require.Equal(t, "dg_truncate_items", relation.RelationName)
	require.Equal(t, uint32(1), truncate.RelationNum)
	require.Equal(t, uint8(0), truncate.Option)
	require.Equal(t, []uint32{relation.RelationID}, truncate.RelationIDs)
	require.Greater(t, commit.CommitLSN, pglogrepl.LSN(0))

	var rowCount int
	require.NoError(t, conn.Current.QueryRow(ctx, "SELECT count(*) FROM dg_truncate_items;").Scan(&rowCount))
	require.Equal(t, 0, rowCount)
}

func TestLogicalReplicationSourcePublishesAllTablesAndSchemaPublications(t *testing.T) {
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	_, err = conn.Current.Exec(ctx, "CREATE TABLE dg_all_pub_items (tenant_id BIGINT PRIMARY KEY, label TEXT);")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "CREATE PUBLICATION dg_all_pub FOR ALL TABLES;")
	require.NoError(t, err)

	allConn := connectReplicationConn(t, ctx, port)
	defer allConn.Close(context.Background())
	_, err = pglogrepl.CreateReplicationSlot(ctx, allConn, "dg_all_pub_slot", "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication,
	})
	require.NoError(t, err)
	require.NoError(t, pglogrepl.StartReplication(ctx, allConn, "dg_all_pub_slot", 0, pglogrepl.StartReplicationOptions{
		Mode: pglogrepl.LogicalReplication,
		PluginArgs: []string{
			`"proto_version" '1'`,
			`"publication_names" 'dg_all_pub'`,
		},
	}))
	keepalive := receiveReplicationCopyData(t, allConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])
	_, err = conn.Current.Exec(ctx, "INSERT INTO dg_all_pub_items VALUES (1, 'all-tables');")
	require.NoError(t, err)
	relation, insert, _ := receiveInsertChange(t, allConn)
	requireInsertChange(t, relation, insert, "dg_all_pub_items", "1", "all-tables")

	_, err = conn.Current.Exec(ctx, "CREATE SCHEMA dg_schema_pub;")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "CREATE TABLE dg_schema_pub.dg_schema_pub_items (tenant_id BIGINT PRIMARY KEY, label TEXT);")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "CREATE TABLE dg_schema_pub_public_items (tenant_id BIGINT PRIMARY KEY, label TEXT);")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "CREATE PUBLICATION dg_schema_only_pub FOR TABLES IN SCHEMA dg_schema_pub;")
	require.NoError(t, err)

	schemaConn := connectReplicationConn(t, ctx, port)
	defer schemaConn.Close(context.Background())
	_, err = pglogrepl.CreateReplicationSlot(ctx, schemaConn, "dg_schema_pub_slot", "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication,
	})
	require.NoError(t, err)
	require.NoError(t, pglogrepl.StartReplication(ctx, schemaConn, "dg_schema_pub_slot", 0, pglogrepl.StartReplicationOptions{
		Mode: pglogrepl.LogicalReplication,
		PluginArgs: []string{
			`"proto_version" '1'`,
			`"publication_names" 'dg_schema_only_pub'`,
		},
	}))
	keepalive = receiveReplicationCopyData(t, schemaConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])
	_, err = conn.Current.Exec(ctx, "INSERT INTO dg_schema_pub_public_items VALUES (1, 'public');")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "INSERT INTO dg_schema_pub.dg_schema_pub_items VALUES (2, 'schema');")
	require.NoError(t, err)
	relation, insert, _ = receiveInsertChange(t, schemaConn)
	requireInsertChangeInSchema(t, relation, insert, "dg_schema_pub", "dg_schema_pub_items", "2", "schema")
}

func TestLogicalReplicationSourcePublishesExplicitTransactionAsOnePgoutputTransaction(t *testing.T) {
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	slotName := "dg_transaction_source_slot"
	_, err = conn.Current.Exec(ctx, "CREATE TABLE dg_tx_items (tenant_id BIGINT PRIMARY KEY, label TEXT);")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "CREATE PUBLICATION dg_tx_pub FOR TABLE dg_tx_items;")
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
			`"publication_names" 'dg_tx_pub'`,
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, "BEGIN;")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "INSERT INTO dg_tx_items VALUES (50, 'fifty');")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "INSERT INTO dg_tx_items VALUES (51, 'fifty-one');")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "COMMIT;")
	require.NoError(t, err)

	txn := receiveLogicalTransaction(t, replConn)
	require.Len(t, txn.inserts, 2)
	require.Equal(t, "50", string(txn.inserts[0].Tuple.Columns[0].Data))
	require.Equal(t, "fifty", string(txn.inserts[0].Tuple.Columns[1].Data))
	require.Equal(t, "51", string(txn.inserts[1].Tuple.Columns[0].Data))
	require.Equal(t, "fifty-one", string(txn.inserts[1].Tuple.Columns[1].Data))
}

func TestLogicalReplicationSourceToleratesStreamingOptionWithoutStreamMessages(t *testing.T) {
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	slotName := "dg_streaming_option_slot"
	_, err = conn.Current.Exec(ctx, "CREATE TABLE dg_streaming_option_items (tenant_id BIGINT PRIMARY KEY, label TEXT);")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "CREATE PUBLICATION dg_streaming_option_pub FOR TABLE dg_streaming_option_items;")
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
			`"proto_version" '2'`,
			`"publication_names" 'dg_streaming_option_pub'`,
			`"streaming" 'true'`,
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, "BEGIN;")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "INSERT INTO dg_streaming_option_items VALUES (1, 'one');")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "INSERT INTO dg_streaming_option_items VALUES (2, 'two');")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "COMMIT;")
	require.NoError(t, err)

	txn := receiveLogicalTransaction(t, replConn)
	require.Len(t, txn.inserts, 2)
	require.False(t, txn.streamMessageSeen)
	require.Equal(t, "1", string(txn.inserts[0].Tuple.Columns[0].Data))
	require.Equal(t, "one", string(txn.inserts[0].Tuple.Columns[1].Data))
	require.Equal(t, "2", string(txn.inserts[1].Tuple.Columns[0].Data))
	require.Equal(t, "two", string(txn.inserts[1].Tuple.Columns[1].Data))
}

func TestLogicalReplicationSourcePublishesPreparedStatementDMLInExplicitTransaction(t *testing.T) {
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	slotName := "dg_prepared_stmt_tx_slot"
	_, err = conn.Current.Exec(ctx, "CREATE TABLE dg_prepared_stmt_tx_items (tenant_id BIGINT PRIMARY KEY, label TEXT);")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "CREATE PUBLICATION dg_prepared_stmt_tx_pub FOR TABLE dg_prepared_stmt_tx_items;")
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
			`"publication_names" 'dg_prepared_stmt_tx_pub'`,
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, "PREPARE dg_prepared_stmt_tx(bigint, text) AS INSERT INTO dg_prepared_stmt_tx_items VALUES ($1, $2);")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "BEGIN;")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "EXECUTE dg_prepared_stmt_tx(60, 'sixty');")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "INSERT INTO dg_prepared_stmt_tx_items VALUES ($1, $2);", int64(61), "sixty-one")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "COMMIT;")
	require.NoError(t, err)

	txn := receiveLogicalTransaction(t, replConn)
	require.Len(t, txn.inserts, 2)
	require.Equal(t, "60", string(txn.inserts[0].Tuple.Columns[0].Data))
	require.Equal(t, "sixty", string(txn.inserts[0].Tuple.Columns[1].Data))
	require.Equal(t, "61", string(txn.inserts[1].Tuple.Columns[0].Data))
	require.Equal(t, "sixty-one", string(txn.inserts[1].Tuple.Columns[1].Data))
}

func TestLogicalReplicationSourcePublishesCommitPreparedAsOnePgoutputTransaction(t *testing.T) {
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	slotName := "dg_commit_prepared_slot"
	_, err = conn.Current.Exec(ctx, "CREATE TABLE dg_commit_prepared_items (tenant_id BIGINT PRIMARY KEY, label TEXT);")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "CREATE PUBLICATION dg_commit_prepared_pub FOR TABLE dg_commit_prepared_items;")
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
			`"publication_names" 'dg_commit_prepared_pub'`,
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, "BEGIN;")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "INSERT INTO dg_commit_prepared_items VALUES (70, 'seventy');")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "INSERT INTO dg_commit_prepared_items VALUES (71, 'seventy-one');")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "PREPARE TRANSACTION 'dg_commit_prepared_tx';")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "COMMIT PREPARED 'dg_commit_prepared_tx';")
	require.NoError(t, err)

	txn := receiveLogicalTransaction(t, replConn)
	require.Len(t, txn.inserts, 2)
	require.Equal(t, "70", string(txn.inserts[0].Tuple.Columns[0].Data))
	require.Equal(t, "seventy", string(txn.inserts[0].Tuple.Columns[1].Data))
	require.Equal(t, "71", string(txn.inserts[1].Tuple.Columns[0].Data))
	require.Equal(t, "seventy-one", string(txn.inserts[1].Tuple.Columns[1].Data))
}

func TestLogicalReplicationSourcePublishesRecoveredCommitPrepared(t *testing.T) {
	replsource.ResetForTests()
	dbDir, err := os.MkdirTemp(os.TempDir(), t.Name())
	require.NoError(t, err)

	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerLocalInDirWithPort(t, "postgres", dbDir, port)
	slotName := "dg_recovered_commit_prepared_slot"
	_, err = conn.Current.Exec(ctx, "CREATE TABLE dg_recovered_commit_prepared_items (tenant_id BIGINT PRIMARY KEY, label TEXT);")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "CREATE PUBLICATION dg_recovered_commit_prepared_pub FOR TABLE dg_recovered_commit_prepared_items;")
	require.NoError(t, err)

	replConn := connectReplicationConn(t, ctx, port)
	_, err = pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication,
	})
	require.NoError(t, err)
	require.NoError(t, replConn.Close(ctx))

	_, err = conn.Current.Exec(ctx, "BEGIN;")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "INSERT INTO dg_recovered_commit_prepared_items VALUES (80, 'eighty');")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "INSERT INTO dg_recovered_commit_prepared_items VALUES (81, 'eighty-one');")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "PREPARE TRANSACTION 'dg_recovered_commit_prepared_tx';")
	require.NoError(t, err)

	conn.Close(ctx)
	controller.Stop()
	require.NoError(t, controller.WaitForStop())

	replsource.ResetForTests()
	sessionstate.ResetPreparedTransactionsForTests()
	ctx, conn, controller = CreateServerLocalInDirWithPort(t, "postgres", dbDir, port)
	defer func() {
		conn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	replConn = connectReplicationConn(t, ctx, port)
	defer replConn.Close(context.Background())
	require.NoError(t, pglogrepl.StartReplication(ctx, replConn, slotName, 0, pglogrepl.StartReplicationOptions{
		Mode: pglogrepl.LogicalReplication,
		PluginArgs: []string{
			`"proto_version" '1'`,
			`"publication_names" 'dg_recovered_commit_prepared_pub'`,
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, "COMMIT PREPARED 'dg_recovered_commit_prepared_tx';")
	require.NoError(t, err)

	txn := receiveLogicalTransaction(t, replConn)
	require.Len(t, txn.inserts, 2)
	require.Equal(t, "80", string(txn.inserts[0].Tuple.Columns[0].Data))
	require.Equal(t, "eighty", string(txn.inserts[0].Tuple.Columns[1].Data))
	require.Equal(t, "81", string(txn.inserts[1].Tuple.Columns[0].Data))
	require.Equal(t, "eighty-one", string(txn.inserts[1].Tuple.Columns[1].Data))
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
	var restartLSN, confirmedFlush string
	require.NoError(t, conn.Current.QueryRow(ctx, `
		SELECT active, restart_lsn::text, confirmed_flush_lsn::text
		FROM pg_catalog.pg_replication_slots
		WHERE slot_name = $1`, slotName).Scan(&active, &restartLSN, &confirmedFlush))
	require.False(t, active)
	require.Equal(t, "0/0", restartLSN)
	require.Equal(t, "0/0", confirmedFlush)
	var totalTxns, totalBytes int64
	require.NoError(t, conn.Current.QueryRow(ctx, `
		SELECT total_txns, total_bytes
		FROM pg_catalog.pg_stat_replication_slots
		WHERE slot_name = $1`, slotName).Scan(&totalTxns, &totalBytes))
	require.Equal(t, int64(0), totalTxns)
	require.Equal(t, int64(0), totalBytes)

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
	require.NoError(t, conn.Current.QueryRow(ctx, `
		SELECT total_txns, total_bytes
		FROM pg_catalog.pg_stat_replication_slots
		WHERE slot_name = $1`, slotName).Scan(&totalTxns, &totalBytes))
	require.Equal(t, int64(1), totalTxns)
	require.Greater(t, totalBytes, int64(0))

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
		SELECT active, restart_lsn::text, confirmed_flush_lsn::text
		FROM pg_catalog.pg_replication_slots
		WHERE slot_name = $1`, slotName).Scan(&active, &restartLSN, &confirmedFlush))
	require.False(t, active)
	require.Equal(t, commit.CommitLSN.String(), restartLSN)
	require.Equal(t, commit.CommitLSN.String(), confirmedFlush)
	require.NoError(t, conn.Current.QueryRow(ctx, `
		SELECT total_txns, total_bytes
		FROM pg_catalog.pg_stat_replication_slots
		WHERE slot_name = $1`, slotName).Scan(&totalTxns, &totalBytes))
	require.Equal(t, int64(1), totalTxns)
	require.Greater(t, totalBytes, int64(0))
}

func TestLogicalReplicationSourceReplaysUpdateAndDeleteAfterRestart(t *testing.T) {
	replsource.ResetForTests()
	dbDir, err := os.MkdirTemp(os.TempDir(), t.Name())
	require.NoError(t, err)

	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerLocalInDirWithPort(t, "postgres", dbDir, port)
	slotName := "dg_replay_update_delete_slot"
	_, err = conn.Current.Exec(ctx, "CREATE TABLE dg_replay_ud_items (tenant_id BIGINT PRIMARY KEY, label TEXT);")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "INSERT INTO dg_replay_ud_items VALUES (1, 'one'), (2, 'two');")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "CREATE PUBLICATION dg_replay_ud_pub FOR TABLE dg_replay_ud_items;")
	require.NoError(t, err)

	replConn := connectReplicationConn(t, ctx, port)
	_, err = pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication,
	})
	require.NoError(t, err)
	require.NoError(t, replConn.Close(ctx))

	_, err = conn.Current.Exec(ctx, "UPDATE dg_replay_ud_items SET label = 'one-updated' WHERE tenant_id = 1;")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "DELETE FROM dg_replay_ud_items WHERE tenant_id = 2;")
	require.NoError(t, err)

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

	replConn = connectReplicationConn(t, ctx, port)
	defer replConn.Close(context.Background())
	require.NoError(t, pglogrepl.StartReplication(ctx, replConn, slotName, 0, pglogrepl.StartReplicationOptions{
		Mode: pglogrepl.LogicalReplication,
		PluginArgs: []string{
			`"proto_version" '1'`,
			`"publication_names" 'dg_replay_ud_pub'`,
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	relation, update, updateCommit := receiveUpdateChange(t, replConn)
	requireUpdateChange(t, relation, update, "dg_replay_ud_items", "1", "one-updated")
	relation, deleteMessage, deleteCommit := receiveDeleteChange(t, replConn)
	requireDeleteChange(t, relation, deleteMessage, "dg_replay_ud_items", "2", "two")
	require.Greater(t, deleteCommit.CommitLSN, updateCommit.CommitLSN)
}

func TestLogicalReplicationSourceDoesNotReplayAcknowledgedBacklogAfterRestart(t *testing.T) {
	replsource.ResetForTests()
	dbDir, err := os.MkdirTemp(os.TempDir(), t.Name())
	require.NoError(t, err)

	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerLocalInDirWithPort(t, "postgres", dbDir, port)
	slotName := "dg_ack_prune_slot"
	_, err = conn.Current.Exec(ctx, "CREATE TABLE dg_ack_prune_items (tenant_id BIGINT PRIMARY KEY, label TEXT);")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "CREATE PUBLICATION dg_ack_prune_pub FOR TABLE dg_ack_prune_items;")
	require.NoError(t, err)

	replConn := connectReplicationConn(t, ctx, port)
	_, err = pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication,
	})
	require.NoError(t, err)
	require.NoError(t, pglogrepl.StartReplication(ctx, replConn, slotName, 0, pglogrepl.StartReplicationOptions{
		Mode: pglogrepl.LogicalReplication,
		PluginArgs: []string{
			`"proto_version" '1'`,
			`"publication_names" 'dg_ack_prune_pub'`,
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, "INSERT INTO dg_ack_prune_items VALUES (1, 'acked');")
	require.NoError(t, err)
	relation, insert, commit := receiveInsertChange(t, replConn)
	requireInsertChange(t, relation, insert, "dg_ack_prune_items", "1", "acked")
	require.NoError(t, pglogrepl.SendStandbyStatusUpdate(ctx, replConn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: commit.CommitLSN,
		WALFlushPosition: commit.CommitLSN,
		WALApplyPosition: commit.CommitLSN,
		ReplyRequested:   true,
	}))
	reply := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), reply.Data[0])
	_, err = pglogrepl.SendStandbyCopyDone(ctx, replConn)
	require.NoError(t, err)
	require.NoError(t, replConn.Close(ctx))

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

	replConn = connectReplicationConn(t, ctx, port)
	defer replConn.Close(context.Background())
	require.NoError(t, pglogrepl.StartReplication(ctx, replConn, slotName, 0, pglogrepl.StartReplicationOptions{
		Mode: pglogrepl.LogicalReplication,
		PluginArgs: []string{
			`"proto_version" '1'`,
			`"publication_names" 'dg_ack_prune_pub'`,
		},
	}))
	keepalive = receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, "INSERT INTO dg_ack_prune_items VALUES (2, 'after-restart');")
	require.NoError(t, err)
	relation, insert, _ = receiveInsertChange(t, replConn)
	requireInsertChange(t, relation, insert, "dg_ack_prune_items", "2", "after-restart")
}

func TestLogicalReplicationSourceDropSlotRemovesDurableStateAfterRestart(t *testing.T) {
	replsource.ResetForTests()
	dbDir, err := os.MkdirTemp(os.TempDir(), t.Name())
	require.NoError(t, err)

	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerLocalInDirWithPort(t, "postgres", dbDir, port)
	slotName := "dg_drop_restart_slot"
	_, err = conn.Current.Exec(ctx, "CREATE TABLE dg_drop_restart_items (tenant_id BIGINT PRIMARY KEY, label TEXT);")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "CREATE PUBLICATION dg_drop_restart_pub FOR TABLE dg_drop_restart_items;")
	require.NoError(t, err)

	replConn := connectReplicationConn(t, ctx, port)
	_, err = pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication,
	})
	require.NoError(t, err)
	require.NoError(t, replConn.Close(ctx))

	_, err = conn.Current.Exec(ctx, "INSERT INTO dg_drop_restart_items VALUES (1, 'queued');")
	require.NoError(t, err)

	dropConn := connectReplicationConn(t, ctx, port)
	require.NoError(t, pglogrepl.DropReplicationSlot(ctx, dropConn, slotName, pglogrepl.DropReplicationSlotOptions{}))
	require.NoError(t, dropConn.Close(ctx))

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

	var slotCount int64
	require.NoError(t, conn.Current.QueryRow(ctx, `
		SELECT count(*)
		FROM pg_catalog.pg_replication_slots
		WHERE slot_name = $1`, slotName).Scan(&slotCount))
	require.EqualValues(t, 0, slotCount)

	replConn = connectReplicationConn(t, ctx, port)
	defer replConn.Close(context.Background())
	err = pglogrepl.StartReplication(ctx, replConn, slotName, 0, pglogrepl.StartReplicationOptions{
		Mode: pglogrepl.LogicalReplication,
		PluginArgs: []string{
			`"proto_version" '1'`,
			`"publication_names" 'dg_drop_restart_pub'`,
		},
	})
	require.ErrorContains(t, err, `replication slot "dg_drop_restart_slot" does not exist`)
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

func requireNoReplicationCopyData(t *testing.T, conn *pgconn.PgConn, wait time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	msg, err := conn.ReceiveMessage(ctx)
	require.Error(t, err, "unexpected replication message: %T %[1]v", msg)
	require.Contains(t, err.Error(), "timeout")
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

func receiveTruncateChange(t *testing.T, conn *pgconn.PgConn) (*pglogrepl.RelationMessageV2, *pglogrepl.TruncateMessageV2, *pglogrepl.CommitMessage) {
	t.Helper()
	var relation *pglogrepl.RelationMessageV2
	var truncate *pglogrepl.TruncateMessageV2
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
		case *pglogrepl.TruncateMessageV2:
			truncate = typed
		case *pglogrepl.CommitMessage:
			commit = typed
		}
		if relation != nil && truncate != nil && commit != nil {
			return relation, truncate, commit
		}
	}
	require.FailNow(t, "timed out waiting for relation, truncate, and commit logical replication messages")
	return nil, nil, nil
}

type logicalTransaction struct {
	inserts           []*pglogrepl.InsertMessageV2
	streamMessageSeen bool
}

func receiveLogicalTransaction(t *testing.T, conn *pgconn.PgConn) logicalTransaction {
	t.Helper()
	var txn logicalTransaction
	deadline := time.Now().Add(5 * time.Second)
	beginSeen := false
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
		case *pglogrepl.BeginMessage:
			require.False(t, beginSeen, "received a second Begin before Commit")
			beginSeen = true
		case *pglogrepl.InsertMessageV2:
			require.True(t, beginSeen, "received Insert before Begin")
			txn.inserts = append(txn.inserts, typed)
		case *pglogrepl.StreamStartMessageV2, *pglogrepl.StreamStopMessageV2, *pglogrepl.StreamCommitMessageV2, *pglogrepl.StreamAbortMessageV2:
			txn.streamMessageSeen = true
		case *pglogrepl.CommitMessage:
			require.True(t, beginSeen, "received Commit before Begin")
			return txn
		}
	}
	require.FailNow(t, "timed out waiting for logical replication transaction")
	return txn
}

func requireInsertChange(t *testing.T, relation *pglogrepl.RelationMessageV2, insert *pglogrepl.InsertMessageV2, table string, tenantID string, label string) {
	t.Helper()
	requireInsertChangeInSchema(t, relation, insert, "public", table, tenantID, label)
}

func requireInsertChangeInSchema(t *testing.T, relation *pglogrepl.RelationMessageV2, insert *pglogrepl.InsertMessageV2, schema string, table string, tenantID string, label string) {
	t.Helper()
	require.Equal(t, schema, relation.Namespace)
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
			var restartLSN, confirmedFlush string
			require.NoError(t, conn.Current.QueryRow(ctx, `
				SELECT active, active_pid IS NOT NULL, restart_lsn::text, confirmed_flush_lsn::text
				FROM pg_catalog.pg_replication_slots
				WHERE slot_name = $1`, slotName).Scan(&active, &activePIDSet, &restartLSN, &confirmedFlush))
			require.True(t, active)
			require.True(t, activePIDSet)
			require.Equal(t, expectedLSN, restartLSN)
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
