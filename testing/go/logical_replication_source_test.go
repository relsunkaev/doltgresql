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
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/doltgresql/server/replsource"
	"github.com/dolthub/doltgresql/server/sessionstate"
)

const electricPublicationValidationQuery = `
SELECT
  pg_get_userbyid(p.pubowner) = current_role as can_alter_publication,
  pubinsert AND pubupdate AND pubdelete AND pubtruncate as publishes_all_operations,
  CASE WHEN current_setting('server_version_num')::int >= 180000
      THEN (to_jsonb(p) ->> 'pubgencols') = 's'
      ELSE FALSE
  END AS publishes_generated_columns
FROM pg_publication as p WHERE pubname = $1;`

const electricPublicationRelationsQuery = `
SELECT
  pc.oid, (pn.nspname, pc.relname), pc.relreplident
FROM
  pg_publication_rel ppr
JOIN
  pg_publication pp ON ppr.prpubid = pp.oid
JOIN
  pg_class pc ON pc.oid = ppr.prrelid
JOIN
  pg_namespace pn ON pc.relnamespace = pn.oid
WHERE
  pp.pubname = $1
ORDER BY
  pn.nspname, pc.relname`

const electricReplicaIdentityByOIDQuery = `
SELECT
  pc.oid, pc.relreplident
FROM
  pg_class pc
WHERE
  pc.oid = ANY($1::oid[])`

const electricInputRelationDriftQuery = `
WITH input_relations AS (
  SELECT
    UNNEST($1::oid[]) AS oid,
    UNNEST($2::text[]) AS input_nspname,
    UNNEST($3::text[]) AS input_relname
)
SELECT
  ir.oid, (ir.input_nspname, ir.input_relname) as input_relation, pc.oid, (pn.nspname, pc.relname)
FROM input_relations ir
LEFT JOIN pg_class pc ON pc.oid = ir.oid
LEFT JOIN pg_namespace pn ON pc.relnamespace = pn.oid
WHERE pc.oid IS NULL OR (pc.relname != input_relname OR pn.nspname != input_nspname)`

const electricSnapshotLSNQuery = `SELECT pg_current_snapshot(), pg_current_wal_lsn()`

const electricReplicationTelemetryQuery = `
SELECT
  (pg_current_wal_lsn() - '0/0' + -9223372036854775808)::int8 AS pg_wal_offset,
  pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::int8 AS retained_wal_size,
  pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::int8 AS confirmed_flush_lsn_lag
FROM
  pg_replication_slots
WHERE
  slot_name = $1`

const debeziumSlotStateQuery = `select * from pg_replication_slots where slot_name = $1 and database = $2 and plugin = $3`

const debeziumCurrentXLogLocationQuery = `select (case pg_is_in_recovery() when 't' then pg_last_wal_receive_lsn() else pg_current_wal_lsn() end) AS pg_current_wal_lsn`

const debeziumServerInfoQuery = `SELECT version(), current_user, current_database()`

const debeziumRoleMembershipQuery = `SELECT oid, rolname, rolsuper, rolinherit, rolcreaterole, rolcreatedb, rolcanlogin, rolreplication FROM pg_roles WHERE pg_has_role('postgres', oid, 'member')`

const debeziumPublicationExistsQuery = `SELECT puballtables FROM pg_publication WHERE pubname = 'debezium_publication'`

const debeziumCurrentPublicationTablesQuery = `SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = 'debezium_publication'`

const debeziumPublicationValidationQuery = `SELECT schemaname, tablename FROM pg_catalog.pg_publication_tables WHERE pubname=$1`

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
	require.Equal(t, "doltgres-snapshot-"+slotName, slot.SnapshotName)

	noExportSlotName := "dg_logical_source_noexport_slot"
	noExportSlot, err := pglogrepl.CreateReplicationSlot(ctx, replConn, noExportSlotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode:           pglogrepl.LogicalReplication,
		SnapshotAction: "NOEXPORT_SNAPSHOT",
		Temporary:      true,
	})
	require.NoError(t, err)
	require.Equal(t, noExportSlotName, noExportSlot.SlotName)
	require.Empty(t, noExportSlot.SnapshotName)
	require.NoError(t, pglogrepl.DropReplicationSlot(ctx, replConn, noExportSlotName, pglogrepl.DropReplicationSlotOptions{}))

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

func TestPgLogicalEmitMessage(t *testing.T) {
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

	slotName := "dg_emit_message_slot"
	_, err = pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication,
	})
	require.NoError(t, err)
	require.NoError(t, pglogrepl.StartReplication(ctx, replConn, slotName, 0, pglogrepl.StartReplicationOptions{
		Mode: pglogrepl.LogicalReplication,
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	var emittedLSNText string
	require.NoError(t, conn.Current.QueryRow(ctx, `
		SELECT pg_logical_emit_message(false, 'zero/lag', '{"id":"abc"}')::text;`).Scan(&emittedLSNText))
	emittedLSN, err := pglogrepl.ParseLSN(emittedLSNText)
	require.NoError(t, err)
	require.Greater(t, emittedLSN, pglogrepl.LSN(0))
	emitted := receiveLogicalDecodingMessage(t, replConn)
	require.Equal(t, emittedLSN, emitted.LSN)
	require.False(t, emitted.Transactional)
	require.Equal(t, "zero/lag", emitted.Prefix)
	require.Equal(t, []byte(`{"id":"abc"}`), emitted.Content)

	var flushedLSNText string
	require.NoError(t, conn.Current.QueryRow(ctx, `
		SELECT pg_logical_emit_message(false, 'zero/lag', '{"id":"def"}', true)::text;`).Scan(&flushedLSNText))
	flushedLSN, err := pglogrepl.ParseLSN(flushedLSNText)
	require.NoError(t, err)
	require.Greater(t, flushedLSN, emittedLSN)
	flushed := receiveLogicalDecodingMessage(t, replConn)
	require.Equal(t, flushedLSN, flushed.LSN)
	require.Equal(t, []byte(`{"id":"def"}`), flushed.Content)
}

func TestPgLogicalEmitTransactionalMessageRollsBackRepro(t *testing.T) {
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

	slotName := "dg_emit_transactional_message_slot"
	_, err = pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication,
	})
	require.NoError(t, err)
	require.NoError(t, pglogrepl.StartReplication(ctx, replConn, slotName, 0, pglogrepl.StartReplicationOptions{
		Mode: pglogrepl.LogicalReplication,
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, "BEGIN;")
	require.NoError(t, err)
	var emittedLSNText string
	require.NoError(t, conn.Current.QueryRow(ctx, `
		SELECT pg_logical_emit_message(true, 'rolled/back', 'discard me')::text;`).Scan(&emittedLSNText))
	_, err = conn.Current.Exec(ctx, "ROLLBACK;")
	require.NoError(t, err)

	requireNoReplicationCopyData(t, replConn, 250*time.Millisecond)
}

func TestPgLogicalEmitTransactionalMessageWaitsForCommitRepro(t *testing.T) {
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

	slotName := "dg_emit_transactional_commit_slot"
	_, err = pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication,
	})
	require.NoError(t, err)
	require.NoError(t, pglogrepl.StartReplication(ctx, replConn, slotName, 0, pglogrepl.StartReplicationOptions{
		Mode: pglogrepl.LogicalReplication,
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, "BEGIN;")
	require.NoError(t, err)
	var emittedLSNText string
	require.NoError(t, conn.Current.QueryRow(ctx, `
		SELECT pg_logical_emit_message(true, 'commit/only', 'after commit')::text;`).Scan(&emittedLSNText))
	requireNoReplicationCopyData(t, replConn, 250*time.Millisecond)
	_, err = conn.Current.Exec(ctx, "COMMIT;")
	require.NoError(t, err)

	emittedLSN, err := pglogrepl.ParseLSN(emittedLSNText)
	require.NoError(t, err)
	emitted := receiveLogicalDecodingMessage(t, replConn)
	require.Equal(t, emittedLSN, emitted.LSN)
	require.True(t, emitted.Transactional)
	require.Equal(t, "commit/only", emitted.Prefix)
	require.Equal(t, []byte("after commit"), emitted.Content)
}

func TestLogicalReplicationConsumerOwnedPublicationAndSlot(t *testing.T) {
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	_, err = conn.Current.Exec(ctx, "CREATE USER electric_owner WITH LOGIN REPLICATION PASSWORD 'secret';")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "GRANT CREATE ON SCHEMA public TO electric_owner;")
	require.NoError(t, err)

	ownerConn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://electric_owner:secret@127.0.0.1:%d/postgres?sslmode=disable", port))
	require.NoError(t, err)
	defer ownerConn.Close(context.Background())

	_, err = ownerConn.Exec(ctx, "CREATE TABLE owned_rep_items (tenant_id INT PRIMARY KEY, label TEXT NOT NULL);")
	require.NoError(t, err)
	_, err = ownerConn.Exec(ctx, "CREATE PUBLICATION owned_rep_pub FOR TABLE owned_rep_items;")
	require.NoError(t, err)
	_, err = ownerConn.Exec(ctx, "ALTER TABLE owned_rep_items REPLICA IDENTITY FULL;")
	require.NoError(t, err)

	var owned bool
	require.NoError(t, ownerConn.QueryRow(ctx, `
		SELECT pg_get_userbyid(pubowner) = current_role
		FROM pg_catalog.pg_publication
		WHERE pubname = 'owned_rep_pub'`).Scan(&owned))
	require.True(t, owned)

	replConn := connectReplicationConnAs(t, ctx, port, "electric_owner", "secret")
	defer replConn.Close(context.Background())

	slotName := "owned_rep_slot"
	slot, err := pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication,
	})
	require.NoError(t, err)
	require.Equal(t, slotName, slot.SlotName)

	var slotType string
	var active bool
	require.NoError(t, ownerConn.QueryRow(ctx, `
		SELECT slot_type, active
		FROM pg_catalog.pg_replication_slots
		WHERE slot_name = $1`, slotName).Scan(&slotType, &active))
	require.Equal(t, "logical", slotType)
	require.False(t, active)

	require.NoError(t, pglogrepl.StartReplication(ctx, replConn, slotName, 0, pglogrepl.StartReplicationOptions{
		Mode: pglogrepl.LogicalReplication,
		PluginArgs: []string{
			`"proto_version" '1'`,
			`"publication_names" 'owned_rep_pub'`,
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	writerConn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://postgres:password@127.0.0.1:%d/postgres?sslmode=disable", port))
	require.NoError(t, err)
	defer writerConn.Close(context.Background())
	_, err = writerConn.Exec(ctx, "INSERT INTO owned_rep_items VALUES (7, 'owned');")
	require.NoError(t, err)
	relation, insert, commit := receiveInsertChange(t, replConn)
	requireInsertChange(t, relation, insert, "owned_rep_items", "7", "owned")
	require.Greater(t, commit.CommitLSN, pglogrepl.LSN(0))
}

func TestLogicalReplicationRequiresReplicationRoleForCreateSlotRepro(t *testing.T) {
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	_, err = conn.Current.Exec(ctx, "CREATE USER ordinary_slot_user WITH LOGIN PASSWORD 'secret';")
	require.NoError(t, err)

	replConn, err := pgconn.Connect(ctx, fmt.Sprintf("postgres://ordinary_slot_user:secret@127.0.0.1:%d/postgres?sslmode=disable&replication=database", port))
	if err == nil {
		defer replConn.Close(context.Background())
		_, err = pglogrepl.CreateReplicationSlot(ctx, replConn, "ordinary_slot_user_slot", "pgoutput", pglogrepl.CreateReplicationSlotOptions{
			Mode: pglogrepl.LogicalReplication,
		})
	}
	require.Error(t, err, "logical replication slot creation should require a replication-capable role")
}

func TestLogicalReplicationRequiresReplicationRoleForStartReplicationRepro(t *testing.T) {
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	slotName := "ordinary_start_replication_slot"
	_, err = conn.Current.Exec(ctx, "CREATE TABLE ordinary_start_replication_items (tenant_id BIGINT PRIMARY KEY, label TEXT);")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "CREATE PUBLICATION ordinary_start_replication_pub FOR TABLE ordinary_start_replication_items;")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "CREATE USER ordinary_repl_reader WITH LOGIN PASSWORD 'secret';")
	require.NoError(t, err)

	ownerReplConn := connectReplicationConn(t, ctx, port)
	_, err = pglogrepl.CreateReplicationSlot(ctx, ownerReplConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication,
	})
	require.NoError(t, err)
	require.NoError(t, ownerReplConn.Close(ctx))

	replConn, err := pgconn.Connect(ctx, fmt.Sprintf("postgres://ordinary_repl_reader:secret@127.0.0.1:%d/postgres?sslmode=disable&replication=database", port))
	if err != nil {
		return
	}
	defer replConn.Close(context.Background())
	err = pglogrepl.StartReplication(ctx, replConn, slotName, 0, pglogrepl.StartReplicationOptions{
		Mode: pglogrepl.LogicalReplication,
		PluginArgs: []string{
			`"proto_version" '1'`,
			`"publication_names" 'ordinary_start_replication_pub'`,
		},
	})
	if err != nil {
		return
	}
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, "INSERT INTO ordinary_start_replication_items VALUES (1, 'visible-to-ordinary-role');")
	require.NoError(t, err)
	relation, insert, _ := receiveInsertChange(t, replConn)
	require.Failf(t,
		"logical replication streaming should require a replication-capable role",
		"ordinary role received %s.%s row %s:%s",
		relation.Namespace,
		relation.RelationName,
		string(insert.Tuple.Columns[0].Data),
		string(insert.Tuple.Columns[1].Data),
	)
}

func TestLogicalReplicationRequiresReplicationRoleForDropSlotRepro(t *testing.T) {
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	slotName := "ordinary_drop_protocol_slot"
	_, err = conn.Current.Exec(ctx, "CREATE USER ordinary_slot_dropper_protocol WITH LOGIN PASSWORD 'secret';")
	require.NoError(t, err)

	ownerReplConn := connectReplicationConn(t, ctx, port)
	_, err = pglogrepl.CreateReplicationSlot(ctx, ownerReplConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication,
	})
	require.NoError(t, err)
	require.NoError(t, ownerReplConn.Close(ctx))

	replConn, err := pgconn.Connect(ctx, fmt.Sprintf("postgres://ordinary_slot_dropper_protocol:secret@127.0.0.1:%d/postgres?sslmode=disable&replication=database", port))
	if err != nil {
		return
	}
	defer replConn.Close(context.Background())
	err = pglogrepl.DropReplicationSlot(ctx, replConn, slotName, pglogrepl.DropReplicationSlotOptions{})
	if err != nil {
		return
	}

	var slotCount int
	require.NoError(t, conn.Current.QueryRow(ctx, `
		SELECT count(*) FROM pg_catalog.pg_replication_slots WHERE slot_name = $1`, slotName).Scan(&slotCount))
	require.Failf(t,
		"DROP_REPLICATION_SLOT should require a replication-capable role",
		"ordinary role dropped slot %s through the replication protocol; remaining slot count=%d",
		slotName,
		slotCount,
	)
}

func TestPgDropReplicationSlotRequiresReplicationRoleRepro(t *testing.T) {
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	slotName := "ordinary_drop_replication_slot"
	_, err = conn.Current.Exec(ctx, "CREATE USER ordinary_slot_dropper WITH LOGIN PASSWORD 'secret';")
	require.NoError(t, err)

	ownerReplConn := connectReplicationConn(t, ctx, port)
	_, err = pglogrepl.CreateReplicationSlot(ctx, ownerReplConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication,
	})
	require.NoError(t, err)
	require.NoError(t, ownerReplConn.Close(ctx))

	ordinaryConn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://ordinary_slot_dropper:secret@127.0.0.1:%d/postgres?sslmode=disable", port))
	require.NoError(t, err)
	defer ordinaryConn.Close(context.Background())

	_, err = ordinaryConn.Exec(ctx, "SELECT pg_drop_replication_slot($1);", slotName)
	if err != nil {
		return
	}

	var slotCount int
	require.NoError(t, conn.Current.QueryRow(ctx, `
		SELECT count(*) FROM pg_catalog.pg_replication_slots WHERE slot_name = $1`, slotName).Scan(&slotCount))
	require.Failf(t,
		"pg_drop_replication_slot should require a replication-capable role",
		"ordinary role dropped slot %s; remaining slot count=%d",
		slotName,
		slotCount,
	)
}

func TestLogicalReplicationTemporarySlotDropsOnSessionCloseRepro(t *testing.T) {
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	slotName := "temporary_slot_session_close"
	replConn := connectReplicationConn(t, ctx, port)
	_, err = pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode:      pglogrepl.LogicalReplication,
		Temporary: true,
	})
	require.NoError(t, err)

	var slotCount int
	require.NoError(t, conn.Current.QueryRow(ctx, `
		SELECT count(*) FROM pg_catalog.pg_replication_slots WHERE slot_name = $1`, slotName).Scan(&slotCount))
	require.Equal(t, 1, slotCount)

	require.NoError(t, replConn.Close(ctx))
	require.NoError(t, conn.Current.QueryRow(ctx, `
		SELECT count(*) FROM pg_catalog.pg_replication_slots WHERE slot_name = $1`, slotName).Scan(&slotCount))
	require.Equal(t, 0, slotCount, "temporary logical replication slots should be dropped when the creating session closes")
}

func TestLogicalReplicationRejectsInvalidSlotNameRepro(t *testing.T) {
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	slotName := "invalid-slot-name"
	replConn := connectReplicationConn(t, ctx, port)
	defer replConn.Close(context.Background())
	_, err = pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication,
	})
	if err != nil {
		return
	}

	var slotCount int
	require.NoError(t, conn.Current.QueryRow(ctx, `
		SELECT count(*) FROM pg_catalog.pg_replication_slots WHERE slot_name = $1`, slotName).Scan(&slotCount))
	require.Failf(t,
		"logical replication should reject invalid slot names",
		"created slot %s with invalid replication slot name; catalog count=%d",
		slotName,
		slotCount,
	)
}

func TestLogicalReplicationCreateSlotTwoPhaseSetsCatalogFlagRepro(t *testing.T) {
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	slotName := "two_phase_catalog_slot"
	replConn := connectReplicationConn(t, ctx, port)
	defer replConn.Close(context.Background())
	slot, err := pglogrepl.ParseCreateReplicationSlot(replConn.Exec(ctx, "CREATE_REPLICATION_SLOT "+slotName+" LOGICAL pgoutput TWO_PHASE"))
	require.NoError(t, err)
	require.Equal(t, slotName, slot.SlotName)

	var twoPhase bool
	require.NoError(t, conn.Current.QueryRow(ctx, `
		SELECT two_phase FROM pg_catalog.pg_replication_slots WHERE slot_name = $1`, slotName).Scan(&twoPhase))
	require.True(t, twoPhase, "CREATE_REPLICATION_SLOT ... TWO_PHASE should mark the logical slot as two_phase")
}

func TestLogicalReplicationCreateSlotUseSnapshotRequiresTransactionRepro(t *testing.T) {
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	slotName := "use_snapshot_outside_tx_slot"
	replConn := connectReplicationConn(t, ctx, port)
	defer replConn.Close(context.Background())
	_, err = pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode:           pglogrepl.LogicalReplication,
		SnapshotAction: "USE_SNAPSHOT",
	})
	if err != nil {
		return
	}

	var slotCount int
	require.NoError(t, conn.Current.QueryRow(ctx, `
		SELECT count(*) FROM pg_catalog.pg_replication_slots WHERE slot_name = $1`, slotName).Scan(&slotCount))
	require.Failf(t,
		"CREATE_REPLICATION_SLOT ... USE_SNAPSHOT should require a transaction",
		"created slot %s with USE_SNAPSHOT outside a transaction; catalog count=%d",
		slotName,
		slotCount,
	)
}

func TestLogicalReplicationCreateSlotRejectsUnknownOptionRepro(t *testing.T) {
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	slotName := "unknown_create_slot_option"
	replConn := connectReplicationConn(t, ctx, port)
	defer replConn.Close(context.Background())
	_, err = pglogrepl.ParseCreateReplicationSlot(replConn.Exec(ctx, "CREATE_REPLICATION_SLOT "+slotName+" LOGICAL pgoutput UNKNOWN_OPTION"))
	if err != nil {
		return
	}

	var slotCount int
	require.NoError(t, conn.Current.QueryRow(ctx, `
		SELECT count(*) FROM pg_catalog.pg_replication_slots WHERE slot_name = $1`, slotName).Scan(&slotCount))
	require.Failf(t,
		"CREATE_REPLICATION_SLOT should reject unknown options",
		"created slot %s despite UNKNOWN_OPTION; catalog count=%d",
		slotName,
		slotCount,
	)
}

func TestLogicalReplicationRequiresPublicationNamesOptionRepro(t *testing.T) {
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	slotName := "missing_publication_names_slot"
	_, err = conn.Current.Exec(ctx, "CREATE TABLE missing_publication_names_items (tenant_id BIGINT PRIMARY KEY, label TEXT);")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "CREATE PUBLICATION missing_publication_names_pub FOR TABLE missing_publication_names_items;")
	require.NoError(t, err)

	replConn := connectReplicationConn(t, ctx, port)
	defer replConn.Close(context.Background())
	_, err = pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication,
	})
	require.NoError(t, err)
	err = pglogrepl.StartReplication(ctx, replConn, slotName, 0, pglogrepl.StartReplicationOptions{
		Mode: pglogrepl.LogicalReplication,
		PluginArgs: []string{
			`"proto_version" '1'`,
		},
	})
	if err != nil {
		return
	}
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, "INSERT INTO missing_publication_names_items VALUES (1, 'visible-without-publication-names');")
	require.NoError(t, err)
	relation, insert, _ := receiveInsertChange(t, replConn)
	require.Failf(t,
		"pgoutput START_REPLICATION should require publication_names",
		"connection without publication_names received %s.%s row %s:%s",
		relation.Namespace,
		relation.RelationName,
		string(insert.Tuple.Columns[0].Data),
		string(insert.Tuple.Columns[1].Data),
	)
}

func TestLogicalReplicationRejectsMissingPublicationNameRepro(t *testing.T) {
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	slotName := "missing_publication_name_slot"
	_, err = conn.Current.Exec(ctx, "CREATE TABLE missing_publication_name_items (tenant_id BIGINT PRIMARY KEY, label TEXT);")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "CREATE PUBLICATION existing_publication_name_pub FOR TABLE missing_publication_name_items;")
	require.NoError(t, err)

	replConn := connectReplicationConn(t, ctx, port)
	defer replConn.Close(context.Background())
	_, err = pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication,
	})
	require.NoError(t, err)
	err = pglogrepl.StartReplication(ctx, replConn, slotName, 0, pglogrepl.StartReplicationOptions{
		Mode: pglogrepl.LogicalReplication,
		PluginArgs: []string{
			`"proto_version" '1'`,
			`"publication_names" 'existing_publication_name_pub,missing_publication_name_pub'`,
		},
	})
	if err != nil {
		return
	}
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, "INSERT INTO missing_publication_name_items VALUES (1, 'visible-with-missing-publication');")
	require.NoError(t, err)
	relation, insert, _ := receiveInsertChange(t, replConn)
	require.Failf(t,
		"pgoutput START_REPLICATION should reject missing publications",
		"connection with a missing publication still received %s.%s row %s:%s",
		relation.Namespace,
		relation.RelationName,
		string(insert.Tuple.Columns[0].Data),
		string(insert.Tuple.Columns[1].Data),
	)
}

func TestLogicalReplicationPublicationNamesAreCaseSensitiveRepro(t *testing.T) {
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	slotName := "case_sensitive_publication_slot"
	_, err = conn.Current.Exec(ctx, "CREATE TABLE case_sensitive_publication_items (tenant_id BIGINT PRIMARY KEY, label TEXT);")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, `CREATE PUBLICATION "CaseSensitivePublication" FOR TABLE case_sensitive_publication_items;`)
	require.NoError(t, err)

	replConn := connectReplicationConn(t, ctx, port)
	defer replConn.Close(context.Background())
	_, err = pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication,
	})
	require.NoError(t, err)
	err = pglogrepl.StartReplication(ctx, replConn, slotName, 0, pglogrepl.StartReplicationOptions{
		Mode: pglogrepl.LogicalReplication,
		PluginArgs: []string{
			`"proto_version" '1'`,
			`"publication_names" 'casesensitivepublication'`,
		},
	})
	if err != nil {
		return
	}
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, "INSERT INTO case_sensitive_publication_items VALUES (1, 'visible-through-wrong-case');")
	require.NoError(t, err)
	relation, insert, _ := receiveInsertChange(t, replConn)
	require.Failf(t,
		"pgoutput publication_names should match publication names case-sensitively",
		"lower-case publication name received %s.%s row %s:%s from quoted mixed-case publication",
		relation.Namespace,
		relation.RelationName,
		string(insert.Tuple.Columns[0].Data),
		string(insert.Tuple.Columns[1].Data),
	)
}

func TestLogicalReplicationPublicationNamesAllowQuotedCommasRepro(t *testing.T) {
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	slotName := "quoted_comma_publication_slot"
	_, err = conn.Current.Exec(ctx, "CREATE TABLE quoted_comma_publication_items (tenant_id BIGINT PRIMARY KEY, label TEXT);")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, `CREATE PUBLICATION "Quoted,Publication" FOR TABLE quoted_comma_publication_items;`)
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
			`"publication_names" '"Quoted,Publication"'`,
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, "INSERT INTO quoted_comma_publication_items VALUES (1, 'visible-through-quoted-comma-publication');")
	require.NoError(t, err)
	relation, insert, _ := receiveInsertChange(t, replConn)
	requireInsertChange(t, relation, insert, "quoted_comma_publication_items", "1", "visible-through-quoted-comma-publication")
}

func TestLogicalReplicationRequiresProtoVersionOptionRepro(t *testing.T) {
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	slotName := "missing_proto_version_slot"
	_, err = conn.Current.Exec(ctx, "CREATE TABLE missing_proto_version_items (tenant_id BIGINT PRIMARY KEY, label TEXT);")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "CREATE PUBLICATION missing_proto_version_pub FOR TABLE missing_proto_version_items;")
	require.NoError(t, err)

	replConn := connectReplicationConn(t, ctx, port)
	defer replConn.Close(context.Background())
	_, err = pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication,
	})
	require.NoError(t, err)
	err = pglogrepl.StartReplication(ctx, replConn, slotName, 0, pglogrepl.StartReplicationOptions{
		Mode: pglogrepl.LogicalReplication,
		PluginArgs: []string{
			`"publication_names" 'missing_proto_version_pub'`,
		},
	})
	if err != nil {
		return
	}
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, "INSERT INTO missing_proto_version_items VALUES (1, 'visible-without-proto-version');")
	require.NoError(t, err)
	relation, insert, _ := receiveInsertChange(t, replConn)
	require.Failf(t,
		"pgoutput START_REPLICATION should require proto_version",
		"connection without proto_version received %s.%s row %s:%s",
		relation.Namespace,
		relation.RelationName,
		string(insert.Tuple.Columns[0].Data),
		string(insert.Tuple.Columns[1].Data),
	)
}

func TestLogicalReplicationRejectsInvalidProtoVersionOptionRepro(t *testing.T) {
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	slotName := "invalid_proto_version_slot"
	_, err = conn.Current.Exec(ctx, "CREATE TABLE invalid_proto_version_items (tenant_id BIGINT PRIMARY KEY, label TEXT);")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "CREATE PUBLICATION invalid_proto_version_pub FOR TABLE invalid_proto_version_items;")
	require.NoError(t, err)

	replConn := connectReplicationConn(t, ctx, port)
	defer replConn.Close(context.Background())
	_, err = pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication,
	})
	require.NoError(t, err)
	err = pglogrepl.StartReplication(ctx, replConn, slotName, 0, pglogrepl.StartReplicationOptions{
		Mode: pglogrepl.LogicalReplication,
		PluginArgs: []string{
			`"proto_version" 'not-a-number'`,
			`"publication_names" 'invalid_proto_version_pub'`,
		},
	})
	if err != nil {
		return
	}
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, "INSERT INTO invalid_proto_version_items VALUES (1, 'visible-with-invalid-proto-version');")
	require.NoError(t, err)
	relation, insert, _ := receiveInsertChange(t, replConn)
	require.Failf(t,
		"pgoutput START_REPLICATION should reject invalid proto_version",
		"connection with invalid proto_version received %s.%s row %s:%s",
		relation.Namespace,
		relation.RelationName,
		string(insert.Tuple.Columns[0].Data),
		string(insert.Tuple.Columns[1].Data),
	)
}

func TestLogicalReplicationRejectsUnknownPgoutputOptionRepro(t *testing.T) {
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	slotName := "unknown_pgoutput_option_slot"
	_, err = conn.Current.Exec(ctx, "CREATE TABLE unknown_pgoutput_option_items (tenant_id BIGINT PRIMARY KEY, label TEXT);")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "CREATE PUBLICATION unknown_pgoutput_option_pub FOR TABLE unknown_pgoutput_option_items;")
	require.NoError(t, err)

	replConn := connectReplicationConn(t, ctx, port)
	defer replConn.Close(context.Background())
	_, err = pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication,
	})
	require.NoError(t, err)
	err = pglogrepl.StartReplication(ctx, replConn, slotName, 0, pglogrepl.StartReplicationOptions{
		Mode: pglogrepl.LogicalReplication,
		PluginArgs: []string{
			`"proto_version" '1'`,
			`"unknown_pgoutput_option" 'true'`,
			`"publication_names" 'unknown_pgoutput_option_pub'`,
		},
	})
	if err != nil {
		return
	}
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, "INSERT INTO unknown_pgoutput_option_items VALUES (1, 'visible-with-unknown-option');")
	require.NoError(t, err)
	relation, insert, _ := receiveInsertChange(t, replConn)
	require.Failf(t,
		"pgoutput START_REPLICATION should reject unknown options",
		"connection with unknown pgoutput option received %s.%s row %s:%s",
		relation.Namespace,
		relation.RelationName,
		string(insert.Tuple.Columns[0].Data),
		string(insert.Tuple.Columns[1].Data),
	)
}

func TestLogicalReplicationHonorsBinaryOptionRepro(t *testing.T) {
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	slotName := "binary_option_slot"
	_, err = conn.Current.Exec(ctx, "CREATE TABLE binary_option_items (tenant_id INT PRIMARY KEY, quantity INT NOT NULL);")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "CREATE PUBLICATION binary_option_pub FOR TABLE binary_option_items;")
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
			`"binary" 'true'`,
			`"publication_names" 'binary_option_pub'`,
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, "INSERT INTO binary_option_items VALUES (1, 42);")
	require.NoError(t, err)
	relation, insert, _ := receiveInsertChange(t, replConn)
	require.Equal(t, "public", relation.Namespace)
	require.Equal(t, "binary_option_items", relation.RelationName)
	require.Len(t, insert.Tuple.Columns, 2)
	for idx, column := range insert.Tuple.Columns {
		require.Equalf(t,
			uint8(pglogrepl.TupleDataTypeBinary),
			column.DataType,
			"column %d should be sent in binary format when pgoutput binary option is true",
			idx,
		)
	}
}

func TestLogicalReplicationMessagesFalseSuppressesLogicalMessagesRepro(t *testing.T) {
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	slotName := "messages_false_slot"
	_, err = conn.Current.Exec(ctx, "CREATE TABLE messages_false_items (tenant_id BIGINT PRIMARY KEY);")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "CREATE PUBLICATION messages_false_pub FOR TABLE messages_false_items;")
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
			`"messages" 'false'`,
			`"publication_names" 'messages_false_pub'`,
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	var emittedLSNText string
	require.NoError(t, conn.Current.QueryRow(ctx, `
		SELECT pg_logical_emit_message(false, 'messages/false', 'should not stream')::text;`).Scan(&emittedLSNText))
	requireNoReplicationCopyData(t, replConn, 250*time.Millisecond)
}

func TestLogicalReplicationElectricCatalogProbeQueries(t *testing.T) {
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	_, err = conn.Current.Exec(ctx, `
		CREATE TABLE electric_catalog_items (tenant_id INT PRIMARY KEY, label TEXT NOT NULL);
		CREATE PUBLICATION electric_publication_default;
		ALTER PUBLICATION electric_publication_default ADD TABLE electric_catalog_items;
		ALTER TABLE electric_catalog_items REPLICA IDENTITY FULL;`)
	require.NoError(t, err)

	replConn := connectReplicationConn(t, ctx, port)
	defer replConn.Close(context.Background())

	slotName := "electric_slot_default"
	_, err = pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication,
	})
	require.NoError(t, err)

	var canAlterPublication, publishesAllOperations, publishesGeneratedColumns bool
	require.NoError(t, conn.Current.QueryRow(ctx, electricPublicationValidationQuery, "electric_publication_default").Scan(
		&canAlterPublication,
		&publishesAllOperations,
		&publishesGeneratedColumns,
	))
	require.True(t, canAlterPublication)
	require.True(t, publishesAllOperations)
	require.False(t, publishesGeneratedColumns)

	var relationOID string
	require.NoError(t, conn.Current.QueryRow(ctx, `
		SELECT c.oid::text
		FROM pg_catalog.pg_class c
		JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'public' AND c.relname = 'electric_catalog_items'`).Scan(&relationOID))

	rows, err := conn.Current.Query(ctx, electricPublicationRelationsQuery, "electric_publication_default")
	require.NoError(t, err)
	require.True(t, rows.Next())
	values, err := rows.Values()
	require.NoError(t, err)
	require.Len(t, values, 3)
	require.Equal(t, relationOID, fmt.Sprint(values[0]))
	require.Contains(t, pgTextValue(values[1]), "electric_catalog_items")
	require.Equal(t, "f", pgCharValue(values[2]))
	require.False(t, rows.Next())
	require.NoError(t, rows.Err())
	rows.Close()

	rows, err = conn.Current.Query(ctx, electricReplicaIdentityByOIDQuery, []string{relationOID})
	require.NoError(t, err)
	require.True(t, rows.Next())
	values, err = rows.Values()
	require.NoError(t, err)
	require.Len(t, values, 2)
	require.Equal(t, relationOID, fmt.Sprint(values[0]))
	require.Equal(t, "f", pgCharValue(values[1]))
	require.False(t, rows.Next())
	require.NoError(t, rows.Err())
	rows.Close()

	rows, err = conn.Current.Query(ctx, electricInputRelationDriftQuery, []string{relationOID}, []string{"public"}, []string{"electric_catalog_items"})
	require.NoError(t, err)
	require.False(t, rows.Next())
	require.NoError(t, rows.Err())
	rows.Close()

	var snapshot, currentLSN string
	require.NoError(t, conn.Current.QueryRow(ctx, electricSnapshotLSNQuery).Scan(&snapshot, &currentLSN))
	require.NotEmpty(t, snapshot)
	require.NotEmpty(t, currentLSN)

	_, err = conn.Current.Exec(ctx, "INSERT INTO electric_catalog_items VALUES (1, 'one');")
	require.NoError(t, err)

	var pgWALOffset, retainedWALSize, confirmedFlushLSNLag int64
	require.NoError(t, conn.Current.QueryRow(ctx, electricReplicationTelemetryQuery, slotName).Scan(
		&pgWALOffset,
		&retainedWALSize,
		&confirmedFlushLSNLag,
	))
	require.NotZero(t, pgWALOffset)
	require.GreaterOrEqual(t, retainedWALSize, int64(0))
	require.GreaterOrEqual(t, confirmedFlushLSNLag, int64(0))
}

func TestLogicalReplicationDebeziumCatalogProbeQueries(t *testing.T) {
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	_, err = conn.Current.Exec(ctx, `
		CREATE TABLE debezium_catalog_items (tenant_id INT PRIMARY KEY, label TEXT NOT NULL);
		CREATE PUBLICATION debezium_publication FOR TABLE debezium_catalog_items;
		ALTER TABLE debezium_catalog_items REPLICA IDENTITY FULL;`)
	require.NoError(t, err)

	replConn := connectReplicationConn(t, ctx, port)
	defer replConn.Close(context.Background())

	slotName := "debezium_slot"
	_, err = pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication,
	})
	require.NoError(t, err)

	rows, err := conn.Current.Query(ctx, debeziumSlotStateQuery, slotName, "postgres", "pgoutput")
	require.NoError(t, err)
	require.True(t, rows.Next())
	values, err := rows.Values()
	require.NoError(t, err)
	slotColumns := rowValuesByFieldName(t, rows.FieldDescriptions(), values)
	require.Equal(t, slotName, pgTextValue(slotColumns["slot_name"]))
	require.Equal(t, "pgoutput", pgTextValue(slotColumns["plugin"]))
	require.Equal(t, "logical", pgTextValue(slotColumns["slot_type"]))
	require.Equal(t, "postgres", pgTextValue(slotColumns["database"]))
	require.False(t, slotColumns["active"].(bool))
	require.Contains(t, slotColumns, "catalog_xmin")
	require.Contains(t, slotColumns, "restart_lsn")
	require.Contains(t, slotColumns, "confirmed_flush_lsn")
	require.False(t, rows.Next())
	require.NoError(t, rows.Err())
	rows.Close()

	var pubAllTables bool
	require.NoError(t, conn.Current.QueryRow(ctx, debeziumPublicationExistsQuery).Scan(&pubAllTables))
	require.False(t, pubAllTables)

	rows, err = conn.Current.Query(ctx, debeziumCurrentPublicationTablesQuery)
	require.NoError(t, err)
	require.True(t, rows.Next())
	values, err = rows.Values()
	require.NoError(t, err)
	require.Equal(t, []any{"public", "debezium_catalog_items"}, values)
	require.False(t, rows.Next())
	require.NoError(t, rows.Err())
	rows.Close()

	rows, err = conn.Current.Query(ctx, debeziumPublicationValidationQuery, "debezium_publication")
	require.NoError(t, err)
	require.True(t, rows.Next())
	values, err = rows.Values()
	require.NoError(t, err)
	require.Equal(t, []any{"public", "debezium_catalog_items"}, values)
	require.False(t, rows.Next())
	require.NoError(t, rows.Err())
	rows.Close()

	var currentLSN string
	require.NoError(t, conn.Current.QueryRow(ctx, debeziumCurrentXLogLocationQuery).Scan(&currentLSN))
	require.NotEmpty(t, currentLSN)

	var version, currentUser, currentDatabase string
	require.NoError(t, conn.Current.QueryRow(ctx, debeziumServerInfoQuery).Scan(&version, &currentUser, &currentDatabase))
	require.NotEmpty(t, version)
	require.Equal(t, "postgres", currentUser)
	require.Equal(t, "postgres", currentDatabase)

	rows, err = conn.Current.Query(ctx, debeziumRoleMembershipQuery)
	require.NoError(t, err)
	foundPostgresRole := false
	for rows.Next() {
		values, err = rows.Values()
		require.NoError(t, err)
		require.Len(t, values, 8)
		if pgTextValue(values[1]) != "postgres" {
			continue
		}
		foundPostgresRole = true
		require.True(t, values[2].(bool))
		require.True(t, values[3].(bool))
		require.True(t, values[4].(bool))
		require.True(t, values[5].(bool))
		require.True(t, values[6].(bool))
		require.False(t, values[7].(bool))
	}
	require.True(t, foundPostgresRole)
	require.NoError(t, rows.Err())
	rows.Close()
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

// TestLogicalReplicationSourcePrimaryKeyUpdateIncludesOldKeyTupleRepro
// reproduces a logical-replication consistency bug: under default replica
// identity, PostgreSQL includes the old replica-identity key when an UPDATE
// changes that key.
func TestLogicalReplicationSourcePrimaryKeyUpdateIncludesOldKeyTupleRepro(t *testing.T) {
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	slotName := "dg_key_update_slot"
	_, err = conn.Current.Exec(ctx, "CREATE TABLE dg_key_update_items (id INT PRIMARY KEY, label TEXT);")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "INSERT INTO dg_key_update_items VALUES (1, 'old-label');")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "CREATE PUBLICATION dg_key_update_pub FOR TABLE dg_key_update_items;")
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
			`"publication_names" 'dg_key_update_pub'`,
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, "UPDATE dg_key_update_items SET id = 2, label = 'new-label' WHERE id = 1;")
	require.NoError(t, err)
	relation, update, _ := receiveUpdateChange(t, replConn)

	require.Equal(t, "public", relation.Namespace)
	require.Equal(t, "dg_key_update_items", relation.RelationName)
	require.Equal(t, relation.RelationID, update.RelationID)
	require.Equal(t, uint8(pglogrepl.UpdateMessageTupleTypeKey), update.OldTupleType)
	require.NotNil(t, update.OldTuple)
	require.Len(t, update.OldTuple.Columns, 1)
	require.Equal(t, "1", string(update.OldTuple.Columns[0].Data))
	require.NotNil(t, update.NewTuple)
	require.Len(t, update.NewTuple.Columns, 2)
	require.Equal(t, "2", string(update.NewTuple.Columns[0].Data))
	require.Equal(t, "new-label", string(update.NewTuple.Columns[1].Data))
}

// TestLogicalReplicationSourceDeleteUsesOldKeyForDefaultReplicaIdentityRepro
// reproduces a logical-replication correctness bug: under default replica
// identity, PostgreSQL DELETE messages include only the replica-identity key,
// not the full deleted row.
func TestLogicalReplicationSourceDeleteUsesOldKeyForDefaultReplicaIdentityRepro(t *testing.T) {
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	slotName := "dg_default_delete_slot"
	_, err = conn.Current.Exec(ctx, "CREATE TABLE dg_default_delete_items (id INT PRIMARY KEY, label TEXT);")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "INSERT INTO dg_default_delete_items VALUES (1, 'delete-label');")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "CREATE PUBLICATION dg_default_delete_pub FOR TABLE dg_default_delete_items;")
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
			`"publication_names" 'dg_default_delete_pub'`,
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, "DELETE FROM dg_default_delete_items WHERE id = 1;")
	require.NoError(t, err)
	relation, deleteMessage, _ := receiveDeleteChange(t, replConn)

	require.Equal(t, "public", relation.Namespace)
	require.Equal(t, "dg_default_delete_items", relation.RelationName)
	require.Equal(t, relation.RelationID, deleteMessage.RelationID)
	require.Equal(t, uint8(pglogrepl.DeleteMessageTupleTypeKey), deleteMessage.OldTupleType)
	require.NotNil(t, deleteMessage.OldTuple)
	require.Len(t, deleteMessage.OldTuple.Columns, 1)
	require.Equal(t, "1", string(deleteMessage.OldTuple.Columns[0].Data))
}

// TestLogicalReplicationSourceDeleteUsesIndexKeyForReplicaIdentityUsingIndexRepro
// reproduces a logical-replication correctness bug: REPLICA IDENTITY USING
// INDEX DELETE messages must send only the configured replica-identity index
// key, not the full deleted row.
func TestLogicalReplicationSourceDeleteUsesIndexKeyForReplicaIdentityUsingIndexRepro(t *testing.T) {
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	slotName := "dg_index_delete_slot"
	_, err = conn.Current.Exec(ctx, `
		CREATE TABLE dg_index_delete_items (
			id INT PRIMARY KEY,
			external_id TEXT NOT NULL,
			private_label TEXT
		);`)
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "CREATE UNIQUE INDEX dg_index_delete_external_idx ON dg_index_delete_items (external_id);")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "ALTER TABLE dg_index_delete_items REPLICA IDENTITY USING INDEX dg_index_delete_external_idx;")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "INSERT INTO dg_index_delete_items VALUES (1, 'external-1', 'private-delete-label');")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "CREATE PUBLICATION dg_index_delete_pub FOR TABLE dg_index_delete_items;")
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
			`"publication_names" 'dg_index_delete_pub'`,
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, "DELETE FROM dg_index_delete_items WHERE id = 1;")
	require.NoError(t, err)
	relation, deleteMessage, _ := receiveDeleteChange(t, replConn)

	require.Equal(t, "public", relation.Namespace)
	require.Equal(t, "dg_index_delete_items", relation.RelationName)
	require.Equal(t, uint8(0), relation.Columns[0].Flags)
	require.Equal(t, uint8(1), relation.Columns[1].Flags)
	require.Equal(t, relation.RelationID, deleteMessage.RelationID)
	require.Equal(t, uint8(pglogrepl.DeleteMessageTupleTypeKey), deleteMessage.OldTupleType)
	require.NotNil(t, deleteMessage.OldTuple)
	require.Len(t, deleteMessage.OldTuple.Columns, 1)
	require.Equal(t, "external-1", string(deleteMessage.OldTuple.Columns[0].Data))
}

// TestLogicalReplicationSourceUpdateIncludesOldIndexKeyForReplicaIdentityUsingIndexRepro
// reproduces a logical-replication consistency bug: when an UPDATE changes a
// REPLICA IDENTITY USING INDEX key, PostgreSQL includes the old index key so
// subscribers can locate the previous row.
func TestLogicalReplicationSourceUpdateIncludesOldIndexKeyForReplicaIdentityUsingIndexRepro(t *testing.T) {
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	slotName := "dg_index_update_slot"
	_, err = conn.Current.Exec(ctx, `
		CREATE TABLE dg_index_update_items (
			id INT PRIMARY KEY,
			external_id TEXT NOT NULL,
			label TEXT
		);`)
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "CREATE UNIQUE INDEX dg_index_update_external_idx ON dg_index_update_items (external_id);")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "ALTER TABLE dg_index_update_items REPLICA IDENTITY USING INDEX dg_index_update_external_idx;")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "INSERT INTO dg_index_update_items VALUES (1, 'external-before', 'before-label');")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "CREATE PUBLICATION dg_index_update_pub FOR TABLE dg_index_update_items;")
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
			`"publication_names" 'dg_index_update_pub'`,
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, `
		UPDATE dg_index_update_items
		SET external_id = 'external-after', label = 'after-label'
		WHERE id = 1;`)
	require.NoError(t, err)
	relation, update, _ := receiveUpdateChange(t, replConn)

	require.Equal(t, "public", relation.Namespace)
	require.Equal(t, "dg_index_update_items", relation.RelationName)
	require.Equal(t, uint8(0), relation.Columns[0].Flags)
	require.Equal(t, uint8(1), relation.Columns[1].Flags)
	require.Equal(t, relation.RelationID, update.RelationID)
	require.Equal(t, uint8(pglogrepl.UpdateMessageTupleTypeKey), update.OldTupleType)
	require.NotNil(t, update.OldTuple)
	require.Len(t, update.OldTuple.Columns, 1)
	require.Equal(t, "external-before", string(update.OldTuple.Columns[0].Data))
	require.NotNil(t, update.NewTuple)
	require.Len(t, update.NewTuple.Columns, 3)
	require.Equal(t, "1", string(update.NewTuple.Columns[0].Data))
	require.Equal(t, "external-after", string(update.NewTuple.Columns[1].Data))
	require.Equal(t, "after-label", string(update.NewTuple.Columns[2].Data))
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

func TestLogicalReplicationDropSlotWaitWaitsForInactiveSlotRepro(t *testing.T) {
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	slotName := "dg_drop_slot_wait_slot"
	_, err = conn.Current.Exec(ctx, "CREATE TABLE dg_drop_slot_wait_items (id INT PRIMARY KEY);")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "CREATE PUBLICATION dg_drop_slot_wait_pub FOR TABLE dg_drop_slot_wait_items;")
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
			`"publication_names" 'dg_drop_slot_wait_pub'`,
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	releaseActiveSlot := make(chan struct{})
	go func() {
		defer close(releaseActiveSlot)
		time.Sleep(100 * time.Millisecond)
		_, _ = pglogrepl.SendStandbyCopyDone(ctx, replConn)
		_ = replConn.Close(ctx)
	}()

	dropConn := connectReplicationConn(t, ctx, port)
	defer dropConn.Close(context.Background())
	err = pglogrepl.DropReplicationSlot(ctx, dropConn, slotName, pglogrepl.DropReplicationSlotOptions{Wait: true})
	<-releaseActiveSlot
	if err != nil {
		cleanupConn := connectReplicationConn(t, ctx, port)
		_ = pglogrepl.DropReplicationSlot(ctx, cleanupConn, slotName, pglogrepl.DropReplicationSlotOptions{})
		_ = cleanupConn.Close(ctx)
	}
	require.NoError(t, err, "DROP_REPLICATION_SLOT WAIT should wait until the active slot becomes inactive and then drop it")

	var slotCount int
	require.NoError(t, conn.Current.QueryRow(ctx, `
		SELECT count(*) FROM pg_catalog.pg_replication_slots WHERE slot_name = $1`, slotName).Scan(&slotCount))
	require.Equal(t, 0, slotCount)
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

// TestLogicalReplicationSourceDoesNotDuplicateOverlappingPublicationsRepro
// reproduces a logical-replication consistency bug: subscribing to two
// publications that both include the same table must not publish the same row
// change twice.
func TestLogicalReplicationSourceDoesNotDuplicateOverlappingPublicationsRepro(t *testing.T) {
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	slotName := "dg_overlap_publications_slot"
	_, err = conn.Current.Exec(ctx, "CREATE TABLE dg_overlap_items (tenant_id BIGINT PRIMARY KEY, label TEXT);")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "CREATE PUBLICATION dg_overlap_pub_a FOR TABLE dg_overlap_items;")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "CREATE PUBLICATION dg_overlap_pub_b FOR TABLE dg_overlap_items;")
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
			`"publication_names" 'dg_overlap_pub_a,dg_overlap_pub_b'`,
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, "INSERT INTO dg_overlap_items VALUES (1, 'published-once');")
	require.NoError(t, err)
	relation, insert, _ := receiveInsertChange(t, replConn)
	requireInsertChange(t, relation, insert, "dg_overlap_items", "1", "published-once")
	requireNoReplicationCopyData(t, replConn, 250*time.Millisecond)
}

// TestLogicalReplicationSourcePublishesUpdateFromRepro reproduces a
// logical-replication consistency bug: UPDATE ... FROM mutates the target table
// and must publish the changed target row.
func TestLogicalReplicationSourcePublishesUpdateFromRepro(t *testing.T) {
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	slotName := "dg_update_from_slot"
	_, err = conn.Current.Exec(ctx, "CREATE TABLE dg_update_from_items (id INT PRIMARY KEY, source_id INT, label TEXT);")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "CREATE TABLE dg_update_from_source (id INT PRIMARY KEY, new_label TEXT);")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "INSERT INTO dg_update_from_items VALUES (1, 10, 'before-update-from');")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "INSERT INTO dg_update_from_source VALUES (10, 'after-update-from');")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "CREATE PUBLICATION dg_update_from_pub FOR TABLE dg_update_from_items;")
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
			`"publication_names" 'dg_update_from_pub'`,
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, `
		UPDATE dg_update_from_items AS i
		SET label = s.new_label
		FROM dg_update_from_source AS s
	WHERE i.source_id = s.id;`)
	require.NoError(t, err)
	var label string
	require.NoError(t, conn.Current.QueryRow(ctx, "SELECT label FROM dg_update_from_items WHERE id = 1;").Scan(&label))
	require.Equal(t, "after-update-from", label)
	relation, update, _ := receiveUpdateChange(t, replConn)

	require.Equal(t, "public", relation.Namespace)
	require.Equal(t, "dg_update_from_items", relation.RelationName)
	require.Equal(t, relation.RelationID, update.RelationID)
	require.NotNil(t, update.NewTuple)
	require.Len(t, update.NewTuple.Columns, 3)
	require.Equal(t, "1", string(update.NewTuple.Columns[0].Data))
	require.Equal(t, "10", string(update.NewTuple.Columns[1].Data))
	require.Equal(t, "after-update-from", string(update.NewTuple.Columns[2].Data))
}

// TestLogicalReplicationSourcePublishesUpdateForOnConflictDoUpdateRepro
// reproduces a logical-replication consistency bug: INSERT ... ON CONFLICT DO
// UPDATE must publish UPDATE when it updates an existing row.
func TestLogicalReplicationSourcePublishesUpdateForOnConflictDoUpdateRepro(t *testing.T) {
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	slotName := "dg_upsert_update_slot"
	_, err = conn.Current.Exec(ctx, "CREATE TABLE dg_upsert_update_items (id INT PRIMARY KEY, label TEXT);")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "INSERT INTO dg_upsert_update_items VALUES (1, 'before-upsert');")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "CREATE PUBLICATION dg_upsert_update_pub FOR TABLE dg_upsert_update_items;")
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
			`"publication_names" 'dg_upsert_update_pub'`,
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, `
	INSERT INTO dg_upsert_update_items VALUES (1, 'after-upsert')
	ON CONFLICT (id) DO UPDATE SET label = EXCLUDED.label;`)
	require.NoError(t, err)
	var label string
	require.NoError(t, conn.Current.QueryRow(ctx, "SELECT label FROM dg_upsert_update_items WHERE id = 1;").Scan(&label))
	require.Equal(t, "after-upsert", label)
	relation, msg, _ := receiveFirstChangeMessage(t, replConn)
	update, ok := msg.(*pglogrepl.UpdateMessageV2)
	require.Truef(t, ok, "expected UPDATE logical replication message, got %T", msg)

	require.Equal(t, "public", relation.Namespace)
	require.Equal(t, "dg_upsert_update_items", relation.RelationName)
	require.Equal(t, relation.RelationID, update.RelationID)
	require.NotNil(t, update.NewTuple)
	require.Len(t, update.NewTuple.Columns, 2)
	require.Equal(t, "1", string(update.NewTuple.Columns[0].Data))
	require.Equal(t, "after-upsert", string(update.NewTuple.Columns[1].Data))
}

// TestLogicalReplicationSourcePublishesCopyFromRowsRepro reproduces a
// logical-replication consistency bug: COPY FROM inserts publisher rows and
// must publish INSERT messages for those rows.
func TestLogicalReplicationSourcePublishesCopyFromRowsRepro(t *testing.T) {
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	slotName := "dg_copy_from_slot"
	_, err = conn.Current.Exec(ctx, "CREATE TABLE dg_copy_from_items (id INT PRIMARY KEY, label TEXT);")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "CREATE PUBLICATION dg_copy_from_pub FOR TABLE dg_copy_from_items;")
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
			`"publication_names" 'dg_copy_from_pub'`,
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	tag, err := conn.Current.PgConn().CopyFrom(ctx, bytes.NewBufferString("1\tcopied-from-stdin\n"), "COPY dg_copy_from_items (id, label) FROM STDIN;")
	require.NoError(t, err)
	require.Equal(t, int64(1), tag.RowsAffected())
	var label string
	require.NoError(t, conn.Current.QueryRow(ctx, "SELECT label FROM dg_copy_from_items WHERE id = 1;").Scan(&label))
	require.Equal(t, "copied-from-stdin", label)

	relation, insert, _ := receiveInsertChange(t, replConn)
	requireInsertChange(t, relation, insert, "dg_copy_from_items", "1", "copied-from-stdin")
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

// TestLogicalReplicationSourceSupportsRangePublicationRowFilterRepro reproduces
// a logical-replication correctness bug: PostgreSQL publication row filters use
// ordinary immutable WHERE expressions, so range comparisons must not make
// publisher DML fail.
func TestLogicalReplicationSourceSupportsRangePublicationRowFilterRepro(t *testing.T) {
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	slotName := "dg_filter_comparison_slot"
	_, err = conn.Current.Exec(ctx, `
		CREATE TABLE dg_filter_comparison_items (
			item_id BIGINT PRIMARY KEY,
			customer_id BIGINT NOT NULL,
			label TEXT
		);`)
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, `
		CREATE PUBLICATION dg_filter_comparison_pub
		FOR TABLE dg_filter_comparison_items
		WHERE (customer_id > 40);`)
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
			`"publication_names" 'dg_filter_comparison_pub'`,
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, "INSERT INTO dg_filter_comparison_items VALUES (1, 42, 'visible-by-range');")
	require.NoError(t, err)

	relation, insert, _ := receiveInsertChange(t, replConn)
	require.Equal(t, "public", relation.Namespace)
	require.Equal(t, "dg_filter_comparison_items", relation.RelationName)
	require.Equal(t, relation.RelationID, insert.RelationID)
	require.Len(t, insert.Tuple.Columns, 3)
	require.Equal(t, "1", string(insert.Tuple.Columns[0].Data))
	require.Equal(t, "42", string(insert.Tuple.Columns[1].Data))
	require.Equal(t, "visible-by-range", string(insert.Tuple.Columns[2].Data))
}

// TestLogicalReplicationSourceTypeCoercesPublicationRowFilterRepro reproduces a
// logical-replication consistency bug: publication row filters are SQL
// expressions, so equality comparisons must use PostgreSQL type coercion rather
// than textual byte equality.
func TestLogicalReplicationSourceTypeCoercesPublicationRowFilterRepro(t *testing.T) {
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	slotName := "dg_filter_coercion_slot"
	_, err = conn.Current.Exec(ctx, `
		CREATE TABLE dg_filter_coercion_items (
			item_id BIGINT PRIMARY KEY,
			customer_id BIGINT NOT NULL,
			label TEXT
		);`)
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, `
		CREATE PUBLICATION dg_filter_coercion_pub
		FOR TABLE dg_filter_coercion_items
		WHERE (customer_id = 42.0);`)
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
			`"publication_names" 'dg_filter_coercion_pub'`,
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, "INSERT INTO dg_filter_coercion_items VALUES (1, 42, 'visible-by-coercion');")
	require.NoError(t, err)

	relation, insert, _ := receiveInsertChange(t, replConn)
	require.Equal(t, "public", relation.Namespace)
	require.Equal(t, "dg_filter_coercion_items", relation.RelationName)
	require.Equal(t, relation.RelationID, insert.RelationID)
	require.Len(t, insert.Tuple.Columns, 3)
	require.Equal(t, "1", string(insert.Tuple.Columns[0].Data))
	require.Equal(t, "42", string(insert.Tuple.Columns[1].Data))
	require.Equal(t, "visible-by-coercion", string(insert.Tuple.Columns[2].Data))
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

// TestLogicalReplicationSourcePublishesDeleteWhenUpdateLeavesRowFilterRepro
// reproduces a logical-replication consistency bug: if an UPDATE changes a row
// from matching a publication row filter to not matching it, PostgreSQL
// publishes a DELETE for the old visible row.
func TestLogicalReplicationSourcePublishesDeleteWhenUpdateLeavesRowFilterRepro(t *testing.T) {
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	slotName := "dg_filter_transition_slot"
	_, err = conn.Current.Exec(ctx, `
		CREATE TABLE dg_filter_transition_items (
			item_id BIGINT PRIMARY KEY,
			customer_id BIGINT NOT NULL,
			label TEXT
		);`)
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, `
		INSERT INTO dg_filter_transition_items VALUES
			(1, 42, 'visible-before');`)
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, `
		CREATE PUBLICATION dg_filter_transition_pub
		FOR TABLE dg_filter_transition_items
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
			`"publication_names" 'dg_filter_transition_pub'`,
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, `
		UPDATE dg_filter_transition_items
		SET customer_id = 7, label = 'hidden-after'
		WHERE item_id = 1;`)
	require.NoError(t, err)
	relation, deleteMessage, _ := receiveDeleteChange(t, replConn)

	require.Equal(t, "public", relation.Namespace)
	require.Equal(t, "dg_filter_transition_items", relation.RelationName)
	require.Equal(t, relation.RelationID, deleteMessage.RelationID)
	require.NotNil(t, deleteMessage.OldTuple)
	require.Len(t, deleteMessage.OldTuple.Columns, 3)
	require.Equal(t, "1", string(deleteMessage.OldTuple.Columns[0].Data))
	require.Equal(t, "42", string(deleteMessage.OldTuple.Columns[1].Data))
	require.Equal(t, "visible-before", string(deleteMessage.OldTuple.Columns[2].Data))
}

// TestLogicalReplicationSourcePublishesInsertWhenUpdateEntersRowFilterRepro
// reproduces a logical-replication consistency bug: if an UPDATE changes a row
// from not matching a publication row filter to matching it, PostgreSQL
// publishes an INSERT for the newly visible row.
func TestLogicalReplicationSourcePublishesInsertWhenUpdateEntersRowFilterRepro(t *testing.T) {
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	slotName := "dg_filter_enter_slot"
	_, err = conn.Current.Exec(ctx, `
		CREATE TABLE dg_filter_enter_items (
			item_id BIGINT PRIMARY KEY,
			customer_id BIGINT NOT NULL,
			label TEXT
		);`)
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, `
		INSERT INTO dg_filter_enter_items VALUES
			(1, 7, 'hidden-before');`)
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, `
		CREATE PUBLICATION dg_filter_enter_pub
		FOR TABLE dg_filter_enter_items
		WHERE (customer_id = 42)
		WITH (publish = 'insert, update');`)
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
			`"publication_names" 'dg_filter_enter_pub'`,
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, `
		UPDATE dg_filter_enter_items
		SET customer_id = 42, label = 'visible-after'
		WHERE item_id = 1;`)
	require.NoError(t, err)
	relation, insert, _ := receiveInsertChange(t, replConn)

	require.Equal(t, "public", relation.Namespace)
	require.Equal(t, "dg_filter_enter_items", relation.RelationName)
	require.Equal(t, relation.RelationID, insert.RelationID)
	require.Len(t, insert.Tuple.Columns, 3)
	require.Equal(t, "1", string(insert.Tuple.Columns[0].Data))
	require.Equal(t, "42", string(insert.Tuple.Columns[1].Data))
	require.Equal(t, "visible-after", string(insert.Tuple.Columns[2].Data))
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

// TestLogicalReplicationSourceSavepointRollbackRestoresTruncatedRowsRepro
// reproduces a data consistency bug in the logical-replication capture path:
// TRUNCATE inside a rolled-back savepoint must restore publisher rows and must
// not emit a truncate message when the outer transaction commits.
func TestLogicalReplicationSourceSavepointRollbackRestoresTruncatedRowsRepro(t *testing.T) {
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	slotName := "dg_savepoint_truncate_slot"
	_, err = conn.Current.Exec(ctx, "CREATE TABLE dg_savepoint_truncate_items (tenant_id BIGINT PRIMARY KEY, label TEXT);")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "INSERT INTO dg_savepoint_truncate_items VALUES (1, 'one'), (2, 'two');")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "CREATE PUBLICATION dg_savepoint_truncate_pub FOR TABLE dg_savepoint_truncate_items WITH (publish = 'truncate');")
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
			`"publication_names" 'dg_savepoint_truncate_pub'`,
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, "BEGIN;")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "SAVEPOINT dg_truncate_sp;")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "TRUNCATE dg_savepoint_truncate_items;")
	require.NoError(t, err)
	var rowCount int
	require.NoError(t, conn.Current.QueryRow(ctx, "SELECT count(*) FROM dg_savepoint_truncate_items;").Scan(&rowCount))
	require.Equal(t, 0, rowCount)
	_, err = conn.Current.Exec(ctx, "ROLLBACK TO SAVEPOINT dg_truncate_sp;")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "COMMIT;")
	require.NoError(t, err)
	require.NoError(t, conn.Current.QueryRow(ctx, "SELECT count(*) FROM dg_savepoint_truncate_items;").Scan(&rowCount))
	require.Equal(t, 2, rowCount)

	requireNoReplicationCopyData(t, replConn, 250*time.Millisecond)
}

// TestLogicalReplicationSourceRollbackRestoresTruncatedRowsRepro reproduces a
// data consistency bug in the logical-replication capture path: TRUNCATE inside
// a rolled-back explicit transaction must restore publisher rows and must not
// emit a truncate message.
func TestLogicalReplicationSourceRollbackRestoresTruncatedRowsRepro(t *testing.T) {
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	slotName := "dg_rollback_truncate_slot"
	_, err = conn.Current.Exec(ctx, "CREATE TABLE dg_rollback_truncate_items (tenant_id BIGINT PRIMARY KEY, label TEXT);")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "INSERT INTO dg_rollback_truncate_items VALUES (1, 'one'), (2, 'two');")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "CREATE PUBLICATION dg_rollback_truncate_pub FOR TABLE dg_rollback_truncate_items WITH (publish = 'truncate');")
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
			`"publication_names" 'dg_rollback_truncate_pub'`,
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, "BEGIN;")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "TRUNCATE dg_rollback_truncate_items;")
	require.NoError(t, err)
	var rowCount int
	require.NoError(t, conn.Current.QueryRow(ctx, "SELECT count(*) FROM dg_rollback_truncate_items;").Scan(&rowCount))
	require.Equal(t, 0, rowCount)
	_, err = conn.Current.Exec(ctx, "ROLLBACK;")
	require.NoError(t, err)
	require.NoError(t, conn.Current.QueryRow(ctx, "SELECT count(*) FROM dg_rollback_truncate_items;").Scan(&rowCount))
	require.Equal(t, 2, rowCount)

	requireNoReplicationCopyData(t, replConn, 250*time.Millisecond)
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

func TestLogicalReplicationSourceSavepointRollbackDropsBufferedChangesRepro(t *testing.T) {
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	slotName := "dg_savepoint_tx_slot"
	_, err = conn.Current.Exec(ctx, "CREATE TABLE dg_savepoint_tx_items (tenant_id BIGINT PRIMARY KEY, label TEXT);")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "CREATE PUBLICATION dg_savepoint_tx_pub FOR TABLE dg_savepoint_tx_items;")
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
			`"publication_names" 'dg_savepoint_tx_pub'`,
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, "BEGIN;")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "INSERT INTO dg_savepoint_tx_items VALUES (50, 'kept-before-savepoint');")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "SAVEPOINT dg_sp;")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "INSERT INTO dg_savepoint_tx_items VALUES (51, 'rolled-back-savepoint');")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "ROLLBACK TO SAVEPOINT dg_sp;")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "INSERT INTO dg_savepoint_tx_items VALUES (52, 'kept-after-savepoint');")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "COMMIT;")
	require.NoError(t, err)

	var rolledBackRows int
	require.NoError(t, conn.Current.QueryRow(ctx, "SELECT count(*) FROM dg_savepoint_tx_items WHERE tenant_id = 51;").Scan(&rolledBackRows))
	require.Equal(t, 0, rolledBackRows)

	txn := receiveLogicalTransaction(t, replConn)
	insertedRows := make([]string, 0, len(txn.inserts))
	for _, insert := range txn.inserts {
		require.Len(t, insert.Tuple.Columns, 2)
		insertedRows = append(insertedRows, fmt.Sprintf("%s:%s",
			string(insert.Tuple.Columns[0].Data),
			string(insert.Tuple.Columns[1].Data),
		))
	}
	require.Equal(t, []string{
		"50:kept-before-savepoint",
		"52:kept-after-savepoint",
	}, insertedRows)
}

func TestLogicalReplicationSourceSavepointRollbackDropsBufferedUpdateRepro(t *testing.T) {
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	slotName := "dg_savepoint_update_slot"
	_, err = conn.Current.Exec(ctx, "CREATE TABLE dg_savepoint_update_items (tenant_id BIGINT PRIMARY KEY, label TEXT);")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "INSERT INTO dg_savepoint_update_items VALUES (50, 'before-50'), (51, 'before-51'), (52, 'before-52');")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "CREATE PUBLICATION dg_savepoint_update_pub FOR TABLE dg_savepoint_update_items;")
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
			`"publication_names" 'dg_savepoint_update_pub'`,
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, "BEGIN;")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "UPDATE dg_savepoint_update_items SET label = 'kept-before-savepoint' WHERE tenant_id = 50;")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "SAVEPOINT dg_sp;")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "UPDATE dg_savepoint_update_items SET label = 'rolled-back-savepoint' WHERE tenant_id = 51;")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "ROLLBACK TO SAVEPOINT dg_sp;")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "UPDATE dg_savepoint_update_items SET label = 'kept-after-savepoint' WHERE tenant_id = 52;")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "COMMIT;")
	require.NoError(t, err)

	var rolledBackLabel string
	require.NoError(t, conn.Current.QueryRow(ctx, "SELECT label FROM dg_savepoint_update_items WHERE tenant_id = 51;").Scan(&rolledBackLabel))
	require.Equal(t, "before-51", rolledBackLabel)

	txn := receiveLogicalTransaction(t, replConn)
	updatedRows := make([]string, 0, len(txn.updates))
	for _, update := range txn.updates {
		require.NotNil(t, update.NewTuple)
		require.Len(t, update.NewTuple.Columns, 2)
		updatedRows = append(updatedRows, fmt.Sprintf("%s:%s",
			string(update.NewTuple.Columns[0].Data),
			string(update.NewTuple.Columns[1].Data),
		))
	}
	require.Equal(t, []string{
		"50:kept-before-savepoint",
		"52:kept-after-savepoint",
	}, updatedRows)
}

func TestLogicalReplicationSourceSavepointRollbackDropsBufferedDeleteRepro(t *testing.T) {
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	slotName := "dg_savepoint_delete_slot"
	_, err = conn.Current.Exec(ctx, "CREATE TABLE dg_savepoint_delete_items (tenant_id BIGINT PRIMARY KEY, label TEXT);")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "ALTER TABLE dg_savepoint_delete_items REPLICA IDENTITY FULL;")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "INSERT INTO dg_savepoint_delete_items VALUES (50, 'delete-before-savepoint'), (51, 'rolled-back-delete'), (52, 'delete-after-savepoint');")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "CREATE PUBLICATION dg_savepoint_delete_pub FOR TABLE dg_savepoint_delete_items;")
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
			`"publication_names" 'dg_savepoint_delete_pub'`,
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, "BEGIN;")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "DELETE FROM dg_savepoint_delete_items WHERE tenant_id = 50;")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "SAVEPOINT dg_sp;")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "DELETE FROM dg_savepoint_delete_items WHERE tenant_id = 51;")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "ROLLBACK TO SAVEPOINT dg_sp;")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "DELETE FROM dg_savepoint_delete_items WHERE tenant_id = 52;")
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, "COMMIT;")
	require.NoError(t, err)

	var rolledBackRows int
	require.NoError(t, conn.Current.QueryRow(ctx, "SELECT count(*) FROM dg_savepoint_delete_items WHERE tenant_id = 51;").Scan(&rolledBackRows))
	require.Equal(t, 1, rolledBackRows)

	txn := receiveLogicalTransaction(t, replConn)
	deletedRows := make([]string, 0, len(txn.deletes))
	for _, deleteMessage := range txn.deletes {
		require.NotNil(t, deleteMessage.OldTuple)
		require.Len(t, deleteMessage.OldTuple.Columns, 2)
		deletedRows = append(deletedRows, fmt.Sprintf("%s:%s",
			string(deleteMessage.OldTuple.Columns[0].Data),
			string(deleteMessage.OldTuple.Columns[1].Data),
		))
	}
	require.Equal(t, []string{
		"50:delete-before-savepoint",
		"52:delete-after-savepoint",
	}, deletedRows)
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
	return connectReplicationConnAs(t, ctx, port, "postgres", "password")
}

func connectReplicationConnAs(t *testing.T, ctx context.Context, port int, user string, password string) *pgconn.PgConn {
	t.Helper()
	conn, err := pgconn.Connect(ctx, fmt.Sprintf("postgres://%s:%s@127.0.0.1:%d/postgres?sslmode=disable&replication=database&application_name=dg-logical-source-test", user, password, port))
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

func receiveLogicalDecodingMessage(t *testing.T, conn *pgconn.PgConn) *pglogrepl.LogicalDecodingMessageV2 {
	t.Helper()
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
		if emitted, ok := msg.(*pglogrepl.LogicalDecodingMessageV2); ok {
			return emitted
		}
	}
	require.FailNow(t, "timed out waiting for logical decoding message")
	return nil
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

func receiveFirstChangeMessage(t *testing.T, conn *pgconn.PgConn) (*pglogrepl.RelationMessageV2, pglogrepl.Message, *pglogrepl.CommitMessage) {
	t.Helper()
	var relation *pglogrepl.RelationMessageV2
	var change pglogrepl.Message
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
		case *pglogrepl.InsertMessageV2, *pglogrepl.UpdateMessageV2, *pglogrepl.DeleteMessageV2:
			change = typed
		case *pglogrepl.CommitMessage:
			commit = typed
		}
		if relation != nil && change != nil && commit != nil {
			return relation, change, commit
		}
	}
	require.FailNow(t, "timed out waiting for relation, first change, and commit logical replication messages")
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
	updates           []*pglogrepl.UpdateMessageV2
	deletes           []*pglogrepl.DeleteMessageV2
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
		case *pglogrepl.UpdateMessageV2:
			require.True(t, beginSeen, "received Update before Begin")
			txn.updates = append(txn.updates, typed)
		case *pglogrepl.DeleteMessageV2:
			require.True(t, beginSeen, "received Delete before Begin")
			txn.deletes = append(txn.deletes, typed)
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

func pgTextValue(value any) string {
	switch typed := value.(type) {
	case []byte:
		return string(typed)
	case uint8:
		return string([]byte{typed})
	default:
		return fmt.Sprint(typed)
	}
}

func pgCharValue(value any) string {
	switch typed := value.(type) {
	case int:
		if typed >= 0 && typed <= 255 {
			return string(byte(typed))
		}
	case int8:
		return string(byte(typed))
	case int16:
		if typed >= 0 && typed <= 255 {
			return string(byte(typed))
		}
	case int32:
		if typed >= 0 && typed <= 255 {
			return string(byte(typed))
		}
	case int64:
		if typed >= 0 && typed <= 255 {
			return string(byte(typed))
		}
	case uint:
		if typed <= 255 {
			return string(byte(typed))
		}
	case uint16:
		if typed <= 255 {
			return string(byte(typed))
		}
	case uint32:
		if typed <= 255 {
			return string(byte(typed))
		}
	case uint64:
		if typed <= 255 {
			return string(byte(typed))
		}
	}
	return pgTextValue(value)
}

func rowValuesByFieldName(t *testing.T, fields []pgconn.FieldDescription, values []any) map[string]any {
	t.Helper()
	require.Len(t, values, len(fields))
	row := make(map[string]any, len(fields))
	for idx, field := range fields {
		row[field.Name] = values[idx]
	}
	return row
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
