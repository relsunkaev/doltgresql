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
	"testing"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/doltgresql/server/replsource"
)

// TestLogicalReplicationRowFilterBooleanColumnRepro reproduces a logical
// replication data-consistency bug: PostgreSQL treats a bare boolean column as
// a valid publication row filter, so matching writes should commit and stream.
func TestLogicalReplicationRowFilterBooleanColumnRepro(t *testing.T) {
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
		CREATE TABLE row_filter_bool_items (
			tenant_id INT PRIMARY KEY,
			visible BOOL NOT NULL,
			label TEXT
		);`)
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, `
		CREATE PUBLICATION row_filter_bool_pub
		FOR TABLE row_filter_bool_items
		WHERE (visible);`)
	require.NoError(t, err)

	replConn := connectReplicationConn(t, ctx, port)
	defer replConn.Close(context.Background())

	slotName := "row_filter_bool_slot"
	_, err = pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication,
	})
	require.NoError(t, err)
	require.NoError(t, pglogrepl.StartReplication(ctx, replConn, slotName, 0, pglogrepl.StartReplicationOptions{
		Mode: pglogrepl.LogicalReplication,
		PluginArgs: []string{
			`"proto_version" '1'`,
			`"publication_names" 'row_filter_bool_pub'`,
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, `
		INSERT INTO row_filter_bool_items
		VALUES (1, TRUE, 'shown');`)
	require.NoError(t, err)

	relation, insert, _ := receiveInsertChange(t, replConn)
	require.Equal(t, "public", relation.Namespace)
	require.Equal(t, "row_filter_bool_items", relation.RelationName)
	require.Equal(t, uint16(3), relation.ColumnNum)
	require.Equal(t, "tenant_id", relation.Columns[0].Name)
	require.Equal(t, "visible", relation.Columns[1].Name)
	require.Equal(t, "label", relation.Columns[2].Name)
	require.Equal(t, relation.RelationID, insert.RelationID)
	require.Len(t, insert.Tuple.Columns, 3)
	require.Equal(t, "1", string(insert.Tuple.Columns[0].Data))
	require.Equal(t, "shown", string(insert.Tuple.Columns[2].Data))
}

// TestLogicalReplicationRowFilterIsFalseRepro reproduces a logical
// replication data-consistency bug: PostgreSQL supports IS FALSE in publication
// row filters, so matching writes should commit and stream.
func TestLogicalReplicationRowFilterIsFalseRepro(t *testing.T) {
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
		CREATE TABLE row_filter_is_false_items (
			tenant_id INT PRIMARY KEY,
			visible BOOL NOT NULL,
			label TEXT
		);`)
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, `
		CREATE PUBLICATION row_filter_is_false_pub
		FOR TABLE row_filter_is_false_items
		WHERE (visible IS FALSE);`)
	require.NoError(t, err)

	replConn := connectReplicationConn(t, ctx, port)
	defer replConn.Close(context.Background())

	slotName := "row_filter_is_false_slot"
	_, err = pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication,
	})
	require.NoError(t, err)
	require.NoError(t, pglogrepl.StartReplication(ctx, replConn, slotName, 0, pglogrepl.StartReplicationOptions{
		Mode: pglogrepl.LogicalReplication,
		PluginArgs: []string{
			`"proto_version" '1'`,
			`"publication_names" 'row_filter_is_false_pub'`,
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, `
		INSERT INTO row_filter_is_false_items
		VALUES (1, FALSE, 'shown');`)
	require.NoError(t, err)

	relation, insert, _ := receiveInsertChange(t, replConn)
	require.Equal(t, "public", relation.Namespace)
	require.Equal(t, "row_filter_is_false_items", relation.RelationName)
	require.Equal(t, relation.RelationID, insert.RelationID)
	require.Len(t, insert.Tuple.Columns, 3)
	require.Equal(t, "1", string(insert.Tuple.Columns[0].Data))
	require.Equal(t, "shown", string(insert.Tuple.Columns[2].Data))
}

// TestLogicalReplicationRowFilterIsDistinctFromRepro reproduces a logical
// replication data-consistency bug: PostgreSQL supports IS DISTINCT FROM in
// publication row filters, so matching writes should commit and stream.
func TestLogicalReplicationRowFilterIsDistinctFromRepro(t *testing.T) {
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
		CREATE TABLE row_filter_distinct_items (
			tenant_id INT PRIMARY KEY,
			customer_id INT NOT NULL,
			label TEXT
		);`)
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, `
		CREATE PUBLICATION row_filter_distinct_pub
		FOR TABLE row_filter_distinct_items
		WHERE (customer_id IS DISTINCT FROM 5);`)
	require.NoError(t, err)

	replConn := connectReplicationConn(t, ctx, port)
	defer replConn.Close(context.Background())

	slotName := "row_filter_distinct_slot"
	_, err = pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication,
	})
	require.NoError(t, err)
	require.NoError(t, pglogrepl.StartReplication(ctx, replConn, slotName, 0, pglogrepl.StartReplicationOptions{
		Mode: pglogrepl.LogicalReplication,
		PluginArgs: []string{
			`"proto_version" '1'`,
			`"publication_names" 'row_filter_distinct_pub'`,
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, `
		INSERT INTO row_filter_distinct_items
		VALUES (1, 7, 'shown');`)
	require.NoError(t, err)

	relation, insert, _ := receiveInsertChange(t, replConn)
	require.Equal(t, "public", relation.Namespace)
	require.Equal(t, "row_filter_distinct_items", relation.RelationName)
	require.Equal(t, relation.RelationID, insert.RelationID)
	require.Len(t, insert.Tuple.Columns, 3)
	require.Equal(t, "1", string(insert.Tuple.Columns[0].Data))
	require.Equal(t, "7", string(insert.Tuple.Columns[1].Data))
	require.Equal(t, "shown", string(insert.Tuple.Columns[2].Data))
}

// TestLogicalReplicationRowFilterCoalesceRepro reproduces a logical
// replication data-consistency bug: PostgreSQL supports immutable built-in
// expressions such as COALESCE in publication row filters.
func TestLogicalReplicationRowFilterCoalesceRepro(t *testing.T) {
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
		CREATE TABLE row_filter_coalesce_items (
			tenant_id INT PRIMARY KEY,
			label TEXT
		);`)
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, `
		CREATE PUBLICATION row_filter_coalesce_pub
		FOR TABLE row_filter_coalesce_items
		WHERE (COALESCE(label, 'fallback') = 'shown');`)
	require.NoError(t, err)

	replConn := connectReplicationConn(t, ctx, port)
	defer replConn.Close(context.Background())

	slotName := "row_filter_coalesce_slot"
	_, err = pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication,
	})
	require.NoError(t, err)
	require.NoError(t, pglogrepl.StartReplication(ctx, replConn, slotName, 0, pglogrepl.StartReplicationOptions{
		Mode: pglogrepl.LogicalReplication,
		PluginArgs: []string{
			`"proto_version" '1'`,
			`"publication_names" 'row_filter_coalesce_pub'`,
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, `
		INSERT INTO row_filter_coalesce_items
		VALUES (1, 'shown');`)
	require.NoError(t, err)

	relation, insert, _ := receiveInsertChange(t, replConn)
	require.Equal(t, "public", relation.Namespace)
	require.Equal(t, "row_filter_coalesce_items", relation.RelationName)
	require.Equal(t, relation.RelationID, insert.RelationID)
	require.Len(t, insert.Tuple.Columns, 2)
	require.Equal(t, "1", string(insert.Tuple.Columns[0].Data))
	require.Equal(t, "shown", string(insert.Tuple.Columns[1].Data))
}

// TestLogicalReplicationRowFilterCastRepro reproduces a logical replication
// data-consistency bug: PostgreSQL supports binary-compatible casts in
// publication row filters.
func TestLogicalReplicationRowFilterCastRepro(t *testing.T) {
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
		CREATE TABLE row_filter_cast_items (
			tenant_id INT PRIMARY KEY,
			code TEXT
		);`)
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, `
		CREATE PUBLICATION row_filter_cast_pub
		FOR TABLE row_filter_cast_items
		WHERE (code::varchar < 'm');`)
	require.NoError(t, err)

	replConn := connectReplicationConn(t, ctx, port)
	defer replConn.Close(context.Background())

	slotName := "row_filter_cast_slot"
	_, err = pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication,
	})
	require.NoError(t, err)
	require.NoError(t, pglogrepl.StartReplication(ctx, replConn, slotName, 0, pglogrepl.StartReplicationOptions{
		Mode: pglogrepl.LogicalReplication,
		PluginArgs: []string{
			`"proto_version" '1'`,
			`"publication_names" 'row_filter_cast_pub'`,
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, `
		INSERT INTO row_filter_cast_items
		VALUES (1, 'alpha');`)
	require.NoError(t, err)

	relation, insert, _ := receiveInsertChange(t, replConn)
	require.Equal(t, "public", relation.Namespace)
	require.Equal(t, "row_filter_cast_items", relation.RelationName)
	require.Equal(t, relation.RelationID, insert.RelationID)
	require.Len(t, insert.Tuple.Columns, 2)
	require.Equal(t, "1", string(insert.Tuple.Columns[0].Data))
	require.Equal(t, "alpha", string(insert.Tuple.Columns[1].Data))
}

// TestLogicalReplicationRowFilterNotInNullSuppressesRowsRepro reproduces a
// logical replication data-consistency bug: SQL NOT IN with a NULL list member
// evaluates to UNKNOWN for non-matching values, so the row must not publish.
func TestLogicalReplicationRowFilterNotInNullSuppressesRowsRepro(t *testing.T) {
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
		CREATE TABLE row_filter_not_in_null_items (
			tenant_id INT PRIMARY KEY,
			customer_id INT,
			label TEXT
		);`)
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, `
		CREATE PUBLICATION row_filter_not_in_null_pub
		FOR TABLE row_filter_not_in_null_items
		WHERE (customer_id NOT IN (1, NULL));`)
	require.NoError(t, err)

	replConn := connectReplicationConn(t, ctx, port)
	defer replConn.Close(context.Background())

	slotName := "row_filter_not_in_null_slot"
	_, err = pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication,
	})
	require.NoError(t, err)
	require.NoError(t, pglogrepl.StartReplication(ctx, replConn, slotName, 0, pglogrepl.StartReplicationOptions{
		Mode: pglogrepl.LogicalReplication,
		PluginArgs: []string{
			`"proto_version" '1'`,
			`"publication_names" 'row_filter_not_in_null_pub'`,
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, `
		INSERT INTO row_filter_not_in_null_items
		VALUES (1, 2, 'must-not-stream');`)
	require.NoError(t, err)

	requireNoReplicationCopyData(t, replConn, 250*time.Millisecond)
}

// TestLogicalReplicationRowFilterNotInInputNullSuppressesRowsRepro reproduces
// a logical replication data-consistency bug: NOT IN over a NULL input value
// evaluates to UNKNOWN, so the row must not publish.
func TestLogicalReplicationRowFilterNotInInputNullSuppressesRowsRepro(t *testing.T) {
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
		CREATE TABLE row_filter_not_in_input_null_items (
			tenant_id INT PRIMARY KEY,
			customer_id INT,
			label TEXT
		);`)
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, `
		CREATE PUBLICATION row_filter_not_in_input_null_pub
		FOR TABLE row_filter_not_in_input_null_items
		WHERE (customer_id NOT IN (1, 2));`)
	require.NoError(t, err)

	replConn := connectReplicationConn(t, ctx, port)
	defer replConn.Close(context.Background())

	slotName := "row_filter_not_in_input_null_slot"
	_, err = pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication,
	})
	require.NoError(t, err)
	require.NoError(t, pglogrepl.StartReplication(ctx, replConn, slotName, 0, pglogrepl.StartReplicationOptions{
		Mode: pglogrepl.LogicalReplication,
		PluginArgs: []string{
			`"proto_version" '1'`,
			`"publication_names" 'row_filter_not_in_input_null_pub'`,
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, `
		INSERT INTO row_filter_not_in_input_null_items
		VALUES (1, NULL, 'must-not-stream');`)
	require.NoError(t, err)

	requireNoReplicationCopyData(t, replConn, 250*time.Millisecond)
}

// TestLogicalReplicationRowFilterNotNullComparisonSuppressesRowsRepro
// reproduces a logical replication data-consistency bug: NOT over an UNKNOWN
// comparison remains UNKNOWN, so the row must not publish.
func TestLogicalReplicationRowFilterNotNullComparisonSuppressesRowsRepro(t *testing.T) {
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
		CREATE TABLE row_filter_not_null_comparison_items (
			tenant_id INT PRIMARY KEY,
			customer_id INT,
			label TEXT
		);`)
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, `
		CREATE PUBLICATION row_filter_not_null_comparison_pub
		FOR TABLE row_filter_not_null_comparison_items
		WHERE (NOT (customer_id = NULL));`)
	require.NoError(t, err)

	replConn := connectReplicationConn(t, ctx, port)
	defer replConn.Close(context.Background())

	slotName := "row_filter_not_null_comparison_slot"
	_, err = pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication,
	})
	require.NoError(t, err)
	require.NoError(t, pglogrepl.StartReplication(ctx, replConn, slotName, 0, pglogrepl.StartReplicationOptions{
		Mode: pglogrepl.LogicalReplication,
		PluginArgs: []string{
			`"proto_version" '1'`,
			`"publication_names" 'row_filter_not_null_comparison_pub'`,
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, `
		INSERT INTO row_filter_not_null_comparison_items
		VALUES (1, 2, 'must-not-stream');`)
	require.NoError(t, err)

	requireNoReplicationCopyData(t, replConn, 250*time.Millisecond)
}
