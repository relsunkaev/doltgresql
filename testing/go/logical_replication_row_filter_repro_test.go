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
	"strings"
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

// TestLogicalReplicationRowFilterIsTrueRepro reproduces a logical replication
// data-consistency bug: PostgreSQL supports IS TRUE in publication row filters,
// so matching writes should commit and stream.
func TestLogicalReplicationRowFilterIsTrueRepro(t *testing.T) {
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
		CREATE TABLE row_filter_is_true_items (
			tenant_id INT PRIMARY KEY,
			visible BOOL,
			label TEXT
		);`)
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, `
		CREATE PUBLICATION row_filter_is_true_pub
		FOR TABLE row_filter_is_true_items
		WHERE (visible IS TRUE);`)
	require.NoError(t, err)

	replConn := connectReplicationConn(t, ctx, port)
	defer replConn.Close(context.Background())

	slotName := "row_filter_is_true_slot"
	_, err = pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication,
	})
	require.NoError(t, err)
	require.NoError(t, pglogrepl.StartReplication(ctx, replConn, slotName, 0, pglogrepl.StartReplicationOptions{
		Mode: pglogrepl.LogicalReplication,
		PluginArgs: []string{
			`"proto_version" '1'`,
			`"publication_names" 'row_filter_is_true_pub'`,
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, `
		INSERT INTO row_filter_is_true_items
		VALUES (1, TRUE, 'shown');`)
	require.NoError(t, err)

	relation, insert, _ := receiveInsertChange(t, replConn)
	require.Equal(t, "public", relation.Namespace)
	require.Equal(t, "row_filter_is_true_items", relation.RelationName)
	require.Equal(t, relation.RelationID, insert.RelationID)
	require.Len(t, insert.Tuple.Columns, 3)
	require.Equal(t, "1", string(insert.Tuple.Columns[0].Data))
	require.Equal(t, "shown", string(insert.Tuple.Columns[2].Data))
}

// TestLogicalReplicationRowFilterIsNotTrueRepro reproduces a logical
// replication data-consistency bug: PostgreSQL supports IS NOT TRUE in
// publication row filters, so FALSE and NULL values can match deterministically.
func TestLogicalReplicationRowFilterIsNotTrueRepro(t *testing.T) {
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
		CREATE TABLE row_filter_is_not_true_items (
			tenant_id INT PRIMARY KEY,
			visible BOOL,
			label TEXT
		);`)
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, `
		CREATE PUBLICATION row_filter_is_not_true_pub
		FOR TABLE row_filter_is_not_true_items
		WHERE (visible IS NOT TRUE);`)
	require.NoError(t, err)

	replConn := connectReplicationConn(t, ctx, port)
	defer replConn.Close(context.Background())

	slotName := "row_filter_is_not_true_slot"
	_, err = pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication,
	})
	require.NoError(t, err)
	require.NoError(t, pglogrepl.StartReplication(ctx, replConn, slotName, 0, pglogrepl.StartReplicationOptions{
		Mode: pglogrepl.LogicalReplication,
		PluginArgs: []string{
			`"proto_version" '1'`,
			`"publication_names" 'row_filter_is_not_true_pub'`,
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, `
		INSERT INTO row_filter_is_not_true_items
		VALUES (1, FALSE, 'shown');`)
	require.NoError(t, err)

	relation, insert, _ := receiveInsertChange(t, replConn)
	require.Equal(t, "public", relation.Namespace)
	require.Equal(t, "row_filter_is_not_true_items", relation.RelationName)
	require.Equal(t, relation.RelationID, insert.RelationID)
	require.Len(t, insert.Tuple.Columns, 3)
	require.Equal(t, "1", string(insert.Tuple.Columns[0].Data))
	require.Equal(t, "shown", string(insert.Tuple.Columns[2].Data))
}

// TestLogicalReplicationRowFilterIsNotFalseRepro reproduces a logical
// replication data-consistency bug: PostgreSQL supports IS NOT FALSE in
// publication row filters.
func TestLogicalReplicationRowFilterIsNotFalseRepro(t *testing.T) {
	runLogicalReplicationBooleanTruthRowFilterRepro(t,
		"row_filter_is_not_false_items",
		"row_filter_is_not_false_pub",
		"row_filter_is_not_false_slot",
		"visible IS NOT FALSE",
		"TRUE",
	)
}

// TestLogicalReplicationRowFilterIsUnknownRepro reproduces a logical
// replication data-consistency bug: PostgreSQL supports IS UNKNOWN in
// publication row filters.
func TestLogicalReplicationRowFilterIsUnknownRepro(t *testing.T) {
	runLogicalReplicationBooleanTruthRowFilterRepro(t,
		"row_filter_is_unknown_items",
		"row_filter_is_unknown_pub",
		"row_filter_is_unknown_slot",
		"visible IS UNKNOWN",
		"NULL",
	)
}

// TestLogicalReplicationRowFilterIsNotUnknownRepro reproduces a logical
// replication data-consistency bug: PostgreSQL supports IS NOT UNKNOWN in
// publication row filters.
func TestLogicalReplicationRowFilterIsNotUnknownRepro(t *testing.T) {
	runLogicalReplicationBooleanTruthRowFilterRepro(t,
		"row_filter_is_not_unknown_items",
		"row_filter_is_not_unknown_pub",
		"row_filter_is_not_unknown_slot",
		"visible IS NOT UNKNOWN",
		"TRUE",
	)
}

// TestLogicalReplicationRowFilterBooleanTrueEqualityRepro reproduces a logical
// replication data-consistency bug: PostgreSQL supports boolean equality in
// publication row filters.
func TestLogicalReplicationRowFilterBooleanTrueEqualityRepro(t *testing.T) {
	runLogicalReplicationBooleanEqualityRowFilterRepro(t,
		"row_filter_bool_eq_true_items",
		"row_filter_bool_eq_true_pub",
		"row_filter_bool_eq_true_slot",
		"visible = true",
		"TRUE",
	)
}

// TestLogicalReplicationRowFilterBooleanFalseEqualityRepro reproduces a
// logical replication data-consistency bug: PostgreSQL supports boolean
// equality in publication row filters.
func TestLogicalReplicationRowFilterBooleanFalseEqualityRepro(t *testing.T) {
	runLogicalReplicationBooleanEqualityRowFilterRepro(t,
		"row_filter_bool_eq_false_items",
		"row_filter_bool_eq_false_pub",
		"row_filter_bool_eq_false_slot",
		"visible = false",
		"FALSE",
	)
}

// TestLogicalReplicationRowFilterBooleanStringLiteralRepro reproduces a logical
// replication data-consistency bug: PostgreSQL coerces unknown string literals
// to boolean when comparing them with boolean columns in row filters.
func TestLogicalReplicationRowFilterBooleanStringLiteralRepro(t *testing.T) {
	runLogicalReplicationBooleanEqualityRowFilterRepro(t,
		"row_filter_bool_string_items",
		"row_filter_bool_string_pub",
		"row_filter_bool_string_slot",
		"visible = 'true'",
		"TRUE",
	)
}

// TestLogicalReplicationRowFilterBooleanStringInequalitySuppressesRowsRepro
// reproduces a logical replication data-consistency bug: PostgreSQL coerces
// unknown string literals to boolean when comparing them with boolean columns
// in row filters, so TRUE does not satisfy visible <> 'true'.
func TestLogicalReplicationRowFilterBooleanStringInequalitySuppressesRowsRepro(t *testing.T) {
	runLogicalReplicationBooleanSuppressingRowFilterRepro(t,
		"row_filter_bool_string_ne_items",
		"row_filter_bool_string_ne_pub",
		"row_filter_bool_string_ne_slot",
		"visible <> 'true'",
		"TRUE",
	)
}

func runLogicalReplicationBooleanTruthRowFilterRepro(t *testing.T, tableName string, publicationName string, slotName string, rowFilter string, visibleValue string) {
	t.Helper()
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	_, err = conn.Current.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			tenant_id INT PRIMARY KEY,
			visible BOOL,
			label TEXT
		);`, tableName))
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, fmt.Sprintf(`
		CREATE PUBLICATION %s
		FOR TABLE %s
		WHERE (%s);`, publicationName, tableName, rowFilter))
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
			fmt.Sprintf(`"publication_names" '%s'`, publicationName),
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s
		VALUES (1, %s, 'shown');`, tableName, visibleValue))
	require.NoError(t, err)

	relation, insert, _ := receiveInsertChange(t, replConn)
	require.Equal(t, "public", relation.Namespace)
	require.Equal(t, tableName, relation.RelationName)
	require.Equal(t, relation.RelationID, insert.RelationID)
	require.Len(t, insert.Tuple.Columns, 3)
	require.Equal(t, "1", string(insert.Tuple.Columns[0].Data))
	require.Equal(t, "shown", string(insert.Tuple.Columns[2].Data))
}

func runLogicalReplicationBooleanEqualityRowFilterRepro(t *testing.T, tableName string, publicationName string, slotName string, rowFilter string, visibleValue string) {
	t.Helper()
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	_, err = conn.Current.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			tenant_id INT PRIMARY KEY,
			visible BOOL,
			label TEXT
		);`, tableName))
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, fmt.Sprintf(`
		CREATE PUBLICATION %s
		FOR TABLE %s
		WHERE (%s);`, publicationName, tableName, rowFilter))
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
			fmt.Sprintf(`"publication_names" '%s'`, publicationName),
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s
		VALUES (1, %s, 'shown');`, tableName, visibleValue))
	require.NoError(t, err)

	relation, insert, _ := receiveInsertChange(t, replConn)
	require.Equal(t, "public", relation.Namespace)
	require.Equal(t, tableName, relation.RelationName)
	require.Equal(t, relation.RelationID, insert.RelationID)
	require.Len(t, insert.Tuple.Columns, 3)
	require.Equal(t, "1", string(insert.Tuple.Columns[0].Data))
	require.Equal(t, "shown", string(insert.Tuple.Columns[2].Data))
}

func runLogicalReplicationBooleanSuppressingRowFilterRepro(t *testing.T, tableName string, publicationName string, slotName string, rowFilter string, visibleValue string) {
	t.Helper()
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	_, err = conn.Current.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			tenant_id INT PRIMARY KEY,
			visible BOOL,
			label TEXT
		);`, tableName))
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, fmt.Sprintf(`
		CREATE PUBLICATION %s
		FOR TABLE %s
		WHERE (%s);`, publicationName, tableName, rowFilter))
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
			fmt.Sprintf(`"publication_names" '%s'`, publicationName),
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s
		VALUES (1, %s, 'must-not-stream');`, tableName, visibleValue))
	require.NoError(t, err)

	requireNoReplicationCopyData(t, replConn, 250*time.Millisecond)
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

// TestLogicalReplicationRowFilterLikeRepro reproduces a logical replication
// data-consistency bug: PostgreSQL supports immutable LIKE predicates in
// publication row filters.
func TestLogicalReplicationRowFilterLikeRepro(t *testing.T) {
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
		CREATE TABLE row_filter_like_items (
			tenant_id INT PRIMARY KEY,
			label TEXT
		);`)
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, `
		CREATE PUBLICATION row_filter_like_pub
		FOR TABLE row_filter_like_items
		WHERE (label LIKE 'show%');`)
	require.NoError(t, err)

	replConn := connectReplicationConn(t, ctx, port)
	defer replConn.Close(context.Background())

	slotName := "row_filter_like_slot"
	_, err = pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication,
	})
	require.NoError(t, err)
	require.NoError(t, pglogrepl.StartReplication(ctx, replConn, slotName, 0, pglogrepl.StartReplicationOptions{
		Mode: pglogrepl.LogicalReplication,
		PluginArgs: []string{
			`"proto_version" '1'`,
			`"publication_names" 'row_filter_like_pub'`,
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, `
		INSERT INTO row_filter_like_items
		VALUES (1, 'shown');`)
	require.NoError(t, err)

	relation, insert, _ := receiveInsertChange(t, replConn)
	require.Equal(t, "public", relation.Namespace)
	require.Equal(t, "row_filter_like_items", relation.RelationName)
	require.Equal(t, relation.RelationID, insert.RelationID)
	require.Len(t, insert.Tuple.Columns, 2)
	require.Equal(t, "1", string(insert.Tuple.Columns[0].Data))
	require.Equal(t, "shown", string(insert.Tuple.Columns[1].Data))
}

// TestLogicalReplicationRowFilterBetweenRepro reproduces a logical replication
// data-consistency bug: PostgreSQL supports BETWEEN predicates in publication
// row filters.
func TestLogicalReplicationRowFilterBetweenRepro(t *testing.T) {
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
		CREATE TABLE row_filter_between_items (
			tenant_id INT PRIMARY KEY,
			score INT,
			label TEXT
		);`)
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, `
		CREATE PUBLICATION row_filter_between_pub
		FOR TABLE row_filter_between_items
		WHERE (score BETWEEN 10 AND 20);`)
	require.NoError(t, err)

	replConn := connectReplicationConn(t, ctx, port)
	defer replConn.Close(context.Background())

	slotName := "row_filter_between_slot"
	_, err = pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication,
	})
	require.NoError(t, err)
	require.NoError(t, pglogrepl.StartReplication(ctx, replConn, slotName, 0, pglogrepl.StartReplicationOptions{
		Mode: pglogrepl.LogicalReplication,
		PluginArgs: []string{
			`"proto_version" '1'`,
			`"publication_names" 'row_filter_between_pub'`,
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, `
		INSERT INTO row_filter_between_items
		VALUES (1, 15, 'shown');`)
	require.NoError(t, err)

	relation, insert, _ := receiveInsertChange(t, replConn)
	require.Equal(t, "public", relation.Namespace)
	require.Equal(t, "row_filter_between_items", relation.RelationName)
	require.Equal(t, relation.RelationID, insert.RelationID)
	require.Len(t, insert.Tuple.Columns, 3)
	require.Equal(t, "1", string(insert.Tuple.Columns[0].Data))
	require.Equal(t, "15", string(insert.Tuple.Columns[1].Data))
	require.Equal(t, "shown", string(insert.Tuple.Columns[2].Data))
}

// TestLogicalReplicationRowFilterArithmeticRepro reproduces a logical
// replication data-consistency bug: PostgreSQL supports immutable arithmetic
// expressions in publication row filters.
func TestLogicalReplicationRowFilterArithmeticRepro(t *testing.T) {
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
		CREATE TABLE row_filter_arithmetic_items (
			tenant_id INT PRIMARY KEY,
			score INT,
			label TEXT
		);`)
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, `
		CREATE PUBLICATION row_filter_arithmetic_pub
		FOR TABLE row_filter_arithmetic_items
		WHERE (score + 1 = 2);`)
	require.NoError(t, err)

	replConn := connectReplicationConn(t, ctx, port)
	defer replConn.Close(context.Background())

	slotName := "row_filter_arithmetic_slot"
	_, err = pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication,
	})
	require.NoError(t, err)
	require.NoError(t, pglogrepl.StartReplication(ctx, replConn, slotName, 0, pglogrepl.StartReplicationOptions{
		Mode: pglogrepl.LogicalReplication,
		PluginArgs: []string{
			`"proto_version" '1'`,
			`"publication_names" 'row_filter_arithmetic_pub'`,
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, `
		INSERT INTO row_filter_arithmetic_items
		VALUES (1, 1, 'shown');`)
	require.NoError(t, err)

	relation, insert, _ := receiveInsertChange(t, replConn)
	require.Equal(t, "public", relation.Namespace)
	require.Equal(t, "row_filter_arithmetic_items", relation.RelationName)
	require.Equal(t, relation.RelationID, insert.RelationID)
	require.Len(t, insert.Tuple.Columns, 3)
	require.Equal(t, "1", string(insert.Tuple.Columns[0].Data))
	require.Equal(t, "1", string(insert.Tuple.Columns[1].Data))
	require.Equal(t, "shown", string(insert.Tuple.Columns[2].Data))
}

// TestLogicalReplicationRowFilterBpcharPaddingRepro reproduces a logical
// replication data-consistency bug: PostgreSQL compares CHAR(n) values using
// bpchar semantics, so trailing padding spaces are insignificant.
func TestLogicalReplicationRowFilterBpcharPaddingRepro(t *testing.T) {
	runLogicalReplicationBpcharFilterRepro(t,
		"row_filter_bpchar_items",
		"row_filter_bpchar_pub",
		"row_filter_bpchar_slot",
		"code = 'a'",
		true,
	)
}

// TestLogicalReplicationRowFilterBpcharInequalitySuppressesRowsRepro
// reproduces a logical replication data-consistency bug: PostgreSQL compares
// CHAR(n) values with trailing-padding-insensitive bpchar semantics.
func TestLogicalReplicationRowFilterBpcharInequalitySuppressesRowsRepro(t *testing.T) {
	runLogicalReplicationBpcharFilterRepro(t,
		"row_filter_bpchar_neq_items",
		"row_filter_bpchar_neq_pub",
		"row_filter_bpchar_neq_slot",
		"code <> 'a'",
		false,
	)
}

func runLogicalReplicationBpcharFilterRepro(t *testing.T, tableName string, publicationName string, slotName string, rowFilter string, expectStream bool) {
	t.Helper()
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	_, err = conn.Current.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			tenant_id INT PRIMARY KEY,
			code CHAR(3),
			label TEXT
		);`, tableName))
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, fmt.Sprintf(`
		CREATE PUBLICATION %s
		FOR TABLE %s
		WHERE (%s);`, publicationName, tableName, rowFilter))
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
			fmt.Sprintf(`"publication_names" '%s'`, publicationName),
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s
		VALUES (1, 'a', 'shown');`, tableName))
	require.NoError(t, err)

	if !expectStream {
		requireNoReplicationCopyData(t, replConn, 250*time.Millisecond)
		return
	}
	relation, insert, _ := receiveInsertChange(t, replConn)
	require.Equal(t, "public", relation.Namespace)
	require.Equal(t, tableName, relation.RelationName)
	require.Equal(t, relation.RelationID, insert.RelationID)
	require.Len(t, insert.Tuple.Columns, 3)
	require.Equal(t, "1", string(insert.Tuple.Columns[0].Data))
	require.Equal(t, "a  ", string(insert.Tuple.Columns[1].Data))
	require.Equal(t, "shown", string(insert.Tuple.Columns[2].Data))
}

// TestLogicalReplicationRowFilterTextNumericEqualitySuppressesRowsRepro
// reproduces a logical replication data-consistency bug: PostgreSQL evaluates
// text equality as text equality, so '1' does not match the literal '1.0'.
func TestLogicalReplicationRowFilterTextNumericEqualitySuppressesRowsRepro(t *testing.T) {
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
		CREATE TABLE row_filter_text_numeric_items (
			tenant_id INT PRIMARY KEY,
			label TEXT
		);`)
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, `
		CREATE PUBLICATION row_filter_text_numeric_pub
		FOR TABLE row_filter_text_numeric_items
		WHERE (label = '1.0');`)
	require.NoError(t, err)

	replConn := connectReplicationConn(t, ctx, port)
	defer replConn.Close(context.Background())

	slotName := "row_filter_text_numeric_slot"
	_, err = pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication,
	})
	require.NoError(t, err)
	require.NoError(t, pglogrepl.StartReplication(ctx, replConn, slotName, 0, pglogrepl.StartReplicationOptions{
		Mode: pglogrepl.LogicalReplication,
		PluginArgs: []string{
			`"proto_version" '1'`,
			`"publication_names" 'row_filter_text_numeric_pub'`,
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, `
		INSERT INTO row_filter_text_numeric_items
		VALUES (1, '1');`)
	require.NoError(t, err)

	requireNoReplicationCopyData(t, replConn, 250*time.Millisecond)
}

// TestLogicalReplicationRowFilterTextNumericOrderingRepro reproduces a logical
// replication data-consistency bug: PostgreSQL orders TEXT lexically, so '10'
// is less than the literal '2' under the default PostgreSQL test collation.
func TestLogicalReplicationRowFilterTextNumericOrderingRepro(t *testing.T) {
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
		CREATE TABLE row_filter_text_numeric_order_items (
			tenant_id INT PRIMARY KEY,
			label TEXT
		);`)
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, `
		CREATE PUBLICATION row_filter_text_numeric_order_pub
		FOR TABLE row_filter_text_numeric_order_items
		WHERE (label < '2');`)
	require.NoError(t, err)

	replConn := connectReplicationConn(t, ctx, port)
	defer replConn.Close(context.Background())

	slotName := "row_filter_text_numeric_order_slot"
	_, err = pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication,
	})
	require.NoError(t, err)
	require.NoError(t, pglogrepl.StartReplication(ctx, replConn, slotName, 0, pglogrepl.StartReplicationOptions{
		Mode: pglogrepl.LogicalReplication,
		PluginArgs: []string{
			`"proto_version" '1'`,
			`"publication_names" 'row_filter_text_numeric_order_pub'`,
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, `
		INSERT INTO row_filter_text_numeric_order_items
		VALUES (1, '10');`)
	require.NoError(t, err)

	relation, insert, _ := receiveInsertChange(t, replConn)
	require.Equal(t, "public", relation.Namespace)
	require.Equal(t, "row_filter_text_numeric_order_items", relation.RelationName)
	require.Equal(t, relation.RelationID, insert.RelationID)
	require.Len(t, insert.Tuple.Columns, 2)
	require.Equal(t, "1", string(insert.Tuple.Columns[0].Data))
	require.Equal(t, "10", string(insert.Tuple.Columns[1].Data))
}

// TestLogicalReplicationRowFilterTextNumericInequalityRepro reproduces a
// logical replication data-consistency bug: PostgreSQL evaluates TEXT
// inequality as text inequality, so '1' is distinct from the literal '1.0'.
func TestLogicalReplicationRowFilterTextNumericInequalityRepro(t *testing.T) {
	runLogicalReplicationTextNumericFilterRepro(t,
		"row_filter_text_numeric_neq_items",
		"row_filter_text_numeric_neq_pub",
		"row_filter_text_numeric_neq_slot",
		"label <> '1.0'",
		"1",
		true,
	)
}

// TestLogicalReplicationRowFilterTextNumericInSuppressesRowsRepro reproduces a
// logical replication data-consistency bug: PostgreSQL evaluates TEXT IN lists
// with text equality, so '1' does not match the literal '1.0'.
func TestLogicalReplicationRowFilterTextNumericInSuppressesRowsRepro(t *testing.T) {
	runLogicalReplicationTextNumericFilterRepro(t,
		"row_filter_text_numeric_in_items",
		"row_filter_text_numeric_in_pub",
		"row_filter_text_numeric_in_slot",
		"label IN ('1.0')",
		"1",
		false,
	)
}

// TestLogicalReplicationRowFilterTextNumericNotInRepro reproduces a logical
// replication data-consistency bug: PostgreSQL evaluates TEXT NOT IN lists with
// text equality, so '1' is not in the singleton list containing '1.0'.
func TestLogicalReplicationRowFilterTextNumericNotInRepro(t *testing.T) {
	runLogicalReplicationTextNumericFilterRepro(t,
		"row_filter_text_numeric_not_in_items",
		"row_filter_text_numeric_not_in_pub",
		"row_filter_text_numeric_not_in_slot",
		"label NOT IN ('1.0')",
		"1",
		true,
	)
}

// TestLogicalReplicationRowFilterTextNumericGreaterThanSuppressesRowsRepro
// reproduces a logical replication data-consistency bug: PostgreSQL orders TEXT
// lexically, so '10' is not greater than the literal '2' under the baseline
// PostgreSQL collation.
func TestLogicalReplicationRowFilterTextNumericGreaterThanSuppressesRowsRepro(t *testing.T) {
	runLogicalReplicationTextNumericFilterRepro(t,
		"row_filter_text_numeric_gt_items",
		"row_filter_text_numeric_gt_pub",
		"row_filter_text_numeric_gt_slot",
		"label > '2'",
		"10",
		false,
	)
}

// TestLogicalReplicationRowFilterEscapedTextLiteralRepro reproduces a logical
// replication data-consistency bug: PostgreSQL supports escaped string
// literals in publication row filters.
func TestLogicalReplicationRowFilterEscapedTextLiteralRepro(t *testing.T) {
	runLogicalReplicationTextNumericFilterRepro(t,
		"row_filter_text_escaped_items",
		"row_filter_text_escaped_pub",
		"row_filter_text_escaped_slot",
		"label = 'can''t'",
		"can't",
		true,
	)
}

// TestLogicalReplicationRowFilterByteaTextLiteralRepro reproduces a logical
// replication data-consistency bug: PostgreSQL coerces string literals to typed
// BYTEA values before comparing them with BYTEA columns in row filters.
func TestLogicalReplicationRowFilterByteaTextLiteralRepro(t *testing.T) {
	runLogicalReplicationByteaTextLiteralRepro(t,
		"row_filter_bytea_items",
		"row_filter_bytea_pub",
		"row_filter_bytea_slot",
		`payload = 'abc'`,
		true,
	)
}

// TestLogicalReplicationRowFilterByteaInequalitySuppressesRowsRepro reproduces
// a logical replication data-consistency bug: BYTEA inequality should compare
// typed bytes, not the source spelling of the row filter literal.
func TestLogicalReplicationRowFilterByteaInequalitySuppressesRowsRepro(t *testing.T) {
	runLogicalReplicationByteaTextLiteralRepro(t,
		"row_filter_bytea_neq_items",
		"row_filter_bytea_neq_pub",
		"row_filter_bytea_neq_slot",
		`payload <> 'abc'`,
		false,
	)
}

// TestLogicalReplicationRowFilterJsonbObjectCanonicalizationRepro reproduces a
// logical replication data-consistency bug: PostgreSQL compares typed JSONB
// values, so object key order in the row filter literal is insignificant.
func TestLogicalReplicationRowFilterJsonbObjectCanonicalizationRepro(t *testing.T) {
	runLogicalReplicationJsonbCanonicalizationRepro(t,
		"row_filter_jsonb_items",
		"row_filter_jsonb_pub",
		"row_filter_jsonb_slot",
		`doc = '{"b":1,"a":2}'`,
		`{"a":2,"b":1}`,
		`{"a": 2, "b": 1}`,
		true,
	)
}

// TestLogicalReplicationRowFilterJsonbInequalitySuppressesRowsRepro reproduces
// a logical replication data-consistency bug: JSONB inequality should compare
// typed JSONB values, not serialized JSON object text.
func TestLogicalReplicationRowFilterJsonbInequalitySuppressesRowsRepro(t *testing.T) {
	runLogicalReplicationJsonbCanonicalizationRepro(t,
		"row_filter_jsonb_neq_items",
		"row_filter_jsonb_neq_pub",
		"row_filter_jsonb_neq_slot",
		`doc <> '{"b":1,"a":2}'`,
		`{"a":2,"b":1}`,
		`{"a": 2, "b": 1}`,
		false,
	)
}

// TestLogicalReplicationRowFilterJsonbNumericCanonicalizationRepro reproduces a
// logical replication data-consistency bug: PostgreSQL compares JSONB numeric
// values semantically, so 1 and 1.0 are equal in row filters.
func TestLogicalReplicationRowFilterJsonbNumericCanonicalizationRepro(t *testing.T) {
	runLogicalReplicationJsonbCanonicalizationRepro(t,
		"row_filter_jsonb_numeric_items",
		"row_filter_jsonb_numeric_pub",
		"row_filter_jsonb_numeric_slot",
		`doc = '{"n":1.0}'`,
		`{"n":1}`,
		`{"n": 1}`,
		true,
	)
}

// TestLogicalReplicationRowFilterJsonbNumericInequalitySuppressesRowsRepro
// reproduces a logical replication data-consistency bug: JSONB inequality should
// compare semantic JSONB numeric values, not serialized numeric spelling.
func TestLogicalReplicationRowFilterJsonbNumericInequalitySuppressesRowsRepro(t *testing.T) {
	runLogicalReplicationJsonbCanonicalizationRepro(t,
		"row_filter_jsonb_numeric_neq_items",
		"row_filter_jsonb_numeric_neq_pub",
		"row_filter_jsonb_numeric_neq_slot",
		`doc <> '{"n":1.0}'`,
		`{"n":1}`,
		`{"n": 1}`,
		false,
	)
}

// TestLogicalReplicationRowFilterUuidLiteralCaseRepro reproduces a logical
// replication data-consistency bug: PostgreSQL coerces unknown string literals
// to UUID when comparing them with UUID columns in row filters.
func TestLogicalReplicationRowFilterUuidLiteralCaseRepro(t *testing.T) {
	runLogicalReplicationUuidLiteralCaseRepro(t,
		"row_filter_uuid_items",
		"row_filter_uuid_pub",
		"row_filter_uuid_slot",
		"external_id = 'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11'",
		true,
	)
}

// TestLogicalReplicationRowFilterUuidInequalitySuppressesRowsRepro reproduces
// a logical replication data-consistency bug: UUID comparisons should compare
// typed UUID values, not the literal source spelling.
func TestLogicalReplicationRowFilterUuidInequalitySuppressesRowsRepro(t *testing.T) {
	runLogicalReplicationUuidLiteralCaseRepro(t,
		"row_filter_uuid_neq_items",
		"row_filter_uuid_neq_pub",
		"row_filter_uuid_neq_slot",
		"external_id <> 'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11'",
		false,
	)
}

// TestLogicalReplicationRowFilterDateLiteralCanonicalizationRepro reproduces a
// logical replication data-consistency bug: PostgreSQL coerces date string
// literals before comparing them with DATE columns in row filters.
func TestLogicalReplicationRowFilterDateLiteralCanonicalizationRepro(t *testing.T) {
	runLogicalReplicationDateLiteralCanonicalizationRepro(t,
		"row_filter_date_items",
		"row_filter_date_pub",
		"row_filter_date_slot",
		"event_date = '2026-5-1'",
		true,
	)
}

// TestLogicalReplicationRowFilterDateInequalitySuppressesRowsRepro reproduces
// a logical replication data-consistency bug: DATE inequality should compare
// typed date values, not the literal source spelling.
func TestLogicalReplicationRowFilterDateInequalitySuppressesRowsRepro(t *testing.T) {
	runLogicalReplicationDateLiteralCanonicalizationRepro(t,
		"row_filter_date_neq_items",
		"row_filter_date_neq_pub",
		"row_filter_date_neq_slot",
		"event_date <> '2026-5-1'",
		false,
	)
}

// TestLogicalReplicationRowFilterTimestampLiteralCanonicalizationRepro
// reproduces a logical replication data-consistency bug: PostgreSQL coerces
// timestamp string literals before comparing them with TIMESTAMP columns.
func TestLogicalReplicationRowFilterTimestampLiteralCanonicalizationRepro(t *testing.T) {
	runLogicalReplicationTimestampLiteralCanonicalizationRepro(t,
		"row_filter_timestamp_items",
		"row_filter_timestamp_pub",
		"row_filter_timestamp_slot",
		"event_ts = '2026-5-1 1:2:3'",
		true,
	)
}

// TestLogicalReplicationRowFilterTimestampInequalitySuppressesRowsRepro
// reproduces a logical replication data-consistency bug: TIMESTAMP inequality
// should compare typed timestamp values, not the literal source spelling.
func TestLogicalReplicationRowFilterTimestampInequalitySuppressesRowsRepro(t *testing.T) {
	runLogicalReplicationTimestampLiteralCanonicalizationRepro(t,
		"row_filter_timestamp_neq_items",
		"row_filter_timestamp_neq_pub",
		"row_filter_timestamp_neq_slot",
		"event_ts <> '2026-5-1 1:2:3'",
		false,
	)
}

// TestLogicalReplicationRowFilterTimestamptzLiteralCanonicalizationRepro
// reproduces a logical replication data-consistency bug: PostgreSQL compares
// timestamptz instants, independent of the row filter literal's source zone.
func TestLogicalReplicationRowFilterTimestamptzLiteralCanonicalizationRepro(t *testing.T) {
	runLogicalReplicationTimestamptzLiteralCanonicalizationRepro(t,
		"row_filter_timestamptz_items",
		"row_filter_timestamptz_pub",
		"row_filter_timestamptz_slot",
		"event_tz = '2026-05-01 01:02:03+00'",
		true,
	)
}

// TestLogicalReplicationRowFilterTimestamptzInequalitySuppressesRowsRepro
// reproduces a logical replication data-consistency bug: TIMESTAMPTZ inequality
// should compare typed instants, not session-rendered timestamp text.
func TestLogicalReplicationRowFilterTimestamptzInequalitySuppressesRowsRepro(t *testing.T) {
	runLogicalReplicationTimestamptzLiteralCanonicalizationRepro(t,
		"row_filter_timestamptz_neq_items",
		"row_filter_timestamptz_neq_pub",
		"row_filter_timestamptz_neq_slot",
		"event_tz <> '2026-05-01 01:02:03+00'",
		false,
	)
}

// TestLogicalReplicationRowFilterTimeLiteralCanonicalizationRepro reproduces a
// logical replication data-consistency bug: PostgreSQL coerces time string
// literals before comparing them with TIME columns in row filters.
func TestLogicalReplicationRowFilterTimeLiteralCanonicalizationRepro(t *testing.T) {
	runLogicalReplicationTimeLiteralCanonicalizationRepro(t,
		"row_filter_time_items",
		"row_filter_time_pub",
		"row_filter_time_slot",
		"event_time = '1:2:3'",
		true,
	)
}

// TestLogicalReplicationRowFilterTimeInequalitySuppressesRowsRepro reproduces
// a logical replication data-consistency bug: TIME inequality should compare
// typed time values, not the literal source spelling.
func TestLogicalReplicationRowFilterTimeInequalitySuppressesRowsRepro(t *testing.T) {
	runLogicalReplicationTimeLiteralCanonicalizationRepro(t,
		"row_filter_time_neq_items",
		"row_filter_time_neq_pub",
		"row_filter_time_neq_slot",
		"event_time <> '1:2:3'",
		false,
	)
}

// TestLogicalReplicationRowFilterIntervalLiteralCanonicalizationRepro reproduces
// a logical replication data-consistency bug: PostgreSQL coerces interval string
// literals before comparing them with INTERVAL columns in row filters.
func TestLogicalReplicationRowFilterIntervalLiteralCanonicalizationRepro(t *testing.T) {
	runLogicalReplicationIntervalLiteralCanonicalizationRepro(t,
		"row_filter_interval_items",
		"row_filter_interval_pub",
		"row_filter_interval_slot",
		"span = '1 day 2 hours'",
		true,
	)
}

// TestLogicalReplicationRowFilterIntervalInequalitySuppressesRowsRepro
// reproduces a logical replication data-consistency bug: INTERVAL inequality
// should compare typed interval values, not the literal source spelling.
func TestLogicalReplicationRowFilterIntervalInequalitySuppressesRowsRepro(t *testing.T) {
	runLogicalReplicationIntervalLiteralCanonicalizationRepro(t,
		"row_filter_interval_neq_items",
		"row_filter_interval_neq_pub",
		"row_filter_interval_neq_slot",
		"span <> '1 day 2 hours'",
		false,
	)
}

func runLogicalReplicationDateLiteralCanonicalizationRepro(t *testing.T, tableName string, publicationName string, slotName string, rowFilter string, expectStream bool) {
	t.Helper()
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	_, err = conn.Current.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			tenant_id INT PRIMARY KEY,
			event_date DATE,
			label TEXT
		);`, tableName))
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, fmt.Sprintf(`
		CREATE PUBLICATION %s
		FOR TABLE %s
		WHERE (%s);`, publicationName, tableName, rowFilter))
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
			fmt.Sprintf(`"publication_names" '%s'`, publicationName),
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s
		VALUES (1, '2026-05-01', 'shown');`, tableName))
	require.NoError(t, err)

	if !expectStream {
		requireNoReplicationCopyData(t, replConn, 250*time.Millisecond)
		return
	}
	relation, insert, _ := receiveInsertChange(t, replConn)
	require.Equal(t, "public", relation.Namespace)
	require.Equal(t, tableName, relation.RelationName)
	require.Equal(t, relation.RelationID, insert.RelationID)
	require.Len(t, insert.Tuple.Columns, 3)
	require.Equal(t, "1", string(insert.Tuple.Columns[0].Data))
	require.Equal(t, "2026-05-01", string(insert.Tuple.Columns[1].Data))
	require.Equal(t, "shown", string(insert.Tuple.Columns[2].Data))
}

func runLogicalReplicationTimeLiteralCanonicalizationRepro(t *testing.T, tableName string, publicationName string, slotName string, rowFilter string, expectStream bool) {
	t.Helper()
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	_, err = conn.Current.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			tenant_id INT PRIMARY KEY,
			event_time TIME,
			label TEXT
		);`, tableName))
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, fmt.Sprintf(`
		CREATE PUBLICATION %s
		FOR TABLE %s
		WHERE (%s);`, publicationName, tableName, rowFilter))
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
			fmt.Sprintf(`"publication_names" '%s'`, publicationName),
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s
		VALUES (1, '01:02:03', 'shown');`, tableName))
	require.NoError(t, err)

	if !expectStream {
		requireNoReplicationCopyData(t, replConn, 250*time.Millisecond)
		return
	}
	relation, insert, _ := receiveInsertChange(t, replConn)
	require.Equal(t, "public", relation.Namespace)
	require.Equal(t, tableName, relation.RelationName)
	require.Equal(t, relation.RelationID, insert.RelationID)
	require.Len(t, insert.Tuple.Columns, 3)
	require.Equal(t, "1", string(insert.Tuple.Columns[0].Data))
	require.Equal(t, "01:02:03", string(insert.Tuple.Columns[1].Data))
	require.Equal(t, "shown", string(insert.Tuple.Columns[2].Data))
}

func runLogicalReplicationIntervalLiteralCanonicalizationRepro(t *testing.T, tableName string, publicationName string, slotName string, rowFilter string, expectStream bool) {
	t.Helper()
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	_, err = conn.Current.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			tenant_id INT PRIMARY KEY,
			span INTERVAL,
			label TEXT
		);`, tableName))
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, fmt.Sprintf(`
		CREATE PUBLICATION %s
		FOR TABLE %s
		WHERE (%s);`, publicationName, tableName, rowFilter))
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
			fmt.Sprintf(`"publication_names" '%s'`, publicationName),
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s
		VALUES (1, '26 hours', 'shown');`, tableName))
	require.NoError(t, err)

	if !expectStream {
		requireNoReplicationCopyData(t, replConn, 250*time.Millisecond)
		return
	}
	relation, insert, _ := receiveInsertChange(t, replConn)
	require.Equal(t, "public", relation.Namespace)
	require.Equal(t, tableName, relation.RelationName)
	require.Equal(t, relation.RelationID, insert.RelationID)
	require.Len(t, insert.Tuple.Columns, 3)
	require.Equal(t, "1", string(insert.Tuple.Columns[0].Data))
	require.Equal(t, "26:00:00", string(insert.Tuple.Columns[1].Data))
	require.Equal(t, "shown", string(insert.Tuple.Columns[2].Data))
}

func runLogicalReplicationTimestamptzLiteralCanonicalizationRepro(t *testing.T, tableName string, publicationName string, slotName string, rowFilter string, expectStream bool) {
	t.Helper()
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	_, err = conn.Current.Exec(ctx, `SET TIME ZONE 'America/Phoenix';`)
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			tenant_id INT PRIMARY KEY,
			event_tz TIMESTAMPTZ,
			label TEXT
		);`, tableName))
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, fmt.Sprintf(`
		CREATE PUBLICATION %s
		FOR TABLE %s
		WHERE (%s);`, publicationName, tableName, rowFilter))
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
			fmt.Sprintf(`"publication_names" '%s'`, publicationName),
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s
		VALUES (1, '2026-04-30 18:02:03-07', 'shown');`, tableName))
	require.NoError(t, err)

	if !expectStream {
		requireNoReplicationCopyData(t, replConn, 250*time.Millisecond)
		return
	}
	relation, insert, _ := receiveInsertChange(t, replConn)
	require.Equal(t, "public", relation.Namespace)
	require.Equal(t, tableName, relation.RelationName)
	require.Equal(t, relation.RelationID, insert.RelationID)
	require.Len(t, insert.Tuple.Columns, 3)
	require.Equal(t, "1", string(insert.Tuple.Columns[0].Data))
	require.Equal(t, "2026-04-30 18:02:03-07", string(insert.Tuple.Columns[1].Data))
	require.Equal(t, "shown", string(insert.Tuple.Columns[2].Data))
}

func runLogicalReplicationTimestampLiteralCanonicalizationRepro(t *testing.T, tableName string, publicationName string, slotName string, rowFilter string, expectStream bool) {
	t.Helper()
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	_, err = conn.Current.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			tenant_id INT PRIMARY KEY,
			event_ts TIMESTAMP,
			label TEXT
		);`, tableName))
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, fmt.Sprintf(`
		CREATE PUBLICATION %s
		FOR TABLE %s
		WHERE (%s);`, publicationName, tableName, rowFilter))
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
			fmt.Sprintf(`"publication_names" '%s'`, publicationName),
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s
		VALUES (1, '2026-05-01 01:02:03', 'shown');`, tableName))
	require.NoError(t, err)

	if !expectStream {
		requireNoReplicationCopyData(t, replConn, 250*time.Millisecond)
		return
	}
	relation, insert, _ := receiveInsertChange(t, replConn)
	require.Equal(t, "public", relation.Namespace)
	require.Equal(t, tableName, relation.RelationName)
	require.Equal(t, relation.RelationID, insert.RelationID)
	require.Len(t, insert.Tuple.Columns, 3)
	require.Equal(t, "1", string(insert.Tuple.Columns[0].Data))
	require.Equal(t, "2026-05-01 01:02:03", string(insert.Tuple.Columns[1].Data))
	require.Equal(t, "shown", string(insert.Tuple.Columns[2].Data))
}

func runLogicalReplicationUuidLiteralCaseRepro(t *testing.T, tableName string, publicationName string, slotName string, rowFilter string, expectStream bool) {
	t.Helper()
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	_, err = conn.Current.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			tenant_id INT PRIMARY KEY,
			external_id UUID,
			label TEXT
		);`, tableName))
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, fmt.Sprintf(`
		CREATE PUBLICATION %s
		FOR TABLE %s
		WHERE (%s);`, publicationName, tableName, rowFilter))
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
			fmt.Sprintf(`"publication_names" '%s'`, publicationName),
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s
		VALUES (1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'shown');`, tableName))
	require.NoError(t, err)

	if !expectStream {
		requireNoReplicationCopyData(t, replConn, 250*time.Millisecond)
		return
	}
	relation, insert, _ := receiveInsertChange(t, replConn)
	require.Equal(t, "public", relation.Namespace)
	require.Equal(t, tableName, relation.RelationName)
	require.Equal(t, relation.RelationID, insert.RelationID)
	require.Len(t, insert.Tuple.Columns, 3)
	require.Equal(t, "1", string(insert.Tuple.Columns[0].Data))
	require.Equal(t, "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", string(insert.Tuple.Columns[1].Data))
	require.Equal(t, "shown", string(insert.Tuple.Columns[2].Data))
}

func runLogicalReplicationTextNumericFilterRepro(t *testing.T, tableName string, publicationName string, slotName string, rowFilter string, labelValue string, expectStream bool) {
	t.Helper()
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	_, err = conn.Current.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			tenant_id INT PRIMARY KEY,
			label TEXT
		);`, tableName))
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, fmt.Sprintf(`
		CREATE PUBLICATION %s
		FOR TABLE %s
		WHERE (%s);`, publicationName, tableName, rowFilter))
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
			fmt.Sprintf(`"publication_names" '%s'`, publicationName),
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s
		VALUES (1, '%s');`, tableName, strings.ReplaceAll(labelValue, `'`, `''`)))
	require.NoError(t, err)

	if !expectStream {
		requireNoReplicationCopyData(t, replConn, 250*time.Millisecond)
		return
	}
	relation, insert, _ := receiveInsertChange(t, replConn)
	require.Equal(t, "public", relation.Namespace)
	require.Equal(t, tableName, relation.RelationName)
	require.Equal(t, relation.RelationID, insert.RelationID)
	require.Len(t, insert.Tuple.Columns, 2)
	require.Equal(t, "1", string(insert.Tuple.Columns[0].Data))
	require.Equal(t, labelValue, string(insert.Tuple.Columns[1].Data))
}

func runLogicalReplicationByteaTextLiteralRepro(t *testing.T, tableName string, publicationName string, slotName string, rowFilter string, expectStream bool) {
	t.Helper()
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	_, err = conn.Current.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			tenant_id INT PRIMARY KEY,
			payload BYTEA,
			label TEXT
		);`, tableName))
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, fmt.Sprintf(`
		CREATE PUBLICATION %s
		FOR TABLE %s
		WHERE (%s);`, publicationName, tableName, rowFilter))
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
			fmt.Sprintf(`"publication_names" '%s'`, publicationName),
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s
		VALUES (1, 'abc', 'shown');`, tableName))
	require.NoError(t, err)

	if !expectStream {
		requireNoReplicationCopyData(t, replConn, 250*time.Millisecond)
		return
	}
	relation, insert, _ := receiveInsertChange(t, replConn)
	require.Equal(t, "public", relation.Namespace)
	require.Equal(t, tableName, relation.RelationName)
	require.Equal(t, relation.RelationID, insert.RelationID)
	require.Len(t, insert.Tuple.Columns, 3)
	require.Equal(t, "1", string(insert.Tuple.Columns[0].Data))
	require.Equal(t, `\x616263`, string(insert.Tuple.Columns[1].Data))
	require.Equal(t, "shown", string(insert.Tuple.Columns[2].Data))
}

func runLogicalReplicationJsonbCanonicalizationRepro(t *testing.T, tableName string, publicationName string, slotName string, rowFilter string, docValue string, expectedDocText string, expectStream bool) {
	t.Helper()
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()
	defer conn.Close(ctx)

	_, err = conn.Current.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			tenant_id INT PRIMARY KEY,
			doc JSONB,
			label TEXT
		);`, tableName))
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, fmt.Sprintf(`
		CREATE PUBLICATION %s
		FOR TABLE %s
		WHERE (%s);`, publicationName, tableName, rowFilter))
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
			fmt.Sprintf(`"publication_names" '%s'`, publicationName),
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s
		VALUES (1, '%s', 'shown');`, tableName, strings.ReplaceAll(docValue, "'", "''")))
	require.NoError(t, err)

	if !expectStream {
		requireNoReplicationCopyData(t, replConn, 250*time.Millisecond)
		return
	}
	relation, insert, _ := receiveInsertChange(t, replConn)
	require.Equal(t, "public", relation.Namespace)
	require.Equal(t, tableName, relation.RelationName)
	require.Equal(t, relation.RelationID, insert.RelationID)
	require.Len(t, insert.Tuple.Columns, 3)
	require.Equal(t, "1", string(insert.Tuple.Columns[0].Data))
	require.Equal(t, expectedDocText, string(insert.Tuple.Columns[1].Data))
	require.Equal(t, "shown", string(insert.Tuple.Columns[2].Data))
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

// TestLogicalReplicationRowFilterNotAroundInNullSuppressesRowsRepro reproduces
// a logical replication data-consistency bug: NOT over an IN expression with a
// NULL list member remains UNKNOWN for non-matching values, so the row must not
// publish.
func TestLogicalReplicationRowFilterNotAroundInNullSuppressesRowsRepro(t *testing.T) {
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
		CREATE TABLE row_filter_not_around_in_null_items (
			tenant_id INT PRIMARY KEY,
			customer_id INT,
			label TEXT
		);`)
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, `
		CREATE PUBLICATION row_filter_not_around_in_null_pub
		FOR TABLE row_filter_not_around_in_null_items
		WHERE (NOT (customer_id IN (1, NULL)));`)
	require.NoError(t, err)

	replConn := connectReplicationConn(t, ctx, port)
	defer replConn.Close(context.Background())

	slotName := "row_filter_not_around_in_null_slot"
	_, err = pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication,
	})
	require.NoError(t, err)
	require.NoError(t, pglogrepl.StartReplication(ctx, replConn, slotName, 0, pglogrepl.StartReplicationOptions{
		Mode: pglogrepl.LogicalReplication,
		PluginArgs: []string{
			`"proto_version" '1'`,
			`"publication_names" 'row_filter_not_around_in_null_pub'`,
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, `
		INSERT INTO row_filter_not_around_in_null_items
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

// TestLogicalReplicationSchemaPublicationOverridesTableRowFilterRepro
// reproduces a logical replication data-consistency bug: when a publication
// includes a table both explicitly with a row filter and through TABLES IN
// SCHEMA, PostgreSQL's effective row filter for the table is empty.
func TestLogicalReplicationSchemaPublicationOverridesTableRowFilterRepro(t *testing.T) {
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
		CREATE SCHEMA row_filter_schema_overlap;
		CREATE TABLE row_filter_schema_overlap.items (
			id INT PRIMARY KEY,
			tenant_id INT NOT NULL,
			label TEXT
		);
		CREATE PUBLICATION row_filter_schema_overlap_pub
		FOR TABLE row_filter_schema_overlap.items
			WHERE (tenant_id = 1),
			TABLES IN SCHEMA row_filter_schema_overlap;`)
	require.NoError(t, err)

	replConn := connectReplicationConn(t, ctx, port)
	defer replConn.Close(context.Background())

	slotName := "row_filter_schema_overlap_slot"
	_, err = pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication,
	})
	require.NoError(t, err)
	require.NoError(t, pglogrepl.StartReplication(ctx, replConn, slotName, 0, pglogrepl.StartReplicationOptions{
		Mode: pglogrepl.LogicalReplication,
		PluginArgs: []string{
			`"proto_version" '1'`,
			`"publication_names" 'row_filter_schema_overlap_pub'`,
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	_, err = conn.Current.Exec(ctx, `
		INSERT INTO row_filter_schema_overlap.items
		VALUES (1, 2, 'visible-through-schema');`)
	require.NoError(t, err)

	relation, insert, _ := receiveInsertChange(t, replConn)
	require.Equal(t, "row_filter_schema_overlap", relation.Namespace)
	require.Equal(t, "items", relation.RelationName)
	require.Equal(t, relation.RelationID, insert.RelationID)
	require.Len(t, insert.Tuple.Columns, 3)
	require.Equal(t, "1", string(insert.Tuple.Columns[0].Data))
	require.Equal(t, "2", string(insert.Tuple.Columns[1].Data))
	require.Equal(t, "visible-through-schema", string(insert.Tuple.Columns[2].Data))
}
