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

// TestLogicalReplicationActiveStreamRollbackToSavepointRestoresDeletedRowsRepro
// reproduces a data-consistency bug: an active pgoutput stream must not change
// savepoint semantics for ordinary table deletes. PostgreSQL restores rows
// deleted after a savepoint when ROLLBACK TO SAVEPOINT is executed.
func TestLogicalReplicationActiveStreamRollbackToSavepointRestoresDeletedRowsRepro(t *testing.T) {
	replsource.ResetForTests()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	t.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	for _, stmt := range []string{
		`CREATE TABLE active_stream_savepoint_items (id INT PRIMARY KEY, label TEXT NOT NULL);`,
		`INSERT INTO active_stream_savepoint_items VALUES (1, 'one'), (2, 'two');`,
		`CREATE PUBLICATION active_stream_savepoint_pub FOR TABLE active_stream_savepoint_items;`,
	} {
		_, err = conn.Current.Exec(ctx, stmt)
		require.NoError(t, err, stmt)
	}

	replConn := connectReplicationConn(t, ctx, port)
	t.Cleanup(func() {
		_ = replConn.Close(context.Background())
	})

	slotName := "active_stream_savepoint_slot"
	_, err = pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication,
	})
	require.NoError(t, err)
	require.NoError(t, pglogrepl.StartReplication(ctx, replConn, slotName, 0, pglogrepl.StartReplicationOptions{
		Mode: pglogrepl.LogicalReplication,
		PluginArgs: []string{
			`"proto_version" '1'`,
			`"publication_names" 'active_stream_savepoint_pub'`,
		},
	}))
	keepalive := receiveReplicationCopyData(t, replConn)
	require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), keepalive.Data[0])

	for _, stmt := range []string{
		`BEGIN;`,
		`SAVEPOINT before_delete;`,
		`DELETE FROM active_stream_savepoint_items;`,
	} {
		_, err = conn.Current.Exec(ctx, stmt)
		require.NoError(t, err, stmt)
	}

	var count int
	require.NoError(t, conn.Current.QueryRow(ctx, `SELECT count(*) FROM active_stream_savepoint_items;`).Scan(&count))
	require.Equal(t, 0, count)

	_, err = conn.Current.Exec(ctx, `ROLLBACK TO SAVEPOINT before_delete;`)
	require.NoError(t, err)
	require.NoError(t, conn.Current.QueryRow(ctx, `SELECT count(*) FROM active_stream_savepoint_items;`).Scan(&count))
	require.Equal(t, 2, count)

	_, err = conn.Current.Exec(ctx, `COMMIT;`)
	require.NoError(t, err)
	requireNoReplicationCopyData(t, replConn, 250*time.Millisecond)
}
