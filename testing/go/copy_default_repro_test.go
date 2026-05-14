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
	"strings"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
)

// TestPostgres16CopyDefaultMappingRepro guards PostgreSQL 16 COPY FROM
// DEFAULT mapping. A matching input field should use the destination column's
// default expression instead of loading the literal marker string.
func TestPostgres16CopyDefaultMappingRepro(t *testing.T) {
	ctx, connection, controller := CreateServer(t, "postgres")
	defer func() {
		connection.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	_, err := connection.Exec(ctx, `CREATE TABLE copy_default_mapping_items (
		id INT PRIMARY KEY,
		label TEXT DEFAULT 'from-default',
		qty INT DEFAULT 7
	);`)
	require.NoError(t, err)

	tag, err := connection.Default.PgConn().CopyFrom(
		ctx,
		strings.NewReader("1,DEFAULT,DEFAULT\n2,explicit,9\n"),
		`COPY copy_default_mapping_items (id, label, qty) FROM STDIN WITH (FORMAT csv, DEFAULT 'DEFAULT');`,
	)
	require.NoError(t, err, "COPY DEFAULT mapping should load both rows; tag=%s", tag.String())
	require.Equal(t, "COPY 2", tag.String())

	rows, err := connection.Query(ctx, `SELECT id, label, qty
		FROM copy_default_mapping_items
		ORDER BY id;`)
	require.NoError(t, err)
	readRows, _, err := ReadRows(rows, true)
	require.NoError(t, err)
	require.Equal(t, []sql.Row{
		{int64(1), "from-default", int64(7)},
		{int64(2), "explicit", int64(9)},
	}, readRows)
}
