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

package postgres18

import (
	. "github.com/dolthub/doltgresql/testing/go"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

// TestPostgres18CopyOnErrorRejectLimitRepro guards PostgreSQL COPY FROM
// best-effort import behavior. ON_ERROR ignore should skip rows with input
// conversion errors while loading valid rows, and REJECT_LIMIT should bound the
// tolerated error count.
func TestPostgres18CopyOnErrorRejectLimitRepro(t *testing.T) {
	ctx, connection, controller := CreateServer(t, "postgres")
	defer func() {
		connection.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	_, err := connection.Exec(ctx, `CREATE TABLE copy_on_error_items (
		id INT PRIMARY KEY,
		qty INT NOT NULL
	);`)
	require.NoError(t, err)

	tag, err := connection.Default.PgConn().CopyFrom(
		ctx,
		strings.NewReader("1,10\nbad,not-an-int\n2,20\n"),
		`COPY copy_on_error_items (id, qty) FROM STDIN WITH (FORMAT csv, ON_ERROR ignore, REJECT_LIMIT 1, LOG_VERBOSITY silent);`,
	)
	require.NoError(t, err, "COPY ON_ERROR ignore should skip the malformed row; tag=%s", tag.String())
	require.Equal(t, "COPY 2", tag.String())

	rows, err := connection.Query(ctx, `SELECT id, qty
		FROM copy_on_error_items
		ORDER BY id;`)
	require.NoError(t, err)
	readRows, _, err := ReadRows(rows, true)
	require.NoError(t, err)
	require.Equal(t, []sql.Row{
		{int64(1), int64(10)},
		{int64(2), int64(20)},
	}, readRows)
}
