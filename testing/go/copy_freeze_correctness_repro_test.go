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

	"github.com/stretchr/testify/require"
)

// TestCopyFreezeAfterTruncateRepro reproduces a COPY correctness bug:
// PostgreSQL accepts COPY FREEZE after the target table is truncated in the
// current transaction.
func TestCopyFreezeAfterTruncateRepro(t *testing.T) {
	ctx, connection, controller := CreateServer(t, "postgres")
	defer func() {
		connection.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	_, err := connection.Exec(ctx, `CREATE TABLE copy_freeze_valid_items (id INT);`)
	require.NoError(t, err)
	_, err = connection.Exec(ctx, `BEGIN;`)
	require.NoError(t, err)
	_, err = connection.Exec(ctx, `TRUNCATE copy_freeze_valid_items;`)
	require.NoError(t, err)

	tag, err := connection.Default.PgConn().CopyFrom(
		ctx,
		strings.NewReader("1\n"),
		`COPY copy_freeze_valid_items FROM STDIN WITH (FREEZE);`,
	)
	require.NoError(t, err, "COPY FREEZE should be accepted after truncating the table in the current transaction; tag=%s", tag.String())
	require.Equal(t, "COPY 1", tag.String())

	_, err = connection.Exec(ctx, `COMMIT;`)
	require.NoError(t, err)

	var count int64
	require.NoError(t, connection.Default.QueryRow(
		ctx,
		`SELECT count(*) FROM copy_freeze_valid_items;`,
	).Scan(&count))
	require.Equal(t, int64(1), count)
}
