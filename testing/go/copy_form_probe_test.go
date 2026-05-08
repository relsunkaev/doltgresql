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
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCopyQueryToStdout covers the query-form COPY syntax emitted by pg_dump
// for filtered exports.
func TestCopyQueryToStdout(t *testing.T) {
	ctx, connection, controller := CreateServer(t, "postgres")
	defer func() {
		connection.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	_, err := connection.Exec(ctx, `CREATE TABLE t (id INT PRIMARY KEY, v TEXT, include BOOL);`)
	require.NoError(t, err)
	_, err = connection.Exec(ctx, `INSERT INTO t VALUES (1, 'a', TRUE), (2, 'b', FALSE), (3, NULL, TRUE);`)
	require.NoError(t, err)

	var textOut bytes.Buffer
	tag, err := connection.Default.PgConn().CopyTo(ctx, &textOut, `COPY (SELECT id, v FROM t WHERE include ORDER BY id) TO STDOUT WITH (FORMAT text);`)
	require.NoError(t, err)
	require.Equal(t, "COPY 2", tag.String())
	require.Equal(t, "1\ta\n3\t\\N\n", textOut.String())

	var csvOut bytes.Buffer
	tag, err = connection.Default.PgConn().CopyTo(ctx, &csvOut, `COPY (SELECT v AS label, id + 10 AS shifted_id FROM t WHERE id < 3 ORDER BY id DESC) TO STDOUT WITH (FORMAT CSV, HEADER TRUE);`)
	require.NoError(t, err)
	require.Equal(t, "COPY 2", tag.String())
	require.Equal(t, "label,shifted_id\nb,12\na,11\n", csvOut.String())
}
