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
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// TestCopyToStdoutAllowsColumnSelectGrantRepro reproduces an authorization
// correctness bug: PostgreSQL COPY TO STDOUT honors column-level SELECT grants
// when the COPY column list names only granted columns.
func TestCopyToStdoutAllowsColumnSelectGrantRepro(t *testing.T) {
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)
	ctx, connection, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		connection.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	for _, stmt := range []string{
		`CREATE USER copy_column_reader PASSWORD 'reader';`,
		`CREATE TABLE copy_column_select_private (
			id INT PRIMARY KEY,
			public_value TEXT,
			private_value TEXT
		);`,
		`INSERT INTO copy_column_select_private VALUES (1, 'public', 'private');`,
		`GRANT USAGE ON SCHEMA public TO copy_column_reader;`,
		`GRANT SELECT (id, public_value)
			ON copy_column_select_private TO copy_column_reader;`,
	} {
		_, err = connection.Exec(ctx, stmt)
		require.NoError(t, err, stmt)
	}

	readerConn, err := pgx.Connect(ctx, fmt.Sprintf(
		"postgres://copy_column_reader:reader@127.0.0.1:%d/postgres?sslmode=disable",
		port,
	))
	require.NoError(t, err)
	defer readerConn.Close(context.Background())

	var out bytes.Buffer
	tag, err := readerConn.PgConn().CopyTo(
		ctx,
		&out,
		`COPY copy_column_select_private (id, public_value) TO STDOUT;`,
	)
	require.NoError(t, err, "COPY TO should allow SELECT on the copied columns; tag=%s output=%q", tag.String(), out.String())
	require.Equal(t, "COPY 1", tag.String())
	require.Equal(t, "1\tpublic\n", out.String())
}
