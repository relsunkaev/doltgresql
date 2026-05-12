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
	"fmt"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestDefaultPrivilegesSurviveRestart(t *testing.T) {
	dbDir := t.TempDir()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, connection, controller := CreateServerLocalInDirWithPort(t, "postgres", dbDir, port)
	for _, query := range []string{
		`CREATE USER restart_default_reader PASSWORD 'reader';`,
		`GRANT USAGE ON SCHEMA public TO restart_default_reader;`,
		`ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO restart_default_reader;`,
	} {
		_, err = connection.Exec(ctx, query)
		require.NoError(t, err, query)
	}
	var beforeCount int64
	require.NoError(t, connection.Current.QueryRow(ctx, `
		SELECT count(*)
		FROM pg_catalog.pg_default_acl
		WHERE defaclnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')
			AND defaclobjtype = 'r';`).Scan(&beforeCount))
	require.EqualValues(t, 1, beforeCount)

	connection.Close(ctx)
	controller.Stop()
	require.NoError(t, controller.WaitForStop())

	ctx, connection, controller = CreateServerLocalInDirWithPort(t, "postgres", dbDir, port)
	defer func() {
		connection.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	var afterCount int64
	require.NoError(t, connection.Current.QueryRow(ctx, `
		SELECT count(*)
		FROM pg_catalog.pg_default_acl
		WHERE defaclnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')
			AND defaclobjtype = 'r';`).Scan(&afterCount))
	require.EqualValues(t, 1, afterCount)

	_, err = connection.Exec(ctx, `CREATE TABLE restart_default_items (id INT PRIMARY KEY, label TEXT);`)
	require.NoError(t, err)
	_, err = connection.Exec(ctx, `INSERT INTO restart_default_items VALUES (1, 'after restart');`)
	require.NoError(t, err)

	userConn, err := pgx.Connect(ctx, fmt.Sprintf(
		"postgres://restart_default_reader:reader@127.0.0.1:%d/postgres?sslmode=disable",
		port,
	))
	require.NoError(t, err)
	defer userConn.Close(ctx)

	var label string
	require.NoError(t, userConn.QueryRow(ctx, `SELECT label FROM restart_default_items WHERE id = 1;`).Scan(&label))
	require.Equal(t, "after restart", label)
}
