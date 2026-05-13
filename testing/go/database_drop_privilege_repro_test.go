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
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// TestDropDatabaseClearsPrivilegesRepro reproduces an ACL persistence bug:
// dropping a database does not clear CONNECT/CREATE privileges granted on it,
// so a later database with the same name inherits access to the dropped one.
func TestDropDatabaseClearsPrivilegesRepro(t *testing.T) {
	ctx, conn, controller := CreateServer(t, "postgres")
	defer func() {
		conn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	_, err := conn.Exec(ctx, `CREATE USER drop_recreate_database_user PASSWORD 'database';`)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, `CREATE DATABASE drop_recreate_database_acl;`)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, `REVOKE ALL ON DATABASE drop_recreate_database_acl FROM PUBLIC;`)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, `GRANT CONNECT, CREATE ON DATABASE drop_recreate_database_acl
		TO drop_recreate_database_user;`)
	require.NoError(t, err)

	userConn := connectToDropRecreateDatabaseAsUser(t, ctx, conn, "drop_recreate_database_user", "database")
	_, err = userConn.Exec(ctx, `CREATE SCHEMA before_drop;`)
	require.NoError(t, err)
	require.NoError(t, userConn.Close(ctx))

	_, err = conn.Exec(ctx, `DROP DATABASE drop_recreate_database_acl;`)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, `CREATE DATABASE drop_recreate_database_acl;`)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, `REVOKE ALL ON DATABASE drop_recreate_database_acl FROM PUBLIC;`)
	require.NoError(t, err)

	recreatedUserConn := connectToDropRecreateDatabaseAsUser(t, ctx, conn, "drop_recreate_database_user", "database")
	defer recreatedUserConn.Close(context.Background())
	_, err = recreatedUserConn.Exec(ctx, `CREATE SCHEMA after_drop;`)
	require.ErrorContains(t, err, "permission denied")
}

func connectToDropRecreateDatabaseAsUser(t *testing.T, ctx context.Context, conn *Connection, username, password string) *pgx.Conn {
	t.Helper()
	config := conn.Default.Config()
	userConn, err := pgx.Connect(ctx, fmt.Sprintf(
		"postgres://%s:%s@127.0.0.1:%d/drop_recreate_database_acl",
		username,
		password,
		config.Port,
	))
	require.NoError(t, err)
	return userConn
}
