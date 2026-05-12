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
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/doltgresql/server/auth"
)

func requireLoginRejected(t *testing.T, ctx context.Context, port int, username, password string) {
	t.Helper()
	conn, err := pgx.Connect(ctx, fmt.Sprintf(
		"postgres://%s:%s@127.0.0.1:%d/postgres?sslmode=disable",
		username,
		password,
		port,
	))
	if conn != nil {
		defer conn.Close(context.Background())
	}
	require.Error(t, err)
}

// TestNoLoginRolePreventsLoginGuard guards that roles without LOGIN cannot
// authenticate even when they have a password.
func TestNoLoginRolePreventsLoginGuard(t *testing.T) {
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)
	ctx, connection, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		connection.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	_, err = connection.Exec(ctx, `CREATE ROLE no_login_role PASSWORD 'pw' NOLOGIN;`)
	require.NoError(t, err)

	requireLoginRejected(t, ctx, port, "no_login_role", "pw")
}

// TestAlterRoleNoLoginPreventsLoginGuard guards that ALTER ROLE ... NOLOGIN is
// enforced for new authentication attempts.
func TestAlterRoleNoLoginPreventsLoginGuard(t *testing.T) {
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)
	ctx, connection, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		connection.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	_, err = connection.Exec(ctx, `CREATE USER altered_no_login PASSWORD 'pw';`)
	require.NoError(t, err)
	_, err = connection.Exec(ctx, `ALTER ROLE altered_no_login NOLOGIN;`)
	require.NoError(t, err)

	requireLoginRejected(t, ctx, port, "altered_no_login", "pw")
}

// TestCreatedUserLoginSurvivesRestartRepro reproduces an auth persistence bug:
// a role created with a password must still be able to authenticate after a
// server restart using the same database directory.
func TestCreatedUserLoginSurvivesRestartRepro(t *testing.T) {
	dbDir := t.TempDir()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, connection, controller := CreateServerLocalInDirWithPort(t, "postgres", dbDir, port)
	_, err = connection.Exec(ctx, `CREATE USER restart_login PASSWORD 'pw';`)
	require.NoError(t, err)

	immediateConn, err := pgx.Connect(ctx, fmt.Sprintf(
		"postgres://restart_login:pw@127.0.0.1:%d/postgres?sslmode=disable",
		port,
	))
	require.NoError(t, err)
	require.NoError(t, immediateConn.Close(ctx))

	connection.Close(ctx)
	controller.Stop()
	require.NoError(t, controller.WaitForStop())

	ctx, connection, controller = CreateServerLocalInDirWithPort(t, "postgres", dbDir, port)
	defer func() {
		connection.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	restartedConn, err := pgx.Connect(ctx, fmt.Sprintf(
		"postgres://restart_login:pw@127.0.0.1:%d/postgres?sslmode=disable",
		port,
	))
	require.NoError(t, err)
	defer restartedConn.Close(context.Background())

	var currentUser string
	require.NoError(t, restartedConn.QueryRow(ctx, `SELECT current_user;`).Scan(&currentUser))
	require.Equal(t, "restart_login", currentUser)
}

// TestExpiredRoleValidUntilPreventsLoginRepro reproduces a security bug:
// Doltgres stores role VALID UNTIL metadata but does not enforce expiration at
// authentication time.
func TestExpiredRoleValidUntilPreventsLoginRepro(t *testing.T) {
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)
	ctx, connection, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		connection.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	_, err = connection.Exec(ctx, `CREATE USER expired_login PASSWORD 'pw';`)
	require.NoError(t, err)

	expiredAt := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	auth.LockWrite(func() {
		role := auth.GetRole("expired_login")
		require.True(t, role.IsValid())
		role.ValidUntil = &expiredAt
		auth.SetRole(role)
		err = auth.PersistChanges()
	})
	require.NoError(t, err)

	expiredConn, err := pgx.Connect(ctx, fmt.Sprintf(
		"postgres://expired_login:pw@127.0.0.1:%d/postgres?sslmode=disable",
		port,
	))
	if expiredConn != nil {
		defer expiredConn.Close(context.Background())
	}
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "password authentication failed") ||
		strings.Contains(err.Error(), "expired"),
		"expected authentication failure for expired role, got %v", err)
}

// TestRoleConnectionLimitPreventsLoginRepro reproduces a security bug:
// Doltgres stores role CONNECTION LIMIT metadata but does not enforce it at
// authentication time.
func TestRoleConnectionLimitPreventsLoginRepro(t *testing.T) {
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)
	ctx, connection, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		connection.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	_, err = connection.Exec(ctx, `CREATE USER connection_limited PASSWORD 'pw' CONNECTION LIMIT 0;`)
	require.NoError(t, err)

	limitedConn, err := pgx.Connect(ctx, fmt.Sprintf(
		"postgres://connection_limited:pw@127.0.0.1:%d/postgres?sslmode=disable",
		port,
	))
	if limitedConn != nil {
		defer limitedConn.Close(context.Background())
	}
	require.ErrorContains(t, err, "too many connections for role")
}
