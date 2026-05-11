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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// TestSessionReplicationRoleRequiresSuperuserRepro reproduces a security bug:
// Doltgres lets a normal role set the superuser-only session_replication_role
// parameter to replica, which then suppresses foreign key enforcement.
func TestSessionReplicationRoleRequiresSuperuserRepro(t *testing.T) {
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)
	ctx, connection, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		connection.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	_, err = connection.Exec(ctx, `CREATE TABLE srr_parent (id INT PRIMARY KEY);`)
	require.NoError(t, err)
	_, err = connection.Exec(ctx, `CREATE TABLE srr_child (id INT PRIMARY KEY, parent_id INT REFERENCES srr_parent(id));`)
	require.NoError(t, err)
	_, err = connection.Exec(ctx, `CREATE USER srr_intruder PASSWORD 'pw';`)
	require.NoError(t, err)
	_, err = connection.Exec(ctx, `GRANT USAGE ON SCHEMA public TO srr_intruder;`)
	require.NoError(t, err)
	_, err = connection.Exec(ctx, `GRANT INSERT ON srr_child TO srr_intruder;`)
	require.NoError(t, err)

	intruderConn, err := pgx.Connect(ctx, fmt.Sprintf(
		"postgres://srr_intruder:pw@127.0.0.1:%d/postgres?sslmode=disable",
		port,
	))
	require.NoError(t, err)
	defer intruderConn.Close(context.Background())

	_, err = intruderConn.Exec(ctx, `SET session_replication_role = 'replica';`)
	if err == nil {
		_, insertErr := intruderConn.Exec(ctx, `INSERT INTO srr_child VALUES (1, 999);`)
		require.NoError(t, insertErr)
		t.Fatalf("expected permission denied setting session_replication_role, but SET succeeded and a foreign key bypass insert succeeded")
	}
	require.ErrorContains(t, err, "permission denied")
}
