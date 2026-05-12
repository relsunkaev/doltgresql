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
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
)

func TestRoleMembershipSurvivesRestart(t *testing.T) {
	dbDir := t.TempDir()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, connection, controller := CreateServerLocalInDirWithPort(t, "postgres", dbDir, port)
	for _, query := range []string{
		`CREATE ROLE restart_parent_role;`,
		`CREATE ROLE restart_child_role;`,
		`GRANT restart_parent_role TO restart_child_role;`,
	} {
		_, err = connection.Exec(ctx, query)
		require.NoError(t, err, query)
	}

	var beforeCount int64
	require.NoError(t, connection.Current.QueryRow(ctx, `
		SELECT count(*)
		FROM pg_catalog.pg_auth_members
		WHERE pg_get_userbyid(roleid) = 'restart_parent_role'
			AND pg_get_userbyid(member) = 'restart_child_role';`).Scan(&beforeCount))
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
		FROM pg_catalog.pg_auth_members
		WHERE pg_get_userbyid(roleid) = 'restart_parent_role'
			AND pg_get_userbyid(member) = 'restart_child_role';`).Scan(&afterCount))
	require.EqualValues(t, 1, afterCount)
}
