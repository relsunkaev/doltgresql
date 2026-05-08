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

	gms "github.com/dolthub/go-mysql-server/sql"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// TestSetLocalSavepointSemantics pins doltgres' current behavior for
// SET LOCAL inside a savepoint and contrasts it with what real
// PostgreSQL does. PG treats SET LOCAL values as part of the
// savepoint snapshot: ROLLBACK TO SAVEPOINT restores the value held
// at SAVEPOINT time. Doltgres only snapshots at BEGIN - savepoint
// rollback must restore the SET LOCAL value held at savepoint time.
func TestSetLocalSavepointSemantics(t *testing.T) {
	port, err := gms.GetEmptyPort()
	require.NoError(t, err)
	ctx, defaultConn, controller := CreateServerWithPort(t, "postgres", port)
	t.Cleanup(func() {
		defaultConn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	conn, err := pgx.Connect(ctx, fmt.Sprintf(
		"postgres://postgres:password@127.0.0.1:%d/postgres?sslmode=disable", port))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(ctx) })

	t.Run("BEGIN snapshot is restored at COMMIT (PG-correct, baseline)", func(t *testing.T) {
		_, err := conn.Exec(ctx, "BEGIN;")
		require.NoError(t, err)
		_, err = conn.Exec(ctx, "SET LOCAL app.actor = 'inside';")
		require.NoError(t, err)
		var inside string
		require.NoError(t, conn.QueryRow(ctx,
			"SELECT COALESCE(current_setting('app.actor', true), '');").Scan(&inside))
		require.Equal(t, "inside", inside)
		_, err = conn.Exec(ctx, "COMMIT;")
		require.NoError(t, err)

		var afterCommit string
		require.NoError(t, conn.QueryRow(ctx,
			"SELECT COALESCE(current_setting('app.actor', true), '');").Scan(&afterCommit))
		require.NotEqual(t, "inside", afterCommit,
			"SET LOCAL must not survive COMMIT")
	})

	t.Run("ROLLBACK TO SAVEPOINT restores SET LOCAL", func(t *testing.T) {
		_, err := conn.Exec(ctx, "BEGIN;")
		require.NoError(t, err)
		_, err = conn.Exec(ctx, "SET LOCAL app.actor = 'before-sp';")
		require.NoError(t, err)
		_, err = conn.Exec(ctx, "SAVEPOINT sp;")
		require.NoError(t, err)
		_, err = conn.Exec(ctx, "SET LOCAL app.actor = 'after-sp';")
		require.NoError(t, err)
		_, err = conn.Exec(ctx, "ROLLBACK TO SAVEPOINT sp;")
		require.NoError(t, err)

		var got string
		require.NoError(t, conn.QueryRow(context.Background(),
			"SELECT COALESCE(current_setting('app.actor', true), '');").Scan(&got))

		require.Equal(t, "before-sp", got,
			"ROLLBACK TO SAVEPOINT must restore the savepoint-time SET LOCAL value")

		_, err = conn.Exec(ctx, "ROLLBACK;")
		require.NoError(t, err)
	})

	t.Run("ROLLBACK TO SAVEPOINT clears SET LOCAL first written after savepoint", func(t *testing.T) {
		_, err := conn.Exec(ctx, "BEGIN;")
		require.NoError(t, err)
		_, err = conn.Exec(ctx, "SAVEPOINT sp;")
		require.NoError(t, err)
		_, err = conn.Exec(ctx, "SET LOCAL app.late = 'after-sp';")
		require.NoError(t, err)
		_, err = conn.Exec(ctx, "ROLLBACK TO SAVEPOINT sp;")
		require.NoError(t, err)

		var got string
		require.NoError(t, conn.QueryRow(context.Background(),
			"SELECT COALESCE(current_setting('app.late', true), '');").Scan(&got))
		require.Equal(t, "", got,
			"ROLLBACK TO SAVEPOINT must clear SET LOCAL values first written after the savepoint")

		_, err = conn.Exec(ctx, "ROLLBACK;")
		require.NoError(t, err)
	})

	t.Run("nested savepoint rollbacks restore each frame", func(t *testing.T) {
		_, err := conn.Exec(ctx, "BEGIN;")
		require.NoError(t, err)
		_, err = conn.Exec(ctx, "SET LOCAL app.actor = 'root';")
		require.NoError(t, err)
		_, err = conn.Exec(ctx, "SAVEPOINT outer_sp;")
		require.NoError(t, err)
		_, err = conn.Exec(ctx, "SET LOCAL app.actor = 'outer';")
		require.NoError(t, err)
		_, err = conn.Exec(ctx, "SAVEPOINT inner_sp;")
		require.NoError(t, err)
		_, err = conn.Exec(ctx, "SET LOCAL app.actor = 'inner';")
		require.NoError(t, err)

		_, err = conn.Exec(ctx, "ROLLBACK TO SAVEPOINT inner_sp;")
		require.NoError(t, err)
		var got string
		require.NoError(t, conn.QueryRow(ctx,
			"SELECT COALESCE(current_setting('app.actor', true), '');").Scan(&got))
		require.Equal(t, "outer", got)

		_, err = conn.Exec(ctx, "ROLLBACK TO SAVEPOINT outer_sp;")
		require.NoError(t, err)
		require.NoError(t, conn.QueryRow(ctx,
			"SELECT COALESCE(current_setting('app.actor', true), '');").Scan(&got))
		require.Equal(t, "root", got)

		_, err = conn.Exec(ctx, "ROLLBACK;")
		require.NoError(t, err)
	})

	t.Run("RELEASE SAVEPOINT preserves value but keeps outer rollback frame", func(t *testing.T) {
		_, err := conn.Exec(ctx, "BEGIN;")
		require.NoError(t, err)
		_, err = conn.Exec(ctx, "SET LOCAL app.actor = 'root';")
		require.NoError(t, err)
		_, err = conn.Exec(ctx, "SAVEPOINT outer_sp;")
		require.NoError(t, err)
		_, err = conn.Exec(ctx, "SET LOCAL app.actor = 'outer';")
		require.NoError(t, err)
		_, err = conn.Exec(ctx, "SAVEPOINT inner_sp;")
		require.NoError(t, err)
		_, err = conn.Exec(ctx, "SET LOCAL app.actor = 'inner';")
		require.NoError(t, err)
		_, err = conn.Exec(ctx, "RELEASE SAVEPOINT inner_sp;")
		require.NoError(t, err)

		var got string
		require.NoError(t, conn.QueryRow(ctx,
			"SELECT COALESCE(current_setting('app.actor', true), '');").Scan(&got))
		require.Equal(t, "inner", got)

		_, err = conn.Exec(ctx, "ROLLBACK TO SAVEPOINT outer_sp;")
		require.NoError(t, err)
		require.NoError(t, conn.QueryRow(ctx,
			"SELECT COALESCE(current_setting('app.actor', true), '');").Scan(&got))
		require.Equal(t, "root", got)

		_, err = conn.Exec(ctx, "ROLLBACK;")
		require.NoError(t, err)
	})

	t.Run("ROLLBACK of whole tx still restores baseline", func(t *testing.T) {
		// The transaction-end restore is unaffected by the savepoint
		// gap: rolling back the whole transaction discards all
		// SET LOCALs as expected.
		_, err := conn.Exec(ctx, "BEGIN;")
		require.NoError(t, err)
		_, err = conn.Exec(ctx, "SET LOCAL app.actor = 'doomed';")
		require.NoError(t, err)
		_, err = conn.Exec(ctx, "SAVEPOINT sp;")
		require.NoError(t, err)
		_, err = conn.Exec(ctx, "SET LOCAL app.actor = 'still-doomed';")
		require.NoError(t, err)
		_, err = conn.Exec(ctx, "ROLLBACK;")
		require.NoError(t, err)

		var got string
		require.NoError(t, conn.QueryRow(ctx,
			"SELECT COALESCE(current_setting('app.actor', true), '');").Scan(&got))
		require.NotEqual(t, "doomed", got,
			"full ROLLBACK must clear all SET LOCALs from the tx")
		require.NotEqual(t, "still-doomed", got)
	})
}
