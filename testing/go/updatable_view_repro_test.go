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

	"github.com/stretchr/testify/require"
)

// TestSimpleViewInsertUpdatesBaseTableRepro reproduces a PostgreSQL
// compatibility correctness bug: simple automatically updatable views should
// accept INSERT and write through to the base table.
func TestSimpleViewInsertUpdatesBaseTableRepro(t *testing.T) {
	ctx, conn, controller := CreateServer(t, "postgres")
	t.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	_, err := conn.Current.Exec(ctx, `CREATE TABLE updatable_view_insert_items (
		id INT PRIMARY KEY,
		label TEXT
	);`)
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, `CREATE VIEW updatable_view_insert AS
		SELECT id, label FROM updatable_view_insert_items;`)
	require.NoError(t, err)

	_, err = conn.Current.Exec(ctx, `INSERT INTO updatable_view_insert VALUES (1, 'through-view');`)
	require.NoError(t, err)

	var label string
	require.NoError(t, conn.Current.QueryRow(ctx, `SELECT label FROM updatable_view_insert_items WHERE id = 1;`).Scan(&label))
	require.Equal(t, "through-view", label)
}

// TestSimpleViewUpdateUpdatesBaseTableRepro reproduces a PostgreSQL
// compatibility correctness bug: simple automatically updatable views should
// accept UPDATE and write through to the base table.
func TestSimpleViewUpdateUpdatesBaseTableRepro(t *testing.T) {
	ctx, conn, controller := CreateServer(t, "postgres")
	t.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	_, err := conn.Current.Exec(ctx, `CREATE TABLE updatable_view_update_items (
		id INT PRIMARY KEY,
		label TEXT
	);`)
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, `INSERT INTO updatable_view_update_items VALUES (1, 'before');`)
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, `CREATE VIEW updatable_view_update AS
		SELECT id, label FROM updatable_view_update_items;`)
	require.NoError(t, err)

	_, err = conn.Current.Exec(ctx, `UPDATE updatable_view_update SET label = 'after' WHERE id = 1;`)
	require.NoError(t, err)

	var label string
	require.NoError(t, conn.Current.QueryRow(ctx, `SELECT label FROM updatable_view_update_items WHERE id = 1;`).Scan(&label))
	require.Equal(t, "after", label)
}

// TestSimpleViewDeleteDeletesBaseTableRepro reproduces a PostgreSQL
// compatibility correctness bug: simple automatically updatable views should
// accept DELETE and delete through from the base table.
func TestSimpleViewDeleteDeletesBaseTableRepro(t *testing.T) {
	ctx, conn, controller := CreateServer(t, "postgres")
	t.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	_, err := conn.Current.Exec(ctx, `CREATE TABLE updatable_view_delete_items (
		id INT PRIMARY KEY,
		label TEXT
	);`)
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, `INSERT INTO updatable_view_delete_items VALUES (1, 'before');`)
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, `CREATE VIEW updatable_view_delete AS
		SELECT id, label FROM updatable_view_delete_items;`)
	require.NoError(t, err)

	_, err = conn.Current.Exec(ctx, `DELETE FROM updatable_view_delete WHERE id = 1;`)
	require.NoError(t, err)

	var count int64
	require.NoError(t, conn.Current.QueryRow(ctx, `SELECT COUNT(*) FROM updatable_view_delete_items;`).Scan(&count))
	require.Equal(t, int64(0), count)
}
