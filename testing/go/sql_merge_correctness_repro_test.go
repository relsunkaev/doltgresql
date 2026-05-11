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

// TestSqlMergeUpdatesAndInsertsRowsRepro reproduces a DML correctness bug:
// PostgreSQL MERGE applies matched UPDATE actions and not-matched INSERT
// actions atomically from a source relation.
func TestSqlMergeUpdatesAndInsertsRowsRepro(t *testing.T) {
	ctx, conn, controller := CreateServer(t, "postgres")
	t.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	for _, stmt := range []string{
		`CREATE TABLE sql_merge_target (
			id INT PRIMARY KEY,
			balance INT NOT NULL
		);`,
		`CREATE TABLE sql_merge_source (
			id INT PRIMARY KEY,
			delta INT NOT NULL
		);`,
		`INSERT INTO sql_merge_target VALUES (1, 10);`,
		`INSERT INTO sql_merge_source VALUES (1, 5), (2, 7);`,
	} {
		_, err := conn.Current.Exec(ctx, stmt)
		require.NoError(t, err, stmt)
	}

	_, err := conn.Current.Exec(ctx, `MERGE INTO sql_merge_target AS t
		USING sql_merge_source AS s
		ON t.id = s.id
		WHEN MATCHED THEN
			UPDATE SET balance = t.balance + s.delta
		WHEN NOT MATCHED THEN
			INSERT (id, balance) VALUES (s.id, s.delta);`)
	require.NoError(t, err)

	rows, err := conn.Current.Query(ctx, `SELECT id, balance
		FROM sql_merge_target
		ORDER BY id;`)
	require.NoError(t, err)
	actual, _, err := ReadRows(rows, true)
	require.NoError(t, err)
	require.Equal(t, []sql.Row{
		{int64(1), int64(15)},
		{int64(2), int64(7)},
	}, actual)
}
