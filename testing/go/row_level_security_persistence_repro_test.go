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

// TestRowLevelSecuritySurvivesRestartRepro reproduces a security persistence
// bug: row-level security metadata is lost on restart, so a restricted reader
// can see rows that should remain hidden by the policy.
func TestRowLevelSecuritySurvivesRestartRepro(t *testing.T) {
	dbDir := t.TempDir()
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, connection, controller := CreateServerLocalInDirWithPort(t, "postgres", dbDir, port)
	for _, query := range []string{
		`CREATE USER restart_rls_reader PASSWORD 'reader';`,
		`CREATE TABLE restart_rls_docs (
			id INT PRIMARY KEY,
			owner_name TEXT,
			label TEXT
		);`,
		`INSERT INTO restart_rls_docs VALUES
			(1, 'restart_rls_reader', 'visible'),
			(2, 'other_user', 'hidden');`,
		`GRANT USAGE ON SCHEMA public TO restart_rls_reader;`,
		`GRANT SELECT ON restart_rls_docs TO restart_rls_reader;`,
		`CREATE POLICY restart_rls_docs_owner_select
			ON restart_rls_docs
			FOR SELECT
			USING (owner_name = current_user);`,
		`ALTER TABLE restart_rls_docs ENABLE ROW LEVEL SECURITY;`,
	} {
		_, err = connection.Exec(ctx, query)
		require.NoError(t, err, query)
	}

	assertReaderRows := func(expectedLabels []string) {
		t.Helper()
		readerConn, err := pgx.Connect(ctx, fmt.Sprintf(
			"postgres://restart_rls_reader:reader@127.0.0.1:%d/postgres?sslmode=disable",
			port,
		))
		require.NoError(t, err)
		defer readerConn.Close(ctx)

		rows, err := readerConn.Query(ctx, `SELECT label FROM restart_rls_docs ORDER BY id;`)
		require.NoError(t, err)
		defer rows.Close()

		var labels []string
		for rows.Next() {
			var label string
			require.NoError(t, rows.Scan(&label))
			labels = append(labels, label)
		}
		require.NoError(t, rows.Err())
		require.Equal(t, expectedLabels, labels)
	}

	assertReaderRows([]string{"visible"})

	connection.Close(ctx)
	controller.Stop()
	require.NoError(t, controller.WaitForStop())

	ctx, connection, controller = CreateServerLocalInDirWithPort(t, "postgres", dbDir, port)
	defer func() {
		connection.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	assertReaderRows([]string{"visible"})

	var rlsEnabled bool
	require.NoError(t, connection.Current.QueryRow(ctx, `
		SELECT relrowsecurity
		FROM pg_catalog.pg_class
		WHERE oid = 'restart_rls_docs'::regclass;`).Scan(&rlsEnabled))
	require.True(t, rlsEnabled)
}
