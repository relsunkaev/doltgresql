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

package postgres16

import (
	. "github.com/dolthub/doltgresql/testing/go"

	"errors"
	"fmt"
	"testing"

	gms "github.com/dolthub/go-mysql-server/sql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

// TestUnsupportedDdlProbes pins the rejection contracts for DDL
// shapes that real PG dumps and migrations emit but are not yet
// supported in doltgresql. Pinning the rejection contracts means
// that dump-rewrite tooling has a stable error string to filter on,
// and that any future incidental support that "starts working"
// without the engineering to back it surfaces as a test break.
// Per the Schema/DDL TODO in
// docs/app-compatibility-checklist.md.
func TestUnsupportedDdlProbes(t *testing.T) {
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

	for _, tc := range []struct {
		name     string
		setup    []string
		query    string
		sqlstate string
	}{
		{
			name: "CREATE AGGREGATE is rejected",
			query: `CREATE AGGREGATE my_sum (int) (
				sfunc = int4pl,
				stype = int
			);`,
			sqlstate: "0A000",
		},
		{
			name: "EXCLUDE constraint via CREATE TABLE is rejected",
			query: `CREATE TABLE bookings (
				id INT PRIMARY KEY,
				room_id INT,
				period TEXT,
				EXCLUDE USING gist (room_id WITH =, period WITH &&)
			);`,
			sqlstate: "0A000",
		},
		{
			name: "BEFORE trigger with REFERENCING NEW TABLE is rejected",
			setup: []string{
				`CREATE TABLE t (id INT PRIMARY KEY, v INT);`,
				`CREATE FUNCTION audit_fn() RETURNS trigger AS $$
					BEGIN
						RETURN NULL;
					END;
				$$ LANGUAGE plpgsql;`,
			},
			query: `CREATE TRIGGER tg
				BEFORE INSERT ON t
				REFERENCING NEW TABLE AS new_rows
				FOR EACH STATEMENT EXECUTE FUNCTION audit_fn();`,
			sqlstate: "XX000",
		},
		{
			name: "CREATE EVENT TRIGGER is rejected",
			query: `CREATE EVENT TRIGGER ddl_audit
				ON ddl_command_end
				EXECUTE FUNCTION audit_fn();`,
			sqlstate: "42501",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, statement := range tc.setup {
				_, err := conn.Exec(ctx, statement)
				require.NoError(t, err, "setup: %s", statement)
			}
			_, err := conn.Exec(ctx, tc.query)
			requireSQLState(t, err, tc.sqlstate, tc.query)
		})
	}
}

func requireSQLState(t *testing.T, err error, code string, errContext string) {
	t.Helper()
	require.Error(t, err, "expected error for %q", errContext)
	var pgErr *pgconn.PgError
	require.True(t, errors.As(err, &pgErr),
		"expected *pgconn.PgError, got %T: %v", err, err)
	require.Equal(t, code, pgErr.Code,
		"SQLSTATE for %q: got %q (%s), want %q", errContext, pgErr.Code, pgErr.Message, code)
}
