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
	"errors"
	"fmt"
	"testing"

	gms "github.com/dolthub/go-mysql-server/sql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

// TestSQLStateCodes asserts that the SQLSTATE codes Doltgres reports
// for common error conditions match what real PostgreSQL emits.
// Drivers and ORMs branch on these to decide whether an error is
// retryable, whether to fall back to a different code path, or
// whether to surface a typed exception:
//
//   - 23505 unique_violation: ORMs treat as upsert-fallback signal.
//   - 23503 foreign_key_violation: triggers cascade / retry logic.
//   - 23502 not_null_violation: surfaces field-validation errors.
//   - 23514 check_violation: surfaces constraint violations.
//   - 42P01 undefined_table / 42703 undefined_column: schema migration drift.
//
// Doltgres previously fell back to XX000 internal_error for everything
// except DuplicateObject, so every constraint failure looked like a
// server fault to drivers.
func TestSQLStateCodes(t *testing.T) {
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

	for _, q := range []string{
		`CREATE TABLE parent (id INT PRIMARY KEY, name TEXT NOT NULL);`,
		`CREATE TABLE child  (id INT PRIMARY KEY, parent_id INT REFERENCES parent(id));`,
		`CREATE TABLE udq    (id INT PRIMARY KEY, email TEXT UNIQUE, age INT CHECK (age >= 0));`,
		`INSERT INTO parent VALUES (1, 'p1'), (2, 'p2');`,
		`INSERT INTO child  VALUES (1, 2);`,
		`INSERT INTO udq    VALUES (1, 'a@x.com', 5);`,
	} {
		_, err := conn.Exec(ctx, q)
		require.NoError(t, err, "setup: %s", q)
	}

	cases := []struct {
		name string
		sql  string
		code string
	}{
		{
			name: "duplicate primary key -> 23505",
			sql:  "INSERT INTO parent VALUES (1, 'dup');",
			code: "23505",
		},
		{
			name: "duplicate unique key -> 23505",
			sql:  "INSERT INTO udq VALUES (2, 'a@x.com', 1);",
			code: "23505",
		},
		{
			name: "null into NOT NULL -> 23502",
			sql:  "INSERT INTO parent (id, name) VALUES (2, NULL);",
			code: "23502",
		},
		{
			name: "FK child violation -> 23503",
			sql:  "INSERT INTO child (id, parent_id) VALUES (1, 999);",
			code: "23503",
		},
		{
			name: "FK parent violation on delete -> 23503",
			sql:  "DELETE FROM parent WHERE id = 2;",
			code: "23503",
		},
		{
			name: "check constraint violation -> 23514",
			sql:  "INSERT INTO udq VALUES (3, 'c@x.com', -1);",
			code: "23514",
		},
		{
			name: "undefined table -> 42P01",
			sql:  "SELECT * FROM nope_nope_nope;",
			code: "42P01",
		},
		{
			name: "undefined column -> 42703",
			sql:  "SELECT nope FROM parent;",
			code: "42703",
		},
		{
			// DO NOTHING multi-unique with a non-target conflict
			// surfaces the unique-violation through the pre-check
			// wrapper (rather than being silently swallowed by
			// INSERT IGNORE). udq has id=1 / email='a@x.com'; the
			// new row reuses email='a@x.com' on a different id,
			// triggering the email index — which is non-target for
			// `ON CONFLICT (id)`.
			name: "DO NOTHING multi-unique non-target conflict -> 23505",
			sql:  "INSERT INTO udq VALUES (2, 'a@x.com', 1) ON CONFLICT (id) DO NOTHING;",
			code: "23505",
		},
		{
			name: "CREATE EVENT TRIGGER unsupported boundary -> 42501",
			sql:  "CREATE EVENT TRIGGER ddl_audit ON ddl_command_end EXECUTE FUNCTION audit_fn();",
			code: "42501",
		},
		{
			name: "CREATE COLLATION unsupported boundary -> 0A000",
			sql:  "CREATE COLLATION case_insensitive (provider = icu, locale = 'und-u-ks-level2', deterministic = false);",
			code: "0A000",
		},
		{
			name: "EXCLUDE constraint unsupported boundary -> 0A000",
			sql:  "CREATE TABLE bookings (id INT PRIMARY KEY, room_id INT, period TEXT, EXCLUDE USING gist (room_id WITH =, period WITH &&));",
			code: "0A000",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := conn.Exec(ctx, tc.sql)
			requireSQLState(t, err, tc.code, tc.sql)
		})
	}
}

func TestSQLStateSerializationFailure(t *testing.T) {
	port, err := gms.GetEmptyPort()
	require.NoError(t, err)
	ctx, defaultConn, controller := CreateServerWithPort(t, "postgres", port)
	t.Cleanup(func() {
		defaultConn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	dial := func(t *testing.T) *pgx.Conn {
		t.Helper()
		conn, err := pgx.Connect(ctx, fmt.Sprintf(
			"postgres://postgres:password@127.0.0.1:%d/postgres?sslmode=disable", port))
		require.NoError(t, err)
		t.Cleanup(func() { _ = conn.Close(ctx) })
		return conn
	}

	_, err = defaultConn.Exec(ctx, `CREATE TABLE sqlstate_retry_t (id INT PRIMARY KEY, v INT);`)
	require.NoError(t, err)
	_, err = defaultConn.Exec(ctx, `INSERT INTO sqlstate_retry_t VALUES (1, 1);`)
	require.NoError(t, err)

	a := dial(t)
	b := dial(t)

	_, err = a.Exec(ctx, `BEGIN;`)
	require.NoError(t, err)
	_, err = b.Exec(ctx, `BEGIN;`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = a.Exec(context.Background(), `ROLLBACK;`)
		_, _ = b.Exec(context.Background(), `ROLLBACK;`)
	})

	_, err = a.Exec(ctx, `INSERT INTO sqlstate_retry_t VALUES (2, 2);`)
	require.NoError(t, err)
	_, err = b.Exec(ctx, `INSERT INTO sqlstate_retry_t VALUES (2, 3);`)
	require.NoError(t, err)
	_, err = a.Exec(ctx, `COMMIT;`)
	require.NoError(t, err)

	_, err = b.Exec(ctx, `COMMIT;`)
	requireSQLState(t, err, "40001", `COMMIT after concurrent conflicting insert`)
}

func TestSQLStateDatetimeFieldOverflow(t *testing.T) {
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
		name string
		sql  string
	}{
		{
			name: "make_timestamp date field overflow",
			sql:  `SELECT make_timestamp(0, 1, 1, 0, 0, 0);`,
		},
		{
			name: "make_timestamp time field overflow",
			sql:  `SELECT make_timestamp(2026, 1, 1, 24, 0, 0);`,
		},
		{
			name: "to_timestamp float8 output overflow",
			sql:  `SELECT to_timestamp(9223372037.0);`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := conn.Exec(ctx, tc.sql)
			requireSQLState(t, err, "22008", tc.sql)
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
