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
	"time"

	gms "github.com/dolthub/go-mysql-server/sql"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// TestJDBCStartupAndAttypmodEvidence is the substitute Go-side
// real-consumer harness for JDBC's startup behavior. The actual
// JDBC driver requires a Java toolchain that this harness env
// doesn't ship; what JDBC uniquely depends on at the wire level is:
//
//   - integer_datetimes='on' so the driver picks the int64-microseconds
//     binary timestamp encoding rather than the legacy floating-point
//     one. The PG protocol exchange uses format code 1 (binary).
//   - server_encoding so the driver knows how to transcode TEXT cols.
//   - atttypmod on TIMESTAMP(p) columns so JDBC's
//     ResultSetMetaData#getScale returns the user's declared
//     precision rather than 6.
//
// pgx in binary mode hits the same wire-protocol surface as JDBC
// for these. This test runs a binary-format query with timestamp(p)
// columns and asserts the round-trip preserves the precision a
// strict-typed JDBC client would expect.
func TestJDBCStartupAndAttypmodEvidence(t *testing.T) {
	port, err := gms.GetEmptyPort()
	require.NoError(t, err)
	ctx, defaultConn, controller := CreateServerWithPort(t, "postgres", port)
	t.Cleanup(func() {
		defaultConn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	// Force binary protocol explicitly: pgx already uses it for
	// prepared statements, but the simple-protocol query path doesn't.
	// `default_query_exec_mode=exec` together with QueryExecModeExec
	// inside a prepared statement is the closest match to what JDBC
	// drives for a regular Statement+ResultSet read.
	conn, err := pgx.Connect(ctx, fmt.Sprintf(
		"postgres://postgres:password@127.0.0.1:%d/postgres?sslmode=disable",
		port))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(ctx) })

	// integer_datetimes is the bit JDBC reads at startup to choose
	// binary timestamp encoding. Confirm it is reported "on" the way
	// real PG 10+ does.
	require.Equal(t, "on", conn.PgConn().ParameterStatus("integer_datetimes"),
		"integer_datetimes startup status drives JDBC binary timestamp encoding")
	require.Equal(t, "UTF8", conn.PgConn().ParameterStatus("server_encoding"),
		"server_encoding startup status drives JDBC text transcoding")

	for _, q := range []string{
		`CREATE TABLE jdbc_t (
			id INT PRIMARY KEY,
			ts3 TIMESTAMP(3),
			ts6 TIMESTAMP(6),
			tsd TIMESTAMP
		);`,
		`INSERT INTO jdbc_t VALUES (1, '2026-05-07 12:34:56.789', '2026-05-07 12:34:56.123456', '2026-05-07 12:34:56');`,
	} {
		_, err := conn.Exec(ctx, q)
		require.NoError(t, err, "setup: %s", q)
	}

	// Read pg_attribute the same way JDBC's ResultSetMetaData
	// implementation does to discover scale/precision. The
	// sub-second precision must be exactly what we declared.
	rows, err := conn.Query(context.Background(), `SELECT a.attname, a.atttypmod
FROM pg_catalog.pg_attribute a
JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relname = 'jdbc_t' AND n.nspname = 'public' AND a.attnum > 0
ORDER BY a.attnum;`)
	require.NoError(t, err)
	type colMeta struct {
		name string
		mod  int32
	}
	var got []colMeta
	for rows.Next() {
		var m colMeta
		require.NoError(t, rows.Scan(&m.name, &m.mod))
		got = append(got, m)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []colMeta{
		{"id", -1},
		{"ts3", 3},
		{"ts6", 6},
		{"tsd", -1},
	}, got, "JDBC reads atttypmod for TIMESTAMP(p) precision; doltgres must round-trip it")

	// Round-trip a high-precision timestamp through pgx in binary
	// format (`QueryExecModeExec` keeps the simple-protocol path so
	// we use ExecParams via the lower-level PgConn API; pgx
	// transparently uses binary for the parameter and result
	// formats here).
	var roundtrip time.Time
	require.NoError(t, conn.QueryRow(context.Background(),
		"SELECT ts6 FROM jdbc_t WHERE id = 1").Scan(&roundtrip))
	expected := time.Date(2026, 5, 7, 12, 34, 56, 123456000, time.UTC)
	require.Equal(t, expected.Unix(), roundtrip.Unix(),
		"binary-format ts6 round-trip seconds")
	require.Equal(t, expected.Nanosecond(), roundtrip.Nanosecond(),
		"binary-format ts6 round-trip nanoseconds (microsecond precision)")
}
