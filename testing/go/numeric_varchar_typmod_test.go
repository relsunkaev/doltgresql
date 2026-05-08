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

	gms "github.com/dolthub/go-mysql-server/sql"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// TestNumericAndVarcharAtttypmod verifies that NUMERIC(p,s) and
// VARCHAR(n) precision/scale survive CREATE TABLE -> pg_attribute ->
// format_type round-trip the same way TIMESTAMP(p)/TIME(p) already
// do. ORM introspection tools (drizzle-kit, prisma db pull,
// SQLAlchemy reflect, JDBC ResultSetMetaData) read pg_attribute's
// atttypmod alongside atttypid to rebuild the original DDL; if
// the precision/scale is lost, an introspect-then-emit pipeline
// produces wrong column types.
//
// PG's atttypmod encoding:
//
//   - NUMERIC(p,s) -> ((p << 16) | s) + 4
//   - VARCHAR(n)   -> n + 4
//   - default (no precision)  -> -1
//
// format_type wraps atttypmod into the original "NUMERIC(p,s)" /
// "VARCHAR(n)" textual form, which is what the ORMs actually
// consume.
func TestNumericAndVarcharAtttypmod(t *testing.T) {
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
		`CREATE TABLE typmod_t (
			id INT PRIMARY KEY,
			price NUMERIC(10, 2),
			pct NUMERIC(5, 4),
			big NUMERIC(38, 0),
			plain_num NUMERIC,
			short_label VARCHAR(20),
			long_label VARCHAR(255),
			plain_text VARCHAR
		);`,
	} {
		_, err := defaultConn.Exec(ctx, q)
		require.NoError(t, err, "setup: %s", q)
	}

	// atttypmod values per PG's typmod_in encoding.
	cases := []struct {
		column       string
		expectTypmod int32
		expectFormat string
	}{
		{"price", ((10 << 16) | 2) + 4, "numeric(10,2)"},
		{"pct", ((5 << 16) | 4) + 4, "numeric(5,4)"},
		{"big", ((38 << 16) | 0) + 4, "numeric(38,0)"},
		{"plain_num", -1, "numeric"},
		{"short_label", 20 + 4, "character varying(20)"},
		{"long_label", 255 + 4, "character varying(255)"},
		{"plain_text", -1, "character varying"},
	}

	for _, tc := range cases {
		t.Run(tc.column+"_atttypmod", func(t *testing.T) {
			var got int32
			err := conn.QueryRow(ctx, `
				SELECT a.atttypmod
				FROM pg_catalog.pg_attribute a
				JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
				JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
				WHERE n.nspname = 'public'
				  AND c.relname = 'typmod_t'
				  AND a.attname = $1
			`, tc.column).Scan(&got)
			require.NoError(t, err)
			require.Equal(t, tc.expectTypmod, got,
				"atttypmod for %s: got %d, want %d", tc.column, got, tc.expectTypmod)
		})
		t.Run(tc.column+"_format_type", func(t *testing.T) {
			var got string
			err := conn.QueryRow(ctx, `
				SELECT format_type(a.atttypid, a.atttypmod)
				FROM pg_catalog.pg_attribute a
				JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
				JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
				WHERE n.nspname = 'public'
				  AND c.relname = 'typmod_t'
				  AND a.attname = $1
			`, tc.column).Scan(&got)
			require.NoError(t, err)
			require.Equal(t, tc.expectFormat, got,
				"format_type for %s", tc.column)
		})
	}
}
