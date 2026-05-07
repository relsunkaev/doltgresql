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

// TestSelectStarFieldMetadata exercises the wire-protocol RowDescription
// metadata that GUI editors (TablePlus, DataGrip, DBeaver, pgAdmin)
// inspect to decide whether a result-set grid is editable. PostgreSQL's
// RowDescription assigns each column a (TableOID, AttributeNumber)
// pair so the client can trace a result column back to a base table
// and generate UPDATE statements when the user edits a cell.
//
// Doltgres previously hard-coded TableOID=0 for every column, which
// caused TablePlus to refuse edits with "could not resolve table name."
// This test asserts the metadata is populated for plain SELECT * over
// base tables and stays zero for derived columns the editor cannot
// safely attribute (literals, computed expressions).
func TestSelectStarFieldMetadata(t *testing.T) {
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

	conn := dial(t)
	for _, q := range []string{
		`CREATE TABLE editable (id INT PRIMARY KEY, name TEXT, hits INT);`,
		`INSERT INTO editable VALUES (1, 'a', 10), (2, 'b', 20);`,
	} {
		_, err := conn.Exec(ctx, q)
		require.NoError(t, err, "setup: %s", q)
	}

	t.Run("SELECT * column metadata names the source table", func(t *testing.T) {
		// Resolve the expected table OID via pg_class first so the
		// editable connection isn't busy when we read FieldDescriptions.
		var expectedTableOID uint32
		require.NoError(t, conn.QueryRow(context.Background(),
			`SELECT c.oid FROM pg_catalog.pg_class c
			 JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
			 WHERE c.relname = 'editable' AND n.nspname = 'public';`).
			Scan(&expectedTableOID))
		require.NotZero(t, expectedTableOID,
			"pg_class missing entry for public.editable")

		rows, err := conn.Query(ctx, "SELECT * FROM editable;")
		require.NoError(t, err)
		defer rows.Close()
		fields := rows.FieldDescriptions()
		require.Len(t, fields, 3)

		for i, f := range fields {
			if f.TableOID == 0 {
				t.Errorf("column %d %q has TableOID=0; GUI editors require a non-zero source table OID", i, f.Name)
			}
			if f.TableOID != expectedTableOID {
				t.Errorf("column %d %q TableOID=%d; want %d (public.editable in pg_class)",
					i, f.Name, f.TableOID, expectedTableOID)
			}
			if f.TableAttributeNumber == 0 {
				t.Errorf("column %d %q has TableAttributeNumber=0; GUI editors require a non-zero attnum", i, f.Name)
			}
		}
	})

	t.Run("computed-expression columns keep TableOID=0", func(t *testing.T) {
		rows, err := conn.Query(ctx, "SELECT id, hits + 1 AS h FROM editable;")
		require.NoError(t, err)
		defer rows.Close()
		fields := rows.FieldDescriptions()
		require.Len(t, fields, 2)
		// id: traceable to the base table.
		require.NotZero(t, fields[0].TableOID,
			"base-column 'id' must carry source table OID")
		// hits + 1: derived. TableOID 0 here is correct — the editor
		// should not offer to edit a computed value.
		require.Zero(t, fields[1].TableOID,
			"derived column 'h' must not advertise a source table")
	})

	t.Run("aliased base column resolves through the AliasedExpr unwrap", func(t *testing.T) {
		// Aliased projections route through plan.Project's AliasedExpr,
		// which strips Source from the schema column GMS hands back.
		// Keeping the source table OID through that unwrap would let
		// GUI editors edit `SELECT name AS x FROM t` results too. That
		// path isn't fixed yet — this subtest documents the gap.
		t.Skip("Source attribution through AliasedExpr is a follow-up; SELECT * is the dominant editor case")
	})

	t.Run("reordered SELECT preserves real attnum, not result position", func(t *testing.T) {
		// Resolve attnum for each base column from pg_attribute first
		// so the assertion fails with a concrete diff if the wire
		// metadata drifts from the catalog.
		var attHits, attName, attID int16
		require.NoError(t, conn.QueryRow(context.Background(),
			`SELECT a.attnum FROM pg_catalog.pg_attribute a
			 JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
			 JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
			 WHERE c.relname = 'editable' AND n.nspname = 'public' AND a.attname = 'hits';`).
			Scan(&attHits))
		require.NoError(t, conn.QueryRow(context.Background(),
			`SELECT a.attnum FROM pg_catalog.pg_attribute a
			 JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
			 JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
			 WHERE c.relname = 'editable' AND n.nspname = 'public' AND a.attname = 'name';`).
			Scan(&attName))
		require.NoError(t, conn.QueryRow(context.Background(),
			`SELECT a.attnum FROM pg_catalog.pg_attribute a
			 JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
			 JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
			 WHERE c.relname = 'editable' AND n.nspname = 'public' AND a.attname = 'id';`).
			Scan(&attID))
		require.NotZero(t, attHits)
		require.NotZero(t, attName)
		require.NotZero(t, attID)

		rows, err := conn.Query(ctx, "SELECT hits, name, id FROM editable;")
		require.NoError(t, err)
		defer rows.Close()
		fields := rows.FieldDescriptions()
		require.Len(t, fields, 3)
		// Each column must report its source-table attnum, not the
		// position within the SELECT list (which would have been
		// 1, 2, 3 — happens to be wrong for at least 'id' here).
		require.Equal(t, uint16(attHits), fields[0].TableAttributeNumber,
			"hits attnum: column 0")
		require.Equal(t, uint16(attName), fields[1].TableAttributeNumber,
			"name attnum: column 1")
		require.Equal(t, uint16(attID), fields[2].TableAttributeNumber,
			"id attnum: column 2")
	})
}
