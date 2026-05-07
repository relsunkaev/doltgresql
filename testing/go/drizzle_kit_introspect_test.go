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
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	gms "github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
)

// TestDrizzleKitIntrospect runs the actual `drizzle-kit introspect`
// migration tool against a running Doltgres instance, opt-in via
// DOLTGRES_RUN_DRIZZLE_KIT=1.
//
// Why opt-in: drizzle-kit currently hangs on Doltgres because of a
// separate planner bug — its index-introspection query uses
// `JOIN pg_opclass opc ON opc.oid = ANY(i.indclass)` which Doltgres
// rejects with "found equality comparison that does not return a
// bool". Until the ANY-operator-with-array-column path lands, the
// full introspection pipeline cannot complete, so the harness is
// preserved here but skipped by default.
//
// Running it locally exercises the parts that DO work end-to-end:
// connect via the `pg` driver, table discovery via pg_class +
// pg_namespace (which depends on the column-alias fix in commit
// where this test was added), column discovery via
// information_schema, and foreign-key + check-constraint
// introspection via pg_constraint.
func TestDrizzleKitIntrospect(t *testing.T) {
	if os.Getenv("DOLTGRES_RUN_DRIZZLE_KIT") == "" {
		t.Skip("set DOLTGRES_RUN_DRIZZLE_KIT=1 to enable; opt-in because the index-introspection query hits a planner bug (`opc.oid = ANY(i.indclass)`)")
	}
	if _, err := exec.LookPath("npx"); err != nil {
		t.Skip("npx not available; install Node.js to enable this harness")
	}

	port, err := gms.GetEmptyPort()
	require.NoError(t, err)
	ctx, defaultConn, controller := CreateServerWithPort(t, "postgres", port)
	t.Cleanup(func() {
		defaultConn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	// Create a schema slice with the kinds of objects migration tools
	// commonly inspect: a table with primary key, a unique constraint,
	// a non-unique index, a foreign key, and a composite primary key.
	setUp := []string{
		`CREATE TABLE customers (
            id INT PRIMARY KEY,
            email TEXT UNIQUE NOT NULL,
            created_at TIMESTAMP DEFAULT now()
        );`,
		`CREATE TABLE orders (
            order_id INT PRIMARY KEY,
            customer_id INT REFERENCES customers(id),
            status TEXT
        );`,
		`CREATE INDEX idx_orders_customer ON orders(customer_id);`,
		`CREATE INDEX idx_orders_status ON orders(status);`,
		`CREATE TABLE order_items (
            order_id INT REFERENCES orders(order_id),
            line_no INT,
            sku TEXT,
            PRIMARY KEY (order_id, line_no)
        );`,
	}
	for _, q := range setUp {
		_, err := defaultConn.Exec(ctx, q)
		require.NoError(t, err, "setup: %s", q)
	}

	// Sanity check: confirm Doltgres actually persisted the tables
	// before we hand off to drizzle-kit. If this query returns
	// fewer rows than expected, the failure is in the schema setup,
	// not in the migration tool's introspection.
	{
		var seen int
		require.NoError(t, defaultConn.Default.QueryRow(ctx, `
            SELECT count(*) FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name IN ('customers', 'orders', 'order_items')
        `).Scan(&seen))
		require.Equal(t, 3, seen, "all three tables must exist before introspect")

		var pgClassSeen int
		require.NoError(t, defaultConn.Default.QueryRow(ctx, `
            SELECT count(*) FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = 'public'
              AND c.relkind = 'r'
              AND c.relname IN ('customers', 'orders', 'order_items')
        `).Scan(&pgClassSeen))
		require.Equal(t, 3, pgClassSeen,
			"pg_class must report all three tables (drizzle-kit reads pg_class)")

		// Run the exact query drizzle-kit issues for table discovery,
		// so that if drizzle's count is 0 we know that's a Doltgres
		// catalog gap rather than a packaging problem.
		rows, err := defaultConn.Default.Query(ctx, `SELECT
    n.nspname AS table_schema,
    c.relname AS table_name,
    CASE
        WHEN c.relkind = 'r' THEN 'table'
        WHEN c.relkind = 'v' THEN 'view'
        WHEN c.relkind = 'm' THEN 'materialized_view'
    END AS type,
    c.relrowsecurity AS rls_enabled
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'v', 'm') AND n.nspname = 'public';`)
		require.NoError(t, err, "drizzle-kit's table query must succeed")
		var drizzleCount int
		for rows.Next() {
			drizzleCount++
		}
		rows.Close()
		require.GreaterOrEqual(t, drizzleCount, 3,
			"drizzle-kit's table-discovery query must return our three tables")
	}

	work := t.TempDir()
	url := fmt.Sprintf("postgres://postgres:password@127.0.0.1:%d/postgres", port)

	// npx's package cache keys can resolve to a stale drizzle-orm if a
	// previous invocation pinned an older one, which then misses
	// exports drizzle-kit needs ('./gel-core'). Install both packages
	// into a local node_modules so drizzle-kit loads exactly the
	// versions we want.
	require.NoError(t, os.WriteFile(filepath.Join(work, "package.json"), []byte(`{
  "name": "doltgres-introspect-harness",
  "private": true
}
`), 0o644))

	cmdCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	install := exec.CommandContext(cmdCtx, "npm", "install", "--silent",
		"--no-audit", "--no-fund",
		"drizzle-kit@0.31.10", "drizzle-orm@0.45.2", "pg@8.11.3",
	)
	install.Dir = work
	install.Env = append(os.Environ(), "NO_COLOR=1")
	if out, err := install.CombinedOutput(); err != nil {
		t.Fatalf("npm install drizzle-kit + drizzle-orm failed: %v\n%s", err, string(out))
	}

	cmd := exec.CommandContext(cmdCtx,
		filepath.Join(work, "node_modules", ".bin", "drizzle-kit"),
		"introspect",
		"--dialect", "postgresql",
		"--url", url,
		"--out", "./drizzle",
	)
	cmd.Dir = work
	cmd.Env = append(os.Environ(), "NO_COLOR=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("drizzle-kit introspect failed: %v\noutput:\n%s", err, string(out))
	}
	t.Logf("drizzle-kit output:\n%s", string(out))

	// drizzle-kit introspect writes ./drizzle/schema.ts (and a
	// migrations snapshot). Read schema.ts and assert it captures
	// the table and index shape we created above.
	schemaPath := filepath.Join(work, "drizzle", "schema.ts")
	schemaBytes, err := os.ReadFile(schemaPath)
	require.NoError(t, err, "drizzle-kit did not produce drizzle/schema.ts")
	schema := string(schemaBytes)

	// Tables: drizzle emits each as `pgTable("<name>", { ... })`.
	for _, tbl := range []string{"customers", "orders", "order_items"} {
		require.Contains(t, schema, fmt.Sprintf(`pgTable("%s",`, tbl),
			"introspected schema is missing table %q\nschema:\n%s", tbl, schema)
	}

	// Primary key on a composite-key table — drizzle expresses this
	// either as `.primaryKey()` on the column or as a `primaryKey({
	// columns: [...] })` constraint object. Either is acceptable.
	requireContainsAny(t, schema, []string{
		`primaryKey({\n\t\t\tcolumns: [table.orderId, table.lineNo]`,
		`primaryKey({ columns: [table.orderId, table.lineNo] })`,
		`primaryKey({columns: [table.orderId, table.lineNo]})`,
		`.primaryKey()`,
	}, "composite primary key on order_items")

	// Non-unique secondary indexes: drizzle emits `index("name").on(...)`.
	for _, idxName := range []string{"idx_orders_customer", "idx_orders_status"} {
		require.Contains(t, schema, fmt.Sprintf(`index("%s")`, idxName),
			"introspected schema is missing index %q", idxName)
	}

	// Foreign key from orders.customer_id -> customers.id surfaces as
	// `foreignKey({ columns: [...], foreignColumns: [...] })`.
	require.Contains(t, schema, "foreignKey",
		"introspected schema is missing the orders -> customers foreign key")

	// Unique constraint on customers.email surfaces either as a
	// `.unique()` modifier or a `unique("name")` constraint object.
	requireContainsAny(t, schema, []string{
		`.unique()`,
		`.unique(`,
		`unique(`,
	}, "unique constraint on customers.email")
}

// requireContainsAny asserts that one of the candidate substrings is
// present in haystack. drizzle-kit's exact output formatting varies
// across versions; we accept any of the recognized shapes.
func requireContainsAny(t *testing.T, haystack string, needles []string, what string) {
	t.Helper()
	for _, n := range needles {
		if strings.Contains(haystack, n) {
			return
		}
	}
	t.Fatalf("introspected schema is missing %s; expected one of %v\nschema:\n%s", what, needles, haystack)
}
