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

// TestPrismaDbPull runs the actual Prisma CLI introspection path against a
// live Doltgres instance. This broadens the migration-tool proof beyond
// drizzle-kit by exercising Prisma's catalog queries and schema renderer.
func TestPrismaDbPull(t *testing.T) {
	if _, err := exec.LookPath("npm"); err != nil {
		t.Skip("npm not available; install Node.js to enable this harness")
	}
	if testing.Short() {
		t.Skip("Prisma installs native query-engine packages; skipped under -short")
	}

	port, err := gms.GetEmptyPort()
	require.NoError(t, err)
	ctx, defaultConn, controller := CreateServerWithPort(t, "postgres", port)
	t.Cleanup(func() {
		defaultConn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	setUp := []string{
		`CREATE TABLE prisma_customers (
            id INT PRIMARY KEY,
            email TEXT UNIQUE NOT NULL,
            metadata JSONB NOT NULL,
            created_at TIMESTAMP DEFAULT now()
        );`,
		`CREATE TABLE prisma_orders (
            id INT PRIMARY KEY,
            customer_id INT NOT NULL REFERENCES prisma_customers(id),
            status TEXT NOT NULL,
            tags TEXT[] NOT NULL,
            total NUMERIC(10, 2) NOT NULL
        );`,
		`CREATE INDEX prisma_orders_customer_idx ON prisma_orders(customer_id);`,
		`CREATE TABLE prisma_order_items (
            order_id INT REFERENCES prisma_orders(id),
            line_no INT,
            sku TEXT NOT NULL,
            PRIMARY KEY (order_id, line_no)
        );`,
	}
	for _, q := range setUp {
		_, err := defaultConn.Exec(ctx, q)
		require.NoError(t, err, "setup: %s", q)
	}

	work := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(work, "package.json"), []byte(`{
  "name": "doltgres-prisma-db-pull-harness",
  "private": true
}
`), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(work, "schema.prisma"), []byte(`datasource db {
  provider = "postgresql"
  url      = env("DATABASE_URL")
}
`), 0o644))

	cmdCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	install := exec.CommandContext(cmdCtx, "npm", "install", "--silent",
		"--no-audit", "--no-fund", "prisma@6.19.3",
	)
	install.Dir = work
	install.Env = append(os.Environ(), "NO_COLOR=1")
	if out, err := install.CombinedOutput(); err != nil {
		t.Fatalf("npm install prisma failed: %v\n%s", err, string(out))
	}

	url := fmt.Sprintf("postgres://postgres:password@127.0.0.1:%d/postgres?sslmode=disable", port)
	cmd := exec.CommandContext(cmdCtx,
		filepath.Join(work, "node_modules", ".bin", "prisma"),
		"db", "pull",
		"--schema", "schema.prisma",
		"--print",
	)
	cmd.Dir = work
	cmd.Env = append(os.Environ(), "NO_COLOR=1", "DATABASE_URL="+url)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "prisma db pull failed: %s", string(out))
	schema := string(out)

	for _, needle := range []string{
		"model prisma_customers",
		"model prisma_orders",
		"model prisma_order_items",
		"@unique",
		"Json",
		"String[]",
		"Decimal",
		"@@id([order_id, line_no])",
		"@@index([customer_id]",
	} {
		require.Contains(t, schema, needle, "prisma schema missing %q\nschema:\n%s", needle, schema)
	}
	if !strings.Contains(schema, "prisma_customers") || !strings.Contains(schema, "prisma_orders") {
		t.Fatalf("prisma schema did not include both related tables:\n%s", schema)
	}
}
