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
	"strings"
	"testing"

	gms "github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
)

// TestPgDumpSchemaOnly runs the real pg_dump binary against Doltgres. This
// pins the catalog-query surface pg_dump needs before any dumped schema can be
// round-tripped back through psql.
func TestPgDumpSchemaOnly(t *testing.T) {
	pgDump, err := exec.LookPath("pg_dump")
	if err != nil {
		t.Skip("pg_dump not available; install PostgreSQL client tools to enable this harness")
	}

	port, err := gms.GetEmptyPort()
	require.NoError(t, err)
	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	t.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	setup := []string{
		`CREATE EXTENSION IF NOT EXISTS pgcrypto;`,
		`CREATE TABLE pg_dump_customers (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			email TEXT UNIQUE NOT NULL,
			created_at TIMESTAMP DEFAULT now()
		);`,
		`CREATE TABLE pg_dump_orders (
			id INT PRIMARY KEY,
			customer_id UUID NOT NULL REFERENCES pg_dump_customers(id),
			status TEXT NOT NULL,
			total NUMERIC(10,2) NOT NULL
		);`,
		`CREATE INDEX pg_dump_orders_customer_idx ON pg_dump_orders(customer_id);`,
		`CREATE VIEW pg_dump_recent_orders AS
			SELECT o.id, c.email, o.status, o.total
			FROM pg_dump_orders o
			JOIN pg_dump_customers c ON c.id = o.customer_id
			WHERE o.status <> 'archived';`,
	}
	for _, query := range setup {
		_, err := conn.Exec(ctx, query)
		require.NoError(t, err, "setup query failed: %s", query)
	}

	cmdCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	url := fmt.Sprintf("postgresql://postgres:password@127.0.0.1:%d/postgres?sslmode=disable", port)
	cmd := exec.CommandContext(cmdCtx, pgDump,
		"--schema-only",
		"--no-owner",
		"--no-privileges",
		"--dbname", url,
	)
	cmd.Env = append(os.Environ(), "NO_COLOR=1", "PGPASSWORD=password")
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "pg_dump --schema-only failed:\n%s", string(out))

	dump := string(out)
	for _, needle := range []string{
		"CREATE TABLE public.pg_dump_customers",
		"CREATE TABLE public.pg_dump_orders",
		"id uuid DEFAULT gen_random_uuid() NOT NULL",
		"created_at timestamp without time zone DEFAULT now()",
		"CREATE VIEW public.pg_dump_recent_orders",
		"WHERE o.status <> 'archived';",
		"CREATE INDEX pg_dump_orders_customer_idx",
		"FOREIGN KEY (customer_id) REFERENCES public.pg_dump_customers(id)",
	} {
		require.Truef(t, strings.Contains(dump, needle), "dump missing %q\n%s", needle, dump)
	}
}
