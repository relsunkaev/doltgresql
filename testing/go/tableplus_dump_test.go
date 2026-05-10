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

// TestTablePlusBundledPgDumpSchemaOnly runs the PostgreSQL dump binary bundled
// with the macOS TablePlus app against live Doltgres. This is not full GUI
// automation, but it pins one real TablePlus-distributed PostgreSQL tooling
// binary instead of relying only on system PostgreSQL client packages.
func TestTablePlusBundledPgDumpSchemaOnly(t *testing.T) {
	const resources = "/Applications/TablePlus.app/Contents/Resources"
	pgDump := filepath.Join(resources, "dump_pg_17.0")
	if _, err := os.Stat(pgDump); err != nil {
		t.Skip("TablePlus bundled pg_dump not available; install TablePlus.app to enable this harness")
	}

	binaryEnv := append(os.Environ(),
		"DYLD_LIBRARY_PATH="+resources,
		"NO_COLOR=1",
	)
	version := exec.Command(pgDump, "--version")
	version.Env = binaryEnv
	if out, err := version.CombinedOutput(); err != nil {
		t.Fatalf("TablePlus bundled pg_dump is not runnable: %v\n%s", err, string(out))
	} else if !strings.Contains(string(out), "pg_dump (PostgreSQL) 17.0") {
		t.Fatalf("unexpected TablePlus pg_dump version: %s", string(out))
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
		`CREATE TABLE tableplus_accounts (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			email TEXT UNIQUE NOT NULL,
			created_at TIMESTAMP DEFAULT now()
		);`,
		`CREATE TABLE tableplus_invoices (
			id INT PRIMARY KEY,
			account_id UUID NOT NULL REFERENCES tableplus_accounts(id),
			status TEXT NOT NULL,
			total NUMERIC(10,2) NOT NULL,
			payload JSONB NOT NULL
		);`,
		`CREATE INDEX tableplus_invoices_account_idx ON tableplus_invoices(account_id);`,
		`CREATE VIEW tableplus_open_invoices AS
			SELECT i.id, a.email, i.total, i.payload
			FROM tableplus_invoices i
			JOIN tableplus_accounts a ON a.id = i.account_id
			WHERE i.status <> 'paid';`,
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
	cmd.Env = append(binaryEnv, "PGPASSWORD=password")
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "TablePlus bundled pg_dump --schema-only failed:\n%s", string(out))

	dump := string(out)
	for _, needle := range []string{
		"CREATE TABLE public.tableplus_accounts",
		"CREATE TABLE public.tableplus_invoices",
		"id uuid DEFAULT gen_random_uuid() NOT NULL",
		"payload jsonb NOT NULL",
		"CREATE VIEW public.tableplus_open_invoices",
		"WHERE i.status != 'paid';",
		"CREATE INDEX tableplus_invoices_account_idx",
		"FOREIGN KEY (account_id) REFERENCES public.tableplus_accounts(id)",
	} {
		require.Truef(t, strings.Contains(dump, needle), "dump missing %q\n%s", needle, dump)
	}
}
