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
	"runtime"
	"sync"
	"testing"

	gms "github.com/dolthub/go-mysql-server/sql"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestPgDumpPartitionedTableOpclassProbe(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_dump partitioned table opclass probe resolves regnamespace in scalar subquery",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT partrelid FROM pg_catalog.pg_partitioned_table WHERE
(SELECT c.oid FROM pg_catalog.pg_opclass c JOIN pg_catalog.pg_am a ON c.opcmethod = a.oid
WHERE opcname = 'enum_ops' AND opcnamespace = 'pg_catalog'::regnamespace AND amname = 'hash') = ANY(partclass);`,
					Expected: []gms.Row{},
				},
			},
		},
	})
}

func TestPgDumpAlterColumnSetStorageProbe(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_dump column storage metadata is accepted",
			SetUpScript: []string{
				`CREATE TABLE storage_probe (id INT PRIMARY KEY, payload TEXT);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `ALTER TABLE ONLY public.storage_probe ALTER COLUMN payload SET STORAGE EXTENDED;`,
					Expected: []gms.Row{},
				},
				{
					Query:    `INSERT INTO storage_probe VALUES (1, 'ok');`,
					Expected: []gms.Row{},
				},
				{
					Query:    `SELECT payload FROM storage_probe WHERE id = 1;`,
					Expected: []gms.Row{{"ok"}},
				},
			},
		},
	})
}

func TestPgDependViewTableDependencies(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_depend exposes view dependencies on referenced tables",
			SetUpScript: []string{
				`CREATE TABLE dep_accounts (id INT PRIMARY KEY, email TEXT);`,
				`CREATE TABLE dep_projects (id INT PRIMARY KEY, account_id INT REFERENCES dep_accounts(id));`,
				`CREATE VIEW dep_active_projects AS
					SELECT p.id, a.email
					FROM dep_projects p
					JOIN dep_accounts a ON a.id = p.account_id;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT view_class.relname, ref_class.relname, d.deptype
FROM pg_catalog.pg_depend d
JOIN pg_catalog.pg_class view_class ON view_class.oid = d.objid
JOIN pg_catalog.pg_class ref_class ON ref_class.oid = d.refobjid
WHERE d.classid = 'pg_class'::regclass
	AND d.refclassid = 'pg_class'::regclass
	AND view_class.relname = 'dep_active_projects'
ORDER BY ref_class.relname;`,
					Expected: []gms.Row{
						{"dep_active_projects", "dep_accounts", "n"},
						{"dep_active_projects", "dep_projects", "n"},
					},
				},
				{
					Query: `SELECT
	strpos(pg_get_viewdef('dep_active_projects'::regclass), 'public.dep_projects') > 0,
	strpos(pg_get_viewdef('dep_active_projects'::regclass), 'public.dep_accounts') > 0;`,
					Expected: []gms.Row{{"t", "t"}},
				},
			},
		},
	})
}

func TestPgDumpPsqlRestoreDrizzleRoundTrip(t *testing.T) {
	pgDump, err := exec.LookPath("pg_dump")
	if err != nil {
		t.Skip("pg_dump not available; install PostgreSQL client tools to enable this harness")
	}
	psql, err := exec.LookPath("psql")
	if err != nil {
		t.Skip("psql not available; install PostgreSQL client tools to enable this harness")
	}
	if _, err = exec.LookPath("npm"); err != nil {
		t.Skip("npm not available; install Node.js to enable the drizzle-kit introspection step")
	}
	if testing.Short() {
		t.Skip("round-trip harness installs drizzle-kit; skipped under -short")
	}

	ctx := context.Background()
	sourcePort, err := gms.GetEmptyPort()
	require.NoError(t, err)
	sourceCtx, sourceConn, sourceController := CreateServerWithPort(t, "postgres", sourcePort)
	var stopSourceOnce sync.Once
	stopSource := func() {
		stopSourceOnce.Do(func() {
			sourceConn.Close(sourceCtx)
			sourceController.Stop()
			require.NoError(t, sourceController.WaitForStop())
		})
	}
	t.Cleanup(func() {
		stopSource()
	})

	sourceSetup := []string{
		`CREATE EXTENSION IF NOT EXISTS pgcrypto;`,
		`CREATE TABLE roundtrip_accounts (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			email TEXT UNIQUE NOT NULL,
			tier TEXT NOT NULL DEFAULT 'free',
			created_at TIMESTAMP NOT NULL DEFAULT now()
		);`,
		`CREATE TABLE roundtrip_projects (
			id INT PRIMARY KEY,
			account_id UUID NOT NULL REFERENCES roundtrip_accounts(id),
			name TEXT NOT NULL,
			status TEXT NOT NULL,
			meta JSONB NOT NULL
		);`,
		`CREATE INDEX roundtrip_projects_account_idx ON roundtrip_projects(account_id);`,
		`CREATE INDEX roundtrip_projects_status_idx ON roundtrip_projects(status);`,
		`CREATE VIEW roundtrip_active_projects AS
			SELECT p.id, a.email, p.name, p.meta
			FROM roundtrip_projects p
			JOIN roundtrip_accounts a ON a.id = p.account_id
			WHERE p.status <> 'archived';`,
		`INSERT INTO roundtrip_accounts (id, email, tier, created_at)
		 VALUES ('00000000-0000-0000-0000-000000000001', 'ada@example.com', 'pro', TIMESTAMP '2026-01-01 00:00:00');`,
		`INSERT INTO roundtrip_projects (id, account_id, name, status, meta)
		 VALUES (1, '00000000-0000-0000-0000-000000000001', 'imported', 'active', '{"features":["dump","restore"],"archived":false}'::jsonb);`,
	}
	for _, query := range sourceSetup {
		_, err = sourceConn.Exec(sourceCtx, query)
		require.NoError(t, err, "source setup query failed: %s", query)
	}

	work := t.TempDir()
	dumpPath := filepath.Join(work, "roundtrip.sql")
	dumpDatabaseWithPgDump(t, ctx, pgDump, sourcePort, dumpPath)
	stopSource()

	restorePort, err := gms.GetEmptyPort()
	require.NoError(t, err)
	restoreCtx, restoreConn, restoreController := CreateServerWithPort(t, "postgres", restorePort)
	t.Cleanup(func() {
		restoreConn.Close(restoreCtx)
		restoreController.Stop()
		require.NoError(t, restoreController.WaitForStop())
	})

	restoreSQLDumpWithPsql(t, ctx, psql, restorePort, dumpPath)

	runDrizzleKitRoundTripIntrospect(t, ctx, restorePort)
	requireRoundTripAppQueries(t, ctx, restorePort)
}

func TestPgDumpPsqlRestoreSequenceRoundTrip(t *testing.T) {
	pgDump, err := exec.LookPath("pg_dump")
	if err != nil {
		t.Skip("pg_dump not available; install PostgreSQL client tools to enable this harness")
	}
	psql, err := exec.LookPath("psql")
	if err != nil {
		t.Skip("psql not available; install PostgreSQL client tools to enable this harness")
	}

	ctx := context.Background()
	sourcePort, err := gms.GetEmptyPort()
	require.NoError(t, err)
	sourceCtx, sourceConn, sourceController := CreateServerWithPort(t, "postgres", sourcePort)
	var stopSourceOnce sync.Once
	stopSource := func() {
		stopSourceOnce.Do(func() {
			sourceConn.Close(sourceCtx)
			sourceController.Stop()
			require.NoError(t, sourceController.WaitForStop())
		})
	}
	t.Cleanup(func() {
		stopSource()
	})

	sourceSetup := []string{
		`CREATE TABLE public.sequence_roundtrip_items (id integer NOT NULL, label text NOT NULL);`,
		`CREATE SEQUENCE public.sequence_roundtrip_items_id_seq AS integer START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;`,
		`ALTER SEQUENCE public.sequence_roundtrip_items_id_seq OWNED BY public.sequence_roundtrip_items.id;`,
		`ALTER TABLE ONLY public.sequence_roundtrip_items ALTER COLUMN id SET DEFAULT nextval('public.sequence_roundtrip_items_id_seq'::regclass);`,
		`INSERT INTO public.sequence_roundtrip_items (label) VALUES ('first'), ('second');`,
		`SELECT setval('public.sequence_roundtrip_items_id_seq', 42, false);`,
	}
	for _, query := range sourceSetup {
		_, err = sourceConn.Exec(sourceCtx, query)
		require.NoError(t, err, "source setup query failed: %s", query)
	}

	work := t.TempDir()
	dumpPath := filepath.Join(work, "sequence-roundtrip.sql")
	dumpDatabaseWithPgDump(t, ctx, pgDump, sourcePort, dumpPath)
	stopSource()

	restorePort, err := gms.GetEmptyPort()
	require.NoError(t, err)
	restoreCtx, restoreConn, restoreController := CreateServerWithPort(t, "postgres", restorePort)
	t.Cleanup(func() {
		restoreConn.Close(restoreCtx)
		restoreController.Stop()
		require.NoError(t, restoreController.WaitForStop())
	})

	restoreSQLDumpWithPsql(t, ctx, psql, restorePort, dumpPath)
	requireSequenceRoundTripQueries(t, ctx, restorePort)
}

func TestPgDumpPsqlRestoreExternalAppDumpRoundTrip(t *testing.T) {
	pgDump, err := exec.LookPath("pg_dump")
	if err != nil {
		t.Skip("pg_dump not available; install PostgreSQL client tools to enable this harness")
	}
	psql, err := exec.LookPath("psql")
	if err != nil {
		t.Skip("psql not available; install PostgreSQL client tools to enable this harness")
	}
	if _, err = exec.LookPath("npm"); err != nil {
		t.Skip("npm not available; install Node.js to enable the drizzle-kit introspection step")
	}
	if testing.Short() {
		t.Skip("round-trip harness installs drizzle-kit; skipped under -short")
	}

	ctx := context.Background()
	testCases := []struct {
		name           string
		dumpFile       string
		setup          []string
		expectedTables []string
		requireQueries func(t *testing.T, ctx context.Context, port int)
	}{
		{
			name:     "Boluwatife-AJB/backend-in-node",
			dumpFile: "Boluwatife-AJB_backend-in-node.sql",
			setup: []string{
				`CREATE USER "USER" WITH SUPERUSER PASSWORD 'password';`,
			},
			expectedTables: []string{"companies", "employees"},
			requireQueries: requireBoluwatifeExternalAppRoundTripQueries,
		},
		{
			name:           "kirooha/adtech-simple",
			dumpFile:       "kirooha_adtech-simple.sql",
			expectedTables: []string{"campaigns", "goose_db_version", "gue_jobs"},
			requireQueries: requireKiroohaExternalAppRoundTripQueries,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			sourcePort, err := gms.GetEmptyPort()
			require.NoError(t, err)
			sourceCtx, sourceConn, sourceController := CreateServerWithPort(t, "postgres", sourcePort)
			var stopSourceOnce sync.Once
			stopSource := func() {
				stopSourceOnce.Do(func() {
					sourceConn.Close(sourceCtx)
					sourceController.Stop()
					require.NoError(t, sourceController.WaitForStop())
				})
			}
			t.Cleanup(func() {
				stopSource()
			})

			for _, query := range testCase.setup {
				_, err = sourceConn.Exec(sourceCtx, query)
				require.NoError(t, err, "external dump setup query failed: %s", query)
			}
			restoreSQLDumpWithPsql(t, ctx, psql, sourcePort, testingDumpSQLPath(t, testCase.dumpFile))

			work := t.TempDir()
			dumpPath := filepath.Join(work, "external-roundtrip.sql")
			dumpDatabaseWithPgDump(t, ctx, pgDump, sourcePort, dumpPath)
			stopSource()

			restorePort, err := gms.GetEmptyPort()
			require.NoError(t, err)
			restoreCtx, restoreConn, restoreController := CreateServerWithPort(t, "postgres", restorePort)
			t.Cleanup(func() {
				restoreConn.Close(restoreCtx)
				restoreController.Stop()
				require.NoError(t, restoreController.WaitForStop())
			})

			restoreSQLDumpWithPsql(t, ctx, psql, restorePort, dumpPath)
			runDrizzleKitExternalDumpIntrospect(t, ctx, restorePort, testCase.expectedTables)
			testCase.requireQueries(t, ctx, restorePort)
		})
	}
}

func dumpDatabaseWithPgDump(t *testing.T, ctx context.Context, pgDump string, port int, dumpPath string) {
	t.Helper()
	sourceURL := fmt.Sprintf("postgresql://postgres:password@127.0.0.1:%d/postgres?sslmode=disable", port)
	dumpCmd := exec.CommandContext(ctx, pgDump,
		"--no-owner",
		"--no-privileges",
		"--exclude-schema", "dolt",
		"--dbname", sourceURL,
		"--file", dumpPath,
	)
	dumpCmd.Env = append(os.Environ(), "NO_COLOR=1", "PGPASSWORD=password")
	out, err := dumpCmd.CombinedOutput()
	require.NoError(t, err, "pg_dump failed:\n%s", string(out))
}

func restoreSQLDumpWithPsql(t *testing.T, ctx context.Context, psql string, port int, dumpPath string) {
	t.Helper()
	restoreURL := fmt.Sprintf("postgresql://postgres:password@127.0.0.1:%d/postgres?sslmode=disable", port)
	restoreCmd := exec.CommandContext(ctx, psql,
		"--no-psqlrc",
		"--set", "ON_ERROR_STOP=1",
		"--dbname", restoreURL,
		"--file", dumpPath,
	)
	restoreCmd.Env = append(os.Environ(), "NO_COLOR=1", "PGPASSWORD=password")
	out, err := restoreCmd.CombinedOutput()
	if err != nil {
		dumpBytes, dumpErr := os.ReadFile(dumpPath)
		require.NoError(t, dumpErr)
		require.NoError(t, err, "psql restore failed:\n%s\nDump:\n%s", string(out), string(dumpBytes))
	}
}

func testingDumpSQLPath(t *testing.T, filename string) string {
	t.Helper()
	_, currentFileLocation, _, ok := runtime.Caller(0)
	require.True(t, ok, "unable to find the folder where the dump files are located")
	return filepath.Clean(filepath.Join(filepath.Dir(currentFileLocation), "../dumps/sql", filename))
}

func runDrizzleKitRoundTripIntrospect(t *testing.T, ctx context.Context, port int) {
	t.Helper()
	schema := runDrizzleKitIntrospect(t, ctx, port)
	for _, tbl := range []string{"roundtrip_accounts", "roundtrip_projects"} {
		require.Contains(t, schema, fmt.Sprintf(`pgTable("%s",`, tbl),
			"introspected schema is missing restored table %q\nschema:\n%s", tbl, schema)
	}
	require.Contains(t, schema, `index("roundtrip_projects_account_idx")`)
	require.Contains(t, schema, `index("roundtrip_projects_status_idx")`)
	require.Contains(t, schema, "foreignKey")
	requireContainsAny(t, schema, []string{`.unique()`, `.unique(`, `unique(`}, "unique constraint on roundtrip_accounts.email")
}

func runDrizzleKitExternalDumpIntrospect(t *testing.T, ctx context.Context, port int, expectedTables []string) {
	t.Helper()
	schema := runDrizzleKitIntrospect(t, ctx, port)
	for _, tbl := range expectedTables {
		require.Contains(t, schema, fmt.Sprintf(`pgTable("%s",`, tbl),
			"introspected schema is missing restored external table %q\nschema:\n%s", tbl, schema)
	}
	requireContainsAny(t, schema, []string{`.primaryKey()`, `primaryKey({`}, "external dump primary keys")
}

func runDrizzleKitIntrospect(t *testing.T, ctx context.Context, port int) string {
	t.Helper()
	work := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(work, "package.json"), []byte(`{
  "name": "doltgres-roundtrip-introspect-harness",
  "private": true
}
`), 0o644))

	install := exec.CommandContext(ctx, "npm", "install", "--silent",
		"--no-audit", "--no-fund",
		"drizzle-kit@0.31.10", "drizzle-orm@0.45.2", "pg@8.11.3",
	)
	install.Dir = work
	install.Env = append(os.Environ(), "NO_COLOR=1")
	if out, err := install.CombinedOutput(); err != nil {
		t.Fatalf("npm install drizzle-kit + drizzle-orm failed: %v\n%s", err, string(out))
	}

	url := fmt.Sprintf("postgres://postgres:password@127.0.0.1:%d/postgres", port)
	cmd := exec.CommandContext(ctx,
		filepath.Join(work, "node_modules", ".bin", "drizzle-kit"),
		"introspect",
		"--dialect", "postgresql",
		"--url", url,
		"--out", "./drizzle",
	)
	cmd.Dir = work
	cmd.Env = append(os.Environ(), "NO_COLOR=1")
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "drizzle-kit introspect after restore failed:\n%s", string(out))

	schemaBytes, err := os.ReadFile(filepath.Join(work, "drizzle", "schema.ts"))
	require.NoError(t, err, "drizzle-kit did not produce drizzle/schema.ts")
	return string(schemaBytes)
}

func requireRoundTripAppQueries(t *testing.T, ctx context.Context, port int) {
	t.Helper()
	conn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://postgres:password@127.0.0.1:%d/postgres?sslmode=disable", port))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, conn.Close(context.Background()))
	})

	rows, err := conn.Query(ctx, `
		SELECT p.name, elem
		FROM roundtrip_active_projects p
		JOIN LATERAL jsonb_array_elements_text(p.meta->'features') AS elem ON true
		WHERE p.email = 'ada@example.com'
		ORDER BY elem
	`)
	require.NoError(t, err)
	var restoredNames, restoredFeatures []string
	for rows.Next() {
		var name, feature string
		require.NoError(t, rows.Scan(&name, &feature))
		restoredNames = append(restoredNames, name)
		restoredFeatures = append(restoredFeatures, feature)
	}
	require.NoError(t, rows.Err())
	rows.Close()
	require.Equal(t, []string{"imported", "imported"}, restoredNames)
	require.Equal(t, []string{"dump", "restore"}, restoredFeatures)

	var newAccountID string
	require.NoError(t, conn.QueryRow(ctx, `
		INSERT INTO roundtrip_accounts (email, tier)
		VALUES ('grace@example.com', 'team')
		RETURNING id::text
	`).Scan(&newAccountID))

	_, err = conn.Exec(ctx, `
		INSERT INTO roundtrip_projects (id, account_id, name, status, meta)
		VALUES (2, $1::uuid, 'restored-app-write', 'active', '{"features":["pgx"],"archived":false}'::jsonb)
	`, newAccountID)
	require.NoError(t, err)

	var projectCount int
	require.NoError(t, conn.QueryRow(ctx, `
		SELECT count(*)
		FROM roundtrip_active_projects
		WHERE email IN ('ada@example.com', 'grace@example.com')
	`).Scan(&projectCount))
	require.Equal(t, 2, projectCount)
}

func requireSequenceRoundTripQueries(t *testing.T, ctx context.Context, port int) {
	t.Helper()
	conn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://postgres:password@127.0.0.1:%d/postgres?sslmode=disable", port))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, conn.Close(context.Background()))
	})

	var lastValue int64
	var isCalled bool
	require.NoError(t, conn.QueryRow(ctx, `
		SELECT last_value, is_called
		FROM public.sequence_roundtrip_items_id_seq
	`).Scan(&lastValue, &isCalled))
	require.Equal(t, int64(42), lastValue)
	require.False(t, isCalled)

	var insertedID int64
	require.NoError(t, conn.QueryRow(ctx, `
		INSERT INTO public.sequence_roundtrip_items (label)
		VALUES ('restored-default')
		RETURNING id
	`).Scan(&insertedID))
	require.Equal(t, int64(42), insertedID)

	var nextValue int64
	require.NoError(t, conn.QueryRow(ctx, `
		SELECT nextval('public.sequence_roundtrip_items_id_seq')
	`).Scan(&nextValue))
	require.Equal(t, int64(43), nextValue)
}

func requireBoluwatifeExternalAppRoundTripQueries(t *testing.T, ctx context.Context, port int) {
	t.Helper()
	conn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://postgres:password@127.0.0.1:%d/postgres?sslmode=disable", port))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, conn.Close(context.Background()))
	})

	var companyCount, employeeCount int
	require.NoError(t, conn.QueryRow(ctx, `SELECT count(*) FROM companies`).Scan(&companyCount))
	require.Equal(t, 2, companyCount)
	require.NoError(t, conn.QueryRow(ctx, `SELECT count(*) FROM employees`).Scan(&employeeCount))
	require.Equal(t, 2, employeeCount)

	_, err = conn.Exec(ctx, `
		INSERT INTO companies (id, name)
		VALUES ('00000000-0000-0000-0000-000000000201'::uuid, 'roundtrip co')
	`)
	require.NoError(t, err)

	_, err = conn.Exec(ctx, `
		INSERT INTO employees (id, first_name, last_name, email)
		VALUES ('00000000-0000-0000-0000-000000000202'::uuid, 'Round', 'Trip', 'roundtrip@example.com')
	`)
	require.NoError(t, err)

	var restoredCompany, restoredEmail string
	require.NoError(t, conn.QueryRow(ctx, `
		SELECT c.name, e.email
		FROM companies c
		CROSS JOIN employees e
		WHERE c.id = '00000000-0000-0000-0000-000000000201'::uuid
			AND e.id = '00000000-0000-0000-0000-000000000202'::uuid
	`).Scan(&restoredCompany, &restoredEmail))
	require.Equal(t, "roundtrip co", restoredCompany)
	require.Equal(t, "roundtrip@example.com", restoredEmail)
}

func requireKiroohaExternalAppRoundTripQueries(t *testing.T, ctx context.Context, port int) {
	t.Helper()
	conn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://postgres:password@127.0.0.1:%d/postgres?sslmode=disable", port))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, conn.Close(context.Background()))
	})

	var lastValue int64
	var isCalled bool
	require.NoError(t, conn.QueryRow(ctx, `
		SELECT last_value, is_called
		FROM public.goose_db_version_id_seq
	`).Scan(&lastValue, &isCalled))
	require.Equal(t, int64(1), lastValue)
	require.False(t, isCalled)

	var versionID int64
	require.NoError(t, conn.QueryRow(ctx, `
		INSERT INTO public.goose_db_version (version_id, is_applied)
		VALUES (20260510, true)
		RETURNING id
	`).Scan(&versionID))
	require.Equal(t, int64(1), versionID)

	var campaignID string
	require.NoError(t, conn.QueryRow(ctx, `
		INSERT INTO public.campaigns (name, description)
		VALUES ('sequence-restored-campaign', 'created after pg_dump restore')
		RETURNING id::text
	`).Scan(&campaignID))
	require.NotEmpty(t, campaignID)

	_, err = conn.Exec(ctx, `
		INSERT INTO public.gue_jobs (
			job_id, priority, run_at, job_type, args, queue, created_at, updated_at
		)
		VALUES (
			'job-roundtrip', 10, now(), 'campaign.created', '\x7b7d'::bytea,
			'default', now(), now()
		)
	`)
	require.NoError(t, err)

	var jobCount int
	require.NoError(t, conn.QueryRow(ctx, `
		SELECT count(*)
		FROM public.gue_jobs
		WHERE job_id = 'job-roundtrip'
	`).Scan(&jobCount))
	require.Equal(t, 1, jobCount)
}
