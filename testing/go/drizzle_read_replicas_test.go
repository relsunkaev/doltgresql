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
	"testing"

	gms "github.com/dolthub/go-mysql-server/sql"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// TestDrizzleReadReplicasRouting runs Drizzle ORM's withReplicas() helper
// against Doltgres. It pins the deployment shape used by reader/writer
// routed apps: read URLs may point at read-only or lagged Doltgres
// deployments, while writes and explicit $primary reads use the primary URL.
func TestDrizzleReadReplicasRouting(t *testing.T) {
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("node not on PATH; install Node.js to enable this harness")
	}
	if _, err := exec.LookPath("npm"); err != nil {
		t.Skip("npm not on PATH; install Node.js to enable this harness")
	}
	if testing.Short() {
		t.Skip("drizzle-orm installs node_modules; skipped under -short")
	}

	port, err := gms.GetEmptyPort()
	require.NoError(t, err)
	ctx, defaultConn, controller := CreateServerWithPort(t, "postgres", port)
	t.Cleanup(func() {
		defaultConn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	_, err = defaultConn.Exec(ctx, `CREATE DATABASE read_replica;`)
	require.NoError(t, err)

	setupPrimary := []string{
		`CREATE TABLE routed_accounts (id INT PRIMARY KEY, name TEXT NOT NULL);`,
		`INSERT INTO routed_accounts VALUES (1, 'primary-seed');`,
	}
	for _, q := range setupPrimary {
		_, err = defaultConn.Exec(ctx, q)
		require.NoError(t, err, "primary setup: %s", q)
	}

	replicaURL := fmt.Sprintf("postgres://postgres:password@127.0.0.1:%d/read_replica?sslmode=disable", port)
	replicaConn, err := pgx.Connect(ctx, replicaURL)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, replicaConn.Close(context.Background()))
	})
	setupReplica := []string{
		`CREATE TABLE routed_accounts (id INT PRIMARY KEY, name TEXT NOT NULL);`,
		`INSERT INTO routed_accounts VALUES (1, 'replica-seed');`,
	}
	for _, q := range setupReplica {
		_, err = replicaConn.Exec(ctx, q)
		require.NoError(t, err, "replica setup: %s", q)
	}

	work := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(work, "package.json"), []byte(`{
  "name": "doltgres-drizzle-read-replicas-harness",
  "private": true,
  "type": "module"
}
`), 0o644))

	cmdCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	install := exec.CommandContext(cmdCtx, "npm", "install", "--silent",
		"--no-audit", "--no-fund",
		"drizzle-orm@0.45.2", "pg@8.11.3",
	)
	install.Dir = work
	install.Env = append(os.Environ(), "NO_COLOR=1")
	if out, err := install.CombinedOutput(); err != nil {
		t.Fatalf("npm install drizzle-orm + pg failed: %v\n%s", err, string(out))
	}

	probe := `
import assert from 'node:assert/strict';
import pg from 'pg';
import { eq } from 'drizzle-orm';
import { drizzle } from 'drizzle-orm/node-postgres';
import { integer, pgTable, text, withReplicas } from 'drizzle-orm/pg-core';

const primaryUrl = process.argv[2];
const replicaUrl = process.argv[3];

const accounts = pgTable('routed_accounts', {
  id: integer('id').primaryKey(),
  name: text('name').notNull(),
});

const primaryPool = new pg.Pool({ connectionString: primaryUrl, max: 2 });
const replicaPool = new pg.Pool({ connectionString: replicaUrl, max: 2 });
const primaryDb = drizzle(primaryPool);
const replicaDb = drizzle(replicaPool);
const db = withReplicas(primaryDb, [replicaDb], replicas => replicas[0]);

try {
  const replicaRead = await db.select().from(accounts);
  assert.deepEqual(replicaRead, [{ id: 1, name: 'replica-seed' }]);

  const primaryRead = await db.$primary.select().from(accounts);
  assert.deepEqual(primaryRead, [{ id: 1, name: 'primary-seed' }]);

  await db.insert(accounts).values({ id: 2, name: 'primary-write' });

  const replicaAfterInsert = await db.select().from(accounts).where(eq(accounts.id, 2));
  assert.deepEqual(replicaAfterInsert, []);

  const primaryAfterInsert = await db.$primary.select().from(accounts).where(eq(accounts.id, 2));
  assert.deepEqual(primaryAfterInsert, [{ id: 2, name: 'primary-write' }]);

  await db.update(accounts).set({ name: 'primary-updated' }).where(eq(accounts.id, 1));

  const replicaAfterUpdate = await db.select().from(accounts).where(eq(accounts.id, 1));
  assert.deepEqual(replicaAfterUpdate, [{ id: 1, name: 'replica-seed' }]);

  const primaryAfterUpdate = await db.$primary.select().from(accounts).where(eq(accounts.id, 1));
  assert.deepEqual(primaryAfterUpdate, [{ id: 1, name: 'primary-updated' }]);

  await db.delete(accounts).where(eq(accounts.id, 2));
  const primaryAfterDelete = await db.$primary.select().from(accounts).where(eq(accounts.id, 2));
  assert.deepEqual(primaryAfterDelete, []);

  console.log(JSON.stringify({ ok: true }));
} finally {
  await Promise.all([
    primaryPool.end(),
    replicaPool.end(),
  ]);
}
`
	require.NoError(t, os.WriteFile(filepath.Join(work, "probe.mjs"), []byte(probe), 0o644))

	primaryURL := fmt.Sprintf("postgres://postgres:password@127.0.0.1:%d/postgres?sslmode=disable", port)
	cmd := exec.CommandContext(cmdCtx, "node", "probe.mjs", primaryURL, replicaURL)
	cmd.Dir = work
	cmd.Env = append(os.Environ(), "NO_COLOR=1")
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "drizzle withReplicas probe failed: %s", string(out))
	require.Contains(t, string(out), `"ok":true`)
}
