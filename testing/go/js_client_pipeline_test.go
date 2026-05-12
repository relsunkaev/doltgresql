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

// TestJSClientSingleConnectionPipelineGuards runs real JS clients through the
// single-socket patterns that exercise extended-protocol pipelining. postgres.js
// can keep several query promises in flight on one connection, and Drizzle's
// postgres-js driver inherits that behavior.
func TestJSClientSingleConnectionPipelineGuards(t *testing.T) {
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("node not on PATH; install Node.js to enable this harness")
	}
	if _, err := exec.LookPath("npm"); err != nil {
		t.Skip("npm not on PATH; install Node.js to enable this harness")
	}
	if testing.Short() {
		t.Skip("JS client pipeline harness installs node_modules; skipped under -short")
	}

	port, err := gms.GetEmptyPort()
	require.NoError(t, err)
	ctx, defaultConn, controller := CreateServerWithPort(t, "postgres", port)
	t.Cleanup(func() {
		defaultConn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	work := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(work, "package.json"), []byte(`{
  "name": "doltgres-js-client-pipeline-harness",
  "private": true,
  "type": "module"
}
`), 0o644))

	cmdCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	install := exec.CommandContext(cmdCtx, "npm", "install", "--silent",
		"--no-audit", "--no-fund",
		"postgres@3.4.5", "drizzle-orm@0.45.2", "pg@8.11.3",
	)
	install.Dir = work
	install.Env = append(os.Environ(), "NO_COLOR=1")
	if out, err := install.CombinedOutput(); err != nil {
		t.Fatalf("npm install JS clients failed: %v\n%s", err, string(out))
	}

	probe := strings.ReplaceAll(`
import assert from 'node:assert/strict';
import postgres from 'postgres';
import pg from 'pg';
import { drizzle } from 'drizzle-orm/postgres-js';
import { sql as dsql } from 'drizzle-orm';

const url = process.argv[2];

async function runPostgresJSPipeline() {
  const sql = postgres(url, {
    max: 1,
    max_pipeline: 50,
    prepare: true,
    idle_timeout: 1,
    connect_timeout: 5,
  });

  try {
    await sql§CREATE TABLE pjs_pipeline_items (
      id integer PRIMARY KEY,
      label text NOT NULL,
      payload jsonb NOT NULL
    )§;

    const inserts = Array.from({ length: 25 }, (_, i) =>
      sql§
        INSERT INTO pjs_pipeline_items VALUES (
          ${i},
          ${'label-' + i},
          ${sql.json({ index: i, parity: i % 2 === 0 ? 'even' : 'odd' })}
        )
        RETURNING id::text AS id, payload #>> '{parity}' AS parity
      §
    );
    const inserted = await Promise.all(inserts);
    assert.equal(inserted.length, 25);
    assert.equal(inserted[0][0].id, '0');
    assert.equal(inserted[24][0].parity, 'even');

    const reads = await Promise.all(Array.from({ length: 25 }, (_, i) =>
      sql§
        SELECT label, payload #>> '{index}' AS idx
        FROM pjs_pipeline_items
        WHERE id = ${i}
      §
    ));
    assert.deepEqual(reads.map(rows => rows[0].label), Array.from({ length: 25 }, (_, i) => 'label-' + i));

    const summary = await sql§
      SELECT count(*)::int AS count,
             array_to_string(array_agg(label ORDER BY id), ',') AS labels
      FROM pjs_pipeline_items
    §;
    assert.equal(summary[0].count, 25);
    assert.equal(summary[0].labels.split(',').at(-1), 'label-24');
    return { count: summary[0].count };
  } finally {
    await sql.end({ timeout: 5 });
  }
}

async function runDrizzlePostgresJSPipeline() {
  const client = postgres(url, {
    max: 1,
    max_pipeline: 50,
    prepare: true,
    idle_timeout: 1,
    connect_timeout: 5,
  });
  const db = drizzle(client);

  try {
    await db.execute(dsql§CREATE TABLE drizzle_pipeline_items (
      id integer PRIMARY KEY,
      label text NOT NULL
    )§);

    await Promise.all(Array.from({ length: 20 }, (_, i) =>
      db.execute(dsql§INSERT INTO drizzle_pipeline_items VALUES (${i}, ${'drizzle-' + i})§)
    ));

    const reads = await Promise.all(Array.from({ length: 20 }, (_, i) =>
      db.execute(dsql§
        SELECT label
        FROM drizzle_pipeline_items
        WHERE id = ${i}
      §)
    ));
    assert.deepEqual(reads.map(result => result[0].label), Array.from({ length: 20 }, (_, i) => 'drizzle-' + i));

    const summary = await db.execute(dsql§SELECT count(*)::int AS count FROM drizzle_pipeline_items§);
    assert.equal(summary[0].count, 20);
    return { count: summary[0].count };
  } finally {
    await client.end({ timeout: 5 });
  }
}

async function runNodePostgresSingleClientQueue() {
  const { Client } = pg;
  const client = new Client({
    connectionString: url,
    application_name: 'node-postgres-single-client-pipeline-guard',
    connectionTimeoutMillis: 5000,
  });
  await client.connect();

  try {
    await client.query(§CREATE TABLE npg_pipeline_items (
      id integer PRIMARY KEY,
      label text NOT NULL
    )§);

    await Promise.all(Array.from({ length: 20 }, (_, i) =>
      client.query(§INSERT INTO npg_pipeline_items VALUES ($1::int4, $2::text)§, [i, 'pg-' + i])
    ));

    const reads = await Promise.all(Array.from({ length: 20 }, (_, i) =>
      client.query(§SELECT label FROM npg_pipeline_items WHERE id = $1::int4§, [i])
    ));
    assert.deepEqual(reads.map(result => result.rows[0].label), Array.from({ length: 20 }, (_, i) => 'pg-' + i));

    const summary = await client.query(§SELECT count(*)::int AS count FROM npg_pipeline_items§);
    assert.equal(summary.rows[0].count, 20);
    return { count: summary.rows[0].count };
  } finally {
    await client.end();
  }
}

const result = {
  postgresJs: await runPostgresJSPipeline(),
  drizzle: await runDrizzlePostgresJSPipeline(),
  nodePostgres: await runNodePostgresSingleClientQueue(),
};
console.log(JSON.stringify({ ok: true, result }));
`, "§", "`")
	require.NoError(t, os.WriteFile(filepath.Join(work, "probe.mjs"), []byte(probe), 0o644))

	url := fmt.Sprintf("postgres://postgres:password@127.0.0.1:%d/postgres?sslmode=disable", port)
	cmd := exec.CommandContext(cmdCtx, "node", "probe.mjs", url)
	cmd.Dir = work
	cmd.Env = append(os.Environ(), "NO_COLOR=1")
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "JS pipeline probe failed: %s", string(out))
	require.Contains(t, string(out), `"ok":true`)
	require.Contains(t, string(out), `"postgresJs":{"count":25}`)
	require.Contains(t, string(out), `"drizzle":{"count":20}`)
	require.Contains(t, string(out), `"nodePostgres":{"count":20}`)
}
