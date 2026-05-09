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

// TestNodePostgresClientSmoke runs the real node-postgres `pg` pool
// against Doltgres. The migration-tool harness already pins catalog
// introspection through `pg`; this test pins the ordinary app driver
// path for pooled queries, parameters, prepared statements, and
// explicit transaction clients.
func TestNodePostgresClientSmoke(t *testing.T) {
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("node not on PATH; install Node.js to enable this harness")
	}
	if _, err := exec.LookPath("npm"); err != nil {
		t.Skip("npm not on PATH; install Node.js to enable this harness")
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
  "name": "doltgres-node-postgres-harness",
  "private": true
}
`), 0o644))

	cmdCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	install := exec.CommandContext(cmdCtx, "npm", "install", "--silent",
		"--no-audit", "--no-fund", "pg@8.11.3",
	)
	install.Dir = work
	install.Env = append(os.Environ(), "NO_COLOR=1")
	if out, err := install.CombinedOutput(); err != nil {
		t.Fatalf("npm install pg failed: %v\n%s", err, string(out))
	}

	probe := strings.ReplaceAll(`
const assert = require('node:assert/strict');
const { Pool } = require('pg');

async function main() {
  const url = process.argv[2];
  const pool = new Pool({
    connectionString: url,
    max: 2,
    application_name: 'node-postgres-harness',
    idleTimeoutMillis: 1000,
    connectionTimeoutMillis: 5000,
  });

  try {
    const appName = await pool.query(§SELECT current_setting('application_name') AS app_name§);
    assert.equal(appName.rows[0].app_name, 'node-postgres-harness');

    await pool.query(§CREATE TABLE npg_accounts (
      id integer PRIMARY KEY,
      name text NOT NULL UNIQUE,
      active boolean NOT NULL
    )§);
    await pool.query(§CREATE TABLE npg_items (
      id integer PRIMARY KEY,
      account_id integer NOT NULL REFERENCES npg_accounts(id),
      amount numeric(10,2) NOT NULL,
      tags text[] NOT NULL,
      payload jsonb NOT NULL
    )§);

    await pool.query(
      §INSERT INTO npg_accounts VALUES ($1::int4, $2::text, $3::bool), ($4::int4, $5::text, $6::bool)§,
      [1, 'acme', true, 2, 'beta', false]
    );
    const inserted = await pool.query(
      §INSERT INTO npg_items VALUES ($1::int4, $2::int4, $3::text::numeric, $4::text[], $5::jsonb)
        RETURNING amount::text AS amount, tags[2] AS second_tag, payload #>> '{kind}' AS kind§,
      [10, 1, '12.34', ['red', 'blue'], JSON.stringify({ kind: 'invoice', lines: [1, 2] })]
    );
    assert.deepEqual(inserted.rows, [{ amount: '12.34', second_tag: 'blue', kind: 'invoice' }]);

    const selected = await pool.query({
      name: 'npg-items-by-account',
      text: §SELECT a.name, a.active, i.amount::text AS amount
             FROM npg_items i
             JOIN npg_accounts a ON a.id = i.account_id
             WHERE i.account_id = $1::int4 AND 'blue' = ANY(i.tags)§,
      values: [1],
    });
    assert.deepEqual(selected.rows, [{ name: 'acme', active: true, amount: '12.34' }]);

    const concurrent = await Promise.all([
      pool.query(§SELECT name FROM npg_accounts WHERE id = $1::int4§, [1]),
      pool.query(§SELECT name FROM npg_accounts WHERE id = $1::int4§, [2]),
    ]);
    assert.deepEqual(concurrent.map(result => result.rows[0].name).sort(), ['acme', 'beta']);

    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(
        §INSERT INTO npg_accounts VALUES ($1::int4, $2::text, $3::bool)§,
        [3, 'gamma', true]
      );
      await client.query('COMMIT');
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    } finally {
      client.release();
    }

    const rollbackClient = await pool.connect();
    try {
      await rollbackClient.query('BEGIN');
      await rollbackClient.query(
        §INSERT INTO npg_accounts VALUES ($1::int4, $2::text, $3::bool)§,
        [4, 'rolled back', true]
      );
      await rollbackClient.query('ROLLBACK');
    } finally {
      rollbackClient.release();
    }

    const summary = await pool.query(§
      SELECT array_to_string(array_agg(name ORDER BY id), ',') AS names
      FROM npg_accounts
    §);
    assert.deepEqual(summary.rows, [{ names: 'acme,beta,gamma' }]);
    console.log(JSON.stringify({ ok: true, accounts: summary.rows[0].names }));
  } finally {
    await pool.end();
  }
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
`, "§", "`")
	require.NoError(t, os.WriteFile(filepath.Join(work, "probe.cjs"), []byte(probe), 0o644))

	url := fmt.Sprintf("postgres://postgres:password@127.0.0.1:%d/postgres?sslmode=disable", port)
	cmd := exec.CommandContext(cmdCtx, "node", "probe.cjs", url)
	cmd.Dir = work
	cmd.Env = append(os.Environ(), "NO_COLOR=1")
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "node-postgres probe failed: %s", string(out))
	require.Contains(t, string(out), `"ok":true`)
	require.Contains(t, string(out), `"accounts":"acme,beta,gamma"`)
}
