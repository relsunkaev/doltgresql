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

// TestPGPromiseClientSmoke runs the real pg-promise client against Doltgres.
// This pins another common Node data-access layer over pg: task/transaction
// helpers, prepared statements, typed parameters, JSONB/text[] values, pooled
// concurrent reads, commit, and rollback.
func TestPGPromiseClientSmoke(t *testing.T) {
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
  "name": "doltgres-pg-promise-harness",
  "private": true
}
`), 0o644))

	cmdCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	install := exec.CommandContext(cmdCtx, "npm", "install", "--silent",
		"--no-audit", "--no-fund", "pg-promise@11.6.0",
	)
	install.Dir = work
	install.Env = append(os.Environ(), "NO_COLOR=1")
	if out, err := install.CombinedOutput(); err != nil {
		t.Fatalf("npm install pg-promise failed: %v\n%s", err, string(out))
	}

	probe := strings.ReplaceAll(`
const assert = require('node:assert/strict');
const pgPromise = require('pg-promise');

async function main() {
  const url = process.argv[2];
  const pgp = pgPromise();
  const db = pgp({
    connectionString: url,
    max: 2,
  });

  try {
    const appName = await db.one(§SELECT current_setting('application_name') AS app_name§);
    assert.equal(appName.app_name, 'pg-promise-harness');

    await db.none(§CREATE TABLE pgp_accounts (
      id integer PRIMARY KEY,
      name text NOT NULL UNIQUE,
      active boolean NOT NULL
    )§);
    await db.none(§CREATE TABLE pgp_items (
      id integer PRIMARY KEY,
      account_id integer NOT NULL REFERENCES pgp_accounts(id),
      amount numeric(10,2) NOT NULL,
      tags text[] NOT NULL,
      payload jsonb NOT NULL
    )§);

    await db.none(
      §INSERT INTO pgp_accounts VALUES ($1::int4, $2::text, $3::bool), ($4::int4, $5::text, $6::bool)§,
      [1, 'acme', true, 2, 'beta', false]
    );
    const inserted = await db.one(
      §INSERT INTO pgp_items VALUES ($1::int4, $2::int4, $3::text::numeric, $4::text[], $5::jsonb)
        RETURNING amount::text AS amount, tags[2] AS second_tag, payload #>> '{kind}' AS kind§,
      [10, 1, '12.34', ['red', 'blue'], { kind: 'invoice', lines: [1, 2] }]
    );
    assert.deepEqual(inserted, { amount: '12.34', second_tag: 'blue', kind: 'invoice' });

    const statement = new pgp.PreparedStatement({
      name: 'pgp-items-by-account',
      text: §SELECT a.name, a.active, i.amount::text AS amount
             FROM pgp_items i
             JOIN pgp_accounts a ON a.id = i.account_id
             WHERE i.account_id = $1::int4 AND $2::text = ANY(i.tags)§,
      values: [1, 'blue'],
    });
    const selected = await db.any(statement);
    assert.deepEqual(selected, [{ name: 'acme', active: true, amount: '12.34' }]);

    const concurrent = await db.task(async t => Promise.all([
      t.one(§SELECT name FROM pgp_accounts WHERE id = $1::int4§, [1]),
      t.one(§SELECT name FROM pgp_accounts WHERE id = $1::int4§, [2]),
    ]));
    assert.deepEqual(concurrent.map(row => row.name).sort(), ['acme', 'beta']);

    await db.tx(t => t.none(
      §INSERT INTO pgp_accounts VALUES ($1::int4, $2::text, $3::bool)§,
      [3, 'gamma', true]
    ));

    let rolledBack = false;
    try {
      await db.tx(async t => {
        await t.none(
          §INSERT INTO pgp_accounts VALUES ($1::int4, $2::text, $3::bool)§,
          [4, 'rolled back', true]
        );
        throw new Error('force rollback');
      });
    } catch (err) {
      rolledBack = err.message === 'force rollback';
    }
    assert.equal(rolledBack, true);

    const summary = await db.one(§
      SELECT array_to_string(array_agg(name ORDER BY id), ',') AS names
      FROM pgp_accounts
    §);
    assert.deepEqual(summary, { names: 'acme,beta,gamma' });
    console.log(JSON.stringify({ ok: true, accounts: summary.names }));
  } finally {
    pgp.end();
  }
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
`, "§", "`")
	require.NoError(t, os.WriteFile(filepath.Join(work, "probe.cjs"), []byte(probe), 0o644))

	url := fmt.Sprintf("postgres://postgres:password@127.0.0.1:%d/postgres?sslmode=disable&application_name=pg-promise-harness", port)
	cmd := exec.CommandContext(cmdCtx, "node", "probe.cjs", url)
	cmd.Dir = work
	cmd.Env = append(os.Environ(), "NO_COLOR=1")
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "pg-promise probe failed: %s", string(out))
	require.Contains(t, string(out), `"ok":true`)
	require.Contains(t, string(out), `"accounts":"acme,beta,gamma"`)
}
