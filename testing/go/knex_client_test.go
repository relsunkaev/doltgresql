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

// TestKnexClientSmoke runs the real Knex query builder over the pg driver
// against Doltgres. The direct node-postgres harness pins the driver itself;
// this covers Knex's schema builder, query builder, pool, raw predicates, and
// transaction handling.
func TestKnexClientSmoke(t *testing.T) {
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
  "name": "doltgres-knex-harness",
  "private": true
}
`), 0o644))

	cmdCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	install := exec.CommandContext(cmdCtx, "npm", "install", "--silent",
		"--no-audit", "--no-fund", "knex@2.5.1", "pg@8.12.0",
	)
	install.Dir = work
	install.Env = append(os.Environ(), "NO_COLOR=1")
	if out, err := install.CombinedOutput(); err != nil {
		t.Fatalf("npm install knex failed: %v\n%s", err, string(out))
	}

	probe := strings.ReplaceAll(`
const assert = require('node:assert/strict');
const knex = require('knex');

async function main() {
  const url = process.argv[2];
  const db = knex({
    client: 'pg',
    connection: url,
    pool: { min: 0, max: 2 },
  });

  try {
    const appName = await db.raw(§SELECT current_setting('application_name') AS app_name§);
    assert.equal(appName.rows[0].app_name, 'knex-harness');

    await db.schema.createTable('knex_accounts', table => {
      table.integer('id').primary();
      table.text('name').notNullable().unique();
      table.boolean('active').notNullable();
    });
    await db.schema.createTable('knex_items', table => {
      table.integer('id').primary();
      table.integer('account_id').notNullable().references('id').inTable('knex_accounts');
      table.decimal('amount', 10, 2).notNullable();
      table.specificType('tags', 'text[]').notNullable();
      table.jsonb('payload').notNullable();
    });

    await db('knex_accounts').insert([
      { id: 1, name: 'acme', active: true },
      { id: 2, name: 'beta', active: false },
    ]);
    const inserted = await db('knex_items')
      .insert({
        id: 10,
        account_id: 1,
        amount: '12.34',
        tags: ['red', 'blue'],
        payload: JSON.stringify({ kind: 'invoice', lines: [1, 2] }),
      })
      .returning([
        db.raw('amount::text AS amount'),
        db.raw('tags[2] AS second_tag'),
        db.raw("payload #>> '{kind}' AS kind"),
      ]);
    assert.deepEqual(inserted, [{ amount: '12.34', second_tag: 'blue', kind: 'invoice' }]);

    const selected = await db('knex_items AS i')
      .join('knex_accounts AS a', 'a.id', 'i.account_id')
      .select('a.name', 'a.active', db.raw('i.amount::text AS amount'))
      .where('i.account_id', 1)
      .whereRaw('? = ANY(??)', ['blue', 'i.tags']);
    assert.deepEqual(selected, [{ name: 'acme', active: true, amount: '12.34' }]);

    const concurrent = await Promise.all([
      db('knex_accounts').select('name').where({ id: 1 }).first(),
      db('knex_accounts').select('name').where({ id: 2 }).first(),
    ]);
    assert.deepEqual(concurrent.map(row => row.name).sort(), ['acme', 'beta']);

    await db.transaction(async trx => {
      await trx('knex_accounts').insert({ id: 3, name: 'gamma', active: true });
    });

    let rolledBack = false;
    try {
      await db.transaction(async trx => {
        await trx('knex_accounts').insert({ id: 4, name: 'rolled back', active: true });
        throw new Error('force rollback');
      });
    } catch (err) {
      rolledBack = err.message === 'force rollback';
    }
    assert.equal(rolledBack, true);

    const summary = await db('knex_accounts')
      .select(db.raw("array_to_string(array_agg(name ORDER BY id), ',') AS names"))
      .first();
    assert.deepEqual(summary, { names: 'acme,beta,gamma' });
    console.log(JSON.stringify({ ok: true, accounts: summary.names }));
  } finally {
    await db.destroy();
  }
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
`, "§", "`")
	require.NoError(t, os.WriteFile(filepath.Join(work, "probe.cjs"), []byte(probe), 0o644))

	url := fmt.Sprintf("postgres://postgres:password@127.0.0.1:%d/postgres?sslmode=disable&application_name=knex-harness", port)
	cmd := exec.CommandContext(cmdCtx, "node", "probe.cjs", url)
	cmd.Dir = work
	cmd.Env = append(os.Environ(), "NO_COLOR=1")
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "knex probe failed: %s", string(out))
	require.Contains(t, string(out), `"ok":true`)
	require.Contains(t, string(out), `"accounts":"acme,beta,gamma"`)
}
