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
	"github.com/stretchr/testify/require"
)

// TestKnexMigrationCLISmoke runs Knex's real migration CLI against Doltgres.
// The runtime Knex harness covers query-builder CRUD; this pins the migration
// binary path, including Knex's migration metadata tables and schema DSL DDL.
func TestKnexMigrationCLISmoke(t *testing.T) {
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("node not on PATH; install Node.js to enable this harness")
	}
	if _, err := exec.LookPath("npm"); err != nil {
		t.Skip("npm not on PATH; install Node.js to enable this harness")
	}
	if testing.Short() {
		t.Skip("Knex migration harness installs npm packages; skipped under -short")
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
	require.NoError(t, os.MkdirAll(filepath.Join(work, "migrations"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(work, "package.json"), []byte(`{
  "name": "doltgres-knex-migration-harness",
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

	url := fmt.Sprintf("postgres://postgres:password@127.0.0.1:%d/postgres?sslmode=disable&application_name=knex-migrate-harness", port)
	require.NoError(t, os.WriteFile(filepath.Join(work, "knexfile.cjs"), []byte(`module.exports = {
  development: {
    client: 'pg',
    connection: process.env.DOLTGRES_URL,
    migrations: {
      directory: './migrations',
      tableName: 'knex_cli_migrations',
    },
  },
};
`), 0o644))

	require.NoError(t, os.WriteFile(filepath.Join(work, "migrations", "20260510130000_create_knex_cli_schema.cjs"), []byte(`exports.up = async function(knex) {
  await knex.schema.createTable('knex_cli_accounts', table => {
    table.integer('id').primary();
    table.text('email').notNullable().unique();
    table.boolean('active').notNullable().defaultTo(true);
    table.jsonb('meta').notNullable();
  });

  await knex.schema.createTable('knex_cli_items', table => {
    table.integer('id').primary();
    table.integer('account_id').notNullable().references('id').inTable('knex_cli_accounts');
    table.decimal('amount', 10, 2).notNullable();
    table.specificType('tags', 'text[]').notNullable();
    table.jsonb('payload').notNullable();
  });

  await knex.schema.alterTable('knex_cli_items', table => {
    table.index(['account_id'], 'idx_knex_cli_items_account');
  });
};

exports.down = async function(knex) {
  await knex.schema.dropTableIfExists('knex_cli_items');
  await knex.schema.dropTableIfExists('knex_cli_accounts');
};
`), 0o644))

	knexBin := filepath.Join(work, "node_modules", ".bin", "knex")
	migrate := exec.CommandContext(cmdCtx, knexBin, "migrate:latest", "--env", "development", "--knexfile", "knexfile.cjs")
	migrate.Dir = work
	migrate.Env = append(os.Environ(), "NO_COLOR=1", "DOLTGRES_URL="+url)
	if out, err := migrate.CombinedOutput(); err != nil {
		t.Fatalf("knex migrate:latest failed: %v\n%s", err, string(out))
	}

	probe := `
const assert = require('node:assert/strict');
const knex = require('knex');

async function main() {
  const url = process.argv[2];
  const db = knex({
    client: 'pg',
    connection: url.replace('knex-migrate-harness', 'knex-migrate-probe'),
    pool: { min: 0, max: 2 },
  });

  try {
    const appName = await db.raw("SELECT current_setting('application_name') AS app_name");
    assert.equal(appName.rows[0].app_name, 'knex-migrate-probe');

    assert.equal(await db.schema.hasTable('knex_cli_accounts'), true);
    assert.equal(await db.schema.hasTable('knex_cli_items'), true);

    const migration = await db('knex_cli_migrations').select('name').first();
    assert.equal(migration.name, '20260510130000_create_knex_cli_schema.cjs');

    await db('knex_cli_accounts').insert([
      { id: 1, email: 'acme@example.com', active: true, meta: JSON.stringify({ tier: 'pro' }) },
      { id: 2, email: 'beta@example.com', active: false, meta: JSON.stringify({ tier: 'free' }) },
    ]);

    const inserted = await db('knex_cli_items')
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

    const selected = await db('knex_cli_items AS i')
      .join('knex_cli_accounts AS a', 'a.id', 'i.account_id')
      .select('a.email', db.raw('i.amount::text AS amount'), db.raw("i.payload #>> '{kind}' AS kind"))
      .whereRaw('? = ANY(??)', ['blue', 'i.tags']);
    assert.deepEqual(selected, [{ email: 'acme@example.com', amount: '12.34', kind: 'invoice' }]);

    await db.transaction(async trx => {
      await trx('knex_cli_accounts').insert({
        id: 3,
        email: 'gamma@example.com',
        active: true,
        meta: JSON.stringify({ tier: 'trial' }),
      });
    });

    let rolledBack = false;
    try {
      await db.transaction(async trx => {
        await trx('knex_cli_accounts').insert({
          id: 4,
          email: 'rolled-back@example.com',
          active: true,
          meta: JSON.stringify({ tier: 'trial' }),
        });
        throw new Error('force rollback');
      });
    } catch (err) {
      rolledBack = err.message === 'force rollback';
    }
    assert.equal(rolledBack, true);

    const summary = await db('knex_cli_accounts')
      .select(db.raw("array_to_string(array_agg(email ORDER BY id), ',') AS emails"))
      .first();
    assert.deepEqual(summary, { emails: 'acme@example.com,beta@example.com,gamma@example.com' });

    console.log(JSON.stringify({ ok: true, emails: summary.emails }));
  } finally {
    await db.destroy();
  }
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
`
	require.NoError(t, os.WriteFile(filepath.Join(work, "probe.cjs"), []byte(probe), 0o644))

	cmd := exec.CommandContext(cmdCtx, "node", "probe.cjs", url)
	cmd.Dir = work
	cmd.Env = append(os.Environ(), "NO_COLOR=1")
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "Knex migration probe failed: %s", string(out))
	require.Contains(t, string(out), `"ok":true`)
	require.Contains(t, string(out), `"emails":"acme@example.com,beta@example.com,gamma@example.com"`)
}
