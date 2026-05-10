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

// TestTypeORMMigrationCLISmoke runs TypeORM's real migration CLI against
// Doltgres. The TypeORM client smoke covers synchronize=true runtime behavior;
// this pins migration:run, TypeORM's migrations metadata table, and
// QueryRunner schema-builder DDL.
func TestTypeORMMigrationCLISmoke(t *testing.T) {
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("node not on PATH; install Node.js to enable this harness")
	}
	if _, err := exec.LookPath("npm"); err != nil {
		t.Skip("npm not on PATH; install Node.js to enable this harness")
	}
	if testing.Short() {
		t.Skip("TypeORM migration harness installs npm packages; skipped under -short")
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
  "name": "doltgres-typeorm-migration-harness",
  "private": true
}
`), 0o644))

	cmdCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	install := exec.CommandContext(cmdCtx, "npm", "install", "--silent",
		"--no-audit", "--no-fund",
		"typeorm@0.3.20", "pg@8.11.3", "reflect-metadata@0.2.2",
	)
	install.Dir = work
	install.Env = append(os.Environ(), "NO_COLOR=1")
	if out, err := install.CombinedOutput(); err != nil {
		t.Fatalf("npm install typeorm + pg failed: %v\n%s", err, string(out))
	}

	url := fmt.Sprintf("postgres://postgres:password@127.0.0.1:%d/postgres?sslmode=disable&application_name=typeorm-migration-harness", port)
	require.NoError(t, os.WriteFile(filepath.Join(work, "data-source.cjs"), []byte(`require('reflect-metadata');
const { DataSource } = require('typeorm');

module.exports = new DataSource({
  type: 'postgres',
  url: process.env.DOLTGRES_URL,
  migrations: ['./migrations/*.cjs'],
  migrationsTableName: 'typeorm_cli_migrations',
  logging: false,
});
`), 0o644))

	require.NoError(t, os.WriteFile(filepath.Join(work, "migrations", "1778445600000-CreateTypeORMMigrationSchema.cjs"), []byte(`const { Table, TableForeignKey, TableIndex } = require('typeorm');

module.exports = class CreateTypeORMMigrationSchema1778445600000 {
  name = 'CreateTypeORMMigrationSchema1778445600000';

  async up(queryRunner) {
    await queryRunner.createTable(new Table({
      name: 'typeorm_cli_accounts',
      columns: [
        { name: 'id', type: 'integer', isPrimary: true },
        { name: 'email', type: 'text', isNullable: false },
        { name: 'active', type: 'boolean', isNullable: false, default: 'true' },
        { name: 'meta', type: 'jsonb', isNullable: false },
      ],
    }));
    await queryRunner.createIndex('typeorm_cli_accounts', new TableIndex({
      name: 'idx_typeorm_cli_accounts_email',
      columnNames: ['email'],
      isUnique: true,
    }));

    await queryRunner.createTable(new Table({
      name: 'typeorm_cli_items',
      columns: [
        { name: 'id', type: 'integer', isPrimary: true },
        { name: 'account_id', type: 'integer', isNullable: false },
        { name: 'amount', type: 'numeric', precision: 10, scale: 2, isNullable: false },
        { name: 'tags', type: 'text', isArray: true, isNullable: false },
        { name: 'payload', type: 'jsonb', isNullable: false },
      ],
    }));
    await queryRunner.createForeignKey('typeorm_cli_items', new TableForeignKey({
      columnNames: ['account_id'],
      referencedTableName: 'typeorm_cli_accounts',
      referencedColumnNames: ['id'],
    }));
    await queryRunner.createIndex('typeorm_cli_items', new TableIndex({
      name: 'idx_typeorm_cli_items_account',
      columnNames: ['account_id'],
    }));
  }

  async down(queryRunner) {
    await queryRunner.dropTable('typeorm_cli_items');
    await queryRunner.dropTable('typeorm_cli_accounts');
  }
};
`), 0o644))

	typeormBin := filepath.Join(work, "node_modules", ".bin", "typeorm")
	migrate := exec.CommandContext(cmdCtx, typeormBin, "migration:run", "-d", "data-source.cjs")
	migrate.Dir = work
	migrate.Env = append(os.Environ(), "NO_COLOR=1", "DOLTGRES_URL="+url)
	if out, err := migrate.CombinedOutput(); err != nil {
		t.Fatalf("typeorm migration:run failed: %v\n%s", err, string(out))
	}

	probe := `
require('reflect-metadata');
const assert = require('node:assert/strict');
const { DataSource } = require('typeorm');

async function main() {
  const url = process.argv[2].replace('typeorm-migration-harness', 'typeorm-migration-probe');
  const ds = new DataSource({ type: 'postgres', url, logging: false });
  await ds.initialize();

  try {
    const appName = await ds.query("SELECT current_setting('application_name') AS app_name");
    assert.equal(appName[0].app_name, 'typeorm-migration-probe');

    const migrations = await ds.query("SELECT name FROM typeorm_cli_migrations ORDER BY id");
    assert.deepEqual(migrations, [{ name: 'CreateTypeORMMigrationSchema1778445600000' }]);

    await ds.query(
      "INSERT INTO typeorm_cli_accounts VALUES ($1::int4, $2::text, $3::bool, $4::jsonb), ($5::int4, $6::text, $7::bool, $8::jsonb)",
      [1, 'acme@example.com', true, JSON.stringify({ tier: 'pro' }), 2, 'beta@example.com', false, JSON.stringify({ tier: 'free' })],
    );

    const inserted = await ds.query(
      "INSERT INTO typeorm_cli_items VALUES ($1::int4, $2::int4, $3::numeric, $4::text[], $5::jsonb) " +
        "RETURNING amount::text AS amount, tags[2] AS second_tag, payload #>> '{kind}' AS kind",
      [10, 1, '12.34', ['red', 'blue'], JSON.stringify({ kind: 'invoice', lines: [1, 2] })],
    );
    assert.deepEqual(inserted, [{ amount: '12.34', second_tag: 'blue', kind: 'invoice' }]);

    const selected = await ds.query(
      "SELECT a.email, i.amount::text AS amount, i.tags[2] AS tag, i.payload #>> '{kind}' AS kind " +
        "FROM typeorm_cli_items i JOIN typeorm_cli_accounts a ON a.id = i.account_id WHERE $1::text = ANY(i.tags)",
      ['blue'],
    );
    assert.deepEqual(selected, [{ email: 'acme@example.com', amount: '12.34', tag: 'blue', kind: 'invoice' }]);

    await ds.transaction(async manager => {
      await manager.query(
        "INSERT INTO typeorm_cli_accounts VALUES ($1::int4, $2::text, $3::bool, $4::jsonb)",
        [3, 'gamma@example.com', true, JSON.stringify({ tier: 'trial' })],
      );
    });

    try {
      await ds.transaction(async manager => {
        await manager.query(
          "INSERT INTO typeorm_cli_accounts VALUES ($1::int4, $2::text, $3::bool, $4::jsonb)",
          [4, 'rolled-back@example.com', true, JSON.stringify({ tier: 'trial' })],
        );
        throw new Error('force rollback');
      });
      assert.fail('rollback transaction should throw');
    } catch (err) {
      assert.equal(err.message, 'force rollback');
    }

    const summary = await ds.query(
      "SELECT array_to_string(array_agg(email ORDER BY id), ',') AS emails FROM typeorm_cli_accounts",
    );
    assert.deepEqual(summary, [{ emails: 'acme@example.com,beta@example.com,gamma@example.com' }]);
    console.log(JSON.stringify({ ok: true, emails: summary[0].emails }));
  } finally {
    await ds.destroy();
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
	require.NoError(t, err, "TypeORM migration probe failed: %s", string(out))
	require.Contains(t, string(out), `"ok":true`)
	require.Contains(t, string(out), `"emails":"acme@example.com,beta@example.com,gamma@example.com"`)
}
