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

// TestTypeORMClientSmoke runs the real TypeORM DataSource over the
// node-postgres driver. This pins ORM-level schema synchronization,
// repository CRUD, JSONB/text[] binding, and transaction boundaries.
func TestTypeORMClientSmoke(t *testing.T) {
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
  "name": "doltgres-typeorm-harness",
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

	probe := strings.ReplaceAll(`
require('reflect-metadata');
const assert = require('node:assert/strict');
const { DataSource, EntitySchema } = require('typeorm');

const Account = new EntitySchema({
  name: 'Account',
  tableName: 'typeorm_accounts',
  columns: {
    id: { type: 'integer', primary: true },
    email: { type: 'text', unique: true },
    active: { type: 'boolean', default: true },
    meta: { type: 'jsonb' },
  },
});

const Item = new EntitySchema({
  name: 'Item',
  tableName: 'typeorm_items',
  columns: {
    id: { type: 'integer', primary: true },
    amount: { type: 'numeric', precision: 10, scale: 2 },
    tags: { type: 'text', array: true },
    payload: { type: 'jsonb' },
  },
  relations: {
    account: {
      type: 'many-to-one',
      target: 'Account',
      joinColumn: { name: 'account_id' },
      nullable: false,
    },
  },
});

async function main() {
  const url = process.argv[2];
  const ds = new DataSource({
    type: 'postgres',
    url,
    entities: [Account, Item],
    synchronize: true,
    logging: false,
    extra: { application_name: 'typeorm-harness' },
  });

  await ds.initialize();
  try {
    const appName = await ds.query(§SELECT current_setting('application_name') AS app_name§);
    assert.equal(appName[0].app_name, 'typeorm-harness');

    const accounts = ds.getRepository('Account');
    const items = ds.getRepository('Item');

    await accounts.save([
      { id: 1, email: 'acme@example.com', active: true, meta: { tier: 'pro' } },
      { id: 2, email: 'beta@example.com', active: false, meta: { tier: 'free' } },
    ]);
    await items.save({
      id: 10,
      account: { id: 1 },
      amount: '12.34',
      tags: ['red', 'blue'],
      payload: { kind: 'invoice', lines: [1, 2] },
    });

    const selected = await ds.query(§
      SELECT a.email, a.active, i.amount::text AS amount, i.tags[2] AS tag, i.payload #>> '{kind}' AS kind
      FROM typeorm_items i
      JOIN typeorm_accounts a ON a.id = i.account_id
      WHERE a.id = $1
    §, [1]);
    assert.deepEqual(selected, [{
      email: 'acme@example.com',
      active: true,
      amount: '12.34',
      tag: 'blue',
      kind: 'invoice',
    }]);

    await ds.transaction(async manager => {
      await manager.getRepository('Account').save({
        id: 3,
        email: 'gamma@example.com',
        active: true,
        meta: { tier: 'trial' },
      });
    });

    try {
      await ds.transaction(async manager => {
        await manager.getRepository('Account').save({
          id: 4,
          email: 'rolled-back@example.com',
          active: true,
          meta: { tier: 'trial' },
        });
        throw new Error('force rollback');
      });
      assert.fail('rollback transaction should throw');
    } catch (err) {
      assert.equal(err.message, 'force rollback');
    }

    const summary = await ds.query(§
      SELECT array_to_string(array_agg(email ORDER BY id), ',') AS emails
      FROM typeorm_accounts
    §);
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
`, "§", "`")
	require.NoError(t, os.WriteFile(filepath.Join(work, "probe.cjs"), []byte(probe), 0o644))

	url := fmt.Sprintf("postgres://postgres:password@127.0.0.1:%d/postgres?sslmode=disable", port)
	cmd := exec.CommandContext(cmdCtx, "node", "probe.cjs", url)
	cmd.Dir = work
	cmd.Env = append(os.Environ(), "NO_COLOR=1")
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "TypeORM probe failed: %s", string(out))
	require.Contains(t, string(out), `"ok":true`)
}
