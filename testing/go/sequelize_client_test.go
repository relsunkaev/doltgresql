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

// TestSequelizeClientSmoke runs the real Sequelize ORM over the
// node-postgres driver. This pins another common ORM path for schema
// synchronization, model CRUD, associations, JSONB/text[] binding, pooled
// reads, and managed transactions.
func TestSequelizeClientSmoke(t *testing.T) {
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
  "name": "doltgres-sequelize-harness",
  "private": true
}
`), 0o644))

	cmdCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	install := exec.CommandContext(cmdCtx, "npm", "install", "--silent",
		"--no-audit", "--no-fund",
		"sequelize@6.37.5", "pg@8.11.3",
	)
	install.Dir = work
	install.Env = append(os.Environ(), "NO_COLOR=1")
	if out, err := install.CombinedOutput(); err != nil {
		t.Fatalf("npm install sequelize + pg failed: %v\n%s", err, string(out))
	}

	probe := strings.ReplaceAll(`
const assert = require('node:assert/strict');
const { DataTypes, Sequelize } = require('sequelize');

async function main() {
  const url = process.argv[2];
  const sequelize = new Sequelize(url, {
    dialect: 'postgres',
    logging: false,
    dialectOptions: { application_name: 'sequelize-harness' },
    pool: { max: 2, min: 0, idle: 1000, acquire: 5000 },
  });

  const Account = sequelize.define('Account', {
    id: { type: DataTypes.INTEGER, primaryKey: true },
    email: { type: DataTypes.TEXT, allowNull: false, unique: true },
    active: { type: DataTypes.BOOLEAN, allowNull: false, defaultValue: true },
    meta: { type: DataTypes.JSONB, allowNull: false },
  }, {
    tableName: 'sequelize_accounts',
    timestamps: false,
  });

  const Item = sequelize.define('Item', {
    id: { type: DataTypes.INTEGER, primaryKey: true },
    accountId: {
      type: DataTypes.INTEGER,
      allowNull: false,
      field: 'account_id',
      references: { model: Account, key: 'id' },
    },
    amount: { type: DataTypes.DECIMAL(10, 2), allowNull: false },
    tags: { type: DataTypes.ARRAY(DataTypes.TEXT), allowNull: false },
    payload: { type: DataTypes.JSONB, allowNull: false },
  }, {
    tableName: 'sequelize_items',
    timestamps: false,
  });

  Account.hasMany(Item, { as: 'items', foreignKey: 'accountId' });
  Item.belongsTo(Account, { as: 'account', foreignKey: 'accountId' });

  try {
    await sequelize.authenticate();
    const appName = await sequelize.query(§SELECT current_setting('application_name') AS app_name§, {
      type: Sequelize.QueryTypes.SELECT,
    });
    assert.equal(appName[0].app_name, 'sequelize-harness');

    await sequelize.sync({ force: true });

    await Account.bulkCreate([
      { id: 1, email: 'acme@example.com', active: true, meta: { tier: 'pro' } },
      { id: 2, email: 'beta@example.com', active: false, meta: { tier: 'free' } },
    ]);
    await Item.create({
      id: 10,
      accountId: 1,
      amount: '12.34',
      tags: ['red', 'blue'],
      payload: { kind: 'invoice', lines: [1, 2] },
    });

    const selected = await Item.findOne({
      where: { accountId: 1 },
      include: [{ model: Account, as: 'account', required: true }],
      order: [['id', 'ASC']],
    });
    assert.equal(selected.account.email, 'acme@example.com');
    assert.equal(selected.account.active, true);
    assert.equal(selected.amount, '12.34');
    assert.deepEqual(selected.tags, ['red', 'blue']);
    assert.equal(selected.payload.kind, 'invoice');

    const raw = await sequelize.query(§
      SELECT a.email, i.tags[2] AS tag, i.payload #>> '{kind}' AS kind
      FROM sequelize_items i
      JOIN sequelize_accounts a ON a.id = i.account_id
      WHERE i.account_id = $1 AND 'blue' = ANY(i.tags)
    §, {
      bind: [1],
      type: Sequelize.QueryTypes.SELECT,
    });
    assert.deepEqual(raw, [{ email: 'acme@example.com', tag: 'blue', kind: 'invoice' }]);

    const concurrent = await Promise.all([
      Account.findByPk(1),
      Account.findByPk(2),
    ]);
    assert.deepEqual(concurrent.map(account => account.email).sort(), [
      'acme@example.com',
      'beta@example.com',
    ]);

    await sequelize.transaction(async transaction => {
      await Account.create({
        id: 3,
        email: 'gamma@example.com',
        active: true,
        meta: { tier: 'trial' },
      }, { transaction });
    });

    try {
      await sequelize.transaction(async transaction => {
        await Account.create({
          id: 4,
          email: 'rolled-back@example.com',
          active: true,
          meta: { tier: 'trial' },
        }, { transaction });
        throw new Error('force rollback');
      });
      assert.fail('rollback transaction should throw');
    } catch (err) {
      assert.equal(err.message, 'force rollback');
    }

    const summary = await sequelize.query(§
      SELECT array_to_string(array_agg(email ORDER BY id), ',') AS emails
      FROM sequelize_accounts
    §, { type: Sequelize.QueryTypes.SELECT });
    assert.deepEqual(summary, [{ emails: 'acme@example.com,beta@example.com,gamma@example.com' }]);
    console.log(JSON.stringify({ ok: true, emails: summary[0].emails }));
  } finally {
    await sequelize.close();
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
	require.NoError(t, err, "Sequelize probe failed: %s", string(out))
	require.Contains(t, string(out), `"ok":true`)
	require.Contains(t, string(out), `"emails":"acme@example.com,beta@example.com,gamma@example.com"`)
}
