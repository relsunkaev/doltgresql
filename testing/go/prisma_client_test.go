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

// TestPrismaClientSmoke runs the real generated Prisma Client against
// Doltgres. This pins runtime ORM behavior separately from Prisma's db pull
// introspection path: generated model CRUD, typed values, JSONB/text[] fields,
// relation reads, raw parameter binding, concurrent reads, commit, and rollback.
func TestPrismaClientSmoke(t *testing.T) {
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("node not on PATH; install Node.js to enable this harness")
	}
	if _, err := exec.LookPath("npm"); err != nil {
		t.Skip("npm not on PATH; install Node.js to enable this harness")
	}
	if testing.Short() {
		t.Skip("Prisma Client installs native query-engine packages; skipped under -short")
	}

	port, err := gms.GetEmptyPort()
	require.NoError(t, err)
	ctx, defaultConn, controller := CreateServerWithPort(t, "postgres", port)
	t.Cleanup(func() {
		defaultConn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	setUp := []string{
		`CREATE TABLE prisma_client_accounts (
			id INT PRIMARY KEY,
			email TEXT UNIQUE NOT NULL,
			active BOOLEAN NOT NULL DEFAULT true,
			meta JSONB NOT NULL
		);`,
		`CREATE TABLE prisma_client_items (
			id INT PRIMARY KEY,
			account_id INT NOT NULL REFERENCES prisma_client_accounts(id),
			amount NUMERIC(10, 2) NOT NULL,
			tags TEXT[] NOT NULL,
			payload JSONB NOT NULL
		);`,
	}
	for _, q := range setUp {
		_, err = defaultConn.Exec(ctx, q)
		require.NoError(t, err, "setup: %s", q)
	}

	work := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(work, "package.json"), []byte(`{
  "name": "doltgres-prisma-client-harness",
  "private": true
}
`), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(work, "schema.prisma"), []byte(`generator client {
  provider = "prisma-client-js"
}

datasource db {
  provider = "postgresql"
  url      = env("DATABASE_URL")
}

model PrismaClientAccount {
  id     Int                @id
  email  String             @unique
  active Boolean            @default(true)
  meta   Json
  items  PrismaClientItem[]

  @@map("prisma_client_accounts")
}

model PrismaClientItem {
  id        Int                 @id
  accountId Int                 @map("account_id")
  amount    Decimal             @db.Decimal(10, 2)
  tags      String[]
  payload   Json
  account   PrismaClientAccount @relation(fields: [accountId], references: [id])

  @@map("prisma_client_items")
}
`), 0o644))

	cmdCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	install := exec.CommandContext(cmdCtx, "npm", "install", "--silent",
		"--no-audit", "--no-fund",
		"prisma@6.19.3", "@prisma/client@6.19.3",
	)
	install.Dir = work
	install.Env = append(os.Environ(), "NO_COLOR=1")
	if out, err := install.CombinedOutput(); err != nil {
		t.Fatalf("npm install prisma + @prisma/client failed: %v\n%s", err, string(out))
	}

	url := fmt.Sprintf("postgres://postgres:password@127.0.0.1:%d/postgres?sslmode=disable&application_name=prisma-client-harness", port)
	generate := exec.CommandContext(cmdCtx,
		filepath.Join(work, "node_modules", ".bin", "prisma"),
		"generate",
		"--schema", "schema.prisma",
	)
	generate.Dir = work
	generate.Env = append(os.Environ(), "NO_COLOR=1", "DATABASE_URL="+url)
	if out, err := generate.CombinedOutput(); err != nil {
		t.Fatalf("prisma generate failed: %v\n%s", err, string(out))
	}

	probe := strings.ReplaceAll(`
const assert = require('node:assert/strict');
const { Prisma, PrismaClient } = require('@prisma/client');

async function main() {
  const url = process.argv[2];
  const prisma = new PrismaClient({
    datasources: {
      db: { url },
    },
  });

  try {
    const appName = await prisma.$queryRaw__BT__SELECT current_setting('application_name') AS app_name__BT__;
    assert.equal(appName[0].app_name, 'prisma-client-harness');

    await prisma.prismaClientAccount.createMany({
      data: [
        { id: 1, email: 'acme@example.com', active: true, meta: { tier: 'pro' } },
        { id: 2, email: 'beta@example.com', active: false, meta: { tier: 'free' } },
      ],
    });

    const created = await prisma.prismaClientItem.create({
      data: {
        id: 10,
        account: { connect: { id: 1 } },
        amount: new Prisma.Decimal('12.34'),
        tags: ['red', 'blue'],
        payload: { kind: 'invoice', lines: [1, 2] },
      },
      include: { account: true },
    });
    assert.equal(created.account.email, 'acme@example.com');
    assert.equal(created.account.active, true);
    assert.equal(created.amount.toString(), '12.34');
    assert.deepEqual(created.tags, ['red', 'blue']);
    assert.equal(created.payload.kind, 'invoice');

    const selected = await prisma.prismaClientItem.findUnique({
      where: { id: 10 },
      include: { account: true },
    });
    assert.equal(selected.account.email, 'acme@example.com');
    assert.equal(selected.amount.toString(), '12.34');
    assert.deepEqual(selected.tags, ['red', 'blue']);
    assert.equal(selected.payload.kind, 'invoice');

    const raw = await prisma.$queryRaw__BT__
      SELECT a.email, i.amount::text AS amount, i.tags[2] AS tag, i.payload #>> '{kind}' AS kind
      FROM prisma_client_items i
      JOIN prisma_client_accounts a ON a.id = i.account_id
      WHERE i.account_id = ${1}::int4 AND ${'blue'} = ANY(i.tags)
    __BT__;
    assert.deepEqual(raw, [{
      email: 'acme@example.com',
      amount: '12.34',
      tag: 'blue',
      kind: 'invoice',
    }]);

    const concurrent = await Promise.all([
      prisma.prismaClientAccount.findUnique({ where: { id: 1 } }),
      prisma.prismaClientAccount.findUnique({ where: { id: 2 } }),
    ]);
    assert.deepEqual(concurrent.map(account => account.email).sort(), [
      'acme@example.com',
      'beta@example.com',
    ]);

    await prisma.$transaction(async tx => {
      await tx.prismaClientAccount.create({
        data: {
          id: 3,
          email: 'gamma@example.com',
          active: true,
          meta: { tier: 'trial' },
        },
      });
    });

    try {
      await prisma.$transaction(async tx => {
        await tx.prismaClientAccount.create({
          data: {
            id: 4,
            email: 'rolled-back@example.com',
            active: true,
            meta: { tier: 'trial' },
          },
        });
        throw new Error('force rollback');
      });
      assert.fail('rollback transaction should throw');
    } catch (err) {
      assert.equal(err.message, 'force rollback');
    }

    const summary = await prisma.$queryRaw__BT__
      SELECT array_to_string(array_agg(email ORDER BY id), ',') AS emails
      FROM prisma_client_accounts
    __BT__;
    assert.deepEqual(summary, [{
      emails: 'acme@example.com,beta@example.com,gamma@example.com',
    }]);
    console.log(JSON.stringify({ ok: true, emails: summary[0].emails }));
  } finally {
    await prisma.$disconnect();
  }
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
`, "__BT__", "`")
	require.NoError(t, os.WriteFile(filepath.Join(work, "probe.cjs"), []byte(probe), 0o644))

	cmd := exec.CommandContext(cmdCtx, "node", "probe.cjs", url)
	cmd.Dir = work
	cmd.Env = append(os.Environ(), "NO_COLOR=1", "DATABASE_URL="+url)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "Prisma Client probe failed: %s", string(out))
	require.Contains(t, string(out), `"ok":true`)
	require.Contains(t, string(out), `"emails":"acme@example.com,beta@example.com,gamma@example.com"`)
}
