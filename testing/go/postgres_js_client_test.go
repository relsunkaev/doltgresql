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

// TestPostgresJSClientSmoke runs the real postgres.js client against
// Doltgres. This pins a secondary Node driver path separate from the
// existing node-postgres / drizzle-kit coverage.
func TestPostgresJSClientSmoke(t *testing.T) {
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
  "name": "doltgres-postgres-js-harness",
  "private": true
}
`), 0o644))

	cmdCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	install := exec.CommandContext(cmdCtx, "npm", "install", "--silent",
		"--no-audit", "--no-fund", "postgres@3.4.5",
	)
	install.Dir = work
	install.Env = append(os.Environ(), "NO_COLOR=1")
	if out, err := install.CombinedOutput(); err != nil {
		t.Fatalf("npm install postgres failed: %v\n%s", err, string(out))
	}

	probe := strings.ReplaceAll(`
const assert = require('node:assert/strict');
const postgres = require('postgres');

async function main() {
  const url = process.argv[2];
  const sql = postgres(url, {
    max: 2,
    prepare: true,
    idle_timeout: 1,
    connect_timeout: 5,
  });

  try {
    await sql§SET application_name TO 'postgres-js-harness'§;
    await sql§CREATE TABLE js_accounts (
      id integer PRIMARY KEY,
      name text NOT NULL UNIQUE
    )§;
    await sql§CREATE TABLE js_items (
      id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
      account_id integer NOT NULL REFERENCES js_accounts(id),
      amount numeric(10,2) NOT NULL,
      active boolean NOT NULL,
      tags text[] NOT NULL,
      payload jsonb NOT NULL
    )§;

    await sql§INSERT INTO js_accounts VALUES (1, 'acme'), (2, 'beta')§;
    const inserted = await sql§
      INSERT INTO js_items (account_id, amount, active, tags, payload)
      VALUES (${1}, ${12.34}, ${true}, ARRAY['red', 'blue'], ${sql.json({ kind: 'invoice', items: [1, 2] })})
      RETURNING length(id::text)::text AS id_length, amount::text AS amount, active::text AS active, payload #>> '{kind}' AS kind
    §;
    assert.deepEqual(Array.from(inserted), [{ id_length: '36', amount: '12.34', active: 'true', kind: 'invoice' }]);

    const filtered = await sql§
      SELECT a.name, array_length(i.tags, 1)::text AS tag_count
      FROM js_items i
      JOIN js_accounts a ON a.id = i.account_id
      WHERE i.account_id = ${1} AND 'blue' = ANY(i.tags)
    §;
    assert.deepEqual(Array.from(filtered), [{ name: 'acme', tag_count: '2' }]);

    const elems = await sql§
      SELECT elem::text AS elem
      FROM js_items i
      JOIN LATERAL jsonb_array_elements(i.payload->'items') AS elem ON true
      ORDER BY elem::text
    §;
    assert.deepEqual(Array.from(elems), [{ elem: '1' }, { elem: '2' }]);

    const concurrent = await Promise.all([
      sql§SELECT name FROM js_accounts WHERE id = ${1}§,
      sql§SELECT name FROM js_accounts WHERE id = ${2}§,
    ]);
    assert.deepEqual(concurrent.map(rows => rows[0].name).sort(), ['acme', 'beta']);

    await sql.begin(async tx => {
      await tx§INSERT INTO js_accounts VALUES (3, 'gamma')§;
    });

    let rolledBack = false;
    try {
      await sql.begin(async tx => {
        await tx§INSERT INTO js_accounts VALUES (4, 'rolled back')§;
        throw new Error('force rollback');
      });
    } catch (err) {
      rolledBack = err.message === 'force rollback';
    }
    assert.equal(rolledBack, true);

    const summary = await sql§
      SELECT array_to_string(array_agg(name ORDER BY id), ',') AS names
      FROM js_accounts
    §;
    assert.deepEqual(Array.from(summary), [{ names: 'acme,beta,gamma' }]);
    console.log(JSON.stringify({ ok: true, accounts: summary[0].names }));
  } finally {
    await sql.end({ timeout: 5 });
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
	require.NoError(t, err, "postgres.js probe failed: %s", string(out))
	require.Contains(t, string(out), `"ok":true`)
	require.Contains(t, string(out), `"accounts":"acme,beta,gamma"`)
}
