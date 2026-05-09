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

// TestTsPostgresClientSmoke runs the real ts-postgres client against
// Doltgres. ts-postgres asks for binary result data by default, so this
// pins a Node client path that differs from postgres.js and pgx.
func TestTsPostgresClientSmoke(t *testing.T) {
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
  "name": "doltgres-ts-postgres-harness",
  "private": true,
  "type": "module"
}
`), 0o644))

	cmdCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	install := exec.CommandContext(cmdCtx, "npm", "install", "--silent",
		"--no-audit", "--no-fund", "ts-postgres@2.0.4",
	)
	install.Dir = work
	install.Env = append(os.Environ(), "NO_COLOR=1")
	if out, err := install.CombinedOutput(); err != nil {
		t.Fatalf("npm install ts-postgres failed: %v\n%s", err, string(out))
	}

	probe := strings.ReplaceAll(`
import assert from 'node:assert/strict';
import { connect, SSLMode } from 'ts-postgres';

const port = Number(process.argv[2]);
const client = await connect({
  host: '127.0.0.1',
  port,
  user: 'postgres',
	password: 'password',
	database: 'postgres',
	ssl: SSLMode.Disable,
});

try {
	await client.query(§SET application_name TO 'ts-postgres-harness'§);
	const setting = await client.query(
		§SELECT current_setting('application_name') AS app_name§
	);
  assert.equal(setting.rows[0].get('app_name'), 'ts-postgres-harness');

	await client.query(§CREATE TABLE tsp_accounts (
		id integer PRIMARY KEY,
		name text NOT NULL UNIQUE,
		active boolean NOT NULL
	)§);
	await client.query(§CREATE TABLE tsp_items (
		id integer PRIMARY KEY,
		account_id integer NOT NULL REFERENCES tsp_accounts(id),
		amount numeric(10,2) NOT NULL,
		note text NOT NULL
	)§);

  await client.query(
		§INSERT INTO tsp_accounts VALUES ($1::int4, $2::text, $3::bool), ($4::int4, $5::text, $6::bool)§,
    [1, 'acme', true, 2, 'beta', false]
  );
	await client.query(
		§INSERT INTO tsp_items VALUES ($1::int4, $2::int4, $3::text::numeric, $4::text)§,
		[10, 1, '12.34', 'first']
	);

  const selected = await client.query(
		§SELECT a.name, a.active, i.amount::text AS amount, i.note
		   FROM tsp_items i
		   JOIN tsp_accounts a ON a.id = i.account_id
		  WHERE i.account_id = $1::int4§,
    [1]
  );
  assert.deepEqual([...selected], [
    { name: 'acme', active: true, amount: '12.34', note: 'first' },
  ]);

  const prepared = await client.prepare(
		§SELECT name FROM tsp_accounts WHERE id = $1::int4§
  );
  try {
    const [first, second] = await Promise.all([
      prepared.execute([1]).one(),
      prepared.execute([2]).one(),
    ]);
    assert.deepEqual([first.name, second.name].sort(), ['acme', 'beta']);
  } finally {
    await prepared.close();
  }

  await client.query('BEGIN');
  await client.query(
		§INSERT INTO tsp_accounts VALUES ($1::int4, $2::text, $3::bool)§,
    [3, 'gamma', true]
  );
  await client.query('COMMIT');

  let rolledBack = false;
  try {
    await client.query('BEGIN');
    await client.query(
			§INSERT INTO tsp_accounts VALUES ($1::int4, $2::text, $3::bool)§,
      [4, 'rolled back', true]
    );
    throw new Error('force rollback');
  } catch (err) {
    rolledBack = err.message === 'force rollback';
    await client.query('ROLLBACK');
  }
  assert.equal(rolledBack, true);

  const summary = await client.query(
		§SELECT array_to_string(array_agg(name ORDER BY id), ',') AS names
		   FROM tsp_accounts§
  );
  assert.deepEqual([...summary], [{ names: 'acme,beta,gamma' }]);
  console.log(JSON.stringify({ ok: true, accounts: summary.rows[0].get('names') }));
} finally {
  await client.end();
}
`, "§", "`")
	require.NoError(t, os.WriteFile(filepath.Join(work, "probe.mjs"), []byte(probe), 0o644))

	cmd := exec.CommandContext(cmdCtx, "node", "probe.mjs", fmt.Sprint(port))
	cmd.Dir = work
	cmd.Env = append(os.Environ(), "NO_COLOR=1")
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "ts-postgres probe failed: %s", string(out))
	require.Contains(t, string(out), `"ok":true`)
	require.Contains(t, string(out), `"accounts":"acme,beta,gamma"`)
}
