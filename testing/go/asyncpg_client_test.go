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

// TestAsyncpgClientSmoke runs the real asyncpg client against Doltgres. The
// psycopg harnesses cover synchronous Python clients; this pins the async pool
// path for startup options, typed parameters, JSONB/text[] values, prepared
// statements, concurrent reads, and transaction boundaries.
func TestAsyncpgClientSmoke(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not available; install Python 3 to enable this harness")
	}
	if testing.Short() {
		t.Skip("asyncpg harness installs asyncpg; skipped under -short")
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
	venv := filepath.Join(work, "venv")
	cmdCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	mk := exec.CommandContext(cmdCtx, "python3", "-m", "venv", venv)
	if out, err := mk.CombinedOutput(); err != nil {
		t.Fatalf("create venv: %v\n%s", err, out)
	}
	pip := filepath.Join(venv, "bin", "pip")
	install := exec.CommandContext(cmdCtx, pip, "install", "--quiet",
		"--disable-pip-version-check",
		"asyncpg==0.31.0")
	install.Env = append(os.Environ(), "PIP_DISABLE_PIP_VERSION_CHECK=1")
	if out, err := install.CombinedOutput(); err != nil {
		t.Fatalf("pip install asyncpg: %v\n%s", err, out)
	}

	script := `import asyncio
import json
import os

import asyncpg

dsn = os.environ["DOLTGRES_URL"]

async def main():
    pool = await asyncpg.create_pool(
        dsn,
        min_size=1,
        max_size=2,
        server_settings={"application_name": "asyncpg-harness"},
    )
    try:
        async with pool.acquire() as conn:
            app_name = await conn.fetchval("SELECT current_setting('application_name')")
            assert app_name == "asyncpg-harness", app_name

            await conn.execute("""CREATE TABLE async_accounts (
                id integer PRIMARY KEY,
                name text NOT NULL UNIQUE,
                active boolean NOT NULL
            )""")
            await conn.execute("""CREATE TABLE async_items (
                id integer PRIMARY KEY,
                account_id integer NOT NULL REFERENCES async_accounts(id),
                amount numeric(10,2) NOT NULL,
                tags text[] NOT NULL,
                payload jsonb NOT NULL
            )""")

            await conn.execute(
                "INSERT INTO async_accounts VALUES ($1::int4, $2::text, $3::bool), ($4::int4, $5::text, $6::bool)",
                1, "acme", True, 2, "beta", False,
            )
            inserted = await conn.fetchrow(
                """INSERT INTO async_items VALUES ($1::int4, $2::int4, $3::text::numeric, $4::text[], $5::jsonb)
                   RETURNING amount::text AS amount, tags[2] AS second_tag, payload #>> '{kind}' AS kind""",
                10, 1, "12.34", ["red", "blue"], json.dumps({"kind": "invoice", "lines": [1, 2]}),
            )
            assert dict(inserted) == {"amount": "12.34", "second_tag": "blue", "kind": "invoice"}, inserted

            stmt = await conn.prepare(
                """SELECT account_id, amount::text AS amount, tags[2] AS second_tag, payload #>> '{kind}' AS kind
                   FROM async_items
                   WHERE account_id = $1::int4 AND tags[2] = $2::text"""
            )
            selected = await stmt.fetchrow(1, "blue")
            assert dict(selected) == {"account_id": 1, "amount": "12.34", "second_tag": "blue", "kind": "invoice"}, selected

        async def account_name(account_id):
            async with pool.acquire() as conn:
                return await conn.fetchval(
                    "SELECT name FROM async_accounts WHERE id = $1::int4",
                    account_id,
                )

        names = sorted(await asyncio.gather(account_name(1), account_name(2)))
        assert names == ["acme", "beta"], names

        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    "INSERT INTO async_accounts VALUES ($1::int4, $2::text, $3::bool)",
                    3, "gamma", True,
                )

        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    await conn.execute(
                        "INSERT INTO async_accounts VALUES ($1::int4, $2::text, $3::bool)",
                        4, "rolled back", True,
                    )
                    raise RuntimeError("rollback transaction")
        except RuntimeError:
            pass

        async with pool.acquire() as conn:
            summary = await conn.fetchval(
                "SELECT array_to_string(array_agg(name ORDER BY id), ',') FROM async_accounts"
            )
            assert summary == "acme,beta,gamma", summary
    finally:
        await pool.close()

    print(json.dumps({"ok": True, "accounts": summary}))

asyncio.run(main())
`
	scriptPath := filepath.Join(work, "harness.py")
	require.NoError(t, os.WriteFile(scriptPath, []byte(script), 0o644))

	url := fmt.Sprintf("postgresql://postgres:password@127.0.0.1:%d/postgres", port)
	pyCmd := exec.CommandContext(cmdCtx, filepath.Join(venv, "bin", "python"), scriptPath)
	pyCmd.Env = append(os.Environ(),
		"DOLTGRES_URL="+url,
		"NO_COLOR=1",
	)
	out, err := pyCmd.CombinedOutput()
	require.NoError(t, err, "asyncpg probe failed: %s", string(out))
	require.Contains(t, string(out), `"ok": true`)
	require.Contains(t, string(out), `"accounts": "acme,beta,gamma"`)
}
