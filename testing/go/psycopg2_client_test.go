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

// TestPsycopg2ClientSmoke runs the real psycopg2 client against Doltgres. The
// psycopg3 harness covers the modern Python driver; this pins the still-common
// psycopg2 pool path, typed parameters, JSONB/array adaptation, prepared
// statements, concurrent reads, and transaction boundaries.
func TestPsycopg2ClientSmoke(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not available; install Python 3 to enable this harness")
	}
	if testing.Short() {
		t.Skip("psycopg2 harness installs psycopg2-binary; skipped under -short")
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
		"psycopg2-binary==2.9.11")
	install.Env = append(os.Environ(), "PIP_DISABLE_PIP_VERSION_CHECK=1")
	if out, err := install.CombinedOutput(); err != nil {
		t.Fatalf("pip install psycopg2-binary: %v\n%s", err, out)
	}

	script := `import json
import os
from concurrent.futures import ThreadPoolExecutor

import psycopg2
from psycopg2.extras import Json, RealDictCursor
from psycopg2.pool import ThreadedConnectionPool

dsn = os.environ["DOLTGRES_URL"]

pool = ThreadedConnectionPool(
    1,
    2,
    dsn,
    application_name="psycopg2-harness",
    cursor_factory=RealDictCursor,
)

def borrow():
    conn = pool.getconn()
    conn.autocommit = False
    return conn

def release(conn):
    pool.putconn(conn)

summary = None

try:
    conn = borrow()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT current_setting('application_name') AS app_name")
            assert cur.fetchone()["app_name"] == "psycopg2-harness"

            cur.execute("""CREATE TABLE py2_accounts (
                id integer PRIMARY KEY,
                name text NOT NULL UNIQUE,
                active boolean NOT NULL
            )""")
            cur.execute("""CREATE TABLE py2_items (
                id integer PRIMARY KEY,
                account_id integer NOT NULL REFERENCES py2_accounts(id),
                amount numeric(10,2) NOT NULL,
                tags text[] NOT NULL,
                payload jsonb NOT NULL
            )""")

            cur.execute(
                "INSERT INTO py2_accounts VALUES (%s::int4, %s::text, %s::bool), (%s::int4, %s::text, %s::bool)",
                (1, "acme", True, 2, "beta", False),
            )
            cur.execute(
                """INSERT INTO py2_items VALUES (%s::int4, %s::int4, %s::text::numeric, %s::text[], %s::jsonb)
                   RETURNING amount::text AS amount, tags[2] AS second_tag, payload #>> '{kind}' AS kind""",
                (10, 1, "12.34", ["red", "blue"], Json({"kind": "invoice", "lines": [1, 2]})),
            )
            inserted = cur.fetchone()
            assert dict(inserted) == {"amount": "12.34", "second_tag": "blue", "kind": "invoice"}, inserted

            cur.execute(
                """PREPARE py2_lookup(integer, text) AS
                   SELECT account_id, amount::text AS amount, tags[2] AS second_tag, payload #>> '{kind}' AS kind
                   FROM py2_items
                   WHERE account_id = $1 AND tags[2] = $2"""
            )
            cur.execute("EXECUTE py2_lookup(%s::int4, %s::text)", (1, "blue"))
            selected = cur.fetchone()
            assert dict(selected) == {"account_id": 1, "amount": "12.34", "second_tag": "blue", "kind": "invoice"}, selected
        conn.commit()
    finally:
        release(conn)

    def account_name(account_id):
        conn = borrow()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT name FROM py2_accounts WHERE id = %s::int4", (account_id,))
                return cur.fetchone()["name"]
        finally:
            release(conn)

    with ThreadPoolExecutor(max_workers=2) as executor:
        names = sorted(executor.map(account_name, [1, 2]))
    assert names == ["acme", "beta"], names

    conn = borrow()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO py2_accounts VALUES (%s::int4, %s::text, %s::bool)",
                    (3, "gamma", True),
                )
    finally:
        release(conn)

    conn = borrow()
    try:
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO py2_accounts VALUES (%s::int4, %s::text, %s::bool)",
                        (4, "rolled back", True),
                    )
                    raise RuntimeError("rollback transaction")
        except RuntimeError:
            pass
    finally:
        release(conn)

    conn = borrow()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT array_to_string(array_agg(name ORDER BY id), ',') AS names FROM py2_accounts")
            summary = cur.fetchone()["names"]
            assert summary == "acme,beta,gamma", summary
    finally:
        release(conn)
finally:
    pool.closeall()

print(json.dumps({"ok": True, "accounts": summary}))
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
	require.NoError(t, err, "psycopg2 probe failed: %s", string(out))
	require.Contains(t, string(out), `"ok": true`)
	require.Contains(t, string(out), `"accounts": "acme,beta,gamma"`)
}
