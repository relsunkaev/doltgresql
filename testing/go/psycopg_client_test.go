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

// TestPsycopgClientSmoke runs the real psycopg3 pool against
// Doltgres. SQLAlchemy coverage already pins ORM SQLSTATE and
// savepoint behavior through psycopg; this test pins the direct
// Python driver path for pooled queries, parameters, JSONB/array
// adaptation, concurrent reads, and transaction boundaries.
func TestPsycopgClientSmoke(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not available; install Python 3 to enable this harness")
	}
	if testing.Short() {
		t.Skip("psycopg harness installs psycopg; skipped under -short")
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
		"psycopg[binary,pool]==3.3.4")
	install.Env = append(os.Environ(), "PIP_DISABLE_PIP_VERSION_CHECK=1")
	if out, err := install.CombinedOutput(); err != nil {
		t.Fatalf("pip install psycopg: %v\n%s", err, out)
	}

	script := `import json
import os
from concurrent.futures import ThreadPoolExecutor

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

conninfo = os.environ["DOLTGRES_URL"]

with ConnectionPool(
    conninfo,
    kwargs={"application_name": "psycopg-harness", "row_factory": dict_row},
    min_size=1,
    max_size=2,
) as pool:
    with pool.connection() as conn:
        app_name = conn.execute(
            "SELECT current_setting('application_name') AS app_name"
        ).fetchone()["app_name"]
        assert app_name == "psycopg-harness", app_name

        conn.execute("""CREATE TABLE py_accounts (
            id integer PRIMARY KEY,
            name text NOT NULL UNIQUE,
            active boolean NOT NULL
        )""")
        conn.execute("""CREATE TABLE py_items (
            id integer PRIMARY KEY,
            account_id integer NOT NULL REFERENCES py_accounts(id),
            amount numeric(10,2) NOT NULL,
            tags text[] NOT NULL,
            payload jsonb NOT NULL
        )""")

        conn.execute(
            "INSERT INTO py_accounts VALUES (%s::int4, %s::text, %s::bool), (%s::int4, %s::text, %s::bool)",
            (1, "acme", True, 2, "beta", False),
        )
        inserted = conn.execute(
            """INSERT INTO py_items VALUES (%s::int4, %s::int4, %s::text::numeric, %s::text[], %s::jsonb)
               RETURNING amount::text AS amount, tags[2] AS second_tag, payload #>> '{kind}' AS kind""",
            (10, 1, "12.34", ["red", "blue"], Jsonb({"kind": "invoice", "lines": [1, 2]})),
        ).fetchone()
        assert inserted == {"amount": "12.34", "second_tag": "blue", "kind": "invoice"}, inserted

        selected = conn.execute(
            """SELECT account_id, amount::text AS amount, tags[2] AS second_tag, payload #>> '{kind}' AS kind
               FROM py_items
               WHERE account_id = %s::int4 AND tags[2] = %s::text""",
            (1, "blue"),
        ).fetchone()
        assert selected == {"account_id": 1, "amount": "12.34", "second_tag": "blue", "kind": "invoice"}, selected
        conn.commit()

    def account_name(account_id):
        with pool.connection() as conn:
            return conn.execute(
                "SELECT name FROM py_accounts WHERE id = %s::int4",
                (account_id,),
            ).fetchone()["name"]

    with ThreadPoolExecutor(max_workers=2) as executor:
        names = sorted(executor.map(account_name, [1, 2]))
    assert names == ["acme", "beta"], names

    with pool.connection() as conn:
        with conn.transaction():
            conn.execute(
                "INSERT INTO py_accounts VALUES (%s::int4, %s::text, %s::bool)",
                (3, "gamma", True),
            )

    with pool.connection() as conn:
        try:
            with conn.transaction():
                conn.execute(
                    "INSERT INTO py_accounts VALUES (%s::int4, %s::text, %s::bool)",
                    (4, "rolled back", True),
                )
                raise RuntimeError("rollback transaction")
        except RuntimeError:
            pass

    with pool.connection() as conn:
        summary = conn.execute(
            "SELECT array_to_string(array_agg(name ORDER BY id), ',') AS names FROM py_accounts"
        ).fetchone()["names"]
        assert summary == "acme,beta,gamma", summary

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
	require.NoError(t, err, "psycopg probe failed: %s", string(out))
	require.Contains(t, string(out), `"ok": true`)
	require.Contains(t, string(out), `"accounts": "acme,beta,gamma"`)
}
