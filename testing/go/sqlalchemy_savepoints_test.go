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

// TestSQLAlchemyNestedTransactions runs SQLAlchemy's nested-
// transaction (savepoint) workflow against a live Doltgres
// instance. SQLAlchemy maps Session.begin_nested() / .commit() /
// .rollback() onto SAVEPOINT / RELEASE / ROLLBACK TO under the
// hood; this is the workload-shape evidence the compatibility
// checklist requires for the savepoint surface.
//
// The harness installs SQLAlchemy and psycopg2-binary into a
// fresh venv and runs a self-contained Python script. Skipped
// under -short so contributors can iterate quickly without
// paying the install cost.
func TestSQLAlchemyNestedTransactions(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not available; install Python 3 to enable this harness")
	}
	if testing.Short() {
		t.Skip("SQLAlchemy harness installs SQLAlchemy + psycopg2-binary; skipped under -short")
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
		"SQLAlchemy==2.0.34", "psycopg[binary]==3.2.13")
	install.Env = append(os.Environ(), "PIP_DISABLE_PIP_VERSION_CHECK=1")
	if out, err := install.CombinedOutput(); err != nil {
		t.Fatalf("pip install SQLAlchemy + psycopg2-binary: %v\n%s", err, out)
	}

	script := `import sys, os
from sqlalchemy import create_engine, text
url = os.environ["DOLTGRES_URL"]
engine = create_engine(url, future=True)

def reset(conn):
    conn.execute(text("DROP TABLE IF EXISTS sa_nested"))
    conn.execute(text("CREATE TABLE sa_nested (id INT PRIMARY KEY, v INT)"))

def assert_rows(conn, expected):
    got = conn.execute(text("SELECT id, v FROM sa_nested ORDER BY id")).fetchall()
    got = [tuple(r) for r in got]
    if got != expected:
        raise SystemExit("rows mismatch: got " + repr(got) + ", expected " + repr(expected))

with engine.begin() as conn:
    reset(conn)

# 1. Outer commit + nested commit -> both writes survive.
with engine.connect() as conn:
    with conn.begin() as outer:
        conn.execute(text("INSERT INTO sa_nested VALUES (1, 1)"))
        with conn.begin_nested() as inner:
            conn.execute(text("INSERT INTO sa_nested VALUES (2, 2)"))
        # leaving the with-block commits the nested savepoint
        conn.execute(text("INSERT INTO sa_nested VALUES (3, 3)"))
    assert_rows(conn, [(1, 1), (2, 2), (3, 3)])

# 2. Nested rollback discards only the inner work.
with engine.connect() as conn:
    with conn.begin() as _:
        reset(conn)
        conn.execute(text("INSERT INTO sa_nested VALUES (10, 10)"))
        try:
            with conn.begin_nested() as _:
                conn.execute(text("INSERT INTO sa_nested VALUES (20, 20)"))
                raise ValueError("rollback the savepoint")
        except ValueError:
            pass
        conn.execute(text("INSERT INTO sa_nested VALUES (30, 30)"))
    assert_rows(conn, [(10, 10), (30, 30)])

# 3. Two-deep nesting: roll back inner only, keep middle, then roll back
#    the middle, keeping only the outermost row.
with engine.connect() as conn:
    with conn.begin() as _:
        reset(conn)
        conn.execute(text("INSERT INTO sa_nested VALUES (100, 1)"))
        with conn.begin_nested() as middle:
            conn.execute(text("INSERT INTO sa_nested VALUES (200, 2)"))
            try:
                with conn.begin_nested() as _:
                    conn.execute(text("INSERT INTO sa_nested VALUES (300, 3)"))
                    raise ValueError("rollback inner")
            except ValueError:
                pass
            # middle still active -> commits when this block exits
        conn.execute(text("INSERT INTO sa_nested VALUES (400, 4)"))
    assert_rows(conn, [(100, 1), (200, 2), (400, 4)])

# 4. Outer rollback throws away every nested commit too.
with engine.connect() as conn:
    with conn.begin() as _:
        reset(conn)
    try:
        with conn.begin() as _:
            conn.execute(text("INSERT INTO sa_nested VALUES (1000, 1)"))
            with conn.begin_nested() as _:
                conn.execute(text("INSERT INTO sa_nested VALUES (2000, 2)"))
            raise ValueError("rollback outer")
    except ValueError:
        pass
    with conn.begin() as _:
        assert_rows(conn, [])

print("ok")
`
	scriptPath := filepath.Join(work, "harness.py")
	require.NoError(t, os.WriteFile(scriptPath, []byte(script), 0o644))

	url := fmt.Sprintf("postgresql+psycopg://postgres:password@127.0.0.1:%d/postgres", port)
	pyCmd := exec.CommandContext(cmdCtx, filepath.Join(venv, "bin", "python"), scriptPath)
	pyCmd.Env = append(os.Environ(),
		"DOLTGRES_URL="+url,
		"NO_COLOR=1",
	)
	out, err := pyCmd.CombinedOutput()
	if err != nil {
		t.Fatalf("SQLAlchemy harness failed: %v\noutput:\n%s", err, out)
	}
	t.Logf("SQLAlchemy harness output:\n%s", out)
}
