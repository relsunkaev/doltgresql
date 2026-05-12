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

// TestSQLAlchemySQLStateBranching is the workload-shape evidence for
// the SQLSTATE error-code mapping flagged in the audit. SQLAlchemy
// translates PostgreSQL SQLSTATE codes (23505, 23503, 23502, 23514,
// 42P01, 42703) into specific SQLAlchemyError subclasses
// (IntegrityError variants and ProgrammingError); other ORMs sitting
// on top of psycopg behave the same way. If doltgres surfaces the
// right SQLSTATE on the wire, the Python script's `except` blocks
// catch the typed exceptions and the script exits with "ok".
func TestSQLAlchemySQLStateBranching(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not available; install Python 3 to enable this harness")
	}
	if testing.Short() {
		t.Skip("SQLAlchemy harness installs SQLAlchemy + psycopg; skipped under -short")
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
		t.Fatalf("pip install SQLAlchemy + psycopg: %v\n%s", err, out)
	}

	script := `import os
from sqlalchemy import create_engine, text
from sqlalchemy import exc

engine = create_engine(os.environ["DOLTGRES_URL"], future=True)

def setup():
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS sa_chk"))
        conn.execute(text("DROP TABLE IF EXISTS sa_child"))
        conn.execute(text("DROP TABLE IF EXISTS sa_par"))
        conn.execute(text("CREATE TABLE sa_par   (id INT PRIMARY KEY, name TEXT NOT NULL)"))
        conn.execute(text("CREATE TABLE sa_child (id INT PRIMARY KEY, parent_id INT REFERENCES sa_par(id))"))
        conn.execute(text("CREATE TABLE sa_chk   (id INT PRIMARY KEY, age INT CHECK (age >= 0))"))
        conn.execute(text("INSERT INTO sa_par VALUES (1, 'p1')"))

def expect_sqlstate(label, sql, code, exc_class):
    try:
        with engine.begin() as conn:
            conn.execute(text(sql))
    except exc_class as e:
        # SQLAlchemy hangs the original SQLSTATE off the underlying
        # DBAPI exception (psycopg's diag.sqlstate).
        sqlstate = getattr(getattr(e.orig, "diag", None), "sqlstate", None)
        if sqlstate == code:
            return
        raise SystemExit("%s: SQLSTATE got %r, want %r (msg: %s)" % (label, sqlstate, code, str(e)))
    raise SystemExit(label + ": expected %s, got success" % exc_class.__name__)

setup()

# 23505 unique_violation -> IntegrityError
expect_sqlstate("duplicate primary key",
    "INSERT INTO sa_par VALUES (1, 'dup')",
    "23505", exc.IntegrityError)

# 23502 not_null_violation -> IntegrityError
expect_sqlstate("null into NOT NULL",
    "INSERT INTO sa_par (id, name) VALUES (2, NULL)",
    "23502", exc.IntegrityError)

# 23503 foreign_key_violation -> IntegrityError
expect_sqlstate("FK child violation",
    "INSERT INTO sa_child VALUES (1, 999)",
    "23503", exc.IntegrityError)

# 23514 check_violation -> IntegrityError
expect_sqlstate("CHECK constraint",
    "INSERT INTO sa_chk VALUES (1, -1)",
    "23514", exc.IntegrityError)

# 42P01 undefined_table -> ProgrammingError
expect_sqlstate("undefined table",
    "SELECT * FROM nope_no_such_table",
    "42P01", exc.ProgrammingError)

# 42703 undefined_column -> ProgrammingError
expect_sqlstate("undefined column",
    "SELECT nope FROM sa_par",
    "42703", exc.ProgrammingError)

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
		t.Fatalf("SQLAlchemy SQLSTATE harness failed: %v\noutput:\n%s", err, out)
	}
	t.Logf("SQLAlchemy SQLSTATE harness output:\n%s", out)
}
