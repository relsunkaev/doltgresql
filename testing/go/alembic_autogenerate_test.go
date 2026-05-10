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

// TestAlembicAutogenerate runs the actual Alembic autogenerate path
// against a live Doltgres instance. It verifies that SQLAlchemy's
// inspector can recover the existing schema shape without producing
// spurious migration operations.
func TestAlembicAutogenerate(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not available; install Python 3 to enable this harness")
	}
	if testing.Short() {
		t.Skip("Alembic autogenerate installs Alembic + SQLAlchemy + psycopg; skipped under -short")
	}

	port, err := gms.GetEmptyPort()
	require.NoError(t, err)
	ctx, defaultConn, controller := CreateServerWithPort(t, "postgres", port)
	t.Cleanup(func() {
		defaultConn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	for _, q := range []string{
		`CREATE TABLE al_autogen_accounts (
			id INT PRIMARY KEY,
			email TEXT UNIQUE NOT NULL,
			active BOOL NOT NULL DEFAULT true,
			metadata JSONB NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);`,
		`CREATE TABLE al_autogen_orders (
			id INT PRIMARY KEY,
			account_id INT NOT NULL REFERENCES al_autogen_accounts(id),
			status TEXT NOT NULL,
			tags TEXT[] NOT NULL,
			total NUMERIC(10, 2) NOT NULL
		);`,
		`CREATE INDEX ix_al_autogen_orders_account ON al_autogen_orders(account_id);`,
		`CREATE TABLE al_autogen_items (
			order_id INT REFERENCES al_autogen_orders(id),
			line_no INT,
			sku TEXT NOT NULL,
			PRIMARY KEY (order_id, line_no)
		);`,
	} {
		_, err := defaultConn.Exec(ctx, q)
		require.NoError(t, err, "seed: %s", q)
	}

	work := t.TempDir()
	venv := filepath.Join(work, "venv")
	cmdCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	if out, err := exec.CommandContext(cmdCtx, "python3", "-m", "venv", venv).CombinedOutput(); err != nil {
		t.Fatalf("create venv: %v\n%s", err, out)
	}
	pip := filepath.Join(venv, "bin", "pip")
	install := exec.CommandContext(cmdCtx, pip, "install", "--quiet",
		"--disable-pip-version-check",
		"alembic==1.13.2", "SQLAlchemy==2.0.34", "psycopg[binary]==3.3.4")
	install.Env = append(os.Environ(), "PIP_DISABLE_PIP_VERSION_CHECK=1")
	if out, err := install.CombinedOutput(); err != nil {
		t.Fatalf("pip install Alembic + SQLAlchemy + psycopg: %v\n%s", err, out)
	}

	require.NoError(t, os.MkdirAll(filepath.Join(work, "alembic", "versions"), 0o755))
	url := fmt.Sprintf("postgresql+psycopg://postgres:password@127.0.0.1:%d/postgres", port)

	require.NoError(t, os.WriteFile(filepath.Join(work, "alembic.ini"), []byte(fmt.Sprintf(`[alembic]
script_location = alembic
sqlalchemy.url = %s

[loggers]
keys = root

[handlers]
keys = console

[formatters]
keys = generic

[logger_root]
level = WARN
handlers = console
qualname =

[handler_console]
class = StreamHandler
args = (sys.stderr,)
level = NOTSET
formatter = generic

[formatter_generic]
format = %%(levelname)-5.5s [%%(name)s] %%(message)s
`, url)), 0o644))

	require.NoError(t, os.WriteFile(filepath.Join(work, "alembic", "script.py.mako"), []byte(`"""${message}

Revision ID: ${up_revision}
Revises: ${down_revision | comma,n}
Create Date: ${create_date}
"""
from alembic import op
import sqlalchemy as sa

${imports if imports else ""}

revision = ${repr(up_revision)}
down_revision = ${repr(down_revision)}
branch_labels = ${repr(branch_labels)}
depends_on = ${repr(depends_on)}

def upgrade() -> None:
    ${upgrades if upgrades else "pass"}

def downgrade() -> None:
    ${downgrades if downgrades else "pass"}
`), 0o644))

	require.NoError(t, os.WriteFile(filepath.Join(work, "alembic", "env.py"), []byte(`from logging.config import fileConfig
import sqlalchemy as sa
from sqlalchemy import (
    Boolean,
    Column,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    Numeric,
    Table,
    Text,
    TIMESTAMP,
    true,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy import engine_from_config, pool
from alembic import context

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = MetaData()

Table(
    "al_autogen_accounts",
    target_metadata,
    Column("id", Integer, primary_key=True),
    Column("email", Text, nullable=False, unique=True),
    Column("active", Boolean, nullable=False, server_default=true()),
    Column("metadata", JSONB, nullable=False),
    Column("created_at", TIMESTAMP, server_default=sa.text("CURRENT_TIMESTAMP")),
)

Table(
    "al_autogen_orders",
    target_metadata,
    Column("id", Integer, primary_key=True),
    Column("account_id", Integer, ForeignKey("al_autogen_accounts.id"), nullable=False),
    Column("status", Text, nullable=False),
    Column("tags", ARRAY(Text), nullable=False),
    Column("total", Numeric(10, 2), nullable=False),
    Index("ix_al_autogen_orders_account", "account_id"),
)

Table(
    "al_autogen_items",
    target_metadata,
    Column("order_id", Integer, ForeignKey("al_autogen_orders.id"), primary_key=True),
    Column("line_no", Integer, primary_key=True),
    Column("sku", Text, nullable=False),
)

def include_name(name, type_, parent_names):
    if type_ == "table":
        return name in {
            "al_autogen_accounts",
            "al_autogen_orders",
            "al_autogen_items",
        }
    return True

def run_migrations_online():
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            include_name=include_name,
            compare_type=True,
            compare_server_default=False,
        )
        with context.begin_transaction():
            context.run_migrations()

run_migrations_online()
`), 0o644))

	alembic := filepath.Join(venv, "bin", "alembic")
	revision := exec.CommandContext(cmdCtx,
		alembic, "-c", "alembic.ini", "revision", "--autogenerate", "-m", "autogen check")
	revision.Dir = work
	revision.Env = append(os.Environ(), "PYTHONUNBUFFERED=1", "NO_COLOR=1")
	out, err := revision.CombinedOutput()
	if err != nil {
		t.Fatalf("alembic revision --autogenerate failed: %v\n%s", err, out)
	}

	files, err := filepath.Glob(filepath.Join(work, "alembic", "versions", "*_autogen_check.py"))
	require.NoError(t, err)
	require.Len(t, files, 1, "autogenerate should write one revision file; output:\n%s", out)

	generatedBytes, err := os.ReadFile(files[0])
	require.NoError(t, err)
	generated := string(generatedBytes)
	require.Contains(t, generated, "def upgrade()")
	require.Contains(t, generated, "def downgrade()")
	for _, unexpected := range []string{
		"op.create_table",
		"op.drop_table",
		"op.add_column",
		"op.drop_column",
		"op.create_index",
		"op.drop_index",
		"op.create_foreign_key",
		"op.drop_constraint",
	} {
		require.NotContains(t, generated, unexpected,
			"Alembic autogenerate produced a spurious migration operation in:\n%s", generated)
	}
	if !strings.Contains(generated, "pass") {
		t.Fatalf("expected an empty autogenerate migration, got:\n%s", generated)
	}
}
