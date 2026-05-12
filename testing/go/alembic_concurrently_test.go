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

// TestAlembicConcurrentlyMigration runs a real Alembic migration
// against a live Doltgres instance, emitting `CREATE INDEX
// CONCURRENTLY` and `DROP INDEX CONCURRENTLY` via the canonical
// op.create_index(postgresql_concurrently=True) /
// op.drop_index(postgresql_concurrently=True) helpers. This is
// the migration-tool workload-corpus evidence the audit requested
// for the CONCURRENTLY surface — without it, doltgres' previously-
// claimed support was only proven against raw SQL, not against
// the migration tooling that actually emits these statements in
// production.
//
// The harness installs Alembic + SQLAlchemy + psycopg3, writes a
// minimal env.py + versions/<id>_concurrent_index.py migration,
// and runs `alembic upgrade head` followed by `alembic downgrade
// -1`. Either step would fail if the CONCURRENTLY keyword no
// longer parsed.
func TestAlembicConcurrentlyMigration(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not available; install Python 3 to enable this harness")
	}
	if testing.Short() {
		t.Skip("Alembic harness installs Alembic + SQLAlchemy + psycopg; skipped under -short")
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

	if out, err := exec.CommandContext(cmdCtx, "python3", "-m", "venv", venv).CombinedOutput(); err != nil {
		t.Fatalf("create venv: %v\n%s", err, out)
	}
	pip := filepath.Join(venv, "bin", "pip")
	install := exec.CommandContext(cmdCtx, pip, "install", "--quiet",
		"--disable-pip-version-check",
		"alembic==1.13.2", "SQLAlchemy==2.0.34", "psycopg[binary]==3.2.13")
	install.Env = append(os.Environ(), "PIP_DISABLE_PIP_VERSION_CHECK=1")
	if out, err := install.CombinedOutput(); err != nil {
		t.Fatalf("pip install Alembic + SQLAlchemy + psycopg: %v\n%s", err, out)
	}

	// Seed the schema Alembic will index. Use the default psycopg
	// connection so we don't have to wire up Alembic's
	// op.create_table here.
	for _, q := range []string{
		`CREATE TABLE alembic_t (id INT PRIMARY KEY, code TEXT, hits INT);`,
		`INSERT INTO alembic_t VALUES (1, 'a', 1), (2, 'b', 2);`,
	} {
		_, err := defaultConn.Exec(ctx, q)
		require.NoError(t, err, "seed: %s", q)
	}

	// Lay down the alembic project layout.
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

	// Minimal env.py that runs migrations online.
	require.NoError(t, os.WriteFile(filepath.Join(work, "alembic", "env.py"), []byte(`from logging.config import fileConfig
from sqlalchemy import engine_from_config, pool
from alembic import context

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = None

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
            transaction_per_migration=True,
        )
        with context.begin_transaction():
            context.run_migrations()

run_migrations_online()
`), 0o644))

	// One migration that mirrors what a developer would hand-write
	// after running `alembic revision -m "concurrent index"`.
	require.NoError(t, os.WriteFile(filepath.Join(work, "alembic", "versions", "001_concurrent.py"), []byte(`"""concurrent index

Revision ID: 001
Revises:
Create Date: 2026-05-07 00:00:00.000000
"""
from alembic import op

revision = "001"
down_revision = None
branch_labels = None
depends_on = None

def upgrade() -> None:
    # Real PG forbids CONCURRENTLY inside a transaction; Alembic's
    # supported escape hatch is an autocommit block around the
    # concurrent DDL.
    with op.get_context().autocommit_block():
        op.create_index(
            "ix_alembic_t_code",
            "alembic_t",
            ["code"],
            postgresql_concurrently=True,
        )
        op.create_index(
            "ix_alembic_t_hits",
            "alembic_t",
            ["hits"],
            unique=True,
            postgresql_concurrently=True,
        )

def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.drop_index(
            "ix_alembic_t_hits",
            table_name="alembic_t",
            postgresql_concurrently=True,
        )
        op.drop_index(
            "ix_alembic_t_code",
            table_name="alembic_t",
            postgresql_concurrently=True,
        )
`), 0o644))

	alembic := filepath.Join(venv, "bin", "alembic")

	upgrade := exec.CommandContext(cmdCtx, alembic, "-c", "alembic.ini", "upgrade", "head")
	upgrade.Dir = work
	upgrade.Env = append(os.Environ(), "PYTHONUNBUFFERED=1", "NO_COLOR=1")
	if out, err := upgrade.CombinedOutput(); err != nil {
		t.Fatalf("alembic upgrade head failed: %v\n%s", err, out)
	}

	// Sanity: confirm both indexes were created via pg_indexes.
	var n int
	require.NoError(t, defaultConn.Default.QueryRow(ctx,
		`SELECT count(*) FROM pg_catalog.pg_indexes
		 WHERE tablename = 'alembic_t'
		   AND indexname IN ('ix_alembic_t_code', 'ix_alembic_t_hits')`).Scan(&n))
	require.Equal(t, 2, n,
		"alembic upgrade should have created both CONCURRENTLY indexes")

	downgrade := exec.CommandContext(cmdCtx, alembic, "-c", "alembic.ini", "downgrade", "-1")
	downgrade.Dir = work
	downgrade.Env = append(os.Environ(), "PYTHONUNBUFFERED=1", "NO_COLOR=1")
	if out, err := downgrade.CombinedOutput(); err != nil {
		t.Fatalf("alembic downgrade -1 failed: %v\n%s", err, out)
	}

	// Both indexes should be gone after the downgrade.
	require.NoError(t, defaultConn.Default.QueryRow(ctx,
		`SELECT count(*) FROM pg_catalog.pg_indexes
		 WHERE tablename = 'alembic_t'
		   AND indexname IN ('ix_alembic_t_code', 'ix_alembic_t_hits')`).Scan(&n))
	require.Equal(t, 0, n,
		"alembic downgrade should have dropped both CONCURRENTLY indexes")
}
