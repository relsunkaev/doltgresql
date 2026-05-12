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

// TestSQLAlchemyClientSmoke runs SQLAlchemy's PostgreSQL dialect and ORM over
// psycopg. The direct psycopg harness pins driver behavior; this test pins
// SQLAlchemy's create_all introspection, ORM mapping, relationship loading,
// pooled connections, and transaction boundaries.
func TestSQLAlchemyClientSmoke(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not available; install Python 3 to enable this harness")
	}
	if testing.Short() {
		t.Skip("SQLAlchemy harness installs Python packages; skipped under -short")
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
		"SQLAlchemy==2.0.49",
		"psycopg[binary]==3.2.13")
	install.Env = append(os.Environ(), "PIP_DISABLE_PIP_VERSION_CHECK=1")
	if out, err := install.CombinedOutput(); err != nil {
		t.Fatalf("pip install SQLAlchemy + psycopg: %v\n%s", err, out)
	}

	script := `import json
import os
from concurrent.futures import ThreadPoolExecutor

from sqlalchemy import Boolean, Column, ForeignKey, Integer, Numeric, String, create_engine, select, text
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import Session, declarative_base, relationship

dsn = os.environ["DOLTGRES_URL"]
Base = declarative_base()

class Account(Base):
    __tablename__ = "sqlalchemy_accounts"

    id = Column(Integer, primary_key=True, autoincrement=False)
    email = Column(String, nullable=False, unique=True)
    active = Column(Boolean, nullable=False)
    meta = Column(JSONB, nullable=False)
    items = relationship("Item", back_populates="account")

class Item(Base):
    __tablename__ = "sqlalchemy_items"

    id = Column(Integer, primary_key=True, autoincrement=False)
    account_id = Column(Integer, ForeignKey("sqlalchemy_accounts.id"), nullable=False)
    amount = Column(Numeric(10, 2), nullable=False)
    tags = Column(ARRAY(String), nullable=False)
    payload = Column(JSONB, nullable=False)
    account = relationship("Account", back_populates="items")

engine = create_engine(
    dsn,
    connect_args={"application_name": "sqlalchemy-harness"},
    pool_size=2,
    max_overflow=0,
    future=True,
)

try:
    with engine.connect() as conn:
        app_name = conn.execute(text("SELECT current_setting('application_name')")).scalar_one()
        assert app_name == "sqlalchemy-harness", app_name

    Base.metadata.create_all(engine)

    with Session(engine) as session:
        session.add_all([
            Account(id=1, email="acme@example.com", active=True, meta={"tier": "pro"}),
            Account(id=2, email="beta@example.com", active=False, meta={"tier": "free"}),
        ])
        session.add(Item(
            id=10,
            account_id=1,
            amount="12.34",
            tags=["red", "blue"],
            payload={"kind": "invoice", "lines": [1, 2]},
        ))
        session.commit()

    with Session(engine) as session:
        account = session.scalars(select(Account).where(Account.email == "acme@example.com")).one()
        assert account.active is True, account.active
        assert account.meta == {"tier": "pro"}, account.meta
        assert len(account.items) == 1, account.items
        item = account.items[0]
        assert str(item.amount) == "12.34", item.amount
        assert item.tags[1] == "blue", item.tags
        assert item.payload["kind"] == "invoice", item.payload

    def account_email(account_id):
        with Session(engine) as session:
            return session.execute(
                select(Account.email).where(Account.id == account_id)
            ).scalar_one()

    with ThreadPoolExecutor(max_workers=2) as executor:
        emails = sorted(executor.map(account_email, [1, 2]))
    assert emails == ["acme@example.com", "beta@example.com"], emails

    with Session(engine) as session:
        with session.begin():
            session.add(Account(id=3, email="gamma@example.com", active=True, meta={"tier": "trial"}))

    try:
        with Session(engine) as session:
            with session.begin():
                session.add(Account(id=4, email="rolled-back@example.com", active=True, meta={"tier": "trial"}))
                raise RuntimeError("rollback transaction")
    except RuntimeError:
        pass

    with engine.connect() as conn:
        summary = conn.execute(text(
            "SELECT array_to_string(array_agg(email ORDER BY id), ',') FROM sqlalchemy_accounts"
        )).scalar_one()
        assert summary == "acme@example.com,beta@example.com,gamma@example.com", summary
finally:
    engine.dispose()

print(json.dumps({"ok": True, "emails": summary}))
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
	require.NoError(t, err, "SQLAlchemy probe failed: %s", string(out))
	require.Contains(t, string(out), `"ok": true`)
	require.Contains(t, string(out), `"emails": "acme@example.com,beta@example.com,gamma@example.com"`)
}
