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

	"github.com/stretchr/testify/require"
)

// TestDjangoMigrationAndORMSmoke runs Django's real migration command and ORM
// runtime against a live Doltgres instance. It pins the PostgreSQL backend path
// for migration DDL, application_name startup options, JSONB/text[] values,
// relations, and transaction commit/rollback behavior.
func TestDjangoMigrationAndORMSmoke(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not available; install Python 3 to enable this harness")
	}
	if testing.Short() {
		t.Skip("Django harness installs Django + psycopg; skipped under -short")
	}

	ctx, defaultConn, controller := CreateServer(t, "postgres")
	t.Cleanup(func() {
		defaultConn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})
	port := int(defaultConn.Default.Config().Port)

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
		"Django==5.2.1", "psycopg[binary]==3.3.4")
	install.Env = append(os.Environ(), "PIP_DISABLE_PIP_VERSION_CHECK=1")
	if out, err := install.CombinedOutput(); err != nil {
		t.Fatalf("pip install Django + psycopg: %v\n%s", err, out)
	}

	require.NoError(t, os.MkdirAll(filepath.Join(work, "config"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(work, "app", "migrations"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(work, "config", "__init__.py"), nil, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(work, "app", "__init__.py"), nil, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(work, "app", "migrations", "__init__.py"), nil, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(work, "manage.py"), []byte(`#!/usr/bin/env python
import os
import sys

if __name__ == "__main__":
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    from django.core.management import execute_from_command_line
    execute_from_command_line(sys.argv)
`), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(work, "config", "settings.py"), []byte(fmt.Sprintf(`SECRET_KEY = "doltgres-django-harness"
DEBUG = False
USE_TZ = True
INSTALLED_APPS = [
    "django.contrib.postgres",
    "app",
]
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": "postgres",
        "USER": "postgres",
        "PASSWORD": "password",
        "HOST": "127.0.0.1",
        "PORT": "%d",
        "OPTIONS": {"application_name": "django-harness"},
    }
}
DEFAULT_AUTO_FIELD = "django.db.models.AutoField"
`, port)), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(work, "app", "models.py"), []byte(`from django.contrib.postgres.fields import ArrayField
from django.db import models


class Account(models.Model):
    id = models.IntegerField(primary_key=True)
    name = models.TextField(unique=True)
    active = models.BooleanField(default=True)


class Item(models.Model):
    id = models.IntegerField(primary_key=True)
    account = models.ForeignKey(Account, on_delete=models.CASCADE)
    amount = models.DecimalField(max_digits=10, decimal_places=2)
    tags = ArrayField(models.TextField())
    payload = models.JSONField()
`), 0o644))

	python := filepath.Join(venv, "bin", "python")
	makemigrations := exec.CommandContext(cmdCtx, python, "manage.py", "makemigrations", "app", "--noinput")
	makemigrations.Dir = work
	makemigrations.Env = append(os.Environ(), "NO_COLOR=1")
	if out, err := makemigrations.CombinedOutput(); err != nil {
		t.Fatalf("django makemigrations failed: %v\n%s", err, out)
	}

	migrate := exec.CommandContext(cmdCtx, python, "manage.py", "migrate", "--noinput")
	migrate.Dir = work
	migrate.Env = append(os.Environ(), "NO_COLOR=1")
	if out, err := migrate.CombinedOutput(); err != nil {
		t.Fatalf("django migrate failed: %v\n%s", err, out)
	}

	require.NoError(t, os.WriteFile(filepath.Join(work, "probe.py"), []byte(`import django
import json
import os

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
django.setup()

from django.db import connection, transaction
from app.models import Account, Item

with connection.cursor() as cur:
    cur.execute("SELECT current_setting('application_name')")
    assert cur.fetchone()[0] == "django-harness"

Account.objects.create(id=1, name="acme", active=True)
Account.objects.create(id=2, name="beta", active=False)
Item.objects.create(
    id=10,
    account_id=1,
    amount="12.34",
    tags=["red", "blue"],
    payload={"kind": "invoice", "lines": [1, 2]},
)

item = Item.objects.select_related("account").get(id=10)
assert item.account.name == "acme"
assert str(item.amount) == "12.34"
assert item.tags == ["red", "blue"]
assert item.payload == {"kind": "invoice", "lines": [1, 2]}

with transaction.atomic():
    Account.objects.create(id=3, name="committed", active=True)
assert Account.objects.get(id=3).name == "committed"

try:
    with transaction.atomic():
        Account.objects.create(id=4, name="rolled-back", active=True)
        raise RuntimeError("force rollback")
except RuntimeError:
    pass
accounts = [(account.id, account.name) for account in Account.objects.order_by("id")]
assert accounts == [(1, "acme"), (2, "beta"), (3, "committed")], accounts

print(json.dumps({"ok": True}))
`), 0o644))
	probe := exec.CommandContext(cmdCtx, python, "probe.py")
	probe.Dir = work
	probe.Env = append(os.Environ(), "NO_COLOR=1")
	out, err := probe.CombinedOutput()
	require.NoError(t, err, "Django ORM probe failed: %s", string(out))
	require.Contains(t, string(out), `"ok": true`)
}
