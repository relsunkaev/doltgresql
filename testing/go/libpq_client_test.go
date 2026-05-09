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

// TestLibpqClientSmoke compiles and runs a tiny C program against
// libpq. The legacy BATS suite has a broader C client script; this
// repo-native harness pins the ordinary libpq path for startup
// parameters, typed parameters, prepared statements, JSONB/text[]
// values, multiple connections, and transaction boundaries.
func TestLibpqClientSmoke(t *testing.T) {
	cc, err := exec.LookPath("cc")
	if err != nil {
		t.Skip("cc not on PATH; install a C compiler to enable this harness")
	}
	pgConfig, err := exec.LookPath("pg_config")
	if err != nil {
		t.Skip("pg_config not on PATH; install libpq/PostgreSQL dev tools to enable this harness")
	}

	port, err := gms.GetEmptyPort()
	require.NoError(t, err)
	ctx, defaultConn, controller := CreateServerWithPort(t, "postgres", port)
	t.Cleanup(func() {
		defaultConn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	cmdCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	pgConfigOut := func(arg string) string {
		t.Helper()
		cmd := exec.CommandContext(cmdCtx, pgConfig, arg)
		out, err := cmd.Output()
		require.NoError(t, err, "pg_config %s", arg)
		return strings.TrimSpace(string(out))
	}
	includeDir := pgConfigOut("--includedir")
	libDir := pgConfigOut("--libdir")

	work := t.TempDir()
	srcPath := filepath.Join(work, "libpq_probe.c")
	binPath := filepath.Join(work, "libpq_probe")
	require.NoError(t, os.WriteFile(srcPath, []byte(libpqProbeSource), 0o644))

	compile := exec.CommandContext(cmdCtx, cc,
		srcPath,
		"-I"+includeDir,
		"-L"+libDir,
		"-lpq",
		"-o", binPath,
	)
	compile.Env = append(os.Environ(), "NO_COLOR=1")
	if out, err := compile.CombinedOutput(); err != nil {
		t.Fatalf("compile libpq probe failed: %v\n%s", err, string(out))
	}

	conninfo := fmt.Sprintf(
		"host=127.0.0.1 port=%d dbname=postgres user=postgres password=password sslmode=disable application_name=libpq-harness",
		port,
	)
	probe := exec.CommandContext(cmdCtx, binPath, conninfo)
	probe.Env = append(os.Environ(),
		"DYLD_LIBRARY_PATH="+libDir,
		"LD_LIBRARY_PATH="+libDir,
		"NO_COLOR=1",
	)
	out, err := probe.CombinedOutput()
	require.NoError(t, err, "libpq probe failed: %s", string(out))
	require.Contains(t, string(out), `{"ok":true,"accounts":"acme,beta,gamma"}`)
}

const libpqProbeSource = `
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libpq-fe.h>

static void finish_bad_conn(PGconn *conn, const char *msg) {
  fprintf(stderr, "%s: %s\n", msg, PQerrorMessage(conn));
  PQfinish(conn);
  exit(1);
}

static PGconn *connect_or_die(const char *conninfo) {
  PGconn *conn = PQconnectdb(conninfo);
  if (PQstatus(conn) != CONNECTION_OK) {
    finish_bad_conn(conn, "connect failed");
  }
  return conn;
}

static PGresult *exec_or_die(PGconn *conn, const char *sql, ExecStatusType expected) {
  PGresult *res = PQexec(conn, sql);
  if (PQresultStatus(res) != expected) {
    fprintf(stderr, "query failed: %s\n%s\n", sql, PQerrorMessage(conn));
    PQclear(res);
    PQfinish(conn);
    exit(1);
  }
  return res;
}

static PGresult *exec_params_or_die(PGconn *conn, const char *sql, int nParams, const char * const *values, ExecStatusType expected) {
  PGresult *res = PQexecParams(conn, sql, nParams, NULL, values, NULL, NULL, 0);
  if (PQresultStatus(res) != expected) {
    fprintf(stderr, "parameter query failed: %s\n%s\n", sql, PQerrorMessage(conn));
    PQclear(res);
    PQfinish(conn);
    exit(1);
  }
  return res;
}

static void expect_value(PGresult *res, int row, int col, const char *expected) {
  const char *actual = PQgetvalue(res, row, col);
  if (strcmp(actual, expected) != 0) {
    fprintf(stderr, "expected %s, got %s\n", expected, actual);
    PQclear(res);
    exit(1);
  }
}

int main(int argc, char **argv) {
  if (argc != 2) {
    fprintf(stderr, "usage: libpq_probe CONNINFO\n");
    return 1;
  }
  const char *conninfo = argv[1];
  PGconn *conn = connect_or_die(conninfo);

  PGresult *res = exec_or_die(conn, "SELECT current_setting('application_name') AS app_name", PGRES_TUPLES_OK);
  expect_value(res, 0, 0, "libpq-harness");
  PQclear(res);

  PQclear(exec_or_die(conn,
    "CREATE TABLE lpq_accounts (id integer PRIMARY KEY, name text NOT NULL UNIQUE, active boolean NOT NULL)",
    PGRES_COMMAND_OK));
  PQclear(exec_or_die(conn,
    "CREATE TABLE lpq_items (id integer PRIMARY KEY, account_id integer NOT NULL REFERENCES lpq_accounts(id), amount numeric(10,2) NOT NULL, tags text[] NOT NULL, payload jsonb NOT NULL)",
    PGRES_COMMAND_OK));

  const char *account_values[] = {"1", "acme", "true", "2", "beta", "false"};
  PQclear(exec_params_or_die(conn,
    "INSERT INTO lpq_accounts VALUES ($1::int4, $2::text, $3::bool), ($4::int4, $5::text, $6::bool)",
    6, account_values, PGRES_COMMAND_OK));

  const char *item_values[] = {"10", "1", "12.34", "{red,blue}", "{\"kind\":\"invoice\",\"lines\":[1,2]}"};
  res = exec_params_or_die(conn,
    "INSERT INTO lpq_items VALUES ($1::int4, $2::int4, $3::text::numeric, $4::text[], $5::jsonb) RETURNING amount::text AS amount, tags[2] AS second_tag, payload #>> '{kind}' AS kind",
    5, item_values, PGRES_TUPLES_OK);
  expect_value(res, 0, 0, "12.34");
  expect_value(res, 0, 1, "blue");
  expect_value(res, 0, 2, "invoice");
  PQclear(res);

  res = PQprepare(conn,
    "lpq_items_by_account",
    "SELECT a.name, a.active::text AS active, i.amount::text AS amount FROM lpq_items i JOIN lpq_accounts a ON a.id = i.account_id WHERE i.account_id = $1::int4 AND 'blue' = ANY(i.tags)",
    0, NULL);
  if (PQresultStatus(res) != PGRES_COMMAND_OK) {
    fprintf(stderr, "prepare failed: %s\n", PQerrorMessage(conn));
    PQclear(res);
    PQfinish(conn);
    exit(1);
  }
  PQclear(res);

  const char *prepared_values[] = {"1"};
  res = PQexecPrepared(conn, "lpq_items_by_account", 1, prepared_values, NULL, NULL, 0);
  if (PQresultStatus(res) != PGRES_TUPLES_OK) {
    fprintf(stderr, "prepared exec failed: %s\n", PQerrorMessage(conn));
    PQclear(res);
    PQfinish(conn);
    exit(1);
  }
  expect_value(res, 0, 0, "acme");
  expect_value(res, 0, 1, "true");
  expect_value(res, 0, 2, "12.34");
  PQclear(res);
  PQclear(exec_or_die(conn, "DEALLOCATE lpq_items_by_account", PGRES_COMMAND_OK));

  PGconn *reader = connect_or_die(conninfo);
  const char *read_one[] = {"1"};
  res = exec_params_or_die(reader, "SELECT name FROM lpq_accounts WHERE id = $1::int4", 1, read_one, PGRES_TUPLES_OK);
  expect_value(res, 0, 0, "acme");
  PQclear(res);
  const char *read_two[] = {"2"};
  res = exec_params_or_die(reader, "SELECT name FROM lpq_accounts WHERE id = $1::int4", 1, read_two, PGRES_TUPLES_OK);
  expect_value(res, 0, 0, "beta");
  PQclear(res);
  PQfinish(reader);

  PQclear(exec_or_die(conn, "BEGIN", PGRES_COMMAND_OK));
  const char *commit_values[] = {"3", "gamma", "true"};
  PQclear(exec_params_or_die(conn,
    "INSERT INTO lpq_accounts VALUES ($1::int4, $2::text, $3::bool)",
    3, commit_values, PGRES_COMMAND_OK));
  PQclear(exec_or_die(conn, "COMMIT", PGRES_COMMAND_OK));

  PQclear(exec_or_die(conn, "BEGIN", PGRES_COMMAND_OK));
  const char *rollback_values[] = {"4", "rolled back", "true"};
  PQclear(exec_params_or_die(conn,
    "INSERT INTO lpq_accounts VALUES ($1::int4, $2::text, $3::bool)",
    3, rollback_values, PGRES_COMMAND_OK));
  PQclear(exec_or_die(conn, "ROLLBACK", PGRES_COMMAND_OK));

  res = exec_or_die(conn,
    "SELECT array_to_string(array_agg(name ORDER BY id), ',') AS names FROM lpq_accounts",
    PGRES_TUPLES_OK);
  expect_value(res, 0, 0, "acme,beta,gamma");
  printf("{\"ok\":true,\"accounts\":\"%s\"}\n", PQgetvalue(res, 0, 0));
  PQclear(res);
  PQfinish(conn);
  return 0;
}
`
