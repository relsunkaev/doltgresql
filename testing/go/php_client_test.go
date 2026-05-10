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

const phpClientImage = "php:8.4-cli"

// TestPHPPgSQLClientSmoke runs PHP's ext-pgsql driver from the official PHP
// container image against Doltgres. This pins the PHP client path for startup
// options, server-side prepared statements, typed parameters, JSONB/text[]
// values, repeated connections, and transaction boundaries.
func TestPHPPgSQLClientSmoke(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("docker is required for the PHP pgsql harness: %v", err)
	}
	if testing.Short() {
		t.Skip("PHP pgsql harness uses Docker; skipped under -short")
	}

	originalServerHost := serverHost
	serverHost = "0.0.0.0"
	t.Cleanup(func() {
		serverHost = originalServerHost
	})

	port, err := gms.GetEmptyPort()
	require.NoError(t, err)
	ctx, defaultConn, controller := CreateServerWithPort(t, "postgres", port)
	t.Cleanup(func() {
		defaultConn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	work := t.TempDir()
	scriptPath := filepath.Join(work, "pgsql_probe.php")
	require.NoError(t, os.WriteFile(scriptPath, []byte(phpPgSQLProbe), 0o644))

	cmdCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	conninfo := fmt.Sprintf(
		"host=host.docker.internal port=%d dbname=postgres user=postgres password=password application_name=php-pgsql-harness",
		port,
	)
	args := []string{
		"run", "--rm",
		"--add-host=host.docker.internal:host-gateway",
		"-e", "DEBIAN_FRONTEND=noninteractive",
		"-e", "DOLTGRES_CONNINFO=" + conninfo,
		"-v", filepath.Clean(scriptPath) + ":/tmp/pgsql_probe.php:ro",
		phpClientImage,
		"sh", "-lc",
		"apt-get update >/dev/null && apt-get install -y --no-install-recommends libpq-dev >/dev/null && docker-php-ext-install pgsql >/dev/null && php /tmp/pgsql_probe.php",
	}
	out, err := exec.CommandContext(cmdCtx, "docker", args...).CombinedOutput()
	require.NoError(t, err, "PHP pgsql probe failed: %s", string(out))
	require.Contains(t, string(out), `"ok":true`)
	require.Contains(t, string(out), `"accounts":"acme,beta,gamma"`)
}

const phpPgSQLProbe = `<?php
function check($condition, $message) {
    if (!$condition) {
        throw new Exception($message);
    }
}

function open_connection() {
    $conn = pg_connect(getenv("DOLTGRES_CONNINFO"), PGSQL_CONNECT_FORCE_NEW);
    if ($conn === false) {
        throw new Exception("pg_connect failed");
    }
    return $conn;
}

function run_query($conn, $query) {
    $result = pg_query($conn, $query);
    if ($result === false) {
        throw new Exception(pg_last_error($conn));
    }
    return $result;
}

function prepare_statement($conn, $name, $query) {
    $result = pg_prepare($conn, $name, $query);
    if ($result === false) {
        throw new Exception(pg_last_error($conn));
    }
}

function execute_statement($conn, $name, $params) {
    $result = pg_execute($conn, $name, $params);
    if ($result === false) {
        throw new Exception(pg_last_error($conn));
    }
    return $result;
}

$conn = open_connection();
$appName = pg_fetch_result(run_query($conn, "SELECT current_setting('application_name')"), 0, 0);
check($appName === "php-pgsql-harness", "unexpected application_name: " . $appName);

run_query($conn, "CREATE TABLE php_accounts (
    id integer PRIMARY KEY,
    name text NOT NULL UNIQUE,
    active boolean NOT NULL
)");
run_query($conn, "CREATE TABLE php_items (
    id integer PRIMARY KEY,
    account_id integer NOT NULL REFERENCES php_accounts(id),
    amount numeric(10,2) NOT NULL,
    tags text[] NOT NULL,
    payload jsonb NOT NULL
)");

prepare_statement($conn, "insert_accounts",
    "INSERT INTO php_accounts VALUES ($1::int4, $2::text, $3::bool), ($4::int4, $5::text, $6::bool)"
);
execute_statement($conn, "insert_accounts", [1, "acme", "true", 2, "beta", "false"]);

prepare_statement($conn, "insert_item",
    "INSERT INTO php_items VALUES ($1::int4, $2::int4, $3::numeric, $4::text[], $5::jsonb)
     RETURNING amount::text AS amount, tags[2] AS second_tag, payload #>> '{kind}' AS kind"
);
$inserted = pg_fetch_assoc(execute_statement($conn, "insert_item", [
    10,
    1,
    "12.34",
    "{\"red\",\"blue\"}",
    json_encode(["kind" => "invoice", "lines" => [1, 2]]),
]));
check($inserted === ["amount" => "12.34", "second_tag" => "blue", "kind" => "invoice"], "unexpected inserted row: " . json_encode($inserted));

prepare_statement($conn, "select_item",
    "SELECT account_id, amount::text AS amount, tags[2] AS second_tag, payload #>> '{kind}' AS kind
     FROM php_items
     WHERE account_id = $1::int4 AND tags[2] = $2::text"
);
$selected = pg_fetch_assoc(execute_statement($conn, "select_item", [1, "blue"]));
check($selected === ["account_id" => "1", "amount" => "12.34", "second_tag" => "blue", "kind" => "invoice"], "unexpected selected row: " . json_encode($selected));

$first = open_connection();
$second = open_connection();
prepare_statement($first, "first_name", "SELECT name FROM php_accounts WHERE id = $1::int4");
prepare_statement($second, "second_name", "SELECT name FROM php_accounts WHERE id = $1::int4");
$names = [
    pg_fetch_result(execute_statement($first, "first_name", [1]), 0, 0),
    pg_fetch_result(execute_statement($second, "second_name", [2]), 0, 0),
];
sort($names);
check($names === ["acme", "beta"], "unexpected repeated-connection names: " . json_encode($names));

run_query($conn, "BEGIN");
prepare_statement($conn, "commit_insert", "INSERT INTO php_accounts VALUES ($1::int4, $2::text, $3::bool)");
execute_statement($conn, "commit_insert", [3, "gamma", "true"]);
run_query($conn, "COMMIT");

run_query($conn, "BEGIN");
prepare_statement($conn, "rollback_insert", "INSERT INTO php_accounts VALUES ($1::int4, $2::text, $3::bool)");
execute_statement($conn, "rollback_insert", [4, "rolled back", "true"]);
run_query($conn, "ROLLBACK");

$summary = pg_fetch_result(run_query($conn, "SELECT array_to_string(array_agg(name ORDER BY id), ',') FROM php_accounts"), 0, 0);
check($summary === "acme,beta,gamma", "unexpected account summary: " . $summary);

echo json_encode(["ok" => true, "accounts" => $summary]), PHP_EOL;
`
