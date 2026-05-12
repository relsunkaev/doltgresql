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
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	gms "github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
)

const perlDBIClientImage = "perl:5.38-bookworm"

// TestPerlDBIClientSmoke runs Perl DBI with DBD::Pg against Doltgres. It
// prefers a local Perl runtime when DBI and DBD::Pg are already installed, and
// otherwise falls back to the official Perl container image. This pins the Perl
// client path for startup options, prepared statements, typed parameters,
// JSONB/text[] values, repeated connections, commit, and rollback.
func TestPerlDBIClientSmoke(t *testing.T) {
	if testing.Short() {
		t.Skip("Perl DBI harness uses an external Perl runtime; skipped under -short")
	}

	runner := getPerlDBIRunner(t)
	t.Logf("using %s runtime for Perl DBI harness", runner.name)

	originalServerHost := serverHost
	serverHost = runner.serverBindHost
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
	scriptPath := filepath.Join(work, "dbi_probe.pl")
	require.NoError(t, os.WriteFile(scriptPath, []byte(perlDBIProbe), 0o644))

	cmdCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	out, err := runner.run(cmdCtx, scriptPath, port)
	if err != nil && runner.usesDocker && dockerInfrastructureUnavailable(err, out, cmdCtx.Err()) {
		t.Skipf("Docker runtime is unavailable for the Perl DBI harness: %v\n%s", err, string(out))
	}
	require.NoError(t, err, "Perl DBI probe failed: %s", string(out))
	require.Contains(t, string(out), `"ok":true`)
	require.Contains(t, string(out), `"accounts":"acme,beta,gamma"`)
}

type perlDBIRunner struct {
	name           string
	serverBindHost string
	usesDocker     bool
	run            func(context.Context, string, int) ([]byte, error)
}

func getPerlDBIRunner(t *testing.T) perlDBIRunner {
	t.Helper()

	if perlPath, ok := localPerlDBIRuntime(); ok {
		return perlDBIRunner{
			name:           "local perl",
			serverBindHost: "127.0.0.1",
			run: func(ctx context.Context, scriptPath string, port int) ([]byte, error) {
				dsn := fmt.Sprintf(
					"dbi:Pg:dbname=postgres;host=127.0.0.1;port=%d;application_name=perl-dbi-harness",
					port,
				)
				cmd := exec.CommandContext(ctx, perlPath, scriptPath)
				cmd.Env = append(os.Environ(), "DOLTGRES_DSN="+dsn)
				return cmd.CombinedOutput()
			},
		}
	}

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("Perl DBI harness requires local perl with DBI/DBD::Pg or docker: %v", err)
	}

	return perlDBIRunner{
		name:           "docker",
		serverBindHost: "0.0.0.0",
		usesDocker:     true,
		run:            runDockerPerlDBIProbe,
	}
}

func localPerlDBIRuntime() (string, bool) {
	perlPath, err := exec.LookPath("perl")
	if err != nil {
		return "", false
	}
	cmd := exec.Command(perlPath, "-MDBI", "-MDBD::Pg", "-e", "1")
	if err := cmd.Run(); err != nil {
		return "", false
	}
	return perlPath, true
}

func runDockerPerlDBIProbe(ctx context.Context, scriptPath string, port int) ([]byte, error) {
	dsn := fmt.Sprintf(
		"dbi:Pg:dbname=postgres;host=host.docker.internal;port=%d;application_name=perl-dbi-harness",
		port,
	)
	args := []string{
		"run", "--rm",
		"--add-host=host.docker.internal:host-gateway",
		"-e", "DEBIAN_FRONTEND=noninteractive",
		"-e", "DOLTGRES_DSN=" + dsn,
		"-v", filepath.Clean(scriptPath) + ":/tmp/dbi_probe.pl:ro",
		perlDBIClientImage,
		"sh", "-lc",
		"apt-get update >/dev/null && apt-get install -y --no-install-recommends libdbd-pg-perl >/dev/null && perl /tmp/dbi_probe.pl",
	}
	return exec.CommandContext(ctx, "docker", args...).CombinedOutput()
}

func dockerInfrastructureUnavailable(err error, out []byte, ctxErr error) bool {
	if errors.Is(ctxErr, context.DeadlineExceeded) {
		return true
	}
	exitErr, ok := err.(*exec.ExitError)
	if !ok || exitErr.ExitCode() != 125 {
		return false
	}
	message := string(out)
	return strings.Contains(message, "Cannot connect to the Docker daemon") ||
		strings.Contains(message, "input/output error") ||
		strings.Contains(message, "no space left on device") ||
		strings.Contains(message, "error during connect")
}

const perlDBIProbe = `use strict;
use warnings;

use DBI;
use JSON::PP qw(encode_json);

sub check {
    my ($condition, $message) = @_;
    die "$message\n" unless $condition;
}

sub assert_equal {
    my ($expected, $actual, $message) = @_;
    if (!defined $expected || !defined $actual || $expected ne $actual) {
        die "$message: expected <$expected>, got <$actual>\n";
    }
}

sub open_connection {
    my $dsn = $ENV{"DOLTGRES_DSN"} or die "DOLTGRES_DSN is required\n";
    return DBI->connect($dsn, "postgres", "password", {
        AutoCommit => 1,
        PrintError => 0,
        RaiseError => 1,
    });
}

my $dbh = open_connection();
my $app_name = $dbh->selectrow_array("SELECT current_setting('application_name')");
assert_equal("perl-dbi-harness", $app_name, "unexpected application_name");

$dbh->do("CREATE TABLE perl_accounts (
    id integer PRIMARY KEY,
    name text NOT NULL UNIQUE,
    active boolean NOT NULL
)");
$dbh->do("CREATE TABLE perl_items (
    id integer PRIMARY KEY,
    account_id integer NOT NULL REFERENCES perl_accounts(id),
    amount numeric(10,2) NOT NULL,
    tags text[] NOT NULL,
    payload jsonb NOT NULL
)");

my $insert_accounts = $dbh->prepare(
    "INSERT INTO perl_accounts VALUES (?::int4, ?::text, ?::bool), (?::int4, ?::text, ?::bool)"
);
$insert_accounts->execute(1, "acme", "true", 2, "beta", "false");

my $inserted = $dbh->selectrow_hashref(
    "INSERT INTO perl_items VALUES (?::int4, ?::int4, ?::numeric, ?::text[], ?::jsonb) " .
    "RETURNING amount::text AS amount, tags[2] AS second_tag, payload #>> '{kind}' AS kind",
    undef,
    10,
    1,
    "12.34",
    '{"red","blue"}',
    encode_json({ kind => "invoice", lines => [1, 2] }),
);
assert_equal("12.34", $inserted->{amount}, "unexpected inserted amount");
assert_equal("blue", $inserted->{second_tag}, "unexpected inserted tag");
assert_equal("invoice", $inserted->{kind}, "unexpected inserted kind");

my $select_item = $dbh->prepare(
    "SELECT account_id::text AS account_id, amount::text AS amount, tags[2] AS second_tag, payload #>> '{kind}' AS kind " .
    "FROM perl_items WHERE account_id = ?::int4 AND tags[2] = ?::text"
);
$select_item->execute(1, "blue");
my $selected = $select_item->fetchrow_hashref();
assert_equal("1", $selected->{account_id}, "unexpected selected account");
assert_equal("12.34", $selected->{amount}, "unexpected selected amount");
assert_equal("blue", $selected->{second_tag}, "unexpected selected tag");
assert_equal("invoice", $selected->{kind}, "unexpected selected kind");

my $first = open_connection();
my $second = open_connection();
my @names = sort (
    $first->selectrow_array("SELECT name FROM perl_accounts WHERE id = ?::int4", undef, 1),
    $second->selectrow_array("SELECT name FROM perl_accounts WHERE id = ?::int4", undef, 2),
);
assert_equal("acme,beta", join(",", @names), "unexpected repeated-connection names");
$first->disconnect();
$second->disconnect();

$dbh->begin_work();
$dbh->do("INSERT INTO perl_accounts VALUES (?::int4, ?::text, ?::bool)", undef, 3, "gamma", "true");
$dbh->commit();

eval {
    $dbh->begin_work();
    $dbh->do("INSERT INTO perl_accounts VALUES (?::int4, ?::text, ?::bool)", undef, 4, "rolled back", "true");
    die "force rollback";
};
if ($@) {
    $dbh->rollback();
}

my $summary = $dbh->selectrow_array("SELECT array_to_string(array_agg(name ORDER BY id), ',') FROM perl_accounts");
assert_equal("acme,beta,gamma", $summary, "unexpected account summary");

print encode_json({ ok => JSON::PP::true, accounts => $summary }), "\n";
$dbh->disconnect();
`
