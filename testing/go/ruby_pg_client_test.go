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

// TestRubyPGClientSmoke runs the real Ruby `pg` client against
// Doltgres. The legacy BATS client suite has a Ruby script, but this
// repo-native Go harness pins the ordinary driver path in the same
// shape as the Node and Python client smokes: startup parameters,
// typed parameters, prepared statements, JSONB/text[] parameter values,
// concurrent reads, and transaction boundaries.
func TestRubyPGClientSmoke(t *testing.T) {
	ruby, err := exec.LookPath("ruby")
	if err != nil {
		t.Skip("ruby not on PATH; install Ruby to enable this harness")
	}
	gem, err := exec.LookPath("gem")
	if err != nil {
		t.Skip("gem not on PATH; install RubyGems to enable this harness")
	}
	pgConfig, err := exec.LookPath("pg_config")
	if err != nil {
		t.Skip("pg_config not on PATH; install libpq/PostgreSQL dev tools to enable this harness")
	}
	if testing.Short() {
		t.Skip("ruby pg harness installs a native gem; skipped under -short")
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
	gemHome := filepath.Join(work, "gems")
	gemSpecCache := filepath.Join(work, "gem-spec-cache")
	binDir := filepath.Join(work, "bin")
	cmdCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	install := exec.CommandContext(cmdCtx, gem, "install",
		"--no-document",
		"--install-dir", gemHome,
		"--bindir", binDir,
		"pg", "-v", "1.5.6",
		"--",
		"--with-pg-config="+pgConfig,
	)
	install.Env = append(os.Environ(),
		"HOME="+work,
		"GEM_HOME="+gemHome,
		"GEM_PATH="+gemHome,
		"GEM_SPEC_CACHE="+gemSpecCache,
		"NO_COLOR=1",
	)
	if out, err := install.CombinedOutput(); err != nil {
		t.Fatalf("gem install pg failed: %v\n%s", err, string(out))
	}

	script := `require 'json'
require 'pg'

conninfo = ARGV.fetch(0)

def assert_equal(expected, actual)
  return if expected == actual
  raise "expected #{expected.inspect}, got #{actual.inspect}"
end

conn = PG.connect(conninfo)

begin
  app_name = conn.exec("SELECT current_setting('application_name') AS app_name")[0]["app_name"]
  assert_equal("ruby-pg-harness", app_name)

  conn.exec(%q{
    CREATE TABLE ruby_accounts (
      id integer PRIMARY KEY,
      name text NOT NULL UNIQUE,
      active boolean NOT NULL
    )
  })
  conn.exec(%q{
    CREATE TABLE ruby_items (
      id integer PRIMARY KEY,
      account_id integer NOT NULL REFERENCES ruby_accounts(id),
      amount numeric(10,2) NOT NULL,
      tags text[] NOT NULL,
      payload jsonb NOT NULL
    )
  })

  conn.exec_params(
    "INSERT INTO ruby_accounts VALUES ($1::int4, $2::text, $3::bool), ($4::int4, $5::text, $6::bool)",
    [1, "acme", "true", 2, "beta", "false"]
  )
  inserted = conn.exec_params(
    "INSERT INTO ruby_items VALUES ($1::int4, $2::int4, $3::text::numeric, $4::text[], $5::jsonb) " \
      "RETURNING amount::text AS amount, tags[2] AS second_tag, payload #>> '{kind}' AS kind",
    [10, 1, "12.34", "{red,blue}", JSON.generate({"kind" => "invoice", "lines" => [1, 2]})]
  )[0]
  assert_equal({"amount" => "12.34", "second_tag" => "blue", "kind" => "invoice"}, inserted)

  conn.prepare(
    "ruby_items_by_account",
    "SELECT a.name, a.active::text AS active, i.amount::text AS amount " \
      "FROM ruby_items i JOIN ruby_accounts a ON a.id = i.account_id " \
      "WHERE i.account_id = $1::int4 AND 'blue' = ANY(i.tags)"
  )
  selected = conn.exec_prepared("ruby_items_by_account", [1])[0]
  assert_equal({"name" => "acme", "active" => "true", "amount" => "12.34"}, selected)
  conn.exec("DEALLOCATE ruby_items_by_account")

  names = [1, 2].map do |account_id|
    Thread.new do
      thread_conn = PG.connect(conninfo)
      begin
        thread_conn.exec_params(
          "SELECT name FROM ruby_accounts WHERE id = $1::int4",
          [account_id]
        )[0]["name"]
      ensure
        thread_conn.close if thread_conn
      end
    end
  end.map(&:value).sort
  assert_equal(["acme", "beta"], names)

  conn.exec("BEGIN")
  conn.exec_params(
    "INSERT INTO ruby_accounts VALUES ($1::int4, $2::text, $3::bool)",
    [3, "gamma", "true"]
  )
  conn.exec("COMMIT")

  begin
    conn.exec("BEGIN")
    conn.exec_params(
      "INSERT INTO ruby_accounts VALUES ($1::int4, $2::text, $3::bool)",
      [4, "rolled back", "true"]
    )
    raise "rollback transaction"
  rescue RuntimeError
    conn.exec("ROLLBACK")
  end

  summary = conn.exec(
    "SELECT array_to_string(array_agg(name ORDER BY id), ',') AS names FROM ruby_accounts"
  )[0]["names"]
  assert_equal("acme,beta,gamma", summary)

  puts JSON.generate({"ok" => true, "accounts" => summary})
ensure
  conn.close if conn
end
`
	scriptPath := filepath.Join(work, "harness.rb")
	require.NoError(t, os.WriteFile(scriptPath, []byte(script), 0o644))

	url := fmt.Sprintf(
		"postgres://postgres:password@127.0.0.1:%d/postgres?sslmode=disable&application_name=ruby-pg-harness",
		port,
	)
	cmd := exec.CommandContext(cmdCtx, ruby, scriptPath, url)
	cmd.Env = append(os.Environ(),
		"HOME="+work,
		"GEM_HOME="+gemHome,
		"GEM_PATH="+gemHome,
		"GEM_SPEC_CACHE="+gemSpecCache,
		"NO_COLOR=1",
	)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "ruby pg probe failed: %s", string(out))
	require.Contains(t, string(out), `"ok":true`)
	require.Contains(t, string(out), `"accounts":"acme,beta,gamma"`)
}
