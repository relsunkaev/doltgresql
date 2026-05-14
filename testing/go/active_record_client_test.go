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

// TestActiveRecordClientSmoke runs Rails ActiveRecord's PostgreSQL adapter
// against Doltgres. This pins the Ruby ORM layer separately from the native
// Ruby pg driver: schema DSL DDL, model CRUD, JSONB/text[] values, relation
// reads, raw parameter binding, connection-pool reads, commit, and rollback.
func TestActiveRecordClientSmoke(t *testing.T) {
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
		t.Skip("ActiveRecord harness installs Ruby gems; skipped under -short")
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

	gemEnv := append(os.Environ(),
		"HOME="+work,
		"GEM_HOME="+gemHome,
		"GEM_PATH="+gemHome,
		"GEM_SPEC_CACHE="+gemSpecCache,
		"NO_COLOR=1",
	)
	installGem := func(name string, version string, preArgs []string, postArgs ...string) {
		gemArgs := []string{
			"install",
			"--no-document",
			"--install-dir", gemHome,
			"--bindir", binDir,
		}
		gemArgs = append(gemArgs, preArgs...)
		gemArgs = append(gemArgs, name, "-v", version)
		gemArgs = append(gemArgs, postArgs...)
		cmd := exec.CommandContext(cmdCtx, gem, gemArgs...)
		cmd.Env = gemEnv
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("gem install %s failed: %v\n%s", name, err, string(out))
		}
	}

	installGem("pg", "1.5.6", nil, "--", "--with-pg-config="+pgConfig)
	installGem("concurrent-ruby", "1.3.4", nil)
	installGem("i18n", "1.14.5", nil)
	installGem("minitest", "5.22.3", nil)
	installGem("tzinfo", "2.0.6", nil)
	installGem("zeitwerk", "2.6.13", nil)
	installGem("activesupport", "6.1.7.10", []string{"--ignore-dependencies"})
	installGem("activemodel", "6.1.7.10", []string{"--ignore-dependencies"})
	installGem("activerecord", "6.1.7.10", []string{"--ignore-dependencies"})

	script := `require 'bigdecimal'
require 'json'
require 'logger'
require 'active_record'

url = ARGV.fetch(0)

def assert_equal(expected, actual)
  return if expected == actual
  raise "expected #{expected.inspect}, got #{actual.inspect}"
end

ActiveRecord::Base.establish_connection(url)

class ActiveRecordAccount < ActiveRecord::Base
  self.table_name = 'active_record_accounts'
  has_many :items, class_name: 'ActiveRecordItem', foreign_key: 'account_id'
end

class ActiveRecordItem < ActiveRecord::Base
  self.table_name = 'active_record_items'
  belongs_to :account, class_name: 'ActiveRecordAccount', foreign_key: 'account_id'
end

begin
  app_name = ActiveRecord::Base.connection.select_value(
    "SELECT current_setting('application_name')"
  )
  assert_equal('active-record-harness', app_name)

  ActiveRecord::Schema.define do
    create_table :active_record_accounts, id: :integer, force: true do |t|
      t.text :email, null: false
      t.boolean :active, null: false, default: true
      t.jsonb :meta, null: false
    end
    add_index :active_record_accounts, :email, unique: true

    create_table :active_record_items, id: :integer, force: true do |t|
      t.integer :account_id, null: false
      t.decimal :amount, precision: 10, scale: 2, null: false
      t.text :tags, array: true, null: false
      t.jsonb :payload, null: false
    end
    add_foreign_key :active_record_items, :active_record_accounts, column: :account_id
  end

  acme = ActiveRecordAccount.create!(
    id: 1,
    email: 'acme@example.com',
    active: true,
    meta: { 'tier' => 'pro' }
  )
  ActiveRecordAccount.create!(
    id: 2,
    email: 'beta@example.com',
    active: false,
    meta: { 'tier' => 'free' }
  )

  created = ActiveRecordItem.create!(
    id: 10,
    account: acme,
    amount: BigDecimal('12.34'),
    tags: ['red', 'blue'],
    payload: { 'kind' => 'invoice', 'lines' => [1, 2] }
  )
  assert_equal('12.34', created.amount.to_s('F'))
  assert_equal(['red', 'blue'], created.tags)
  assert_equal('invoice', created.payload.fetch('kind'))

  selected = ActiveRecordItem.includes(:account).find(10)
  assert_equal('acme@example.com', selected.account.email)
  assert_equal('12.34', selected.amount.to_s('F'))
  assert_equal(['red', 'blue'], selected.tags)
  assert_equal('invoice', selected.payload.fetch('kind'))

  binds = [
    ActiveRecord::Relation::QueryAttribute.new(
      'account_id',
      1,
      ActiveRecord::Type::Integer.new
    ),
    ActiveRecord::Relation::QueryAttribute.new(
      'tag',
      'blue',
      ActiveRecord::Type::String.new
    )
  ]
  raw = ActiveRecord::Base.connection.exec_query(
    "SELECT a.email, i.amount::text AS amount, i.tags[2] AS tag, i.payload #>> '{kind}' AS kind " \
      "FROM active_record_items i " \
      "JOIN active_record_accounts a ON a.id = i.account_id " \
      "WHERE i.account_id = $1::int4 AND $2::text = ANY(i.tags)",
    'ActiveRecord raw bind probe',
    binds
  ).to_a
  assert_equal(
    [{'email' => 'acme@example.com', 'amount' => '12.34', 'tag' => 'blue', 'kind' => 'invoice'}],
    raw
  )

  names = [1, 2].map do |account_id|
    Thread.new do
      ActiveRecord::Base.connection_pool.with_connection do
        ActiveRecordAccount.find(account_id).email
      end
    end
  end.map(&:value).sort
  assert_equal(['acme@example.com', 'beta@example.com'], names)

  ActiveRecord::Base.transaction do
    ActiveRecordAccount.create!(
      id: 3,
      email: 'gamma@example.com',
      active: true,
      meta: { 'tier' => 'trial' }
    )
  end

  begin
    ActiveRecord::Base.transaction do
      ActiveRecordAccount.create!(
        id: 4,
        email: 'rolled-back@example.com',
        active: true,
        meta: { 'tier' => 'trial' }
      )
      raise 'force rollback'
    end
    raise 'rollback transaction should throw'
  rescue RuntimeError => e
    assert_equal('force rollback', e.message)
  end

  summary = ActiveRecordAccount.order(:id).pluck(:email).join(',')
  assert_equal('acme@example.com,beta@example.com,gamma@example.com', summary)

  puts JSON.generate({ 'ok' => true, 'emails' => summary })
ensure
  ActiveRecord::Base.connection_handler.clear_active_connections!
end
`
	scriptPath := filepath.Join(work, "active_record_harness.rb")
	require.NoError(t, os.WriteFile(scriptPath, []byte(script), 0o644))

	url := fmt.Sprintf(
		"postgres://postgres:password@127.0.0.1:%d/postgres?sslmode=disable&application_name=active-record-harness",
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
	require.NoError(t, err, "ActiveRecord probe failed: %s", string(out))
	require.Contains(t, string(out), `"ok":true`)
	require.Contains(t, string(out), `"emails":"acme@example.com,beta@example.com,gamma@example.com"`)
}
