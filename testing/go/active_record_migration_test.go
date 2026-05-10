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

// TestActiveRecordMigrationSmoke runs ActiveRecord's migration framework
// against Doltgres. The ActiveRecord client smoke covers runtime ORM behavior;
// this pins MigrationContext, schema_migrations metadata, and migration-file
// schema DSL DDL.
func TestActiveRecordMigrationSmoke(t *testing.T) {
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
		t.Skip("ActiveRecord migration harness installs Ruby gems; skipped under -short")
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
	binDir := filepath.Join(work, "bin")
	cmdCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	gemEnv := append(os.Environ(),
		"GEM_HOME="+gemHome,
		"GEM_PATH="+gemHome,
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

	migrationsDir := filepath.Join(work, "db", "migrate")
	require.NoError(t, os.MkdirAll(migrationsDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(migrationsDir, "20260510141000_create_active_record_migration_accounts.rb"), []byte(`class CreateActiveRecordMigrationAccounts < ActiveRecord::Migration[6.1]
  def change
    create_table :active_record_migration_accounts, id: :integer do |t|
      t.text :email, null: false
      t.boolean :active, null: false, default: true
      t.jsonb :meta, null: false
    end
    add_index :active_record_migration_accounts, :email, unique: true
  end
end
`), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(migrationsDir, "20260510141100_create_active_record_migration_items.rb"), []byte(`class CreateActiveRecordMigrationItems < ActiveRecord::Migration[6.1]
  def change
    create_table :active_record_migration_items, id: :integer do |t|
      t.integer :account_id, null: false
      t.decimal :amount, precision: 10, scale: 2, null: false
      t.text :tags, array: true, null: false
      t.jsonb :payload, null: false
    end
    add_foreign_key :active_record_migration_items, :active_record_migration_accounts, column: :account_id
    add_index :active_record_migration_items, :account_id
  end
end
`), 0o644))

	script := `require 'bigdecimal'
require 'json'
require 'logger'
require 'active_record'

url = ARGV.fetch(0)
migrations_path = ARGV.fetch(1)

def assert_equal(expected, actual)
  return if expected == actual
  raise "expected #{expected.inspect}, got #{actual.inspect}"
end

ActiveRecord::Base.establish_connection(url)

class ActiveRecordMigrationAccount < ActiveRecord::Base
  self.table_name = 'active_record_migration_accounts'
  has_many :items, class_name: 'ActiveRecordMigrationItem', foreign_key: 'account_id'
end

class ActiveRecordMigrationItem < ActiveRecord::Base
  self.table_name = 'active_record_migration_items'
  belongs_to :account, class_name: 'ActiveRecordMigrationAccount', foreign_key: 'account_id'
end

begin
  ActiveRecord::SchemaMigration.create_table
  ActiveRecord::InternalMetadata.create_table
  context = ActiveRecord::MigrationContext.new(migrations_path, ActiveRecord::SchemaMigration)
  context.up

  versions = ActiveRecord::Base.connection.select_values(
    "SELECT version FROM schema_migrations ORDER BY version"
  )
  assert_equal(['20260510141000', '20260510141100'], versions)

  app_name = ActiveRecord::Base.connection.select_value(
    "SELECT current_setting('application_name')"
  )
  assert_equal('active-record-migration-harness', app_name)

  acme = ActiveRecordMigrationAccount.create!(
    id: 1,
    email: 'acme@example.com',
    active: true,
    meta: { 'tier' => 'pro' }
  )
  ActiveRecordMigrationAccount.create!(
    id: 2,
    email: 'beta@example.com',
    active: false,
    meta: { 'tier' => 'free' }
  )

  created = ActiveRecordMigrationItem.create!(
    id: 10,
    account: acme,
    amount: BigDecimal('12.34'),
    tags: ['red', 'blue'],
    payload: { 'kind' => 'invoice', 'lines' => [1, 2] }
  )
  assert_equal('12.34', created.amount.to_s('F'))
  assert_equal(['red', 'blue'], created.tags)
  assert_equal('invoice', created.payload.fetch('kind'))

  selected = ActiveRecordMigrationItem.includes(:account).find(10)
  assert_equal('acme@example.com', selected.account.email)
  assert_equal(['red', 'blue'], selected.tags)

  ActiveRecord::Base.transaction do
    ActiveRecordMigrationAccount.create!(
      id: 3,
      email: 'gamma@example.com',
      active: true,
      meta: { 'tier' => 'trial' }
    )
  end

  begin
    ActiveRecord::Base.transaction do
      ActiveRecordMigrationAccount.create!(
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

  summary = ActiveRecordMigrationAccount.order(:id).pluck(:email).join(',')
  assert_equal('acme@example.com,beta@example.com,gamma@example.com', summary)

  puts JSON.generate({ 'ok' => true, 'emails' => summary })
ensure
  ActiveRecord::Base.connection_handler.clear_active_connections!
end
`
	scriptPath := filepath.Join(work, "active_record_migration_harness.rb")
	require.NoError(t, os.WriteFile(scriptPath, []byte(script), 0o644))

	url := fmt.Sprintf(
		"postgres://postgres:password@127.0.0.1:%d/postgres?sslmode=disable&application_name=active-record-migration-harness",
		port,
	)
	cmd := exec.CommandContext(cmdCtx, ruby, scriptPath, url, migrationsDir)
	cmd.Env = append(os.Environ(),
		"GEM_HOME="+gemHome,
		"GEM_PATH="+gemHome,
		"NO_COLOR=1",
	)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "ActiveRecord migration probe failed: %s", string(out))
	require.Contains(t, string(out), `"ok":true`)
	require.Contains(t, string(out), `"emails":"acme@example.com,beta@example.com,gamma@example.com"`)
}
