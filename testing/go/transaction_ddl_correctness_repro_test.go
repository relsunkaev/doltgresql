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
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
)

// TestCreateTableRollbackRemovesTableGuard covers transactional DDL
// persistence: PostgreSQL removes a table created in a transaction when that
// transaction rolls back.
func TestCreateTableRollbackRemovesTableGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE rolls back with transaction",
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `CREATE TABLE tx_create_rollback_items (
						id INT PRIMARY KEY,
						label TEXT
					);`,
				},
				{
					Query: `INSERT INTO tx_create_rollback_items VALUES (1, 'transient');`,
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `SELECT count(*)
						FROM information_schema.tables
						WHERE table_schema = 'public'
							AND table_name = 'tx_create_rollback_items';`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
	})
}

// TestDropTableRollbackRestoresTableGuard covers transactional DDL
// persistence: PostgreSQL restores a dropped table when the transaction rolls
// back.
func TestDropTableRollbackRestoresTableGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP TABLE rolls back with transaction",
			SetUpScript: []string{
				`CREATE TABLE tx_drop_rollback_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO tx_drop_rollback_items VALUES (1, 'kept');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `DROP TABLE tx_drop_rollback_items;`,
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query:    `SELECT id, label FROM tx_drop_rollback_items;`,
					Expected: []sql.Row{{1, "kept"}},
				},
			},
		},
	})
}

// TestAlterTableAddColumnRollbackRestoresSchemaGuard covers transactional DDL
// persistence: PostgreSQL removes a column added in a rolled-back transaction.
func TestAlterTableAddColumnRollbackRestoresSchemaGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TABLE ADD COLUMN rolls back with transaction",
			SetUpScript: []string{
				`CREATE TABLE tx_alter_rollback_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO tx_alter_rollback_items VALUES (1, 'kept');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `ALTER TABLE tx_alter_rollback_items
						ADD COLUMN leaked TEXT;`,
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `SELECT count(*)
						FROM information_schema.columns
						WHERE table_schema = 'public'
							AND table_name = 'tx_alter_rollback_items'
							AND column_name = 'leaked';`,
					Expected: []sql.Row{{int64(0)}},
				},
				{
					Query:    `SELECT id, label FROM tx_alter_rollback_items;`,
					Expected: []sql.Row{{1, "kept"}},
				},
			},
		},
	})
}

// TestCreateIndexRollbackRemovesIndexGuard covers transactional DDL
// persistence: PostgreSQL removes an index created in a rolled-back
// transaction.
func TestCreateIndexRollbackRemovesIndexGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE INDEX rolls back with transaction",
			SetUpScript: []string{
				`CREATE TABLE tx_index_rollback_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `CREATE INDEX tx_index_rollback_items_label_idx
						ON tx_index_rollback_items (label);`,
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_class
						WHERE relname = 'tx_index_rollback_items_label_idx';`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
	})
}

// TestDdlSavepointRollbackRestoresStateGuard covers PostgreSQL savepoint DDL
// persistence: rolling back to a savepoint must undo schema changes made after
// that savepoint without aborting the outer transaction.
func TestDdlSavepointRollbackRestoresStateGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE rolls back to savepoint",
			Assertions: []ScriptTestAssertion{
				{Query: `BEGIN;`},
				{Query: `SAVEPOINT ddl_sp;`},
				{
					Query: `CREATE TABLE sp_create_rollback_items (
						id INT PRIMARY KEY
					);`,
				},
				{Query: `ROLLBACK TO SAVEPOINT ddl_sp;`},
				{Query: `COMMIT;`},
				{
					Query: `SELECT count(*)
						FROM information_schema.tables
						WHERE table_schema = 'public'
							AND table_name = 'sp_create_rollback_items';`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
		{
			Name: "DROP TABLE rolls back to savepoint",
			SetUpScript: []string{
				`CREATE TABLE sp_drop_rollback_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO sp_drop_rollback_items VALUES (1, 'kept');`,
			},
			Assertions: []ScriptTestAssertion{
				{Query: `BEGIN;`},
				{Query: `SAVEPOINT ddl_sp;`},
				{Query: `DROP TABLE sp_drop_rollback_items;`},
				{Query: `ROLLBACK TO SAVEPOINT ddl_sp;`},
				{Query: `COMMIT;`},
				{
					Query:    `SELECT id, label FROM sp_drop_rollback_items;`,
					Expected: []sql.Row{{1, "kept"}},
				},
			},
		},
		{
			Name: "ALTER TABLE ADD COLUMN rolls back to savepoint",
			SetUpScript: []string{
				`CREATE TABLE sp_alter_rollback_items (
					id INT PRIMARY KEY
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{Query: `BEGIN;`},
				{Query: `SAVEPOINT ddl_sp;`},
				{
					Query: `ALTER TABLE sp_alter_rollback_items
						ADD COLUMN leaked TEXT;`,
				},
				{Query: `ROLLBACK TO SAVEPOINT ddl_sp;`},
				{Query: `COMMIT;`},
				{
					Query: `SELECT count(*)
						FROM information_schema.columns
						WHERE table_schema = 'public'
							AND table_name = 'sp_alter_rollback_items'
							AND column_name = 'leaked';`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
		{
			Name: "CREATE INDEX rolls back to savepoint",
			SetUpScript: []string{
				`CREATE TABLE sp_index_rollback_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{Query: `BEGIN;`},
				{Query: `SAVEPOINT ddl_sp;`},
				{
					Query: `CREATE INDEX sp_index_rollback_items_label_idx
						ON sp_index_rollback_items (label);`,
				},
				{Query: `ROLLBACK TO SAVEPOINT ddl_sp;`},
				{Query: `COMMIT;`},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_class
						WHERE relname = 'sp_index_rollback_items_label_idx';`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
	})
}
