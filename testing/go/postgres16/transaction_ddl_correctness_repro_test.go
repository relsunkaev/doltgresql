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

package postgres16

import (
	. "github.com/dolthub/doltgresql/testing/go"
	"testing"
)

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
					Query: `SELECT id, label FROM tx_drop_rollback_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-correctness-repro-test-testdroptablerollbackrestorestableguard-0001-select-id-label-from-tx_drop_rollback_items"},
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
						WHERE relname = 'tx_index_rollback_items_label_idx';`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-correctness-repro-test-testcreateindexrollbackremovesindexguard-0001-select-count-*-from-pg_catalog.pg_class"},
				},
			},
		},
	})
}
