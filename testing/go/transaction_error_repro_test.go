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

// TestTransactionErrorRollsBackTransactionOnCommitRepro reproduces a
// transaction correctness bug: PostgreSQL marks the transaction aborted after a
// statement error, rejects later statements until rollback, and treats COMMIT
// of the failed transaction as a rollback.
func TestTransactionErrorRollsBackTransactionOnCommitRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "transaction error rolls back on commit",
			SetUpScript: []string{
				`CREATE TABLE transaction_error_items (id INT PRIMARY KEY, label TEXT);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `INSERT INTO transaction_error_items VALUES (1, 'before error');`,
				},
				{
					Query: `INSERT INTO transaction_error_items VALUES (1, 'duplicate');`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-error-repro-test-testtransactionerrorrollsbacktransactiononcommitrepro-0001-insert-into-transaction_error_items-values-1", Compare: "sqlstate"},
				},
				{
					Query:       `INSERT INTO transaction_error_items VALUES (2, 'after error');`,
					ExpectedErr: `current transaction is aborted`,
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query:    `SELECT * FROM transaction_error_items ORDER BY id;`,
					Expected: []sql.Row{},
				},
			},
		},
	})
}

// TestSavepointErrorRequiresRollbackToSavepointRepro reproduces a transaction
// correctness bug: after an error inside a savepoint, PostgreSQL rejects all
// statements except transaction-control recovery until ROLLBACK TO SAVEPOINT.
func TestSavepointErrorRequiresRollbackToSavepointRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "savepoint error requires rollback to savepoint",
			SetUpScript: []string{
				`CREATE TABLE savepoint_error_items (id INT PRIMARY KEY, label TEXT);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `INSERT INTO savepoint_error_items VALUES (1, 'before savepoint');`,
				},
				{
					Query: `SAVEPOINT sp;`,
				},
				{
					Query:       `INSERT INTO savepoint_error_items VALUES (1, 'duplicate');`,
					ExpectedErr: `duplicate`,
				},
				{
					Query:       `INSERT INTO savepoint_error_items VALUES (2, 'before rollback to savepoint');`,
					ExpectedErr: `current transaction is aborted`,
				},
				{
					Query: `ROLLBACK TO SAVEPOINT sp;`,
				},
				{
					Query: `INSERT INTO savepoint_error_items VALUES (3, 'after rollback to savepoint');`,
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query: `SELECT id, label
						FROM savepoint_error_items
						ORDER BY id;`,
					Expected: []sql.Row{
						{1, "before savepoint"},
						{3, "after rollback to savepoint"},
					},
				},
			},
		},
	})
}
