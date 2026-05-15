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

func TestPreparedTransactions(t *testing.T) {
	RunScripts(
		t,
		[]ScriptTest{
			{
				Name: "prepared transaction errors",
				SetUpScript: []string{
					"CREATE TABLE prepared_tx_items (id INT PRIMARY KEY, label TEXT);",
				},
				Assertions: []ScriptTestAssertion{
					{
						Query: "PREPARE TRANSACTION 'dg_no_transaction';", PostgresOracle: ScriptTestPostgresOracle{ID: "prepared-transaction-test-testpreparedtransactions-0008-prepare-transaction-dg_no_transaction"},
					},
					{
						Query: "COMMIT PREPARED 'dg_missing';", PostgresOracle: ScriptTestPostgresOracle{ID: "prepared-transaction-test-testpreparedtransactions-0009-commit-prepared-dg_missing", Compare: "sqlstate"},
					},
					{
						Query: "ROLLBACK PREPARED 'dg_missing';", PostgresOracle: ScriptTestPostgresOracle{ID: "prepared-transaction-test-testpreparedtransactions-0010-rollback-prepared-dg_missing", Compare: "sqlstate"},
					},
					{
						Query: "BEGIN;",
					},
					{
						Query: "COMMIT PREPARED 'dg_missing';", PostgresOracle: ScriptTestPostgresOracle{ID: "prepared-transaction-test-testpreparedtransactions-0011-commit-prepared-dg_missing",

							// TestCommitPreparedRequiresTransactionOwnerRepro reproduces a security bug:
							// Doltgres lets a role commit a prepared transaction that was prepared by a
							// different role.
							Compare: "sqlstate"},
					},
					{
						Query: "ROLLBACK;",
					},
				},
			},
		},
	)
}
