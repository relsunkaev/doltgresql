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

func TestPreparedTransactions(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "commit prepared transaction",
			SetUpScript: []string{
				"CREATE TABLE prepared_tx_items (id INT PRIMARY KEY, label TEXT);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "BEGIN;",
				},
				{
					Query: "INSERT INTO prepared_tx_items VALUES (1, 'one');",
				},
				{
					Query: "PREPARE TRANSACTION 'dg_prepared_commit';",
				},
				{
					Query: "SELECT count(*) FROM prepared_tx_items;",
					Expected: []sql.Row{
						{0},
					},
				},
				{
					Query: "SELECT gid, database FROM pg_catalog.pg_prepared_xacts WHERE gid = 'dg_prepared_commit';",
					Expected: []sql.Row{
						{"dg_prepared_commit", "postgres"},
					},
				},
				{
					Query: "COMMIT PREPARED 'dg_prepared_commit';",
				},
				{
					Query: "SELECT id, label FROM prepared_tx_items;",
					Expected: []sql.Row{
						{1, "one"},
					},
				},
				{
					Query:    "SELECT gid FROM pg_catalog.pg_prepared_xacts WHERE gid = 'dg_prepared_commit';",
					Expected: []sql.Row{},
				},
			},
		},
		{
			Name: "rollback prepared transaction",
			SetUpScript: []string{
				"CREATE TABLE prepared_tx_items (id INT PRIMARY KEY, label TEXT);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "BEGIN;",
				},
				{
					Query: "INSERT INTO prepared_tx_items VALUES (1, 'one');",
				},
				{
					Query: "PREPARE TRANSACTION 'dg_prepared_rollback';",
				},
				{
					Query: "SELECT gid FROM pg_catalog.pg_prepared_xacts WHERE gid = 'dg_prepared_rollback';",
					Expected: []sql.Row{
						{"dg_prepared_rollback"},
					},
				},
				{
					Query: "ROLLBACK PREPARED 'dg_prepared_rollback';",
				},
				{
					Query:    "SELECT * FROM prepared_tx_items;",
					Expected: []sql.Row{},
				},
				{
					Query:    "SELECT gid FROM pg_catalog.pg_prepared_xacts WHERE gid = 'dg_prepared_rollback';",
					Expected: []sql.Row{},
				},
			},
		},
		{
			Name: "prepared transaction errors",
			SetUpScript: []string{
				"CREATE TABLE prepared_tx_items (id INT PRIMARY KEY, label TEXT);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       "PREPARE TRANSACTION 'dg_no_transaction';",
					ExpectedErr: "can only be used in transaction blocks",
				},
				{
					Query:       "COMMIT PREPARED 'dg_missing';",
					ExpectedErr: "does not exist",
				},
				{
					Query:       "ROLLBACK PREPARED 'dg_missing';",
					ExpectedErr: "does not exist",
				},
				{
					Query: "BEGIN;",
				},
				{
					Query:       "COMMIT PREPARED 'dg_missing';",
					ExpectedErr: "cannot run inside a transaction block",
				},
				{
					Query: "ROLLBACK;",
				},
			},
		},
	})
}
