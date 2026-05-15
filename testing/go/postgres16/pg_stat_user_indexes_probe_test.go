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

	"github.com/dolthub/go-mysql-server/sql"

	"testing"
)

// TestPgStatUserIndexesProbe pins pg_stat_user_indexes row shape and live
// counter behavior for ordinary user index scans. PG admin tooling reads
// idx_scan / idx_tup_read / idx_tup_fetch to identify unused indexes and tune
// planner stats. Per the Dump/admin/tooling TODO in
// docs/app-compatibility-checklist.md.
func TestPgStatUserIndexesProbe(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_stat_user_indexes returns and updates user index counters",
			SetUpScript: []string{
				`CREATE TABLE accounts (id INT PRIMARY KEY, email TEXT);`,
				`INSERT INTO accounts VALUES (1, 'a@example.com'), (2, 'b@example.com');`,
				`CREATE INDEX accounts_email_idx ON accounts (email);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					// Index exists in the view (expect 2: pkey +
					// secondary).
					Query: `SELECT count(*)::text
						FROM pg_stat_user_indexes
						WHERE relname = 'accounts';`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-stat-user-indexes-probe-test-testpgstatuserindexesprobe-0001-select-count-*-::text-from"},
				},
				{
					// Counter columns are queryable in the exact
					// shape admin scripts expect. We don't assert
					// the values (just that the columns are
					// present and the query plans).
					Query: `SELECT count(*)::text
						FROM (
							SELECT idx_scan, idx_tup_read, idx_tup_fetch
							FROM pg_stat_user_indexes
							WHERE relname = 'accounts'
					) t;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-stat-user-indexes-probe-test-testpgstatuserindexesprobe-0002-select-count-*-::text-from"},
				},
				{
					Query: `SELECT idx_scan::text, (last_idx_scan IS NULL)::text, idx_tup_read::text, idx_tup_fetch::text
						FROM pg_stat_user_indexes
						WHERE relname = 'accounts' AND indexrelname = 'accounts_email_idx';`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-stat-user-indexes-probe-test-testpgstatuserindexesprobe-0003-select-idx_scan::text-last_idx_scan-is-null"},
				},
				{
					Query: `EXPLAIN SELECT id FROM accounts WHERE email = 'a@example.com';`,
					Expected: []sql.Row{
						{"Index Scan using accounts_email_idx on accounts  (cost=0.15..8.17 rows=1 width=4)"},
						{"  Index Cond: (email = 'a@example.com'::text)"},
					},
				},
				{
					Query: `SELECT id FROM accounts WHERE email = 'a@example.com';`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-stat-user-indexes-probe-test-testpgstatuserindexesprobe-0005-select-id-from-accounts-where"},
				},
				{
					Query: `SELECT idx_scan::text, (last_idx_scan IS NOT NULL)::text, idx_tup_read::text, idx_tup_fetch::text
						FROM pg_stat_user_indexes
						WHERE relname = 'accounts' AND indexrelname = 'accounts_email_idx';`,
					Expected: []sql.Row{{"1", "true", "1", "1"}},
				},
				{
					Query: `SELECT id FROM accounts WHERE email = 'missing@example.com';`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-stat-user-indexes-probe-test-testpgstatuserindexesprobe-0007-select-id-from-accounts-where"},
				},
				{
					Query: `SELECT idx_scan::text, (last_idx_scan IS NOT NULL)::text, idx_tup_read::text, idx_tup_fetch::text
						FROM pg_stat_user_indexes
						WHERE relname = 'accounts' AND indexrelname = 'accounts_email_idx';`,
					Expected: []sql.Row{{"2", "true", "1", "1"}},
				},
			},
		},
	})
}
