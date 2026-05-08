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

// TestPgStatUserIndexesProbe pins what `pg_stat_user_indexes` returns
// today. PG admin tooling reads idx_scan / idx_tup_read /
// idx_tup_fetch counters from this view to identify unused indexes
// and tune planner stats. Doltgres has no live counter
// instrumentation, so the contract we want is "the view exists,
// returns rows with the expected columns shape, and counters are
// either zero or stable" — that lets admin scripts run without
// branching on missing-view errors. Per the Dump/admin/tooling TODO
// in docs/app-compatibility-checklist.md.
func TestPgStatUserIndexesProbe(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_stat_user_indexes returns rows for user indexes with expected columns",
			SetUpScript: []string{
				`CREATE TABLE accounts (id INT PRIMARY KEY, email TEXT);`,
				`CREATE INDEX accounts_email_idx ON accounts (email);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					// Index exists in the view (expect 2: pkey +
					// secondary).
					Query: `SELECT count(*)::text
						FROM pg_stat_user_indexes
						WHERE relname = 'accounts';`,
					Expected: []sql.Row{{"2"}},
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
						) t;`,
					Expected: []sql.Row{{"2"}},
				},
			},
		},
	})
}
