// Copyright 2025 Dolthub, Inc.
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

// TestLocks tests the advisory lock functions, such as pg_try_advisory_lock and pg_advisory_unlock.
func TestAdvisoryLocks(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "basic lock tests",
			SetUpScript: []string{
				`CREATE USER user1 PASSWORD 'password';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					// pg_advisory_lock returns void.
					Query:    `SELECT pg_advisory_lock(1)`,
					Expected: []sql.Row{{nil}},
				},
				{
					Query:    `SELECT pg_try_advisory_lock(2)`,
					Expected: []sql.Row{{"t"}},
				},
				{
					// When a different session tries to acquire the same lock, it fails.
					Username: "user1",
					Password: "password",
					Query:    `SELECT pg_try_advisory_lock(1)`,
					Expected: []sql.Row{{"f"}},
				},
				{
					// When a different session tries to acquire the same lock, it fails.
					Username: "user1",
					Password: "password",
					Query:    `SELECT pg_try_advisory_lock(2)`,
					Expected: []sql.Row{{"f"}},
				},
				{
					Query:    `SELECT pg_advisory_unlock(1)`,
					Expected: []sql.Row{{"t"}},
				},
				{
					Query:    `SELECT pg_advisory_unlock(2)`,
					Expected: []sql.Row{{"t"}},
				},
				{
					Query:    `SELECT pg_advisory_unlock(3)`,
					Expected: []sql.Row{{"f"}},
				},
			},
		},
		{
			Name: "pg_advisory_xact_lock auto-releases at end of transaction",
			SetUpScript: []string{
				`CREATE USER xactuser PASSWORD 'password';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `BEGIN;`,
					Expected: []sql.Row{},
				},
				{
					Query:    `SELECT pg_advisory_xact_lock(101);`,
					Expected: []sql.Row{{nil}},
				},
				{
					// Another session must not acquire while held inside the txn.
					Username: "xactuser",
					Password: "password",
					Query:    `SELECT pg_try_advisory_xact_lock(101);`,
					Expected: []sql.Row{{"f"}},
				},
				{
					Query:    `COMMIT;`,
					Expected: []sql.Row{},
				},
				{
					// After COMMIT, the lock is released automatically.
					Username: "xactuser",
					Password: "password",
					Query:    `SELECT pg_try_advisory_xact_lock(101);`,
					Expected: []sql.Row{{"t"}},
				},
			},
		},
		{
			Name: "pg_advisory_xact_lock auto-releases on ROLLBACK",
			SetUpScript: []string{
				`CREATE USER rolluser PASSWORD 'password';`,
			},
			Assertions: []ScriptTestAssertion{
				{Query: `BEGIN;`, Expected: []sql.Row{}},
				{
					Query:    `SELECT pg_advisory_xact_lock(202);`,
					Expected: []sql.Row{{nil}},
				},
				{
					Username: "rolluser",
					Password: "password",
					Query:    `SELECT pg_try_advisory_xact_lock(202);`,
					Expected: []sql.Row{{"f"}},
				},
				{Query: `ROLLBACK;`, Expected: []sql.Row{}},
				{
					Username: "rolluser",
					Password: "password",
					Query:    `SELECT pg_try_advisory_xact_lock(202);`,
					Expected: []sql.Row{{"t"}},
				},
			},
		},
		{
			Name: "pg_advisory_xact_lock under autocommit releases per-statement",
			SetUpScript: []string{
				`CREATE USER autouser PASSWORD 'password';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT pg_advisory_xact_lock(303);`,
					Expected: []sql.Row{{nil}},
				},
				{
					// In autocommit mode the lock is released as the statement ends, so
					// another session can acquire it on the next statement.
					Username: "autouser",
					Password: "password",
					Query:    `SELECT pg_try_advisory_xact_lock(303);`,
					Expected: []sql.Row{{"t"}},
				},
			},
		},
		{
			Name: "pg_try_advisory_xact_lock returns false without blocking on contention",
			SetUpScript: []string{
				`CREATE USER tryuser PASSWORD 'password';`,
				`SELECT pg_advisory_lock(404);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Username: "tryuser",
					Password: "password",
					Query:    `SELECT pg_try_advisory_xact_lock(404);`,
					Expected: []sql.Row{{"f"}},
				},
				{
					// Releasing on the original session lets the next attempt succeed.
					Query:    `SELECT pg_advisory_unlock(404);`,
					Expected: []sql.Row{{"t"}},
				},
				{
					Username: "tryuser",
					Password: "password",
					Query:    `SELECT pg_try_advisory_xact_lock(404);`,
					Expected: []sql.Row{{"t"}},
				},
			},
		},
		{
			Name: "advisory locks integrate with hashtext for derived keys",
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT pg_advisory_xact_lock(hashtext('job-1'));`,
					Expected: []sql.Row{{nil}},
				},
				{
					Query:    `SELECT pg_try_advisory_lock(hashtext('job-2'));`,
					Expected: []sql.Row{{"t"}},
				},
				{
					Query:    `SELECT pg_advisory_unlock(hashtext('job-2'));`,
					Expected: []sql.Row{{"t"}},
				},
			},
		},
		{
			Name: "pg_advisory_unlock_all releases session locks",
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT pg_advisory_lock(501);`,
					Expected: []sql.Row{{nil}},
				},
				{
					Query:    `SELECT pg_advisory_lock(502);`,
					Expected: []sql.Row{{nil}},
				},
				{
					Query:            `SELECT pg_advisory_unlock_all();`,
					SkipResultsCheck: true,
				},
				{
					// After unlock_all, the session no longer holds the locks,
					// so pg_advisory_unlock returns false.
					Query:    `SELECT pg_advisory_unlock(501), pg_advisory_unlock(502);`,
					Expected: []sql.Row{{"f", "f"}},
				},
			},
		},
		{
			Name: "advisory lock (int4, int4) overloads",
			SetUpScript: []string{
				`CREATE USER pairuser PASSWORD 'password';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					// pg_advisory_lock returns void.
					Query:    `SELECT pg_advisory_lock(10::int4, 20::int4);`,
					Expected: []sql.Row{{nil}},
				},
				{
					Username: "pairuser",
					Password: "password",
					Query:    `SELECT pg_try_advisory_lock(10::int4, 20::int4);`,
					Expected: []sql.Row{{"f"}},
				},
				{
					Query:    `SELECT pg_advisory_unlock(10::int4, 20::int4);`,
					Expected: []sql.Row{{"t"}},
				},
				{
					Username: "pairuser",
					Password: "password",
					Query:    `SELECT pg_try_advisory_lock(10::int4, 20::int4);`,
					Expected: []sql.Row{{"t"}},
				},
				{
					Username: "pairuser",
					Password: "password",
					Query:    `SELECT pg_advisory_unlock(10::int4, 20::int4);`,
					Expected: []sql.Row{{"t"}},
				},
			},
		},
		{
			Name: "pg_try_advisory_xact_lock is reentrant within the same session",
			Assertions: []ScriptTestAssertion{
				{Query: `BEGIN;`, Expected: []sql.Row{}},
				{
					Query:    `SELECT pg_advisory_xact_lock(707);`,
					Expected: []sql.Row{{nil}},
				},
				{
					// The same session can reacquire its own lock — PostgreSQL
					// permits this and increments the hold count.
					Query:    `SELECT pg_try_advisory_xact_lock(707);`,
					Expected: []sql.Row{{"t"}},
				},
				{Query: `COMMIT;`, Expected: []sql.Row{}},
			},
		},
	})
}
