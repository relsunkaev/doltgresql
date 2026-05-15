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

package postgres16

import (
	. "github.com/dolthub/doltgresql/testing/go"

	"testing"
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
					Query: `SELECT pg_advisory_lock(1)`, PostgresOracle: ScriptTestPostgresOracle{ID: "lock-test-testadvisorylocks-0001-select-pg_advisory_lock-1"},
				},
				{
					Query: `SELECT pg_try_advisory_lock(2)`, PostgresOracle: ScriptTestPostgresOracle{ID: "lock-test-testadvisorylocks-0002-select-pg_try_advisory_lock-2"},
				},
				{
					// When a different session tries to acquire the same lock, it fails.
					Username: "user1",
					Password: "password",
					Query:    `SELECT pg_try_advisory_lock(1)`, PostgresOracle: ScriptTestPostgresOracle{ID: "lock-test-testadvisorylocks-0003-select-pg_try_advisory_lock-1"},
				},
				{
					// When a different session tries to acquire the same lock, it fails.
					Username: "user1",
					Password: "password",
					Query:    `SELECT pg_try_advisory_lock(2)`, PostgresOracle: ScriptTestPostgresOracle{ID: "lock-test-testadvisorylocks-0004-select-pg_try_advisory_lock-2"},
				},
				{
					Query: `SELECT pg_advisory_unlock(1)`, PostgresOracle: ScriptTestPostgresOracle{ID: "lock-test-testadvisorylocks-0005-select-pg_advisory_unlock-1"},
				},
				{
					Query: `SELECT pg_advisory_unlock(2)`, PostgresOracle: ScriptTestPostgresOracle{ID: "lock-test-testadvisorylocks-0006-select-pg_advisory_unlock-2"},
				},
				{
					Query: `SELECT pg_advisory_unlock(3)`, PostgresOracle: ScriptTestPostgresOracle{ID: "lock-test-testadvisorylocks-0007-select-pg_advisory_unlock-3"},
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
					Query: `BEGIN;`, PostgresOracle: ScriptTestPostgresOracle{ID: "lock-test-testadvisorylocks-0008-begin"},
				},
				{
					Query: `SELECT pg_advisory_xact_lock(101);`, PostgresOracle: ScriptTestPostgresOracle{ID: "lock-test-testadvisorylocks-0009-select-pg_advisory_xact_lock-101"},
				},
				{
					// Another session must not acquire while held inside the txn.
					Username: "xactuser",
					Password: "password",
					Query:    `SELECT pg_try_advisory_xact_lock(101);`, PostgresOracle: ScriptTestPostgresOracle{ID: "lock-test-testadvisorylocks-0010-select-pg_try_advisory_xact_lock-101"},
				},
				{
					Query: `COMMIT;`, PostgresOracle: ScriptTestPostgresOracle{ID: "lock-test-testadvisorylocks-0011-commit"},
				},
				{
					// After COMMIT, the lock is released automatically.
					Username: "xactuser",
					Password: "password",
					Query:    `SELECT pg_try_advisory_xact_lock(101);`, PostgresOracle: ScriptTestPostgresOracle{ID: "lock-test-testadvisorylocks-0012-select-pg_try_advisory_xact_lock-101"},
				},
			},
		},
		{
			Name: "pg_advisory_xact_lock auto-releases on ROLLBACK",
			SetUpScript: []string{
				`CREATE USER rolluser PASSWORD 'password';`,
			},
			Assertions: []ScriptTestAssertion{
				{Query: `BEGIN;`, PostgresOracle: ScriptTestPostgresOracle{ID: "lock-test-testadvisorylocks-0013-begin"}},
				{
					Query: `SELECT pg_advisory_xact_lock(202);`, PostgresOracle: ScriptTestPostgresOracle{ID: "lock-test-testadvisorylocks-0014-select-pg_advisory_xact_lock-202"},
				},
				{
					Username: "rolluser",
					Password: "password",
					Query:    `SELECT pg_try_advisory_xact_lock(202);`, PostgresOracle: ScriptTestPostgresOracle{ID: "lock-test-testadvisorylocks-0015-select-pg_try_advisory_xact_lock-202"},
				},
				{Query: `ROLLBACK;`, PostgresOracle: ScriptTestPostgresOracle{ID: "lock-test-testadvisorylocks-0016-rollback"}},
				{
					Username: "rolluser",
					Password: "password",
					Query:    `SELECT pg_try_advisory_xact_lock(202);`, PostgresOracle: ScriptTestPostgresOracle{ID: "lock-test-testadvisorylocks-0017-select-pg_try_advisory_xact_lock-202"},
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
					Query: `SELECT pg_advisory_xact_lock(303);`, PostgresOracle: ScriptTestPostgresOracle{ID: "lock-test-testadvisorylocks-0018-select-pg_advisory_xact_lock-303"},
				},
				{
					// In autocommit mode the lock is released as the statement ends, so
					// another session can acquire it on the next statement.
					Username: "autouser",
					Password: "password",
					Query:    `SELECT pg_try_advisory_xact_lock(303);`, PostgresOracle: ScriptTestPostgresOracle{ID: "lock-test-testadvisorylocks-0019-select-pg_try_advisory_xact_lock-303"},
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
					Query:    `SELECT pg_try_advisory_xact_lock(404);`, PostgresOracle: ScriptTestPostgresOracle{ID: "lock-test-testadvisorylocks-0020-select-pg_try_advisory_xact_lock-404"},
				},
				{
					// Releasing on the original session lets the next attempt succeed.
					Query: `SELECT pg_advisory_unlock(404);`, PostgresOracle: ScriptTestPostgresOracle{ID: "lock-test-testadvisorylocks-0021-select-pg_advisory_unlock-404"},
				},
				{
					Username: "tryuser",
					Password: "password",
					Query:    `SELECT pg_try_advisory_xact_lock(404);`, PostgresOracle: ScriptTestPostgresOracle{ID: "lock-test-testadvisorylocks-0022-select-pg_try_advisory_xact_lock-404"},
				},
			},
		},
		{
			Name: "advisory locks integrate with hashtext for derived keys",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT pg_advisory_xact_lock(hashtext('job-1'));`, PostgresOracle: ScriptTestPostgresOracle{ID: "lock-test-testadvisorylocks-0023-select-pg_advisory_xact_lock-hashtext-job-1"},
				},
				{
					Query: `SELECT pg_try_advisory_lock(hashtext('job-2'));`, PostgresOracle: ScriptTestPostgresOracle{ID: "lock-test-testadvisorylocks-0024-select-pg_try_advisory_lock-hashtext-job-2"},
				},
				{
					Query: `SELECT pg_advisory_unlock(hashtext('job-2'));`, PostgresOracle: ScriptTestPostgresOracle{ID: "lock-test-testadvisorylocks-0025-select-pg_advisory_unlock-hashtext-job-2"},
				},
			},
		},
		{
			Name: "pg_advisory_unlock_all releases session locks",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT pg_advisory_lock(501);`, PostgresOracle: ScriptTestPostgresOracle{ID: "lock-test-testadvisorylocks-0026-select-pg_advisory_lock-501"},
				},
				{
					Query: `SELECT pg_advisory_lock(502);`, PostgresOracle: ScriptTestPostgresOracle{ID: "lock-test-testadvisorylocks-0027-select-pg_advisory_lock-502"},
				},
				{
					Query:            `SELECT pg_advisory_unlock_all();`,
					SkipResultsCheck: true,
				},
				{
					// After unlock_all, the session no longer holds the locks,
					// so pg_advisory_unlock returns false.
					Query: `SELECT pg_advisory_unlock(501), pg_advisory_unlock(502);`, PostgresOracle: ScriptTestPostgresOracle{ID: "lock-test-testadvisorylocks-0028-select-pg_advisory_unlock-501-pg_advisory_unlock-502"},
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
					Query: `SELECT pg_advisory_lock(10::int4, 20::int4);`, PostgresOracle: ScriptTestPostgresOracle{ID: "lock-test-testadvisorylocks-0029-select-pg_advisory_lock-10::int4-20::int4"},
				},
				{
					Username: "pairuser",
					Password: "password",
					Query:    `SELECT pg_try_advisory_lock(10::int4, 20::int4);`, PostgresOracle: ScriptTestPostgresOracle{ID: "lock-test-testadvisorylocks-0030-select-pg_try_advisory_lock-10::int4-20::int4"},
				},
				{
					Query: `SELECT pg_advisory_unlock(10::int4, 20::int4);`, PostgresOracle: ScriptTestPostgresOracle{ID: "lock-test-testadvisorylocks-0031-select-pg_advisory_unlock-10::int4-20::int4"},
				},
				{
					Username: "pairuser",
					Password: "password",
					Query:    `SELECT pg_try_advisory_lock(10::int4, 20::int4);`, PostgresOracle: ScriptTestPostgresOracle{ID: "lock-test-testadvisorylocks-0032-select-pg_try_advisory_lock-10::int4-20::int4"},
				},
				{
					Username: "pairuser",
					Password: "password",
					Query:    `SELECT pg_advisory_unlock(10::int4, 20::int4);`, PostgresOracle: ScriptTestPostgresOracle{ID: "lock-test-testadvisorylocks-0033-select-pg_advisory_unlock-10::int4-20::int4"},
				},
			},
		},
		{
			Name: "pg_try_advisory_xact_lock is reentrant within the same session",
			Assertions: []ScriptTestAssertion{
				{Query: `BEGIN;`, PostgresOracle: ScriptTestPostgresOracle{ID: "lock-test-testadvisorylocks-0034-begin"}},
				{
					Query: `SELECT pg_advisory_xact_lock(707);`, PostgresOracle: ScriptTestPostgresOracle{ID: "lock-test-testadvisorylocks-0035-select-pg_advisory_xact_lock-707"},
				},
				{
					// The same session can reacquire its own lock — PostgreSQL
					// permits this and increments the hold count.
					Query: `SELECT pg_try_advisory_xact_lock(707);`, PostgresOracle: ScriptTestPostgresOracle{ID: "lock-test-testadvisorylocks-0036-select-pg_try_advisory_xact_lock-707"},
				},
				{Query: `COMMIT;`, PostgresOracle: ScriptTestPostgresOracle{ID: "lock-test-testadvisorylocks-0037-commit"}},
			},
		},
	})
}
