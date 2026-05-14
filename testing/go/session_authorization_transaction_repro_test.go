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

// TestSetSessionAuthorizationRejectedInTransactionRepro reproduces a
// transaction/session-state bug: SET SESSION AUTHORIZATION succeeds inside an
// open transaction, then leaves the session identity changed after ROLLBACK.
func TestSetSessionAuthorizationRejectedInTransactionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "SET SESSION AUTHORIZATION is rejected inside transaction",
			SetUpScript: []string{
				`CREATE USER txn_session_auth_target PASSWORD 'pw';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `SET SESSION AUTHORIZATION txn_session_auth_target;`, PostgresOracle: ScriptTestPostgresOracle{ID: "session-authorization-transaction-repro-test-testsetsessionauthorizationrejectedintransactionrepro-0001-set-session-authorization-txn_session_auth_target"},
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query:    `SELECT current_user, session_user;`,
					Expected: []sql.Row{{"postgres", "postgres"}},
				},
			},
		},
	})
}
