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

import "testing"

// TestCreateRoleRejectsInvalidConnectionLimitRepro reproduces a role metadata
// correctness bug: PostgreSQL only accepts -1 or non-negative connection
// limits.
func TestCreateRoleRejectsInvalidConnectionLimitRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE ROLE rejects invalid connection limit",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE ROLE invalid_create_conn_limit CONNECTION LIMIT -2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "role-option-correctness-repro-test-testcreaterolerejectsinvalidconnectionlimitrepro-0001-create-role-invalid_create_conn_limit-connection-limit",

					// TestAlterRoleRejectsInvalidConnectionLimitRepro reproduces a role metadata
					// correctness bug: ALTER ROLE should reject connection limits below -1 instead
					// of persisting invalid role metadata.
					Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestAlterRoleRejectsInvalidConnectionLimitRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER ROLE rejects invalid connection limit",
			SetUpScript: []string{
				`CREATE ROLE invalid_alter_conn_limit;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER ROLE invalid_alter_conn_limit CONNECTION LIMIT -2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "role-option-correctness-repro-test-testalterrolerejectsinvalidconnectionlimitrepro-0001-alter-role-invalid_alter_conn_limit-connection-limit", Compare: "sqlstate"},
				},
			},
		},
	})
}
