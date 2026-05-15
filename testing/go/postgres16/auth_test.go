// Copyright 2024 Dolthub, Inc.
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

func TestAuthTestsPostgresOraclePrefix(t *testing.T) {
	RunScripts(
		t,
		[]ScriptTest{
			{
				Name: `ALTER LOGIN`,
				Assertions: []ScriptTestAssertion{
					{ // By default, roles cannot be logged into
						Query: `CREATE ROLE user1 PASSWORD 'pass1';`, PostgresOracle: ScriptTestPostgresOracle{ID:

						// Users can be logged into by default, this is the only difference between roles and users
						"auth-test-testauthtests-0017-create-role-user1-password-pass1"},
					},
					{
						Query: `CREATE USER user2 PASSWORD 'pass2';`, PostgresOracle: ScriptTestPostgresOracle{ID:

						// A role with LOGIN defined is exactly equivalent to a default user
						"auth-test-testauthtests-0018-create-user-user2-password-pass2"},
					},
					{
						Query: `CREATE ROLE user3 PASSWORD 'pass3' LOGIN;`, PostgresOracle: ScriptTestPostgresOracle{ID:

						// A user with NOLOGIN defined is exactly equivalent to a default role
						"auth-test-testauthtests-0019-create-role-user3-password-pass3"},
					},
					{
						Query: `CREATE USER user4 PASSWORD 'pass4' NOLOGIN;`, PostgresOracle: ScriptTestPostgresOracle{ID: "auth-test-testauthtests-0020-create-user-user4-password-pass4"},
					},
				},
			},
		},
	)
}
