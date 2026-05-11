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

// TestRevokedDatabaseConnectPreventsNewSessionRepro reproduces a security bug:
// revoking CONNECT on the database from PUBLIC does not prevent a normal user
// from opening a new session and querying that database.
func TestRevokedDatabaseConnectPreventsNewSessionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "REVOKE CONNECT ON DATABASE prevents new sessions",
			SetUpScript: []string{
				`CREATE USER no_connect PASSWORD 'pw';`,
				`REVOKE CONNECT ON DATABASE postgres FROM PUBLIC;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `SELECT 1;`,
					ExpectedErr: `permission denied`,
					Username:    `no_connect`,
					Password:    `pw`,
				},
			},
		},
	})
}
