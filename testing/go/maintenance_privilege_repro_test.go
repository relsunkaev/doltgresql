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

// TestVacuumTableRequiresOwnershipRepro reproduces a security bug: Doltgres
// accepts VACUUM on another role's table, while PostgreSQL requires table
// ownership or an equivalent maintenance privilege.
func TestVacuumTableRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "VACUUM requires table ownership",
			SetUpScript: []string{
				`CREATE TABLE vacuum_private (id INT PRIMARY KEY, secret TEXT);`,
				`INSERT INTO vacuum_private VALUES (1, 'hidden');`,
				`CREATE USER vacuum_intruder PASSWORD 'pw';`,
				`GRANT USAGE ON SCHEMA public TO vacuum_intruder;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `VACUUM vacuum_private;`,
					ExpectedErr: `permission denied`,
					Username:    `vacuum_intruder`,
					Password:    `pw`,
				},
			},
		},
	})
}
