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

// TestGrantOnConfigurationParameterRepro reproduces an admin ACL correctness
// bug: PostgreSQL supports granting SET and ALTER SYSTEM privileges on
// configuration parameters.
func TestGrantOnConfigurationParameterRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "GRANT SET on configuration parameter",
			SetUpScript: []string{
				`CREATE USER parameter_set_grantee PASSWORD 'pw';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `GRANT SET ON PARAMETER work_mem TO parameter_set_grantee;`,
					ExpectedTag: `GRANT`,
				},
			},
		},
		{
			Name: "GRANT ALTER SYSTEM on configuration parameter",
			SetUpScript: []string{
				`CREATE USER parameter_alter_system_grantee PASSWORD 'pw';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `GRANT ALTER SYSTEM ON PARAMETER work_mem TO parameter_alter_system_grantee;`,
					ExpectedTag: `GRANT`,
				},
			},
		},
	})
}

// TestRevokeOnConfigurationParameterRepro reproduces an admin ACL correctness
// bug: PostgreSQL supports revoking SET and ALTER SYSTEM privileges from
// configuration parameters.
func TestRevokeOnConfigurationParameterRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "REVOKE SET on configuration parameter",
			SetUpScript: []string{
				`CREATE USER parameter_set_revokee PASSWORD 'pw';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `REVOKE SET ON PARAMETER work_mem FROM parameter_set_revokee;`,
					ExpectedTag: `REVOKE`,
				},
			},
		},
		{
			Name: "REVOKE ALTER SYSTEM on configuration parameter",
			SetUpScript: []string{
				`CREATE USER parameter_alter_system_revokee PASSWORD 'pw';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `REVOKE ALTER SYSTEM ON PARAMETER work_mem FROM parameter_alter_system_revokee;`,
					ExpectedTag: `REVOKE`,
				},
			},
		},
	})
}
