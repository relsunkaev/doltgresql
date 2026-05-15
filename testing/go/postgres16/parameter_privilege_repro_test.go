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

	"testing"
)

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
					Query: `GRANT SET ON PARAMETER work_mem TO parameter_set_grantee;`, PostgresOracle: ScriptTestPostgresOracle{ID: "parameter-privilege-repro-test-testgrantonconfigurationparameterrepro-0001-grant-set-on-parameter-work_mem", Compare: "tag"},
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
					Query: `GRANT ALTER SYSTEM ON PARAMETER work_mem TO parameter_alter_system_grantee;`, PostgresOracle: ScriptTestPostgresOracle{ID: "parameter-privilege-repro-test-testgrantonconfigurationparameterrepro-0002-grant-alter-system-on-parameter",

						// TestRevokeOnConfigurationParameterRepro reproduces an admin ACL correctness
						// bug: PostgreSQL supports revoking SET and ALTER SYSTEM privileges from
						// configuration parameters.
						Compare: "tag"},
				},
			},
		},
	})
}

func TestRevokeOnConfigurationParameterRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "REVOKE SET on configuration parameter",
			SetUpScript: []string{
				`CREATE USER parameter_set_revokee PASSWORD 'pw';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `REVOKE SET ON PARAMETER work_mem FROM parameter_set_revokee;`, PostgresOracle: ScriptTestPostgresOracle{ID: "parameter-privilege-repro-test-testrevokeonconfigurationparameterrepro-0001-revoke-set-on-parameter-work_mem", Compare: "tag"},
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
					Query: `REVOKE ALTER SYSTEM ON PARAMETER work_mem FROM parameter_alter_system_revokee;`, PostgresOracle: ScriptTestPostgresOracle{ID: "parameter-privilege-repro-test-testrevokeonconfigurationparameterrepro-0002-revoke-alter-system-on-parameter",

						// TestHasParameterPrivilegeHelperRepro reproduces an admin ACL helper gap:
						// PostgreSQL exposes has_parameter_privilege for parameter-level SET and ALTER
						// SYSTEM privileges.
						Compare: "tag"},
				},
			},
		},
	})
}

func TestHasParameterPrivilegeHelperRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "has_parameter_privilege reports parameter privileges",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT has_parameter_privilege('work_mem', 'SET');`, PostgresOracle: ScriptTestPostgresOracle{ID: "parameter-privilege-repro-test-testhasparameterprivilegehelperrepro-0001-select-has_parameter_privilege-work_mem-set"},
				},
			},
		},
	})
}

// TestSetSuperuserOnlyParameterRequiresSuperuserRepro reproduces a security
// bug: PostgreSQL's superuser-context parameters require superuser rights or an
// explicit parameter-level SET privilege before a normal role can change them.
func TestSetSuperuserOnlyParameterRequiresSuperuserRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "SET superuser-only parameter requires superuser",
			SetUpScript: []string{
				`CREATE USER parameter_superuser_intruder PASSWORD 'pw';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SET session_preload_libraries = 'unsafe_library';`,

					Username: `parameter_superuser_intruder`,
					Password: `pw`, PostgresOracle: ScriptTestPostgresOracle{ID: "parameter-privilege-repro-test-testsetsuperuseronlyparameterrequiressuperuserrepro-0001-set-session_preload_libraries-=-unsafe_library", Compare: "sqlstate"},
				},
				{
					Query: `SELECT current_setting('session_preload_libraries');`,

					Username: `parameter_superuser_intruder`,
					Password: `pw`, PostgresOracle: ScriptTestPostgresOracle{ID: "parameter-privilege-repro-test-testsetsuperuseronlyparameterrequiressuperuserrepro-0002-select-current_setting-session_preload_libraries", Compare: "sqlstate"},
				},
			},
		},
	})
}
