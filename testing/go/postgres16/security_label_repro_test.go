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

// TestSecurityLabelReachesProviderValidationRepro reproduces a catalog
// correctness bug: PostgreSQL parses SECURITY LABEL statements and rejects
// them at provider validation when no security-label provider is loaded.
func TestSecurityLabelReachesProviderValidationRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "SECURITY LABEL reaches provider validation",
			SetUpScript: []string{
				`CREATE TABLE security_label_target (id integer);`,
				`CREATE USER security_label_role PASSWORD 'pw';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SECURITY LABEL ON TABLE security_label_target IS 'classified';`, PostgresOracle: ScriptTestPostgresOracle{ID: "security-label-repro-test-testsecuritylabelreachesprovidervalidationrepro-0001-security-label-on-table-security_label_target", Compare: "sqlstate"},
				},
				{
					Query: `SECURITY LABEL ON COLUMN security_label_target.id IS 'classified';`, PostgresOracle: ScriptTestPostgresOracle{ID: "security-label-repro-test-testsecuritylabelreachesprovidervalidationrepro-0002-security-label-on-column-security_label_target.id", Compare: "sqlstate"},
				},
				{
					Query: `SECURITY LABEL FOR 'dummy' ON TABLE security_label_target IS 'classified';`, PostgresOracle: ScriptTestPostgresOracle{ID: "security-label-repro-test-testsecuritylabelreachesprovidervalidationrepro-0003-security-label-for-dummy-on", Compare: "sqlstate"},
				},
				{
					Query: `SECURITY LABEL ON ROLE security_label_role IS 'classified';`, PostgresOracle: ScriptTestPostgresOracle{ID: "security-label-repro-test-testsecuritylabelreachesprovidervalidationrepro-0004-security-label-on-role-security_label_role", Compare: "sqlstate"},
				},
			},
		},
	})
}
