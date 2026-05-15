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

// TestAlterDefaultPrivilegesGrantAppliesToFutureFunctionsRepro reproduces a
// security/ACL persistence bug: Doltgres accepts ALTER DEFAULT PRIVILEGES for
// functions, but the default grant is not applied to functions created later.
func TestAlterDefaultPrivilegesGrantAppliesToFutureFunctionsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "default EXECUTE grant applies to future functions",
			SetUpScript: []string{
				`CREATE USER default_function_user PASSWORD 'function';`,
				`GRANT USAGE ON SCHEMA public TO default_function_user;`,
				`ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT EXECUTE ON FUNCTIONS TO default_function_user;`,
				`CREATE FUNCTION default_priv_function() RETURNS int AS $$ BEGIN RETURN 7; END; $$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT default_priv_function();`,

					Username: `default_function_user`,
					Password: `function`, PostgresOracle: ScriptTestPostgresOracle{

						// TestAlterDefaultPrivilegesPopulatesPgDefaultAclRepro reproduces a catalog
						// persistence bug: Doltgres accepts ALTER DEFAULT PRIVILEGES but pg_default_acl
						// does not expose the default ACL row.
						ID: "default-privileges-repro-test-testalterdefaultprivilegesgrantappliestofuturefunctionsrepro-0001-select-default_priv_function"},
				},
			},
		},
	})
}
