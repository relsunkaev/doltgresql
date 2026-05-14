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

package postgres18

import (
	. "github.com/dolthub/doltgresql/testing/go"

	"testing"
)

// TestPostgres18LargeObjectDefaultPrivilegesRepro reproduces a PostgreSQL 18
// ACL parity gap: ALTER DEFAULT PRIVILEGES can set default SELECT/UPDATE
// privileges for future large objects and records them with defaclobjtype = 'L'.
func TestPostgres18LargeObjectDefaultPrivilegesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER DEFAULT PRIVILEGES supports large objects",
			SetUpScript: []string{
				`CREATE ROLE default_large_object_reader;`,
				`ALTER DEFAULT PRIVILEGES
					GRANT SELECT ON LARGE OBJECTS TO default_large_object_reader;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT defaclobjtype
						FROM pg_catalog.pg_default_acl
						WHERE defaclnamespace = 0
							AND defaclobjtype = 'L';`, PostgresOracle: ScriptTestPostgresOracle{ID: "postgres18/default-privileges-repro-test-testpostgres18largeobjectdefaultprivilegesrepro-0001-select-defaclobjtype-from-pg_catalog.pg_default_acl-where"},
				},
			},
		},
	})
}
