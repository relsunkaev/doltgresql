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

// TestDropTypeClearsGrantOptionRepro reproduces an ACL persistence bug:
// dropping a type does not clear USAGE grant options, so a later type with the
// same name inherits delegation rights granted on the dropped type.
func TestDropTypeClearsGrantOptionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP TYPE clears USAGE grant option before recreate",
			SetUpScript: []string{
				`CREATE USER drop_recreate_type_grantor PASSWORD 'type';`,
				`CREATE USER drop_recreate_type_grantee PASSWORD 'type';`,
				`CREATE USER drop_recreate_type_after_grantee PASSWORD 'type';`,
				`CREATE TYPE drop_recreate_acl_type AS ENUM ('old');`,
				`GRANT USAGE ON TYPE drop_recreate_acl_type
					TO drop_recreate_type_grantor WITH GRANT OPTION;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `GRANT USAGE ON TYPE drop_recreate_acl_type TO drop_recreate_type_grantee;`,
					Username: `drop_recreate_type_grantor`,
					Password: `type`,
				},
				{
					Query: `DROP TYPE drop_recreate_acl_type;`,
				},
				{
					Query: `CREATE TYPE drop_recreate_acl_type AS ENUM ('new');`,
				},
				{
					Query: `GRANT USAGE ON TYPE drop_recreate_acl_type TO drop_recreate_type_after_grantee;`,

					Username: `drop_recreate_type_grantor`,
					Password: `type`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-drop-privilege-repro-test-testdroptypeclearsgrantoptionrepro-0001-grant-usage-on-type-drop_recreate_acl_type", Compare: "sqlstate"},
				},
			},
		},
	})
}
