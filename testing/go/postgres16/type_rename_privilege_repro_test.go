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

// TestAlterTypeRenameMovesGrantOptionRepro reproduces an ACL persistence bug:
// renaming a type leaves USAGE grant options attached to the old type name, so
// a later type with the old name inherits delegation rights from the renamed
// type.
func TestAlterTypeRenameMovesGrantOptionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TYPE RENAME moves USAGE grant option off old name",
			SetUpScript: []string{
				`CREATE USER rename_type_grantor PASSWORD 'type';`,
				`CREATE USER rename_type_before_grantee PASSWORD 'type';`,
				`CREATE USER rename_type_after_grantee PASSWORD 'type';`,
				`CREATE TYPE rename_acl_type AS ENUM ('old');`,
				`GRANT USAGE ON TYPE rename_acl_type TO rename_type_grantor WITH GRANT OPTION;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `GRANT USAGE ON TYPE rename_acl_type TO rename_type_before_grantee;`,
					Username: `rename_type_grantor`,
					Password: `type`,
				},
				{
					Query: `ALTER TYPE rename_acl_type RENAME TO renamed_acl_type;`,
				},
				{
					Query: `CREATE TYPE rename_acl_type AS ENUM ('replacement');`,
				},
				{
					Query: `REVOKE USAGE ON TYPE rename_acl_type FROM PUBLIC;`,
				},
				{
					Query:       `GRANT USAGE ON TYPE rename_acl_type TO rename_type_after_grantee;`,
					ExpectedErr: `type "rename_acl_type" does not exist`,
					Username:    `rename_type_grantor`,
					Password:    `type`,
				},
			},
		},
	})
}
