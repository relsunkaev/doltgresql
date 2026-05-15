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

// TestAlterTypeSetSchemaMovesGrantOptionRepro reproduces an ACL persistence
// bug: moving a type to another schema leaves USAGE grant options attached to
// the old schema/name, so a later type there inherits delegation rights.
func TestAlterTypeSetSchemaMovesGrantOptionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TYPE SET SCHEMA moves USAGE grant option off old schema",
			SetUpScript: []string{
				`CREATE USER set_schema_type_grantor PASSWORD 'type';`,
				`CREATE USER set_schema_type_before_grantee PASSWORD 'type';`,
				`CREATE USER set_schema_type_after_grantee PASSWORD 'type';`,
				`CREATE SCHEMA set_schema_type_old;`,
				`CREATE SCHEMA set_schema_type_new;`,
				`CREATE TYPE set_schema_type_old.move_acl_type AS ENUM ('old');`,
				`GRANT USAGE ON TYPE set_schema_type_old.move_acl_type
					TO set_schema_type_grantor WITH GRANT OPTION;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `GRANT USAGE ON TYPE set_schema_type_old.move_acl_type
						TO set_schema_type_before_grantee;`,
					Username: `set_schema_type_grantor`,
					Password: `type`,
				},
				{
					Query: `ALTER TYPE set_schema_type_old.move_acl_type SET SCHEMA set_schema_type_new;`,
				},
				{
					Query: `CREATE TYPE set_schema_type_old.move_acl_type AS ENUM ('replacement');`,
				},
				{
					Query: `GRANT USAGE ON TYPE set_schema_type_old.move_acl_type
						TO set_schema_type_after_grantee;`,

					Username: `set_schema_type_grantor`,
					Password: `type`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-set-schema-privilege-repro-test-testaltertypesetschemamovesgrantoptionrepro-0001-grant-usage-on-type-set_schema_type_old.move_acl_type", Compare: "sqlstate"},
				},
			},
		},
	})
}
