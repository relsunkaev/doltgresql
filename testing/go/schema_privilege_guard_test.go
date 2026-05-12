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

import (
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
)

// TestCreateSchemaRejectsDoltReservedNamespaceRepro reproduces a namespace
// security bug: user-created schemas must not be allowed to occupy Dolt's
// reserved dolt_ namespace.
func TestCreateSchemaRejectsDoltReservedNamespaceRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE SCHEMA rejects reserved dolt namespace",
			Assertions: []ScriptTestAssertion{
				{
					Query:       `CREATE SCHEMA dolt_reserved_schema;`,
					ExpectedErr: `invalid schema name`,
				},
			},
		},
	})
}

// TestCreateSchemaRequiresCreatePrivilegeGuard covers a sensitive privilege
// boundary: a normal login role must not create schemas without CREATE
// privilege on the database.
func TestCreateSchemaRequiresCreatePrivilegeGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE SCHEMA requires database CREATE privilege",
			SetUpScript: []string{
				`CREATE USER schema_creator PASSWORD 'creator';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `CREATE SCHEMA unauthorized_schema;`,
					ExpectedErr: `permission denied`,
					Username:    `schema_creator`,
					Password:    `creator`,
				},
			},
		},
	})
}

// TestRelationAccessRequiresSchemaUsageRepro reproduces a schema security bug:
// a table privilege should not bypass missing USAGE on the table's schema.
func TestRelationAccessRequiresSchemaUsageRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "table SELECT requires schema USAGE",
			SetUpScript: []string{
				`CREATE USER schema_usage_reader PASSWORD 'reader';`,
				`CREATE SCHEMA schema_usage_private;`,
				`CREATE TABLE schema_usage_private.items (id INT PRIMARY KEY);`,
				`INSERT INTO schema_usage_private.items VALUES (1);`,
				`GRANT SELECT ON schema_usage_private.items TO schema_usage_reader;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `SELECT id FROM schema_usage_private.items;`,
					ExpectedErr: `permission denied for schema`,
					Username:    `schema_usage_reader`,
					Password:    `reader`,
				},
			},
		},
	})
}

// TestMaterializedViewAccessRequiresSchemaUsageRepro reproduces a schema
// security bug: materialized-view privileges should not bypass missing USAGE on
// the containing schema.
func TestMaterializedViewAccessRequiresSchemaUsageRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "materialized view SELECT requires schema USAGE",
			SetUpScript: []string{
				`CREATE USER schema_view_user PASSWORD 'view';`,
				`CREATE SCHEMA schema_view_private;`,
				`CREATE TABLE schema_view_base (id INT PRIMARY KEY);`,
				`INSERT INTO schema_view_base VALUES (1);`,
				`CREATE MATERIALIZED VIEW schema_view_private.visible_matview AS
					SELECT id FROM schema_view_base;`,
				`GRANT SELECT ON schema_view_base TO schema_view_user;`,
				`GRANT SELECT ON schema_view_private.visible_matview TO schema_view_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `SELECT id FROM schema_view_private.visible_matview;`,
					ExpectedErr: `permission denied for schema`,
					Username:    `schema_view_user`,
					Password:    `view`,
				},
			},
		},
	})
}

// TestProcedureAndTypeAccessRequiresSchemaUsageRepro reproduces schema security
// bugs: procedure EXECUTE and type USAGE privileges should not bypass missing
// USAGE on the containing schema.
func TestProcedureAndTypeAccessRequiresSchemaUsageRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "procedure and type access requires schema USAGE",
			SetUpScript: []string{
				`CREATE USER schema_object_user PASSWORD 'object';`,
				`CREATE SCHEMA schema_object_private;`,
				`CREATE PROCEDURE schema_object_private.hidden_procedure()
					LANGUAGE SQL
					AS $$ SELECT 7 $$;`,
				`CREATE TYPE schema_object_private.hidden_type AS ENUM ('ok');`,
				`GRANT EXECUTE ON PROCEDURE schema_object_private.hidden_procedure()
					TO schema_object_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `CALL schema_object_private.hidden_procedure();`,
					ExpectedErr: `permission denied for schema`,
					Username:    `schema_object_user`,
					Password:    `object`,
				},
				{
					Query:       `SELECT 'ok'::schema_object_private.hidden_type::text;`,
					ExpectedErr: `permission denied for schema`,
					Username:    `schema_object_user`,
					Password:    `object`,
				},
			},
		},
	})
}

// TestCreateSchemaAuthorizationRequiresTargetRoleMembershipRepro reproduces a
// security bug: a normal role cannot create a schema owned by another role
// unless it is allowed to act as that role.
func TestCreateSchemaAuthorizationRequiresTargetRoleMembershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE SCHEMA AUTHORIZATION requires target role membership",
			SetUpScript: []string{
				`CREATE USER schema_auth_actor PASSWORD 'actor';`,
				`CREATE USER schema_auth_target PASSWORD 'target';`,
				`GRANT CREATE ON DATABASE postgres TO schema_auth_actor;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `CREATE SCHEMA unauthorized_schema_auth AUTHORIZATION schema_auth_target;`,
					ExpectedErr: `permission denied`,
					Username:    `schema_auth_actor`,
					Password:    `actor`,
				},
			},
		},
	})
}

// TestDropSchemaRequiresOwnershipGuard covers a sensitive privilege boundary:
// a normal login role must not drop schemas owned by another role.
func TestDropSchemaRequiresOwnershipGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP SCHEMA requires schema ownership",
			SetUpScript: []string{
				`CREATE USER schema_dropper PASSWORD 'dropper';`,
				`CREATE SCHEMA protected_schema;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DROP SCHEMA protected_schema;`,
					ExpectedErr: `permission denied`,
					Username:    `schema_dropper`,
					Password:    `dropper`,
				},
			},
		},
	})
}

// TestDropSchemaRequiresOwnershipDespiteAllPrivilegesGuard guards that GRANT
// ALL PRIVILEGES ON SCHEMA does not transfer ownership and does not allow the
// grantee to DROP the schema.
func TestDropSchemaRequiresOwnershipDespiteAllPrivilegesGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP SCHEMA requires ownership despite ALL PRIVILEGES",
			SetUpScript: []string{
				`CREATE USER drop_schema_intruder PASSWORD 'dropper';`,
				`CREATE SCHEMA protected_all_schema;`,
				`GRANT ALL PRIVILEGES ON SCHEMA protected_all_schema TO drop_schema_intruder;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DROP SCHEMA protected_all_schema;`,
					ExpectedErr: `permission denied`,
					Username:    `drop_schema_intruder`,
					Password:    `dropper`,
				},
				{
					Query:    `SELECT nspname FROM pg_namespace WHERE nspname = 'protected_all_schema';`,
					Expected: []sql.Row{{"protected_all_schema"}},
				},
			},
		},
	})
}
