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

// TestCreateTableForeignKeyRequiresReferencesPrivilegeRepro reproduces a
// security bug: Doltgres does not require REFERENCES privilege on the
// referenced table when creating a foreign key.
func TestCreateTableForeignKeyRequiresReferencesPrivilegeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE with foreign key requires REFERENCES on parent",
			SetUpScript: []string{
				`CREATE USER fk_creator PASSWORD 'creator';`,
				`CREATE TABLE fk_parent_private (id INT PRIMARY KEY);`,
				`GRANT USAGE, CREATE ON SCHEMA public TO fk_creator;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE fk_child_private (
						id INT PRIMARY KEY,
						parent_id INT REFERENCES fk_parent_private(id)
					);`,
					ExpectedErr: `permission denied`,
					Username:    `fk_creator`,
					Password:    `creator`,
				},
			},
		},
	})
}

// TestCreateTableForeignKeyRequiresReferencesOnReferencedColumnRepro reproduces
// a security bug: Doltgres ignores which parent columns were covered by a
// column-scoped REFERENCES grant when creating a foreign key.
func TestCreateTableForeignKeyRequiresReferencesOnReferencedColumnRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE with foreign key requires REFERENCES on referenced column",
			SetUpScript: []string{
				`CREATE USER fk_column_scope_creator PASSWORD 'creator';`,
				`CREATE TABLE fk_column_scope_parent_private (
					id INT PRIMARY KEY,
					other_id INT UNIQUE
				);`,
				`GRANT USAGE, CREATE ON SCHEMA public TO fk_column_scope_creator;`,
				`GRANT REFERENCES (other_id) ON fk_column_scope_parent_private TO fk_column_scope_creator;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE fk_column_scope_child_private (
						id INT PRIMARY KEY,
						parent_id INT REFERENCES fk_column_scope_parent_private(id)
					);`,
					ExpectedErr: `permission denied`,
					Username:    `fk_column_scope_creator`,
					Password:    `creator`,
				},
				{
					Query:    `SELECT to_regclass('fk_column_scope_child_private')::text;`,
					Expected: []sql.Row{{nil}},
				},
				{
					Query: `CREATE TABLE fk_column_scope_child_allowed (
						id INT PRIMARY KEY,
						parent_id INT REFERENCES fk_column_scope_parent_private(other_id)
					);`,
					Username: `fk_column_scope_creator`,
					Password: `creator`,
				},
				{
					Query:    `SELECT to_regclass('fk_column_scope_child_allowed')::text;`,
					Expected: []sql.Row{{"fk_column_scope_child_allowed"}},
				},
			},
		},
	})
}

// TestAlterTableAddForeignKeyRequiresReferencesPrivilegeRepro reproduces a
// security bug: Doltgres does not require REFERENCES privilege on the
// referenced table when adding a foreign key to an existing table.
func TestAlterTableAddForeignKeyRequiresReferencesPrivilegeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TABLE ADD FOREIGN KEY requires REFERENCES on parent",
			SetUpScript: []string{
				`CREATE USER alter_fk_creator PASSWORD 'creator';`,
				`CREATE TABLE alter_fk_parent_private (id INT PRIMARY KEY);`,
				`GRANT USAGE, CREATE ON SCHEMA public TO alter_fk_creator;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE alter_fk_child_private (
						id INT PRIMARY KEY,
						parent_id INT
					);`,
					Username: `alter_fk_creator`,
					Password: `creator`,
				},
				{
					Query: `ALTER TABLE alter_fk_child_private
						ADD CONSTRAINT alter_fk_child_private_parent_fk
						FOREIGN KEY (parent_id) REFERENCES alter_fk_parent_private(id);`,
					ExpectedErr: `permission denied`,
					Username:    `alter_fk_creator`,
					Password:    `creator`,
				},
			},
		},
	})
}

// TestAlterTableAddForeignKeyRequiresReferencesOnReferencedColumnRepro
// reproduces a security bug: Doltgres ignores which parent columns were
// covered by a column-scoped REFERENCES grant when adding a foreign key.
func TestAlterTableAddForeignKeyRequiresReferencesOnReferencedColumnRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TABLE ADD FOREIGN KEY requires REFERENCES on referenced column",
			SetUpScript: []string{
				`CREATE USER alter_fk_column_scope_creator PASSWORD 'creator';`,
				`CREATE TABLE alter_fk_column_scope_parent_private (
					id INT PRIMARY KEY,
					other_id INT UNIQUE
				);`,
				`GRANT USAGE, CREATE ON SCHEMA public TO alter_fk_column_scope_creator;`,
				`GRANT REFERENCES (other_id) ON alter_fk_column_scope_parent_private TO alter_fk_column_scope_creator;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE alter_fk_column_scope_child_private (
						id INT PRIMARY KEY,
						parent_id INT
					);`,
					Username: `alter_fk_column_scope_creator`,
					Password: `creator`,
				},
				{
					Query: `ALTER TABLE alter_fk_column_scope_child_private
						ADD CONSTRAINT alter_fk_column_scope_child_parent_fk
						FOREIGN KEY (parent_id) REFERENCES alter_fk_column_scope_parent_private(id);`,
					ExpectedErr: `permission denied`,
					Username:    `alter_fk_column_scope_creator`,
					Password:    `creator`,
				},
				{
					Query: `SELECT count(*)
						FROM information_schema.table_constraints
						WHERE table_name = 'alter_fk_column_scope_child_private'
							AND constraint_type = 'FOREIGN KEY';`,
					Expected: []sql.Row{{int64(0)}},
				},
				{
					Query: `ALTER TABLE alter_fk_column_scope_child_private
						ADD CONSTRAINT alter_fk_column_scope_child_parent_allowed_fk
						FOREIGN KEY (parent_id) REFERENCES alter_fk_column_scope_parent_private(other_id);`,
					Username: `alter_fk_column_scope_creator`,
					Password: `creator`,
				},
				{
					Query: `SELECT count(*)
						FROM information_schema.table_constraints
						WHERE table_name = 'alter_fk_column_scope_child_private'
							AND constraint_type = 'FOREIGN KEY';`,
					Expected: []sql.Row{{int64(1)}},
				},
			},
		},
	})
}

// TestAlterTableAddColumnReferencesRequiresReferencesPrivilegeRepro reproduces
// a security bug: Doltgres does not require REFERENCES privilege on the
// referenced table when adding a column with an inline foreign key.
func TestAlterTableAddColumnReferencesRequiresReferencesPrivilegeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TABLE ADD COLUMN REFERENCES requires REFERENCES on parent",
			SetUpScript: []string{
				`CREATE USER alter_column_fk_creator PASSWORD 'creator';`,
				`CREATE TABLE alter_column_fk_parent_private (id INT PRIMARY KEY);`,
				`GRANT USAGE, CREATE ON SCHEMA public TO alter_column_fk_creator;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE alter_column_fk_child_private (
						id INT PRIMARY KEY
					);`,
					Username: `alter_column_fk_creator`,
					Password: `creator`,
				},
				{
					Query: `ALTER TABLE alter_column_fk_child_private
						ADD COLUMN parent_id INT REFERENCES alter_column_fk_parent_private(id);`,
					ExpectedErr: `permission denied`,
					Username:    `alter_column_fk_creator`,
					Password:    `creator`,
				},
			},
		},
	})
}

// TestAlterTableAddColumnReferencesRequiresReferencesOnReferencedColumnRepro
// reproduces a security bug: Doltgres ignores which parent columns were
// covered by a column-scoped REFERENCES grant when adding a column with an
// inline foreign key.
func TestAlterTableAddColumnReferencesRequiresReferencesOnReferencedColumnRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TABLE ADD COLUMN REFERENCES requires REFERENCES on referenced column",
			SetUpScript: []string{
				`CREATE USER alter_column_fk_scope_creator PASSWORD 'creator';`,
				`CREATE TABLE alter_column_fk_scope_parent_private (
					id INT PRIMARY KEY,
					other_id INT UNIQUE
				);`,
				`GRANT USAGE, CREATE ON SCHEMA public TO alter_column_fk_scope_creator;`,
				`GRANT REFERENCES (other_id) ON alter_column_fk_scope_parent_private TO alter_column_fk_scope_creator;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE alter_column_fk_scope_child_private (
						id INT PRIMARY KEY
					);`,
					Username: `alter_column_fk_scope_creator`,
					Password: `creator`,
				},
				{
					Query: `ALTER TABLE alter_column_fk_scope_child_private
						ADD COLUMN parent_id INT REFERENCES alter_column_fk_scope_parent_private(id);`,
					ExpectedErr: `permission denied`,
					Username:    `alter_column_fk_scope_creator`,
					Password:    `creator`,
				},
				{
					Query: `SELECT count(*)
						FROM information_schema.columns
						WHERE table_name = 'alter_column_fk_scope_child_private'
							AND column_name = 'parent_id';`,
					Expected: []sql.Row{{int64(0)}},
				},
				{
					Query: `ALTER TABLE alter_column_fk_scope_child_private
						ADD COLUMN parent_other_id INT REFERENCES alter_column_fk_scope_parent_private(other_id);`,
					Username: `alter_column_fk_scope_creator`,
					Password: `creator`,
				},
				{
					Query: `SELECT count(*)
						FROM information_schema.columns
						WHERE table_name = 'alter_column_fk_scope_child_private'
							AND column_name = 'parent_other_id';`,
					Expected: []sql.Row{{int64(1)}},
				},
			},
		},
	})
}

// TestPublicSchemaDefaultCreateRequiresExplicitGrantGuard covers a PostgreSQL 15
// security default: PUBLIC should not have CREATE privilege on the public schema
// by default.
func TestPublicSchemaDefaultCreateRequiresExplicitGrantGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "public schema CREATE requires explicit grant",
			SetUpScript: []string{
				`CREATE USER public_schema_creator PASSWORD 'creator';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `CREATE TABLE public_schema_default_private (id INT PRIMARY KEY);`,
					ExpectedErr: `permission denied for schema public`,
					Username:    `public_schema_creator`,
					Password:    `creator`,
				},
			},
		},
	})
}

// TestCreateAggregateRequiresSchemaCreatePrivilegeGuard covers CREATE
// AGGREGATE authorization: the creator must have CREATE privilege on the target
// schema.
func TestCreateAggregateRequiresSchemaCreatePrivilegeGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE AGGREGATE requires schema CREATE privilege",
			SetUpScript: []string{
				`CREATE USER aggregate_creator PASSWORD 'creator';`,
				`CREATE FUNCTION create_aggregate_private_sfunc(state INT, next_value INT)
					RETURNS INT
					LANGUAGE SQL
					IMMUTABLE
					AS $$ SELECT COALESCE(state, 0) + COALESCE(next_value, 0) $$;`,
				`GRANT USAGE ON SCHEMA public TO aggregate_creator;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE AGGREGATE create_aggregate_private(INT) (
						SFUNC = create_aggregate_private_sfunc,
						STYPE = INT,
						INITCOND = '0'
					);`,
					ExpectedErr: `permission denied for schema public`,
					Username:    `aggregate_creator`,
					Password:    `creator`,
				},
			},
		},
	})
}

// TestCreateOperatorRequiresSchemaCreatePrivilegeRepro reproduces a security
// bug: Doltgres allows a role without CREATE privilege on the target schema to
// create an operator in that schema.
func TestCreateOperatorRequiresSchemaCreatePrivilegeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE OPERATOR requires schema CREATE privilege",
			SetUpScript: []string{
				`CREATE USER operator_creator PASSWORD 'creator';`,
				`CREATE FUNCTION create_operator_private_func(left_value INT, right_value INT)
					RETURNS BOOL
					LANGUAGE SQL
					IMMUTABLE
					AS $$ SELECT left_value = right_value $$;`,
				`GRANT USAGE ON SCHEMA public TO operator_creator;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE OPERATOR === (
						LEFTARG = INT,
						RIGHTARG = INT,
						PROCEDURE = create_operator_private_func
					);`,
					ExpectedErr: `permission denied for schema public`,
					Username:    `operator_creator`,
					Password:    `creator`,
				},
			},
		},
	})
}

// TestCreateOperatorRequiresFunctionExecutePrivilegeRepro reproduces a
// security bug: Doltgres allows a role without EXECUTE privilege on the
// underlying function to create an operator backed by it.
func TestCreateOperatorRequiresFunctionExecutePrivilegeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE OPERATOR requires EXECUTE on backing function",
			SetUpScript: []string{
				`CREATE USER operator_function_user PASSWORD 'creator';`,
				`CREATE FUNCTION create_operator_execute_private_func(left_value INT, right_value INT)
					RETURNS BOOL
					LANGUAGE SQL
					IMMUTABLE
					AS $$ SELECT left_value = right_value $$;`,
				`REVOKE ALL ON FUNCTION create_operator_execute_private_func(INT, INT) FROM PUBLIC;`,
				`GRANT USAGE, CREATE ON SCHEMA public TO operator_function_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE OPERATOR === (
						LEFTARG = INT,
						RIGHTARG = INT,
						PROCEDURE = create_operator_execute_private_func
					);`,
					ExpectedErr: `permission denied`,
					Username:    `operator_function_user`,
					Password:    `creator`,
				},
			},
		},
	})
}

// TestCreateCastRequiresTypeOwnershipRepro reproduces a security bug:
// Doltgres allows a role that owns neither the source nor target type to create
// a cast between them.
func TestCreateCastRequiresTypeOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE CAST requires source or target type ownership",
			SetUpScript: []string{
				`CREATE USER cast_creator PASSWORD 'creator';`,
				`CREATE TYPE cast_private_color AS ENUM ('red', 'green');`,
				`CREATE FUNCTION int_to_cast_private_color(input_value INT)
					RETURNS cast_private_color
					LANGUAGE SQL
					IMMUTABLE
					AS $$ SELECT CASE WHEN input_value = 1 THEN 'red'::cast_private_color ELSE 'green'::cast_private_color END $$;`,
				`GRANT USAGE ON SCHEMA public TO cast_creator;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE CAST (INT AS cast_private_color)
						WITH FUNCTION int_to_cast_private_color(INT);`,
					ExpectedErr: `must be owner`,
					Username:    `cast_creator`,
					Password:    `creator`,
				},
			},
		},
	})
}

// TestCreateCastRequiresFunctionExecutePrivilegeRepro reproduces a security
// bug: Doltgres allows a role without EXECUTE privilege on the cast function to
// create a cast backed by it.
func TestCreateCastRequiresFunctionExecutePrivilegeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE CAST requires EXECUTE on backing function",
			SetUpScript: []string{
				`CREATE USER cast_function_creator PASSWORD 'creator';`,
				`GRANT USAGE, CREATE ON SCHEMA public TO cast_function_creator;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `CREATE TYPE cast_execute_color AS ENUM ('red', 'green');`,
					Username: `cast_function_creator`,
					Password: `creator`,
				},
				{
					Query: `CREATE FUNCTION int_to_cast_execute_color(input_value INT)
						RETURNS cast_execute_color
						LANGUAGE SQL
						IMMUTABLE
						AS $$ SELECT CASE WHEN input_value = 1 THEN 'red'::cast_execute_color ELSE 'green'::cast_execute_color END $$;`,
				},
				{
					Query: `REVOKE ALL ON FUNCTION int_to_cast_execute_color(INT)
						FROM PUBLIC;`,
				},
				{
					Query: `CREATE CAST (INT AS cast_execute_color)
						WITH FUNCTION int_to_cast_execute_color(INT);`,
					ExpectedErr: `permission denied`,
					Username:    `cast_function_creator`,
					Password:    `creator`,
				},
			},
		},
	})
}

// TestDropCastRequiresTypeOwnershipRepro reproduces a security bug: Doltgres
// allows a role that owns neither side of a cast to drop it.
func TestDropCastRequiresTypeOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP CAST requires source or target type ownership",
			SetUpScript: []string{
				`CREATE USER cast_dropper PASSWORD 'dropper';`,
				`CREATE TYPE drop_cast_private_color AS ENUM ('red', 'green');`,
				`CREATE FUNCTION int_to_drop_cast_private_color(input_value INT)
					RETURNS drop_cast_private_color
					LANGUAGE SQL
					IMMUTABLE
					AS $$ SELECT CASE WHEN input_value = 1 THEN 'red'::drop_cast_private_color ELSE 'green'::drop_cast_private_color END $$;`,
				`CREATE CAST (INT AS drop_cast_private_color)
					WITH FUNCTION int_to_drop_cast_private_color(INT);`,
				`GRANT USAGE ON SCHEMA public TO cast_dropper;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DROP CAST (INT AS drop_cast_private_color);`,
					ExpectedErr: `must be owner`,
					Username:    `cast_dropper`,
					Password:    `dropper`,
				},
				{
					Query:    `SELECT ((1)::drop_cast_private_color)::text;`,
					Expected: []sql.Row{{"red"}},
				},
			},
		},
	})
}

// TestCreateTextSearchConfigurationRequiresSchemaCreatePrivilegeRepro
// reproduces a security bug: Doltgres allows a role without CREATE privilege on
// the target schema to create a text-search configuration in that schema.
func TestCreateTextSearchConfigurationRequiresSchemaCreatePrivilegeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TEXT SEARCH CONFIGURATION requires schema CREATE privilege",
			SetUpScript: []string{
				`CREATE USER ts_config_creator PASSWORD 'creator';`,
				`GRANT USAGE ON SCHEMA public TO ts_config_creator;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `CREATE TEXT SEARCH CONFIGURATION unauthorized_ts_config (COPY = pg_catalog.simple);`,
					ExpectedErr: `permission denied for schema public`,
					Username:    `ts_config_creator`,
					Password:    `creator`,
				},
			},
		},
	})
}

// TestCreateAccessMethodRequiresSuperuserRepro reproduces a security bug:
// Doltgres allows a non-superuser to define an access method.
func TestCreateAccessMethodRequiresSuperuserRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE ACCESS METHOD requires superuser",
			SetUpScript: []string{
				`CREATE USER access_method_creator PASSWORD 'creator';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `CREATE ACCESS METHOD unauthorized_table_am TYPE TABLE HANDLER heap_tableam_handler;`,
					ExpectedErr: `superuser`,
					Username:    `access_method_creator`,
					Password:    `creator`,
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_am
						WHERE amname = 'unauthorized_table_am';`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestDropAccessMethodRequiresSuperuserRepro reproduces a security bug:
// Doltgres allows a non-superuser to drop a user-defined access method.
func TestDropAccessMethodRequiresSuperuserRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP ACCESS METHOD requires superuser",
			SetUpScript: []string{
				`CREATE USER access_method_dropper PASSWORD 'dropper';`,
				`CREATE ACCESS METHOD private_drop_am TYPE TABLE HANDLER heap_tableam_handler;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DROP ACCESS METHOD private_drop_am;`,
					ExpectedErr: `superuser`,
					Username:    `access_method_dropper`,
					Password:    `dropper`,
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_am
						WHERE amname = 'private_drop_am';`,
					Expected: []sql.Row{{int64(1)}},
				},
			},
		},
	})
}

// TestCreateTableAsRequiresSelectOnSourceTableGuard covers CREATE TABLE AS
// authorization: the creator must have SELECT privileges on the source table.
func TestCreateTableAsRequiresSelectOnSourceTableGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE AS requires SELECT on source table",
			SetUpScript: []string{
				`CREATE USER ctas_user PASSWORD 'ctas';`,
				`CREATE TABLE ctas_source_private (id INT PRIMARY KEY, secret TEXT);`,
				`INSERT INTO ctas_source_private VALUES (1, 'secret');`,
				`GRANT USAGE, CREATE ON SCHEMA public TO ctas_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `CREATE TABLE ctas_leak AS SELECT id, secret FROM ctas_source_private;`,
					ExpectedErr: `permission denied`,
					Username:    `ctas_user`,
					Password:    `ctas`,
				},
			},
		},
	})
}

// TestGrantWithoutGrantOptionDoesNotErrorRepro reproduces a GRANT correctness
// bug: PostgreSQL permits a grantee without grant option to attempt delegation
// with a warning, but grants no privileges onward.
func TestGrantWithoutGrantOptionDoesNotErrorRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "GRANT without grant option does not delegate",
			SetUpScript: []string{
				`CREATE USER grant_option_plain_user PASSWORD 'plain';`,
				`CREATE USER grant_option_recipient PASSWORD 'recipient';`,
				`CREATE TABLE grant_option_private (
					id INT PRIMARY KEY,
					secret TEXT
				);`,
				`INSERT INTO grant_option_private VALUES (1, 'owner-only');`,
				`GRANT USAGE ON SCHEMA public TO grant_option_plain_user, grant_option_recipient;`,
				`GRANT SELECT ON grant_option_private TO grant_option_plain_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `GRANT SELECT ON grant_option_private TO grant_option_recipient;`,
					Username: `grant_option_plain_user`,
					Password: `plain`,
				},
				{
					Query:       `SELECT secret FROM grant_option_private;`,
					ExpectedErr: `permission denied`,
					Username:    `grant_option_recipient`,
					Password:    `recipient`,
				},
			},
		},
	})
}

// TestCreateTableAsRequiresSchemaCreatePrivilegeGuard covers target-schema
// authorization: CREATE TABLE AS must require CREATE on the schema where the
// durable result table is created.
func TestCreateTableAsRequiresSchemaCreatePrivilegeGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE AS requires CREATE on target schema",
			SetUpScript: []string{
				`CREATE USER ctas_schema_creator PASSWORD 'ctas';`,
				`CREATE SCHEMA ctas_private_schema;`,
				`CREATE TABLE ctas_schema_source (id INT PRIMARY KEY, label TEXT);`,
				`INSERT INTO ctas_schema_source VALUES (1, 'visible');`,
				`GRANT USAGE ON SCHEMA ctas_private_schema TO ctas_schema_creator;`,
				`GRANT SELECT ON ctas_schema_source TO ctas_schema_creator;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE ctas_private_schema.created_without_create AS
						SELECT id, label FROM ctas_schema_source;`,
					ExpectedErr: `permission denied`,
					Username:    `ctas_schema_creator`,
					Password:    `ctas`,
				},
				{
					Query:    `SELECT to_regclass('ctas_private_schema.created_without_create')::text;`,
					Expected: []sql.Row{{nil}},
				},
			},
		},
	})
}

// TestCreateTableLikeRequiresSchemaCreatePrivilegeGuard covers target-schema
// authorization: CREATE TABLE ... LIKE must require CREATE on the schema where
// the durable result table is created.
func TestCreateTableLikeRequiresSchemaCreatePrivilegeGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE LIKE requires CREATE on target schema",
			SetUpScript: []string{
				`CREATE USER like_schema_creator PASSWORD 'like';`,
				`CREATE SCHEMA like_private_schema;`,
				`CREATE TABLE like_schema_source (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`GRANT USAGE ON SCHEMA like_private_schema TO like_schema_creator;`,
				`GRANT SELECT ON like_schema_source TO like_schema_creator;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE like_private_schema.created_without_create (
						LIKE like_schema_source
					);`,
					ExpectedErr: `permission denied`,
					Username:    `like_schema_creator`,
					Password:    `like`,
				},
				{
					Query:    `SELECT to_regclass('like_private_schema.created_without_create')::text;`,
					Expected: []sql.Row{{nil}},
				},
			},
		},
	})
}

// TestCreateTypedTableRequiresSchemaCreatePrivilegeRepro reproduces a CREATE
// TABLE OF authorization bug: the typed-table path bypasses the target
// schema's CREATE privilege check.
func TestCreateTypedTableRequiresSchemaCreatePrivilegeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE OF requires CREATE on target schema",
			SetUpScript: []string{
				`CREATE USER typed_table_schema_creator PASSWORD 'typed';`,
				`CREATE SCHEMA typed_table_private_schema;`,
				`CREATE TYPE typed_table_private_row AS (
					id INT,
					label TEXT
				);`,
				`GRANT USAGE ON SCHEMA typed_table_private_schema TO typed_table_schema_creator;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `CREATE TABLE typed_table_private_schema.created_without_create OF typed_table_private_row;`,
					ExpectedErr: `permission denied`,
					Username:    `typed_table_schema_creator`,
					Password:    `typed`,
				},
				{
					Query:    `SELECT to_regclass('typed_table_private_schema.created_without_create')::text;`,
					Expected: []sql.Row{{nil}},
				},
			},
		},
	})
}

// TestSelectIntoCreatesTableRepro reproduces a DDL/query correctness gap:
// PostgreSQL SELECT ... INTO creates a table from the query result.
func TestSelectIntoCreatesTableRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "SELECT INTO creates a table from query results",
			SetUpScript: []string{
				`CREATE TABLE select_into_source (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO select_into_source VALUES (1, 'one'), (2, 'two');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, label
						INTO select_into_created
						FROM select_into_source
						WHERE id = 2;`,
				},
				{
					Query:    `SELECT id, label FROM select_into_created;`,
					Expected: []sql.Row{{2, "two"}},
				},
			},
		},
	})
}

// TestAlterTableAddColumnRequiresOwnershipRepro reproduces a security bug:
// Doltgres allows a role that does not own a table to add a column to it.
func TestAlterTableAddColumnRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TABLE ADD COLUMN requires table ownership",
			SetUpScript: []string{
				`CREATE USER table_column_adder PASSWORD 'adder';`,
				`CREATE TABLE alter_add_column_private (id INT PRIMARY KEY);`,
				`GRANT USAGE ON SCHEMA public TO table_column_adder;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `ALTER TABLE alter_add_column_private ADD COLUMN label TEXT;`,
					ExpectedErr: `permission denied`,
					Username:    `table_column_adder`,
					Password:    `adder`,
				},
			},
		},
	})
}

// TestAlterTableDropColumnRequiresOwnershipRepro reproduces a security bug:
// Doltgres allows a role that does not own a table to drop a column from it.
func TestAlterTableDropColumnRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TABLE DROP COLUMN requires table ownership",
			SetUpScript: []string{
				`CREATE USER table_column_dropper PASSWORD 'dropper';`,
				`CREATE TABLE alter_drop_column_private (id INT PRIMARY KEY, label TEXT);`,
				`GRANT USAGE ON SCHEMA public TO table_column_dropper;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `ALTER TABLE alter_drop_column_private DROP COLUMN label;`,
					ExpectedErr: `permission denied`,
					Username:    `table_column_dropper`,
					Password:    `dropper`,
				},
			},
		},
	})
}

// TestAlterTableAlterColumnTypeRequiresOwnershipRepro reproduces a security
// bug: Doltgres allows a role that does not own a table to change a column's
// stored type.
func TestAlterTableAlterColumnTypeRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TABLE ALTER COLUMN TYPE requires table ownership",
			SetUpScript: []string{
				`CREATE USER table_column_type_alterer PASSWORD 'alterer';`,
				`CREATE TABLE alter_column_type_private (
					id INT PRIMARY KEY,
					amount INT
				);`,
				`GRANT USAGE ON SCHEMA public TO table_column_type_alterer;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE alter_column_type_private
						ALTER COLUMN amount TYPE BIGINT;`,
					ExpectedErr: `permission denied`,
					Username:    `table_column_type_alterer`,
					Password:    `alterer`,
				},
				{
					Query: `SELECT data_type
						FROM information_schema.columns
						WHERE table_name = 'alter_column_type_private'
							AND column_name = 'amount';`,
					Expected: []sql.Row{{"integer"}},
				},
			},
		},
	})
}

// TestAlterTableAlterColumnSetDefaultRequiresOwnershipRepro reproduces a
// security bug: Doltgres allows a role that does not own a table to change a
// column default.
func TestAlterTableAlterColumnSetDefaultRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TABLE ALTER COLUMN SET DEFAULT requires table ownership",
			SetUpScript: []string{
				`CREATE USER table_column_default_alterer PASSWORD 'alterer';`,
				`CREATE TABLE alter_column_default_private (
					id INT PRIMARY KEY,
					amount INT
				);`,
				`GRANT USAGE ON SCHEMA public TO table_column_default_alterer;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE alter_column_default_private
						ALTER COLUMN amount SET DEFAULT 7;`,
					ExpectedErr: `permission denied`,
					Username:    `table_column_default_alterer`,
					Password:    `alterer`,
				},
				{
					Query: `INSERT INTO alter_column_default_private (id) VALUES (1);`,
				},
				{
					Query:    `SELECT id, amount FROM alter_column_default_private;`,
					Expected: []sql.Row{{1, nil}},
				},
			},
		},
	})
}

// TestAlterTableAlterColumnDropDefaultRequiresOwnershipRepro reproduces a
// security bug: Doltgres allows a role that does not own a table to remove a
// column default.
func TestAlterTableAlterColumnDropDefaultRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TABLE ALTER COLUMN DROP DEFAULT requires table ownership",
			SetUpScript: []string{
				`CREATE USER table_column_default_dropper PASSWORD 'dropper';`,
				`CREATE TABLE alter_column_drop_default_private (
					id INT PRIMARY KEY,
					amount INT DEFAULT 7
				);`,
				`GRANT USAGE ON SCHEMA public TO table_column_default_dropper;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE alter_column_drop_default_private
						ALTER COLUMN amount DROP DEFAULT;`,
					ExpectedErr: `permission denied`,
					Username:    `table_column_default_dropper`,
					Password:    `dropper`,
				},
				{
					Query: `INSERT INTO alter_column_drop_default_private (id) VALUES (1);`,
				},
				{
					Query:    `SELECT id, amount FROM alter_column_drop_default_private;`,
					Expected: []sql.Row{{1, 7}},
				},
			},
		},
	})
}

// TestAlterTableAlterColumnSetNotNullRequiresOwnershipRepro reproduces a
// security bug: Doltgres allows a role that does not own a table to tighten a
// column's nullability.
func TestAlterTableAlterColumnSetNotNullRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TABLE ALTER COLUMN SET NOT NULL requires table ownership",
			SetUpScript: []string{
				`CREATE USER table_column_not_null_alterer PASSWORD 'alterer';`,
				`CREATE TABLE alter_column_not_null_private (
					id INT PRIMARY KEY,
					amount INT
				);`,
				`GRANT USAGE ON SCHEMA public TO table_column_not_null_alterer;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE alter_column_not_null_private
						ALTER COLUMN amount SET NOT NULL;`,
					ExpectedErr: `permission denied`,
					Username:    `table_column_not_null_alterer`,
					Password:    `alterer`,
				},
				{
					Query: `INSERT INTO alter_column_not_null_private VALUES (1, NULL);`,
				},
				{
					Query:    `SELECT id, amount FROM alter_column_not_null_private;`,
					Expected: []sql.Row{{1, nil}},
				},
			},
		},
	})
}

// TestAlterTableAlterColumnDropNotNullRequiresOwnershipRepro reproduces a
// security bug: Doltgres allows a role that does not own a table to loosen a
// column's nullability.
func TestAlterTableAlterColumnDropNotNullRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TABLE ALTER COLUMN DROP NOT NULL requires table ownership",
			SetUpScript: []string{
				`CREATE USER table_column_not_null_dropper PASSWORD 'dropper';`,
				`CREATE TABLE alter_column_drop_not_null_private (
					id INT PRIMARY KEY,
					amount INT NOT NULL
				);`,
				`GRANT USAGE ON SCHEMA public TO table_column_not_null_dropper;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE alter_column_drop_not_null_private
						ALTER COLUMN amount DROP NOT NULL;`,
					ExpectedErr: `permission denied`,
					Username:    `table_column_not_null_dropper`,
					Password:    `dropper`,
				},
				{
					Query:       `INSERT INTO alter_column_drop_not_null_private VALUES (1, NULL);`,
					ExpectedErr: `non-nullable`,
				},
				{
					Query:    `SELECT count(*) FROM alter_column_drop_not_null_private;`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
	})
}

// TestTruncateRequiresTablePrivilegeGuard guards destructive table-operation
// authorization for TRUNCATE.
func TestTruncateRequiresTablePrivilegeGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "TRUNCATE requires table privileges",
			SetUpScript: []string{
				`CREATE USER table_truncater PASSWORD 'truncater';`,
				`CREATE TABLE truncate_private (id INT PRIMARY KEY);`,
				`INSERT INTO truncate_private VALUES (1);`,
				`GRANT USAGE ON SCHEMA public TO table_truncater;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `TRUNCATE truncate_private;`,
					ExpectedErr: `permission denied`,
					Username:    `table_truncater`,
					Password:    `truncater`,
				},
				{
					Query:    `SELECT COUNT(*) FROM truncate_private;`,
					Expected: []sql.Row{{1}},
				},
			},
		},
	})
}

// TestAlterTableRenameColumnRequiresOwnershipRepro reproduces a security bug:
// Doltgres allows a role that does not own a table to rename one of its
// columns.
func TestAlterTableRenameColumnRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TABLE RENAME COLUMN requires table ownership",
			SetUpScript: []string{
				`CREATE USER table_column_renamer PASSWORD 'renamer';`,
				`CREATE TABLE alter_rename_column_private (id INT PRIMARY KEY, label TEXT);`,
				`GRANT USAGE ON SCHEMA public TO table_column_renamer;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `ALTER TABLE alter_rename_column_private RENAME COLUMN label TO renamed_label;`,
					ExpectedErr: `permission denied`,
					Username:    `table_column_renamer`,
					Password:    `renamer`,
				},
			},
		},
	})
}

// TestAlterTableAddConstraintRequiresOwnershipRepro reproduces a security bug:
// Doltgres allows a role that does not own a table to add a constraint to it.
func TestAlterTableAddConstraintRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TABLE ADD CONSTRAINT requires table ownership",
			SetUpScript: []string{
				`CREATE USER table_constraint_adder PASSWORD 'adder';`,
				`CREATE TABLE alter_add_constraint_private (id INT PRIMARY KEY, value INT);`,
				`GRANT USAGE ON SCHEMA public TO table_constraint_adder;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `ALTER TABLE alter_add_constraint_private ADD CONSTRAINT value_positive CHECK (value > 0);`,
					ExpectedErr: `permission denied`,
					Username:    `table_constraint_adder`,
					Password:    `adder`,
				},
			},
		},
	})
}

// TestAlterTableDropConstraintRequiresOwnershipRepro reproduces a security
// bug: Doltgres allows a role that does not own a table to drop a constraint
// from it.
func TestAlterTableDropConstraintRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TABLE DROP CONSTRAINT requires table ownership",
			SetUpScript: []string{
				`CREATE USER table_constraint_dropper PASSWORD 'dropper';`,
				`CREATE TABLE alter_drop_constraint_private (
					id INT PRIMARY KEY,
					value INT CONSTRAINT value_positive CHECK (value > 0)
				);`,
				`GRANT USAGE ON SCHEMA public TO table_constraint_dropper;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `ALTER TABLE alter_drop_constraint_private DROP CONSTRAINT value_positive;`,
					ExpectedErr: `permission denied`,
					Username:    `table_constraint_dropper`,
					Password:    `dropper`,
				},
			},
		},
	})
}

// TestAlterTableReplicaIdentityRequiresOwnershipRepro reproduces a security bug:
// Doltgres allows a role that does not own a table to change its logical
// replication identity.
func TestAlterTableReplicaIdentityRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TABLE REPLICA IDENTITY requires table ownership",
			SetUpScript: []string{
				`CREATE USER table_replica_identity_editor PASSWORD 'editor';`,
				`CREATE TABLE replica_identity_owner_private (
					id INT PRIMARY KEY,
					label TEXT NOT NULL
				);`,
				`GRANT USAGE ON SCHEMA public TO table_replica_identity_editor;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `ALTER TABLE replica_identity_owner_private REPLICA IDENTITY FULL;`,
					ExpectedErr: `must be owner`,
					Username:    `table_replica_identity_editor`,
					Password:    `editor`,
				},
				{
					Query: `SELECT relreplident
						FROM pg_catalog.pg_class
						WHERE relname = 'replica_identity_owner_private';`,
					Expected: []sql.Row{{"d"}},
				},
			},
		},
	})
}

// TestAlterTableRowLevelSecurityRequiresOwnershipRepro reproduces a security
// bug: PostgreSQL only allows a table owner to change row-level security modes.
func TestAlterTableRowLevelSecurityRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TABLE row-level security requires table ownership",
			SetUpScript: []string{
				`CREATE USER rls_mode_editor PASSWORD 'editor';`,
				`CREATE TABLE rls_mode_owner_private (
					id INT PRIMARY KEY
				);`,
				`GRANT USAGE ON SCHEMA public TO rls_mode_editor;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `ALTER TABLE rls_mode_owner_private ENABLE ROW LEVEL SECURITY;`,
					ExpectedErr: `must be owner`,
					Username:    `rls_mode_editor`,
					Password:    `editor`,
				},
				{
					Query: `SELECT relrowsecurity
						FROM pg_catalog.pg_class
						WHERE oid = 'rls_mode_owner_private'::regclass;`,
					Expected: []sql.Row{{"f"}},
				},
			},
		},
	})
}

// TestCreatePolicyRequiresTableOwnershipRepro reproduces a security bug:
// Doltgres allows a role that does not own a table to create a row-level
// security policy on it.
func TestCreatePolicyRequiresTableOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE POLICY requires table ownership",
			SetUpScript: []string{
				`CREATE USER policy_creator PASSWORD 'creator';`,
				`CREATE TABLE policy_private_docs (
					id INT PRIMARY KEY,
					owner_name TEXT
				);`,
				`GRANT USAGE ON SCHEMA public TO policy_creator;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE POLICY policy_private_docs_select
						ON policy_private_docs
						FOR SELECT
						USING (owner_name = current_user);`,
					ExpectedErr: `must be owner`,
					Username:    `policy_creator`,
					Password:    `creator`,
				},
			},
		},
	})
}

// TestCreateRuleRequiresTableOwnershipRepro reproduces a security bug:
// Doltgres allows a role that does not own a table to create a rewrite rule on
// it when the role has enough privileges for Doltgres' trigger-based rewrite.
func TestCreateRuleRequiresTableOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE RULE requires table ownership",
			SetUpScript: []string{
				`CREATE USER rule_creator PASSWORD 'creator';`,
				`CREATE TABLE rule_private_source (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`CREATE TABLE rule_private_audit (
					source_id INT,
					label TEXT
				);`,
				`GRANT USAGE, CREATE ON SCHEMA public TO rule_creator;`,
				`GRANT TRIGGER ON rule_private_source TO rule_creator;`,
				`GRANT INSERT ON rule_private_audit TO rule_creator;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE RULE rule_private_source_audit AS
						ON INSERT TO rule_private_source
						DO ALSO
						INSERT INTO rule_private_audit VALUES (NEW.id, NEW.label);`,
					ExpectedErr: `must be owner`,
					Username:    `rule_creator`,
					Password:    `creator`,
				},
			},
		},
	})
}

// TestRenameTableRequiresOwnershipRepro reproduces a security bug: Doltgres
// allows a role that does not own a table to rename it.
func TestRenameTableRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TABLE RENAME TO requires table ownership",
			SetUpScript: []string{
				`CREATE USER table_renamer PASSWORD 'renamer';`,
				`CREATE TABLE rename_table_private (id INT PRIMARY KEY);`,
				`GRANT USAGE ON SCHEMA public TO table_renamer;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `ALTER TABLE rename_table_private RENAME TO rename_table_private_new;`,
					ExpectedErr: `permission denied`,
					Username:    `table_renamer`,
					Password:    `renamer`,
				},
			},
		},
	})
}

// TestAlterTableOwnerToRequiresOwnershipRepro reproduces a security bug:
// Doltgres allows a role that does not own a table to transfer its ownership.
func TestAlterTableOwnerToRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TABLE OWNER TO requires table ownership",
			SetUpScript: []string{
				`CREATE USER table_owner_hijacker PASSWORD 'hijacker';`,
				`CREATE TABLE owner_to_table_private (id INT PRIMARY KEY);`,
				`GRANT USAGE ON SCHEMA public TO table_owner_hijacker;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `ALTER TABLE owner_to_table_private OWNER TO table_owner_hijacker;`,
					ExpectedErr: `must be owner`,
					Username:    `table_owner_hijacker`,
					Password:    `hijacker`,
				},
				{
					Query: `SELECT pg_get_userbyid(relowner)
						FROM pg_catalog.pg_class
						WHERE oid = 'owner_to_table_private'::regclass;`,
					Expected: []sql.Row{{"postgres"}},
				},
			},
		},
	})
}

// TestAlterViewOwnerToRequiresOwnershipRepro reproduces a security bug:
// Doltgres allows a role that does not own a view to transfer its ownership.
func TestAlterViewOwnerToRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER VIEW OWNER TO requires view ownership",
			SetUpScript: []string{
				`CREATE USER view_owner_hijacker PASSWORD 'hijacker';`,
				`CREATE TABLE owner_to_view_base (id INT PRIMARY KEY);`,
				`CREATE VIEW owner_to_view_private AS
					SELECT id FROM owner_to_view_base;`,
				`GRANT USAGE ON SCHEMA public TO view_owner_hijacker;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `ALTER VIEW owner_to_view_private OWNER TO view_owner_hijacker;`,
					ExpectedErr: `must be owner`,
					Username:    `view_owner_hijacker`,
					Password:    `hijacker`,
				},
				{
					Query: `SELECT pg_get_userbyid(relowner)
						FROM pg_catalog.pg_class
						WHERE oid = 'owner_to_view_private'::regclass;`,
					Expected: []sql.Row{{"postgres"}},
				},
			},
		},
	})
}

// TestCreateTriggerRequiresTriggerPrivilegeRepro reproduces a security bug:
// Doltgres accepts CREATE TRIGGER from a role that has EXECUTE on the trigger
// function but no TRIGGER privilege on the target table.
func TestCreateTriggerRequiresTriggerPrivilegeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TRIGGER requires TRIGGER privilege on target table",
			SetUpScript: []string{
				`CREATE USER trigger_creator PASSWORD 'creator';`,
				`CREATE TABLE trigger_private (id INT PRIMARY KEY, label TEXT);`,
				`CREATE FUNCTION trigger_noop() RETURNS TRIGGER AS $$
					BEGIN
						RETURN NEW;
					END;
				$$ LANGUAGE plpgsql;`,
				`GRANT USAGE ON SCHEMA public TO trigger_creator;`,
				`GRANT EXECUTE ON FUNCTION trigger_noop() TO trigger_creator;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TRIGGER trigger_private_before_insert
						BEFORE INSERT ON trigger_private
						FOR EACH ROW
						EXECUTE FUNCTION trigger_noop();`,
					ExpectedErr: `permission denied`,
					Username:    `trigger_creator`,
					Password:    `creator`,
				},
			},
		},
	})
}

// TestDropTriggerRequiresTableOwnershipRepro reproduces a security bug:
// Doltgres allows a role that does not own the target table to drop its
// trigger.
func TestDropTriggerRequiresTableOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP TRIGGER requires ownership of target table",
			SetUpScript: []string{
				`CREATE USER trigger_dropper PASSWORD 'dropper';`,
				`CREATE TABLE drop_trigger_private (id INT PRIMARY KEY, label TEXT);`,
				`CREATE FUNCTION drop_trigger_noop() RETURNS TRIGGER AS $$
					BEGIN
						RETURN NEW;
					END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER drop_trigger_private_before_insert
					BEFORE INSERT ON drop_trigger_private
					FOR EACH ROW
					EXECUTE FUNCTION drop_trigger_noop();`,
				`GRANT USAGE ON SCHEMA public TO trigger_dropper;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DROP TRIGGER drop_trigger_private_before_insert ON drop_trigger_private;`,
					ExpectedErr: `permission denied`,
					Username:    `trigger_dropper`,
					Password:    `dropper`,
				},
			},
		},
	})
}

// TestDropFunctionRequiresOwnershipRepro reproduces a security bug: Doltgres
// allows a role that does not own a function to drop it.
func TestDropFunctionRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP FUNCTION requires function ownership",
			SetUpScript: []string{
				`CREATE USER function_dropper PASSWORD 'dropper';`,
				`CREATE FUNCTION drop_function_private() RETURNS INT
					LANGUAGE SQL
					AS $$ SELECT 1 $$;`,
				`GRANT USAGE ON SCHEMA public TO function_dropper;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DROP FUNCTION drop_function_private();`,
					ExpectedErr: `permission denied`,
					Username:    `function_dropper`,
					Password:    `dropper`,
				},
			},
		},
	})
}

// TestDropFunctionRequiresOwnershipDespiteAllPrivilegesRepro reproduces a
// PostgreSQL authorization bug: GRANT ALL PRIVILEGES ON FUNCTION does not
// transfer ownership and should not allow the grantee to DROP the function.
func TestDropFunctionRequiresOwnershipDespiteAllPrivilegesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP FUNCTION requires ownership despite ALL PRIVILEGES",
			SetUpScript: []string{
				`CREATE USER drop_function_intruder PASSWORD 'dropper';`,
				`CREATE FUNCTION drop_function_all_private() RETURNS INT
					LANGUAGE SQL
					AS $$ SELECT 1 $$;`,
				`GRANT USAGE ON SCHEMA public TO drop_function_intruder;`,
				`GRANT ALL PRIVILEGES ON FUNCTION drop_function_all_private() TO drop_function_intruder;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DROP FUNCTION drop_function_all_private();`,
					ExpectedErr: `must be owner`,
					Username:    `drop_function_intruder`,
					Password:    `dropper`,
				},
				{
					Query:    `SELECT drop_function_all_private();`,
					Expected: []sql.Row{{int32(1)}},
				},
			},
		},
	})
}

// TestDropAggregateRequiresOwnershipRepro reproduces a security bug: Doltgres
// allows a role that does not own an aggregate to drop it.
func TestDropAggregateRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP AGGREGATE requires aggregate ownership",
			SetUpScript: []string{
				`CREATE USER aggregate_dropper PASSWORD 'dropper';`,
				`CREATE FUNCTION drop_aggregate_private_sfunc(state INT, next_value INT)
					RETURNS INT
					LANGUAGE SQL
					IMMUTABLE
					AS $$ SELECT COALESCE(state, 0) + COALESCE(next_value, 0) $$;`,
				`CREATE AGGREGATE drop_aggregate_private(INT) (
					SFUNC = drop_aggregate_private_sfunc,
					STYPE = INT,
					INITCOND = '0'
				);`,
				`GRANT USAGE ON SCHEMA public TO aggregate_dropper;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DROP AGGREGATE drop_aggregate_private(INT);`,
					ExpectedErr: `permission denied`,
					Username:    `aggregate_dropper`,
					Password:    `dropper`,
				},
			},
		},
	})
}

// TestCreateOrReplaceAggregateRequiresOwnershipRepro reproduces a security
// bug: Doltgres allows a role that does not own an existing aggregate to
// replace it.
func TestCreateOrReplaceAggregateRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE OR REPLACE AGGREGATE requires aggregate ownership",
			SetUpScript: []string{
				`CREATE USER aggregate_replacer PASSWORD 'replacer';`,
				`CREATE FUNCTION replace_aggregate_private_sfunc(state INT, next_value INT)
					RETURNS INT
					LANGUAGE SQL
					IMMUTABLE
					AS $$ SELECT COALESCE(state, 0) + COALESCE(next_value, 0) $$;`,
				`CREATE AGGREGATE replace_aggregate_private(INT) (
					SFUNC = replace_aggregate_private_sfunc,
					STYPE = INT,
					INITCOND = '0'
				);`,
				`GRANT USAGE, CREATE ON SCHEMA public TO aggregate_replacer;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE OR REPLACE AGGREGATE replace_aggregate_private(INT) (
						SFUNC = replace_aggregate_private_sfunc,
						STYPE = INT,
						INITCOND = '1'
					);`,
					ExpectedErr: `permission denied`,
					Username:    `aggregate_replacer`,
					Password:    `replacer`,
				},
			},
		},
	})
}

// TestAlterAggregateOwnerToRequiresOwnershipRepro reproduces a security bug:
// Doltgres allows a role that does not own an aggregate to transfer ownership.
func TestAlterAggregateOwnerToRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER AGGREGATE OWNER TO requires aggregate ownership",
			SetUpScript: []string{
				`CREATE USER aggregate_owner_to_hijacker PASSWORD 'hijacker';`,
				`CREATE FUNCTION owner_to_aggregate_private_sfunc(state INT, next_value INT)
					RETURNS INT
					LANGUAGE SQL
					IMMUTABLE
					AS $$ SELECT COALESCE(state, 0) + COALESCE(next_value, 0) $$;`,
				`CREATE AGGREGATE owner_to_aggregate_private(INT) (
					SFUNC = owner_to_aggregate_private_sfunc,
					STYPE = INT,
					INITCOND = '0'
				);`,
				`GRANT USAGE ON SCHEMA public TO aggregate_owner_to_hijacker;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `ALTER AGGREGATE owner_to_aggregate_private(INT) OWNER TO aggregate_owner_to_hijacker;`,
					ExpectedErr: `must be owner`,
					Username:    `aggregate_owner_to_hijacker`,
					Password:    `hijacker`,
				},
				{
					Query: `SELECT pg_get_userbyid(proowner)
						FROM pg_catalog.pg_proc
						WHERE proname = 'owner_to_aggregate_private';`,
					Expected: []sql.Row{{"postgres"}},
				},
			},
		},
	})
}

// TestAlterAggregateRenameRequiresOwnershipRepro reproduces a security bug:
// Doltgres allows a role that does not own an aggregate to rename it.
func TestAlterAggregateRenameRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER AGGREGATE RENAME TO requires aggregate ownership",
			SetUpScript: []string{
				`CREATE USER aggregate_renamer PASSWORD 'renamer';`,
				`CREATE FUNCTION rename_aggregate_private_sfunc(state INT, next_value INT)
					RETURNS INT
					LANGUAGE SQL
					IMMUTABLE
					AS $$ SELECT COALESCE(state, 0) + COALESCE(next_value, 0) $$;`,
				`CREATE AGGREGATE rename_aggregate_private_old(INT) (
					SFUNC = rename_aggregate_private_sfunc,
					STYPE = INT,
					INITCOND = '0'
				);`,
				`GRANT USAGE ON SCHEMA public TO aggregate_renamer;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `ALTER AGGREGATE rename_aggregate_private_old(INT) RENAME TO rename_aggregate_private_new;`,
					ExpectedErr: `must be owner`,
					Username:    `aggregate_renamer`,
					Password:    `renamer`,
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_proc
						WHERE proname = 'rename_aggregate_private_old';`,
					Expected: []sql.Row{{int64(1)}},
				},
			},
		},
	})
}

// TestAlterAggregateSetSchemaRequiresOwnershipRepro reproduces a security bug:
// Doltgres allows a role that does not own an aggregate to move it to another
// schema.
func TestAlterAggregateSetSchemaRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER AGGREGATE SET SCHEMA requires aggregate ownership",
			SetUpScript: []string{
				`CREATE USER aggregate_schema_mover PASSWORD 'mover';`,
				`CREATE SCHEMA aggregate_private_target;`,
				`CREATE FUNCTION set_schema_aggregate_private_sfunc(state INT, next_value INT)
					RETURNS INT
					LANGUAGE SQL
					IMMUTABLE
					AS $$ SELECT COALESCE(state, 0) + COALESCE(next_value, 0) $$;`,
				`CREATE AGGREGATE set_schema_aggregate_private(INT) (
					SFUNC = set_schema_aggregate_private_sfunc,
					STYPE = INT,
					INITCOND = '0'
				);`,
				`GRANT USAGE ON SCHEMA public TO aggregate_schema_mover;`,
				`GRANT USAGE ON SCHEMA aggregate_private_target TO aggregate_schema_mover;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `ALTER AGGREGATE set_schema_aggregate_private(INT) SET SCHEMA aggregate_private_target;`,
					ExpectedErr: `must be owner`,
					Username:    `aggregate_schema_mover`,
					Password:    `mover`,
				},
				{
					Query: `SELECT n.nspname
						FROM pg_catalog.pg_proc p
						JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
						WHERE p.proname = 'set_schema_aggregate_private';`,
					Expected: []sql.Row{{"public"}},
				},
			},
		},
	})
}

// TestAlterFunctionOwnerToRequiresOwnershipRepro reproduces a security bug:
// Doltgres allows a role that does not own a function to transfer ownership.
func TestAlterFunctionOwnerToRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER FUNCTION OWNER TO requires function ownership",
			SetUpScript: []string{
				`CREATE USER function_owner_to_hijacker PASSWORD 'hijacker';`,
				`CREATE FUNCTION owner_to_function_private() RETURNS INT
					LANGUAGE SQL
					AS $$ SELECT 1 $$;`,
				`GRANT USAGE ON SCHEMA public TO function_owner_to_hijacker;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `ALTER FUNCTION owner_to_function_private() OWNER TO function_owner_to_hijacker;`,
					ExpectedErr: `must be owner`,
					Username:    `function_owner_to_hijacker`,
					Password:    `hijacker`,
				},
				{
					Query: `SELECT pg_get_userbyid(proowner)
						FROM pg_catalog.pg_proc
						WHERE proname = 'owner_to_function_private';`,
					Expected: []sql.Row{{"postgres"}},
				},
			},
		},
	})
}

// TestCreateOrReplaceFunctionRequiresOwnershipRepro reproduces a security bug:
// Doltgres allows a role that does not own an existing function to replace it.
func TestCreateOrReplaceFunctionRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE OR REPLACE FUNCTION requires function ownership",
			SetUpScript: []string{
				`CREATE USER function_replacer PASSWORD 'replacer';`,
				`CREATE FUNCTION replace_function_private() RETURNS INT
					LANGUAGE SQL
					AS $$ SELECT 1 $$;`,
				`GRANT USAGE, CREATE ON SCHEMA public TO function_replacer;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE OR REPLACE FUNCTION replace_function_private() RETURNS INT
						LANGUAGE SQL
						AS $$ SELECT 2 $$;`,
					ExpectedErr: `permission denied`,
					Username:    `function_replacer`,
					Password:    `replacer`,
				},
			},
		},
	})
}

// TestDropProcedureRequiresOwnershipRepro reproduces a security bug: Doltgres
// allows a role that does not own a procedure to drop it.
func TestDropProcedureRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP PROCEDURE requires procedure ownership",
			SetUpScript: []string{
				`CREATE USER procedure_dropper PASSWORD 'dropper';`,
				`CREATE PROCEDURE drop_procedure_private()
					AS $$
					BEGIN
						NULL;
					END;
					$$ LANGUAGE plpgsql;`,
				`GRANT USAGE ON SCHEMA public TO procedure_dropper;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DROP PROCEDURE drop_procedure_private();`,
					ExpectedErr: `permission denied`,
					Username:    `procedure_dropper`,
					Password:    `dropper`,
				},
			},
		},
	})
}

// TestAlterProcedureOwnerToRequiresOwnershipRepro reproduces a security bug:
// Doltgres allows a role that does not own a procedure to transfer ownership.
func TestAlterProcedureOwnerToRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER PROCEDURE OWNER TO requires procedure ownership",
			SetUpScript: []string{
				`CREATE USER procedure_owner_to_hijacker PASSWORD 'hijacker';`,
				`CREATE PROCEDURE owner_to_procedure_private()
					LANGUAGE SQL
					AS $$ SELECT 1 $$;`,
				`GRANT USAGE ON SCHEMA public TO procedure_owner_to_hijacker;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `ALTER PROCEDURE owner_to_procedure_private() OWNER TO procedure_owner_to_hijacker;`,
					ExpectedErr: `must be owner`,
					Username:    `procedure_owner_to_hijacker`,
					Password:    `hijacker`,
				},
				{
					Query: `SELECT pg_get_userbyid(proowner)
						FROM pg_catalog.pg_proc
						WHERE proname = 'owner_to_procedure_private';`,
					Expected: []sql.Row{{"postgres"}},
				},
			},
		},
	})
}

// TestCreateOrReplaceProcedureRequiresOwnershipRepro reproduces a security
// bug: Doltgres allows a role that does not own an existing procedure to
// replace it.
func TestCreateOrReplaceProcedureRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE OR REPLACE PROCEDURE requires procedure ownership",
			SetUpScript: []string{
				`CREATE USER procedure_replacer PASSWORD 'replacer';`,
				`CREATE PROCEDURE replace_procedure_private()
					LANGUAGE SQL
					AS $$ SELECT 1 $$;`,
				`GRANT USAGE, CREATE ON SCHEMA public TO procedure_replacer;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE OR REPLACE PROCEDURE replace_procedure_private()
						LANGUAGE SQL
						AS $$ SELECT 2 $$;`,
					ExpectedErr: `permission denied`,
					Username:    `procedure_replacer`,
					Password:    `replacer`,
				},
			},
		},
	})
}

// TestCreateTypeAndDomainRequireSchemaCreatePrivilegeRepro reproduces schema
// security bugs: creating types and domains should require CREATE on the target
// schema. The adjacent function, procedure, and sequence checks guard object
// classes that already enforce the boundary.
func TestCreateTypeAndDomainRequireSchemaCreatePrivilegeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE schema objects requires CREATE on target schema",
			SetUpScript: []string{
				`CREATE USER schema_object_creator PASSWORD 'creator';`,
				`CREATE SCHEMA create_object_private;`,
				`GRANT USAGE ON SCHEMA create_object_private TO schema_object_creator;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE FUNCTION create_object_private.created_without_create_function()
						RETURNS INT
						LANGUAGE SQL
						AS $$ SELECT 1 $$;`,
					ExpectedErr: `permission denied`,
					Username:    `schema_object_creator`,
					Password:    `creator`,
				},
				{
					Query: `CREATE PROCEDURE create_object_private.created_without_create_procedure()
						LANGUAGE SQL
						AS $$ SELECT 1 $$;`,
					ExpectedErr: `permission denied`,
					Username:    `schema_object_creator`,
					Password:    `creator`,
				},
				{
					Query:       `CREATE TYPE create_object_private.created_without_create_type AS ENUM ('one');`,
					ExpectedErr: `permission denied`,
					Username:    `schema_object_creator`,
					Password:    `creator`,
				},
				{
					Query:       `CREATE DOMAIN create_object_private.created_without_create_domain AS INT;`,
					ExpectedErr: `permission denied`,
					Username:    `schema_object_creator`,
					Password:    `creator`,
				},
				{
					Query:       `CREATE SEQUENCE create_object_private.created_without_create_sequence;`,
					ExpectedErr: `permission denied`,
					Username:    `schema_object_creator`,
					Password:    `creator`,
				},
			},
		},
	})
}

// TestDropSequenceRequiresOwnershipRepro reproduces a security bug: Doltgres
// allows a role that does not own a sequence to drop it.
func TestDropSequenceRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP SEQUENCE requires sequence ownership",
			SetUpScript: []string{
				`CREATE USER sequence_dropper PASSWORD 'dropper';`,
				`CREATE SEQUENCE drop_sequence_private;`,
				`GRANT USAGE ON SCHEMA public TO sequence_dropper;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DROP SEQUENCE drop_sequence_private;`,
					ExpectedErr: `permission denied`,
					Username:    `sequence_dropper`,
					Password:    `dropper`,
				},
			},
		},
	})
}

// TestDropSequenceRequiresOwnershipDespiteAllPrivilegesRepro reproduces a
// PostgreSQL authorization bug: GRANT ALL PRIVILEGES ON SEQUENCE does not
// transfer ownership and should not allow the grantee to DROP the sequence.
func TestDropSequenceRequiresOwnershipDespiteAllPrivilegesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP SEQUENCE requires ownership despite ALL PRIVILEGES",
			SetUpScript: []string{
				`CREATE USER drop_sequence_intruder PASSWORD 'dropper';`,
				`CREATE SEQUENCE drop_sequence_all_private;`,
				`GRANT USAGE ON SCHEMA public TO drop_sequence_intruder;`,
				`GRANT ALL PRIVILEGES ON SEQUENCE drop_sequence_all_private TO drop_sequence_intruder;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DROP SEQUENCE drop_sequence_all_private;`,
					ExpectedErr: `must be owner`,
					Username:    `drop_sequence_intruder`,
					Password:    `dropper`,
				},
				{
					Query:    `SELECT to_regclass('drop_sequence_all_private')::text;`,
					Expected: []sql.Row{{"drop_sequence_all_private"}},
				},
			},
		},
	})
}

// TestCreateSequenceOwnedByRequiresTableOwnershipRepro reproduces a security
// bug: Doltgres lets a role create a sequence owned by a table it does not own.
func TestCreateSequenceOwnedByRequiresTableOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE SEQUENCE OWNED BY requires table ownership",
			SetUpScript: []string{
				`CREATE USER sequence_owner_hijacker PASSWORD 'hijacker';`,
				`CREATE TABLE sequence_owner_private (id INT PRIMARY KEY);`,
				`GRANT USAGE, CREATE ON SCHEMA public TO sequence_owner_hijacker;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `CREATE SEQUENCE sequence_owner_hijacked OWNED BY sequence_owner_private.id;`,
					ExpectedErr: `permission denied`,
					Username:    `sequence_owner_hijacker`,
					Password:    `hijacker`,
				},
			},
		},
	})
}

// TestAlterSequenceOwnedByRequiresTableOwnershipRepro reproduces a security
// bug: Doltgres lets a role change sequence ownership metadata to point at a
// table it does not own.
func TestAlterSequenceOwnedByRequiresTableOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER SEQUENCE OWNED BY requires table ownership",
			SetUpScript: []string{
				`CREATE USER sequence_owner_alterer PASSWORD 'alterer';`,
				`CREATE TABLE alter_sequence_owner_private (id INT PRIMARY KEY);`,
				`CREATE SEQUENCE alter_sequence_owned;`,
				`GRANT USAGE ON SCHEMA public TO sequence_owner_alterer;`,
				`GRANT UPDATE ON SEQUENCE alter_sequence_owned TO sequence_owner_alterer;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `ALTER SEQUENCE alter_sequence_owned OWNED BY alter_sequence_owner_private.id;`,
					ExpectedErr: `permission denied`,
					Username:    `sequence_owner_alterer`,
					Password:    `alterer`,
				},
			},
		},
	})
}

// TestAlterSequenceRequiresSequenceOwnershipRepro reproduces a security bug:
// Doltgres lets a role that does not own a sequence change its ownership
// dependency metadata.
func TestAlterSequenceRequiresSequenceOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER SEQUENCE requires sequence ownership",
			SetUpScript: []string{
				`CREATE USER sequence_alterer PASSWORD 'alterer';`,
				`CREATE TABLE alter_sequence_owner_items (id INT PRIMARY KEY);`,
				`CREATE SEQUENCE alter_sequence_private
					OWNED BY alter_sequence_owner_items.id;`,
				`GRANT USAGE ON SCHEMA public TO sequence_alterer;`,
				`GRANT UPDATE ON SEQUENCE alter_sequence_private TO sequence_alterer;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `ALTER SEQUENCE alter_sequence_private OWNED BY NONE;`,
					ExpectedErr: `permission denied`,
					Username:    `sequence_alterer`,
					Password:    `alterer`,
				},
				{
					Query: `SELECT pg_get_serial_sequence(
							'alter_sequence_owner_items',
							'id'
						);`,
					Expected: []sql.Row{{"public.alter_sequence_private"}},
				},
			},
		},
	})
}

// TestAlterSequenceOwnerToRequiresOwnershipRepro reproduces a security bug:
// Doltgres allows a role that does not own a sequence to transfer ownership.
func TestAlterSequenceOwnerToRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER SEQUENCE OWNER TO requires sequence ownership",
			SetUpScript: []string{
				`CREATE USER sequence_owner_to_hijacker PASSWORD 'hijacker';`,
				`CREATE SEQUENCE owner_to_sequence_private;`,
				`GRANT USAGE ON SCHEMA public TO sequence_owner_to_hijacker;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `ALTER SEQUENCE owner_to_sequence_private OWNER TO sequence_owner_to_hijacker;`,
					ExpectedErr: `must be owner`,
					Username:    `sequence_owner_to_hijacker`,
					Password:    `hijacker`,
				},
				{
					Query: `SELECT pg_get_userbyid(relowner)
						FROM pg_catalog.pg_class
						WHERE oid = 'owner_to_sequence_private'::regclass;`,
					Expected: []sql.Row{{"postgres"}},
				},
			},
		},
	})
}

// TestDropTypeRequiresOwnershipRepro reproduces a security bug: Doltgres
// allows a role that does not own a type to drop it.
func TestDropTypeRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP TYPE requires type ownership",
			SetUpScript: []string{
				`CREATE USER type_dropper PASSWORD 'dropper';`,
				`CREATE TYPE drop_type_private AS ENUM ('one');`,
				`GRANT USAGE ON SCHEMA public TO type_dropper;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DROP TYPE drop_type_private;`,
					ExpectedErr: `permission denied`,
					Username:    `type_dropper`,
					Password:    `dropper`,
				},
			},
		},
	})
}

// TestDropTypeRequiresOwnershipDespiteAllPrivilegesGuard guards that GRANT ALL
// PRIVILEGES ON TYPE does not transfer ownership and does not allow the grantee
// to DROP the type.
func TestDropTypeRequiresOwnershipDespiteAllPrivilegesGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP TYPE requires ownership despite ALL PRIVILEGES",
			SetUpScript: []string{
				`CREATE USER drop_type_intruder PASSWORD 'dropper';`,
				`CREATE TYPE drop_type_all_private AS ENUM ('one');`,
				`GRANT USAGE ON SCHEMA public TO drop_type_intruder;`,
				`GRANT ALL PRIVILEGES ON TYPE drop_type_all_private TO drop_type_intruder;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DROP TYPE drop_type_all_private;`,
					ExpectedErr: `must be owner`,
					Username:    `drop_type_intruder`,
					Password:    `dropper`,
				},
				{
					Query:    `SELECT 'one'::drop_type_all_private::text;`,
					Expected: []sql.Row{{"one"}},
				},
			},
		},
	})
}

// TestAlterTypeOwnerToRequiresOwnershipRepro reproduces a security bug:
// Doltgres allows a role that does not own a type to transfer ownership.
func TestAlterTypeOwnerToRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TYPE OWNER TO requires type ownership",
			SetUpScript: []string{
				`CREATE USER type_owner_to_hijacker PASSWORD 'hijacker';`,
				`CREATE TYPE owner_to_type_private AS ENUM ('one');`,
				`GRANT USAGE ON SCHEMA public TO type_owner_to_hijacker;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `ALTER TYPE owner_to_type_private OWNER TO type_owner_to_hijacker;`,
					ExpectedErr: `must be owner`,
					Username:    `type_owner_to_hijacker`,
					Password:    `hijacker`,
				},
				{
					Query: `SELECT pg_get_userbyid(typowner)
						FROM pg_catalog.pg_type
						WHERE typname = 'owner_to_type_private';`,
					Expected: []sql.Row{{"postgres"}},
				},
			},
		},
	})
}

// TestDropDomainRequiresOwnershipRepro reproduces a security bug: Doltgres
// allows a role that does not own a domain to drop it.
func TestDropDomainRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP DOMAIN requires domain ownership",
			SetUpScript: []string{
				`CREATE USER domain_dropper PASSWORD 'dropper';`,
				`CREATE DOMAIN drop_domain_private AS INT;`,
				`GRANT USAGE ON SCHEMA public TO domain_dropper;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DROP DOMAIN drop_domain_private;`,
					ExpectedErr: `permission denied`,
					Username:    `domain_dropper`,
					Password:    `dropper`,
				},
			},
		},
	})
}

// TestDropDomainRequiresOwnershipDespiteAllPrivilegesGuard guards that GRANT
// ALL PRIVILEGES ON TYPE for a domain does not transfer ownership and does not
// allow the grantee to DROP the domain.
func TestDropDomainRequiresOwnershipDespiteAllPrivilegesGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP DOMAIN requires ownership despite ALL PRIVILEGES",
			SetUpScript: []string{
				`CREATE USER drop_domain_intruder PASSWORD 'dropper';`,
				`CREATE DOMAIN drop_domain_all_private AS INT CHECK (VALUE > 0);`,
				`GRANT USAGE ON SCHEMA public TO drop_domain_intruder;`,
				`GRANT ALL PRIVILEGES ON TYPE drop_domain_all_private TO drop_domain_intruder;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DROP DOMAIN drop_domain_all_private;`,
					ExpectedErr: `must be owner`,
					Username:    `drop_domain_intruder`,
					Password:    `dropper`,
				},
				{
					Query:    `SELECT 1::drop_domain_all_private::int;`,
					Expected: []sql.Row{{int32(1)}},
				},
			},
		},
	})
}

// TestAlterDomainOwnerToRequiresOwnershipRepro reproduces a security bug:
// Doltgres allows a role that does not own a domain to transfer ownership.
func TestAlterDomainOwnerToRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER DOMAIN OWNER TO requires domain ownership",
			SetUpScript: []string{
				`CREATE USER domain_owner_to_hijacker PASSWORD 'hijacker';`,
				`CREATE DOMAIN owner_to_domain_private AS INT;`,
				`GRANT USAGE ON SCHEMA public TO domain_owner_to_hijacker;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `ALTER DOMAIN owner_to_domain_private OWNER TO domain_owner_to_hijacker;`,
					ExpectedErr: `must be owner`,
					Username:    `domain_owner_to_hijacker`,
					Password:    `hijacker`,
				},
				{
					Query: `SELECT pg_get_userbyid(typowner)
						FROM pg_catalog.pg_type
						WHERE typname = 'owner_to_domain_private';`,
					Expected: []sql.Row{{"postgres"}},
				},
			},
		},
	})
}

// TestAlterSchemaOwnerToRequiresOwnershipRepro reproduces a security bug:
// Doltgres allows a role that does not own a schema to transfer ownership.
func TestAlterSchemaOwnerToRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER SCHEMA OWNER TO requires schema ownership",
			SetUpScript: []string{
				`CREATE USER schema_owner_to_hijacker PASSWORD 'hijacker';`,
				`CREATE SCHEMA owner_to_schema_private;`,
				`GRANT USAGE ON SCHEMA owner_to_schema_private TO schema_owner_to_hijacker;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `ALTER SCHEMA owner_to_schema_private OWNER TO schema_owner_to_hijacker;`,
					ExpectedErr: `must be owner`,
					Username:    `schema_owner_to_hijacker`,
					Password:    `hijacker`,
				},
				{
					Query: `SELECT pg_get_userbyid(nspowner)
						FROM pg_catalog.pg_namespace
						WHERE nspname = 'owner_to_schema_private';`,
					Expected: []sql.Row{{"postgres"}},
				},
			},
		},
	})
}

// TestDropIndexRequiresTableOwnershipRepro reproduces a security bug: Doltgres
// allows a role that does not own a table to drop one of its indexes.
func TestDropIndexRequiresTableOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP INDEX requires ownership of indexed table",
			SetUpScript: []string{
				`CREATE USER index_dropper PASSWORD 'dropper';`,
				`CREATE TABLE drop_index_private (id INT PRIMARY KEY, label TEXT);`,
				`CREATE INDEX drop_index_private_label_idx ON drop_index_private (label);`,
				`GRANT USAGE ON SCHEMA public TO index_dropper;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DROP INDEX drop_index_private_label_idx;`,
					ExpectedErr: `permission denied`,
					Username:    `index_dropper`,
					Password:    `dropper`,
				},
			},
		},
	})
}

// TestCreateExtensionRequiresCreatePrivilegeRepro reproduces a security bug:
// Doltgres allows a normal role without database CREATE privilege to install an
// extension.
func TestCreateExtensionRequiresCreatePrivilegeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE EXTENSION requires database CREATE privilege",
			SetUpScript: []string{
				`CREATE USER extension_creator PASSWORD 'creator';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `CREATE EXTENSION "uuid-ossp";`,
					ExpectedErr: `permission denied`,
					Username:    `extension_creator`,
					Password:    `creator`,
				},
			},
		},
	})
}

// TestDropExtensionRequiresOwnershipRepro reproduces a security bug: Doltgres
// allows a normal role to drop an extension it does not own.
func TestDropExtensionRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP EXTENSION requires extension ownership",
			SetUpScript: []string{
				`CREATE USER extension_dropper PASSWORD 'dropper';`,
				`CREATE EXTENSION hstore WITH SCHEMA public;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DROP EXTENSION hstore;`,
					ExpectedErr: `permission denied`,
					Username:    `extension_dropper`,
					Password:    `dropper`,
				},
			},
		},
	})
}

// TestCreateExtensionWithSchemaRequiresSchemaCreatePrivilegeRepro reproduces a
// security bug: Doltgres allows a role to install extension objects into a
// schema where it only has USAGE.
func TestCreateExtensionWithSchemaRequiresSchemaCreatePrivilegeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE EXTENSION WITH SCHEMA requires schema CREATE privilege",
			SetUpScript: []string{
				`CREATE USER extension_schema_intruder PASSWORD 'intruder';`,
				`CREATE SCHEMA extension_private_schema;`,
				`GRANT CREATE ON DATABASE postgres TO extension_schema_intruder;`,
				`GRANT USAGE ON SCHEMA extension_private_schema TO extension_schema_intruder;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `CREATE EXTENSION hstore WITH SCHEMA extension_private_schema;`,
					ExpectedErr: `permission denied`,
					Username:    `extension_schema_intruder`,
					Password:    `intruder`,
				},
			},
		},
	})
}

// TestDropTableRequiresOwnershipDespiteAllPrivilegesRepro reproduces a
// PostgreSQL authorization bug: GRANT ALL PRIVILEGES ON TABLE does not transfer
// ownership and should not allow the grantee to DROP the table.
func TestDropTableRequiresOwnershipDespiteAllPrivilegesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP TABLE requires ownership despite ALL PRIVILEGES",
			SetUpScript: []string{
				`CREATE USER drop_table_intruder PASSWORD 'dropper';`,
				`CREATE TABLE drop_table_private (id INT PRIMARY KEY, secret TEXT);`,
				`GRANT USAGE ON SCHEMA public TO drop_table_intruder;`,
				`GRANT ALL PRIVILEGES ON TABLE drop_table_private TO drop_table_intruder;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DROP TABLE drop_table_private;`,
					ExpectedErr: `must be owner`,
					Username:    `drop_table_intruder`,
					Password:    `dropper`,
				},
				{
					Query:    `SELECT to_regclass('drop_table_private')::text;`,
					Expected: []sql.Row{{"drop_table_private"}},
				},
			},
		},
	})
}

// TestCreateViewRequiresSchemaCreatePrivilegeRepro reproduces a security bug:
// Doltgres allows a role without CREATE on the target schema to create a view.
func TestCreateViewRequiresSchemaCreatePrivilegeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE VIEW requires CREATE on target schema",
			SetUpScript: []string{
				`CREATE USER view_creator PASSWORD 'creator';`,
				`CREATE SCHEMA view_private_schema;`,
				`GRANT USAGE ON SCHEMA view_private_schema TO view_creator;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `CREATE VIEW view_private_schema.created_without_create AS SELECT 1 AS id;`,
					ExpectedErr: `permission denied`,
					Username:    `view_creator`,
					Password:    `creator`,
				},
			},
		},
	})
}

// TestCreateViewRequiresSelectOnSourceTableGuard covers a sensitive privilege
// boundary: creating a view over a table requires SELECT on that source table.
func TestCreateViewRequiresSelectOnSourceTableGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE VIEW requires SELECT on source table",
			SetUpScript: []string{
				`CREATE USER view_source_reader PASSWORD 'reader';`,
				`CREATE TABLE view_source_private (id INT PRIMARY KEY);`,
				`GRANT USAGE, CREATE ON SCHEMA public TO view_source_reader;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `CREATE VIEW view_from_private_source AS SELECT id FROM view_source_private;`,
					ExpectedErr: `permission denied`,
					Username:    `view_source_reader`,
					Password:    `reader`,
				},
			},
		},
	})
}

// TestDropViewRequiresOwnershipRepro reproduces a security bug: Doltgres
// allows a role that does not own a view to drop it.
func TestDropViewRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP VIEW requires view ownership",
			SetUpScript: []string{
				`CREATE USER view_dropper PASSWORD 'dropper';`,
				`CREATE VIEW drop_view_private AS SELECT 1 AS id;`,
				`GRANT USAGE ON SCHEMA public TO view_dropper;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DROP VIEW drop_view_private;`,
					ExpectedErr: `permission denied`,
					Username:    `view_dropper`,
					Password:    `dropper`,
				},
			},
		},
	})
}

// TestDropViewRequiresOwnershipDespiteAllPrivilegesRepro reproduces a
// PostgreSQL authorization bug: GRANT ALL PRIVILEGES on a view does not
// transfer ownership and should not allow the grantee to DROP the view.
func TestDropViewRequiresOwnershipDespiteAllPrivilegesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP VIEW requires ownership despite ALL PRIVILEGES",
			SetUpScript: []string{
				`CREATE USER drop_view_intruder PASSWORD 'dropper';`,
				`CREATE VIEW drop_view_all_private AS SELECT 1 AS id;`,
				`GRANT USAGE ON SCHEMA public TO drop_view_intruder;`,
				`GRANT ALL PRIVILEGES ON TABLE drop_view_all_private TO drop_view_intruder;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DROP VIEW drop_view_all_private;`,
					ExpectedErr: `must be owner`,
					Username:    `drop_view_intruder`,
					Password:    `dropper`,
				},
				{
					Query:    `SELECT to_regclass('drop_view_all_private')::text;`,
					Expected: []sql.Row{{"drop_view_all_private"}},
				},
			},
		},
	})
}

// TestRefreshMaterializedViewRequiresOwnershipRepro reproduces a security bug:
// Doltgres allows a role that does not own a materialized view to refresh it.
func TestRefreshMaterializedViewRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "REFRESH MATERIALIZED VIEW requires ownership",
			SetUpScript: []string{
				`CREATE USER mv_refresher PASSWORD 'refresher';`,
				`CREATE TABLE mv_source_private (id INT PRIMARY KEY, label TEXT);`,
				`INSERT INTO mv_source_private VALUES (1, 'one');`,
				`CREATE MATERIALIZED VIEW refresh_mv_private AS SELECT id, label FROM mv_source_private;`,
				`INSERT INTO mv_source_private VALUES (2, 'two');`,
				`GRANT USAGE ON SCHEMA public TO mv_refresher;`,
				`GRANT SELECT ON mv_source_private TO mv_refresher;`,
				`GRANT ALL PRIVILEGES ON refresh_mv_private TO mv_refresher;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `REFRESH MATERIALIZED VIEW refresh_mv_private;`,
					ExpectedErr: `permission denied`,
					Username:    `mv_refresher`,
					Password:    `refresher`,
				},
			},
		},
	})
}

// TestDropMaterializedViewRequiresOwnershipDespiteAllPrivilegesRepro
// reproduces a PostgreSQL authorization bug: GRANT ALL PRIVILEGES on a
// materialized view does not transfer ownership and should not allow the
// grantee to DROP it.
func TestDropMaterializedViewRequiresOwnershipDespiteAllPrivilegesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP MATERIALIZED VIEW requires ownership despite ALL PRIVILEGES",
			SetUpScript: []string{
				`CREATE USER drop_matview_intruder PASSWORD 'dropper';`,
				`CREATE MATERIALIZED VIEW drop_matview_all_private AS SELECT 1 AS id;`,
				`GRANT USAGE ON SCHEMA public TO drop_matview_intruder;`,
				`GRANT ALL PRIVILEGES ON TABLE drop_matview_all_private TO drop_matview_intruder;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DROP MATERIALIZED VIEW drop_matview_all_private;`,
					ExpectedErr: `must be owner`,
					Username:    `drop_matview_intruder`,
					Password:    `dropper`,
				},
				{
					Query:    `SELECT to_regclass('drop_matview_all_private')::text;`,
					Expected: []sql.Row{{"drop_matview_all_private"}},
				},
			},
		},
	})
}

// TestAlterMaterializedViewRenameColumnRequiresOwnershipRepro reproduces a
// security bug: Doltgres allows a role that does not own a materialized view to
// rename one of its columns.
func TestAlterMaterializedViewRenameColumnRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER MATERIALIZED VIEW RENAME COLUMN requires ownership",
			SetUpScript: []string{
				`CREATE USER mv_column_renamer PASSWORD 'renamer';`,
				`CREATE TABLE mv_rename_source_private (id INT PRIMARY KEY, label TEXT);`,
				`INSERT INTO mv_rename_source_private VALUES (1, 'one');`,
				`CREATE MATERIALIZED VIEW rename_mv_private AS SELECT id, label FROM mv_rename_source_private;`,
				`GRANT USAGE ON SCHEMA public TO mv_column_renamer;`,
				`GRANT ALL PRIVILEGES ON rename_mv_private TO mv_column_renamer;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `ALTER MATERIALIZED VIEW rename_mv_private RENAME COLUMN label TO renamed_label;`,
					ExpectedErr: `permission denied`,
					Username:    `mv_column_renamer`,
					Password:    `renamer`,
				},
			},
		},
	})
}

// TestAlterMaterializedViewRenameToRequiresOwnershipRepro reproduces a security
// bug: Doltgres allows a role that does not own a materialized view to rename
// the relation itself.
func TestAlterMaterializedViewRenameToRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER MATERIALIZED VIEW RENAME TO requires ownership",
			SetUpScript: []string{
				`CREATE USER mv_relation_renamer PASSWORD 'renamer';`,
				`CREATE TABLE mv_rename_to_source_private (id INT PRIMARY KEY, label TEXT);`,
				`INSERT INTO mv_rename_to_source_private VALUES (1, 'one');`,
				`CREATE MATERIALIZED VIEW rename_to_mv_private AS
					SELECT id, label FROM mv_rename_to_source_private;`,
				`GRANT USAGE ON SCHEMA public TO mv_relation_renamer;`,
				`GRANT ALL PRIVILEGES ON rename_to_mv_private TO mv_relation_renamer;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `ALTER MATERIALIZED VIEW rename_to_mv_private RENAME TO renamed_by_non_owner;`,
					ExpectedErr: `permission denied`,
					Username:    `mv_relation_renamer`,
					Password:    `renamer`,
				},
			},
		},
	})
}

// TestCreateMaterializedViewRequiresSchemaCreatePrivilegeGuard covers
// materialized-view creation authorization: creating the destination relation
// requires CREATE on the target schema.
func TestCreateMaterializedViewRequiresSchemaCreatePrivilegeGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE MATERIALIZED VIEW requires CREATE on target schema",
			SetUpScript: []string{
				`CREATE USER mv_creator PASSWORD 'creator';`,
				`CREATE SCHEMA mv_private_schema;`,
				`GRANT USAGE ON SCHEMA mv_private_schema TO mv_creator;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `CREATE MATERIALIZED VIEW mv_private_schema.created_without_create AS SELECT 1 AS id;`,
					ExpectedErr: `permission denied`,
					Username:    `mv_creator`,
					Password:    `creator`,
				},
			},
		},
	})
}

// TestCreateMaterializedViewRequiresSelectOnSourceTableGuard covers a
// sensitive privilege boundary: creating a materialized view over a table
// requires SELECT on that source table.
func TestCreateMaterializedViewRequiresSelectOnSourceTableGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE MATERIALIZED VIEW requires SELECT on source table",
			SetUpScript: []string{
				`CREATE USER mv_source_reader PASSWORD 'reader';`,
				`CREATE TABLE mv_create_source_private (id INT PRIMARY KEY, secret TEXT);`,
				`INSERT INTO mv_create_source_private VALUES (1, 'secret');`,
				`GRANT USAGE, CREATE ON SCHEMA public TO mv_source_reader;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `CREATE MATERIALIZED VIEW mv_from_private_source AS SELECT id, secret FROM mv_create_source_private;`,
					ExpectedErr: `permission denied`,
					Username:    `mv_source_reader`,
					Password:    `reader`,
				},
			},
		},
	})
}

// TestCreateIndexRequiresTableOwnershipRepro reproduces a security bug:
// Doltgres allows a role that does not own a table to create an index on it.
func TestCreateIndexRequiresTableOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE INDEX requires ownership of indexed table",
			SetUpScript: []string{
				`CREATE USER index_creator PASSWORD 'creator';`,
				`CREATE TABLE create_index_private (id INT PRIMARY KEY, label TEXT);`,
				`GRANT USAGE ON SCHEMA public TO index_creator;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `CREATE INDEX create_index_private_label_idx ON create_index_private (label);`,
					ExpectedErr: `permission denied`,
					Username:    `index_creator`,
					Password:    `creator`,
				},
			},
		},
	})
}

// TestAlterIndexRequiresTableOwnershipRepro reproduces a security bug:
// Doltgres allows a role that does not own the indexed table to alter index
// storage metadata.
func TestAlterIndexRequiresTableOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER INDEX requires ownership of indexed table",
			SetUpScript: []string{
				`CREATE USER index_alterer PASSWORD 'alterer';`,
				`CREATE TABLE alter_index_private (id INT PRIMARY KEY, label TEXT);`,
				`CREATE INDEX alter_index_private_label_idx ON alter_index_private (label);`,
				`GRANT USAGE ON SCHEMA public TO index_alterer;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `ALTER INDEX alter_index_private_label_idx SET (fillfactor = 80);`,
					ExpectedErr: `permission denied`,
					Username:    `index_alterer`,
					Password:    `alterer`,
				},
			},
		},
	})
}

// TestRenameIndexRequiresTableOwnershipRepro reproduces a security bug:
// Doltgres allows a role that does not own the indexed table to rename an
// index.
func TestRenameIndexRequiresTableOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER INDEX RENAME requires ownership of indexed table",
			SetUpScript: []string{
				`CREATE USER index_renamer PASSWORD 'renamer';`,
				`CREATE TABLE rename_index_private (id INT PRIMARY KEY, label TEXT);`,
				`CREATE INDEX rename_index_private_label_idx ON rename_index_private (label);`,
				`GRANT USAGE ON SCHEMA public TO index_renamer;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `ALTER INDEX rename_index_private_label_idx RENAME TO rename_index_private_label_renamed_idx;`,
					ExpectedErr: `permission denied`,
					Username:    `index_renamer`,
					Password:    `renamer`,
				},
			},
		},
	})
}

// TestReindexTableRequiresOwnershipRepro reproduces a security bug: Doltgres
// allows a role that does not own a table to REINDEX it.
func TestReindexTableRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "REINDEX TABLE requires table ownership",
			SetUpScript: []string{
				`CREATE USER reindex_table_user PASSWORD 'reindexer';`,
				`CREATE TABLE reindex_table_private (id INT PRIMARY KEY, label TEXT);`,
				`CREATE INDEX reindex_table_private_label_idx ON reindex_table_private (label);`,
				`GRANT USAGE ON SCHEMA public TO reindex_table_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `REINDEX TABLE reindex_table_private;`,
					ExpectedErr: `permission denied`,
					Username:    `reindex_table_user`,
					Password:    `reindexer`,
				},
			},
		},
	})
}

// TestReindexIndexRequiresTableOwnershipRepro reproduces a security bug:
// Doltgres allows a role that does not own the indexed table to REINDEX one of
// its indexes.
func TestReindexIndexRequiresTableOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "REINDEX INDEX requires ownership of indexed table",
			SetUpScript: []string{
				`CREATE USER reindex_index_user PASSWORD 'reindexer';`,
				`CREATE TABLE reindex_index_private (id INT PRIMARY KEY, label TEXT);`,
				`CREATE INDEX reindex_index_private_label_idx ON reindex_index_private (label);`,
				`GRANT USAGE ON SCHEMA public TO reindex_index_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `REINDEX INDEX reindex_index_private_label_idx;`,
					ExpectedErr: `permission denied`,
					Username:    `reindex_index_user`,
					Password:    `reindexer`,
				},
			},
		},
	})
}

// TestCreatePublicationRequiresTableOwnershipRepro reproduces a security bug:
// Doltgres allows a role that does not own a table to publish it.
func TestCreatePublicationRequiresTableOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE PUBLICATION FOR TABLE requires table ownership",
			SetUpScript: []string{
				`CREATE USER publication_creator PASSWORD 'creator';`,
				`CREATE TABLE publication_private (id INT PRIMARY KEY);`,
				`GRANT USAGE ON SCHEMA public TO publication_creator;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `CREATE PUBLICATION publication_private_pub FOR TABLE publication_private;`,
					ExpectedErr: `permission denied`,
					Username:    `publication_creator`,
					Password:    `creator`,
				},
			},
		},
	})
}

// TestAlterPublicationAddTableRequiresTableOwnershipRepro reproduces a
// security bug: Doltgres allows a publication owner to add a table they do not
// own to the publication.
func TestAlterPublicationAddTableRequiresTableOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER PUBLICATION ADD TABLE requires table ownership",
			SetUpScript: []string{
				`CREATE USER publication_table_adder PASSWORD 'adder';`,
				`CREATE TABLE publication_add_private (id INT PRIMARY KEY);`,
				`GRANT USAGE ON SCHEMA public TO publication_table_adder;`,
				`GRANT CREATE ON DATABASE postgres TO publication_table_adder;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `CREATE PUBLICATION publication_add_pub;`,
					Username: `publication_table_adder`,
					Password: `adder`,
				},
				{
					Query:       `ALTER PUBLICATION publication_add_pub ADD TABLE publication_add_private;`,
					ExpectedErr: `owner of table`,
					Username:    `publication_table_adder`,
					Password:    `adder`,
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_publication_tables
						WHERE pubname = 'publication_add_pub';`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestAlterPublicationSetTableRequiresTableOwnershipRepro reproduces a
// security bug: Doltgres allows a publication owner to replace publication
// membership with a table they do not own.
func TestAlterPublicationSetTableRequiresTableOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER PUBLICATION SET TABLE requires table ownership",
			SetUpScript: []string{
				`CREATE USER publication_table_setter PASSWORD 'setter';`,
				`CREATE TABLE publication_set_private (id INT PRIMARY KEY);`,
				`GRANT USAGE ON SCHEMA public TO publication_table_setter;`,
				`GRANT CREATE ON DATABASE postgres TO publication_table_setter;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `CREATE PUBLICATION publication_set_pub;`,
					Username: `publication_table_setter`,
					Password: `setter`,
				},
				{
					Query:       `ALTER PUBLICATION publication_set_pub SET TABLE publication_set_private;`,
					ExpectedErr: `owner of table`,
					Username:    `publication_table_setter`,
					Password:    `setter`,
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_publication_tables
						WHERE pubname = 'publication_set_pub';`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestCreatePublicationForAllTablesRequiresSuperuserRepro reproduces a
// security bug: PostgreSQL restricts FOR ALL TABLES publications to superusers.
func TestCreatePublicationForAllTablesRequiresSuperuserRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE PUBLICATION FOR ALL TABLES requires superuser",
			SetUpScript: []string{
				`CREATE USER publication_all_tables_user PASSWORD 'alltables';`,
				`GRANT CREATE ON DATABASE postgres TO publication_all_tables_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `CREATE PUBLICATION publication_all_tables_pub FOR ALL TABLES;`,
					ExpectedErr: `superuser`,
					Username:    `publication_all_tables_user`,
					Password:    `alltables`,
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_publication
						WHERE pubname = 'publication_all_tables_pub';`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestCreatePublicationForTablesInSchemaRequiresSuperuserRepro reproduces a
// security bug: PostgreSQL restricts FOR TABLES IN SCHEMA publications to
// superusers because they implicitly publish all current and future tables in
// the named schemas.
func TestCreatePublicationForTablesInSchemaRequiresSuperuserRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE PUBLICATION FOR TABLES IN SCHEMA requires superuser",
			SetUpScript: []string{
				`CREATE USER publication_schema_user PASSWORD 'schemapub';`,
				`CREATE SCHEMA publication_schema_private;`,
				`CREATE TABLE publication_schema_private.items (id INT PRIMARY KEY);`,
				`GRANT CREATE ON DATABASE postgres TO publication_schema_user;`,
				`GRANT USAGE ON SCHEMA publication_schema_private TO publication_schema_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE PUBLICATION publication_schema_pub
						FOR TABLES IN SCHEMA publication_schema_private;`,
					ExpectedErr: `superuser`,
					Username:    `publication_schema_user`,
					Password:    `schemapub`,
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_publication
						WHERE pubname = 'publication_schema_pub';`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestAlterPublicationAddTablesInSchemaRequiresSuperuserRepro reproduces a
// security bug: PostgreSQL restricts adding FOR TABLES IN SCHEMA membership to
// superusers because it implicitly publishes all current and future tables in
// the named schemas.
func TestAlterPublicationAddTablesInSchemaRequiresSuperuserRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER PUBLICATION ADD TABLES IN SCHEMA requires superuser",
			SetUpScript: []string{
				`CREATE USER publication_schema_adder PASSWORD 'schemaadder';`,
				`CREATE SCHEMA publication_schema_add_private;`,
				`CREATE TABLE publication_schema_add_private.items (id INT PRIMARY KEY);`,
				`GRANT CREATE ON DATABASE postgres TO publication_schema_adder;`,
				`GRANT USAGE ON SCHEMA publication_schema_add_private TO publication_schema_adder;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `CREATE PUBLICATION publication_schema_add_pub;`,
					Username: `publication_schema_adder`,
					Password: `schemaadder`,
				},
				{
					Query: `ALTER PUBLICATION publication_schema_add_pub
						ADD TABLES IN SCHEMA publication_schema_add_private;`,
					ExpectedErr: `superuser`,
					Username:    `publication_schema_adder`,
					Password:    `schemaadder`,
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_publication_namespace pn
						JOIN pg_catalog.pg_publication p ON p.oid = pn.pnpubid
						WHERE p.pubname = 'publication_schema_add_pub';`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestAlterPublicationSetTablesInSchemaRequiresSuperuserRepro reproduces a
// security bug: PostgreSQL restricts replacing a publication with FOR TABLES IN
// SCHEMA membership to superusers because it implicitly publishes all current
// and future tables in the named schemas.
func TestAlterPublicationSetTablesInSchemaRequiresSuperuserRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER PUBLICATION SET TABLES IN SCHEMA requires superuser",
			SetUpScript: []string{
				`CREATE USER publication_schema_setter PASSWORD 'schemasetter';`,
				`CREATE SCHEMA publication_schema_set_private;`,
				`CREATE TABLE publication_schema_set_private.items (id INT PRIMARY KEY);`,
				`GRANT CREATE ON DATABASE postgres TO publication_schema_setter;`,
				`GRANT USAGE ON SCHEMA publication_schema_set_private TO publication_schema_setter;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `CREATE PUBLICATION publication_schema_set_pub;`,
					Username: `publication_schema_setter`,
					Password: `schemasetter`,
				},
				{
					Query: `ALTER PUBLICATION publication_schema_set_pub
						SET TABLES IN SCHEMA publication_schema_set_private;`,
					ExpectedErr: `superuser`,
					Username:    `publication_schema_setter`,
					Password:    `schemasetter`,
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_publication_namespace pn
						JOIN pg_catalog.pg_publication p ON p.oid = pn.pnpubid
						WHERE p.pubname = 'publication_schema_set_pub';`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestAlterPublicationRequiresOwnershipRepro reproduces a security bug:
// Doltgres allows a role that does not own a publication to alter it.
func TestAlterPublicationRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER PUBLICATION requires publication ownership",
			SetUpScript: []string{
				`CREATE USER publication_alterer PASSWORD 'alterer';`,
				`CREATE TABLE publication_alter_private (id INT PRIMARY KEY);`,
				`CREATE PUBLICATION publication_alter_pub FOR TABLE publication_alter_private;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `ALTER PUBLICATION publication_alter_pub RENAME TO publication_alter_renamed_pub;`,
					ExpectedErr: `permission denied`,
					Username:    `publication_alterer`,
					Password:    `alterer`,
				},
			},
		},
	})
}

// TestAlterPublicationOwnerToUpdatesCatalogRepro reproduces a PostgreSQL
// compatibility gap: ALTER PUBLICATION OWNER TO should be accepted and update
// pg_publication.pubowner.
func TestAlterPublicationOwnerToUpdatesCatalogRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER PUBLICATION OWNER TO updates publication owner",
			SetUpScript: []string{
				`CREATE ROLE publication_owner_target;`,
				`CREATE TABLE publication_owner_to_private (id INT PRIMARY KEY);`,
				`CREATE PUBLICATION publication_owner_to_pub FOR TABLE publication_owner_to_private;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER PUBLICATION publication_owner_to_pub OWNER TO publication_owner_target;`,
				},
				{
					Query: `SELECT pg_get_userbyid(pubowner)
						FROM pg_catalog.pg_publication
						WHERE pubname = 'publication_owner_to_pub';`,
					Expected: []sql.Row{{"publication_owner_target"}},
				},
			},
		},
	})
}

// TestDropPublicationRequiresOwnershipRepro reproduces a security bug:
// Doltgres allows a role that does not own a publication to drop it.
func TestDropPublicationRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP PUBLICATION requires publication ownership",
			SetUpScript: []string{
				`CREATE USER publication_dropper PASSWORD 'dropper';`,
				`CREATE TABLE publication_drop_private (id INT PRIMARY KEY);`,
				`CREATE PUBLICATION publication_drop_pub FOR TABLE publication_drop_private;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DROP PUBLICATION publication_drop_pub;`,
					ExpectedErr: `permission denied`,
					Username:    `publication_dropper`,
					Password:    `dropper`,
				},
			},
		},
	})
}

// TestCreateSubscriptionRequiresSuperuserRepro reproduces a security bug:
// Doltgres allows a normal role to create subscription metadata.
func TestCreateSubscriptionRequiresSuperuserRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE SUBSCRIPTION requires elevated privilege",
			SetUpScript: []string{
				`CREATE USER subscription_creator PASSWORD 'creator';`,
				`CREATE TABLE subscription_private (id INT PRIMARY KEY);`,
				`CREATE PUBLICATION subscription_pub FOR TABLE subscription_private;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE SUBSCRIPTION subscription_created
						CONNECTION 'host=127.0.0.1 dbname=postgres'
						PUBLICATION subscription_pub
						WITH (connect = false, enabled = false, create_slot = false, slot_name = NONE);`,
					ExpectedErr: `permission denied`,
					Username:    `subscription_creator`,
					Password:    `creator`,
				},
			},
		},
	})
}

// TestCreateSubscriptionSetsOwnerCatalogRepro reproduces a subscription
// ownership catalog bug: PostgreSQL records the creating role as subowner.
func TestCreateSubscriptionSetsOwnerCatalogRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE SUBSCRIPTION records creating owner",
			SetUpScript: []string{
				`CREATE USER subscription_owner_creator SUPERUSER PASSWORD 'creator';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE SUBSCRIPTION subscription_owner_created
						CONNECTION 'host=127.0.0.1 dbname=postgres'
						PUBLICATION subscription_owner_pub
						WITH (connect = false, enabled = false, create_slot = false, slot_name = NONE);`,
					Username: `subscription_owner_creator`,
					Password: `creator`,
				},
				{
					Query: `SELECT pg_get_userbyid(subowner)
						FROM pg_catalog.pg_subscription
						WHERE subname = 'subscription_owner_created';`,
					Expected: []sql.Row{{"subscription_owner_creator"}},
				},
			},
		},
	})
}

// TestAlterSubscriptionOwnerUpdatesCatalogRepro reproduces a subscription
// ownership catalog bug: PostgreSQL updates pg_subscription.subowner when a
// subscription is reassigned.
func TestAlterSubscriptionOwnerUpdatesCatalogRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER SUBSCRIPTION OWNER TO updates pg_subscription owner",
			SetUpScript: []string{
				`CREATE USER subscription_owner_from SUPERUSER PASSWORD 'fromowner';`,
				`CREATE USER subscription_owner_to SUPERUSER PASSWORD 'toowner';`,
				`CREATE SUBSCRIPTION subscription_owner_transfer
					CONNECTION 'host=127.0.0.1 dbname=postgres'
					PUBLICATION subscription_owner_pub
					WITH (connect = false, enabled = false, create_slot = false, slot_name = NONE);`,
				`ALTER SUBSCRIPTION subscription_owner_transfer OWNER TO subscription_owner_from;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER SUBSCRIPTION subscription_owner_transfer
						OWNER TO subscription_owner_to;`,
					Username: `subscription_owner_from`,
					Password: `fromowner`,
				},
				{
					Query: `SELECT pg_get_userbyid(subowner)
						FROM pg_catalog.pg_subscription
						WHERE subname = 'subscription_owner_transfer';`,
					Expected: []sql.Row{{"subscription_owner_to"}},
				},
			},
		},
	})
}

// TestAlterSubscriptionOwnerRejectsMissingRoleRepro reproduces a subscription
// ownership validation bug: PostgreSQL validates the target owner role exists.
func TestAlterSubscriptionOwnerRejectsMissingRoleRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER SUBSCRIPTION OWNER TO rejects missing roles",
			SetUpScript: []string{
				`CREATE SUBSCRIPTION subscription_missing_owner
					CONNECTION 'host=127.0.0.1 dbname=postgres'
					PUBLICATION subscription_owner_pub
					WITH (connect = false, enabled = false, create_slot = false, slot_name = NONE);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER SUBSCRIPTION subscription_missing_owner
						OWNER TO missing_subscription_owner;`,
					ExpectedErr: `does not exist`,
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_subscription
						WHERE subname = 'subscription_missing_owner';`,
					Expected: []sql.Row{{1}},
				},
			},
		},
	})
}

// TestAlterSubscriptionRequiresOwnershipRepro reproduces a security bug:
// Doltgres allows a role that does not own a subscription to alter it.
func TestAlterSubscriptionRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER SUBSCRIPTION requires subscription ownership",
			SetUpScript: []string{
				`CREATE USER subscription_alterer PASSWORD 'alterer';`,
				`CREATE TABLE subscription_alter_private (id INT PRIMARY KEY);`,
				`CREATE PUBLICATION subscription_alter_pub FOR TABLE subscription_alter_private;`,
				`CREATE SUBSCRIPTION subscription_alter_sub
					CONNECTION 'host=127.0.0.1 dbname=postgres'
					PUBLICATION subscription_alter_pub
					WITH (connect = false, enabled = false, create_slot = false, slot_name = NONE);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `ALTER SUBSCRIPTION subscription_alter_sub RENAME TO subscription_alter_renamed_sub;`,
					ExpectedErr: `permission denied`,
					Username:    `subscription_alterer`,
					Password:    `alterer`,
				},
			},
		},
	})
}

// TestDropSubscriptionRequiresOwnershipRepro reproduces a security bug:
// Doltgres allows a role that does not own a subscription to drop it.
func TestDropSubscriptionRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP SUBSCRIPTION requires subscription ownership",
			SetUpScript: []string{
				`CREATE USER subscription_dropper PASSWORD 'dropper';`,
				`CREATE TABLE subscription_drop_private (id INT PRIMARY KEY);`,
				`CREATE PUBLICATION subscription_drop_pub FOR TABLE subscription_drop_private;`,
				`CREATE SUBSCRIPTION subscription_drop_sub
					CONNECTION 'host=127.0.0.1 dbname=postgres'
					PUBLICATION subscription_drop_pub
					WITH (connect = false, enabled = false, create_slot = false, slot_name = NONE);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DROP SUBSCRIPTION subscription_drop_sub;`,
					ExpectedErr: `permission denied`,
					Username:    `subscription_dropper`,
					Password:    `dropper`,
				},
			},
		},
	})
}
