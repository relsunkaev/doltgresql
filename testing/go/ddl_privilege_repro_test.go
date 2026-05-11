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
