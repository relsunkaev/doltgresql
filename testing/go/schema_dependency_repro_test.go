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
	"github.com/stretchr/testify/require"
)

// TestCreateSchemaExecutesContainedTableRepro reproduces a DDL correctness bug:
// PostgreSQL executes schema elements included in a CREATE SCHEMA statement.
func TestCreateSchemaExecutesContainedTableRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE SCHEMA executes contained table definition",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE SCHEMA inline_schema_repro
						CREATE TABLE inline_items (
							id INT PRIMARY KEY,
							label TEXT
						);`,
				},
				{
					Query: `INSERT INTO inline_schema_repro.inline_items
						VALUES (1, 'ok');`,
				},
				{
					Query: `SELECT id, label
						FROM inline_schema_repro.inline_items;`,
					Expected: []sql.Row{{1, "ok"}},
				},
			},
		},
	})
}

// TestDropSchemaContainingTableRequiresCascadeRepro guards basic non-empty
// schema dependency enforcement for tables.
func TestDropSchemaContainingTableRequiresCascadeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP SCHEMA rejects schemas containing tables",
			SetUpScript: []string{
				`CREATE SCHEMA schema_with_table;`,
				`CREATE TABLE schema_with_table.schema_table (id INT PRIMARY KEY);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DROP SCHEMA schema_with_table;`,
					ExpectedErr: `because other objects depend on it`,
				},
			},
		},
	})
}

// TestDropSchemaContainingViewRequiresCascadeRepro guards non-empty schema
// dependency enforcement for views.
func TestDropSchemaContainingViewRequiresCascadeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP SCHEMA rejects schemas containing views",
			SetUpScript: []string{
				`CREATE TABLE schema_view_source (id INT PRIMARY KEY);`,
				`CREATE SCHEMA schema_with_view;`,
				`CREATE VIEW schema_with_view.schema_view_reader AS
					SELECT id FROM schema_view_source;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DROP SCHEMA schema_with_view;`,
					ExpectedErr: `because other objects depend on it`,
				},
			},
		},
	})
}

// TestDropSchemaContainingMaterializedViewRequiresCascadeRepro guards non-empty
// schema dependency enforcement for materialized views.
func TestDropSchemaContainingMaterializedViewRequiresCascadeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP SCHEMA rejects schemas containing materialized views",
			SetUpScript: []string{
				`CREATE TABLE schema_matview_source (id INT PRIMARY KEY);`,
				`INSERT INTO schema_matview_source VALUES (1);`,
				`CREATE SCHEMA schema_with_matview;`,
				`CREATE MATERIALIZED VIEW schema_with_matview.schema_matview_reader AS
					SELECT id FROM schema_matview_source;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DROP SCHEMA schema_with_matview;`,
					ExpectedErr: `because other objects depend on it`,
				},
			},
		},
	})
}

// TestDropSchemaCascadeDropsContainedRelationsRepro reproduces a schema
// dependency bug: PostgreSQL DROP SCHEMA CASCADE removes contained objects.
func TestDropSchemaCascadeDropsContainedRelationsRepro(t *testing.T) {
	ctx, conn, controller := CreateServer(t, "postgres")
	t.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	for _, stmt := range []string{
		`CREATE SCHEMA cascade_schema;`,
		`CREATE TABLE cascade_schema.cascade_items (id INT PRIMARY KEY);`,
		`INSERT INTO cascade_schema.cascade_items VALUES (1);`,
	} {
		_, err := conn.Current.Exec(ctx, stmt)
		require.NoError(t, err, stmt)
	}

	_, err := conn.Current.Exec(ctx, `DROP SCHEMA cascade_schema CASCADE;`)
	require.NoError(t, err)

	_, err = conn.Current.Exec(ctx, `SELECT id FROM cascade_schema.cascade_items;`)
	require.Error(t, err)
}

// TestAlterTableQualifiedRenameStaysInSchemaRepro reproduces a schema-qualified
// DDL correctness bug: ALTER TABLE schema.table RENAME TO new_name should rename
// the relation within the original schema.
func TestAlterTableQualifiedRenameStaysInSchemaRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "schema-qualified ALTER TABLE RENAME keeps relation in schema",
			SetUpScript: []string{
				`CREATE SCHEMA rename_schema;`,
				`CREATE TABLE rename_schema.source_table (
					id INT PRIMARY KEY,
					note TEXT
				);`,
				`INSERT INTO rename_schema.source_table VALUES (1, 'kept');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE rename_schema.source_table RENAME TO renamed_table;`,
				},
				{
					Query: `SELECT id, note
						FROM rename_schema.renamed_table;`,
					Expected: []sql.Row{{1, "kept"}},
				},
				{
					Query:       `SELECT id, note FROM rename_schema.source_table;`,
					ExpectedErr: `not found`,
				},
			},
		},
	})
}

func TestAlterTableQualifiedRenamePreservesForeignKeysRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "schema-qualified ALTER TABLE RENAME preserves foreign keys",
			SetUpScript: []string{
				`CREATE SCHEMA rename_fk_schema;`,
				`CREATE TABLE rename_fk_schema.parent_table (
					id INT PRIMARY KEY
				);`,
				`CREATE TABLE rename_fk_schema.child_table (
					id INT PRIMARY KEY,
					parent_id INT REFERENCES rename_fk_schema.parent_table(id)
				);`,
				`INSERT INTO rename_fk_schema.parent_table VALUES (1);`,
				`INSERT INTO rename_fk_schema.child_table VALUES (10, 1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE rename_fk_schema.parent_table RENAME TO renamed_parent;`,
				},
				{
					Query: `INSERT INTO rename_fk_schema.child_table VALUES (11, 1);`,
				},
				{
					Query:       `INSERT INTO rename_fk_schema.child_table VALUES (12, 99);`,
					ExpectedErr: `Foreign key violation`,
				},
			},
		},
	})
}

// TestAlterSchemaRenameRepro reproduces a schema DDL correctness bug:
// PostgreSQL supports ALTER SCHEMA ... RENAME TO and keeps contained objects
// accessible through the new schema name.
func TestAlterSchemaRenameRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER SCHEMA RENAME TO renames namespace",
			SetUpScript: []string{
				`CREATE SCHEMA rename_namespace_old;`,
				`CREATE TABLE rename_namespace_old.items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO rename_namespace_old.items VALUES (1, 'kept');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER SCHEMA rename_namespace_old
						RENAME TO rename_namespace_new;`,
				},
				{
					Query: `SELECT id, label
						FROM rename_namespace_new.items;`,
					Expected: []sql.Row{{1, "kept"}},
				},
				{
					Query:       `SELECT id, label FROM rename_namespace_old.items;`,
					ExpectedErr: `not found`,
				},
			},
		},
		{
			Name: "ALTER SCHEMA RENAME TO rejects existing target",
			SetUpScript: []string{
				`CREATE SCHEMA rename_namespace_collision_old;`,
				`CREATE SCHEMA rename_namespace_collision_new;`,
				`CREATE TABLE rename_namespace_collision_old.items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO rename_namespace_collision_old.items VALUES (1, 'kept');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER SCHEMA rename_namespace_collision_old
						RENAME TO rename_namespace_collision_new;`,
					ExpectedErr: `schema exists`,
				},
				{
					Query: `SELECT id, label
						FROM rename_namespace_collision_old.items;`,
					Expected: []sql.Row{{1, "kept"}},
				},
			},
		},
	})
}

// TestAlterTableSetSchemaMovesRelationRepro reproduces a DDL correctness gap:
// PostgreSQL supports moving a relation to another schema with ALTER TABLE ...
// SET SCHEMA.
func TestAlterTableSetSchemaMovesRelationRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TABLE SET SCHEMA moves relation",
			SetUpScript: []string{
				`CREATE SCHEMA move_target_schema;`,
				`CREATE TABLE alter_set_schema_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO alter_set_schema_items VALUES (1, 'moved');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE alter_set_schema_items SET SCHEMA move_target_schema;`,
				},
				{
					Query: `SELECT id, label
						FROM move_target_schema.alter_set_schema_items;`,
					Expected: []sql.Row{{1, "moved"}},
				},
				{
					Query:       `SELECT id, label FROM public.alter_set_schema_items;`,
					ExpectedErr: `not found`,
				},
			},
		},
	})
}

// TestAlterViewSetSchemaMovesViewRepro reproduces a DDL correctness gap:
// PostgreSQL supports moving a view to another schema with ALTER VIEW ...
// SET SCHEMA.
func TestAlterViewSetSchemaMovesViewRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER VIEW SET SCHEMA moves view",
			SetUpScript: []string{
				`CREATE SCHEMA move_view_target_schema;`,
				`CREATE TABLE alter_view_set_schema_source (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO alter_view_set_schema_source VALUES (1, 'visible');`,
				`CREATE VIEW alter_view_set_schema_reader AS
					SELECT id, label FROM alter_view_set_schema_source;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER VIEW alter_view_set_schema_reader SET SCHEMA move_view_target_schema;`,
				},
				{
					Query: `SELECT id, label
						FROM move_view_target_schema.alter_view_set_schema_reader;`,
					Expected: []sql.Row{{1, "visible"}},
				},
				{
					Query:       `SELECT id, label FROM public.alter_view_set_schema_reader;`,
					ExpectedErr: `not found`,
				},
			},
		},
	})
}

// TestAlterMaterializedViewSetSchemaMovesViewRepro reproduces a DDL correctness
// gap: PostgreSQL supports moving a materialized view to another schema with
// ALTER MATERIALIZED VIEW ... SET SCHEMA.
func TestAlterMaterializedViewSetSchemaMovesViewRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER MATERIALIZED VIEW SET SCHEMA moves materialized view",
			SetUpScript: []string{
				`CREATE SCHEMA move_matview_target_schema;`,
				`CREATE TABLE alter_matview_set_schema_source (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO alter_matview_set_schema_source VALUES (1, 'materialized');`,
				`CREATE MATERIALIZED VIEW alter_matview_set_schema_reader AS
					SELECT id, label FROM alter_matview_set_schema_source;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER MATERIALIZED VIEW alter_matview_set_schema_reader
						SET SCHEMA move_matview_target_schema;`,
				},
				{
					Query: `SELECT id, label
						FROM move_matview_target_schema.alter_matview_set_schema_reader;`,
					Expected: []sql.Row{{1, "materialized"}},
				},
				{
					Query:       `SELECT id, label FROM public.alter_matview_set_schema_reader;`,
					ExpectedErr: `not found`,
				},
			},
		},
	})
}

// TestAlterSequenceSetSchemaMovesSequenceRepro reproduces a DDL correctness
// gap: PostgreSQL supports moving a sequence to another schema with ALTER
// SEQUENCE ... SET SCHEMA.
func TestAlterSequenceSetSchemaMovesSequenceRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER SEQUENCE SET SCHEMA moves sequence",
			SetUpScript: []string{
				`CREATE SCHEMA move_sequence_target_schema;`,
				`CREATE SEQUENCE alter_sequence_set_schema_seq START 10;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER SEQUENCE alter_sequence_set_schema_seq SET SCHEMA move_sequence_target_schema;`,
				},
				{
					Query:    `SELECT nextval('move_sequence_target_schema.alter_sequence_set_schema_seq');`,
					Expected: []sql.Row{{int64(10)}},
				},
				{
					Query:       `SELECT nextval('public.alter_sequence_set_schema_seq');`,
					ExpectedErr: `does not exist`,
				},
			},
		},
	})
}

// TestAlterFunctionSetSchemaMovesFunctionRepro reproduces a DDL correctness
// gap: PostgreSQL supports moving a function to another schema with ALTER
// FUNCTION ... SET SCHEMA.
func TestAlterFunctionSetSchemaMovesFunctionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER FUNCTION SET SCHEMA moves function",
			SetUpScript: []string{
				`CREATE SCHEMA move_function_target_schema;`,
				`CREATE FUNCTION alter_function_set_schema_value()
					RETURNS integer
					LANGUAGE SQL
					AS $$ SELECT 7 $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER FUNCTION alter_function_set_schema_value()
						SET SCHEMA move_function_target_schema;`,
				},
				{
					Query:    `SELECT move_function_target_schema.alter_function_set_schema_value();`,
					Expected: []sql.Row{{7}},
				},
				{
					Query:       `SELECT public.alter_function_set_schema_value();`,
					ExpectedErr: `not found`,
				},
			},
		},
	})
}

// TestAlterProcedureSetSchemaMovesProcedureRepro reproduces a DDL correctness
// gap: PostgreSQL supports moving a procedure to another schema with ALTER
// PROCEDURE ... SET SCHEMA.
func TestAlterProcedureSetSchemaMovesProcedureRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER PROCEDURE SET SCHEMA moves procedure",
			SetUpScript: []string{
				`CREATE SCHEMA move_procedure_target_schema;`,
				`CREATE TABLE alter_procedure_set_schema_audit (
					id INT PRIMARY KEY
				);`,
				`CREATE PROCEDURE alter_procedure_set_schema_log()
					LANGUAGE SQL
					AS $$ INSERT INTO alter_procedure_set_schema_audit VALUES (1) $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER PROCEDURE alter_procedure_set_schema_log()
						SET SCHEMA move_procedure_target_schema;`,
				},
				{
					Query: `CALL move_procedure_target_schema.alter_procedure_set_schema_log();`,
				},
				{
					Query:    `SELECT COUNT(*) FROM alter_procedure_set_schema_audit;`,
					Expected: []sql.Row{{1}},
				},
				{
					Query:       `CALL public.alter_procedure_set_schema_log();`,
					ExpectedErr: `does not exist`,
				},
			},
		},
	})
}

// TestAlterRoutineSetSchemaMovesFunctionRepro reproduces a DDL correctness
// gap: PostgreSQL supports moving a function through the generic ALTER ROUTINE
// ... SET SCHEMA syntax.
func TestAlterRoutineSetSchemaMovesFunctionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER ROUTINE SET SCHEMA moves function",
			SetUpScript: []string{
				`CREATE SCHEMA move_routine_target_schema;`,
				`CREATE FUNCTION alter_routine_set_schema_value()
					RETURNS integer
					LANGUAGE SQL
					AS $$ SELECT 11 $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER ROUTINE alter_routine_set_schema_value()
						SET SCHEMA move_routine_target_schema;`,
				},
				{
					Query:    `SELECT move_routine_target_schema.alter_routine_set_schema_value();`,
					Expected: []sql.Row{{11}},
				},
				{
					Query:       `SELECT public.alter_routine_set_schema_value();`,
					ExpectedErr: `not found`,
				},
			},
		},
	})
}

// TestAlterTypeSetSchemaMovesEnumRepro reproduces a DDL correctness gap:
// PostgreSQL supports moving an enum type to another schema with ALTER TYPE ...
// SET SCHEMA.
func TestAlterTypeSetSchemaMovesEnumRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TYPE SET SCHEMA moves enum",
			SetUpScript: []string{
				`CREATE SCHEMA move_type_target_schema;`,
				`CREATE TYPE alter_type_set_schema_enum AS ENUM ('one', 'two');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TYPE alter_type_set_schema_enum SET SCHEMA move_type_target_schema;`,
				},
				{
					Query:    `SELECT 'one'::move_type_target_schema.alter_type_set_schema_enum::text;`,
					Expected: []sql.Row{{"one"}},
				},
				{
					Query:       `SELECT 'one'::public.alter_type_set_schema_enum::text;`,
					ExpectedErr: `does not exist`,
				},
			},
		},
	})
}

// TestAlterDomainSetSchemaMovesDomainRepro reproduces a DDL correctness gap:
// PostgreSQL supports moving a domain to another schema with ALTER DOMAIN ...
// SET SCHEMA.
func TestAlterDomainSetSchemaMovesDomainRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER DOMAIN SET SCHEMA moves domain",
			SetUpScript: []string{
				`CREATE SCHEMA move_domain_target_schema;`,
				`CREATE DOMAIN alter_domain_set_schema_positive AS integer
					CONSTRAINT alter_domain_set_schema_positive_check CHECK (VALUE > 0);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER DOMAIN alter_domain_set_schema_positive SET SCHEMA move_domain_target_schema;`,
				},
				{
					Query:    `SELECT 5::move_domain_target_schema.alter_domain_set_schema_positive;`,
					Expected: []sql.Row{{5}},
				},
				{
					Query:       `SELECT 5::public.alter_domain_set_schema_positive;`,
					ExpectedErr: `does not exist`,
				},
			},
		},
	})
}
