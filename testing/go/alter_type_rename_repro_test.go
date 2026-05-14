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

// PostgreSQL can rename enum labels without recreating the type. Doltgres
// currently rejects ALTER TYPE before the label can be persisted.
func TestAlterEnumRenameValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TYPE RENAME VALUE updates enum label",
			SetUpScript: []string{
				`CREATE TYPE rename_enum_status AS ENUM ('new', 'done');`,
				`ALTER TYPE rename_enum_status RENAME VALUE 'done' TO 'archived';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT enumlabel
						FROM pg_catalog.pg_enum
						WHERE enumtypid = 'rename_enum_status'::regtype
						ORDER BY enumsortorder;`, PostgresOracle: ScriptTestPostgresOracle{ID: "alter-type-rename-repro-test-testalterenumrenamevaluerepro-0001-select-enumlabel-from-pg_catalog.pg_enum-where"},
				},
			},
		},
	})
}

// TestAlterCompositeTypeRenameAttribute guards that ALTER TYPE RENAME
// ATTRIBUTE updates the composite attribute name and that the renamed
// attribute is accessible via row-field selection.
func TestAlterCompositeTypeRenameAttribute(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TYPE RENAME ATTRIBUTE updates composite field",
			SetUpScript: []string{
				`CREATE TYPE rename_composite_item AS (old_name INT);`,
				`ALTER TYPE rename_composite_item RENAME ATTRIBUTE old_name TO new_name;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT (ROW(7)::rename_composite_item).new_name;`, PostgresOracle: ScriptTestPostgresOracle{ID: "alter-type-rename-repro-test-testaltercompositetyperenameattribute-0001-select-row-7-::rename_composite_item-.new_name"},
				},
				{
					Query: `SELECT (ROW(7)::rename_composite_item).old_name;`, PostgresOracle: ScriptTestPostgresOracle{ID: "alter-type-rename-repro-test-testaltercompositetyperenameattribute-0002-select-row-7-::rename_composite_item-.old_name",

						// TestAlterCompositeTypeRenameAttributeMultipleFields guards that renaming
						// one attribute on a multi-attribute composite type leaves every other
						// attribute readable under its original name.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestAlterCompositeTypeRenameAttributeMultipleFields(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "RENAME ATTRIBUTE preserves siblings",
			SetUpScript: []string{
				`CREATE TYPE rename_attr_multi AS (a INT, b TEXT, c INT);`,
				`ALTER TYPE rename_attr_multi RENAME ATTRIBUTE b TO renamed_b;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT (ROW(1, 'kept', 3)::rename_attr_multi).a;`, PostgresOracle: ScriptTestPostgresOracle{ID: "alter-type-rename-repro-test-testaltercompositetyperenameattributemultiplefields-0001-select-row-1-kept-3"},
				},
				{
					Query: `SELECT (ROW(1, 'kept', 3)::rename_attr_multi).renamed_b;`, PostgresOracle: ScriptTestPostgresOracle{ID: "alter-type-rename-repro-test-testaltercompositetyperenameattributemultiplefields-0002-select-row-1-kept-3"},
				},
				{
					Query: `SELECT (ROW(1, 'kept', 3)::rename_attr_multi).c;`, PostgresOracle: ScriptTestPostgresOracle{ID: "alter-type-rename-repro-test-testaltercompositetyperenameattributemultiplefields-0003-select-row-1-kept-3"},
				},
			},
		},
	})
}

// TestAlterCompositeTypeRenameAttributeMissingErrors guards that renaming a
// non-existent attribute errors with PostgreSQL's "column does not exist"
// SQLSTATE.
func TestAlterCompositeTypeRenameAttributeMissingErrors(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "RENAME ATTRIBUTE on missing attribute errors",
			SetUpScript: []string{
				`CREATE TYPE rename_attr_missing AS (a INT);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TYPE rename_attr_missing RENAME ATTRIBUTE not_a_field TO new_field;`, PostgresOracle: ScriptTestPostgresOracle{ID: "alter-type-rename-repro-test-testaltercompositetyperenameattributemissingerrors-0001-alter-type-rename_attr_missing-rename-attribute",

						// TestAlterCompositeTypeRenameAttributeCollisionErrors guards that renaming an
						// attribute to a name already used by another attribute on the same composite
						// type errors with PostgreSQL's "already exists" message rather than silently
						// shadowing the existing attribute.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestAlterCompositeTypeRenameAttributeCollisionErrors(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "RENAME ATTRIBUTE to existing attribute name errors",
			SetUpScript: []string{
				`CREATE TYPE rename_attr_collision AS (a INT, b INT);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TYPE rename_attr_collision RENAME ATTRIBUTE a TO b;`, PostgresOracle: ScriptTestPostgresOracle{ID: "alter-type-rename-repro-test-testaltercompositetyperenameattributecollisionerrors-0001-alter-type-rename_attr_collision-rename-attribute",

						// TestAlterCompositeTypeRenameAttributeNonComposite guards that RENAME
						// ATTRIBUTE is rejected when applied to a non-composite user type (enum here)
						// so that the operation does not silently corrupt enum metadata.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestAlterCompositeTypeRenameAttributeNonComposite(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "RENAME ATTRIBUTE on non-composite type errors",
			SetUpScript: []string{
				`CREATE TYPE rename_attr_enum AS ENUM ('a', 'b');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TYPE rename_attr_enum RENAME ATTRIBUTE a TO renamed_a;`, PostgresOracle: ScriptTestPostgresOracle{ID: "alter-type-rename-repro-test-testaltercompositetyperenameattributenoncomposite-0001-alter-type-rename_attr_enum-rename-attribute",

						// TestAlterCompositeTypeRenameAttributeSchemaQualified guards that the
						// schema-qualified ALTER TYPE form resolves the type through the search path
						// and renames the attribute in that schema's namespace.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestAlterCompositeTypeRenameAttributeSchemaQualified(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "RENAME ATTRIBUTE works with schema-qualified type name",
			SetUpScript: []string{
				`CREATE SCHEMA rename_attr_schema;`,
				`CREATE TYPE rename_attr_schema.qualified_item AS (old_field INT);`,
				`ALTER TYPE rename_attr_schema.qualified_item RENAME ATTRIBUTE old_field TO new_field;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT (ROW(42)::rename_attr_schema.qualified_item).new_field;`, PostgresOracle: ScriptTestPostgresOracle{ID: "alter-type-rename-repro-test-testaltercompositetyperenameattributeschemaqualified-0001-select-row-42-::rename_attr_schema.qualified_item-.new_field"},
				},
			},
		},
	})
}

// TestAlterCompositeTypeRenameAttributeUpdatesStoredColumnsRepro reproduces a
// catalog persistence bug: renaming a composite type attribute should update
// stored table columns using that composite type.
func TestAlterCompositeTypeRenameAttributeUpdatesStoredColumnsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "RENAME ATTRIBUTE updates stored composite columns",
			SetUpScript: []string{
				`CREATE TYPE rename_attr_stored_item AS (old_name INT);`,
				`CREATE TABLE rename_attr_stored_items (
					id INT PRIMARY KEY,
					payload rename_attr_stored_item
				);`,
				`INSERT INTO rename_attr_stored_items
					VALUES (1, ROW(7)::rename_attr_stored_item);`,
				`ALTER TYPE rename_attr_stored_item
					RENAME ATTRIBUTE old_name TO new_name;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT (payload).new_name
						FROM rename_attr_stored_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "alter-type-rename-repro-test-testaltercompositetyperenameattributeupdatesstoredcolumnsrepro-0001-select-payload-.new_name-from-rename_attr_stored_items"},
				},
				{
					Query: `SELECT (payload).old_name FROM rename_attr_stored_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "alter-type-rename-repro-test-testaltercompositetyperenameattributeupdatesstoredcolumnsrepro-0002-select-payload-.old_name-from-rename_attr_stored_items",

						// TestAlterCompositeTypeRenameAttributeUpdatesViewMetadataRepro reproduces a
						// catalog persistence bug: renaming a composite type attribute should update
						// views that reference that attribute.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestAlterCompositeTypeRenameAttributeUpdatesViewMetadataRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "RENAME ATTRIBUTE updates dependent view metadata",
			SetUpScript: []string{
				`CREATE TYPE rename_attr_view_item AS (old_name INT);`,
				`CREATE VIEW rename_attr_view_reader AS
					SELECT (ROW(7)::rename_attr_view_item).old_name AS value;`,
				`ALTER TYPE rename_attr_view_item
					RENAME ATTRIBUTE old_name TO new_name;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT value FROM rename_attr_view_reader;`, PostgresOracle: ScriptTestPostgresOracle{ID: "alter-type-rename-repro-test-testaltercompositetyperenameattributeupdatesviewmetadatarepro-0001-select-value-from-rename_attr_view_reader"},
				},
				{
					Query: `SELECT (ROW(7)::rename_attr_view_item).new_name;`, PostgresOracle: ScriptTestPostgresOracle{ID: "alter-type-rename-repro-test-testaltercompositetyperenameattributeupdatesviewmetadatarepro-0002-select-row-7-::rename_attr_view_item-.new_name"},
				},
			},
		},
	})
}

// TestAlterCompositeTypeRenameAttributeUpdatesMaterializedViewMetadataRepro
// reproduces a catalog persistence bug: renaming a composite type attribute
// should update materialized views that reference that attribute so refreshes
// keep working.
func TestAlterCompositeTypeRenameAttributeUpdatesMaterializedViewMetadataRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "RENAME ATTRIBUTE updates dependent materialized view metadata",
			SetUpScript: []string{
				`CREATE TYPE rename_attr_matview_item AS (old_name INT);`,
				`CREATE MATERIALIZED VIEW rename_attr_matview_reader AS
					SELECT (ROW(7)::rename_attr_matview_item).old_name AS value;`,
				`ALTER TYPE rename_attr_matview_item
					RENAME ATTRIBUTE old_name TO new_name;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT value FROM rename_attr_matview_reader;`, PostgresOracle: ScriptTestPostgresOracle{ID: "alter-type-rename-repro-test-testaltercompositetyperenameattributeupdatesmaterializedviewmetadatarepro-0001-select-value-from-rename_attr_matview_reader"},
				},
				{
					Query: `REFRESH MATERIALIZED VIEW rename_attr_matview_reader;`,
				},
				{
					Query: `SELECT value FROM rename_attr_matview_reader;`, PostgresOracle: ScriptTestPostgresOracle{ID: "alter-type-rename-repro-test-testaltercompositetyperenameattributeupdatesmaterializedviewmetadatarepro-0002-select-value-from-rename_attr_matview_reader"},
				},
			},
		},
	})
}

// TestAlterCompositeTypeRenameAttributeUpdatesGeneratedColumnMetadataRepro
// reproduces a catalog persistence bug: renaming a composite type attribute
// should update stored generated column expressions that reference that
// attribute.
func TestAlterCompositeTypeRenameAttributeUpdatesGeneratedColumnMetadataRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "RENAME ATTRIBUTE updates generated column expressions",
			SetUpScript: []string{
				`CREATE TYPE rename_attr_generated_item AS (old_name INT);`,
				`CREATE TABLE rename_attr_generated_items (
					id INT PRIMARY KEY,
					payload rename_attr_generated_item,
					extracted INT GENERATED ALWAYS AS ((payload).old_name) STORED
				);`,
				`INSERT INTO rename_attr_generated_items (id, payload)
					VALUES (1, ROW(7)::rename_attr_generated_item);`,
				`ALTER TYPE rename_attr_generated_item
					RENAME ATTRIBUTE old_name TO new_name;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO rename_attr_generated_items (id, payload)
						VALUES (2, ROW(11)::rename_attr_generated_item);`,
				},
				{
					Query: `SELECT id, (payload).new_name, extracted
						FROM rename_attr_generated_items
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "alter-type-rename-repro-test-testaltercompositetyperenameattributeupdatesgeneratedcolumnmetadatarepro-0001-select-id-payload-.new_name-extracted"},
				},
			},
		},
	})
}

// TestAlterCompositeTypeRenameAttributeUpdatesFunctionBodyMetadataRepro
// reproduces a catalog persistence bug: renaming a composite type attribute
// should update SQL function bodies that reference that attribute.
func TestAlterCompositeTypeRenameAttributeUpdatesFunctionBodyMetadataRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "RENAME ATTRIBUTE updates dependent function bodies",
			SetUpScript: []string{
				`CREATE TYPE rename_attr_function_item AS (old_name INT);`,
				`CREATE FUNCTION rename_attr_function_value()
					RETURNS INT
					LANGUAGE SQL
					IMMUTABLE
					AS $$ SELECT (ROW(7)::rename_attr_function_item).old_name $$;`,
				`ALTER TYPE rename_attr_function_item
					RENAME ATTRIBUTE old_name TO new_name;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT rename_attr_function_value();`, PostgresOracle: ScriptTestPostgresOracle{ID: "alter-type-rename-repro-test-testaltercompositetyperenameattributeupdatesfunctionbodymetadatarepro-0001-select-rename_attr_function_value",

						// PostgreSQL can rename type and domain objects in place. Doltgres should
						// update type lookup metadata so the old name disappears and the new name is
						// usable.
						Compare: "sqlstate"},
				},
				{
					Query: `SELECT (ROW(7)::rename_attr_function_item).new_name;`, PostgresOracle: ScriptTestPostgresOracle{ID: "alter-type-rename-repro-test-testaltercompositetyperenameattributeupdatesfunctionbodymetadatarepro-0002-select-row-7-::rename_attr_function_item-.new_name"},
				},
			},
		},
	})
}

func TestAlterTypeAndDomainRenameToRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TYPE RENAME TO updates enum type lookup",
			SetUpScript: []string{
				`CREATE TYPE rename_enum_object AS ENUM ('new', 'done');`,
				`ALTER TYPE rename_enum_object RENAME TO renamed_enum_object;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT
							to_regtype('rename_enum_object')::text,
							to_regtype('renamed_enum_object')::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "alter-type-rename-repro-test-testaltertypeanddomainrenametorepro-0001-select-to_regtype-rename_enum_object-::text-to_regtype"},
				},
			},
		},
		{
			Name: "ALTER DOMAIN RENAME TO updates domain type lookup",
			SetUpScript: []string{
				`CREATE DOMAIN rename_domain_object AS INT;`,
				`ALTER DOMAIN rename_domain_object RENAME TO renamed_domain_object;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT
							to_regtype('rename_domain_object')::text,
							to_regtype('renamed_domain_object')::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "alter-type-rename-repro-test-testaltertypeanddomainrenametorepro-0002-select-to_regtype-rename_domain_object-::text-to_regtype"},
				},
			},
		},
	})
}

// TestAlterTypeRenameUpdatesExistingColumnMetadataRepro reproduces a catalog
// persistence bug: renaming a type or domain should update existing table
// columns that reference it, so stored rows and new writes keep working through
// the renamed type.
func TestAlterTypeRenameUpdatesExistingColumnMetadataRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TYPE RENAME TO updates existing enum columns",
			SetUpScript: []string{
				`CREATE TYPE rename_column_status AS ENUM ('new', 'done');`,
				`CREATE TABLE rename_column_status_items (
					id INT PRIMARY KEY,
					status rename_column_status
				);`,
				`INSERT INTO rename_column_status_items VALUES (1, 'new');`,
				`ALTER TYPE rename_column_status RENAME TO renamed_column_status;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO rename_column_status_items
						VALUES (2, 'done'::renamed_column_status);`,
				},
				{
					Query: `SELECT id, status::text
						FROM rename_column_status_items
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "alter-type-rename-repro-test-testaltertyperenameupdatesexistingcolumnmetadatarepro-0001-select-id-status::text-from-rename_column_status_items"},
				},
			},
		},
		{
			Name: "ALTER DOMAIN RENAME TO updates existing domain columns",
			SetUpScript: []string{
				`CREATE DOMAIN rename_column_domain AS INT CHECK (VALUE > 0);`,
				`CREATE TABLE rename_column_domain_items (
					id INT PRIMARY KEY,
					amount rename_column_domain
				);`,
				`INSERT INTO rename_column_domain_items VALUES (1, 10);`,
				`ALTER DOMAIN rename_column_domain RENAME TO renamed_column_domain;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO rename_column_domain_items
						VALUES (2, 20::renamed_column_domain);`,
				},
				{
					Query: `INSERT INTO rename_column_domain_items VALUES (3, -1);`, PostgresOracle: ScriptTestPostgresOracle{ID: "alter-type-rename-repro-test-testaltertyperenameupdatesexistingcolumnmetadatarepro-0002-insert-into-rename_column_domain_items-values-3", Compare: "sqlstate"},
				},
				{
					Query: `SELECT id, amount
						FROM rename_column_domain_items
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "alter-type-rename-repro-test-testaltertyperenameupdatesexistingcolumnmetadatarepro-0003-select-id-amount-from-rename_column_domain_items"},
				},
			},
		},
	})
}

// TestAlterTypeRenameUpdatesColumnDefaultMetadataRepro reproduces a catalog
// persistence bug: column defaults that reference a renamed type should keep
// working after ALTER TYPE RENAME TO.
func TestAlterTypeRenameUpdatesColumnDefaultMetadataRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TYPE RENAME TO updates enum column defaults",
			SetUpScript: []string{
				`CREATE TYPE rename_default_status AS ENUM ('new', 'done');`,
				`CREATE TABLE rename_default_status_items (
					id INT PRIMARY KEY,
					status rename_default_status
						DEFAULT 'new'::rename_default_status
				);`,
				`ALTER TYPE rename_default_status RENAME TO renamed_default_status;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO rename_default_status_items (id)
						VALUES (1);`,
				},
				{
					Query: `SELECT id::text, status::text, pg_typeof(status)::text
						FROM rename_default_status_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "alter-type-rename-repro-test-testaltertyperenameupdatescolumndefaultmetadatarepro-0001-select-id::text-status::text-pg_typeof-status"},
				},
			},
		},
	})
}

// TestAlterTypeRenameUpdatesCheckConstraintMetadataRepro reproduces a catalog
// persistence bug: check constraints that reference a renamed type should keep
// resolving through the renamed type after ALTER TYPE RENAME TO.
func TestAlterTypeRenameUpdatesCheckConstraintMetadataRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TYPE RENAME TO updates check constraint expressions",
			SetUpScript: []string{
				`CREATE TYPE rename_check_status AS ENUM ('new', 'done');`,
				`CREATE TABLE rename_check_status_items (
					id INT PRIMARY KEY,
					status TEXT,
					CONSTRAINT rename_check_not_done
						CHECK (status::rename_check_status <> 'done'::rename_check_status)
				);`,
				`ALTER TYPE rename_check_status RENAME TO renamed_check_status;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO rename_check_status_items
						VALUES (1, 'new');`,
				},
				{
					Query: `INSERT INTO rename_check_status_items VALUES (2, 'done');`, PostgresOracle: ScriptTestPostgresOracle{ID: "alter-type-rename-repro-test-testaltertyperenameupdatescheckconstraintmetadatarepro-0001-insert-into-rename_check_status_items-values-2", Compare: "sqlstate"},
				},
				{
					Query: `SELECT id::text, status
						FROM rename_check_status_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "alter-type-rename-repro-test-testaltertyperenameupdatescheckconstraintmetadatarepro-0002-select-id::text-status-from-rename_check_status_items"},
				},
			},
		},
	})
}

// TestAlterTypeRenameUpdatesGeneratedColumnMetadataRepro reproduces a catalog
// persistence bug: stored generated column expressions that reference a renamed
// type should keep resolving through the renamed type.
func TestAlterTypeRenameUpdatesGeneratedColumnMetadataRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TYPE RENAME TO updates generated column expressions",
			SetUpScript: []string{
				`CREATE TYPE rename_generated_status AS ENUM ('new', 'done');`,
				`CREATE TABLE rename_generated_status_items (
					id INT PRIMARY KEY,
					status TEXT,
					normalized TEXT GENERATED ALWAYS AS (
						(status::rename_generated_status)::text
					) STORED
				);`,
				`INSERT INTO rename_generated_status_items (id, status)
					VALUES (1, 'new');`,
				`ALTER TYPE rename_generated_status RENAME TO renamed_generated_status;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO rename_generated_status_items (id, status)
						VALUES (2, 'done');`,
				},
				{
					Query: `SELECT id::text, status, normalized
						FROM rename_generated_status_items
						ORDER BY id;`,
					Expected: []sql.Row{{"1", "new", "new"}, {"2", "done", "done"}},
				},
			},
		},
	})
}

// TestAlterTypeRenameUpdatesExpressionIndexMetadataRepro reproduces a catalog
// persistence bug: expression indexes that reference a renamed type should keep
// resolving through the renamed type after ALTER TYPE RENAME TO.
func TestAlterTypeRenameUpdatesExpressionIndexMetadataRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TYPE RENAME TO updates expression index metadata",
			SetUpScript: []string{
				`CREATE TYPE rename_index_status AS ENUM ('new', 'done');`,
				`CREATE TABLE rename_index_status_items (
					id INT PRIMARY KEY,
					status TEXT
				);`,
				`CREATE INDEX rename_index_status_expr_idx
					ON rename_index_status_items (((status::rename_index_status)::text));`,
				`ALTER TYPE rename_index_status RENAME TO renamed_index_status;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO rename_index_status_items
						VALUES (1, 'new');`,
				},
				{
					Query: `SELECT id::text, status
						FROM rename_index_status_items;`,
					Expected: []sql.Row{{"1", "new"}},
				},
			},
		},
	})
}

// TestAlterTypeRenameUpdatesFunctionSignatureMetadataRepro reproduces a
// catalog persistence bug: renaming a type should update function signatures
// that reference it, so overload lookup with the renamed type keeps working.
func TestAlterTypeRenameUpdatesFunctionSignatureMetadataRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TYPE RENAME TO updates function signatures",
			SetUpScript: []string{
				`CREATE TYPE rename_function_status AS ENUM ('new', 'done');`,
				`CREATE FUNCTION rename_function_status_echo(input_status rename_function_status)
					RETURNS rename_function_status
					LANGUAGE SQL
					IMMUTABLE
					AS $$ SELECT input_status $$;`,
				`ALTER TYPE rename_function_status RENAME TO renamed_function_status;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT to_regprocedure(
							'rename_function_status_echo(renamed_function_status)'
						)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "alter-type-rename-repro-test-testaltertyperenameupdatesfunctionsignaturemetadatarepro-0001-select-to_regprocedure-rename_function_status_echo-renamed_function_status-::text"},
				},
				{
					Query: `SELECT rename_function_status_echo('done'::renamed_function_status)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "alter-type-rename-repro-test-testaltertyperenameupdatesfunctionsignaturemetadatarepro-0002-select-rename_function_status_echo-done-::renamed_function_status-::text"},
				},
			},
		},
	})
}

// TestAlterTypeRenameUpdatesViewMetadataRepro reproduces a catalog persistence
// bug: views that reference a renamed type should keep resolving through the
// renamed type rather than retaining a stale textual reference to the old name.
func TestAlterTypeRenameUpdatesViewMetadataRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TYPE RENAME TO updates view metadata",
			SetUpScript: []string{
				`CREATE TYPE rename_view_status AS ENUM ('new', 'done');`,
				`CREATE VIEW rename_view_status_view AS
					SELECT 'done'::rename_view_status AS status;`,
				`ALTER TYPE rename_view_status RENAME TO renamed_view_status;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT status::text FROM rename_view_status_view;`, PostgresOracle: ScriptTestPostgresOracle{ID: "alter-type-rename-repro-test-testaltertyperenameupdatesviewmetadatarepro-0001-select-status::text-from-rename_view_status_view"},
				},
				{
					Query: `SELECT pg_typeof(status)::text
						FROM rename_view_status_view;`, PostgresOracle: ScriptTestPostgresOracle{ID: "alter-type-rename-repro-test-testaltertyperenameupdatesviewmetadatarepro-0002-select-pg_typeof-status-::text-from"},
				},
			},
		},
	})
}

// TestAlterTypeRenameUpdatesMaterializedViewMetadataRepro reproduces a catalog
// persistence bug: materialized views that reference a renamed type should keep
// reading and refreshing through the renamed type rather than retaining the old
// textual type name.
func TestAlterTypeRenameUpdatesMaterializedViewMetadataRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TYPE RENAME TO updates materialized view metadata",
			SetUpScript: []string{
				`CREATE TYPE rename_matview_status AS ENUM ('new', 'done');`,
				`CREATE MATERIALIZED VIEW rename_matview_status_view AS
					SELECT 'done'::rename_matview_status AS status;`,
				`ALTER TYPE rename_matview_status RENAME TO renamed_matview_status;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT status::text FROM rename_matview_status_view;`, PostgresOracle: ScriptTestPostgresOracle{ID: "alter-type-rename-repro-test-testaltertyperenameupdatesmaterializedviewmetadatarepro-0001-select-status::text-from-rename_matview_status_view"},
				},
				{
					Query: `SELECT pg_typeof(status)::text
						FROM rename_matview_status_view;`, PostgresOracle: ScriptTestPostgresOracle{ID: "alter-type-rename-repro-test-testaltertyperenameupdatesmaterializedviewmetadatarepro-0002-select-pg_typeof-status-::text-from"},
				},
				{
					Query: `REFRESH MATERIALIZED VIEW rename_matview_status_view;`,
				},
				{
					Query: `SELECT status::text FROM rename_matview_status_view;`, PostgresOracle: ScriptTestPostgresOracle{ID: "alter-type-rename-repro-test-testaltertyperenameupdatesmaterializedviewmetadatarepro-0003-select-status::text-from-rename_matview_status_view"},
				},
			},
		},
	})
}
