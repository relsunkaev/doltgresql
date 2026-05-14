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

// TestCreateExtensionVersionOptionRepro reproduces an extension compatibility
// gap: PostgreSQL accepts the VERSION option syntax and validates the requested
// version against extension install scripts.
func TestCreateExtensionVersionOptionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "CREATE EXTENSION validates requested version",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE EXTENSION hstore VERSION "999.0";`, PostgresOracle: ScriptTestPostgresOracle{ID: "extension-dependency-repro-test-testcreateextensionversionoptionrepro-0001-create-extension-hstore-version-999.0", Compare: "sqlstate"},
				},
				{
					Query: `CREATE EXTENSION hstore VERSION "1.8";`,
				},
				{
					Query: `SELECT extversion FROM pg_catalog.pg_extension WHERE extname = 'hstore';`, PostgresOracle: ScriptTestPostgresOracle{ID: "extension-dependency-repro-test-testcreateextensionversionoptionrepro-0002-select-extversion-from-pg_catalog.pg_extension-where"},
				},
			},
		},
	})
}

// TestCreateExtensionHstoreWithSchemaQualifiesRuntimeObjectsRepro reproduces
// an extension schema relocation gap: hstore member functions and operators
// should be created in the extension's target schema.
func TestCreateExtensionHstoreWithSchemaQualifiesRuntimeObjectsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "hstore WITH SCHEMA qualifies runtime objects",
			SetUpScript: []string{
				`CREATE SCHEMA extensions;`,
				`CREATE EXTENSION hstore WITH SCHEMA extensions;`,
				`CREATE TABLE hstore_schema_qualified_items (
					id INT PRIMARY KEY,
					attrs extensions.hstore
				);`,
				`INSERT INTO hstore_schema_qualified_items VALUES (1, '"A"=>"2", "B"=>"5"');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT to_regtype('extensions.hstore')::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "extension-dependency-repro-test-testcreateextensionhstorewithschemaqualifiesruntimeobjectsrepro-0001-select-to_regtype-extensions.hstore-::text"},
				},
				{
					Query: `SELECT attrs::text, extensions.fetchval(attrs, 'A') FROM hstore_schema_qualified_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "extension-dependency-repro-test-testcreateextensionhstorewithschemaqualifiesruntimeobjectsrepro-0002-select-attrs::text-extensions.fetchval-attrs-a"},
				},
				{
					Query: `SELECT attrs OPERATOR(extensions.?) 'B' FROM hstore_schema_qualified_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "extension-dependency-repro-test-testcreateextensionhstorewithschemaqualifiesruntimeobjectsrepro-0003-select-attrs-operator-extensions.?-b"},
				},
			},
		},
	})
}

// TestCreateExtensionCitextWithSchemaRegtypeRepro reproduces an extension
// schema relocation catalog gap: citext can be used from a target schema, but
// regtype lookup should resolve the relocated extension type too.
func TestCreateExtensionCitextWithSchemaRegtypeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "citext WITH SCHEMA qualifies regtype lookup",
			SetUpScript: []string{
				`CREATE SCHEMA extensions;`,
				`CREATE EXTENSION citext WITH SCHEMA extensions;`,
				`CREATE TABLE citext_schema_qualified_items (
					id INT PRIMARY KEY,
					email extensions.citext UNIQUE
				);`,
				`INSERT INTO citext_schema_qualified_items VALUES (1, 'Alice@Example.com');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT to_regtype('extensions.citext')::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "extension-dependency-repro-test-testcreateextensioncitextwithschemaregtyperepro-0001-select-to_regtype-extensions.citext-::text"},
				},
				{
					Query: `SELECT id FROM citext_schema_qualified_items WHERE email = 'alice@example.com'::extensions.citext;`, PostgresOracle: ScriptTestPostgresOracle{ID: "extension-dependency-repro-test-testcreateextensioncitextwithschemaregtyperepro-0002-select-id-from-citext_schema_qualified_items-where"},
				},
				{
					Query: `INSERT INTO citext_schema_qualified_items VALUES (2, 'ALICE@example.com');`, PostgresOracle: ScriptTestPostgresOracle{ID: "extension-dependency-repro-test-testcreateextensioncitextwithschemaregtyperepro-0003-insert-into-citext_schema_qualified_items-values-2",

						// TestCreateExtensionVectorWithSchemaQualifiesTypesRepro reproduces an
						// extension schema relocation gap: pgvector member types should be created in
						// the extension's target schema.
						Compare: "sqlstate"},
				},
				{
					Query: `SET search_path TO extensions, public, pg_catalog;`,
				},
				{
					Query:    `SELECT ('Alice@Example.com'::citext = 'alice@example.com'::citext)::text;`,
					Expected: []sql.Row{{"true"}},
				},
			},
		},
	})
}

func TestCreateExtensionVectorWithSchemaQualifiesTypesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "vector WITH SCHEMA qualifies vector type",
			SetUpScript: []string{
				`CREATE SCHEMA extensions;`,
				`CREATE EXTENSION vector WITH SCHEMA extensions;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT e.extname, n.nspname
						FROM pg_catalog.pg_extension e
						JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
						WHERE e.extname = 'vector';`,
					Expected: []sql.Row{{"vector", "extensions"}},
				},
				{
					Query:    `SELECT to_regtype('extensions.vector')::text;`,
					Expected: []sql.Row{{"extensions.vector"}},
				},
				{
					Query:    `CREATE TABLE vector_schema_qualified_items (id INT PRIMARY KEY, embedding extensions.vector(3));`,
					Expected: []sql.Row{},
				},
			},
		},
	})
}

// TestAlterExtensionSetSchemaMovesObjectsRepro reproduces an extension
// compatibility gap: ALTER EXTENSION ... SET SCHEMA should move a relocatable
// extension and its member objects to the target schema.
func TestAlterExtensionSetSchemaMovesObjectsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER EXTENSION SET SCHEMA moves extension objects",
			SetUpScript: []string{
				`CREATE SCHEMA extension_move_target;`,
				`CREATE EXTENSION hstore WITH SCHEMA public;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER EXTENSION hstore SET SCHEMA extension_move_target;`,
				},
				{
					Query: `SELECT n.nspname
						FROM pg_catalog.pg_extension e
						JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
						WHERE e.extname = 'hstore';`,
					Expected: []sql.Row{{"extension_move_target"}},
				},
				{
					Query:    `SELECT to_regtype('extension_move_target.hstore')::text;`,
					Expected: []sql.Row{{"extension_move_target.hstore"}},
				},
				{
					Query:    `SELECT to_regtype('public.hstore')::text;`,
					Expected: []sql.Row{{nil}},
				},
			},
		},
	})
}

// TestAlterFunctionDependsOnExtensionRepro reproduces a routine dependency
// gap: ALTER FUNCTION ... DEPENDS ON EXTENSION should record a pg_depend edge,
// block DROP EXTENSION by default, and remove the function on CASCADE.
func TestAlterFunctionDependsOnExtensionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER FUNCTION DEPENDS ON EXTENSION records dependency",
			SetUpScript: []string{
				`CREATE EXTENSION hstore WITH SCHEMA public;`,
				`CREATE FUNCTION extension_dependent_function()
					RETURNS INT LANGUAGE SQL AS $$ SELECT 42 $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER FUNCTION extension_dependent_function()
						DEPENDS ON EXTENSION hstore;`,
				},
				{
					Query: `SELECT d.deptype
						FROM pg_catalog.pg_depend d
						JOIN pg_catalog.pg_proc p ON p.oid = d.objid
						JOIN pg_catalog.pg_extension e ON e.oid = d.refobjid
						WHERE p.proname = 'extension_dependent_function'
							AND e.extname = 'hstore';`,
					Expected: []sql.Row{{"x"}},
				},
				{
					Query:       `DROP EXTENSION hstore;`,
					ExpectedErr: `depend`,
				},
				{
					Query:    `DROP EXTENSION hstore CASCADE;`,
					Expected: []sql.Row{},
				},
				{
					Query: `SELECT count(*) = 0
						FROM pg_catalog.pg_proc
						WHERE proname = 'extension_dependent_function';`,
					Expected: []sql.Row{{"t"}},
				},
			},
		},
		{
			Name: "ALTER FUNCTION NO DEPENDS removes dependency",
			SetUpScript: []string{
				`CREATE EXTENSION hstore WITH SCHEMA public;`,
				`CREATE FUNCTION extension_independent_function()
					RETURNS INT LANGUAGE SQL AS $$ SELECT 7 $$;`,
				`ALTER FUNCTION extension_independent_function()
					DEPENDS ON EXTENSION hstore;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER FUNCTION extension_independent_function()
						NO DEPENDS ON EXTENSION hstore;`,
				},
				{
					Query: `SELECT count(*) = 0
						FROM pg_catalog.pg_depend d
						JOIN pg_catalog.pg_proc p ON p.oid = d.objid
						JOIN pg_catalog.pg_extension e ON e.oid = d.refobjid
						WHERE p.proname = 'extension_independent_function'
							AND e.extname = 'hstore';`,
					Expected: []sql.Row{{"t"}},
				},
				{
					Query:    `DROP EXTENSION hstore;`,
					Expected: []sql.Row{},
				},
				{
					Query: `SELECT count(*) = 1
						FROM pg_catalog.pg_proc
						WHERE proname = 'extension_independent_function';`,
					Expected: []sql.Row{{"t"}},
				},
			},
		},
	})
}

// TestDropExtensionRestrictRejectsDependentObjectsRepro reproduces an
// extension dependency bug: PostgreSQL's default RESTRICT behavior prevents
// dropping an extension while user objects depend on extension member objects.
func TestDropExtensionRestrictRejectsDependentObjectsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP EXTENSION rejects dependent objects by default",
			SetUpScript: []string{
				`CREATE EXTENSION hstore WITH SCHEMA public;`,
				`CREATE TABLE hstore_extension_dependents (
					id INT PRIMARY KEY,
					payload public.hstore
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DROP EXTENSION hstore;`,
					ExpectedErr: `depend`,
				},
				{
					Query: `SELECT extname
						FROM pg_catalog.pg_extension
						WHERE extname = 'hstore';`,
					Expected: []sql.Row{{"hstore"}},
				},
				{
					Query:    `SELECT to_regtype('public.hstore') IS NOT NULL;`,
					Expected: []sql.Row{{"t"}},
				},
			},
		},
	})
}

// TestDropExtensionCascadeRemovesDependentColumnsRepro reproduces an extension
// dependency bug: DROP EXTENSION ... CASCADE should remove user objects that
// depend on extension member objects, including columns of extension types.
func TestDropExtensionCascadeRemovesDependentColumnsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP EXTENSION CASCADE removes dependent columns",
			SetUpScript: []string{
				`CREATE EXTENSION hstore WITH SCHEMA public;`,
				`CREATE TABLE hstore_extension_cascade_dependents (
					id INT PRIMARY KEY,
					payload public.hstore
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP EXTENSION hstore CASCADE;`,
				},
				{
					Query: `SELECT column_name
						FROM information_schema.columns
						WHERE table_schema = 'public'
							AND table_name = 'hstore_extension_cascade_dependents'
							AND column_name = 'payload';`,
					Expected: []sql.Row{},
				},
				{
					Query:    `SELECT to_regtype('public.hstore') IS NULL;`,
					Expected: []sql.Row{{"t"}},
				},
			},
		},
	})
}

// TestDropExtensionMemberTypeRequiresDropExtensionRepro reproduces an
// extension dependency bug: extension member objects should not be dropped
// directly while their owning extension is installed.
func TestDropExtensionMemberTypeRequiresDropExtensionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP TYPE rejects extension member type",
			SetUpScript: []string{
				`CREATE EXTENSION hstore WITH SCHEMA public;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DROP TYPE public.hstore;`,
					ExpectedErr: `extension`,
				},
				{
					Query: `SELECT extname
						FROM pg_catalog.pg_extension
						WHERE extname = 'hstore';`,
					Expected: []sql.Row{{"hstore"}},
				},
				{
					Query:    `SELECT to_regtype('public.hstore') IS NOT NULL;`,
					Expected: []sql.Row{{"t"}},
				},
			},
		},
	})
}

// TestCreateExtensionRejectsSchemaForNonRelocatableExtensionRepro reproduces
// an extension correctness bug: non-relocatable extensions with a fixed schema
// should reject an explicit conflicting schema.
func TestCreateExtensionRejectsSchemaForNonRelocatableExtensionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE EXTENSION rejects explicit schema for non-relocatable extension",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE EXTENSION plpgsql WITH SCHEMA public;`, PostgresOracle: ScriptTestPostgresOracle{ID: "extension-dependency-repro-test-testcreateextensionrejectsschemafornonrelocatableextensionrepro-0001-create-extension-plpgsql-with-schema", Compare: "sqlstate"},
				},
				{
					Query: `SELECT n.nspname
						FROM pg_catalog.pg_extension e
						JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
						WHERE e.extname = 'plpgsql';`, PostgresOracle: ScriptTestPostgresOracle{ID: "extension-dependency-repro-test-testcreateextensionrejectsschemafornonrelocatableextensionrepro-0002-select-n.nspname-from-pg_catalog.pg_extension-e"},
				},
			},
		},
	})
}
