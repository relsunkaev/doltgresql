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

// TestPgClassNamespaceOidLookupDoesNotRequireWarmCacheRepro guards
// pg_class.relnamespace comparisons against a schema OID literal.
func TestPgClassNamespaceOidLookupDoesNotRequireWarmCacheRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_class namespace OID lookup does not require warmed cache",
			SetUpScript: []string{
				`CREATE SCHEMA testschema;`,
				`CREATE TABLE testschema.testtable (id INT PRIMARY KEY, v1 TEXT);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT c.relname
						FROM pg_catalog.pg_class c
						WHERE c.relnamespace = 2638679668
							AND c.relname = 'testtable';`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testpgclassnamespaceoidlookupdoesnotrequirewarmcacherepro-0001-select-c.relname-from-pg_catalog.pg_class-c"},
				},
			},
		},
	})
}

// TestQuotedSchemaNamesAreCaseSensitiveRepro reproduces an identifier/catalog
// bug: quoted schema names preserve case, so "CaseSchema" and caseschema are
// distinct PostgreSQL schemas.
func TestQuotedSchemaNamesAreCaseSensitiveRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "quoted schema names remain distinct from folded unquoted names",
			SetUpScript: []string{
				`CREATE SCHEMA "CaseSchema";`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE SCHEMA caseschema;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testquotedschemanamesarecasesensitiverepro-0001-create-schema-caseschema"},
				},
				{
					Query: `SELECT nspname
						FROM pg_catalog.pg_namespace
						WHERE nspname IN ('CaseSchema', 'caseschema')
						ORDER BY nspname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testquotedschemanamesarecasesensitiverepro-0002-select-nspname-from-pg_catalog.pg_namespace-where"},
				},
			},
		},
	})
}

// TestQuotedDatabaseNamesAreCaseSensitiveRepro reproduces an identifier/catalog
// bug: quoted database names preserve case, so "CaseDatabase" and casedatabase
// are distinct PostgreSQL databases.
func TestQuotedDatabaseNamesAreCaseSensitiveRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "quoted database names remain distinct from folded unquoted names",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE DATABASE "CaseDatabase";`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testquoteddatabasenamesarecasesensitiverepro-0001-create-database-casedatabase"},
				},
				{
					Query: `CREATE DATABASE casedatabase;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testquoteddatabasenamesarecasesensitiverepro-0002-create-database-casedatabase"},
				},
				{
					Query: `SELECT datname
						FROM pg_catalog.pg_database
						WHERE datname IN ('CaseDatabase', 'casedatabase')
						ORDER BY datname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testquoteddatabasenamesarecasesensitiverepro-0003-select-datname-from-pg_catalog.pg_database-where"},
				},
			},
		},
	})
}

// TestQuotedTableNamesAreCaseSensitiveRepro reproduces an identifier/catalog
// bug: quoted table names preserve case, so "CaseTable" and casetable are
// distinct PostgreSQL relations in the same schema.
func TestQuotedTableNamesAreCaseSensitiveRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "quoted table names remain distinct from folded unquoted names",
			SetUpScript: []string{
				`CREATE TABLE "CaseTable" (id INT PRIMARY KEY, label TEXT);`,
				`INSERT INTO "CaseTable" VALUES (1, 'quoted');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE casetable (id INT PRIMARY KEY, label TEXT);`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testquotedtablenamesarecasesensitiverepro-0001-create-table-casetable-id-int"},
				},
				{
					Query: `INSERT INTO casetable VALUES (2, 'folded');`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testquotedtablenamesarecasesensitiverepro-0002-insert-into-casetable-values-2"},
				},
				{
					Query: `SELECT id, label FROM "CaseTable" ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testquotedtablenamesarecasesensitiverepro-0003-select-id-label-from-casetable"},
				},
				{
					Query: `SELECT id, label FROM casetable ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testquotedtablenamesarecasesensitiverepro-0004-select-id-label-from-casetable"},
				},
				{
					Query: `SELECT relname
						FROM pg_catalog.pg_class
						WHERE relnamespace = 'public'::regnamespace
							AND relname IN ('CaseTable', 'casetable')
						ORDER BY relname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testquotedtablenamesarecasesensitiverepro-0005-select-relname-from-pg_catalog.pg_class-where"},
				},
			},
		},
	})
}

// TestQuotedViewNamesAreCaseSensitiveRepro reproduces an identifier/catalog
// bug: quoted view names preserve case, so "CaseView" and caseview are
// distinct PostgreSQL views in the same schema.
func TestQuotedViewNamesAreCaseSensitiveRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "quoted view names remain distinct from folded unquoted names",
			SetUpScript: []string{
				`CREATE TABLE quoted_view_source (id INT PRIMARY KEY, label TEXT);`,
				`INSERT INTO quoted_view_source VALUES (1, 'quoted'), (2, 'folded');`,
				`CREATE VIEW "CaseView" AS SELECT id, label FROM quoted_view_source WHERE id = 1;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE VIEW caseview AS
						SELECT id, label FROM quoted_view_source WHERE id = 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testquotedviewnamesarecasesensitiverepro-0001-create-view-caseview-as-select"},
				},
				{
					Query: `SELECT id, label FROM "CaseView";`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testquotedviewnamesarecasesensitiverepro-0002-select-id-label-from-caseview"},
				},
				{
					Query: `SELECT id, label FROM caseview;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testquotedviewnamesarecasesensitiverepro-0003-select-id-label-from-caseview"},
				},
				{
					Query: `SELECT relname
						FROM pg_catalog.pg_class
						WHERE relnamespace = 'public'::regnamespace
							AND relkind = 'v'
							AND relname IN ('CaseView', 'caseview')
						ORDER BY relname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testquotedviewnamesarecasesensitiverepro-0004-select-relname-from-pg_catalog.pg_class-where"},
				},
			},
		},
	})
}

// TestQuotedMaterializedViewNamesAreCaseSensitiveRepro reproduces an
// identifier/catalog bug: quoted materialized view names preserve case, so
// "CaseMatView" and casematview are distinct PostgreSQL materialized views.
func TestQuotedMaterializedViewNamesAreCaseSensitiveRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "quoted materialized view names remain distinct from folded unquoted names",
			SetUpScript: []string{
				`CREATE TABLE quoted_matview_source (id INT PRIMARY KEY, label TEXT);`,
				`INSERT INTO quoted_matview_source VALUES (1, 'quoted'), (2, 'folded');`,
				`CREATE MATERIALIZED VIEW "CaseMatView" AS
					SELECT id, label FROM quoted_matview_source WHERE id = 1;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE MATERIALIZED VIEW casematview AS
						SELECT id, label FROM quoted_matview_source WHERE id = 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testquotedmaterializedviewnamesarecasesensitiverepro-0001-create-materialized-view-casematview-as"},
				},
				{
					Query: `SELECT id, label FROM "CaseMatView";`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testquotedmaterializedviewnamesarecasesensitiverepro-0002-select-id-label-from-casematview"},
				},
				{
					Query: `SELECT id, label FROM casematview;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testquotedmaterializedviewnamesarecasesensitiverepro-0003-select-id-label-from-casematview"},
				},
				{
					Query: `SELECT relname
						FROM pg_catalog.pg_class
						WHERE relnamespace = 'public'::regnamespace
							AND relkind = 'm'
							AND relname IN ('CaseMatView', 'casematview')
						ORDER BY relname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testquotedmaterializedviewnamesarecasesensitiverepro-0004-select-relname-from-pg_catalog.pg_class-where"},
				},
			},
		},
	})
}

// TestQuotedSequenceNamesAreCaseSensitive guards quoted sequence-name
// behavior: "CaseSequence" and casesequence are distinct PostgreSQL sequence
// relations.
func TestQuotedSequenceNamesAreCaseSensitive(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "quoted sequence names remain distinct from folded unquoted names",
			SetUpScript: []string{
				`CREATE SEQUENCE "CaseSequence";`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE SEQUENCE casesequence;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testquotedsequencenamesarecasesensitive-0001-create-sequence-casesequence"},
				},
				{
					Query: `SELECT relname
						FROM pg_catalog.pg_class
						WHERE relnamespace = 'public'::regnamespace
							AND relkind = 'S'
							AND relname IN ('CaseSequence', 'casesequence')
						ORDER BY relname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testquotedsequencenamesarecasesensitive-0002-select-relname-from-pg_catalog.pg_class-where"},
				},
			},
		},
	})
}

// TestQuotedColumnNamesAreCaseSensitiveRepro reproduces an identifier/catalog
// bug: quoted column names preserve case, so "CaseColumn" and casecolumn are
// distinct PostgreSQL columns in the same table.
func TestQuotedColumnNamesAreCaseSensitiveRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "quoted column names remain distinct from folded unquoted names",
			SetUpScript: []string{
				`CREATE TABLE quoted_column_items (
					id INT PRIMARY KEY,
					"CaseColumn" TEXT,
					casecolumn TEXT
				);`,
				`INSERT INTO quoted_column_items (id, "CaseColumn", casecolumn)
					VALUES (1, 'quoted', 'folded');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT "CaseColumn", casecolumn FROM quoted_column_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testquotedcolumnnamesarecasesensitiverepro-0001-select-casecolumn-casecolumn-from-quoted_column_items"},
				},
				{
					Query: `SELECT attname
						FROM pg_catalog.pg_attribute
						WHERE attrelid = 'quoted_column_items'::regclass
							AND attname IN ('CaseColumn', 'casecolumn')
						ORDER BY attname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testquotedcolumnnamesarecasesensitiverepro-0002-select-attname-from-pg_catalog.pg_attribute-where"},
				},
				{
					Query: `SELECT column_name
						FROM information_schema.columns
						WHERE table_name = 'quoted_column_items'
							AND column_name IN ('CaseColumn', 'casecolumn')
						ORDER BY column_name;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testquotedcolumnnamesarecasesensitiverepro-0003-select-column_name-from-information_schema.columns-where"},
				},
			},
		},
	})
}

// TestDroppedColumnRemainsInPgAttributeRepro reproduces a catalog correctness
// bug: PostgreSQL preserves a dropped column's attribute slot with
// pg_attribute.attisdropped = true.
func TestDroppedColumnRemainsInPgAttributeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "dropped columns remain in pg_attribute",
			SetUpScript: []string{
				`CREATE TABLE dropped_column_metadata_items (
					a INT,
					b TEXT,
					c INT
				);`,
				`ALTER TABLE dropped_column_metadata_items DROP COLUMN b;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT attnum::TEXT,
							attisdropped::TEXT,
							CASE WHEN attisdropped THEN 'dropped' ELSE attname END
						FROM pg_catalog.pg_attribute
						WHERE attrelid = 'dropped_column_metadata_items'::regclass
							AND attnum > 0
						ORDER BY attnum;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testdroppedcolumnremainsinpgattributerepro-0001-select-attnum::text-attisdropped::text-case-when"},
				},
			},
		},
	})
}

// TestPgAttributePhysicalTypeMetadataRepro reproduces a catalog correctness
// bug: pg_attribute exposes type-specific physical metadata for client
// introspection.
func TestPgAttributePhysicalTypeMetadataRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_attribute exposes type-specific physical metadata",
			SetUpScript: []string{
				`CREATE TABLE attribute_type_metadata_items (
					i INT4,
					t TEXT,
					b BOOL,
					n NUMERIC(5, 2),
					ts TIMESTAMPTZ
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT attname,
							attlen::TEXT,
							attbyval::TEXT,
							attalign,
							attstorage
						FROM pg_catalog.pg_attribute
						WHERE attrelid = 'attribute_type_metadata_items'::regclass
							AND attnum > 0
						ORDER BY attnum;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testpgattributephysicaltypemetadatarepro-0001-select-attname-attlen::text-attbyval::text-attalign"},
				},
			},
		},
	})
}

// TestPgAttributeMissingValueMetadataRepro reproduces a catalog persistence bug:
// adding a constant-default column to a populated table records the synthesized
// default in pg_attribute missing-value metadata.
func TestPgAttributeMissingValueMetadataRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_attribute records missing value metadata",
			SetUpScript: []string{
				`CREATE TABLE attribute_missing_value_items (id INT);`,
				`INSERT INTO attribute_missing_value_items VALUES (1);`,
				`ALTER TABLE attribute_missing_value_items
					ADD COLUMN marker INT DEFAULT 7;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT attname,
							atthasmissing::TEXT,
							attmissingval::TEXT
						FROM pg_catalog.pg_attribute
						WHERE attrelid = 'attribute_missing_value_items'::regclass
							AND attname = 'marker';`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testpgattributemissingvaluemetadatarepro-0001-select-attname-atthasmissing::text-attmissingval::text-from"},
				},
				{
					Query: `SELECT id, marker FROM attribute_missing_value_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testpgattributemissingvaluemetadatarepro-0002-select-id-marker-from-attribute_missing_value_items"},
				},
			},
		},
	})
}

// TestPgAttributeColumnAclMetadataRepro reproduces a security catalog bug:
// column-level grants are stored in pg_attribute.attacl.
func TestPgAttributeColumnAclMetadataRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_attribute records column ACL metadata",
			SetUpScript: []string{
				`CREATE ROLE attribute_attacl_reader;`,
				`CREATE TABLE attribute_attacl_items (
					id INT,
					secret TEXT
				);`,
				`GRANT SELECT (secret)
					ON attribute_attacl_items
					TO attribute_attacl_reader;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT attname,
							(attacl IS NULL)::TEXT,
							COALESCE(attacl::TEXT LIKE '{attribute_attacl_reader=r/%}', false)::TEXT
						FROM pg_catalog.pg_attribute
						WHERE attrelid = 'attribute_attacl_items'::regclass
							AND attname IN ('id', 'secret')
						ORDER BY attname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testpgattributecolumnaclmetadatarepro-0001-select-attname-attacl-is-null"},
				},
			},
		},
	})
}

// TestPgAttributeColumnOptionsMetadataRepro reproduces a catalog persistence
// bug: per-column planner options are exposed through pg_attribute.attoptions.
func TestPgAttributeColumnOptionsMetadataRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_attribute records column options metadata",
			SetUpScript: []string{
				`CREATE TABLE attribute_options_items (
					id INT,
					category TEXT
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE attribute_options_items
						ALTER COLUMN category
						SET (n_distinct = 100, n_distinct_inherited = 200);`,
				},
				{
					Query: `SELECT attname, CAST(attoptions AS TEXT)
						FROM pg_catalog.pg_attribute
						WHERE attrelid = 'attribute_options_items'::regclass
							AND attname = 'category';`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testpgattributecolumnoptionsmetadatarepro-0001-select-attname-cast-attoptions-as"},
				},
			},
		},
	})
}

// TestQuotedIndexNamesAreCaseSensitiveRepro reproduces an identifier/catalog
// bug: quoted index names preserve case, so "CaseIndex" and caseindex are
// distinct PostgreSQL index relations in the same schema.
func TestQuotedIndexNamesAreCaseSensitiveRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "quoted index names remain distinct from folded unquoted names",
			SetUpScript: []string{
				`CREATE TABLE quoted_index_items (
					id INT PRIMARY KEY,
					first_label TEXT,
					second_label TEXT
				);`,
				`CREATE INDEX "CaseIndex" ON quoted_index_items (first_label);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE INDEX caseindex ON quoted_index_items (second_label);`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testquotedindexnamesarecasesensitiverepro-0001-create-index-caseindex-on-quoted_index_items"},
				},
				{
					Query: `SELECT relname
						FROM pg_catalog.pg_class
						WHERE relnamespace = 'public'::regnamespace
							AND relkind = 'i'
							AND relname IN ('CaseIndex', 'caseindex')
						ORDER BY relname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testquotedindexnamesarecasesensitiverepro-0002-select-relname-from-pg_catalog.pg_class-where"},
				},
			},
		},
	})
}

// TestQuotedConstraintNamesAreCaseSensitiveRepro reproduces an
// identifier/catalog bug: quoted constraint names preserve case, so
// "CaseConstraint" and caseconstraint are distinct PostgreSQL constraints.
func TestQuotedConstraintNamesAreCaseSensitiveRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "quoted constraint names remain distinct from folded unquoted names",
			SetUpScript: []string{
				`CREATE TABLE quoted_constraint_items (
					id INT PRIMARY KEY,
					amount INT,
					CONSTRAINT "CaseConstraint" CHECK (amount > 0)
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE quoted_constraint_items
						ADD CONSTRAINT caseconstraint CHECK (amount < 100);`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testquotedconstraintnamesarecasesensitiverepro-0001-alter-table-quoted_constraint_items-add-constraint"},
				},
				{
					Query: `SELECT conname
						FROM pg_catalog.pg_constraint
						WHERE conrelid = 'quoted_constraint_items'::regclass
							AND conname IN ('CaseConstraint', 'caseconstraint')
						ORDER BY conname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testquotedconstraintnamesarecasesensitiverepro-0002-select-conname-from-pg_catalog.pg_constraint-where"},
				},
			},
		},
	})
}

// TestQuotedFunctionNamesAreCaseSensitiveRepro reproduces a function identity
// bug: quoted function names preserve case, so "CaseFunction"(int) and
// casefunction(int) are distinct PostgreSQL routines.
func TestQuotedFunctionNamesAreCaseSensitiveRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "quoted function names remain distinct from folded unquoted names",
			SetUpScript: []string{
				`CREATE FUNCTION "CaseFunction"(value integer)
					RETURNS integer
					LANGUAGE SQL
					AS $$ SELECT value + 1 $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE FUNCTION casefunction(value integer)
						RETURNS integer
						LANGUAGE SQL
						AS $$ SELECT value + 2 $$;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testquotedfunctionnamesarecasesensitiverepro-0001-create-function-casefunction-value-integer"},
				},
				{
					Query: `SELECT "CaseFunction"(10), casefunction(10);`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testquotedfunctionnamesarecasesensitiverepro-0002-select-casefunction-10-casefunction-10"},
				},
			},
		},
	})
}

// TestQuotedProcedureNamesAreCaseSensitiveRepro reproduces a routine identity
// bug: quoted procedure names preserve case, so "CaseProcedure"(text) and
// caseprocedure(text) are distinct PostgreSQL procedures.
func TestQuotedProcedureNamesAreCaseSensitiveRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "quoted procedure names remain distinct from folded unquoted names",
			SetUpScript: []string{
				`CREATE TABLE quoted_procedure_calls (label TEXT);`,
				`CREATE PROCEDURE "CaseProcedure"(input TEXT)
					LANGUAGE plpgsql
					AS $$
					BEGIN
						INSERT INTO quoted_procedure_calls VALUES ('quoted:' || input);
					END;
					$$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE PROCEDURE caseprocedure(input TEXT)
						LANGUAGE plpgsql
						AS $$
						BEGIN
							INSERT INTO quoted_procedure_calls VALUES ('folded:' || input);
						END;
						$$;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testquotedprocedurenamesarecasesensitiverepro-0001-create-procedure-caseprocedure-input-text"},
				},
				{
					Query: `CALL "CaseProcedure"('one');`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testquotedprocedurenamesarecasesensitiverepro-0002-call-caseprocedure-one"},
				},
				{
					Query: `CALL caseprocedure('two');`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testquotedprocedurenamesarecasesensitiverepro-0003-call-caseprocedure-two"},
				},
				{
					Query: `SELECT label FROM quoted_procedure_calls ORDER BY label;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testquotedprocedurenamesarecasesensitiverepro-0004-select-label-from-quoted_procedure_calls-order"},
				},
			},
		},
	})
}

// TestQuotedDomainNamesAreCaseSensitive guards quoted domain-name behavior:
// "CaseDomain" and casedomain are distinct PostgreSQL types in the same schema.
func TestQuotedDomainNamesAreCaseSensitive(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "quoted domain names remain distinct from folded unquoted names",
			SetUpScript: []string{
				`CREATE DOMAIN "CaseDomain" AS integer CHECK (VALUE > 0);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE DOMAIN casedomain AS integer CHECK (VALUE < 100);`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testquoteddomainnamesarecasesensitive-0001-create-domain-casedomain-as-integer"},
				},
				{
					Query: `SELECT typname
						FROM pg_catalog.pg_type
						WHERE typnamespace = 'public'::regnamespace
							AND typname IN ('CaseDomain', 'casedomain')
						ORDER BY typname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testquoteddomainnamesarecasesensitive-0002-select-typname-from-pg_catalog.pg_type-where"},
				},
			},
		},
	})
}

// TestPgGetExprReturnsGeneratedColumnExpressionRepro reproduces a catalog
// persistence bug: stored generated-column expressions are not exposed through
// pg_attrdef.adbin and pg_get_expr.
func TestPgGetExprReturnsGeneratedColumnExpressionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_get_expr returns generated column expression",
			SetUpScript: []string{
				`CREATE TABLE attrdef_generated_temperature (
					celsius SMALLINT NOT NULL,
					fahrenheit SMALLINT GENERATED ALWAYS AS ((celsius * 9 / 5) + 32) STORED
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT pg_get_expr(adbin, adrelid)
						FROM pg_catalog.pg_attrdef
						WHERE adrelid = 'attrdef_generated_temperature'::regclass;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testpggetexprreturnsgeneratedcolumnexpressionrepro-0001-select-pg_get_expr-adbin-adrelid-from"},
				},
			},
		},
	})
}

// TestPgAttrdefDefaultExpressionsRepro guards ordinary column default metadata
// in pg_attrdef and pg_get_expr.
func TestPgAttrdefDefaultExpressionsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_attrdef exposes ordinary column defaults",
			SetUpScript: []string{
				`CREATE TABLE attrdef_default_items (
					id INT DEFAULT 42,
					label TEXT DEFAULT lower('ABC')
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT a.attname,
							pg_get_expr(d.adbin, d.adrelid)
						FROM pg_catalog.pg_attrdef d
						JOIN pg_catalog.pg_attribute a
							ON a.attrelid = d.adrelid
							AND a.attnum = d.adnum
						WHERE d.adrelid = 'attrdef_default_items'::regclass
						ORDER BY a.attnum;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testpgattrdefdefaultexpressionsrepro-0001-select-a.attname-pg_get_expr-d.adbin-d.adrelid"},
				},
			},
		},
	})
}

// TestInformationSchemaGeneratedColumnExpressionRepro reproduces a catalog
// correctness bug: generated columns are flagged in information_schema.columns,
// but their generation_expression metadata is missing.
func TestInformationSchemaGeneratedColumnExpressionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "information_schema exposes generated column expression",
			SetUpScript: []string{
				`CREATE TABLE information_schema_generated_items (
					id INT PRIMARY KEY,
					base_value INT,
					doubled INT GENERATED ALWAYS AS (base_value * 2) STORED
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT generation_expression IS NOT NULL
						FROM information_schema.columns
						WHERE table_name = 'information_schema_generated_items'
							AND column_name = 'doubled';`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testinformationschemageneratedcolumnexpressionrepro-0001-select-generation_expression-is-not-null"},
				},
			},
		},
	})
}

// TestIdentityColumnCatalogMetadataRepro reproduces a catalog correctness bug:
// identity columns work for DML, but identity metadata is not exposed through
// pg_attribute or information_schema.columns.
func TestIdentityColumnCatalogMetadataRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "identity columns expose catalog metadata",
			SetUpScript: []string{
				`CREATE TABLE identity_catalog_items (
					always_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
					default_id INT GENERATED BY DEFAULT AS IDENTITY,
					label TEXT
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT attname, attidentity
						FROM pg_catalog.pg_attribute
						WHERE attrelid = 'identity_catalog_items'::regclass
							AND attname IN ('always_id', 'default_id')
						ORDER BY attname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testidentitycolumncatalogmetadatarepro-0001-select-attname-attidentity-from-pg_catalog.pg_attribute"},
				},
				{
					Query: `SELECT column_name, is_identity, identity_generation
						FROM information_schema.columns
						WHERE table_name = 'identity_catalog_items'
							AND column_name IN ('always_id', 'default_id')
						ORDER BY column_name;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testidentitycolumncatalogmetadatarepro-0002-select-column_name-is_identity-identity_generation-from"},
				},
			},
		},
	})
}

// TestInformationSchemaDomainColumnMetadataRepro reproduces a catalog
// correctness bug: domain-typed columns do not expose domain metadata in
// information_schema.columns.
func TestInformationSchemaDomainColumnMetadataRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "information_schema exposes domain column metadata",
			SetUpScript: []string{
				`CREATE DOMAIN positive_amount AS integer CHECK (VALUE > 0);`,
				`CREATE TABLE domain_metadata_items (
					id INT PRIMARY KEY,
					amount positive_amount
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT data_type, domain_catalog, domain_schema, domain_name
						FROM information_schema.columns
						WHERE table_name = 'domain_metadata_items'
							AND column_name = 'amount';`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testinformationschemadomaincolumnmetadatarepro-0001-select-data_type-domain_catalog-domain_schema-domain_name",

						// TestInformationSchemaViewUpdatabilityMetadataRepro reproduces a catalog
						// correctness bug: information_schema.views leaves the view updatability
						// columns null instead of reporting YES/NO.
						ColumnModes: []string{"structural", "structural", "schema"}},
				},
			},
		},
	})
}

func TestInformationSchemaViewUpdatabilityMetadataRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "information_schema exposes view updatability metadata",
			SetUpScript: []string{
				`CREATE TABLE view_metadata_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`CREATE VIEW view_metadata_simple AS
					SELECT id, label FROM view_metadata_items;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT check_option, is_updatable, is_insertable_into,
							is_trigger_updatable, is_trigger_deletable, is_trigger_insertable_into
						FROM information_schema.views
						WHERE table_schema = 'public'
							AND table_name = 'view_metadata_simple';`,
					Expected: []sql.Row{{"NONE", "YES", "YES", "NO", "NO", "NO"}},
				},
			},
		},
	})
}

// TestPgViewsViewownerMetadataRepro reproduces a catalog correctness bug:
// pg_views.viewowner should identify the owner of each view.
func TestPgViewsViewownerMetadataRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_views exposes view owner",
			SetUpScript: []string{
				`CREATE TABLE pg_views_owner_source (
					id INT PRIMARY KEY
				);`,
				`CREATE VIEW pg_views_owner_reader AS
					SELECT id FROM pg_views_owner_source;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT viewowner
						FROM pg_catalog.pg_views
						WHERE schemaname = 'public'
							AND viewname = 'pg_views_owner_reader';`,
					Expected: []sql.Row{{"postgres"}},
				},
			},
		},
	})
}

// TestPgGetViewdefWrapColumnOverloadRepro reproduces a catalog correctness bug:
// pg_get_viewdef(oid, integer) should return a view definition with optional
// line wrapping, not error.
func TestPgGetViewdefWrapColumnOverloadRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_get_viewdef oid integer overload returns definition",
			SetUpScript: []string{
				`CREATE TABLE pg_get_viewdef_wrap_source (
					id INT PRIMARY KEY
				);`,
				`CREATE VIEW pg_get_viewdef_wrap_reader AS
					SELECT id FROM pg_get_viewdef_wrap_source;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT pg_get_viewdef('pg_get_viewdef_wrap_reader'::regclass, 0) IS NOT NULL;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testpggetviewdefwrapcolumnoverloadrepro-0001-select-pg_get_viewdef-pg_get_viewdef_wrap_reader-::regclass-0"},
				},
			},
		},
	})
}

// TestPgGetTriggerdefPrettyOverloadRepro reproduces a catalog correctness bug:
// pg_get_triggerdef(oid, true) should return a trigger definition, not error
// because pretty printing was requested.
func TestPgGetTriggerdefPrettyOverloadRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_get_triggerdef pretty overload returns definition",
			SetUpScript: []string{
				`CREATE TABLE pg_get_triggerdef_pretty_target (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`CREATE FUNCTION pg_get_triggerdef_pretty_func() RETURNS trigger AS $$
					BEGIN
						NEW.label := upper(NEW.label);
						RETURN NEW;
					END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER pg_get_triggerdef_pretty_before_insert
					BEFORE INSERT ON pg_get_triggerdef_pretty_target
					FOR EACH ROW EXECUTE FUNCTION pg_get_triggerdef_pretty_func();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT pg_get_triggerdef(oid, true) IS NOT NULL
						FROM pg_catalog.pg_trigger
						WHERE tgname = 'pg_get_triggerdef_pretty_before_insert';`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testpggettriggerdefprettyoverloadrepro-0001-select-pg_get_triggerdef-oid-true-is"},
				},
			},
		},
	})
}

// TestTriggerConditionMetadataRepro reproduces a catalog correctness bug:
// trigger WHEN conditions should be exposed through pg_trigger.tgqual and
// information_schema.triggers.action_condition.
func TestTriggerConditionMetadataRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "trigger condition metadata is exposed",
			SetUpScript: []string{
				`CREATE TABLE trigger_condition_metadata_target (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`CREATE FUNCTION trigger_condition_metadata_func() RETURNS trigger AS $$
					BEGIN
						NEW.label := upper(NEW.label);
						RETURN NEW;
					END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER trigger_condition_metadata_before_insert
					BEFORE INSERT ON trigger_condition_metadata_target
					FOR EACH ROW
					WHEN (NEW.id > 10)
					EXECUTE FUNCTION trigger_condition_metadata_func();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT tgqual IS NOT NULL
						FROM pg_catalog.pg_trigger
						WHERE tgname = 'trigger_condition_metadata_before_insert';`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testtriggerconditionmetadatarepro-0001-select-tgqual-is-not-null"},
				},
				{
					Query: `SELECT action_condition IS NOT NULL
						FROM information_schema.triggers
						WHERE trigger_name = 'trigger_condition_metadata_before_insert';`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testtriggerconditionmetadatarepro-0002-select-action_condition-is-not-null"},
				},
			},
		},
	})
}

// TestPgGetFunctionCatalogIntrospectionRepro reproduces a catalog correctness
// bug: PostgreSQL renders function signatures and definitions for catalog
// function OIDs, but Doltgres returns empty strings from these helpers.
func TestPgGetFunctionCatalogIntrospectionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "pg_get_function helpers render built-in function metadata",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT pg_get_function_result(31::oid);`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testpggetfunctioncatalogintrospectionrepro-0001-select-pg_get_function_result-31::oid"},
				},
				{
					Query: `SELECT pg_get_function_identity_arguments(31::oid);`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testpggetfunctioncatalogintrospectionrepro-0002-select-pg_get_function_identity_arguments-31::oid"},
				},
				{
					Query: `SELECT pg_get_functiondef(31::oid) LIKE
						'CREATE OR REPLACE FUNCTION pg_catalog.byteaout(bytea)%RETURNS cstring%LANGUAGE internal%byteaout%';`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testpggetfunctioncatalogintrospectionrepro-0003-select-pg_get_functiondef-31::oid-like-create"},
				},
			},
		},
	})
}

// TestPgGetFunctionUserDefinedIntrospectionRepro reproduces a catalog
// correctness gap: PostgreSQL renders user-defined function signatures,
// results, and definitions through the pg_get_function_* helpers.
func TestPgGetFunctionUserDefinedIntrospectionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_get_function helpers render user-defined function metadata",
			SetUpScript: []string{
				`CREATE FUNCTION introspect_user_function(input_value INTEGER, label TEXT)
					RETURNS TEXT
					LANGUAGE SQL
					IMMUTABLE
					STRICT
					AS $$ SELECT label || input_value::TEXT $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT
							pg_get_function_result(p.oid),
							pg_get_function_identity_arguments(p.oid),
							pg_get_function_arguments(p.oid),
							pg_get_functiondef(p.oid) LIKE
								'CREATE OR REPLACE FUNCTION public.introspect_user_function(input_value integer, label text)%RETURNS text%LANGUAGE sql%IMMUTABLE STRICT%SELECT%'
						FROM pg_catalog.pg_proc p
						JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
						WHERE n.nspname = 'public'
							AND p.proname = 'introspect_user_function';`,
					Expected: []sql.Row{{
						"text",
						"input_value integer, label text",
						"input_value integer, label text",
						"t",
					}},
				},
			},
		},
	})
}

// TestPgEncodingToCharMapsKnownEncodingIdsRepro reproduces a catalog utility
// correctness bug: PostgreSQL maps known encoding IDs to their canonical names.
func TestPgEncodingToCharMapsKnownEncodingIdsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "pg_encoding_to_char maps known encoding ids",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT pg_encoding_to_char(0), pg_encoding_to_char(6);`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testpgencodingtocharmapsknownencodingidsrepro-0001-select-pg_encoding_to_char-0-pg_encoding_to_char-6"},
				},
			},
		},
	})
}

// TestRelationSizeHelpersReportStoredDataRepro reproduces a catalog/admin
// correctness bug: PostgreSQL relation-size helpers report nonzero byte counts
// for populated tables and their indexes, but Doltgres returns zero.
func TestRelationSizeHelpersReportStoredDataRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "relation size helpers report stored table and index data",
			SetUpScript: []string{
				`CREATE TABLE relation_size_items (
					id INT PRIMARY KEY,
					label TEXT NOT NULL
				);`,
				`INSERT INTO relation_size_items
					SELECT i, repeat('x', 200)
					FROM generate_series(1, 25) AS s(i);`,
				`CREATE INDEX relation_size_items_label_idx
					ON relation_size_items (label);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT
							pg_relation_size('relation_size_items'::regclass) > 0,
							pg_relation_size('relation_size_items'::regclass, 'main') > 0,
							pg_table_size('relation_size_items'::regclass) > 0,
							pg_indexes_size('relation_size_items'::regclass) > 0,
							pg_total_relation_size('relation_size_items'::regclass) > 0;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testrelationsizehelpersreportstoreddatarepro-0001-select-pg_relation_size-relation_size_items-::regclass->"},
				},
			},
		},
	})
}

// TestPgBackendMemoryContextsReportsTopContextRepro reproduces an admin/catalog
// correctness bug: PostgreSQL's pg_backend_memory_contexts exposes the current
// backend's top-level memory context.
func TestPgBackendMemoryContextsReportsTopContextRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "pg_backend_memory_contexts reports TopMemoryContext",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT count(*) > 0
						FROM pg_catalog.pg_backend_memory_contexts
						WHERE level = 0
							AND name = 'TopMemoryContext'
							AND total_bytes >= free_bytes;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testpgbackendmemorycontextsreportstopcontextrepro-0001-select-count-*->-0"},
				},
			},
		},
	})
}

// TestPgHbaFileRulesReportsParsedRulesRepro reproduces an admin/catalog
// correctness bug: PostgreSQL's pg_hba_file_rules exposes parsed HBA rules.
func TestPgHbaFileRulesReportsParsedRulesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "pg_hba_file_rules reports parsed rules",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT count(*) > 0,
							count(*) FILTER (WHERE error IS NOT NULL) = 0
						FROM pg_catalog.pg_hba_file_rules;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testpghbafilerulesreportsparsedrulesrepro-0001-select-count-*->-0"},
				},
			},
		},
	})
}

// TestPgShmemAllocationsReportsAllocationRowsRepro reproduces an admin/catalog
// correctness bug: PostgreSQL's pg_shmem_allocations exposes shared-memory
// allocation rows with non-negative sizes.
func TestPgShmemAllocationsReportsAllocationRowsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "pg_shmem_allocations reports allocation rows",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT count(*) > 0
						FROM pg_catalog.pg_shmem_allocations
						WHERE allocated_size >= size
							AND size >= 0;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testpgshmemallocationsreportsallocationrowsrepro-0001-select-count-*->-0"},
				},
			},
		},
	})
}

// TestPgStatActivityReportsCurrentBackendRepro reproduces an admin/catalog
// correctness bug: PostgreSQL's pg_stat_activity includes the current backend.
func TestPgStatActivityReportsCurrentBackendRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "pg_stat_activity reports the current backend",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT count(*) > 0
						FROM pg_catalog.pg_stat_activity
						WHERE pid = pg_backend_pid();`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testpgstatactivityreportscurrentbackendrepro-0001-select-count-*->-0"},
				},
			},
		},
	})
}

// TestPgStatSslReportsCurrentBackendRepro reproduces an admin/catalog
// correctness bug: PostgreSQL's pg_stat_ssl includes one row for each backend.
func TestPgStatSslReportsCurrentBackendRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "pg_stat_ssl reports the current backend",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT count(*) > 0
						FROM pg_catalog.pg_stat_ssl
						WHERE pid = pg_backend_pid();`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testpgstatsslreportscurrentbackendrepro-0001-select-count-*->-0"},
				},
			},
		},
	})
}

// TestPgStatGssapiReportsCurrentBackendRepro reproduces an admin/catalog
// correctness bug: PostgreSQL's pg_stat_gssapi includes one row for each
// backend.
func TestPgStatGssapiReportsCurrentBackendRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "pg_stat_gssapi reports the current backend",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT count(*) > 0
						FROM pg_catalog.pg_stat_gssapi
						WHERE pid = pg_backend_pid();`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testpgstatgssapireportscurrentbackendrepro-0001-select-count-*->-0"},
				},
			},
		},
	})
}

// TestPgStatUserTablesReportsUserRelationsRepro reproduces an admin/catalog
// correctness bug: PostgreSQL's pg_stat_user_tables exposes user table rows.
func TestPgStatUserTablesReportsUserRelationsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_stat_user_tables reports user tables",
			SetUpScript: []string{
				`CREATE TABLE stat_user_table_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT schemaname, relname, n_tup_ins >= 0
						FROM pg_catalog.pg_stat_user_tables
						WHERE relname = 'stat_user_table_items';`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testpgstatusertablesreportsuserrelationsrepro-0001-select-schemaname-relname-n_tup_ins->=",

						// TestPgStatAllTablesReportsUserRelationsRepro reproduces an admin/catalog
						// correctness bug: PostgreSQL's pg_stat_all_tables exposes user table rows.
						ColumnModes: []string{"schema"}},
				},
			},
		},
	})
}

func TestPgStatAllTablesReportsUserRelationsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_stat_all_tables reports user tables",
			SetUpScript: []string{
				`CREATE TABLE stat_all_table_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT schemaname, relname, n_tup_ins >= 0
						FROM pg_catalog.pg_stat_all_tables
						WHERE relname = 'stat_all_table_items';`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testpgstatalltablesreportsuserrelationsrepro-0001-select-schemaname-relname-n_tup_ins->=",

						// TestPgStatSysTablesReportsSystemRelationsRepro reproduces an admin/catalog
						// correctness bug: PostgreSQL's pg_stat_sys_tables exposes system table rows.
						ColumnModes: []string{"schema"}},
				},
			},
		},
	})
}

func TestPgStatSysTablesReportsSystemRelationsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "pg_stat_sys_tables reports system tables",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT count(*) > 0
						FROM pg_catalog.pg_stat_sys_tables
						WHERE schemaname = 'pg_catalog'
							AND relname = 'pg_class';`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testpgstatsystablesreportssystemrelationsrepro-0001-select-count-*->-0"},
				},
			},
		},
	})
}

// TestPgStatioUserTablesReportsUserRelationsRepro reproduces an admin/catalog
// correctness bug: PostgreSQL's pg_statio_user_tables exposes user table rows.
func TestPgStatioUserTablesReportsUserRelationsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_statio_user_tables reports user tables",
			SetUpScript: []string{
				`CREATE TABLE statio_user_table_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT schemaname, relname, heap_blks_read >= 0
						FROM pg_catalog.pg_statio_user_tables
						WHERE relname = 'statio_user_table_items';`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testpgstatiousertablesreportsuserrelationsrepro-0001-select-schemaname-relname-heap_blks_read->=",

						// TestPgStatioAllTablesReportsUserRelationsRepro reproduces an admin/catalog
						// correctness bug: PostgreSQL's pg_statio_all_tables exposes user table rows.
						ColumnModes: []string{"schema"}},
				},
			},
		},
	})
}

func TestPgStatioAllTablesReportsUserRelationsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_statio_all_tables reports user tables",
			SetUpScript: []string{
				`CREATE TABLE statio_all_table_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT schemaname, relname, heap_blks_read >= 0
						FROM pg_catalog.pg_statio_all_tables
						WHERE relname = 'statio_all_table_items';`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testpgstatioalltablesreportsuserrelationsrepro-0001-select-schemaname-relname-heap_blks_read->=",

						// TestPgStatioSysTablesReportsSystemRelationsRepro reproduces an admin/catalog
						// correctness bug: PostgreSQL's pg_statio_sys_tables exposes system table rows.
						ColumnModes: []string{"schema"}},
				},
			},
		},
	})
}

func TestPgStatioSysTablesReportsSystemRelationsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "pg_statio_sys_tables reports system tables",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT count(*) > 0
						FROM pg_catalog.pg_statio_sys_tables
						WHERE schemaname = 'pg_catalog'
							AND relname = 'pg_class';`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testpgstatiosystablesreportssystemrelationsrepro-0001-select-count-*->-0"},
				},
			},
		},
	})
}

// TestPgStatXactUserTablesReportsUserRelationsRepro reproduces an
// admin/catalog correctness bug: PostgreSQL's pg_stat_xact_user_tables exposes
// user table rows for current-transaction statistics.
func TestPgStatXactUserTablesReportsUserRelationsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_stat_xact_user_tables reports user tables",
			SetUpScript: []string{
				`CREATE TABLE stat_xact_user_table_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT schemaname, relname, n_tup_ins >= 0
						FROM pg_catalog.pg_stat_xact_user_tables
						WHERE relname = 'stat_xact_user_table_items';`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testpgstatxactusertablesreportsuserrelationsrepro-0001-select-schemaname-relname-n_tup_ins->=",

						// TestPgStatXactAllTablesReportsUserRelationsRepro reproduces an admin/catalog
						// correctness bug: PostgreSQL's pg_stat_xact_all_tables exposes user table
						// rows for current-transaction statistics.
						ColumnModes: []string{"schema"}},
				},
			},
		},
	})
}

func TestPgStatXactAllTablesReportsUserRelationsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_stat_xact_all_tables reports user tables",
			SetUpScript: []string{
				`CREATE TABLE stat_xact_all_table_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT schemaname, relname, n_tup_ins >= 0
						FROM pg_catalog.pg_stat_xact_all_tables
						WHERE relname = 'stat_xact_all_table_items';`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testpgstatxactalltablesreportsuserrelationsrepro-0001-select-schemaname-relname-n_tup_ins->=",

						// TestPgStatXactSysTablesReportsSystemRelationsRepro reproduces an
						// admin/catalog correctness bug: PostgreSQL's pg_stat_xact_sys_tables exposes
						// system table rows for current-transaction statistics.
						ColumnModes: []string{"schema"}},
				},
			},
		},
	})
}

func TestPgStatXactSysTablesReportsSystemRelationsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "pg_stat_xact_sys_tables reports system tables",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT count(*) > 0
						FROM pg_catalog.pg_stat_xact_sys_tables
						WHERE schemaname = 'pg_catalog'
							AND relname = 'pg_class';`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testpgstatxactsystablesreportssystemrelationsrepro-0001-select-count-*->-0"},
				},
			},
		},
	})
}

// TestPgStatUserFunctionsTracksUserFunctionCallsRepro reproduces an
// admin/catalog correctness bug: when track_functions is enabled, PostgreSQL's
// pg_stat_user_functions exposes cumulative rows for called user functions.
func TestPgStatUserFunctionsTracksUserFunctionCallsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_stat_user_functions reports called user functions",
			SetUpScript: []string{
				`SET track_functions TO 'all';`,
				`CREATE FUNCTION stat_user_function_target(input_value INT)
					RETURNS INT
					LANGUAGE SQL
					AS $$ SELECT input_value + 1 $$;`,
				`SELECT stat_user_function_target(41);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT count(*) > 0
						FROM pg_catalog.pg_stat_user_functions
						WHERE schemaname = 'public'
							AND funcname = 'stat_user_function_target'
							AND calls >= 1
							AND total_time >= 0
							AND self_time >= 0;`,
					Expected: []sql.Row{{"t"}},
				},
			},
		},
	})
}

// TestPgStatXactUserFunctionsTracksCurrentTransactionCallsRepro reproduces an
// admin/catalog correctness bug: when track_functions is enabled, PostgreSQL's
// pg_stat_xact_user_functions exposes current-transaction rows for called user
// functions.
func TestPgStatXactUserFunctionsTracksCurrentTransactionCallsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_stat_xact_user_functions reports current transaction calls",
			SetUpScript: []string{
				`SET track_functions TO 'all';`,
				`CREATE FUNCTION stat_xact_user_function_target(input_value INT)
					RETURNS INT
					LANGUAGE SQL
					AS $$ SELECT input_value + 1 $$;`,
				`BEGIN;`,
				`SELECT stat_xact_user_function_target(41);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT count(*) > 0
						FROM pg_catalog.pg_stat_xact_user_functions
						WHERE schemaname = 'public'
							AND funcname = 'stat_xact_user_function_target'
							AND calls >= 1
							AND total_time >= 0
							AND self_time >= 0;`,
					Expected: []sql.Row{{"t"}},
				},
			},
		},
	})
}

// TestPgStatDatabaseReportsCurrentDatabaseRepro reproduces an admin/catalog
// correctness bug: PostgreSQL's pg_stat_database exposes the current database.
func TestPgStatDatabaseReportsCurrentDatabaseRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "pg_stat_database reports the current database",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT count(*) > 0
						FROM pg_catalog.pg_stat_database
						WHERE datname = current_database();`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testpgstatdatabasereportscurrentdatabaserepro-0001-select-count-*->-0"},
				},
			},
		},
	})
}

// TestPgStatArchiverReportsClusterRowRepro reproduces an admin/catalog
// correctness bug: PostgreSQL's pg_stat_archiver reports one cluster-wide row.
func TestPgStatArchiverReportsClusterRowRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "pg_stat_archiver reports one cluster-wide row",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT count(*) = 1 FROM pg_catalog.pg_stat_archiver;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testpgstatarchiverreportsclusterrowrepro-0001-select-count-*-=-1"},
				},
			},
		},
	})
}

// TestPgStatBgwriterReportsClusterRowRepro reproduces an admin/catalog
// correctness bug: PostgreSQL's pg_stat_bgwriter reports one cluster-wide row.
func TestPgStatBgwriterReportsClusterRowRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "pg_stat_bgwriter reports one cluster-wide row",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT count(*) = 1 FROM pg_catalog.pg_stat_bgwriter;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testpgstatbgwriterreportsclusterrowrepro-0001-select-count-*-=-1"},
				},
			},
		},
	})
}

// TestPgStatDatabaseConflictsReportsCurrentDatabaseRepro reproduces an
// admin/catalog correctness bug: PostgreSQL's pg_stat_database_conflicts
// reports one row for the current database.
func TestPgStatDatabaseConflictsReportsCurrentDatabaseRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "pg_stat_database_conflicts reports the current database",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT count(*) > 0
						FROM pg_catalog.pg_stat_database_conflicts
						WHERE datname = current_database();`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testpgstatdatabaseconflictsreportscurrentdatabaserepro-0001-select-count-*->-0"},
				},
			},
		},
	})
}

// TestPgStatWalReportsClusterRowRepro reproduces an admin/catalog correctness
// bug: PostgreSQL's pg_stat_wal always reports one cluster-wide stats row.
func TestPgStatWalReportsClusterRowRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "pg_stat_wal reports one cluster-wide row",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT count(*) = 1 FROM pg_catalog.pg_stat_wal;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testpgstatwalreportsclusterrowrepro-0001-select-count-*-=-1"},
				},
			},
		},
	})
}

// TestPgStatSlruReportsCacheRowsRepro reproduces an admin/catalog correctness
// bug: PostgreSQL's pg_stat_slru reports rows for SLRU caches.
func TestPgStatSlruReportsCacheRowsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "pg_stat_slru reports cache rows",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT count(*) > 0 FROM pg_catalog.pg_stat_slru;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testpgstatslrureportscacherowsrepro-0001-select-count-*->-0"},
				},
			},
		},
	})
}

// TestPgStatIoCatalogShapeRepro reproduces a PostgreSQL 16 monitoring-catalog
// compatibility gap: pg_stat_io should expose cluster-wide I/O statistics.
func TestPgStatIoCatalogShapeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_stat_io exposes PostgreSQL 16 columns and rows",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT count(*) = 18
						FROM pg_catalog.pg_attribute
						WHERE attrelid = 'pg_catalog.pg_stat_io'::regclass
							AND NOT attisdropped
							AND attname IN (
								'backend_type', 'object', 'context', 'reads', 'read_time',
								'writes', 'write_time', 'writebacks', 'writeback_time',
								'extends', 'extend_time', 'op_bytes', 'hits', 'evictions',
								'reuses', 'fsyncs', 'fsync_time', 'stats_reset'
							);`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testpgstatiocatalogshaperepro-0001-select-count-*-=-18"},
				},
				{
					Query: `SELECT count(*) > 0 FROM pg_catalog.pg_stat_io;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testpgstatiocatalogshaperepro-0002-select-count-*->-0"},
				},
			},
		},
	})
}

// TestPgTableIsVisibleHonorsSearchPathShadowingRepro reproduces a catalog
// visibility correctness bug: a relation is not visible when an earlier
// search-path schema contains another relation with the same name.
func TestPgTableIsVisibleHonorsSearchPathShadowingRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_table_is_visible honors search path shadowing",
			SetUpScript: []string{
				`CREATE SCHEMA visible_first;`,
				`CREATE SCHEMA visible_second;`,
				`CREATE TABLE visible_first.shadowed_table (id INT PRIMARY KEY);`,
				`CREATE TABLE visible_second.shadowed_table (id INT PRIMARY KEY);`,
				`SET search_path = visible_first, visible_second;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT pg_table_is_visible('visible_first.shadowed_table'::regclass),
						pg_table_is_visible('visible_second.shadowed_table'::regclass);`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testpgtableisvisiblehonorssearchpathshadowingrepro-0001-select-pg_table_is_visible-visible_first.shadowed_table-::regclass-pg_table_is_visible"},
				},
			},
		},
	})
}

// TestRegtypeResolvesSchemaQualifiedDomainsRepro reproduces a catalog lookup
// correctness bug: PostgreSQL regtype input resolves schema-qualified
// user-defined domain types.
func TestRegtypeResolvesSchemaQualifiedDomainsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "regtype resolves schema-qualified domains",
			SetUpScript: []string{
				`CREATE SCHEMA regtype_schema_first;`,
				`CREATE SCHEMA regtype_schema_second;`,
				`CREATE DOMAIN regtype_schema_first.lookup_domain AS integer;`,
				`CREATE DOMAIN regtype_schema_second.lookup_domain AS integer;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT 'regtype_schema_first.lookup_domain'::regtype IS NOT NULL,
						'regtype_schema_second.lookup_domain'::regtype IS NOT NULL;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testregtyperesolvesschemaqualifieddomainsrepro-0001-select-regtype_schema_first.lookup_domain-::regtype-is-not"},
				},
			},
		},
	})
}

// TestRegtypeResolvesUserDefinedTypesRepro reproduces a catalog lookup
// correctness bug: PostgreSQL regtype input resolves user-defined enum,
// composite, and domain types through the active search path.
func TestRegtypeResolvesUserDefinedTypesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "regtype resolves user-defined types",
			SetUpScript: []string{
				`CREATE TYPE regtype_lookup_enum AS ENUM ('one', 'two');`,
				`CREATE TYPE regtype_lookup_composite AS (id integer);`,
				`CREATE DOMAIN regtype_lookup_domain AS integer;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT
							'regtype_lookup_enum'::regtype::text,
							'regtype_lookup_composite'::regtype::text,
							'regtype_lookup_domain'::regtype::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testregtyperesolvesuserdefinedtypesrepro-0001-select-regtype_lookup_enum-::regtype::text-regtype_lookup_composite-::regtype::text"},
				},
			},
		},
	})
}

// TestToRegtypeResolvesUserDefinedTypesRepro reproduces a catalog lookup
// correctness bug: PostgreSQL to_regtype resolves user-defined enum,
// composite, and domain types through the active search path.
func TestToRegtypeResolvesUserDefinedTypesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "to_regtype resolves user-defined types",
			SetUpScript: []string{
				`CREATE TYPE to_regtype_lookup_enum AS ENUM ('one', 'two');`,
				`CREATE TYPE to_regtype_lookup_composite AS (id integer);`,
				`CREATE DOMAIN to_regtype_lookup_domain AS integer;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT
							to_regtype('to_regtype_lookup_enum')::text,
							to_regtype('to_regtype_lookup_composite')::text,
							to_regtype('to_regtype_lookup_domain')::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testtoregtyperesolvesuserdefinedtypesrepro-0001-select-to_regtype-to_regtype_lookup_enum-::text-to_regtype"},
				},
			},
		},
	})
}

// TestPgTypeIsVisibleHonorsSearchPathShadowingRepro reproduces a catalog
// visibility correctness bug: a type is not visible when an earlier
// search-path schema contains another type with the same name.
func TestPgTypeIsVisibleHonorsSearchPathShadowingRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_type_is_visible honors search path shadowing",
			SetUpScript: []string{
				`CREATE SCHEMA visible_type_first;`,
				`CREATE SCHEMA visible_type_second;`,
				`CREATE DOMAIN visible_type_first.shadowed_domain AS integer;`,
				`CREATE DOMAIN visible_type_second.shadowed_domain AS integer;`,
				`SET search_path = visible_type_first, visible_type_second;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT
							pg_type_is_visible((
								SELECT t.oid
								FROM pg_catalog.pg_type t
								JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
								WHERE n.nspname = 'visible_type_first'
									AND t.typname = 'shadowed_domain'
							)),
							pg_type_is_visible((
								SELECT t.oid
								FROM pg_catalog.pg_type t
								JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
								WHERE n.nspname = 'visible_type_second'
									AND t.typname = 'shadowed_domain'
							));`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testpgtypeisvisiblehonorssearchpathshadowingrepro-0001-select-pg_type_is_visible-select-t.oid-from"},
				},
			},
		},
	})
}

// TestPgCastExposesBuiltinCastsRepro reproduces a catalog correctness bug:
// PostgreSQL exposes built-in cast metadata in pg_cast, but Doltgres returns an
// empty stub.
func TestPgCastExposesBuiltinCastsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "pg_cast exposes built-in integer to bigint cast",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT castsource::regtype::text, casttarget::regtype::text, castcontext, castmethod
						FROM pg_catalog.pg_cast
						WHERE castsource = 'integer'::regtype
							AND casttarget = 'bigint'::regtype;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testpgcastexposesbuiltincastsrepro-0001-select-castsource::regtype::text-casttarget::regtype::text-castcontext-castmethod"},
				},
			},
		},
	})
}

// TestPgOperatorEqualityMergeHashFlagsRepro reproduces a catalog correctness
// bug: PostgreSQL marks integer equality as merge-joinable and hash-joinable,
// but Doltgres reports false for both flags.
func TestPgOperatorEqualityMergeHashFlagsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "pg_operator exposes equality merge and hash flags",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT oprcanmerge, oprcanhash
						FROM pg_catalog.pg_operator
						WHERE oprname = '='
							AND oprleft = 'integer'::regtype
							AND oprright = 'integer'::regtype;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testpgoperatorequalitymergehashflagsrepro-0001-select-oprcanmerge-oprcanhash-from-pg_catalog.pg_operator"},
				},
			},
		},
	})
}

// TestPgLanguageExposesBuiltinLanguagesRepro reproduces a catalog correctness
// bug: PostgreSQL exposes installed procedural languages in pg_language, but
// Doltgres returns an empty stub.
func TestPgLanguageExposesBuiltinLanguagesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "pg_language exposes SQL and PLpgSQL languages",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT lanname
						FROM pg_catalog.pg_language
						WHERE lanname IN ('sql', 'plpgsql')
						ORDER BY lanname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testpglanguageexposesbuiltinlanguagesrepro-0001-select-lanname-from-pg_catalog.pg_language-where"},
				},
			},
		},
	})
}

// TestPgTablespaceExposesBuiltinTablespacesRepro reproduces a catalog
// correctness bug: PostgreSQL exposes pg_default and pg_global in
// pg_tablespace, but Doltgres returns an empty stub.
func TestPgTablespaceExposesBuiltinTablespacesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "pg_tablespace exposes built-in tablespaces",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT spcname
						FROM pg_catalog.pg_tablespace
						WHERE spcname IN ('pg_default', 'pg_global')
						ORDER BY spcname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testpgtablespaceexposesbuiltintablespacesrepro-0001-select-spcname-from-pg_catalog.pg_tablespace-where"},
				},
			},
		},
	})
}

// TestPgTimezoneCatalogsExposeUtcRepro reproduces a catalog correctness bug:
// PostgreSQL exposes built-in UTC timezone metadata, but Doltgres returns empty
// stubs for the timezone catalog views.
func TestPgTimezoneCatalogsExposeUtcRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "pg_timezone catalog views expose UTC",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT name, abbrev
						FROM pg_catalog.pg_timezone_names
						WHERE name = 'UTC';`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testpgtimezonecatalogsexposeutcrepro-0001-select-name-abbrev-from-pg_catalog.pg_timezone_names"},
				},
				{
					Query: `SELECT abbrev
						FROM pg_catalog.pg_timezone_abbrevs
						WHERE abbrev = 'UTC';`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testpgtimezonecatalogsexposeutcrepro-0002-select-abbrev-from-pg_catalog.pg_timezone_abbrevs-where"},
				},
			},
		},
	})
}

// TestPgRangeExposesBuiltinRangesRepro reproduces a catalog correctness bug:
// PostgreSQL exposes built-in range type metadata in pg_range, but Doltgres
// returns an empty stub.
func TestPgRangeExposesBuiltinRangesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "pg_range exposes built-in range types",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT rngtypid::regtype::text, rngsubtype::regtype::text
						FROM pg_catalog.pg_range
						WHERE rngtypid::regtype::text = 'int4range';`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testpgrangeexposesbuiltinrangesrepro-0001-select-rngtypid::regtype::text-rngsubtype::regtype::text-from-pg_catalog.pg_range"},
				},
			},
		},
	})
}

// TestTextSearchCatalogsExposeBuiltinsRepro reproduces catalog correctness
// bugs: PostgreSQL exposes built-in text-search metadata, but Doltgres returns
// empty stubs for the text-search catalog tables.
func TestTextSearchCatalogsExposeBuiltinsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "text-search catalogs expose built-ins",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT cfgname
						FROM pg_catalog.pg_ts_config
						WHERE cfgname = 'english';`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testtextsearchcatalogsexposebuiltinsrepro-0001-select-cfgname-from-pg_catalog.pg_ts_config-where"},
				},
				{
					Query: `SELECT dictname
						FROM pg_catalog.pg_ts_dict
						WHERE dictname = 'english_stem';`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testtextsearchcatalogsexposebuiltinsrepro-0002-select-dictname-from-pg_catalog.pg_ts_dict-where"},
				},
				{
					Query: `SELECT prsname
						FROM pg_catalog.pg_ts_parser
						WHERE prsname = 'default';`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testtextsearchcatalogsexposebuiltinsrepro-0003-select-prsname-from-pg_catalog.pg_ts_parser-where"},
				},
				{
					Query: `SELECT tmplname
						FROM pg_catalog.pg_ts_template
						WHERE tmplname = 'simple';`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testtextsearchcatalogsexposebuiltinsrepro-0004-select-tmplname-from-pg_catalog.pg_ts_template-where"},
				},
			},
		},
	})
}

// TestMiscBuiltinCatalogsExposeRowsRepro reproduces catalog correctness bugs:
// PostgreSQL exposes built-in aggregate, conversion, and pg_config rows, but
// Doltgres returns empty stubs.
func TestMiscBuiltinCatalogsExposeRowsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "misc built-in catalogs expose rows",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT name
						FROM pg_catalog.pg_config
						WHERE name = 'BINDIR';`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testmiscbuiltincatalogsexposerowsrepro-0001-select-name-from-pg_catalog.pg_config-where"},
				},
				{
					Query: `SELECT conname
						FROM pg_catalog.pg_conversion
						WHERE conname = 'utf8_to_iso_8859_1';`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testmiscbuiltincatalogsexposerowsrepro-0002-select-conname-from-pg_catalog.pg_conversion-where"},
				},
				{
					Query: `SELECT count(*) > 0
						FROM pg_catalog.pg_aggregate;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testmiscbuiltincatalogsexposerowsrepro-0003-select-count-*->-0"},
				},
			},
		},
	})
}

// TestPgProcExposesBuiltinFunctionsRepro reproduces a catalog correctness bug:
// PostgreSQL exposes built-in functions in pg_proc, but Doltgres returns an
// empty stub.
func TestPgProcExposesBuiltinFunctionsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "pg_proc exposes built-in functions",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT count(*) > 0
						FROM pg_catalog.pg_proc
						WHERE proname = 'abs';`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testpgprocexposesbuiltinfunctionsrepro-0001-select-count-*->-0"},
				},
			},
		},
	})
}

// TestPgInitPrivsExposesBuiltinInitialPrivilegesRepro reproduces a catalog
// correctness bug: PostgreSQL exposes initial privilege metadata for built-in
// objects in pg_init_privs, but Doltgres returns an empty stub.
func TestPgInitPrivsExposesBuiltinInitialPrivilegesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "pg_init_privs exposes built-in initial privileges",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT count(*) > 0
						FROM pg_catalog.pg_init_privs;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testpginitprivsexposesbuiltininitialprivilegesrepro-0001-select-count-*->-0"},
				},
			},
		},
	})
}

// TestAlterRoleSetPopulatesPgDbRoleSettingRepro reproduces a catalog
// persistence bug: PostgreSQL persists ALTER ROLE ... SET configuration in
// pg_db_role_setting.
func TestAlterRoleSetPopulatesPgDbRoleSettingRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER ROLE SET populates pg_db_role_setting",
			SetUpScript: []string{
				`CREATE ROLE role_setting_catalog;`,
				`ALTER ROLE role_setting_catalog SET work_mem = '64kB';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT setrole::regrole::text, setdatabase, array_to_string(setconfig, ',')
						FROM pg_catalog.pg_db_role_setting
						WHERE setrole = 'role_setting_catalog'::regrole;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testalterrolesetpopulatespgdbrolesettingrepro-0001-select-setrole::regrole::text-setdatabase-array_to_string-setconfig"},
				},
			},
		},
	})
}

// TestAlterRoleResetSettingRepro reproduces a PostgreSQL compatibility gap:
// ALTER ROLE ... RESET should be accepted, even when there is no stored value.
func TestAlterRoleResetSettingRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER ROLE RESET setting succeeds",
			SetUpScript: []string{
				`CREATE ROLE role_reset_setting_catalog;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER ROLE role_reset_setting_catalog RESET work_mem;`,
				},
				{
					Query: `SELECT COUNT(*)
						FROM pg_catalog.pg_db_role_setting
						WHERE setrole = 'role_reset_setting_catalog'::regrole;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testalterroleresetsettingrepro-0001-select-count-*-from-pg_catalog.pg_db_role_setting"},
				},
			},
		},
	})
}

// TestAlterRoleInDatabaseSetPopulatesPgDbRoleSettingRepro reproduces a catalog
// persistence bug: PostgreSQL persists role settings scoped to a specific
// database in pg_db_role_setting.
func TestAlterRoleInDatabaseSetPopulatesPgDbRoleSettingRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER ROLE IN DATABASE SET populates pg_db_role_setting",
			SetUpScript: []string{
				`CREATE ROLE role_database_setting_catalog;`,
				`CREATE DATABASE role_database_setting_db;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER ROLE role_database_setting_catalog
						IN DATABASE role_database_setting_db
						SET work_mem = '64kB';`,
				},
				{
					Query: `SELECT setrole::regrole::text, datname, array_to_string(setconfig, ',')
						FROM pg_catalog.pg_db_role_setting
						JOIN pg_catalog.pg_database ON setdatabase = pg_database.oid
						WHERE setrole = 'role_database_setting_catalog'::regrole;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testalterroleindatabasesetpopulatespgdbrolesettingrepro-0001-select-setrole::regrole::text-datname-array_to_string-setconfig"},
				},
			},
		},
	})
}

// TestCompositeTypeCatalogRelidRepro reproduces a catalog correctness bug:
// CREATE TYPE ... AS (...) should create a composite pg_class row and point
// pg_type.typrelid at it.
func TestCompositeTypeCatalogRelidRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "composite types expose pg_type typrelid and pg_class row",
			SetUpScript: []string{
				`CREATE TYPE composite_catalog_type AS (
					id INTEGER,
					label TEXT
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT typname, typtype, typrelid <> 0::oid
						FROM pg_catalog.pg_type
						WHERE typname = 'composite_catalog_type';`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testcompositetypecatalogrelidrepro-0001-select-typname-typtype-typrelid-<>"},
				},
				{
					Query: `SELECT c.relname, c.relkind
						FROM pg_catalog.pg_class c
						JOIN pg_catalog.pg_type t ON t.typrelid = c.oid
						WHERE t.typname = 'composite_catalog_type';`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testcompositetypecatalogrelidrepro-0002-select-c.relname-c.relkind-from-pg_catalog.pg_class"},
				},
			},
		},
	})
}

// TestFormatTypeInvalidOidRepro reproduces a catalog correctness bug:
// PostgreSQL renders InvalidOid as "-", while Doltgres treats it like an
// arbitrary unknown type OID.
func TestFormatTypeInvalidOidRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "format_type renders InvalidOid",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT format_type(0::oid, NULL), format_type(0::oid, 20);`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testformattypeinvalidoidrepro-0001-select-format_type-0::oid-null-format_type"},
				},
			},
		},
	})
}

// TestFormatTypeDomainAttributeRepro reproduces a catalog correctness bug:
// PostgreSQL format_type renders domain OIDs from pg_attribute as the domain
// type name.
func TestFormatTypeDomainAttributeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "format_type renders domain attribute types",
			SetUpScript: []string{
				`CREATE DOMAIN format_type_domain AS integer
					CHECK (VALUE > 0);`,
				`CREATE TABLE format_type_domain_items (
					amount format_type_domain
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT format_type(atttypid, atttypmod)
						FROM pg_catalog.pg_attribute
						WHERE attrelid = 'format_type_domain_items'::regclass
							AND attname = 'amount';`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testformattypedomainattributerepro-0001-select-format_type-atttypid-atttypmod-from"},
				},
			},
		},
	})
}

// TestRegroleTypeResolvesRolesRepro reproduces a catalog type correctness bug:
// PostgreSQL's regrole pseudo-OID type resolves role names and InvalidOid.
func TestRegroleTypeResolvesRolesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "regrole resolves role names",
			SetUpScript: []string{
				`CREATE ROLE regrole_catalog_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT 'regrole_catalog_user'::regrole::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testregroletyperesolvesrolesrepro-0001-select-regrole_catalog_user-::regrole::text"},
				},
				{
					Query: `SELECT 0::regrole::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testregroletyperesolvesrolesrepro-0002-select-0::regrole::text"},
				},
			},
		},
	})
}

// TestAdditionalRegTypesResolveBuiltinsRepro reproduces catalog type
// correctness bugs: several PostgreSQL reg* pseudo-OID types are not available
// even though built-in clients use them for object introspection.
func TestAdditionalRegTypesResolveBuiltinsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "additional reg types resolve built-ins",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT 'abs(integer)'::regprocedure::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testadditionalregtypesresolvebuiltinsrepro-0001-select-abs-integer-::regprocedure::text"},
				},
				{
					Query: `SELECT '+(integer,integer)'::regoperator::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testadditionalregtypesresolvebuiltinsrepro-0002-select-+-integer-integer-::regoperator::text"},
				},
				{
					Query: `SELECT 'english'::regconfig::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testadditionalregtypesresolvebuiltinsrepro-0003-select-english-::regconfig::text"},
				},
				{
					Query: `SELECT 'simple'::regdictionary::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testadditionalregtypesresolvebuiltinsrepro-0004-select-simple-::regdictionary::text"},
				},
			},
		},
	})
}

// TestToRegnamespaceResolvesSchemaNamesRepro reproduces a catalog lookup
// correctness bug: PostgreSQL exposes to_regnamespace(text) as a null-on-miss
// helper for schema OID lookup.
func TestToRegnamespaceResolvesSchemaNamesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "to_regnamespace resolves schema names",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT to_regnamespace('pg_catalog')::text,
						to_regnamespace('missing_schema_for_to_regnamespace') IS NULL;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testtoregnamespaceresolvesschemanamesrepro-0001-select-to_regnamespace-pg_catalog-::text-to_regnamespace"},
				},
			},
		},
	})
}

// TestToRegprocedureResolvesFunctionSignaturesRepro reproduces a catalog lookup
// correctness bug: PostgreSQL exposes to_regprocedure(text) as a null-on-miss
// helper for function signature OID lookup.
func TestToRegprocedureResolvesFunctionSignaturesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "to_regprocedure resolves function signatures",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT to_regprocedure('array_in(cstring,oid,integer)')::text,
						to_regprocedure('missing_function_for_to_regprocedure(integer)') IS NULL;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testtoregprocedureresolvesfunctionsignaturesrepro-0001-select-to_regprocedure-array_in-cstring-oid"},
				},
			},
		},
	})
}

// TestIntervalTypmodCatalogMetadataRepro reproduces a catalog correctness bug:
// interval field restrictions and fractional precision should round-trip
// through pg_attribute.atttypmod and format_type.
func TestIntervalTypmodCatalogMetadataRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "interval typmods round-trip through catalog metadata",
			SetUpScript: []string{
				`CREATE TABLE interval_typmod_catalog (
					ym INTERVAL YEAR TO MONTH,
					ds3 INTERVAL DAY TO SECOND(3),
					p2 INTERVAL(2)
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT attname, atttypmod, format_type(atttypid, atttypmod)
						FROM pg_catalog.pg_attribute
						WHERE attrelid = 'interval_typmod_catalog'::regclass
							AND attnum > 0
						ORDER BY attnum;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testintervaltypmodcatalogmetadatarepro-0001-select-attname-atttypmod-format_type-atttypid"},
				},
			},
		},
	})
}

// TestTemporaryTableRelpersistenceCatalogMetadataRepro reproduces a catalog
// persistence bug: temporary tables should have pg_class rows marked with
// relpersistence = 't'.
func TestTemporaryTableRelpersistenceCatalogMetadataRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "temporary table relpersistence metadata",
			SetUpScript: []string{
				`CREATE TEMPORARY TABLE temp_rel_persistence (id INT);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT relpersistence
						FROM pg_catalog.pg_class
						WHERE relname = 'temp_rel_persistence';`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testtemporarytablerelpersistencecatalogmetadatarepro-0001-select-relpersistence-from-pg_catalog.pg_class-where"},
				},
			},
		},
	})
}

// TestUnloggedTableRelpersistenceCatalogMetadataRepro reproduces a persistence
// correctness bug: PostgreSQL supports unlogged tables and marks their
// pg_class rows with relpersistence = 'u'.
func TestUnloggedTableRelpersistenceCatalogMetadataRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "unlogged table relpersistence metadata",
			SetUpScript: []string{
				`CREATE UNLOGGED TABLE unlogged_rel_persistence (id INT);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT relpersistence
						FROM pg_catalog.pg_class
						WHERE relname = 'unlogged_rel_persistence';`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testunloggedtablerelpersistencecatalogmetadatarepro-0001-select-relpersistence-from-pg_catalog.pg_class-where"},
				},
			},
		},
	})
}

// TestUnloggedSequenceRelpersistenceCatalogMetadataRepro reproduces a
// persistence correctness bug: PostgreSQL supports unlogged sequences and
// marks their pg_class rows with relpersistence = 'u'.
func TestUnloggedSequenceRelpersistenceCatalogMetadataRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "unlogged sequence relpersistence metadata",
			SetUpScript: []string{
				`CREATE UNLOGGED SEQUENCE unlogged_sequence_rel_persistence;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT relpersistence
						FROM pg_catalog.pg_class
						WHERE relname = 'unlogged_sequence_rel_persistence';`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testunloggedsequencerelpersistencecatalogmetadatarepro-0001-select-relpersistence-from-pg_catalog.pg_class-where"},
				},
			},
		},
	})
}

// TestPgClassColumnAndCheckCountsRepro reproduces a catalog correctness bug:
// pg_class should expose the number of user columns and check constraints for
// ordinary tables.
func TestPgClassColumnAndCheckCountsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_class exposes relnatts and relchecks",
			SetUpScript: []string{
				`CREATE TABLE pg_class_count_target (
					id INT PRIMARY KEY,
					amount INT CHECK (amount > 0),
					label TEXT
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT relnatts, relchecks
						FROM pg_catalog.pg_class
						WHERE oid = 'pg_class_count_target'::regclass;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testpgclasscolumnandcheckcountsrepro-0001-select-relnatts-relchecks-from-pg_catalog.pg_class"},
				},
			},
		},
	})
}

// TestPgClassViewRuleMetadataRepro reproduces a catalog correctness bug:
// views should expose their rewrite-rule-backed shape in pg_class.
func TestPgClassViewRuleMetadataRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_class exposes view column and rule metadata",
			SetUpScript: []string{
				`CREATE TABLE pg_class_view_source (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`CREATE VIEW pg_class_view_metadata AS
					SELECT id, label FROM pg_class_view_source;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT relnatts, relhasrules
						FROM pg_catalog.pg_class
						WHERE oid = 'pg_class_view_metadata'::regclass;`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testpgclassviewrulemetadatarepro-0001-select-relnatts-relhasrules-from-pg_catalog.pg_class"},
				},
			},
		},
	})
}

// TestCreateViewPopulatesPgRewriteRepro reproduces a catalog persistence bug:
// CREATE VIEW should create a pg_rewrite _RETURN rule for the view.
func TestCreateViewPopulatesPgRewriteRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE VIEW populates pg_rewrite",
			SetUpScript: []string{
				`CREATE TABLE rewrite_catalog_base (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`CREATE VIEW rewrite_catalog_view AS
					SELECT id, label FROM rewrite_catalog_base;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT rulename
						FROM pg_catalog.pg_rewrite
						WHERE ev_class = 'rewrite_catalog_view'::regclass
							AND rulename = '_RETURN';`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testcreateviewpopulatespgrewriterepro-0001-select-rulename-from-pg_catalog.pg_rewrite-where"},
				},
			},
		},
	})
}

// TestColumnDefaultSequenceDependencyPopulatesPgDependRepro reproduces a
// catalog correctness bug: defaults that call nextval() should create pg_depend
// rows linking the default expression to the referenced sequence.
func TestColumnDefaultSequenceDependencyPopulatesPgDependRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "column default sequence dependency populates pg_depend",
			SetUpScript: []string{
				`CREATE SEQUENCE depend_catalog_seq;`,
				`CREATE TABLE depend_catalog_items (
					id INT DEFAULT nextval('depend_catalog_seq')
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT d.deptype
						FROM pg_catalog.pg_depend d
						JOIN pg_catalog.pg_class s ON d.refobjid = s.oid
						JOIN pg_catalog.pg_attrdef ad ON d.objid = ad.oid
						JOIN pg_catalog.pg_class t ON ad.adrelid = t.oid
						WHERE s.relname = 'depend_catalog_seq'
							AND t.relname = 'depend_catalog_items';`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testcolumndefaultsequencedependencypopulatespgdependrepro-0001-select-d.deptype-from-pg_catalog.pg_depend-d"},
				},
			},
		},
	})
}

// TestTableOwnershipPopulatesPgShdependRepro reproduces a catalog persistence
// bug: shared dependencies should record role ownership of relations.
func TestTableOwnershipPopulatesPgShdependRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "table ownership populates pg_shdepend",
			SetUpScript: []string{
				`CREATE ROLE shdepend_catalog_owner;`,
				`CREATE TABLE shdepend_catalog_items (id INT);`,
				`ALTER TABLE shdepend_catalog_items OWNER TO shdepend_catalog_owner;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT d.deptype
						FROM pg_catalog.pg_shdepend d
						JOIN pg_catalog.pg_class c ON d.objid = c.oid
						JOIN pg_catalog.pg_roles r ON d.refobjid = r.oid
						WHERE c.relname = 'shdepend_catalog_items'
							AND r.rolname = 'shdepend_catalog_owner';`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testtableownershippopulatespgshdependrepro-0001-select-d.deptype-from-pg_catalog.pg_shdepend-d"},
				},
			},
		},
	})
}

// TestPgTypeRegprocColumnComparisonGuard guards catalog query compatibility:
// PostgreSQL can compare pg_type.typsubscript directly with a regproc literal.
func TestPgTypeRegprocColumnComparisonGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_type typsubscript compares to regproc literal",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT t1.oid, t1.typname AS basetype, t2.typname AS arraytype, t2.typsubscript
						FROM pg_catalog.pg_type t1
						LEFT JOIN pg_catalog.pg_type t2 ON t1.typarray = t2.oid
						WHERE t1.typarray <> 0
							AND (t2.oid IS NULL OR t2.typsubscript <> 'array_subscript_handler'::regproc);`, PostgresOracle: ScriptTestPostgresOracle{ID: "catalog-correctness-repro-test-testpgtyperegproccolumncomparisonguard-0001-select-t1.oid-t1.typname-as-basetype"},
				},
			},
		},
	})
}
