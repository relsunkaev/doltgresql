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

// TestPgRelationIsPublishableClassifiesRelationsRepro reproduces a publication
// catalog correctness bug: PostgreSQL classifies ordinary persistent tables as
// publishable while excluding views.
func TestPgRelationIsPublishableClassifiesRelationsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_relation_is_publishable classifies publication-eligible relations",
			SetUpScript: []string{
				`CREATE TABLE publishable_regular_items (id INT PRIMARY KEY);`,
				`CREATE VIEW publishable_view_items AS
					SELECT id FROM publishable_regular_items;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT
							pg_relation_is_publishable('publishable_regular_items'::regclass),
							pg_relation_is_publishable('publishable_view_items'::regclass);`,
					Expected: []sql.Row{{"t", "f"}},
				},
			},
		},
	})
}

// TestDropPublicationMissingNameIsAtomic guards multi-name DROP PUBLICATION
// atomicity when one of the requested publications does not exist.
func TestDropPublicationMissingNameIsAtomic(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP PUBLICATION missing name preserves existing publications",
			SetUpScript: []string{
				`CREATE PUBLICATION publication_partial_drop_pub;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DROP PUBLICATION publication_partial_drop_pub, publication_missing_drop_pub;`,
					ExpectedErr: `does not exist`,
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_publication
						WHERE pubname = 'publication_partial_drop_pub';`,
					Expected: []sql.Row{{1}},
				},
			},
		},
	})
}

// TestPublicationColumnListRejectsDuplicateColumnsRepro reproduces a
// publication metadata correctness bug: PostgreSQL rejects duplicate column
// names in publication column lists.
func TestPublicationColumnListRejectsDuplicateColumnsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE PUBLICATION rejects duplicate column list entries",
			SetUpScript: []string{
				`CREATE TABLE publication_duplicate_columns (
					id INT PRIMARY KEY,
					label TEXT
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE PUBLICATION publication_duplicate_columns_create_pub
						FOR TABLE publication_duplicate_columns (id, id);`,
					ExpectedErr: `duplicate column`,
				},
			},
		},
	})
}

// TestPublicationAddTableColumnListRejectsDuplicateColumnsRepro reproduces a
// publication metadata correctness bug: PostgreSQL rejects duplicate column
// names when adding a table to a publication.
func TestPublicationAddTableColumnListRejectsDuplicateColumnsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER PUBLICATION ADD TABLE rejects duplicate column list entries",
			SetUpScript: []string{
				`CREATE TABLE publication_duplicate_add_columns (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`CREATE PUBLICATION publication_duplicate_add_columns_pub;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER PUBLICATION publication_duplicate_add_columns_pub
						ADD TABLE publication_duplicate_add_columns (id, id);`,
					ExpectedErr: `duplicate column`,
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_publication_rel pr
						JOIN pg_catalog.pg_publication p ON p.oid = pr.prpubid
						WHERE p.pubname = 'publication_duplicate_add_columns_pub';`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestPublicationSetTableColumnListRejectsDuplicateColumnsRepro reproduces a
// publication metadata correctness bug: PostgreSQL rejects duplicate column
// names when replacing a publication's table membership.
func TestPublicationSetTableColumnListRejectsDuplicateColumnsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER PUBLICATION SET TABLE rejects duplicate column list entries",
			SetUpScript: []string{
				`CREATE TABLE publication_duplicate_set_columns (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`CREATE PUBLICATION publication_duplicate_set_columns_pub;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER PUBLICATION publication_duplicate_set_columns_pub
						SET TABLE publication_duplicate_set_columns (id, id);`,
					ExpectedErr: `duplicate column`,
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_publication_rel pr
						JOIN pg_catalog.pg_publication p ON p.oid = pr.prpubid
						WHERE p.pubname = 'publication_duplicate_set_columns_pub';`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestPublicationAllowsDuplicatePlainTablesRepro reproduces a publication
// correctness bug: PostgreSQL accepts redundant duplicate table entries when no
// row filter or column list makes the duplicate ambiguous.
func TestPublicationAllowsDuplicatePlainTablesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE PUBLICATION accepts duplicate plain table entries",
			SetUpScript: []string{
				`CREATE TABLE publication_duplicate_plain_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE PUBLICATION publication_duplicate_plain_pub
						FOR TABLE publication_duplicate_plain_items,
							publication_duplicate_plain_items;`,
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_publication_rel pr
						JOIN pg_catalog.pg_publication p ON p.oid = pr.prpubid
						WHERE p.pubname = 'publication_duplicate_plain_pub';`,
					Expected: []sql.Row{{1}},
				},
			},
		},
	})
}

// TestPublicationAddDuplicatePlainTablesRepro reproduces a publication
// correctness bug: PostgreSQL accepts redundant duplicate table entries in
// ALTER PUBLICATION ADD TABLE when the duplicates have no row filters or
// column lists.
func TestPublicationAddDuplicatePlainTablesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER PUBLICATION ADD TABLE accepts duplicate plain table entries",
			SetUpScript: []string{
				`CREATE TABLE publication_duplicate_add_plain_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`CREATE PUBLICATION publication_duplicate_add_plain_pub;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER PUBLICATION publication_duplicate_add_plain_pub
						ADD TABLE publication_duplicate_add_plain_items,
							publication_duplicate_add_plain_items;`,
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_publication_rel pr
						JOIN pg_catalog.pg_publication p ON p.oid = pr.prpubid
						WHERE p.pubname = 'publication_duplicate_add_plain_pub';`,
					Expected: []sql.Row{{1}},
				},
			},
		},
	})
}

// TestPublicationSetDuplicatePlainTablesRepro reproduces a publication
// correctness bug: PostgreSQL accepts redundant duplicate table entries in
// ALTER PUBLICATION SET TABLE when the duplicates have no row filters or
// column lists.
func TestPublicationSetDuplicatePlainTablesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER PUBLICATION SET TABLE accepts duplicate plain table entries",
			SetUpScript: []string{
				`CREATE TABLE publication_duplicate_set_plain_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`CREATE PUBLICATION publication_duplicate_set_plain_pub;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER PUBLICATION publication_duplicate_set_plain_pub
						SET TABLE publication_duplicate_set_plain_items,
							publication_duplicate_set_plain_items;`,
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_publication_rel pr
						JOIN pg_catalog.pg_publication p ON p.oid = pr.prpubid
						WHERE p.pubname = 'publication_duplicate_set_plain_pub';`,
					Expected: []sql.Row{{1}},
				},
			},
		},
	})
}

// TestPublicationCreateRejectsDuplicatePublishOptionRepro reproduces a
// publication option correctness bug: PostgreSQL rejects duplicate option names
// instead of silently keeping one value.
func TestPublicationCreateRejectsDuplicatePublishOptionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE PUBLICATION rejects duplicate publish options",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE PUBLICATION publication_duplicate_publish_option_pub
						WITH (publish = 'insert', publish = 'update');`,
					ExpectedErr: `conflicting or redundant options`,
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_publication
						WHERE pubname = 'publication_duplicate_publish_option_pub';`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestPublicationCreateRejectsDuplicatePublishViaRootOptionRepro reproduces a
// publication option correctness bug: PostgreSQL rejects duplicate
// publish_via_partition_root options instead of silently keeping one value.
func TestPublicationCreateRejectsDuplicatePublishViaRootOptionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE PUBLICATION rejects duplicate publish_via_partition_root options",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE PUBLICATION publication_duplicate_via_root_option_pub
						WITH (publish_via_partition_root = true,
							publish_via_partition_root = false);`,
					ExpectedErr: `conflicting or redundant options`,
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_publication
						WHERE pubname = 'publication_duplicate_via_root_option_pub';`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestPublicationAlterRejectsDuplicatePublishOptionRepro reproduces a
// publication option consistency bug: duplicate publish options in ALTER
// PUBLICATION must be rejected before changing the publication flags.
func TestPublicationAlterRejectsDuplicatePublishOptionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER PUBLICATION rejects duplicate publish options",
			SetUpScript: []string{
				`CREATE PUBLICATION publication_alter_duplicate_publish_option_pub;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER PUBLICATION publication_alter_duplicate_publish_option_pub
						SET (publish = 'insert', publish = 'update');`,
					ExpectedErr: `conflicting or redundant options`,
				},
				{
					Query: `SELECT pubinsert, pubupdate, pubdelete, pubtruncate
						FROM pg_catalog.pg_publication
						WHERE pubname = 'publication_alter_duplicate_publish_option_pub';`,
					Expected: []sql.Row{{"t", "t", "t", "t"}},
				},
			},
		},
	})
}

// TestPublicationAlterRejectsDuplicatePublishViaRootOptionRepro reproduces a
// publication option correctness bug: duplicate publish_via_partition_root
// options in ALTER PUBLICATION must be rejected.
func TestPublicationAlterRejectsDuplicatePublishViaRootOptionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER PUBLICATION rejects duplicate publish_via_partition_root options",
			SetUpScript: []string{
				`CREATE PUBLICATION publication_alter_duplicate_via_root_option_pub
					WITH (publish_via_partition_root = false);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER PUBLICATION publication_alter_duplicate_via_root_option_pub
						SET (publish_via_partition_root = true,
							publish_via_partition_root = false);`,
					ExpectedErr: `conflicting or redundant options`,
				},
			},
		},
	})
}

// TestPublicationAllowsEmptyPublishOptionRepro reproduces a publication option
// correctness bug: PostgreSQL accepts an empty publish action list and stores a
// publication that publishes no actions.
func TestPublicationAllowsEmptyPublishOptionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE PUBLICATION accepts empty publish option",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE PUBLICATION publication_empty_publish_option_pub
						WITH (publish = '');`,
				},
				{
					Query: `SELECT pubinsert, pubupdate, pubdelete, pubtruncate
						FROM pg_catalog.pg_publication
						WHERE pubname = 'publication_empty_publish_option_pub';`,
					Expected: []sql.Row{{"f", "f", "f", "f"}},
				},
			},
		},
	})
}

// TestPublicationCreateSchemaCurrentSchemaResolvesSearchPathRepro reproduces a
// publication schema-list correctness bug: PostgreSQL resolves CURRENT_SCHEMA
// in schema publication lists to the active search-path schema.
func TestPublicationCreateSchemaCurrentSchemaResolvesSearchPathRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE PUBLICATION resolves CURRENT_SCHEMA in schema lists",
			SetUpScript: []string{
				`CREATE SCHEMA publication_current_schema_create_actual;`,
				`CREATE TABLE publication_current_schema_create_actual.items (
					id INT PRIMARY KEY
				);`,
				`SET search_path TO publication_current_schema_create_actual;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE PUBLICATION publication_current_schema_create_pub
						FOR TABLES IN SCHEMA CURRENT_SCHEMA;`,
				},
				{
					Query: `SELECT p.pubname, n.nspname
						FROM pg_catalog.pg_publication_namespace pn
						JOIN pg_catalog.pg_publication p ON p.oid = pn.pnpubid
						JOIN pg_catalog.pg_namespace n ON n.oid = pn.pnnspid
						WHERE p.pubname = 'publication_current_schema_create_pub';`,
					Expected: []sql.Row{{"publication_current_schema_create_pub", "publication_current_schema_create_actual"}},
				},
			},
		},
	})
}

// TestPublicationAddSchemaCurrentSchemaResolvesSearchPathRepro reproduces a
// publication schema-list correctness bug: ALTER PUBLICATION ADD TABLES IN
// SCHEMA CURRENT_SCHEMA should add the active search-path schema.
func TestPublicationAddSchemaCurrentSchemaResolvesSearchPathRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER PUBLICATION ADD SCHEMA resolves CURRENT_SCHEMA",
			SetUpScript: []string{
				`CREATE SCHEMA publication_current_schema_add_actual;`,
				`CREATE TABLE publication_current_schema_add_actual.items (
					id INT PRIMARY KEY
				);`,
				`CREATE PUBLICATION publication_current_schema_add_pub;`,
				`SET search_path TO publication_current_schema_add_actual;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER PUBLICATION publication_current_schema_add_pub
						ADD TABLES IN SCHEMA CURRENT_SCHEMA;`,
				},
				{
					Query: `SELECT p.pubname, n.nspname
						FROM pg_catalog.pg_publication_namespace pn
						JOIN pg_catalog.pg_publication p ON p.oid = pn.pnpubid
						JOIN pg_catalog.pg_namespace n ON n.oid = pn.pnnspid
						WHERE p.pubname = 'publication_current_schema_add_pub';`,
					Expected: []sql.Row{{"publication_current_schema_add_pub", "publication_current_schema_add_actual"}},
				},
			},
		},
	})
}

// TestPublicationSetSchemaCurrentSchemaResolvesSearchPathRepro reproduces a
// publication schema-list correctness bug: ALTER PUBLICATION SET TABLES IN
// SCHEMA CURRENT_SCHEMA should replace membership with the active search-path
// schema.
func TestPublicationSetSchemaCurrentSchemaResolvesSearchPathRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER PUBLICATION SET SCHEMA resolves CURRENT_SCHEMA",
			SetUpScript: []string{
				`CREATE SCHEMA publication_current_schema_set_actual;`,
				`CREATE TABLE publication_current_schema_set_actual.items (
					id INT PRIMARY KEY
				);`,
				`CREATE PUBLICATION publication_current_schema_set_pub;`,
				`SET search_path TO publication_current_schema_set_actual;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER PUBLICATION publication_current_schema_set_pub
						SET TABLES IN SCHEMA CURRENT_SCHEMA;`,
				},
				{
					Query: `SELECT p.pubname, n.nspname
						FROM pg_catalog.pg_publication_namespace pn
						JOIN pg_catalog.pg_publication p ON p.oid = pn.pnpubid
						JOIN pg_catalog.pg_namespace n ON n.oid = pn.pnnspid
						WHERE p.pubname = 'publication_current_schema_set_pub';`,
					Expected: []sql.Row{{"publication_current_schema_set_pub", "publication_current_schema_set_actual"}},
				},
			},
		},
	})
}

// TestPublicationDropSchemaCurrentSchemaResolvesSearchPathRepro reproduces a
// publication schema-list consistency bug: ALTER PUBLICATION DROP TABLES IN
// SCHEMA CURRENT_SCHEMA should remove the active search-path schema membership.
func TestPublicationDropSchemaCurrentSchemaResolvesSearchPathRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER PUBLICATION DROP SCHEMA resolves CURRENT_SCHEMA",
			SetUpScript: []string{
				`CREATE SCHEMA publication_current_schema_drop_actual;`,
				`CREATE TABLE publication_current_schema_drop_actual.items (
					id INT PRIMARY KEY
				);`,
				`CREATE PUBLICATION publication_current_schema_drop_pub
					FOR TABLES IN SCHEMA publication_current_schema_drop_actual;`,
				`SET search_path TO publication_current_schema_drop_actual;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER PUBLICATION publication_current_schema_drop_pub
						DROP TABLES IN SCHEMA CURRENT_SCHEMA;`,
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_publication_namespace pn
						JOIN pg_catalog.pg_publication p ON p.oid = pn.pnpubid
						WHERE p.pubname = 'publication_current_schema_drop_pub';`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestPublicationCreateDuplicateSchemasNormalizes guards publication
// schema-list parity: PostgreSQL accepts redundant duplicate schemas in CREATE
// PUBLICATION and stores one namespace membership.
func TestPublicationCreateDuplicateSchemasNormalizes(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE PUBLICATION normalizes duplicate schemas",
			SetUpScript: []string{
				`CREATE SCHEMA publication_duplicate_schema_create;`,
				`CREATE TABLE publication_duplicate_schema_create.items (
					id INT PRIMARY KEY
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE PUBLICATION publication_duplicate_schema_create_pub
						FOR TABLES IN SCHEMA publication_duplicate_schema_create,
							publication_duplicate_schema_create;`,
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_publication_namespace pn
						JOIN pg_catalog.pg_publication p ON p.oid = pn.pnpubid
						WHERE p.pubname = 'publication_duplicate_schema_create_pub';`,
					Expected: []sql.Row{{1}},
				},
			},
		},
	})
}

// TestPublicationSetDuplicateSchemasNormalizes guards publication schema-list
// parity: PostgreSQL accepts redundant duplicate schemas in ALTER PUBLICATION
// SET and stores one namespace membership.
func TestPublicationSetDuplicateSchemasNormalizes(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER PUBLICATION SET SCHEMA normalizes duplicate schemas",
			SetUpScript: []string{
				`CREATE SCHEMA publication_duplicate_schema_set;`,
				`CREATE TABLE publication_duplicate_schema_set.items (
					id INT PRIMARY KEY
				);`,
				`CREATE PUBLICATION publication_duplicate_schema_set_pub;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER PUBLICATION publication_duplicate_schema_set_pub
						SET TABLES IN SCHEMA publication_duplicate_schema_set,
							publication_duplicate_schema_set;`,
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_publication_namespace pn
						JOIN pg_catalog.pg_publication p ON p.oid = pn.pnpubid
						WHERE p.pubname = 'publication_duplicate_schema_set_pub';`,
					Expected: []sql.Row{{1}},
				},
			},
		},
	})
}

// TestPublicationAddExistingSchemaRejectsDuplicate guards publication
// schema-list parity: PostgreSQL rejects adding a schema that is already a
// member of the publication and leaves membership unchanged.
func TestPublicationAddExistingSchemaRejectsDuplicate(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER PUBLICATION ADD SCHEMA rejects existing schema membership",
			SetUpScript: []string{
				`CREATE SCHEMA publication_duplicate_schema_add;`,
				`CREATE TABLE publication_duplicate_schema_add.items (
					id INT PRIMARY KEY
				);`,
				`CREATE PUBLICATION publication_duplicate_schema_add_pub
					FOR TABLES IN SCHEMA publication_duplicate_schema_add;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER PUBLICATION publication_duplicate_schema_add_pub
						ADD TABLES IN SCHEMA publication_duplicate_schema_add;`,
					ExpectedErr: `already member of publication`,
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_publication_namespace pn
						JOIN pg_catalog.pg_publication p ON p.oid = pn.pnpubid
						WHERE p.pubname = 'publication_duplicate_schema_add_pub';`,
					Expected: []sql.Row{{1}},
				},
			},
		},
	})
}

// TestPublicationCreateRejectsColumnListWithSchemaRepro reproduces a
// publication metadata correctness bug: PostgreSQL rejects publication column
// lists when any schema is part of the same publication.
func TestPublicationCreateRejectsColumnListWithSchemaRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE PUBLICATION rejects column list with schema publication",
			SetUpScript: []string{
				`CREATE SCHEMA publication_column_schema_create;`,
				`CREATE TABLE publication_column_schema_create.items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE PUBLICATION publication_column_schema_create_pub
						FOR TABLES IN SCHEMA publication_column_schema_create,
							TABLE publication_column_schema_create.items (id);`,
					ExpectedErr: `cannot use column list`,
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_publication
						WHERE pubname = 'publication_column_schema_create_pub';`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestPublicationAddTableRejectsColumnListWithSchemaPublicationRepro
// reproduces a publication metadata consistency bug: PostgreSQL rejects adding
// a table column list to a publication that already contains schema
// membership.
func TestPublicationAddTableRejectsColumnListWithSchemaPublicationRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER PUBLICATION ADD TABLE rejects column list with existing schema",
			SetUpScript: []string{
				`CREATE SCHEMA publication_column_schema_add_table;`,
				`CREATE TABLE publication_column_schema_add_table.items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`CREATE PUBLICATION publication_column_schema_add_table_pub
					FOR TABLES IN SCHEMA publication_column_schema_add_table;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER PUBLICATION publication_column_schema_add_table_pub
						ADD TABLE publication_column_schema_add_table.items (id);`,
					ExpectedErr: `cannot use column list`,
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_publication_rel pr
						JOIN pg_catalog.pg_publication p ON p.oid = pr.prpubid
						WHERE p.pubname = 'publication_column_schema_add_table_pub';`,
					Expected: []sql.Row{{0}},
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_publication_namespace pn
						JOIN pg_catalog.pg_publication p ON p.oid = pn.pnpubid
						WHERE p.pubname = 'publication_column_schema_add_table_pub';`,
					Expected: []sql.Row{{1}},
				},
			},
		},
	})
}

// TestPublicationAddSchemaRejectsExistingColumnListRepro reproduces a
// publication metadata consistency bug: PostgreSQL rejects adding schema
// membership to a publication that already contains a table column list.
func TestPublicationAddSchemaRejectsExistingColumnListRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER PUBLICATION ADD SCHEMA rejects existing column list",
			SetUpScript: []string{
				`CREATE SCHEMA publication_column_schema_add_schema;`,
				`CREATE TABLE publication_column_schema_add_schema.items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`CREATE PUBLICATION publication_column_schema_add_schema_pub
					FOR TABLE publication_column_schema_add_schema.items (id);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER PUBLICATION publication_column_schema_add_schema_pub
						ADD TABLES IN SCHEMA publication_column_schema_add_schema;`,
					ExpectedErr: `cannot add schema`,
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_publication_rel pr
						JOIN pg_catalog.pg_publication p ON p.oid = pr.prpubid
						WHERE p.pubname = 'publication_column_schema_add_schema_pub';`,
					Expected: []sql.Row{{1}},
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_publication_namespace pn
						JOIN pg_catalog.pg_publication p ON p.oid = pn.pnpubid
						WHERE p.pubname = 'publication_column_schema_add_schema_pub';`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestPublicationSetRejectsColumnListWithSchemaRepro reproduces a publication
// metadata consistency bug: PostgreSQL rejects ALTER PUBLICATION SET when the
// replacement mixes schema membership with a table column list.
func TestPublicationSetRejectsColumnListWithSchemaRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER PUBLICATION SET rejects column list with schema",
			SetUpScript: []string{
				`CREATE SCHEMA publication_column_schema_set;`,
				`CREATE TABLE publication_column_schema_set.items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`CREATE PUBLICATION publication_column_schema_set_pub;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER PUBLICATION publication_column_schema_set_pub
						SET TABLES IN SCHEMA publication_column_schema_set,
							TABLE publication_column_schema_set.items (id);`,
					ExpectedErr: `cannot use column list`,
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_publication_rel pr
						JOIN pg_catalog.pg_publication p ON p.oid = pr.prpubid
						WHERE p.pubname = 'publication_column_schema_set_pub';`,
					Expected: []sql.Row{{0}},
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_publication_namespace pn
						JOIN pg_catalog.pg_publication p ON p.oid = pn.pnpubid
						WHERE p.pubname = 'publication_column_schema_set_pub';`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestPublicationColumnListRespectsQuotedColumnCaseRepro reproduces a
// publication metadata correctness bug: PostgreSQL does not resolve an unquoted
// lower-case column list entry to a quoted mixed-case column.
func TestPublicationColumnListRespectsQuotedColumnCaseRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE PUBLICATION column list respects quoted column case",
			SetUpScript: []string{
				`CREATE TABLE publication_case_column_items (
					id INT PRIMARY KEY,
					"CaseColumn" TEXT
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE PUBLICATION publication_case_column_pub
						FOR TABLE publication_case_column_items (casecolumn);`,
					ExpectedErr: `does not exist`,
				},
			},
		},
	})
}

// TestPublicationSetTableColumnListRespectsQuotedColumnCaseRepro reproduces a
// publication metadata correctness bug: PostgreSQL rejects unquoted lower-case
// column list entries that do not match quoted mixed-case column names.
func TestPublicationSetTableColumnListRespectsQuotedColumnCaseRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER PUBLICATION SET TABLE column list respects quoted column case",
			SetUpScript: []string{
				`CREATE TABLE publication_case_column_set_items (
					id INT PRIMARY KEY,
					"CaseColumn" TEXT
				);`,
				`CREATE PUBLICATION publication_case_column_set_pub;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER PUBLICATION publication_case_column_set_pub
						SET TABLE publication_case_column_set_items (casecolumn);`,
					ExpectedErr: `does not exist`,
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_publication_rel pr
						JOIN pg_catalog.pg_publication p ON p.oid = pr.prpubid
						WHERE p.pubname = 'publication_case_column_set_pub';`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestPublicationColumnListRejectsGeneratedColumnsRepro reproduces a
// publication column-list correctness bug: PostgreSQL rejects generated columns
// in publication column lists because their values are derived on subscribers.
func TestPublicationColumnListRejectsGeneratedColumnsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE PUBLICATION rejects generated columns in column lists",
			SetUpScript: []string{
				`CREATE TABLE publication_generated_column_items (
					id INT PRIMARY KEY,
					label TEXT,
					generated_value INT GENERATED ALWAYS AS (id + 1) STORED
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE PUBLICATION publication_generated_column_pub
						FOR TABLE publication_generated_column_items (id, generated_value);`,
					ExpectedErr: `generated column`,
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_publication
						WHERE pubname = 'publication_generated_column_pub';`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestPublicationRejectsSystemSchemaRepro reproduces a publication metadata
// validation bug: PostgreSQL rejects publishing system schemas.
func TestPublicationRejectsSystemSchemaRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE PUBLICATION rejects pg_catalog schema",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE PUBLICATION publication_pg_catalog_schema_pub
						FOR TABLES IN SCHEMA pg_catalog;`,
					ExpectedErr: `system schemas`,
				},
			},
		},
	})
}

// TestPublicationAddSchemaRejectsSystemSchemaRepro reproduces a publication
// metadata validation bug: PostgreSQL rejects adding system schemas to existing
// publications.
func TestPublicationAddSchemaRejectsSystemSchemaRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER PUBLICATION ADD TABLES IN SCHEMA rejects pg_catalog",
			SetUpScript: []string{
				`CREATE PUBLICATION publication_pg_catalog_schema_add_pub;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER PUBLICATION publication_pg_catalog_schema_add_pub
						ADD TABLES IN SCHEMA pg_catalog;`,
					ExpectedErr: `system schemas`,
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_publication_namespace pn
						JOIN pg_catalog.pg_publication p ON p.oid = pn.pnpubid
						WHERE p.pubname = 'publication_pg_catalog_schema_add_pub';`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestPublicationSetSchemaRejectsSystemSchemaRepro reproduces a publication
// metadata validation bug: PostgreSQL rejects replacing schema membership with
// a system schema.
func TestPublicationSetSchemaRejectsSystemSchemaRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER PUBLICATION SET TABLES IN SCHEMA rejects pg_catalog",
			SetUpScript: []string{
				`CREATE PUBLICATION publication_pg_catalog_schema_set_pub;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER PUBLICATION publication_pg_catalog_schema_set_pub
						SET TABLES IN SCHEMA pg_catalog;`,
					ExpectedErr: `system schemas`,
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_publication_namespace pn
						JOIN pg_catalog.pg_publication p ON p.oid = pn.pnpubid
						WHERE p.pubname = 'publication_pg_catalog_schema_set_pub';`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestPublicationRejectsSystemTableRepro reproduces a publication metadata
// validation bug: PostgreSQL rejects publishing system tables.
func TestPublicationRejectsSystemTableRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE PUBLICATION rejects pg_catalog table",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE PUBLICATION publication_pg_catalog_table_pub
						FOR TABLE pg_catalog.pg_class;`,
					ExpectedErr: `system tables`,
				},
			},
		},
	})
}

// TestPublicationAddTableRejectsSystemTableRepro reproduces a publication
// metadata validation bug: PostgreSQL rejects adding system tables to existing
// publications.
func TestPublicationAddTableRejectsSystemTableRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER PUBLICATION ADD TABLE rejects pg_catalog table",
			SetUpScript: []string{
				`CREATE PUBLICATION publication_pg_catalog_table_add_pub;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER PUBLICATION publication_pg_catalog_table_add_pub
						ADD TABLE pg_catalog.pg_class;`,
					ExpectedErr: `system tables`,
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_publication_rel pr
						JOIN pg_catalog.pg_publication p ON p.oid = pr.prpubid
						WHERE p.pubname = 'publication_pg_catalog_table_add_pub';`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestPublicationSetTableRejectsSystemTableRepro reproduces a publication
// metadata validation bug: PostgreSQL rejects replacing publication membership
// with a system table.
func TestPublicationSetTableRejectsSystemTableRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER PUBLICATION SET TABLE rejects pg_catalog table",
			SetUpScript: []string{
				`CREATE PUBLICATION publication_pg_catalog_table_set_pub;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER PUBLICATION publication_pg_catalog_table_set_pub
						SET TABLE pg_catalog.pg_class;`,
					ExpectedErr: `system tables`,
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_publication_rel pr
						JOIN pg_catalog.pg_publication p ON p.oid = pr.prpubid
						WHERE p.pubname = 'publication_pg_catalog_table_set_pub';`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestPublicationAllTablesRejectsAddTableRepro reproduces a publication
// metadata consistency bug: PostgreSQL rejects adding explicit table membership
// to FOR ALL TABLES publications.
func TestPublicationAllTablesRejectsAddTableRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER PUBLICATION ADD TABLE rejects FOR ALL TABLES publications",
			SetUpScript: []string{
				`CREATE TABLE publication_all_add_items (
					id INT PRIMARY KEY
				);`,
				`CREATE PUBLICATION publication_all_add_pub FOR ALL TABLES;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER PUBLICATION publication_all_add_pub
						ADD TABLE publication_all_add_items;`,
					ExpectedErr: `FOR ALL TABLES`,
				},
				{
					Query: `SELECT puballtables
						FROM pg_catalog.pg_publication
						WHERE pubname = 'publication_all_add_pub';`,
					Expected: []sql.Row{{"t"}},
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_publication_rel pr
						JOIN pg_catalog.pg_publication p ON p.oid = pr.prpubid
						WHERE p.pubname = 'publication_all_add_pub';`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestPublicationAllTablesRejectsSetTableRepro reproduces a publication
// metadata consistency bug: PostgreSQL rejects replacing FOR ALL TABLES
// membership with an explicit table list.
func TestPublicationAllTablesRejectsSetTableRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER PUBLICATION SET TABLE rejects FOR ALL TABLES publications",
			SetUpScript: []string{
				`CREATE TABLE publication_all_set_items (
					id INT PRIMARY KEY
				);`,
				`CREATE PUBLICATION publication_all_set_pub FOR ALL TABLES;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER PUBLICATION publication_all_set_pub
						SET TABLE publication_all_set_items;`,
					ExpectedErr: `FOR ALL TABLES`,
				},
				{
					Query: `SELECT puballtables
						FROM pg_catalog.pg_publication
						WHERE pubname = 'publication_all_set_pub';`,
					Expected: []sql.Row{{"t"}},
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_publication_rel pr
						JOIN pg_catalog.pg_publication p ON p.oid = pr.prpubid
						WHERE p.pubname = 'publication_all_set_pub';`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestPublicationAllTablesRejectsAddSchemaRepro reproduces a publication
// metadata consistency bug: PostgreSQL rejects adding explicit schema
// membership to FOR ALL TABLES publications.
func TestPublicationAllTablesRejectsAddSchemaRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER PUBLICATION ADD SCHEMA rejects FOR ALL TABLES publications",
			SetUpScript: []string{
				`CREATE SCHEMA publication_all_add_schema;`,
				`CREATE PUBLICATION publication_all_add_schema_pub FOR ALL TABLES;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER PUBLICATION publication_all_add_schema_pub
						ADD TABLES IN SCHEMA publication_all_add_schema;`,
					ExpectedErr: `FOR ALL TABLES`,
				},
				{
					Query: `SELECT puballtables
						FROM pg_catalog.pg_publication
						WHERE pubname = 'publication_all_add_schema_pub';`,
					Expected: []sql.Row{{"t"}},
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_publication_namespace pn
						JOIN pg_catalog.pg_publication p ON p.oid = pn.pnpubid
						WHERE p.pubname = 'publication_all_add_schema_pub';`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestPublicationAllTablesRejectsSetSchemaRepro reproduces a publication
// metadata consistency bug: PostgreSQL rejects replacing FOR ALL TABLES
// membership with an explicit schema list.
func TestPublicationAllTablesRejectsSetSchemaRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER PUBLICATION SET SCHEMA rejects FOR ALL TABLES publications",
			SetUpScript: []string{
				`CREATE SCHEMA publication_all_set_schema;`,
				`CREATE PUBLICATION publication_all_set_schema_pub FOR ALL TABLES;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER PUBLICATION publication_all_set_schema_pub
						SET TABLES IN SCHEMA publication_all_set_schema;`,
					ExpectedErr: `FOR ALL TABLES`,
				},
				{
					Query: `SELECT puballtables
						FROM pg_catalog.pg_publication
						WHERE pubname = 'publication_all_set_schema_pub';`,
					Expected: []sql.Row{{"t"}},
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_publication_namespace pn
						JOIN pg_catalog.pg_publication p ON p.oid = pn.pnpubid
						WHERE p.pubname = 'publication_all_set_schema_pub';`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestPublicationDropTableRejectsWhereClauseRepro reproduces a publication
// metadata consistency bug: PostgreSQL rejects WHERE clauses on DROP TABLE so
// invalid syntax cannot remove a publication's table membership.
func TestPublicationDropTableRejectsWhereClauseRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER PUBLICATION DROP TABLE rejects WHERE clauses",
			SetUpScript: []string{
				`CREATE TABLE publication_drop_where_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`CREATE PUBLICATION publication_drop_where_pub
					FOR TABLE publication_drop_where_items;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER PUBLICATION publication_drop_where_pub
						DROP TABLE publication_drop_where_items
						WHERE (id = 1);`,
					ExpectedErr: `WHERE clause`,
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_publication_rel pr
						JOIN pg_catalog.pg_publication p ON p.oid = pr.prpubid
						WHERE p.pubname = 'publication_drop_where_pub';`,
					Expected: []sql.Row{{1}},
				},
			},
		},
	})
}

// TestPublicationColumnListRequiresReplicaIdentityForUpdatesRepro reproduces a
// logical-replication consistency bug: publications that include UPDATE or
// DELETE must not project away the replica-identity columns needed by
// downstream consumers.
func TestPublicationColumnListRequiresReplicaIdentityForUpdatesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "publication column list must include replica identity for updates",
			SetUpScript: []string{
				`CREATE TABLE publication_identity_columns (
					id INT PRIMARY KEY,
					label TEXT,
					internal_note TEXT
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE PUBLICATION publication_insert_only_columns
						FOR TABLE publication_identity_columns (label)
						WITH (publish = 'insert');`,
				},
				{
					Query: `CREATE PUBLICATION publication_update_columns
						FOR TABLE publication_identity_columns (label)
						WITH (publish = 'update');`,
					ExpectedErr: `replica identity`,
				},
			},
		},
	})
}

// TestPublicationColumnListRequiresReplicaIdentityIndexForUpdatesRepro
// reproduces a logical-replication consistency bug: when REPLICA IDENTITY
// USING INDEX is configured, publication column lists for UPDATE or DELETE must
// include that index's columns.
func TestPublicationColumnListRequiresReplicaIdentityIndexForUpdatesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "publication column list must include replica identity index for updates",
			SetUpScript: []string{
				`CREATE TABLE publication_identity_index_columns (
					id INT PRIMARY KEY,
					external_id TEXT NOT NULL,
					label TEXT
				);`,
				`CREATE UNIQUE INDEX publication_identity_index_columns_external_idx
					ON publication_identity_index_columns (external_id);`,
				`ALTER TABLE publication_identity_index_columns
					REPLICA IDENTITY USING INDEX publication_identity_index_columns_external_idx;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE PUBLICATION publication_identity_index_insert_columns
						FOR TABLE publication_identity_index_columns (label)
						WITH (publish = 'insert');`,
				},
				{
					Query: `CREATE PUBLICATION publication_identity_index_update_columns
						FOR TABLE publication_identity_index_columns (label)
						WITH (publish = 'update');`,
					ExpectedErr: `replica identity`,
				},
			},
		},
	})
}

// TestPublicationMembershipSurvivesTableRenameRepro reproduces a publication
// catalog consistency bug: explicit publication membership is tied to the
// relation, so renaming the table should update catalog-visible membership.
func TestPublicationMembershipSurvivesTableRenameRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "publication membership survives table rename",
			SetUpScript: []string{
				`CREATE TABLE publication_rename_old (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`CREATE PUBLICATION publication_rename_pub
					FOR TABLE publication_rename_old;`,
				`ALTER TABLE publication_rename_old
					RENAME TO publication_rename_new;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT pubname, schemaname, tablename, array_to_string(attnames, ',')
						FROM pg_catalog.pg_publication_tables
						WHERE pubname = 'publication_rename_pub';`,
					Expected: []sql.Row{{"publication_rename_pub", "public", "publication_rename_new", "id,label"}},
				},
			},
		},
	})
}

// TestPublicationMembershipClearedWhenTableDroppedRepro reproduces a
// publication catalog consistency bug: dropping a published table should remove
// its explicit publication membership, so a later same-name table is not
// automatically published.
func TestPublicationMembershipClearedWhenTableDroppedRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "publication membership is cleared when table is dropped",
			SetUpScript: []string{
				`CREATE TABLE publication_drop_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`CREATE PUBLICATION publication_drop_pub
					FOR TABLE publication_drop_items;`,
				`DROP TABLE publication_drop_items;`,
				`CREATE TABLE publication_drop_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_publication_tables
						WHERE pubname = 'publication_drop_pub';`,
					Expected: []sql.Row{{0}},
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_publication_rel pr
						JOIN pg_catalog.pg_publication p ON p.oid = pr.prpubid
						WHERE p.pubname = 'publication_drop_pub';`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestPublicationSchemaMembershipClearedWhenSchemaDroppedRepro reproduces a
// publication catalog consistency bug: dropping a schema should remove explicit
// TABLES IN SCHEMA publication membership, so a later same-name schema is not
// automatically published.
func TestPublicationSchemaMembershipClearedWhenSchemaDroppedRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "publication schema membership is cleared when schema is dropped",
			SetUpScript: []string{
				`CREATE SCHEMA publication_empty_schema_drop;`,
				`CREATE PUBLICATION publication_empty_schema_drop_pub
					FOR TABLES IN SCHEMA publication_empty_schema_drop;`,
				`DROP SCHEMA publication_empty_schema_drop;`,
				`CREATE SCHEMA publication_empty_schema_drop;`,
				`CREATE TABLE publication_empty_schema_drop.items (
					id INT PRIMARY KEY
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_publication_namespace pn
						JOIN pg_catalog.pg_publication p ON p.oid = pn.pnpubid
						WHERE p.pubname = 'publication_empty_schema_drop_pub';`,
					Expected: []sql.Row{{0}},
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_publication_tables
						WHERE pubname = 'publication_empty_schema_drop_pub';`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestPublicationColumnListSurvivesColumnRenameRepro reproduces a publication
// catalog consistency bug: PostgreSQL stores publication column lists by
// attribute number, so renaming a published column should expose the new name.
func TestPublicationColumnListSurvivesColumnRenameRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "publication column list survives column rename",
			SetUpScript: []string{
				`CREATE TABLE publication_column_rename_items (
					id INT PRIMARY KEY,
					old_label TEXT,
					note TEXT
				);`,
				`CREATE PUBLICATION publication_column_rename_pub
					FOR TABLE publication_column_rename_items (id, old_label);`,
				`ALTER TABLE publication_column_rename_items
					RENAME COLUMN old_label TO new_label;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT pubname, schemaname, tablename, array_to_string(attnames, ',')
						FROM pg_catalog.pg_publication_tables
						WHERE pubname = 'publication_column_rename_pub';`,
					Expected: []sql.Row{{"publication_column_rename_pub", "public", "publication_column_rename_items", "id,new_label"}},
				},
				{
					Query: `SELECT p.pubname, c.relname, array_to_string(pr.prattrs, ',')
						FROM pg_catalog.pg_publication_rel pr
						JOIN pg_catalog.pg_publication p ON p.oid = pr.prpubid
						JOIN pg_catalog.pg_class c ON c.oid = pr.prrelid
						WHERE p.pubname = 'publication_column_rename_pub';`,
					Expected: []sql.Row{{"publication_column_rename_pub", "publication_column_rename_items", "1,2"}},
				},
			},
		},
	})
}

// TestDropColumnUsedByPublicationColumnListRequiresCascadeRepro reproduces a
// dependency correctness bug: PostgreSQL rejects dropping a column that an
// explicit publication column list depends on unless CASCADE is requested.
func TestDropColumnUsedByPublicationColumnListRequiresCascadeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP COLUMN rejects publication column-list dependencies",
			SetUpScript: []string{
				`CREATE TABLE publication_drop_column_items (
					id INT PRIMARY KEY,
					label TEXT,
					note TEXT
				);`,
				`CREATE PUBLICATION publication_drop_column_pub
					FOR TABLE publication_drop_column_items (id, label);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE publication_drop_column_items
						DROP COLUMN label;`,
					ExpectedErr: `publication`,
				},
				{
					Query: `SELECT count(*)
						FROM information_schema.columns
						WHERE table_schema = 'public'
						  AND table_name = 'publication_drop_column_items'
						  AND column_name = 'label';`,
					Expected: []sql.Row{{1}},
				},
				{
					Query: `SELECT pubname, schemaname, tablename, array_to_string(attnames, ',')
						FROM pg_catalog.pg_publication_tables
						WHERE pubname = 'publication_drop_column_pub';`,
					Expected: []sql.Row{{"publication_drop_column_pub", "public", "publication_drop_column_items", "id,label"}},
				},
			},
		},
	})
}

// TestDropColumnUsedByPublicationRowFilterRequiresCascadeRepro reproduces a
// dependency correctness bug: PostgreSQL rejects dropping a column referenced
// by a publication row filter unless CASCADE is requested.
func TestDropColumnUsedByPublicationRowFilterRequiresCascadeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP COLUMN rejects publication row-filter dependencies",
			SetUpScript: []string{
				`CREATE TABLE publication_filter_drop_items (
					id INT PRIMARY KEY,
					visible BOOL,
					note TEXT
				);`,
				`CREATE PUBLICATION publication_filter_drop_pub
					FOR TABLE publication_filter_drop_items
					WHERE (visible);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE publication_filter_drop_items
						DROP COLUMN visible;`,
					ExpectedErr: `publication`,
				},
				{
					Query: `SELECT count(*)
						FROM information_schema.columns
						WHERE table_schema = 'public'
						  AND table_name = 'publication_filter_drop_items'
						  AND column_name = 'visible';`,
					Expected: []sql.Row{{1}},
				},
			},
		},
	})
}

// TestPublicationRowFilterSurvivesColumnRenameRepro reproduces a publication
// catalog consistency bug: publication row filters are parsed expressions tied
// to table attributes, so renaming a referenced column should expose the new
// column name in pg_publication_tables.
func TestPublicationRowFilterSurvivesColumnRenameRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "publication row filter survives column rename",
			SetUpScript: []string{
				`CREATE TABLE publication_filter_rename_items (
					id INT PRIMARY KEY,
					visible BOOL,
					note TEXT
				);`,
				`CREATE PUBLICATION publication_filter_rename_pub
					FOR TABLE publication_filter_rename_items
					WHERE (visible);`,
				`ALTER TABLE publication_filter_rename_items
					RENAME COLUMN visible TO is_visible;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT pubname, schemaname, tablename,
							replace(replace(rowfilter, '(', ''), ')', '')
						FROM pg_catalog.pg_publication_tables
						WHERE pubname = 'publication_filter_rename_pub';`,
					Expected: []sql.Row{{"publication_filter_rename_pub", "public", "publication_filter_rename_items", "is_visible"}},
				},
			},
		},
	})
}

// TestPublicationRowFilterRejectsUnknownColumnRepro reproduces a publication
// correctness bug: PostgreSQL validates row-filter expressions when the
// publication is created, including rejecting missing table columns.
func TestPublicationRowFilterRejectsUnknownColumnRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "publication row filter rejects unknown columns",
			SetUpScript: []string{
				`CREATE TABLE publication_filter_missing_col_items (
					id INT PRIMARY KEY,
					visible BOOL
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE PUBLICATION publication_filter_missing_col_pub
						FOR TABLE publication_filter_missing_col_items
						WHERE (missing_col);`,
					ExpectedErr: `does not exist`,
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_publication
						WHERE pubname = 'publication_filter_missing_col_pub';`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestPublicationRowFilterRespectsQuotedColumnCaseRepro reproduces a
// publication correctness bug: PostgreSQL treats quoted mixed-case identifiers
// as distinct from their unquoted folded spelling in publication row filters.
func TestPublicationRowFilterRespectsQuotedColumnCaseRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "publication row filter respects quoted column case",
			SetUpScript: []string{
				`CREATE TABLE publication_filter_case_column_items (
					id INT PRIMARY KEY,
					"CaseColumn" TEXT
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE PUBLICATION publication_filter_case_column_pub
						FOR TABLE publication_filter_case_column_items
						WHERE (casecolumn = 'visible');`,
					ExpectedErr: `does not exist`,
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_publication
						WHERE pubname = 'publication_filter_case_column_pub';`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestPublicationAddTableRowFilterRejectsUnknownColumnRepro reproduces a
// publication correctness bug: ALTER PUBLICATION ADD TABLE must validate row
// filters against the target table before adding publication membership.
func TestPublicationAddTableRowFilterRejectsUnknownColumnRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER PUBLICATION ADD TABLE row filter rejects unknown columns",
			SetUpScript: []string{
				`CREATE TABLE publication_filter_add_missing_items (
					id INT PRIMARY KEY,
					visible BOOL
				);`,
				`CREATE PUBLICATION publication_filter_add_missing_pub;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER PUBLICATION publication_filter_add_missing_pub
						ADD TABLE publication_filter_add_missing_items
						WHERE (missing_col);`,
					ExpectedErr: `does not exist`,
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_publication_rel pr
						JOIN pg_catalog.pg_publication p ON p.oid = pr.prpubid
						WHERE p.pubname = 'publication_filter_add_missing_pub';`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestPublicationSetTableRowFilterRejectsUnknownColumnRepro reproduces a
// publication metadata consistency bug: ALTER PUBLICATION SET TABLE must reject
// invalid row filters before replacing publication membership.
func TestPublicationSetTableRowFilterRejectsUnknownColumnRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER PUBLICATION SET TABLE row filter rejects unknown columns",
			SetUpScript: []string{
				`CREATE TABLE publication_filter_set_missing_items (
					id INT PRIMARY KEY,
					visible BOOL
				);`,
				`CREATE PUBLICATION publication_filter_set_missing_pub;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER PUBLICATION publication_filter_set_missing_pub
						SET TABLE publication_filter_set_missing_items
						WHERE (missing_col);`,
					ExpectedErr: `does not exist`,
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_publication_rel pr
						JOIN pg_catalog.pg_publication p ON p.oid = pr.prpubid
						WHERE p.pubname = 'publication_filter_set_missing_pub';`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestPublicationRowFilterRejectsVolatileFunctionRepro reproduces a
// publication correctness bug: PostgreSQL rejects mutable functions in
// publication row filters because they would make replication routing
// nondeterministic.
func TestPublicationRowFilterRejectsVolatileFunctionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "publication row filter rejects volatile functions",
			SetUpScript: []string{
				`CREATE TABLE publication_filter_volatile_items (
					id INT PRIMARY KEY,
					visible BOOL
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE PUBLICATION publication_filter_volatile_pub
						FOR TABLE publication_filter_volatile_items
						WHERE (random() > 0.5);`,
					ExpectedErr: `mutable functions`,
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_publication
						WHERE pubname = 'publication_filter_volatile_pub';`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestPublicationRowFilterRejectsSubqueryRepro reproduces a publication
// correctness bug: PostgreSQL rejects subqueries in publication row filters.
func TestPublicationRowFilterRejectsSubqueryRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "publication row filter rejects subqueries",
			SetUpScript: []string{
				`CREATE TABLE publication_filter_subquery_items (
					id INT PRIMARY KEY,
					visible BOOL
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE PUBLICATION publication_filter_subquery_pub
						FOR TABLE publication_filter_subquery_items
						WHERE (id IN (SELECT id FROM publication_filter_subquery_items));`,
					ExpectedErr: `invalid publication WHERE expression`,
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_publication
						WHERE pubname = 'publication_filter_subquery_pub';`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestPublicationRowFilterRejectsAggregateRepro reproduces a publication
// correctness bug: PostgreSQL rejects aggregate functions in publication row
// filters.
func TestPublicationRowFilterRejectsAggregateRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "publication row filter rejects aggregate functions",
			SetUpScript: []string{
				`CREATE TABLE publication_filter_aggregate_items (
					id INT PRIMARY KEY,
					visible BOOL
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE PUBLICATION publication_filter_aggregate_pub
						FOR TABLE publication_filter_aggregate_items
						WHERE (count(*) > 0);`,
					ExpectedErr: `aggregate functions are not allowed`,
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_publication
						WHERE pubname = 'publication_filter_aggregate_pub';`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestPublicationRowFilterRejectsWindowFunctionRepro reproduces a publication
// correctness bug: PostgreSQL rejects window functions in publication row
// filters.
func TestPublicationRowFilterRejectsWindowFunctionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "publication row filter rejects window functions",
			SetUpScript: []string{
				`CREATE TABLE publication_filter_window_items (
					id INT PRIMARY KEY,
					visible BOOL
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE PUBLICATION publication_filter_window_pub
						FOR TABLE publication_filter_window_items
						WHERE (row_number() OVER () = 1);`,
					ExpectedErr: `window functions are not allowed`,
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_publication
						WHERE pubname = 'publication_filter_window_pub';`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestPublicationRowFilterRejectsNonBooleanExpressionRepro reproduces a
// publication correctness bug: PostgreSQL requires publication row filters to
// be boolean expressions.
func TestPublicationRowFilterRejectsNonBooleanExpressionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "publication row filter rejects non-boolean expressions",
			SetUpScript: []string{
				`CREATE TABLE publication_filter_non_boolean_items (
					id INT PRIMARY KEY,
					visible BOOL
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE PUBLICATION publication_filter_non_boolean_pub
						FOR TABLE publication_filter_non_boolean_items
						WHERE (1234);`,
					ExpectedErr: `must be type boolean`,
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_publication
						WHERE pubname = 'publication_filter_non_boolean_pub';`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestPublicationRowFilterRejectsSystemColumnRepro reproduces a publication
// correctness bug: PostgreSQL rejects system columns such as ctid in
// publication row filters.
func TestPublicationRowFilterRejectsSystemColumnRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "publication row filter rejects system columns",
			SetUpScript: []string{
				`CREATE TABLE publication_filter_system_column_items (
					id INT PRIMARY KEY,
					visible BOOL
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE PUBLICATION publication_filter_system_column_pub
						FOR TABLE publication_filter_system_column_items
						WHERE ('(0,1)'::tid = ctid);`,
					ExpectedErr: `invalid publication WHERE expression`,
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_publication
						WHERE pubname = 'publication_filter_system_column_pub';`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestReplicaIdentityRejectsDeferrableUniqueIndexRepro reproduces a logical
// replication correctness bug: PostgreSQL requires a replica identity index to
// be immediate, so deferrable unique indexes cannot identify row changes.
func TestReplicaIdentityRejectsDeferrableUniqueIndexRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "replica identity rejects deferrable unique indexes",
			SetUpScript: []string{
				`CREATE TABLE replica_identity_deferrable_items (
					id INT PRIMARY KEY,
					code INT NOT NULL UNIQUE DEFERRABLE INITIALLY IMMEDIATE
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE replica_identity_deferrable_items
						REPLICA IDENTITY USING INDEX replica_identity_deferrable_items_code_key;`,
					ExpectedErr: `non-immediate index`,
				},
				{
					Query: `SELECT relreplident
						FROM pg_catalog.pg_class
						WHERE relname = 'replica_identity_deferrable_items';`,
					Expected: []sql.Row{{"d"}},
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_index i
						JOIN pg_catalog.pg_class c ON c.oid = i.indrelid
						WHERE c.relname = 'replica_identity_deferrable_items'
						  AND i.indisreplident;`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestReplicaIdentityIndexColumnDropNotNullRejectedRepro reproduces a logical
// replication consistency bug: PostgreSQL prevents removing NOT NULL from a
// column that participates in the configured replica identity index.
func TestReplicaIdentityIndexColumnDropNotNullRejectedRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "replica identity index columns must remain not null",
			SetUpScript: []string{
				`CREATE TABLE replica_identity_not_null_items (
					id INT PRIMARY KEY,
					code INT NOT NULL
				);`,
				`CREATE UNIQUE INDEX replica_identity_not_null_items_code_idx
					ON replica_identity_not_null_items (code);`,
				`ALTER TABLE replica_identity_not_null_items
					REPLICA IDENTITY USING INDEX replica_identity_not_null_items_code_idx;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE replica_identity_not_null_items
						ALTER COLUMN code DROP NOT NULL;`,
					ExpectedErr: `replica identity`,
				},
				{
					Query: `SELECT a.attnotnull, c.relreplident, i.indisreplident
						FROM pg_catalog.pg_attribute a
						JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
						JOIN pg_catalog.pg_index i ON i.indrelid = c.oid
						JOIN pg_catalog.pg_class ic ON ic.oid = i.indexrelid
						WHERE c.relname = 'replica_identity_not_null_items'
						  AND a.attname = 'code'
						  AND ic.relname = 'replica_identity_not_null_items_code_idx';`,
					Expected: []sql.Row{{"t", "i", "t"}},
				},
			},
		},
	})
}

// TestReplicaIdentityUsingIndexSurvivesTableRenameRepro reproduces a logical
// replication catalog consistency bug: renaming a table should preserve its
// configured REPLICA IDENTITY USING INDEX metadata.
func TestReplicaIdentityUsingIndexSurvivesTableRenameRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "replica identity using index survives table rename",
			SetUpScript: []string{
				`CREATE TABLE replica_identity_rename_old (
					id INT PRIMARY KEY,
					code INT NOT NULL
				);`,
				`CREATE UNIQUE INDEX replica_identity_rename_old_code_idx
					ON replica_identity_rename_old (code);`,
				`ALTER TABLE replica_identity_rename_old
					REPLICA IDENTITY USING INDEX replica_identity_rename_old_code_idx;`,
				`ALTER TABLE replica_identity_rename_old
					RENAME TO replica_identity_rename_new;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT c.relreplident, count(*) FILTER (WHERE i.indisreplident)
						FROM pg_catalog.pg_class c
						JOIN pg_catalog.pg_index i ON i.indrelid = c.oid
						WHERE c.relname = 'replica_identity_rename_new'
						GROUP BY c.relreplident;`,
					Expected: []sql.Row{{"i", 1}},
				},
			},
		},
	})
}

// TestReplicaIdentityClearedWhenTableDroppedRepro reproduces a logical
// replication catalog consistency bug: dropping a table should discard its
// replica identity metadata so a later table with the same name starts with
// DEFAULT identity.
func TestReplicaIdentityClearedWhenTableDroppedRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "replica identity is cleared when table is dropped",
			SetUpScript: []string{
				`CREATE TABLE replica_identity_recreate_items (
					id INT PRIMARY KEY
				);`,
				`ALTER TABLE replica_identity_recreate_items
					REPLICA IDENTITY FULL;`,
				`DROP TABLE replica_identity_recreate_items;`,
				`CREATE TABLE replica_identity_recreate_items (
					id INT PRIMARY KEY
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT relreplident
						FROM pg_catalog.pg_class
						WHERE relname = 'replica_identity_recreate_items';`,
					Expected: []sql.Row{{"d"}},
				},
			},
		},
	})
}

// TestReplicaIdentityUsingIndexSurvivesIndexRenameRepro reproduces a logical
// replication catalog consistency bug: renaming the selected replica identity
// index should keep pg_index.indisreplident on the renamed index.
func TestReplicaIdentityUsingIndexSurvivesIndexRenameRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "replica identity using index survives index rename",
			SetUpScript: []string{
				`CREATE TABLE replica_identity_index_rename_items (
					id INT PRIMARY KEY,
					code INT NOT NULL
				);`,
				`CREATE UNIQUE INDEX replica_identity_index_rename_old_idx
					ON replica_identity_index_rename_items (code);`,
				`ALTER TABLE replica_identity_index_rename_items
					REPLICA IDENTITY USING INDEX replica_identity_index_rename_old_idx;`,
				`ALTER INDEX replica_identity_index_rename_old_idx
					RENAME TO replica_identity_index_rename_new_idx;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT c.relreplident, ic.relname, i.indisreplident
						FROM pg_catalog.pg_class c
						JOIN pg_catalog.pg_index i ON i.indrelid = c.oid
						JOIN pg_catalog.pg_class ic ON ic.oid = i.indexrelid
						WHERE c.relname = 'replica_identity_index_rename_items'
						  AND ic.relname = 'replica_identity_index_rename_new_idx';`,
					Expected: []sql.Row{{"i", "replica_identity_index_rename_new_idx", "t"}},
				},
			},
		},
	})
}

// TestPublicationDeleteRequiresReplicaIdentityRepro reproduces a
// logical-replication consistency bug: PostgreSQL rejects DELETE on a table
// that publishes deletes but has REPLICA IDENTITY NOTHING.
func TestPublicationDeleteRequiresReplicaIdentityRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DELETE requires replica identity when publication publishes deletes",
			SetUpScript: []string{
				`CREATE TABLE publication_delete_identity (
					id INT PRIMARY KEY,
					label TEXT,
					private_note TEXT
				);`,
				`INSERT INTO publication_delete_identity VALUES
					(1, 'delete-me', 'subscriber-cannot-identify-this-row');`,
				`ALTER TABLE publication_delete_identity REPLICA IDENTITY NOTHING;`,
				`CREATE PUBLICATION publication_delete_identity_pub
					FOR TABLE publication_delete_identity
					WITH (publish = 'delete');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DELETE FROM publication_delete_identity WHERE id = 1;`,
					ExpectedErr: `replica identity`,
				},
				{
					Query:    `SELECT count(*) FROM publication_delete_identity;`,
					Expected: []sql.Row{{1}},
				},
			},
		},
	})
}

// TestPublicationUpdateRequiresReplicaIdentityRepro reproduces a
// logical-replication consistency bug: PostgreSQL rejects UPDATE on a table
// that publishes updates but has REPLICA IDENTITY NOTHING.
func TestPublicationUpdateRequiresReplicaIdentityRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "UPDATE requires replica identity when publication publishes updates",
			SetUpScript: []string{
				`CREATE TABLE publication_update_identity (
					id INT PRIMARY KEY,
					label TEXT,
					private_note TEXT
				);`,
				`INSERT INTO publication_update_identity VALUES
					(1, 'before-update', 'subscriber-cannot-identify-this-row');`,
				`ALTER TABLE publication_update_identity REPLICA IDENTITY NOTHING;`,
				`CREATE PUBLICATION publication_update_identity_pub
					FOR TABLE publication_update_identity
					WITH (publish = 'update');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `UPDATE publication_update_identity SET label = 'after-update' WHERE id = 1;`,
					ExpectedErr: `replica identity`,
				},
				{
					Query:    `SELECT label FROM publication_update_identity WHERE id = 1;`,
					Expected: []sql.Row{{"before-update"}},
				},
			},
		},
	})
}
