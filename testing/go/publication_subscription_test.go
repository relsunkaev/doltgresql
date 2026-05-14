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

func TestPublicationDDLAndCatalogs(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "publication ddl and catalogs",
			SetUpScript: []string{
				"CREATE TABLE pub_items (tenant_id BIGINT PRIMARY KEY, label TEXT, flag BOOLEAN);",
				"CREATE TABLE pub_more (tenant_id BIGINT PRIMARY KEY, label TEXT);",
				"CREATE SCHEMA aux;",
				"CREATE TABLE aux.schema_items (tenant_id BIGINT PRIMARY KEY, label TEXT);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       "CREATE PUBLICATION dg_bad_db_qual_pub FOR TABLE postgres.public.pub_items;",
					ExpectedErr: "publication table database qualifiers are not yet supported",
				},
				{
					Query: "CREATE PUBLICATION dg_pub FOR TABLE pub_items (tenant_id, label) WHERE (tenant_id > 0) WITH (publish = 'insert, update', publish_via_partition_root = true);",
				},
				{
					Query: "SELECT pubname, puballtables, pubinsert, pubupdate, pubdelete, pubtruncate, pubviaroot FROM pg_catalog.pg_publication WHERE pubname = 'dg_pub';", PostgresOracle: ScriptTestPostgresOracle{ID: "publication-subscription-test-testpublicationddlandcatalogs-0002-select-pubname-puballtables-pubinsert-pubupdate"},
				},
				{
					Query: `SELECT
						  pg_get_userbyid(p.pubowner) = current_role AS can_alter_publication,
						  pubinsert AND pubupdate AND pubdelete AND pubtruncate AS publishes_all_operations,
						  CASE WHEN current_setting('server_version_num')::int >= 180000
						      THEN (to_jsonb(p) ->> 'pubgencols') = 's'
						      ELSE FALSE
						  END AS publishes_generated_columns
						FROM pg_publication AS p WHERE pubname = 'dg_pub';`, PostgresOracle: ScriptTestPostgresOracle{ID: "publication-subscription-test-testpublicationddlandcatalogs-0003-select-pg_get_userbyid-p.pubowner-=-current_role"},
				},
				{
					Query: "SELECT pubname, schemaname, tablename, array_to_string(attnames, ','), rowfilter IS NOT NULL FROM pg_catalog.pg_publication_tables WHERE pubname = 'dg_pub';", PostgresOracle: ScriptTestPostgresOracle{ID: "publication-subscription-test-testpublicationddlandcatalogs-0004-select-pubname-schemaname-tablename-array_to_string", ColumnModes: []string{"structural", "schema"}},
				},
				{
					Query: "SELECT p.pubname, c.relname, array_to_string(pr.prattrs, ',') FROM pg_catalog.pg_publication_rel pr JOIN pg_catalog.pg_publication p ON p.oid = pr.prpubid JOIN pg_catalog.pg_class c ON c.oid = pr.prrelid WHERE p.pubname = 'dg_pub';", PostgresOracle: ScriptTestPostgresOracle{ID: "publication-subscription-test-testpublicationddlandcatalogs-0005-select-p.pubname-c.relname-array_to_string-pr.prattrs"},
				},
				{
					Query: "ALTER PUBLICATION dg_pub ADD TABLE pub_more;",
				},
				{
					Query: "ALTER PUBLICATION dg_pub ADD TABLES IN SCHEMA aux;",
				},
				{
					Query: "SELECT schemaname, tablename FROM pg_catalog.pg_publication_tables WHERE pubname = 'dg_pub' ORDER BY schemaname, tablename;",
					Expected: []sql.Row{
						{"aux", "schema_items"},
						{"public", "pub_items"},
						{"public", "pub_more"},
					},
				},
				{
					Query: "SELECT p.pubname, n.nspname FROM pg_catalog.pg_publication_namespace pn JOIN pg_catalog.pg_publication p ON p.oid = pn.pnpubid JOIN pg_catalog.pg_namespace n ON n.oid = pn.pnnspid WHERE p.pubname = 'dg_pub';",
					Expected: []sql.Row{
						{"dg_pub", "aux"},
					},
				},
				{
					Query: "ALTER PUBLICATION dg_pub SET (publish = 'delete, truncate', publish_via_partition_root = false);",
				},
				{
					Query: "SELECT pubinsert, pubupdate, pubdelete, pubtruncate, pubviaroot FROM pg_catalog.pg_publication WHERE pubname = 'dg_pub';",
					Expected: []sql.Row{
						{"f", "f", "t", "t", "f"},
					},
				},
				{
					Query: "ALTER PUBLICATION dg_pub DROP TABLE pub_more;",
				},
				{
					Query: "ALTER PUBLICATION dg_pub DROP TABLES IN SCHEMA aux;",
				},
				{
					Query: "SELECT schemaname, tablename FROM pg_catalog.pg_publication_tables WHERE pubname = 'dg_pub';",
					Expected: []sql.Row{
						{"public", "pub_items"},
					},
				},
				{
					Query: "ALTER PUBLICATION dg_pub RENAME TO dg_pub_renamed;",
				},
				{
					Query: "SELECT pubname FROM pg_catalog.pg_publication WHERE pubname = 'dg_pub_renamed';",
					Expected: []sql.Row{
						{"dg_pub_renamed"},
					},
				},
				{
					Query: "DROP PUBLICATION dg_pub_renamed;",
				},
				{
					Query: "SELECT count(*) FROM pg_catalog.pg_publication WHERE pubname = 'dg_pub_renamed';",
					Expected: []sql.Row{
						{0},
					},
				},
			},
		},
		{
			Name: "publication table lists accept repeated and omitted table keywords",
			SetUpScript: []string{
				`CREATE SCHEMA dgzero;`,
				`CREATE SCHEMA zmeta;`,
				`CREATE TABLE dgzero.permissions (id BIGINT PRIMARY KEY);`,
				`CREATE TABLE zmeta.clients (id BIGINT PRIMARY KEY);`,
				`CREATE TABLE zmeta.mutations (id BIGINT PRIMARY KEY);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE PUBLICATION "_dgzero_metadata_0" FOR TABLE dgzero.permissions, TABLE zmeta.clients, zmeta.mutations;`,
				},
				{
					Query: `SELECT schemaname, tablename FROM pg_catalog.pg_publication_tables WHERE pubname = '_dgzero_metadata_0' ORDER BY schemaname, tablename;`, PostgresOracle: ScriptTestPostgresOracle{ID: "publication-subscription-test-testpublicationddlandcatalogs-0012-select-schemaname-tablename-from-pg_catalog.pg_publication_tables"},
				},
				{
					Query: `ALTER PUBLICATION "_dgzero_metadata_0" SET TABLE dgzero.permissions, TABLE zmeta.clients, zmeta.mutations;`,
				},
				{
					Query: `SELECT count(*) FROM pg_catalog.pg_publication_tables WHERE pubname = '_dgzero_metadata_0';`, PostgresOracle: ScriptTestPostgresOracle{ID: "publication-subscription-test-testpublicationddlandcatalogs-0013-select-count-*-from-pg_catalog.pg_publication_tables"},
				},
			},
		},
	})
}

func TestReplicaIdentityDDLAndCatalogs(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "replica identity ddl and catalogs",
			SetUpScript: []string{
				"CREATE TABLE repl_ident_items (id INT PRIMARY KEY, label TEXT NOT NULL);",
				"CREATE UNIQUE INDEX repl_ident_label_idx ON repl_ident_items (label);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT relreplident FROM pg_catalog.pg_class WHERE relname = 'repl_ident_items';", PostgresOracle: ScriptTestPostgresOracle{ID: "publication-subscription-test-testreplicaidentityddlandcatalogs-0001-select-relreplident-from-pg_catalog.pg_class-where"},
				},
				{
					Query: "ALTER TABLE repl_ident_items REPLICA IDENTITY FULL;",
				},
				{
					Query: "SELECT relreplident FROM pg_catalog.pg_class WHERE relname = 'repl_ident_items';", PostgresOracle: ScriptTestPostgresOracle{ID: "publication-subscription-test-testreplicaidentityddlandcatalogs-0002-select-relreplident-from-pg_catalog.pg_class-where"},
				},
				{
					Query: "ALTER TABLE repl_ident_items REPLICA IDENTITY NOTHING;",
				},
				{
					Query: "SELECT relreplident FROM pg_catalog.pg_class WHERE relname = 'repl_ident_items';", PostgresOracle: ScriptTestPostgresOracle{ID: "publication-subscription-test-testreplicaidentityddlandcatalogs-0003-select-relreplident-from-pg_catalog.pg_class-where"},
				},
				{
					Query: "ALTER TABLE repl_ident_items REPLICA IDENTITY USING INDEX repl_ident_label_idx;",
				},
				{
					Query: "SELECT c.relreplident, i.indisreplident FROM pg_catalog.pg_class c JOIN pg_catalog.pg_index i ON i.indrelid = c.oid JOIN pg_catalog.pg_class ic ON ic.oid = i.indexrelid WHERE c.relname = 'repl_ident_items' AND ic.relname = 'repl_ident_label_idx';", PostgresOracle: ScriptTestPostgresOracle{ID: "publication-subscription-test-testreplicaidentityddlandcatalogs-0004-select-c.relreplident-i.indisreplident-from-pg_catalog.pg_class"},
				},
				{
					Query: "ALTER TABLE repl_ident_items REPLICA IDENTITY DEFAULT;",
				},
				{
					Query: "SELECT c.relreplident, i.indisreplident FROM pg_catalog.pg_class c JOIN pg_catalog.pg_index i ON i.indrelid = c.oid JOIN pg_catalog.pg_class ic ON ic.oid = i.indexrelid WHERE c.relname = 'repl_ident_items' AND ic.relname = 'repl_ident_label_idx';", PostgresOracle: ScriptTestPostgresOracle{ID: "publication-subscription-test-testreplicaidentityddlandcatalogs-0005-select-c.relreplident-i.indisreplident-from-pg_catalog.pg_class"},
				},
				{
					Query: "ALTER TABLE repl_ident_items REPLICA IDENTITY USING INDEX repl_ident_missing_idx;", PostgresOracle: ScriptTestPostgresOracle{ID: "publication-subscription-test-testreplicaidentityddlandcatalogs-0006-alter-table-repl_ident_items-replica-identity", Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestElectricInspectorArrayAlias(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "electric inspector array alias",
			SetUpScript: []string{
				"CREATE SCHEMA electric_alias;",
				"CREATE TABLE electric_alias.electric_alias_items (id INT PRIMARY KEY);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT ARRAY[pn.nspname, pc.relname] parent FROM pg_catalog.pg_class pc JOIN pg_catalog.pg_namespace pn ON pn.oid = pc.relnamespace WHERE pc.relname = 'electric_alias_items' AND pn.nspname = 'electric_alias';", PostgresOracle: ScriptTestPostgresOracle{ID: "publication-subscription-test-testelectricinspectorarrayalias-0001-select-array[pn.nspname-pc.relname]-parent-from"},
				},
			},
		},
	})
}

func TestSubscriptionDDLAndCatalogs(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "subscription ddl and catalogs",
			SetUpScript: []string{
				"CREATE TABLE sub_items (tenant_id BIGINT PRIMARY KEY, label TEXT);",
				"CREATE PUBLICATION dg_pub FOR TABLE sub_items;",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "CREATE SUBSCRIPTION dg_bad_sub CONNECTION 'host=127.0.0.1 dbname=postgres' PUBLICATION dg_pub;", PostgresOracle: ScriptTestPostgresOracle{ID: "publication-subscription-test-testsubscriptionddlandcatalogs-0001-create-subscription-dg_bad_sub-connection-host=127.0.0.1", Compare: "sqlstate"},
				},
				{
					Query: "CREATE SUBSCRIPTION dg_bad_slot_sub CONNECTION 'host=127.0.0.1 dbname=postgres' PUBLICATION dg_pub WITH (connect = false, enabled = false, create_slot = true);", PostgresOracle: ScriptTestPostgresOracle{ID: "publication-subscription-test-testsubscriptionddlandcatalogs-0002-create-subscription-dg_bad_slot_sub-connection-host=127.0.0.1", Compare: "sqlstate"},
				},
				{
					Query: "CREATE SUBSCRIPTION dg_bad_enabled_sub CONNECTION 'host=127.0.0.1 dbname=postgres' PUBLICATION dg_pub WITH (connect = false, enabled = true, create_slot = false);", PostgresOracle: ScriptTestPostgresOracle{ID: "publication-subscription-test-testsubscriptionddlandcatalogs-0003-create-subscription-dg_bad_enabled_sub-connection-host=127.0.0.1", Compare: "sqlstate"},
				},
				{
					Query: "CREATE SUBSCRIPTION dg_bad_copy_sub CONNECTION 'host=127.0.0.1 dbname=postgres' PUBLICATION dg_pub WITH (connect = false, enabled = false, create_slot = false, copy_data = true);", PostgresOracle: ScriptTestPostgresOracle{ID: "publication-subscription-test-testsubscriptionddlandcatalogs-0004-create-subscription-dg_bad_copy_sub-connection-host=127.0.0.1", Compare: "sqlstate"},
				},
				{
					Query: "CREATE SUBSCRIPTION dg_sub CONNECTION 'host=127.0.0.1 dbname=postgres' PUBLICATION dg_pub WITH (connect = false, enabled = false, slot_name = NONE, create_slot = false, binary = true, streaming = true, two_phase = false, disable_on_error = true, synchronous_commit = 'remote_apply');",
				},
				{
					Query: "SELECT subname, subenabled, subbinary, substream, subtwophasestate, subdisableonerr, subslotname IS NULL, subsynccommit, array_to_string(subpublications, ','), subskiplsn::text FROM pg_catalog.pg_subscription WHERE subname = 'dg_sub';", PostgresOracle: ScriptTestPostgresOracle{ID: "publication-subscription-test-testsubscriptionddlandcatalogs-0005-select-subname-subenabled-subbinary-substream"},
				},
				{
					Query: "SELECT count(*) FROM pg_catalog.pg_subscription_rel;", PostgresOracle: ScriptTestPostgresOracle{ID: "publication-subscription-test-testsubscriptionddlandcatalogs-0006-select-count-*-from-pg_catalog.pg_subscription_rel"},
				},
				{
					Query: "ALTER SUBSCRIPTION dg_sub ADD PUBLICATION dg_pub2 WITH (copy_data = false, refresh = false);",
				},
				{
					Query: "SELECT array_to_string(subpublications, ',') FROM pg_catalog.pg_subscription WHERE subname = 'dg_sub';", PostgresOracle: ScriptTestPostgresOracle{ID: "publication-subscription-test-testsubscriptionddlandcatalogs-0007-select-array_to_string-subpublications-from-pg_catalog.pg_subscription"},
				},
				{
					Query: "ALTER SUBSCRIPTION dg_sub SET PUBLICATION dg_pub2 WITH (refresh = false);",
				},
				{
					Query: "ALTER SUBSCRIPTION dg_sub ENABLE;", PostgresOracle: ScriptTestPostgresOracle{ID: "publication-subscription-test-testsubscriptionddlandcatalogs-0008-alter-subscription-dg_sub-enable", Compare: "sqlstate"},
				},
				{
					Query: "ALTER SUBSCRIPTION dg_sub SKIP (lsn = '0/16');",
				},
				{
					Query: "SELECT subenabled, subskiplsn::text, array_to_string(subpublications, ',') FROM pg_catalog.pg_subscription WHERE subname = 'dg_sub';", PostgresOracle: ScriptTestPostgresOracle{ID: "publication-subscription-test-testsubscriptionddlandcatalogs-0009-select-subenabled-subskiplsn::text-array_to_string-subpublications"},
				},
				{
					Query: "ALTER SUBSCRIPTION dg_sub REFRESH PUBLICATION WITH (copy_data = false);", PostgresOracle: ScriptTestPostgresOracle{ID: "publication-subscription-test-testsubscriptionddlandcatalogs-0010-alter-subscription-dg_sub-refresh-publication", Compare: "sqlstate"},
				},
				{
					Query: "ALTER SUBSCRIPTION dg_sub DISABLE;",
				},
				{
					Query: "CREATE SUBSCRIPTION dg_enabled_metadata_sub CONNECTION 'host=127.0.0.1 dbname=postgres' PUBLICATION dg_pub WITH (connect = false, enabled = false, create_slot = false);",
				},
				{
					Query: "ALTER SUBSCRIPTION dg_enabled_metadata_sub ENABLE;",
				},
				{
					Query: "SELECT subenabled, subslotname FROM pg_catalog.pg_subscription WHERE subname = 'dg_enabled_metadata_sub';", PostgresOracle: ScriptTestPostgresOracle{ID: "publication-subscription-test-testsubscriptionddlandcatalogs-0011-select-subenabled-subslotname-from-pg_catalog.pg_subscription"},
				},
				{
					Query: "SELECT count(*) FROM pg_catalog.pg_subscription_rel;",
					Expected: []sql.Row{
						{0},
					},
				},
				{
					Query: "SELECT count(*) FROM pg_catalog.pg_stat_subscription;",
					Expected: []sql.Row{
						{0},
					},
				},
				{
					Query:       "ALTER SUBSCRIPTION dg_enabled_metadata_sub REFRESH PUBLICATION WITH (copy_data = false);",
					ExpectedErr: "subscription refresh requires publisher connections, which are not yet supported",
				},
				{
					Query: "ALTER SUBSCRIPTION dg_sub OWNER TO CURRENT_USER;",
				},
				{
					Query: "ALTER SUBSCRIPTION dg_sub RENAME TO dg_sub_renamed;",
				},
				{
					Query: "SELECT subname, apply_error_count, sync_error_count, stats_reset IS NULL FROM pg_catalog.pg_stat_subscription_stats WHERE subname = 'dg_sub_renamed';",
					Expected: []sql.Row{
						{"dg_sub_renamed", 0, 0, "t"},
					},
				},
				{
					Query: "DROP SUBSCRIPTION dg_sub_renamed;",
				},
				{
					Query: "SELECT count(*) FROM pg_catalog.pg_subscription WHERE subname = 'dg_sub_renamed';",
					Expected: []sql.Row{
						{0},
					},
				},
			},
		},
	})
}
