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
					Query: "CREATE PUBLICATION dg_pub FOR TABLE pub_items (tenant_id, label) WHERE (tenant_id > 0) WITH (publish = 'insert, update', publish_via_partition_root = true);",
				},
				{
					Query: "SELECT pubname, puballtables, pubinsert, pubupdate, pubdelete, pubtruncate, pubviaroot FROM pg_catalog.pg_publication WHERE pubname = 'dg_pub';",
					Expected: []sql.Row{
						{"dg_pub", "f", "t", "t", "f", "f", "t"},
					},
				},
				{
					Query: `SELECT
						  pg_get_userbyid(p.pubowner) = current_role AS can_alter_publication,
						  pubinsert AND pubupdate AND pubdelete AND pubtruncate AS publishes_all_operations,
						  CASE WHEN current_setting('server_version_num')::int >= 180000
						      THEN (to_jsonb(p) ->> 'pubgencols') = 's'
						      ELSE FALSE
						  END AS publishes_generated_columns
						FROM pg_publication AS p WHERE pubname = 'dg_pub';`,
					Expected: []sql.Row{
						{"t", "f", "f"},
					},
				},
				{
					Query: "SELECT pubname, schemaname, tablename, array_to_string(attnames, ','), rowfilter IS NOT NULL FROM pg_catalog.pg_publication_tables WHERE pubname = 'dg_pub';",
					Expected: []sql.Row{
						{"dg_pub", "public", "pub_items", "tenant_id,label", "t"},
					},
				},
				{
					Query: "SELECT p.pubname, c.relname, array_to_string(pr.prattrs, ',') FROM pg_catalog.pg_publication_rel pr JOIN pg_catalog.pg_publication p ON p.oid = pr.prpubid JOIN pg_catalog.pg_class c ON c.oid = pr.prrelid WHERE p.pubname = 'dg_pub';",
					Expected: []sql.Row{
						{"dg_pub", "pub_items", "1,2"},
					},
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
					Query: `SELECT schemaname, tablename FROM pg_catalog.pg_publication_tables WHERE pubname = '_dgzero_metadata_0' ORDER BY schemaname, tablename;`,
					Expected: []sql.Row{
						{"dgzero", "permissions"},
						{"zmeta", "clients"},
						{"zmeta", "mutations"},
					},
				},
				{
					Query: `ALTER PUBLICATION "_dgzero_metadata_0" SET TABLE dgzero.permissions, TABLE zmeta.clients, zmeta.mutations;`,
				},
				{
					Query: `SELECT count(*) FROM pg_catalog.pg_publication_tables WHERE pubname = '_dgzero_metadata_0';`,
					Expected: []sql.Row{
						{3},
					},
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
					Query: "SELECT relreplident FROM pg_catalog.pg_class WHERE relname = 'repl_ident_items';",
					Expected: []sql.Row{
						{"d"},
					},
				},
				{
					Query: "ALTER TABLE repl_ident_items REPLICA IDENTITY FULL;",
				},
				{
					Query: "SELECT relreplident FROM pg_catalog.pg_class WHERE relname = 'repl_ident_items';",
					Expected: []sql.Row{
						{"f"},
					},
				},
				{
					Query: "ALTER TABLE repl_ident_items REPLICA IDENTITY NOTHING;",
				},
				{
					Query: "SELECT relreplident FROM pg_catalog.pg_class WHERE relname = 'repl_ident_items';",
					Expected: []sql.Row{
						{"n"},
					},
				},
				{
					Query: "ALTER TABLE repl_ident_items REPLICA IDENTITY USING INDEX repl_ident_label_idx;",
				},
				{
					Query: "SELECT c.relreplident, i.indisreplident FROM pg_catalog.pg_class c JOIN pg_catalog.pg_index i ON i.indrelid = c.oid JOIN pg_catalog.pg_class ic ON ic.oid = i.indexrelid WHERE c.relname = 'repl_ident_items' AND ic.relname = 'repl_ident_label_idx';",
					Expected: []sql.Row{
						{"i", "t"},
					},
				},
				{
					Query: "ALTER TABLE repl_ident_items REPLICA IDENTITY DEFAULT;",
				},
				{
					Query: "SELECT c.relreplident, i.indisreplident FROM pg_catalog.pg_class c JOIN pg_catalog.pg_index i ON i.indrelid = c.oid JOIN pg_catalog.pg_class ic ON ic.oid = i.indexrelid WHERE c.relname = 'repl_ident_items' AND ic.relname = 'repl_ident_label_idx';",
					Expected: []sql.Row{
						{"d", "f"},
					},
				},
				{
					Query:       "ALTER TABLE repl_ident_items REPLICA IDENTITY USING INDEX repl_ident_missing_idx;",
					ExpectedErr: `index "repl_ident_missing_idx" does not exist`,
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
				"CREATE TABLE electric_alias_items (id INT PRIMARY KEY);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT ARRAY[pn.nspname, pc.relname] parent FROM pg_catalog.pg_class pc JOIN pg_catalog.pg_namespace pn ON pn.oid = pc.relnamespace WHERE pc.relname = 'electric_alias_items';",
					Expected: []sql.Row{
						{"{public,electric_alias_items}"},
					},
					ExpectedColNames: []string{"parent"},
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
					Query:       "CREATE SUBSCRIPTION dg_bad_sub CONNECTION 'host=127.0.0.1 dbname=postgres' PUBLICATION dg_pub;",
					ExpectedErr: "connect=false",
				},
				{
					Query: "CREATE SUBSCRIPTION dg_sub CONNECTION 'host=127.0.0.1 dbname=postgres' PUBLICATION dg_pub WITH (connect = false, enabled = false, slot_name = NONE, create_slot = false, binary = true, streaming = parallel, two_phase = false, disable_on_error = true, synchronous_commit = 'remote_apply');",
				},
				{
					Query: "SELECT subname, subenabled, subbinary, substream, subtwophasestate, subdisableonerr, subslotname IS NULL, subsynccommit, array_to_string(subpublications, ','), subskiplsn::text FROM pg_catalog.pg_subscription WHERE subname = 'dg_sub';",
					Expected: []sql.Row{
						{"dg_sub", "f", "t", "t", "d", "t", "t", "remote_apply", "dg_pub", "0/0"},
					},
				},
				{
					Query: "ALTER SUBSCRIPTION dg_sub ADD PUBLICATION dg_pub2 WITH (copy_data = false);",
				},
				{
					Query: "SELECT array_to_string(subpublications, ',') FROM pg_catalog.pg_subscription WHERE subname = 'dg_sub';",
					Expected: []sql.Row{
						{"dg_pub,dg_pub2"},
					},
				},
				{
					Query: "ALTER SUBSCRIPTION dg_sub SET PUBLICATION dg_pub2;",
				},
				{
					Query: "ALTER SUBSCRIPTION dg_sub ENABLE;",
				},
				{
					Query: "ALTER SUBSCRIPTION dg_sub SKIP (lsn = '0/16');",
				},
				{
					Query: "SELECT subenabled, subskiplsn::text, array_to_string(subpublications, ',') FROM pg_catalog.pg_subscription WHERE subname = 'dg_sub';",
					Expected: []sql.Row{
						{"t", "0/16", "dg_pub2"},
					},
				},
				{
					Query: "ALTER SUBSCRIPTION dg_sub DISABLE;",
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
