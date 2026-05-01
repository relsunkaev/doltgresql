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
	"context"
	"fmt"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestPgDogCompatibilityBoundary(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "PgDog supported primary shard boundary",
			SetUpScript: []string{
				"CREATE TABLE pgdog_items (tenant_id BIGINT PRIMARY KEY, label TEXT);",
				"INSERT INTO pgdog_items VALUES (1, 'one'), (2, 'two');",
				"CREATE TABLE pgdog_vectors (tenant_id vector PRIMARY KEY, label TEXT);",
				"INSERT INTO pgdog_vectors VALUES ('[1,0]'::vector, 'vector-one'), ('[2,0]'::vector, 'vector-two');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    "SELECT label FROM pgdog_items WHERE tenant_id = 1;",
					Expected: []sql.Row{{"one"}},
				},
				{
					Query:    "SELECT count(*) FROM pg_replication_slots;",
					Expected: []sql.Row{{0}},
				},
				{
					Query:    "SELECT count(*) FROM pg_stat_replication;",
					Expected: []sql.Row{{0}},
				},
				{
					Query:    "SELECT count(*) FROM pg_publication;",
					Expected: []sql.Row{{0}},
				},
				{
					Query:    "SELECT pg_is_in_recovery();",
					Expected: []sql.Row{{"f"}},
				},
				{
					Query: "SELECT label FROM pgdog_vectors WHERE tenant_id = '[1,0]'::vector;",
					Expected: []sql.Row{
						{"vector-one"},
					},
				},
			},
		},
		{
			Name: "PgDog prepared transaction lifecycle",
			SetUpScript: []string{
				"CREATE TABLE pgdog_items (tenant_id BIGINT PRIMARY KEY, label TEXT);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "BEGIN;",
				},
				{
					Query: "INSERT INTO pgdog_items VALUES (10, 'ten');",
				},
				{
					Query: "PREPARE TRANSACTION 'dg_pgdog';",
				},
				{
					Query: "SELECT gid FROM pg_catalog.pg_prepared_xacts WHERE gid = 'dg_pgdog';",
					Expected: []sql.Row{
						{"dg_pgdog"},
					},
				},
				{
					Query: "COMMIT PREPARED 'dg_pgdog';",
				},
				{
					Query: "SELECT label FROM pgdog_items WHERE tenant_id = 10;",
					Expected: []sql.Row{
						{"ten"},
					},
				},
			},
		},
		{
			Name: "PgDog logical replication metadata probes",
			SetUpScript: []string{
				"CREATE TABLE pgdog_items (tenant_id BIGINT PRIMARY KEY, label TEXT);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "CREATE PUBLICATION dg_pgdog_pub FOR TABLE pgdog_items;",
				},
				{
					Query: "SELECT pubname FROM pg_catalog.pg_publication WHERE pubname = 'dg_pgdog_pub';",
					Expected: []sql.Row{
						{"dg_pgdog_pub"},
					},
				},
				{
					Query: "CREATE SUBSCRIPTION dg_pgdog_sub CONNECTION 'host=127.0.0.1 dbname=postgres' PUBLICATION dg_pgdog_pub WITH (connect=false, enabled=false, create_slot=false, slot_name=NONE);",
				},
				{
					Query: "SELECT subname, subenabled, subslotname IS NULL, array_to_string(subpublications, ',') FROM pg_catalog.pg_subscription WHERE subname = 'dg_pgdog_sub';",
					Expected: []sql.Row{
						{"dg_pgdog_sub", "f", "t", "dg_pgdog_pub"},
					},
				},
				{
					Query:       "CREATE SUBSCRIPTION dg_pgdog_bad_sub CONNECTION 'host=127.0.0.1 dbname=postgres' PUBLICATION dg_pgdog_pub;",
					ExpectedErr: "connect=false",
				},
				{
					Query: "PREPARE dg_pgdog_stmt(int) AS SELECT $1::int + 1;",
				},
				{
					Query: "EXECUTE dg_pgdog_stmt(41);",
					Expected: []sql.Row{
						{42},
					},
				},
				{
					Query: "SELECT name, from_sql FROM pg_catalog.pg_prepared_statements WHERE name = 'dg_pgdog_stmt';",
					Expected: []sql.Row{
						{"dg_pgdog_stmt", "t"},
					},
				},
				{
					Query: "SELECT pg_current_wal_lsn();",
					Expected: []sql.Row{
						{"0/0"},
					},
				},
				{
					Query: "SELECT pg_wal_lsn_diff('0/1'::pg_lsn, '0/0'::pg_lsn);",
					Expected: []sql.Row{
						{Numeric("1")},
					},
				},
			},
		},
		{
			Name: "PgDog schema loader columns query",
			SetUpScript: []string{
				"CREATE TABLE pgdog_schema_items (tenant_id BIGINT PRIMARY KEY, label TEXT NOT NULL);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `
SELECT
    c.table_catalog::text,
    c.table_schema::text,
    c.table_name::text,
    c.column_name::text,
    COALESCE(c.column_default, CASE WHEN c.is_identity = 'YES' THEN 'generated ' || lower(c.identity_generation) || ' as identity' ELSE NULL END)::text AS column_default,
    (c.is_nullable != 'NO')::text AS is_nullable,
    c.data_type::text,
    c.ordinal_position::int,
    (pk.column_name IS NOT NULL)::text AS is_primary_key
FROM
    information_schema.columns c
LEFT JOIN (
    SELECT
        kcu.table_schema,
        kcu.table_name,
        kcu.column_name
    FROM
        information_schema.table_constraints tc
    JOIN
        information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema = kcu.table_schema
    WHERE
        tc.constraint_type = 'PRIMARY KEY'
) pk ON c.table_schema = pk.table_schema
    AND c.table_name = pk.table_name
    AND c.column_name = pk.column_name
WHERE
    c.table_schema NOT IN ('pg_catalog', 'information_schema')

UNION ALL

SELECT
    current_database()::text AS table_catalog,
    n.nspname::text AS table_schema,
    cls.relname::text AS table_name,
    a.attname::text AS column_name,
    pg_get_expr(d.adbin, d.adrelid)::text AS column_default,
    (NOT a.attnotnull)::text AS is_nullable,
    pg_catalog.format_type(a.atttypid, a.atttypmod)::text AS data_type,
    a.attnum::int AS ordinal_position,
    'false'::text AS is_primary_key
FROM
    pg_catalog.pg_class cls
JOIN
    pg_catalog.pg_namespace n ON n.oid = cls.relnamespace
JOIN
    pg_catalog.pg_attribute a ON a.attrelid = cls.oid
LEFT JOIN
    pg_catalog.pg_attrdef d ON d.adrelid = cls.oid AND d.adnum = a.attnum
WHERE
    cls.relkind = 'm'
    AND a.attnum > 0
    AND NOT a.attisdropped
    AND n.nspname NOT IN ('pg_catalog', 'information_schema')

ORDER BY
    table_schema, table_name, ordinal_position;`,
					SkipResultsCheck: true,
				},
			},
		},
		{
			Name: "PgDog schema loader foreign keys query",
			SetUpScript: []string{
				"CREATE TABLE pgdog_fk_parent (id INT PRIMARY KEY);",
				"CREATE TABLE pgdog_fk_child (id INT PRIMARY KEY, parent_id INT REFERENCES pgdog_fk_parent(id));",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `
SELECT DISTINCT
      n.nspname::text AS source_schema,
      c.relname::text AS source_table,
      a.attname::text AS source_column,
      rn.nspname::text AS ref_schema,
      rc.relname::text AS ref_table,
      ra.attname::text AS ref_column,
      CASE con.confdeltype
          WHEN 'a' THEN 'NO ACTION'
          WHEN 'r' THEN 'RESTRICT'
          WHEN 'c' THEN 'CASCADE'
          WHEN 'n' THEN 'SET NULL'
          WHEN 'd' THEN 'SET DEFAULT'
      END AS on_delete,
      CASE con.confupdtype
          WHEN 'a' THEN 'NO ACTION'
          WHEN 'r' THEN 'RESTRICT'
          WHEN 'c' THEN 'CASCADE'
          WHEN 'n' THEN 'SET NULL'
          WHEN 'd' THEN 'SET DEFAULT'
      END AS on_update
  FROM pg_constraint con
  JOIN pg_class c ON c.oid = con.conrelid
  JOIN pg_namespace n ON n.oid = c.relnamespace
  JOIN pg_class rc ON rc.oid = con.confrelid
  JOIN pg_namespace rn ON rn.oid = rc.relnamespace
  JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS src(attnum, ord) ON true
  JOIN LATERAL unnest(con.confkey) WITH ORDINALITY AS dst(attnum, ord) ON src.ord = dst.ord
  JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = src.attnum
  JOIN pg_attribute ra ON ra.attrelid = con.confrelid AND ra.attnum = dst.attnum
  WHERE con.contype = 'f'
    AND n.nspname NOT IN ('pg_catalog', 'information_schema')
  ORDER BY source_schema, source_table, source_column;`,
					SkipResultsCheck: true,
				},
			},
		},
	})
}

func TestPgDogStartupRuntimeParameters(t *testing.T) {
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		err := controller.WaitForStop()
		require.NoError(t, err)
	}()
	defer conn.Close(ctx)

	config, err := pgx.ParseConfig(fmt.Sprintf("postgres://postgres:password@127.0.0.1:%d/postgres?sslmode=disable", port))
	require.NoError(t, err)
	config.RuntimeParams["timezone"] = "UTC"

	pgxConn, err := pgx.ConnectConfig(ctx, config)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, pgxConn.Close(context.Background()))
	}()

	var timezone string
	require.NoError(t, pgxConn.QueryRow(ctx, "SHOW TimeZone;").Scan(&timezone))
	require.Equal(t, "UTC", timezone)
}
