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

package postgres16

import (
	. "github.com/dolthub/doltgresql/testing/go"

	"testing"

	"github.com/dolthub/go-mysql-server/sql"
)

// TestPgIndexIndclassAny exercises the planner path for
// `oid = ANY(int2vector_column)`. drizzle-kit's index-introspection
// query joins pg_opclass to pg_index via this exact pattern; without
// it, the migration tool hangs on every Doltgres database.
//
// The simplest reproducer is the same shape: select rows from
// pg_index where indclass contains a given oid. PostgreSQL's
// indclass is an `oidvector` (typed as int2vector / oidvector
// depending on catalog version); ANY(...) on it must return
// boolean. Doltgres previously rejected that with
// "found equality comparison that does not return a bool".
func TestPgIndexIndclassAny(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ANY(indclass) is a boolean predicate",
			SetUpScript: []string{
				`CREATE TABLE idxany_t (id INT PRIMARY KEY, code TEXT);`,
				`CREATE INDEX idxany_code_idx ON idxany_t (code);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					// drizzle-kit's exact join shape (simplified).
					// The query asks: which indexes use opclass OID X?
					// Doltgres needs to handle ANY(indclass) without
					// claiming the comparison "doesn't return a bool".
					Query: `SELECT c.relname
FROM pg_catalog.pg_index i
JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
JOIN pg_catalog.pg_opclass opc ON opc.oid = ANY(i.indclass)
WHERE c.relname = 'idxany_code_idx';`,
					// At least one row — drizzle-kit only needs the
					// query to execute; the count of joined opclass
					// rows depends on how many opclasses are
					// registered, which is implementation-defined.
					SkipResultsCheck: true,
				},
				{
					// Plain ANY(array_literal) for sanity — must
					// already work and serve as a baseline.
					Query: `SELECT 1 WHERE 2 = ANY(ARRAY[1, 2, 3]);`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-index-indclass-any-test-testpgindexindclassany-0001-select-1-where-2-="},
				},
			},
		},
	})
}

func TestPgIndexVectorSlices(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_index indkey preserves vector lower bound through array casts",
			SetUpScript: []string{
				`CREATE TABLE idxslice_t (id INT, label TEXT, category TEXT);`,
				`CREATE INDEX idxslice_label_category_idx ON idxslice_t (label, category);`,
				`CREATE PUBLICATION idxslice_pub FOR TABLE idxslice_t;`,
				`CREATE TABLE idxslice_pk_probe (id INT PRIMARY KEY, label TEXT);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT i.indkey[0]::text, i.indkey[1]::text
FROM pg_catalog.pg_index i
JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
WHERE c.relname = 'idxslice_label_category_idx';`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-index-indclass-any-test-testpgindexvectorslices-0001-select-i.indkey[0]::text-i.indkey[1]::text-from-pg_catalog.pg_index"},
				},
				{
					Query: `SELECT array_to_string((i.indkey::smallint[])[:i.indnkeyatts - 1], ',')
FROM pg_catalog.pg_index i
JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
WHERE c.relname = 'idxslice_label_category_idx';`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-index-indclass-any-test-testpgindexvectorslices-0002-select-array_to_string-i.indkey::smallint[]-[:i.indnkeyatts-1]"},
				},
				{
					Query: `WITH indexed_columns AS (
	  SELECT col.table_pos
  FROM pg_catalog.pg_index i
  JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
  JOIN LATERAL (
    SELECT col.table_pos
    FROM UNNEST((i.indkey::smallint[])[:i.indnkeyatts - 1])
      WITH ORDINALITY AS col(table_pos, index_pos)
  ) AS col ON true
  WHERE c.relname = 'idxslice_label_category_idx'
)
SELECT array_to_string(array_agg(table_pos ORDER BY table_pos), ',') FROM indexed_columns;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-index-indclass-any-test-testpgindexvectorslices-0003-with-indexed_columns-as-select-col.table_pos"},
				},
				{
					Query: `SELECT array_to_string(array_agg(a.attname ORDER BY idx.ord), ',')
FROM (
  SELECT i.indrelid,
         pg_catalog.unnest(i.indkey) AS attnum,
         pg_catalog.generate_subscripts(i.indkey, 1) AS ord
  FROM pg_catalog.pg_index i
  JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
  WHERE c.relname = 'idxslice_label_category_idx'
) AS idx
JOIN pg_catalog.pg_attribute a
  ON a.attrelid = idx.indrelid
	AND a.attnum = idx.attnum;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-index-indclass-any-test-testpgindexvectorslices-0004-select-array_to_string-array_agg-a.attname-order"},
				},
				{
					Query: `WITH indexed_columns AS (
  SELECT
      pg_indexes.schemaname as "schema",
      pg_indexes.tablename as "tableName",
      pg_indexes.indexname as "name",
      index_column.name as "col",
      CASE WHEN pg_index.indoption[index_column.pos-1] & 1 = 1 THEN 'DESC' ELSE 'ASC' END as "dir",
      pg_index.indisunique as "unique",
      pg_index.indisprimary as "isPrimaryKey",
      pg_index.indisreplident as "isReplicaIdentity",
      pg_index.indimmediate as "isImmediate"
    FROM pg_catalog.pg_indexes
    JOIN pg_catalog.pg_namespace ON pg_indexes.schemaname = pg_namespace.nspname
    JOIN pg_catalog.pg_class pc ON
      pc.relname = pg_indexes.indexname
      AND pc.relnamespace = pg_namespace.oid
    JOIN pg_catalog.pg_publication_tables as pb ON
      pb.schemaname = pg_indexes.schemaname AND
      pb.tablename = pg_indexes.tablename
    JOIN pg_catalog.pg_index ON pg_index.indexrelid = pc.oid
    JOIN LATERAL (
      SELECT array_agg(attname) as attnames, array_agg(attgenerated != '') as generated FROM pg_catalog.pg_attribute
        WHERE attrelid = pg_index.indrelid
          AND attnum = ANY( (pg_index.indkey::smallint[] )[:pg_index.indnkeyatts - 1] )
    ) as indexed ON true
    JOIN LATERAL (
      SELECT pg_attribute.attname as name, col.index_pos as pos
        FROM UNNEST( (pg_index.indkey::smallint[])[:pg_index.indnkeyatts - 1] )
          WITH ORDINALITY as col(table_pos, index_pos)
        JOIN pg_catalog.pg_attribute ON attrelid = pg_index.indrelid AND attnum = col.table_pos
    ) AS index_column ON true
    LEFT JOIN pg_catalog.pg_constraint ON pg_constraint.conindid = pc.oid
    WHERE pb.pubname IN ('idxslice_pub')
      AND pg_index.indexprs IS NULL
      AND pg_index.indpred IS NULL
      AND (pg_constraint.contype IS NULL OR pg_constraint.contype IN ('p', 'u'))
      AND indexed.attnames <@ pb.attnames
      AND (current_setting('server_version_num')::int >= 160000 OR false = ALL(indexed.generated))
)
SELECT "schema", "tableName", "name", "col", "dir", "unique", "isPrimaryKey", "isReplicaIdentity", "isImmediate"
FROM indexed_columns
ORDER BY "schema", "tableName", "name", "col";`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-index-indclass-any-test-testpgindexvectorslices-0005-with-indexed_columns-as-select-pg_indexes.schemaname", ColumnModes: []string{"schema"}},
				},
				{
					Query: `SELECT array_to_string(array_agg(a.attname ORDER BY i.idx), ',')
FROM (
  SELECT indrelid, indkey, generate_subscripts(indkey, 1) idx
  FROM pg_catalog.pg_index
  WHERE indrelid = 'idxslice_pk_probe'::regclass
    AND indisprimary
) i
JOIN pg_catalog.pg_attribute a
  ON a.attrelid = i.indrelid
 AND a.attnum = i.indkey[i.idx];`,
					Expected: []sql.Row{{"id"}},
				},
			},
		},
	})
}

func TestZeroPublishedColumnsCatalogCasts(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "Zero published_columns catalog oid casts",
			SetUpScript: []string{
				`CREATE TABLE zero_pub_items (id INT PRIMARY KEY, label TEXT NOT NULL);`,
				`CREATE PUBLICATION _dgzero_public_0 FOR TABLES IN SCHEMA public WITH (publish_via_partition_root = true);`,
				`CREATE SCHEMA dgzero;`,
				`CREATE SCHEMA dgzero_0;`,
				`CREATE TABLE dgzero.permissions (
					permissions JSONB,
					hash TEXT,
					lock BOOL PRIMARY KEY DEFAULT true CHECK (lock)
				);`,
				`CREATE TABLE dgzero_0.clients (
					"clientGroupID" TEXT NOT NULL,
					"clientID" TEXT NOT NULL,
					"lastMutationID" BIGINT NOT NULL,
					"userID" TEXT,
					PRIMARY KEY("clientGroupID", "clientID")
				);`,
				`CREATE TABLE dgzero_0.mutations (
					"clientGroupID" TEXT NOT NULL,
					"clientID" TEXT NOT NULL,
					"mutationID" BIGINT NOT NULL,
					result JSON NOT NULL,
					PRIMARY KEY("clientGroupID", "clientID", "mutationID")
				);`,
				`CREATE PUBLICATION _dgzero_metadata_0 FOR TABLE dgzero.permissions, TABLE dgzero_0.clients, dgzero_0.mutations;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT
  pc.oid::int8 AS "oid",
  pc.relnamespace::int8 AS "schemaOID",
  atttypid::int8 AS "typeOID"
FROM pg_attribute
JOIN pg_class pc ON pc.oid = attrelid
JOIN pg_namespace pns ON pns.oid = relnamespace
JOIN pg_type pt ON atttypid = pt.oid
JOIN pg_publication_tables as pb ON
  pb.schemaname = nspname AND
  pb.tablename = pc.relname AND
  attname = ANY(pb.attnames)
WHERE pb.pubname IN ('_dgzero_public_0', '_dgzero_metadata_0')
ORDER BY attnum;`,
					SkipResultsCheck: true,
				},
				{
					Query: `WITH published_columns AS (SELECT
  pc.oid::int8 AS "oid",
  nspname AS "schema",
  pc.relnamespace::int8 AS "schemaOID",
  pc.relname AS "name",
  pc.relreplident AS "replicaIdentity",
  attnum AS "pos",
  attname AS "col",
  pt.typname AS "type",
  atttypid::int8 AS "typeOID",
  pt.typtype,
  elem_pt.typtype AS "elemTyptype",
  NULLIF(atttypmod, -1) AS "maxLen",
  attndims "arrayDims",
  attnotnull AS "notNull",
  pg_get_expr(pd.adbin, pd.adrelid) as "dflt",
  NULLIF(ARRAY_POSITION(conkey, attnum), -1) AS "keyPos",
  pb.rowfilter as "rowFilter",
  pb.pubname as "publication"
FROM pg_attribute
JOIN pg_class pc ON pc.oid = attrelid
JOIN pg_namespace pns ON pns.oid = relnamespace
JOIN pg_type pt ON atttypid = pt.oid
LEFT JOIN pg_type elem_pt ON elem_pt.oid = pt.typelem
JOIN pg_publication_tables as pb ON
  pb.schemaname = nspname AND
  pb.tablename = pc.relname AND
  attname = ANY(pb.attnames)
LEFT JOIN pg_constraint pk ON pk.contype = 'p' AND pk.connamespace = relnamespace AND pk.conrelid = attrelid
LEFT JOIN pg_attrdef pd ON pd.adrelid = attrelid AND pd.adnum = attnum
WHERE pb.pubname IN ('_dgzero_public_0', '_dgzero_metadata_0') AND
      (current_setting('server_version_num')::int >= 160000 OR attgenerated = '')
ORDER BY nspname, pc.relname)
SELECT json_build_object(
  'oid', "oid",
  'schema', "schema",
  'schemaOID', "schemaOID",
  'name', "name",
  'replicaIdentity', "replicaIdentity",
  'columns', json_object_agg(
    DISTINCT
    col,
    jsonb_build_object(
      'pos', "pos",
      'dataType', CASE WHEN "arrayDims" = 0
                       THEN "type"
                       ELSE substring("type" from 2) || repeat('[]', "arrayDims") END,
      'pgTypeClass', "typtype",
      'elemPgTypeClass', "elemTyptype",
      'typeOID', "typeOID",
      'characterMaximumLength', CASE WHEN "typeOID" = 1043 OR "typeOID" = 1042
                                     THEN "maxLen" - 4
                                     ELSE "maxLen" END,
      'notNull', "notNull",
      'dflt', "dflt"
    )
  ),
  'primaryKey', ARRAY( SELECT json_object_keys(
    json_strip_nulls(
      json_object_agg(
        DISTINCT "col", "keyPos" ORDER BY "keyPos"
      )
    )
  )),
  'publications', json_object_agg(
    DISTINCT
    "publication",
    jsonb_build_object('rowFilter', "rowFilter")
  )
) AS "table" FROM published_columns
  GROUP BY "schema", "schemaOID", "name", "oid", "replicaIdentity";`,
					SkipResultsCheck: true,
				},
				{
					Query: `WITH published_columns AS (SELECT
  pc.oid::int8 AS "oid",
  nspname AS "schema",
  pc.relnamespace::int8 AS "schemaOID" ,
  pc.relname AS "name",
  pc.relreplident AS "replicaIdentity",
  attnum AS "pos",
  attname AS "col",
  pt.typname AS "type",
  atttypid::int8 AS "typeOID",
  pt.typtype,
  elem_pt.typtype AS "elemTyptype",
  NULLIF(atttypmod, -1) AS "maxLen",
  attndims "arrayDims",
  attnotnull AS "notNull",
  pg_get_expr(pd.adbin, pd.adrelid) as "dflt",
  NULLIF(ARRAY_POSITION(conkey, attnum), -1) AS "keyPos",
  pb.rowfilter as "rowFilter",
  pb.pubname as "publication"
FROM pg_attribute
JOIN pg_class pc ON pc.oid = attrelid
JOIN pg_namespace pns ON pns.oid = relnamespace
JOIN pg_type pt ON atttypid = pt.oid
LEFT JOIN pg_type elem_pt ON elem_pt.oid = pt.typelem
JOIN pg_publication_tables as pb ON
  pb.schemaname = nspname AND
  pb.tablename = pc.relname AND
  attname = ANY(pb.attnames)
LEFT JOIN pg_constraint pk ON pk.contype = 'p' AND pk.connamespace = relnamespace AND pk.conrelid = attrelid
LEFT JOIN pg_attrdef pd ON pd.adrelid = attrelid AND pd.adnum = attnum
WHERE pb.pubname IN ('_dgzero_public_0', '_dgzero_metadata_0') AND
      (current_setting('server_version_num')::int >= 160000 OR attgenerated = '')
ORDER BY nspname, pc.relname),

tables AS (SELECT json_build_object(
  'oid', "oid",
  'schema', "schema",
  'schemaOID', "schemaOID",
  'name', "name",
  'replicaIdentity', "replicaIdentity",
  'columns', json_object_agg(
    DISTINCT
    col,
    jsonb_build_object(
      'pos', "pos",
      'dataType', CASE WHEN "arrayDims" = 0
                       THEN "type"
                       ELSE substring("type" from 2) || repeat('[]', "arrayDims") END,
      'pgTypeClass', "typtype",
      'elemPgTypeClass', "elemTyptype",
      'typeOID', "typeOID",
      'characterMaximumLength', CASE WHEN "typeOID" = 1043 OR "typeOID" = 1042
                                     THEN "maxLen" - 4
                                     ELSE "maxLen" END,
      'notNull', "notNull",
      'dflt', "dflt"
    )
  ),
  'primaryKey', ARRAY( SELECT json_object_keys(
    json_strip_nulls(
      json_object_agg(
        DISTINCT "col", "keyPos" ORDER BY "keyPos"
      )
    )
  )),
  'publications', json_object_agg(
    DISTINCT
    "publication",
    jsonb_build_object('rowFilter', "rowFilter")
  )
) AS "table" FROM published_columns
  GROUP BY "schema", "schemaOID", "name", "oid", "replicaIdentity"),

indexed_columns AS (SELECT
    pg_indexes.schemaname as "schema",
    pg_indexes.tablename as "tableName",
    pg_indexes.indexname as "name",
    index_column.name as "col",
    CASE WHEN pg_index.indoption[index_column.pos-1] & 1 = 1 THEN 'DESC' ELSE 'ASC' END as "dir",
    pg_index.indisunique as "unique",
    pg_index.indisprimary as "isPrimaryKey",
    pg_index.indisreplident as "isReplicaIdentity",
    pg_index.indimmediate as "isImmediate"
  FROM pg_indexes
  JOIN pg_namespace ON pg_indexes.schemaname = pg_namespace.nspname
  JOIN pg_class pc ON
    pc.relname = pg_indexes.indexname
    AND pc.relnamespace = pg_namespace.oid
  JOIN pg_publication_tables as pb ON
    pb.schemaname = pg_indexes.schemaname AND
    pb.tablename = pg_indexes.tablename
  JOIN pg_index ON pg_index.indexrelid = pc.oid
  JOIN LATERAL (
    SELECT array_agg(attname) as attnames, array_agg(attgenerated != '') as generated FROM pg_attribute
      WHERE attrelid = pg_index.indrelid
        AND attnum = ANY( (pg_index.indkey::smallint[] )[:pg_index.indnkeyatts - 1] )
  ) as indexed ON true
  JOIN LATERAL (
    SELECT pg_attribute.attname as name, col.index_pos as pos
      FROM UNNEST( (pg_index.indkey::smallint[])[:pg_index.indnkeyatts - 1] )
        WITH ORDINALITY as col(table_pos, index_pos)
      JOIN pg_attribute ON attrelid = pg_index.indrelid AND attnum = col.table_pos
  ) AS index_column ON true
  LEFT JOIN pg_constraint ON pg_constraint.conindid = pc.oid
  WHERE pb.pubname IN ('_dgzero_public_0', '_dgzero_metadata_0')
    AND pg_index.indexprs IS NULL
    AND pg_index.indpred IS NULL
    AND (pg_constraint.contype IS NULL OR pg_constraint.contype IN ('p', 'u'))
    AND indexed.attnames <@ pb.attnames
    AND (current_setting('server_version_num')::int >= 160000 OR false = ALL(indexed.generated))
  ORDER BY
    pg_indexes.schemaname,
    pg_indexes.tablename,
    pg_indexes.indexname,
    index_column.pos ASC),

indexes AS (SELECT json_build_object(
    'schema', "schema",
    'tableName', "tableName",
    'name', "name",
    'unique', "unique",
    'isPrimaryKey', "isPrimaryKey",
    'isReplicaIdentity', "isReplicaIdentity",
    'isImmediate', "isImmediate",
    'columns', json_object_agg("col", "dir")
  ) AS index FROM indexed_columns
    GROUP BY "schema", "tableName", "name", "unique",
       "isPrimaryKey", "isReplicaIdentity", "isImmediate")

SELECT json_build_object(
  'tables', COALESCE((SELECT json_agg("table") FROM tables), '[]'::json),
  'indexes', COALESCE((SELECT json_agg("index") FROM indexes), '[]'::json)
) as "publishedSchema";`,
					SkipResultsCheck: true,
				},
			},
		},
	})
}
