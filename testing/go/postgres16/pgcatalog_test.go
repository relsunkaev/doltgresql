package postgres16

import (
	. "github.com/dolthub/doltgresql/testing/go"

	"testing"

	"github.com/dolthub/go-mysql-server/sql"
)

func TestPgAggregate(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_aggregate",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT count(*) > 0 FROM "pg_catalog"."pg_aggregate";`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgaggregate-0001-select-count-*->-0"},
				},
				{ // Different cases and quoted, so it fails
					Query: `SELECT * FROM "PG_catalog"."pg_aggregate";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgaggregate-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_aggregate";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgaggregate-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT count(*) > 0 FROM PG_catalog.pg_AGGREGATE;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgaggregate-0004-select-count-*->-0"},
				},
			},
		},
	})
}

func TestPgAm(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_am",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_am";`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgam-0001-select-*-from-pg_catalog-."},
				},
				{ // Different cases and quoted, so it fails
					Query: `SELECT * FROM "PG_catalog"."pg_am";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgam-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_am";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgam-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT amname FROM PG_catalog.pg_AM ORDER BY amname;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgam-0004-select-amname-from-pg_catalog.pg_am-order"},
				},
			},
		},
	})
}

func TestPgAmop(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_amop",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT opf.opfname, COUNT(*)
FROM "pg_catalog"."pg_amop" amop
JOIN "pg_catalog"."pg_opfamily" opf ON opf.oid = amop.amopfamily
JOIN "pg_catalog"."pg_am" am ON am.oid = amop.amopmethod
WHERE am.amname = 'btree'
GROUP BY opf.opfname
ORDER BY opf.opfname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgamop-0001-select-opf.opfname-count-*-from"},
				},
				{
					Query: `SELECT lt.typname, rt.typname, COUNT(*)
FROM "pg_catalog"."pg_amop" amop
JOIN "pg_catalog"."pg_opfamily" opf ON opf.oid = amop.amopfamily
JOIN "pg_catalog"."pg_type" lt ON lt.oid = amop.amoplefttype
JOIN "pg_catalog"."pg_type" rt ON rt.oid = amop.amoprighttype
WHERE opf.opfname = 'datetime_ops'
GROUP BY lt.typname, rt.typname
ORDER BY lt.typname, rt.typname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgamop-0002-select-lt.typname-rt.typname-count-*"},
				},
				{
					Query: `SELECT lt.typname, rt.typname, COUNT(*)
FROM "pg_catalog"."pg_amop" amop
JOIN "pg_catalog"."pg_opfamily" opf ON opf.oid = amop.amopfamily
JOIN "pg_catalog"."pg_type" lt ON lt.oid = amop.amoplefttype
JOIN "pg_catalog"."pg_type" rt ON rt.oid = amop.amoprighttype
WHERE opf.opfname = 'integer_ops'
GROUP BY lt.typname, rt.typname
ORDER BY lt.typname, rt.typname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgamop-0003-select-lt.typname-rt.typname-count-*"},
				},
				{
					Query: `SELECT lt.typname, rt.typname, COUNT(*)
FROM "pg_catalog"."pg_amop" amop
JOIN "pg_catalog"."pg_opfamily" opf ON opf.oid = amop.amopfamily
JOIN "pg_catalog"."pg_type" lt ON lt.oid = amop.amoplefttype
JOIN "pg_catalog"."pg_type" rt ON rt.oid = amop.amoprighttype
WHERE opf.opfname = 'float_ops'
GROUP BY lt.typname, rt.typname
ORDER BY lt.typname, rt.typname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgamop-0004-select-lt.typname-rt.typname-count-*"},
				},
				{
					Query: `SELECT lt.typname, rt.typname, COUNT(*)
FROM "pg_catalog"."pg_amop" amop
JOIN "pg_catalog"."pg_opfamily" opf ON opf.oid = amop.amopfamily
JOIN "pg_catalog"."pg_type" lt ON lt.oid = amop.amoplefttype
JOIN "pg_catalog"."pg_type" rt ON rt.oid = amop.amoprighttype
WHERE opf.opfname = 'text_ops'
GROUP BY lt.typname, rt.typname
ORDER BY lt.typname, rt.typname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgamop-0005-select-lt.typname-rt.typname-count-*"},
				},
				{
					Query: `SELECT opf.opfname, lt.typname, rt.typname, amop.amopstrategy, opr.oprname, opr.oprcode
FROM "pg_catalog"."pg_amop" amop
JOIN "pg_catalog"."pg_opfamily" opf ON opf.oid = amop.amopfamily
JOIN "pg_catalog"."pg_type" lt ON lt.oid = amop.amoplefttype
JOIN "pg_catalog"."pg_type" rt ON rt.oid = amop.amoprighttype
JOIN "pg_catalog"."pg_operator" opr ON opr.oid = amop.amopopr
WHERE opf.opfname IN ('bytea_ops', 'interval_ops', 'oid_ops', 'time_ops', 'timetz_ops')
ORDER BY opf.opfname, amop.amopstrategy, opr.oprname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgamop-0006-select-opf.opfname-lt.typname-rt.typname-amop.amopstrategy"},
				},
				{
					Query: `SELECT opf.opfname, lt.typname, rt.typname, amop.amopstrategy, opr.oprname, opr.oprcode
FROM "pg_catalog"."pg_amop" amop
JOIN "pg_catalog"."pg_opfamily" opf ON opf.oid = amop.amopfamily
JOIN "pg_catalog"."pg_type" lt ON lt.oid = amop.amoplefttype
JOIN "pg_catalog"."pg_type" rt ON rt.oid = amop.amoprighttype
JOIN "pg_catalog"."pg_operator" opr ON opr.oid = amop.amopopr
WHERE opf.opfname IN ('bit_ops', 'char_ops', 'oidvector_ops', 'pg_lsn_ops', 'varbit_ops')
ORDER BY opf.opfname, amop.amopstrategy, opr.oprname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgamop-0007-select-opf.opfname-lt.typname-rt.typname-amop.amopstrategy"},
				},
				{
					Query: `SELECT am.amname, opf.opfname, amop.amopstrategy, opr.oprname
FROM "pg_catalog"."pg_amop" amop
JOIN "pg_catalog"."pg_opfamily" opf ON opf.oid = amop.amopfamily
JOIN "pg_catalog"."pg_am" am ON am.oid = amop.amopmethod
JOIN "pg_catalog"."pg_operator" opr ON opr.oid = amop.amopopr
WHERE opf.opfname LIKE 'jsonb%'
ORDER BY am.amname, opf.opfname, amop.amopstrategy;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgamop-0008-select-am.amname-opf.opfname-amop.amopstrategy-opr.oprname"},
				},
				{ // Different cases and quoted, so it fails
					Query: `SELECT * FROM "PG_catalog"."pg_amop";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgamop-0009-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_amop";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgamop-0010-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT COUNT(*) FROM PG_catalog.pg_AMOP;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgamop-0011-select-count-*-from-pg_catalog.pg_amop"},
				},
			},
		},
	})
}

func TestPgAmproc(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_amproc",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT opf.opfname, COUNT(*)
FROM "pg_catalog"."pg_amproc" amproc
JOIN "pg_catalog"."pg_opfamily" opf ON opf.oid = amproc.amprocfamily
JOIN "pg_catalog"."pg_am" am ON am.oid = opf.opfmethod
WHERE am.amname = 'btree'
GROUP BY opf.opfname
ORDER BY opf.opfname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgamproc-0001-select-opf.opfname-count-*-from"},
				},
				{
					Query: `SELECT lt.typname, rt.typname, amproc.amprocnum, amproc.amproc
FROM "pg_catalog"."pg_amproc" amproc
JOIN "pg_catalog"."pg_opfamily" opf ON opf.oid = amproc.amprocfamily
JOIN "pg_catalog"."pg_type" lt ON lt.oid = amproc.amproclefttype
JOIN "pg_catalog"."pg_type" rt ON rt.oid = amproc.amprocrighttype
WHERE opf.opfname = 'datetime_ops'
ORDER BY lt.typname, rt.typname, amproc.amprocnum, amproc.amproc;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgamproc-0002-select-lt.typname-rt.typname-amproc.amprocnum-amproc.amproc"},
				},
				{
					Query: `SELECT opf.opfname, lt.typname, rt.typname, amproc.amprocnum, amproc.amproc
FROM "pg_catalog"."pg_amproc" amproc
JOIN "pg_catalog"."pg_opfamily" opf ON opf.oid = amproc.amprocfamily
JOIN "pg_catalog"."pg_type" lt ON lt.oid = amproc.amproclefttype
JOIN "pg_catalog"."pg_type" rt ON rt.oid = amproc.amprocrighttype
WHERE opf.opfname IN ('bool_ops', 'bpchar_ops', 'numeric_ops', 'uuid_ops')
ORDER BY opf.opfname, lt.typname, rt.typname, amproc.amprocnum, amproc.amproc;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgamproc-0003-select-opf.opfname-lt.typname-rt.typname-amproc.amprocnum"},
				},
				{
					Query: `SELECT lt.typname, rt.typname, amproc.amprocnum, amproc.amproc
FROM "pg_catalog"."pg_amproc" amproc
JOIN "pg_catalog"."pg_opfamily" opf ON opf.oid = amproc.amprocfamily
JOIN "pg_catalog"."pg_type" lt ON lt.oid = amproc.amproclefttype
JOIN "pg_catalog"."pg_type" rt ON rt.oid = amproc.amprocrighttype
WHERE opf.opfname = 'integer_ops'
ORDER BY lt.typname, rt.typname, amproc.amprocnum, amproc.amproc;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgamproc-0004-select-lt.typname-rt.typname-amproc.amprocnum-amproc.amproc"},
				},
				{
					Query: `SELECT lt.typname, rt.typname, amproc.amprocnum, amproc.amproc
FROM "pg_catalog"."pg_amproc" amproc
JOIN "pg_catalog"."pg_opfamily" opf ON opf.oid = amproc.amprocfamily
JOIN "pg_catalog"."pg_type" lt ON lt.oid = amproc.amproclefttype
JOIN "pg_catalog"."pg_type" rt ON rt.oid = amproc.amprocrighttype
WHERE opf.opfname = 'float_ops'
ORDER BY lt.typname, rt.typname, amproc.amprocnum, amproc.amproc;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgamproc-0005-select-lt.typname-rt.typname-amproc.amprocnum-amproc.amproc"},
				},
				{
					Query: `SELECT lt.typname, rt.typname, amproc.amprocnum, amproc.amproc
FROM "pg_catalog"."pg_amproc" amproc
JOIN "pg_catalog"."pg_opfamily" opf ON opf.oid = amproc.amprocfamily
JOIN "pg_catalog"."pg_type" lt ON lt.oid = amproc.amproclefttype
JOIN "pg_catalog"."pg_type" rt ON rt.oid = amproc.amprocrighttype
WHERE opf.opfname = 'text_ops'
ORDER BY lt.typname, rt.typname, amproc.amprocnum, amproc.amproc;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgamproc-0006-select-lt.typname-rt.typname-amproc.amprocnum-amproc.amproc"},
				},
				{
					Query: `SELECT opf.opfname, lt.typname, rt.typname, amproc.amprocnum, amproc.amproc
FROM "pg_catalog"."pg_amproc" amproc
JOIN "pg_catalog"."pg_opfamily" opf ON opf.oid = amproc.amprocfamily
JOIN "pg_catalog"."pg_type" lt ON lt.oid = amproc.amproclefttype
JOIN "pg_catalog"."pg_type" rt ON rt.oid = amproc.amprocrighttype
WHERE opf.opfname IN ('bytea_ops', 'interval_ops', 'oid_ops', 'time_ops', 'timetz_ops')
ORDER BY opf.opfname, lt.typname, rt.typname, amproc.amprocnum, amproc.amproc;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgamproc-0007-select-opf.opfname-lt.typname-rt.typname-amproc.amprocnum"},
				},
				{
					Query: `SELECT opf.opfname, lt.typname, rt.typname, amproc.amprocnum, amproc.amproc
FROM "pg_catalog"."pg_amproc" amproc
JOIN "pg_catalog"."pg_opfamily" opf ON opf.oid = amproc.amprocfamily
JOIN "pg_catalog"."pg_type" lt ON lt.oid = amproc.amproclefttype
JOIN "pg_catalog"."pg_type" rt ON rt.oid = amproc.amprocrighttype
WHERE opf.opfname IN ('bit_ops', 'char_ops', 'oidvector_ops', 'pg_lsn_ops', 'varbit_ops')
ORDER BY opf.opfname, lt.typname, rt.typname, amproc.amprocnum, amproc.amproc;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgamproc-0008-select-opf.opfname-lt.typname-rt.typname-amproc.amprocnum"},
				},
				{
					Query: `SELECT am.amname, opf.opfname, amproc.amprocnum, amproc.amproc
FROM "pg_catalog"."pg_amproc" amproc
JOIN "pg_catalog"."pg_opfamily" opf ON opf.oid = amproc.amprocfamily
JOIN "pg_catalog"."pg_am" am ON am.oid = opf.opfmethod
WHERE opf.opfname LIKE 'jsonb%'
ORDER BY am.amname, opf.opfname, amproc.amprocnum;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgamproc-0009-select-am.amname-opf.opfname-amproc.amprocnum-amproc.amproc"},
				},
				{ // Different cases and quoted, so it fails
					Query: `SELECT * FROM "PG_catalog"."pg_amproc";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgamproc-0010-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_amproc";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgamproc-0011-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT COUNT(*) FROM PG_catalog.pg_AMPROC;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgamproc-0012-select-count-*-from-pg_catalog.pg_amproc"},
				},
			},
		},
	})
}

func TestPgAttribute(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_attribute",
			SetUpScript: []string{
				`CREATE SCHEMA testschema;`,
				`SET search_path TO testschema;`,
				`CREATE TABLE test (pk INT primary key, v1 TEXT DEFAULT 'hey');`,
				`CREATE TABLE test2 (pk INT primary key, pktesting INT REFERENCES test(pk), v1 TEXT);`,

				// Should show attributes for all schemas
				`CREATE SCHEMA testschema2;`,
				`SET search_path TO testschema2;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT attrelid::regclass::text, attname, atttypid::regtype::text, attlen, attnum, attcacheoff, atttypmod, attndims, attbyval, attalign, attstorage, attcompression, attnotnull, atthasdef, atthasmissing, attidentity, attgenerated, attisdropped, attislocal, attinhcount, attstattarget, attcollation::text, attacl, attoptions, attfdwoptions, attmissingval::text FROM "pg_catalog"."pg_attribute" WHERE attname='pk' AND attrelid='testschema.test'::regclass;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgattribute-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT attrelid::regclass::text, attname, atttypid::regtype::text, attlen, attnum, attcacheoff, atttypmod, attndims, attbyval, attalign, attstorage, attcompression, attnotnull, atthasdef, atthasmissing, attidentity, attgenerated, attisdropped, attislocal, attinhcount, attstattarget, attcollation::text, attacl, attoptions, attfdwoptions, attmissingval::text FROM "pg_catalog"."pg_attribute" WHERE attname='v1' AND attrelid='testschema.test'::regclass;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgattribute-0002-select-*-from-pg_catalog-."},
				},
				{ // Different cases and quoted, so it fails
					Query: `SELECT * FROM "PG_catalog"."pg_attribute";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgattribute-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_attribute";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgattribute-0004-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT attname FROM PG_catalog.pg_ATTRIBUTE WHERE attrelid='testschema.test'::regclass AND attnum > 0 ORDER BY attname LIMIT 3;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgattribute-0005-select-attname-from-pg_catalog.pg_attribute-order"},
				},
				{
					Query: `SELECT attname FROM "pg_catalog"."pg_attribute" a
    JOIN "pg_catalog"."pg_class" c ON a.attrelid = c.oid
               WHERE c.oid = 'testschema.test'::regclass
               ORDER BY a.attnum;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgattribute-0006-select-attname-from-pg_catalog-."},
				},
				{
					Query: `SELECT count(*) FROM pg_attribute as a1
				WHERE a1.attrelid = 'testschema.test'::regclass AND (a1.attrelid = 0 OR a1.atttypid = 0 OR a1.attnum = 0 OR
				a1.attcacheoff != -1 OR a1.attinhcount < 0 OR
				(a1.attinhcount = 0 AND NOT a1.attislocal));`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgattribute-0007-select-count-*-from-pg_attribute"},
				},
				{
					Query: `SELECT "con"."conname" AS "constraint_name", 
					"con"."nspname" AS "table_schema",
					"con"."relname" AS "table_name",
					"con"."confdeltype" AS "on_delete",
					"con"."confupdtype" AS "on_update",
					"con"."condeferrable" AS "deferrable",
					"con"."condeferred" AS "deferred",
          "con"."parent" as "parent",
          "con"."child" as "child"
					FROM
				( SELECT UNNEST ("con1"."conkey") AS "parent",
					UNNEST ("con1"."confkey") AS "child",
					"con1"."confrelid",
					"con1"."conrelid",
					"con1"."conname",
					"con1"."contype",
					"ns"."nspname",
					"cl"."relname",
					"con1"."condeferrable",
					CASE
					WHEN "con1"."condeferred" THEN 'INITIALLY DEFERRED'
					ELSE 'INITIALLY IMMEDIATE'
					END as condeferred,
					CASE "con1"."confdeltype"
					WHEN 'a' THEN 'NO ACTION'
					WHEN 'r' THEN 'RESTRICT'
					WHEN 'c' THEN 'CASCADE'
					WHEN 'n' THEN 'SET NULL'
					WHEN 'd' THEN 'SET DEFAULT'
					END as "confdeltype",
					CASE "con1"."confupdtype"
					WHEN 'a' THEN 'NO ACTION'
					WHEN 'r' THEN 'RESTRICT'
					WHEN 'c' THEN 'CASCADE'
					WHEN 'n' THEN 'SET NULL'
					WHEN 'd' THEN 'SET DEFAULT'
					END as "confupdtype"
					FROM "pg_class" "cl"
					INNER JOIN "pg_namespace" "ns" ON "cl"."relnamespace" = "ns"."oid"
					INNER JOIN "pg_constraint" "con1" ON "con1"."conrelid" = "cl"."oid"
					WHERE "con1"."contype" = 'f'
					AND (("ns"."nspname" = 'testschema' AND "cl"."relname" = 'test2')) ) "con" order by 1`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgattribute-0008-select-con-.-conname-as"},
				},
				{
					Query: `SELECT "con"."conname" AS "constraint_name", 
       "con"."nspname" AS "table_schema", 
       "con"."relname" AS "table_name", 
       "att2"."attname" AS "column_name", 
       "ns"."nspname" AS "referenced_table_schema", 
       "cl"."relname" AS "referenced_table_name", 
       "att"."attname" AS "referenced_column_name", 
       "con"."confdeltype" AS "on_delete", 
       "con"."confupdtype" AS "on_update", 
       "con"."condeferrable" AS "deferrable", 
       "con"."condeferred" AS "deferred"
FROM 
    ( SELECT UNNEST ("con1"."conkey") AS "parent", 
              UNNEST ("con1"."confkey") AS "child", 
              "con1"."confrelid", 
              "con1"."conrelid", 
              "con1"."conname", 
              "con1"."contype", 
              "ns"."nspname", 
              "cl"."relname", 
              "con1"."condeferrable", 
              CASE 
                  WHEN "con1"."condeferred" THEN 'INITIALLY DEFERRED' 
                  ELSE 'INITIALLY IMMEDIATE' 
                  END as condeferred, 
           CASE "con1"."confdeltype" 
               WHEN 'a' THEN 'NO ACTION' 
               WHEN 'r' THEN 'RESTRICT' 
               WHEN 'c' THEN 'CASCADE' 
               WHEN 'n' THEN 'SET NULL' 
               WHEN 'd' THEN 'SET DEFAULT' 
               END as "confdeltype", 
           CASE "con1"."confupdtype" 
               WHEN 'a' THEN 'NO ACTION' 
               WHEN 'r' THEN 'RESTRICT' 
               WHEN 'c' THEN 'CASCADE' 
               WHEN 'n' THEN 'SET NULL' 
               WHEN 'd' THEN 'SET DEFAULT' 
               END as "confupdtype" 
       FROM "pg_class" "cl" 
           INNER JOIN "pg_namespace" "ns" ON "cl"."relnamespace" = "ns"."oid" 
           INNER JOIN "pg_constraint" "con1" ON "con1"."conrelid" = "cl"."oid" 
       WHERE "con1"."contype" = 'f' 
         AND (("ns"."nspname" = 'testschema' AND "cl"."relname" = 'test2')) ) "con" 
    INNER JOIN "pg_attribute" "att" ON "att"."attrelid" = "con"."confrelid" AND "att"."attnum" = "con"."child"
    INNER JOIN "pg_class" "cl" ON "cl"."oid" = "con"."confrelid"  AND "cl"."relispartition" = 'f'
    INNER JOIN "pg_namespace" "ns" ON "cl"."relnamespace" = "ns"."oid" 
    INNER JOIN "pg_attribute" "att2" ON "att2"."attrelid" = "con"."conrelid" AND "att2"."attnum" = "con"."parent"
order by 1,2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgattribute-0009-select-con-.-conname-as"},
				},
			},
		},
	})
}

func TestPgAttrdef(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_attrdef",
			SetUpScript: []string{
				`CREATE SCHEMA testschema;`,
				`SET search_path TO testschema;`,
				`CREATE TABLE test (pk INT primary key, v1 TEXT DEFAULT 'hey');`,

				// Should show attributes for all schemas
				`CREATE SCHEMA testschema2;`,
				`SET search_path TO testschema2;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT adrelid::regclass::text, adnum, pg_get_expr(adbin, adrelid) FROM "pg_catalog"."pg_attrdef" WHERE adrelid='testschema.test'::regclass;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgattrdef-0001-select-*-from-pg_catalog-."},
				},
				{ // Different cases and quoted, so it fails
					Query: `SELECT * FROM "PG_catalog"."pg_attrdef";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgattrdef-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_attrdef";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgattrdef-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT adrelid::regclass::text, adnum FROM PG_catalog.pg_ATTRDEF WHERE adrelid='testschema.test'::regclass ORDER BY adnum;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgattrdef-0004-select-oid-from-pg_catalog.pg_attrdef-order"},
				},
			},
		},
	})
}

func TestPgAuthMembers(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_auth_members",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_auth_members";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgauthmembers-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_auth_members";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgauthmembers-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_auth_members";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgauthmembers-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT member FROM PG_catalog.pg_AUTH_MEMBERS ORDER BY member;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgauthmembers-0004-select-member-from-pg_catalog.pg_auth_members-order"},
				},
			},
		},
	})
}

func TestPgAuthid(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_authid",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT oid, rolname, rolsuper, rolcanlogin
						FROM "pg_catalog"."pg_authid"
						WHERE rolname IN (
							'pg_checkpoint',
							'pg_create_subscription',
							'pg_database_owner',
							'pg_execute_server_program',
							'pg_monitor',
							'pg_read_all_data',
							'pg_read_all_settings',
							'pg_read_all_stats',
							'pg_read_server_files',
							'pg_signal_backend',
							'pg_stat_scan_tables',
							'pg_use_reserved_connections',
							'pg_write_all_data',
							'pg_write_server_files'
						)
						ORDER BY rolname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgauthid-0001-select-oid-rolname-rolsuper-rolcanlogin"},
				},
				{ // Different cases and quoted, so it fails
					Query: `SELECT * FROM "PG_catalog"."pg_authid";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgauthid-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_authid";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgauthid-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT rolname
						FROM PG_catalog.pg_AUTHID
						WHERE rolname IN (
							'pg_checkpoint',
							'pg_create_subscription',
							'pg_database_owner',
							'pg_execute_server_program',
							'pg_monitor',
							'pg_read_all_data',
							'pg_read_all_settings',
							'pg_read_all_stats',
							'pg_read_server_files',
							'pg_signal_backend',
							'pg_stat_scan_tables',
							'pg_use_reserved_connections',
							'pg_write_all_data',
							'pg_write_server_files'
						)
						ORDER BY rolname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgauthid-0004-select-rolname-from-pg_catalog.pg_authid-order"},
				},
			},
		},
	})
}

func TestPgAvailableExtensionVersions(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_available_extension_versions",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_available_extension_versions" WHERE false;`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgavailableextensionversions-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_available_extension_versions";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgavailableextensionversions-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_available_extension_versions";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgavailableextensionversions-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT name FROM PG_catalog.pg_AVAILABLE_EXTENSION_VERSIONS WHERE name = 'citext';", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgavailableextensionversions-0004-select-name-from-where-name"},
				},
			},
		},
	})
}

func TestPgAvailableExtensions(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_available_extensions",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_available_extensions" WHERE false;`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgavailableextensions-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_available_extensions";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgavailableextensions-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_available_extensions";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgavailableextensions-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT name FROM PG_catalog.pg_AVAILABLE_EXTENSIONS WHERE name = 'citext';", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgavailableextensions-0004-select-name-from-pg_catalog.pg_available_extensions-where"},
				},
			},
		},
	})
}

func TestPgBackendMemoryContexts(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_backend_memory_contexts",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT name, parent, level::text, (total_bytes >= free_bytes)::text AS valid_bytes
						FROM "pg_catalog"."pg_backend_memory_contexts"
						WHERE level = 0
							AND name = 'TopMemoryContext';`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgbackendmemorycontexts-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_backend_memory_contexts";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgbackendmemorycontexts-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_backend_memory_contexts";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgbackendmemorycontexts-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT name
						FROM PG_catalog.pg_BACKEND_MEMORY_CONTEXTS
						WHERE level = 0
							AND name = 'TopMemoryContext'
						ORDER BY name;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgbackendmemorycontexts-0004-select-name-from-pg_catalog.pg_backend_memory_contexts-order"},
				},
			},
		},
	})
}

func TestPgCast(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_cast",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT oid::text, castsource::regtype::text, casttarget::regtype::text, castfunc::text, castcontext, castmethod
						FROM "pg_catalog"."pg_cast"
						WHERE castsource = 'integer'::regtype
							AND casttarget = 'bigint'::regtype;`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgcast-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_cast";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgcast-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_cast";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgcast-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT oid::text, castsource::regtype::text, casttarget::regtype::text
						FROM PG_catalog.pg_CAST
						WHERE castsource = 'integer'::regtype
							AND casttarget = 'bigint'::regtype
						ORDER BY oid;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgcast-0004-select-oid-from-pg_catalog.pg_cast-order"},
				},
			},
		},
	})
}

func TestPgClass(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_class",
			SetUpScript: []string{
				`CREATE SCHEMA testschema;`,
				`SET search_path TO testschema;`,
				`CREATE TABLE testing (pk INT primary key, v1 INT UNIQUE);`,
				`CREATE VIEW testview AS SELECT * FROM testing LIMIT 1;`,

				// Should show classes for all schemas
				`CREATE SCHEMA testschema2;`,
				`SET search_path TO testschema2;`,
			},
			Assertions: []ScriptTestAssertion{
				// Table
				{
					Query: `SELECT c.relname, n.nspname, c.relkind, c.relpersistence, c.relnatts::text,
							c.relhasindex::text, c.relhasrules::text, c.relhastriggers::text,
							c.relrowsecurity::text, c.relreplident
						FROM "pg_catalog"."pg_class" c
						JOIN "pg_catalog"."pg_namespace" n ON n.oid = c.relnamespace
						WHERE c.relname='testing' AND n.nspname = 'testschema'
						ORDER BY c.relname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgclass-0001-select-*-from-pg_catalog-."},
				},
				// Index
				{
					Query: `SELECT c.relname, n.nspname, c.relkind, am.amname, c.relpersistence,
							c.relhasindex::text, c.relhasrules::text, c.relhastriggers::text,
							c.relrowsecurity::text
						FROM "pg_catalog"."pg_class" c
						JOIN "pg_catalog"."pg_namespace" n ON n.oid = c.relnamespace
						LEFT JOIN "pg_catalog"."pg_am" am ON am.oid = c.relam
						WHERE c.relname='testing_pkey' AND n.nspname = 'testschema'
						ORDER BY c.relname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgclass-0002-select-*-from-pg_catalog-."},
				},
				// View
				{
					Query: `SELECT c.relname, n.nspname, c.relkind, c.relpersistence, c.relnatts::text,
							c.relhasindex::text, c.relhasrules::text, c.relhastriggers::text,
							c.relrowsecurity::text
						FROM "pg_catalog"."pg_class" c
						JOIN "pg_catalog"."pg_namespace" n ON n.oid = c.relnamespace
						WHERE c.relname='testview' AND n.nspname = 'testschema'
						ORDER BY c.relname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgclass-0003-select-*-from-pg_catalog-."},
				},
				{ // Different cases and quoted, so it fails
					Query: `SELECT * FROM "PG_catalog"."pg_class";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgclass-0004-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_class";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgclass-0005-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT c.relname
						FROM PG_catalog.pg_CLASS c
						JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
						WHERE n.nspname = 'testschema'
							AND c.relname IN ('testing', 'testing_pkey', 'testview')
						ORDER BY c.relname ASC;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgclass-0006-select-relname-from-pg_catalog.pg_class-where"},
				},
				{
					Query: "SELECT relname from pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid  WHERE n.nspname = 'testschema' and left(relname, 5) <> 'dolt_' ORDER BY relname;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgclass-0007-select-relname-from-pg_catalog.pg_class-c"},
				},
				{
					Query: `SELECT relname
						FROM pg_catalog.pg_class c
						JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
						WHERE n.nspname = 'pg_catalog'
							AND relname IN ('pg_am', 'pg_class', 'pg_namespace')
						ORDER BY relname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgclass-0008-select-relname-from-pg_catalog.pg_class-c"},
				},
				{
					Query: `SELECT relname FROM "pg_class" WHERE relname='testing';`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgclass-0009-select-relname-from-pg_class-where"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."pg_class" WHERE oid=1234`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgclass-0010-select-*-from-pg_catalog-."},
				},
			},
		},
		{
			Name: "pg_class with regclass",
			SetUpScript: []string{
				`CREATE SCHEMA testschema;`,
				`SET search_path TO testschema;`,
				`CREATE TABLE testing (pk INT primary key, v1 INT UNIQUE);`,
				`CREATE VIEW testview AS SELECT * FROM testing LIMIT 1;`,
				`CREATE SCHEMA testschema2;`,
				`SET search_path TO testschema2;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_class" WHERE oid='testing'::regclass;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgclass-0011-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT c.relname, n.nspname, c.relkind, c.relpersistence, c.relnatts::text,
							c.relhasindex::text, c.relhasrules::text, c.relhastriggers::text,
							c.relrowsecurity::text, c.relreplident
						FROM "pg_catalog"."pg_class" c
						JOIN "pg_catalog"."pg_namespace" n ON n.oid = c.relnamespace
						WHERE c.oid='testschema.testing'::regclass;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgclass-0012-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT c.relname, n.nspname, c.relkind, am.amname, c.relpersistence,
							c.relhasindex::text, c.relhasrules::text, c.relhastriggers::text,
							c.relrowsecurity::text
						FROM "pg_catalog"."pg_class" c
						JOIN "pg_catalog"."pg_namespace" n ON n.oid = c.relnamespace
						LEFT JOIN "pg_catalog"."pg_am" am ON am.oid = c.relam
						WHERE c.oid='testschema.testing_pkey'::regclass;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgclass-0013-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT c.relname, n.nspname, c.relkind, c.relpersistence, c.relnatts::text,
							c.relhasindex::text, c.relhasrules::text, c.relhastriggers::text,
							c.relrowsecurity::text
						FROM "pg_catalog"."pg_class" c
						JOIN "pg_catalog"."pg_namespace" n ON n.oid = c.relnamespace
						WHERE c.oid='testschema.testview'::regclass;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgclass-0014-select-*-from-pg_catalog-."},
				},
			},
		},
		{
			Name: "pg_class joined with other pg_catalog tables to retrieve indexes",
			SetUpScript: []string{
				`CREATE TABLE foo (a INTEGER NOT NULL PRIMARY KEY, b INTEGER NULL);`,
				`CREATE INDEX ON foo ( b ASC ) NULLS NOT DISTINCT;`,
				`CREATE INDEX ON foo ( b ASC , a DESC ) NULLS NOT DISTINCT;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT ix.relname AS index_name, upper(am.amname) AS index_algorithm FROM pg_index i 
JOIN pg_class t ON t.oid = i.indrelid 
JOIN pg_class ix ON ix.oid = i.indexrelid 
JOIN pg_namespace n ON t.relnamespace = n.oid 
JOIN pg_am AS am ON ix.relam = am.oid WHERE t.relname = 'foo' AND n.nspname = current_schema() ORDER BY ix.relname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgclass-0015-select-ix.relname-as-index_name-upper"},
				},
			},
		},
	})
}

func TestPgCollation(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_collation",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT c.collname, n.nspname, c.collprovider, c.collisdeterministic::text, c.collencoding::text
FROM "pg_catalog"."pg_collation" c
JOIN "pg_catalog"."pg_namespace" n ON n.oid = c.collnamespace
WHERE c.collname IN ('default', 'C', 'POSIX', 'ucs_basic', 'und-x-icu')
ORDER BY c.collname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgcollation-0001-select-oid-collname-collnamespace-collprovider"},
				},
				{ // Different cases and quoted, so it fails
					Query: `SELECT * FROM "PG_catalog"."pg_collation";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgcollation-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_collation";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgcollation-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT collname FROM PG_catalog.pg_COLLATION WHERE collname IN ('default', 'C', 'POSIX', 'ucs_basic', 'und-x-icu') ORDER BY collname;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgcollation-0004-select-collname-from-pg_catalog.pg_collation-order"},
				},
			},
		},
	})
}

func TestPgConfig(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_config",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT name FROM "pg_catalog"."pg_config" WHERE name = 'BINDIR';`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgconfig-0001-select-name-from-pg_catalog-."},
				},
				{ // Different cases and quoted, so it fails
					Query: `SELECT * FROM "PG_catalog"."pg_config";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgconfig-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_config";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgconfig-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT name FROM PG_catalog.pg_CONFIG ORDER BY name;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgconfig-0004-select-name-from-pg_catalog.pg_config-order"},
				},
			},
		},
	})
}

func TestPgConstraint(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_constraint",
			SetUpScript: []string{
				`CREATE TABLE testing (pk INT primary key, v1 INT UNIQUE);`,
				`CREATE TABLE testing2 (pk INT primary key, pktesting INT REFERENCES testing(pk), v1 TEXT);`,
				`ALTER TABLE testing2 ADD CONSTRAINT v1_check CHECK (v1 != '')`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT co.conname, cl.relname, co.contype, co.condeferrable::text, co.condeferred::text, co.convalidated::text, co.confupdtype, co.confdeltype, co.confmatchtype, co.conislocal::text, co.coninhcount::text, co.connoinherit::text, co.conkey::text, co.confkey::text, COALESCE(ref.relname, '') AS confrelname
						FROM "pg_catalog"."pg_constraint" co
						JOIN "pg_catalog"."pg_class" cl ON co.conrelid = cl.oid
						LEFT JOIN "pg_catalog"."pg_class" ref ON co.confrelid = ref.oid
						WHERE co.conrelid='testing2'::regclass OR co.conrelid='testing'::regclass
						ORDER BY co.conname`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgconstraint-0001-select-*-from-pg_catalog-."},
				},
				{ // Different cases and quoted, so it fails
					Query: `SELECT * FROM "PG_catalog"."pg_constraint";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgconstraint-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_constraint";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgconstraint-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT conname FROM PG_catalog.pg_CONSTRAINT WHERE conrelid IN ('testing'::regclass, 'testing2'::regclass) ORDER BY conname;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgconstraint-0004-select-conname-from-pg_catalog.pg_constraint-order"},
				},
				{
					Query: "SELECT co.conname, co.conrelid::regclass::text, cl.relname FROM pg_catalog.pg_constraint co JOIN pg_catalog.pg_class cl ON co.conrelid = cl.oid WHERE cl.relname = 'testing2' ORDER BY co.conname;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgconstraint-0005-select-co.oid-co.conname-co.conrelid-cl.relname"},
				},
			},
		},
	})
}

func TestPgConstraintIndexes(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_constraint",
			SetUpScript: []string{
				`CREATE TABLE testing (pk INT primary key, v1 INT UNIQUE);`,
				`CREATE TABLE testing2 (pk INT primary key, pktesting INT REFERENCES testing(pk), v1 TEXT);`,
				`ALTER TABLE testing2 ADD CONSTRAINT v1_check CHECK (v1 != '')`,
				`CREATE DOMAIN mydomain AS INT CHECK (VALUE > 0);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT conname FROM pg_catalog.pg_constraint WHERE oid = (SELECT oid FROM pg_catalog.pg_constraint WHERE conname = 'testing2_pkey' AND connamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = current_schema()));", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgconstraintindexes-0001-select-oid-conname-from-pg_catalog.pg_constraint"},
				},
				{
					Query: "explain SELECT conname FROM pg_catalog.pg_constraint WHERE oid = 2068729390 order by 1",
					Expected: []sql.Row{
						{"Project"},
						{" ├─ columns: [pg_constraint.conname]"},
						{" └─ Sort(pg_constraint.conname ASC)"},
						{"     └─ Filter"},
						{"         ├─ pg_constraint.oid = 2068729390"},
						{"         └─ IndexedTableAccess(pg_constraint)"},
						{"             ├─ index: [pg_constraint.oid]"},
						{`             └─ filters: [{[{Index:["public","testing2","PRIMARY"]}, {Index:["public","testing2","PRIMARY"]}]}]`},
					},
				},
				{
					Query: "SELECT conname FROM pg_catalog.pg_constraint WHERE conrelid = 'testing2'::regclass ORDER BY conname;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgconstraintindexes-0003-select-oid-conname-from-pg_catalog.pg_constraint"},
				},
				{
					Query: "SELECT conname FROM pg_catalog.pg_constraint WHERE conname = 'testing_pkey' AND connamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = current_schema());", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgconstraintindexes-0004-select-oid-conname-from-pg_catalog.pg_constraint"},
				},
				{
					Query: "SELECT conname FROM pg_catalog.pg_constraint WHERE conrelid IN ('testing'::regclass, 'testing2'::regclass) AND conname >= 'testing' AND conname < 'testingz' ORDER BY conname;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgconstraintindexes-0005-select-oid-conname-from-pg_catalog.pg_constraint"},
				},
			},
		},
		{
			Name: "pg_constraint comprehensive index tests",
			SetUpScript: []string{
				`CREATE TABLE test_table1 (pk INT primary key, val1 INT UNIQUE, val2 TEXT);`,
				`CREATE TABLE test_table2 (id INT primary key, fk_col INT REFERENCES test_table1(pk), name TEXT UNIQUE);`,
				`CREATE TABLE test_table3 (pk1 INT, pk2 INT, val TEXT, PRIMARY KEY(pk1, pk2));`,
				`ALTER TABLE test_table2 ADD CONSTRAINT name_check CHECK (name != '');`,
				`ALTER TABLE test_table1 ADD CONSTRAINT val2_check CHECK (val2 IS NOT NULL);`,
				`CREATE DOMAIN test_domain AS INT CHECK (VALUE > 0);`,
				`CREATE DOMAIN test_domain2 AS TEXT CHECK (LENGTH(VALUE) > 2);`,
			},
			Assertions: []ScriptTestAssertion{
				// Primary key index tests (pg_constraint_oid_index)
				{
					Query: "SELECT conname FROM pg_catalog.pg_constraint WHERE oid = (SELECT oid FROM pg_catalog.pg_constraint WHERE conname = 'test_table1_pkey' AND connamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = current_schema()));", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgconstraintindexes-0006-select-conname-from-pg_catalog.pg_constraint-where"},
				},
				{
					Query: "SELECT conname FROM pg_catalog.pg_constraint WHERE oid IN (SELECT oid FROM pg_catalog.pg_constraint WHERE conname IN ('test_table1_pkey', 'test_table2_pkey', 'test_table3_pkey') AND connamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = current_schema())) ORDER BY conname;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgconstraintindexes-0007-select-conname-from-pg_catalog.pg_constraint-where"},
				},
				// conname + connamespace index tests (pg_constraint_conname_nsp_index)
				{
					Query: "SELECT conname FROM pg_catalog.pg_constraint WHERE conname = 'test_table1_pkey' AND connamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = current_schema());", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgconstraintindexes-0008-select-conname-connamespace-from-pg_catalog.pg_constraint"},
				},
				{
					Query: "explain SELECT conname FROM pg_catalog.pg_constraint WHERE conname = 'test_table1_pkey' AND connamespace = 2200 order by 1",
					Expected: []sql.Row{
						{"Project"},
						{" ├─ columns: [pg_constraint.conname]"},
						{" └─ Sort(pg_constraint.conname ASC)"},
						{"     └─ Filter"},
						{"         ├─ (pg_constraint.conname = 'test_table1_pkey' AND pg_constraint.connamespace = 2200)"},
						{"         └─ IndexedTableAccess(pg_constraint)"},
						{"             ├─ index: [pg_constraint.conname,pg_constraint.connamespace]"},
						{`             └─ filters: [{[test_table1_pkey, test_table1_pkey], [{Namespace:["public"]}, {Namespace:["public"]}]}]`},
					},
				},
				{
					Query: "SELECT conname FROM pg_catalog.pg_constraint WHERE conname IN ('test_table1_pkey', 'test_table2_pkey', 'name_check') AND connamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = current_schema()) ORDER BY conname;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgconstraintindexes-0010-select-conname-from-pg_catalog.pg_constraint-where"},
				},
				{
					Query: "SELECT conname FROM pg_catalog.pg_constraint WHERE conname >= 'name_' AND conname < 'name_z' AND connamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = current_schema()) ORDER BY conname;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgconstraintindexes-0011-select-conname-from-pg_catalog.pg_constraint-where"},
				},
				// conrelid + contypid + conname index tests (pg_constraint_conrelid_contypid_conname_index)
				{
					Query: "SELECT conname FROM pg_catalog.pg_constraint WHERE conrelid = 'test_table1'::regclass AND contypid = 0 ORDER BY conname;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgconstraintindexes-0012-select-conname-from-pg_catalog.pg_constraint-where"},
				},
				{
					Query: "explain SELECT conname FROM pg_catalog.pg_constraint WHERE conrelid = 'test_table1'::regclass AND contypid = 0 ORDER BY conname;",
					Expected: []sql.Row{
						{"Project"},
						{" ├─ columns: [pg_constraint.conname]"},
						{" └─ Sort(pg_constraint.conname ASC)"},
						{"     └─ Filter"},
						{`         ├─ (pg_constraint.conrelid = {Table:["public","test_table1"]} AND pg_constraint.contypid = 0)`},
						{"         └─ IndexedTableAccess(pg_constraint)"},
						{"             ├─ index: [pg_constraint.conrelid,pg_constraint.contypid,pg_constraint.conname]"},
						{`             └─ filters: [{[{Table:["public","test_table1"]}, {Table:["public","test_table1"]}], [{OID:["0"]}, {OID:["0"]}], [NULL, ∞)}]`},
					},
				},
				{
					Query: "SELECT conname FROM pg_catalog.pg_constraint WHERE conrelid = 'test_table1'::regclass AND contypid = 0 ORDER BY conname;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgconstraintindexes-0014-select-conname-from-pg_catalog.pg_constraint-where"},
				},
				{
					Query: "SELECT conname FROM pg_catalog.pg_constraint WHERE conrelid IN ('test_table1'::regclass, 'test_table2'::regclass) AND contypid = 0 ORDER BY conname;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgconstraintindexes-0015-select-conname-from-pg_catalog.pg_constraint-where"},
				},
				{
					Query: "SELECT conname FROM pg_catalog.pg_constraint WHERE conrelid = 'test_table2'::regclass AND contypid = 0 AND conname = 'name_check';", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgconstraintindexes-0016-select-conname-from-pg_catalog.pg_constraint-where"},
				},

				// contypid index tests (pg_constraint_contypid_index)
				{
					Query: "SELECT conname FROM pg_catalog.pg_constraint WHERE contypid = 'test_domain'::regtype ORDER BY conname;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgconstraintindexes-0017-select-conname-from-pg_catalog.pg_constraint-where"},
				},
				{
					Query: "SELECT conname FROM pg_catalog.pg_constraint WHERE contypid = 'test_domain'::regtype ORDER BY conname;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgconstraintindexes-0018-select-conname-from-pg_catalog.pg_constraint-where"},
				},
				{
					Query: "explain SELECT conname FROM pg_catalog.pg_constraint WHERE contypid = 'test_domain'::regtype ORDER BY conname;",
					Expected: []sql.Row{
						{"Project"},
						{" ├─ columns: [pg_constraint.conname]"},
						{" └─ Sort(pg_constraint.conname ASC)"},
						{"     └─ Filter"},
						{`         ├─ pg_constraint.contypid = {Type:["public","test_domain"]}`},
						{"         └─ IndexedTableAccess(pg_constraint)"},
						{"             ├─ index: [pg_constraint.contypid]"},
						{`             └─ filters: [{[{Type:["public","test_domain"]}, {Type:["public","test_domain"]}]}]`},
					},
				},
				{
					Query: "SELECT conname FROM pg_catalog.pg_constraint WHERE contypid IN ('test_domain'::regtype, 'test_domain2'::regtype) ORDER BY conname;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgconstraintindexes-0020-select-conname-from-pg_catalog.pg_constraint-where"},
				},
				{
					Query: "SELECT conname FROM pg_catalog.pg_constraint WHERE contypid IN ('test_domain'::regtype, 'test_domain2'::regtype) AND contypid > 0 ORDER BY conname;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgconstraintindexes-0021-select-conname-from-pg_catalog.pg_constraint-where"},
				},
				{
					Query: "SELECT conname FROM pg_catalog.pg_constraint WHERE conrelid IN ('test_table1'::regclass, 'test_table2'::regclass, 'test_table3'::regclass) AND contypid = 0 ORDER BY conname;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgconstraintindexes-0022-select-conname-from-pg_catalog.pg_constraint-where"},
				},
				{
					Query: "explain SELECT conname FROM pg_catalog.pg_constraint WHERE conrelid IN ('test_table1'::regclass, 'test_table2'::regclass, 'test_table3'::regclass) AND contypid = 0 ORDER BY conname;",
					Expected: []sql.Row{
						{"Project"},
						{" ├─ columns: [pg_constraint.conname]"},
						{" └─ Sort(pg_constraint.conname ASC)"},
						{"     └─ Filter"},
						{`         ├─ (pg_constraint.conrelid IN ({Table:["public","test_table1"]}, {Table:["public","test_table2"]}, {Table:["public","test_table3"]}) AND pg_constraint.contypid = 0)`},
						{"         └─ IndexedTableAccess(pg_constraint)"},
						{"             ├─ index: [pg_constraint.conrelid,pg_constraint.contypid,pg_constraint.conname]"},
						{`             └─ filters: [{[{Table:["public","test_table3"]}, {Table:["public","test_table3"]}], [{OID:["0"]}, {OID:["0"]}], [NULL, ∞)}, {[{Table:["public","test_table2"]}, {Table:["public","test_table2"]}], [{OID:["0"]}, {OID:["0"]}], [NULL, ∞)}, {[{Table:["public","test_table1"]}, {Table:["public","test_table1"]}], [{OID:["0"]}, {OID:["0"]}], [NULL, ∞)}]`},
					},
				},
				// Prefix match on 3-column index
				{
					Query: "SELECT conname FROM pg_catalog.pg_constraint WHERE conrelid = 'test_table1'::regclass ORDER BY conname;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgconstraintindexes-0024-select-conname-from-pg_catalog.pg_constraint-where"},
				},
				{
					Query: "SELECT conname FROM pg_catalog.pg_constraint WHERE conrelid = 'test_table1'::regclass ORDER BY conname;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgconstraintindexes-0025-select-conname-from-pg_catalog.pg_constraint-where"},
				},
				{
					Query: "explain SELECT conname FROM pg_catalog.pg_constraint WHERE conrelid = 'test_table1'::regclass ORDER BY conname;",
					Expected: []sql.Row{
						{"Project"},
						{" ├─ columns: [pg_constraint.conname]"},
						{" └─ Sort(pg_constraint.conname ASC)"},
						{"     └─ Filter"},
						{`         ├─ pg_constraint.conrelid = {Table:["public","test_table1"]}`},
						{"         └─ IndexedTableAccess(pg_constraint)"},
						{"             ├─ index: [pg_constraint.conrelid,pg_constraint.contypid,pg_constraint.conname]"},
						{`             └─ filters: [{[{Table:["public","test_table1"]}, {Table:["public","test_table1"]}], [NULL, ∞), [NULL, ∞)}]`},
					},
				},
				{
					Query: "SELECT co.conname FROM pg_catalog.pg_constraint co JOIN pg_catalog.pg_class cl ON co.conrelid = cl.oid WHERE cl.relname IN ('test_table1', 'test_table2', 'test_table3') AND (co.conname LIKE '%_pkey' OR co.conname LIKE '%_key') AND co.connamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = current_schema()) ORDER BY co.conname;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgconstraintindexes-0027-select-conname-from-pg_catalog.pg_constraint-where"},
				},
				{
					Query: "EXPLAIN SELECT conname FROM pg_catalog.pg_constraint WHERE conname = 'test_table1_pkey' AND connamespace = 2200;",
					Expected: []sql.Row{
						{"Index Scan using pg_constraint_conname_nsp_index on pg_constraint  (cost=0.15..8.17 rows=1 width=32)"},
						{"  Index Cond: (conname = 'test_table1_pkey' AND pg_constraint.connamespace = 2200::name)"},
					},
				},
				{
					Query: "EXPLAIN SELECT conname FROM pg_catalog.pg_constraint WHERE conrelid = 'test_table1'::regclass AND contypid > 0 order by 1;",
					Expected: []sql.Row{
						{"Project"},
						{" ├─ columns: [pg_constraint.conname]"},
						{" └─ Sort(pg_constraint.conname ASC)"},
						{"     └─ Filter"},
						{`         ├─ (pg_constraint.conrelid = {Table:["public","test_table1"]} AND pg_constraint.contypid > 0)`},
						{"         └─ IndexedTableAccess(pg_constraint)"},
						{"             ├─ index: [pg_constraint.conrelid,pg_constraint.contypid,pg_constraint.conname]"},
						{`             └─ filters: [{[{Table:["public","test_table1"]}, {Table:["public","test_table1"]}], ({OID:["0"]}, ∞), [NULL, ∞)}]`},
					},
				},
			},
		},
		{
			Name: "text constant to oid conversion",
			SetUpScript: []string{
				`CREATE TABLE testing (pk INT primary key, v1 INT UNIQUE);`,
				`CREATE TABLE testing2 (pk INT primary key, pktesting INT REFERENCES testing(pk), v1 TEXT);`,
				`ALTER TABLE testing2 ADD CONSTRAINT v1_check CHECK (v1 != '')`,
				`CREATE DOMAIN mydomain AS INT CHECK (VALUE > 0);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					// We don't care about the result, we just want to make sure it doens't error
					Query: "SELECT true as sametable, conname," +
						"pg_catalog.pg_get_constraintdef(r.oid, true) as condef," +
						"conrelid::pg_catalog.regclass AS ontable " +
						"FROM pg_catalog.pg_constraint r " +
						"WHERE r.conrelid = '145181' AND r.contype = 'f' " +
						"     AND conparentid = 0 " +
						"ORDER BY conname",
				},
			},
		},
	})
}

func TestPgConversion(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_conversion",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT conname FROM "pg_catalog"."pg_conversion" WHERE conname = 'utf8_to_iso_8859_1';`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgconversion-0001-select-conname-from-pg_catalog-."},
				},
				{ // Different cases and quoted, so it fails
					Query: `SELECT * FROM "PG_catalog"."pg_conversion";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgconversion-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_conversion";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgconversion-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT conname FROM PG_catalog.pg_CONVERSION WHERE conname = 'utf8_to_iso_8859_1' ORDER BY conname;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgconversion-0004-select-conname-from-pg_catalog.pg_conversion-order"},
				},
			},
		},
	})
}

func TestPgCursors(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_cursors",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_cursors";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgcursors-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_cursors";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgcursors-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_cursors";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgcursors-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT name FROM PG_catalog.pg_CURSORS ORDER BY name;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgcursors-0004-select-name-from-pg_catalog.pg_cursors-order"},
				},
			},
		},
	})
}

func TestPgDatabase(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_database",
			SetUpScript: []string{
				`CREATE DATABASE test
					WITH TEMPLATE = template0
					LOCALE_PROVIDER = libc
					LOCALE = 'C';`,
				`CREATE TABLE test (pk INT primary key, v1 INT);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT datname FROM "pg_catalog"."pg_database" WHERE datname IN ('postgres', 'test') ORDER BY datname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgdatabase-0001-select-datname-from-pg_catalog-."},
				},
				{
					Query: `SELECT datname, oid::int > 0 AS has_oid FROM "pg_catalog"."pg_database" WHERE datname IN ('postgres', 'test') ORDER BY datname DESC;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgdatabase-0002-select-oid-datname-from-pg_catalog"},
				},
				{ // Different cases and quoted, so it fails
					Query: `SELECT * FROM "PG_catalog"."pg_database";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgdatabase-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_database";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgdatabase-0004-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT datname FROM PG_catalog.pg_DATABASE WHERE datname IN ('postgres', 'test') ORDER BY datname ASC;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgdatabase-0005-select-oid-datname-from-pg_catalog.pg_database"},
				},
				{
					Query: `SELECT
							datname,
							pg_encoding_to_char(encoding),
							datlocprovider,
							datistemplate,
							datallowconn,
							datconnlimit,
							datcollate,
							datctype,
							daticulocale,
							daticurules,
							datcollversion,
							dattablespace = (SELECT oid FROM pg_catalog.pg_tablespace WHERE spcname = 'pg_default') AS uses_default_tablespace
						FROM pg_catalog.pg_database
						WHERE datname = 'test';`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgdatabase-0006-select-*-from-pg_catalog.pg_database-where"},
				},
			},
		},
	})
}

func TestPgDbRoleSetting(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_db_role_setting",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_db_role_setting";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgdbrolesetting-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_db_role_setting";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgdbrolesetting-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_db_role_setting";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgdbrolesetting-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT setdatabase FROM PG_catalog.pg_DB_ROLE_SETTING ORDER BY setdatabase;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgdbrolesetting-0004-select-setdatabase-from-pg_catalog.pg_db_role_setting-order"},
				},
			},
		},
	})
}

func TestPgDefaultAcl(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_default_acl",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_default_acl";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgdefaultacl-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_default_acl";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgdefaultacl-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_default_acl";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgdefaultacl-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT oid FROM PG_catalog.pg_DEFAULT_ACL ORDER BY oid;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgdefaultacl-0004-select-oid-from-pg_catalog.pg_default_acl-order"},
				},
			},
		},
	})
}

func TestPgDepend(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_depend",
			SetUpScript: []string{
				`CREATE SEQUENCE depend_items_id_seq;`,
				`CREATE TABLE depend_items (
					id integer DEFAULT nextval('depend_items_id_seq'),
					label text
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT d.classid::regclass::text, d.refclassid::regclass::text, d.deptype
						FROM "pg_catalog"."pg_depend" d
						JOIN pg_catalog.pg_attrdef ad ON d.classid = 'pg_attrdef'::regclass AND d.objid = ad.oid
						JOIN pg_catalog.pg_class seq ON d.refclassid = 'pg_class'::regclass AND d.refobjid = seq.oid
						JOIN pg_catalog.pg_class tbl ON ad.adrelid = tbl.oid
						WHERE tbl.relname = 'depend_items' AND seq.relname = 'depend_items_id_seq'
						ORDER BY 1,2,3;`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgdepend-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_depend";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgdepend-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_depend";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgdepend-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT d.deptype
						FROM PG_catalog.pg_DEPEND d
						JOIN PG_catalog.pg_ATTRDEF ad ON d.classid = 'pg_attrdef'::regclass AND d.objid = ad.oid
						JOIN PG_catalog.pg_CLASS seq ON d.refclassid = 'pg_class'::regclass AND d.refobjid = seq.oid
						JOIN PG_catalog.pg_CLASS tbl ON ad.adrelid = tbl.oid
						WHERE tbl.relname = 'depend_items' AND seq.relname = 'depend_items_id_seq'
						ORDER BY d.deptype;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgdepend-0004-select-classid-from-pg_catalog.pg_depend-order"},
				},
				{
					Query: `SELECT deps.classid::regclass::text, deps.deptype
						FROM (
							SELECT d.classid, d.objid, d.refclassid, d.refobjid, d.deptype
							FROM pg_catalog.pg_depend d
							WHERE d.deptype != 'p' AND d.deptype != 'e'
							UNION ALL
							SELECT 'pg_opfamily'::pg_catalog.regclass AS classid, amopfamily AS objid, d.refclassid, d.refobjid, d.deptype
							FROM pg_catalog.pg_depend d, pg_catalog.pg_amop o
							WHERE d.deptype NOT IN ('p', 'e', 'i')
								AND d.classid = 'pg_amop'::pg_catalog.regclass
								AND d.objid = o.oid
								AND NOT (d.refclassid = 'pg_opfamily'::pg_catalog.regclass AND o.amopfamily = d.refobjid)
							UNION ALL
							SELECT 'pg_opfamily'::pg_catalog.regclass AS classid, amprocfamily AS objid, d.refclassid, d.refobjid, d.deptype
							FROM pg_catalog.pg_depend d, pg_catalog.pg_amproc p
							WHERE d.deptype NOT IN ('p', 'e', 'i')
								AND d.classid = 'pg_amproc'::pg_catalog.regclass
								AND d.objid = p.oid
								AND NOT (d.refclassid = 'pg_opfamily'::pg_catalog.regclass AND p.amprocfamily = d.refobjid)
						) AS deps(classid, objid, refclassid, refobjid, deptype)
						WHERE deps.classid = 'pg_attrdef'::pg_catalog.regclass
							AND deps.deptype = 'n'
							AND deps.objid = (
								SELECT ad.oid
								FROM pg_catalog.pg_attrdef ad
								JOIN pg_catalog.pg_class tbl ON ad.adrelid = tbl.oid
								WHERE tbl.relname = 'depend_items'
							)
						ORDER BY 1,2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgdepend-0005-select-classid-objid-refclassid-refobjid"},
				},
			},
		},
	})
}

func TestPgDescription(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_description comments",
			SetUpScript: []string{
				`CREATE TABLE pg_description_items (
					id integer PRIMARY KEY,
					note text
				);`,
				`COMMENT ON TABLE pg_description_items IS 'visible table description';`,
				`COMMENT ON COLUMN pg_description_items.note IS 'visible column description';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT d.classoid::regclass::text, c.relname, d.objsubid, d.description
						FROM "pg_catalog"."pg_description" d
						JOIN pg_catalog.pg_class c ON d.objoid = c.oid
						JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
						WHERE c.relname = 'pg_description_items'
							AND n.nspname = current_schema()
						ORDER BY d.objsubid, d.description;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgdescription-0001-select-*-from-pg_catalog-."},
				},
			},
		},
		{
			Name: "pg_description case sensitivity",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "PG_catalog"."pg_description";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgdescription-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_description";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgdescription-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "pg_description mixed-case identifiers",
			SetUpScript: []string{
				`CREATE TABLE pg_description_items (
					id integer PRIMARY KEY,
					note text
				);`,
				`COMMENT ON TABLE pg_description_items IS 'visible table description';`,
				`COMMENT ON COLUMN pg_description_items.note IS 'visible column description';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT d.objsubid
						FROM PG_catalog.pg_DESCRIPTION d
						JOIN PG_catalog.pg_CLASS c ON d.objoid = c.oid
						JOIN PG_catalog.pg_NAMESPACE n ON c.relnamespace = n.oid
						WHERE c.relname = 'pg_description_items'
							AND n.nspname = current_schema()
						ORDER BY d.objsubid;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgdescription-0004-select-objoid-from-pg_catalog.pg_description-order"},
				},
			},
		},
	})
}

func TestPgEnum(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_enum",
			SetUpScript: []string{
				`CREATE TYPE pg_enum_mood AS ENUM ('sad', 'ok', 'happy');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT t.typname, e.enumsortorder, e.enumlabel
						FROM "pg_catalog"."pg_enum" e
						JOIN pg_catalog.pg_type t ON e.enumtypid = t.oid
						JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid
						WHERE t.typname = 'pg_enum_mood'
							AND n.nspname = current_schema()
						ORDER BY e.enumsortorder;`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgenum-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_enum";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgenum-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_enum";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgenum-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT e.enumlabel
						FROM PG_catalog.pg_ENUM e
						JOIN PG_catalog.pg_TYPE t ON e.enumtypid = t.oid
						JOIN PG_catalog.pg_NAMESPACE n ON t.typnamespace = n.oid
						WHERE t.typname = 'pg_enum_mood'
							AND n.nspname = current_schema()
						ORDER BY e.enumlabel;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgenum-0004-select-enumlabel-from-pg_catalog.pg_enum-order"},
				},
			},
		},
	})
}

func TestPgEventTrigger(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_event_trigger",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_event_trigger";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgeventtrigger-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_event_trigger";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgeventtrigger-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_event_trigger";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgeventtrigger-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT evtname FROM PG_catalog.pg_EVENT_TRIGGER ORDER BY evtname;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgeventtrigger-0004-select-evtname-from-pg_catalog.pg_event_trigger-order"},
				},
			},
		},
	})
}

func TestPgExtension(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_extension row",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT e.extname, n.nspname, e.extrelocatable, e.extversion,
							e.extconfig IS NULL, e.extcondition IS NULL
						FROM "pg_catalog"."pg_extension" e
						JOIN pg_catalog.pg_namespace n ON e.extnamespace = n.oid
						WHERE e.extname = 'plpgsql'
						ORDER BY e.extname;`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgextension-0001-select-*-from-pg_catalog-."},
				},
			},
		},
		{
			Name: "pg_extension case sensitivity",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "PG_catalog"."pg_extension";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgextension-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_extension";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgextension-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "pg_extension mixed-case identifiers",
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT extname FROM PG_catalog.pg_EXTENSION WHERE extname = 'plpgsql' ORDER BY extname;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgextension-0004-select-extname-from-pg_catalog.pg_extension-order"},
				},
			},
		},
	})
}

func TestPgFileSettings(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_file_settings",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT name, applied, error IS NULL
						FROM "pg_catalog"."pg_file_settings"
						WHERE name = 'datestyle'
						ORDER BY name;`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgfilesettings-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_file_settings";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgfilesettings-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_file_settings";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgfilesettings-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT name
						FROM PG_catalog.pg_FILE_SETTINGS
						WHERE name = 'datestyle'
						ORDER BY name;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgfilesettings-0004-select-name-from-pg_catalog.pg_file_settings-order"},
				},
			},
		},
	})
}

func TestPgForeignDataWrapper(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_foreign_data_wrapper",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_foreign_data_wrapper";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgforeigndatawrapper-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_foreign_data_wrapper";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgforeigndatawrapper-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_foreign_data_wrapper";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgforeigndatawrapper-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT fdwname FROM PG_catalog.pg_FOREIGN_DATA_WRAPPER ORDER BY fdwname;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgforeigndatawrapper-0004-select-fdwname-from-pg_catalog.pg_foreign_data_wrapper-order"},
				},
			},
		},
	})
}

func TestPgForeignServer(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_foreign_server",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_foreign_server";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgforeignserver-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_foreign_server";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgforeignserver-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_foreign_server";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgforeignserver-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT srvname FROM PG_catalog.pg_FOREIGN_SERVER ORDER BY srvname;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgforeignserver-0004-select-srvname-from-pg_catalog.pg_foreign_server-order"},
				},
			},
		},
	})
}

func TestPgForeignTable(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_foreign_table",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_foreign_table";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgforeigntable-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_foreign_table";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgforeigntable-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_foreign_table";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgforeigntable-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT ftrelid FROM PG_catalog.pg_FOREIGN_TABLE ORDER BY ftrelid;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgforeigntable-0004-select-ftrelid-from-pg_catalog.pg_foreign_table-order"},
				},
			},
		},
	})
}

func TestPgGroup(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_group membership row",
			SetUpScript: []string{
				`CREATE ROLE catalog_group_parent;`,
				`CREATE ROLE catalog_group_member LOGIN;`,
				`GRANT catalog_group_parent TO catalog_group_member;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT groname, pg_get_userbyid(grosysid), pg_get_userbyid(grolist[1])
						FROM "pg_catalog"."pg_group"
						WHERE groname = 'catalog_group_parent';`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpggroup-0001-select-*-from-pg_catalog-."},
				},
			},
		},
		{
			Name: "pg_group case sensitivity",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "PG_catalog"."pg_group";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpggroup-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_group";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpggroup-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "pg_group mixed-case identifiers",
			SetUpScript: []string{
				`CREATE ROLE catalog_group_parent;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT groname FROM PG_catalog.pg_GROUP WHERE groname = 'catalog_group_parent' ORDER BY groname;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpggroup-0004-select-groname-from-pg_catalog.pg_group-order"},
				},
			},
		},
	})
}

func TestPgHbaFileRules(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_hba_file_rules",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT rule_number > 0, file_name IS NOT NULL, line_number > 0,
							type, database, user_name, address IS NOT NULL, auth_method, error IS NULL
						FROM "pg_catalog"."pg_hba_file_rules"
						WHERE type = 'host' AND auth_method = 'trust'
						ORDER BY rule_number
						LIMIT 1;`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpghbafilerules-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_hba_file_rules";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpghbafilerules-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_hba_file_rules";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpghbafilerules-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT auth_method
						FROM PG_catalog.pg_HBA_FILE_RULES
						WHERE type = 'host'
						ORDER BY rule_number
						LIMIT 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpghbafilerules-0004-select-line_number-from-pg_catalog.pg_hba_file_rules-order"},
				},
			},
		},
	})
}

func TestPgIdentFileMappings(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_ident_file_mappings",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_ident_file_mappings";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgidentfilemappings-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_ident_file_mappings";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgidentfilemappings-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_ident_file_mappings";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgidentfilemappings-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT line_number FROM PG_catalog.pg_IDENT_FILE_MAPPINGS ORDER BY line_number;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgidentfilemappings-0004-select-line_number-from-pg_catalog.pg_ident_file_mappings-order"},
				},
			},
		},
	})
}

func TestPgIndex(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_index",
			SetUpScript: []string{
				`CREATE SCHEMA testschema;`,
				`SET search_path TO testschema;`,
				`CREATE TABLE testing (pk INT primary key, v1 INT UNIQUE);`,
				`CREATE TABLE testing2 (pk INT, v1 INT, PRIMARY KEY (pk, v1));`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT c.relname AS index_name,
							t.relname AS table_name,
							i.indnatts,
							i.indnkeyatts,
							i.indisunique,
							i.indnullsnotdistinct,
							i.indisprimary,
							i.indisexclusion,
							i.indimmediate,
							i.indisclustered,
							i.indisvalid,
							i.indcheckxmin,
							i.indisready,
							i.indislive,
							i.indisreplident,
							i.indkey::text,
							i.indcollation::text,
							i.indclass::text,
							i.indoption::text,
							i.indexprs IS NULL AS indexprs_is_null,
							i.indpred IS NULL AS indpred_is_null
						FROM pg_catalog.pg_class c
						JOIN pg_catalog.pg_index i ON c.oid = i.indexrelid
						JOIN pg_catalog.pg_class t ON t.oid = i.indrelid
						JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
						WHERE n.nspname = 'testschema' and left(c.relname, 5) <> 'dolt_'
						ORDER BY c.relname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgindex-0001-select-i.*-from-pg_class-c"},
				},
				{ // Different cases and quoted, so it fails
					Query: `SELECT * FROM "PG_catalog"."pg_index";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgindex-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_index";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgindex-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT i.indexrelid from pg_class c " +
						"JOIN PG_catalog.pg_INDEX i ON c.oid = i.indexrelid " +
						"JOIN pg_namespace n ON c.relnamespace = n.oid " +
						"WHERE n.nspname = 'testschema' and left(c.relname, 5) <> 'dolt_' " +
						"ORDER BY 1;",
					Expected: []sql.Row{{1067629180}, {2070175302}, {3185790121}},
				},
				{
					Query: "SELECT i.indexrelid, i.indrelid, c.relname, t.relname  FROM pg_catalog.pg_index i " +
						"JOIN pg_catalog.pg_class c ON i.indexrelid = c.oid " +
						"JOIN pg_catalog.pg_class t ON i.indrelid = t.oid " +
						"JOIN pg_namespace n ON t.relnamespace = n.oid " +
						"WHERE n.nspname = 'testschema' and left(c.relname, 5) <> 'dolt_'",
					Expected: []sql.Row{
						{1067629180, 3120782595, "testing_pkey", "testing"},
						{2070175302, 3120782595, "testing_v1_key", "testing"},
						{3185790121, 1784425749, "testing2_pkey", "testing2"},
					},
				},
				{
					Query: "SELECT unnest(indoption) FROM pg_index LIMIT 1;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgindex-0006-select-unnest-indoption-from-pg_index"},
				},
				{
					Query: `SELECT c.relname, a.attname, array_position(i.indkey, a.attnum::int2)
						FROM pg_catalog.pg_class c
						JOIN pg_catalog.pg_index i ON i.indrelid = c.oid
						JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
						WHERE c.relname = 'testing2' AND i.indisprimary AND a.attname IN ('pk', 'v1')
						ORDER BY a.attnum;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgindex-0007-select-c.relname-a.attname-array_position-i.indkey"},
				},
			},
		},
	})
}

func TestPgIndexes(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_indexes",
			SetUpScript: []string{
				"CREATE SCHEMA testschema;",
				"SET search_path TO testschema;",
				`CREATE TABLE testing (pk INT primary key, v1 INT UNIQUE);`,
				`CREATE TABLE testing2 (pk INT, v1 INT, PRIMARY KEY (pk, v1));`,
				"CREATE INDEX my_index ON testing2(v1);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_indexes" where schemaname = 'testschema';`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgindexes-0001-select-*-from-pg_catalog-."},
				},
				{ // Different cases and quoted, so it fails
					Query: `SELECT * FROM "PG_catalog"."pg_indexes";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgindexes-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_indexes";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgindexes-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT indexname FROM PG_catalog.pg_INDEXES where schemaname='testschema' ORDER BY indexname;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgindexes-0004-select-indexname-from-pg_catalog.pg_indexes-where"},
				},
				{
					// Workload pattern: ORM/migration tools test for index
					// existence with EXISTS before issuing conditional DDL.
					Query: `SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_indexes
                                        WHERE schemaname = 'testschema' AND indexname = 'my_index');`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgindexes-0005-select-exists-select-1-from"},
				},
				{
					// And the same pattern returning false for a missing index.
					Query: `SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_indexes
                                        WHERE schemaname = 'testschema' AND indexname = 'not_yet');`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgindexes-0006-select-exists-select-1-from"},
				},
				{
					// drizzle-kit-style: filter to a specific table and read indexdef.
					Query: `SELECT indexname, indexdef FROM pg_catalog.pg_indexes
                            WHERE schemaname = 'testschema' AND tablename = 'testing'
                            ORDER BY indexname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgindexes-0007-select-indexname-indexdef-from-pg_catalog.pg_indexes"},
				},
				{
					// Simulate the conditional DDL pattern: only issue
					// CREATE INDEX when pg_indexes does not list it yet.
					// The view is the source of truth that the migration
					// tool consults; the second invocation is a no-op
					// because the index now exists.
					Query: `SELECT count(*) FROM pg_catalog.pg_indexes
                            WHERE schemaname = 'testschema' AND indexname = 'my_index';`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgindexes-0008-select-count-*-from-pg_catalog.pg_indexes"},
				},
				{
					// Coverage of CREATE INDEX IF NOT EXISTS. The first
					// invocation was already in setup; the second must
					// be a no-op even though the index already exists.
					Query: `CREATE INDEX IF NOT EXISTS my_index ON testschema.testing2(v1);`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgindexes-0009-create-index-if-not-exists"},
				},
				{
					// After the no-op CREATE INDEX IF NOT EXISTS the view
					// should still report exactly one matching index.
					Query: `SELECT count(*) FROM pg_catalog.pg_indexes
                            WHERE schemaname = 'testschema' AND indexname = 'my_index';`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgindexes-0010-select-count-*-from-pg_catalog.pg_indexes"},
				},
				{
					// Joined query against pg_class — admin tools commonly
					// stitch pg_indexes with pg_class/pg_namespace to derive
					// owner/oid information.
					Query: `SELECT idx.indexname, c.relname
                            FROM pg_catalog.pg_indexes idx
                            JOIN pg_catalog.pg_class c ON c.relname = idx.indexname
                            WHERE idx.schemaname = 'testschema'
                              AND idx.indexname = 'my_index';`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgindexes-0011-select-idx.indexname-c.relname-from-pg_catalog.pg_indexes"},
				},
			},
		},
	})
}

func TestPgInherits(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_inherits",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_inherits";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpginherits-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_inherits";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpginherits-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_inherits";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpginherits-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT inhrelid FROM PG_catalog.pg_INHERITS ORDER BY inhrelid;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpginherits-0004-select-inhrelid-from-pg_catalog.pg_inherits-order"},
				},
			},
		},
	})
}

func TestPgInitPrivs(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_init_privs",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT count(*) > 0 FROM "pg_catalog"."pg_init_privs";`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpginitprivs-0001-select-count-*->-0"},
				},
				{ // Different cases and quoted, so it fails
					Query: `SELECT * FROM "PG_catalog"."pg_init_privs";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpginitprivs-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_init_privs";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpginitprivs-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT count(*) > 0 FROM PG_catalog.pg_INIT_PRIVS;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpginitprivs-0004-select-count-*->-0"},
				},
			},
		},
	})
}

func TestPgLanguage(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_language",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT lanname, lanispl, lanpltrusted,
							lanplcallfoid <> 0, laninline <> 0, lanvalidator <> 0
						FROM "pg_catalog"."pg_language"
						WHERE lanname IN ('c', 'internal', 'plpgsql', 'sql')
						ORDER BY lanname;`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpglanguage-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_language";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpglanguage-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_language";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpglanguage-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT lanname FROM PG_catalog.pg_LANGUAGE ORDER BY lanname;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpglanguage-0004-select-lanname-from-pg_catalog.pg_language-order"},
				},
			},
		},
	})
}

func TestPgLargeobject(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_largeobject row",
			SetUpScript: []string{
				`SELECT lo_create(424270);`,
				`SELECT lo_put(424270, 0, decode('0102', 'hex'));`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT loid::text, pageno, data
						FROM "pg_catalog"."pg_largeobject"
						WHERE loid = 424270
						ORDER BY pageno;`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpglargeobject-0001-select-*-from-pg_catalog-.", ColumnModes: []string{"structural", "structural", "bytea"}},
				},
			},
		},
		{
			Name: "pg_largeobject case sensitivity",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "PG_catalog"."pg_largeobject";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpglargeobject-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_largeobject";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpglargeobject-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "pg_largeobject mixed-case lookup",
			SetUpScript: []string{
				`SELECT lo_create(424272);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT loid::text FROM PG_catalog.pg_LARGEOBJECT WHERE loid = 424272 ORDER BY loid;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpglargeobject-0004-select-loid-from-pg_catalog.pg_largeobject-order"},
				},
			},
		},
	})
}

func TestPgLargeobjectMetadata(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_largeobject_metadata row",
			SetUpScript: []string{
				`SELECT lo_create(424271);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT oid::text, lomacl IS NULL
						FROM "pg_catalog"."pg_largeobject_metadata"
						WHERE oid = 424271
						ORDER BY oid;`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpglargeobjectmetadata-0001-select-*-from-pg_catalog-."},
				},
			},
		},
		{
			Name: "pg_largeobject_metadata case sensitivity",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "PG_catalog"."pg_largeobject_metadata";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpglargeobjectmetadata-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_largeobject_metadata";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpglargeobjectmetadata-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "pg_largeobject_metadata mixed-case lookup",
			SetUpScript: []string{
				`SELECT lo_create(424273);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT oid::text FROM PG_catalog.pg_LARGEOBJECT_METADATA WHERE oid = 424273 ORDER BY oid;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpglargeobjectmetadata-0004-select-oid-from-pg_catalog.pg_largeobject_metadata-order"},
				},
			},
		},
	})
}

func TestPgLocks(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_locks relation row",
			SetUpScript: []string{
				`CREATE TABLE pg_locks_probe (id INT PRIMARY KEY);`,
				`BEGIN;`,
				`LOCK TABLE pg_locks_probe IN ACCESS SHARE MODE;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT locktype, relation = 'pg_locks_probe'::regclass,
							mode, granted, waitstart IS NULL
						FROM "pg_catalog"."pg_locks"
						WHERE locktype = 'relation'
							AND relation = 'pg_locks_probe'::regclass
						ORDER BY mode;`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpglocks-0001-select-*-from-pg_catalog-."},
				},
			},
		},
		{
			Name: "pg_locks case sensitivity",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "PG_catalog"."pg_locks";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpglocks-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_locks";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpglocks-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "pg_locks mixed-case lookup",
			SetUpScript: []string{
				`CREATE TABLE pg_locks_mixed_probe (id INT PRIMARY KEY);`,
				`BEGIN;`,
				`LOCK TABLE pg_locks_mixed_probe IN ACCESS SHARE MODE;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT mode FROM PG_catalog.pg_LOCKS WHERE relation = 'pg_locks_mixed_probe'::regclass ORDER BY mode;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpglocks-0004-select-objid-from-pg_catalog.pg_locks-order"},
				},
			},
		},
	})
}

func TestPgMatviews(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_matviews row",
			SetUpScript: []string{
				`CREATE SCHEMA matviews_oracle_schema;`,
				`SET search_path TO matviews_oracle_schema;`,
				`CREATE MATERIALIZED VIEW pg_matviews_probe_reader AS SELECT 1 AS id;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT schemaname,
							matviewname,
							matviewowner IS NOT NULL,
							tablespace IS NULL,
							hasindexes,
							ispopulated,
							definition LIKE '%1 AS id%'
						FROM "pg_catalog"."pg_matviews"
						WHERE schemaname = 'matviews_oracle_schema'
							AND matviewname = 'pg_matviews_probe_reader'
						ORDER BY matviewname;`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgmatviews-0001-select-*-from-pg_catalog-.", ColumnModes: []string{"schema", "structural", "structural", "structural", "structural",

						// Different cases and quoted, so it fails
						"structural", "schema"}},
				},
			},
		},
		{
			Name: "pg_matviews case sensitivity",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "PG_catalog"."pg_matviews";`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgmatviews-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_matviews";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgmatviews-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "pg_matviews mixed-case lookup",
			SetUpScript: []string{
				`CREATE SCHEMA matviews_mixed_schema;`,
				`SET search_path TO matviews_mixed_schema;`,
				`CREATE MATERIALIZED VIEW pg_matviews_mixed_reader AS SELECT 1 AS id;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT matviewname FROM PG_catalog.pg_MATVIEWS WHERE schemaname = 'matviews_mixed_schema' ORDER BY matviewname;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgmatviews-0004-select-matviewname-from-pg_catalog.pg_matviews-order"},
				},
			},
		},
	})
}

func TestPgNamespace(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_namespace builtin rows",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT nspname,
							nspowner <> 0,
							oid <> 0
						FROM "pg_catalog"."pg_namespace"
						WHERE nspname IN ('information_schema', 'pg_catalog', 'public')
						ORDER BY nspname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgnamespace-0001-select-*-from-pg_catalog-.", ColumnModes: []string{"schema", "structural", "structural"}},
				},
			},
		},
		{
			Name: "pg_namespace case sensitivity",
			Assertions: []ScriptTestAssertion{
				{ // Different cases and quoted, so it fails
					Query: `SELECT * FROM "PG_catalog"."pg_namespace";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgnamespace-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_namespace";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgnamespace-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "pg_namespace mixed-case lookup",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT nspname
						FROM PG_catalog.pg_NAMESPACE
						WHERE nspname IN ('information_schema', 'pg_catalog', 'public')
						ORDER BY nspname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgnamespace-0004-select-nspname-from-pg_catalog.pg_namespace-order", ColumnModes: []string{"schema"}},
				},
			},
		},
		{
			Name: "pg_namespace created schema",
			Assertions: []ScriptTestAssertion{
				{
					Query: "CREATE SCHEMA namespace_oracle_schema;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgnamespace-0005-create-schema-testschema"},
				},
				{
					Query: `SELECT nspname,
							nspowner <> 0,
							oid <> 0
						FROM "pg_catalog"."pg_namespace"
						WHERE nspname = 'namespace_oracle_schema'
						ORDER BY nspname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgnamespace-0006-select-*-from-pg_catalog-.", ColumnModes: []string{"schema", "structural", "structural"}},
				},
				// Test index lookups - first let's see what the actual OID values are
				{
					Query: `SELECT oid <> 0,
							nspname
						FROM "pg_catalog"."pg_namespace"
						WHERE nspname = 'namespace_oracle_schema'
						ORDER BY nspname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgnamespace-0007-select-oid-nspname-from-pg_catalog", ColumnModes: []string{"structural", "schema"}},
				},
			},
		},
		{
			Name: "pg_namespace index lookups",
			Assertions: []ScriptTestAssertion{
				// Test simple index lookups
				{
					Query: `SELECT nspname FROM "pg_catalog"."pg_namespace" WHERE oid = 11;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgnamespace-0008-select-nspname-from-pg_catalog-."},
				},
				{
					Query: `SELECT oid FROM "pg_catalog"."pg_namespace" WHERE nspname = 'public';`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgnamespace-0009-select-oid-from-pg_catalog-."},
				},
			},
		},
	})
}

func TestPgNamespaceIndexLookups(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_namespace_index_lookups",
			Assertions: []ScriptTestAssertion{
				{
					Query: "CREATE SCHEMA testschema1;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgnamespaceindexlookups-0001-create-schema-testschema1"},
				},
				{
					Query: "CREATE SCHEMA testschema2;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgnamespaceindexlookups-0002-create-schema-testschema2"},
				},
				{
					Query: "CREATE SCHEMA testschema3;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgnamespaceindexlookups-0003-create-schema-testschema3"},
				},
				{
					Query: "CREATE SCHEMA z_schema;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgnamespaceindexlookups-0004-create-schema-z_schema"},
				},
				{
					Query: "CREATE SCHEMA a_schema;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgnamespaceindexlookups-0005-create-schema-a_schema"},
				},

				{
					Query: `SELECT nspname FROM "pg_catalog"."pg_namespace" WHERE oid = 11;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgnamespaceindexlookups-0006-select-nspname-from-pg_catalog-."},
				},
				{
					Query: `SELECT nspname FROM "pg_catalog"."pg_namespace" WHERE oid = 99;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgnamespaceindexlookups-0007-select-nspname-from-pg_catalog-."},
				},
				{
					Query: `SELECT nspname FROM "pg_catalog"."pg_namespace" WHERE oid = 2200;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgnamespaceindexlookups-0008-select-nspname-from-pg_catalog-."},
				},
				{
					Query: `SELECT nspname
						FROM "pg_catalog"."pg_namespace"
						WHERE oid = (SELECT oid FROM "pg_catalog"."pg_namespace" WHERE nspname = 'information_schema')
						ORDER BY nspname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgnamespaceindexlookups-0009-explain-select-nspname-from-pg_namespace", ColumnModes: []string{"schema"}},
				},

				{
					Query: `SELECT oid, nspname FROM "pg_catalog"."pg_namespace" WHERE oid >= 11 AND oid <= 2200 ORDER BY oid;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgnamespaceindexlookups-0010-select-oid-nspname-from-pg_catalog"},
				},
				{
					Query: `SELECT nspname FROM "pg_catalog"."pg_namespace" WHERE oid > 10000 AND nspname = 'information_schema' ORDER BY nspname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgnamespaceindexlookups-0011-select-oid-nspname-from-pg_catalog", ColumnModes: []string{"schema"}},
				},
				{
					Query: `SELECT nspname FROM "pg_catalog"."pg_namespace" WHERE oid = 999999;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgnamespaceindexlookups-0012-explain-select-oid-nspname-from"},
				},
				{
					Query: `SELECT nspname FROM "pg_catalog"."pg_namespace" WHERE oid < 5;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgnamespaceindexlookups-0013-select-oid-nspname-from-pg_catalog"},
				},

				{
					Query: `SELECT oid FROM "pg_catalog"."pg_namespace" WHERE nspname = 'public';`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgnamespaceindexlookups-0014-select-oid-nspname-from-pg_catalog"},
				},
				{
					Query: `SELECT oid FROM "pg_catalog"."pg_namespace" WHERE nspname = 'pg_catalog';`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgnamespaceindexlookups-0015-select-oid-nspname-from-pg_catalog"},
				},
				{
					Query: `SELECT oid FROM "pg_catalog"."pg_namespace" WHERE nspname = 'pg_toast';`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgnamespaceindexlookups-0016-select-oid-nspname-from-pg_catalog"},
				},
				{
					Query: `SELECT oid <> 0 FROM "pg_catalog"."pg_namespace" WHERE nspname = 'information_schema';`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgnamespaceindexlookups-0017-select-nspname-from-pg_catalog-.", ColumnModes: []string{"structural"}},
				},

				{
					Query: `SELECT nspname
						FROM "pg_catalog"."pg_namespace"
						WHERE nspname IN ('a_schema', 'information_schema', 'pg_catalog', 'pg_toast', 'public', 'testschema1', 'testschema2', 'testschema3', 'z_schema')
							AND nspname >= 'a' AND nspname < 'p'
						ORDER BY nspname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgnamespaceindexlookups-0018-select-nspname-from-pg_catalog-.", ColumnModes: []string{"schema"}},
				},
				{
					Query: `SELECT nspname
						FROM "pg_catalog"."pg_namespace"
						WHERE nspname IN ('a_schema', 'information_schema', 'pg_catalog', 'pg_toast', 'public', 'testschema1', 'testschema2', 'testschema3', 'z_schema')
							AND nspname > 'a' AND nspname <= 'public'
						ORDER BY nspname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgnamespaceindexlookups-0019-select-oid-from-pg_catalog-.", ColumnModes: []string{"schema"}},
				},
				{
					Query: `SELECT nspname
						FROM "pg_catalog"."pg_namespace"
						WHERE nspname IN ('pg_catalog', 'pg_toast', 'public', 'testschema1', 'testschema2', 'testschema3')
							AND nspname BETWEEN 'p' AND 't'
						ORDER BY nspname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgnamespaceindexlookups-0020-select-oid-from-pg_catalog-.", ColumnModes: []string{"schema"}},
				},
				{
					Query: `SELECT nspname
						FROM "pg_catalog"."pg_namespace"
						WHERE nspname IN ('pg_catalog', 'pg_toast', 'public', 'testschema1', 'testschema2', 'testschema3', 'z_schema')
							AND nspname > 'p'
						ORDER BY nspname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgnamespaceindexlookups-0021-explain-select-oid-from-pg_catalog", ColumnModes: []string{"schema"}},
				},
				{
					Query: `SELECT oid <> 0, nspname
						FROM "pg_catalog"."pg_namespace"
						WHERE nspname >= 'test' AND nspname IN ('testschema1', 'testschema2', 'testschema3')
						ORDER BY nspname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgnamespaceindexlookups-0022-select-oid-from-pg_catalog-.", ColumnModes: []string{"structural", "schema"}},
				},
				{
					Query: `SELECT nspname
						FROM "pg_catalog"."pg_namespace"
						WHERE nspname IN ('a_schema', 'information_schema', 'pg_catalog', 'pg_toast', 'public')
							AND nspname < 'p'
						ORDER BY nspname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgnamespaceindexlookups-0023-select-nspname-from-pg_catalog-.", ColumnModes: []string{"schema"}},
				},
				{
					Query: `SELECT nspname
						FROM "pg_catalog"."pg_namespace"
						WHERE nspname IN ('a_schema', 'information_schema', 'pg_catalog', 'pg_toast', 'public')
							AND nspname <= 'information_schema'
						ORDER BY nspname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgnamespaceindexlookups-0024-select-nspname-from-pg_catalog-.", ColumnModes: []string{"schema"}},
				},

				{
					Query: `SELECT oid FROM "pg_catalog"."pg_namespace" WHERE nspname = 'nonexistent';`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgnamespaceindexlookups-0025-select-nspname-from-pg_catalog-."},
				},
				{
					Query: `SELECT oid FROM "pg_catalog"."pg_namespace" WHERE nspname > 'zzz';`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgnamespaceindexlookups-0026-select-nspname-from-pg_catalog-."},
				},
				{
					Query: `SELECT oid FROM "pg_catalog"."pg_namespace" WHERE nspname < 'a';`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgnamespaceindexlookups-0027-select-nspname-from-pg_catalog-."},
				},
				{
					Query: `SELECT oid FROM "pg_catalog"."pg_namespace" WHERE nspname = 'PUBLIC';`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgnamespaceindexlookups-0028-select-nspname-from-pg_catalog-."},
				},
				{
					Query: `SELECT oid FROM "pg_catalog"."pg_namespace" WHERE nspname = 'Public';`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgnamespaceindexlookups-0029-select-nspname-from-pg_catalog-.", ColumnModes: []string{"schema"}},
				},
				{
					Query: `SELECT oid <> 0, nspname
						FROM "pg_catalog"."pg_namespace"
						WHERE oid >= 11
							AND nspname >= 'p'
							AND nspname IN ('pg_catalog', 'pg_toast', 'public', 'testschema1', 'testschema2', 'testschema3', 'z_schema')
						ORDER BY nspname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgnamespaceindexlookups-0030-select-oid-from-pg_catalog-.", ColumnModes: []string{"structural", "schema"}},
				},
				{
					Query: `SELECT oid <> 0, nspname
						FROM "pg_catalog"."pg_namespace"
						WHERE oid < 20000
							AND nspname < 't'
							AND nspname IN ('information_schema', 'pg_catalog', 'pg_toast', 'public')
						ORDER BY nspname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgnamespaceindexlookups-0031-select-oid-from-pg_catalog-.", ColumnModes: []string{"structural", "schema"}},
				},
				{
					Query: `SELECT oid FROM "pg_catalog"."pg_namespace" WHERE nspname = '';`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgnamespaceindexlookups-0032-select-oid-from-pg_catalog-."},
				},
				{
					Query: `SELECT nspname
						FROM "pg_catalog"."pg_namespace"
						WHERE nspname >= ''
							AND nspname IN ('a_schema', 'information_schema', 'pg_catalog', 'pg_toast', 'public', 'testschema1', 'testschema2', 'testschema3', 'z_schema')
						ORDER BY nspname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgnamespaceindexlookups-0033-select-oid-from-pg_catalog-."},
				},
				{
					Query: `SELECT oid <> 0, nspname
						FROM "pg_catalog"."pg_namespace"
						WHERE nspname >= 't' AND nspname IN ('testschema1', 'testschema2', 'testschema3', 'z_schema')
						ORDER BY nspname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgnamespaceindexlookups-0034-select-oid-from-pg_catalog-.", ColumnModes: []string{"structural", "schema"}},
				},
				{
					Query: `SELECT DISTINCT nspname
						FROM "pg_catalog"."pg_namespace"
						WHERE oid > 10000
							AND nspname IN ('information_schema', 'testschema1', 'testschema2', 'testschema3', 'z_schema')
						ORDER BY nspname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgnamespaceindexlookups-0035-select-oid-nspname-from-pg_catalog", ColumnModes: []string{"schema"}},
				},
				{
					Query: `SELECT nspname FROM "pg_catalog"."pg_namespace" WHERE oid IN (11, 99, 2200) ORDER BY nspname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgnamespaceindexlookups-0036-select-oid-nspname-from-pg_catalog", ColumnModes: []string{"schema"}},
				},
				{
					Query: `SELECT nspname FROM "pg_catalog"."pg_namespace" WHERE nspname IN ('public', 'pg_catalog', 'pg_toast') ORDER BY nspname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgnamespaceindexlookups-0037-select-oid-from-pg_catalog-."},
				},
				{
					Query: `SELECT nspname
						FROM "pg_catalog"."pg_namespace"
						WHERE oid != 11
							AND nspname IN ('information_schema', 'pg_catalog', 'pg_toast', 'public')
						ORDER BY nspname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgnamespaceindexlookups-0038-select-nspname-from-pg_catalog-.", ColumnModes: []string{"schema"}},
				},
				{
					Query: `SELECT nspname
						FROM "pg_catalog"."pg_namespace"
						WHERE nspname != 'public'
							AND nspname IN ('information_schema', 'pg_catalog', 'pg_toast', 'public')
						ORDER BY nspname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgnamespaceindexlookups-0039-select-nspname-from-pg_catalog-.", ColumnModes: []string{"schema"}},
				},
			},
		},
	})
}

func TestPgOpclass(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_opclass",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT opc.opcname, opf.opfname, typ.typname, opc.opcdefault, opc.opckeytype
FROM "pg_catalog"."pg_opclass" opc
JOIN "pg_catalog"."pg_opfamily" opf ON opf.oid = opc.opcfamily
JOIN "pg_catalog"."pg_am" am ON am.oid = opc.opcmethod
JOIN "pg_catalog"."pg_type" typ ON typ.oid = opc.opcintype
WHERE am.amname = 'btree'
	AND opc.opcname IN ('name_ops', 'text_ops', 'varchar_ops')
ORDER BY opc.opcname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgopclass-0001-select-opc.opcname-opf.opfname-typ.typname-opc.opcdefault"},
				},
				{
					Query: `SELECT opc.opcname, opf.opfname, typ.typname, opc.opcdefault, opc.opckeytype
FROM "pg_catalog"."pg_opclass" opc
JOIN "pg_catalog"."pg_opfamily" opf ON opf.oid = opc.opcfamily
JOIN "pg_catalog"."pg_am" am ON am.oid = opc.opcmethod
JOIN "pg_catalog"."pg_type" typ ON typ.oid = opc.opcintype
WHERE am.amname = 'btree'
	AND opc.opcname IN ('text_pattern_ops', 'varchar_pattern_ops', 'bpchar_pattern_ops')
ORDER BY opc.opcname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgopclass-0002-select-opc.opcname-opf.opfname-typ.typname-opc.opcdefault"},
				},
				{
					Query: `SELECT opc.opcname, opf.opfname, typ.typname, opc.opcdefault, opc.opckeytype
FROM "pg_catalog"."pg_opclass" opc
JOIN "pg_catalog"."pg_opfamily" opf ON opf.oid = opc.opcfamily
JOIN "pg_catalog"."pg_am" am ON am.oid = opc.opcmethod
JOIN "pg_catalog"."pg_type" typ ON typ.oid = opc.opcintype
WHERE am.amname = 'btree'
	AND opc.opcname IN ('bytea_ops', 'interval_ops', 'oid_ops', 'time_ops', 'timetz_ops')
ORDER BY opc.opcname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgopclass-0003-select-opc.opcname-opf.opfname-typ.typname-opc.opcdefault"},
				},
				{
					Query: `SELECT opc.opcname, opf.opfname, typ.typname, opc.opcdefault, opc.opckeytype
FROM "pg_catalog"."pg_opclass" opc
JOIN "pg_catalog"."pg_opfamily" opf ON opf.oid = opc.opcfamily
JOIN "pg_catalog"."pg_am" am ON am.oid = opc.opcmethod
JOIN "pg_catalog"."pg_type" typ ON typ.oid = opc.opcintype
WHERE am.amname = 'btree'
	AND opc.opcname IN ('bit_ops', 'char_ops', 'oidvector_ops', 'pg_lsn_ops', 'varbit_ops')
ORDER BY opc.opcname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgopclass-0004-select-opc.opcname-opf.opfname-typ.typname-opc.opcdefault"},
				},
				{
					Query: `SELECT opc.opcname, am.amname, opc.opcdefault, inputtyp.typname, COALESCE(keytyp.typname, ''), opc.opckeytype
FROM "pg_catalog"."pg_opclass" opc
JOIN "pg_catalog"."pg_am" am ON am.oid = opc.opcmethod
JOIN "pg_catalog"."pg_type" inputtyp ON inputtyp.oid = opc.opcintype
LEFT JOIN "pg_catalog"."pg_type" keytyp ON keytyp.oid = opc.opckeytype
WHERE opc.opcname LIKE 'jsonb_%'
ORDER BY opc.opcname, am.amname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgopclass-0005-select-opc.opcname-am.amname-opc.opcdefault-inputtyp.typname"},
				},
				{ // Different cases and quoted, so it fails
					Query: `SELECT * FROM "PG_catalog"."pg_opclass";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgopclass-0006-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_opclass";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgopclass-0007-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT opc.opcname
FROM PG_catalog.pg_OPCLASS opc
JOIN pg_catalog.pg_am am ON am.oid = opc.opcmethod
WHERE am.amname = 'btree'
	AND opc.opcname IN ('name_ops', 'text_ops', 'varchar_ops')
ORDER BY opc.opcname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgopclass-0008-select-opcname-from-pg_catalog.pg_opclass-order"},
				},
			},
		},
	})
}

func TestPgOperator(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_operator",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT o.oprname, lt.typname, rt.typname
FROM "pg_catalog"."pg_operator" o
JOIN "pg_catalog"."pg_type" lt ON lt.oid = o.oprleft
JOIN "pg_catalog"."pg_type" rt ON rt.oid = o.oprright
WHERE lt.typname = 'int4'
	AND rt.typname = 'int4'
	AND o.oprname IN ('<', '<=', '=', '>=', '>')
ORDER BY CASE o.oprname
	WHEN '<' THEN 1
	WHEN '<=' THEN 2
	WHEN '=' THEN 3
	WHEN '>=' THEN 4
	WHEN '>' THEN 5
END;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgoperator-0001-select-o.oprname-lt.typname-rt.typname-from"},
				},
				{
					Query: `SELECT o.oprname, lt.typname, rt.typname
FROM "pg_catalog"."pg_operator" o
JOIN "pg_catalog"."pg_type" lt ON lt.oid = o.oprleft
JOIN "pg_catalog"."pg_type" rt ON rt.oid = o.oprright
WHERE lt.typname = 'jsonb'
	AND (
		(o.oprname IN ('@>', '<@') AND rt.typname = 'jsonb')
		OR (o.oprname = '?' AND rt.typname = 'text')
		OR (o.oprname IN ('?|', '?&') AND rt.typname = '_text')
	)
ORDER BY CASE o.oprname
	WHEN '@>' THEN 1
	WHEN '<@' THEN 2
	WHEN '?' THEN 3
	WHEN '?|' THEN 4
	WHEN '?&' THEN 5
END;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgoperator-0002-select-o.oprname-lt.typname-rt.typname-from"},
				},
				{
					Query: `SELECT lt.typname, COUNT(*)
FROM "pg_catalog"."pg_operator" o
JOIN "pg_catalog"."pg_type" lt ON lt.oid = o.oprleft
JOIN "pg_catalog"."pg_type" rt ON rt.oid = o.oprright
WHERE lt.oid = rt.oid
	AND o.oprname IN ('<', '<=', '=', '>=', '>')
	AND lt.typname IN ('bit', 'bool', 'bpchar', 'bytea', 'char', 'date', 'float4', 'float8', 'int2', 'int4', 'int8', 'interval', 'name', 'numeric', 'oid', 'oidvector', 'pg_lsn', 'text', 'time', 'timestamp', 'timestamptz', 'timetz', 'uuid', 'varbit')
GROUP BY lt.typname
ORDER BY lt.typname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgoperator-0003-select-lt.typname-count-*-from"},
				},
				{
					Query: `SELECT lt.typname, rt.typname, COUNT(*)
FROM "pg_catalog"."pg_operator" o
JOIN "pg_catalog"."pg_type" lt ON lt.oid = o.oprleft
JOIN "pg_catalog"."pg_type" rt ON rt.oid = o.oprright
WHERE lt.typname IN ('name', 'text', 'varchar')
	AND rt.typname IN ('name', 'text', 'varchar')
	AND o.oprname IN ('<', '<=', '=', '>=', '>')
GROUP BY lt.typname, rt.typname
ORDER BY lt.typname, rt.typname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgoperator-0004-select-lt.typname-rt.typname-count-*"},
				},
				{
					Query: `SELECT lt.typname, rt.typname, COUNT(*)
FROM "pg_catalog"."pg_operator" o
JOIN "pg_catalog"."pg_type" lt ON lt.oid = o.oprleft
JOIN "pg_catalog"."pg_type" rt ON rt.oid = o.oprright
WHERE lt.typname IN ('date', 'timestamp', 'timestamptz')
	AND rt.typname IN ('date', 'timestamp', 'timestamptz')
	AND lt.oid <> rt.oid
	AND o.oprname IN ('<', '<=', '=', '>=', '>')
GROUP BY lt.typname, rt.typname
ORDER BY lt.typname, rt.typname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgoperator-0005-select-lt.typname-rt.typname-count-*"},
				},
				{
					Query: `SELECT o.oprname, com.oprname, clt.typname, crt.typname
FROM "pg_catalog"."pg_operator" o
JOIN "pg_catalog"."pg_type" lt ON lt.oid = o.oprleft
JOIN "pg_catalog"."pg_type" rt ON rt.oid = o.oprright
JOIN "pg_catalog"."pg_operator" com ON com.oid = o.oprcom
JOIN "pg_catalog"."pg_type" clt ON clt.oid = com.oprleft
JOIN "pg_catalog"."pg_type" crt ON crt.oid = com.oprright
WHERE lt.typname = 'date'
	AND rt.typname = 'timestamptz'
	AND o.oprname IN ('<', '<=', '=', '>=', '>')
ORDER BY CASE o.oprname
	WHEN '<' THEN 1
	WHEN '<=' THEN 2
	WHEN '=' THEN 3
	WHEN '>=' THEN 4
	WHEN '>' THEN 5
END;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgoperator-0006-select-o.oprname-com.oprname-clt.typname-crt.typname"},
				},
				{
					Query: `SELECT lt.typname, rt.typname, COUNT(*)
FROM "pg_catalog"."pg_operator" o
JOIN "pg_catalog"."pg_type" lt ON lt.oid = o.oprleft
JOIN "pg_catalog"."pg_type" rt ON rt.oid = o.oprright
WHERE lt.typname IN ('int2', 'int4', 'int8')
	AND rt.typname IN ('int2', 'int4', 'int8')
	AND lt.oid <> rt.oid
	AND o.oprname IN ('<', '<=', '=', '>=', '>')
GROUP BY lt.typname, rt.typname
ORDER BY lt.typname, rt.typname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgoperator-0007-select-lt.typname-rt.typname-count-*"},
				},
				{
					Query: `SELECT o.oprname, com.oprname, clt.typname, crt.typname
FROM "pg_catalog"."pg_operator" o
JOIN "pg_catalog"."pg_type" lt ON lt.oid = o.oprleft
JOIN "pg_catalog"."pg_type" rt ON rt.oid = o.oprright
JOIN "pg_catalog"."pg_operator" com ON com.oid = o.oprcom
JOIN "pg_catalog"."pg_type" clt ON clt.oid = com.oprleft
JOIN "pg_catalog"."pg_type" crt ON crt.oid = com.oprright
WHERE lt.typname = 'int2'
	AND rt.typname = 'int8'
	AND o.oprname IN ('<', '<=', '=', '>=', '>')
ORDER BY CASE o.oprname
	WHEN '<' THEN 1
	WHEN '<=' THEN 2
	WHEN '=' THEN 3
	WHEN '>=' THEN 4
	WHEN '>' THEN 5
END;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgoperator-0008-select-o.oprname-com.oprname-clt.typname-crt.typname"},
				},
				{
					Query: `SELECT lt.typname, rt.typname, COUNT(*)
FROM "pg_catalog"."pg_operator" o
JOIN "pg_catalog"."pg_type" lt ON lt.oid = o.oprleft
JOIN "pg_catalog"."pg_type" rt ON rt.oid = o.oprright
WHERE lt.typname IN ('float4', 'float8')
	AND rt.typname IN ('float4', 'float8')
	AND lt.oid <> rt.oid
	AND o.oprname IN ('<', '<=', '=', '>=', '>')
GROUP BY lt.typname, rt.typname
ORDER BY lt.typname, rt.typname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgoperator-0009-select-lt.typname-rt.typname-count-*"},
				},
				{
					Query: `SELECT o.oprname, com.oprname, clt.typname, crt.typname
FROM "pg_catalog"."pg_operator" o
JOIN "pg_catalog"."pg_type" lt ON lt.oid = o.oprleft
JOIN "pg_catalog"."pg_type" rt ON rt.oid = o.oprright
JOIN "pg_catalog"."pg_operator" com ON com.oid = o.oprcom
JOIN "pg_catalog"."pg_type" clt ON clt.oid = com.oprleft
JOIN "pg_catalog"."pg_type" crt ON crt.oid = com.oprright
WHERE lt.typname = 'float4'
	AND rt.typname = 'float8'
	AND o.oprname IN ('<', '<=', '=', '>=', '>')
ORDER BY CASE o.oprname
	WHEN '<' THEN 1
	WHEN '<=' THEN 2
	WHEN '=' THEN 3
	WHEN '>=' THEN 4
	WHEN '>' THEN 5
END;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgoperator-0010-select-o.oprname-com.oprname-clt.typname-crt.typname"},
				},
				{
					Query: `SELECT o.oprname, lt.typname, rt.typname
FROM "pg_catalog"."pg_operator" o
JOIN "pg_catalog"."pg_type" lt ON lt.oid = o.oprleft
JOIN "pg_catalog"."pg_type" rt ON rt.oid = o.oprright
WHERE o.oprname IN ('~<~', '~<=~', '~>=~', '~>~')
	AND lt.typname IN ('bpchar', 'text')
ORDER BY lt.typname, CASE o.oprname
	WHEN '~<~' THEN 1
	WHEN '~<=~' THEN 2
	WHEN '~>=~' THEN 3
	WHEN '~>~' THEN 4
END;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgoperator-0011-select-o.oprname-lt.typname-rt.typname-from"},
				},
				{ // Different cases and quoted, so it fails
					Query: `SELECT * FROM "PG_catalog"."pg_operator";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgoperator-0012-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_operator";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgoperator-0013-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT COUNT(*)
FROM PG_catalog.pg_OPERATOR o
JOIN pg_catalog.pg_type lt ON lt.oid = o.oprleft
JOIN pg_catalog.pg_type rt ON rt.oid = o.oprright
WHERE lt.typname = 'jsonb'
	AND (
		(o.oprname IN ('@>', '<@') AND rt.typname = 'jsonb')
		OR (o.oprname = '?' AND rt.typname = 'text')
		OR (o.oprname IN ('?|', '?&') AND rt.typname = '_text')
	);`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgoperator-0014-select-count-*-from-pg_catalog.pg_operator"},
				},
			},
		},
	})
}

func TestPgOpfamily(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_opfamily rows",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT opf.opfname, am.amname
FROM "pg_catalog"."pg_opfamily" opf
JOIN "pg_catalog"."pg_am" am ON am.oid = opf.opfmethod
WHERE (am.amname = 'btree' AND opf.opfname IN ('datetime_ops', 'integer_ops', 'jsonb_ops', 'text_ops'))
	OR (am.amname = 'hash' AND opf.opfname IN ('integer_ops', 'jsonb_ops', 'text_ops'))
	OR (am.amname = 'gin' AND opf.opfname IN ('jsonb_ops', 'jsonb_path_ops'))
	OR (am.amname = 'spgist' AND opf.opfname = 'text_ops')
ORDER BY opf.opfname, am.amname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgopfamily-0001-select-opf.opfname-am.amname-from-pg_catalog"},
				},
			},
		},
		{
			Name: "pg_opfamily case sensitivity",
			Assertions: []ScriptTestAssertion{
				{ // Different cases and quoted, so it fails
					Query: `SELECT * FROM "PG_catalog"."pg_opfamily";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgopfamily-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_opfamily";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgopfamily-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "pg_opfamily mixed-case lookup",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT opf.opfname, am.amname
FROM PG_catalog.pg_OPFAMILY opf
JOIN pg_catalog.pg_am am ON am.oid = opf.opfmethod
WHERE (am.amname = 'btree' AND opf.opfname IN ('datetime_ops', 'integer_ops', 'jsonb_ops', 'text_ops'))
	OR (am.amname = 'hash' AND opf.opfname IN ('integer_ops', 'jsonb_ops', 'text_ops'))
	OR (am.amname = 'gin' AND opf.opfname IN ('jsonb_ops', 'jsonb_path_ops'))
	OR (am.amname = 'spgist' AND opf.opfname = 'text_ops')
ORDER BY opf.opfname, am.amname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgopfamily-0004-select-opfname-from-pg_catalog.pg_opfamily-order"},
				},
			},
		},
	})
}

func TestPgParameterAcl(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_parameter_acl",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_parameter_acl";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgparameteracl-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_parameter_acl";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgparameteracl-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_parameter_acl";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgparameteracl-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT parname FROM PG_catalog.pg_PARAMETER_ACL ORDER BY parname;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgparameteracl-0004-select-parname-from-pg_catalog.pg_parameter_acl-order"},
				},
			},
		},
	})
}

func TestPgPartitionedTable(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_partitioned_table",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_partitioned_table";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgpartitionedtable-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_partitioned_table";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgpartitionedtable-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_partitioned_table";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgpartitionedtable-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT partrelid FROM PG_catalog.pg_PARTITIONED_TABLE ORDER BY partrelid;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgpartitionedtable-0004-select-partrelid-from-pg_catalog.pg_partitioned_table-order"},
				},
			},
		},
	})
}

func TestPgPolicies(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_policies",
			SetUpScript: []string{
				`CREATE TABLE pg_policies_docs (
					id int PRIMARY KEY,
					owner_name text
				);`,
				`CREATE POLICY pg_policies_docs_owner_insert
					ON pg_policies_docs
					FOR INSERT
					WITH CHECK (owner_name = CURRENT_USER);`,
				`CREATE POLICY pg_policies_docs_owner_select
					ON pg_policies_docs
					FOR SELECT
					USING (owner_name = CURRENT_USER);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT schemaname, tablename, policyname, permissive, roles, cmd, qual, with_check
FROM "pg_catalog"."pg_policies"
WHERE tablename = 'pg_policies_docs'
ORDER BY policyname;`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgpolicies-0001-select-*-from-pg_catalog-.", ColumnModes: []string{"schema"}},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_policies";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgpolicies-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_policies";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgpolicies-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT policyname FROM PG_catalog.pg_POLICIES WHERE tablename = 'pg_policies_docs' ORDER BY policyname;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgpolicies-0004-select-policyname-from-pg_catalog.pg_policies-order"},
				},
			},
		},
	})
}

func TestPgPolicy(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_policy",
			SetUpScript: []string{
				`CREATE TABLE pg_policy_docs (
					id int PRIMARY KEY,
					owner_name text
				);`,
				`CREATE POLICY pg_policy_docs_owner_insert
					ON pg_policy_docs
					FOR INSERT
					WITH CHECK (owner_name = CURRENT_USER);`,
				`CREATE POLICY pg_policy_docs_owner_select
					ON pg_policy_docs
					FOR SELECT
					USING (owner_name = CURRENT_USER);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT polname, polcmd, polpermissive, polroles,
	pg_get_expr(polqual, polrelid),
	pg_get_expr(polwithcheck, polrelid)
FROM "pg_catalog"."pg_policy"
WHERE polname IN ('pg_policy_docs_owner_insert', 'pg_policy_docs_owner_select')
ORDER BY polname;`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgpolicy-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_policy";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgpolicy-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_policy";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgpolicy-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT polname FROM PG_catalog.pg_POLICY WHERE polname IN ('pg_policy_docs_owner_insert', 'pg_policy_docs_owner_select') ORDER BY polname;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgpolicy-0004-select-polname-from-pg_catalog.pg_policy-order"},
				},
			},
		},
	})
}

func TestPgPreparedStatements(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_prepared_statements",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_prepared_statements";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgpreparedstatements-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_prepared_statements";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgpreparedstatements-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_prepared_statements";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgpreparedstatements-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT name FROM PG_catalog.pg_PREPARED_STATEMENTS ORDER BY name;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgpreparedstatements-0004-select-name-from-pg_catalog.pg_prepared_statements-order"},
				},
			},
		},
	})
}

func TestPgPreparedXacts(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_prepared_xacts",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_prepared_xacts";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgpreparedxacts-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_prepared_xacts";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgpreparedxacts-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_prepared_xacts";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgpreparedxacts-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT gid FROM PG_catalog.pg_PREPARED_XACTS ORDER BY gid;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgpreparedxacts-0004-select-gid-from-pg_catalog.pg_prepared_xacts-order"},
				},
			},
		},
	})
}

func TestPgProc(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_proc",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT count(*) > 0 FROM "pg_catalog"."pg_proc";`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgproc-0001-select-count-*->-0"},
				},
				{ // Schema lookup is case-insensitive for pg_catalog handlers.
					Query: `SELECT count(*) > 0 FROM "PG_catalog"."pg_proc";`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgproc-0002-select-count-*->-0",

						// Different cases and quoted, so it fails
						Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_proc";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgproc-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT count(*) > 0 FROM PG_catalog.pg_PROC WHERE proname = 'abs';", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgproc-0004-select-count-*->-0"},
				},
			},
		},
	})
}

func TestPgPublication(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_publication",
			SetUpScript: []string{
				`CREATE TABLE pg_publication_oracle_items (
					id int PRIMARY KEY
				);`,
				`CREATE PUBLICATION pg_publication_oracle_pub
					FOR TABLE pg_publication_oracle_items
					WITH (publish = 'insert, update', publish_via_partition_root = true);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT pubname,
	oid <> 0 AS has_oid,
	pubowner <> 0 AS has_owner,
	puballtables,
	pubinsert,
	pubupdate,
	pubdelete,
	pubtruncate,
	pubviaroot,
	tableoid <> 0 AS has_tableoid
FROM "pg_catalog"."pg_publication"
WHERE pubname = 'pg_publication_oracle_pub'
ORDER BY pubname;`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgpublication-0001-select-*-from-pg_catalog-."},
				},
			},
		},
		{
			Name: "pg_publication schema case sensitivity",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "PG_catalog"."pg_publication";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgpublication-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "pg_publication relation case sensitivity",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."PG_publication";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgpublication-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "pg_publication mixed-case lookup",
			SetUpScript: []string{
				`CREATE TABLE pg_publication_mixed_items (
					id int PRIMARY KEY
				);`,
				`CREATE PUBLICATION pg_publication_mixed_pub
					FOR TABLE pg_publication_mixed_items
					WITH (publish = 'insert');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT pubname FROM PG_catalog.pg_PUBLICATION WHERE pubname = 'pg_publication_mixed_pub' ORDER BY pubname;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgpublication-0004-select-pubname-from-pg_catalog.pg_publication-order"},
				},
			},
		},
	})
}

func TestPgPublicationNamespace(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_publication_namespace",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_publication_namespace";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgpublicationnamespace-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_publication_namespace";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgpublicationnamespace-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_publication_namespace";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgpublicationnamespace-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT oid FROM PG_catalog.pg_PUBLICATION_NAMESPACE ORDER BY oid;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgpublicationnamespace-0004-select-oid-from-pg_catalog.pg_publication_namespace-order"},
				},
			},
		},
	})
}

func TestPgPublicationRel(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_publication_rel",
			SetUpScript: []string{
				`CREATE TABLE pg_publication_rel_oracle_items (
					id int PRIMARY KEY
				);`,
				`CREATE PUBLICATION pg_publication_rel_oracle_pub
					FOR TABLE pg_publication_rel_oracle_items
					WITH (publish = 'insert');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT p.pubname,
	c.relname,
	pr.oid <> 0 AS has_oid,
	pr.prpubid = p.oid AS pubid_matches,
	pr.prrelid = c.oid AS relid_matches,
	pr.prqual IS NULL AS no_filter,
	pr.prattrs IS NULL AS all_columns,
	pr.tableoid <> 0 AS has_tableoid
FROM "pg_catalog"."pg_publication_rel" pr
JOIN "pg_catalog"."pg_publication" p ON p.oid = pr.prpubid
JOIN "pg_catalog"."pg_class" c ON c.oid = pr.prrelid
WHERE p.pubname = 'pg_publication_rel_oracle_pub'
ORDER BY p.pubname, c.relname;`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgpublicationrel-0001-select-*-from-pg_catalog-."},
				},
			},
		},
		{
			Name: "pg_publication_rel schema case sensitivity",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "PG_catalog"."pg_publication_rel";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgpublicationrel-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "pg_publication_rel relation case sensitivity",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."PG_publication_rel";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgpublicationrel-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "pg_publication_rel mixed-case lookup",
			SetUpScript: []string{
				`CREATE TABLE pg_publication_rel_mixed_items (
					id int PRIMARY KEY
				);`,
				`CREATE PUBLICATION pg_publication_rel_mixed_pub
					FOR TABLE pg_publication_rel_mixed_items
					WITH (publish = 'insert');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT c.relname
FROM PG_catalog.pg_PUBLICATION_REL pr
JOIN PG_catalog.pg_PUBLICATION p ON p.oid = pr.prpubid
JOIN PG_catalog.pg_CLASS c ON c.oid = pr.prrelid
WHERE p.pubname = 'pg_publication_rel_mixed_pub'
ORDER BY c.relname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgpublicationrel-0004-select-oid-from-pg_catalog.pg_publication_rel-order"},
				},
			},
		},
	})
}

func TestPgPublicationTables(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_publication_tables",
			SetUpScript: []string{
				`CREATE TABLE pg_publication_tables_oracle_items (
					id int PRIMARY KEY,
					note text
				);`,
				`CREATE PUBLICATION pg_publication_tables_oracle_pub
					FOR TABLE pg_publication_tables_oracle_items (id)
					WITH (publish = 'insert');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT pubname,
	schemaname,
	tablename,
	array_to_string(attnames, ',') AS attnames,
	rowfilter IS NULL AS no_filter
FROM "pg_catalog"."pg_publication_tables"
WHERE pubname = 'pg_publication_tables_oracle_pub'
ORDER BY pubname, schemaname, tablename;`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgpublicationtables-0001-select-*-from-pg_catalog-.", ColumnModes: []string{"structural", "schema"}},
				},
			},
		},
		{
			Name: "pg_publication_tables schema case sensitivity",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "PG_catalog"."pg_publication_tables";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgpublicationtables-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "pg_publication_tables relation case sensitivity",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."PG_publication_tables";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgpublicationtables-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "pg_publication_tables mixed-case lookup",
			SetUpScript: []string{
				`CREATE TABLE pg_publication_tables_mixed_items (
					id int PRIMARY KEY
				);`,
				`CREATE PUBLICATION pg_publication_tables_mixed_pub
					FOR TABLE pg_publication_tables_mixed_items
					WITH (publish = 'insert');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT pubname FROM PG_catalog.pg_PUBLICATION_TABLES WHERE pubname = 'pg_publication_tables_mixed_pub' ORDER BY pubname;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgpublicationtables-0004-select-pubname-from-pg_catalog.pg_publication_tables-order"},
				},
			},
		},
	})
}

func TestPgRange(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_range",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_range";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgrange-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_range";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgrange-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_range";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgrange-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT rngtypid FROM PG_catalog.pg_RANGE ORDER BY rngtypid;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgrange-0004-select-rngtypid-from-pg_catalog.pg_range-order"},
				},
			},
		},
	})
}

func TestPgReplicationOrigin(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_replication_origin",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT *
FROM "pg_catalog"."pg_replication_origin"
WHERE roname = 'doltgresql_oracle_origin';`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgreplicationorigin-0001-select-*-from-pg_catalog-."},
				},
			},
		},
		{
			Name: "pg_replication_origin schema case sensitivity",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "PG_catalog"."pg_replication_origin";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgreplicationorigin-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "pg_replication_origin relation case sensitivity",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."PG_replication_origin";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgreplicationorigin-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "pg_replication_origin mixed-case lookup",
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT roname FROM PG_catalog.pg_REPLICATION_ORIGIN WHERE roname = 'doltgresql_oracle_origin' ORDER BY roname;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgreplicationorigin-0004-select-roname-from-pg_catalog.pg_replication_origin-order"},
				},
			},
		},
	})
}

func TestPgReplicationOriginStatus(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_replication_origin_status",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT *
FROM "pg_catalog"."pg_replication_origin_status"
WHERE external_id = 'doltgresql_oracle_origin';`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgreplicationoriginstatus-0001-select-*-from-pg_catalog-."},
				},
			},
		},
		{
			Name: "pg_replication_origin_status schema case sensitivity",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "PG_catalog"."pg_replication_origin_status";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgreplicationoriginstatus-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "pg_replication_origin_status relation case sensitivity",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."PG_replication_origin_status";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgreplicationoriginstatus-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "pg_replication_origin_status mixed-case lookup",
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT local_id FROM PG_catalog.pg_REPLICATION_ORIGIN_STATUS WHERE external_id = 'doltgresql_oracle_origin' ORDER BY local_id;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgreplicationoriginstatus-0004-select-local_id-from-pg_catalog.pg_replication_origin_status-order"},
				},
			},
		},
	})
}

func TestPgReplicationSlots(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_replication_slot",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_replication_slots";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgreplicationslots-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_replication_slots";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgreplicationslots-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_replication_slots";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgreplicationslots-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT slot_name FROM PG_catalog.pg_REPLICATION_SLOTS ORDER BY slot_name;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgreplicationslots-0004-select-slot_name-from-pg_catalog.pg_replication_slots-order"},
				},
			},
		},
	})
}

func TestPgRewrite(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_rewrite",
			SetUpScript: []string{
				`CREATE VIEW pg_rewrite_oracle_view AS SELECT 1 AS id;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT c.relname,
	r.rulename,
	r.oid <> 0 AS has_oid,
	r.ev_class = c.oid AS class_matches,
	r.ev_type,
	r.ev_enabled,
	r.is_instead,
	r.ev_qual IS NOT NULL AS has_qual,
	r.ev_action IS NOT NULL AS has_action,
	r.tableoid <> 0 AS has_tableoid
FROM "pg_catalog"."pg_rewrite" r
JOIN "pg_catalog"."pg_class" c ON c.oid = r.ev_class
JOIN "pg_catalog"."pg_namespace" n ON n.oid = c.relnamespace
WHERE n.nspname = current_schema()
	AND c.relname = 'pg_rewrite_oracle_view'
ORDER BY c.relname, r.rulename;`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgrewrite-0001-select-*-from-pg_catalog-."},
				},
			},
		},
		{
			Name: "pg_rewrite schema case sensitivity",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "PG_catalog"."pg_rewrite";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgrewrite-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "pg_rewrite relation case sensitivity",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."PG_rewrite";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgrewrite-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "pg_rewrite mixed-case lookup",
			SetUpScript: []string{
				`CREATE VIEW pg_rewrite_mixed_view AS SELECT 1 AS id;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT c.relname
FROM PG_catalog.pg_REWRITE r
JOIN PG_catalog.pg_CLASS c ON c.oid = r.ev_class
JOIN PG_catalog.pg_NAMESPACE n ON n.oid = c.relnamespace
WHERE n.nspname = current_schema()
	AND c.relname = 'pg_rewrite_mixed_view'
ORDER BY c.relname, r.rulename;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgrewrite-0004-select-oid-from-pg_catalog.pg_rewrite-order"},
				},
			},
		},
	})
}

func TestPgRoles(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_roles",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT rolname, rolcanlogin, oid <> 0 AS has_oid
							FROM "pg_catalog"."pg_roles"
							WHERE rolname IN (
								'pg_checkpoint',
								'pg_create_subscription',
								'pg_database_owner',
								'pg_execute_server_program',
								'pg_maintain',
								'pg_monitor',
								'pg_read_all_data',
								'pg_read_all_settings',
								'pg_read_all_stats',
								'pg_read_server_files',
								'pg_signal_autovacuum_worker',
								'pg_signal_backend',
								'pg_stat_scan_tables',
								'pg_use_reserved_connections',
								'pg_write_all_data',
								'pg_write_server_files',
								'postgres',
								'public'
							)
							ORDER BY rolname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgroles-0001-select-oid-rolname-rolcanlogin-from"},
				},
				{ // Different cases and quoted, so it fails
					Query: `SELECT * FROM "PG_catalog"."pg_roles";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgroles-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_roles";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgroles-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT rolname
							FROM PG_catalog.pg_ROLES
							WHERE rolname IN (
								'pg_checkpoint',
								'pg_create_subscription',
								'pg_database_owner',
								'pg_execute_server_program',
								'pg_maintain',
								'pg_monitor',
								'pg_read_all_data',
								'pg_read_all_settings',
								'pg_read_all_stats',
								'pg_read_server_files',
								'pg_signal_autovacuum_worker',
								'pg_signal_backend',
								'pg_stat_scan_tables',
								'pg_use_reserved_connections',
								'pg_write_all_data',
								'pg_write_server_files',
								'postgres',
								'public'
							)
							ORDER BY rolname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgroles-0004-select-rolname-from-pg_catalog.pg_roles-order"},
				},
			},
		},
	})
}

func TestPgRules(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_rules",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_rules";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgrules-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_rules";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgrules-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_rules";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgrules-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT rulename FROM PG_catalog.pg_RULES ORDER BY rulename;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgrules-0004-select-rulename-from-pg_catalog.pg_rules-order"},
				},
			},
		},
	})
}

func TestPgSeclabel(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_seclabel",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_seclabel";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgseclabel-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_seclabel";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgseclabel-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_seclabel";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgseclabel-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT objoid FROM PG_catalog.pg_SECLABEL ORDER BY objoid;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgseclabel-0004-select-objoid-from-pg_catalog.pg_seclabel-order"},
				},
			},
		},
	})
}

func TestPgSeclabels(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_seclabels",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_seclabels";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgseclabels-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_seclabels";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgseclabels-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_seclabels";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgseclabels-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT objoid FROM PG_catalog.pg_SECLABELS ORDER BY objoid;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgseclabels-0004-select-objoid-from-pg_catalog.pg_seclabels-order"},
				},
			},
		},
	})
}

func TestPgSequences(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_sequences",
			SetUpScript: []string{
				`CREATE SEQUENCE pg_sequences_alpha START WITH 5 INCREMENT BY 2 MINVALUE 1 MAXVALUE 51;`,
				`CREATE SEQUENCE pg_sequences_beta;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT schemaname, sequencename, sequenceowner, data_type,
							start_value, min_value, max_value, increment_by, cycle,
							cache_size, last_value IS NULL AS last_value_is_null
						FROM "pg_catalog"."pg_sequences"
						WHERE sequencename IN ('pg_sequences_alpha', 'pg_sequences_beta')
						ORDER BY sequencename;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgsequences-0001-select-schemaname-sequencename-sequenceowner-from-pg_catalog.pg_sequences-order"},
				},
			},
		},
		{
			Name: "pg_sequences schema case sensitivity",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "PG_catalog"."pg_sequences";`, PostgresOracle: ScriptTestPostgresOracle{
						ID: "pgcatalog-test-testpgsequences-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "pg_sequences relation case sensitivity",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."PG_sequences";`, PostgresOracle: ScriptTestPostgresOracle{
						ID: "pgcatalog-test-testpgsequences-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "pg_sequences mixed-case lookup",
			SetUpScript: []string{
				`CREATE SEQUENCE pg_sequences_alpha START WITH 5 INCREMENT BY 2 MINVALUE 1 MAXVALUE 51;`,
				`CREATE SEQUENCE pg_sequences_beta;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT sequencename
						FROM PG_catalog.pg_SEQUENCES
						WHERE sequencename IN ('pg_sequences_alpha', 'pg_sequences_beta')
						ORDER BY sequencename;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgsequences-0004-select-sequencename-from-pg_catalog.pg_sequences-order"},
				},
			},
		},
	})
}

func TestPgSettings(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_settings supported parameters",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT name, setting FROM "pg_catalog"."pg_settings"
						WHERE name IN ('server_version_num', 'wal_sender_timeout') ORDER BY name;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgsettings-0001-select-name-setting-from-pg_catalog"},
				},
			},
		},
		{
			Name: "pg_settings schema case sensitivity",
			Assertions: []ScriptTestAssertion{
				{ // Different cases and quoted, so it fails
					Query: `SELECT * FROM "PG_catalog"."pg_settings";`, PostgresOracle: ScriptTestPostgresOracle{
						ID: "pgcatalog-test-testpgsettings-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "pg_settings relation case sensitivity",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."PG_settings";`, PostgresOracle: ScriptTestPostgresOracle{
						ID: "pgcatalog-test-testpgsettings-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "pg_settings mixed-case lookup",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT name
						FROM PG_catalog.pg_SETTINGS
						WHERE name IN ('server_version_num', 'wal_sender_timeout')
						ORDER BY name;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgsettings-0004-select-name-from-pg_catalog.pg_settings-order"},
				},
			},
		},
	})
}

func TestPgShadow(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_shadow default login role",
			SetUpScript: []string{
				`CREATE USER shadow_alpha_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT usename, usesuper, usecreatedb, userepl, usebypassrls, usesysid <> 0 AS has_oid
						FROM "pg_catalog"."pg_shadow"
						WHERE usename = 'shadow_alpha_user';`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgshadow-0001-select-*-from-pg_catalog-."},
				},
			},
		},
		{
			Name: "pg_shadow schema case sensitivity",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "PG_catalog"."pg_shadow";`, PostgresOracle: ScriptTestPostgresOracle{
						ID: "pgcatalog-test-testpgshadow-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "pg_shadow relation case sensitivity",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."PG_shadow";`, PostgresOracle: ScriptTestPostgresOracle{
						ID: "pgcatalog-test-testpgshadow-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "pg_shadow mixed-case lookup",
			SetUpScript: []string{
				`CREATE USER shadow_mixed_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT usename
						FROM PG_catalog.pg_SHADOW
						WHERE usename = 'shadow_mixed_user'
						ORDER BY usename;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgshadow-0004-select-usename-from-pg_catalog.pg_shadow-order"},
				},
			},
		},
	})
}

func TestPgShdepend(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_shdepend",
			SetUpScript: []string{
				`CREATE ROLE shdepend_generic_owner;`,
				`CREATE TABLE shdepend_generic_items (id INT);`,
				`ALTER TABLE shdepend_generic_items OWNER TO shdepend_generic_owner;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT r.rolname, c.relname, d.deptype
						FROM "pg_catalog"."pg_shdepend" d
						JOIN "pg_catalog"."pg_class" c ON d.objid = c.oid
						JOIN "pg_catalog"."pg_roles" r ON d.refobjid = r.oid
						WHERE c.relname = 'shdepend_generic_items'
							AND r.rolname = 'shdepend_generic_owner'
						ORDER BY r.rolname, c.relname, d.deptype;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgshdepend-0001-select-r.rolname-c.relname-d.deptype-from-pg_catalog.pg_shdepend-d"},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_shdepend";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgshdepend-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_shdepend";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgshdepend-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT d.deptype
						FROM PG_catalog.pg_SHDEPEND d
						JOIN PG_catalog.pg_CLASS c ON d.objid = c.oid
						JOIN PG_catalog.pg_ROLES r ON d.refobjid = r.oid
						WHERE c.relname = 'shdepend_generic_items'
							AND r.rolname = 'shdepend_generic_owner'
						ORDER BY d.deptype;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgshdepend-0004-select-d.deptype-from-pg_catalog.pg_shdepend-d"},
				},
			},
		},
	})
}

func TestPgShdescription(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_shdescription",
			SetUpScript: []string{
				`COMMENT ON DATABASE postgres IS 'generic pg_shdescription comment';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT description
						FROM "pg_catalog"."pg_shdescription"
						WHERE objoid = (SELECT oid FROM "pg_catalog"."pg_database" WHERE datname = 'postgres')
							AND classoid = 'pg_database'::regclass
							AND description = 'generic pg_shdescription comment';`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgshdescription-0001-select-description-from-pg_catalog.pg_shdescription-where"},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_shdescription";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgshdescription-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_shdescription";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgshdescription-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT description
						FROM PG_catalog.pg_SHDESCRIPTION
						WHERE description = 'generic pg_shdescription comment'
						ORDER BY description;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgshdescription-0004-select-description-from-pg_catalog.pg_shdescription-where"},
				},
			},
		},
	})
}

func TestPgShmemAllocations(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_shmem_allocations",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT count(*) > 0
						FROM "pg_catalog"."pg_shmem_allocations"
						WHERE allocated_size >= size
							AND size >= 0;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgshmemallocations-0001-select-count-*-from-pg_catalog.pg_shmem_allocations"},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_shmem_allocations";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgshmemallocations-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_shmem_allocations";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgshmemallocations-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT count(*) > 0
						FROM PG_catalog.pg_SHMEM_ALLOCATIONS
						WHERE allocated_size >= size
							AND size >= 0;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgshmemallocations-0004-select-count-*-from-pg_catalog.pg_shmem_allocations"},
				},
			},
		},
	})
}

func TestPgShseclabel(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_shseclabel",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_shseclabel";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgshseclabel-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_shseclabel";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgshseclabel-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_shseclabel";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgshseclabel-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT objoid FROM PG_catalog.pg_SHSECLABEL ORDER BY objoid;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgshseclabel-0004-select-objoid-from-pg_catalog.pg_shseclabel-order"},
				},
			},
		},
	})
}

func TestPgStatActivity(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_stat_activity",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT count(*) > 0
						FROM "pg_catalog"."pg_stat_activity"
						WHERE pid = pg_backend_pid()
							AND state = 'active'
							AND backend_type = 'client backend';`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatactivity-0001-select-count-*-from-pg_catalog.pg_stat_activity"},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_stat_activity";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgstatactivity-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_stat_activity";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgstatactivity-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT count(*) > 0
						FROM PG_catalog.pg_STAT_ACTIVITY
						WHERE pid = pg_backend_pid()
							AND state = 'active'
							AND backend_type = 'client backend';`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatactivity-0004-select-count-*-from-pg_catalog.pg_stat_activity"},
				},
			},
		},
	})
}

func TestPgStatAllIndexes(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_stat_all_indexes",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT count(*) > 0
FROM "pg_catalog"."pg_stat_all_indexes"
WHERE schemaname = 'pg_catalog'
  AND relname = 'pg_class'
  AND indexrelname = 'pg_class_oid_index';`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatallindexes-0001-select-count-from-pg_catalog.pg_stat_all_indexes-where"},
				},
				{ // Different cases and quoted, so it fails
					Query: `SELECT * FROM "PG_catalog"."pg_stat_all_indexes";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgstatallindexes-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_stat_all_indexes";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgstatallindexes-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT count(*) > 0 FROM PG_catalog.pg_STAT_ALL_INDEXES
WHERE schemaname = 'pg_catalog'
  AND relname = 'pg_class'
  AND indexrelname = 'pg_class_oid_index';`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatallindexes-0004-select-count-from-pg_catalog.pg_stat_all_indexes-where"},
				},
			},
		},
	})
}

func TestPgStatAllTables(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_stat_all_tables",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT count(*) > 0
FROM "pg_catalog"."pg_stat_all_tables"
WHERE schemaname = 'pg_catalog'
  AND relname = 'pg_class';`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatalltables-0001-select-count-from-pg_catalog.pg_stat_all_tables-where"},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_stat_all_tables";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgstatalltables-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_stat_all_tables";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgstatalltables-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT count(*) > 0 FROM PG_catalog.pg_STAT_ALL_TABLES
WHERE schemaname = 'pg_catalog'
  AND relname = 'pg_class';`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatalltables-0004-select-count-from-pg_catalog.pg_stat_all_tables-where"},
				},
			},
		},
	})
}

func TestPgStatArchiver(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_stat_archiver",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT archived_count, failed_count
FROM "pg_catalog"."pg_stat_archiver";`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatarchiver-0001-select-archived_count-failed_count-from-pg_catalog.pg_stat_archiver"},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_stat_archiver";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgstatarchiver-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_stat_archiver";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgstatarchiver-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT archived_count FROM PG_catalog.pg_STAT_ARCHIVER ORDER BY archived_count;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatarchiver-0004-select-archived_count-from-pg_catalog.pg_stat_archiver-order"},
				},
			},
		},
	})
}

func TestPgStatBgwriter(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_stat_bgwriter",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT count(*) = 1
FROM "pg_catalog"."pg_stat_bgwriter"
WHERE checkpoints_timed >= 0
  AND checkpoints_req >= 0
  AND buffers_backend_fsync >= 0;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatbgwriter-0001-select-count-1-from-pg_catalog.pg_stat_bgwriter-where"},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_stat_bgwriter";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgstatbgwriter-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_stat_bgwriter";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgstatbgwriter-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT count(*) = 1 FROM PG_catalog.pg_STAT_BGWRITER
WHERE checkpoints_timed >= 0
  AND checkpoints_req >= 0
  AND buffers_backend_fsync >= 0;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatbgwriter-0004-select-count-1-from-pg_catalog.pg_stat_bgwriter-where"},
				},
			},
		},
	})
}

func TestPgStatDatabase(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_stat_database",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT count(*) = 1
FROM "pg_catalog"."pg_stat_database"
WHERE datname = current_database()
  AND numbackends >= 0
  AND xact_commit >= 0
  AND sessions >= 0;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatdatabase-0001-select-count-1-from-pg_catalog.pg_stat_database-where"},
				},
				{ // Different cases and quoted, so it fails
					Query: `SELECT * FROM "PG_catalog"."pg_stat_database";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgstatdatabase-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_stat_database";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgstatdatabase-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT count(*) = 1 FROM PG_catalog.pg_STAT_DATABASE
WHERE datname = current_database()
  AND numbackends >= 0
  AND xact_commit >= 0
  AND sessions >= 0;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatdatabase-0004-select-count-1-from-pg_catalog.pg_stat_database-where"},
				},
			},
		},
	})
}

func TestPgStatDatabaseConflicts(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_stat_database_conflicts",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT count(*) = 1
FROM "pg_catalog"."pg_stat_database_conflicts"
WHERE datname = current_database()
  AND confl_tablespace >= 0
  AND confl_lock >= 0
  AND confl_active_logicalslot >= 0;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatdatabaseconflicts-0001-select-count-1-from-pg_catalog.pg_stat_database_conflicts"},
				},
				{ // Different cases and quoted, so it fails
					Query: `SELECT * FROM "PG_catalog"."pg_stat_database_conflicts";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgstatdatabaseconflicts-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_stat_database_conflicts";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgstatdatabaseconflicts-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT count(*) = 1 FROM PG_catalog.pg_STAT_DATABASE_CONFLICTS
WHERE datname = current_database()
  AND confl_tablespace >= 0
  AND confl_lock >= 0
  AND confl_active_logicalslot >= 0;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatdatabaseconflicts-0004-select-count-1-from-pg_catalog.pg_stat_database_conflicts"},
				},
			},
		},
	})
}

func TestPgStatGssapi(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_stat_gssapi",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT count(*) = 1
FROM "pg_catalog"."pg_stat_gssapi"
WHERE gss_authenticated IS FALSE
  AND encrypted IS FALSE
  AND credentials_delegated IS FALSE;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatgssapi-0001-select-count-1-from-pg_catalog.pg_stat_gssapi-where"},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_stat_gssapi";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgstatgssapi-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_stat_gssapi";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgstatgssapi-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT count(*) = 1 FROM PG_catalog.pg_STAT_GSSAPI
WHERE gss_authenticated IS FALSE
  AND encrypted IS FALSE
  AND credentials_delegated IS FALSE;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatgssapi-0004-select-count-1-from-pg_catalog.pg_stat_gssapi-where"},
				},
			},
		},
	})
}

func TestPgStatProgressAnalyze(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_stat_progress_analyze",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_stat_progress_analyze";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgstatprogressanalyze-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_stat_progress_analyze";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgstatprogressanalyze-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_stat_progress_analyze";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgstatprogressanalyze-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT datname FROM PG_catalog.pg_STAT_PROGRESS_ANALYZE ORDER BY datname;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatprogressanalyze-0004-select-datname-from-pg_catalog.pg_stat_progress_analyze-order"},
				},
			},
		},
	})
}

func TestPgStatProgressBasebackup(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_stat_progress_basebackup",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_stat_progress_basebackup";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgstatprogressbasebackup-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_stat_progress_basebackup";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgstatprogressbasebackup-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_stat_progress_basebackup";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgstatprogressbasebackup-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT pid FROM PG_catalog.pg_STAT_PROGRESS_BASEBACKUP ORDER BY pid;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatprogressbasebackup-0004-select-pid-from-pg_catalog.pg_stat_progress_basebackup-order"},
				},
			},
		},
	})
}

func TestPgStatProgressCluster(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_stat_progress_cluster",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_stat_progress_cluster";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgstatprogresscluster-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_stat_progress_cluster";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgstatprogresscluster-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_stat_progress_cluster";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgstatprogresscluster-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT pid FROM PG_catalog.pg_STAT_PROGRESS_CLUSTER ORDER BY pid;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatprogresscluster-0004-select-pid-from-pg_catalog.pg_stat_progress_cluster-order"},
				},
			},
		},
	})
}

func TestPgStatProgressCopy(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_stat_progress_copy",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_stat_progress_copy";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgstatprogresscopy-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_stat_progress_copy";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgstatprogresscopy-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_stat_progress_copy";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgstatprogresscopy-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT pid FROM PG_catalog.pg_STAT_PROGRESS_COPY ORDER BY pid;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatprogresscopy-0004-select-pid-from-pg_catalog.pg_stat_progress_copy-order"},
				},
			},
		},
	})
}

func TestPgStatProgressCreateIndex(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_stat_progress_create_index",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_stat_progress_create_index";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgstatprogresscreateindex-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_stat_progress_create_index";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgstatprogresscreateindex-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_stat_progress_create_index";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgstatprogresscreateindex-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT pid FROM PG_catalog.pg_STAT_PROGRESS_CREATE_INDEX ORDER BY pid;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatprogresscreateindex-0004-select-pid-from-pg_catalog.pg_stat_progress_create_index-order"},
				},
			},
		},
	})
}

func TestPgStatProgressVacuum(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_stat_progress_vacuum",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_stat_progress_vacuum";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgstatprogressvacuum-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_stat_progress_vacuum";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgstatprogressvacuum-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_stat_progress_vacuum";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgstatprogressvacuum-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT pid FROM PG_catalog.pg_STAT_PROGRESS_VACUUM ORDER BY pid;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatprogressvacuum-0004-select-pid-from-pg_catalog.pg_stat_progress_vacuum-order"},
				},
			},
		},
	})
}

func TestPgStatRecoveryPrefetch(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_stat_recovery_prefetch",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT count(*) = 1, count(prefetch) = 1, count(hit) = 1, count(wal_distance) = 1 FROM "pg_catalog"."pg_stat_recovery_prefetch";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgstatrecoveryprefetch-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_stat_recovery_prefetch";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgstatrecoveryprefetch-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_stat_recovery_prefetch";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgstatrecoveryprefetch-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT count(*) = 1, count(hit) = 1 FROM PG_catalog.pg_STAT_RECOVERY_PREFETCH;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatrecoveryprefetch-0004-select-hit-from-pg_catalog.pg_stat_recovery_prefetch-order"},
				},
			},
		},
	})
}

func TestPgStatReplication(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_stat_replication",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_stat_replication";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgstatreplication-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_stat_replication";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgstatreplication-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_stat_replication";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgstatreplication-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT pid FROM PG_catalog.pg_STAT_REPLICATION ORDER BY pid;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatreplication-0004-select-pid-from-pg_catalog.pg_stat_replication-order"},
				},
			},
		},
	})
}

func TestPgStatReplicationSlots(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_stat_replication_slots",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_stat_replication_slots";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgstatreplicationslots-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_stat_replication_slots";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgstatreplicationslots-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_stat_replication_slots";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgstatreplicationslots-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT slot_name FROM PG_catalog.pg_STAT_REPLICATION_SLOTS ORDER BY slot_name;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatreplicationslots-0004-select-slot_name-from-pg_catalog.pg_stat_replication_slots-order"},
				},
			},
		},
	})
}

func TestPgStatSlru(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_stat_slru",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT name,
							blks_zeroed >= 0 AS blks_zeroed_nonnegative,
							blks_hit >= 0 AS blks_hit_nonnegative,
							blks_read >= 0 AS blks_read_nonnegative,
							blks_written >= 0 AS blks_written_nonnegative,
							blks_exists >= 0 AS blks_exists_nonnegative,
							flushes >= 0 AS flushes_nonnegative,
							truncates >= 0 AS truncates_nonnegative
						FROM "pg_catalog"."pg_stat_slru"
						ORDER BY name;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatslru-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_stat_slru";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgstatslru-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_stat_slru";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgstatslru-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT name FROM PG_catalog.pg_STAT_SLRU ORDER BY name;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatslru-0004-select-name-from-pg_catalog.pg_stat_slru-order"},
				},
			},
		},
	})
}

func TestPgStatSsl(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_stat_ssl",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT ssl,
							version IS NULL AS version_is_null,
							cipher IS NULL AS cipher_is_null,
							bits IS NULL AS bits_is_null,
							client_dn IS NULL AS client_dn_is_null,
							client_serial IS NULL AS client_serial_is_null,
							issuer_dn IS NULL AS issuer_dn_is_null
						FROM "pg_catalog"."pg_stat_ssl"
						WHERE pid = pg_backend_pid();`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatssl-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_stat_ssl";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgstatssl-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_stat_ssl";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgstatssl-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT count(*) = 1 FROM PG_catalog.pg_STAT_SSL WHERE pid = pg_backend_pid();", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatssl-0004-select-pid-from-pg_catalog.pg_stat_ssl-order"},
				},
			},
		},
	})
}

func TestPgStatSubscription(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_stat_subscription",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT count(*) = 0 FROM "pg_catalog"."pg_stat_subscription";`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatsubscription-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_stat_subscription";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgstatsubscription-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_stat_subscription";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgstatsubscription-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT count(*) = 0 FROM PG_catalog.pg_STAT_SUBSCRIPTION;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatsubscription-0004-select-subid-from-pg_catalog.pg_stat_subscription-order"},
				},
			},
		},
	})
}

func TestPgStatSubscriptionStats(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_stat_subscription_stats",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT count(*) = 0 FROM "pg_catalog"."pg_stat_subscription_stats";`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatsubscriptionstats-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_stat_subscription_stats";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgstatsubscriptionstats-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_stat_subscription_stats";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgstatsubscriptionstats-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT count(*) = 0 FROM PG_catalog.pg_STAT_SUBSCRIPTION_STATS;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatsubscriptionstats-0004-select-subid-from-pg_catalog.pg_stat_subscription_stats-order"},
				},
			},
		},
	})
}

func TestPgStatSysIndexes(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_stat_sys_indexes",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT schemaname, relname, indexrelname,
       idx_scan >= 0 AS idx_scan_nonnegative,
       idx_tup_read >= 0 AS idx_tup_read_nonnegative,
       idx_tup_fetch >= 0 AS idx_tup_fetch_nonnegative
FROM "pg_catalog"."pg_stat_sys_indexes"
WHERE schemaname = 'pg_catalog'
  AND relname = 'pg_class'
  AND indexrelname IN ('pg_class_oid_index', 'pg_class_relname_nsp_index')
ORDER BY indexrelname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatsysindexes-0001-select-schemaname-relname-indexrelname-idx_scan"},
				},
				{ // Different cases and quoted, so it fails
					Query: `SELECT * FROM "PG_catalog"."pg_stat_sys_indexes";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgstatsysindexes-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_stat_sys_indexes";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgstatsysindexes-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT count(*) = 2 FROM PG_catalog.pg_STAT_SYS_INDEXES
WHERE schemaname = 'pg_catalog'
  AND relname = 'pg_class'
  AND indexrelname IN ('pg_class_oid_index', 'pg_class_relname_nsp_index');`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatsysindexes-0004-select-indexrelname-from-pg_catalog.pg_stat_sys_indexes-where"},
				},
			},
		},
	})
}

func TestPgStatSysTables(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_stat_sys_tables",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT schemaname, relname,
       seq_scan >= 0 AS seq_scan_nonnegative,
       seq_tup_read >= 0 AS seq_tup_read_nonnegative,
       idx_scan >= 0 AS idx_scan_nonnegative,
       idx_tup_fetch >= 0 AS idx_tup_fetch_nonnegative
FROM "pg_catalog"."pg_stat_sys_tables"
WHERE schemaname = 'pg_catalog'
  AND relname = 'pg_class';`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgstatsystables-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_stat_sys_tables";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgstatsystables-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_stat_sys_tables";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgstatsystables-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT count(*) = 1 FROM PG_catalog.pg_STAT_SYS_TABLES
WHERE schemaname = 'pg_catalog'
  AND relname = 'pg_class';`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatsystables-0004-select-relid-from-pg_catalog.pg_stat_sys_tables-order"},
				},
			},
		},
	})
}

func TestPgStatUserFunctions(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_stat_user_functions",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_stat_user_functions";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgstatuserfunctions-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_stat_user_functions";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgstatuserfunctions-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_stat_user_functions";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgstatuserfunctions-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT funcid FROM PG_catalog.pg_STAT_USER_FUNCTIONS ORDER BY funcid;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatuserfunctions-0004-select-funcid-from-pg_catalog.pg_stat_user_functions-order"},
				},
			},
		},
	})
}

func TestPgStatUserIndexes(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_stat_user_indexes",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT count(*) = 0 FROM "pg_catalog"."pg_stat_user_indexes";`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatuserindexes-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_stat_user_indexes";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgstatuserindexes-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_stat_user_indexes";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgstatuserindexes-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT count(*) = 0 FROM PG_catalog.pg_STAT_USER_INDEXES;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatuserindexes-0004-select-relid-from-pg_catalog.pg_stat_user_indexes-order"},
				},
			},
		},
	})
}

func TestPgStatUserTables(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_stat_user_tables",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT count(*) = 0 FROM "pg_catalog"."pg_stat_user_tables";`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatusertables-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_stat_user_tables";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgstatusertables-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_stat_user_tables";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgstatusertables-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT count(*) = 0 FROM PG_catalog.pg_STAT_USER_TABLES;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatusertables-0004-select-relid-from-pg_catalog.pg_stat_user_tables-order"},
				},
			},
		},
	})
}

func TestPgStatWal(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_stat_wal",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_stat_wal";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgstatwal-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_stat_wal";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgstatwal-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_stat_wal";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgstatwal-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT wal_records FROM PG_catalog.pg_STAT_WAL ORDER BY wal_records;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatwal-0004-select-wal_records-from-pg_catalog.pg_stat_wal-order"},
				},
			},
		},
	})
}

func TestPgStatWalReceiver(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_stat_wal_receiver",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_stat_wal_receiver";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgstatwalreceiver-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_stat_wal_receiver";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgstatwalreceiver-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_stat_wal_receiver";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgstatwalreceiver-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT pid FROM PG_catalog.pg_STAT_WAL_RECEIVER ORDER BY pid;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatwalreceiver-0004-select-pid-from-pg_catalog.pg_stat_wal_receiver-order"},
				},
			},
		},
	})
}

func TestPgStatXactAllTables(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_stat_xact_all_tables",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_stat_xact_all_tables";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgstatxactalltables-0001-select-*-from-pg_catalog-.", ColumnModes: []string{"structural", "schema"}},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_stat_xact_all_tables";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgstatxactalltables-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_stat_xact_all_tables";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgstatxactalltables-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT relid FROM PG_catalog.pg_STAT_XACT_ALL_TABLES ORDER BY relid;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatxactalltables-0004-select-relid-from-pg_catalog.pg_stat_xact_all_tables-order"},
				},
			},
		},
	})
}

func TestPgStatXactSysTables(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_stat_xact_sys_tables",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_stat_xact_sys_tables";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgstatxactsystables-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_stat_xact_sys_tables";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgstatxactsystables-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_stat_xact_sys_tables";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgstatxactsystables-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT relid FROM PG_catalog.pg_STAT_XACT_SYS_TABLES ORDER BY relid;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatxactsystables-0004-select-relid-from-pg_catalog.pg_stat_xact_sys_tables-order"},
				},
			},
		},
	})
}

func TestPgStatXactUserFunctions(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_stat_xact_user_functions",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_stat_xact_user_functions";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgstatxactuserfunctions-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_stat_xact_user_functions";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgstatxactuserfunctions-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_stat_xact_user_functions";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgstatxactuserfunctions-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT funcid FROM PG_catalog.pg_STAT_XACT_USER_FUNCTIONS ORDER BY funcid;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatxactuserfunctions-0004-select-funcid-from-pg_catalog.pg_stat_xact_user_functions-order"},
				},
			},
		},
	})
}

func TestPgStatXactUserTables(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_stat_xact_user_tables",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_stat_xact_user_tables";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgstatxactusertables-0001-select-*-from-pg_catalog-.", ColumnModes: []string{"structural", "schema"}},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_stat_xact_user_tables";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgstatxactusertables-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_stat_xact_user_tables";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgstatxactusertables-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT relid FROM PG_catalog.pg_STAT_XACT_USER_TABLES ORDER BY relid;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatxactusertables-0004-select-relid-from-pg_catalog.pg_stat_xact_user_tables-order"},
				},
			},
		},
	})
}

func TestPgStatioAllIndexes(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_statio_all_indexes",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT schemaname, relname, indexrelname, idx_blks_read, idx_blks_hit
FROM "pg_catalog"."pg_statio_all_indexes"
WHERE schemaname = 'pg_catalog' AND relname = 'pg_class'
ORDER BY indexrelname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatioallindexes-0001-select-schemaname-relname-indexrelname-idx_blks_read"},
				},
				{ // Different cases and quoted, so it fails
					Query: `SELECT * FROM "PG_catalog"."pg_statio_all_indexes";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgstatioallindexes-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_statio_all_indexes";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgstatioallindexes-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT indexrelname FROM PG_catalog.pg_STATIO_ALL_INDEXES
WHERE schemaname = 'pg_catalog' AND relname = 'pg_class'
ORDER BY indexrelname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatioallindexes-0004-select-indexrelname-from-pg_catalog.pg_statio_all_indexes-where"},
				},
			},
		},
	})
}

func TestPgStatioAllSequences(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_statio_all_sequences",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_statio_all_sequences";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgstatioallsequences-0001-select-*-from-pg_catalog-.", ColumnModes: []string{"structural", "schema"}},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_statio_all_sequences";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgstatioallsequences-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_statio_all_sequences";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgstatioallsequences-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT relid FROM PG_catalog.pg_STATIO_ALL_SEQUENCES ORDER BY relid;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatioallsequences-0004-select-relid-from-pg_catalog.pg_statio_all_sequences-order"},
				},
			},
		},
	})
}

func TestPgStatioAllTables(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_statio_all_tables",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_statio_all_tables";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgstatioalltables-0001-select-*-from-pg_catalog-.", ColumnModes: []string{"structural", "schema"}},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_statio_all_tables";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgstatioalltables-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_statio_all_tables";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgstatioalltables-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT relid FROM PG_catalog.pg_STATIO_ALL_TABLES ORDER BY relid;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatioalltables-0004-select-relid-from-pg_catalog.pg_statio_all_tables-order"},
				},
			},
		},
	})
}

func TestPgStatioSysIndexes(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_statio_sys_indexes",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT schemaname, relname, indexrelname, idx_blks_read, idx_blks_hit
FROM "pg_catalog"."pg_statio_sys_indexes"
WHERE schemaname = 'pg_catalog' AND relname = 'pg_class'
ORDER BY indexrelname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatiosysindexes-0001-select-schemaname-relname-indexrelname-idx_blks_read"},
				},
				{ // Different cases and quoted, so it fails
					Query: `SELECT * FROM "PG_catalog"."pg_statio_sys_indexes";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgstatiosysindexes-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_statio_sys_indexes";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgstatiosysindexes-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT indexrelname FROM PG_catalog.pg_STATIO_SYS_INDEXES
WHERE schemaname = 'pg_catalog' AND relname = 'pg_class'
ORDER BY indexrelname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatiosysindexes-0004-select-indexrelname-from-pg_catalog.pg_statio_sys_indexes-where"},
				},
			},
		},
	})
}

func TestPgStatioSysSequences(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_statio_sys_sequences",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_statio_sys_sequences";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgstatiosyssequences-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_statio_sys_sequences";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgstatiosyssequences-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_statio_sys_sequences";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgstatiosyssequences-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT relid FROM PG_catalog.pg_STATIO_SYS_SEQUENCES ORDER BY relid;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatiosyssequences-0004-select-relid-from-pg_catalog.pg_statio_sys_sequences-order"},
				},
			},
		},
	})
}

func TestPgStatioSysTables(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_statio_sys_tables",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_statio_sys_tables";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgstatiosystables-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_statio_sys_tables";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgstatiosystables-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_statio_sys_tables";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgstatiosystables-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT relid FROM PG_catalog.pg_STATIO_SYS_TABLES ORDER BY relid;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatiosystables-0004-select-relid-from-pg_catalog.pg_statio_sys_tables-order"},
				},
			},
		},
	})
}

func TestPgStatioUserIndexes(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_statio_user_indexes",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_statio_user_indexes";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgstatiouserindexes-0001-select-*-from-pg_catalog-.", ColumnModes: []string{"structural", "structural", "schema"}},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_statio_user_indexes";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgstatiouserindexes-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_statio_user_indexes";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgstatiouserindexes-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT relid FROM PG_catalog.pg_STATIO_USER_INDEXES ORDER BY relid;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatiouserindexes-0004-select-relid-from-pg_catalog.pg_statio_user_indexes-order"},
				},
			},
		},
	})
}

func TestPgStatioUserSequences(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_statio_user_sequences",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_statio_user_sequences";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgstatiousersequences-0001-select-*-from-pg_catalog-.", ColumnModes: []string{"structural", "schema"}},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_statio_user_sequences";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgstatiousersequences-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_statio_user_sequences";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgstatiousersequences-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT relid FROM PG_catalog.pg_STATIO_USER_SEQUENCES ORDER BY relid;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatiousersequences-0004-select-relid-from-pg_catalog.pg_statio_user_sequences-order"},
				},
			},
		},
	})
}

func TestPgStatioUserTables(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_statio_user_tables",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_statio_user_tables";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgstatiousertables-0001-select-*-from-pg_catalog-.", ColumnModes: []string{"structural", "schema"}},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_statio_user_tables";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgstatiousertables-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_statio_user_tables";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgstatiousertables-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT relid FROM PG_catalog.pg_STATIO_USER_TABLES ORDER BY relid;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatiousertables-0004-select-relid-from-pg_catalog.pg_statio_user_tables-order"},
				},
			},
		},
	})
}

func TestPgStatistic(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_statistic",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_statistic";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgstatistic-0001-select-*-from-pg_catalog-.", ColumnModes: []string{"structural", "structural", "structural", "structural", "structural",

						// Different cases and quoted, so it fails
						"structural", "structural", "structural", "structural", "structural", "structural", "structural", "structural", "structural", "structural", "structural",

						// Different cases but non-quoted, so it works
						"structural", "structural", "structural", "structural", "structural", "structural", "structural", "structural", "structural", "structural", "schema"}},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_statistic";`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatistic-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_statistic";`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatistic-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT starelid FROM PG_catalog.pg_STATISTIC ORDER BY starelid;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatistic-0004-select-starelid-from-pg_catalog.pg_statistic-order"},
				},
			},
		},
	})
}

func TestPgStatisticExt(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_statistic_ext",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_statistic_ext";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgstatisticext-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_statistic_ext";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgstatisticext-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_statistic_ext";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgstatisticext-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT stxname FROM PG_catalog.pg_STATISTIC_EXT ORDER BY stxname;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatisticext-0004-select-stxname-from-pg_catalog.pg_statistic_ext-order"},
				},
			},
		},
	})
}

func TestPgStatisticExtData(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_statistic_ext_data",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_statistic_ext_data";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgstatisticextdata-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_statistic_ext_data";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgstatisticextdata-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_statistic_ext_data";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgstatisticextdata-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT stxoid FROM PG_catalog.pg_STATISTIC_EXT_DATA ORDER BY stxoid;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatisticextdata-0004-select-stxoid-from-pg_catalog.pg_statistic_ext_data-order"},
				},
			},
		},
	})
}

func TestPgStats(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_stats",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_stats";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgstats-0001-select-*-from-pg_catalog-.", ColumnModes: []string{"structural", "structural", "structural", "structural", "structural",

						// Different cases and quoted, so it fails
						"structural", "structural", "structural", "structural", "schema"}},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_stats";`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstats-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_stats";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgstats-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT attname FROM PG_catalog.pg_STATS ORDER BY attname;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstats-0004-select-attname-from-pg_catalog.pg_stats-order"},
				},
			},
		},
	})
}

func TestPgStatsExt(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_stats_ext",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_stats_ext";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgstatsext-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_stats_ext";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgstatsext-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_stats_ext";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgstatsext-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT statistics_name FROM PG_catalog.pg_STATS_EXT ORDER BY statistics_name;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatsext-0004-select-statistics_name-from-pg_catalog.pg_stats_ext-order"},
				},
			},
		},
	})
}

func TestPgStatsExtExprs(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_stats_ext_exprs",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_stats_ext_exprs";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgstatsextexprs-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_stats_ext_exprs";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgstatsextexprs-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_stats_ext_exprs";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgstatsextexprs-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT statistics_name FROM PG_catalog.pg_STATS_EXT_EXPRS ORDER BY statistics_name;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgstatsextexprs-0004-select-statistics_name-from-pg_catalog.pg_stats_ext_exprs-order"},
				},
			},
		},
	})
}

func TestPgSubscription(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_subscription",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_subscription";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgsubscription-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_subscription";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgsubscription-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_subscription";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgsubscription-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT subname FROM PG_catalog.pg_SUBSCRIPTION ORDER BY subname;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgsubscription-0004-select-subname-from-pg_catalog.pg_subscription-order"},
				},
			},
		},
	})
}

func TestPgSubscriptionRel(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_subscription_rel",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_subscription_rel";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgsubscriptionrel-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_subscription_rel";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgsubscriptionrel-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_subscription_rel";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgsubscriptionrel-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT srsubid FROM PG_catalog.pg_SUBSCRIPTION_REL ORDER BY srsubid;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgsubscriptionrel-0004-select-srsubid-from-pg_catalog.pg_subscription_rel-order"},
				},
			},
		},
	})
}

func TestPgTables(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_tables",
			SetUpScript: []string{
				`create table t1 (pk int primary key, v1 int);`,
				`create table t2 (pk int primary key, v1 int);`,
				`CREATE SCHEMA testschema;`,
				`SET search_path TO testschema;`,
				`CREATE TABLE testing (pk INT primary key, v1 INT);`,

				// Should show classes for all schemas
				`CREATE SCHEMA testschema2;`,
				`SET search_path TO testschema2;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_tables" WHERE tablename='testing' order by 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgtables-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT count(*) FROM "pg_catalog"."pg_tables" WHERE schemaname='pg_catalog';`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgtables-0002-select-count-*-from-pg_catalog"},
				},
				{ // Different cases and quoted, so it fails
					Query: `SELECT * FROM "PG_catalog"."pg_tables";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgtables-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_tables";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgtables-0004-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT schemaname, tablename FROM PG_catalog.pg_TABLES WHERE schemaname not in ('information_schema', 'dolt', 'public') ORDER BY tablename DESC LIMIT 3;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgtables-0005-select-schemaname-tablename-from-pg_catalog.pg_tables", ColumnModes: []string{"schema"}},
				},
				{
					Query: "SELECT schemaname, tablename FROM PG_catalog.pg_TABLES WHERE schemaname  ='public' ORDER BY tablename;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgtables-0006-select-schemaname-tablename-from-pg_catalog.pg_tables"},
				},
			},
		},
	})
}

func TestPgTablespace(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_tablespace",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_tablespace";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgtablespace-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_tablespace";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgtablespace-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_tablespace";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgtablespace-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT spcname FROM PG_catalog.pg_TABLESPACE ORDER BY spcname;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgtablespace-0004-select-spcname-from-pg_catalog.pg_tablespace-order"},
				},
			},
		},
	})
}

func TestPgTimezoneAbbrevs(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_timezone_abbrevs",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_timezone_abbrevs";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgtimezoneabbrevs-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_timezone_abbrevs";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgtimezoneabbrevs-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_timezone_abbrevs";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgtimezoneabbrevs-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT abbrev FROM PG_catalog.pg_TIMEZONE_ABBREVS ORDER BY abbrev;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgtimezoneabbrevs-0004-select-abbrev-from-pg_catalog.pg_timezone_abbrevs-order"},
				},
			},
		},
	})
}

func TestPgPgTimezoneNames(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_timezone_names",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_timezone_names";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgpgtimezonenames-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_timezone_names";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgpgtimezonenames-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_timezone_names";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgpgtimezonenames-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT name FROM PG_catalog.pg_TIMEZONE_NAMES ORDER BY name;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgpgtimezonenames-0004-select-name-from-pg_catalog.pg_timezone_names-order"},
				},
			},
		},
	})
}

func TestPgTransform(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_transform",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_transform";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgtransform-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_transform";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgtransform-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_transform";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgtransform-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT oid FROM PG_catalog.pg_TRANSFORM ORDER BY oid;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgtransform-0004-select-oid-from-pg_catalog.pg_transform-order"},
				},
			},
		},
	})
}

func TestPgTrigger(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_trigger",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_trigger";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgtrigger-0001-select-*-from-pg_catalog-.", ColumnModes: []string{"structural", "structural", "structural", "structural", "structural",

						// Different cases and quoted, so it fails
						"structural", "structural", "structural", "structural", "structural", "structural", "structural", "structural", "structural", "structural", "bytea"}},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_trigger";`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgtrigger-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_trigger";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgtrigger-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT tgname FROM PG_catalog.pg_TRIGGER ORDER BY tgname;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgtrigger-0004-select-tgname-from-pg_catalog.pg_trigger-order"},
				},
			},
		},
	})
}

func TestPgTsConfig(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_ts_config",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_ts_config";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgtsconfig-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_ts_config";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgtsconfig-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_ts_config";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgtsconfig-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT cfgname FROM PG_catalog.pg_TS_CONFIG ORDER BY cfgname;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgtsconfig-0004-select-cfgname-from-pg_catalog.pg_ts_config-order"},
				},
			},
		},
	})
}

func TestPgTsConfigMap(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_ts_config_map",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_ts_config_map";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgtsconfigmap-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_ts_config_map";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgtsconfigmap-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_ts_config_map";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgtsconfigmap-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT mapcfg FROM PG_catalog.pg_TS_CONFIG_MAP ORDER BY mapcfg;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgtsconfigmap-0004-select-mapcfg-from-pg_catalog.pg_ts_config_map-order"},
				},
			},
		},
	})
}

func TestPgTsDict(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_ts_dict",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_ts_dict";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgtsdict-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_ts_dict";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgtsdict-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_ts_dict";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgtsdict-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT dictname FROM PG_catalog.pg_TS_DICT ORDER BY dictname;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgtsdict-0004-select-dictname-from-pg_catalog.pg_ts_dict-order"},
				},
			},
		},
	})
}

func TestPgTsParser(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_ts_parser",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_ts_parser";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgtsparser-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_ts_parser";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgtsparser-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_ts_parser";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgtsparser-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT prsname FROM PG_catalog.pg_TS_PARSER ORDER BY prsname;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgtsparser-0004-select-prsname-from-pg_catalog.pg_ts_parser-order"},
				},
			},
		},
	})
}

func TestPgTsTemplate(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_ts_template",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_ts_template";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgtstemplate-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_ts_template";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgtstemplate-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_ts_template";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgtstemplate-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT tmplname FROM PG_catalog.pg_TS_TEMPLATE ORDER BY tmplname;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgtstemplate-0004-select-tmplname-from-pg_catalog.pg_ts_template-order"},
				},
			},
		},
	})
}

func TestPgType(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_type",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT oid, typname, typnamespace, typowner, typsubscript, typinput, typoutput, typreceive, typsend, typmodin, typmodout, typanalyze, tableoid
						FROM "pg_catalog"."pg_type" WHERE typname = 'float8' order by 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgtype-0001-select-oid-typname-typnamespace-typowner"},
				},
				{ // Different cases and quoted, so it fails
					Query: `SELECT * FROM "PG_catalog"."pg_type";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgtype-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_type";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgtype-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT typname FROM PG_catalog.pg_TYPE WHERE typname LIKE '%char' ORDER BY typname;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgtype-0004-select-typname-from-pg_catalog.pg_type-where"},
				},
				{
					Query: `SELECT t1.oid, t1.typname as basetype, t2.typname as arraytype, t2.typsubscript
					FROM   pg_type t1 LEFT JOIN pg_type t2 ON (t1.typarray = t2.oid)
					WHERE  t1.typarray <> 0 AND (t2.oid IS NULL OR t2.typsubscript::regproc <> 'array_subscript_handler'::regproc);`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgtype-0005-select-t1.oid-t1.typname-as-basetype"},
				},
				{
					Skip: true, // TODO: ERROR: function internal_binary_operator_func_<>(text, regproc) does not exist
					Query: `SELECT t1.oid, t1.typname as basetype, t2.typname as arraytype, t2.typsubscript
					FROM   pg_type t1 LEFT JOIN pg_type t2 ON (t1.typarray = t2.oid)
					WHERE  t1.typarray <> 0 AND (t2.oid IS NULL OR t2.typsubscript <> 'array_subscript_handler'::regproc);`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgtype-0006-select-t1.oid-t1.typname-as-basetype"},
				},
			},
		},
		{
			Name: "pg_type with regtype",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT oid, typname, typnamespace, typowner, typsubscript, typinput, typoutput, typreceive, typsend, typmodin, typmodout, typanalyze, tableoid
						FROM "pg_catalog"."pg_type" WHERE oid='float8'::regtype;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgtype-0007-select-oid-typname-typnamespace-typowner"},
				},
				{
					Query: `SELECT oid, typname FROM "pg_catalog"."pg_type" WHERE oid='double precision'::regtype;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgtype-0008-select-oid-typname-from-pg_catalog"},
				},
				{
					Query: `SELECT oid, typname FROM "pg_catalog"."pg_type" WHERE oid='DOUBLE PRECISION'::regtype;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgtype-0009-select-oid-typname-from-pg_catalog"},
				},
				{
					Query: `SELECT oid, typname FROM "pg_catalog"."pg_type" WHERE oid='pg_catalog.float8'::regtype;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgtype-0010-select-oid-typname-from-pg_catalog"},
				},
				{
					Query:       `SELECT oid, typname FROM "pg_catalog"."pg_type" WHERE oid='public.float8'::regtype;`,
					ExpectedErr: `type "public.float8" does not exist`,
				},
				{
					Query: `SELECT oid, typname FROM "pg_catalog"."pg_type" WHERE oid='VARCHAR'::regtype;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgtype-0012-select-oid-typname-from-pg_catalog"},
				},
				{
					Query: `SELECT oid, typname FROM "pg_catalog"."pg_type" WHERE oid='1043'::regtype;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgtype-0013-select-oid-typname-from-pg_catalog"},
				},
				{
					Query: `SELECT oid, typname FROM "pg_catalog"."pg_type" WHERE oid='VARCHAR(10)'::regtype;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgtype-0014-select-oid-typname-from-pg_catalog"},
				},
				{
					Query: `SELECT oid, typname FROM "pg_catalog"."pg_type" WHERE oid='character varying'::regtype;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgtype-0015-select-oid-typname-from-pg_catalog"},
				},
				{
					Query: `SELECT oid, typname FROM "pg_catalog"."pg_type" WHERE oid='timestamptz'::regtype;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgtype-0016-select-oid-typname-from-pg_catalog"},
				},
				{
					Query: `SELECT oid, typname FROM "pg_catalog"."pg_type" WHERE oid='timestamp with time zone'::regtype;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgtype-0017-select-oid-typname-from-pg_catalog"},
				},
				{
					Query: `SELECT oid, typname FROM "pg_catalog"."pg_type" WHERE oid='regtype'::regtype;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgtype-0018-select-oid-typname-from-pg_catalog"},
				},
				{
					Query: `SELECT oid, typname, typnamespace, typowner, typsubscript, typelem, typarray, typinput, typoutput, typreceive, typsend, typmodin, typmodout, typanalyze, tableoid
						FROM "pg_catalog"."pg_type" WHERE oid='integer[]'::regtype;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgtype-0019-select-oid-typname-typnamespace-typowner"},
				},
				{
					Query: `SELECT oid, typname, typnamespace, typowner, typsubscript, typelem, typarray, typinput, typoutput, typreceive, typsend, typmodin, typmodout, typanalyze, tableoid
						FROM "pg_catalog"."pg_type" WHERE oid='anyarray'::regtype;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgtype-0020-select-oid-typname-typnamespace-typowner"},
				},
				{
					Query: `SELECT oid, typname, typnamespace, typowner, typsubscript, typelem, typarray, typinput, typoutput, typreceive, typsend, typmodin, typmodout, typanalyze, tableoid
						FROM "pg_catalog"."pg_type" WHERE oid='anyelement'::regtype;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgtype-0021-select-oid-typname-typnamespace-typowner"},
				},
				{
					Query: `SELECT oid, typname, typnamespace, typowner, typsubscript, typelem, typarray, typinput, typoutput, typreceive, typsend, typmodin, typmodout, typanalyze, tableoid
						FROM "pg_catalog"."pg_type" WHERE oid='json'::regtype;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgtype-0022-select-oid-typname-typnamespace-typowner"},
				},
				{
					Query: `SELECT oid, typname, typnamespace, typowner, typsubscript, typelem, typarray, typinput, typoutput, typreceive, typsend, typmodin, typmodout, typanalyze, tableoid
						FROM "pg_catalog"."pg_type" WHERE oid='char'::regtype;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgtype-0023-select-oid-typname-typnamespace-typowner"},
				},
				{
					Query: `SELECT oid, typname, typnamespace, typowner, typsubscript, typelem, typarray, typinput, typoutput, typreceive, typsend, typmodin, typmodout, typanalyze, tableoid
						FROM "pg_catalog"."pg_type" WHERE oid='"char"'::regtype;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgtype-0024-select-oid-typname-typnamespace-typowner"},
				},
			},
		},
		{
			Name: "user defined type",
			SetUpScript: []string{
				`CREATE DOMAIN domain_type AS INTEGER NOT NULL;`,
				`CREATE TYPE enum_type AS ENUM ('1','2','3')`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT oid, typname, typnamespace, typowner, typtype, typcategory, typsubscript, typelem, typarray, typinput, typoutput, typreceive, typsend, typmodin, typmodout, typanalyze, typbasetype, tableoid
						FROM "pg_catalog"."pg_type" WHERE typname = 'domain_type' order by 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgtype-0025-select-oid-typname-typnamespace-typowner"},
				},
				{
					Query: `SELECT oid, typname, typnamespace, typowner, typtype, typcategory, typsubscript, typelem, typarray, typinput, typoutput, typreceive, typsend, typmodin, typmodout, typanalyze, typbasetype, tableoid
						FROM "pg_catalog"."pg_type" WHERE typname = '_domain_type' order by 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgtype-0026-select-oid-typname-typnamespace-typowner"},
				},
				{
					Query: `SELECT oid, typname, typnamespace, typowner, typtype, typcategory, typsubscript, typelem, typarray, typinput, typoutput, typreceive, typsend, typmodin, typmodout, typanalyze, typbasetype, tableoid
						FROM "pg_catalog"."pg_type" WHERE typname = 'enum_type' order by 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgtype-0027-select-oid-typname-typnamespace-typowner"},
				},
				{
					Query: `SELECT oid, typname, typnamespace, typowner, typtype, typcategory, typsubscript, typelem, typarray, typinput, typoutput, typreceive, typsend, typmodin, typmodout, typanalyze, typbasetype, tableoid
						FROM "pg_catalog"."pg_type" WHERE typname = '_enum_type' order by 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgtype-0028-select-oid-typname-typnamespace-typowner"},
				},
			},
		},
	})
}

func TestPgUser(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_user",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT usesysid, usename, usesuper FROM "pg_catalog"."pg_user" ORDER BY usename;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpguser-0001-select-usesysid-usename-usesuper-from"},
				},
				{ // Different cases and quoted, so it fails
					Query: `SELECT * FROM "PG_catalog"."pg_user";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpguser-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_user";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpguser-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT usename FROM PG_catalog.pg_USER ORDER BY usename;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpguser-0004-select-usename-from-pg_catalog.pg_user-order"},
				},
			},
		},
	})
}

func TestPgUserMapping(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_user_mapping",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_user_mapping";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgusermapping-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_user_mapping";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgusermapping-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_user_mapping";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgusermapping-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT umuser FROM PG_catalog.pg_USER_MAPPING ORDER BY umuser;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgusermapping-0004-select-umuser-from-pg_catalog.pg_user_mapping-order"},
				},
			},
		},
	})
}

func TestPgUserMappings(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_user_mappings",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_user_mappings";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"pgcatalog-test-testpgusermappings-0001-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_user_mappings";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgusermappings-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_user_mappings";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgusermappings-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT umid FROM PG_catalog.pg_USER_MAPPINGS ORDER BY umid;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgusermappings-0004-select-umid-from-pg_catalog.pg_user_mappings-order"},
				},
			},
		},
	})
}

func TestPgViews(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_views",
			SetUpScript: []string{
				`CREATE SCHEMA testschema;`,
				`SET search_path TO testschema;`,
				"CREATE TABLE testing (pk INT primary key, v1 INT);",
				`CREATE VIEW testview AS SELECT * FROM testing LIMIT 1;`,
				`CREATE VIEW testview2 AS SELECT * FROM testing LIMIT 2;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_views" WHERE viewname='testview';`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgviews-0001-select-*-from-pg_catalog-."},
				},
				{ // Different cases and quoted, so it fails
					Query: `SELECT * FROM "PG_catalog"."pg_views";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "pgcatalog-test-testpgviews-0002-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_views";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases but non-quoted, so it works
						ID: "pgcatalog-test-testpgviews-0003-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "SELECT viewname FROM PG_catalog.pg_VIEWS ORDER BY viewname;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgviews-0004-select-viewname-from-pg_catalog.pg_views-order"},
				},
			},
		},
	})
}

func TestPgClassIndexes(t *testing.T) {
	sharedSetupScript := []string{
		`create table t1 (a int primary key, b int not null)`,
		`create table t2 (c int primary key, d int not null)`,
		`create index on t2 (d)`,
	}

	RunScripts(t, []ScriptTest{
		{
			Name:        "pg_class index lookup",
			SetUpScript: sharedSetupScript,
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT c.oid
FROM pg_catalog.pg_class c 
WHERE c.relname = 't2' and c.relnamespace = 2200 -- public
ORDER BY 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgclassindexes-0001-select-c.oid-from-pg_catalog.pg_class-c"},
				},
				{
					Query: `SELECT c.relname
FROM pg_catalog.pg_class c 
WHERE c.relname > 't' AND c.relname < 't2' AND c.relnamespace = 2200 -- public
AND relkind = 'r'
ORDER BY 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgclassindexes-0002-select-c.relname-from-pg_catalog.pg_class-c"},
				},
				{
					Query: `SELECT c.relname
FROM pg_catalog.pg_class c 
WHERE c.relname >= 't1' AND c.relname <= 't2' AND c.relnamespace = 2200 -- public
AND relkind = 'r'
ORDER BY 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgclassindexes-0003-select-c.relname-from-pg_catalog.pg_class-c"},
				},
				{
					Query: `SELECT c.relname
FROM pg_catalog.pg_class c 
WHERE c.relname >= 't1' AND c.relname < 't2' AND c.relnamespace = 2200 -- public
AND relkind = 'r'
ORDER BY 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgclassindexes-0004-select-c.relname-from-pg_catalog.pg_class-c"},
				},
				{
					Query: `SELECT c.relname
FROM pg_catalog.pg_class c 
WHERE c.relname > 't1' AND c.relname <= 't2' AND c.relnamespace = 2200 -- public
AND relkind = 'r'
ORDER BY 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgclassindexes-0005-select-c.relname-from-pg_catalog.pg_class-c"},
				},
				{
					Query: `SELECT c.relname
FROM pg_catalog.pg_class c 
WHERE c.relname > 't1' AND c.relname <= 't2' AND c.relnamespace > 2199 AND c.relnamespace < 2201 -- public
AND relkind = 'r'
ORDER BY 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgclassindexes-0006-select-c.relname-from-pg_catalog.pg_class-c"},
				},
				{
					Query: `SELECT c.relname
FROM pg_catalog.pg_class c 
WHERE c.oid = 1496157034
ORDER BY 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgclassindexes-0007-select-c.relname-from-pg_catalog.pg_class-c"},
				},
				{
					Query: `SELECT c.relname
FROM pg_catalog.pg_class c 
WHERE c.oid = '1496157034'
ORDER BY 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgclassindexes-0008-select-c.relname-from-pg_catalog.pg_class-c"},
				},
				{
					Query: `SELECT c.relname
FROM pg_catalog.pg_class c 
WHERE c.oid IN (1496157034, 1496157035) 
ORDER BY 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgclassindexes-0009-select-c.relname-from-pg_catalog.pg_class-c"},
				},
				{
					Query: `SELECT c.relname
FROM pg_catalog.pg_class c 
WHERE c.oid > 1496157033 AND c.oid < 1496157035
ORDER BY 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgclassindexes-0010-select-c.relname-from-pg_catalog.pg_class-c"},
				},
				{
					// This is to make sure a full range scan works (we don't support a full range scan on the index yet)
					Query: `SELECT relname from pg_catalog.pg_class order by oid limit 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgclassindexes-0011-select-relname-from-pg_catalog.pg_class-order"},
				},
				{
					Query: `EXPLAIN SELECT c.oid
FROM pg_catalog.pg_class c 
WHERE c.relname = 't2' and c.relnamespace = 2200
ORDER BY 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgclassindexes-0012-explain-select-c.oid-from-pg_catalog.pg_class", ColumnModes: []string{"explain"}},
				},
				{
					Query: `EXPLAIN SELECT c.relname
FROM pg_catalog.pg_class c
WHERE c.relname > 't' AND c.relname < 't2' AND c.relnamespace = 2200 -- public
AND relkind = 'r'
ORDER BY 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgclassindexes-0013-explain-select-c.relname-from-pg_catalog.pg_class", ColumnModes: []string{"explain"}},
				},
				{
					Query: `EXPLAIN SELECT c.relname
FROM pg_catalog.pg_class c 
WHERE c.oid = 1496157034
ORDER BY 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgclassindexes-0014-explain-select-c.relname-from-pg_catalog.pg_class", ColumnModes: []string{"explain"}},
				},
				{
					Query: `EXPLAIN SELECT c.relname
FROM pg_catalog.pg_class c 
WHERE c.oid > 1496157033 AND c.oid < 1496157035
ORDER BY 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgclassindexes-0015-explain-select-c.relname-from-pg_catalog.pg_class", ColumnModes: []string{"explain"}},
				},
				{
					Query: `EXPLAIN SELECT c.relname
FROM pg_catalog.pg_class c 
WHERE c.oid IN (1496157034, 1496157035) 
ORDER BY 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgclassindexes-0016-explain-select-c.relname-from-pg_catalog.pg_class", ColumnModes: []string{"explain"}},
				},
			},
		},
		{
			Name:        "join on pg_class",
			SetUpScript: sharedSetupScript,
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT c.relname, a.attname 
FROM pg_catalog.pg_class c 
    JOIN pg_catalog.pg_attribute a 
        ON c.oid = a.attrelid 
WHERE c.relkind = 'r' AND a.attnum > 0 
  AND NOT a.attisdropped
  AND c.relname = 't2'
ORDER BY 1,2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgclassindexes-0017-select-c.relname-a.attname-from-pg_catalog.pg_class"},
				},
				{
					Query: `EXPLAIN SELECT c.relname, a.attname 
FROM pg_catalog.pg_class c 
    JOIN pg_catalog.pg_attribute a 
        ON c.oid = a.attrelid 
WHERE c.relkind = 'r' AND a.attnum > 0 
  AND NOT a.attisdropped
  AND c.relname = 't2'
ORDER BY 1,2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgclassindexes-0018-explain-select-c.relname-a.attname-from", ColumnModes: []string{"explain"}},
				},
			},
		},
		{
			Name:        "left join with nil left result",
			SetUpScript: sharedSetupScript,
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT n.nspname as "Schema",
  c.relname as "Name",
  pg_catalog.pg_get_userbyid(c.relowner) as "Owner",
 c2.oid::pg_catalog.regclass as "Table"
FROM pg_catalog.pg_class c
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
     LEFT JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid
     LEFT JOIN pg_catalog.pg_class c2 ON i.indrelid = c2.oid
WHERE c.relkind IN ('I','')
 AND NOT c.relispartition
      AND n.nspname <> 'pg_catalog'
      AND n.nspname !~ '^pg_toast'
      AND n.nspname <> 'information_schema'
  AND pg_catalog.pg_table_is_visible(c.oid)
ORDER BY "Schema", "Name"`,
				},
			},
		},
		{
			Name: "tables in multiple schemas",
			SetUpScript: []string{
				`CREATE SCHEMA s1;`,
				`CREATE SCHEMA s2;`,
				`create schema s3;`,
				`CREATE TABLE s2.t (a INT);`,
				`CREATE TABLE s1.t (b INT);`,
				`CREATE TABLE s3.t (c INT);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `select relname, nspname FROM pg_catalog.pg_class c 
join pg_catalog.pg_namespace n on c.relnamespace = n.oid
where c.relname = 't' and c.relkind = 'r'
order by 1,2`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgclassindexes-0019-select-relname-nspname-from-pg_catalog.pg_class", ColumnModes: []string{"structural", "schema"}},
				},
				{
					Query: `select relname, relnamespace FROM pg_catalog.pg_class c 
where c.relname = 't' and c.relkind = 'r'
order by 1,2`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgclassindexes-0020-select-relname-relnamespace-from-pg_catalog.pg_class"},
				},
				{
					Query: `explain select relname, nspname FROM pg_catalog.pg_class c 
join pg_catalog.pg_namespace n on c.relnamespace = n.oid
where c.relname = 't' and c.relkind = 'r'
order by 1,2`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgclassindexes-0021-explain-select-relname-nspname-from", ColumnModes: []string{"explain"}},
				},
				{
					Query: `explain select relname, relnamespace FROM pg_catalog.pg_class c
where c.relname = 't' and c.relkind = 'r'
order by 1,2`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgclassindexes-0022-explain-select-relname-relnamespace-from", ColumnModes: []string{"explain"}},
				},
			},
		},
		{
			Name: "regression test for in-memory index corruption (caused by empty schema name)",
			SetUpScript: []string{
				`CREATE SCHEMA AUTHORIZATION s1`,
				`create table idxpart (a int, b int, c text) partition by range (a);`,
				`create index idxpart_idx on idxpart (a);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `select count(*) from pg_class where relname = 'idxpart_idx';`,
					Expected: []sql.Row{{1}},
				},
			},
		},
	})
}

func TestPgIndexIndexes(t *testing.T) {
	sharedSetupScript := []string{
		`create table t1 (a int primary key, b int not null)`,
		`create table t2 (c int primary key, d int not null)`,
		`create index on t2 (d)`,
	}

	RunScripts(t, []ScriptTest{
		{
			Name:        "pg_index index lookup",
			SetUpScript: sharedSetupScript,
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM pg_catalog.pg_index i 
WHERE i.indrelid = 1496157034 order by 1`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgindexindexes-0001-select-*-from-pg_catalog.pg_index-i"},
				},
				{
					Query: `SELECT c.relname, c2.relname FROM pg_catalog.pg_index i
         join pg_class c on i.indrelid = c.oid
         join pg_class c2 on i.indexrelid = c2.oid
WHERE c.relname = 't2' order by 1,2`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgindexindexes-0002-select-c.relname-c2.relname-from-pg_catalog.pg_index"},
				},
				{
					Query: `SELECT i.indrelid FROM pg_catalog.pg_index i 
WHERE i.indexrelid = (SELECT c.oid FROM pg_catalog.pg_class c WHERE c.relname = 't2_pkey')
ORDER BY 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgindexindexes-0003-select-i.indrelid-from-pg_catalog.pg_index-i"},
				},
				{
					Query: `SELECT count(*) FROM pg_catalog.pg_index i 
WHERE i.indrelid = 1496157034`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgindexindexes-0004-select-count-*-from-pg_catalog.pg_index"},
				},
				{
					Query: `SELECT i.indisprimary FROM pg_catalog.pg_index i 
WHERE i.indrelid = 1496157034 AND i.indisprimary = true
ORDER BY 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgindexindexes-0005-select-i.indisprimary-from-pg_catalog.pg_index-i"},
				},
				{
					Query: `SELECT COUNT(*) FROM pg_catalog.pg_index i 
WHERE i.indrelid IN (1496157033, 1496157034)`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgindexindexes-0006-select-count-*-from-pg_catalog.pg_index"},
				},
				{
					// TODO: this uses an index but the plan doesn't show it because of prepared statements
					Query: `EXPLAIN SELECT i.indrelid FROM pg_catalog.pg_index i 
WHERE i.indexrelid = (SELECT c.oid FROM pg_catalog.pg_class c WHERE c.relname = 't1_pkey')
ORDER BY 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgindexindexes-0007-explain-select-i.indrelid-from-pg_catalog.pg_index", ColumnModes: []string{"explain"}},
				},
				{
					Query: `EXPLAIN SELECT COUNT(*) FROM pg_catalog.pg_index i 
WHERE i.indrelid = 1496157034 ORDER BY 1`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgindexindexes-0008-explain-select-count-*-from", ColumnModes: []string{"explain"}},
				},
				{
					Query: `EXPLAIN SELECT COUNT(*) FROM pg_catalog.pg_index i 
WHERE i.indrelid IN (1496157033, 1496157034) ORDER BY 1`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgindexindexes-0009-explain-select-count-*-from", ColumnModes: []string{"explain"}},
				},
			},
		},
	})
}

func TestPgTypeIndexes(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_type_oid_index",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT typname FROM pg_catalog.pg_type WHERE oid = 23 ORDER BY 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgtypeindexes-0001-select-typname-from-pg_catalog.pg_type-where"},
				},
				{
					Query: `SELECT typname FROM pg_catalog.pg_type WHERE oid = '23' ORDER BY 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgtypeindexes-0002-select-typname-from-pg_catalog.pg_type-where"},
				},
				{
					Query: `SELECT typname FROM pg_catalog.pg_type WHERE oid > 22 AND oid < 25 ORDER BY typname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgtypeindexes-0003-select-typname-from-pg_catalog.pg_type-where"},
				},
				{
					Query: `SELECT typname FROM pg_catalog.pg_type WHERE oid IN (23, 25) ORDER BY typname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgtypeindexes-0004-select-typname-from-pg_catalog.pg_type-where"},
				},
				{
					// Full scan still works without index filter
					Query: `SELECT typname FROM pg_catalog.pg_type ORDER BY oid LIMIT 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgtypeindexes-0005-select-typname-from-pg_catalog.pg_type-order"},
				},
				{
					Query: `EXPLAIN SELECT typname FROM pg_catalog.pg_type WHERE oid = 23 ORDER BY 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgtypeindexes-0006-explain-select-typname-from-pg_catalog.pg_type", ColumnModes: []string{"explain"}},
				},
				{
					Query: `EXPLAIN SELECT typname FROM pg_catalog.pg_type WHERE oid > 22 AND oid < 25 ORDER BY 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgtypeindexes-0007-explain-select-typname-from-pg_catalog.pg_type", ColumnModes: []string{"explain"}},
				},
				{
					Query: `EXPLAIN SELECT typname FROM pg_catalog.pg_type WHERE oid IN (23, 25) ORDER BY typname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgtypeindexes-0008-explain-select-typname-from-pg_catalog.pg_type", ColumnModes: []string{"explain"}},
				},
			},
		},
		{
			Name: "pg_type_typname_nsp_index",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT oid FROM pg_catalog.pg_type WHERE typname = 'int4' AND typnamespace = 11 ORDER BY 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgtypeindexes-0009-select-oid-from-pg_catalog.pg_type-where"},
				},
				{
					Query: `SELECT typname FROM pg_catalog.pg_type WHERE typname > 'int2' AND typname < 'int8' AND typnamespace = 11 ORDER BY 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgtypeindexes-0010-select-typname-from-pg_catalog.pg_type-where"},
				},
				{
					Query: `SELECT typname FROM pg_catalog.pg_type WHERE typname >= 'int2' AND typname <= 'int4' AND typnamespace = 11 ORDER BY 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgtypeindexes-0011-select-typname-from-pg_catalog.pg_type-where"},
				},
				{
					Query: `SELECT typname FROM pg_catalog.pg_type WHERE typname > 'int2' AND typname <= 'int4' AND typnamespace = 11 ORDER BY 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgtypeindexes-0012-select-typname-from-pg_catalog.pg_type-where"},
				},
				{
					Query: `SELECT typname FROM pg_catalog.pg_type WHERE typname >= 'int4' AND typname <= 'int4' AND typnamespace = 11 ORDER BY 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgtypeindexes-0013-select-typname-from-pg_catalog.pg_type-where"},
				},
				{
					Query: `EXPLAIN SELECT oid FROM pg_catalog.pg_type WHERE typname = 'int4' AND typnamespace = 11 ORDER BY 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgtypeindexes-0014-explain-select-oid-from-pg_catalog.pg_type", ColumnModes: []string{"explain"}},
				},
				{
					Query: `EXPLAIN SELECT typname FROM pg_catalog.pg_type WHERE typname > 'int2' AND typname < 'int8' AND typnamespace = 11 ORDER BY 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgtypeindexes-0015-explain-select-typname-from-pg_catalog.pg_type", ColumnModes: []string{"explain"}},
				},
			},
		},
		{
			Name: "join on pg_type using index",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT t.typname, n.nspname
  FROM pg_catalog.pg_type t
  JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid
  WHERE t.typname = 'int4'
  ORDER BY 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgtypeindexes-0016-select-t.typname-n.nspname-from-pg_catalog.pg_type"},
				},
				{
					Query: `EXPLAIN SELECT t.typname, n.nspname
								FROM pg_catalog.pg_type t
								JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid
					   			WHERE t.typname = 'int4'
								ORDER BY 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgtypeindexes-0017-explain-select-t.typname-n.nspname-from", ColumnModes: []string{"explain"}},
				},
			},
		},
	})
}

func TestSqlAlchemyQueries(t *testing.T) {
	sharedSetupScript := []string{
		`create table t1 (a int primary key, b int not null)`,
		`create table t2 (a int primary key, b int not null)`,
		`create index on t2 (b)`,
		`set dolt_show_system_tables=1`,
	}

	RunScripts(t, []ScriptTest{
		{
			Name:        "schema for dolt_log",
			SetUpScript: sharedSetupScript,
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT pg_catalog.pg_attribute.attname AS name, pg_catalog.format_type(pg_catalog.pg_attribute.atttypid, pg_catalog.pg_attribute.atttypmod) AS format_type, (SELECT pg_catalog.pg_get_expr(pg_catalog.pg_attrdef.adbin, pg_catalog.pg_attrdef.adrelid) AS pg_get_expr_1 
FROM pg_catalog.pg_attrdef 
WHERE pg_catalog.pg_attrdef.adrelid = pg_catalog.pg_attribute.attrelid AND pg_catalog.pg_attrdef.adnum = pg_catalog.pg_attribute.attnum AND pg_catalog.pg_attribute.atthasdef) AS "default", pg_catalog.pg_attribute.attnotnull AS not_null, pg_catalog.pg_class.relname AS table_name, pg_catalog.pg_description.description AS comment, pg_catalog.pg_attribute.attgenerated AS generated, (SELECT json_build_object('always', pg_catalog.pg_attribute.attidentity = 'a', 'start', pg_catalog.pg_sequence.seqstart, 'increment', pg_catalog.pg_sequence.seqincrement, 'minvalue', pg_catalog.pg_sequence.seqmin, 'maxvalue', pg_catalog.pg_sequence.seqmax, 'cache', pg_catalog.pg_sequence.seqcache, 'cycle', pg_catalog.pg_sequence.seqcycle) AS json_build_object_1 
FROM pg_catalog.pg_sequence 
WHERE pg_catalog.pg_attribute.attidentity != '' AND pg_catalog.pg_sequence.seqrelid = CAST(CAST(pg_catalog.pg_get_serial_sequence(CAST(CAST(pg_catalog.pg_attribute.attrelid AS REGCLASS) AS TEXT), pg_catalog.pg_attribute.attname) AS REGCLASS) AS OID)) AS identity_options 
FROM pg_catalog.pg_class LEFT OUTER JOIN pg_catalog.pg_attribute ON pg_catalog.pg_class.oid = pg_catalog.pg_attribute.attrelid AND pg_catalog.pg_attribute.attnum > 0 AND NOT pg_catalog.pg_attribute.attisdropped LEFT OUTER JOIN pg_catalog.pg_description ON pg_catalog.pg_description.objoid = pg_catalog.pg_attribute.attrelid AND pg_catalog.pg_description.objsubid = pg_catalog.pg_attribute.attnum JOIN pg_catalog.pg_namespace ON pg_catalog.pg_namespace.oid = pg_catalog.pg_class.relnamespace 
WHERE pg_catalog.pg_class.relkind = ANY (ARRAY['r', 'p', 'f', 'v', 'm']) AND pg_catalog.pg_table_is_visible(pg_catalog.pg_class.oid) AND pg_catalog.pg_namespace.nspname != 'pg_catalog' AND pg_catalog.pg_class.relname IN ('dolt_log') ORDER BY pg_catalog.pg_class.relname, pg_catalog.pg_attribute.attnum`,
					Expected: []sql.Row{
						{"commit_hash", "text", nil, "t", "dolt_log", nil, "", nil},
						{"committer", "text", nil, "t", "dolt_log", nil, "", nil},
						{"email", "text", nil, "t", "dolt_log", nil, "", nil},
						{"date", "timestamp without time zone", nil, "t", "dolt_log", nil, "", nil},
						{"message", "text", nil, "t", "dolt_log", nil, "", nil},
						{"commit_order", "numeric", nil, "t", "dolt_log", nil, "", nil},
						{"parents", "text", nil, "f", "dolt_log", nil, "", nil},
						{"refs", "text", nil, "t", "dolt_log", nil, "", nil},
						{"signature", "text", nil, "f", "dolt_log", nil, "", nil},
						{"author", "text", nil, "t", "dolt_log", nil, "", nil},
						{"author_email", "text", nil, "t", "dolt_log", nil, "", nil},
						{"author_date", "timestamp without time zone", nil, "t", "dolt_log", nil, "", nil},
					},
				},
			},
		},
		{
			Name:        "type queries",
			SetUpScript: sharedSetupScript,
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT pg_catalog.pg_type.typname AS name,
       pg_catalog.pg_type_is_visible(pg_catalog.pg_type.oid) AS visible,
       pg_catalog.pg_namespace.nspname AS schema,
       lbl_agg.labels AS labels
FROM pg_catalog.pg_type
JOIN pg_catalog.pg_namespace ON pg_catalog.pg_namespace.oid = pg_catalog.pg_type.typnamespace
    LEFT OUTER JOIN 
    (SELECT pg_catalog.pg_enum.enumtypid AS enumtypid, 
    array_agg(CAST(pg_catalog.pg_enum.enumlabel AS TEXT) ORDER BY pg_catalog.pg_enum.enumsortorder) 
    AS labels FROM pg_catalog.pg_enum GROUP BY pg_catalog.pg_enum.enumtypid) AS lbl_agg
    ON pg_catalog.pg_type.oid = lbl_agg.enumtypid WHERE pg_catalog.pg_type.typtype = 'e'
    ORDER BY pg_catalog.pg_namespace.nspname, pg_catalog.pg_type.typname`,
				},
			},
		},
		{
			Name:        "dolt_log schema 2",
			SetUpScript: sharedSetupScript,
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT pg_catalog.pg_attribute.attname AS name,
    pg_catalog.format_type(pg_catalog.pg_attribute.atttypid,
    pg_catalog.pg_attribute.atttypmod) AS format_type,
    (SELECT pg_catalog.pg_get_expr(pg_catalog  .pg_attrdef.adbin, pg_catalog.pg_attrdef.adrelid) AS pg_get_expr_1
			 FROM pg_catalog.pg_attrdef 
			 WHERE pg_catalog.pg_attrdef.adrelid = pg_catalog.pg_attribute.attrelid
				 AND pg_catalog.pg_attrdef.adnum = pg_catalog.pg_attribute.attnum
				 AND pg_catalog.pg_attribute.atthasdef) AS "default",
    pg_catalog.pg_attribute.attnotnull AS not_null,
    pg_catalog.pg_class.relname AS table_name,
    pg_catalog.pg_description.description AS comment,
    pg_catalog.pg_attribute.attgenerated AS generated,
    (SELECT json_build_object('always', pg_catalog.pg_attribute.attidentity = 'a',
                              'start', pg_catalog.pg_sequence.seqstart,
                              'increment', pg_catalog.pg_sequence.seqincrement,
                              'minvalue', pg_catalog.pg_sequence.seqmin,
                              'maxvalue', pg_catalog.pg_sequence.seqmax,
                              'cache', pg_catalog.pg_sequence.seqcache,
                              'cycle', pg_catalog.pg_sequence.seqcycle) AS json_build_object_1
    			FROM pg_catalog.pg_sequence
       		WHERE pg_catalog.pg_attribute.attidentity != ''
       		AND pg_catalog.pg_sequence.seqrelid = CAST(CAST(pg_catalog.pg_get_serial_sequence(CAST(CAST(pg_catalog.pg_attribute.attrelid AS REGCLASS) AS TEXT), pg_catalog.pg_attribute.attname) AS REGCLASS) AS OID)
       ) AS identity_options
   FROM pg_catalog.pg_class
   LEFT OUTER JOIN pg_catalog.pg_attribute ON pg_catalog.pg_class.oid = pg_catalog.pg_attribute.attrelid 
       AND pg_catalog.pg_attribute.attnum > 0 AND NOT pg_catalog.pg_attribute.attisdropped 
       LEFT OUTER JOIN pg_catalog.pg_description ON pg_catalog.pg_description.objoid = pg_catalog.pg_attribute.attrelid 
       AND pg_catalog.pg_description.objsubid = pg_catalog.pg_attribute.attnum
       JOIN pg_catalog.pg_namespace ON pg_catalog.pg_namespace.oid = pg_catalog.pg_class.relnamespace
       WHERE pg_catalog.pg_class.relkind = ANY (ARRAY['r', 'p', 'f  ', 'v', 'm']) 
       AND pg_catalog.pg_table_is_visible(pg_catalog.pg_class.oid) 
       AND pg_catalog.pg_namespace.nspname != 'pg_catalog' 
       AND pg_catalog.pg_class.relname IN ('dolt_log') 
       ORDER BY pg_catalog.pg_class.relname, pg_catalog.pg_attribute.attnum`,
					Expected: []sql.Row{
						{"commit_hash", "text", nil, "t", "dolt_log", nil, "", nil},
						{"committer", "text", nil, "t", "dolt_log", nil, "", nil},
						{"email", "text", nil, "t", "dolt_log", nil, "", nil},
						{"date", "timestamp without time zone", nil, "t", "dolt_log", nil, "", nil},
						{"message", "text", nil, "t", "dolt_log", nil, "", nil},
						{"commit_order", "numeric", nil, "t", "dolt_log", nil, "", nil},
						{"parents", "text", nil, "f", "dolt_log", nil, "", nil},
						{"refs", "text", nil, "t", "dolt_log", nil, "", nil},
						{"signature", "text", nil, "f", "dolt_log", nil, "", nil},
						{"author", "text", nil, "t", "dolt_log", nil, "", nil},
						{"author_email", "text", nil, "t", "dolt_log", nil, "", nil},
						{"author_date", "timestamp without time zone", nil, "t", "dolt_log", nil, "", nil},
					},
				},
			},
		},
		{
			Name:        "constraints",
			SetUpScript: sharedSetupScript,
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT attr.conrelid, 
       array_agg(CAST(attr.attname AS TEXT) ORDER BY attr.ord) AS cols,
       attr.conname,
       min(attr.description) AS description,
       NULL AS extra FROM 
				(SELECT con.conrelid AS conrelid,
				        con.conname AS conname,
				        con.conindid AS conindid,
				        con.description AS description,
				        con.ord AS ord,
				        pg_catalog.pg_attribute.attname AS attname
				 FROM pg_catalog.pg_attribute JOIN 
				     (SELECT pg_catalog.pg_constraint.conrelid AS conrelid,
				             pg_catalog.pg_constraint.conname AS conname,
				             pg_catalog.pg_constraint.conindid AS conindid,
				             unnest(pg_catalog.pg_constraint.conkey) AS attnum,
				             generate_subscripts(pg_catalog.pg_constraint.conkey, 1) AS ord,
				             pg_catalog.pg_description.description AS description 
				      FROM pg_catalog.pg_constraint 
				          LEFT OUTER JOIN pg_catalog.pg_description 
				              ON pg_catalog.pg_description.objoid = pg_catalog.pg_constraint.oid
				      WHERE pg_catalog.pg_constraint.contype = 'p'
				        AND pg_catalog.pg_constraint.conrelid IN (3491847678)) AS con
				     ON pg_catalog.pg_attribute.attnum = con.attnum 
				            AND pg_catalog.pg_attribute.attrelid = con.conrelid
				 WHERE con.conrelid IN (3491847678)) AS attr 
            GROUP BY attr.conrelid, attr.conname ORDER BY attr.conrelid, attr.conname`,
				},
			},
		},
		{
			Name:        "has constraints",
			SetUpScript: sharedSetupScript,
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT pg_catalog.pg_index.indrelid,
       cls_idx.relname AS relname_index,
       pg_catalog.pg_index.indisunique,
       pg_catalog.pg_constraint.conrelid IS NOT NULL AS has_constraint,
       pg_catalog.pg_index.indoption,
       cls_idx.reloptions,
       pg_catalog.pg_am.amname,
       CASE WHEN (pg_catalog.pg_index.indpred IS NOT NULL) 
           THEN pg_catalog.pg_get_expr(pg_catalog.pg_index.indpred, pg_catalog.pg_index.indrelid) 
           END AS filter_definition,
    	 pg_catalog.pg_index.indnkeyatts,
    	 pg_catalog.pg_index.indnullsnotdistinct,
    	 idx_cols.elements,
    	 idx_cols.elements_is_expr 
FROM pg_catalog.pg_index 
    JOIN pg_catalog.pg_class AS cls_idx 
        ON pg_catalog.pg_index.indexrelid = cls_idx.oid 
    JOIN pg_catalog.pg_am 
        ON cls_idx.relam = pg_catalog.pg_am.oid 
    LEFT OUTER JOIN (SELECT idx_attr.indexrelid AS indexrelid, min(idx_attr.indrelid) AS min_1,
                            array_agg(idx_attr.element ORDER BY idx_attr.ord) AS elements,
                            array_agg(idx_attr.is_expr ORDER BY idx_attr.ord) AS elements_is_expr
                     FROM (SELECT idx.indexrelid AS indexrelid,
                                  idx.indrelid AS indrelid,
                                  idx.ord AS ord,
                                  CASE WHEN (idx.attnum = 0) THEN pg_catalog.pg_get_indexdef(idx.indexrelid, idx.ord + 1, true)
                                      ELSE CAST(pg_catalog.pg_attribute.attname AS TEXT) 
                                      END AS element,
                                  idx.attnum = 0 AS is_expr
                           FROM (SELECT pg_catalog.pg_index.indexrelid AS indexrelid,
                                        pg_catalog.pg_index.indrelid AS indrelid,
                                        unnest(pg_catalog.pg_index.indkey) AS attnum,
                                        generate_subscripts(pg_catalog.pg_index.indkey, 1) AS ord
                                 FROM pg_catalog.pg_index
                                 WHERE NOT pg_catalog.pg_index.indisprimary
                                   AND pg_catalog.pg_index.indrelid IN (3491847678)) AS idx
                           LEFT OUTER JOIN pg_catalog.pg_attribute
                               ON pg_catalog.pg_attribute.attnum = idx.attnum
                                      AND pg_catalog.pg_attribute.attrelid = idx.indrelid
                           WHERE idx.indrelid IN (3491847678)) AS idx_attr
                     GROUP BY idx_attr.indexrelid) AS idx_cols
        ON pg_catalog.pg_index.indexrelid = idx_cols.indexrelid
    LEFT OUTER JOIN pg_catalog.pg_constraint
        ON pg_catalog.pg_index.indrelid = pg_catalog.pg_constraint.conrelid
               AND pg_catalog.pg_index.indexrelid = pg_catalog.pg_constraint.conindid
               AND pg_catalog.pg_constraint.contype = ANY (ARRAY['p', 'u', 'x'])
WHERE pg_catalog.pg_index.indrelid IN (3491847678)
  AND NOT pg_catalog.pg_index.indisprimary
ORDER BY pg_catalog.pg_index.indrelid, cls_idx.relname`,
					Expected: []sql.Row{
						{3491847678, "dolt_log_commit_hash_key", "t", "t", "0", interface{}(nil), "btree", interface{}(nil), 1, "f", "{commit_hash}", "{f}"},
					},
				},
			},
		},
		{
			Name:        "attributes",
			SetUpScript: sharedSetupScript,
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT attr.conrelid,
       array_agg(CAST(attr.attname AS TEXT) ORDER BY attr.ord) AS cols,
       attr.conname,
       min(attr.description) AS description,
       bool_and(pg_catalog.pg_index.indnullsnotdistinct) AS indnullsnotdistinct
FROM (SELECT con.conrelid AS conrelid,
             con.conname AS conname,
             con.conindid AS conindid,
             con.description AS description,
             con.ord AS ord, pg_catalog.pg_attribute.attname AS attname
      FROM pg_catalog.pg_attribute 
          JOIN (SELECT pg_catalog.pg_constraint.conrelid AS conrelid,
                       pg_catalog.pg_constraint.conname AS conname,
                       pg_catalog.pg_constraint.conindid AS conindid,
                       unnest(pg_catalog.pg_constraint.conkey) AS attnum,
                       generate_subscripts(pg_catalog.pg_constraint.conkey, 1) AS ord,
                       pg_catalog.pg_description.description AS description
                FROM pg_catalog.pg_constraint 
                    LEFT OUTER JOIN pg_catalog.pg_description 
                        ON pg_catalog.pg_description.objoid = pg_catalog.pg_constraint.oid
                WHERE pg_catalog.pg_constraint.contype = 'u'
                  AND pg_catalog.pg_constraint.conrelid IN (3491847678)) AS con
              ON pg_catalog.pg_attribute.attnum = con.attnum
                     AND pg_catalog.pg_attribute.attrelid = con.conrelid
      WHERE con.conrelid IN (3491847678)) AS attr
    JOIN pg_catalog.pg_index 
        ON attr.conindid = pg_catalog.pg_index.indexrelid
GROUP BY attr.conrelid, attr.conname
ORDER BY attr.conrelid, attr.conname`,
					Expected: []sql.Row{
						{3491847678, "{commit_hash}", "dolt_log_commit_hash_key", interface{}(nil), "f"},
					},
				},
			},
		},
		{
			Name:        "key constraints",
			SetUpScript: sharedSetupScript,
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT attr.conrelid,
       array_agg(CAST(attr.attname AS TEXT) ORDER BY attr.ord) AS cols,
       attr.conname,
       min(attr.description) AS description,
       NULL AS extra FROM
                         (SELECT con.conrelid AS conrelid,
                                 con.conname AS conname,
                                 con.conindid AS conindid,
                                 con.description AS description,
                                 con.ord AS ord,
                                 pg_catalog.pg_attribute.attname AS attname
                          FROM pg_catalog.pg_attribute 
                              JOIN (SELECT pg_catalog.pg_constraint.conrelid AS conrelid,
                                           pg_catalog.pg_constraint.conname AS conname,
                                           pg_catalog.pg_constraint.conindid AS conindid,
                                           unnest(pg_catalog.pg_constraint.conkey) AS attnum,
                                           generate_subscripts(pg_catalog.pg_constraint.conkey, 1) AS ord,
                                           pg_catalog.pg_description.description AS description
                                    FROM pg_catalog.pg_constraint
                                        LEFT OUTER JOIN pg_catalog.pg_description
                                            ON pg_catalog.pg_description.objoid = pg_catalog.pg_constraint.oid
                                    WHERE pg_catalog.pg_constraint.contype = 'p'
                                      AND pg_catalog.pg_constraint.conrelid IN (select oid from pg_class where relname='t1'))
                                  AS con
                                  ON pg_catalog.pg_attribute.attnum = con.attnum
                                         AND pg_catalog.pg_attribute.attrelid = con.conrelid
                          WHERE con.conrelid IN (select oid from pg_class where relname='t1')) AS attr
                     GROUP BY attr.conrelid, attr.conname
                     ORDER BY attr.conrelid, attr.conname`,
					Expected: []sql.Row{
						{1249736862, "{a}", "t1_pkey", nil, nil},
					},
				},
			},
		},
		{
			Name:        "index queries",
			SetUpScript: sharedSetupScript,
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT pg_catalog.pg_index.indrelid,
       cls_idx.relname AS relname_index,
       pg_catalog.pg_index.indisunique,
       pg_catalog.pg_constraint.conrelid IS NOT NULL AS has_constraint,
       pg_catalog.pg_index.indoption,
       cls_idx.reloptions,
       pg_catalog.pg_am.amname,
       CASE WHEN (pg_catalog.pg_index.indpred IS NOT NULL)
           THEN pg_catalog.pg_get_expr(pg_catalog.pg_index.indpred, pg_catalog.pg_index.indrelid)
           END AS filter_definition,
       pg_catalog.pg_index.indnkeyatts,
       pg_catalog.pg_index.indnullsnotdistinct,
       idx_cols.elements,
       idx_cols.elements_is_expr
FROM pg_catalog.pg_index
    JOIN pg_catalog.pg_class AS cls_idx 
        ON pg_catalog.pg_index.indexrelid = cls_idx.oid
    JOIN pg_catalog.pg_am ON cls_idx.relam = pg_catalog.pg_am.oid
    LEFT OUTER JOIN 
    (SELECT idx_attr.indexrelid AS indexrelid,
            min(idx_attr.indrelid) AS min_1,
            array_agg(idx_attr.element ORDER BY idx_attr.ord) AS elements,
            array_agg(idx_attr.is_expr ORDER BY idx_attr.ord) AS elements_is_expr
     FROM (SELECT idx.indexrelid AS indexrelid,
                  idx.indrelid AS indrelid,
                  idx.ord AS ord,
                  CASE WHEN (idx.attnum = 0)
                      THEN pg_catalog.pg_get_indexdef(idx.indexrelid, idx.ord + 1, true)
                      ELSE CAST(pg_catalog.pg_attribute.attname AS TEXT)
                      END AS element,
               idx.attnum = 0 AS is_expr 
           FROM (SELECT pg_catalog.pg_index.indexrelid AS indexrelid,
                        pg_catalog.pg_index.indrelid AS indrelid,
                        unnest(pg_catalog.pg_index.indkey) AS attnum,
                        generate_subscripts(pg_catalog.pg_index.indkey, 1) AS ord
                 FROM pg_catalog.pg_index 
                 WHERE NOT pg_catalog.pg_index.indisprimary 
                   AND pg_catalog.pg_index.indrelid IN (select oid from pg_class where relname='t2')) AS idx
               LEFT OUTER JOIN pg_catalog.pg_attribute
                   ON pg_catalog.pg_attribute.attnum = idx.attnum
                          AND pg_catalog.pg_attribute.attrelid = idx.indrelid
           WHERE idx.indrelid IN (select oid from pg_class where relname='t2')) AS idx_attr GROUP BY idx_attr.indexrelid) AS idx_cols
        ON pg_catalog.pg_index.indexrelid = idx_cols.indexrelid
    LEFT OUTER JOIN pg_catalog.pg_constraint
        ON pg_catalog.pg_index.indrelid = pg_catalog.pg_constraint.conrelid
               AND pg_catalog.pg_index.indexrelid = pg_catalog.pg_constraint.conindid
               AND pg_catalog.pg_constraint.contype = ANY (ARRAY['p', 'u', 'x']) 
WHERE pg_catalog.pg_index.indrelid IN (select oid from pg_class where relname='t2')
  AND NOT pg_catalog.pg_index.indisprimary ORDER BY pg_catalog.pg_index.indrelid, cls_idx.relname`,
					Expected: []sql.Row{
						{1496157034, "t2_b_idx", "f", "f", "0", nil, "btree", nil, 1, "f", "{b}", "{f}"},
					},
				},
			},
		},
	})
}

func TestSystemTablesInPgcatalog(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_tables",
			SetUpScript: []string{
				`CREATE SCHEMA s1;`,
				`CREATE TABLE s1.t1 (pk INT primary key, v1 INT);`,
				`set dolt_show_system_tables=1`,
			},
			// TODO: some of these dolt_ table names are wrong, see https://github.com/dolthub/doltgresql/issues/1560
			Assertions: []ScriptTestAssertion{
				{
					Query: "select * from pg_catalog.pg_tables where schemaname not in ('information_schema', 'pg_catalog') order by schemaname, tablename;",
					Expected: []sql.Row{
						{"dolt", "branches", "postgres", nil, "t", "f", "f", "f"},
						{"dolt", "commit_ancestors", "postgres", nil, "t", "f", "f", "f"},
						{"dolt", "commits", "postgres", nil, "t", "f", "f", "f"},
						{"dolt", "conflicts", "postgres", nil, "f", "f", "f", "f"},
						{"dolt", "constraint_violations", "postgres", nil, "f", "f", "f", "f"},
						{"dolt", "dolt_backups", "postgres", nil, "f", "f", "f", "f"},
						{"dolt", "dolt_branch_activity", "postgres", nil, "f", "f", "f", "f"},
						{"dolt", "dolt_help", "postgres", nil, "f", "f", "f", "f"},
						{"dolt", "dolt_stashes", "postgres", nil, "f", "f", "f", "f"},
						{"dolt", "log", "postgres", nil, "t", "f", "f", "f"},
						{"dolt", "remote_branches", "postgres", nil, "t", "f", "f", "f"},
						{"dolt", "remotes", "postgres", nil, "f", "f", "f", "f"},
						{"dolt", "status", "postgres", nil, "f", "f", "f", "f"},
						{"dolt", "status_ignored", "postgres", nil, "f", "f", "f", "f"},
						{"public", "dolt_branches", "postgres", nil, "t", "f", "f", "f"},
						{"public", "dolt_column_diff", "postgres", nil, "f", "f", "f", "f"},
						{"public", "dolt_commit_ancestors", "postgres", nil, "t", "f", "f", "f"},
						{"public", "dolt_commits", "postgres", nil, "t", "f", "f", "f"},
						{"public", "dolt_conflicts", "postgres", nil, "f", "f", "f", "f"},
						{"public", "dolt_constraint_violations", "postgres", nil, "f", "f", "f", "f"},
						{"public", "dolt_diff", "postgres", nil, "t", "f", "f", "f"},
						{"public", "dolt_log", "postgres", nil, "t", "f", "f", "f"},
						{"public", "dolt_merge_status", "postgres", nil, "f", "f", "f", "f"},
						{"public", "dolt_remote_branches", "postgres", nil, "t", "f", "f", "f"},
						{"public", "dolt_remotes", "postgres", nil, "f", "f", "f", "f"},
						{"public", "dolt_schema_conflicts", "postgres", nil, "f", "f", "f", "f"},
						{"public", "dolt_status", "postgres", nil, "f", "f", "f", "f"},
						{"public", "dolt_status_ignored", "postgres", nil, "f", "f", "f", "f"},
						{"public", "dolt_tags", "postgres", nil, "f", "f", "f", "f"},
						{"s1", "dolt_branches", "postgres", nil, "t", "f", "f", "f"},
						{"s1", "dolt_column_diff", "postgres", nil, "f", "f", "f", "f"},
						{"s1", "dolt_commit_ancestors", "postgres", nil, "t", "f", "f", "f"},
						{"s1", "dolt_commit_diff_t1", "postgres", nil, "t", "f", "f", "f"},
						{"s1", "dolt_commits", "postgres", nil, "t", "f", "f", "f"},
						{"s1", "dolt_conflicts", "postgres", nil, "f", "f", "f", "f"},
						{"s1", "dolt_conflicts_t1", "postgres", nil, "f", "f", "f", "f"},
						{"s1", "dolt_constraint_violations", "postgres", nil, "f", "f", "f", "f"},
						{"s1", "dolt_constraint_violations_t1", "postgres", nil, "f", "f", "f", "f"},
						{"s1", "dolt_diff", "postgres", nil, "t", "f", "f", "f"},
						{"s1", "dolt_diff_t1", "postgres", nil, "t", "f", "f", "f"},
						{"s1", "dolt_history_t1", "postgres", nil, "t", "f", "f", "f"},
						{"s1", "dolt_log", "postgres", nil, "t", "f", "f", "f"},
						{"s1", "dolt_merge_status", "postgres", nil, "f", "f", "f", "f"},
						{"s1", "dolt_remote_branches", "postgres", nil, "t", "f", "f", "f"},
						{"s1", "dolt_remotes", "postgres", nil, "f", "f", "f", "f"},
						{"s1", "dolt_schema_conflicts", "postgres", nil, "f", "f", "f", "f"},
						{"s1", "dolt_status", "postgres", nil, "f", "f", "f", "f"},
						{"s1", "dolt_status_ignored", "postgres", nil, "f", "f", "f", "f"},
						{"s1", "dolt_tags", "postgres", nil, "f", "f", "f", "f"},
						{"s1", "dolt_workspace_t1", "postgres", nil, "f", "f", "f", "f"},
						{"s1", "t1", "postgres", nil, "t", "f", "f", "f"},
					},
				},
			},
		},
		{
			Name: "pg_class",
			SetUpScript: []string{
				`CREATE SCHEMA s1;`,
				`CREATE TABLE s1.t1 (pk INT primary key, v1 INT);`,
				`set dolt_show_system_tables=1`,
			},
			Assertions: []ScriptTestAssertion{
				{
					// TODO: some of these dolt_ table names are wrong, see https://github.com/dolthub/doltgresql/issues/1560
					Query: `select oid, relname, relnamespace, relkind from pg_class where relnamespace not in (select oid from pg_namespace where nspname in ('information_schema', 'pg_catalog')) order by relnamespace, relname;`,
					Expected: []sql.Row{
						{458530874, "dolt_branches", 2200, "r"},
						{1691921080, "dolt_branches_dolt_branches_name_idx_key", 2200, "i"},
						{2056815203, "dolt_column_diff", 2200, "r"},
						{1555944102, "dolt_commit_ancestors", 2200, "r"},
						{1005534291, "dolt_commit_ancestors_commit_hash_key", 2200, "i"},
						{3152041833, "dolt_commits", 2200, "r"},
						{761030765, "dolt_commits_commit_hash_key", 2200, "i"},
						{245736992, "dolt_conflicts", 2200, "r"},
						{1932298159, "dolt_constraint_violations", 2200, "r"},
						{2357712556, "dolt_diff", 2200, "r"},
						sql.Row{101228732, "dolt_diff_commit_hash_key", 2200, "i"},
						{3491847678, "dolt_log", 2200, "r"},
						sql.Row{2292720014, "dolt_log_commit_hash_key", 2200, "i"},
						{604995978, "dolt_merge_status", 2200, "r"},
						{887648921, "dolt_remote_branches", 2200, "r"},
						{1471391189, "dolt_remote_branches_dolt_branches_name_idx_key", 2200, "i"},
						{341706375, "dolt_remotes", 2200, "r"},
						{3210116770, "dolt_schema_conflicts", 2200, "r"},
						{1060579466, "dolt_status", 2200, "r"},
						{1523309269, "dolt_status_ignored", 2200, "r"},
						{1807684176, "dolt_tags", 2200, "r"},
						{2969045375, "commits_from", 1634633383, "i"},
						{1819666711, "commits_to", 1634633383, "i"},
						{1763579892, "dolt_branches", 1634633383, "r"},
						{3929519011, "dolt_branches_dolt_branches_name_idx_key", 1634633383, "i"},
						{1212681264, "dolt_column_diff", 1634633383, "r"},
						{4001633963, "dolt_commit_ancestors", 1634633383, "r"},
						{2729363914, "dolt_commit_ancestors_commit_hash_key", 1634633383, "i"},
						{115796810, "dolt_commit_diff_t1", 1634633383, "r"},
						{3112353516, "dolt_commits", 1634633383, "r"},
						{2203440081, "dolt_commits_commit_hash_key", 1634633383, "i"},
						{2517735330, "dolt_conflicts", 1634633383, "r"},
						{2419641880, "dolt_conflicts_t1", 1634633383, "r"},
						{1322753784, "dolt_constraint_violations", 1634633383, "r"},
						{3390577184, "dolt_constraint_violations_t1", 1634633383, "r"},
						{649632770, "dolt_diff", 1634633383, "r"},
						{3688144202, "dolt_diff_commit_hash_key", 1634633383, "i"},
						{876336553, "dolt_diff_t1", 1634633383, "r"},
						{3054553486, "dolt_diff_t1_from_commit_key", 1634633383, "i"},
						{2818605050, "dolt_diff_t1_to_commit_key", 1634633383, "i"},
						{3422698383, "dolt_history_t1", 1634633383, "r"},
						{2283462952, "dolt_history_t1_commit_hash_key", 1634633383, "i"},
						{610108332, "dolt_history_t1_pkey", 1634633383, "i"},
						{2067982358, "dolt_log", 1634633383, "r"},
						{2677251944, "dolt_log_commit_hash_key", 1634633383, "i"},
						{3947121936, "dolt_merge_status", 1634633383, "r"},
						{867423409, "dolt_remote_branches", 1634633383, "r"},
						{3149300779, "dolt_remote_branches_dolt_branches_name_idx_key", 1634633383, "i"},
						{373092098, "dolt_remotes", 1634633383, "r"},
						{225426095, "dolt_schema_conflicts", 1634633383, "r"},
						{3554775706, "dolt_status", 1634633383, "r"},
						{1227149778, "dolt_status_ignored", 1634633383, "r"},
						{3246414078, "dolt_tags", 1634633383, "r"},
						{1640933374, "dolt_workspace_t1", 1634633383, "r"},
						{170053857, "from_pks", 1634633383, "i"},
						{2849341124, "t1", 1634633383, "r"},
						{512149063, "t1_pkey", 1634633383, "i"},
						{1064271386, "to_pks", 1634633383, "i"},
						{398111247, "branches", 1882653564, "r"},
						{1131770412, "branches_dolt_branches_name_idx_key", 1882653564, "i"},
						{4126412490, "commit_ancestors", 1882653564, "r"},
						{3726205325, "commit_ancestors_commit_hash_key", 1882653564, "i"},
						{3425483043, "commits", 1882653564, "r"},
						{1121983063, "commits_commit_hash_key", 1882653564, "i"},
						{1218627310, "conflicts", 1882653564, "r"},
						{1967026500, "constraint_violations", 1882653564, "r"},
						{1167248682, "dolt_backups", 1882653564, "r"},
						{3999387287, "dolt_branch_activity", 1882653564, "r"},
						{629684363, "dolt_help", 1882653564, "r"},
						{1384122262, "dolt_stashes", 1882653564, "r"},
						{909123395, "log", 1882653564, "r"},
						{1789897782, "log_commit_hash_key", 1882653564, "i"},
						{148630507, "remote_branches", 1882653564, "r"},
						{2777869953, "remote_branches_dolt_branches_name_idx_key", 1882653564, "i"},
						{1670572237, "remotes", 1882653564, "r"},
						{3431637196, "status", 1882653564, "r"},
						{3418072419, "status_ignored", 1882653564, "r"},
					},
				},
			},
		},
		{
			Name: "pg_attribute",
			SetUpScript: []string{
				`CREATE SCHEMA s1;`,
				`CREATE TABLE s1.t1 (pk INT primary key, v1 INT);`,
				`set dolt_show_system_tables=1`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `select attrelid, attname, atttypid, attnum, attnotnull, atthasdef, attisdropped from pg_catalog.pg_attribute where attrelid in (select oid from pg_catalog.pg_class where relnamespace not in (select oid from pg_namespace where nspname in ('information_schema', 'pg_catalog'))) order by attrelid, attnum;`,
					Expected: []sql.Row{
						{101228732, "commit_hash", 25, 1, "f", "f", "f"},
						{115796810, "to_pk", 23, 1, "f", "f", "f"},
						{115796810, "to_v1", 23, 2, "f", "f", "f"},
						{115796810, "to_commit", 25, 3, "f", "f", "f"},
						{115796810, "to_commit_date", 1114, 4, "f", "f", "f"},
						{115796810, "from_pk", 23, 5, "f", "f", "f"},
						{115796810, "from_v1", 23, 6, "f", "f", "f"},
						{115796810, "from_commit", 25, 7, "f", "f", "f"},
						{115796810, "from_commit_date", 1114, 8, "f", "f", "f"},
						{115796810, "diff_type", 25, 9, "f", "f", "f"},
						{148630507, "name", 25, 1, "t", "f", "f"},
						{148630507, "hash", 25, 2, "t", "f", "f"},
						{148630507, "latest_committer", 25, 3, "f", "f", "f"},
						{148630507, "latest_committer_email", 25, 4, "f", "f", "f"},
						{148630507, "latest_commit_date", 1114, 5, "f", "f", "f"},
						{148630507, "latest_commit_message", 25, 6, "f", "f", "f"},
						{148630507, "latest_author", 25, 7, "f", "f", "f"},
						{148630507, "latest_author_email", 25, 8, "f", "f", "f"},
						{148630507, "latest_author_date", 1114, 9, "f", "f", "f"},
						{170053857, "from_pk", 23, 1, "f", "f", "f"},
						{225426095, "table_name", 25, 1, "t", "f", "f"},
						{225426095, "base_schema", 25, 2, "t", "f", "f"},
						{225426095, "our_schema", 25, 3, "t", "f", "f"},
						{225426095, "their_schema", 25, 4, "t", "f", "f"},
						{225426095, "description", 25, 5, "t", "f", "f"},
						{245736992, "table", 25, 1, "t", "f", "f"},
						{245736992, "num_conflicts", 1700, 2, "t", "f", "f"},
						{341706375, "name", 25, 1, "t", "f", "f"},
						{341706375, "url", 25, 2, "t", "f", "f"},
						{341706375, "fetch_specs", 114, 3, "f", "f", "f"},
						{341706375, "params", 114, 4, "f", "f", "f"},
						{373092098, "name", 25, 1, "t", "f", "f"},
						{373092098, "url", 25, 2, "t", "f", "f"},
						{373092098, "fetch_specs", 114, 3, "f", "f", "f"},
						{373092098, "params", 114, 4, "f", "f", "f"},
						{398111247, "name", 25, 1, "t", "f", "f"},
						{398111247, "hash", 25, 2, "t", "f", "f"},
						{398111247, "latest_committer", 25, 3, "f", "f", "f"},
						{398111247, "latest_committer_email", 25, 4, "f", "f", "f"},
						{398111247, "latest_commit_date", 1114, 5, "f", "f", "f"},
						{398111247, "latest_commit_message", 25, 6, "f", "f", "f"},
						{398111247, "remote", 25, 7, "f", "f", "f"},
						{398111247, "branch", 25, 8, "f", "f", "f"},
						{398111247, "dirty", 16, 9, "f", "f", "f"},
						{398111247, "latest_author", 25, 10, "f", "f", "f"},
						{398111247, "latest_author_email", 25, 11, "f", "f", "f"},
						{398111247, "latest_author_date", 1114, 12, "f", "f", "f"},
						{458530874, "name", 25, 1, "t", "f", "f"},
						{458530874, "hash", 25, 2, "t", "f", "f"},
						{458530874, "latest_committer", 25, 3, "f", "f", "f"},
						{458530874, "latest_committer_email", 25, 4, "f", "f", "f"},
						{458530874, "latest_commit_date", 1114, 5, "f", "f", "f"},
						{458530874, "latest_commit_message", 25, 6, "f", "f", "f"},
						{458530874, "remote", 25, 7, "f", "f", "f"},
						{458530874, "branch", 25, 8, "f", "f", "f"},
						{458530874, "dirty", 16, 9, "f", "f", "f"},
						{458530874, "latest_author", 25, 10, "f", "f", "f"},
						{458530874, "latest_author_email", 25, 11, "f", "f", "f"},
						{458530874, "latest_author_date", 1114, 12, "f", "f", "f"},
						{512149063, "pk", 23, 1, "f", "f", "f"},
						{604995978, "is_merging", 16, 1, "t", "f", "f"},
						{604995978, "source", 25, 2, "f", "f", "f"},
						{604995978, "source_commit", 25, 3, "f", "f", "f"},
						{604995978, "target", 25, 4, "f", "f", "f"},
						{604995978, "unmerged_tables", 25, 5, "f", "f", "f"},
						{610108332, "pk", 23, 1, "f", "f", "f"},
						{629684363, "name", 25, 1, "t", "f", "f"},
						{629684363, "type", 25, 2, "t", "f", "f"},
						{629684363, "synopsis", 25, 3, "t", "f", "f"},
						{629684363, "short_description", 25, 4, "t", "f", "f"},
						{629684363, "long_description", 25, 5, "t", "f", "f"},
						{629684363, "arguments", 114, 6, "t", "f", "f"},
						{649632770, "commit_hash", 25, 1, "t", "f", "f"},
						{649632770, "table_name", 25, 2, "t", "f", "f"},
						{649632770, "committer", 25, 3, "t", "f", "f"},
						{649632770, "email", 25, 4, "t", "f", "f"},
						{649632770, "date", 1114, 5, "t", "f", "f"},
						{649632770, "message", 25, 6, "t", "f", "f"},
						{649632770, "data_change", 16, 7, "t", "f", "f"},
						{649632770, "schema_change", 16, 8, "t", "f", "f"},
						{649632770, "author", 25, 9, "t", "f", "f"},
						{649632770, "author_email", 25, 10, "t", "f", "f"},
						{649632770, "author_date", 1114, 11, "t", "f", "f"},
						{761030765, "commit_hash", 25, 1, "f", "f", "f"},
						{867423409, "name", 25, 1, "t", "f", "f"},
						{867423409, "hash", 25, 2, "t", "f", "f"},
						{867423409, "latest_committer", 25, 3, "f", "f", "f"},
						{867423409, "latest_committer_email", 25, 4, "f", "f", "f"},
						{867423409, "latest_commit_date", 1114, 5, "f", "f", "f"},
						{867423409, "latest_commit_message", 25, 6, "f", "f", "f"},
						{867423409, "latest_author", 25, 7, "f", "f", "f"},
						{867423409, "latest_author_email", 25, 8, "f", "f", "f"},
						{867423409, "latest_author_date", 1114, 9, "f", "f", "f"},
						{876336553, "to_pk", 23, 1, "f", "f", "f"},
						{876336553, "to_v1", 23, 2, "f", "f", "f"},
						{876336553, "to_commit", 25, 3, "f", "f", "f"},
						{876336553, "to_commit_date", 1114, 4, "f", "f", "f"},
						{876336553, "from_pk", 23, 5, "f", "f", "f"},
						{876336553, "from_v1", 23, 6, "f", "f", "f"},
						{876336553, "from_commit", 25, 7, "f", "f", "f"},
						{876336553, "from_commit_date", 1114, 8, "f", "f", "f"},
						{876336553, "diff_type", 25, 9, "f", "f", "f"},
						{887648921, "name", 25, 1, "t", "f", "f"},
						{887648921, "hash", 25, 2, "t", "f", "f"},
						{887648921, "latest_committer", 25, 3, "f", "f", "f"},
						{887648921, "latest_committer_email", 25, 4, "f", "f", "f"},
						{887648921, "latest_commit_date", 1114, 5, "f", "f", "f"},
						{887648921, "latest_commit_message", 25, 6, "f", "f", "f"},
						{887648921, "latest_author", 25, 7, "f", "f", "f"},
						{887648921, "latest_author_email", 25, 8, "f", "f", "f"},
						{887648921, "latest_author_date", 1114, 9, "f", "f", "f"},
						{909123395, "commit_hash", 25, 1, "t", "f", "f"},
						{909123395, "committer", 25, 2, "t", "f", "f"},
						{909123395, "email", 25, 3, "t", "f", "f"},
						{909123395, "date", 1114, 4, "t", "f", "f"},
						{909123395, "message", 25, 5, "t", "f", "f"},
						{909123395, "commit_order", 1700, 6, "t", "f", "f"},
						{909123395, "parents", 25, 7, "f", "f", "f"},
						{909123395, "refs", 25, 8, "t", "f", "f"},
						{909123395, "signature", 25, 9, "f", "f", "f"},
						{909123395, "author", 25, 10, "t", "f", "f"},
						{909123395, "author_email", 25, 11, "t", "f", "f"},
						{909123395, "author_date", 1114, 12, "t", "f", "f"},
						{1005534291, "commit_hash", 25, 1, "f", "f", "f"},
						{1060579466, "table_name", 25, 1, "t", "f", "f"},
						{1060579466, "staged", 16, 2, "t", "f", "f"},
						{1060579466, "status", 25, 3, "t", "f", "f"},
						{1064271386, "to_pk", 23, 1, "f", "f", "f"},
						{1121983063, "commit_hash", 25, 1, "f", "f", "f"},
						{1131770412, "name", 25, 1, "f", "f", "f"},
						{1167248682, "name", 25, 1, "t", "f", "f"},
						{1167248682, "url", 25, 2, "t", "f", "f"},
						{1167248682, "params", 114, 3, "t", "f", "f"},
						{1212681264, "commit_hash", 25, 1, "t", "f", "f"},
						{1212681264, "table_name", 25, 2, "t", "f", "f"},
						{1212681264, "column_name", 25, 3, "t", "f", "f"},
						{1212681264, "committer", 25, 4, "t", "f", "f"},
						{1212681264, "email", 25, 5, "t", "f", "f"},
						{1212681264, "date", 1114, 6, "t", "f", "f"},
						{1212681264, "message", 25, 7, "t", "f", "f"},
						{1212681264, "diff_type", 25, 8, "t", "f", "f"},
						{1212681264, "author", 25, 9, "t", "f", "f"},
						{1212681264, "author_email", 25, 10, "t", "f", "f"},
						{1212681264, "author_date", 1114, 11, "t", "f", "f"},
						{1218627310, "table", 25, 1, "t", "f", "f"},
						{1218627310, "num_conflicts", 1700, 2, "t", "f", "f"},
						{1227149778, "table_name", 25, 1, "t", "f", "f"},
						{1227149778, "staged", 16, 2, "t", "f", "f"},
						{1227149778, "status", 25, 3, "t", "f", "f"},
						{1227149778, "ignored", 16, 4, "t", "f", "f"},
						{1322753784, "table", 25, 1, "t", "f", "f"},
						{1322753784, "num_violations", 1700, 2, "t", "f", "f"},
						{1384122262, "name", 25, 1, "t", "f", "f"},
						{1384122262, "stash_id", 25, 2, "t", "f", "f"},
						{1384122262, "branch", 25, 3, "t", "f", "f"},
						{1384122262, "hash", 25, 4, "t", "f", "f"},
						{1384122262, "commit_message", 25, 5, "f", "f", "f"},
						{1471391189, "name", 25, 1, "f", "f", "f"},
						{1523309269, "table_name", 25, 1, "t", "f", "f"},
						{1523309269, "staged", 16, 2, "t", "f", "f"},
						{1523309269, "status", 25, 3, "t", "f", "f"},
						{1523309269, "ignored", 16, 4, "t", "f", "f"},
						{1555944102, "commit_hash", 25, 1, "t", "f", "f"},
						{1555944102, "parent_hash", 25, 2, "t", "f", "f"},
						{1555944102, "parent_index", 23, 3, "t", "f", "f"},
						{1640933374, "id", 20, 1, "t", "f", "f"},
						{1640933374, "staged", 16, 2, "t", "f", "f"},
						{1640933374, "diff_type", 25, 3, "t", "f", "f"},
						{1640933374, "to_pk", 23, 4, "f", "f", "f"},
						{1640933374, "to_v1", 23, 5, "f", "f", "f"},
						{1640933374, "from_pk", 23, 6, "f", "f", "f"},
						{1640933374, "from_v1", 23, 7, "f", "f", "f"},
						{1670572237, "name", 25, 1, "t", "f", "f"},
						{1670572237, "url", 25, 2, "t", "f", "f"},
						{1670572237, "fetch_specs", 114, 3, "f", "f", "f"},
						{1670572237, "params", 114, 4, "f", "f", "f"},
						{1691921080, "name", 25, 1, "f", "f", "f"},
						{1763579892, "name", 25, 1, "t", "f", "f"},
						{1763579892, "hash", 25, 2, "t", "f", "f"},
						{1763579892, "latest_committer", 25, 3, "f", "f", "f"},
						{1763579892, "latest_committer_email", 25, 4, "f", "f", "f"},
						{1763579892, "latest_commit_date", 1114, 5, "f", "f", "f"},
						{1763579892, "latest_commit_message", 25, 6, "f", "f", "f"},
						{1763579892, "remote", 25, 7, "f", "f", "f"},
						{1763579892, "branch", 25, 8, "f", "f", "f"},
						{1763579892, "dirty", 16, 9, "f", "f", "f"},
						{1763579892, "latest_author", 25, 10, "f", "f", "f"},
						{1763579892, "latest_author_email", 25, 11, "f", "f", "f"},
						{1763579892, "latest_author_date", 1114, 12, "f", "f", "f"},
						{1789897782, "commit_hash", 25, 1, "f", "f", "f"},
						{1807684176, "tag_name", 25, 1, "t", "f", "f"},
						{1807684176, "tag_hash", 25, 2, "t", "f", "f"},
						{1807684176, "tagger", 25, 3, "t", "f", "f"},
						{1807684176, "email", 25, 4, "t", "f", "f"},
						{1807684176, "date", 1114, 5, "t", "f", "f"},
						{1807684176, "message", 25, 6, "t", "f", "f"},
						{1819666711, "to_commit", 25, 1, "f", "f", "f"},
						{1819666711, "from_commit", 25, 2, "f", "f", "f"},
						{1819666711, "to_pk", 23, 3, "f", "f", "f"},
						{1932298159, "table", 25, 1, "t", "f", "f"},
						{1932298159, "num_violations", 1700, 2, "t", "f", "f"},
						{1967026500, "table", 25, 1, "t", "f", "f"},
						{1967026500, "num_violations", 1700, 2, "t", "f", "f"},
						{2056815203, "commit_hash", 25, 1, "t", "f", "f"},
						{2056815203, "table_name", 25, 2, "t", "f", "f"},
						{2056815203, "column_name", 25, 3, "t", "f", "f"},
						{2056815203, "committer", 25, 4, "t", "f", "f"},
						{2056815203, "email", 25, 5, "t", "f", "f"},
						{2056815203, "date", 1114, 6, "t", "f", "f"},
						{2056815203, "message", 25, 7, "t", "f", "f"},
						{2056815203, "diff_type", 25, 8, "t", "f", "f"},
						{2056815203, "author", 25, 9, "t", "f", "f"},
						{2056815203, "author_email", 25, 10, "t", "f", "f"},
						{2056815203, "author_date", 1114, 11, "t", "f", "f"},
						{2067982358, "commit_hash", 25, 1, "t", "f", "f"},
						{2067982358, "committer", 25, 2, "t", "f", "f"},
						{2067982358, "email", 25, 3, "t", "f", "f"},
						{2067982358, "date", 1114, 4, "t", "f", "f"},
						{2067982358, "message", 25, 5, "t", "f", "f"},
						{2067982358, "commit_order", 1700, 6, "t", "f", "f"},
						{2067982358, "parents", 25, 7, "f", "f", "f"},
						{2067982358, "refs", 25, 8, "t", "f", "f"},
						{2067982358, "signature", 25, 9, "f", "f", "f"},
						{2067982358, "author", 25, 10, "t", "f", "f"},
						{2067982358, "author_email", 25, 11, "t", "f", "f"},
						{2067982358, "author_date", 1114, 12, "t", "f", "f"},
						{2203440081, "commit_hash", 25, 1, "f", "f", "f"},
						{2283462952, "commit_hash", 25, 1, "f", "f", "f"},
						{2292720014, "commit_hash", 25, 1, "f", "f", "f"},
						{2357712556, "commit_hash", 25, 1, "t", "f", "f"},
						{2357712556, "table_name", 25, 2, "t", "f", "f"},
						{2357712556, "committer", 25, 3, "t", "f", "f"},
						{2357712556, "email", 25, 4, "t", "f", "f"},
						{2357712556, "date", 1114, 5, "t", "f", "f"},
						{2357712556, "message", 25, 6, "t", "f", "f"},
						{2357712556, "data_change", 16, 7, "t", "f", "f"},
						{2357712556, "schema_change", 16, 8, "t", "f", "f"},
						{2357712556, "author", 25, 9, "t", "f", "f"},
						{2357712556, "author_email", 25, 10, "t", "f", "f"},
						{2357712556, "author_date", 1114, 11, "t", "f", "f"},
						{2419641880, "from_root_ish", 25, 1, "f", "f", "f"},
						{2419641880, "base_pk", 23, 2, "f", "f", "f"},
						{2419641880, "base_v1", 23, 3, "f", "f", "f"},
						{2419641880, "our_pk", 23, 4, "t", "f", "f"},
						{2419641880, "our_v1", 23, 5, "f", "f", "f"},
						{2419641880, "our_diff_type", 25, 6, "f", "f", "f"},
						{2419641880, "their_pk", 23, 7, "f", "f", "f"},
						{2419641880, "their_v1", 23, 8, "f", "f", "f"},
						{2419641880, "their_diff_type", 25, 9, "f", "f", "f"},
						{2419641880, "dolt_conflict_id", 25, 10, "f", "f", "f"},
						{2517735330, "table", 25, 1, "t", "f", "f"},
						{2517735330, "num_conflicts", 1700, 2, "t", "f", "f"},
						{2677251944, "commit_hash", 25, 1, "f", "f", "f"},
						{2729363914, "commit_hash", 25, 1, "f", "f", "f"},
						{2777869953, "name", 25, 1, "f", "f", "f"},
						{2818605050, "to_commit", 25, 1, "f", "f", "f"},
						{2849341124, "pk", 23, 1, "t", "f", "f"},
						{2849341124, "v1", 23, 2, "f", "f", "f"},
						{2969045375, "to_commit", 25, 1, "f", "f", "f"},
						{2969045375, "from_commit", 25, 2, "f", "f", "f"},
						{2969045375, "from_pk", 23, 3, "f", "f", "f"},
						{3054553486, "from_commit", 25, 1, "f", "f", "f"},
						{3112353516, "commit_hash", 25, 1, "t", "f", "f"},
						{3112353516, "committer", 25, 2, "t", "f", "f"},
						{3112353516, "email", 25, 3, "t", "f", "f"},
						{3112353516, "date", 1114, 4, "t", "f", "f"},
						{3112353516, "message", 25, 5, "t", "f", "f"},
						{3112353516, "author", 25, 6, "t", "f", "f"},
						{3112353516, "author_email", 25, 7, "t", "f", "f"},
						{3112353516, "author_date", 1114, 8, "t", "f", "f"},
						{3149300779, "name", 25, 1, "f", "f", "f"},
						{3152041833, "commit_hash", 25, 1, "t", "f", "f"},
						{3152041833, "committer", 25, 2, "t", "f", "f"},
						{3152041833, "email", 25, 3, "t", "f", "f"},
						{3152041833, "date", 1114, 4, "t", "f", "f"},
						{3152041833, "message", 25, 5, "t", "f", "f"},
						{3152041833, "author", 25, 6, "t", "f", "f"},
						{3152041833, "author_email", 25, 7, "t", "f", "f"},
						{3152041833, "author_date", 1114, 8, "t", "f", "f"},
						{3210116770, "table_name", 25, 1, "t", "f", "f"},
						{3210116770, "base_schema", 25, 2, "t", "f", "f"},
						{3210116770, "our_schema", 25, 3, "t", "f", "f"},
						{3210116770, "their_schema", 25, 4, "t", "f", "f"},
						{3210116770, "description", 25, 5, "t", "f", "f"},
						{3246414078, "tag_name", 25, 1, "t", "f", "f"},
						{3246414078, "tag_hash", 25, 2, "t", "f", "f"},
						{3246414078, "tagger", 25, 3, "t", "f", "f"},
						{3246414078, "email", 25, 4, "t", "f", "f"},
						{3246414078, "date", 1114, 5, "t", "f", "f"},
						{3246414078, "message", 25, 6, "t", "f", "f"},
						{3390577184, "from_root_ish", 25, 1, "f", "f", "f"},
						{3390577184, "violation_type", 1043, 2, "t", "f", "f"},
						{3390577184, "pk", 23, 3, "t", "f", "f"},
						{3390577184, "v1", 23, 4, "f", "f", "f"},
						{3390577184, "violation_info", 114, 5, "t", "f", "f"},
						{3418072419, "table_name", 25, 1, "t", "f", "f"},
						{3418072419, "staged", 16, 2, "t", "f", "f"},
						{3418072419, "status", 25, 3, "t", "f", "f"},
						{3418072419, "ignored", 16, 4, "t", "f", "f"},
						{3422698383, "pk", 23, 1, "t", "f", "f"},
						{3422698383, "v1", 23, 2, "f", "f", "f"},
						{3422698383, "commit_hash", 25, 3, "t", "f", "f"},
						{3422698383, "committer", 25, 4, "t", "f", "f"},
						{3422698383, "commit_date", 1114, 5, "t", "f", "f"},
						{3425483043, "commit_hash", 25, 1, "t", "f", "f"},
						{3425483043, "committer", 25, 2, "t", "f", "f"},
						{3425483043, "email", 25, 3, "t", "f", "f"},
						{3425483043, "date", 1114, 4, "t", "f", "f"},
						{3425483043, "message", 25, 5, "t", "f", "f"},
						{3425483043, "author", 25, 6, "t", "f", "f"},
						{3425483043, "author_email", 25, 7, "t", "f", "f"},
						{3425483043, "author_date", 1114, 8, "t", "f", "f"},
						{3431637196, "table_name", 25, 1, "t", "f", "f"},
						{3431637196, "staged", 16, 2, "t", "f", "f"},
						{3431637196, "status", 25, 3, "t", "f", "f"},
						{3491847678, "commit_hash", 25, 1, "t", "f", "f"},
						{3491847678, "committer", 25, 2, "t", "f", "f"},
						{3491847678, "email", 25, 3, "t", "f", "f"},
						{3491847678, "date", 1114, 4, "t", "f", "f"},
						{3491847678, "message", 25, 5, "t", "f", "f"},
						{3491847678, "commit_order", 1700, 6, "t", "f", "f"},
						{3491847678, "parents", 25, 7, "f", "f", "f"},
						{3491847678, "refs", 25, 8, "t", "f", "f"},
						{3491847678, "signature", 25, 9, "f", "f", "f"},
						{3491847678, "author", 25, 10, "t", "f", "f"},
						{3491847678, "author_email", 25, 11, "t", "f", "f"},
						{3491847678, "author_date", 1114, 12, "t", "f", "f"},
						{3554775706, "table_name", 25, 1, "t", "f", "f"},
						{3554775706, "staged", 16, 2, "t", "f", "f"},
						{3554775706, "status", 25, 3, "t", "f", "f"},
						{3688144202, "commit_hash", 25, 1, "f", "f", "f"},
						{3726205325, "commit_hash", 25, 1, "f", "f", "f"},
						{3929519011, "name", 25, 1, "f", "f", "f"},
						{3947121936, "is_merging", 16, 1, "t", "f", "f"},
						{3947121936, "source", 25, 2, "f", "f", "f"},
						{3947121936, "source_commit", 25, 3, "f", "f", "f"},
						{3947121936, "target", 25, 4, "f", "f", "f"},
						{3947121936, "unmerged_tables", 25, 5, "f", "f", "f"},
						{3999387287, "branch", 25, 1, "t", "f", "f"},
						{3999387287, "last_read", 1114, 2, "f", "f", "f"},
						{3999387287, "last_write", 1114, 3, "f", "f", "f"},
						{3999387287, "active_sessions", 23, 4, "t", "f", "f"},
						{3999387287, "system_start_time", 1114, 5, "t", "f", "f"},
						{4001633963, "commit_hash", 25, 1, "t", "f", "f"},
						{4001633963, "parent_hash", 25, 2, "t", "f", "f"},
						{4001633963, "parent_index", 23, 3, "t", "f", "f"},
						{4126412490, "commit_hash", 25, 1, "t", "f", "f"},
						{4126412490, "parent_hash", 25, 2, "t", "f", "f"},
						{4126412490, "parent_index", 23, 3, "t", "f", "f"},
					},
				},
			},
		},
	})
}

func TestPgAttributeIndexes(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_attribute indexes",
			SetUpScript: []string{
				`CREATE SCHEMA test_schema;`,
				`SET search_path TO test_schema;`,
				`CREATE TABLE test_table (
					id INT PRIMARY KEY,
					name TEXT NOT NULL,
					description VARCHAR(255),
					created_at TIMESTAMP DEFAULT NOW()
				);`,
				`CREATE TABLE another_table (
					pk BIGINT PRIMARY KEY,
					value TEXT
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					// Test index on attrelid (non-unique index) using JOIN instead of regclass
					Query: `SELECT a.attname, a.attnum FROM pg_catalog.pg_attribute a
							JOIN pg_catalog.pg_class c ON a.attrelid = c.oid 
							WHERE c.relname = 'test_table'
							ORDER BY a.attnum;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgattributeindexes-0001-select-a.attname-a.attnum-from-pg_catalog.pg_attribute"},
				},
				{
					// Test unique index on attrelid + attname (using string values for boolean fields)
					Query: `SELECT a.attnum, a.attnotnull, a.atthasdef FROM pg_catalog.pg_attribute a
							JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
							WHERE c.relname = 'test_table' 
							AND a.attname = 'name';`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgattributeindexes-0002-select-a.attnum-a.attnotnull-a.atthasdef-from"},
				},
				{
					// Test another unique index lookup
					Query: `SELECT a.attnum FROM pg_catalog.pg_attribute a
							JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
							WHERE c.relname = 'another_table' 
							AND a.attname = 'pk';`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgattributeindexes-0003-select-a.attnum-from-pg_catalog.pg_attribute-a"},
				},
				{
					// Test range lookup on attrelid index
					Query: `SELECT COUNT(*) FROM pg_catalog.pg_attribute a
							WHERE a.attrelid IN (
								SELECT oid FROM pg_catalog.pg_class 
								WHERE relname IN ('test_table', 'another_table')
							);`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgattributeindexes-0004-select-count-*-from-pg_catalog.pg_attribute"},
				},
				{
					// Test JOIN using the indexes
					Query: `SELECT c.relname, a.attname, a.attnum 
							FROM pg_catalog.pg_class c 
							JOIN pg_catalog.pg_attribute a ON c.oid = a.attrelid 
							WHERE c.relname IN ('test_table', 'another_table') 
							ORDER BY c.relname, a.attnum;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgcatalog-test-testpgattributeindexes-0005-select-c.relname-a.attname-a.attnum-from"},
				},
			},
		},
	})
}
