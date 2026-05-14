package _go

import (
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
)

func TestInfoSchemaRevisionDb(t *testing.T) {
	RunScripts(t, InfoSchemaRevisionDbScripts)
}

var InfoSchemaRevisionDbScripts = []ScriptTest{
	{
		Name: "info_schema changes with dolt_checkout",
		SetUpScript: []string{
			"create table t (a int primary key, b int);",
			"select dolt_commit('-Am', 'creating table t');",
			"select dolt_branch('b2');",
			"select dolt_branch('b3');",
			"select dolt_checkout('b2');",
			"alter table t add column c int;",
			"select dolt_commit('-am', 'added column c on branch b2');",
			"select dolt_checkout('b3');",
			"alter table t add column d int;",
			"select dolt_commit('-am', 'added column d on branch b3');",
			"select dolt_checkout('main');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select active_branch();",
				Expected: []sql.Row{{"main"}},
			},
			{
				Query:    "select column_name from information_schema.columns where table_catalog = 'postgres' and table_name = 't' order by 1;",
				Expected: []sql.Row{{"a"}, {"b"}},
			},
			{
				Query:            "select dolt_checkout('b2');",
				SkipResultsCheck: true,
			},
			{
				Query:    "select active_branch();",
				Expected: []sql.Row{{"b2"}},
			},
			{
				Query:    "select column_name from information_schema.columns where table_catalog = 'postgres' and table_name = 't' order by 1;",
				Expected: []sql.Row{{"a"}, {"b"}, {"c"}},
			},
			{
				Query:            "select dolt_checkout('b3');",
				SkipResultsCheck: true,
			},
			{
				Query:    "select active_branch();",
				Expected: []sql.Row{{"b3"}},
			},
			{
				Query:    "select column_name from information_schema.columns where table_catalog = 'postgres' and table_name = 't' order by 1;",
				Expected: []sql.Row{{"a"}, {"b"}, {"d"}},
			},
		},
	},
	{
		Name: "info_schema with detached HEAD",
		SetUpScript: []string{
			"create table t (a int primary key, b int);",
			"select dolt_commit('-Am', 'creating table t');",
			"select dolt_branch('b2');",
			"select dolt_branch('b3');",
			"select dolt_checkout('b2');",
			"alter table t add column c int;",
			"select dolt_commit('-am', 'added column c on branch b2');",
			"select dolt_tag('t2')",
			"select dolt_checkout('b3');",
			"alter table t add column d int;",
			"select dolt_commit('-am', 'added column d on branch b3');",
			"select dolt_tag('t3')",
			"select dolt_checkout('main');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select active_branch();",
				Expected: []sql.Row{{"main"}},
			},
			{
				Query:    "select column_name from information_schema.columns where table_catalog = 'postgres' and table_name = 't' order by 1;",
				Expected: []sql.Row{{"a"}, {"b"}},
			},
			{
				Query:            "use postgres/t2;",
				SkipResultsCheck: true,
			},
			{
				Query:    "select active_branch();",
				Expected: []sql.Row{{nil}},
			},
			{
				Query:    "select column_name from information_schema.columns where table_catalog = 'postgres/t2' and table_name = 't' order by 1;",
				Expected: []sql.Row{{"a"}, {"b"}, {"c"}},
			},
			{
				Query:            "use postgres/t3;",
				SkipResultsCheck: true,
			},
			{
				Query:    "select active_branch();",
				Expected: []sql.Row{{nil}},
			},
			{
				Query:    "select column_name from information_schema.columns where table_catalog = 'postgres/t3' and table_name = 't' order by 1;",
				Expected: []sql.Row{{"a"}, {"b"}, {"d"}},
			},
			{
				Query:    "select relname from pg_class where oid = 't'::regclass;",
				Expected: []sql.Row{{"t"}},
			},
		},
	},
}

func TestInfoSchemaColumns(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "information_schema.columns",
			SetUpScript: []string{
				"create table test_table (id int primary key, col1 varchar(255));",
				"create view test_view as select * from test_table;",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT DISTINCT table_schema FROM information_schema.columns ORDER BY table_schema;`,
					Expected: []sql.Row{
						{"information_schema"},
						{"pg_catalog"},
						{"public"},
					},
				},
				{
					Query: `SELECT table_catalog, table_schema, table_name, column_name FROM information_schema.columns WHERE table_schema='public' ORDER BY table_name;`,
					Expected: []sql.Row{
						{"postgres", "public", "test_table", "id"},
						{"postgres", "public", "test_table", "col1"},
						{"postgres", "public", "test_view", ""},
					},
				},
				{
					Query: `SELECT 
						columns.column_name, 
						pg_catalog.col_description(('"' || table_catalog || '"."' || table_schema || '"."' || table_name || '"')::regclass::oid, ordinal_position) AS description, 
						('"' || "udt_schema" || '"."' || "udt_name" || '"')::"regtype" AS "regtype", 
						pg_catalog.format_type("col_attr"."atttypid", "col_attr"."atttypmod") AS "format_type" 
						FROM "information_schema"."columns" 
						LEFT JOIN "pg_catalog"."pg_attribute" AS "col_attr" 
						ON "col_attr"."attname" = "columns"."column_name" AND "col_attr"."attrelid" = ( 
							SELECT "cls"."oid" FROM "pg_catalog"."pg_class" AS "cls" 
							LEFT JOIN "pg_catalog"."pg_namespace" AS "ns" ON "ns"."oid" = "cls"."relnamespace" 
							WHERE "cls"."relname" = "columns"."table_name" AND "ns"."nspname" = "columns"."table_schema" 
						) WHERE ("table_schema" = 'public' AND "table_name" = 'test_table');`, PostgresOracle: ScriptTestPostgresOracle{ID: "information-schema-test-testinfoschemacolumns-0003-select-columns.column_name-pg_catalog.col_description-||-table_catalog", Cleanup: []string{"DROP TABLE IF EXISTS test_table CASCADE"}},
				},
				{
					Query: `CREATE SCHEMA test_schema;`, PostgresOracle: ScriptTestPostgresOracle{ID: "information-schema-test-testinfoschemacolumns-0004-create-schema-test_schema", Cleanup: []string{"DROP TABLE IF EXISTS test_table CASCADE", "DROP SCHEMA IF EXISTS test_schema CASCADE"}},
				},
				{
					Query: `SET SEARCH_PATH TO test_schema;`, PostgresOracle: ScriptTestPostgresOracle{ID: "information-schema-test-testinfoschemacolumns-0005-set-search_path-to-test_schema", Cleanup: []string{"DROP TABLE IF EXISTS test_table CASCADE", "DROP SCHEMA IF EXISTS test_schema CASCADE"}},
				},
				{
					Query: `CREATE TABLE test_table2 (id2 INT);`, PostgresOracle: ScriptTestPostgresOracle{ID: "information-schema-test-testinfoschemacolumns-0006-create-table-test_table2-id2-int", Cleanup: []string{"DROP TABLE IF EXISTS test_table CASCADE", "DROP TABLE IF EXISTS test_table2 CASCADE", "DROP SCHEMA IF EXISTS test_schema CASCADE"}},
				},
				{
					Query: `SELECT DISTINCT table_schema FROM information_schema.columns order by table_schema;`,
					Expected: []sql.Row{
						{"information_schema"}, {"pg_catalog"}, {"public"}, {"test_schema"},
					},
				},
				{
					Query: `SELECT table_catalog, table_schema, table_name, column_name FROM information_schema.columns WHERE table_schema='test_schema';`, PostgresOracle: ScriptTestPostgresOracle{ID: "information-schema-test-testinfoschemacolumns-0008-select-table_catalog-table_schema-table_name-column_name", Cleanup: []string{"DROP TABLE IF EXISTS test_table CASCADE", "DROP TABLE IF EXISTS test_table2 CASCADE", "DROP SCHEMA IF EXISTS test_schema CASCADE"}},
				},
				{
					Query: "SELECT * FROM information_schema.columns WHERE table_name='test_table';",
					Expected: []sql.Row{
						{"postgres", "public", "test_table", "id", 1, nil, "NO", "integer", nil, nil, 32, 2, 0, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, "postgres", "pg_catalog", "int4", nil, nil, nil, nil, nil, "NO", "NO", nil, nil, nil, nil, nil, "NO", "NEVER", nil, "YES"},
						{"postgres", "public", "test_table", "col1", 2, nil, "YES", "character varying", 255, 1020, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, "postgres", "pg_catalog", "varchar", nil, nil, nil, nil, nil, "NO", "NO", nil, nil, nil, nil, nil, "NO", "NEVER", nil, "YES"},
					},
				},
				{
					Skip:  true, // TODO: Don't have complete view information to fill out these rows
					Query: "SELECT * FROM information_schema.columns WHERE table_name='test_view';", PostgresOracle: ScriptTestPostgresOracle{ID: "information-schema-test-testinfoschemacolumns-0010-select-*-from-information_schema.columns-where", Cleanup: []string{"DROP TABLE IF EXISTS test_table CASCADE", "DROP TABLE IF EXISTS test_table2 CASCADE", "DROP SCHEMA IF EXISTS test_schema CASCADE"}},
				},
				{
					Query: `SELECT columns.table_name, columns.column_name from "information_schema"."columns" WHERE table_name='test_table';`, PostgresOracle: ScriptTestPostgresOracle{ID: "information-schema-test-testinfoschemacolumns-0011-select-columns.table_name-columns.column_name-from-information_schema", Cleanup: []string{"DROP TABLE IF EXISTS test_table CASCADE", "DROP TABLE IF EXISTS test_table2 CASCADE", "DROP SCHEMA IF EXISTS test_schema CASCADE"}},
				},
				{
					Query: `CREATE TABLE testnumtypes (id INT PRIMARY KEY, col1 SMALLINT, col2 BIGINT, col3 REAL, col4 DOUBLE PRECISION, col5 NUMERIC, col6 DECIMAL(10, 2), col7 OID, col8 XID);`, PostgresOracle: ScriptTestPostgresOracle{ID: "information-schema-test-testinfoschemacolumns-0012-create-table-testnumtypes-id-int", Cleanup: []string{"DROP TABLE IF EXISTS test_table CASCADE", "DROP TABLE IF EXISTS test_table2 CASCADE", "DROP TABLE IF EXISTS testnumtypes CASCADE", "DROP SCHEMA IF EXISTS test_schema CASCADE"}},
				},
				{
					Query: "SELECT column_name, ordinal_position, data_type, udt_name, numeric_precision, numeric_precision_radix, numeric_scale FROM information_schema.columns WHERE table_name='testnumtypes' ORDER BY ordinal_position ASC;", PostgresOracle: ScriptTestPostgresOracle{ID: "information-schema-test-testinfoschemacolumns-0013-select-column_name-ordinal_position-data_type-udt_name", Cleanup: []string{"DROP TABLE IF EXISTS test_table CASCADE", "DROP TABLE IF EXISTS test_table2 CASCADE", "DROP TABLE IF EXISTS testnumtypes CASCADE", "DROP SCHEMA IF EXISTS test_schema CASCADE"}},
				},
				{
					Query: `CREATE TABLE teststringtypes (id INT PRIMARY KEY, col1 CHAR(10), col2 VARCHAR(10), col3 TEXT, col4 "char", col5 CHARACTER, col6 VARCHAR, col7 UUID);`, PostgresOracle: ScriptTestPostgresOracle{ID: "information-schema-test-testinfoschemacolumns-0014-create-table-teststringtypes-id-int", Cleanup: []string{"DROP TABLE IF EXISTS test_table CASCADE", "DROP TABLE IF EXISTS test_table2 CASCADE", "DROP TABLE IF EXISTS testnumtypes CASCADE", "DROP TABLE IF EXISTS teststringtypes CASCADE", "DROP SCHEMA IF EXISTS test_schema CASCADE"}},
				},
				{
					Query: "SELECT column_name, ordinal_position, data_type, udt_name, character_maximum_length, character_octet_length FROM information_schema.columns WHERE table_name='teststringtypes' ORDER BY ordinal_position ASC;", PostgresOracle: ScriptTestPostgresOracle{ID: "information-schema-test-testinfoschemacolumns-0015-select-column_name-ordinal_position-data_type-udt_name", Cleanup: []string{"DROP TABLE IF EXISTS test_table CASCADE", "DROP TABLE IF EXISTS test_table2 CASCADE", "DROP TABLE IF EXISTS testnumtypes CASCADE", "DROP TABLE IF EXISTS teststringtypes CASCADE", "DROP SCHEMA IF EXISTS test_schema CASCADE"}},
				},
				{
					Query: `CREATE TABLE testtimetypes (id INT PRIMARY KEY, col1 DATE, col2 TIME, col3 TIMESTAMP, col4 TIMESTAMPTZ,  col5 TIMETZ);`, PostgresOracle: ScriptTestPostgresOracle{ID: "information-schema-test-testinfoschemacolumns-0016-create-table-testtimetypes-id-int", Cleanup: []string{"DROP TABLE IF EXISTS test_table CASCADE", "DROP TABLE IF EXISTS test_table2 CASCADE", "DROP TABLE IF EXISTS testnumtypes CASCADE", "DROP TABLE IF EXISTS teststringtypes CASCADE", "DROP TABLE IF EXISTS testtimetypes CASCADE", "DROP SCHEMA IF EXISTS test_schema CASCADE"}},
				},
				{
					// TODO: Test timestamps with precision when it is implemented
					Query: "SELECT column_name, ordinal_position, data_type, datetime_precision FROM information_schema.columns WHERE table_name='testtimetypes' ORDER BY ordinal_position ASC;", PostgresOracle: ScriptTestPostgresOracle{ID: "information-schema-test-testinfoschemacolumns-0017-select-column_name-ordinal_position-data_type-datetime_precision", Cleanup: []string{"DROP TABLE IF EXISTS test_table CASCADE", "DROP TABLE IF EXISTS test_table2 CASCADE", "DROP TABLE IF EXISTS testnumtypes CASCADE", "DROP TABLE IF EXISTS teststringtypes CASCADE", "DROP TABLE IF EXISTS testtimetypes CASCADE", "DROP SCHEMA IF EXISTS test_schema CASCADE"}},
				},
				{
					Query:    `SELECT p.oid AS oid, p.relname AS table_name, n.nspname as table_schema FROM pg_class AS p JOIN pg_namespace AS n ON p.relnamespace=n.oid WHERE (n.nspname='public' AND p.relkind='r') AND left(relname, 5) <> 'dolt_';`,
					Expected: []sql.Row{{2957635223, "test_table", "public"}},
				},
				{
					Query: `select col_description(2957635223, ordinal_position) as comment from information_schema.columns limit 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "information-schema-test-testinfoschemacolumns-0019-select-col_description-2957635223-ordinal_position-as", Cleanup: []string{"DROP TABLE IF EXISTS test_table CASCADE", "DROP TABLE IF EXISTS test_table2 CASCADE", "DROP TABLE IF EXISTS testnumtypes CASCADE", "DROP TABLE IF EXISTS teststringtypes CASCADE", "DROP TABLE IF EXISTS testtimetypes CASCADE", "DROP SCHEMA IF EXISTS test_schema CASCADE"}},
				},
			},
		},
	})
}

func TestInfoSchemaSchemata(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:     "information_schema.schemata",
			Database: "newdb",
			SetUpScript: []string{
				"create schema test_schema",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT catalog_name, schema_name FROM information_schema.schemata order by schema_name;`,
					Expected: []sql.Row{
						{"newdb", "dolt"},
						{"newdb", "information_schema"},
						{"newdb", "pg_catalog"},
						{"newdb", "public"},
						{"newdb", "test_schema"},
					},
				},
				{
					Query: `SELECT * FROM information_schema.schemata order by schema_name;`,
					Expected: []sql.Row{
						{"newdb", "dolt", "", nil, nil, nil, nil},
						{"newdb", "information_schema", "", nil, nil, nil, nil},
						{"newdb", "pg_catalog", "", nil, nil, nil, nil},
						{"newdb", "public", "", nil, nil, nil, nil},
						{"newdb", "test_schema", "", nil, nil, nil, nil},
					},
				},
			},
		},
	})
}

func TestInfoSchemaTables(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "information_schema.tables",
			SetUpScript: []string{
				"create table test_table (id int)",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM information_schema.tables WHERE table_name='test_table';`, PostgresOracle: ScriptTestPostgresOracle{ID: "information-schema-test-testinfoschematables-0001-select-*-from-information_schema.tables-where", ColumnModes: []string{"structural", "schema"}},
				},
				{
					Query: `SELECT DISTINCT table_schema FROM information_schema.tables order by table_schema;`, PostgresOracle: ScriptTestPostgresOracle{ID: "information-schema-test-testinfoschematables-0002-select-distinct-table_schema-from-information_schema.tables", ColumnModes: []string{"schema"}},
				},
				{
					Query: `SELECT table_catalog, table_schema FROM information_schema.tables group by table_catalog, table_schema order by table_schema;`, PostgresOracle: ScriptTestPostgresOracle{ID: "information-schema-test-testinfoschematables-0003-select-table_catalog-table_schema-from-information_schema.tables", ColumnModes: []string{"structural", "schema"}},
				},
				{
					Query: `SELECT table_catalog, table_schema, table_name FROM information_schema.tables WHERE table_schema='public';`,
					Expected: []sql.Row{
						{"postgres", "public", "test_table"},
					},
				},
				{
					Query: `CREATE SCHEMA test_schema;`, PostgresOracle: ScriptTestPostgresOracle{ID: "information-schema-test-testinfoschematables-0005-create-schema-test_schema", Cleanup: []string{"DROP TABLE IF EXISTS test_table CASCADE", "DROP SCHEMA IF EXISTS test_schema CASCADE"}},
				},
				{
					Query: `SET SEARCH_PATH TO test_schema;`, PostgresOracle: ScriptTestPostgresOracle{ID: "information-schema-test-testinfoschematables-0006-set-search_path-to-test_schema", Cleanup: []string{"DROP TABLE IF EXISTS test_table CASCADE", "DROP SCHEMA IF EXISTS test_schema CASCADE"}},
				},
				{
					Query: `CREATE TABLE test_table2 (id INT);`, PostgresOracle: ScriptTestPostgresOracle{ID: "information-schema-test-testinfoschematables-0007-create-table-test_table2-id-int", Cleanup: []string{"DROP TABLE IF EXISTS test_table CASCADE", "DROP TABLE IF EXISTS test_table2 CASCADE", "DROP SCHEMA IF EXISTS test_schema CASCADE"}},
				},
				{
					Query: `SELECT DISTINCT table_schema FROM information_schema.tables order by table_schema;`,
					Expected: []sql.Row{
						{"information_schema"}, {"pg_catalog"}, {"public"}, {"test_schema"},
					},
				},
				{
					Query: `SELECT table_catalog, table_schema FROM information_schema.tables group by table_catalog, table_schema order by table_schema;`,
					Expected: []sql.Row{
						{"postgres", "information_schema"},
						{"postgres", "pg_catalog"},
						{"postgres", "public"},
						{"postgres", "test_schema"},
					},
				},
				{
					Query: `SELECT table_catalog, table_schema, table_name FROM information_schema.tables WHERE table_schema='test_schema';`, PostgresOracle: ScriptTestPostgresOracle{ID: "information-schema-test-testinfoschematables-0010-select-table_catalog-table_schema-table_name-from", Cleanup: []string{"DROP TABLE IF EXISTS test_table CASCADE", "DROP TABLE IF EXISTS test_table2 CASCADE", "DROP SCHEMA IF EXISTS test_schema CASCADE"}},
				},
				{
					Query: "SELECT table_catalog, table_schema, table_name, table_type FROM information_schema.tables WHERE table_schema = 'test_schema' ORDER BY table_name;", PostgresOracle: ScriptTestPostgresOracle{ID: "information-schema-test-testinfoschematables-0011-select-table_catalog-table_schema-table_name-table_type", Cleanup: []string{"DROP TABLE IF EXISTS test_table CASCADE", "DROP TABLE IF EXISTS test_table2 CASCADE", "DROP SCHEMA IF EXISTS test_schema CASCADE"}},
				},
				{
					Query: `SELECT "table_schema", "table_name", obj_description(('"' || "table_schema" || '"."' || "table_name" || '"')::regclass, 'pg_class') AS table_comment FROM "information_schema"."tables" WHERE ("table_schema" = 'test_schema' AND "table_name" = 'test_table2')`, PostgresOracle: ScriptTestPostgresOracle{ID: "information-schema-test-testinfoschematables-0012-select-table_schema-table_name-obj_description-||", Cleanup: []string{"DROP TABLE IF EXISTS test_table CASCADE", "DROP TABLE IF EXISTS test_table2 CASCADE", "DROP SCHEMA IF EXISTS test_schema CASCADE"}},
				},
				{
					Query: `CREATE VIEW test_view AS SELECT * FROM test_table2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "information-schema-test-testinfoschematables-0013-create-view-test_view-as-select", Cleanup: []string{"DROP TABLE IF EXISTS test_table CASCADE", "DROP TABLE IF EXISTS test_table2 CASCADE", "DROP SCHEMA IF EXISTS test_schema CASCADE"}},
				},
				{
					Query: `SELECT table_catalog, table_schema, table_name, table_type  FROM information_schema.tables WHERE table_schema='test_schema' OR table_schema='public';`,
					Expected: []sql.Row{
						{"postgres", "public", "test_table", "BASE TABLE"},
						{"postgres", "test_schema", "test_view", "VIEW"},
						{"postgres", "test_schema", "test_table2", "BASE TABLE"},
					},
				},
			},
		},
	})
}

func TestInfoSchemaViews(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "information_schema.views",
			SetUpScript: []string{
				"create table test_table (id int)",
				"create view test_view as select * from test_table",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM information_schema.views order by table_schema;`, PostgresOracle: ScriptTestPostgresOracle{ID: "information-schema-test-testinfoschemaviews-0001-select-*-from-information_schema.views-order", ColumnModes: []string{"structural", "schema"}},
				},
				{
					Query: `SELECT DISTINCT table_schema FROM information_schema.views order by table_schema;`, PostgresOracle: ScriptTestPostgresOracle{ID: "information-schema-test-testinfoschemaviews-0002-select-distinct-table_schema-from-information_schema.views", ColumnModes: []string{"schema"}},
				},
				{
					Query: `SELECT table_catalog, table_schema FROM information_schema.views group by table_catalog, table_schema order by table_schema;`, PostgresOracle: ScriptTestPostgresOracle{ID: "information-schema-test-testinfoschemaviews-0003-select-table_catalog-table_schema-from-information_schema.views", ColumnModes: []string{"structural", "schema"}},
				},
				{
					Query: `SELECT table_catalog, table_schema, table_name FROM information_schema.views WHERE table_schema='public';`, PostgresOracle: ScriptTestPostgresOracle{ID: "information-schema-test-testinfoschemaviews-0004-select-table_catalog-table_schema-table_name-from", Cleanup: []string{"DROP TABLE IF EXISTS test_table CASCADE"}},
				},
				{
					Query: `CREATE SCHEMA test_schema;`, PostgresOracle: ScriptTestPostgresOracle{ID: "information-schema-test-testinfoschemaviews-0005-create-schema-test_schema", Cleanup: []string{"DROP TABLE IF EXISTS test_table CASCADE", "DROP SCHEMA IF EXISTS test_schema CASCADE"}},
				},
				{
					Query: `SET SEARCH_PATH TO test_schema;`, PostgresOracle: ScriptTestPostgresOracle{ID: "information-schema-test-testinfoschemaviews-0006-set-search_path-to-test_schema", Cleanup: []string{"DROP TABLE IF EXISTS test_table CASCADE", "DROP SCHEMA IF EXISTS test_schema CASCADE"}},
				},
				{
					Query: `CREATE TABLE test_table2 (id int);`, PostgresOracle: ScriptTestPostgresOracle{ID: "information-schema-test-testinfoschemaviews-0007-create-table-test_table2-id-int", Cleanup: []string{"DROP TABLE IF EXISTS test_table CASCADE", "DROP TABLE IF EXISTS test_table2 CASCADE", "DROP SCHEMA IF EXISTS test_schema CASCADE"}},
				},
				{
					Query: `CREATE VIEW test_view2 as select * from test_table2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "information-schema-test-testinfoschemaviews-0008-create-view-test_view2-as-select", Cleanup: []string{"DROP TABLE IF EXISTS test_table CASCADE", "DROP TABLE IF EXISTS test_table2 CASCADE", "DROP SCHEMA IF EXISTS test_schema CASCADE"}},
				},
				{
					Query: `SELECT DISTINCT table_schema FROM information_schema.views order by table_schema;`,
					Expected: []sql.Row{
						{"public"},
						{"test_schema"},
					},
				},
				{
					Query: `SELECT table_catalog, table_schema FROM information_schema.views group by table_catalog, table_schema order by table_schema;`,
					Expected: []sql.Row{
						{"postgres", "public"},
						{"postgres", "test_schema"},
					},
				},
				{
					Query: `SELECT table_catalog, table_schema, table_name FROM information_schema.views WHERE table_schema='test_schema';`, PostgresOracle: ScriptTestPostgresOracle{ID: "information-schema-test-testinfoschemaviews-0011-select-table_catalog-table_schema-table_name-from", Cleanup: []string{"DROP TABLE IF EXISTS test_table CASCADE", "DROP TABLE IF EXISTS test_table2 CASCADE", "DROP SCHEMA IF EXISTS test_schema CASCADE"}},
				},
				{
					Query: "SELECT table_catalog, table_schema, table_name, view_definition FROM information_schema.views WHERE table_schema = 'test_schema' ORDER BY table_name;",
					Expected: []sql.Row{
						{"postgres", "test_schema", "test_view2", "SELECT * FROM test_table2"},
					},
				},
			},
		},
	})
}

func TestInfoSchemaSequences(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "information_schema.sequences",
			SetUpScript: []string{
				"create sequence standard as smallint;",
				"create schema test_schema",
				"create table test_schema.test_table (id serial);",
				"create sequence big increment by 3 start with 10 minvalue 1 cycle;",
				"create sequence negative increment by -1",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "select * from information_schema.sequences where sequence_name = 'standard';", PostgresOracle: ScriptTestPostgresOracle{ID: "information-schema-test-testinfoschemasequences-0001-select-*-from-information_schema.sequences-where", ColumnModes: []string{"structural", "schema"}},
				},
				{
					Query: "select sequence_schema,sequence_name,data_type,numeric_precision from information_schema.sequences where sequence_name = 'test_table_id_seq';", PostgresOracle: ScriptTestPostgresOracle{ID: "information-schema-test-testinfoschemasequences-0002-select-sequence_schema-sequence_name-data_type-numeric_precision"},
				},
				{
					Query: "select sequence_name,data_type,numeric_precision,minimum_value,increment,cycle_option from information_schema.sequences where sequence_name = 'big';", PostgresOracle: ScriptTestPostgresOracle{ID: "information-schema-test-testinfoschemasequences-0003-select-sequence_name-data_type-numeric_precision-minimum_value"},
				},
				{
					Query: "select sequence_name, increment from information_schema.sequences where sequence_name = 'negative';", PostgresOracle: ScriptTestPostgresOracle{ID: "information-schema-test-testinfoschemasequences-0004-select-sequence_name-increment-from-information_schema.sequences"},
				},
			},
		},
	})
}
