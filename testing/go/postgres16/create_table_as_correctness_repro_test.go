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
)

// TestCreateTableAsWithNoDataDoesNotEvaluateQuery guards PostgreSQL's CTAS
// WITH NO DATA semantics: the table is created from the query's projection
// shape but the query is not executed, so a row-time error like 1/0 cannot
// fire and no rows are inserted.
func TestCreateTableAsWithNoDataDoesNotEvaluateQuery(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE AS WITH NO DATA does not evaluate query",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE ctas_no_data_error AS
						SELECT 1 / 0 AS value
						WITH NO DATA;`,
				},
				{
					Query: `SELECT value FROM ctas_no_data_error;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-as-correctness-repro-test-testcreatetableaswithnodatadoesnotevaluatequery-0001-select-value-from-ctas_no_data_error"},
				},
			},
		},
	})
}

// TestCreateTableAsWithNoDataPreservesColumnSchema guards that CREATE TABLE
// AS ... WITH NO DATA still derives the projected column names and types from
// the query, so subsequent inserts succeed against the inferred schema and
// queries against unknown columns fail.
func TestCreateTableAsWithNoDataPreservesColumnSchema(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "WITH NO DATA infers projection schema",
			SetUpScript: []string{
				`CREATE TABLE ctas_no_data_schema AS
					SELECT 1::INT AS id, 'kept'::TEXT AS label
					WITH NO DATA;`,
				`INSERT INTO ctas_no_data_schema VALUES (1, 'hello');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, label FROM ctas_no_data_schema;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-as-correctness-repro-test-testcreatetableaswithnodatapreservescolumnschema-0001-select-id-label-from-ctas_no_data_schema"},
				},
				{
					Query: `SELECT missing_col FROM ctas_no_data_schema;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-as-correctness-repro-test-testcreatetableaswithnodatapreservescolumnschema-0002-select-missing_col-from-ctas_no_data_schema",

						// TestCreateTableAsWithNoDataAcceptsUnionSource guards that WITH NO DATA
						// works when the source query is a set operation rather than a bare SELECT,
						// so the LIMIT 0 transform applied to suppress evaluation also threads
						// through UNION/INTERSECT/EXCEPT.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestCreateTableAsWithNoDataAcceptsUnionSource(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "WITH NO DATA accepts UNION source",
			SetUpScript: []string{
				`CREATE TABLE ctas_no_data_union AS
					SELECT 1 / 0 AS value
					UNION ALL
					SELECT 2 / 0 AS value
					WITH NO DATA;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT value FROM ctas_no_data_union;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-as-correctness-repro-test-testcreatetableaswithnodataacceptsunionsource-0001-select-value-from-ctas_no_data_union"},
				},
			},
		},
	})
}

// TestCreateTableAsWithDataClauseRepro guards PostgreSQL's explicit WITH DATA
// clause, which is equivalent to the default CTAS behavior.
func TestCreateTableAsWithDataClauseRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE AS WITH DATA evaluates query",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE ctas_with_data AS
						SELECT 42 AS value
						WITH DATA;`,
				},
				{
					Query: `SELECT value FROM ctas_with_data;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-as-correctness-repro-test-testcreatetableaswithdataclauserepro-0001-select-value-from-ctas_with_data"},
				},
			},
		},
	})
}

// TestCreateTableAsIfNotExistsDoesNotEvaluateQueryRepro reproduces a CTAS
// correctness bug: once the relation already exists, IF NOT EXISTS should skip
// the replacement query.
func TestCreateTableAsIfNotExistsDoesNotEvaluateQueryRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE AS IF NOT EXISTS skips replacement query",
			SetUpScript: []string{
				`CREATE TABLE ctas_if_not_exists_error AS SELECT 1 AS value;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE IF NOT EXISTS ctas_if_not_exists_error AS
						SELECT 1 / 0 AS value;`,
				},
				{
					Query: `SELECT value FROM ctas_if_not_exists_error;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-as-correctness-repro-test-testcreatetableasifnotexistsdoesnotevaluatequeryrepro-0001-select-value-from-ctas_if_not_exists_error"},
				},
			},
		},
	})
}

// TestCreateTableAsDoesNotCopyDefaultsRepro reproduces a CTAS schema bug: CTAS
// creates regular result columns and must not copy source-table defaults.
func TestCreateTableAsDoesNotCopyDefaultsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE AS does not copy defaults",
			SetUpScript: []string{
				`CREATE TABLE ctas_default_source (
					id INT,
					label TEXT DEFAULT 'source default'
				);`,
				`INSERT INTO ctas_default_source VALUES (1, 'source row');`,
				`CREATE TABLE ctas_default_copy AS
					SELECT id, label FROM ctas_default_source;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO ctas_default_copy (id) VALUES (2);`,
				},
				{
					Query: `SELECT id, label FROM ctas_default_copy ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-as-correctness-repro-test-testcreatetableasdoesnotcopydefaultsrepro-0001-select-id-label-from-ctas_default_copy"},
				},
			},
		},
	})
}

// TestCreateTableAsDoesNotCopyCheckConstraintsRepro guards that CTAS does not
// copy source-table CHECK constraints.
func TestCreateTableAsDoesNotCopyCheckConstraintsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE AS does not copy check constraints",
			SetUpScript: []string{
				`CREATE TABLE ctas_check_source (
					id INT,
					amount INT CHECK (amount > 0)
				);`,
				`INSERT INTO ctas_check_source VALUES (1, 10);`,
				`CREATE TABLE ctas_check_copy AS
					SELECT id, amount FROM ctas_check_source;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO ctas_check_copy VALUES (2, -10);`,
				},
				{
					Query: `SELECT id, amount FROM ctas_check_copy ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-as-correctness-repro-test-testcreatetableasdoesnotcopycheckconstraintsrepro-0001-select-id-amount-from-ctas_check_copy"},
				},
			},
		},
	})
}

// TestCreateTableAsDoesNotCopyUniqueIndexesRepro guards that CTAS does not copy
// source-table indexes or unique constraints.
func TestCreateTableAsDoesNotCopyUniqueIndexesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE AS does not copy unique indexes",
			SetUpScript: []string{
				`CREATE TABLE ctas_unique_source (
					id INT,
					code INT UNIQUE
				);`,
				`INSERT INTO ctas_unique_source VALUES (1, 7);`,
				`CREATE TABLE ctas_unique_copy AS
					SELECT id, code FROM ctas_unique_source;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO ctas_unique_copy VALUES (2, 7);`,
				},
				{
					Query: `SELECT id, code FROM ctas_unique_copy ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-as-correctness-repro-test-testcreatetableasdoesnotcopyuniqueindexesrepro-0001-select-id-code-from-ctas_unique_copy"},
				},
			},
		},
	})
}

// TestCreateTableAsPreservesDomainColumnTypesGuard guards that CTAS preserves
// domain-typed query output columns so future writes to the new table still
// enforce the domain's constraints.
func TestCreateTableAsPreservesDomainColumnTypesGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE AS preserves domain column types",
			SetUpScript: []string{
				`CREATE DOMAIN ctas_positive_domain AS integer
					CONSTRAINT ctas_positive_domain_check CHECK (VALUE > 0);`,
				`CREATE TABLE ctas_domain_source (
					id INT,
					amount ctas_positive_domain
				);`,
				`INSERT INTO ctas_domain_source VALUES (1, 10);`,
				`CREATE TABLE ctas_domain_copy AS
					SELECT id, amount FROM ctas_domain_source;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO ctas_domain_copy VALUES (2, -1);`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-as-correctness-repro-test-testcreatetableaspreservesdomaincolumntypesguard-0001-insert-into-ctas_domain_copy-values-2", Compare: "sqlstate"},
				},
				{
					Query: `SELECT id, amount
						FROM ctas_domain_copy
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-as-correctness-repro-test-testcreatetableaspreservesdomaincolumntypesguard-0002-select-id-amount-from-ctas_domain_copy"},
				},
			},
		},
	})
}

// TestCreateTableAsTextDomainTypmodMaterializesCoercedValueRepro reproduces a
// CTAS persistence bug: PostgreSQL materializes text-domain query outputs using
// the domain base type's typmod and preserves the output domain type.
func TestCreateTableAsTextDomainTypmodMaterializesCoercedValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE AS text domain typmod materializes coerced value",
			SetUpScript: []string{
				`CREATE DOMAIN varchar3_ctas_domain AS varchar(3);`,
				`CREATE DOMAIN char3_ctas_domain AS character(3);`,
				`CREATE TABLE ctas_text_domain_typmod_items AS
					SELECT 'abc   '::varchar3_ctas_domain AS v,
						'ab'::char3_ctas_domain AS c;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT v, length(v), c = 'ab '::CHARACTER(3), octet_length(c), pg_typeof(v)::text, pg_typeof(c)::text
						FROM ctas_text_domain_typmod_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-as-correctness-repro-test-testcreatetableastextdomaintypmodmaterializescoercedvaluerepro-0001-select-v-length-v-c"},
				},
				{
					Query: `SELECT format_type(atttypid, atttypmod)
						FROM pg_attribute
						WHERE attrelid = 'ctas_text_domain_typmod_items'::regclass
							AND attnum > 0
						ORDER BY attnum;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-as-correctness-repro-test-testcreatetableastextdomaintypmodmaterializescoercedvaluerepro-0002-select-format_type-atttypid-atttypmod-from"},
				},
			},
		},
	})
}

// TestCreateTableAsTimetzTypmodMaterializesRoundedValueRepro reproduces a CTAS
// persistence bug: PostgreSQL materializes typmod-constrained timetz query
// output using the rounded value and preserves the output column typmod.
func TestCreateTableAsTimetzTypmodMaterializesRoundedValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE AS timetz typmod materializes rounded value",
			SetUpScript: []string{
				`CREATE TABLE ctas_timetz_typmod_items AS
					SELECT CAST('21:43:56.789+00'::timetz AS TIMETZ(0)) AS tz;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT tz::text
						FROM ctas_timetz_typmod_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-as-correctness-repro-test-testcreatetableastimetztypmodmaterializesroundedvaluerepro-0001-select-tz::text-from-ctas_timetz_typmod_items"},
				},
				{
					Query: `SELECT format_type(a.atttypid, a.atttypmod)
						FROM pg_catalog.pg_attribute a
						WHERE a.attrelid = 'ctas_timetz_typmod_items'::regclass
							AND a.attname = 'tz';`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-as-correctness-repro-test-testcreatetableastimetztypmodmaterializesroundedvaluerepro-0002-select-format_type-a.atttypid-a.atttypmod-from"},
				},
			},
		},
	})
}

// TestCreateTableAsTimeTypmodMaterializesRoundedValueGuard guards that CTAS
// materializes typmod-constrained time query output using the rounded value and
// preserves the output column typmod.
func TestCreateTableAsTimeTypmodMaterializesRoundedValueGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE AS time typmod materializes rounded value",
			SetUpScript: []string{
				`CREATE TABLE ctas_time_typmod_items AS
					SELECT CAST('21:43:56.789'::time AS TIME(0)) AS t;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT t::text
						FROM ctas_time_typmod_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-as-correctness-repro-test-testcreatetableastimetypmodmaterializesroundedvalueguard-0001-select-t::text-from-ctas_time_typmod_items"},
				},
				{
					Query: `SELECT format_type(a.atttypid, a.atttypmod)
						FROM pg_catalog.pg_attribute a
						WHERE a.attrelid = 'ctas_time_typmod_items'::regclass
							AND a.attname = 't';`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-as-correctness-repro-test-testcreatetableastimetypmodmaterializesroundedvalueguard-0002-select-format_type-a.atttypid-a.atttypmod-from"},
				},
			},
		},
	})
}

// TestCreateTableAsNumericTypmodMaterializesRoundedValueGuard guards that CTAS
// materializes typmod-constrained numeric query output using the rounded value
// and preserves the output column typmod.
func TestCreateTableAsNumericTypmodMaterializesRoundedValueGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE AS numeric typmod materializes rounded value",
			SetUpScript: []string{
				`CREATE TABLE ctas_numeric_typmod_items AS
					SELECT CAST(123.456 AS NUMERIC(5,2)) AS amount;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT amount::text
						FROM ctas_numeric_typmod_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-as-correctness-repro-test-testcreatetableasnumerictypmodmaterializesroundedvalueguard-0001-select-amount::text-from-ctas_numeric_typmod_items"},
				},
				{
					Query: `SELECT format_type(a.atttypid, a.atttypmod)
						FROM pg_catalog.pg_attribute a
						WHERE a.attrelid = 'ctas_numeric_typmod_items'::regclass
							AND a.attname = 'amount';`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-as-correctness-repro-test-testcreatetableasnumerictypmodmaterializesroundedvalueguard-0002-select-format_type-a.atttypid-a.atttypmod-from"},
				},
			},
		},
	})
}

// TestCreateTableAsTimestampTypmodMaterializesRoundedValueRepro reproduces a
// CTAS persistence bug: PostgreSQL materializes typmod-constrained timestamp
// query output using the rounded value and preserves the output column typmod.
func TestCreateTableAsTimestampTypmodMaterializesRoundedValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE AS timestamp typmod materializes rounded value",
			SetUpScript: []string{
				`CREATE TABLE ctas_timestamp_typmod_items AS
					SELECT CAST(TIMESTAMP '2021-09-15 21:43:56.789' AS TIMESTAMP(0)) AS ts;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT ts::text
						FROM ctas_timestamp_typmod_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-as-correctness-repro-test-testcreatetableastimestamptypmodmaterializesroundedvaluerepro-0001-select-ts::text-from-ctas_timestamp_typmod_items"},
				},
				{
					Query: `SELECT format_type(a.atttypid, a.atttypmod)
						FROM pg_catalog.pg_attribute a
						WHERE a.attrelid = 'ctas_timestamp_typmod_items'::regclass
							AND a.attname = 'ts';`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-as-correctness-repro-test-testcreatetableastimestamptypmodmaterializesroundedvaluerepro-0002-select-format_type-a.atttypid-a.atttypmod-from"},
				},
			},
		},
	})
}

// TestCreateTableAsTimestamptzTypmodMaterializesRoundedValueRepro reproduces a
// CTAS persistence bug: PostgreSQL materializes typmod-constrained timestamptz
// query output using the rounded value and preserves the output column typmod.
func TestCreateTableAsTimestamptzTypmodMaterializesRoundedValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE AS timestamptz typmod materializes rounded value",
			SetUpScript: []string{
				`SET TIME ZONE 'UTC';`,
				`CREATE TABLE ctas_timestamptz_typmod_items AS
					SELECT CAST(TIMESTAMPTZ '2021-09-15 21:43:56.789+00' AS TIMESTAMPTZ(0)) AS ts;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT ts::text
						FROM ctas_timestamptz_typmod_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-as-correctness-repro-test-testcreatetableastimestamptztypmodmaterializesroundedvaluerepro-0001-select-ts::text-from-ctas_timestamptz_typmod_items"},
				},
				{
					Query: `SELECT format_type(a.atttypid, a.atttypmod)
						FROM pg_catalog.pg_attribute a
						WHERE a.attrelid = 'ctas_timestamptz_typmod_items'::regclass
							AND a.attname = 'ts';`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-as-correctness-repro-test-testcreatetableastimestamptztypmodmaterializesroundedvaluerepro-0002-select-format_type-a.atttypid-a.atttypmod-from"},
				},
			},
		},
	})
}

// TestCreateTableAsIntervalTypmodMaterializesRestrictedValueRepro reproduces a
// CTAS persistence bug: PostgreSQL materializes typmod-constrained interval
// query output using the restricted value and preserves the output column typmod.
func TestCreateTableAsIntervalTypmodMaterializesRestrictedValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE AS interval typmod materializes restricted value",
			SetUpScript: []string{
				`CREATE TABLE ctas_interval_typmod_items AS
					SELECT CAST(INTERVAL '3 days 04:05:06.789' AS INTERVAL DAY TO SECOND(0)) AS ds;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT ds::text
						FROM ctas_interval_typmod_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-as-correctness-repro-test-testcreatetableasintervaltypmodmaterializesrestrictedvaluerepro-0001-select-ds::text-from-ctas_interval_typmod_items"},
				},
				{
					Query: `SELECT format_type(a.atttypid, a.atttypmod)
						FROM pg_catalog.pg_attribute a
						WHERE a.attrelid = 'ctas_interval_typmod_items'::regclass
							AND a.attname = 'ds';`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-as-correctness-repro-test-testcreatetableasintervaltypmodmaterializesrestrictedvaluerepro-0002-select-format_type-a.atttypid-a.atttypmod-from"},
				},
			},
		},
	})
}

// TestCreateTableAsVarcharTypmodMaterializesTruncatedValueGuard guards that
// CTAS materializes typmod-constrained varchar output using the truncated value
// and preserves the output column typmod.
func TestCreateTableAsVarcharTypmodMaterializesTruncatedValueGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE AS varchar typmod materializes truncated value",
			SetUpScript: []string{
				`CREATE TABLE ctas_varchar_typmod_items AS
					SELECT CAST('abcd' AS VARCHAR(3)) AS label;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT label
						FROM ctas_varchar_typmod_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-as-correctness-repro-test-testcreatetableasvarchartypmodmaterializestruncatedvalueguard-0001-select-label-from-ctas_varchar_typmod_items"},
				},
				{
					Query: `SELECT format_type(a.atttypid, a.atttypmod)
						FROM pg_catalog.pg_attribute a
						WHERE a.attrelid = 'ctas_varchar_typmod_items'::regclass
							AND a.attname = 'label';`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-as-correctness-repro-test-testcreatetableasvarchartypmodmaterializestruncatedvalueguard-0002-select-format_type-a.atttypid-a.atttypmod-from"},
				},
			},
		},
	})
}

// TestCreateTableAsCharacterTypmodMaterializesPaddedValueRepro reproduces a
// CTAS persistence bug: PostgreSQL materializes typmod-constrained character
// query output using the padded value and preserves the output column typmod.
func TestCreateTableAsCharacterTypmodMaterializesPaddedValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE AS character typmod materializes padded value",
			SetUpScript: []string{
				`CREATE TABLE ctas_character_typmod_items AS
					SELECT CAST('ab' AS CHARACTER(3)) AS label;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT octet_length(label)
						FROM ctas_character_typmod_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-as-correctness-repro-test-testcreatetableascharactertypmodmaterializespaddedvaluerepro-0001-select-octet_length-label-from-ctas_character_typmod_items"},
				},
				{
					Query: `SELECT format_type(a.atttypid, a.atttypmod)
						FROM pg_catalog.pg_attribute a
						WHERE a.attrelid = 'ctas_character_typmod_items'::regclass
							AND a.attname = 'label';`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-as-correctness-repro-test-testcreatetableascharactertypmodmaterializespaddedvaluerepro-0002-select-format_type-a.atttypid-a.atttypmod-from"},
				},
			},
		},
	})
}

// TestCreateTableAsExplicitColumnNamesRepro reproduces a CTAS column-list bug:
// an explicit column list should rename the query output columns.
func TestCreateTableAsExplicitColumnNamesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE AS explicit column names rename output",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE ctas_explicit_names (new_id, new_label) AS
						SELECT 1 AS old_id, 'one'::text AS old_label;`,
				},
				{
					Query: `SELECT column_name
						FROM information_schema.columns
						WHERE table_schema = 'public'
							AND table_name = 'ctas_explicit_names'
						ORDER BY ordinal_position;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-as-correctness-repro-test-testcreatetableasexplicitcolumnnamesrepro-0001-select-column_name-from-information_schema.columns-where"},
				},
				{
					Query: `SELECT new_id, new_label FROM ctas_explicit_names;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-as-correctness-repro-test-testcreatetableasexplicitcolumnnamesrepro-0002-select-new_id-new_label-from-ctas_explicit_names"},
				},
			},
		},
	})
}
