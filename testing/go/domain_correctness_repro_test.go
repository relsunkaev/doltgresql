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

// TestDomainCastEnforcesConstraintsGuard guards that domain constraints are
// checked when a value is cast to that domain, even outside table DML.
func TestDomainCastEnforcesConstraintsGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "domain casts enforce constraints",
			SetUpScript: []string{
				`CREATE DOMAIN positive_cast_domain AS integer
					CONSTRAINT positive_cast_domain_check CHECK (VALUE > 0);`,
				`CREATE DOMAIN not_null_cast_domain AS integer NOT NULL;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT (-1)::positive_cast_domain;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testdomaincastenforcesconstraintsguard-0001-select-1-::positive_cast_domain", Compare: "sqlstate"},
				},
				{
					Query: `SELECT NULL::not_null_cast_domain;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testdomaincastenforcesconstraintsguard-0002-select-null::not_null_cast_domain",

						// TestDomainValueCastsToBaseTypeRepro reproduces a domain correctness bug:
						// PostgreSQL allows a domain value to be cast back to its base type.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestDomainValueCastsToBaseTypeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "domain values cast to base type",
			SetUpScript: []string{
				`CREATE DOMAIN base_cast_domain AS integer
					CONSTRAINT base_cast_domain_check CHECK (VALUE > 0);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT 7::base_cast_domain::integer;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testdomainvaluecaststobasetyperepro-0001-select-7::base_cast_domain::integer"},
				},
			},
		},
	})
}

// TestDomainValuesUseBaseTypeOperatorsRepro reproduces a domain expression
// correctness bug: domain values should participate in operators defined for
// their base type.
func TestDomainValuesUseBaseTypeOperatorsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "domain values use base type operators",
			SetUpScript: []string{
				`CREATE DOMAIN operator_domain AS integer
					CONSTRAINT operator_domain_check CHECK (VALUE > 0);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT 7::operator_domain + 5;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testdomainvaluesusebasetypeoperatorsrepro-0001-select-7::operator_domain-+-5"},
				},
				{
					Query: `SELECT 5 + 7::operator_domain;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testdomainvaluesusebasetypeoperatorsrepro-0002-select-5-+-7::operator_domain"},
				},
				{
					Query: `SELECT 7::operator_domain + '5';`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testdomainvaluesusebasetypeoperatorsrepro-0003-select-7::operator_domain-+-5"},
				},
				{
					Query: `SELECT 7::operator_domain = 7;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testdomainvaluesusebasetypeoperatorsrepro-0004-select-7::operator_domain-=-7"},
				},
				{
					Query: `SELECT 7::operator_domain = 7::operator_domain;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testdomainvaluesusebasetypeoperatorsrepro-0005-select-7::operator_domain-=-7::operator_domain"},
				},
			},
		},
	})
}

// TestTemporalDomainTypmodsRoundStoredValuesRepro reproduces a domain storage
// correctness bug: PostgreSQL applies typmod precision from a domain's base
// type when values are assigned to domain-typed columns.
func TestTemporalDomainTypmodsRoundStoredValuesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "temporal domain typmods round stored values",
			SetUpScript: []string{
				`SET TIME ZONE 'UTC';`,
				`CREATE DOMAIN ts0_domain AS timestamp(0);`,
				`CREATE DOMAIN interval_ds0_domain AS interval day to second(0);`,
				`CREATE TABLE temporal_domain_typmod_items (
					id INT PRIMARY KEY,
					ts ts0_domain,
					ds interval_ds0_domain
				);`,
				`INSERT INTO temporal_domain_typmod_items VALUES
					(1, '2021-09-15 21:43:56.789',
						'3 days 04:05:06.789');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT ts::text, ds::text
						FROM temporal_domain_typmod_items
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testtemporaldomaintypmodsroundstoredvaluesrepro-0001-select-ts::text-ds::text-from-temporal_domain_typmod_items"},
				},
			},
		},
	})
}

// TestTimetzDomainTypmodsRoundValuesRepro reproduces a domain correctness bug:
// PostgreSQL applies a timetz domain's base-type typmod when values are stored
// or explicitly cast to that domain.
func TestTimetzDomainTypmodsRoundValuesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "timetz domain typmods round values",
			SetUpScript: []string{
				`CREATE DOMAIN timetz0_domain AS timetz(0);`,
				`CREATE TABLE timetz_domain_typmod_items (
					id INT PRIMARY KEY,
					tz timetz0_domain
				);`,
				`INSERT INTO timetz_domain_typmod_items VALUES
					(1, '21:43:56.789+00'::timetz);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT tz::text
						FROM timetz_domain_typmod_items
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testtimetzdomaintypmodsroundvaluesrepro-0001-select-tz::text-from-timetz_domain_typmod_items-order"},
				},
				{
					Query: `SELECT '21:43:56.789+00'::timetz::timetz0_domain::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testtimetzdomaintypmodsroundvaluesrepro-0002-select-21:43:56.789+00-::timetz::timetz0_domain::text"},
				},
			},
		},
	})
}

// TestTextDomainTypmodsCoerceValuesRepro reproduces a domain correctness bug:
// PostgreSQL applies varchar(n) and character(n) typmods from a domain's base
// type when values are stored or explicitly cast to that domain.
func TestTextDomainTypmodsCoerceValuesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "text domain typmods coerce stored values",
			SetUpScript: []string{
				`CREATE DOMAIN varchar3_domain_items AS varchar(3);`,
				`CREATE DOMAIN char3_domain_items AS character(3);`,
				`CREATE TABLE text_domain_typmod_items (
					id INT PRIMARY KEY,
					v varchar3_domain_items,
					c char3_domain_items
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO text_domain_typmod_items VALUES (1, 'abc   ', 'ab');`,
				},
				{
					Query: `SELECT v, length(v), c = 'ab '::CHARACTER(3), octet_length(c), pg_typeof(v)::text, pg_typeof(c)::text
						FROM text_domain_typmod_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testtextdomaintypmodscoercevaluesrepro-0001-select-v-length-v-c"},
				},
			},
		},
		{
			Name: "text domain typmod casts coerce values",
			SetUpScript: []string{
				`CREATE DOMAIN varchar3_domain_casts AS varchar(3);`,
				`CREATE DOMAIN char3_domain_casts AS character(3);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT 'abc   '::varchar3_domain_casts::text,
						length('abc   '::varchar3_domain_casts),
						'ab'::char3_domain_casts = 'ab '::CHARACTER(3),
						octet_length('ab'::char3_domain_casts);`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testtextdomaintypmodscoercevaluesrepro-0002-select-abc-::varchar3_domain_casts::text-length-abc"},
				},
			},
		},
	})
}

// TestNumericDomainTypmodsRoundStoredValuesRepro reproduces a domain storage
// correctness bug: PostgreSQL applies numeric precision and scale from a
// domain's base type when values are assigned to domain-typed columns.
func TestNumericDomainTypmodsRoundStoredValuesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "numeric domain typmods round stored values",
			SetUpScript: []string{
				`CREATE DOMAIN num52_domain AS numeric(5,2);`,
				`CREATE TABLE numeric_domain_typmod_items (
					id INT PRIMARY KEY,
					amount num52_domain
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO numeric_domain_typmod_items VALUES (1, 123.456);`,
				},
				{
					Query: `SELECT amount::text
						FROM numeric_domain_typmod_items
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testnumericdomaintypmodsroundstoredvaluesrepro-0001-select-amount::text-from-numeric_domain_typmod_items-order"},
				},
				{
					Query: `INSERT INTO numeric_domain_typmod_items VALUES (2, 999.995);`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testnumericdomaintypmodsroundstoredvaluesrepro-0002-insert-into-numeric_domain_typmod_items-values-2",

						// TestDomainTypmodCastsUseCoercedValueRepro reproduces a correctness bug:
						// PostgreSQL applies a domain's base-type typmod when a value is explicitly
						// cast to that domain.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestDomainTypmodCastsUseCoercedValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "domain typmod casts use coerced values",
			SetUpScript: []string{
				`CREATE DOMAIN num52_cast_domain AS numeric(5,2);`,
				`CREATE DOMAIN ts0_cast_domain AS timestamp(0);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT 123.456::num52_cast_domain::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testdomaintypmodcastsusecoercedvaluerepro-0001-select-123.456::num52_cast_domain::text"},
				},
				{
					Query: `SELECT timestamp '2021-09-15 21:43:56.789'::ts0_cast_domain::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testdomaintypmodcastsusecoercedvaluerepro-0002-select-timestamp-2021-09-15-21:43:56.789-::ts0_cast_domain::text"},
				},
			},
		},
	})
}

// TestDomainTypmodSqlFunctionReturnUsesCoercedValueRepro reproduces a
// correctness bug: PostgreSQL applies a domain's base-type typmod when a SQL
// function returns a value declared as that domain.
func TestDomainTypmodSqlFunctionReturnUsesCoercedValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "domain typmod SQL function return uses coerced value",
			SetUpScript: []string{
				`CREATE DOMAIN num52_return_domain AS numeric(5,2);`,
				`CREATE DOMAIN ts0_return_domain AS timestamp(0);`,
				`CREATE FUNCTION domain_typmod_return_numeric()
					RETURNS num52_return_domain
					LANGUAGE SQL AS $$ SELECT 123.456 $$;`,
				`CREATE FUNCTION domain_typmod_return_timestamp()
					RETURNS ts0_return_domain
					LANGUAGE SQL AS $$ SELECT timestamp '2021-09-15 21:43:56.789' $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT domain_typmod_return_numeric()::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testdomaintypmodsqlfunctionreturnusescoercedvaluerepro-0001-select-domain_typmod_return_numeric-::text"},
				},
				{
					Query: `SELECT domain_typmod_return_timestamp()::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testdomaintypmodsqlfunctionreturnusescoercedvaluerepro-0002-select-domain_typmod_return_timestamp-::text"},
				},
			},
		},
	})
}

// TestTextDomainTypmodSqlFunctionReturnUsesCoercedValueRepro reproduces a
// correctness bug: PostgreSQL applies varchar(n) and character(n) base-type
// typmods when a SQL function returns a value declared as that domain.
func TestTextDomainTypmodSqlFunctionReturnUsesCoercedValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "text domain SQL function return uses coerced value",
			SetUpScript: []string{
				`CREATE DOMAIN varchar3_return_domain AS varchar(3);`,
				`CREATE DOMAIN char3_return_domain AS character(3);`,
				`CREATE FUNCTION text_domain_return_v()
					RETURNS varchar3_return_domain
					LANGUAGE SQL AS $$ SELECT 'abc   ' $$;`,
				`CREATE FUNCTION text_domain_return_c()
					RETURNS char3_return_domain
					LANGUAGE SQL AS $$ SELECT 'ab' $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT text_domain_return_v()::text,
						length(text_domain_return_v()),
						text_domain_return_c() = 'ab '::CHARACTER(3),
						octet_length(text_domain_return_c()),
						pg_typeof(text_domain_return_v())::text,
						pg_typeof(text_domain_return_c())::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testtextdomaintypmodsqlfunctionreturnusescoercedvaluerepro-0001-select-text_domain_return_v-::text-length-text_domain_return_v"},
				},
			},
		},
	})
}

// TestDomainTypmodUniqueUsesCoercedValuesRepro reproduces a data consistency
// bug: PostgreSQL enforces unique constraints after applying typmods from a
// domain's base type.
func TestDomainTypmodUniqueUsesCoercedValuesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "numeric domain unique constraint uses rounded values",
			SetUpScript: []string{
				`CREATE DOMAIN num52_unique_domain AS numeric(5,2);`,
				`CREATE TABLE numeric_domain_unique_items (
					id INT PRIMARY KEY,
					amount num52_unique_domain UNIQUE
				);`,
				`INSERT INTO numeric_domain_unique_items VALUES (1, 1.231);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO numeric_domain_unique_items VALUES (2, 1.234);`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testdomaintypmoduniqueusescoercedvaluesrepro-0001-insert-into-numeric_domain_unique_items-values-2", Compare: "sqlstate"},
				},
				{
					Query: `SELECT id, amount::text
						FROM numeric_domain_unique_items
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testdomaintypmoduniqueusescoercedvaluesrepro-0002-select-id-amount::text-from-numeric_domain_unique_items"},
				},
			},
		},
		{
			Name: "timestamp domain unique constraint uses rounded values",
			SetUpScript: []string{
				`CREATE DOMAIN ts0_unique_domain AS timestamp(0);`,
				`CREATE TABLE timestamp_domain_unique_items (
					id INT PRIMARY KEY,
					ts ts0_unique_domain UNIQUE
				);`,
				`INSERT INTO timestamp_domain_unique_items VALUES
					(1, '2021-09-15 21:43:56.600');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO timestamp_domain_unique_items VALUES
						(2, '2021-09-15 21:43:56.700');`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testdomaintypmoduniqueusescoercedvaluesrepro-0003-insert-into-timestamp_domain_unique_items-values-2", Compare: "sqlstate"},
				},
				{
					Query: `SELECT id, ts::text
						FROM timestamp_domain_unique_items
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testdomaintypmoduniqueusescoercedvaluesrepro-0004-select-id-ts::text-from-timestamp_domain_unique_items"},
				},
			},
		},
	})
}

// TestTextDomainTypmodUniqueUsesCoercedValuesRepro reproduces a data
// consistency bug: PostgreSQL enforces unique constraints over text-domain
// columns after applying varchar(n) and character(n) base-type typmods.
func TestTextDomainTypmodUniqueUsesCoercedValuesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "varchar domain unique constraint uses coerced values",
			SetUpScript: []string{
				`CREATE DOMAIN varchar3_unique_domain AS varchar(3);`,
				`CREATE TABLE varchar_domain_unique_items (
					id INT PRIMARY KEY,
					label varchar3_unique_domain UNIQUE
				);`,
				`INSERT INTO varchar_domain_unique_items VALUES (1, 'abc');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO varchar_domain_unique_items VALUES (2, 'abc   ');`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testtextdomaintypmoduniqueusescoercedvaluesrepro-0001-insert-into-varchar_domain_unique_items-values-2", Compare: "sqlstate"},
				},
				{
					Query: `SELECT id, label, length(label)
						FROM varchar_domain_unique_items
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testtextdomaintypmoduniqueusescoercedvaluesrepro-0002-select-id-label-length-label"},
				},
			},
		},
		{
			Name: "character domain unique constraint uses padded equality",
			SetUpScript: []string{
				`CREATE DOMAIN char3_unique_domain AS character(3);`,
				`CREATE TABLE char_domain_unique_items (
					id INT PRIMARY KEY,
					label char3_unique_domain UNIQUE
				);`,
				`INSERT INTO char_domain_unique_items VALUES (1, 'a');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO char_domain_unique_items VALUES (2, 'a  ');`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testtextdomaintypmoduniqueusescoercedvaluesrepro-0003-insert-into-char_domain_unique_items-values-2", Compare: "sqlstate"},
				},
				{
					Query: `SELECT id, label = 'a  '::CHARACTER(3), octet_length(label)
						FROM char_domain_unique_items
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testtextdomaintypmoduniqueusescoercedvaluesrepro-0004-select-id-label-=-a"},
				},
			},
		},
	})
}

// TestDomainTypmodGeneratedColumnUsesCoercedValueRepro reproduces a data
// consistency bug: PostgreSQL computes stored generated columns from the
// coerced domain value.
func TestDomainTypmodGeneratedColumnUsesCoercedValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "domain typmod generated column uses coerced value",
			SetUpScript: []string{
				`CREATE DOMAIN num52_generated_domain AS numeric(5,2);`,
				`CREATE TABLE domain_typmod_generated_items (
					id INT PRIMARY KEY,
					amount num52_generated_domain,
					amount_text TEXT GENERATED ALWAYS AS (amount::text) STORED
				);`,
				`INSERT INTO domain_typmod_generated_items (id, amount) VALUES (1, 123.456);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT amount::text, amount_text
						FROM domain_typmod_generated_items
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testdomaintypmodgeneratedcolumnusescoercedvaluerepro-0001-select-amount::text-amount_text-from-domain_typmod_generated_items"},
				},
			},
		},
	})
}

// TestTextDomainTypmodGeneratedColumnUsesCoercedValueRepro reproduces a data
// consistency bug: PostgreSQL computes stored generated columns from text
// domain values after applying the domain base-type typmod.
func TestTextDomainTypmodGeneratedColumnUsesCoercedValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "text domain typmod generated columns use coerced values",
			SetUpScript: []string{
				`CREATE DOMAIN varchar3_generated_domain AS varchar(3);`,
				`CREATE DOMAIN char3_generated_domain AS character(3);`,
				`CREATE TABLE text_domain_generated_items (
					id INT PRIMARY KEY,
					v varchar3_generated_domain,
					v_len INT GENERATED ALWAYS AS (length(v)) STORED,
					c char3_generated_domain,
					c_octets INT GENERATED ALWAYS AS (octet_length(c)) STORED
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO text_domain_generated_items (id, v, c)
						VALUES (1, 'abc   ', 'ab');`,
				},
				{
					Query: `SELECT v, v_len, c = 'ab '::CHARACTER(3), c_octets, pg_typeof(v)::text, pg_typeof(c)::text
						FROM text_domain_generated_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testtextdomaintypmodgeneratedcolumnusescoercedvaluerepro-0001-select-v-v_len-c-="},
				},
			},
		},
	})
}

// TestDomainTypmodDefaultUsesCoercedValueRepro reproduces a data consistency
// bug: PostgreSQL applies base-type typmods to domain default values before
// storing them in domain-typed columns.
func TestDomainTypmodDefaultUsesCoercedValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "domain typmod default uses coerced value",
			SetUpScript: []string{
				`CREATE DOMAIN num52_default_domain AS numeric(5,2) DEFAULT 123.456;`,
				`CREATE TABLE domain_typmod_default_items (
					id INT PRIMARY KEY,
					amount num52_default_domain
				);`,
				`INSERT INTO domain_typmod_default_items (id) VALUES (1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT amount::text
						FROM domain_typmod_default_items
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testdomaintypmoddefaultusescoercedvaluerepro-0001-select-amount::text-from-domain_typmod_default_items-order"},
				},
			},
		},
	})
}

// TestTextDomainTypmodDefaultUsesCoercedValueRepro reproduces a data
// consistency bug: PostgreSQL applies varchar(n) and character(n) base-type
// typmods to domain default values before storing them.
func TestTextDomainTypmodDefaultUsesCoercedValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "varchar domain typmod default uses coerced value",
			SetUpScript: []string{
				`CREATE DOMAIN varchar3_default_domain AS varchar(3) DEFAULT 'abc   ';`,
				`CREATE TABLE varchar_domain_default_items (
					id INT PRIMARY KEY,
					label varchar3_default_domain
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO varchar_domain_default_items (id) VALUES (1);`,
				},
				{
					Query: `SELECT label, length(label), pg_typeof(label)::text
						FROM varchar_domain_default_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testtextdomaintypmoddefaultusescoercedvaluerepro-0001-select-label-length-label-pg_typeof"},
				},
			},
		},
		{
			Name: "character domain typmod default uses padded value",
			SetUpScript: []string{
				`CREATE DOMAIN char3_default_domain AS character(3) DEFAULT 'ab';`,
				`CREATE TABLE char_domain_default_items (
					id INT PRIMARY KEY,
					label char3_default_domain
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO char_domain_default_items (id) VALUES (1);`,
				},
				{
					Query: `SELECT label = 'ab '::CHARACTER(3), octet_length(label), pg_typeof(label)::text
						FROM char_domain_default_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testtextdomaintypmoddefaultusescoercedvaluerepro-0002-select-label-=-ab-::character"},
				},
			},
		},
	})
}

// TestDomainTypmodCheckUsesCoercedValueRepro reproduces a data consistency bug:
// PostgreSQL validates domain CHECK constraints after applying the domain base
// type's typmod.
func TestDomainTypmodCheckUsesCoercedValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "domain typmod check uses coerced value",
			SetUpScript: []string{
				`CREATE DOMAIN num52_check_domain AS numeric(5,2)
					CHECK (VALUE = 123.456);`,
				`CREATE TABLE domain_typmod_check_items (
					id INT PRIMARY KEY,
					amount num52_check_domain
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO domain_typmod_check_items VALUES (1, 123.456);`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testdomaintypmodcheckusescoercedvaluerepro-0001-insert-into-domain_typmod_check_items-values-1", Compare: "sqlstate"},
				},
				{
					Query: `SELECT count(*) FROM domain_typmod_check_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testdomaintypmodcheckusescoercedvaluerepro-0002-select-count-*-from-domain_typmod_check_items"},
				},
			},
		},
	})
}

// TestTextDomainTypmodCheckUsesCoercedValueRepro reproduces a correctness bug:
// PostgreSQL evaluates text-domain CHECK constraints after applying the domain
// base-type typmod.
func TestTextDomainTypmodCheckUsesCoercedValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "text domain CHECK uses coerced value",
			SetUpScript: []string{
				`CREATE DOMAIN varchar3_check_domain AS varchar(3)
					CHECK (length(VALUE) = 3);`,
				`CREATE DOMAIN char3_check_domain AS character(3)
					CHECK (octet_length(VALUE) = 3);`,
				`CREATE TABLE text_domain_check_items (
					id INT PRIMARY KEY,
					v varchar3_check_domain,
					c char3_check_domain
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO text_domain_check_items VALUES (1, 'abc   ', 'ab');`,
				},
				{
					Query: `SELECT v, length(v), c = 'ab '::CHARACTER(3), octet_length(c)
						FROM text_domain_check_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testtextdomaintypmodcheckusescoercedvaluerepro-0001-select-v-length-v-c"},
				},
			},
		},
	})
}

// TestDomainTypmodTableCheckUsesCoercedValueRepro reproduces a data consistency
// bug: PostgreSQL evaluates table CHECK constraints over domain-typed columns
// after applying the domain base-type typmod.
func TestDomainTypmodTableCheckUsesCoercedValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "domain typmod table CHECK uses coerced value",
			SetUpScript: []string{
				`CREATE DOMAIN num52_table_check_domain AS numeric(5,2);`,
				`CREATE TABLE domain_typmod_table_check_items (
					id INT PRIMARY KEY,
					amount num52_table_check_domain CHECK (amount = 123.456)
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO domain_typmod_table_check_items VALUES (1, 123.456);`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testdomaintypmodtablecheckusescoercedvaluerepro-0001-insert-into-domain_typmod_table_check_items-values-1", Compare: "sqlstate"},
				},
				{
					Query: `SELECT count(*) FROM domain_typmod_table_check_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testdomaintypmodtablecheckusescoercedvaluerepro-0002-select-count-*-from-domain_typmod_table_check_items"},
				},
			},
		},
	})
}

// TestTextDomainTypmodTableCheckUsesCoercedValueRepro reproduces a correctness
// bug: PostgreSQL evaluates table CHECK constraints over text-domain columns
// after applying the domain base-type typmod.
func TestTextDomainTypmodTableCheckUsesCoercedValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "text domain table CHECK uses coerced value",
			SetUpScript: []string{
				`CREATE DOMAIN varchar3_table_check_domain AS varchar(3);`,
				`CREATE DOMAIN char3_table_check_domain AS character(3);`,
				`CREATE TABLE text_domain_table_check_items (
					id INT PRIMARY KEY,
					v varchar3_table_check_domain CHECK (v = 'abc'),
					c char3_table_check_domain CHECK (c = 'ab '::CHARACTER(3))
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO text_domain_table_check_items VALUES (1, 'abc   ', 'ab');`,
				},
				{
					Query: `SELECT v, length(v), c = 'ab '::CHARACTER(3), octet_length(c)
						FROM text_domain_table_check_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testtextdomaintypmodtablecheckusescoercedvaluerepro-0001-select-v-length-v-c"},
				},
			},
		},
	})
}

// TestDomainTypmodCopyFromUsesCoercedValueRepro reproduces a data consistency
// bug: PostgreSQL applies domain base-type typmods to values loaded through
// COPY FROM STDIN.
func TestDomainTypmodCopyFromUsesCoercedValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "domain typmod COPY FROM uses coerced value",
			SetUpScript: []string{
				`CREATE DOMAIN num52_copy_domain AS numeric(5,2);`,
				`CREATE TABLE domain_typmod_copy_items (
					id INT PRIMARY KEY,
					amount num52_copy_domain
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:             `COPY domain_typmod_copy_items (id, amount) FROM STDIN;`,
					CopyFromStdInFile: "domain-typmod-copy.tsv",
				},
				{
					Query: `SELECT amount::text
						FROM domain_typmod_copy_items
						ORDER BY id;`,
					Expected: []sql.Row{{"123.46"}},
				},
			},
		},
	})
}

// TestDomainTypmodUpdateUsesCoercedValueRepro reproduces a data consistency
// bug: PostgreSQL applies domain base-type typmods when UPDATE assigns a new
// value to a domain-typed column.
func TestDomainTypmodUpdateUsesCoercedValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "domain typmod UPDATE uses coerced value",
			SetUpScript: []string{
				`CREATE DOMAIN num52_update_domain AS numeric(5,2);`,
				`CREATE TABLE domain_typmod_update_items (
					id INT PRIMARY KEY,
					amount num52_update_domain
				);`,
				`INSERT INTO domain_typmod_update_items VALUES (1, 1.23);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE domain_typmod_update_items
						SET amount = 123.456
						WHERE id = 1;`,
				},
				{
					Query: `SELECT amount::text
						FROM domain_typmod_update_items
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testdomaintypmodupdateusescoercedvaluerepro-0001-select-amount::text-from-domain_typmod_update_items-order"},
				},
			},
		},
	})
}

// TestDomainTypmodOnConflictUpdateUsesCoercedValueRepro reproduces a data
// consistency bug: PostgreSQL applies domain base-type typmods on the
// ON CONFLICT DO UPDATE assignment path.
func TestDomainTypmodOnConflictUpdateUsesCoercedValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "domain typmod ON CONFLICT UPDATE uses coerced value",
			SetUpScript: []string{
				`CREATE DOMAIN num52_upsert_domain AS numeric(5,2);`,
				`CREATE TABLE domain_typmod_upsert_items (
					id INT PRIMARY KEY,
					amount num52_upsert_domain
				);`,
				`INSERT INTO domain_typmod_upsert_items VALUES (1, 1.23);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO domain_typmod_upsert_items VALUES (1, 123.456)
						ON CONFLICT (id) DO UPDATE SET amount = EXCLUDED.amount;`,
				},
				{
					Query: `SELECT amount::text
						FROM domain_typmod_upsert_items
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testdomaintypmodonconflictupdateusescoercedvaluerepro-0001-select-amount::text-from-domain_typmod_upsert_items-order"},
				},
			},
		},
	})
}

// TestDomainTypmodInsertSelectUsesCoercedValueRepro reproduces a data
// consistency bug: PostgreSQL applies domain base-type typmods when
// INSERT ... SELECT writes into a domain-typed column.
func TestDomainTypmodInsertSelectUsesCoercedValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "domain typmod INSERT SELECT uses coerced value",
			SetUpScript: []string{
				`CREATE DOMAIN num52_insert_select_domain AS numeric(5,2);`,
				`CREATE TABLE domain_typmod_insert_select_source (
					id INT PRIMARY KEY,
					amount NUMERIC
				);`,
				`CREATE TABLE domain_typmod_insert_select_items (
					id INT PRIMARY KEY,
					amount num52_insert_select_domain
				);`,
				`INSERT INTO domain_typmod_insert_select_source VALUES (1, 123.456);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO domain_typmod_insert_select_items
						SELECT id, amount FROM domain_typmod_insert_select_source;`,
				},
				{
					Query: `SELECT amount::text
						FROM domain_typmod_insert_select_items
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testdomaintypmodinsertselectusescoercedvaluerepro-0001-select-amount::text-from-domain_typmod_insert_select_items-order"},
				},
			},
		},
	})
}

// TestDomainTypmodUpdateFromUsesCoercedValueRepro reproduces a data consistency
// bug: PostgreSQL applies domain base-type typmods when UPDATE ... FROM assigns
// a joined source value to a domain-typed column.
func TestDomainTypmodUpdateFromUsesCoercedValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "domain typmod UPDATE FROM uses coerced value",
			SetUpScript: []string{
				`CREATE DOMAIN num52_update_from_domain AS numeric(5,2);`,
				`CREATE TABLE domain_typmod_update_from_source (
					id INT PRIMARY KEY,
					new_amount NUMERIC
				);`,
				`CREATE TABLE domain_typmod_update_from_items (
					id INT PRIMARY KEY,
					amount num52_update_from_domain
				);`,
				`INSERT INTO domain_typmod_update_from_items VALUES (1, 1.23);`,
				`INSERT INTO domain_typmod_update_from_source VALUES (1, 123.456);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE domain_typmod_update_from_items AS t
						SET amount = s.new_amount
						FROM domain_typmod_update_from_source AS s
						WHERE t.id = s.id;`,
				},
				{
					Query: `SELECT amount::text
						FROM domain_typmod_update_from_items
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testdomaintypmodupdatefromusescoercedvaluerepro-0001-select-amount::text-from-domain_typmod_update_from_items-order"},
				},
			},
		},
	})
}

// TestDomainTypmodBindVarsUseCoercedValueRepro reproduces data consistency bugs:
// PostgreSQL applies domain base-type typmods to values assigned through
// extended-protocol bind variables.
func TestDomainTypmodBindVarsUseCoercedValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "domain typmod bind variable assignments use coerced values",
			SetUpScript: []string{
				`SET TIME ZONE 'UTC';`,
				`CREATE DOMAIN num52_bind_domain AS numeric(5,2);`,
				`CREATE DOMAIN ts0_bind_domain AS timestamp(0);`,
				`CREATE TABLE domain_bind_items (
					id INT PRIMARY KEY,
					amount num52_bind_domain,
					ts ts0_bind_domain
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `INSERT INTO domain_bind_items VALUES ($1, $2, $3);`,
					BindVars: []any{1, "123.456", "2021-09-15 21:43:56.789"},
				},
				{
					Query: `SELECT amount::text, ts::text
						FROM domain_bind_items;`,
					Expected: []sql.Row{{"123.46", "2021-09-15 21:43:57"}},
				},
			},
		},
	})
}

// TestTextDomainTypmodDmlUsesCoercedValuesRepro reproduces data consistency
// bugs: PostgreSQL applies text-domain base-type typmods on UPDATE,
// INSERT ... SELECT, and ON CONFLICT DO UPDATE assignment paths.
func TestTextDomainTypmodDmlUsesCoercedValuesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "text domain UPDATE uses coerced values",
			SetUpScript: []string{
				`CREATE DOMAIN varchar3_update_domain AS varchar(3);`,
				`CREATE DOMAIN char3_update_domain AS character(3);`,
				`CREATE TABLE text_domain_update_items (
					id INT PRIMARY KEY,
					v varchar3_update_domain,
					c char3_update_domain
				);`,
				`INSERT INTO text_domain_update_items VALUES (1, 'abc', 'abc');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE text_domain_update_items
						SET v = 'abc   ', c = 'ab'
						WHERE id = 1;`,
				},
				{
					Query: `SELECT v, length(v), c = 'ab '::CHARACTER(3), octet_length(c)
						FROM text_domain_update_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testtextdomaintypmoddmlusescoercedvaluesrepro-0001-select-v-length-v-c"},
				},
			},
		},
		{
			Name: "text domain INSERT SELECT uses coerced values",
			SetUpScript: []string{
				`CREATE DOMAIN varchar3_insert_select_domain AS varchar(3);`,
				`CREATE DOMAIN char3_insert_select_domain AS character(3);`,
				`CREATE TABLE text_domain_insert_select_source (
					id INT PRIMARY KEY,
					v TEXT,
					c TEXT
				);`,
				`CREATE TABLE text_domain_insert_select_items (
					id INT PRIMARY KEY,
					v varchar3_insert_select_domain,
					c char3_insert_select_domain
				);`,
				`INSERT INTO text_domain_insert_select_source VALUES (1, 'abc   ', 'ab');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO text_domain_insert_select_items
						SELECT id, v, c FROM text_domain_insert_select_source;`,
				},
				{
					Query: `SELECT v, length(v), c = 'ab '::CHARACTER(3), octet_length(c)
						FROM text_domain_insert_select_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testtextdomaintypmoddmlusescoercedvaluesrepro-0002-select-v-length-v-c"},
				},
			},
		},
		{
			Name: "text domain ON CONFLICT UPDATE uses coerced values",
			SetUpScript: []string{
				`CREATE DOMAIN varchar3_upsert_domain AS varchar(3);`,
				`CREATE DOMAIN char3_upsert_domain AS character(3);`,
				`CREATE TABLE text_domain_upsert_items (
					id INT PRIMARY KEY,
					v varchar3_upsert_domain,
					c char3_upsert_domain
				);`,
				`INSERT INTO text_domain_upsert_items VALUES (1, 'abc', 'abc');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO text_domain_upsert_items VALUES (1, 'abc   ', 'ab')
						ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v, c = EXCLUDED.c;`,
				},
				{
					Query: `SELECT v, length(v), c = 'ab '::CHARACTER(3), octet_length(c)
						FROM text_domain_upsert_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testtextdomaintypmoddmlusescoercedvaluesrepro-0003-select-v-length-v-c"},
				},
			},
		},
	})
}

// TestTextDomainTypmodBindVarsUseCoercedValuesRepro reproduces data
// consistency bugs: PostgreSQL applies text-domain base-type typmods to values
// assigned through extended-protocol bind variables.
func TestTextDomainTypmodBindVarsUseCoercedValuesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "text domain bind variable assignments use coerced values",
			SetUpScript: []string{
				`CREATE DOMAIN varchar3_bind_domain AS varchar(3);`,
				`CREATE DOMAIN char3_bind_domain AS character(3);`,
				`CREATE TABLE text_domain_bind_items (
					id INT PRIMARY KEY,
					v varchar3_bind_domain,
					c char3_bind_domain
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `INSERT INTO text_domain_bind_items VALUES ($1, $2, $3);`,
					BindVars: []any{1, "abc   ", "ab"},
				},
				{
					Query: `SELECT v, length(v), c = 'ab '::CHARACTER(3), octet_length(c), pg_typeof(v)::text, pg_typeof(c)::text
						FROM text_domain_bind_items;`,
					Expected: []sql.Row{{"abc", 3, true, 3, "varchar3_bind_domain", "char3_bind_domain"}},
				},
			},
		},
	})
}

// TestTextDomainTypmodBulkWritesUseCoercedValuesRepro reproduces data
// consistency bugs: PostgreSQL applies text-domain base-type typmods on
// UPDATE ... FROM and COPY FROM assignment paths.
func TestTextDomainTypmodBulkWritesUseCoercedValuesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "text domain UPDATE FROM uses coerced values",
			SetUpScript: []string{
				`CREATE DOMAIN varchar3_update_from_domain AS varchar(3);`,
				`CREATE DOMAIN char3_update_from_domain AS character(3);`,
				`CREATE TABLE text_domain_update_from_source (
					id INT PRIMARY KEY,
					new_v TEXT,
					new_c TEXT
				);`,
				`CREATE TABLE text_domain_update_from_items (
					id INT PRIMARY KEY,
					v varchar3_update_from_domain,
					c char3_update_from_domain
				);`,
				`INSERT INTO text_domain_update_from_items VALUES (1, 'abc', 'abc');`,
				`INSERT INTO text_domain_update_from_source VALUES (1, 'abc   ', 'ab');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE text_domain_update_from_items AS t
						SET v = s.new_v, c = s.new_c
						FROM text_domain_update_from_source AS s
						WHERE t.id = s.id;`,
				},
				{
					Query: `SELECT v, length(v), c = 'ab '::CHARACTER(3), octet_length(c)
						FROM text_domain_update_from_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testtextdomaintypmodbulkwritesusecoercedvaluesrepro-0001-select-v-length-v-c"},
				},
			},
		},
		{
			Name: "text domain COPY FROM uses coerced values",
			SetUpScript: []string{
				`CREATE DOMAIN varchar3_copy_domain AS varchar(3);`,
				`CREATE DOMAIN char3_copy_domain AS character(3);`,
				`CREATE TABLE text_domain_copy_items (
					id INT PRIMARY KEY,
					v varchar3_copy_domain,
					c char3_copy_domain
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:             `COPY text_domain_copy_items (id, v, c) FROM STDIN;`,
					CopyFromStdInFile: "text-domain-typmod-copy.tsv",
				},
				{
					Query: `SELECT v, length(v), c = 'ab '::CHARACTER(3), octet_length(c)
						FROM text_domain_copy_items;`,
					Expected: []sql.Row{{"abc", 3, true, 3}},
				},
			},
		},
	})
}

// TestDomainCheckRejectsNonScalarExpressionsRepro reproduces domain
// correctness bugs if Doltgres accepts CHECK expressions PostgreSQL rejects for
// domains.
func TestDomainCheckRejectsNonScalarExpressionsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "domain CHECK rejects subquery expressions",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE DOMAIN domain_check_subquery AS integer
						CHECK (VALUE > (SELECT 0));`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testdomaincheckrejectsnonscalarexpressionsrepro-0001-create-domain-domain_check_subquery-as-integer", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "domain CHECK rejects aggregate expressions",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE DOMAIN domain_check_aggregate AS integer
						CHECK (avg(VALUE) > 0);`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testdomaincheckrejectsnonscalarexpressionsrepro-0002-create-domain-domain_check_aggregate-as-integer", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "domain CHECK rejects window expressions",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE DOMAIN domain_check_window AS integer
						CHECK (row_number() OVER () > 0);`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testdomaincheckrejectsnonscalarexpressionsrepro-0003-create-domain-domain_check_window-as-integer", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "domain CHECK rejects set-returning expressions",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE DOMAIN domain_check_srf AS integer
						CHECK (generate_series(1, 2) > 0);`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testdomaincheckrejectsnonscalarexpressionsrepro-0004-create-domain-domain_check_srf-as-integer",

						// TestDomainCheckAllowsUserDefinedFunctionRepro guards PostgreSQL parity:
						// domain CHECK constraints may call user-defined validation functions and
						// enforce their result.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestDomainCheckAllowsUserDefinedFunctionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "domain CHECK allows user-defined function",
			SetUpScript: []string{
				`CREATE FUNCTION dg_domain_fn_is_valid(input INT) RETURNS BOOL AS $$
					SELECT input > 0
				$$ LANGUAGE sql;`,
				`CREATE DOMAIN dg_domain_fn_type AS INT
					CHECK (dg_domain_fn_is_valid(VALUE));`,
				`CREATE TABLE dg_domain_fn_items (
					id INT PRIMARY KEY,
					value dg_domain_fn_type
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO dg_domain_fn_items VALUES (1, -5);`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testdomaincheckallowsuserdefinedfunctionrepro-0001-insert-into-dg_domain_fn_items-values-1", Compare: "sqlstate"},
				},
				{
					Query: `SELECT count(*) FROM dg_domain_fn_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testdomaincheckallowsuserdefinedfunctionrepro-0002-select-count-*-from-dg_domain_fn_items"},
				},
			},
		},
	})
}

// TestSchemaQualifiedDomainCheckFunctionUsesExplicitSchemaRepro reproduces a
// data integrity bug: a domain CHECK that explicitly calls a schema-qualified
// validation function should use that schema, not a same-name function from the
// current search path.
func TestSchemaQualifiedDomainCheckFunctionUsesExplicitSchemaRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "schema-qualified domain CHECK function uses explicit schema",
			SetUpScript: []string{
				`CREATE SCHEMA dg_domain_lookup_a;`,
				`CREATE SCHEMA dg_domain_lookup_b;`,
				`CREATE FUNCTION dg_domain_lookup_a.is_valid(input INT) RETURNS BOOL AS $$
					SELECT true
				$$ LANGUAGE sql;`,
				`CREATE FUNCTION dg_domain_lookup_b.is_valid(input INT) RETURNS BOOL AS $$
					SELECT false
				$$ LANGUAGE sql;`,
				`SET search_path = dg_domain_lookup_a, public;`,
				`CREATE DOMAIN dg_domain_lookup_type AS INT
					CHECK (dg_domain_lookup_b.is_valid(VALUE));`,
				`CREATE TABLE dg_domain_lookup_items (
					id INT PRIMARY KEY,
					value dg_domain_lookup_type
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO dg_domain_lookup_items VALUES (1, 5);`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testschemaqualifieddomaincheckfunctionusesexplicitschemarepro-0001-insert-into-dg_domain_lookup_items-values-1",

						// TestDomainDefaultFunctionEvaluatesOnInsertRepro reproduces a correctness bug:
						// PostgreSQL evaluates a domain default that calls a user-defined function when
						// inserting into a domain-typed column.
						Compare: "sqlstate"},
				},
				{
					Query: `SELECT count(*) FROM dg_domain_lookup_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testschemaqualifieddomaincheckfunctionusesexplicitschemarepro-0002-select-count-*-from-dg_domain_lookup_items"},
				},
			},
		},
	})
}

func TestDomainDefaultFunctionEvaluatesOnInsertRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "domain default function evaluates on insert",
			SetUpScript: []string{
				`CREATE FUNCTION dg_domain_default_value() RETURNS INT AS $$
					SELECT 2
				$$ LANGUAGE sql;`,
				`CREATE DOMAIN dg_domain_default_type AS INT
					DEFAULT (dg_domain_default_value());`,
				`CREATE TABLE dg_domain_default_items (
					id INT PRIMARY KEY,
					value dg_domain_default_type
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO dg_domain_default_items (id) VALUES (1) RETURNING value;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testdomaindefaultfunctionevaluatesoninsertrepro-0001-insert-into-dg_domain_default_items-id-values"},
				},
			},
		},
	})
}

// TestArrayDomainAcceptsValidValuesRepro reproduces a correctness bug:
// domains over array types reject or panic on valid array values instead of
// storing and returning them normally. The invalid-value assertions remain as
// useful guards for the constraint path.
func TestArrayDomainAcceptsValidValuesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "array domain enforces constraints",
			SetUpScript: []string{
				`CREATE DOMAIN int_pair_domain AS integer[]
					CONSTRAINT int_pair_domain_check CHECK (array_length(VALUE, 1) = 2);`,
				`CREATE TABLE array_domain_items (
					id INT PRIMARY KEY,
					pair int_pair_domain
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT ARRAY[1, 2]::int_pair_domain;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testarraydomainacceptsvalidvaluesrepro-0001-select-array[1-2]::int_pair_domain"},
				},
				{
					Query: `SELECT ARRAY[1, 2, 3]::int_pair_domain;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testarraydomainacceptsvalidvaluesrepro-0002-select-array[1-2-3]::int_pair_domain", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO array_domain_items VALUES (1, ARRAY[1, 2]);`,
				},
				{
					Query: `INSERT INTO array_domain_items VALUES (2, ARRAY[1, 2, 3]);`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testarraydomainacceptsvalidvaluesrepro-0003-insert-into-array_domain_items-values-2", Compare: "sqlstate"},
				},
				{
					Query: `SELECT id, pair FROM array_domain_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testarraydomainacceptsvalidvaluesrepro-0004-select-id-pair-from-array_domain_items"},
				},
			},
		},
	})
}

// TestArrayOverDomainEnforcesElementConstraintsRepro reproduces a data
// consistency bug: PostgreSQL arrays whose element type is a domain validate
// each element against the domain constraints.
func TestArrayOverDomainEnforcesElementConstraintsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "array over domain enforces element constraints",
			SetUpScript: []string{
				`CREATE DOMAIN positive_array_element_domain AS integer
					CONSTRAINT positive_array_element_domain_check CHECK (VALUE > 0);`,
				`CREATE TABLE array_over_domain_items (
					id INT PRIMARY KEY,
					values_int positive_array_element_domain[]
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO array_over_domain_items VALUES
						(1, ARRAY[1, 2]::positive_array_element_domain[]);`,
				},
				{
					Query: `INSERT INTO array_over_domain_items VALUES
						(2, ARRAY[1, -2]::positive_array_element_domain[]);`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testarrayoverdomainenforceselementconstraintsrepro-0001-insert-into-array_over_domain_items-values-2", Compare: "sqlstate"},
				},
				{
					Query: `SELECT id, values_int FROM array_over_domain_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testarrayoverdomainenforceselementconstraintsrepro-0002-select-id-values_int-from-array_over_domain_items"},
				},
			},
		},
	})
}

// TestEnumDomainEnforcesConstraintsRepro reproduces a domain correctness bug:
// PostgreSQL supports domains over enum types and evaluates domain checks
// against enum values on assignment.
func TestEnumDomainEnforcesConstraintsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "enum domain enforces constraints",
			SetUpScript: []string{
				`CREATE TYPE enum_domain_mood AS ENUM ('sad', 'ok', 'happy');`,
				`CREATE DOMAIN enum_domain_happyish AS enum_domain_mood
					CONSTRAINT enum_domain_happyish_check CHECK (VALUE <> 'sad');`,
				`CREATE TABLE enum_domain_items (
					id INT PRIMARY KEY,
					mood enum_domain_happyish
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO enum_domain_items VALUES (1, 'ok');`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testenumdomainenforcesconstraintsrepro-0001-insert-into-enum_domain_items-values-1", Compare: "tag"},
				},
				{
					Query: `INSERT INTO enum_domain_items VALUES (2, 'sad');`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testenumdomainenforcesconstraintsrepro-0002-insert-into-enum_domain_items-values-2", Compare: "sqlstate"},
				},
				{
					Query: `SELECT id, mood::text
						FROM enum_domain_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testenumdomainenforcesconstraintsrepro-0003-select-id-mood::text-from-enum_domain_items"},
				},
			},
		},
	})
}

// TestCompositeTypeDomainFieldEnforcesConstraintsRepro reproduces a domain
// correctness bug: PostgreSQL composite types can contain domain-typed fields,
// accept valid composite rows, and reject invalid field values through the
// domain constraint.
func TestCompositeTypeDomainFieldEnforcesConstraintsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "composite type domain field enforces constraints",
			SetUpScript: []string{
				`CREATE DOMAIN composite_field_positive AS INT
					CONSTRAINT composite_field_positive_check CHECK (VALUE > 0);`,
				`CREATE TYPE composite_field_row AS (amount composite_field_positive);`,
				`CREATE TABLE composite_field_items (item composite_field_row);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO composite_field_items VALUES (ROW(1));`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testcompositetypedomainfieldenforcesconstraintsrepro-0001-insert-into-composite_field_items-values-row", Compare: "tag"},
				},
				{
					Query: `INSERT INTO composite_field_items VALUES (ROW(-1));`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testcompositetypedomainfieldenforcesconstraintsrepro-0002-insert-into-composite_field_items-values-row", Compare: "sqlstate"},
				},
				{
					Query: `SELECT item::text FROM composite_field_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testcompositetypedomainfieldenforcesconstraintsrepro-0003-select-item::text-from-composite_field_items"},
				},
			},
		},
	})
}

// TestCompositeDomainAcceptsValidValuesRepro reproduces a domain correctness
// bug: PostgreSQL supports domains over composite types and evaluates domain
// CHECK constraints against composite fields.
func TestCompositeDomainAcceptsValidValuesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "composite domain enforces field constraints",
			SetUpScript: []string{
				`CREATE TYPE composite_domain_unordered_pair AS (
					x INT,
					y INT
				);`,
				`CREATE DOMAIN composite_domain_ordered_pair AS composite_domain_unordered_pair
					CONSTRAINT composite_domain_ordered_pair_check CHECK ((VALUE).x <= (VALUE).y);`,
				`CREATE TABLE composite_domain_items (
					id INT PRIMARY KEY,
					pair composite_domain_ordered_pair
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO composite_domain_items VALUES
						(1, ROW(1, 2)::composite_domain_ordered_pair);`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testcompositedomainacceptsvalidvaluesrepro-0001-insert-into-composite_domain_items-values-1", Compare: "tag"},
				},
				{
					Query: `INSERT INTO composite_domain_items VALUES
						(2, ROW(3, 2)::composite_domain_ordered_pair);`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testcompositedomainacceptsvalidvaluesrepro-0002-insert-into-composite_domain_items-values-2", Compare: "sqlstate"},
				},
				{
					Query: `SELECT id, pair::text FROM composite_domain_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testcompositedomainacceptsvalidvaluesrepro-0003-select-id-pair::text-from-composite_domain_items"},
				},
			},
		},
	})
}

// TestSqlFunctionReturnEnforcesDomainConstraintsRepro reproduces a data
// consistency bug: SQL functions returning a domain do not validate returned
// values against the domain constraints at execution time.
func TestSqlFunctionReturnEnforcesDomainConstraintsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "SQL function return enforces domain constraints",
			SetUpScript: []string{
				`CREATE DOMAIN function_return_positive_domain AS integer
					CONSTRAINT function_return_positive_domain_check CHECK (VALUE > 0);`,
				`CREATE FUNCTION function_return_valid_domain()
					RETURNS function_return_positive_domain
					LANGUAGE SQL AS $$ SELECT 7 $$;`,
				`CREATE FUNCTION function_return_invalid_domain()
					RETURNS function_return_positive_domain
					LANGUAGE SQL AS $$ SELECT -1 $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT function_return_valid_domain();`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testsqlfunctionreturnenforcesdomainconstraintsrepro-0001-select-function_return_valid_domain"},
				},
				{
					Query: `SELECT function_return_invalid_domain();`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testsqlfunctionreturnenforcesdomainconstraintsrepro-0002-select-function_return_invalid_domain",

						// TestPlpgsqlFunctionReturnDomainValueRepro reproduces a correctness bug:
						// PL/pgSQL functions returning a domain reject a valid base-type return value
						// instead of coercing it to the domain.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestPlpgsqlFunctionReturnDomainValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "PL/pgSQL function returns valid domain value",
			SetUpScript: []string{
				`CREATE DOMAIN plpgsql_return_positive_domain AS integer
					CONSTRAINT plpgsql_return_positive_domain_check CHECK (VALUE > 0);`,
				`CREATE FUNCTION plpgsql_return_valid_domain()
					RETURNS plpgsql_return_positive_domain AS $$
				BEGIN
					RETURN 7;
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT plpgsql_return_valid_domain();`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testplpgsqlfunctionreturndomainvaluerepro-0001-select-plpgsql_return_valid_domain"},
				},
			},
		},
	})
}

// TestSqlFunctionArgumentResolvesDomainInputRepro reproduces a correctness bug:
// SQL functions declared with a domain-typed argument are not resolved when the
// caller passes a value of the domain's base type.
func TestSqlFunctionArgumentResolvesDomainInputRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "SQL function argument resolves domain input",
			SetUpScript: []string{
				`CREATE DOMAIN function_arg_positive_domain AS integer
					CONSTRAINT function_arg_positive_domain_check CHECK (VALUE > 0);`,
				`CREATE FUNCTION function_arg_identity(input_value function_arg_positive_domain)
					RETURNS function_arg_positive_domain
					LANGUAGE SQL AS $$ SELECT input_value $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT function_arg_identity(7);`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testsqlfunctionargumentresolvesdomaininputrepro-0001-select-function_arg_identity-7"},
				},
			},
		},
	})
}

// TestDomainDefaultAppliesToColumnRepro guards PostgreSQL domain defaults:
// domain-typed columns use the domain default when they do not declare their
// own column default.
func TestDomainDefaultAppliesToColumnRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "domain default applies to domain typed column",
			SetUpScript: []string{
				`CREATE DOMAIN defaulted_domain AS integer
					DEFAULT 7
					CONSTRAINT defaulted_domain_check CHECK (VALUE > 0);`,
				`CREATE TABLE defaulted_domain_items (
					id INT PRIMARY KEY,
					amount defaulted_domain
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO defaulted_domain_items (id) VALUES (1);`,
				},
				{
					Query: `SELECT id, amount FROM defaulted_domain_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testdomaindefaultappliestocolumnrepro-0001-select-id-amount-from-defaulted_domain_items"},
				},
			},
		},
	})
}

// TestUpdateSetDefaultUsesDomainDefaultRepro reproduces a correctness bug:
// UPDATE ... SET DEFAULT on a domain-typed column rejects the DEFAULT
// expression instead of applying the domain default.
func TestUpdateSetDefaultUsesDomainDefaultRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "UPDATE SET DEFAULT uses domain default",
			SetUpScript: []string{
				`CREATE DOMAIN update_defaulted_domain AS integer
					DEFAULT 7
					CONSTRAINT update_defaulted_domain_check CHECK (VALUE > 0);`,
				`CREATE TABLE update_defaulted_domain_items (
					id INT PRIMARY KEY,
					amount update_defaulted_domain
				);`,
				`INSERT INTO update_defaulted_domain_items VALUES (1, 3);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE update_defaulted_domain_items
						SET amount = DEFAULT
						WHERE id = 1;`,
				},
				{
					Query: `SELECT id, amount FROM update_defaulted_domain_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testupdatesetdefaultusesdomaindefaultrepro-0001-select-id-amount-from-update_defaulted_domain_items"},
				},
			},
		},
	})
}

// TestAlterDomainSetDefaultAppliesToColumnsRepro reproduces a persistence bug:
// ALTER DOMAIN ... SET DEFAULT changes the domain default used by future
// inserts into domain-typed columns that do not declare their own default.
func TestAlterDomainSetDefaultAppliesToColumnsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER DOMAIN SET DEFAULT applies to domain typed column",
			SetUpScript: []string{
				`CREATE DOMAIN alter_defaulted_domain AS integer
					CONSTRAINT alter_defaulted_domain_check CHECK (VALUE > 0);`,
				`CREATE TABLE alter_defaulted_domain_items (
					id INT PRIMARY KEY,
					amount alter_defaulted_domain
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER DOMAIN alter_defaulted_domain SET DEFAULT 11;`,
				},
				{
					Query: `INSERT INTO alter_defaulted_domain_items (id) VALUES (1);`,
				},
				{
					Query: `SELECT id, amount FROM alter_defaulted_domain_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testalterdomainsetdefaultappliestocolumnsrepro-0001-select-id-amount-from-alter_defaulted_domain_items"},
				},
			},
		},
	})
}

// TestDomainTypedColumnAcceptsValidColumnDefaultRepro reproduces a persistence
// bug: a valid column default declared on a domain-typed column is ignored when
// the column is omitted from INSERT.
func TestDomainTypedColumnAcceptsValidColumnDefaultRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "domain typed column accepts valid column default",
			SetUpScript: []string{
				`CREATE DOMAIN column_default_positive_domain AS integer
					CONSTRAINT column_default_positive_domain_check CHECK (VALUE > 0);`,
				`CREATE TABLE column_default_domain_items (
					id INT PRIMARY KEY,
					amount column_default_positive_domain DEFAULT 5
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO column_default_domain_items (id) VALUES (1);`,
				},
				{
					Query: `SELECT id, amount FROM column_default_domain_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testdomaintypedcolumnacceptsvalidcolumndefaultrepro-0001-select-id-amount-from-column_default_domain_items"},
				},
			},
		},
	})
}

// TestInsertSelectEnforcesDomainConstraintsGuard guards that INSERT ... SELECT
// enforces constraints declared on a domain-typed target column and leaves no
// partial rows behind.
func TestInsertSelectEnforcesDomainConstraintsGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "INSERT SELECT enforces domain constraints",
			SetUpScript: []string{
				`CREATE DOMAIN insert_select_positive_domain AS integer
					CONSTRAINT insert_select_positive_domain_check CHECK (VALUE > 0);`,
				`CREATE TABLE insert_select_domain_items (
					id INT PRIMARY KEY,
					amount insert_select_positive_domain
				);`,
				`CREATE TABLE insert_select_domain_source (
					id INT PRIMARY KEY,
					amount INT
				);`,
				`INSERT INTO insert_select_domain_source VALUES (1, 10), (2, -1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO insert_select_domain_items
						SELECT id, amount FROM insert_select_domain_source ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testinsertselectenforcesdomainconstraintsguard-0001-insert-into-insert_select_domain_items-select-id", Compare: "sqlstate"},
				},
				{
					Query: `SELECT count(*) FROM insert_select_domain_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testinsertselectenforcesdomainconstraintsguard-0002-select-count-*-from-insert_select_domain_items"},
				},
			},
		},
	})
}

// TestUpdateAliasEnforcesDomainConstraintsRepro reproduces a correctness bug:
// updating a domain-typed column through a table alias panics while analyzing
// the domain check instead of enforcing the constraint cleanly.
func TestUpdateAliasEnforcesDomainConstraintsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "UPDATE alias enforces domain constraints",
			SetUpScript: []string{
				`CREATE DOMAIN update_alias_positive_domain AS integer
					CONSTRAINT update_alias_positive_domain_check CHECK (VALUE > 0);`,
				`CREATE TABLE update_alias_domain_items (
					id INT PRIMARY KEY,
					amount update_alias_positive_domain
				);`,
				`INSERT INTO update_alias_domain_items VALUES (1, 1), (2, 2);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE update_alias_domain_items AS t
						SET amount = -1
						WHERE id = 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testupdatealiasenforcesdomainconstraintsrepro-0001-update-update_alias_domain_items-as-t-set", Compare: "sqlstate"},
				},
				{
					Query: `SELECT id, amount
						FROM update_alias_domain_items
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testupdatealiasenforcesdomainconstraintsrepro-0002-select-id-amount-from-update_alias_domain_items"},
				},
			},
		},
	})
}

// TestUpdateAliasDomainColumnValidAssignmentRepro reproduces a correctness
// bug: updating a domain-typed column through a table alias panics during
// domain check analysis even when the new value satisfies the domain.
func TestUpdateAliasDomainColumnValidAssignmentRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "UPDATE alias accepts valid domain assignment",
			SetUpScript: []string{
				`CREATE DOMAIN update_alias_valid_domain AS integer
					CONSTRAINT update_alias_valid_domain_check CHECK (VALUE > 0);`,
				`CREATE TABLE update_alias_valid_domain_items (
					id INT PRIMARY KEY,
					amount update_alias_valid_domain
				);`,
				`INSERT INTO update_alias_valid_domain_items VALUES (1, 1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE update_alias_valid_domain_items AS t
						SET amount = 10
						WHERE id = 1;`,
				},
				{
					Query: `SELECT id, amount FROM update_alias_valid_domain_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testupdatealiasdomaincolumnvalidassignmentrepro-0001-select-id-amount-from-update_alias_valid_domain_items"},
				},
			},
		},
	})
}

// TestUpdateFromEnforcesDomainConstraintsRepro reproduces a correctness bug:
// UPDATE ... FROM panics while analyzing constraints declared on a domain-typed
// target column instead of enforcing the domain check cleanly.
func TestUpdateFromEnforcesDomainConstraintsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "UPDATE FROM enforces domain constraints",
			SetUpScript: []string{
				`CREATE DOMAIN update_from_positive_domain AS integer
					CONSTRAINT update_from_positive_domain_check CHECK (VALUE > 0);`,
				`CREATE TABLE update_from_domain_items (
					id INT PRIMARY KEY,
					amount update_from_positive_domain
				);`,
				`CREATE TABLE update_from_domain_source (
					id INT PRIMARY KEY,
					new_amount INT
				);`,
				`INSERT INTO update_from_domain_items VALUES (1, 1), (2, 2);`,
				`INSERT INTO update_from_domain_source VALUES (1, 10), (2, -1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE update_from_domain_items AS t
						SET amount = s.new_amount
						FROM update_from_domain_source AS s
						WHERE t.id = s.id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testupdatefromenforcesdomainconstraintsrepro-0001-update-update_from_domain_items-as-t-set", Compare: "sqlstate"},
				},
				{
					Query: `SELECT id, amount
						FROM update_from_domain_items
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testupdatefromenforcesdomainconstraintsrepro-0002-select-id-amount-from-update_from_domain_items"},
				},
			},
		},
	})
}

// TestOnConflictUpdateEnforcesDomainConstraintsRepro reproduces a data
// consistency bug: ON CONFLICT DO UPDATE bypasses constraints declared on a
// domain-typed target column and persists invalid domain values.
func TestOnConflictUpdateEnforcesDomainConstraintsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ON CONFLICT UPDATE enforces domain constraints",
			SetUpScript: []string{
				`CREATE DOMAIN on_conflict_positive_domain AS integer
					CONSTRAINT on_conflict_positive_domain_check CHECK (VALUE > 0);`,
				`CREATE TABLE on_conflict_domain_items (
					id INT PRIMARY KEY,
					amount on_conflict_positive_domain
				);`,
				`INSERT INTO on_conflict_domain_items VALUES (1, 1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO on_conflict_domain_items VALUES (1, 2)
						ON CONFLICT (id) DO UPDATE SET amount = -1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testonconflictupdateenforcesdomainconstraintsrepro-0001-insert-into-on_conflict_domain_items-values-1", Compare: "sqlstate"},
				},
				{
					Query: `SELECT id, amount FROM on_conflict_domain_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testonconflictupdateenforcesdomainconstraintsrepro-0002-select-id-amount-from-on_conflict_domain_items"},
				},
			},
		},
	})
}

// TestOnConflictUpdateEnforcesDomainNotNullRepro reproduces a data consistency
// bug: ON CONFLICT DO UPDATE bypasses NOT NULL constraints declared by a
// domain-typed target column and persists NULL.
func TestOnConflictUpdateEnforcesDomainNotNullRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ON CONFLICT UPDATE enforces domain not null",
			SetUpScript: []string{
				`CREATE DOMAIN on_conflict_required_domain AS integer NOT NULL;`,
				`CREATE TABLE on_conflict_domain_not_null_items (
					id INT PRIMARY KEY,
					amount on_conflict_required_domain
				);`,
				`INSERT INTO on_conflict_domain_not_null_items VALUES (1, 1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO on_conflict_domain_not_null_items VALUES (1, 2)
						ON CONFLICT (id) DO UPDATE SET amount = NULL;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testonconflictupdateenforcesdomainnotnullrepro-0001-insert-into-on_conflict_domain_not_null_items-values-1", Compare: "sqlstate"},
				},
				{
					Query: `SELECT id, amount FROM on_conflict_domain_not_null_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testonconflictupdateenforcesdomainnotnullrepro-0002-select-id-amount-from-on_conflict_domain_not_null_items"},
				},
			},
		},
	})
}

// TestAlterTableAddDomainColumnValidatesDefaultGuard guards that adding a
// domain-typed column with a default validates the backfilled default value
// against the domain constraints.
func TestAlterTableAddDomainColumnValidatesDefaultGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TABLE ADD domain column validates default",
			SetUpScript: []string{
				`CREATE DOMAIN add_column_positive_domain AS integer
					CONSTRAINT add_column_positive_domain_check CHECK (VALUE > 0);`,
				`CREATE TABLE add_column_domain_items (
					id INT PRIMARY KEY
				);`,
				`INSERT INTO add_column_domain_items VALUES (1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE add_column_domain_items
						ADD COLUMN amount add_column_positive_domain DEFAULT -1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testaltertableadddomaincolumnvalidatesdefaultguard-0001-alter-table-add_column_domain_items-add-column",

						// TestAlterTableAddDomainColumnValidDefaultRepro reproduces a correctness bug:
						// adding a domain-typed column with a valid base-type default is rejected
						// instead of backfilling existing rows with that default.
						Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM add_column_domain_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testaltertableadddomaincolumnvalidatesdefaultguard-0002-select-*-from-add_column_domain_items"},
				},
			},
		},
	})
}

func TestAlterTableAddDomainColumnValidDefaultRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TABLE ADD domain column accepts valid default",
			SetUpScript: []string{
				`CREATE DOMAIN add_column_valid_default_domain AS integer
					CONSTRAINT add_column_valid_default_domain_check CHECK (VALUE > 0);`,
				`CREATE TABLE add_column_valid_default_items (
					id INT PRIMARY KEY
				);`,
				`INSERT INTO add_column_valid_default_items VALUES (1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE add_column_valid_default_items
						ADD COLUMN amount add_column_valid_default_domain DEFAULT 5;`,
				},
				{
					Query: `SELECT id, amount FROM add_column_valid_default_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testaltertableadddomaincolumnvaliddefaultrepro-0001-select-id-amount-from-add_column_valid_default_items"},
				},
			},
		},
	})
}

// TestAlterTableAddDomainNotNullColumnValidatesExistingRowsRepro reproduces a
// persistence bug: adding a NOT NULL domain-typed column to a non-empty table
// accepts the implicit NULL backfill and persists rows that violate the domain.
func TestAlterTableAddDomainNotNullColumnValidatesExistingRowsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TABLE ADD domain NOT NULL column validates existing rows",
			SetUpScript: []string{
				`CREATE DOMAIN add_column_required_domain AS integer NOT NULL;`,
				`CREATE TABLE add_required_domain_items (
					id INT PRIMARY KEY
				);`,
				`INSERT INTO add_required_domain_items VALUES (1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE add_required_domain_items
						ADD COLUMN amount add_column_required_domain;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testaltertableadddomainnotnullcolumnvalidatesexistingrowsrepro-0001-alter-table-add_required_domain_items-add-column",

						// TestAlterColumnTypeToDomainPreservesAndEnforcesDomainGuard guards that
						// changing a column's type to a domain preserves stored valid values and uses
						// the domain for later assignments.
						Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM add_required_domain_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testaltertableadddomainnotnullcolumnvalidatesexistingrowsrepro-0002-select-*-from-add_required_domain_items"},
				},
			},
		},
	})
}

func TestAlterColumnTypeToDomainPreservesAndEnforcesDomainGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER COLUMN TYPE to domain preserves and enforces domain",
			SetUpScript: []string{
				`CREATE DOMAIN alter_type_positive_domain AS integer
					CONSTRAINT alter_type_positive_domain_check CHECK (VALUE > 0);`,
				`CREATE TABLE alter_type_domain_items (
					id INT PRIMARY KEY,
					amount INT
				);`,
				`INSERT INTO alter_type_domain_items VALUES (1, 10);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE alter_type_domain_items
						ALTER COLUMN amount TYPE alter_type_positive_domain;`,
				},
				{
					Query: `INSERT INTO alter_type_domain_items VALUES (2, -1);`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testaltercolumntypetodomainpreservesandenforcesdomainguard-0001-insert-into-alter_type_domain_items-values-2", Compare: "sqlstate"},
				},
				{
					Query: `SELECT id, amount FROM alter_type_domain_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testaltercolumntypetodomainpreservesandenforcesdomainguard-0002-select-id-amount-from-alter_type_domain_items"},
				},
			},
		},
	})
}

// TestAlterColumnTypeToDomainValidatesExistingRowsRepro reproduces a data
// consistency bug: changing a column's type to a domain accepts existing
// values that violate the new domain invariant.
func TestAlterColumnTypeToDomainValidatesExistingRowsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER COLUMN TYPE to domain validates existing rows",
			SetUpScript: []string{
				`CREATE DOMAIN alter_type_existing_positive_domain AS integer
					CONSTRAINT alter_type_existing_positive_domain_check CHECK (VALUE > 0);`,
				`CREATE TABLE alter_type_existing_domain_items (
					id INT PRIMARY KEY,
					amount INT
				);`,
				`INSERT INTO alter_type_existing_domain_items VALUES (1, -1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE alter_type_existing_domain_items
						ALTER COLUMN amount TYPE alter_type_existing_positive_domain;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testaltercolumntypetodomainvalidatesexistingrowsrepro-0001-alter-table-alter_type_existing_domain_items-alter-column", Compare: "sqlstate"},
				},
				{
					Query: `SELECT id, amount FROM alter_type_existing_domain_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testaltercolumntypetodomainvalidatesexistingrowsrepro-0002-select-id-amount-from-alter_type_existing_domain_items"},
				},
			},
		},
	})
}

// TestAlterColumnTypeToNotNullDomainValidatesExistingRowsRepro reproduces a
// data consistency bug: changing a column's type to a NOT NULL domain accepts
// existing NULL values.
func TestAlterColumnTypeToNotNullDomainValidatesExistingRowsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER COLUMN TYPE to NOT NULL domain validates existing rows",
			SetUpScript: []string{
				`CREATE DOMAIN alter_type_required_domain AS integer NOT NULL;`,
				`CREATE TABLE alter_type_required_domain_items (
					id INT PRIMARY KEY,
					amount INT
				);`,
				`INSERT INTO alter_type_required_domain_items VALUES (1, NULL);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE alter_type_required_domain_items
						ALTER COLUMN amount TYPE alter_type_required_domain;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testaltercolumntypetonotnulldomainvalidatesexistingrowsrepro-0001-alter-table-alter_type_required_domain_items-alter-column",

						// TestNestedDomainEnforcesBaseDomainConstraintsRepro reproduces a domain
						// integrity bug: domains built on top of another domain should enforce both
						// the base domain's constraints and their own constraints.
						Compare: "sqlstate"},
				},
				{
					Query: `SELECT id, amount FROM alter_type_required_domain_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testaltercolumntypetonotnulldomainvalidatesexistingrowsrepro-0002-select-id-amount-from-alter_type_required_domain_items"},
				},
			},
		},
	})
}

func TestNestedDomainEnforcesBaseDomainConstraintsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "nested domains enforce base domain constraints",
			SetUpScript: []string{
				`CREATE DOMAIN nested_base_positive AS integer
					CONSTRAINT nested_base_positive_check CHECK (VALUE > 0);`,
				`CREATE DOMAIN nested_small_positive AS nested_base_positive
					CONSTRAINT nested_small_positive_check CHECK (VALUE < 10);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT 5::nested_small_positive;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testnesteddomainenforcesbasedomainconstraintsrepro-0001-select-5::nested_small_positive"},
				},
				{
					Query: `SELECT (-1)::nested_small_positive;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testnesteddomainenforcesbasedomainconstraintsrepro-0002-select-1-::nested_small_positive", Compare: "sqlstate"},
				},
				{
					Query: `SELECT 11::nested_small_positive;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testnesteddomainenforcesbasedomainconstraintsrepro-0003-select-11::nested_small_positive",

						// TestAlterDomainAddConstraintValidatesExistingRowsRepro reproduces a domain
						// integrity bug: PostgreSQL validates existing domain-typed columns before
						// accepting a new domain CHECK constraint.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestAlterDomainAddConstraintValidatesExistingRowsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER DOMAIN ADD CONSTRAINT validates existing rows",
			SetUpScript: []string{
				`CREATE DOMAIN alter_domain_positive AS integer;`,
				`CREATE TABLE alter_domain_items (
					id INT PRIMARY KEY,
					amount alter_domain_positive
				);`,
				`INSERT INTO alter_domain_items VALUES (1, -1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER DOMAIN alter_domain_positive
						ADD CONSTRAINT alter_domain_positive_check CHECK (VALUE > 0);`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testalterdomainaddconstraintvalidatesexistingrowsrepro-0001-alter-domain-alter_domain_positive-add-constraint",

						// TestAlterDomainSetNotNullValidatesExistingRowsRepro reproduces a domain
						// integrity bug: PostgreSQL validates existing domain-typed columns before
						// accepting ALTER DOMAIN ... SET NOT NULL.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestAlterDomainSetNotNullValidatesExistingRowsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER DOMAIN SET NOT NULL validates existing rows",
			SetUpScript: []string{
				`CREATE DOMAIN alter_domain_required AS integer;`,
				`CREATE TABLE alter_domain_required_items (
					id INT PRIMARY KEY,
					amount alter_domain_required
				);`,
				`INSERT INTO alter_domain_required_items VALUES (1, NULL);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER DOMAIN alter_domain_required SET NOT NULL;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testalterdomainsetnotnullvalidatesexistingrowsrepro-0001-alter-domain-alter_domain_required-set-not",

						// TestDropDomainUsedByViewRequiresCascadeRepro reproduces a dependency bug:
						// PostgreSQL rejects dropping a domain referenced by a view unless CASCADE is
						// requested.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestDropDomainUsedByViewRequiresCascadeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP DOMAIN rejects view expression dependencies",
			SetUpScript: []string{
				`CREATE DOMAIN domain_view_dependency_positive AS integer
					CONSTRAINT domain_view_dependency_positive_check CHECK (VALUE > 0);`,
				`CREATE VIEW domain_view_dependency_reader AS
					SELECT 1::domain_view_dependency_positive AS amount;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP DOMAIN domain_view_dependency_positive;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testdropdomainusedbyviewrequirescascaderepro-0001-drop-domain-domain_view_dependency_positive",

						// TestDropDomainUsedByFunctionRequiresCascadeRepro reproduces a dependency
						// bug: PostgreSQL rejects dropping a domain referenced by a function signature
						// unless CASCADE is requested.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestDropDomainUsedByFunctionRequiresCascadeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP DOMAIN rejects function signature dependencies",
			SetUpScript: []string{
				`CREATE DOMAIN domain_function_dependency_positive AS integer
					CONSTRAINT domain_function_dependency_positive_check CHECK (VALUE > 0);`,
				`CREATE FUNCTION domain_function_dependency_double(
					input_value domain_function_dependency_positive
				) RETURNS INT
					LANGUAGE SQL IMMUTABLE AS $$ SELECT input_value::int * 2 $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP DOMAIN domain_function_dependency_positive;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testdropdomainusedbyfunctionrequirescascaderepro-0001-drop-domain-domain_function_dependency_positive",

						// TestDropDomainUsedByColumnDefaultRequiresCascadeRepro reproduces a
						// dependency bug: PostgreSQL rejects dropping a domain referenced by a column
						// default unless CASCADE is requested.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestDropDomainUsedByColumnDefaultRequiresCascadeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP DOMAIN rejects column default expression dependencies",
			SetUpScript: []string{
				`CREATE DOMAIN domain_default_dependency_positive AS integer
					CONSTRAINT domain_default_dependency_positive_check CHECK (VALUE > 0);`,
				`CREATE TABLE domain_default_dependency_items (
					id INT PRIMARY KEY,
					amount INT DEFAULT (1::domain_default_dependency_positive)::integer
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP DOMAIN domain_default_dependency_positive;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testdropdomainusedbycolumndefaultrequirescascaderepro-0001-drop-domain-domain_default_dependency_positive", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO domain_default_dependency_items (id)
						VALUES (1);`,
				},
				{
					Query: `SELECT id, amount
						FROM domain_default_dependency_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testdropdomainusedbycolumndefaultrequirescascaderepro-0002-select-id-amount-from-domain_default_dependency_items"},
				},
			},
		},
	})
}

// TestDropDomainUsedByCheckConstraintRequiresCascadeRepro reproduces a
// dependency bug: PostgreSQL rejects dropping a domain referenced by a CHECK
// constraint unless CASCADE is requested.
func TestDropDomainUsedByCheckConstraintRequiresCascadeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP DOMAIN rejects check constraint expression dependencies",
			SetUpScript: []string{
				`CREATE DOMAIN domain_check_dependency_positive AS integer
					CONSTRAINT domain_check_dependency_positive_check CHECK (VALUE > 0);`,
				`CREATE TABLE domain_check_dependency_items (
					id INT PRIMARY KEY,
					amount INT,
					CONSTRAINT domain_check_dependency_amount_check
						CHECK ((amount::domain_check_dependency_positive)::integer > 0)
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP DOMAIN domain_check_dependency_positive;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testdropdomainusedbycheckconstraintrequirescascaderepro-0001-drop-domain-domain_check_dependency_positive",

						// TestDropDomainUsedByGeneratedColumnRequiresCascadeRepro reproduces a
						// dependency bug: PostgreSQL rejects dropping a domain referenced by a stored
						// generated column unless CASCADE is requested.
						Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO domain_check_dependency_items
						VALUES (1, 7);`,
				},
			},
		},
	})
}

func TestDropDomainUsedByGeneratedColumnRequiresCascadeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP DOMAIN rejects generated column expression dependencies",
			SetUpScript: []string{
				`CREATE DOMAIN domain_generated_dependency_positive AS integer
					CONSTRAINT domain_generated_dependency_positive_check CHECK (VALUE > 0);`,
				`CREATE TABLE domain_generated_dependency_items (
					id INT PRIMARY KEY,
					amount INT,
					normalized INT GENERATED ALWAYS AS (
						(amount::domain_generated_dependency_positive)::integer
					) STORED
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP DOMAIN domain_generated_dependency_positive;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testdropdomainusedbygeneratedcolumnrequirescascaderepro-0001-drop-domain-domain_generated_dependency_positive", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO domain_generated_dependency_items (id, amount)
						VALUES (1, 7);`,
				},
				{
					Query: `SELECT id, amount, normalized
						FROM domain_generated_dependency_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testdropdomainusedbygeneratedcolumnrequirescascaderepro-0002-select-id-amount-normalized-from"},
				},
			},
		},
	})
}

// TestDropDomainUsedByExpressionIndexRequiresCascadeRepro reproduces a
// dependency bug: PostgreSQL rejects dropping a domain referenced by an
// expression index unless CASCADE is requested.
func TestDropDomainUsedByExpressionIndexRequiresCascadeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP DOMAIN rejects expression index dependencies",
			SetUpScript: []string{
				`CREATE DOMAIN domain_index_dependency_positive AS integer
					CONSTRAINT domain_index_dependency_positive_check CHECK (VALUE > 0);`,
				`CREATE TABLE domain_index_dependency_items (
					id INT PRIMARY KEY,
					amount INT
				);`,
				`CREATE INDEX domain_index_dependency_amount_idx
					ON domain_index_dependency_items (((amount::domain_index_dependency_positive)::integer));`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP DOMAIN domain_index_dependency_positive;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testdropdomainusedbyexpressionindexrequirescascaderepro-0001-drop-domain-domain_index_dependency_positive", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO domain_index_dependency_items
						VALUES (1, 7);`,
				},
				{
					Query: `SELECT id, amount
						FROM domain_index_dependency_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testdropdomainusedbyexpressionindexrequirescascaderepro-0002-select-id-amount-from-domain_index_dependency_items"},
				},
			},
		},
	})
}

// TestDropDomainDependencyChecksSchemaQualifiedDomainRepro reproduces a
// dependency correctness bug: dropping an unused domain in one schema should
// not be blocked by columns that use a distinct same-named domain in another
// schema.
func TestDropDomainDependencyChecksSchemaQualifiedDomainRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP DOMAIN dependency checks use schema-qualified domain identity",
			SetUpScript: []string{
				`CREATE SCHEMA drop_domain_schema_a;`,
				`CREATE SCHEMA drop_domain_schema_b;`,
				`CREATE DOMAIN drop_domain_schema_a.same_named_domain AS INT CHECK (VALUE > 0);`,
				`CREATE DOMAIN drop_domain_schema_b.same_named_domain AS INT CHECK (VALUE > 0);`,
				`CREATE TABLE drop_domain_schema_uses_b (
					id INT PRIMARY KEY,
					amount drop_domain_schema_b.same_named_domain
				);`,
				`INSERT INTO drop_domain_schema_uses_b VALUES (1, 2);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP DOMAIN drop_domain_schema_a.same_named_domain;`,
				},
				{
					Query: `SELECT n.nspname, t.typname
						FROM pg_catalog.pg_type t
						JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
						WHERE n.nspname IN ('drop_domain_schema_a', 'drop_domain_schema_b')
							AND t.typname = 'same_named_domain'
						ORDER BY n.nspname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testdropdomaindependencychecksschemaqualifieddomainrepro-0001-select-n.nspname-t.typname-from-pg_catalog.pg_type"},
				},
				{
					Query: `SELECT id
						FROM drop_domain_schema_uses_b;`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testdropdomaindependencychecksschemaqualifieddomainrepro-0002-select-id-from-drop_domain_schema_uses_b"},
				},
			},
		},
	})
}

// TestDropDomainCascadeWithoutDependentsRepro reproduces a DDL correctness bug:
// PostgreSQL accepts CASCADE on DROP DOMAIN even when no dependent objects need
// to be removed.
func TestDropDomainCascadeWithoutDependentsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP DOMAIN CASCADE works without dependents",
			SetUpScript: []string{
				`CREATE DOMAIN drop_domain_cascade_unused AS INT CHECK (VALUE > 0);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP DOMAIN drop_domain_cascade_unused CASCADE;`,
				},
				{
					Query: `SELECT t.typname
						FROM pg_catalog.pg_type t
						JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
						WHERE n.nspname = 'public'
							AND t.typname = 'drop_domain_cascade_unused';`, PostgresOracle: ScriptTestPostgresOracle{ID: "domain-correctness-repro-test-testdropdomaincascadewithoutdependentsrepro-0001-select-t.typname-from-pg_catalog.pg_type-t"},
				},
			},
		},
	})
}
