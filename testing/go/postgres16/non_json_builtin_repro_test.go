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

// TestIntegerBaseFormattingBuiltinsRepro reproduces a PostgreSQL compatibility
// gap: PostgreSQL exposes to_bin and to_oct for base-formatting integers.
func TestIntegerBaseFormattingBuiltinsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "to_bin and to_oct format integers",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT to_bin(10);`, PostgresOracle: ScriptTestPostgresOracle{ID: "non-json-builtin-repro-test-testintegerbaseformattingbuiltinsrepro-0001-select-to_bin-10"},
				},
				{
					Query: `SELECT to_oct(10);`, PostgresOracle: ScriptTestPostgresOracle{ID: "non-json-builtin-repro-test-testintegerbaseformattingbuiltinsrepro-0002-select-to_oct-10"},
				},
			},
		},
	})
}

// TestUuidExtractionBuiltinsRepro reproduces a PostgreSQL compatibility gap:
// PostgreSQL exposes built-ins for extracting version and timestamp metadata
// from UUID values.
func TestUuidExtractionBuiltinsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "uuid_extract_version and uuid_extract_timestamp inspect UUIDs",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT uuid_extract_version('41db1265-8bc1-4ab3-992f-885799a4af1d'::uuid)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "non-json-builtin-repro-test-testuuidextractionbuiltinsrepro-0001-select-uuid_extract_version-41db1265-8bc1-4ab3-992f-885799a4af1d-::uuid-::text"},
				},
				{
					Query: `SELECT uuid_extract_timestamp('41db1265-8bc1-4ab3-992f-885799a4af1d'::uuid) IS NULL;`, PostgresOracle: ScriptTestPostgresOracle{ID: "non-json-builtin-repro-test-testuuidextractionbuiltinsrepro-0002-select-uuid_extract_timestamp-41db1265-8bc1-4ab3-992f-885799a4af1d-::uuid-is"},
				},
			},
		},
	})
}

// TestTypeMetadataBuiltinsRepro reproduces PostgreSQL compatibility gaps:
// PostgreSQL exposes pg_basetype for domain base-type lookup and to_regtypemod
// for parsing type modifiers from textual type specifications.
func TestTypeMetadataBuiltinsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_basetype and to_regtypemod expose type metadata",
			SetUpScript: []string{
				`CREATE DOMAIN non_json_builtin_domain AS text;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT pg_basetype('non_json_builtin_domain'::regtype)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "non-json-builtin-repro-test-testtypemetadatabuiltinsrepro-0001-select-pg_basetype-non_json_builtin_domain-::regtype-::text"},
				},
				{
					Query: `SELECT format_type(to_regtype('varchar(32)'), to_regtypemod('varchar(32)'));`, PostgresOracle: ScriptTestPostgresOracle{ID: "non-json-builtin-repro-test-testtypemetadatabuiltinsrepro-0002-select-format_type-to_regtype-varchar-32"},
				},
			},
		},
	})
}

// TestUnicodeInformationBuiltinsRepro reproduces PostgreSQL compatibility gaps:
// PostgreSQL exposes Unicode metadata helpers for checking assigned codepoints
// and reporting the built-in Unicode data version.
func TestUnicodeInformationBuiltinsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "unicode_assigned and unicode_version report Unicode metadata",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT unicode_assigned('abc');`, PostgresOracle: ScriptTestPostgresOracle{ID: "non-json-builtin-repro-test-testunicodeinformationbuiltinsrepro-0001-select-unicode_assigned-abc"},
				},
				{
					Query: `SELECT unicode_version() IS NOT NULL;`, PostgresOracle: ScriptTestPostgresOracle{ID: "non-json-builtin-repro-test-testunicodeinformationbuiltinsrepro-0002-select-unicode_version-is-not-null"},
				},
			},
		},
	})
}

// TestPostgres16InputValidationBuiltinsRepro reproduces PostgreSQL 16
// compatibility gaps: pg_input_is_valid and pg_input_error_info should expose
// soft input-validation checks.
func TestPostgres16InputValidationBuiltinsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_input_is_valid and pg_input_error_info report soft input errors",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT pg_input_is_valid('42', 'integer'), pg_input_is_valid('42000000000', 'integer');`, PostgresOracle: ScriptTestPostgresOracle{ID: "non-json-builtin-repro-test-testpostgres16inputvalidationbuiltinsrepro-0001-select-pg_input_is_valid-42-integer-pg_input_is_valid"},
				},
				{
					Query: `SELECT (pg_input_error_info('42000000000', 'integer')).sql_error_code;`, PostgresOracle: ScriptTestPostgresOracle{ID: "non-json-builtin-repro-test-testpostgres16inputvalidationbuiltinsrepro-0002-select-pg_input_error_info-42000000000-integer-.sql_error_code"},
				},
			},
		},
	})
}

// TestPostgres16ArrayRandomBuiltinsRepro reproduces PostgreSQL 16
// compatibility gaps: array_sample and array_shuffle should randomly sample
// or shuffle the first array dimension.
func TestPostgres16ArrayRandomBuiltinsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "array_sample and array_shuffle preserve requested cardinality",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT array_length(array_sample(ARRAY[1,2,3,4], 2), 1);`, PostgresOracle: ScriptTestPostgresOracle{ID: "non-json-builtin-repro-test-testpostgres16arrayrandombuiltinsrepro-0001-select-array_length-array_sample-array[1-2"},
				},
				{
					Query: `SELECT array_length(array_shuffle(ARRAY[1,2,3,4]), 1);`, PostgresOracle: ScriptTestPostgresOracle{ID: "non-json-builtin-repro-test-testpostgres16arrayrandombuiltinsrepro-0002-select-array_length-array_shuffle-array[1-2"},
				},
			},
		},
	})
}

// TestPostgres16DateAndRandomBuiltinsRepro reproduces PostgreSQL 16
// compatibility gaps: date_add, date_subtract, and random_normal should be
// available as built-in functions.
func TestPostgres16DateAndRandomBuiltinsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "date_add, date_subtract, and random_normal evaluate",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT date_add(
						'2024-01-01 00:00:00+00'::timestamptz,
						'1 day'::interval,
						'UTC') = '2024-01-02 00:00:00+00'::timestamptz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "non-json-builtin-repro-test-testpostgres16dateandrandombuiltinsrepro-0001-select-date_add-2024-01-01-00:00:00+00-::timestamptz"},
				},
				{
					Query: `SELECT date_subtract(
						'2024-01-02 00:00:00+00'::timestamptz,
						'1 day'::interval,
						'UTC') = '2024-01-01 00:00:00+00'::timestamptz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "non-json-builtin-repro-test-testpostgres16dateandrandombuiltinsrepro-0002-select-date_subtract-2024-01-02-00:00:00+00-::timestamptz"},
				},
				{
					Query: `SELECT random_normal(0.0, 1.0) IS NOT NULL;`, PostgresOracle: ScriptTestPostgresOracle{ID: "non-json-builtin-repro-test-testpostgres16dateandrandombuiltinsrepro-0003-select-random_normal-0.0-1.0-is"},
				},
			},
		},
	})
}

// TestPostgres16AnyValueAndSystemUserRepro reproduces PostgreSQL 16
// compatibility gaps: ANY_VALUE should be available as an aggregate and
// SYSTEM_USER should expose the authenticated identity.
func TestPostgres16AnyValueAndSystemUserRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ANY_VALUE and SYSTEM_USER evaluate",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT any_value(v) FROM (VALUES (NULL::text), ('picked')) AS t(v);`, PostgresOracle: ScriptTestPostgresOracle{ID: "non-json-builtin-repro-test-testpostgres16anyvalueandsystemuserrepro-0001-select-any_value-v-from-values"},
				},
				{
					Query: `SELECT system_user IS NOT NULL;`, PostgresOracle: ScriptTestPostgresOracle{ID: "non-json-builtin-repro-test-testpostgres16anyvalueandsystemuserrepro-0002-select-system_user-is-not-null"},
				},
			},
		},
	})
}
