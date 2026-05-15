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
	"github.com/dolthub/go-mysql-server/sql"
	. "github.com/dolthub/doltgresql/testing/go"

	"testing"
)

// TestUnboundedVarcharPrimaryKeyPreservesRowsGuard guards that unbounded
// varchar columns can be primary keys and store distinct key values normally.
func TestUnboundedVarcharPrimaryKeyPreservesRowsGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "unbounded varchar primary key preserves rows",
			SetUpScript: []string{
				`CREATE TABLE unbounded_varchar_pk_items (
					id INT,
					code CHARACTER VARYING PRIMARY KEY
				);`,
				`INSERT INTO unbounded_varchar_pk_items VALUES
					(1, 'abcdefghij'),
					(2, 'klmnopqrst');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, code
						FROM unbounded_varchar_pk_items
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testunboundedvarcharprimarykeypreservesrowsguard-0001-select-id-code-from-unbounded_varchar_pk_items"},
				},
			},
		},
	})
}

// TestByteaPrimaryKeySupportsLookupGuard guards that bytea primary keys support
// equality lookups over the key.
func TestByteaPrimaryKeySupportsLookupGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "bytea primary key supports equality lookup",
			SetUpScript: []string{
				`CREATE TABLE bytea_pk_items (
					id BYTEA PRIMARY KEY,
					payload BYTEA
				);`,
				`INSERT INTO bytea_pk_items VALUES
					(E'\\xCAFEBABE', E'\\xDEADBEEF'),
					('\xBADD00D5', '\xC0FFEE');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, payload
						FROM bytea_pk_items
						WHERE id = E'\\xCAFEBABE';`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testbyteaprimarykeysupportslookupguard-0001-select-id-payload-from-bytea_pk_items",

						// TestInternalCharCastsHighBitByteToSignedIntRepro reproduces a type-value
						// correctness bug: casting a one-byte "char" value to integer should use
						// PostgreSQL's signed-byte semantics.
						ColumnModes: []string{"bytea", "bytea"}},
				},
			},
		},
	})
}

func TestInternalCharCastsHighBitByteToSignedIntRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: `"char" high-bit byte casts to signed integer`,
			SetUpScript: []string{
				`CREATE TABLE internal_char_cast_items (
					id INT PRIMARY KEY,
					value "char"
				);`,
				`INSERT INTO internal_char_cast_items VALUES (1, 'こんにちは');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT value::int
						FROM internal_char_cast_items
						WHERE id = 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testinternalcharcastshighbitbytetosignedintrepro-0001-select-value::int-from-internal_char_cast_items-where"},
				},
			},
		},
	})
}

// TestDropTypeDependencyChecksSchemaQualifiedTypeRepro reproduces a dependency
// correctness bug: dropping an unused type in one schema should not be blocked
// by columns that use a distinct same-named type in another schema.
func TestDropTypeDependencyChecksSchemaQualifiedTypeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP TYPE dependency checks use schema-qualified type identity",
			SetUpScript: []string{
				`CREATE SCHEMA drop_type_schema_a;`,
				`CREATE SCHEMA drop_type_schema_b;`,
				`CREATE TYPE drop_type_schema_a.same_named_enum AS ENUM ('one');`,
				`CREATE TYPE drop_type_schema_b.same_named_enum AS ENUM ('two');`,
				`CREATE TABLE drop_type_schema_uses_b (
					id INT PRIMARY KEY,
					status drop_type_schema_b.same_named_enum
				);`,
				`INSERT INTO drop_type_schema_uses_b VALUES (1, 'two');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP TYPE drop_type_schema_a.same_named_enum;`,
				},
				{
					Query: `SELECT n.nspname, t.typname
						FROM pg_catalog.pg_type t
						JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
						WHERE n.nspname IN ('drop_type_schema_a', 'drop_type_schema_b')
							AND t.typname = 'same_named_enum'
						ORDER BY n.nspname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testdroptypedependencychecksschemaqualifiedtyperepro-0001-select-n.nspname-t.typname-from-pg_catalog.pg_type"},
				},
				{
					Query: `SELECT id, status::text
						FROM drop_type_schema_uses_b;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testdroptypedependencychecksschemaqualifiedtyperepro-0002-select-id-status::text-from-drop_type_schema_uses_b"},
				},
			},
		},
	})
}

// TestDropCompositeTypeUsedByTypedTableRequiresCascadeRepro reproduces a typed
// table dependency bug: PostgreSQL rejects dropping the composite type that a
// typed table was created OF unless CASCADE is requested.
func TestDropCompositeTypeUsedByTypedTableRequiresCascadeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP TYPE rejects typed table dependency",
			SetUpScript: []string{
				`CREATE TYPE typed_drop_dependency_row AS (
					id INT,
					note TEXT
				);`,
				`CREATE TABLE typed_drop_dependency_items OF typed_drop_dependency_row;`,
				`INSERT INTO typed_drop_dependency_items VALUES (1, 'kept');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP TYPE typed_drop_dependency_row;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testdropcompositetypeusedbytypedtablerequirescascaderepro-0001-drop-type-typed_drop_dependency_row", Compare: "sqlstate"},
				},
				{
					Query: `SELECT c.reloftype = t.oid
						FROM pg_catalog.pg_class c
						JOIN pg_catalog.pg_type t ON t.typname = 'typed_drop_dependency_row'
						WHERE c.relname = 'typed_drop_dependency_items';`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testdropcompositetypeusedbytypedtablerequirescascaderepro-0002-select-c.reloftype-=-t.oid-from"},
				},
				{
					Query: `SELECT id, note
						FROM typed_drop_dependency_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testdropcompositetypeusedbytypedtablerequirescascaderepro-0003-select-id-note-from-typed_drop_dependency_items"},
				},
			},
		},
	})
}

// TestDropTypeUsedByFunctionRequiresCascadeRepro reproduces a dependency bug:
// PostgreSQL rejects dropping a type referenced by a function signature unless
// CASCADE is requested.
func TestDropTypeUsedByFunctionRequiresCascadeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP TYPE rejects function signature dependencies",
			SetUpScript: []string{
				`CREATE TYPE type_function_dependency_status AS ENUM ('ready');`,
				`CREATE FUNCTION type_function_dependency_text(
					input_value type_function_dependency_status
				) RETURNS TEXT
					LANGUAGE SQL IMMUTABLE AS $$ SELECT input_value::text $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP TYPE type_function_dependency_status;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testdroptypeusedbyfunctionrequirescascaderepro-0001-drop-type-type_function_dependency_status",

						// TestDropTypeCascadeWithoutDependentsRepro reproduces a DDL correctness bug:
						// PostgreSQL accepts CASCADE on DROP TYPE even when no dependent objects need
						// to be removed.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestDropTypeCascadeWithoutDependentsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP TYPE CASCADE works without dependents",
			SetUpScript: []string{
				`CREATE TYPE drop_type_cascade_unused AS ENUM ('one');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP TYPE drop_type_cascade_unused CASCADE;`,
				},
				{
					Query: `SELECT t.typname
						FROM pg_catalog.pg_type t
						JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
						WHERE n.nspname = 'public'
							AND t.typname = 'drop_type_cascade_unused';`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testdroptypecascadewithoutdependentsrepro-0001-select-t.typname-from-pg_catalog.pg_type-t"},
				},
			},
		},
	})
}

// TestNameTypeAcceptsIntegerAssignmentRepro reproduces a type correctness bug:
// PostgreSQL assignment-coerces integer expressions to name columns via output text.
func TestNameTypeAcceptsIntegerAssignmentRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "name column accepts integer assignment",
			SetUpScript: []string{
				`CREATE TABLE name_assignment_items (
					id INT PRIMARY KEY,
					label NAME
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO name_assignment_items VALUES (1, 12345);`,
				},
				{
					Query:    `SELECT id, label::text FROM name_assignment_items ORDER BY id;`,
					Expected: []sql.Row{{1, "12345"}},
				},
			},
		},
	})
}

// TestXidRejectsInvalidInputRepro reproduces a type correctness bug:
// PostgreSQL's xid input rejects out-of-range or non-numeric transaction IDs.
func TestXidRejectsInvalidInputRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "xid rejects invalid inputs",
			SetUpScript: []string{
				`CREATE TABLE xid_input_items (
					id INT PRIMARY KEY,
					x XID
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO xid_input_items VALUES (1, '4294967296');`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testxidrejectsinvalidinputrepro-0001-insert-into-xid_input_items-values-1", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO xid_input_items VALUES (2, '-1');`,
				},
				{
					Query:    `SELECT id, x::text FROM xid_input_items ORDER BY id;`,
					Expected: []sql.Row{{2, "4294967295"}},
				},
				{
					Query: `INSERT INTO xid_input_items VALUES (3, 'abc');`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testxidrejectsinvalidinputrepro-0003-insert-into-xid_input_items-values-3",

						// TestXidOrderingRequiresOrderingOperatorRepro reproduces a type correctness
						// bug: PostgreSQL rejects ORDER BY xid because xid has no ordering operator.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestXidOrderingRequiresOrderingOperatorRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "xid ORDER BY requires ordering operator",
			SetUpScript: []string{
				`CREATE TABLE xid_order_items (
					id INT PRIMARY KEY,
					x XID
				);`,
				`INSERT INTO xid_order_items VALUES (1, '100'), (2, '200');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, x FROM xid_order_items ORDER BY x;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testxidorderingrequiresorderingoperatorrepro-0001-select-id-x-from-xid_order_items",

						// TestInternalCharCastToIntegerUsesSignedByteRepro reproduces a type
						// correctness bug: PostgreSQL casts the internal "char" type to integer using
						// signed one-byte semantics.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestInternalCharCastToIntegerUsesSignedByteRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: `internal "char" casts high-bit bytes to signed integers`,
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT 'こんにちは'::"char"::int;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testinternalcharcasttointegerusessignedbyterepro-0001-select-こんにちは-::-char-::int"},
				},
			},
		},
	})
}

// TestRegtypeLiteralHasRegtypeTypeGuard guards that regtype casts expose the
// regtype result type, not the underlying OID integer type.
func TestRegtypeLiteralHasRegtypeTypeGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "regtype literal exposes regtype result type",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT pg_typeof('integer'::regtype)::text, 'integer'::regtype::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testregtypeliteralhasregtypetypeguard-0001-select-pg_typeof-integer-::regtype-::text"},
				},
			},
		},
	})
}

// TestNumericPrimaryKeyEqualityLookupGuard guards that numeric primary-key
// rows can be found by an equivalent numeric literal.
func TestNumericPrimaryKeyEqualityLookupGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "numeric primary key equality lookup",
			SetUpScript: []string{
				`CREATE TABLE numeric_pk_items (
					id NUMERIC(5,2) PRIMARY KEY,
					amount NUMERIC(5,2)
				);`,
				`INSERT INTO numeric_pk_items VALUES
					(123.45, 67.89),
					(67.89, 100.3);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, amount
						FROM numeric_pk_items
						WHERE id = 123.45
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testnumericprimarykeyequalitylookupguard-0001-select-id-amount-from-numeric_pk_items"},
				},
			},
		},
	})
}

// TestNumericTypmodDefaultWritePathsGuard guards that PostgreSQL applies the
// target numeric typmod when DEFAULT is assigned through insert, update, and
// altered default write paths.
func TestNumericTypmodDefaultWritePathsGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "numeric typmod default write paths coerce stored values",
			SetUpScript: []string{
				`CREATE TABLE numeric_typmod_default_items (
					id INT PRIMARY KEY,
					amount NUMERIC(5,2) DEFAULT 123.456
				);`,
				`INSERT INTO numeric_typmod_default_items (id) VALUES (1);`,
				`INSERT INTO numeric_typmod_default_items (id, amount)
					VALUES (2, DEFAULT);`,
				`INSERT INTO numeric_typmod_default_items VALUES (3, 0);`,
				`UPDATE numeric_typmod_default_items
					SET amount = DEFAULT
					WHERE id = 3;`,
				`ALTER TABLE numeric_typmod_default_items
					ALTER COLUMN amount SET DEFAULT 234.567;`,
				`INSERT INTO numeric_typmod_default_items (id) VALUES (4);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, amount::text
						FROM numeric_typmod_default_items
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testnumerictypmoddefaultwritepathsguard-0001-select-id-amount::text-from-numeric_typmod_default_items"},
				},
			},
		},
	})
}

// TestVarcharTypmodEnforcesLengthGuard guards PostgreSQL varchar(n) storage
// semantics: implicit assignment rejects over-length values, while explicit
// casts may truncate to the declared length.
func TestVarcharTypmodEnforcesLengthGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "varchar typmod enforces length",
			SetUpScript: []string{
				`CREATE TABLE varchar_typmod_items (
					id INT PRIMARY KEY,
					label VARCHAR(3)
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO varchar_typmod_items VALUES (1, 'abcd');`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testvarchartypmodenforceslengthguard-0001-insert-into-varchar_typmod_items-values-1", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO varchar_typmod_items VALUES (2, 'abcd'::varchar(3));`,
				},
				{
					Query: `SELECT id, label FROM varchar_typmod_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testvarchartypmodenforceslengthguard-0002-select-id-label-from-varchar_typmod_items"},
				},
			},
		},
	})
}

// TestVarcharTypmodTruncatesTrailingSpacesRepro reproduces a varchar typmod
// correctness bug: PostgreSQL accepts over-length implicit assignments when
// every excess character is a space, truncating the trailing spaces to the
// declared length.
func TestVarcharTypmodTruncatesTrailingSpacesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "varchar typmod truncates trailing spaces",
			SetUpScript: []string{
				`CREATE TABLE varchar_trailing_space_items (
					id INT PRIMARY KEY,
					label VARCHAR(3)
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO varchar_trailing_space_items VALUES (1, 'abc   ');`,
				},
				{
					Query: `SELECT id, label, length(label) FROM varchar_trailing_space_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testvarchartypmodtruncatestrailingspacesrepro-0001-select-id-label-length-label"},
				},
			},
		},
	})
}

// TestCharacterTypmodTruncatesTrailingSpacesRepro reproduces a character
// typmod correctness bug: PostgreSQL accepts over-length implicit assignments
// to character(n) when every excess character is a space, truncating the
// trailing spaces to the declared length.
func TestCharacterTypmodTruncatesTrailingSpacesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "character typmod truncates trailing spaces",
			SetUpScript: []string{
				`CREATE TABLE character_trailing_space_items (
					id INT PRIMARY KEY,
					label CHARACTER(3)
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO character_trailing_space_items VALUES (1, 'abc   ');`,
				},
				{
					Query: `SELECT id, label, length(label) FROM character_trailing_space_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testcharactertypmodtruncatestrailingspacesrepro-0001-select-id-label-length-label"},
				},
			},
		},
	})
}

// TestVarcharTypmodDefaultTruncatesTrailingSpacesRepro reproduces a varchar
// typmod correctness bug: PostgreSQL accepts a default whose only excess
// characters are spaces, truncating the trailing spaces when the default is
// used.
func TestVarcharTypmodDefaultTruncatesTrailingSpacesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "varchar typmod default truncates trailing spaces",
			SetUpScript: []string{
				`CREATE TABLE varchar_default_trailing_space_items (
					id INT PRIMARY KEY,
					label VARCHAR(3) DEFAULT 'abc   '
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO varchar_default_trailing_space_items (id)
						VALUES (1);`,
				},
				{
					Query: `SELECT label, length(label), pg_typeof(label)::text
						FROM varchar_default_trailing_space_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testvarchartypmoddefaulttruncatestrailingspacesrepro-0001-select-label-length-label-pg_typeof"},
				},
			},
		},
	})
}

// TestCharacterTypmodDefaultTruncatesTrailingSpacesRepro reproduces a character
// typmod correctness bug: PostgreSQL accepts a default whose only excess
// characters are spaces, truncating the trailing spaces when the default is
// used.
func TestCharacterTypmodDefaultTruncatesTrailingSpacesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "character typmod default truncates trailing spaces",
			SetUpScript: []string{
				`CREATE TABLE character_default_trailing_space_items (
					id INT PRIMARY KEY,
					label CHARACTER(3) DEFAULT 'abc   '
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO character_default_trailing_space_items (id)
						VALUES (1);`,
				},
				{
					Query: `SELECT label = 'abc'::CHARACTER(3), octet_length(label), pg_typeof(label)::text
						FROM character_default_trailing_space_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testcharactertypmoddefaulttruncatestrailingspacesrepro-0001-select-label-=-abc-::character"},
				},
			},
		},
	})
}

// TestCharacterTypmodIgnoresTrailingSpacesForUniquenessRepro reproduces a
// character(n) correctness bug: PostgreSQL treats trailing spaces as
// semantically insignificant for character(n) equality, so unique constraints
// reject values that differ only by padding spaces.
func TestCharacterTypmodIgnoresTrailingSpacesForUniquenessRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "character typmod ignores trailing spaces for uniqueness",
			SetUpScript: []string{
				`CREATE TABLE character_unique_padding_items (
					id INT PRIMARY KEY,
					label CHARACTER(3) UNIQUE
				);`,
				`INSERT INTO character_unique_padding_items VALUES (1, 'a');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO character_unique_padding_items VALUES (2, 'a  ');`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testcharactertypmodignorestrailingspacesforuniquenessrepro-0001-insert-into-character_unique_padding_items-values-2", Compare: "sqlstate"},
				},
				{
					Query: `SELECT id, label = 'a  '::character(3) FROM character_unique_padding_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testcharactertypmodignorestrailingspacesforuniquenessrepro-0002-select-id-label-=-a"},
				},
			},
		},
	})
}

// TestCharacterTypmodStoresPaddedValuesRepro reproduces a persistence bug:
// PostgreSQL pads shorter values assigned to character(n) columns to the
// declared fixed width before storage.
func TestCharacterTypmodStoresPaddedValuesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "character typmod stores padded values",
			SetUpScript: []string{
				`CREATE TABLE character_storage_items (
					id INT PRIMARY KEY,
					label CHARACTER(3)
				);`,
				`INSERT INTO character_storage_items VALUES (1, 'ab');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT octet_length(label)
						FROM character_storage_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testcharactertypmodstorespaddedvaluesrepro-0001-select-octet_length-label-from-character_storage_items"},
				},
				{
					Query: `SELECT label = 'ab '::CHARACTER(3)
						FROM character_storage_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testcharactertypmodstorespaddedvaluesrepro-0002-select-label-=-ab-::character"},
				},
			},
		},
	})
}

// TestVarcharArrayTypmodTruncatesTrailingSpacesGuard guards that PostgreSQL's
// varchar(n) trailing-space truncation rule applies to array elements as well
// as scalar columns.
func TestVarcharArrayTypmodTruncatesTrailingSpacesGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "varchar array typmod truncates trailing spaces",
			SetUpScript: []string{
				`CREATE TABLE varchar_array_trailing_space_items (
					id INT PRIMARY KEY,
					labels VARCHAR(3)[]
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO varchar_array_trailing_space_items
						VALUES (1, ARRAY['abc   ']::varchar(3)[]);`,
				},
				{
					Query: `SELECT id, labels FROM varchar_array_trailing_space_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testvarchararraytypmodtruncatestrailingspacesguard-0001-select-id-labels-from-varchar_array_trailing_space_items"},
				},
			},
		},
	})
}

// TestVarcharArrayAppendValidatesElementTypmodRepro reproduces an array
// persistence bug: array mutation results assigned into varchar(n)[] columns
// must validate the appended element against the column's element typmod.
func TestVarcharArrayAppendValidatesElementTypmodRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "varchar array_append validates element typmod",
			SetUpScript: []string{
				`CREATE TABLE varchar_array_append_typmod_items (
					id INT PRIMARY KEY,
					labels VARCHAR(3)[]
				);`,
				`INSERT INTO varchar_array_append_typmod_items
					VALUES (1, ARRAY['abc']::varchar(3)[]);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE varchar_array_append_typmod_items
						SET labels = array_append(labels, 'abcd')
						WHERE id = 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testvarchararrayappendvalidateselementtypmodrepro-0001-update-varchar_array_append_typmod_items-set-labels-=", Compare: "sqlstate"},
				},
				{
					Query: `SELECT labels::text
						FROM varchar_array_append_typmod_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testvarchararrayappendvalidateselementtypmodrepro-0002-select-labels::text-from-varchar_array_append_typmod_items"},
				},
			},
		},
	})
}

// TestVarcharArrayPrependValidatesElementTypmodRepro reproduces an array
// persistence bug: array_prepend results assigned into varchar(n)[] columns
// must validate the prepended element against the column's element typmod.
func TestVarcharArrayPrependValidatesElementTypmodRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "varchar array_prepend validates element typmod",
			SetUpScript: []string{
				`CREATE TABLE varchar_array_prepend_typmod_items (
					id INT PRIMARY KEY,
					labels VARCHAR(3)[]
				);`,
				`INSERT INTO varchar_array_prepend_typmod_items
					VALUES (1, ARRAY['abc']::varchar(3)[]);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE varchar_array_prepend_typmod_items
						SET labels = array_prepend('abcd', labels)
						WHERE id = 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testvarchararrayprependvalidateselementtypmodrepro-0001-update-varchar_array_prepend_typmod_items-set-labels-=", Compare: "sqlstate"},
				},
				{
					Query: `SELECT labels::text
						FROM varchar_array_prepend_typmod_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testvarchararrayprependvalidateselementtypmodrepro-0002-select-labels::text-from-varchar_array_prepend_typmod_items"},
				},
			},
		},
	})
}

// TestVarcharArrayCatReportsAssignmentTypmodErrorRepro reproduces an array
// correctness bug: PostgreSQL resolves array_cat with a compatible untyped
// array literal before assignment validation rejects the oversized element.
func TestVarcharArrayCatReportsAssignmentTypmodErrorRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "varchar array_cat reports assignment typmod error",
			SetUpScript: []string{
				`CREATE TABLE varchar_array_cat_typmod_items (
					id INT PRIMARY KEY,
					labels VARCHAR(3)[]
				);`,
				`INSERT INTO varchar_array_cat_typmod_items
					VALUES (1, ARRAY['abc']::varchar(3)[]);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE varchar_array_cat_typmod_items
						SET labels = array_cat(labels, ARRAY['abcd'])
						WHERE id = 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testvarchararraycatreportsassignmenttypmoderrorrepro-0001-update-varchar_array_cat_typmod_items-set-labels-=", Compare: "sqlstate"},
				},
				{
					Query: `SELECT labels::text
						FROM varchar_array_cat_typmod_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testvarchararraycatreportsassignmenttypmoderrorrepro-0002-select-labels::text-from-varchar_array_cat_typmod_items"},
				},
			},
		},
	})
}

// TestCharacterArrayAppendAppliesElementTypmodRepro reproduces array
// persistence bugs: array_append results assigned into character(n)[] columns
// must pad shorter appended elements and reject over-length appended elements.
func TestCharacterArrayAppendAppliesElementTypmodRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "character array_append applies element typmod",
			SetUpScript: []string{
				`CREATE TABLE character_array_append_typmod_items (
					id INT PRIMARY KEY,
					labels CHARACTER(3)[]
				);`,
				`INSERT INTO character_array_append_typmod_items
					VALUES (1, ARRAY['abc']::character(3)[]);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE character_array_append_typmod_items
						SET labels = array_append(labels, 'ab')
						WHERE id = 1;`,
				},
				{
					Query: `SELECT labels::text, octet_length(labels[2]), labels[2] = 'ab '::CHARACTER(3)
						FROM character_array_append_typmod_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testcharacterarrayappendapplieselementtypmodrepro-0001-select-labels::text-octet_length-labels[2]-labels[2]"},
				},
				{
					Query: `UPDATE character_array_append_typmod_items
						SET labels = array_append(labels, 'abcd')
						WHERE id = 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testcharacterarrayappendapplieselementtypmodrepro-0002-update-character_array_append_typmod_items-set-labels-=", Compare: "sqlstate"},
				},
				{
					Query: `SELECT labels::text
						FROM character_array_append_typmod_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testcharacterarrayappendapplieselementtypmodrepro-0003-select-labels::text-from-character_array_append_typmod_items"},
				},
			},
		},
	})
}

// TestCharacterArrayPrependAppliesElementTypmodRepro reproduces array
// persistence bugs: array_prepend results assigned into character(n)[] columns
// must pad shorter prepended elements and reject over-length prepended
// elements.
func TestCharacterArrayPrependAppliesElementTypmodRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "character array_prepend applies element typmod",
			SetUpScript: []string{
				`CREATE TABLE character_array_prepend_typmod_items (
					id INT PRIMARY KEY,
					labels CHARACTER(3)[]
				);`,
				`INSERT INTO character_array_prepend_typmod_items
					VALUES (1, ARRAY['abc']::character(3)[]);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE character_array_prepend_typmod_items
						SET labels = array_prepend('ab', labels)
						WHERE id = 1;`,
				},
				{
					Query: `SELECT labels::text, octet_length(labels[1]), labels[1] = 'ab '::CHARACTER(3)
						FROM character_array_prepend_typmod_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testcharacterarrayprependapplieselementtypmodrepro-0001-select-labels::text-octet_length-labels[1]-labels[1]"},
				},
				{
					Query: `UPDATE character_array_prepend_typmod_items
						SET labels = array_prepend('abcd', labels)
						WHERE id = 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testcharacterarrayprependapplieselementtypmodrepro-0002-update-character_array_prepend_typmod_items-set-labels-=", Compare: "sqlstate"},
				},
				{
					Query: `SELECT labels::text
						FROM character_array_prepend_typmod_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testcharacterarrayprependapplieselementtypmodrepro-0003-select-labels::text-from-character_array_prepend_typmod_items"},
				},
			},
		},
	})
}

// TestCharacterArrayCatResolvesTypmodArrayRepro reproduces an array correctness
// bug: PostgreSQL resolves array_cat for character(n)[] columns and compatible
// untyped array literals before applying assignment typmods.
func TestCharacterArrayCatResolvesTypmodArrayRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "character array_cat resolves typmod array arguments",
			SetUpScript: []string{
				`CREATE TABLE character_array_cat_typmod_items (
					id INT PRIMARY KEY,
					labels CHARACTER(3)[]
				);`,
				`INSERT INTO character_array_cat_typmod_items
					VALUES (1, ARRAY['abc']::character(3)[]);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE character_array_cat_typmod_items
						SET labels = array_cat(labels, ARRAY['ab'])
						WHERE id = 1;`,
				},
				{
					Query: `SELECT labels::text, octet_length(labels[2]), labels[2] = 'ab '::CHARACTER(3)
						FROM character_array_cat_typmod_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testcharacterarraycatresolvestypmodarrayrepro-0001-select-labels::text-octet_length-labels[2]-labels[2]"},
				},
			},
		},
	})
}

// TestCharacterArrayTypmodSupportsEqualityRepro reproduces an array
// correctness bug: PostgreSQL array equality delegates to the element type, so
// character(n) array elements that differ only by trailing padding spaces
// compare equal.
func TestCharacterArrayTypmodSupportsEqualityRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "character array typmod supports equality",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT ARRAY['a']::character(3)[] = ARRAY['a  ']::character(3)[];`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testcharacterarraytypmodsupportsequalityrepro-0001-select-array[-a-]::character-3"},
				},
			},
		},
	})
}

// TestCharacterArrayTypmodStoresPaddedElementsRepro reproduces an array
// persistence bug: PostgreSQL pads shorter character(n) array elements to the
// declared fixed width before storage.
func TestCharacterArrayTypmodStoresPaddedElementsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "character array typmod stores padded elements",
			SetUpScript: []string{
				`CREATE TABLE character_array_storage_items (
					id INT PRIMARY KEY,
					labels CHARACTER(3)[]
				);`,
				`INSERT INTO character_array_storage_items
					VALUES (1, ARRAY['ab']::CHARACTER(3)[]);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT octet_length(labels[1])
						FROM character_array_storage_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testcharacterarraytypmodstorespaddedelementsrepro-0001-select-octet_length-labels[1]-from-character_array_storage_items"},
				},
				{
					Query: `SELECT labels[1] = 'ab '::CHARACTER(3)
						FROM character_array_storage_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testcharacterarraytypmodstorespaddedelementsrepro-0002-select-labels[1]-=-ab-::character"},
				},
			},
		},
	})
}

// TestVarcharArrayTypmodSupportsEqualityRepro reproduces an array correctness
// bug: PostgreSQL supports equality comparisons for arrays whose element type
// has a varchar typmod.
func TestVarcharArrayTypmodSupportsEqualityRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "varchar array typmod supports equality",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT ARRAY['abc']::varchar(3)[] = ARRAY['abc']::varchar(3)[];`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testvarchararraytypmodsupportsequalityrepro-0001-select-array[-abc-]::varchar-3"},
				},
			},
		},
	})
}

// TestNumericArrayTypmodSupportsEqualityRepro reproduces an array correctness
// bug: PostgreSQL supports equality comparisons for arrays whose element type
// has a numeric typmod.
func TestNumericArrayTypmodSupportsEqualityRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "numeric array typmod supports equality",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT ARRAY[1.23]::numeric(5,2)[] = ARRAY[1.23]::numeric(5,2)[];`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testnumericarraytypmodsupportsequalityrepro-0001-select-array[1.23]::numeric-5-2-[]"},
				},
			},
		},
	})
}

// TestTypmodArrayContainmentOperatorsGuard guards that PostgreSQL containment
// and overlap operators work for arrays whose element type has a typmod.
func TestTypmodArrayContainmentOperatorsGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "typmod array containment operators",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT ARRAY['abc']::varchar(3)[] @> ARRAY['abc']::varchar(3)[],
							ARRAY['a']::character(3)[] @> ARRAY['a  ']::character(3)[],
							ARRAY[1.23]::numeric(5,2)[] && ARRAY[1.23]::numeric(5,2)[];`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testtypmodarraycontainmentoperatorsguard-0001-select-array[-abc-]::varchar-3"},
				},
			},
		},
	})
}

// TestTypmodArrayDistinctUsesElementEqualityRepro reproduces an array
// correctness bug: PostgreSQL DISTINCT semantics for arrays whose element type
// has a typmod use the same equality semantics as the element type.
func TestTypmodArrayDistinctUsesElementEqualityRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "character typmod array distinct uses element equality",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT count(DISTINCT labels)
						FROM (VALUES
							(ARRAY['a']::character(3)[]),
							(ARRAY['a  ']::character(3)[])
						) AS v(labels);`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testtypmodarraydistinctuseselementequalityrepro-0001-select-count-distinct-labels-from"},
				},
			},
		},
	})
}

// TestVarcharArrayDistinctUsesElementEqualityRepro reproduces an array
// correctness bug: PostgreSQL DISTINCT semantics work for varchar(n) arrays.
func TestVarcharArrayDistinctUsesElementEqualityRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "varchar typmod array distinct uses element equality",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT count(DISTINCT labels)
						FROM (VALUES
							(ARRAY['abc']::varchar(3)[]),
							(ARRAY['abc']::varchar(3)[])
						) AS v(labels);`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testvarchararraydistinctuseselementequalityrepro-0001-select-count-distinct-labels-from"},
				},
			},
		},
	})
}

// TestNumericArrayDistinctUsesElementEqualityRepro reproduces an array
// correctness bug: PostgreSQL DISTINCT semantics work for numeric(p,s) arrays.
func TestNumericArrayDistinctUsesElementEqualityRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "numeric typmod array distinct uses element equality",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT count(DISTINCT labels)
						FROM (VALUES
							(ARRAY[1.23]::numeric(5,2)[]),
							(ARRAY[1.23]::numeric(5,2)[])
						) AS v(labels);`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testnumericarraydistinctuseselementequalityrepro-0001-select-count-distinct-labels-from"},
				},
			},
		},
	})
}

// TestTypmodArrayGroupByUsesElementEqualityGuard guards that PostgreSQL GROUP
// BY semantics work for arrays whose element type has a typmod.
func TestTypmodArrayGroupByUsesElementEqualityGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "typmod array group by uses element equality",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT count(*)
						FROM (VALUES
							(ARRAY['abc']::varchar(3)[]),
							(ARRAY['abc']::varchar(3)[])
						) AS v(labels)
						GROUP BY labels;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testtypmodarraygroupbyuseselementequalityguard-0001-select-count-*-from-values"},
				},
			},
		},
	})
}

// TestVarcharArrayTypmodWherePredicateUsesElementEqualityRepro reproduces an
// array correctness bug: PostgreSQL can compare stored arrays whose element type
// has a varchar typmod in WHERE predicates.
func TestVarcharArrayTypmodWherePredicateUsesElementEqualityRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "varchar typmod array WHERE predicate uses element equality",
			SetUpScript: []string{
				`CREATE TABLE varchar_array_where_items (
					id INT PRIMARY KEY,
					labels VARCHAR(3)[]
				);`,
				`INSERT INTO varchar_array_where_items VALUES
					(1, ARRAY['abc']::varchar(3)[]),
					(2, ARRAY['def']::varchar(3)[]);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id
						FROM varchar_array_where_items
						WHERE labels = ARRAY['abc']::varchar(3)[];`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testvarchararraytypmodwherepredicateuseselementequalityrepro-0001-select-id-from-varchar_array_where_items-where"},
				},
			},
		},
	})
}

// TestVarcharArrayTypmodOrderByUsesElementOrderingGuard guards that PostgreSQL
// can order stored arrays whose element type has a varchar typmod using the
// element type's ordering semantics.
func TestVarcharArrayTypmodOrderByUsesElementOrderingGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "varchar typmod array ORDER BY uses element ordering",
			SetUpScript: []string{
				`CREATE TABLE varchar_array_order_items (
					id INT PRIMARY KEY,
					labels VARCHAR(3)[]
				);`,
				`INSERT INTO varchar_array_order_items VALUES
					(1, ARRAY['bbb']::varchar(3)[]),
					(2, ARRAY['aaa']::varchar(3)[]);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, labels
						FROM varchar_array_order_items
						ORDER BY labels;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testvarchararraytypmodorderbyuseselementorderingguard-0001-select-id-labels-from-varchar_array_order_items"},
				},
			},
		},
	})
}

// TestCharacterArrayTypmodUniqueUsesPaddingEqualityGuard guards that UNIQUE
// constraints on character(n) arrays reject values that differ only by element
// padding spaces.
func TestCharacterArrayTypmodUniqueUsesPaddingEqualityGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "character array typmod ignores trailing spaces for uniqueness",
			SetUpScript: []string{
				`CREATE TABLE character_array_unique_padding_items (
					id INT PRIMARY KEY,
					labels CHARACTER(3)[] UNIQUE
				);`,
				`INSERT INTO character_array_unique_padding_items
					VALUES (1, ARRAY['a']::character(3)[]);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO character_array_unique_padding_items
						VALUES (2, ARRAY['a  ']::character(3)[]);`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testcharacterarraytypmoduniqueusespaddingequalityguard-0001-insert-into-character_array_unique_padding_items-values-2",

						// TestCharacterTypmodExplicitCastTruncatesGuard guards PostgreSQL's explicit
						// character(n) cast path: explicit casts may truncate to the declared length.
						Compare: "sqlstate"},
				},
				{
					Query: `SELECT count(*) FROM character_array_unique_padding_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testcharacterarraytypmoduniqueusespaddingequalityguard-0002-select-count-*-from-character_array_unique_padding_items"},
				},
			},
		},
	})
}

func TestCharacterTypmodExplicitCastTruncatesGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "character typmod explicit cast truncates",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT 'abcd'::character(3);`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testcharactertypmodexplicitcasttruncatesguard-0001-select-abcd-::character-3"},
				},
			},
		},
	})
}

// TestCharacterTypmodLiteralCastPadsToDeclaredLengthRepro reproduces a
// character(n) cast correctness bug: PostgreSQL pads shorter literal values to
// the declared fixed width.
func TestCharacterTypmodLiteralCastPadsToDeclaredLengthRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "character typmod literal cast pads to declared length",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT octet_length(CAST('ab' AS CHARACTER(3)));`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testcharactertypmodliteralcastpadstodeclaredlengthrepro-0001-select-octet_length-cast-ab-as"},
				},
				{
					Query: `SELECT CAST('ab' AS CHARACTER(3)) = 'ab '::CHARACTER(3);`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testcharactertypmodliteralcastpadstodeclaredlengthrepro-0002-select-cast-ab-as-character"},
				},
			},
		},
	})
}

// TestCharacterTypmodColumnCastPadsToDeclaredLengthRepro reproduces a
// character(n) cast correctness bug: PostgreSQL pads shorter values cast from
// columns to the declared fixed width.
func TestCharacterTypmodColumnCastPadsToDeclaredLengthRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "character typmod column cast pads to declared length",
			SetUpScript: []string{
				`CREATE TABLE character_column_cast_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO character_column_cast_items VALUES (1, 'ab');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT octet_length(CAST(label AS CHARACTER(3)))
						FROM character_column_cast_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testcharactertypmodcolumncastpadstodeclaredlengthrepro-0001-select-octet_length-cast-label-as"},
				},
			},
		},
	})
}

// TestNumericTypmodRoundsAndRejectsOverflowGuard guards PostgreSQL numeric(p,s)
// storage semantics: values round to the declared scale, but values that cannot
// fit the declared precision are rejected.
func TestNumericTypmodRoundsAndRejectsOverflowGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "numeric typmod rounds and rejects overflow",
			SetUpScript: []string{
				`CREATE TABLE numeric_typmod_items (
					id INT PRIMARY KEY,
					amount NUMERIC(5,2)
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO numeric_typmod_items VALUES (1, 123.456);`,
				},
				{
					Query: `INSERT INTO numeric_typmod_items VALUES (2, 1000.00);`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testnumerictypmodroundsandrejectsoverflowguard-0001-insert-into-numeric_typmod_items-values-2", Compare: "sqlstate"},
				},
				{
					Query: `SELECT id, amount FROM numeric_typmod_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testnumerictypmodroundsandrejectsoverflowguard-0002-select-id-amount-from-numeric_typmod_items"},
				},
			},
		},
	})
}

// TestNumericTypmodRejectsOverflowAfterRoundingGuard guards that a value that
// rounds outside the declared precision is rejected rather than stored.
func TestNumericTypmodRejectsOverflowAfterRoundingGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "numeric typmod rejects overflow after rounding",
			SetUpScript: []string{
				`CREATE TABLE numeric_round_overflow_items (
					id INT PRIMARY KEY,
					amount NUMERIC(5,2)
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO numeric_round_overflow_items VALUES (1, 999.995);`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testnumerictypmodrejectsoverflowafterroundingguard-0001-insert-into-numeric_round_overflow_items-values-1", Compare: "sqlstate"},
				},
				{
					Query: `SELECT count(*) FROM numeric_round_overflow_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testnumerictypmodrejectsoverflowafterroundingguard-0002-select-count-*-from-numeric_round_overflow_items"},
				},
			},
		},
	})
}

// TestNumericArrayTypmodsRoundStoredElementsGuard guards numeric array storage
// semantics: PostgreSQL applies the declared element typmod to every stored
// numeric array element.
func TestNumericArrayTypmodsRoundStoredElementsGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "numeric array typmods round stored elements",
			SetUpScript: []string{
				`CREATE TABLE numeric_array_typmod_items (
					id INT PRIMARY KEY,
					amounts NUMERIC(5,2)[]
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO numeric_array_typmod_items VALUES
						(1, ARRAY[123.456, 7.891]::numeric[]);`,
				},
				{
					Query: `SELECT amounts::text
						FROM numeric_array_typmod_items
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testnumericarraytypmodsroundstoredelementsguard-0001-select-amounts::text-from-numeric_array_typmod_items-order"},
				},
				{
					Query: `INSERT INTO numeric_array_typmod_items VALUES (2, ARRAY[999.995]::numeric[]);`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testnumericarraytypmodsroundstoredelementsguard-0002-insert-into-numeric_array_typmod_items-values-2",

						// TestNumericArrayAppendValidatesElementTypmodRepro reproduces an array
						// persistence bug: array mutation results assigned into numeric(p,s)[] columns
						// must validate the appended element against the column's element typmod.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestNumericArrayAppendValidatesElementTypmodRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "numeric array_append validates element typmod",
			SetUpScript: []string{
				`CREATE TABLE numeric_array_append_typmod_items (
					id INT PRIMARY KEY,
					amounts NUMERIC(5,2)[]
				);`,
				`INSERT INTO numeric_array_append_typmod_items
					VALUES (1, ARRAY[1.23]::numeric(5,2)[]);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE numeric_array_append_typmod_items
						SET amounts = array_append(amounts, 999.995)
						WHERE id = 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testnumericarrayappendvalidateselementtypmodrepro-0001-update-numeric_array_append_typmod_items-set-amounts-=", Compare: "sqlstate"},
				},
				{
					Query: `SELECT amounts::text
						FROM numeric_array_append_typmod_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testnumericarrayappendvalidateselementtypmodrepro-0002-select-amounts::text-from-numeric_array_append_typmod_items"},
				},
			},
		},
	})
}

// TestNumericArrayPrependValidatesElementTypmodGuard guards that array_prepend
// results assigned into numeric(p,s)[] columns validate the prepended element
// against the column's element typmod.
func TestNumericArrayPrependValidatesElementTypmodGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "numeric array_prepend validates element typmod",
			SetUpScript: []string{
				`CREATE TABLE numeric_array_prepend_typmod_items (
					id INT PRIMARY KEY,
					amounts NUMERIC(5,2)[]
				);`,
				`INSERT INTO numeric_array_prepend_typmod_items
					VALUES (1, ARRAY[1.23]::numeric(5,2)[]);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE numeric_array_prepend_typmod_items
						SET amounts = array_prepend(999.995, amounts)
						WHERE id = 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testnumericarrayprependvalidateselementtypmodguard-0001-update-numeric_array_prepend_typmod_items-set-amounts-=", Compare: "sqlstate"},
				},
				{
					Query: `SELECT amounts::text
						FROM numeric_array_prepend_typmod_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testnumericarrayprependvalidateselementtypmodguard-0002-select-amounts::text-from-numeric_array_prepend_typmod_items"},
				},
			},
		},
	})
}

// TestNumericArrayCatValidatesElementTypmodRepro reproduces an array
// persistence bug: array_cat results assigned into numeric(p,s)[] columns must
// validate the concatenated elements against the column's element typmod.
func TestNumericArrayCatValidatesElementTypmodRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "numeric array_cat validates element typmod",
			SetUpScript: []string{
				`CREATE TABLE numeric_array_cat_typmod_items (
					id INT PRIMARY KEY,
					amounts NUMERIC(5,2)[]
				);`,
				`INSERT INTO numeric_array_cat_typmod_items
					VALUES (1, ARRAY[1.23]::numeric(5,2)[]);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE numeric_array_cat_typmod_items
						SET amounts = array_cat(amounts, ARRAY[999.995])
						WHERE id = 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testnumericarraycatvalidateselementtypmodrepro-0001-update-numeric_array_cat_typmod_items-set-amounts-=", Compare: "sqlstate"},
				},
				{
					Query: `SELECT amounts::text
						FROM numeric_array_cat_typmod_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testnumericarraycatvalidateselementtypmodrepro-0002-select-amounts::text-from-numeric_array_cat_typmod_items"},
				},
			},
		},
	})
}

// TestNumericNegativeScaleRoundsStoredValuesRepro reproduces a numeric storage
// correctness bug: PostgreSQL supports negative numeric scales, which round
// assigned values to the left of the decimal point before enforcing precision.
func TestNumericNegativeScaleRoundsStoredValuesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "numeric negative scale rounds stored values",
			SetUpScript: []string{
				`CREATE TABLE numeric_negative_scale_items (
					id INT PRIMARY KEY,
					amount NUMERIC(2, -3)
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO numeric_negative_scale_items VALUES
						(1, 12345),
						(2, 99499);`,
				},
				{
					Query: `INSERT INTO numeric_negative_scale_items VALUES (3, 99500);`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testnumericnegativescaleroundsstoredvaluesrepro-0001-insert-into-numeric_negative_scale_items-values-3", Compare: "sqlstate"},
				},
				{
					Query: `SELECT id, amount::text
						FROM numeric_negative_scale_items
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testnumericnegativescaleroundsstoredvaluesrepro-0002-select-id-amount::text-from-numeric_negative_scale_items"},
				},
			},
		},
	})
}

// TestNumericScaleGreaterThanPrecisionRoundsStoredValuesRepro reproduces a
// numeric storage correctness bug: PostgreSQL supports numeric typmods whose
// scale is greater than precision, for fractional values close to zero.
func TestNumericScaleGreaterThanPrecisionRoundsStoredValuesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "numeric scale greater than precision rounds stored values",
			SetUpScript: []string{
				`CREATE TABLE numeric_large_scale_items (
					id INT PRIMARY KEY,
					amount NUMERIC(3, 5)
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO numeric_large_scale_items VALUES
						(1, 0.001234),
						(2, 0.009994);`,
				},
				{
					Query: `INSERT INTO numeric_large_scale_items VALUES (3, 0.09999);`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testnumericscalegreaterthanprecisionroundsstoredvaluesrepro-0001-insert-into-numeric_large_scale_items-values-3", Compare: "sqlstate"},
				},
				{
					Query: `SELECT id, amount::text
						FROM numeric_large_scale_items
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testnumericscalegreaterthanprecisionroundsstoredvaluesrepro-0002-select-id-amount::text-from-numeric_large_scale_items"},
				},
			},
		},
	})
}

// TestNumericSpecialValuesRoundTripRepro reproduces a numeric storage
// correctness bug: PostgreSQL numeric columns can store NaN and infinity
// values and compare them with PostgreSQL's numeric ordering semantics.
func TestNumericSpecialValuesRoundTripRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "numeric special values round trip",
			SetUpScript: []string{
				`CREATE TABLE numeric_special_value_items (
					id INT PRIMARY KEY,
					amount NUMERIC
				);`,
				`INSERT INTO numeric_special_value_items VALUES
					(1, 'NaN'),
					(2, 'Infinity'),
					(3, '-Infinity'),
					(4, 1.5);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, amount::text
						FROM numeric_special_value_items
						ORDER BY amount;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testnumericspecialvaluesroundtriprepro-0001-select-id-amount::text-from-numeric_special_value_items"},
				},
				{
					Query: `SELECT 'NaN'::numeric = 'NaN'::numeric,
							'Infinity'::numeric > 999999999999999999999999999999::numeric,
							'-Infinity'::numeric < -999999999999999999999999999999::numeric;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testnumericspecialvaluesroundtriprepro-0002-select-nan-::numeric-=-nan"},
				},
			},
		},
	})
}

// TestMultidimensionalArrayColumnRoundTripRepro reproduces a type persistence
// bug: PostgreSQL supports storing and reading multidimensional arrays.
func TestMultidimensionalArrayColumnRoundTripRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "multidimensional array column round trip",
			SetUpScript: []string{
				`CREATE TABLE multidimensional_array_items (
					id INT PRIMARY KEY,
					labels VARCHAR[][]
				);`,
				`INSERT INTO multidimensional_array_items VALUES
					(1, ARRAY[['abc', 'def'], ['ghi', 'jkl']]);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT labels
						FROM multidimensional_array_items
						WHERE id = 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testmultidimensionalarraycolumnroundtriprepro-0001-select-labels-from-multidimensional_array_items-where"},
				},
			},
		},
	})
}

// TestArrayLiteralPreservesLowerBoundsRepro reproduces an array persistence
// bug: PostgreSQL arrays preserve explicit lower bounds, and subscripting uses
// those stored bounds.
func TestArrayLiteralPreservesLowerBoundsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "array literal preserves lower bounds",
			SetUpScript: []string{
				`CREATE TABLE array_lower_bound_items (
					id INT PRIMARY KEY,
					values_int INT[]
				);`,
				`INSERT INTO array_lower_bound_items VALUES (1, '[0:2]={10,20,30}'::int[]);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT values_int[0], values_int[1], values_int[2],
							array_upper(values_int, 1)
						FROM array_lower_bound_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testarrayliteralpreserveslowerboundsrepro-0001-select-values_int[0]-values_int[1]-values_int[2]-array_upper"},
				},
			},
		},
	})
}

// TestArrayLowerReportsDefaultLowerBoundRepro reproduces an array metadata
// correctness bug: PostgreSQL exposes the lower bound of array dimensions via
// array_lower.
func TestArrayLowerReportsDefaultLowerBoundRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "array_lower reports default lower bound",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT array_lower(ARRAY[10,20,30], 1);`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testarraylowerreportsdefaultlowerboundrepro-0001-select-array_lower-array[10-20-30]"},
				},
				{
					Query: `SELECT array_lower(ARRAY[10,20,30], 2);`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testarraylowerreportsdefaultlowerboundrepro-0002-select-array_lower-array[10-20-30]"},
				},
			},
		},
	})
}

// TestArrayDimsReportsDimensionsRepro reproduces an array metadata correctness
// bug: PostgreSQL exposes array dimension bounds via array_dims.
func TestArrayDimsReportsDimensionsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "array_dims reports dimensions",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT array_dims(ARRAY[10,20,30]);`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testarraydimsreportsdimensionsrepro-0001-select-array_dims-array[10-20-30]"},
				},
				{
					Query: `SELECT array_dims(ARRAY[[1,2],[3,4]]);`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testarraydimsreportsdimensionsrepro-0002-select-array_dims-array[[1-2]-[3"},
				},
			},
		},
	})
}

// TestNestedArrayConstructorReportsDimensionsRepro reproduces an array
// correctness bug: PostgreSQL supports multidimensional array constructors and
// exposes their dimensions through array_length and array_upper.
func TestNestedArrayConstructorReportsDimensionsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "nested array constructor reports dimensions",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT array_length(ARRAY[[1,2],[3,4]], 1),
							array_length(ARRAY[[1,2],[3,4]], 2),
							array_upper(ARRAY[[1,2],[3,4]], 1),
							array_upper(ARRAY[[1,2],[3,4]], 2);`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testnestedarrayconstructorreportsdimensionsrepro-0001-select-array_length-array[[1-2]-[3"},
				},
			},
		},
	})
}

// TestArrayNdimsReportsDimensionCountRepro reproduces an array metadata
// correctness bug: PostgreSQL exposes the number of array dimensions through
// array_ndims.
func TestArrayNdimsReportsDimensionCountRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "array_ndims reports dimension count",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT array_ndims(ARRAY[10,20,30]),
							array_ndims(ARRAY[]::int[]),
							array_ndims(NULL::int[]);`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testarrayndimsreportsdimensioncountrepro-0001-select-array_ndims-array[10-20-30]"},
				},
			},
		},
	})
}

// TestCardinalityCountsArrayElementsRepro reproduces an array metadata
// correctness bug: PostgreSQL exposes the total number of array elements
// through cardinality.
func TestCardinalityCountsArrayElementsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "cardinality counts array elements",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT cardinality(NULL::int[]),
							cardinality('{}'::int[]),
							cardinality(ARRAY[10,20,30]);`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testcardinalitycountsarrayelementsrepro-0001-select-cardinality-null::int[]-cardinality-{}"},
				},
			},
		},
	})
}

// TestArrayPositionFindsNullElementsGuard guards PostgreSQL's array_position
// and array_positions NULL-search semantics: comparisons use IS NOT DISTINCT
// FROM semantics, so searching for NULL finds NULL elements.
func TestArrayPositionFindsNullElementsGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "array_position finds null elements",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT array_position(ARRAY[1,NULL,3,NULL], NULL),
							array_positions(ARRAY[1,NULL,3,NULL], NULL);`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testarraypositionfindsnullelementsguard-0001-select-array_position-array[1-null-3"},
				},
			},
		},
	})
}

// TestArraySubscriptAssignmentPersistsElementRepro reproduces an array
// persistence bug: PostgreSQL supports updating one element of a stored array
// with subscript assignment.
func TestArraySubscriptAssignmentPersistsElementRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "array subscript assignment persists element",
			SetUpScript: []string{
				`CREATE TABLE array_subscript_assignment_items (
					id INT PRIMARY KEY,
					values_int INT[]
				);`,
				`INSERT INTO array_subscript_assignment_items VALUES (1, ARRAY[1, 2, 3]);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE array_subscript_assignment_items
						SET values_int[2] = 22
						WHERE id = 1;`,
				},
				{
					Query: `SELECT values_int::text
						FROM array_subscript_assignment_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testarraysubscriptassignmentpersistselementrepro-0001-select-values_int::text-from-array_subscript_assignment_items"},
				},
			},
		},
	})
}

// TestStringToArraySplitsTextRepro reproduces an array construction
// correctness bug: PostgreSQL supports string_to_array, including NULL
// delimiters and NULL-token replacement.
func TestStringToArraySplitsTextRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "string_to_array splits text",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT string_to_array('1|2|3', '|'),
							string_to_array('abc', NULL),
							string_to_array('1,*,3', ',', '*');`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-teststringtoarraysplitstextrepro-0001-select-string_to_array-1|2|3-|-string_to_array"},
				},
			},
		},
	})
}

// TestArrayToStringNullArrayWithNullReplacementRepro reproduces an array
// conversion correctness bug: PostgreSQL returns NULL when array_to_string is
// called with a NULL array, even when a NULL replacement argument is supplied.
func TestArrayToStringNullArrayWithNullReplacementRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "array_to_string handles null array with null replacement",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT array_to_string(NULL::int[], ',', '*') IS NULL;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testarraytostringnullarraywithnullreplacementrepro-0001-select-array_to_string-null::int[]-*-is"},
				},
			},
		},
	})
}

// TestArrayRemoveRejectsMultidimensionalArraysRepro reproduces an array
// correctness bug: PostgreSQL parses multidimensional array literals and then
// reports that array_remove only supports one-dimensional arrays.
func TestArrayRemoveRejectsMultidimensionalArraysRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "array_remove rejects multidimensional arrays",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT array_remove('{{1,2,2},{1,4,3}}'::int[], 2);`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testarrayremoverejectsmultidimensionalarraysrepro-0001-select-array_remove-{{1-2-2}",

						// TestArrayReplaceReplacesMatchingElementsRepro reproduces an array
						// correctness bug: PostgreSQL supports array_replace for replacing all
						// matching elements, including NULL matches.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestArrayReplaceReplacesMatchingElementsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "array_replace replaces matching elements",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT array_replace(ARRAY[1,2,1], 1, 9),
							array_replace(ARRAY[1,NULL,3,NULL], NULL, 0);`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testarrayreplacereplacesmatchingelementsrepro-0001-select-array_replace-array[1-2-1]"},
				},
				{
					Query: `SELECT array_replace(ARRAY[1,2,3], 2, NULL),
							array_replace(ARRAY[1,2,3], 9, 0);`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testarrayreplacereplacesmatchingelementsrepro-0002-select-array_replace-array[1-2-3]"},
				},
			},
		},
	})
}

// TestArrayFillConstructsArraysRepro reproduces an array correctness bug:
// PostgreSQL supports array_fill for constructing arrays of a requested shape.
func TestArrayFillConstructsArraysRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "array_fill constructs arrays",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT array_fill(7, ARRAY[3]);`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testarrayfillconstructsarraysrepro-0001-select-array_fill-7-array[3]"},
				},
				{
					Query: `SELECT array_fill(NULL::int, ARRAY[2]);`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testarrayfillconstructsarraysrepro-0002-select-array_fill-null::int-array[2]"},
				},
				{
					Query: `SELECT array_fill(7, ARRAY[0]);`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testarrayfillconstructsarraysrepro-0003-select-array_fill-7-array[0]"},
				},
				{
					Query: `SELECT array_fill(7, ARRAY[-1]);`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testarrayfillconstructsarraysrepro-0004-select-array_fill-7-array[-1]",

						// TestTrimArrayRemovesTrailingElementsRepro reproduces an array correctness
						// bug: PostgreSQL supports trim_array for removing elements from the end of an
						// array.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestTrimArrayRemovesTrailingElementsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "trim_array removes trailing elements",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT trim_array(ARRAY[1,2,3,4], 2);`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testtrimarrayremovestrailingelementsrepro-0001-select-trim_array-array[1-2-3"},
				},
				{
					Query: `SELECT trim_array(ARRAY[1,2,3,4], 0),
							trim_array(ARRAY[1,2,3,4], 4);`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testtrimarrayremovestrailingelementsrepro-0002-select-trim_array-array[1-2-3"},
				},
				{
					Query: `SELECT trim_array(ARRAY[1,2,3,4], -1);`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testtrimarrayremovestrailingelementsrepro-0003-select-trim_array-array[1-2-3", Compare: "sqlstate"},
				},
				{
					Query: `SELECT trim_array(ARRAY[1,2,3,4], 5);`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testtrimarrayremovestrailingelementsrepro-0004-select-trim_array-array[1-2-3",

						// TestBuiltinRangeTypesRoundTripRepro reproduces a type persistence bug:
						// PostgreSQL supports storing and reading built-in range values.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestBuiltinRangeTypesRoundTripRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "built-in range types round trip",
			SetUpScript: []string{
				`CREATE TABLE range_round_trip_items (
					id INT PRIMARY KEY,
					int_span int4range,
					num_span numrange,
					date_span daterange
				);`,
				`INSERT INTO range_round_trip_items VALUES
					(1, '[1,3)'::int4range, '[1.5,3.5)'::numrange, '[2026-01-01,2026-02-01)'::daterange);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT int_span::text, num_span::text, date_span::text
						FROM range_round_trip_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testbuiltinrangetypesroundtriprepro-0001-select-int_span::text-num_span::text-date_span::text-from"},
				},
			},
		},
	})
}

// TestBuiltinMultirangeTypesRoundTripRepro reproduces a type persistence bug:
// PostgreSQL supports storing and reading built-in multirange values.
func TestBuiltinMultirangeTypesRoundTripRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "built-in multirange types round trip",
			SetUpScript: []string{
				`CREATE TABLE multirange_round_trip_items (
					id INT PRIMARY KEY,
					int_spans int4multirange,
					num_spans nummultirange
				);`,
				`INSERT INTO multirange_round_trip_items VALUES
					(1, '{[1,3),[5,7)}'::int4multirange, '{[1.5,3.5)}'::nummultirange);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT int_spans::text, num_spans::text
						FROM multirange_round_trip_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testbuiltinmultirangetypesroundtriprepro-0001-select-int_spans::text-num_spans::text-from-multirange_round_trip_items"},
				},
			},
		},
	})
}

// TestCommonBuiltinTypesRoundTripRepro reproduces type persistence bugs:
// PostgreSQL supports round trips for common network and geometric scalar
// types.
func TestCommonBuiltinTypesRoundTripRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "cidr column round trip",
			SetUpScript: []string{
				`CREATE TABLE cidr_round_trip_items (
					id INT PRIMARY KEY,
					addr CIDR
				);`,
				`INSERT INTO cidr_round_trip_items VALUES
					(1, '192.168.1.0/24'),
					(2, '10.0.0.0/8');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, addr FROM cidr_round_trip_items ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testcommonbuiltintypesroundtriprepro-0001-select-id-addr-from-cidr_round_trip_items"},
				},
			},
		},
		{
			Name: "inet column round trip",
			SetUpScript: []string{
				`CREATE TABLE inet_round_trip_items (
					id INT PRIMARY KEY,
					addr INET
				);`,
				`INSERT INTO inet_round_trip_items VALUES
					(1, '192.168.1.1'),
					(2, '10.0.0.1');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, addr FROM inet_round_trip_items ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testcommonbuiltintypesroundtriprepro-0002-select-id-addr-from-inet_round_trip_items"},
				},
			},
		},
		{
			Name: "macaddr column round trip",
			SetUpScript: []string{
				`CREATE TABLE macaddr_round_trip_items (
					id INT PRIMARY KEY,
					addr MACADDR
				);`,
				`INSERT INTO macaddr_round_trip_items VALUES
					(1, '08:00:2b:01:02:03'),
					(2, '00:11:22:33:44:55');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, addr FROM macaddr_round_trip_items ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testcommonbuiltintypesroundtriprepro-0003-select-id-addr-from-macaddr_round_trip_items"},
				},
			},
		},
		{
			Name: "money column round trip",
			SetUpScript: []string{
				`CREATE TABLE money_round_trip_items (
					id INT PRIMARY KEY,
					amount MONEY
				);`,
				`INSERT INTO money_round_trip_items VALUES
					(1, '$100.25'),
					(2, '$50.50');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, amount FROM money_round_trip_items ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testcommonbuiltintypesroundtriprepro-0004-select-id-amount-from-money_round_trip_items"},
				},
			},
		},
		{
			Name: "point column round trip",
			SetUpScript: []string{
				`CREATE TABLE point_round_trip_items (
					id INT PRIMARY KEY,
					p POINT
				);`,
				`INSERT INTO point_round_trip_items VALUES
					(1, '(1,2)'),
					(2, '(3,4)');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, p FROM point_round_trip_items ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testcommonbuiltintypesroundtriprepro-0005-select-id-p-from-point_round_trip_items"},
				},
			},
		},
		{
			Name: "box column round trip",
			SetUpScript: []string{
				`CREATE TABLE box_round_trip_items (
					id INT PRIMARY KEY,
					b BOX
				);`,
				`INSERT INTO box_round_trip_items VALUES
					(1, '((1,2),(3,4))'),
					(2, '((5,6),(7,8))');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, b FROM box_round_trip_items ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testcommonbuiltintypesroundtriprepro-0006-select-id-b-from-box_round_trip_items"},
				},
			},
		},
		{
			Name: "circle column round trip",
			SetUpScript: []string{
				`CREATE TABLE circle_round_trip_items (
					id INT PRIMARY KEY,
					c CIRCLE
				);`,
				`INSERT INTO circle_round_trip_items VALUES
					(1, '<(1,2),3>'),
					(2, '<(4,5),6>');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, c FROM circle_round_trip_items ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testcommonbuiltintypesroundtriprepro-0007-select-id-c-from-circle_round_trip_items"},
				},
			},
		},
		{
			Name: "line column round trip",
			SetUpScript: []string{
				`CREATE TABLE line_round_trip_items (
					id INT PRIMARY KEY,
					ln LINE
				);`,
				`INSERT INTO line_round_trip_items VALUES
					(1, '{1,2,3}'),
					(2, '{4,5,6}');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, ln FROM line_round_trip_items ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testcommonbuiltintypesroundtriprepro-0008-select-id-ln-from-line_round_trip_items"},
				},
			},
		},
		{
			Name: "line segment column round trip",
			SetUpScript: []string{
				`CREATE TABLE lseg_round_trip_items (
					id INT PRIMARY KEY,
					seg LSEG
				);`,
				`INSERT INTO lseg_round_trip_items VALUES
					(1, '((1,2),(3,4))'),
					(2, '((5,6),(7,8))');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, seg FROM lseg_round_trip_items ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testcommonbuiltintypesroundtriprepro-0009-select-id-seg-from-lseg_round_trip_items"},
				},
			},
		},
		{
			Name: "path column round trip",
			SetUpScript: []string{
				`CREATE TABLE path_round_trip_items (
					id INT PRIMARY KEY,
					p PATH
				);`,
				`INSERT INTO path_round_trip_items VALUES
					(1, '((1,2),(3,4),(5,6))'),
					(2, '((7,8),(9,10),(11,12))');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, p FROM path_round_trip_items ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testcommonbuiltintypesroundtriprepro-0010-select-id-p-from-path_round_trip_items"},
				},
			},
		},
		{
			Name: "polygon column round trip",
			SetUpScript: []string{
				`CREATE TABLE polygon_round_trip_items (
					id INT PRIMARY KEY,
					p POLYGON
				);`,
				`INSERT INTO polygon_round_trip_items VALUES
					(1, '((1,2),(3,4),(5,6))'),
					(2, '((7,8),(9,10),(11,12))');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, p FROM polygon_round_trip_items ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testcommonbuiltintypesroundtriprepro-0011-select-id-p-from-polygon_round_trip_items"},
				},
			},
		},
		{
			Name: "xml column round trip",
			SetUpScript: []string{
				`CREATE TABLE xml_round_trip_items (
					id INT PRIMARY KEY,
					doc XML
				);`,
				`INSERT INTO xml_round_trip_items VALUES
					(1, '<doc><title>one</title></doc>'),
					(2, '<doc><title>two</title></doc>');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, doc FROM xml_round_trip_items ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testcommonbuiltintypesroundtriprepro-0012-select-id-doc-from-xml_round_trip_items"},
				},
			},
		},
		{
			Name: "tsquery column round trip",
			SetUpScript: []string{
				`CREATE TABLE tsquery_round_trip_items (
					id INT PRIMARY KEY,
					query TSQUERY
				);`,
				`INSERT INTO tsquery_round_trip_items VALUES
					(1, 'fat & rat'),
					(2, 'cat | dog');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, query FROM tsquery_round_trip_items ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testcommonbuiltintypesroundtriprepro-0013-select-id-query-from-tsquery_round_trip_items"},
				},
			},
		},
		{
			Name: "tsvector column round trip",
			SetUpScript: []string{
				`CREATE TABLE tsvector_round_trip_items (
					id INT PRIMARY KEY,
					doc TSVECTOR
				);`,
				`INSERT INTO tsvector_round_trip_items VALUES
					(1, 'a fat cat sat'),
					(2, 'a dog ran');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, doc FROM tsvector_round_trip_items ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "type-correctness-repro-test-testcommonbuiltintypesroundtriprepro-0014-select-id-doc-from-tsvector_round_trip_items"},
				},
			},
		},
	})
}
