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
						ORDER BY id;`,
					Expected: []sql.Row{
						{1, "abcdefghij"},
						{2, "klmnopqrst"},
					},
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
						WHERE id = E'\\xCAFEBABE';`,
					Expected: []sql.Row{
						{[]byte{0xCA, 0xFE, 0xBA, 0xBE}, []byte{0xDE, 0xAD, 0xBE, 0xEF}},
					},
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
						ORDER BY n.nspname;`,
					Expected: []sql.Row{{"drop_type_schema_b", "same_named_enum"}},
				},
				{
					Query: `SELECT id, status::text
						FROM drop_type_schema_uses_b;`,
					Expected: []sql.Row{{1, "two"}},
				},
			},
		},
	})
}

// TestDropTypeCascadeWithoutDependentsRepro reproduces a DDL correctness bug:
// PostgreSQL accepts CASCADE on DROP TYPE even when no dependent objects need
// to be removed.
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
							AND t.typname = 'drop_type_cascade_unused';`,
					Expected: []sql.Row{},
				},
			},
		},
	})
}

// TestNameTypeRejectsIntegerAssignmentRepro reproduces a type correctness bug:
// PostgreSQL does not implicitly assign integer expressions to name columns.
func TestNameTypeRejectsIntegerAssignmentRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "name column rejects integer assignment",
			SetUpScript: []string{
				`CREATE TABLE name_assignment_items (
					id INT PRIMARY KEY,
					label NAME
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `INSERT INTO name_assignment_items VALUES (1, 12345);`,
					ExpectedErr: `type name`,
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
					Query:       `INSERT INTO xid_input_items VALUES (1, '4294967296');`,
					ExpectedErr: `out of range`,
				},
				{
					Query:       `INSERT INTO xid_input_items VALUES (2, '-1');`,
					ExpectedErr: `invalid input syntax`,
				},
				{
					Query:       `INSERT INTO xid_input_items VALUES (3, 'abc');`,
					ExpectedErr: `invalid input syntax`,
				},
			},
		},
	})
}

// TestXidOrderingRequiresOrderingOperatorRepro reproduces a type correctness
// bug: PostgreSQL rejects ORDER BY xid because xid has no ordering operator.
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
					Query:       `SELECT id, x FROM xid_order_items ORDER BY x;`,
					ExpectedErr: `could not identify an ordering operator for type xid`,
				},
			},
		},
	})
}

// TestInternalCharCastToIntegerUsesSignedByteRepro reproduces a type
// correctness bug: PostgreSQL casts the internal "char" type to integer using
// signed one-byte semantics.
func TestInternalCharCastToIntegerUsesSignedByteRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: `internal "char" casts high-bit bytes to signed integers`,
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT 'こんにちは'::"char"::int;`,
					Expected: []sql.Row{{-29}},
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
					Query:    `SELECT pg_typeof('integer'::regtype)::text, 'integer'::regtype::text;`,
					Expected: []sql.Row{{"regtype", "integer"}},
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
						ORDER BY id;`,
					Expected: []sql.Row{{Numeric("123.45"), Numeric("67.89")}},
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
						ORDER BY id;`,
					Expected: []sql.Row{
						{1, "123.46"},
						{2, "123.46"},
						{3, "123.46"},
						{4, "234.57"},
					},
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
					Query:       `INSERT INTO varchar_typmod_items VALUES (1, 'abcd');`,
					ExpectedErr: `value too long`,
				},
				{
					Query: `INSERT INTO varchar_typmod_items VALUES (2, 'abcd'::varchar(3));`,
				},
				{
					Query:    `SELECT id, label FROM varchar_typmod_items;`,
					Expected: []sql.Row{{2, "abc"}},
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
					Query:    `SELECT id, label, length(label) FROM varchar_trailing_space_items;`,
					Expected: []sql.Row{{1, "abc", 3}},
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
					Query:    `SELECT id, label, length(label) FROM character_trailing_space_items;`,
					Expected: []sql.Row{{1, "abc", 3}},
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
					Query:       `INSERT INTO character_unique_padding_items VALUES (2, 'a  ');`,
					ExpectedErr: `duplicate`,
				},
				{
					Query:    `SELECT id, label = 'a  '::character(3) FROM character_unique_padding_items;`,
					Expected: []sql.Row{{1, "t"}},
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
						FROM character_storage_items;`,
					Expected: []sql.Row{{int64(3)}},
				},
				{
					Query: `SELECT label = 'ab '::CHARACTER(3)
						FROM character_storage_items;`,
					Expected: []sql.Row{{"t"}},
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
					Query:    `SELECT id, labels FROM varchar_array_trailing_space_items;`,
					Expected: []sql.Row{{1, "{abc}"}},
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
						WHERE id = 1;`,
					ExpectedErr: `value too long`,
				},
				{
					Query: `SELECT labels::text
						FROM varchar_array_append_typmod_items;`,
					Expected: []sql.Row{{"{abc}"}},
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
					Query:    `SELECT ARRAY['a']::character(3)[] = ARRAY['a  ']::character(3)[];`,
					Expected: []sql.Row{{"t"}},
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
						FROM character_array_storage_items;`,
					Expected: []sql.Row{{int64(3)}},
				},
				{
					Query: `SELECT labels[1] = 'ab '::CHARACTER(3)
						FROM character_array_storage_items;`,
					Expected: []sql.Row{{"t"}},
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
					Query:    `SELECT ARRAY['abc']::varchar(3)[] = ARRAY['abc']::varchar(3)[];`,
					Expected: []sql.Row{{"t"}},
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
					Query:    `SELECT ARRAY[1.23]::numeric(5,2)[] = ARRAY[1.23]::numeric(5,2)[];`,
					Expected: []sql.Row{{"t"}},
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
							ARRAY[1.23]::numeric(5,2)[] && ARRAY[1.23]::numeric(5,2)[];`,
					Expected: []sql.Row{{"t", "t", "t"}},
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
						) AS v(labels);`,
					Expected: []sql.Row{{int64(1)}},
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
						) AS v(labels);`,
					Expected: []sql.Row{{int64(1)}},
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
						) AS v(labels);`,
					Expected: []sql.Row{{int64(1)}},
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
						GROUP BY labels;`,
					Expected: []sql.Row{{int64(2)}},
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
						WHERE labels = ARRAY['abc']::varchar(3)[];`,
					Expected: []sql.Row{{1}},
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
						ORDER BY labels;`,
					Expected: []sql.Row{
						{2, "{aaa}"},
						{1, "{bbb}"},
					},
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
						VALUES (2, ARRAY['a  ']::character(3)[]);`,
					ExpectedErr: `duplicate`,
				},
				{
					Query:    `SELECT count(*) FROM character_array_unique_padding_items;`,
					Expected: []sql.Row{{int64(1)}},
				},
			},
		},
	})
}

// TestCharacterTypmodExplicitCastTruncatesGuard guards PostgreSQL's explicit
// character(n) cast path: explicit casts may truncate to the declared length.
func TestCharacterTypmodExplicitCastTruncatesGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "character typmod explicit cast truncates",
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT 'abcd'::character(3);`,
					Expected: []sql.Row{{"abc"}},
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
					Query:    `SELECT octet_length(CAST('ab' AS CHARACTER(3)));`,
					Expected: []sql.Row{{int64(3)}},
				},
				{
					Query:    `SELECT CAST('ab' AS CHARACTER(3)) = 'ab '::CHARACTER(3);`,
					Expected: []sql.Row{{"t"}},
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
						FROM character_column_cast_items;`,
					Expected: []sql.Row{{int64(3)}},
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
					Query:       `INSERT INTO numeric_typmod_items VALUES (2, 1000.00);`,
					ExpectedErr: `numeric field overflow`,
				},
				{
					Query:    `SELECT id, amount FROM numeric_typmod_items;`,
					Expected: []sql.Row{{1, Numeric("123.46")}},
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
					Query:       `INSERT INTO numeric_round_overflow_items VALUES (1, 999.995);`,
					ExpectedErr: `numeric field overflow`,
				},
				{
					Query:    `SELECT count(*) FROM numeric_round_overflow_items;`,
					Expected: []sql.Row{{int64(0)}},
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
						ORDER BY id;`,
					Expected: []sql.Row{{"{123.46,7.89}"}},
				},
				{
					Query:       `INSERT INTO numeric_array_typmod_items VALUES (2, ARRAY[999.995]::numeric[]);`,
					ExpectedErr: `numeric field overflow`,
				},
			},
		},
	})
}

// TestNumericArrayAppendValidatesElementTypmodRepro reproduces an array
// persistence bug: array mutation results assigned into numeric(p,s)[] columns
// must validate the appended element against the column's element typmod.
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
						WHERE id = 1;`,
					ExpectedErr: `numeric field overflow`,
				},
				{
					Query: `SELECT amounts::text
						FROM numeric_array_append_typmod_items;`,
					Expected: []sql.Row{{"{1.23}"}},
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
					Query:       `INSERT INTO numeric_negative_scale_items VALUES (3, 99500);`,
					ExpectedErr: `numeric field overflow`,
				},
				{
					Query: `SELECT id, amount::text
						FROM numeric_negative_scale_items
						ORDER BY id;`,
					Expected: []sql.Row{
						{1, "12000"},
						{2, "99000"},
					},
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
					Query:       `INSERT INTO numeric_large_scale_items VALUES (3, 0.09999);`,
					ExpectedErr: `numeric field overflow`,
				},
				{
					Query: `SELECT id, amount::text
						FROM numeric_large_scale_items
						ORDER BY id;`,
					Expected: []sql.Row{
						{1, "0.00123"},
						{2, "0.00999"},
					},
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
						ORDER BY amount;`,
					Expected: []sql.Row{
						{3, "-Infinity"},
						{4, "1.5"},
						{2, "Infinity"},
						{1, "NaN"},
					},
				},
				{
					Query: `SELECT 'NaN'::numeric = 'NaN'::numeric,
							'Infinity'::numeric > 999999999999999999999999999999::numeric,
							'-Infinity'::numeric < -999999999999999999999999999999::numeric;`,
					Expected: []sql.Row{{"t", "t", "t"}},
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
						WHERE id = 1;`,
					Expected: []sql.Row{{"{{abc,def},{ghi,jkl}}"}},
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
						FROM array_lower_bound_items;`,
					Expected: []sql.Row{{10, 20, 30, 2}},
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
					Query:    `SELECT array_lower(ARRAY[10,20,30], 1);`,
					Expected: []sql.Row{{1}},
				},
				{
					Query:    `SELECT array_lower(ARRAY[10,20,30], 2);`,
					Expected: []sql.Row{{nil}},
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
					Query:    `SELECT array_dims(ARRAY[10,20,30]);`,
					Expected: []sql.Row{{"[1:3]"}},
				},
				{
					Query:    `SELECT array_dims(ARRAY[[1,2],[3,4]]);`,
					Expected: []sql.Row{{"[1:2][1:2]"}},
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
							array_upper(ARRAY[[1,2],[3,4]], 2);`,
					Expected: []sql.Row{{2, 2, 2, 2}},
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
							array_ndims(NULL::int[]);`,
					Expected: []sql.Row{{1, nil, nil}},
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
							cardinality(ARRAY[10,20,30]);`,
					Expected: []sql.Row{{nil, 0, 3}},
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
							array_positions(ARRAY[1,NULL,3,NULL], NULL);`,
					Expected: []sql.Row{{2, "{2,4}"}},
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
							string_to_array('1,*,3', ',', '*');`,
					Expected: []sql.Row{{"{1,2,3}", "{a,b,c}", "{1,NULL,3}"}},
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
					Query:    `SELECT array_to_string(NULL::int[], ',', '*') IS NULL;`,
					Expected: []sql.Row{{true}},
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
					Query:       `SELECT array_remove('{{1,2,2},{1,4,3}}'::int[], 2);`,
					ExpectedErr: `multidimensional arrays`,
				},
			},
		},
	})
}

// TestArrayReplaceReplacesMatchingElementsRepro reproduces an array
// correctness bug: PostgreSQL supports array_replace for replacing all
// matching elements, including NULL matches.
func TestArrayReplaceReplacesMatchingElementsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "array_replace replaces matching elements",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT array_replace(ARRAY[1,2,1], 1, 9),
							array_replace(ARRAY[1,NULL,3,NULL], NULL, 0);`,
					Expected: []sql.Row{{"{9,2,9}", "{1,0,3,0}"}},
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
					Query:    `SELECT array_fill(7, ARRAY[3]);`,
					Expected: []sql.Row{{"{7,7,7}"}},
				},
			},
		},
	})
}

// TestTrimArrayRemovesTrailingElementsRepro reproduces an array correctness
// bug: PostgreSQL supports trim_array for removing elements from the end of an
// array.
func TestTrimArrayRemovesTrailingElementsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "trim_array removes trailing elements",
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT trim_array(ARRAY[1,2,3,4], 2);`,
					Expected: []sql.Row{{"{1,2}"}},
				},
			},
		},
	})
}

// TestBuiltinRangeTypesRoundTripRepro reproduces a type persistence bug:
// PostgreSQL supports storing and reading built-in range values.
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
						FROM range_round_trip_items;`,
					Expected: []sql.Row{{"[1,3)", "[1.5,3.5)", "[2026-01-01,2026-02-01)"}},
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
						FROM multirange_round_trip_items;`,
					Expected: []sql.Row{{"{[1,3),[5,7)}", "{[1.5,3.5)}"}},
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
					Query: `SELECT id, addr FROM cidr_round_trip_items ORDER BY id;`,
					Expected: []sql.Row{
						{1, "192.168.1.0/24"},
						{2, "10.0.0.0/8"},
					},
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
					Query: `SELECT id, addr FROM inet_round_trip_items ORDER BY id;`,
					Expected: []sql.Row{
						{1, "192.168.1.1"},
						{2, "10.0.0.1"},
					},
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
					Query: `SELECT id, addr FROM macaddr_round_trip_items ORDER BY id;`,
					Expected: []sql.Row{
						{1, "08:00:2b:01:02:03"},
						{2, "00:11:22:33:44:55"},
					},
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
					Query: `SELECT id, amount FROM money_round_trip_items ORDER BY id;`,
					Expected: []sql.Row{
						{1, "$100.25"},
						{2, "$50.50"},
					},
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
					Query: `SELECT id, p FROM point_round_trip_items ORDER BY id;`,
					Expected: []sql.Row{
						{1, "(1,2)"},
						{2, "(3,4)"},
					},
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
					Query: `SELECT id, b FROM box_round_trip_items ORDER BY id;`,
					Expected: []sql.Row{
						{1, "(3,4),(1,2)"},
						{2, "(7,8),(5,6)"},
					},
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
					Query: `SELECT id, c FROM circle_round_trip_items ORDER BY id;`,
					Expected: []sql.Row{
						{1, "<(1,2),3>"},
						{2, "<(4,5),6>"},
					},
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
					Query: `SELECT id, ln FROM line_round_trip_items ORDER BY id;`,
					Expected: []sql.Row{
						{1, "{1,2,3}"},
						{2, "{4,5,6}"},
					},
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
					Query: `SELECT id, seg FROM lseg_round_trip_items ORDER BY id;`,
					Expected: []sql.Row{
						{1, "[(1,2),(3,4)]"},
						{2, "[(5,6),(7,8)]"},
					},
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
					Query: `SELECT id, p FROM path_round_trip_items ORDER BY id;`,
					Expected: []sql.Row{
						{1, "((1,2),(3,4),(5,6))"},
						{2, "((7,8),(9,10),(11,12))"},
					},
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
					Query: `SELECT id, p FROM polygon_round_trip_items ORDER BY id;`,
					Expected: []sql.Row{
						{1, "((1,2),(3,4),(5,6))"},
						{2, "((7,8),(9,10),(11,12))"},
					},
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
					Query: `SELECT id, doc FROM xml_round_trip_items ORDER BY id;`,
					Expected: []sql.Row{
						{1, "<doc><title>one</title></doc>"},
						{2, "<doc><title>two</title></doc>"},
					},
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
					Query: `SELECT id, query FROM tsquery_round_trip_items ORDER BY id;`,
					Expected: []sql.Row{
						{1, "'fat' & 'rat'"},
						{2, "'cat' | 'dog'"},
					},
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
					Query: `SELECT id, doc FROM tsvector_round_trip_items ORDER BY id;`,
					Expected: []sql.Row{
						{1, "'a' 'cat' 'fat' 'sat'"},
						{2, "'a' 'dog' 'ran'"},
					},
				},
			},
		},
	})
}
