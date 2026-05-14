// Copyright 2023 Dolthub, Inc.
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

func TestTypes(t *testing.T) {
	RunScripts(t, typesTests)
}

var typesTests = []ScriptTest{
	{
		Name: "Bigint type",
		SetUpScript: []string{
			"CREATE TABLE t_bigint (id INTEGER primary key, v1 BIGINT);",
			"INSERT INTO t_bigint VALUES (1, 123456789012345), (2, 987654321098765);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_bigint ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0001-select-*-from-t_bigint-order"},
			},
			{
				Query: `SELECT 1::pg_catalog.int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0002-select-1::pg_catalog.int8"},
			},
		},
	},
	{
		Name: "Bigint key",
		SetUpScript: []string{
			"CREATE TABLE t_bigint (id BIGINT primary key, v1 BIGINT);",
			"INSERT INTO t_bigint VALUES (1, 123456789012345), (2, 987654321098765);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_bigint WHERE id = 1 ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0003-select-*-from-t_bigint-where"},
			},
		},
	},
	{
		Name: "Bigint array type",
		SetUpScript: []string{
			"CREATE TABLE t_bigint (id INTEGER primary key, v1 BIGINT[]);",
			"INSERT INTO t_bigint VALUES (1, ARRAY[123456789012345, NULL]), (2, ARRAY[987654321098765, 5]), (3, ARRAY[4, 5]);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_bigint ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0004-select-*-from-t_bigint-order"},
			},
		},
	},
	{
		Name:        "schema-qualified array type cast",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT pg_catalog.array_to_string('{70000,70001}'::pg_catalog.oid[], ',');`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0005-select-pg_catalog.array_to_string-{70000-70001}-::pg_catalog.oid[]"},
			},
			{
				Query: `SELECT count(*) FROM unnest('{70000,70001}'::pg_catalog.oid[]) AS src(tbloid);`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0006-select-count-*-from-unnest"},
			},
			{
				Query: `SELECT count(*) FROM unnest('{70000,70001}'::pg_catalog.oid[]) AS src(tbloid) JOIN (SELECT 70000::pg_catalog.oid AS oid) AS t ON src.tbloid = t.oid;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0007-select-count-*-from-unnest"},
			},
		},
	},
	{
		Name: "Bit type",
		SetUpScript: []string{
			"CREATE TABLE t_bit (id INTEGER primary key, v1 BIT(8), v2 BIT(3));",
			"INSERT INTO t_bit VALUES (1, B'11011010', '101'), (2, B'00101011', '000');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_bit ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0008-select-*-from-t_bit-order"},
			},
			{
				Query: "INSERT INTO t_bit VALUES (3, B'101', '111');", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0009-insert-into-t_bit-values-3", Compare: "sqlstate"},
			},
			{
				Query: "INSERT INTO t_bit VALUES (3, B'1001000110', '111');", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0010-insert-into-t_bit-values-3", Compare: "sqlstate"},
			},
			{
				Query: "INSERT INTO t_bit VALUES (3, B'10010001', '11100100');", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0011-insert-into-t_bit-values-3", Compare: "sqlstate"},
			},
			{
				Query: "INSERT INTO t_bit VALUES (3, B'10012345', '111');", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0012-insert-into-t_bit-values-3", Compare: "sqlstate"},
			},
			{
				Query: "INSERT INTO t_bit VALUES (3, '10012345', '111');", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0013-insert-into-t_bit-values-3", Compare: "sqlstate"},
			},
		},
	},
	{
		Name: "Bit key",
		SetUpScript: []string{
			"CREATE TABLE t_bit (id BIT(8) primary key, v1 BIT(8));",
			"INSERT INTO t_bit VALUES (B'11011010', B'11011010'), (B'00101011', B'00101011');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_bit WHERE id = B'11011010' ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0014-select-*-from-t_bit-where"},
			},
		},
	},
	{
		Name: "Boolean type",
		SetUpScript: []string{
			"CREATE TABLE t_boolean (id INTEGER primary key, v1 BOOLEAN);",
			"INSERT INTO t_boolean VALUES (1, true), (2, 'false'), (3, NULL);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_boolean ORDER BY id;",
				Skip:  true, PostgresOracle: // Proper NULL-ordering has not yet been implemented
				ScriptTestPostgresOracle{ID: "types-test-testtypes-0015-select-*-from-t_boolean-order"},
			},
			{
				Query: "SELECT * FROM t_boolean ORDER BY v1;",
				Skip:  true, PostgresOracle: // Proper NULL-ordering has not yet been implemented
				ScriptTestPostgresOracle{ID: "types-test-testtypes-0016-select-*-from-t_boolean-order"},
			},
			{
				Query: "SELECT * FROM t_boolean WHERE v1 IS NOT NULL ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0017-select-*-from-t_boolean-where"},
			},
			{
				Query: "SELECT * FROM t_boolean WHERE v1 IS NOT NULL ORDER BY v1;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0018-select-*-from-t_boolean-where"},
			},
		},
	},
	{
		Name: "Boolean key",
		SetUpScript: []string{
			"CREATE TABLE t_boolean (id boolean primary key, v1 BOOLEAN);",
			"INSERT INTO t_boolean VALUES (true, true), (false, 'false')",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_boolean where id ORDER BY id;",
				Skip:  true, PostgresOracle: // Proper NULL-ordering has not yet been implemented
				ScriptTestPostgresOracle{ID: "types-test-testtypes-0019-select-*-from-t_boolean-where"},
			},
		},
	},
	{
		Name: "boolean indexes",
		Skip: true, // panic
		SetUpScript: []string{
			"create table t (b bool);",
			"insert into t values (false);",
			"create table t_idx (b bool);",
			"create index idx on t_idx(b);",
			"insert into t_idx values (false);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from t where (b in (false));", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0020-select-*-from-t-where"},
			},
			{
				Query: "select * from t_idx where (b in (false));", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0021-select-*-from-t_idx-where"},
			},
		},
	},
	{
		Name: "Boolean array type",
		SetUpScript: []string{
			"CREATE TABLE t_boolean_array (id INTEGER primary key, v1 BOOLEAN[]);",
			"INSERT INTO t_boolean_array VALUES (1, ARRAY[true, false]), (2, ARRAY[false, true]), (3, ARRAY[true, true]), (4, ARRAY[false, false]), (5, ARRAY[true]), (6, ARRAY[false]), (7, NULL);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_boolean_array ORDER BY id;",
				Skip:  true, PostgresOracle: // Proper NULL-ordering has not yet been implemented
				ScriptTestPostgresOracle{ID: "types-test-testtypes-0022-select-*-from-t_boolean_array-order"},
			},
			{
				Query: "SELECT * FROM t_boolean_array ORDER BY v1;",
				Skip:  true, PostgresOracle: // Proper NULL-ordering has not yet been implemented
				ScriptTestPostgresOracle{ID: "types-test-testtypes-0023-select-*-from-t_boolean_array-order"},
			},
			{
				Query: "SELECT * FROM t_boolean_array WHERE v1 IS NOT NULL ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0024-select-*-from-t_boolean_array-where"},
			},
			{
				Query: "SELECT * FROM t_boolean_array WHERE v1 IS NOT NULL ORDER BY v1;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0025-select-*-from-t_boolean_array-where"},
			},
		},
	},
	{
		Name: "Bigserial type",
		SetUpScript: []string{
			"CREATE TABLE t_bigserial (id INTEGER primary key, v1 BIGSERIAL);",
			"INSERT INTO t_bigserial VALUES (1, 123456789012345), (2, 987654321098765);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_bigserial ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0026-select-*-from-t_bigserial-order"},
			},
		},
	},
	{
		Name: "Bigserial key",
		SetUpScript: []string{
			"CREATE TABLE t_bigserial (id BIGSERIAL primary key, v1 BIGSERIAL);",
			"INSERT INTO t_bigserial VALUES (123456789012345, 123456789012345), (987654321098765, 987654321098765);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_bigserial where ID = 987654321098765 ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0027-select-*-from-t_bigserial-where"},
			},
		},
	},
	{
		Name: "Bit varying type",
		SetUpScript: []string{
			"CREATE TABLE t_bit_varying (id INTEGER primary key, v1 BIT VARYING(16));",
			"INSERT INTO t_bit_varying VALUES (1, B'1101101010101010'), (2, B'0010101101010101');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_bit_varying ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0028-select-*-from-t_bit_varying-order"},
			},
			{
				Query: "INSERT INTO t_bit_varying VALUES (3, B'101010101010101010');", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0029-insert-into-t_bit_varying-values-3", Compare: "sqlstate"},
			},
		},
	},
	{
		Name: "Bit varying type, unbounded",
		SetUpScript: []string{
			"CREATE TABLE t_bit_varying (id INTEGER primary key, v1 BIT VARYING);",
			"INSERT INTO t_bit_varying VALUES (1, B'1101101010101010'), (2, B'0010101101010101');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_bit_varying ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0030-select-*-from-t_bit_varying-order"},
			},
			{
				Query: "INSERT INTO t_bit_varying VALUES (3, B'101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010');",
			},
			{
				Query: "SELECT * FROM t_bit_varying WHERE id = 3 order by 1;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0031-select-*-from-t_bit_varying-where"},
			},
		},
	},
	{
		Name: "Box type",
		Skip: true,
		SetUpScript: []string{
			"CREATE TABLE t_box (id INTEGER primary key, v1 BOX);",
			"INSERT INTO t_box VALUES (1, '(1,2),(3,4)'), (2, '(5,6),(7,8)');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_box ORDER BY id;", PostgresOracle:
				// TODO: the output and ordering of points here varies from postgres, probably need a GMS type, not a string
				ScriptTestPostgresOracle{ID: "types-test-testtypes-0032-select-*-from-t_box-order"},
			},
		},
	},
	{
		Name: "Bytea type",
		SetUpScript: []string{
			"CREATE TABLE t_bytea (id INTEGER primary key, v1 BYTEA);",
			"INSERT INTO t_bytea VALUES (1, E'\\\\xDEADBEEF'), (2, '\\xC0FFEE'), (3, ''), (4, NULL);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_bytea ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0033-select-*-from-t_bytea-order", ColumnModes: []string{"structural", "bytea"}},
			},
			{
				Query: "SELECT octet_length(v1), bit_length(v1) FROM t_bytea ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0034-select-octet_length-v1-bit_length-v1"},
			},
		},
	},
	{
		Name: "Bytea key",
		Skip: true, // blob/text column 'id' used in key specification without a key length
		SetUpScript: []string{
			"CREATE TABLE t_bytea (id BYTEA primary key, v1 BYTEA);",
			"INSERT INTO t_bytea VALUES (E'\\\\xCAFEBABE', E'\\\\xDEADBEEF'), ('\\xBADD00D5', '\\xC0FFEE');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_bytea WHERE ID = E'\\\\xCAFEBABE' ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0035-select-*-from-t_bytea-where", ColumnModes: []string{"bytea",

					// https://github.com/dolthub/doltgresql/issues/2145
					"bytea"}},
			},
		},
	},
	{

		Name: "bpchar type",
		Assertions: []ScriptTestAssertion{
			{
				Query: "create table bptest1 (pk int primary key, c1 bpchar, c2 bpchar(12));", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0036-create-table-bptest1-pk-int"},
			},
			{
				Query: "insert into bptest1 values (1, '1', '1');", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0037-insert-into-bptest1-values-1"},
			},
			{
				Query: "select * from bptest1;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0038-select-*-from-bptest1"},
			},
			{
				Query: "SELECT '!'::bpchar;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0039-select-!-::bpchar"},
			},
			{
				Query: "SELECT '!'::bpchar(1);", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0040-select-!-::bpchar-1"},
			},
			{
				Query: "SELECT '!'::bpchar(2);", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0041-select-!-::bpchar-2"},
			},
		},
	},
	{
		Name: "Character type",
		SetUpScript: []string{
			"CREATE TABLE t_character (id INTEGER primary key, v1 CHARACTER(5));",
			"INSERT INTO t_character VALUES (1, 'abcde'), (2, 'vwxyz'), (3, 'ghi'), (4, ''), (5, NULL);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_character ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0042-select-*-from-t_character-order"},
			},
			{
				Query: "SELECT length(v1) FROM t_character ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0043-select-length-v1-from-t_character"},
			},
			{
				Query: `SELECT char(20) 'characters' || ' and text' AS "Concat char to unknown type";`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0044-select-char-20-characters-||"},
			},
			{
				Query: "SELECT true::char, false::char;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0045-select-true::char-false::char"},
			},
			{
				Query: "SELECT true::character(5), false::character(5);", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0046-select-true::character-5-false::character-5"},
			},
			{
				Query: "SELECT char 'c' = char 'c' AS true;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0047-select-char-c-=-char"},
			},
		},
	},
	{
		Name: "Character key",
		SetUpScript: []string{
			"CREATE TABLE t_character (id CHAR(5) primary key, v1 CHARACTER(5));",
			"INSERT INTO t_character VALUES ('abcde', 'fghjk'), ('vwxyz', '12345'), ('vwxy', '1234')",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_character WHERE ID = 'vwxyz' ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0048-select-*-from-t_character-where"},
			},
			{
				Query: "SELECT length(id) FROM t_character;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0049-select-length-id-from-t_character"},
			},
		},
	},
	{
		Name: "Internal char type",
		SetUpScript: []string{
			`CREATE TABLE t_char (id INTEGER primary key, v1 "char");`,
			`INSERT INTO t_char VALUES (1, 'abcde'), (2, 'vwxyz'), (3, '123'), (4, ''), (5, NULL), (100, 'こんにちは');`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_char ORDER BY id;",
				Expected: []sql.Row{
					{1, "a"},
					{2, "v"},
					{3, "1"},
					{4, ""},
					{5, nil},
					{100, "\343"},
				},
			},
			{
				Query: "INSERT INTO t_char VALUES (6, 7);", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0051-insert-into-t_char-values-6", Compare: "sqlstate"},
			},
			{
				Query: "INSERT INTO t_char VALUES (6, true);", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0052-insert-into-t_char-values-6", Compare: "sqlstate"},
			},
			{
				Query: `SELECT true::"char";`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0053-select-true::-char", Compare: "sqlstate"},
			},
			{
				Query: `SELECT 100000::bigint::"char";`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0054-select-100000::bigint::-char", Compare: "sqlstate"},
			},
			{
				Query: `SELECT 'abc'::"char", '123'::varchar(3)::"char";`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0055-select-abc-::-char-123"},
			},
			{
				Query: `SELECT 'def'::name::"char";`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0056-select-def-::name::-char"},
			},
			{
				Query: `SELECT id, v1::int, v1::text FROM t_char WHERE id < 10;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0057-select-id-v1::int-v1::text-from"},
			},
			{
				Skip:  true, // TODO: We currently return '227'
				Query: `SELECT v1::int FROM t_char WHERE id = 100;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0058-select-v1::int-from-t_char-where"},
			},
			{
				Query: "INSERT INTO t_char VALUES (6, '0123456789012345678901234567890123456789012345678901234567890123456789');", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0059-insert-into-t_char-values-6"},
			},
			{
				Query: "SELECT * FROM t_char WHERE id=6;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0060-select-*-from-t_char-where"},
			},
			{
				Query: "INSERT INTO t_char VALUES (7, 'abc'::name);", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0061-insert-into-t_char-values-7", Compare: "sqlstate"},
			},
			{
				Query: "INSERT INTO t_char VALUES (8, 'def'::text);", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0062-insert-into-t_char-values-8"},
			},
			{
				Query: "INSERT INTO t_char VALUES (9, 'ghi'::varchar);", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0063-insert-into-t_char-values-9"},
			},
			{
				Query: `SELECT * FROM t_char WHERE id >= 7 AND id < 10 ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0064-select-*-from-t_char-where"},
			},
		},
	},
	{
		Name: "Character varying type",
		SetUpScript: []string{
			"CREATE TABLE t_varchar (id INTEGER primary key, v1 CHARACTER VARYING(10));",
			"INSERT INTO t_varchar VALUES (1, 'abcdefghij'), (2, 'klmnopqrst'), (3, ''), (4, NULL);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_varchar ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0065-select-*-from-t_varchar-order"},
			},
			{
				Query: "SELECT true::character varying(10), false::character varying(10);", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0066-select-true::character-varying-10-false::character"},
			},
		},
	},
	{
		Name: "Character varying type as primary key",
		SetUpScript: []string{
			"CREATE TABLE t_varchar (id INTEGER, v1 CHARACTER VARYING(10) primary key);",
			"INSERT INTO t_varchar VALUES (1, 'abcdefghij'), (2, 'klmnopqrst'), (3, '');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_varchar ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0067-select-*-from-t_varchar-order"},
			},
			{
				Query: "SELECT true::character varying(10), false::character varying(10);", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0068-select-true::character-varying-10-false::character"},
			},
		},
	},
	{
		Name: "Character varying array type, with length",
		SetUpScript: []string{
			"CREATE TABLE t_varchar1 (v1 CHARACTER VARYING[]);",
			"CREATE TABLE t_varchar2 (v1 CHARACTER VARYING(1)[]);",
			"INSERT INTO t_varchar1 VALUES (ARRAY['ab''cdef', 'what', 'is,hi', 'wh\"at']);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT v1::varchar(1)[] FROM t_varchar1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0069-select-v1::varchar-1-[]-from"},
			},
			{
				Query: "INSERT INTO t_varchar2 VALUES (ARRAY['ab''cdef', 'what', 'is,hi', 'wh\"at']);", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0070-insert-into-t_varchar2-values-array[", Compare: "sqlstate"},
			},
			{
				Query: "INSERT INTO t_varchar2 VALUES (ARRAY['a', 'w', 'i', 'w']);", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0071-insert-into-t_varchar2-values-array["},
			},
			{
				Query: `SELECT * FROM t_varchar2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0072-select-*-from-t_varchar2"},
			},
		},
	},
	{
		Name: "Character varying type, no length",
		SetUpScript: []string{
			"CREATE TABLE t_varchar (id INTEGER primary key, v1 CHARACTER VARYING);",
			"INSERT INTO t_varchar VALUES (1, 'abcdefghij'), (2, 'klmnopqrst');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_varchar ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0073-select-*-from-t_varchar-order"},
			},
		},
	},
	{
		Name: "Character varying type, no length, as primary key",
		Skip: true, // panic
		SetUpScript: []string{
			"CREATE TABLE t_varchar (id INTEGER, v1 CHARACTER VARYING primary key);",
			"INSERT INTO t_varchar VALUES (1, 'abcdefghij'), (2, 'klmnopqrst');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_varchar ORDER BY id;",
				Skip:  true, PostgresOracle: // missing the second row
				ScriptTestPostgresOracle{ID: "types-test-testtypes-0074-select-*-from-t_varchar-order"},
			},
		},
	},
	{
		Name: "Character varying array type, no length",
		SetUpScript: []string{
			"CREATE TABLE t_varchar (id INTEGER primary key, v1 CHARACTER VARYING[]);",
			"INSERT INTO t_varchar VALUES (1, '{abcdefghij, NULL}'), (2, ARRAY['ab''cdef', 'what', 'is,hi', 'wh\"at', '}', '{', '{}']);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_varchar ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0075-select-*-from-t_varchar-order"},
			},
		},
	},
	{
		Name: "2D array",
		Skip: true, // multiple dimensions not supported yet
		SetUpScript: []string{
			"CREATE TABLE t_varchar (id INTEGER primary key, v1 CHARACTER VARYING[][]);",
			"INSERT INTO t_varchar VALUES (1, '{{abcdefghij, NULL}, {1234, abc}}'), (2, ARRAY['ab''cdef', 'what', 'is,hi', 'wh\"at', '}', '{', '{}']);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_varchar ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0076-select-*-from-t_varchar-order"},
			},
		},
	}, {
		Name: "Cidr type",
		Skip: true,
		SetUpScript: []string{
			"CREATE TABLE t_cidr (id INTEGER primary key, v1 CIDR);",
			"INSERT INTO t_cidr VALUES (1, '192.168.1.0/24'), (2, '10.0.0.0/8');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_cidr ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0077-select-*-from-t_cidr-order"},
			},
		},
	},
	{
		Name: "Circle type",
		Skip: true,
		SetUpScript: []string{
			"CREATE TABLE t_circle (id INTEGER primary key, v1 CIRCLE);",
			"INSERT INTO t_circle VALUES (1, '<(1,2),3>'), (2, '<(4,5),6>');",
		},
		Assertions: []ScriptTestAssertion{
			{
				// TODO: might need a GMS type here, not a string
				Query: "SELECT * FROM t_circle ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0078-select-*-from-t_circle-order"},
			},
		},
	},
	{
		Name: "Date type",
		SetUpScript: []string{
			"CREATE TABLE t_date (id INTEGER primary key, v1 DATE);",
			"INSERT INTO t_date VALUES (1, '2023-01-01'), (2, '2023-02-02');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_date ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0079-select-*-from-t_date-order"},
			},
			{
				Query: "SELECT date '2022-2-2'", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0080-select-date-2022-2-2"},
			},
			{
				Query: "SELECT date '2022-02-02'", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0081-select-date-2022-02-02"},
			},
			{
				Query: "select '2024-10-31'::date;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0082-select-2024-10-31-::date"},
			},
			{
				Query: "select '2024-OCT-31'::date;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0083-select-2024-oct-31-::date"},
			},
			{
				Query: "select '20241031'::date;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0084-select-20241031-::date"},
			},
			{
				Query: "select '2024Oct31'::date;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0085-select-2024oct31-::date"},
			},
			{
				Query: "select '10 31 2024'::date;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0086-select-10-31-2024-::date"},
			},
			{
				Query: "select 'Oct 31 2024'::date;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0087-select-oct-31-2024-::date"},
			},
			{
				Query: "SELECT date 'J2451187';", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0088-select-date-j2451187"},
			},
			{
				Query: `SELECT date '08-Jan-99';`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0089-select-date-08-jan-99"},
			},
			{
				Query: `SELECT date '2025-07-21' - 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0090-select-date-2025-07-21-1"},
			},
			{
				Query: `SELECT date '2025-07-21' - date '2025-07-18';`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0091-select-date-2025-07-21-date-2025-07-18"},
			},
			{
				Query: `SELECT date '2025-07-21' - interval '2 days';`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0092-select-date-2025-07-21-interval-2"},
			},
			{
				Query: `SELECT date '1991-02-03' - time '04:05:06';`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0093-select-date-1991-02-03-time-04:05:06"},
			},
			{
				Query: `SELECT date '2025-07-21' - 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0094-select-date-2025-07-21-1"},
			},
			{
				Query: `SELECT date '1991-02-03' - time '04:05:06';`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0095-select-date-1991-02-03-time-04:05:06"},
			},
			{
				Query: `SELECT date '2025-07-21' + 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0096-select-date-2025-07-21-+-1"},
			},
			{
				Query: `SELECT date '2025-07-21' + interval '2 days';`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0097-select-date-2025-07-21-+-interval"},
			},
			{
				Query: `SELECT date '2025-07-21' + time '04:05:06';`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0098-select-date-2025-07-21-+-time"},
			},
			{
				Query: `SELECT date '2025-07-21' + time '04:05:06 UTC';`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0099-select-date-2025-07-21-+-time"},
			},
		},
	},
	{
		Name: "Date key",
		SetUpScript: []string{
			"CREATE TABLE t_date (id DATE primary key, v1 DATE);",
			"INSERT INTO t_date VALUES ('2025-01-01', '2023-01-01'), ('2026-01-01', '2023-02-02');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_date where Id = '2025-01-01' ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0100-select-*-from-t_date-where"},
			},
		},
	},
	{
		Name: "Double precision type",
		SetUpScript: []string{
			"CREATE TABLE t_double_precision (id INTEGER primary key, v1 DOUBLE PRECISION);",
			"INSERT INTO t_double_precision VALUES (1, 123.456), (2, 789.012);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_double_precision ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0101-select-*-from-t_double_precision-order"},
			},
		},
	},
	{
		Name: "Double precision key",
		SetUpScript: []string{
			"CREATE TABLE t_double_precision (id DOUBLE PRECISION primary key, v1 DOUBLE PRECISION);",
			"INSERT INTO t_double_precision VALUES (456.789, 123.456), (123.456, 789.012);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_double_precision WHERE id = 456.789 ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0102-select-*-from-t_double_precision-where"},
			},
		},
	},
	{
		Name: "Double precision array type",
		SetUpScript: []string{
			"CREATE TABLE t_double_precision (id INTEGER primary key, v1 DOUBLE PRECISION[]);",
			"INSERT INTO t_double_precision VALUES (1, ARRAY[123.456, NULL]), (2, ARRAY[789.012, 125.125]);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_double_precision ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0103-select-*-from-t_double_precision-order"},
			},
		},
	},
	{
		Name: "Inet type",
		Skip: true,
		SetUpScript: []string{
			"CREATE TABLE t_inet (id INTEGER primary key, v1 INET);",
			"INSERT INTO t_inet VALUES (1, '192.168.1.1'), (2, '10.0.0.1');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_inet ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0104-select-*-from-t_inet-order"},
			},
		},
	},
	{
		Name: "Integer type",
		SetUpScript: []string{
			"CREATE TABLE t_integer (id INTEGER primary key, v1 INTEGER);",
			"INSERT INTO t_integer VALUES (1, 123), (2, 456);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_integer ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0105-select-*-from-t_integer-order"},
			},
		},
	},
	{
		Name: "Integer array type",
		SetUpScript: []string{
			"CREATE TABLE t_integer (id INTEGER primary key, v1 INTEGER[]);",
			"INSERT INTO t_integer VALUES (1, ARRAY[123,NULL]), (2, ARRAY[456,823753913]);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_integer ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0106-select-*-from-t_integer-order"},
			},
		},
	},
	{
		Name: "Interval type",
		SetUpScript: []string{
			"CREATE TABLE t_interval (id INTEGER primary key, v1 INTERVAL);",
			"INSERT INTO t_interval VALUES (1, '1 day 3 hours'), (2, '23 hours 30 minutes'), (3, '@ 1 minute');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_interval ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0107-select-*-from-t_interval-order"},
			},
			{
				Query: "SELECT * FROM t_interval ORDER BY v1;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0108-select-*-from-t_interval-order"},
			},
			{
				Query: `SELECT id, v1::char, v1::name FROM t_interval;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0109-select-id-v1::char-v1::name-from"},
			},
			{
				Query: `SELECT '2 years 15 months 100 weeks 99 hours 123456789 milliseconds'::interval;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0110-select-2-years-15-months"},
			},
			{
				Query: `SELECT '2 years 15 months 100 weeks 99 hours 123456789 milliseconds'::interval::char;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0111-select-2-years-15-months"},
			},
			{
				Query: `SELECT '2 years 15 months 100 weeks 99 hours 123456789 milliseconds'::interval::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0112-select-2-years-15-months"},
			},
			{
				Query: `SELECT '2 years 15 months 100 weeks 99 hours 123456789 milliseconds'::char::interval;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0113-select-2-years-15-months"},
			},
			{
				Query: `SELECT '13 months'::name::interval;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0114-select-13-months-::name::interval"},
			},
			{
				Query: `SELECT '13 months'::bpchar::interval;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0115-select-13-months-::bpchar::interval"},
			},
			{
				Query: `SELECT '13 months'::varchar::interval;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0116-select-13-months-::varchar::interval"},
			},
			{
				Query: `SELECT '13 months'::text::interval;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0117-select-13-months-::text::interval"},
			},
			{
				Query: `SELECT '13 months'::char::interval;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0118-select-13-months-::char::interval"},
			},
			{
				Query: "INSERT INTO t_interval VALUES (3, 7);", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0119-insert-into-t_interval-values-3", Compare: "sqlstate"},
			},
			{
				Query: "INSERT INTO t_interval VALUES (3, true);", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0120-insert-into-t_interval-values-3", Compare: "sqlstate"},
			},
			{
				Query: `SELECT CAST(interval '02:03' AS time) AS "02:03:00";`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0121-select-cast-interval-02:03-as"},
			},
		},
	},
	{
		Name: "Interval key",
		SetUpScript: []string{
			"CREATE TABLE t_interval (id interval primary key, v1 INTERVAL);",
			"INSERT INTO t_interval VALUES ('1 hour', '1 day 3 hours'), ('2 days', '23 hours 30 minutes');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_interval WHERE id = '1 hour' ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0122-select-*-from-t_interval-where"},
			},
		},
	},
	{
		Name: "Interval array type",
		SetUpScript: []string{
			"CREATE TABLE t_interval_array (id INTEGER primary key, v1 INTERVAL[]);",
			"INSERT INTO t_interval_array VALUES (1, ARRAY['1 day 3 hours'::interval,'5 days 2 hours'::interval]), (2, ARRAY['3 years 3 mons 700 days 133:17:36.789'::interval,'200 hours'::interval]);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_interval_array ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0123-select-*-from-t_interval_array-order"},
			},
		},
	},
	{
		Name:        "JSON key",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "CREATE TABLE t_json (id JSON primary key, v1 JSON);",

				Skip: true, PostgresOracle: // current error message is blob/text column 'id' used in key specification without a key length
				ScriptTestPostgresOracle{ID: "types-test-testtypes-0124-create-table-t_json-id-json", Compare: "sqlstate"},
			},
		},
	},
	{
		Name: "JSON type",
		SetUpScript: []string{
			"CREATE TABLE t_json (id INTEGER primary key, v1 JSON);",
			`INSERT INTO t_json VALUES (1, '{"key1": {"key": "value"}}'), (2, '{"num":42}'), (3, '{"key1": "value1", "key2": "value2"}'), (4, '{"key1": {"key": [2,3]}}');`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_json ORDER BY 1;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0125-select-*-from-t_json-order"},
			},
			{
				Query: "SELECT * FROM t_json ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0126-select-*-from-t_json-order"},
			},
			{
				Query: "Insert into t_json values (100, null) returning *", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0127-insert-into-t_json-values-100"},
			},
			{
				Query: "select * from t_json where id = 100", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0128-select-*-from-t_json-where"},
			},
			{
				Query:    "Insert into t_json values ($1, $2) returning *",
				BindVars: []any{"101", nil}, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0129-insert-into-t_json-values-$1"},
			},
			{
				Query: "SELECT '5'::json;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0130-select-5-::json"},
			},
			{
				Query: "SELECT 'false'::json;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0131-select-false-::json"},
			},
			{
				Query: `SELECT '"hi"'::json;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0132-select-hi-::json"},
			},
			{
				Query: `SELECT null::json;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0133-select-null::json"},
			},
			{
				Skip:  true, // https://github.com/jackc/pgx/issues/2430
				Query: `SELECT 'null'::json;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0134-select-null-::json"},
			},
			{
				Query: `SELECT '{"reading": 1.230e-5}'::json;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0135-select-{-reading-:-1.230e-5}"},
			},
			{
				Query: `select json '{ "a":  "\ud83d\ude04\ud83d\udc36" }' -> 'a'`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0136-select-json-{-a-:"},
			},
		},
	},
	{
		Name: "JSON column default",
		SetUpScript: []string{
			`CREATE TABLE t_json (id INTEGER primary key, v1 JSON DEFAULT '{"num": 42}'::JSON);`,
			`INSERT INTO t_json VALUES (1, '{"key1": {"key": "value"}}');`,
			`INSERT INTO t_json (id) VALUES (2);`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_json ORDER BY 1;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0137-select-*-from-t_json-order"},
			},
		},
	},
	{
		Name: "JSONB type",
		SetUpScript: []string{
			"CREATE TABLE t_jsonb (id INTEGER primary key, v1 JSONB);",
			"INSERT INTO t_jsonb VALUES (1, '{\"key\": \"value\"}'), (2, '{\"num\": 42}');",
			"CREATE TABLE t_jsonb_unique (id INTEGER primary key, v1 JSONB UNIQUE);",
			"INSERT INTO t_jsonb_unique VALUES (1, '{\"key\": \"value\"}');",
			"CREATE TABLE t_jsonb_unique_build (id INTEGER primary key, v1 JSONB);",
			"INSERT INTO t_jsonb_unique_build VALUES (1, '{\"key\": \"value\"}'), (2, '{\"key\": \"value\"}');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_jsonb ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0138-select-*-from-t_jsonb-order"},
			},
			{
				Query: "insert into t_jsonb values (3, null) returning *", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0139-insert-into-t_jsonb-values-3"},
			},
			{
				Query:    "insert into t_jsonb values ($1, $2) returning *",
				BindVars: []any{"4", nil}, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0140-insert-into-t_jsonb-values-$1"},
			},
			{
				Query: `SELECT '{"bar": "baz", "balance": 7.77, "active":false}'::jsonb;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0141-select-{-bar-:-baz"},
			},
			{
				Query: `SELECT '{"active": "baz", "active":false, "balance": 7.77}'::jsonb;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0142-select-{-active-:-baz"},
			},
			{
				Query: `SELECT '{"active":false, "balance": 7.77, "bar": "baz"}'::jsonb;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0143-select-{-active-:false-balance"},
			},
			{
				Query: `SELECT jsonb '{"a":null, "b":"qq"}' ? 'a';`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0144-select-jsonb-{-a-:null"},
			},
			{
				Query: `INSERT INTO t_jsonb_unique VALUES (2, '{"key": "value"}');`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0145-insert-into-t_jsonb_unique-values-2", Compare: "sqlstate"},
			},
			{
				Query: `ALTER TABLE t_jsonb_unique_build ADD CONSTRAINT t_jsonb_unique_build_v1_key UNIQUE (v1);`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0146-alter-table-t_jsonb_unique_build-add-constraint", Compare: "sqlstate"},
			},
		},
	},
	{
		Name: "JSONB column default",
		SetUpScript: []string{
			`CREATE TABLE t_json (id INTEGER primary key, v1 JSONB DEFAULT '{"num": 42}'::JSONB);`,
			`INSERT INTO t_json VALUES (1, '{"key1": {"key": "value"}}');`,
			`INSERT INTO t_json (id) VALUES (2);`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_json ORDER BY 1;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0147-select-*-from-t_json-order"},
			},
		},
	},
	{
		Name: "JSONB ORDER BY",
		SetUpScript: []string{
			`CREATE TABLE t_jsonb (v1 JSONB);`,
			`INSERT INTO t_jsonb VALUES
				('["string_with_emoji_😊"]'),
				('[null, "null_as_string", false, 0]'),
				('{"key1": "value1", "key2": "value2", "key3": "value3"}'),
				('{"simple": "object"}'),
				('["special_chars_!@#$%^&*()_+", {"more": "!@#$"}]'),
				('[null, 1, "two", true, {"five": 5}]'),
				('[true, false, true]'),
				('{"key1": 123, "key2": "duplicate_key", "common_key": "same_value"}'),
				('["emoji_😀", "nested_😂", {"key": "value"}]'),
				('{"common_key": 456}'),
				('{"common_key": 123}'),
				('{"mixed_data": {"number": 100, "string": "text", "bool": false, "null": null}}'),
				('{"nested": {"level1": {"level2": {"key": "deep_value"}}}}'),
				('[1.1, 2.2, 3.3, 4.4, 5.5]'),
				('[{"nested_array": [1, 2, {"deep": {"inner": "value"}}]}, "text"]'),
				('{"common_key": "same_value"}'),
				('["end", "of", "array", 123, true]'),
				('"random string"'),
				('{"unicode": "こんにちは", "emoji": "😊"}'),
				('{"keyX": "string_value", "keyY": 123.456, "keyZ": null}'),
				('[{"key1": "value1"}, {"key2": "value2"}]'),
				('{"array_of_arrays": {"array1": [1, 2, 3], "array2": [4, 5, 6], "array3": [7, 8, 9]}}'),
				('{"key1": 123, "key2": "value", "key3": true}'),
				('{"key1": 1, "key2": 2, "key3": 3, "key4": 4, "key5": 5}'),
				('{"numbers": [1, 2, 3], "strings": ["a", "b", "c"], "booleans": [true, false]}'),
				('{"unicode_chars": {"char1": "あ", "char2": "い", "char3": "う"}}'),
				('[true, null, "string", 3.14]'),
				('{"array_of_bools": [true, false, true]}'),
				('[-1, -2, -3, -4]'),
				('[{"nested_array": [1, 2, 3]}, {"nested_object": {"inner_key": "inner_value"}}]'),
				('{"single": 1, "double": 2, "triple": 3, "quadruple": 4}'),
				('true'),
				('{"complex_array": {"array1": [1, 2, 3], "array2": ["a", "b", "c"]}}'),
				('["mixed", 123, false, null, {"complex": {"key": "value"}}]'),
				('{"array_of_strings": ["one", "two", "three"]}'),
				('["simple_text"]'),
				('{"mixed": {"number": 100, "string": "text", "bool": false, "null": null}}'),
				('{"boolean_true": true, "boolean_false": false, "null_value": null}'),
				('[{"deep": {"structure": {"key": "value"}}}, 123, false]'),
				('{"nested_numbers": {"one": 1, "two": 2, "three": 3}}'),
				('[{"emoji": "😊"}, {"another_emoji": "😢"}]'),
				('["just_text"]'),
				('{"common_key": "different_value"}'),
				('[[], [], []]'),
				('{"array_of_objects": [{"key1": "value1"}, {"key2": "value2"}, {"key3": "value3"}]}'),
				('{"combos": [{"number": 1}, {"string": "two"}, {"boolean": true}]}'),
				('{"keyA": 456, "keyB": "another_value", "keyC": false, "keyD": [1, 2, 3]}'),
				('[true, false, true, false, null]'),
				('[{"deep_nested": {"level1": {"level2": {"level3": "value"}}}}, 42, "text"]'),
				('{"empty": {}}'),
				('{"common_key": {"nested_key": "different_value"}}'),
				('["a", "b", "c", {"nested": {"key": "value"}}]'),
				('{"deep_nesting": {"level1": {"level2": {"level3": {"key": "value"}}}}}'),
				('{"random_text": "Lorem ipsum dolor sit amet"}'),
				('{"nested_string": {"outer": {"inner": "text"}}}'),
				('[1, 2, 3, 4, 5]'),
				('{"single_bool": true}'),
				('[1234567890, "large_number", false]'),
				('{"array_of_numbers": [1, 2, 3]}'),
				('[3.14159, 2.71828, 1.61803]'),
				('{"common_key": {"nested_key": "value"}}'),
				('["string1", "string2", "string3"]'),
				('{"single_string": "hello"}'),
				('{"nested_mixed": {"key1": 1, "key2": [true, false], "key3": {"inner_key": "inner_value"}}}'),
				('[0.1, 0.2, 0.3, 0.4]'),
				('[{"unicode": "こんにちは"}, {"another": "你好"}]'),
				('[1, "two", true, null, [1, 2, 3]]'),
				('["flat", "array", "of", "strings"]'),
				('123456'),
				('{"nested_object": {"subkey1": 789, "subkey2": [true, false], "subkey3": {"deep": "value"}}}'),
				('[{"key": {"subkey": [1, 2, 3]}}, 42, "text", false]'),
				('{"string_with_numbers": {"key": "123abc", "another_key": "456def"}}'),
				('{"unicode_string": {"greeting": "你好"}}'),
				('[{"key": "value"}, {"array": [1, 2, 3]}, {"nested": {"inner": "deep"}}]'),
				('["simple", "array", "of", "strings"]'),
				('{"text": "simple_string", "integer": 123, "float": 3.14}'),
				('[[], ["nested", "array"], 123]'),
				('{"object_in_array": [{"key": "value"}, {"another": "one"}]}'),
				('{"single_number": 42}'),
				('[null, null, null]'),
				('{"random_mixed": {"number": 1, "string": "two", "boolean": true, "null": null}}'),
				('null'),
				('["varied", "types", true, 123, {"key": "value"}]'),
				('[true, false, null, "end"]'),
				('789.123'),
				('["unicode_안녕하세요", "string"]'),
				('{"empty_object": {}, "empty_array": [], "boolean": true}'),
				('["text", 123, false, {"key": "value"}, [1, 2, 3]]'),
				('["multiple", "types", 123, true, {"key": "value"}]'),
				('{"boolean_mixed": {"true": true, "false": false, "null": null}}'),
				('{"object_in_array": {"array": [1, 2, 3], "nested": {"key": "value"}}}'),
				('[123, 456, 789]'),
				('[{"obj_in_array": {"key": "value"}}, [1, 2, 3], false]'),
				('false'),
				('[{"complex": {"nested": {"structure": "value"}}}, [1, 2, 3], false]'),
				('{"simple_object": {"key": "value"}}'),
				('{"number_key": {"integer": 1, "float": 2.3, "negative": -1}}'),
				('{"complex_object": {"key1": {"subkey": "value1"}, "key2": {"subkey": "value2"}}}'),
				('[1, "two", true, null, {"key": "value"}]');`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_jsonb ORDER BY v1;", PostgresOracle: ScriptTestPostgresOracle{

					// should be "null", but https://github.com/jackc/pgx/issues/2430
					ID: "types-test-testtypes-0148-select-*-from-t_jsonb-order"},
			},
		},
	},
	{
		Name: "JSONB large string",
		SetUpScript: []string{
			`CREATE TABLE t_jsonl (pk INT4 PRIMARY KEY, v1 JSONB);`,
			`INSERT INTO t_jsonl VALUES (1, '{"key1": "01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"}');`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT pk, length(v1::TEXT) FROM t_jsonl;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0149-select-pk-length-v1::text-from"},
			},
		},
	},
	{
		Name: "Line type",
		Skip: true,
		SetUpScript: []string{
			"CREATE TABLE t_line (id INTEGER primary key, v1 LINE);",
			"INSERT INTO t_line VALUES (1, '{1,2,3}'), (2, '{4,5,6}');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_line ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0150-select-*-from-t_line-order"},
			},
		},
	},
	{
		Name: "Lseg type",
		Skip: true,
		SetUpScript: []string{
			"CREATE TABLE t_lseg (id INTEGER primary key, v1 LSEG);",
			"INSERT INTO t_lseg VALUES (1, '((1,2),(3,4))'), (2, '((5,6),(7,8))');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_lseg ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0151-select-*-from-t_lseg-order"},
			},
		},
	},
	{
		Name: "Macaddr type",
		Skip: true,
		SetUpScript: []string{
			"CREATE TABLE t_macaddr (id INTEGER primary key, v1 MACADDR);",
			"INSERT INTO t_macaddr VALUES (1, '08:00:2b:01:02:03'), (2, '00:11:22:33:44:55');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_macaddr ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0152-select-*-from-t_macaddr-order"},
			},
		},
	},
	{
		Name: "Money type",
		Skip: true,
		SetUpScript: []string{
			"CREATE TABLE t_money (id INTEGER primary key, v1 MONEY);",
			"INSERT INTO t_money VALUES (1, '$100.25'), (2, '$50.50');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_money ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0153-select-*-from-t_money-order"},
			},
		},
	},
	{
		Name: "Name type",
		SetUpScript: []string{
			"CREATE TABLE t_name (id INTEGER primary key, v1 NAME);",
			"INSERT INTO t_name VALUES (1, 'abcdefghij'), (2, 'klmnopqrst');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_name ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0154-select-*-from-t_name-order"},
			},
			{
				Query: "SELECT * FROM t_name ORDER BY v1 DESC;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0155-select-*-from-t_name-order"},
			},
			{
				Query: "SELECT v1::char(1) FROM t_name WHERE v1='klmnopqrst';", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0156-select-v1::char-1-from-t_name"},
			},
			{
				Query: "UPDATE t_name SET v1='tuvwxyz' WHERE id=2;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0157-update-t_name-set-v1=-tuvwxyz"},
			},
			{
				Query: "DELETE FROM t_name WHERE v1='abcdefghij';", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0158-delete-from-t_name-where-v1="},
			},
			{
				Query: "SELECT id::name, v1::text FROM t_name ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0159-select-id::name-v1::text-from-t_name"},
			},
			{
				Query: "INSERT INTO t_name VALUES (3, '0123456789012345678901234567890123456789012345678901234567890123456789');", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0160-insert-into-t_name-values-3"},
			},
			{
				Query: "SELECT * FROM t_name ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0161-select-*-from-t_name-order"},
			},
			{
				Query: "INSERT INTO t_name VALUES (4, 12345);",
				Skip:  true, PostgresOracle: // TODO: according to casting rules this shouldn't work but it does, investigate why
				ScriptTestPostgresOracle{ID: "types-test-testtypes-0162-insert-into-t_name-values-4"},
			},
			{
				Query: "SELECT * FROM t_name ORDER BY id;",
				Skip:  true, PostgresOracle: // This is skipped because the one above is skipped
				ScriptTestPostgresOracle{ID: "types-test-testtypes-0163-select-*-from-t_name-order"},
			},
			{
				Query: `SELECT name 'name string' = name 'name string' AS "True";`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0164-select-name-name-string-="},
			},
		},
	},
	{
		Name: "Name key",
		SetUpScript: []string{
			"CREATE TABLE t_name (id NAME primary key, v1 NAME);",
			"INSERT INTO t_name VALUES ('wxyz', 'abcdefghij'), ('abcd', 'klmnopqrst');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_name WHERE id = 'wxyz' ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0165-select-*-from-t_name-where"},
			},
		},
	},
	{
		Name: "Name type, explicit casts",
		SetUpScript: []string{
			"CREATE TABLE t_name (id INTEGER primary key, v1 NAME);",
			"INSERT INTO t_name VALUES (1, 'abcdefghij'), (2, '12345');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_name ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0166-select-*-from-t_name-order"},
			},
			// Cast from Name to types
			{
				Query: "SELECT v1::char(1), v1::varchar(2), v1::text FROM t_name WHERE id=1;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0167-select-v1::char-1-v1::varchar-2"},
			},
			{
				Query: "SELECT v1::smallint, v1::integer, v1::bigint, v1::float4, v1::float8, v1::numeric FROM t_name WHERE id=2;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0168-select-v1::smallint-v1::integer-v1::bigint-v1::float4"},
			},
			{
				Query: "SELECT v1::oid, v1::xid FROM t_name WHERE id=2;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0169-select-v1::oid-v1::xid-from-t_name"},
			},
			{
				Query: "SELECT v1::xid FROM t_name WHERE id=1;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0170-select-v1::xid-from-t_name-where", Compare: "sqlstate"},
			},
			{
				Query: "SELECT ('0'::name)::boolean, ('1'::name)::boolean;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0171-select-0-::name-::boolean-1"},
			},
			{
				Query: "SELECT v1::smallint FROM t_name WHERE id=1;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0172-select-v1::smallint-from-t_name-where", Compare: "sqlstate"},
			},
			{
				Query: "SELECT v1::integer FROM t_name WHERE id=1;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0173-select-v1::integer-from-t_name-where", Compare: "sqlstate"},
			},
			{
				Query: "SELECT v1::bigint FROM t_name WHERE id=1;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0174-select-v1::bigint-from-t_name-where", Compare: "sqlstate"},
			},
			{
				Query: "SELECT v1::float4 FROM t_name WHERE id=1;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0175-select-v1::float4-from-t_name-where", Compare: "sqlstate"},
			},
			{
				Query: "SELECT v1::float8 FROM t_name WHERE id=1;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0176-select-v1::float8-from-t_name-where", Compare: "sqlstate"},
			},
			{
				Query: "SELECT v1::numeric FROM t_name WHERE id=1;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0177-select-v1::numeric-from-t_name-where", Compare: "sqlstate"},
			},
			{
				Query: "SELECT v1::boolean FROM t_name WHERE id=1;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0178-select-v1::boolean-from-t_name-where", Compare: "sqlstate"},
			},
			{
				Query: "SELECT v1::oid FROM t_name WHERE id=1;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0179-select-v1::oid-from-t_name-where", Compare: "sqlstate"},
			},

			// Cast to Name from types
			{
				Query: "SELECT ('abc'::char(3))::name, ('abc'::varchar)::name, ('abc'::text)::name;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0180-select-abc-::char-3-::name"},
			},
			{
				Query: "SELECT (10::int2)::name, (100::int4)::name, (1000::int8)::name;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0181-select-10::int2-::name-100::int4-::name"},
			},
			{
				Query: "SELECT (1.1::float4)::name, (10.1::float8)::name;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0182-select-1.1::float4-::name-10.1::float8-::name"},
			},
			{
				Query: "SELECT (100.0::numeric)::name;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0183-select-100.0::numeric-::name"},
			},
			{
				Query: "SELECT false::name, true::name, ('0'::boolean)::name, ('1'::boolean)::name;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0184-select-false::name-true::name-0-::boolean"},
			},
			{
				Query: "SELECT ('123'::xid)::name, (123::oid)::name;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0185-select-123-::xid-::name-123::oid"},
			},
		},
	},
	{
		Name: "Name array type",
		SetUpScript: []string{
			"CREATE TABLE t_namea (id INTEGER primary key, v1 NAME[], v2 CHARACTER(100), v3 BOOLEAN);",
			"INSERT INTO t_namea VALUES (1, ARRAY['ab''cdef', 'what', 'is,hi', 'wh\"at'], '1234567890123456789012345678901234567890123456789012345678901234567890', true);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT v1::varchar(1)[] FROM t_namea;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0186-select-v1::varchar-1-[]-from"},
			},
			{
				Query: `SELECT v2::name, v3::name FROM t_namea;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0187-select-v2::name-v3::name-from-t_namea"},
			},
		},
	},
	{
		Name: "Numeric type",
		SetUpScript: []string{
			"CREATE TABLE t_numeric (id INTEGER primary key, v1 NUMERIC(5,2));",
			"INSERT INTO t_numeric VALUES (1, 123.45), (2, 67.89), (3, 100.3);",
			"CREATE TABLE fract_only (id int, val numeric(4,4));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_numeric ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0188-select-*-from-t_numeric-order"},
			},
			{
				Query: "INSERT INTO fract_only VALUES (1, '0.0');", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0189-insert-into-fract_only-values-1"},
			},
			{
				Query: "SELECT numeric '10.00';", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0190-select-numeric-10.00"},
			},
			{
				Query: "SELECT numeric '-10.00';", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0191-select-numeric-10.00"},
			},
			{
				Query: "select 0.03::numeric(3,3);", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0192-select-0.03::numeric-3-3"},
			},
			{
				Query: "select 1.03::numeric(2,2);", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0193-select-1.03::numeric-2-2", Compare: "sqlstate"},
			},
			{
				Query: "select 1.03::float4::numeric(2,2);", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0194-select-1.03::float4::numeric-2-2", Compare: "sqlstate"},
			},
		},
	},
	{
		Name: "Numeric key",
		SetUpScript: []string{
			"CREATE TABLE t_numeric (id numeric(5,2) primary key, v1 NUMERIC(5,2));",
			"INSERT INTO t_numeric VALUES (123.45, 67.89), (67.89, 100.3);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_numeric;",
				Skip:  true, PostgresOracle: // test setup problem, values are logically equivalent but don't match
				ScriptTestPostgresOracle{ID: "types-test-testtypes-0195-select-*-from-t_numeric"},
			},
			{
				Query: "SELECT * FROM t_numeric WHERE ID = 123.45 ORDER BY id;",
				Skip:  true, PostgresOracle: // value not found
				ScriptTestPostgresOracle{ID: "types-test-testtypes-0196-select-*-from-t_numeric-where"},
			},
		},
	},
	{
		Name: "Numeric type, no scale or precision",
		SetUpScript: []string{
			"CREATE TABLE t_numeric (id INTEGER primary key, v1 NUMERIC);",
			"INSERT INTO t_numeric VALUES (1, 123.45), (2, 67.875), (3, 100.3);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_numeric ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0197-select-*-from-t_numeric-order"},
			},
		},
	},
	{
		Name: "Numeric array type, no scale or precision",
		SetUpScript: []string{
			"CREATE TABLE t_numeric (id INTEGER primary key, v1 NUMERIC[]);",
			"INSERT INTO t_numeric VALUES (1, ARRAY[NULL,123.45]), (2, ARRAY[67.89,572903.1468]);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_numeric ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0198-select-*-from-t_numeric-order"},
			},
		},
	},
	{
		Name: "Oid type",
		SetUpScript: []string{
			"CREATE TABLE t_oid (id INTEGER primary key, v1 OID);",
			"INSERT INTO t_oid VALUES (1, 1234), (2, 5678);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_oid ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0199-select-*-from-t_oid-order"},
			},
			{
				Query: "SELECT * FROM t_oid ORDER BY v1 DESC;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0200-select-*-from-t_oid-order"},
			},
			{
				Query: "UPDATE t_oid SET v1=9012 WHERE id=2;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0201-update-t_oid-set-v1=9012-where"},
			},
			{
				Query: "DELETE FROM t_oid WHERE v1=1234;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0202-delete-from-t_oid-where-v1=1234"},
			},
			{
				Query: "SELECT * FROM t_oid ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0203-select-*-from-t_oid-order"},
			},
			{
				Query: "INSERT INTO t_oid VALUES (3, '2345');", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0204-insert-into-t_oid-values-3"},
			},
			{
				Query: "SELECT * FROM t_oid ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0205-select-*-from-t_oid-order"},
			},
			{
				Query: "INSERT INTO t_oid VALUES (4, 4294967295);", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0206-insert-into-t_oid-values-4"},
			},
			{
				Query: "INSERT INTO t_oid VALUES (5, 4294967296);", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0207-insert-into-t_oid-values-5", Compare: "sqlstate"},
			},
			{
				Query: "INSERT INTO t_oid VALUES (6, 0);", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0208-insert-into-t_oid-values-6"},
			},
			{
				Query: "INSERT INTO t_oid VALUES (7, -1);", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0209-insert-into-t_oid-values-7"},
			},
			{
				Query: "SELECT * FROM t_oid ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0210-select-*-from-t_oid-order"},
			},
			{
				Query: "select oid '20304';", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0211-select-oid-20304"},
			},
		},
	},
	{
		Name: "Oidvector type",
		SetUpScript: []string{
			"CREATE TABLE t_oidvector (id INTEGER primary key, v1 oidvector);",
			"INSERT INTO t_oidvector VALUES (1, '1234 5678 9012'), (2, '556 778 223');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_oidvector ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0212-select-*-from-t_oidvector-order"},
			},
		},
	},
	{
		Name: "Oidvector array type",
		SetUpScript: []string{
			"CREATE TABLE t_oidvector (id INTEGER primary key, v1 oidvector[]);",
			`INSERT INTO t_oidvector VALUES (1, '{"1234 5678 9012", "556 778 223"}');`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_oidvector ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0213-select-*-from-t_oidvector-order"},
			},
		},
	},
	{
		Name: "Vector type",
		SetUpScript: []string{
			"CREATE TABLE t_vector (id INTEGER primary key, v1 vector);",
			"INSERT INTO t_vector VALUES (1, '[1,2,3]'), (2, '[-1.5,0,2.25]');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_vector ORDER BY id;",
				Expected: []sql.Row{
					{1, "[1,2,3]"},
					{2, "[-1.5,0,2.25]"},
				},
			},
			{
				Query: "SELECT '[1,2,3]'::vector = '[1,2,3]'::vector, '[1,2,3]'::vector <> '[1,2,4]'::vector;",
				Expected: []sql.Row{
					{"t", "t"},
				},
			},
			{
				Query:       "SELECT '[1,]'::vector;",
				ExpectedErr: "invalid input syntax for type vector",
			},
			{
				Query:       "SELECT '[]'::vector;",
				ExpectedErr: "invalid input syntax for type vector",
			},
		},
	},
	{
		Name: "Vector typmod",
		SetUpScript: []string{
			"CREATE TABLE t_vector_dim (id INTEGER primary key, v1 vector(3));",
			"INSERT INTO t_vector_dim VALUES (1, '[1,2,3]');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_vector_dim ORDER BY id;",
				Expected: []sql.Row{
					{1, "[1,2,3]"},
				},
			},
			{
				Query:       "INSERT INTO t_vector_dim VALUES (2, '[1,2]');",
				ExpectedErr: "expected 3 dimensions, not 2",
			},
			{
				Query:       "CREATE TABLE t_vector_bad_dim (v1 vector(0));",
				ExpectedErr: "dimensions for type vector must be between 1 and 16000",
			},
		},
	},
	{
		Name: "Vector key",
		SetUpScript: []string{
			"CREATE TABLE t_vector_key (tenant_id vector primary key, label TEXT);",
			"INSERT INTO t_vector_key VALUES ('[1,0]', 'one'), ('[2,0]', 'two');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT label FROM t_vector_key WHERE tenant_id = '[1,0]'::vector;",
				Expected: []sql.Row{
					{"one"},
				},
			},
			{
				Query: "SELECT tenant_id FROM t_vector_key ORDER BY tenant_id;",
				Expected: []sql.Row{
					{"[1,0]"},
					{"[2,0]"},
				},
			},
		},
	},
	{
		Name: "Oid type, explicit casts",
		SetUpScript: []string{
			"CREATE TABLE t_oid (id INTEGER primary key, coid OID);",
			"INSERT INTO t_oid VALUES (1, 1234), (2, 4294967295);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_oid ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0223-select-*-from-t_oid-order"},
			},
			// Cast from OID to types
			{
				Query: "SELECT coid::char(1) FROM t_oid WHERE id=1;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0224-select-coid::char-1-from-t_oid"},
			},
			{
				Query: "SELECT coid::varchar(2) FROM t_oid WHERE id=1;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0225-select-coid::varchar-2-from-t_oid"},
			},
			{
				Query: "SELECT coid::text FROM t_oid WHERE id=1;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0226-select-coid::text-from-t_oid-where"},
			},
			{
				Query: "SELECT coid::smallint FROM t_oid WHERE id=1;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0227-select-coid::smallint-from-t_oid-where", Compare: "sqlstate"},
			},
			{
				Query: "SELECT coid::smallint FROM t_oid WHERE id=2;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0228-select-coid::smallint-from-t_oid-where", Compare: "sqlstate"},
			},
			{
				Query: "SELECT coid::integer FROM t_oid WHERE id=1;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0229-select-coid::integer-from-t_oid-where"},
			},
			{
				Query: "SELECT coid::integer FROM t_oid WHERE id=2;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0230-select-coid::integer-from-t_oid-where"},
			},
			{
				Query: "SELECT coid::bigint FROM t_oid WHERE id=1;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0231-select-coid::bigint-from-t_oid-where"},
			},
			{
				Query: "SELECT coid::name FROM t_oid WHERE id=1;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0232-select-coid::name-from-t_oid-where"},
			},
			{
				Query: "SELECT coid::bigint FROM t_oid WHERE id=2;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0233-select-coid::bigint-from-t_oid-where"},
			},
			{
				Query: "SELECT coid::float4 FROM t_oid WHERE id=1;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0234-select-coid::float4-from-t_oid-where", Compare: "sqlstate"},
			},
			{
				Query: "SELECT coid::float8 FROM t_oid WHERE id=1;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0235-select-coid::float8-from-t_oid-where", Compare: "sqlstate"},
			},
			{
				Query: "SELECT coid::numeric FROM t_oid WHERE id=1;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0236-select-coid::numeric-from-t_oid-where", Compare: "sqlstate"},
			},
			{
				Query: "SELECT coid::xid FROM t_oid WHERE id=1;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0237-select-coid::xid-from-t_oid-where",

					// Cast to OID from types
					Compare: "sqlstate"},
			},

			{
				Query: "SELECT ('123'::char(3))::oid, ('123'::varchar)::oid, ('0'::text)::oid, ('400'::name)::oid;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0238-select-123-::char-3-::oid"},
			},
			{
				Query: "SELECT ('-1'::char(3))::oid, ('-1'::varchar)::oid, ('-1'::text)::oid, ('-1'::name)::oid;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0239-select-1-::char-3-::oid"},
			},
			{
				Query: "SELECT ('-2147483648'::char(11))::oid, ('-2147483648'::varchar)::oid, ('-2147483648'::text)::oid, ('-2147483648'::name)::oid;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0240-select-2147483648-::char-11-::oid"},
			},
			{
				Query: "SELECT (10::int2)::oid, (10::int4)::oid, (100::int8)::oid;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0241-select-10::int2-::oid-10::int4-::oid"},
			},
			{
				Query: "SELECT (-1::int2)::oid, (-1::int4)::oid;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0242-select-1::int2-::oid-1::int4-::oid"},
			},
			{
				Query: "SELECT (-1::int8)::oid;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0243-select-1::int8-::oid", Compare: "sqlstate"},
			},
			{
				Query: "SELECT (922337203685477580::int8)::oid;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0244-select-922337203685477580::int8-::oid", Compare: "sqlstate"},
			},
			{
				Query: "SELECT (1.1::float4)::oid;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0245-select-1.1::float4-::oid", Compare: "sqlstate"},
			},
			{
				Query: "SELECT (1.1::float8)::oid;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0246-select-1.1::float8-::oid", Compare: "sqlstate"},
			},
			{
				Query: "SELECT (1.1::decimal)::oid;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0247-select-1.1::decimal-::oid", Compare: "sqlstate"},
			},
			{
				Query: "SELECT ('922337203685477580'::text)::oid;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0248-select-922337203685477580-::text-::oid", Compare: "sqlstate"},
			},
			{
				Query: "SELECT ('abc'::char(3))::oid;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0249-select-abc-::char-3-::oid", Compare: "sqlstate"},
			},
			{
				Query: "SELECT ('-2147483649'::char(11))::oid;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0250-select-2147483649-::char-11-::oid", Compare: "sqlstate"},
			},
			{
				Query: "SELECT ('-2147483649'::varchar)::oid;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0251-select-2147483649-::varchar-::oid", Compare: "sqlstate"},
			},
			{
				Query: "SELECT ('-2147483649'::text)::oid;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0252-select-2147483649-::text-::oid", Compare: "sqlstate"},
			},
			{
				Query: "SELECT ('-2147483649'::name)::oid;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0253-select-2147483649-::name-::oid", Compare: "sqlstate"},
			},
		},
	},
	{
		Name: "Oid array type",
		SetUpScript: []string{
			"CREATE TABLE t_oid (id INTEGER primary key, v1 OID[], v2 CHARACTER(100), v3 BOOLEAN);",
			"INSERT INTO t_oid VALUES (1, ARRAY[123, 456, 789, 101], '1234567890', true);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT v1::varchar(1)[] FROM t_oid;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0254-select-v1::varchar-1-[]-from"},
			},
			{
				Query: `SELECT v2::oid, v3::oid FROM t_oid;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0255-select-v2::oid-v3::oid-from-t_oid", Compare: "sqlstate"},
			},
		},
	},
	{
		Name: "Path type",
		Skip: true,
		SetUpScript: []string{
			"CREATE TABLE t_path (id INTEGER primary key, v1 PATH);",
			"INSERT INTO t_path VALUES (1, '((1,2),(3,4),(5,6))'), (2, '((7,8),(9,10),(11,12))');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_path ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0256-select-*-from-t_path-order"},
			},
		},
	},
	{
		Name: "Pg_lsn type",
		SetUpScript: []string{
			"CREATE TABLE t_pg_lsn (id INTEGER primary key, v1 PG_LSN);",
			"INSERT INTO t_pg_lsn VALUES (1, '16/B8E36C60'), (2, '16/B8E36C70');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_pg_lsn ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0257-select-*-from-t_pg_lsn-order"},
			},
		},
	},
	{
		Name: "Pg_lsn functions and operators",
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT '0/16AE7F8'::pg_lsn = '0/16AE7F8'::pg_lsn;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0258-select-0/16ae7f8-::pg_lsn-=-0/16ae7f8"},
			},
			{
				Query: "SELECT '0/16AE7F8'::pg_lsn != '0/16AE7F7'::pg_lsn;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0259-select-0/16ae7f8-::pg_lsn-!=-0/16ae7f7"},
			},
			{
				Query: "SELECT '0/16AE7F7'::pg_lsn < '0/16AE7F8'::pg_lsn;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0260-select-0/16ae7f7-::pg_lsn-<-0/16ae7f8"},
			},
			{
				Query: "SELECT '0/16AE7F8'::pg_lsn > '0/16AE7F7'::pg_lsn;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0261-select-0/16ae7f8-::pg_lsn->-0/16ae7f7"},
			},
			{
				Query: "SELECT pg_wal_lsn_diff('0/16AE7F8'::pg_lsn, '0/16AE7F7'::pg_lsn);", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0262-select-pg_wal_lsn_diff-0/16ae7f8-::pg_lsn-0/16ae7f7"},
			},
			{
				Query: "SELECT '0/16AE7F8'::pg_lsn - '0/16AE7F7'::pg_lsn;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0263-select-0/16ae7f8-::pg_lsn-0/16ae7f7-::pg_lsn"},
			},
			{
				Query: "SELECT '0/10'::pg_lsn + 16::numeric, 16::numeric + '0/10'::pg_lsn, '0/10'::pg_lsn - 16::numeric;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0264-select-0/10-::pg_lsn-+-16::numeric"},
			},
			{
				Query: "SELECT pg_lsn_larger('0/1'::pg_lsn, '0/2'::pg_lsn), pg_lsn_smaller('0/1'::pg_lsn, '0/2'::pg_lsn);", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0265-select-pg_lsn_larger-0/1-::pg_lsn-0/2"},
			},
			{
				Query: "SELECT pg_wal_lsn_diff('0/0'::pg_lsn, '0/1'::pg_lsn);", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0266-select-pg_wal_lsn_diff-0/0-::pg_lsn-0/1"},
			},
			{
				Query: "SELECT pg_current_wal_lsn(), pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn();", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0267-select-pg_current_wal_lsn-pg_last_wal_receive_lsn-pg_last_wal_replay_lsn"},
			},
			{
				Query: "SELECT 'G/0'::pg_lsn;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0268-select-g/0-::pg_lsn", Compare: "sqlstate"},
			},
			{
				Query: "SELECT ' 0/12345678'::pg_lsn;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0269-select-0/12345678-::pg_lsn", Compare: "sqlstate"},
			},
		},
	},
	{
		Name: "Point type",
		Skip: true,
		SetUpScript: []string{
			"CREATE TABLE t_point (id INTEGER primary key, v1 POINT);",
			"INSERT INTO t_point VALUES (1, '(1,2)'), (2, '(3,4)');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_point ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0270-select-*-from-t_point-order"},
			},
		},
	},
	{
		Name: "Polygon type",
		Skip: true,
		SetUpScript: []string{
			"CREATE TABLE t_polygon (id INTEGER primary key, v1 POLYGON);",
			"INSERT INTO t_polygon VALUES (1, '((1,2),(3,4),(5,6))'), (2, '((7,8),(9,10),(11,12))');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_polygon ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0271-select-*-from-t_polygon-order"},
			},
		},
	},
	{
		Name: "Real type",
		SetUpScript: []string{
			"CREATE TABLE t_real (id INTEGER primary key, v1 REAL);",
			"INSERT INTO t_real VALUES (1, 123.875), (2, 67.125);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_real ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0272-select-*-from-t_real-order"},
			},
		},
	},
	{
		Name: "Real key",
		SetUpScript: []string{
			"CREATE TABLE t_real (id REAL primary key, v1 REAL);",
			"INSERT INTO t_real VALUES (123.875, 67.125), (67.125, 123.875);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_real WHERE ID = 123.875 ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0273-select-*-from-t_real-where"},
			},
		},
	},
	{
		Name: "Real array type",
		SetUpScript: []string{
			"CREATE TABLE t_real (id INTEGER primary key, v1 REAL[]);",
			"INSERT INTO t_real VALUES (1, ARRAY[NULL,123.875]), (2, ARRAY[67.125, 84256]);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_real ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0274-select-*-from-t_real-order"},
			},
		},
	},
	{
		Name: "Regclass type",
		SetUpScript: []string{
			`CREATE TABLE testing (pk INT primary key, v1 INT UNIQUE);`,
			`CREATE TABLE "Testing2" (pk INT primary key, v1 INT);`,
			`CREATE VIEW testview AS SELECT * FROM testing LIMIT 1;`,
			`CREATE SEQUENCE seq1;`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT 'testing'::regclass;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0275-select-testing-::regclass"},
			},
			{
				Query: `SELECT 'public.testing'::regclass;`,
				Expected: []sql.Row{
					{"testing"},
				},
			},
			{
				Query: `SELECT 'postgres.public.testing'::regclass;`,
				Expected: []sql.Row{
					{"testing"},
				},
			},
			{
				Query:       `SELECT 'doesnotexist.public.testing'::regclass;`,
				ExpectedErr: "database not found",
			},
			{
				Query: `SELECT 'testview'::regclass;`,
				Expected: []sql.Row{
					{"testview"},
				},
			},
			{
				Query: `SELECT ' testing'::regclass;`,
				Expected: []sql.Row{
					{"testing"},
				},
			},
			{
				Query: `SELECT 'seq1'::regclass;`,
				Expected: []sql.Row{
					{"seq1"},
				},
			},
			{
				Query:       `SELECT 'Testing2'::regclass;`,
				ExpectedErr: "does not exist",
			},
			{
				Query: `SELECT '"Testing2"'::regclass;`,
				Expected: []sql.Row{
					{`"Testing2"`},
				},
			},
			{ // This tests that an invalid OID returns itself in string form
				Query: `SELECT 4294967295::regclass;`,
				Expected: []sql.Row{
					{"4294967295"},
				},
			},
			{
				Query: "SELECT relname FROM pg_catalog.pg_class WHERE oid = 'testing'::regclass;",
				Skip:  true, // panic converting string to regclass
				Expected: []sql.Row{
					{"testing"},
				},
			},
			{
				// schema-qualified relation names are not returned if the schema is on the search path
				Query: `SELECT 'public.testing'::regclass, 'public.seq1'::regclass, 'public.testview'::regclass, 'public.testing_pkey'::regclass;`,
				Expected: []sql.Row{
					{"testing", "seq1", "testview", "testing_pkey"},
				},
			},
			{
				// Clear out the current search_path setting to test schema-qualified relation names
				Query:    `SET search_path = '';`,
				Expected: []sql.Row{},
			},
			{
				// Without 'public' on search_path, we get a does not exist error
				Query:       `SELECT 'testing'::regclass;`,
				ExpectedErr: "does not exist",
			},
			{
				Query: `SELECT 'public.testing'::regclass, 'public.seq1'::regclass, 'public.testview'::regclass, 'public.testing_pkey'::regclass;`,
				Expected: []sql.Row{
					{"public.testing", "public.seq1", "public.testview", "public.testing_pkey"},
				},
			},
		},
	},
	{
		Name: "Regproc type",
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT 'acos'::regproc;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0290-select-acos-::regproc"},
			},
			{
				Query: `SELECT ' acos'::regproc;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0291-select-acos-::regproc"},
			},
			{
				Query: `SELECT '"acos"'::regproc;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0292-select-acos-::regproc"},
			},
			{ // This tests that a raw OID properly converts
				Query: `SELECT (('acos'::regproc)::oid)::regproc;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0293-select-acos-::regproc-::oid-::regproc"},
			},
			{ // This tests that a string representing a raw OID converts the same as a raw OID
				Query: `SELECT ((('acos'::regproc)::oid)::text)::regproc;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0294-select-acos-::regproc-::oid-::text"},
			},
			{ // This tests that an invalid OID returns itself in string form
				Query: `SELECT 4294967295::regproc;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0295-select-4294967295::regproc"},
			},
			{
				Query: `SELECT '"Abs"'::regproc;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0296-select-abs-::regproc", Compare: "sqlstate"},
			},
			{
				Query: `SELECT '"acos'::regproc;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0297-select-acos-::regproc", Compare: "sqlstate"},
			},
			{
				Query: `SELECT 'acos"'::regproc;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0298-select-acos-::regproc", Compare: "sqlstate"},
			},
			{
				Query: `SELECT '""acos'::regproc;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0299-select-acos-::regproc", Compare: "sqlstate"},
			},
		},
	},
	{
		Name: "Regprocedure type",
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT 'array_in(cstring,oid,integer)'::regprocedure;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0300-select-array_in-cstring-oid-integer"},
			},
			{
				Query: `SELECT 'array_in(cstring, oid, int4)'::regprocedure::oid = (SELECT typinput::oid FROM pg_catalog.pg_type WHERE typname = '_int4');`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0301-select-array_in-cstring-oid-int4"},
			},
			{
				Query: `SELECT EXISTS (
					SELECT 1
					FROM pg_catalog.pg_type t
					WHERE t.typname = '_int4'
					  AND t.typinput = 'array_in(cstring,oid,integer)'::regprocedure
				);`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0302-select-exists-select-1-from"},
			},
			{
				Query: `SELECT 4294967295::regprocedure;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0303-select-4294967295::regprocedure"},
			},
			{
				Query: `SELECT 'array_in(cstring,oid,does_not_exist)'::regprocedure;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0304-select-array_in-cstring-oid-does_not_exist", Compare: "sqlstate"},
			},
		},
	},
	{
		Name: "Regtype type",
		Assertions: []ScriptTestAssertion{
			{
				Skip:  true, // TODO: Column should be regtype, not "integer"
				Query: `SELECT 'integer'::regtype;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0305-select-integer-::regtype"},
			},
			{
				Query: `SELECT 'integer'::regtype;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0306-select-integer-::regtype"},
			},
			{
				Query: `SELECT 'integer[]'::regtype;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0307-select-integer[]-::regtype"},
			},
			{
				Query: `SELECT 'int4'::regtype;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0308-select-int4-::regtype"},
			},
			{
				Query: `SELECT 'float8'::regtype;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0309-select-float8-::regtype"},
			},
			{
				Query: `SELECT 'character varying'::regtype;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0310-select-character-varying-::regtype"},
			},
			{
				Query: `SELECT '"char"'::regtype;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0311-select-char-::regtype"},
			},
			{
				Query: `SELECT 'char'::regtype;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0312-select-char-::regtype"},
			},
			{
				Query: `SELECT 'char(10)'::regtype;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0313-select-char-10-::regtype"},
			},
			{
				Query: `SELECT '"char"'::regtype::oid;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0314-select-char-::regtype::oid"},
			},
			{
				Query: `SELECT 'char'::regtype::oid;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0315-select-char-::regtype::oid"},
			},
			{
				Query: `SELECT '"char"[]'::regtype;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0316-select-char-[]-::regtype"},
			},
			{
				Query: `SELECT ' integer'::regtype;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0317-select-integer-::regtype"},
			},
			{
				Query: `SELECT '"integer"'::regtype;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0318-select-integer-::regtype",

					// This tests that a raw OID properly converts
					Compare: "sqlstate"},
			},
			{
				Query: `SELECT (('integer'::regtype)::oid)::regtype;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0319-select-integer-::regtype-::oid-::regtype"},
			},
			{ // This tests that a string representing a raw OID converts the same as a raw OID
				Query: `SELECT ((('integer'::regtype)::oid)::text)::regtype;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0320-select-integer-::regtype-::oid-::text"},
			},
			{ // This tests that an invalid OID returns itself in string form
				Query: `SELECT 4294967295::regtype;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0321-select-4294967295::regtype"},
			},
			{
				Query: `SELECT '"Integer"'::regtype;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0322-select-integer-::regtype", Compare: "sqlstate"},
			},
			{
				Query: `SELECT '"integer'::regtype;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0323-select-integer-::regtype", Compare: "sqlstate"},
			},
			{
				Query: `SELECT 'integer"'::regtype;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0324-select-integer-::regtype", Compare: "sqlstate"},
			},
			{
				Query: `SELECT '""integer'::regtype;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0325-select-integer-::regtype", Compare: "sqlstate"},
			},
		},
	},
	{
		Name: "Smallint type",
		SetUpScript: []string{
			"CREATE TABLE t_smallint (id INTEGER primary key, v1 SMALLINT);",
			"INSERT INTO t_smallint VALUES (1, 42), (2, 99);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_smallint ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0326-select-*-from-t_smallint-order"},
			},
		},
	},
	{
		Name: "Int2vector type",
		SetUpScript: []string{
			"CREATE TABLE t_int2vector (id INTEGER primary key, v1 int2vector);",
			"INSERT INTO t_int2vector VALUES (1, '1 2 3'), (2, '6 7 8 9');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_int2vector ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0327-select-*-from-t_int2vector-order"},
			},
			{
				Query: `SELECT unnest(v1) FROM t_int2vector ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0328-select-unnest-v1-from-t_int2vector"},
			},
		},
	},
	{
		Name: "Int2vector array type",
		SetUpScript: []string{
			"CREATE TABLE t_int2vector (id INTEGER primary key, v1 int2vector[]);",
			`INSERT INTO t_int2vector VALUES (1, '{"1 2", "3 4"}');`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_int2vector ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0329-select-*-from-t_int2vector-order"},
			},
			{
				Skip:  true,
				Query: `SELECT unnest(v1) FROM t_int2vector ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0330-select-unnest-v1-from-t_int2vector"},
			},
			{
				Skip:  true,
				Query: `SELECT unnest(unnest(v1)) FROM t_int2vector ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0331-select-unnest-unnest-v1-from"},
			},
		},
	},
	{
		Name: "Smallint key",
		SetUpScript: []string{
			"CREATE TABLE t_smallint (id smallint primary key, v1 SMALLINT);",
			"INSERT INTO t_smallint VALUES (1, 42), (2, 99);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_smallint WHERE ID = 1 ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0332-select-*-from-t_smallint-where"},
			},
		},
	},
	{
		Name: "Smallint array type",
		SetUpScript: []string{
			"CREATE TABLE t_smallint (id INTEGER primary key, v1 SMALLINT[]);",
			"INSERT INTO t_smallint VALUES (1, ARRAY[42,NULL]), (2, ARRAY[99,126]);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_smallint ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0333-select-*-from-t_smallint-order"},
			},
		},
	},
	{
		Name: "Smallserial type",
		SetUpScript: []string{
			"CREATE TABLE t_smallserial (id SERIAL primary key, v1 SMALLSERIAL);",
			"INSERT INTO t_smallserial (v1) VALUES (42), (99);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_smallserial ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0334-select-*-from-t_smallserial-order"},
			},
		},
	},
	{
		Name: "Smallserial key",
		SetUpScript: []string{
			"CREATE TABLE t_smallserial (id smallserial primary key, v1 SMALLSERIAL);",
			"INSERT INTO t_smallserial (v1) VALUES (42), (99);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_smallserial WHERE ID = 1 ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0335-select-*-from-t_smallserial-where"},
			},
		},
	},
	{
		Name: "Serial type",
		SetUpScript: []string{
			"CREATE TABLE t_serial (id SERIAL primary key, v1 SERIAL);",
			"INSERT INTO t_serial (v1) VALUES (123), (456);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_serial ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0336-select-*-from-t_serial-order"},
			},
			{
				Query: "SELECT * FROM t_serial WHERE ID = 2 ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0337-select-*-from-t_serial-where"},
			},
		},
	},
	{
		Name: "Text type",
		SetUpScript: []string{
			// Test a table with a TEXT column
			"CREATE TABLE t_text (id INTEGER primary key, v1 TEXT);",
			"INSERT INTO t_text VALUES (1, 'Hello'), (2, 'World'), (3, ''), (4, NULL);",

			// Test a table created with a TEXT column in a unique, secondary index
			"CREATE TABLE t_text_unique (id INTEGER primary key, v1 TEXT, v2 TEXT NOT NULL UNIQUE);",
			"INSERT INTO t_text_unique VALUES (1, 'Hello', 'Bonjour'), (2, 'World', 'tout le monde'), (3, '', ''), (4, NULL, '!');",
		},
		Assertions: []ScriptTestAssertion{
			{
				// Use the text keyword to cast
				Query: `SELECT text 'text' || ' and unknown';`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0338-select-text-text-||-and"},
			},
			{
				// Use the text keyword to cast
				Query: `SELECT text 'this is a text string' = text 'this is a text string' AS true;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0339-select-text-this-is-a"},
			},
			{
				// Basic select from a table with a TEXT column
				Query: "SELECT * FROM t_text ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0340-select-*-from-t_text-order"},
			},
			{
				// Create a unique, secondary index on a TEXT column
				Query: "CREATE UNIQUE INDEX v1_unique ON t_text(v1);", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0341-create-unique-index-v1_unique-on"},
			},
			{
				Query: "SELECT * FROM t_text WHERE v1 = 'World';", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0342-select-*-from-t_text-where"},
			},
			{
				// Test the new unique constraint on the TEXT column
				Query: "INSERT INTO t_text VALUES (5, 'World');", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0343-insert-into-t_text-values-5", Compare: "sqlstate"},
			},
			{
				Query: "SELECT * FROM t_text_unique WHERE v2 = '!';", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0344-select-*-from-t_text_unique-where"},
			},
			{
				Query: "SELECT * FROM t_text_unique WHERE v2 >= '!' ORDER BY v2;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0345-select-*-from-t_text_unique-where"},
			},
			{
				// Test ordering by TEXT column in a secondary index
				Query: "SELECT * FROM t_text_unique ORDER BY v2;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0346-select-*-from-t_text_unique-order"},
			},
			{
				Query: "SELECT * FROM t_text_unique ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0347-select-*-from-t_text_unique-order"},
			},
			{
				Query: "INSERT INTO t_text_unique VALUES (5, 'Another', 'Bonjour');", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0348-insert-into-t_text_unique-values-5",

					// Create a secondary index over multiple text fields
					Compare: "sqlstate"},
			},
			{

				Query: "CREATE INDEX on t_text_unique(v1, v2);", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0349-create-index-on-t_text_unique-v1"},
			},
			{
				Query: "SELECT id FROM t_text_unique WHERE v1='Hello' and v2='Bonjour';", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0350-select-id-from-t_text_unique-where"},
			},
			{
				// Create a table with a TEXT column to test adding a non-unique, secondary index
				Query: `CREATE TABLE t2 (pk int primary key, c1 TEXT);`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0351-create-table-t2-pk-int"},
			},
			{
				Query: `CREATE INDEX idx1 ON t2(c1);`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0352-create-index-idx1-on-t2"},
			},
			{
				Query: `INSERT INTO t2 VALUES (1, 'one'), (2, 'two');`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0353-insert-into-t2-values-1"},
			},
			{
				Query: `SELECT c1 from t2 order by c1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0354-select-c1-from-t2-order"},
			},
		},
	},
	{
		Name: "Text key",
		SetUpScript: []string{
			"CREATE TABLE t_text (id TEXT primary key, v1 TEXT);",
			"INSERT INTO t_text VALUES ('Hello', 'World'), ('goodbye', 'cruel world');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_text where id = 'goodbye' ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0355-select-*-from-t_text-where"},
			},
		},
	},
	{
		Name: "Time without time zone type",
		SetUpScript: []string{
			"CREATE TABLE t_time_without_zone (id INTEGER primary key, v1 TIME);",
			"INSERT INTO t_time_without_zone VALUES (1, '12:34:56'), (2, '23:45:01'), (3, '02:03 EDT');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_time_without_zone ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0356-select-*-from-t_time_without_zone-order"},
			},
			{
				Query: "SELECT v1::interval FROM t_time_without_zone ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0357-select-v1::interval-from-t_time_without_zone-order"},
			},
			{
				Query: `SELECT '00:00:00'::time;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0358-select-00:00:00-::time"},
			},
			{
				Query: `SELECT '23:59:59.999999'::time;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0359-select-23:59:59.999999-::time"},
			},
			{
				Query: "SELECT time without time zone '040506.789+08';", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0360-select-time-without-time-zone"},
			},
			{
				Query: `SELECT time '04:05:06' + date '2025-07-21';`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0361-select-time-04:05:06-+-date"},
			},
			{
				Query: `SELECT time without time zone '04:05:06' + interval '2 minutes';`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0362-select-time-without-time-zone"},
			},
		},
	},
	{
		Name: "Time without time zone key",
		SetUpScript: []string{
			"CREATE TABLE t_time_without_zone (id TIME primary key, v1 TIME);",
			"INSERT INTO t_time_without_zone VALUES ('12:34:56', '23:45:01'), ('23:45:01', '12:34:56');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_time_without_zone WHERE ID = '12:34:56' ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0363-select-*-from-t_time_without_zone-where"},
			},
		},
	},
	{ // TODO: timezone representation is reported via local time, need to account for that in testing?
		Name: "Time with time zone type",
		SetUpScript: []string{
			"CREATE TABLE t_time_with_zone (id INTEGER primary key, v1 TIME WITH TIME ZONE);",
			"INSERT INTO t_time_with_zone VALUES (1, '12:34:56 UTC'), (2, '23:45:01-0200'), (3, '2025-06-03 02:03 EDT');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_time_with_zone ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0364-select-*-from-t_time_with_zone-order"},
			},
			{
				Query: `SET TIMEZONE TO 'UTC';`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0365-set-timezone-to-utc"},
			},
			{
				Query: `SELECT '00:00:00'::timetz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0366-select-00:00:00-::timetz"},
			},
			{
				Query: `SELECT time with time zone '04:05:06 UTC' + date '2025-07-21';`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0367-select-time-with-time-zone"},
			},
			{
				Query: `SELECT time with time zone '04:05:06 UTC' + interval '2 minutes';`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0368-select-time-with-time-zone"},
			},
			{
				Query: `SET TIMEZONE TO DEFAULT;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0369-set-timezone-to-default"},
			},
			{
				Query: `SELECT '00:00:00-07'::timetz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0370-select-00:00:00-07-::timetz"},
			},
		},
	},
	{
		Name: "Timestamp without time zone type",
		SetUpScript: []string{
			"CREATE TABLE t_timestamp_without_zone (id INTEGER primary key, v1 TIMESTAMP);",
			"INSERT INTO t_timestamp_without_zone VALUES (1, '2022-01-01 12:34:56'), (2, '2022-02-01 23:45:01'), (3, 'Feb 10 5:32PM 1997'), (4, 'Feb 10 16:32:05 99');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_timestamp_without_zone ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0371-select-*-from-t_timestamp_without_zone-order"},
			},
			{
				Query: "SELECT '2000-01-01'::timestamp;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0372-select-2000-01-01-::timestamp"},
			},
			{
				Query: `SELECT '2000-01-01 00:00:00'::timestamp;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0373-select-2000-01-01-00:00:00-::timestamp"},
			},
			{
				Query: `SELECT timestamp without time zone '2025-07-21 04:05:06' + interval '2 minutes';`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0374-select-timestamp-without-time-zone"},
			},
		},
	},
	{
		Name: "Timestamp with time zone type",
		SetUpScript: []string{
			"CREATE TABLE t_timestamp_with_zone (id INTEGER primary key, v1 TIMESTAMP WITH TIME ZONE);",
			"INSERT INTO t_timestamp_with_zone VALUES (1, '2022-01-01 12:34:56 UTC'), (2, '2022-02-01 23:45:01 America/New_York');",
		},
		Assertions: []ScriptTestAssertion{
			{
				// timezone representation is reported via local time, need to account for that in testing
				Query: "SET timezone TO '-04:25'", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0375-set-timezone-to-04:25"},
			},
			{
				Query: "SELECT * FROM t_timestamp_with_zone ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0376-select-*-from-t_timestamp_with_zone-order"},
			},
			{
				Query: "SELECT '2000-01-01'::timestamptz;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0377-select-2000-01-01-::timestamptz"},
			},
			{
				Query: `SELECT '2000-01-01 00:00:00'::timestamptz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0378-select-2000-01-01-00:00:00-::timestamptz"},
			},
			{
				// timezone representation is reported via local time, need to account for that in testing
				Query: "SET timezone TO '-06:00'", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0379-set-timezone-to-06:00"},
			},
			{
				Query: "SELECT * FROM t_timestamp_with_zone ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0380-select-*-from-t_timestamp_with_zone-order"},
			},
			{
				Query: `SELECT timestamp with time zone '2025-07-21 04:05:06 UTC' + interval '2 minutes';`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0381-select-timestamp-with-time-zone"},
			},
			{
				Query: "SET timezone TO default", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0382-set-timezone-to-default"},
			},
		},
	},
	{
		Name: "Tsquery type",
		Skip: true,
		SetUpScript: []string{
			"CREATE TABLE t_tsquery (id INTEGER primary key, v1 TSQUERY);",
			"INSERT INTO t_tsquery VALUES (1, 'word'), (2, 'phrase & (another | term)');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_tsquery ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0383-select-*-from-t_tsquery-order"},
			},
		},
	},
	{
		Name: "Tsvector type",
		Skip: true,
		SetUpScript: []string{
			"CREATE TABLE t_tsvector (id INTEGER primary key, v1 TSVECTOR);",
			"INSERT INTO t_tsvector VALUES (1, 'simple'), (2, 'complex & (query | terms)');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_tsvector ORDER BY id;", PostgresOracle:
				// TODO: output differs from postgres, may need a custom type, not a string
				ScriptTestPostgresOracle{ID: "types-test-testtypes-0384-select-*-from-t_tsvector-order"},
			},
		},
	},
	{
		// This syntax had a very bad error message ("unsupported: this syntax"), this test just assert it's better
		// It can be retired when we support the type.
		Name: "tsvector unsupported error",
		Assertions: []ScriptTestAssertion{
			{
				Query: `CREATE TABLE t_tsvector (id INTEGER primary key, v1 TSVECTOR);`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0385-create-table-t_tsvector-id-integer", Compare: "sqlstate"},
			},
		},
	},
	{
		Name: "Uuid type",
		SetUpScript: []string{
			"CREATE TABLE t_uuid (id INTEGER primary key, v1 UUID);",
			"INSERT INTO t_uuid VALUES (1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'), (2, 'f47ac10b58cc4372a567-0e02b2c3d479');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_uuid ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0386-select-*-from-t_uuid-order"},
			},
			{
				Query: "select uuid 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11';", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0387-select-uuid-a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"},
			},
		},
	},
	{
		Name: "Uuid default value",
		SetUpScript: []string{
			"CREATE TABLE t_uuid (id INTEGER primary key, v1 UUID default 'a1eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid);",
			"INSERT INTO t_uuid VALUES (1, 'f47ac10b58cc4372a567-0e02b2c3d479');",
			"INSERT INTO t_uuid (id) VALUES (2);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_uuid ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0388-select-*-from-t_uuid-order"},
			},
		},
	},
	{
		Name: "Uuid key",
		SetUpScript: []string{
			"CREATE TABLE t_uuid (id UUID primary key, v1 UUID);",
			"INSERT INTO t_uuid VALUES ('f47ac10b58cc4372a567-0e02b2c3d479', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'), ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'f47ac10b58cc4372a567-0e02b2c3d479');",
			"create table t_uuid2 (id int primary key, v1 uuid, v2 uuid);",
			"create index on t_uuid2(v1, v2);",
			"insert into t_uuid2 values " +
				"(1, 'f47ac10b58cc4372a567-0e02b2c3d479', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'), " +
				"(2, 'dcf783c8-49c2-44b4-8b90-34ad8c52ea1e', 'f99802e8-0018-4913-806c-bcad5d246d46');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_uuid WHERE ID = 'f47ac10b58cc4372a567-0e02b2c3d479' ORDER BY id;",
				Expected: []sql.Row{
					{"f47ac10b-58cc-4372-a567-0e02b2c3d479", "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"},
				},
			},
			{
				Query: "SELECT * FROM t_uuid2 WHERE v1 = 'f47ac10b58cc4372a567-0e02b2c3d479' and v2 = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' ORDER BY id;",
				Expected: []sql.Row{
					{1, "f47ac10b-58cc-4372-a567-0e02b2c3d479", "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"},
				},
			},
			{
				Query: "SELECT * FROM t_uuid2 WHERE v1 < 'f47ac10b58cc4372a567-0e02b2c3d479' ORDER BY id;",
				Expected: []sql.Row{
					{2, "dcf783c8-49c2-44b4-8b90-34ad8c52ea1e", "f99802e8-0018-4913-806c-bcad5d246d46"},
				},
			},
		},
	},
	{
		Name: "Uuid array type",
		SetUpScript: []string{
			"CREATE TABLE t_uuid (id INTEGER primary key, v1 UUID[]);",
			"INSERT INTO t_uuid VALUES (1, ARRAY['a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid, NULL]), (2, ARRAY[NULL, 'f47ac10b58cc4372a567-0e02b2c3d479'::uuid]);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_uuid ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0392-select-*-from-t_uuid-order"},
			},
		},
	},
	{
		Name: "Xid type",
		SetUpScript: []string{
			"CREATE TABLE t_xid (id INTEGER primary key, v1 XID, v2 VARCHAR(20));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "INSERT INTO t_xid VALUES (1, 1234, '100');", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0393-insert-into-t_xid-values-1", Compare: "sqlstate"},
			},
			{
				Query: "INSERT INTO t_xid VALUES (1, 1234::xid, '100');", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0394-insert-into-t_xid-values-1", Compare: "sqlstate"},
			},
			{
				Query: "INSERT INTO t_xid VALUES (1, NULL, '100');", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0395-insert-into-t_xid-values-1"},
			},
			{
				Query: "SELECT * FROM t_xid ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0396-select-*-from-t_xid-order"},
			},
			{
				Query: "INSERT INTO t_xid VALUES (2, '100', '101');", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0397-insert-into-t_xid-values-2"},
			},
			{
				Query: "SELECT * FROM t_xid WHERE v1 IS NOT NULL;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0398-select-*-from-t_xid-where"},
			},
			{
				Query: "UPDATE t_xid SET v1='9012' WHERE id=1;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0399-update-t_xid-set-v1=-9012"},
			},
			{
				Query: "DELETE FROM t_xid WHERE v1=100;",
				Skip:  true, PostgresOracle: // TODO: need to implement comparisons, cast interface isn't adequate enough
				ScriptTestPostgresOracle{ID: "types-test-testtypes-0400-delete-from-t_xid-where-v1=100"},
			},
			{
				Query: "SELECT * FROM t_xid ORDER BY v1 DESC;",
				Skip:  true, PostgresOracle: // TODO: should error with "could not identify an ordering operator for type xid"
				ScriptTestPostgresOracle{ID: "types-test-testtypes-0401-select-*-from-t_xid-order", Compare: "sqlstate"},
			},
			{
				Query: "INSERT INTO t_xid VALUES (4, '4294967295', 'a');", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0402-insert-into-t_xid-values-4"},
			},
			{
				Query: "INSERT INTO t_xid VALUES (5, '4294967296', 'b');", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0403-insert-into-t_xid-values-5", Compare: "sqlstate"},
			},
			{
				Query: "INSERT INTO t_xid VALUES (6, '0', 'c');", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0404-insert-into-t_xid-values-6"},
			},
			{
				Query: "INSERT INTO t_xid VALUES (7, '-1', 'd');", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0405-insert-into-t_xid-values-7"},
			},
			{
				Query: "INSERT INTO t_xid VALUES (8, 'abc', 'd');", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0406-insert-into-t_xid-values-8", Compare: "sqlstate"},
			},
			{
				Query: "SELECT * FROM t_xid ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0407-select-*-from-t_xid-order"},
			},
		},
	},
	{
		Name: "Xid type, explicit casts",
		SetUpScript: []string{
			"CREATE TABLE t_xid (id INTEGER primary key, v1 XID);",
			"INSERT INTO t_xid VALUES (1, '1234'), (2, '4294967295');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_xid ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0408-select-*-from-t_xid-order"},
			},
			// Cast from XID to types
			{
				Query: "SELECT v1::char(1), v1::varchar(2), v1::text, v1::name FROM t_xid WHERE id=1;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0409-select-v1::char-1-v1::varchar-2"},
			},
			{
				Query: "SELECT v1::smallint FROM t_xid WHERE id=1;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0410-select-v1::smallint-from-t_xid-where", Compare: "sqlstate"},
			},
			{
				Query: "SELECT v1::integer FROM t_xid WHERE id=1;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0411-select-v1::integer-from-t_xid-where", Compare: "sqlstate"},
			},
			{
				Query: "SELECT v1::bigint FROM t_xid WHERE id=1;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0412-select-v1::bigint-from-t_xid-where", Compare: "sqlstate"},
			},
			{
				Query: "SELECT v1::oid FROM t_xid WHERE id=1;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0413-select-v1::oid-from-t_xid-where", Compare: "sqlstate"},
			},
			{
				Query: "SELECT v1::float4 FROM t_xid WHERE id=1;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0414-select-v1::float4-from-t_xid-where", Compare: "sqlstate"},
			},
			{
				Query: "SELECT v1::float8 FROM t_xid WHERE id=1;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0415-select-v1::float8-from-t_xid-where", Compare: "sqlstate"},
			},
			{
				Query: "SELECT v1::numeric FROM t_xid WHERE id=1;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0416-select-v1::numeric-from-t_xid-where", Compare: "sqlstate"},
			},
			{
				Query: "SELECT v1::boolean FROM t_xid WHERE id=1;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0417-select-v1::boolean-from-t_xid-where",

					// Cast to XID from types
					Compare: "sqlstate"},
			},

			{
				Query: "SELECT ('123'::char(3))::xid, ('123'::varchar)::xid, ('0'::text)::xid, ('400'::name)::xid;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0418-select-123-::char-3-::xid"},
			},
			{
				Query: "SELECT ('-1'::char(3))::xid, ('-1'::varchar)::xid, ('-1'::text)::xid, ('-1'::name)::xid;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0419-select-1-::char-3-::xid"},
			},
			{
				Query: "SELECT ('-2147483648'::char(11))::xid, ('-2147483648'::varchar)::xid, ('-2147483648'::text)::xid, ('-2147483648'::name)::xid;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0420-select-2147483648-::char-11-::xid"},
			},
			{
				Query: "SELECT (10::int2)::xid;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0421-select-10::int2-::xid", Compare: "sqlstate"},
			},
			{
				Query: "SELECT (10::boolean)::xid;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0422-select-10::boolean-::xid", Compare: "sqlstate"},
			},
			{
				Query: "SELECT (10::int4)::xid;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0423-select-10::int4-::xid", Compare: "sqlstate"},
			},
			{
				Query: "SELECT (10::int8)::xid;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0424-select-10::int8-::xid", Compare: "sqlstate"},
			},
			{
				Query: "SELECT (1.1::float4)::xid;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0425-select-1.1::float4-::xid", Compare: "sqlstate"},
			},
			{
				Query: "SELECT (1.1::float8)::xid;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0426-select-1.1::float8-::xid", Compare: "sqlstate"},
			},
			{
				Query: "SELECT (1.1::decimal)::xid;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0427-select-1.1::decimal-::xid", Compare: "sqlstate"},
			},
			{
				Query: "SELECT ('4294967295'::text)::xid, ('4294967297'::text)::xid;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0428-select-4294967295-::text-::xid-4294967297", Compare: "sqlstate"},
			},
			{
				Query: "SELECT ('-4294967295'::text)::xid, ('-4294967297'::text)::xid;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0429-select-4294967295-::text-::xid-4294967297", Compare: "sqlstate"},
			},
			{
				Query: "SELECT ('4294967295'::varchar)::xid, ('4294967296232'::varchar)::xid;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0430-select-4294967295-::varchar-::xid-4294967296232", Compare: "sqlstate"},
			},
			{
				Query: "SELECT ('-4294967295'::varchar)::xid, ('-4294967296232'::varchar)::xid;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0431-select-4294967295-::varchar-::xid-4294967296232", Compare: "sqlstate"},
			},
			{
				Query: "SELECT ('4294967295'::char(11))::xid, ('4294967296'::char(11))::xid;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0432-select-4294967295-::char-11-::xid", Compare: "sqlstate"},
			},
			{
				Query: "SELECT ('4294967295'::name)::xid, ('4294967296'::name)::xid;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0433-select-4294967295-::name-::xid-4294967296", Compare: "sqlstate"},
			},
			{
				Query: "SELECT ('abc'::text)::xid, ('abc'::char(3))::xid, ('abc'::varchar)::xid, ('abc'::name)::xid;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0434-select-abc-::text-::xid-abc", Compare: "sqlstate"},
			},
		},
	},
	{
		Name: "Xid array type",
		SetUpScript: []string{
			"CREATE TABLE t_xid (id INTEGER primary key, v1 XID[], v2 CHARACTER(100), v3 BOOLEAN);",
			"INSERT INTO t_xid VALUES (2, '{123, 456, 789, 101}', '1234567890', true);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT v1::varchar(1)[] FROM t_xid;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0435-select-v1::varchar-1-[]-from"},
			},
			{
				Query: `INSERT INTO t_xid VALUES (2, ARRAY[123, 456, 789, 101], '1234567890', true);`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0436-insert-into-t_xid-values-2", Compare: "sqlstate"},
			},
		},
	},
	{
		Name: "Xml type",
		Skip: true,
		SetUpScript: []string{
			"CREATE TABLE t_xml (id INTEGER primary key, v1 XML);",
			"INSERT INTO t_xml VALUES (1, '<note><to>Tove</to><from>Jani</from><body>Don''t forget me this weekend!</body></note>'), (2, '<book><title>Introduction to Golang</title><author>John Doe</author></book>');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_xml ORDER BY id;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0437-select-*-from-t_xml-order"},
			},
		},
	},
	{
		Name: "Polymorphic types",
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT array_append(ARRAY[1], 2);", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0438-select-array_append-array[1]-2"},
			},
			{
				Query: "SELECT array_append(ARRAY['abc','def'], 'ghi');", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0439-select-array_append-array[-abc-def"},
			},
			{
				Query: "SELECT array_append(ARRAY['abc','def'], null);", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0440-select-array_append-array[-abc-def"},
			},
			{
				Query: "SELECT array_append(null, null);", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0441-select-array_append-null-null"},
			},
			{
				Query: "SELECT array_append(null, 'ghi');", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0442-select-array_append-null-ghi"},
			},
			{
				Query: "SELECT array_append(null, 3);", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0443-select-array_append-null-3"},
			},
			{
				Query: "SELECT array_append(1, 2);", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0444-select-array_append-1-2", Compare: "sqlstate"},
			},
			{
				Query: "SELECT array_append(1, ARRAY[2]);", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0445-select-array_append-1-array[2]", Compare: "sqlstate"},
			},
			{
				Query: "SELECT array_append(ARRAY[1], ARRAY[2]);", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testtypes-0446-select-array_append-array[1]-array[2]", Compare: "sqlstate"},
			},
		},
	},
}

func TestSameTypes(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "Integer types",
			SetUpScript: []string{
				"CREATE TABLE test1 (v1 SMALLINT, v2 INTEGER, v3 BIGINT);",
				"CREATE TABLE test2 (v1 INT2, v2 INT4, v3 INT8);",
				"INSERT INTO test1 VALUES (1, 2, 3), (4, 5, 6);",
				"INSERT INTO test2 VALUES (1, 2, 3), (4, 5, 6);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT * FROM test1 ORDER BY 1;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testsametypes-0001-select-*-from-test1-order"},
				},
				{
					Query: "SELECT * FROM test1 ORDER BY v1;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testsametypes-0002-select-*-from-test1-order"},
				},
				{
					Query: "SELECT * FROM test2 ORDER BY 1;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testsametypes-0003-select-*-from-test2-order"},
				},
				{
					Query: "SELECT * FROM test2 ORDER BY v1;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testsametypes-0004-select-*-from-test2-order"},
				},
				{
					Query: "select int2 '2', int4 '3', int8 '4'", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testsametypes-0005-select-int2-2-int4-3"},
				},
			},
		},
		{
			Name: "Arbitrary precision types",
			SetUpScript: []string{
				"CREATE TABLE test (v1 DECIMAL(10, 1), v2 NUMERIC(11, 2));",
				"INSERT INTO test VALUES (14854.5, 2504.25), (566821525.5, 735134574.75), (21525, 134574.7);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT * FROM test ORDER BY 1;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testsametypes-0006-select-*-from-test-order"},
				},
			},
		},
		{
			Name: "Floating point types",
			SetUpScript: []string{
				"CREATE TABLE test1 (v1 REAL, v2 DOUBLE PRECISION);",
				"CREATE TABLE test2 (v1 FLOAT4, v2 FLOAT8);",
				"INSERT INTO test1 VALUES (10.125, 20.4), (40.875, 81.6);",
				"INSERT INTO test2 VALUES (10.125, 20.4), (40.875, 81.6);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT * FROM test1 ORDER BY 1;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testsametypes-0007-select-*-from-test1-order"},
				},
				{
					Query: "SELECT * FROM test2 ORDER BY 1;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testsametypes-0008-select-*-from-test2-order"},
				},
			},
		},
		{
			// TIME has the same name, but operates a bit differently, so it's not included as a "same type"
			Name: "Date and time types",
			SetUpScript: []string{
				"CREATE TABLE test (v1 TIMESTAMP, v2 DATE);",
				"INSERT INTO test VALUES ('1986-08-02 17:04:22', '2023-09-03');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT * FROM test ORDER BY 1;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testsametypes-0009-select-*-from-test-order"},
				},
			},
		},
		{
			// ENUM exists, but features too many differences to incorporate as a "same type"
			// BLOB exists, but functions as a BYTEA, which operates differently than a BINARY/VARBINARY in MySQL
			Name: "Text types",
			SetUpScript: []string{
				"CREATE TABLE test (v1 CHARACTER VARYING(255), v2 CHARACTER(3), v3 TEXT);",
				"INSERT INTO test VALUES ('abc', 'def', 'ghi'), ('jkl', 'mno', 'pqr');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT * FROM test ORDER BY 1;", PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testsametypes-0010-select-*-from-test-order"},
				},
			},
		},
	})
}

func TestEnumTypes(t *testing.T) {
	RunScripts(t, enumTypeTests)
}

var enumTypeTests = []ScriptTest{
	{
		Name: "create enum type",
		SetUpScript: []string{
			`CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `CREATE TABLE person (name text, current_mood mood);`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testenumtypes-0001-create-table-person-name-text"},
			},
			{
				Query: `INSERT INTO person VALUES ('Moe', 'happy'), ('Larry', 'sad'), ('Curly', 'ok');`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testenumtypes-0002-insert-into-person-values-moe"},
			},
			{
				Query: `SELECT 'happy'::mood;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testenumtypes-0003-select-happy-::mood"},
			},
			{
				Query: `SELECT current_mood::mood from person where name = 'Moe';`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testenumtypes-0004-select-current_mood::mood-from-person-where"},
			},
			{
				Query: `SELECT * FROM person order by current_mood;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testenumtypes-0005-select-*-from-person-order"},
			},
			{
				Query: `SELECT * FROM person order by name;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testenumtypes-0006-select-*-from-person-order"},
			},
			{
				Query: `SELECT * FROM person;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testenumtypes-0007-select-*-from-person"},
			},
			{
				Query: `SELECT * FROM person WHERE current_mood = 'happy';`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testenumtypes-0008-select-*-from-person-where"},
			},
			{
				Query: `SELECT * FROM person WHERE current_mood > 'sad';`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testenumtypes-0009-select-*-from-person-where"},
			},
			{
				Query: `SELECT * FROM person WHERE current_mood > 'sad' ORDER BY current_mood;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testenumtypes-0010-select-*-from-person-where"},
			},
			{
				Query: `INSERT INTO person VALUES ('Joey', 'invalid');`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testenumtypes-0011-insert-into-person-values-joey", Compare: "sqlstate"},
			},
			{
				Query: `CREATE TYPE failure AS ENUM ('ok','ok');`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testenumtypes-0012-create-type-failure-as-enum", Compare: "sqlstate"},
			},
			{
				Query: `CREATE TYPE empty_mood AS ENUM ();`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testenumtypes-0013-create-type-empty_mood-as-enum"},
			},
		},
	},
	{
		Name: "drop enum type",
		SetUpScript: []string{
			`CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')`,
			`CREATE TYPE empty_enum AS ENUM ()`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `DROP TYPE mood, empty_enum;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testenumtypes-0014-drop-type-mood-empty_enum", Compare: "sqlstate"},
			},
			{
				Query: `DROP TYPE empty_enum;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testenumtypes-0015-drop-type-empty_enum"},
			},
			{
				Query: `DROP TYPE empty_enum;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testenumtypes-0016-drop-type-empty_enum", Compare: "sqlstate"},
			},
			{
				Query: `DROP TYPE IF EXISTS empty_enum;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testenumtypes-0017-drop-type-if-exists-empty_enum"},
			},
			{
				Query: `DROP TYPE _mood;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testenumtypes-0018-drop-type-mood", Compare: "sqlstate"},
			},
		},
	},
	{
		Name: "enum type cast",
		SetUpScript: []string{
			`CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `select 'sad'::mood`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testenumtypes-0019-select-sad-::mood"},
			},
			{
				Query: `select 'invalid'::mood`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testenumtypes-0020-select-invalid-::mood", Compare: "sqlstate"},
			},
		},
	},
	{
		Skip: true,
		Name: "enum type function",
		SetUpScript: []string{
			`CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')`,
		},
		Assertions: []ScriptTestAssertion{
			{
				// oid of type 'mood' = 16675
				Query: `select enum_in('sad'::cstring, 16675);`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testenumtypes-0021-select-enum_in-sad-::cstring-16675", Compare: "sqlstate"},
			},
		},
	},
	{
		Skip: true,
		Name: "create type with existing array type name updates the name of the array type",
		Assertions: []ScriptTestAssertion{
			{
				Query: `CREATE TYPE my_type AS ENUM ();`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testenumtypes-0022-create-type-my_type-as-enum"},
			},
			{
				Query: `CREATE TYPE _my_type;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testenumtypes-0023-create-type-my_type"},
			},
			{
				Query: `SELECT typname from pg_type where typname like '%my_type'`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testenumtypes-0024-select-typname-from-pg_type-where"},
			},
			{
				Query: `DROP TYPE my_type;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testenumtypes-0025-drop-type-my_type"},
			},
			{
				Query: `DROP TYPE _my_type;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testenumtypes-0026-drop-type-my_type"},
			},
		},
	},
}

func TestShellTypes(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "shell type use cases",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TYPE undefined_type;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testshelltypes-0001-create-type-undefined_type"},
				},
				{
					Query: `select 1::undefined_type;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testshelltypes-0002-select-1::undefined_type", Compare: "sqlstate"},
				},
				{
					Query: `DROP TYPE undefined_type;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testshelltypes-0003-drop-type-undefined_type"},
				},
				{
					Query: `DROP TYPE IF EXISTS undefined_type;`, PostgresOracle: ScriptTestPostgresOracle{ID: "types-test-testshelltypes-0004-drop-type-if-exists-undefined_type"},
				},
			},
		},
	})
}
