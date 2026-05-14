// Copyright 2024 Dolthub, Inc.
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

func TestSequences(t *testing.T) {
	// special setup script that drops and creates a table >1k times to ensure that sequence
	// name generation still works
	dropAndCreateTableSetUpScript := []string{
		"create table serial_table (pk serial primary key);",
		"drop table serial_table;",
	}
	for i := 0; i < 10; i++ {
		dropAndCreateTableSetUpScript = append(dropAndCreateTableSetUpScript, dropAndCreateTableSetUpScript...)
	}

	RunScripts(t, []ScriptTest{
		{
			Name: "Basic CREATE SEQUENCE and DROP SEQUENCE",
			Assertions: []ScriptTestAssertion{
				{
					Query: "CREATE SEQUENCE test;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0001-create-sequence-test"},
				},
				{
					Query: "SELECT nextval('test');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0002-select-nextval-test"},
				},
				{
					Query: "SELECT nextval('test');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0003-select-nextval-test"},
				},
				{
					Query: "SELECT nextval('test');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0004-select-nextval-test"},
				},
				{
					Query: "SELECT nextval('test'::regclass);", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0005-select-nextval-test-::regclass"},
				},
				{
					Query: "SELECT nextval('doesnotexist'::regclass);", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0006-select-nextval-doesnotexist-::regclass", Compare: "sqlstate"},
				},
				{
					Query: "DROP SEQUENCE test;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0007-drop-sequence-test"},
				},
				{
					Query: "SELECT nextval('test');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0008-select-nextval-test"},
				},
			},
		},
		{
			Name: "CREATE SEQUENCE IF NOT EXISTS",
			Assertions: []ScriptTestAssertion{
				{
					Query: "CREATE SEQUENCE test1;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0009-create-sequence-test1"},
				},
				{
					Query: "CREATE SEQUENCE test1;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0010-create-sequence-test1", Compare: "sqlstate"},
				},
				{
					Query: "SELECT nextval('test1');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0011-select-nextval-test1"},
				},
				{
					Query: "CREATE SEQUENCE IF NOT EXISTS test1;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0012-create-sequence-if-not-exists"},
				},
				{
					Query: "SELECT nextval('test1');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0013-select-nextval-test1"},
				},
				{
					Query: "CREATE SEQUENCE IF NOT EXISTS test2;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0014-create-sequence-if-not-exists"},
				},
				{
					Query: "SELECT nextval('test2');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0015-select-nextval-test2"},
				},
				{
					Query: "CREATE SEQUENCE IF NOT EXISTS test2;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0016-create-sequence-if-not-exists"},
				},
				{
					Query: "SELECT nextval('test2');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0017-select-nextval-test2"},
				},
			},
		},
		{
			Name: "DROP SEQUENCE IF NOT EXISTS",
			Assertions: []ScriptTestAssertion{
				{
					Query: "CREATE SEQUENCE test1;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0018-create-sequence-test1", Compare: "sqlstate"},
				},
				{
					Query: "CREATE SEQUENCE test2;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0019-create-sequence-test2"},
				},
				{
					Query: "SELECT nextval('test1');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0020-select-nextval-test1"},
				},
				{
					Query: "SELECT nextval('test2');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0021-select-nextval-test2"},
				},
				{
					Query: "DROP SEQUENCE test1;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0022-drop-sequence-test1"},
				},
				{
					Query: "DROP SEQUENCE test1;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0023-drop-sequence-test1", Compare: "sqlstate"},
				},
				{
					Query: "DROP SEQUENCE IF EXISTS test1;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0024-drop-sequence-if-exists-test1"},
				},
				{
					Query: "SELECT nextval('test1');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0025-select-nextval-test1", Compare: "sqlstate"},
				},
				{
					Query: "SELECT nextval('test2');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0026-select-nextval-test2"},
				},
				{
					Query: "DROP SEQUENCE IF EXISTS test2;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0027-drop-sequence-if-exists-test2"},
				},
				{
					Query: "SELECT nextval('test2');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0028-select-nextval-test2", Compare: "sqlstate"},
				},
				{
					Query: "DROP SEQUENCE IF EXISTS test2;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0029-drop-sequence-if-exists-test2"},
				},
			},
		},
		{
			Name: "MINVALUE and MAXVALUE with DATA TYPE",
			Assertions: []ScriptTestAssertion{
				{
					Query: "CREATE SEQUENCE test1 AS SMALLINT MINVALUE -32768;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0030-create-sequence-test1-as-smallint"},
				},
				{
					Query: "CREATE SEQUENCE test2 AS SMALLINT MINVALUE -32769;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0031-create-sequence-test2-as-smallint", Compare: "sqlstate"},
				},
				{
					Query: "CREATE SEQUENCE test3 AS SMALLINT MAXVALUE 32767;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0032-create-sequence-test3-as-smallint"},
				},
				{
					Query: "CREATE SEQUENCE test4 AS SMALLINT MINVALUE 32768;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0033-create-sequence-test4-as-smallint", Compare: "sqlstate"},
				},
				{
					Query: "CREATE SEQUENCE test5 AS INTEGER MINVALUE -2147483648;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0034-create-sequence-test5-as-integer"},
				},
				{
					Query: "CREATE SEQUENCE test6 AS INTEGER MINVALUE -2147483649;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0035-create-sequence-test6-as-integer", Compare: "sqlstate"},
				},
				{
					Query: "CREATE SEQUENCE test7 AS INTEGER MAXVALUE 2147483647;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0036-create-sequence-test7-as-integer"},
				},
				{
					Query: "CREATE SEQUENCE test8 AS INTEGER MINVALUE 2147483648;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0037-create-sequence-test8-as-integer", Compare: "sqlstate"},
				},
				{
					Query: "CREATE SEQUENCE test9 AS BIGINT MINVALUE -9223372036854775808;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0038-create-sequence-test9-as-bigint"},
				},
				{
					Query: "CREATE SEQUENCE test10 AS BIGINT MINVALUE -9223372036854775809;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0039-create-sequence-test10-as-bigint", Compare: "sqlstate"},
				},
				{
					Query: "CREATE SEQUENCE test11 AS BIGINT MAXVALUE 9223372036854775807;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0040-create-sequence-test11-as-bigint"},
				},
				{
					Query: "CREATE SEQUENCE test12 AS BIGINT MINVALUE 9223372036854775808;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0041-create-sequence-test12-as-bigint", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "CREATE SEQUENCE START",
			Assertions: []ScriptTestAssertion{
				{
					Query: "CREATE SEQUENCE test1 START 39;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0042-create-sequence-test1-start-39", Compare: "sqlstate"},
				},
				{
					Query: "SELECT nextval('test1');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0043-select-nextval-test1"},
				},
				{
					Query: "CREATE SEQUENCE test2 START 0;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0044-create-sequence-test2-start-0", Compare: "sqlstate"},
				},
				{
					Query: "CREATE SEQUENCE test2 MINVALUE 0 START 0;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0045-create-sequence-test2-minvalue-0"},
				},
				{
					Query: "SELECT nextval('test2');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0046-select-nextval-test2"},
				},
				{
					Query: "CREATE SEQUENCE test3 MINVALUE -100 START -7;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0047-create-sequence-test3-minvalue-100"},
				},
				{
					Query: "SELECT nextval('test3');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0048-select-nextval-test3"},
				},
				{
					Query: "CREATE SEQUENCE test4 START -5 INCREMENT 1;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0049-create-sequence-test4-start-5", Compare: "sqlstate"},
				},
				{
					Query: "CREATE SEQUENCE test4 START -5 INCREMENT -1;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0050-create-sequence-test4-start-5"},
				},
				{
					Query: "SELECT nextval('test4');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0051-select-nextval-test4"},
				},
				{
					Query: "CREATE SEQUENCE test5 START 25 INCREMENT -1;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0052-create-sequence-test5-start-25", Compare: "sqlstate"},
				},
				{
					Query: "CREATE SEQUENCE test5 START 25 MAXVALUE 25 INCREMENT -1;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0053-create-sequence-test5-start-25"},
				},
				{
					Query: "SELECT nextval('test5');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0054-select-nextval-test5"},
				},
				{
					Query: "SELECT nextval('test5');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0055-select-nextval-test5"},
				},
			},
		},
		{
			Name: "CYCLE and NO CYCLE",
			Assertions: []ScriptTestAssertion{
				{
					Query: "CREATE SEQUENCE test1 MINVALUE 0 MAXVALUE 3 START 2 INCREMENT 1 NO CYCLE;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0056-create-sequence-test1-minvalue-0", Compare: "sqlstate"},
				},
				{
					Query: "SELECT nextval('test1');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0057-select-nextval-test1"},
				},
				{
					Query: "SELECT nextval('test1');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0058-select-nextval-test1"},
				},
				{
					Query: "SELECT nextval('test1');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0059-select-nextval-test1", Compare: "sqlstate"},
				},
				{
					Query: "CREATE SEQUENCE test2 MINVALUE 0 MAXVALUE 3 START 2 INCREMENT 1 CYCLE;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0060-create-sequence-test2-minvalue-0"},
				},
				{
					Query: "SELECT nextval('test2');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0061-select-nextval-test2"},
				},
				{
					Query: "SELECT nextval('test2');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0062-select-nextval-test2"},
				},
				{
					Query: "SELECT nextval('test2');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0063-select-nextval-test2"},
				},
				{
					Query: "CREATE SEQUENCE test3 MINVALUE 0 MAXVALUE 3 START 1 INCREMENT -1 NO CYCLE;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0064-create-sequence-test3-minvalue-0"},
				},
				{
					Query: "SELECT nextval('test3');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0065-select-nextval-test3"},
				},
				{
					Query: "SELECT nextval('test3');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0066-select-nextval-test3"},
				},
				{
					Query: "SELECT nextval('test3');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0067-select-nextval-test3", Compare: "sqlstate"},
				},
				{
					Query: "CREATE SEQUENCE test4 MINVALUE 0 MAXVALUE 3 START 1 INCREMENT -1 CYCLE;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0068-create-sequence-test4-minvalue-0"},
				},
				{
					Query: "SELECT nextval('test4');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0069-select-nextval-test4"},
				},
				{
					Query: "SELECT nextval('test4');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0070-select-nextval-test4"},
				},
				{
					Query: "SELECT nextval('test4');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0071-select-nextval-test4"},
				},
				{
					Query: "CREATE SEQUENCE test5 MINVALUE 1 MAXVALUE 7 START 1 INCREMENT 5 CYCLE;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0072-create-sequence-test5-minvalue-1"},
				},
				{
					Query: "SELECT nextval('test5');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0073-select-nextval-test5"},
				},
				{
					Query: "SELECT nextval('test5');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0074-select-nextval-test5"},
				},
				{
					Query: "SELECT nextval('test5');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0075-select-nextval-test5"},
				},
				{
					Query: "CREATE SEQUENCE test6 MINVALUE 1 MAXVALUE 7 START 6 INCREMENT -5 CYCLE;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0076-create-sequence-test6-minvalue-1"},
				},
				{
					Query: "SELECT nextval('test6');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0077-select-nextval-test6"},
				},
				{
					Query: "SELECT nextval('test6');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0078-select-nextval-test6"},
				},
				{
					Query: "SELECT nextval('test6');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0079-select-nextval-test6"},
				},
				{
					Query: "SELECT nextval('test6');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0080-select-nextval-test6"},
				},
			},
		},
		{
			Name: "nextval() over multiple rows/columns",
			Assertions: []ScriptTestAssertion{
				{
					Query: "CREATE TABLE test (v1 INTEGER, v2 INTEGER);", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0081-create-table-test-v1-integer"},
				},
				{
					Query: "CREATE SEQUENCE seq1;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0082-create-sequence-seq1"},
				},
				{
					Query: "INSERT INTO test VALUES (nextval('seq1'), 7), (nextval('seq1'), 11), (nextval('seq1'), 17);", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0083-insert-into-test-values-nextval"},
				},
				{
					Query: "SELECT * FROM test ORDER BY v1;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0084-select-*-from-test-order"},
				},
				{
					Query: "INSERT INTO test VALUES (nextval('seq1'), nextval('seq1')), (nextval('seq1'), nextval('seq1'));", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0085-insert-into-test-values-nextval"},
				},
				{
					Query: "SELECT * FROM test ORDER BY v1;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0086-select-*-from-test-order"},
				},
			},
		},
		{
			Name: "nextval() with double-quoted identifiers",
			SetUpScript: []string{
				"CREATE SEQUENCE test_sequence;",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT nextval('test_sequence');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0087-select-nextval-test_sequence"},
				},
				{
					Query: "SELECT nextval('public.test_sequence');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0088-select-nextval-public.test_sequence", Cleanup: []string{"DROP SEQUENCE IF EXISTS test_sequence CASCADE"}},
				},
				{
					Query: `SELECT nextval('"test_sequence"');`, PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0089-select-nextval-test_sequence", Cleanup: []string{"DROP SEQUENCE IF EXISTS test_sequence CASCADE"}},
				},
				{
					Query: `SELECT nextval('public."test_sequence"');`, PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0090-select-nextval-public.-test_sequence", Cleanup: []string{"DROP SEQUENCE IF EXISTS test_sequence CASCADE"}},
				},
			},
		},
		{
			Name: "nextval() in filter",
			Skip: true, // GMS seems to call nextval once and cache the value, which is incorrect here
			SetUpScript: []string{
				"CREATE TABLE test_serial (v1 SERIAL, v2 INTEGER);",
				"INSERT INTO test_serial (v2) VALUES (4), (5), (6);",
				"CREATE TABLE test_seq (v1 INTEGER, v2 INTEGER);",
				"CREATE SEQUENCE test_sequence OWNED BY test_seq.v1;",
				"INSERT INTO test_seq VALUES (nextval('test_sequence'), 4), (nextval('test_sequence'), 5), (nextval('test_sequence'), 6);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT * FROM test_serial WHERE nextval('test_serial_v1_seq') = v2 ORDER BY v1;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0091-select-*-from-test_serial-where"},
				},
				{
					Query: "SELECT nextval('test_serial_v1_seq');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0092-select-nextval-test_serial_v1_seq"},
				},
				{
					Query: "SELECT * FROM test_seq WHERE nextval('test_sequence') = v2 ORDER BY v1;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0093-select-*-from-test_seq-where"},
				},
				{
					Query: "SELECT nextval('test_sequence');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0094-select-nextval-test_sequence"},
				},
			},
		},
		{
			Name: "setval()",
			Assertions: []ScriptTestAssertion{
				{
					Query: "CREATE SEQUENCE test1 MINVALUE 1 MAXVALUE 10 START 5 INCREMENT 1;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0095-create-sequence-test1-minvalue-1", Compare: "sqlstate"},
				},
				{
					Query: "CREATE SEQUENCE test2 MINVALUE 1 MAXVALUE 10 START 5 INCREMENT -1;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0096-create-sequence-test2-minvalue-1"},
				},
				{
					Query: "SELECT setval('test1', 2);", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0097-select-setval-test1-2"},
				},
				{
					Query: "SELECT nextval('test1');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0098-select-nextval-test1"},
				},
				{
					Query: "SELECT setval('test1', 10);", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0099-select-setval-test1-10"},
				},
				{
					Query: "SELECT nextval('test1');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0100-select-nextval-test1", Compare: "sqlstate"},
				},
				{
					Query: "SELECT setval('test1', 10, false);", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0101-select-setval-test1-10-false"},
				},
				{
					Query: "SELECT nextval('test1');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0102-select-nextval-test1"},
				},
				{
					Query: "SELECT setval('test1', 10, true);", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0103-select-setval-test1-10-true"},
				},
				{
					Query: "SELECT nextval('test1');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0104-select-nextval-test1", Compare: "sqlstate"},
				},
				{
					Query: "SELECT setval('test2', 9);", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0105-select-setval-test2-9"},
				},
				{
					Query: "SELECT nextval('test2');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0106-select-nextval-test2"},
				},
				{
					Query: "SELECT setval('test2', 1);", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0107-select-setval-test2-1"},
				},
				{
					Query: "SELECT nextval('test2');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0108-select-nextval-test2", Compare: "sqlstate"},
				},
				{
					Query: "SELECT setval('test2', 1, false);", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0109-select-setval-test2-1-false"},
				},
				{
					Query: "SELECT nextval('test2');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0110-select-nextval-test2"},
				},
				{
					Query: "SELECT setval('test2', 1, true);", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0111-select-setval-test2-1-true"},
				},
				{
					Query: "SELECT nextval('test2');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0112-select-nextval-test2", Compare: "sqlstate"},
				},
				{
					Query: "CREATE SEQUENCE test3 MINVALUE 3 MAXVALUE 7 START 5 INCREMENT 1 CYCLE;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0113-create-sequence-test3-minvalue-3"},
				},
				{
					Query: "CREATE SEQUENCE test4 MINVALUE 3 MAXVALUE 7 START 5 INCREMENT -1 CYCLE;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0114-create-sequence-test4-minvalue-3"},
				},
				{
					Query: "SELECT setval('test3', 7, true);", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0115-select-setval-test3-7-true"},
				},
				{
					Query: "SELECT nextval('test3');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0116-select-nextval-test3"},
				},
				{
					Query: "SELECT setval('test4', 3, true);", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0117-select-setval-test4-3-true"},
				},
				{
					Query: "SELECT nextval('test4');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0118-select-nextval-test4"},
				},
				{
					Query: "CREATE SEQUENCE test5;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0119-create-sequence-test5"},
				},
				{
					// test with a double-quoted identifier
					Query: `SELECT setval('public."test5"', 100, true);`, PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0120-select-setval-public.-test5-100", Cleanup: []string{"DROP SEQUENCE IF EXISTS test2 CASCADE", "DROP SEQUENCE IF EXISTS test3 CASCADE", "DROP SEQUENCE IF EXISTS test4 CASCADE", "DROP SEQUENCE IF EXISTS test5 CASCADE"}},
				},
				{
					Query: "SELECT nextval('test5');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0121-select-nextval-test5", Cleanup: []string{"DROP SEQUENCE IF EXISTS test2 CASCADE", "DROP SEQUENCE IF EXISTS test3 CASCADE", "DROP SEQUENCE IF EXISTS test4 CASCADE", "DROP SEQUENCE IF EXISTS test5 CASCADE"}},
				},
			},
		},
		{
			Name: "SERIAL",
			Assertions: []ScriptTestAssertion{
				{
					Query: "CREATE TABLE test (pk SERIAL PRIMARY KEY, v1 INTEGER);", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0122-create-table-test-pk-serial"},
				},
				{
					Query: "CREATE TABLE test_small (pk SMALLSERIAL PRIMARY KEY, v1 INTEGER);", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0123-create-table-test_small-pk-smallserial"},
				},
				{
					Query: "CREATE TABLE test_big (pk BIGSERIAL PRIMARY KEY, v1 INTEGER);", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0124-create-table-test_big-pk-bigserial"},
				},
				{
					Query: "INSERT INTO test (v1) VALUES (2), (3), (5), (7), (11);", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0125-insert-into-test-v1-values"},
				},
				{
					Query: "INSERT INTO test_small (v1) VALUES (2), (3), (5), (7), (11);", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0126-insert-into-test_small-v1-values"},
				},
				{
					Query: "INSERT INTO test_big (v1) VALUES (2), (3), (5), (7), (11);", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0127-insert-into-test_big-v1-values"},
				},
				{
					Query: "SELECT * FROM test;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0128-select-*-from-test"},
				},
				{
					Query: "SELECT * FROM test_small;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0129-select-*-from-test_small"},
				},
				{
					Query: "SELECT * FROM test_big;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0130-select-*-from-test_big"},
				},
			},
		},
		{
			Name: "SERIAL type created in table of different schema",
			SetUpScript: []string{
				"CREATE SCHEMA myschema",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "CREATE TABLE myschema.test (pk SERIAL PRIMARY KEY, v1 INTEGER);", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0131-create-table-myschema.test-pk-serial"},
				},
				{
					Query: "INSERT INTO myschema.test (v1) VALUES (2), (3), (5), (7), (11);", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0132-insert-into-myschema.test-v1-values"},
				},
				{
					Query: "SELECT * FROM myschema.test;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0133-select-*-from-myschema.test"},
				},
				{
					Query: "SELECT nextval('test_pk_seq');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0134-select-nextval-test_pk_seq", Compare: "sqlstate"},
				},
				{
					Query: "SELECT nextval('myschema.test_pk_seq');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0135-select-nextval-myschema.test_pk_seq"},
				},
				{
					Query: "SELECT nextval('postgres.myschema.test_pk_seq');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0136-select-nextval-postgres.myschema.test_pk_seq"},
				},
			},
		},
		{
			Name: "Default emulating SERIAL",
			Assertions: []ScriptTestAssertion{
				{
					Query: "CREATE SEQUENCE seq1;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0137-create-sequence-seq1"},
				},
				{
					Query: "CREATE TABLE test (pk INTEGER DEFAULT (nextval('seq1')), v1 INTEGER);", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0138-create-table-test-pk-integer"},
				},
				{
					Query: "INSERT INTO test (v1) VALUES (2), (3), (5), (7), (11);", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0139-insert-into-test-v1-values"},
				},
				{
					Query: "SELECT * FROM test ORDER BY v1;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0140-select-*-from-test-order"},
				},
			},
		},
		{
			Name: "Default emulating SERIAL in non default schema",
			SetUpScript: []string{
				"CREATE SCHEMA myschema",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "CREATE SEQUENCE myschema.seq1;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0141-create-sequence-myschema.seq1"},
				},
				{
					Query: "CREATE TABLE myschema.test (pk INTEGER DEFAULT (nextval('seq1')), v1 INTEGER);", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0142-create-table-myschema.test-pk-integer"},
				},
				{
					Skip:  true, // TODO: relation "seq1" does not exist
					Query: "INSERT INTO myschema.test (v1) VALUES (2), (3), (5), (7), (11);", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0143-insert-into-myschema.test-v1-values"},
				},
				{
					Skip:  true, // TODO: unskip when INSERT above is unskipped
					Query: "SELECT * FROM myschema.test ORDER BY v1;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0144-select-*-from-myschema.test-order"},
				},
			},
		},
		{
			Name: "pg_sequence",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT * FROM "pg_catalog"."pg_sequence";`, PostgresOracle: ScriptTestPostgresOracle{ID:

					// Different cases and quoted, so it fails
					"sequences-test-testsequences-0145-select-*-from-pg_catalog-."},
				},
				{
					Query: `SELECT * FROM "PG_catalog"."pg_sequence";`, PostgresOracle: ScriptTestPostgresOracle{

						// Different cases and quoted, so it fails
						ID: "sequences-test-testsequences-0146-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: `SELECT * FROM "pg_catalog"."PG_sequence";`, PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0147-select-*-from-pg_catalog-.", Compare: "sqlstate"},
				},
				{
					Query: "CREATE SEQUENCE some_sequence;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0148-create-sequence-some_sequence"},
				},
				{
					Query: "CREATE SEQUENCE another_sequence INCREMENT 3 CYCLE;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0149-create-sequence-another_sequence-increment-3"},
				},
				{
					Query: "SELECT * FROM pg_catalog.pg_sequence ORDER BY seqrelid;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0150-select-*-from-pg_catalog.pg_sequence-order"},
				},
				{ // Different cases but non-quoted, so it works
					Query: "SELECT * FROM PG_catalog.pg_SEQUENCE ORDER BY seqrelid;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0151-select-*-from-pg_catalog.pg_sequence-order"},
				},
				{
					Query: "SELECT * FROM pg_catalog.pg_sequence WHERE seqrelid = 'some_sequence'::regclass;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0152-select-*-from-pg_catalog.pg_sequence-where"},
				},
				{
					Query: "SELECT * FROM pg_catalog.pg_sequence WHERE seqrelid = 'another_sequence'::regclass;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0153-select-*-from-pg_catalog.pg_sequence-where"},
				},
				{
					Query: "SELECT nextval('another_sequence');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0154-select-nextval-another_sequence"},
				},
				{
					Query: "SELECT nextval('another_sequence');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0155-select-nextval-another_sequence"},
				},
			},
		},
		{
			Name: "sequence relation scans",
			SetUpScript: []string{
				"CREATE SEQUENCE public.dump_seq START 5 INCREMENT 2;",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT last_value, is_called FROM public.dump_seq;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0156-select-last_value-is_called-from-public.dump_seq", Cleanup: []string{"DROP SEQUENCE IF EXISTS public.dump_seq CASCADE"}},
				},
				{
					Query: "SELECT nextval('public.dump_seq');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0157-select-nextval-public.dump_seq", Cleanup: []string{"DROP SEQUENCE IF EXISTS public.dump_seq CASCADE"}},
				},
				{
					Query: "SELECT last_value, is_called FROM public.dump_seq;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0158-select-last_value-is_called-from-public.dump_seq", Cleanup: []string{"DROP SEQUENCE IF EXISTS public.dump_seq CASCADE"}},
				},
				{
					Query: "SELECT setval('public.dump_seq', 11, false);", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0159-select-setval-public.dump_seq-11-false", Cleanup: []string{"DROP SEQUENCE IF EXISTS public.dump_seq CASCADE"}},
				},
				{
					Query: "SELECT last_value, is_called FROM public.dump_seq;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0160-select-last_value-is_called-from-public.dump_seq", Cleanup: []string{"DROP SEQUENCE IF EXISTS public.dump_seq CASCADE"}},
				},
				{
					Query: "SELECT nextval('public.dump_seq');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0161-select-nextval-public.dump_seq", Cleanup: []string{"DROP SEQUENCE IF EXISTS public.dump_seq CASCADE"}},
				},
				{
					Query: "SELECT last_value, is_called FROM public.dump_seq;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0162-select-last_value-is_called-from-public.dump_seq", Cleanup: []string{"DROP SEQUENCE IF EXISTS public.dump_seq CASCADE"}},
				},
			},
		},
		{
			Name: "DROP TABLE",
			SetUpScript: []string{
				"CREATE TABLE test (pk SERIAL PRIMARY KEY, v1 INTEGER);",
				"INSERT INTO test (v1) VALUES (2), (3), (5), (7), (11);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT * FROM test;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0163-select-*-from-test"},
				},
				{
					Query: "SELECT * FROM pg_catalog.pg_sequence;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0164-select-*-from-pg_catalog.pg_sequence"},
				},
				{
					Query: "DROP TABLE test;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0165-drop-table-test"},
				},
				{
					Query: "SELECT * FROM pg_catalog.pg_sequence;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0166-select-*-from-pg_catalog.pg_sequence"},
				},
			},
		},
		{
			Name: "seq name generation",
			SetUpScript: []string{
				"CREATE SEQUENCE my_table_id_seq;",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "CREATE TABLE my_table (id SERIAL PRIMARY KEY, val INT);",
				},
				{
					Query: "select nextval('my_table_id_seq1');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0167-select-nextval-my_table_id_seq1"},
				},
				{
					Query: "Select count(*) from pg_catalog.pg_sequence where seqrelid = 'my_table_id_seq1'::regclass;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0168-select-count-*-from-pg_catalog.pg_sequence"},
				},
			},
		},
		{
			Name:        "drop and create table with same name (issue 659)",
			SetUpScript: dropAndCreateTableSetUpScript,
			Assertions: []ScriptTestAssertion{
				{
					Query: "create table serial_table (pk serial primary key);",
				},
			},
		},
		{
			Name: "identity generated by default",
			SetUpScript: []string{
				`CREATE TABLE "django_migrations" (
    "id" bigint NOT NULL PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
		"app" varchar(255) NOT NULL,
		"name" varchar(255) NOT NULL,
		"applied" timestamp with time zone NOT NULL)`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO "django_migrations" ("app", "name", "applied") VALUES ('contenttypes', '0001_initial', '2025-03-25T17:45:54.794344+00:00'::timestamptz) RETURNING "django_migrations"."id"`, PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0169-insert-into-django_migrations-app-name"},
				},
				{
					Query: `INSERT INTO "django_migrations" ("app", "name", "applied") VALUES ('contenttypes', '0001_initial', '2025-03-25T17:45:54.794344+00:00'::timestamptz) RETURNING "django_migrations"."id"`, PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0170-insert-into-django_migrations-app-name"},
				},
				{
					Query: `INSERT INTO "django_migrations" ("id", "app", "name", "applied") VALUES (100, 'contenttypes', '0001_initial', '2025-03-25T17:45:54.794344+00:00'::timestamptz) RETURNING "django_migrations"."id"`, PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0171-insert-into-django_migrations-id-app"},
				},
			},
		},
		{
			Name: "identity generated by default with sequence options",
			Skip: true, // not supported yet, need to add sequence info into DML node given to GMS
			SetUpScript: []string{
				`CREATE TABLE "django_migrations" (
    "id" bigint NOT NULL PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY (START WITH 100 INCREMENT BY 2),
		"app" varchar(255) NOT NULL,
		"name" varchar(255) NOT NULL,
		"applied" timestamp with time zone NOT NULL)`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO "django_migrations" ("app", "name", "applied") VALUES ('contenttypes', '0001_initial', '2025-03-25T17:45:54.794344+00:00'::timestamptz) RETURNING "django_migrations"."id"`, PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0172-insert-into-django_migrations-app-name"},
				},
				{
					Query: `INSERT INTO "django_migrations" ("app", "name", "applied") VALUES ('contenttypes', '0001_initial', '2025-03-25T17:45:54.794344+00:00'::timestamptz) RETURNING "django_migrations"."id"`, PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0173-insert-into-django_migrations-app-name"},
				},
			},
		},
		{
			Name: "insert on a different branch",
			Skip: true, // currently dies on creating the sequence on a non-current DB table
			SetUpScript: []string{
				"create table test (pk serial primary key, v1 int);",
				"insert into test (v1) values (2), (3), (5), (7), (11);",
				"call dolt_branch('b1');",
				`create table "postgres/b1".public.test2 (pk serial primary key, v1 int);`,
				`insert into "postgres/b1".public.test2 (v1) values (2), (3), (5), (7), (11);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT pk FROM test ORDER BY v1;",
					Expected: []sql.Row{
						{1},
						{2},
						{3},
						{4},
						{5},
					},
				},
				{
					Query: `SELECT pk FROM "postgres/b1".public.test2 ORDER BY v1;`,
					Expected: []sql.Row{
						{1},
						{2},
						{3},
						{4},
						{5},
					},
				},
			},
		},
		{
			Name: "dolt_add, dolt_branch, dolt_checkout, dolt_commit, dolt_reset",
			Assertions: []ScriptTestAssertion{
				{
					Query: "CREATE SEQUENCE test;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0176-create-sequence-test", Compare: "sqlstate"},
				},
				{
					Query: "SELECT setval('test', 10);", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0177-select-setval-test-10"},
				},
				{
					Query: "SELECT nextval('test');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0178-select-nextval-test"},
				},
				{
					Query: "SELECT * FROM dolt_diff_summary('HEAD', 'WORKING')",
					Expected: []sql.Row{
						{"", "public.test", "added", "t", "t"},
					},
				},
				{
					Query:    "SELECT dolt_add('test');",
					Expected: []sql.Row{{"{0}"}},
				},
				{
					Query:    "SELECT length(dolt_commit('-m', 'initial')::text) = 34;",
					Expected: []sql.Row{{"t"}},
				},
				{
					Query:    "SELECT dolt_branch('other');",
					Expected: []sql.Row{{"{0}"}},
				},
				{
					Query:    "SELECT setval('test', 20);",
					Expected: []sql.Row{{20}},
				},
				{
					Query:    "SELECT dolt_add('.');",
					Expected: []sql.Row{{"{0}"}},
				},
				{
					Query:    "SELECT length(dolt_commit('-m', 'next')::text) = 34;",
					Expected: []sql.Row{{"t"}},
				},
				{
					Query:    "SELECT nextval('test');",
					Expected: []sql.Row{{21}},
				},
				{
					Query:    "SELECT dolt_checkout('other');",
					Expected: []sql.Row{{`{0,"Switched to branch 'other'"}`}},
				},
				{
					Query:    "SELECT nextval('test');",
					Expected: []sql.Row{{12}},
				},
				{
					Query:    "SELECT dolt_reset('--hard');",
					Expected: []sql.Row{{"{0}"}},
				},
				{
					Query:    "SELECT nextval('test');",
					Expected: []sql.Row{{12}},
				},
			},
		},
		{
			Name: "dolt_clean",
			Assertions: []ScriptTestAssertion{
				{
					Query: "CREATE SEQUENCE test1;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0191-create-sequence-test1", Compare: "sqlstate"},
				},
				{
					Query: "CREATE SEQUENCE test2;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0192-create-sequence-test2"},
				},
				{
					Query: "SELECT setval('test1', 10);", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0193-select-setval-test1-10"},
				},
				{
					Query: "SELECT nextval('test1');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0194-select-nextval-test1"},
				},
				{
					Query: "SELECT setval('test2', 10);", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0195-select-setval-test2-10"},
				},
				{
					Query: "SELECT nextval('test2');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0196-select-nextval-test2"},
				},
				{
					Query:    "SELECT dolt_add('test1');",
					Expected: []sql.Row{{"{0}"}},
				},
				{
					Query: "SELECT * FROM dolt.status;",
					Expected: []sql.Row{
						{"public.test1", "t", "new table"},
						{"public.test2", "f", "new table"},
					},
				},
				{
					Query:    "SELECT dolt_clean('test2');", // TODO: dolt_clean() requires a param, need to fix procedure to func conversion
					Expected: []sql.Row{{"{0}"}},
				},
				{
					Query: "SELECT * FROM dolt.status;",
					Expected: []sql.Row{
						{"public.test1", "t", "new table"},
					},
				},
			},
		},
		{
			Name: "dolt_merge",
			Assertions: []ScriptTestAssertion{
				{
					Query: "CREATE SEQUENCE test;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0201-create-sequence-test", Compare: "sqlstate"},
				},
				{
					Query: "SELECT setval('test', 10);", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0202-select-setval-test-10"},
				},
				{
					Query:    "SELECT length(dolt_commit('-Am', 'initial')::text) = 34;",
					Expected: []sql.Row{{"t"}},
				},
				{
					Query:    "SELECT dolt_branch('other');",
					Expected: []sql.Row{{"{0}"}},
				},
				{
					Query:    "SELECT setval('test', 20);",
					Expected: []sql.Row{{20}},
				},
				{
					Query:    "SELECT length(dolt_commit('-am', 'next')::text) = 34;",
					Expected: []sql.Row{{"t"}},
				},
				{
					Query:    "SELECT dolt_checkout('other');",
					Expected: []sql.Row{{`{0,"Switched to branch 'other'"}`}},
				},
				{
					Query:    "SELECT setval('test', 30);",
					Expected: []sql.Row{{30}},
				},
				{
					Query:    "SELECT length(dolt_commit('-am', 'next2')::text) = 34;",
					Expected: []sql.Row{{"t"}},
				},
				{
					Query:    "SELECT dolt_checkout('main');",
					Expected: []sql.Row{{`{0,"Switched to branch 'main'"}`}},
				},
				{
					Query:    "SELECT nextval('test');",
					Expected: []sql.Row{{21}},
				},
				{
					Query:    "SELECT dolt_reset('--hard');",
					Expected: []sql.Row{{"{0}"}},
				},
				{
					Query:    "SELECT strpos(dolt_merge('other')::text, 'merge successful') > 32;",
					Expected: []sql.Row{{"t"}},
				},
				{
					Query:    "SELECT nextval('test');",
					Expected: []sql.Row{{31}},
				},
			},
		},
		{
			Name: "Information Schema & DIFF_STAT regression testing",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE "user" ("id" bigint NOT NULL GENERATED BY DEFAULT AS IDENTITY, PRIMARY KEY ("id"));`, PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0215-create-table-user-id-bigint"},
				},
				{
					Query: `CREATE TABLE "call" (
  "id" bigint NOT NULL GENERATED BY DEFAULT AS IDENTITY,
  "state" character varying NOT NULL,
  "content" jsonb NOT NULL,
  "created_at" timestamptz NOT NULL,
  "updated_at" timestamptz NOT NULL,
  "ended_at" timestamptz NULL,
  "user" bigint NOT NULL,
  PRIMARY KEY ("id"),
  CONSTRAINT "call_user_user_fk" FOREIGN KEY ("user") REFERENCES "user" ("id") ON DELETE NO ACTION
);`, PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0216-create-table-call-id-bigint"},
				},
				{
					Query: "SELECT * FROM information_schema.key_column_usage where constraint_schema <> 'pg_catalog';", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0217-select-*-from-information_schema.key_column_usage-where", ColumnModes: []string{"structural", "schema", "structural", "structural", "schema"}},
				},
				{
					Query: "SELECT * FROM DOLT_DIFF_STAT('HEAD', 'WORKING');",
					Expected: []sql.Row{
						{"public.call_id_seq", 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0},
						{"public.user_id_seq", 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					},
				},
				{ // This is the same as running "\d" in PSQL
					Query: `SELECT n.nspname as "Schema",
  c.relname as "Name",
  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 't' THEN 'TOAST table' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index' END as "Type",
  pg_catalog.pg_get_userbyid(c.relowner) as "Owner"
FROM pg_catalog.pg_class c
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
     LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam
WHERE c.relkind IN ('r','p','v','m','S','f','')
      AND n.nspname <> 'pg_catalog'
      AND n.nspname !~ '^pg_toast'
      AND n.nspname <> 'information_schema'
  AND pg_catalog.pg_table_is_visible(c.oid)
ORDER BY 1,2;`,
					Expected: []sql.Row{
						{"public", "call", "table", "postgres"},
						{"public", "call_id_seq", "sequence", "postgres"},
						{"public", "user", "table", "postgres"},
						{"public", "user_id_seq", "sequence", "postgres"},
					},
				},
			},
		},
		{
			Name: "ALTER COLUMN ADD GENERATED BY DEFAULT",
			SetUpScript: []string{
				"CREATE TABLE public.test1 (id int2 NOT NULL, name character varying(150) NOT NULL);",
				"CREATE TABLE public.test2 (id int4 NOT NULL, name character varying(150) NOT NULL);",
				"CREATE TABLE public.test3 (id int8 NOT NULL, name character varying(150) NOT NULL);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    "ALTER TABLE public.test1 ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (SEQUENCE NAME public.test1_id_seq START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1);",
					Expected: []sql.Row{},
				},
				{
					Query:    "ALTER TABLE public.test2 ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (SEQUENCE NAME public.test2_id_seq START WITH 10 INCREMENT BY 5);",
					Expected: []sql.Row{},
				},
				{
					Query:    "ALTER TABLE public.test3 ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (SEQUENCE NAME public.test3_id_seq START WITH 100 INCREMENT BY -1 MAXVALUE 100);",
					Expected: []sql.Row{},
				},
				{
					Query:    "INSERT INTO public.test1 (name) VALUES ('abc'), ('def');",
					Expected: []sql.Row{},
				},
				{
					Query:    "INSERT INTO public.test2 (name) VALUES ('abc'), ('def');",
					Expected: []sql.Row{},
				},
				{
					Query:    "INSERT INTO public.test3 (name) VALUES ('abc'), ('def');",
					Expected: []sql.Row{},
				},
				{
					Query:    "SELECT * FROM public.test1;",
					Expected: []sql.Row{{1, "abc"}, {2, "def"}},
				},
				{
					Query:    "SELECT * FROM public.test2;",
					Expected: []sql.Row{{10, "abc"}, {15, "def"}},
				},
				{
					Query:    "SELECT * FROM public.test3;",
					Expected: []sql.Row{{100, "abc"}, {99, "def"}},
				},
			},
		},
		{
			Name: "ALTER SEQUENCE OWNED BY",
			SetUpScript: []string{
				"CREATE SCHEMA other;",
				"CREATE TABLE test (id int4 NOT NULL);",
				"CREATE TABLE other.test (id int4 NOT NULL);",
				"CREATE SEQUENCE seq1;",
				"CREATE SEQUENCE seq2;",
				"CREATE FUNCTION f_trigger() RETURNS TRIGGER AS $$ BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql;",
				"CREATE TRIGGER trig_trigger BEFORE INSERT ON test FOR EACH ROW EXECUTE FUNCTION f_trigger();",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT nextval('seq1');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0229-select-nextval-seq1"},
				},
				{
					Query: "SELECT nextval('seq2');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0230-select-nextval-seq2"},
				},
				{
					Query: "ALTER SEQUENCE seq1 OWNED BY test.non_existent;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0231-alter-sequence-seq1-owned-by", Compare: "sqlstate"},
				},
				{
					Query: "ALTER SEQUENCE public.seq1 OWNED BY other.test.non_existent;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0232-alter-sequence-public.seq1-owned-by", Compare: "sqlstate"},
				},
				{
					Query: "ALTER SEQUENCE seq1 OWNED BY test;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0233-alter-sequence-seq1-owned-by", Compare: "sqlstate"},
				},
				{
					Query: "ALTER SEQUENCE seq1 OWNED BY trig_trigger.trig;",
					Skip:  true, PostgresOracle: // TODO: need to add triggers to relation checking
					ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0234-alter-sequence-seq1-owned-by", Compare: "sqlstate"},
				},
				{
					Query: "ALTER SEQUENCE seq1 OWNED BY test.id;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0235-alter-sequence-seq1-owned-by"},
				},
				{
					Query: "ALTER SEQUENCE seq2 OWNED BY test.id;", PostgresOracle: ScriptTestPostgresOracle{ID:

					// Setting OWNED BY back to NONE ensures that we properly handle this case
					"sequences-test-testsequences-0236-alter-sequence-seq2-owned-by"},
				},
				{
					Query: "ALTER SEQUENCE seq2 OWNED BY NONE;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0237-alter-sequence-seq2-owned-by"},
				},
				{
					Query: "DROP TABLE test;", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0238-drop-table-test"},
				},
				{
					Query: "SELECT nextval('seq1');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0239-select-nextval-seq1"},
				},
				{
					Query: "SELECT nextval('seq2');", PostgresOracle: ScriptTestPostgresOracle{ID: "sequences-test-testsequences-0240-select-nextval-seq2"},
				},
			},
		},
		{
			Name: "sequence collection loaded independently with multiple databases",
			SetUpScript: []string{
				"CREATE DATABASE testdb2",
				"USE testdb2",
				"CREATE SEQUENCE seq_in_testdb2",
				"USE postgres",
				"CREATE SEQUENCE seq_in_postgres",
			},
			Assertions: []ScriptTestAssertion{
				{
					// nextval loads cv.seqs["postgres"]; the ::regclass cast for testdb2
					// then calls GetSequencesCollectionFromContext("testdb2"), to test
					// sequence loading across multiple databases.
					Query:    "SELECT nextval('seq_in_postgres'), 'testdb2.public.seq_in_testdb2'::regclass IS NOT NULL",
					Expected: []sql.Row{{1, "t"}},
				},
			},
		},
	})
}
