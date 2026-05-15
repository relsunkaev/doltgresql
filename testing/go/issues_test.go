/// Copyright 2023 Dolthub, Inc.
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
	"github.com/jackc/pgx/v5/pgproto3"
)

func TestIssues(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "Issue #25",
			SetUpScript: []string{
				"create table tbl (pk int);",
				"insert into tbl values (1);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `select dolt_add(".");`,
					ExpectedErr: "could not be found in any table in scope",
				},
				{
					Query:    `select dolt_add('.');`,
					Expected: []sql.Row{{"{0}"}},
				},
				{
					Query:       `select dolt_commit("-m", "look ma");`,
					ExpectedErr: "could not be found in any table in scope",
				},
				{
					Query:    `select length(dolt_commit('-m', 'look ma')::text);`,
					Expected: []sql.Row{{34}},
				},
				{
					Query:       `select dolt_branch("br1");`,
					ExpectedErr: "could not be found in any table in scope",
				},
				{
					Query:    `select dolt_branch('br1');`,
					Expected: []sql.Row{{"{0}"}},
				},
			},
		},
		{
			Name: "Issue #2049",
			SetUpScript: []string{
				`CREATE TABLE jsonb_test (id VARCHAR(256) NOT NULL PRIMARY KEY, "jsonbColumn" JSONB);`,
				`INSERT INTO jsonb_test VALUES ('test', '{"test": "value\n"}');`,
				`INSERT INTO jsonb_test VALUES ('test2', '{"test": "value\t"}');`,
				`INSERT INTO jsonb_test VALUES ('test3', '{"test": "value\r"}');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT * FROM jsonb_test;",
					// The pgx library incorrectly reinterprets our JSON value by replacing the individual newline
					// characters (ASCII 92,110) with the actual newline character (ASCII 10), which is incorrect for us.
					// Therefore, we have to use the raw returned values. To make it more clear, we aren't using a raw
					// string literal and instead escaping the characters in the byte slice. We also test other escape
					// characters that are replaced.
					ExpectedRaw: [][][]byte{
						{[]byte("test"), []byte("{\"test\": \"value\\n\"}")},
						{[]byte("test2"), []byte("{\"test\": \"value\\t\"}")},
						{[]byte("test3"), []byte("{\"test\": \"value\\r\"}")},
					},
				},
			},
		},
		{
			Name: "Issue #2604",
			SetUpScript: []string{
				"CREATE TABLE t (id INT PRIMARY KEY, a TEXT, b TEXT DEFAULT 'x');",
				"CREATE UNIQUE INDEX idx_t_a ON t(a);",
				"SELECT dolt_add('-A');",
				"SELECT dolt_commit('-m', 'schema');",
				"SELECT dolt_branch('f', 'main');",
				"SELECT dolt_checkout('f');",
				"INSERT INTO t (id, a) VALUES (1, 'feat');",
				"SELECT dolt_add('-A');",
				"SELECT dolt_commit('-m', 'feat');",
				"SELECT dolt_checkout('main');",
				"INSERT INTO t (id, a) VALUES (2, 'main');",
				"SELECT dolt_add('-A');",
				"SELECT dolt_commit('-m', 'main');",
				"SELECT dolt_checkout('f');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    "SELECT length(dolt_merge('main')::text) = 57;",
					Expected: []sql.Row{{"t"}},
				},
			},
		},
	})
}

func TestIssuesWire(t *testing.T) {
	RunWireScripts(t, []WireScriptTest{
		{
			Name: "Issue #2546",
			Assertions: []WireScriptTestAssertion{
				{
					Send: []pgproto3.FrontendMessage{
						&pgproto3.Query{String: "SELECT 'foo';"},
					},
					Receive: []pgproto3.BackendMessage{
						&pgproto3.RowDescription{
							Fields: []pgproto3.FieldDescription{
								{
									Name:                 []byte("?column?"),
									TableOID:             0,
									TableAttributeNumber: 0,
									DataTypeOID:          25,
									DataTypeSize:         -1,
									TypeModifier:         -1,
									Format:               0,
								},
							},
						},
						&pgproto3.DataRow{Values: [][]byte{[]byte("foo")}},
						&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")},
						&pgproto3.ReadyForQuery{TxStatus: 'I'},
					},
				},
			},
		},
		{
			Name: "Issue #2557",
			Assertions: []WireScriptTestAssertion{
				{
					Send: []pgproto3.FrontendMessage{
						&pgproto3.Parse{
							Name:  "stmt_name1",
							Query: `SELECT '{"v":"a\\nb"}'::jsonb;`,
						},
						&pgproto3.Sync{},
					},
					Receive: []pgproto3.BackendMessage{
						&pgproto3.ParseComplete{},
						&pgproto3.ReadyForQuery{TxStatus: 'I'},
					},
				},
				{
					Send: []pgproto3.FrontendMessage{
						&pgproto3.Bind{
							PreparedStatement: "stmt_name1",
							ResultFormatCodes: []int16{0},
						},
						&pgproto3.Execute{},
						&pgproto3.Close{
							ObjectType: 'P',
						},
						&pgproto3.Sync{},
					},
					Receive: []pgproto3.BackendMessage{
						&pgproto3.BindComplete{},
						&pgproto3.DataRow{
							Values: [][]byte{
								[]byte(`{"v": "a\\nb"}`),
							},
						},
						&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")},
						&pgproto3.CloseComplete{},
						&pgproto3.ReadyForQuery{TxStatus: 'I'},
					},
				},
				{
					Send: []pgproto3.FrontendMessage{
						&pgproto3.Parse{
							Name:  "stmt_name2",
							Query: `SELECT $${"v":"a\\nb"}$$::jsonb;`,
						},
						&pgproto3.Sync{},
					},
					Receive: []pgproto3.BackendMessage{
						&pgproto3.ParseComplete{},
						&pgproto3.ReadyForQuery{TxStatus: 'I'},
					},
				},
				{
					Send: []pgproto3.FrontendMessage{
						&pgproto3.Bind{
							PreparedStatement: "stmt_name2",
							ResultFormatCodes: []int16{0},
						},
						&pgproto3.Execute{},
						&pgproto3.Close{
							ObjectType: 'P',
						},
						&pgproto3.Sync{},
					},
					Receive: []pgproto3.BackendMessage{
						&pgproto3.BindComplete{},
						&pgproto3.DataRow{
							Values: [][]byte{
								[]byte(`{"v": "a\\nb"}`),
							},
						},
						&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")},
						&pgproto3.CloseComplete{},
						&pgproto3.ReadyForQuery{TxStatus: 'I'},
					},
				},
				{
					Send: []pgproto3.FrontendMessage{
						&pgproto3.Parse{
							Name:  "stmt_name3",
							Query: `SELECT $${"v":"a\\\nb"}$$::jsonb;`,
						},
						&pgproto3.Sync{},
					},
					Receive: []pgproto3.BackendMessage{
						&pgproto3.ParseComplete{},
						&pgproto3.ReadyForQuery{TxStatus: 'I'},
					},
				},
				{
					Send: []pgproto3.FrontendMessage{
						&pgproto3.Bind{
							PreparedStatement: "stmt_name3",
							ResultFormatCodes: []int16{0},
						},
						&pgproto3.Execute{},
						&pgproto3.Close{
							ObjectType: 'P',
						},
						&pgproto3.Sync{},
					},
					Receive: []pgproto3.BackendMessage{
						&pgproto3.BindComplete{},
						&pgproto3.DataRow{
							Values: [][]byte{
								[]byte(`{"v": "a\\\nb"}`),
							},
						},
						&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")},
						&pgproto3.CloseComplete{},
						&pgproto3.ReadyForQuery{TxStatus: 'I'},
					},
				},
				{
					Send: []pgproto3.FrontendMessage{
						&pgproto3.Parse{
							Name:  "stmt_name4",
							Query: `select json '{ "a":  "dollar \\u0024 character" }' ->> 'a' as not_an_escape;`,
						},
						&pgproto3.Sync{},
					},
					Receive: []pgproto3.BackendMessage{
						&pgproto3.ParseComplete{},
						&pgproto3.ReadyForQuery{TxStatus: 'I'},
					},
				},
				{
					Send: []pgproto3.FrontendMessage{
						&pgproto3.Bind{
							PreparedStatement: "stmt_name4",
							ResultFormatCodes: []int16{0},
						},
						&pgproto3.Execute{},
						&pgproto3.Close{
							ObjectType: 'P',
						},
						&pgproto3.Sync{},
					},
					Receive: []pgproto3.BackendMessage{
						&pgproto3.BindComplete{},
						&pgproto3.DataRow{
							Values: [][]byte{
								[]byte(`dollar $ character`),
							},
						},
						&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")},
						&pgproto3.CloseComplete{},
						&pgproto3.ReadyForQuery{TxStatus: 'I'},
					},
				},
			},
		},
	})
}
