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
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestCopy(t *testing.T) {
	absTestDataDir, err := filepath.Abs("testdata")
	require.NoError(t, err)

	RunScripts(t, []ScriptTest{
		{
			Name: "tab delimited with header",
			SetUpScript: []string{
				"CREATE TABLE test (pk int primary key);",
				"INSERT INTO test VALUES (0), (1);",
				"CREATE TABLE test_info (id int, info varchar(255), test_pk int, primary key(id), foreign key (test_pk) references test(pk));",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:             "COPY test_info FROM STDIN WITH (HEADER);",
					CopyFromStdInFile: "tab-load-with-header.sql",
				},
				{
					Query: "SELECT * FROM test_info order by 1;",
					Expected: []sql.Row{
						{4, "string for 4", 1},
						{5, "string for 5", 0},
						{6, "string for 6", 0},
					},
				},
			},
		},
		{
			Name: "tab delimited with header and column names",
			SetUpScript: []string{
				"CREATE TABLE test (pk int primary key);",
				"INSERT INTO test VALUES (0), (1);",
				"CREATE TABLE test_info (id int, info varchar(255), test_pk int, primary key(id), foreign key (test_pk) references test(pk));",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:             "COPY test_info (id, info, test_pk) FROM STDIN WITH (HEADER);",
					CopyFromStdInFile: "tab-load-with-header.sql",
				},
				{
					Query: "SELECT * FROM test_info order by 1;",
					Expected: []sql.Row{
						{4, "string for 4", 1},
						{5, "string for 5", 0},
						{6, "string for 6", 0},
					},
				},
			},
		},
		{
			Name: "tab delimited with quoted column names",
			SetUpScript: []string{
				`CREATE TABLE Regions (
   "Id" SERIAL UNIQUE NOT NULL,
   "Code" VARCHAR(4) UNIQUE NOT NULL,
   "Capital" VARCHAR(10) NOT NULL,
   "Name" VARCHAR(255) UNIQUE NOT NULL
);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:             "COPY regions (\"Id\", \"Code\", \"Capital\", \"Name\") FROM stdin;\n",
					CopyFromStdInFile: "tab-load-with-quoted-column-names.sql",
				},
			},
		},
		{
			Name: "timestamp columns",
			SetUpScript: []string{
				"CREATE TABLE tbl1 (pk timestamp primary key, ts timestamp);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:             "COPY tbl1 FROM STDIN WITH (HEADER)",
					CopyFromStdInFile: "tab-load-with-timestamp-col.sql",
				},
				{
					Query: "select * from tbl1 order by pk;",
					Expected: []sql.Row{
						{"2020-12-19 19:00:00", "2021-04-04 20:00:00"},
						{"2020-12-19 21:36:32.188", "2020-12-19 19:00:00"},
						{"2021-04-04 20:00:00", "2020-12-19 21:36:32.188"},
					},
				},
			},
		},
		{
			Name: "basic csv",
			SetUpScript: []string{
				"CREATE TABLE tbl1 (pk int primary key, c1 varchar(100), c2 varchar(250));",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:             "COPY tbl1 FROM STDIN (FORMAT CSV)",
					CopyFromStdInFile: "csv-load-basic-cases.sql",
				},
				{
					Query: "select * from tbl1 where pk = 6 order by pk;",
					Expected: []sql.Row{
						{6, `foo
\\.
bar`, "baz"},
					},
				},
				{
					Query: "select * from tbl1 where pk = 9;",
					Expected: []sql.Row{
						{9, nil, "''"},
					},
				},
			},
		},
		{
			Name: "csv with header",
			SetUpScript: []string{
				"CREATE TABLE tbl1 (pk int primary key, c1 varchar(100), c2 varchar(250));",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:             " COPY tbl1 FROM STDIN (FORMAT CSV, HEADER TRUE);",
					CopyFromStdInFile: "csv-load-with-header.sql",
				},
				{
					Query: "select * from tbl1 where pk = 6 order by pk;",
					Expected: []sql.Row{
						{6, `foo
\\.
bar`, "baz"},
					},
				},
			},
		},
		{
			Name: "generated column",
			SetUpScript: []string{
				"CREATE TABLE tbl1 (pk int primary key, c1 varchar(100), c2 varchar(250), c3 int generated always as (pk + 10) stored);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:             "COPY tbl1 (pk, c1, c2) FROM STDIN (FORMAT CSV)",
					CopyFromStdInFile: "csv-load-basic-cases.sql",
				},
				{
					Query: "select * from tbl1 where pk = 6 order by pk;",
					Expected: []sql.Row{
						{6, `foo
\\.
bar`, "baz", 16},
					},
				},
				{
					Query: "select * from tbl1 where pk = 9;",
					Expected: []sql.Row{
						{9, nil, "''", 19},
					},
				},
			},
		},
		{
			Name: "load multiple chunks",
			SetUpScript: []string{
				"CREATE TABLE tbl1 (pk int primary key, c1 varchar(100), c2 varchar(250));",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:             "COPY tbl1 FROM STDIN (FORMAT CSV);",
					CopyFromStdInFile: "csv-load-multi-chunk.sql",
				},
				{
					Query: "select * from tbl1 where pk = 99 order by pk;",
					Expected: []sql.Row{
						{99, "foo", "barbazbashbarbazbashbarbazbashbarbazbashbarbazbashbarbazbashbarbazbashbarbazbashbarbazbashbarbazbashbarbazbash"},
					},
				},
			},
		},
		{
			Name: "load psv with headers",
			SetUpScript: []string{
				"CREATE TABLE test (pk int primary key);",
				"INSERT INTO test VALUES (0), (1);",
				"CREATE TABLE test_info (id int, info varchar(255), test_pk int, primary key(id), foreign key (test_pk) references test(pk));",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:             "COPY test_info FROM STDIN (FORMAT CSV, HEADER TRUE, DELIMITER '|');",
					CopyFromStdInFile: "psv-load.sql",
				},
				{
					Query: "SELECT * FROM test_info order by 1;",
					Expected: []sql.Row{
						{4, "string for 4", 1},
						{5, "string for 5", 0},
						{6, "string for 6", 0},
					},
				},
			},
		},
		{
			Name: "csv from file",
			SetUpScript: []string{
				"CREATE TABLE tbl1 (pk int primary key, c1 varchar(100), c2 varchar(250));",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:            fmt.Sprintf("COPY tbl1 FROM '%s' (FORMAT CSV)", filepath.Join(absTestDataDir, "csv-load-basic-cases.sql")),
					SkipResultsCheck: true,
				},
				{
					Query: "select * from tbl1 where pk = 6 order by pk;",
					Expected: []sql.Row{
						{6, `foo
\\.
bar`, "baz"},
					},
				},
				{
					Query: "select * from tbl1 where pk = 9;",
					Expected: []sql.Row{
						{9, nil, "''"},
					},
				},
			},
		},
		{
			Name: "csv from file with column names",
			SetUpScript: []string{
				"CREATE TABLE tbl1 (pk int primary key, c1 varchar(100), c2 varchar(250));",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:            fmt.Sprintf("COPY tbl1 (pk, c1, c2) FROM '%s' (FORMAT CSV)", filepath.Join(absTestDataDir, "csv-load-basic-cases.sql")),
					SkipResultsCheck: true,
				},
				{
					Query: "select * from tbl1 where pk = 6 order by pk;",
					Expected: []sql.Row{
						{6, `foo
\\.
bar`, "baz"},
					},
				},
				{
					Query: "select * from tbl1 where pk = 9;",
					Expected: []sql.Row{
						{9, nil, "''"},
					},
				},
			},
		},
		{
			Name: "tab delimited with header from file",
			SetUpScript: []string{
				"CREATE TABLE test (pk int primary key);",
				"INSERT INTO test VALUES (0), (1);",
				"CREATE TABLE test_info (id int, info varchar(255), test_pk int, primary key(id), foreign key (test_pk) references test(pk));",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: fmt.Sprintf("COPY test_info FROM '%s' WITH (HEADER)", filepath.Join(absTestDataDir, "tab-load-with-header.sql")),
				},
				{
					Query: "SELECT * FROM test_info order by 1;",
					Expected: []sql.Row{
						{4, "string for 4", 1},
						{5, "string for 5", 0},
						{6, "string for 6", 0},
					},
				},
			},
		},
		{
			Name: "tab delimited with uuid values",
			SetUpScript: []string{
				`CREATE TABLE public.uuid_table (
    id uuid NOT NULL,
    name character varying NOT NULL,
    second_uuid uuid DEFAULT '428d0815-d95b-4cfc-89af-9fca38585dcc'::uuid);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:             "COPY uuid_table (id, name, second_uuid) FROM STDIN",
					CopyFromStdInFile: "uuid-table.sql",
				},
				{
					Query: "SELECT * FROM uuid_table order by id;",
					Expected: []sql.Row{
						{"1077f506-a6fc-4cb2-aed2-9dea9351ed9c", "Company A", "428d0815-d95b-4cfc-89af-9fca38585dcc"},
						{"5e080b3a-361f-4e16-b7a4-70d4f175e283", "Company B", "428d0815-d95b-4cfc-89af-9fca38585dcc"},
					},
				},
			},
		},
		{
			Name: "file not found",
			SetUpScript: []string{
				"CREATE TABLE test (pk int primary key);",
				"INSERT INTO test VALUES (0), (1);",
				"CREATE TABLE test_info (id int, info varchar(255), test_pk int, primary key(id), foreign key (test_pk) references test(pk));",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       fmt.Sprintf("COPY test_info FROM '%s' WITH (HEADER)", filepath.Join(absTestDataDir, "file-not-found.sql")),
					ExpectedErr: "file", // exact error message varies by platform
				},
			},
		},
		{
			Name: "wrong columns",
			SetUpScript: []string{
				"CREATE TABLE tbl1 (pk int primary key, c1 varchar(100), c2 varchar(250));",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       fmt.Sprintf("COPY tbl1 (pk, c1) FROM '%s' (FORMAT CSV)", filepath.Join(absTestDataDir, "csv-load-basic-cases.sql")),
					ExpectedErr: "extra data after last expected column",
				},
				{
					Query:       fmt.Sprintf("COPY tbl1 (pk, c1, c3) FROM '%s' (FORMAT CSV)", filepath.Join(absTestDataDir, "csv-load-basic-cases.sql")),
					ExpectedErr: "Unknown column",
				},
			},
		},
		{
			Name: "table not found",
			SetUpScript: []string{
				"CREATE TABLE tbl1 (pk int primary key, c1 varchar(100), c2 varchar(250));",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       fmt.Sprintf("COPY tbl2 (pk, c1) FROM '%s' (FORMAT CSV)", filepath.Join(absTestDataDir, "csv-load-basic-cases.sql")),
					ExpectedErr: "table not found: tbl2",
				},
			},
		},
		{
			Name: "read only table",
			Assertions: []ScriptTestAssertion{
				{
					Query:       fmt.Sprintf("COPY dolt_log FROM '%s' (FORMAT CSV)", filepath.Join(absTestDataDir, "csv-load-basic-cases.sql")),
					ExpectedErr: "table doesn't support INSERT INTO",
				},
			},
		},
		{
			Name: "bad data rows",
			SetUpScript: []string{
				"CREATE TABLE tbl1 (pk int primary key, c1 varchar(100), c2 varchar(250));",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       fmt.Sprintf("COPY tbl1 (pk, c1, c2) FROM '%s' (FORMAT CSV)", filepath.Join(absTestDataDir, "missing-columns.sql")),
					ExpectedErr: "record on line 2: wrong number of fields",
				},
				{
					Query:    "select count(*) from tbl1;",
					Expected: []sql.Row{{0}},
				},
				{
					Query:       fmt.Sprintf("COPY tbl1 (pk, c1, c2) FROM '%s' (FORMAT CSV)", filepath.Join(absTestDataDir, "too-many-columns.sql")),
					ExpectedErr: "record on line 6: wrong number of fields",
				},
				{
					Query:    "select count(*) from tbl1;",
					Expected: []sql.Row{{0}},
				},
				{
					Query:       fmt.Sprintf("COPY tbl1 (pk, c1, c2) FROM '%s' (FORMAT CSV)", filepath.Join(absTestDataDir, "wrong-types.sql")),
					ExpectedErr: "invalid input syntax for type int4",
				},
				{
					Query:    "select count(*) from tbl1;",
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

func TestCopyToStdout(t *testing.T) {
	ctx, connection, controller := CreateServer(t, "postgres")
	defer func() {
		connection.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	_, err := connection.Exec(ctx, "CREATE TABLE copy_out (id int primary key, name text, note text);")
	require.NoError(t, err)
	_, err = connection.Exec(ctx, "INSERT INTO copy_out VALUES (1, 'alice', 'one'), (2, 'bob', NULL);")
	require.NoError(t, err)

	var textOut bytes.Buffer
	tag, err := connection.Default.PgConn().CopyTo(ctx, &textOut, "COPY copy_out (id, name, note) TO STDOUT;")
	require.NoError(t, err)
	require.Equal(t, "COPY 2", tag.String())
	require.Equal(t, "1\talice\tone\n2\tbob\t\\N\n", textOut.String())

	var csvOut bytes.Buffer
	tag, err = connection.Default.PgConn().CopyTo(ctx, &csvOut, "COPY copy_out (id, name, note) TO STDOUT WITH (FORMAT CSV, HEADER TRUE);")
	require.NoError(t, err)
	require.Equal(t, "COPY 2", tag.String())
	require.Equal(t, "id,name,note\n1,alice,one\n2,bob,\n", csvOut.String())
}

func TestCopyToStdoutRequiresSelectPrivilegeGuard(t *testing.T) {
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)
	ctx, connection, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		connection.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	for _, stmt := range []string{
		`CREATE USER copy_reader PASSWORD 'reader';`,
		`CREATE TABLE copy_private (id INT PRIMARY KEY, secret TEXT);`,
		`INSERT INTO copy_private VALUES (1, 'alpha'), (2, 'beta');`,
		`GRANT USAGE ON SCHEMA public TO copy_reader;`,
	} {
		_, err = connection.Exec(ctx, stmt)
		require.NoError(t, err, stmt)
	}

	readerConn, err := pgx.Connect(ctx, fmt.Sprintf(
		"postgres://copy_reader:reader@127.0.0.1:%d/postgres?sslmode=disable",
		port,
	))
	require.NoError(t, err)
	defer readerConn.Close(context.Background())

	var out bytes.Buffer
	tag, err := readerConn.PgConn().CopyTo(ctx, &out, `COPY copy_private (id, secret) TO STDOUT;`)
	require.Errorf(t, err, "COPY TO should require SELECT privilege; tag=%s output=%q", tag.String(), out.String())
	require.Contains(t, err.Error(), "denied")
}

func TestCopyFromStdinRequiresInsertPrivilegeRepro(t *testing.T) {
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)
	ctx, connection, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		connection.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	for _, stmt := range []string{
		`CREATE USER copy_writer PASSWORD 'writer';`,
		`CREATE TABLE copy_from_private (id INT PRIMARY KEY, label TEXT);`,
		`GRANT USAGE ON SCHEMA public TO copy_writer;`,
	} {
		_, err = connection.Exec(ctx, stmt)
		require.NoError(t, err, stmt)
	}

	writerConn, err := pgx.Connect(ctx, fmt.Sprintf(
		"postgres://copy_writer:writer@127.0.0.1:%d/postgres?sslmode=disable",
		port,
	))
	require.NoError(t, err)
	defer writerConn.Close(context.Background())

	tag, err := writerConn.PgConn().CopyFrom(
		ctx,
		strings.NewReader("1\talpha\n"),
		`COPY copy_from_private (id, label) FROM STDIN;`,
	)
	require.Errorf(t, err, "COPY FROM should require INSERT privilege; tag=%s", tag.String())
	require.Contains(t, err.Error(), "denied")
}

func TestCopyCustomerSyncTypeMatrix(t *testing.T) {
	ctx, connection, controller := CreateServer(t, "postgres")
	defer func() {
		connection.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	_, err := connection.Exec(ctx, `CREATE TABLE copy_customer_sync (
		customer_id bigint NOT NULL,
		item_id uuid NOT NULL,
		flag boolean NOT NULL DEFAULT false,
		amount numeric(12,2),
		payload json,
		payloadb jsonb,
		raw bytea,
		tags text[],
		created_at timestamp,
		updated_at timestamptz,
		embedding vector,
		note text DEFAULT 'default-note',
		note_len int generated always as (length(note)) stored,
		PRIMARY KEY (customer_id, item_id)
	);`)
	require.NoError(t, err)

	longNote := string(bytes.Repeat([]byte("customer-note-"), 260))
	textRows := fmt.Sprintf("42\t11111111-1111-1111-1111-111111111111\tt\t123.45\t{\"plan\":\"pro\"}\t{\"state\":\"active\"}\t\\\\x0102ff\t{\"alpha\",\"beta\"}\t2026-01-02 03:04:05\t2026-01-02 03:04:05+00\t[1,2,3]\t%s\n43\t22222222-2222-2222-2222-222222222222\tf\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\tnullable-copy\n", longNote)
	tag, err := connection.Default.PgConn().CopyFrom(ctx, bytes.NewBufferString(textRows), `COPY copy_customer_sync (
		customer_id, item_id, flag, amount, payload, payloadb, raw, tags,
		created_at, updated_at, embedding, note
	) FROM STDIN;`)
	require.NoError(t, err)
	require.Equal(t, "COPY 2", tag.String())

	csvRows := "customer_id,item_id,flag,amount,payload,payloadb,raw,tags,created_at,updated_at,embedding,note\n" +
		"45,33333333-3333-3333-3333-333333333333,t,9999.99,\"{\"\"plan\"\":\"\"csv\"\"}\",\"{\"\"state\"\":\"\"csv\"\"}\",\\x0a0b,\"{\"\"quoted,tag\"\",\"\"plain\"\"}\",\"2026-02-03 04:05:06\",\"2026-02-03 04:05:06+00\",\"[4,5,6]\",\"csv note, with comma\"\n"
	tag, err = connection.Default.PgConn().CopyFrom(ctx, bytes.NewBufferString(csvRows), `COPY copy_customer_sync (
		customer_id, item_id, flag, amount, payload, payloadb, raw, tags,
		created_at, updated_at, embedding, note
	) FROM STDIN WITH (FORMAT CSV, HEADER TRUE);`)
	require.NoError(t, err)
	require.Equal(t, "COPY 1", tag.String())

	_, err = connection.Exec(ctx, `UPDATE copy_customer_sync
		SET amount = amount + 1,
			payloadb = jsonb_set(payloadb, '{state}', '"updated"'::jsonb),
			tags = ARRAY['gamma', 'delta'],
			note = 'patched'
		WHERE customer_id = 42 AND item_id = '11111111-1111-1111-1111-111111111111';`)
	require.NoError(t, err)
	_, err = connection.Exec(ctx, `DELETE FROM copy_customer_sync WHERE customer_id = 43;`)
	require.NoError(t, err)
	_, err = connection.Exec(ctx, `INSERT INTO copy_customer_sync (customer_id, item_id, flag) VALUES (46, '44444444-4444-4444-4444-444444444444', true);`)
	require.NoError(t, err)

	rows, err := connection.Query(ctx, `SELECT
			customer_id::text,
			item_id::text,
			flag::text,
			amount::text,
			payload->>'plan',
			payloadb->>'state',
			raw,
			array_to_string(tags, ','),
			created_at::text,
			(updated_at IS NOT NULL)::text,
			embedding::text,
			length(note)::text,
			note_len::text
		FROM copy_customer_sync
		ORDER BY customer_id, item_id;`)
	require.NoError(t, err)
	readRows, _, err := ReadRows(rows, true)
	require.NoError(t, err)
	require.Equal(t, []sql.Row{
		{"42", "11111111-1111-1111-1111-111111111111", "true", "124.45", "pro", "updated", []byte{0x01, 0x02, 0xff}, "gamma,delta", "2026-01-02 03:04:05", "true", "[1,2,3]", "7", "7"},
		{"45", "33333333-3333-3333-3333-333333333333", "true", "9999.99", "csv", "csv", []byte{0x0a, 0x0b}, "quoted,tag,plain", "2026-02-03 04:05:06", "true", "[4,5,6]", "20", "20"},
		{"46", "44444444-4444-4444-4444-444444444444", "true", nil, nil, nil, nil, nil, nil, "false", nil, "12", "12"},
	}, readRows)
}

func TestBinaryCopyFromAndToStdout(t *testing.T) {
	ctx, connection, controller := CreateServer(t, "postgres")
	defer func() {
		connection.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	_, err := connection.Exec(ctx, "CREATE TABLE copy_binary (id int primary key, name text, amount bigint, note text, embedding vector);")
	require.NoError(t, err)

	binaryInput := buildBinaryCopyData([][][]byte{
		{
			int32Binary(1),
			[]byte("alice"),
			int64Binary(100),
			[]byte("loaded"),
			vectorBinary(1, 2, 3),
		},
		{
			int32Binary(2),
			[]byte("bob"),
			int64Binary(200),
			nil,
			vectorBinary(-1.5, 0, 2.25),
		},
	})
	tag, err := connection.Default.PgConn().CopyFrom(context.Background(), bytes.NewReader(binaryInput), "COPY copy_binary (id, name, amount, note, embedding) FROM STDIN WITH (FORMAT BINARY);")
	require.NoError(t, err)
	require.Equal(t, "COPY 2", tag.String())

	rows, err := connection.Query(ctx, "SELECT * FROM copy_binary ORDER BY id;")
	require.NoError(t, err)
	readRows, _, err := ReadRows(rows, true)
	require.NoError(t, err)
	require.Equal(t, []sql.Row{
		{int64(1), "alice", int64(100), "loaded", "[1,2,3]"},
		{int64(2), "bob", int64(200), nil, "[-1.5,0,2.25]"},
	}, readRows)

	var binaryOut bytes.Buffer
	tag, err = connection.Default.PgConn().CopyTo(ctx, &binaryOut, "COPY copy_binary (id, name, amount, note, embedding) TO STDOUT WITH (FORMAT BINARY);")
	require.NoError(t, err)
	require.Equal(t, "COPY 2", tag.String())

	_, err = connection.Exec(ctx, "CREATE TABLE copy_binary_roundtrip (id int primary key, name text, amount bigint, note text, embedding vector);")
	require.NoError(t, err)
	tag, err = connection.Default.PgConn().CopyFrom(context.Background(), bytes.NewReader(binaryOut.Bytes()), "COPY copy_binary_roundtrip (id, name, amount, note, embedding) FROM STDIN WITH (FORMAT BINARY);")
	require.NoError(t, err)
	require.Equal(t, "COPY 2", tag.String())

	rows, err = connection.Query(ctx, "SELECT * FROM copy_binary_roundtrip ORDER BY id;")
	require.NoError(t, err)
	readRows, _, err = ReadRows(rows, true)
	require.NoError(t, err)
	require.Equal(t, []sql.Row{
		{int64(1), "alice", int64(100), "loaded", "[1,2,3]"},
		{int64(2), "bob", int64(200), nil, "[-1.5,0,2.25]"},
	}, readRows)
}

func buildBinaryCopyData(rows [][][]byte) []byte {
	data := []byte{'P', 'G', 'C', 'O', 'P', 'Y', '\n', 0xff, '\r', '\n', 0}
	data = binary.BigEndian.AppendUint32(data, 0)
	data = binary.BigEndian.AppendUint32(data, 0)
	for _, row := range rows {
		data = binary.BigEndian.AppendUint16(data, uint16(len(row)))
		for _, value := range row {
			if value == nil {
				data = binary.BigEndian.AppendUint32(data, uint32(0xffffffff))
				continue
			}
			data = binary.BigEndian.AppendUint32(data, uint32(len(value)))
			data = append(data, value...)
		}
	}
	return append(data, 0xff, 0xff)
}

func int32Binary(value int32) []byte {
	data := make([]byte, 4)
	binary.BigEndian.PutUint32(data, uint32(value))
	return data
}

func int64Binary(value int64) []byte {
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, uint64(value))
	return data
}

func vectorBinary(values ...float32) []byte {
	data := make([]byte, 4+(len(values)*4))
	binary.BigEndian.PutUint16(data, uint16(len(values)))
	binary.BigEndian.PutUint16(data[2:], 0)
	for i, value := range values {
		binary.BigEndian.PutUint32(data[4+(i*4):], math.Float32bits(value))
	}
	return data
}
