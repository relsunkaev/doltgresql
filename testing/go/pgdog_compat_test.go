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
	"context"
	"fmt"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestPgDogCompatibilityBoundary(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "PgDog supported primary shard boundary",
			SetUpScript: []string{
				"CREATE TABLE pgdog_items (tenant_id BIGINT PRIMARY KEY, label TEXT);",
				"INSERT INTO pgdog_items VALUES (1, 'one'), (2, 'two');",
				"CREATE TABLE pgdog_vectors (tenant_id vector PRIMARY KEY, label TEXT);",
				"INSERT INTO pgdog_vectors VALUES ('[1,0]'::vector, 'vector-one'), ('[2,0]'::vector, 'vector-two');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    "SELECT label FROM pgdog_items WHERE tenant_id = 1;",
					Expected: []sql.Row{{"one"}},
				},
				{
					Query:    "SELECT count(*) FROM pg_replication_slots;",
					Expected: []sql.Row{{0}},
				},
				{
					Query:    "SELECT count(*) FROM pg_stat_replication;",
					Expected: []sql.Row{{0}},
				},
				{
					Query:    "SELECT count(*) FROM pg_publication;",
					Expected: []sql.Row{{0}},
				},
				{
					Query:    "SELECT pg_is_in_recovery();",
					Expected: []sql.Row{{"f"}},
				},
				{
					Query: "SELECT label FROM pgdog_vectors WHERE tenant_id = '[1,0]'::vector;",
					Expected: []sql.Row{
						{"vector-one"},
					},
				},
			},
		},
		{
			Name: "PgDog prepared transaction lifecycle",
			SetUpScript: []string{
				"CREATE TABLE pgdog_items (tenant_id BIGINT PRIMARY KEY, label TEXT);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "BEGIN;",
				},
				{
					Query: "INSERT INTO pgdog_items VALUES (10, 'ten');",
				},
				{
					Query: "PREPARE TRANSACTION 'dg_pgdog';",
				},
				{
					Query: "SELECT gid FROM pg_catalog.pg_prepared_xacts WHERE gid = 'dg_pgdog';",
					Expected: []sql.Row{
						{"dg_pgdog"},
					},
				},
				{
					Query: "COMMIT PREPARED 'dg_pgdog';",
				},
				{
					Query: "SELECT label FROM pgdog_items WHERE tenant_id = 10;",
					Expected: []sql.Row{
						{"ten"},
					},
				},
			},
		},
		{
			Name: "PgDog logical replication metadata probes",
			SetUpScript: []string{
				"CREATE TABLE pgdog_items (tenant_id BIGINT PRIMARY KEY, label TEXT);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "CREATE PUBLICATION dg_pgdog_pub FOR TABLE pgdog_items;",
				},
				{
					Query: "SELECT pubname FROM pg_catalog.pg_publication WHERE pubname = 'dg_pgdog_pub';",
					Expected: []sql.Row{
						{"dg_pgdog_pub"},
					},
				},
				{
					Query: "CREATE SUBSCRIPTION dg_pgdog_sub CONNECTION 'host=127.0.0.1 dbname=postgres' PUBLICATION dg_pgdog_pub WITH (connect=false, enabled=false, create_slot=false, slot_name=NONE);",
				},
				{
					Query: "SELECT subname, subenabled, subslotname IS NULL, array_to_string(subpublications, ',') FROM pg_catalog.pg_subscription WHERE subname = 'dg_pgdog_sub';",
					Expected: []sql.Row{
						{"dg_pgdog_sub", "f", "t", "dg_pgdog_pub"},
					},
				},
				{
					Query:       "CREATE SUBSCRIPTION dg_pgdog_bad_sub CONNECTION 'host=127.0.0.1 dbname=postgres' PUBLICATION dg_pgdog_pub;",
					ExpectedErr: "connect=false",
				},
				{
					Query: "PREPARE dg_pgdog_stmt(int) AS SELECT $1::int + 1;",
				},
				{
					Query: "EXECUTE dg_pgdog_stmt(41);",
					Expected: []sql.Row{
						{42},
					},
				},
				{
					Query: "SELECT name, from_sql FROM pg_catalog.pg_prepared_statements WHERE name = 'dg_pgdog_stmt';",
					Expected: []sql.Row{
						{"dg_pgdog_stmt", "t"},
					},
				},
				{
					Query: "SELECT pg_current_wal_lsn();",
					Expected: []sql.Row{
						{"0/0"},
					},
				},
				{
					Query: "SELECT pg_wal_lsn_diff('0/1'::pg_lsn, '0/0'::pg_lsn);",
					Expected: []sql.Row{
						{Numeric("1")},
					},
				},
			},
		},
	})
}

func TestPgDogStartupRuntimeParameters(t *testing.T) {
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		controller.Stop()
		err := controller.WaitForStop()
		require.NoError(t, err)
	}()
	defer conn.Close(ctx)

	config, err := pgx.ParseConfig(fmt.Sprintf("postgres://postgres:password@127.0.0.1:%d/postgres?sslmode=disable", port))
	require.NoError(t, err)
	config.RuntimeParams["timezone"] = "UTC"

	pgxConn, err := pgx.ConnectConfig(ctx, config)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, pgxConn.Close(context.Background()))
	}()

	var timezone string
	require.NoError(t, pgxConn.QueryRow(ctx, "SHOW TimeZone;").Scan(&timezone))
	require.Equal(t, "UTC", timezone)
}
