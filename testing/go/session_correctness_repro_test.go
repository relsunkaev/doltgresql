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
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// TestDiscardAllRejectedInsideTransaction guards that PostgreSQL's
// transaction-block restriction on DISCARD ALL is enforced: the statement
// resets session state that cannot be rolled back, so it must not run while
// a transaction is open.
func TestDiscardAllRejectedInsideTransaction(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DISCARD ALL inside transaction is rejected",
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query:       `DISCARD ALL;`,
					ExpectedErr: `DISCARD ALL cannot run inside a transaction block`,
				},
				{
					Query: `ROLLBACK;`,
				},
			},
		},
		{
			Name: "DISCARD ALL outside transaction succeeds",
			Assertions: []ScriptTestAssertion{
				{
					Query: `DISCARD ALL;`,
				},
			},
		},
		{
			Name: "DISCARD ALL after COMMIT succeeds in fresh session state",
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query: `DISCARD ALL;`,
				},
			},
		},
		{
			Name: "DISCARD ALL after ROLLBACK succeeds in fresh session state",
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `DISCARD ALL;`,
				},
			},
		},
	})
}

// TestDiscardTempDropsTemporaryTablesRepro reproduces a temp-object persistence
// bug: PostgreSQL accepts DISCARD TEMP and drops all temporary tables in the
// current session.
func TestDiscardTempDropsTemporaryTablesRepro(t *testing.T) {
	ctx, conn, controller := CreateServer(t, "postgres")
	t.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	for _, stmt := range []string{
		`CREATE TEMPORARY TABLE discard_temp_items (id INT);`,
		`INSERT INTO discard_temp_items VALUES (1);`,
	} {
		_, err := conn.Exec(ctx, stmt)
		require.NoError(t, err, stmt)
	}

	_, discardErr := conn.Exec(ctx, `DISCARD TEMP;`)

	var remainingRows int
	countErr := conn.Current.QueryRow(ctx, `SELECT count(*) FROM discard_temp_items;`).Scan(&remainingRows)

	require.NoError(t, discardErr, "DISCARD TEMP should succeed and drop temporary tables; remainingRows=%d countErr=%v", remainingRows, countErr)
	require.Error(t, countErr)
	require.Contains(t, countErr.Error(), "not found")
}

// TestDiscardAllUnlistensChannelsRepro reproduces a session-state persistence
// bug: PostgreSQL's DISCARD ALL includes UNLISTEN *, so notifications sent
// afterward should not be delivered to the discarded session.
func TestDiscardAllUnlistensChannelsRepro(t *testing.T) {
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)
	ctx, defaultConn, controller := CreateServerWithPort(t, "postgres", port)
	t.Cleanup(func() {
		defaultConn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	dial := func(t *testing.T) *pgx.Conn {
		t.Helper()
		conn, err := pgx.Connect(ctx, fmt.Sprintf(
			"postgres://postgres:password@127.0.0.1:%d/postgres?sslmode=disable",
			port,
		))
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, conn.Close(context.Background()))
		})
		return conn
	}

	listener := dial(t)
	notifier := dial(t)

	_, err = listener.Exec(ctx, `LISTEN discard_all_channel;`)
	require.NoError(t, err)
	_, err = listener.Exec(ctx, `DISCARD ALL;`)
	require.NoError(t, err)

	_, err = notifier.Exec(ctx, `NOTIFY discard_all_channel, 'after discard';`)
	require.NoError(t, err)

	notificationCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	notification, err := listener.WaitForNotification(notificationCtx)
	delivered := "<none>"
	if notification != nil {
		delivered = fmt.Sprintf("%s/%s", notification.Channel, notification.Payload)
	}
	require.ErrorIs(t, err, context.DeadlineExceeded, "DISCARD ALL should unlisten all channels; delivered=%s", delivered)
}

// TestSetSessionAuthorizationChangesCurrentAndSessionUserRepro reproduces a
// session authorization correctness gap: PostgreSQL lets an elevated session
// switch session authorization to another role.
func TestSetSessionAuthorizationChangesCurrentAndSessionUserRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "SET SESSION AUTHORIZATION changes current and session user",
			SetUpScript: []string{
				`CREATE USER session_auth_target PASSWORD 'pw';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SET SESSION AUTHORIZATION session_auth_target;`,
				},
				{
					Query:    `SELECT current_user, session_user;`,
					Expected: []sql.Row{{"session_auth_target", "session_auth_target"}},
				},
				{
					Query: `RESET SESSION AUTHORIZATION;`,
				},
				{
					Query:    `SELECT current_user, session_user;`,
					Expected: []sql.Row{{"postgres", "postgres"}},
				},
			},
		},
	})
}

// TestSetLocalSearchPathAcceptsSchemaListRepro reproduces a session
// correctness bug: PostgreSQL accepts SET LOCAL search_path with the same
// comma-separated schema list syntax as SET search_path.
func TestSetLocalSearchPathAcceptsSchemaListRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "SET LOCAL search_path accepts a schema list",
			SetUpScript: []string{
				`CREATE SCHEMA set_local_path_first;`,
				`CREATE TABLE set_local_path_first.local_items (
					id INT PRIMARY KEY
				);`,
				`INSERT INTO set_local_path_first.local_items VALUES (1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query:            `SET LOCAL search_path = set_local_path_first, public;`,
					SkipResultsCheck: true,
				},
				{
					Query:    `SELECT count(*) FROM local_items;`,
					Expected: []sql.Row{{int64(1)}},
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query:       `SELECT count(*) FROM local_items;`,
					ExpectedErr: `not found`,
				},
			},
		},
	})
}

// TestSetLocalSearchPathDefaultUsesDefaultValueRepro reproduces a session
// correctness bug: SET LOCAL search_path TO DEFAULT should reset the
// transaction-local search_path to PostgreSQL's default value, not store the
// literal string DEFAULT.
func TestSetLocalSearchPathDefaultUsesDefaultValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "SET LOCAL search_path TO DEFAULT uses the default search path",
			SetUpScript: []string{
				`CREATE SCHEMA set_local_default_shadow;`,
				`CREATE TABLE public.local_default_items (
					id INT PRIMARY KEY
				);`,
				`CREATE TABLE set_local_default_shadow.local_default_items (
					id INT PRIMARY KEY
				);`,
				`INSERT INTO public.local_default_items VALUES (1);`,
				`INSERT INTO set_local_default_shadow.local_default_items VALUES (2);`,
				`SET search_path = set_local_default_shadow, public;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query:            `SET LOCAL search_path TO DEFAULT;`,
					SkipResultsCheck: true,
				},
				{
					Query:    `SELECT id FROM local_default_items;`,
					Expected: []sql.Row{{1}},
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query:    `SELECT id FROM local_default_items;`,
					Expected: []sql.Row{{2}},
				},
			},
		},
	})
}
