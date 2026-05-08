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
	"bytes"
	"fmt"
	"os/exec"
	"strings"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
)

// TestCopyFromStdinPsqlRestore feeds a dump-shaped SQL stream through psql so
// COPY FROM stdin is proven on the real restore protocol path.
func TestCopyFromStdinPsqlRestore(t *testing.T) {
	psqlPath, err := exec.LookPath("psql")
	if err != nil {
		t.Skip("psql is not installed")
	}

	port, err := sql.GetEmptyPort()
	require.NoError(t, err)
	ctx, connection, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		connection.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	restoreSQL := `CREATE TABLE public.copy_restore_seed (
    id integer NOT NULL,
    tenant_id uuid NOT NULL,
    flag boolean NOT NULL DEFAULT false,
    amount numeric(12,2),
    payload jsonb,
    note text,
    PRIMARY KEY (id, tenant_id)
);

COPY public.copy_restore_seed (id, tenant_id, flag, amount, payload, note) FROM stdin;
1	11111111-1111-1111-1111-111111111111	t	12.34	{"state":"text"}	text seed
2	22222222-2222-2222-2222-222222222222	f	\N	\N	\N
\.

COPY public.copy_restore_seed (id, tenant_id, flag, amount, payload, note) FROM stdin WITH (FORMAT csv, HEADER true);
id,tenant_id,flag,amount,payload,note
3,33333333-3333-3333-3333-333333333333,t,99.01,"{""state"":""csv""}","csv note, with comma"
\.
`

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd := exec.Command(
		psqlPath,
		"-v", "ON_ERROR_STOP=1",
		fmt.Sprintf("postgresql://postgres:password@localhost:%d/postgres?sslmode=disable", port),
	)
	cmd.Stdin = strings.NewReader(restoreSQL)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	require.NoErrorf(t, cmd.Run(), "stdout:\n%s\nstderr:\n%s", stdout.String(), stderr.String())

	rows, err := connection.Query(ctx, `SELECT
			id::text,
			tenant_id::text,
			flag::text,
			amount::text,
			payload->>'state',
			note
		FROM public.copy_restore_seed
		ORDER BY id;`)
	require.NoError(t, err)
	readRows, _, err := ReadRows(rows, true)
	require.NoError(t, err)
	require.Equal(t, []sql.Row{
		{"1", "11111111-1111-1111-1111-111111111111", "true", "12.34", "text", "text seed"},
		{"2", "22222222-2222-2222-2222-222222222222", "false", nil, nil, nil},
		{"3", "33333333-3333-3333-3333-333333333333", "true", "99.01", "csv", "csv note, with comma"},
	}, readRows)
}
