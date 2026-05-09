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

// TestAppCompatibilityBasicSmoke keeps the lower-risk application
// compatibility surfaces pinned through a real pgx wire-protocol client.
func TestAppCompatibilityBasicSmoke(t *testing.T) {
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)
	ctx, defaultConn, controller := CreateServerWithPort(t, "postgres", port)
	t.Cleanup(func() {
		defaultConn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	conn, err := pgx.Connect(ctx, fmt.Sprintf(
		"postgres://postgres:password@127.0.0.1:%d/postgres?sslmode=disable",
		port,
	))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, conn.Close(context.Background()))
	})
	_, err = conn.Exec(ctx, `SET TimeZone TO 'UTC'`)
	require.NoError(t, err)

	for _, stmt := range []string{
		`CREATE TYPE smoke_status AS ENUM ('new', 'done')`,
		`CREATE TABLE smoke_accounts (
			id INT PRIMARY KEY,
			name TEXT NOT NULL UNIQUE
		)`,
		`CREATE TABLE smoke_items (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			account_id INT NOT NULL REFERENCES smoke_accounts(id),
			status smoke_status NOT NULL DEFAULT 'new',
			created_at TIMESTAMP NOT NULL,
			seen_at TIMESTAMPTZ NOT NULL,
			amount NUMERIC(10,2) NOT NULL,
			active BOOLEAN NOT NULL,
			label VARCHAR(32) NOT NULL,
			tags TEXT[] NOT NULL,
			payload JSONB NOT NULL,
			UNIQUE (account_id, label)
		)`,
		`CREATE INDEX smoke_items_account_idx ON smoke_items(account_id)`,
		`INSERT INTO smoke_accounts VALUES (1, 'acme'), (2, 'beta')`,
		`INSERT INTO smoke_items (account_id, created_at, seen_at, amount, active, label, tags, payload) VALUES
			(1, TIMESTAMP '2026-01-01 10:00:00', TIMESTAMPTZ '2026-01-01 10:00:00+00', 12.34, TRUE, 'first', ARRAY['red','blue'], '{"items":[1,2],"meta":{"kind":"invoice"}}'::jsonb),
			(2, TIMESTAMP '2026-01-02 10:00:00', TIMESTAMPTZ '2026-01-02 10:00:00+00', 56.78, FALSE, 'second', ARRAY['green'], '{"items":[3],"meta":{"kind":"receipt"}}'::jsonb)`,
	} {
		_, err = conn.Exec(ctx, stmt)
		require.NoError(t, err, stmt)
	}

	var idLength, status, createdAt, seenAt, amount, active, label, kind, tagCount string
	err = conn.QueryRow(ctx, `
		SELECT length(id::text)::text, status::text, created_at::text, seen_at::text, amount::text, active::text, label,
		       payload #>> '{meta,kind}', array_length(tags, 1)::text
		FROM smoke_items
		WHERE account_id = 1
	`).Scan(&idLength, &status, &createdAt, &seenAt, &amount, &active, &label, &kind, &tagCount)
	require.NoError(t, err)
	require.Equal(t, "36", idLength)
	require.Equal(t, "new", status)
	require.Equal(t, "2026-01-01 10:00:00", createdAt)
	require.Equal(t, "2026-01-01 10:00:00+00", seenAt)
	require.Equal(t, "12.34", amount)
	require.Equal(t, "true", active)
	require.Equal(t, "first", label)
	require.Equal(t, "invoice", kind)
	require.Equal(t, "2", tagCount)

	_, err = conn.Exec(ctx, `
		UPDATE smoke_items
		SET payload = jsonb_set(payload, '{meta,kind}', '"updated"'::jsonb)
		WHERE label = 'first'
	`)
	require.NoError(t, err)

	keyRows, err := conn.Query(ctx, `
		SELECT key
		FROM smoke_items AS s
		JOIN LATERAL jsonb_object_keys(s.payload) AS key ON true
		WHERE s.label = 'first'
		ORDER BY key
	`)
	require.NoError(t, err)
	var jsonKeys []string
	for keyRows.Next() {
		var key string
		require.NoError(t, keyRows.Scan(&key))
		jsonKeys = append(jsonKeys, key)
	}
	require.NoError(t, keyRows.Err())
	keyRows.Close()
	require.Equal(t, []string{"items", "meta"}, jsonKeys)

	itemRows, err := conn.Query(ctx, `
		SELECT elem
		FROM smoke_items AS s
		JOIN LATERAL jsonb_array_elements(s.payload->'items') AS elem ON true
		WHERE s.label = 'first'
		ORDER BY elem
	`)
	require.NoError(t, err)
	var jsonItems []string
	for itemRows.Next() {
		var elem string
		require.NoError(t, itemRows.Scan(&elem))
		jsonItems = append(jsonItems, elem)
	}
	require.NoError(t, itemRows.Err())
	itemRows.Close()
	require.Equal(t, []string{"1", "2"}, jsonItems)

	var jsonAggregate string
	err = conn.QueryRow(ctx, `
		SELECT json_agg(label ORDER BY label)::text
		FROM smoke_items
	`).Scan(&jsonAggregate)
	require.NoError(t, err)
	require.Equal(t, `["first","second"]`, jsonAggregate)

	var arraySummary string
	err = conn.QueryRow(ctx, `
		SELECT array_to_string(array_agg(label ORDER BY label), ',')
		FROM smoke_items
		WHERE account_id = ANY(ARRAY[1, 2]) AND 'red' = ANY(tags)
	`).Scan(&arraySummary)
	require.NoError(t, err)
	require.Equal(t, "first", arraySummary)

	tx, err := conn.Begin(ctx)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, `INSERT INTO smoke_accounts VALUES (3, 'gamma')`)
	require.NoError(t, err)
	nested, err := tx.Begin(ctx)
	require.NoError(t, err)
	_, err = nested.Exec(ctx, `INSERT INTO smoke_accounts VALUES (4, 'rolled back')`)
	require.NoError(t, err)
	require.NoError(t, nested.Rollback(ctx))
	_, err = tx.Exec(ctx, `INSERT INTO smoke_accounts VALUES (5, 'delta')`)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	var accounts string
	err = conn.QueryRow(ctx, `
		SELECT array_to_string(array_agg(name ORDER BY id), ',')
		FROM smoke_accounts
	`).Scan(&accounts)
	require.NoError(t, err)
	require.Equal(t, "acme,beta,gamma,delta", accounts)
}
