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
	gosql "database/sql"
	"fmt"
	"sync"
	"testing"

	gms "github.com/dolthub/go-mysql-server/sql"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

// TestGoSQLLibPQClientSmoke runs the common Go database/sql client path
// through github.com/lib/pq. The C libpq harness pins the raw client library;
// this covers the Go driver wrapper that real Go applications usually use.
func TestGoSQLLibPQClientSmoke(t *testing.T) {
	port, err := gms.GetEmptyPort()
	require.NoError(t, err)
	ctx, defaultConn, controller := CreateServerWithPort(t, "postgres", port)
	t.Cleanup(func() {
		defaultConn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	db, err := gosql.Open("postgres", fmt.Sprintf(
		"postgres://postgres:password@127.0.0.1:%d/postgres?sslmode=disable&application_name=go-sql-pq-harness",
		port,
	))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)
	require.NoError(t, db.PingContext(ctx))

	var appName string
	require.NoError(t, db.QueryRowContext(ctx, `SELECT current_setting('application_name')`).Scan(&appName))
	require.Equal(t, "go-sql-pq-harness", appName)

	for _, stmt := range []string{
		`CREATE TABLE go_sql_pq_accounts (
			id INT PRIMARY KEY,
			name TEXT NOT NULL UNIQUE,
			active BOOLEAN NOT NULL
		)`,
		`CREATE TABLE go_sql_pq_items (
			id INT PRIMARY KEY,
			account_id INT NOT NULL REFERENCES go_sql_pq_accounts(id),
			amount NUMERIC(10,2) NOT NULL,
			tags TEXT[] NOT NULL,
			payload JSONB NOT NULL
		)`,
	} {
		_, err = db.ExecContext(ctx, stmt)
		require.NoError(t, err, stmt)
	}

	accountInsert, err := db.PrepareContext(ctx, `
		INSERT INTO go_sql_pq_accounts (id, name, active)
		VALUES ($1::int4, $2::text, $3::bool)
	`)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, accountInsert.Close()) })
	for _, account := range []struct {
		id     int
		name   string
		active bool
	}{
		{1, "acme", true},
		{2, "beta", false},
	} {
		_, err = accountInsert.ExecContext(ctx, account.id, account.name, account.active)
		require.NoError(t, err)
	}

	var amount, secondTag, kind string
	err = db.QueryRowContext(ctx, `
		INSERT INTO go_sql_pq_items (id, account_id, amount, tags, payload)
		VALUES ($1::int4, $2::int4, $3::text::numeric, $4::text[], $5::jsonb)
		RETURNING amount::text, tags[2], payload #>> '{kind}'
	`, 10, 1, "12.34", pq.Array([]string{"red", "blue"}), `{"kind":"invoice","lines":[1,2]}`).Scan(&amount, &secondTag, &kind)
	require.NoError(t, err)
	require.Equal(t, "12.34", amount)
	require.Equal(t, "blue", secondTag)
	require.Equal(t, "invoice", kind)

	prepared, err := db.PrepareContext(ctx, `
		SELECT a.name, a.active, i.amount::text, i.tags, i.payload #>> '{kind}'
		FROM go_sql_pq_items i
		JOIN go_sql_pq_accounts a ON a.id = i.account_id
		WHERE i.account_id = $1::int4 AND $2::text = ANY(i.tags)
	`)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, prepared.Close()) })

	var name string
	var active bool
	var tags pq.StringArray
	err = prepared.QueryRowContext(ctx, 1, "blue").Scan(&name, &active, &amount, &tags, &kind)
	require.NoError(t, err)
	require.Equal(t, "acme", name)
	require.True(t, active)
	require.Equal(t, "12.34", amount)
	require.Equal(t, pq.StringArray{"red", "blue"}, tags)
	require.Equal(t, "invoice", kind)

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, id := range []int{1, 2} {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			var got string
			errs <- db.QueryRowContext(ctx, `
				SELECT name
				FROM go_sql_pq_accounts
				WHERE id = $1::int4
			`, id).Scan(&got)
		}(id)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, `INSERT INTO go_sql_pq_accounts VALUES ($1::int4, $2::text, $3::bool)`, 3, "gamma", true)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	tx, err = db.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, `INSERT INTO go_sql_pq_accounts VALUES ($1::int4, $2::text, $3::bool)`, 4, "rolled back", true)
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())

	var accounts string
	err = db.QueryRowContext(ctx, `
		SELECT array_to_string(array_agg(name ORDER BY id), ',')
		FROM go_sql_pq_accounts
	`).Scan(&accounts)
	require.NoError(t, err)
	require.Equal(t, "acme,beta,gamma", accounts)
}
