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
	"sync"
	"sync/atomic"
	"testing"

	gms "github.com/dolthub/go-mysql-server/sql"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// TestOnConflictDoNothingMultiUniqueConcurrency probes the race
// window between OnConflictDoNothingArbiterTable's pre-check and
// the underlying insert. The pre-check looks at non-target unique
// indexes per-row; if two sessions both pass the pre-check (because
// the conflicting row hasn't committed yet) and then both insert,
// only one of them must end up in the table - the other must
// surface the non-target unique violation, NOT silently no-op via
// GMS's INSERT IGNORE handler.
//
// N goroutines each insert (id = N, email = 'shared@x.com'). Every
// id is unique, so the target index never fires; every email is
// the same, so the non-target index must fire on N-1 of them. The
// expected outcome is one row with one specific id, and N-1 errors
// with SQLSTATE 23505. If we see more than one row, or fewer than
// N-1 errors, the race window slipped through.
func TestOnConflictDoNothingMultiUniqueConcurrency(t *testing.T) {
	port, err := gms.GetEmptyPort()
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
			"postgres://postgres:password@127.0.0.1:%d/postgres?sslmode=disable", port))
		require.NoError(t, err)
		t.Cleanup(func() { _ = conn.Close(ctx) })
		return conn
	}

	for _, q := range []string{
		`CREATE TABLE accounts (id INT PRIMARY KEY, email TEXT UNIQUE);`,
	} {
		_, err := defaultConn.Exec(ctx, q)
		require.NoError(t, err, "setup: %s", q)
	}

	const workers = 8
	const sharedEmail = "shared@x.com"

	var (
		wg            sync.WaitGroup
		successCount  atomic.Int32
		conflictCount atomic.Int32
		otherErrCount atomic.Int32
	)
	start := make(chan struct{})
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			conn := dial(t)
			<-start
			tag, err := conn.Exec(context.Background(),
				`INSERT INTO accounts (id, email) VALUES ($1, $2)
				 ON CONFLICT (id) DO NOTHING;`,
				id+1, sharedEmail)
			if err == nil {
				if tag.RowsAffected() == 1 {
					successCount.Add(1)
				}
				return
			}
			// pgx surfaces the SQLSTATE on the wrapped error.
			pgErr, ok := isPgErrorWithCode(err)
			if ok && pgErr == "23505" {
				conflictCount.Add(1)
				return
			}
			otherErrCount.Add(1)
		}(i)
	}
	close(start)
	wg.Wait()

	// End state: exactly one row.
	var rows int
	require.NoError(t, defaultConn.Default.QueryRow(ctx,
		`SELECT count(*) FROM accounts;`).Scan(&rows))

	t.Logf("success=%d conflict=%d otherErr=%d rows=%d",
		successCount.Load(), conflictCount.Load(), otherErrCount.Load(), rows)

	require.Equal(t, 1, rows,
		"exactly one row must end up in accounts; if >1, the email unique "+
			"constraint slipped through; if 0, all sessions racing a no-op")
	require.Equal(t, int32(0), otherErrCount.Load(),
		"non-23505 errors are unexpected")

	// Of the workers that did NOT win the race, every single one must
	// have observed the non-target unique violation as 23505 - not as
	// a silent no-op via INSERT IGNORE. successCount + conflictCount
	// should equal workers; if the race window leaked, conflictCount
	// will be lower than (workers - successCount).
	require.Equal(t, int32(workers),
		successCount.Load()+conflictCount.Load(),
		"every worker must terminate with success or 23505; "+
			"any silent no-op means the pre-check missed a concurrent insert")
}

// isPgErrorWithCode unwraps err looking for *pgconn.PgError and
// returns its Code.
func isPgErrorWithCode(err error) (string, bool) {
	type pgErr interface {
		SQLState() string
	}
	for cur := err; cur != nil; {
		if pe, ok := cur.(pgErr); ok {
			return pe.SQLState(), true
		}
		type unwrapper interface{ Unwrap() error }
		if u, ok := cur.(unwrapper); ok {
			cur = u.Unwrap()
			continue
		}
		break
	}
	return "", false
}
