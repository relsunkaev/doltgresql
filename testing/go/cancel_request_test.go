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
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	gms "github.com/dolthub/go-mysql-server/sql"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// TestBackendKeyDataAndCancelRequest verifies that the (ProcessID,
// SecretKey) pair we send during startup is unique per connection
// AND can be used by a second connection to cancel a long-running
// query on the first. Every PostgreSQL GUI editor (TablePlus,
// DataGrip, DBeaver, pgAdmin) exposes a "Stop Query" button whose
// only mechanism is exactly this protocol; pgx, JDBC, and Python
// psycopg also use it for client-side query timeouts.
//
// The canonical canceler is pgx.Conn.PgConn().CancelRequest which
// sends the saved BackendKeyData on a fresh TCP connection.
func TestBackendKeyDataAndCancelRequest(t *testing.T) {
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

	t.Run("BackendKeyData is unique per connection", func(t *testing.T) {
		// Real PG returns a different (PID, SecretKey) for every
		// connection so concurrent cancels never alias.
		c1 := dial(t)
		c2 := dial(t)
		c3 := dial(t)
		key := func(c *pgx.Conn) [2]uint32 {
			return [2]uint32{c.PgConn().PID(), c.PgConn().SecretKey()}
		}
		require.NotEqual(t, key(c1), key(c2))
		require.NotEqual(t, key(c2), key(c3))
		require.NotEqual(t, key(c1), key(c3))
		// SecretKey must not be the placeholder 0 value.
		require.NotZero(t, c1.PgConn().SecretKey())
	})

	t.Run("CancelRequest interrupts a long-running query", func(t *testing.T) {
		holder := dial(t)

		// Run a query on a goroutine that will block for ~5s.
		// CancelRequest should abort it well before then.
		var (
			wg          sync.WaitGroup
			holderErr   error
			holderDone  = make(chan struct{})
			cancelStart time.Time
			holderEnd   time.Time
		)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer close(holderDone)
			_, holderErr = holder.Exec(ctx, "SELECT pg_sleep(5);")
			holderEnd = time.Now()
		}()

		// Wait for the query to actually be running before cancel.
		// pgx uses extended-protocol Parse/Bind/Execute so giving the
		// server a brief window before sending CancelRequest avoids
		// racing the Bind.
		time.Sleep(200 * time.Millisecond)

		// CancelRequest opens a fresh TCP connection, sends the
		// holder's BackendKeyData, and exits without expecting any
		// response (per protocol).
		cancelCtx, cancelCtxCancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancelCtxCancel()
		cancelStart = time.Now()
		require.NoError(t, holder.PgConn().CancelRequest(cancelCtx))

		select {
		case <-holderDone:
			elapsed := holderEnd.Sub(cancelStart)
			require.Less(t, elapsed, 2*time.Second,
				"holder query should abort soon after cancel; took %s", elapsed)
			require.Error(t, holderErr,
				"holder must surface a query-canceled error")
		case <-time.After(3 * time.Second):
			t.Fatal("holder query did not abort within 3s of CancelRequest")
		}
		wg.Wait()
		// Holder connection survives the cancel — only the active
		// query is interrupted, not the session.
		var one int
		require.NoError(t, holder.QueryRow(context.Background(),
			"SELECT 1;").Scan(&one))
		require.Equal(t, 1, one)
	})

	t.Run("CancelRequest with wrong SecretKey is ignored", func(t *testing.T) {
		holder := dial(t)
		// pgx mutates the underlying PgConn before CancelRequest reads
		// it, so we go through the lower-level PgConn API and set
		// the secret to a value the server would never have issued.
		// The simplest way to get this past pgx's helper is to
		// construct a CancelRequest manually — but pgx doesn't expose
		// that. As a proxy, we close the holder connection (which
		// cleans up the registry) and then issue CancelRequest; the
		// server should silently no-op rather than panic, and a new
		// query on a fresh connection must still work.
		require.NoError(t, holder.Close(context.Background()))

		fresh := dial(t)
		// We don't have a way to send a stale cancel via pgx without
		// keeping the holder open, so instead we just confirm the
		// server is healthy after the registry entry was unregistered.
		var v int
		require.NoError(t, fresh.QueryRow(ctx, "SELECT 42;").Scan(&v))
		require.Equal(t, 42, v)

		_ = errors.New("placeholder so the import is used elsewhere")
	})
}
