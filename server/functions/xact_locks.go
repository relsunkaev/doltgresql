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

package functions

import (
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
)

// PostgreSQL distinguishes session-scope advisory locks (released via
// pg_advisory_unlock or session end) from transaction-scope advisory
// locks (released automatically when the surrounding transaction ends).
// The underlying go-mysql-server LockSubsystem only models session-scope
// locks, so we maintain a side table here that records every transaction-
// scope acquisition. The connection layer calls ReleaseSessionXactLocks
// when a transaction commits, rolls back, or the connection drops, and
// we replay the recorded acquisitions as Unlocks at that point.

var xactLocksMu sync.Mutex

// xactLocks maps session id to the ordered list of transaction-scope lock
// names acquired by that session. Each entry corresponds to one successful
// Lock call against the LockSubsystem; reentrant acquisitions are tracked
// by repeated names and balanced by repeated Unlocks at release time.
var xactLocks = map[uint32][]string{}

// recordXactLock records that the current session has acquired the given
// transaction-scope advisory lock. Each call appends one entry, even when
// the same lock is reacquired by the same session, so that the matching
// number of Unlocks runs at transaction end.
func recordXactLock(sessionID uint32, name string) {
	xactLocksMu.Lock()
	defer xactLocksMu.Unlock()
	xactLocks[sessionID] = append(xactLocks[sessionID], name)
}

// ReleaseSessionXactLocks releases every transaction-scope advisory lock
// recorded for the current session. It is intended to be called from the
// connection layer at transaction commit, rollback, statement-end in
// autocommit mode, and connection close. It is safe to call when the
// session holds no transaction-scope locks.
func ReleaseSessionXactLocks(ctx *sql.Context) error {
	return ReleaseSessionXactLocksWithSubsystem(ctx, getLockSubsystem())
}

// ReleaseSessionXactLocksWithSubsystem releases transaction-scope advisory locks
// using the lock subsystem owned by the caller's server instance.
func ReleaseSessionXactLocksWithSubsystem(ctx *sql.Context, ls *sql.LockSubsystem) error {
	if ctx == nil || ctx.Session == nil {
		return nil
	}
	id := uint32(ctx.Session.ID())

	xactLocksMu.Lock()
	names := xactLocks[id]
	delete(xactLocks, id)
	xactLocksMu.Unlock()

	if len(names) == 0 {
		return nil
	}
	if ls == nil {
		return nil
	}
	var firstErr error
	for _, name := range names {
		if err := ls.Unlock(ctx, name); err != nil && firstErr == nil && !sql.ErrLockDoesNotExist.Is(err) && !sql.ErrLockNotOwned.Is(err) {
			firstErr = err
		}
	}
	return firstErr
}

// HasSessionXactLocks reports whether the session has any outstanding
// transaction-scope advisory lock acquisitions. Used by the connection
// layer to short-circuit the release path when the session is empty.
func HasSessionXactLocks(sessionID uint32) bool {
	xactLocksMu.Lock()
	defer xactLocksMu.Unlock()
	return len(xactLocks[sessionID]) > 0
}
