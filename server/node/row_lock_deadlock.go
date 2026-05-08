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

package node

import (
	"time"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
)

// PostgreSQL aborts one transaction when two sessions deadlock on
// row locks (SQLSTATE 40P01). Doltgres' synthesized row lock sits
// on top of an advisory-lock subsystem that is unaware of
// transaction-level wait relationships, so we detect the cycle in
// the registry instead. Without this, an application that takes
// row locks in opposite orders across sessions would hang forever
// rather than getting a "deadlock detected" error and the chance
// to retry - which is the behavior every PostgreSQL ORM and
// transaction helper assumes is in place.

// deadlockPollInterval controls how often the poll loop wakes up
// to retry the lock and re-walk the wait-graph. 10ms is short
// enough to keep latency on uncontended waits low, and long
// enough to avoid burning CPU on busy systems. Real PostgreSQL's
// deadlock_timeout default is 1s, but it has a smarter primary
// path (block until signaled). Our poll cadence is the
// price-equivalent because we do not have signals from the lock
// subsystem.
const deadlockPollInterval = 10 * time.Millisecond

// acquireWithDeadlockDetection blocks until the calling session
// holds lockName, returning a SQLSTATE-40P01 deadlock error if
// waiting would form a cycle in the row-lock wait-graph. The
// caller is responsible for the registry RegisterRowLockWaiter /
// MarkRowLockGranted / ReleaseRowLockEntry sequencing - this
// function only blocks on TryAcquire and walks the registry.
//
// Polling cadence: if the lock cannot be taken immediately we
// re-check every deadlockPollInterval. Each tick we recompute the
// wait-graph, so a cycle that forms midway through the wait is
// caught - not only those present at request time. Context
// cancellation breaks the loop with ctx.Err().
func acquireWithDeadlockDetection(ctx *sql.Context, lockName string) error {
	sessionID := uint32(ctx.Session.ID())
	for {
		ok, err := rowLockerFuncs.TryAcquire(ctx, lockName)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		if detectDeadlock(sessionID, lockName) {
			return pgerror.Newf(pgcode.DeadlockDetected, "deadlock detected")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(deadlockPollInterval):
		}
	}
}

// detectDeadlock walks the wait-graph from (starterSession, waiting
// for starterLock) following the chain
//
//	starterSession --waits-for--> starterLock
//	starterLock    --held-by-->   holder1
//	holder1        --waits-for--> lockHolder1IsWaitingFor
//	lockHolder1IsWaitingFor --held-by--> holder2
//	...
//
// and returns true when the chain re-enters starterSession AND
// starterSession is the smallest-ID member of the cycle. The
// "smallest-ID is victim" rule lets every participant in the
// same cycle reach the same verdict independently - so two
// detectors firing concurrently converge on a single abort
// rather than both aborting (which would be the natural race
// outcome of "whoever notices the cycle aborts itself").
//
// Cycles among sessions that don't include the starter are not
// our deadlock to break. A holder that isn't itself waiting
// terminates the chain (false). A holder that re-appears in the
// visited set without being the starter also terminates (also
// false).
func detectDeadlock(starterSession uint32, starterLock string) bool {
	cycleMembers := []uint32{starterSession}
	visited := map[uint32]bool{starterSession: true}
	currentLock := starterLock
	for {
		holder, ok := FindHolder(currentLock)
		if !ok {
			return false
		}
		if holder == starterSession {
			return isMinOf(starterSession, cycleMembers)
		}
		if visited[holder] {
			return false
		}
		visited[holder] = true
		cycleMembers = append(cycleMembers, holder)
		next, waiting := WaitingFor(holder)
		if !waiting {
			return false
		}
		currentLock = next
	}
}

// isMinOf returns whether candidate is the smallest value in
// members. Used to pick the canonical deadlock victim.
func isMinOf(candidate uint32, members []uint32) bool {
	for _, m := range members {
		if m < candidate {
			return false
		}
	}
	return true
}
