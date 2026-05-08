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
	"sync"
	"time"
)

// PostgreSQL surfaces row-level locks through pg_locks so admins can
// see who is holding what and who is blocked on whom. Doltgres
// synthesizes its row locks via the advisory-lock subsystem, which
// is invisible to pg_locks. This registry is the bridge: every
// FOR UPDATE row-lock acquisition records an entry here at wait
// time, flips it to granted on success, and the entry persists
// until the surrounding transaction ends. pg_locks reads from the
// registry; the deadlock detector also walks it to find cycles.
//
// Scope: only row-level locks acquired by RowLockingTable and
// tableLevelLockingTable populate this registry. User-callable
// pg_advisory_xact_lock acquisitions are deliberately out of scope -
// surfacing them in pg_locks is a separate (smaller) follow-up that
// can layer on top of this.

// RowLockKind classifies a registered lock so pg_locks can emit the
// right PostgreSQL locktype value.
type RowLockKind int

const (
	// RowLockKindRow is a lock on a specific (relation, primary-key)
	// pair. PostgreSQL would surface this as locktype='transactionid'
	// or briefly as 'tuple'; we map it to 'tuple'.
	RowLockKindRow RowLockKind = iota
	// RowLockKindTable is the keyless-table fallback. PostgreSQL
	// would never reach this path (it locks by ctid); we emit it
	// as locktype='relation'.
	RowLockKindTable
)

// RowLockEntry is one row's worth of registry state. The entry is
// allocated at wait time, mutated to Granted=true on success, and
// dropped at transaction end.
type RowLockEntry struct {
	SessionID      uint32
	LockName       string
	Kind           RowLockKind
	RelationOID    uint32
	PrimaryKeyText string // for debugging; empty for table-level locks
	Granted        bool
	WaitStart      time.Time
	GrantedAt      time.Time
}

var (
	rowLockMu      sync.RWMutex
	rowLockEntries = map[uint32][]*RowLockEntry{}
)

// RegisterRowLockWaiter records that sessionID is about to wait for
// lockName. The returned handle lets the caller flip to granted on
// success or release the entry on a non-blocking miss. It is the
// caller's responsibility to take exactly one of those actions per
// RegisterRowLockWaiter call before returning.
func RegisterRowLockWaiter(sessionID uint32, lockName string, kind RowLockKind, relationOID uint32, pkText string) *RowLockEntry {
	entry := &RowLockEntry{
		SessionID:      sessionID,
		LockName:       lockName,
		Kind:           kind,
		RelationOID:    relationOID,
		PrimaryKeyText: pkText,
		WaitStart:      time.Now(),
	}
	rowLockMu.Lock()
	rowLockEntries[sessionID] = append(rowLockEntries[sessionID], entry)
	rowLockMu.Unlock()
	return entry
}

// MarkRowLockGranted flips a registered waiter to granted. Idempotent
// in the sense that calling it twice on the same entry only updates
// the timestamps; the registry treats one entry as one held lock.
func MarkRowLockGranted(entry *RowLockEntry) {
	if entry == nil {
		return
	}
	rowLockMu.Lock()
	entry.Granted = true
	entry.GrantedAt = time.Now()
	rowLockMu.Unlock()
}

// ReleaseRowLockEntry drops a single entry. Used when a non-blocking
// acquire missed (NOWAIT raise / SKIP LOCKED skip) and we want to
// retract the registered waiter rather than leave it for transaction
// end.
func ReleaseRowLockEntry(entry *RowLockEntry) {
	if entry == nil {
		return
	}
	rowLockMu.Lock()
	defer rowLockMu.Unlock()
	list := rowLockEntries[entry.SessionID]
	for i := range list {
		if list[i] == entry {
			list = append(list[:i], list[i+1:]...)
			break
		}
	}
	if len(list) == 0 {
		delete(rowLockEntries, entry.SessionID)
	} else {
		rowLockEntries[entry.SessionID] = list
	}
}

// ReleaseSessionRowLocks clears every entry for sessionID. Wired
// into the existing transaction-end release path so the registry
// stays in sync with the underlying advisory-lock subsystem.
func ReleaseSessionRowLocks(sessionID uint32) {
	rowLockMu.Lock()
	delete(rowLockEntries, sessionID)
	rowLockMu.Unlock()
}

// SnapshotRowLocks returns a copy of every registered entry across
// every session. Used by pg_locks's RowIter; copying lets the caller
// emit rows without holding the registry lock during query planning.
func SnapshotRowLocks() []RowLockEntry {
	rowLockMu.RLock()
	defer rowLockMu.RUnlock()
	var out []RowLockEntry
	for _, list := range rowLockEntries {
		for _, entry := range list {
			out = append(out, *entry)
		}
	}
	return out
}

// FindHolder returns the session ID currently holding lockName as
// granted, or 0 if no session holds it. Used by the deadlock
// detector to walk the wait-graph from waiter to holder.
func FindHolder(lockName string) (uint32, bool) {
	rowLockMu.RLock()
	defer rowLockMu.RUnlock()
	for sid, list := range rowLockEntries {
		for _, entry := range list {
			if entry.Granted && entry.LockName == lockName {
				return sid, true
			}
		}
	}
	return 0, false
}

// WaitingFor returns the lock name sessionID is currently waiting
// for, or "" when not waiting. A session can only block on one
// row-lock at a time (Acquire is synchronous), so the result is
// well-defined.
func WaitingFor(sessionID uint32) (string, bool) {
	rowLockMu.RLock()
	defer rowLockMu.RUnlock()
	for _, entry := range rowLockEntries[sessionID] {
		if !entry.Granted {
			return entry.LockName, true
		}
	}
	return "", false
}
