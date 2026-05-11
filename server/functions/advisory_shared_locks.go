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
	"time"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

type sharedAdvisoryLockRegistry struct {
	mu      sync.Mutex
	holders map[string]map[uint32]int
}

var sharedAdvisoryLocks = &sharedAdvisoryLockRegistry{
	holders: make(map[string]map[uint32]int),
}

// pg_advisory_lock_shared_bigint represents pg_advisory_lock_shared(bigint).
var pg_advisory_lock_shared_bigint = framework.Function1{
	Name:               "pg_advisory_lock_shared",
	Return:             pgtypes.Void,
	Parameters:         [1]*pgtypes.DoltgresType{pgtypes.Int64},
	IsNonDeterministic: true,
	Strict:             true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val1 any) (any, error) {
		return nil, acquireSharedAdvisoryLock(ctx, advisoryLockNameInt8(val1.(int64)), -1)
	},
}

// pg_advisory_lock_shared_int4_int4 represents pg_advisory_lock_shared(int4, int4).
var pg_advisory_lock_shared_int4_int4 = framework.Function2{
	Name:               "pg_advisory_lock_shared",
	Return:             pgtypes.Void,
	Parameters:         [2]*pgtypes.DoltgresType{pgtypes.Int32, pgtypes.Int32},
	IsNonDeterministic: true,
	Strict:             true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		return nil, acquireSharedAdvisoryLock(ctx, advisoryLockNameInt4Pair(val1.(int32), val2.(int32)), -1)
	},
}

// pg_try_advisory_lock_shared_bigint represents pg_try_advisory_lock_shared(bigint).
var pg_try_advisory_lock_shared_bigint = framework.Function1{
	Name:               "pg_try_advisory_lock_shared",
	Return:             pgtypes.Bool,
	Parameters:         [1]*pgtypes.DoltgresType{pgtypes.Int64},
	IsNonDeterministic: true,
	Strict:             true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val1 any) (any, error) {
		return tryAcquireSharedAdvisoryLock(ctx, advisoryLockNameInt8(val1.(int64)))
	},
}

// pg_try_advisory_lock_shared_int4_int4 represents pg_try_advisory_lock_shared(int4, int4).
var pg_try_advisory_lock_shared_int4_int4 = framework.Function2{
	Name:               "pg_try_advisory_lock_shared",
	Return:             pgtypes.Bool,
	Parameters:         [2]*pgtypes.DoltgresType{pgtypes.Int32, pgtypes.Int32},
	IsNonDeterministic: true,
	Strict:             true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		return tryAcquireSharedAdvisoryLock(ctx, advisoryLockNameInt4Pair(val1.(int32), val2.(int32)))
	},
}

// pg_advisory_unlock_shared_bigint represents pg_advisory_unlock_shared(bigint).
var pg_advisory_unlock_shared_bigint = framework.Function1{
	Name:               "pg_advisory_unlock_shared",
	Return:             pgtypes.Bool,
	Parameters:         [1]*pgtypes.DoltgresType{pgtypes.Int64},
	IsNonDeterministic: true,
	Strict:             true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val1 any) (any, error) {
		return releaseSharedAdvisoryLock(ctx, advisoryLockNameInt8(val1.(int64)))
	},
}

// pg_advisory_unlock_shared_int4_int4 represents pg_advisory_unlock_shared(int4, int4).
var pg_advisory_unlock_shared_int4_int4 = framework.Function2{
	Name:               "pg_advisory_unlock_shared",
	Return:             pgtypes.Bool,
	Parameters:         [2]*pgtypes.DoltgresType{pgtypes.Int32, pgtypes.Int32},
	IsNonDeterministic: true,
	Strict:             true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		return releaseSharedAdvisoryLock(ctx, advisoryLockNameInt4Pair(val1.(int32), val2.(int32)))
	},
}

func acquireExclusiveAdvisoryLock(ctx *sql.Context, name string, timeout time.Duration, txn bool) error {
	start := time.Now()
	for {
		ok, err := tryAcquireExclusiveAdvisoryLock(ctx, name, txn)
		if ok || err != nil {
			return err
		}
		if timeout == 0 || (timeout > 0 && time.Since(start) >= timeout) {
			return sql.ErrLockTimeout.New(name)
		}
		time.Sleep(100 * time.Microsecond)
	}
}

func tryAcquireExclusiveAdvisoryLock(ctx *sql.Context, name string, txn bool) (bool, error) {
	ls := getLockSubsystem()
	if ls == nil {
		return false, errors.Errorf("lock subsystem not available")
	}
	sessionID := uint32(ctx.Session.ID())

	sharedAdvisoryLocks.mu.Lock()
	defer sharedAdvisoryLocks.mu.Unlock()

	if sharedAdvisoryLocks.hasConflictingSharedHoldersLocked(sessionID, name) {
		return false, nil
	}
	err := ls.Lock(ctx, name, 0)
	if sql.ErrLockTimeout.Is(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if txn {
		recordXactLock(sessionID, name)
	}
	return true, nil
}

func acquireSharedAdvisoryLock(ctx *sql.Context, name string, timeout time.Duration) error {
	start := time.Now()
	for {
		ok, err := tryAcquireSharedAdvisoryLock(ctx, name)
		if ok || err != nil {
			return err
		}
		if timeout == 0 || (timeout > 0 && time.Since(start) >= timeout) {
			return sql.ErrLockTimeout.New(name)
		}
		time.Sleep(100 * time.Microsecond)
	}
}

func tryAcquireSharedAdvisoryLock(ctx *sql.Context, name string) (bool, error) {
	ls := getLockSubsystem()
	if ls == nil {
		return false, errors.Errorf("lock subsystem not available")
	}
	sessionID := uint32(ctx.Session.ID())

	sharedAdvisoryLocks.mu.Lock()
	defer sharedAdvisoryLocks.mu.Unlock()

	state, owner := ls.GetLockState(name)
	if state == sql.LockInUse && owner != sessionID {
		return false, nil
	}
	sharedAdvisoryLocks.addSharedHolderLocked(sessionID, name)
	return true, nil
}

func releaseSharedAdvisoryLock(ctx *sql.Context, name string) (bool, error) {
	sessionID := uint32(ctx.Session.ID())
	sharedAdvisoryLocks.mu.Lock()
	defer sharedAdvisoryLocks.mu.Unlock()
	return sharedAdvisoryLocks.releaseSharedHolderLocked(sessionID, name), nil
}

// ReleaseSessionSharedAdvisoryLocks releases all session-level shared advisory
// locks owned by sessionID. It is used for pg_advisory_unlock_all and best-effort
// cleanup when a wire-protocol session closes.
func ReleaseSessionSharedAdvisoryLocks(sessionID uint32) {
	sharedAdvisoryLocks.mu.Lock()
	defer sharedAdvisoryLocks.mu.Unlock()
	for name, holders := range sharedAdvisoryLocks.holders {
		delete(holders, sessionID)
		if len(holders) == 0 {
			delete(sharedAdvisoryLocks.holders, name)
		}
	}
}

func (r *sharedAdvisoryLockRegistry) hasConflictingSharedHoldersLocked(sessionID uint32, name string) bool {
	for holder, count := range r.holders[name] {
		if holder != sessionID && count > 0 {
			return true
		}
	}
	return false
}

func (r *sharedAdvisoryLockRegistry) addSharedHolderLocked(sessionID uint32, name string) {
	holders := r.holders[name]
	if holders == nil {
		holders = make(map[uint32]int)
		r.holders[name] = holders
	}
	holders[sessionID]++
}

func (r *sharedAdvisoryLockRegistry) releaseSharedHolderLocked(sessionID uint32, name string) bool {
	holders := r.holders[name]
	if holders == nil || holders[sessionID] == 0 {
		return false
	}
	if holders[sessionID] == 1 {
		delete(holders, sessionID)
	} else {
		holders[sessionID]--
	}
	if len(holders) == 0 {
		delete(r.holders, name)
	}
	return true
}
