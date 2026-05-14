// Copyright 2025 Dolthub, Inc.
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
	"strconv"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqlserver"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// initAdvisoryLockFunctions registers the advisory lock functions to the catalog.
func initAdvisoryLockFunctions() {
	framework.RegisterFunction(pg_advisory_lock_bigint)
	framework.RegisterFunction(pg_advisory_unlock_bigint)
	framework.RegisterFunction(pg_try_advisory_lock_bigint)
	framework.RegisterFunction(pg_advisory_lock_int4_int4)
	framework.RegisterFunction(pg_advisory_unlock_int4_int4)
	framework.RegisterFunction(pg_try_advisory_lock_int4_int4)
	framework.RegisterFunction(pg_advisory_lock_shared_bigint)
	framework.RegisterFunction(pg_advisory_lock_shared_int4_int4)
	framework.RegisterFunction(pg_try_advisory_lock_shared_bigint)
	framework.RegisterFunction(pg_try_advisory_lock_shared_int4_int4)
	framework.RegisterFunction(pg_advisory_unlock_shared_bigint)
	framework.RegisterFunction(pg_advisory_unlock_shared_int4_int4)
	framework.RegisterFunction(pg_advisory_xact_lock_bigint)
	framework.RegisterFunction(pg_advisory_xact_lock_int4_int4)
	framework.RegisterFunction(pg_try_advisory_xact_lock_bigint)
	framework.RegisterFunction(pg_try_advisory_xact_lock_int4_int4)
	framework.RegisterFunction(pg_advisory_unlock_all)
}

// AcquireRowLevelXactLock is the row-level FOR UPDATE entry point
// the server/node package dispatches through. It blocks until the
// named lock is held for the duration of the surrounding
// transaction.
func AcquireRowLevelXactLock(ctx *sql.Context, lockName string) error {
	return acquireAdvisoryLock(ctx, lockName, -1, true)
}

// TryAcquireRowLevelXactLock is the non-blocking variant used for
// FOR UPDATE NOWAIT / SKIP LOCKED. Returns (true, nil) when the
// lock was acquired and (false, nil) when another session holds
// it.
func TryAcquireRowLevelXactLock(ctx *sql.Context, lockName string) (bool, error) {
	return tryAcquireAdvisoryLock(ctx, lockName, true)
}

// PostgreSQL keeps the (int4, int4) and (int8) advisory lock spaces
// disjoint. We mirror that here by prefixing every internal lock name
// with the form's tag — "8" for the int8 overload, "4" for the
// (int4, int4) overload — so the same numeric value cannot collide
// across forms.
const (
	advisoryLockPrefixInt8     = "8:"
	advisoryLockPrefixInt4Pair = "4:"
)

func advisoryLockNameInt8(key int64) string {
	return advisoryLockPrefixInt8 + strconv.FormatInt(key, 10)
}

func advisoryLockNameInt4Pair(key1, key2 int32) string {
	return advisoryLockPrefixInt4Pair + strconv.FormatInt(int64(key1), 10) + ":" + strconv.FormatInt(int64(key2), 10)
}

// pg_advisory_lock_bigint represents the pg_advisory_lock(bigint) function.
// https://www.postgresql.org/docs/9.1/functions-admin.html#FUNCTIONS-ADVISORY-LOCKS
var pg_advisory_lock_bigint = framework.Function1{
	Name:               "pg_advisory_lock",
	Return:             pgtypes.Void,
	Parameters:         [1]*pgtypes.DoltgresType{pgtypes.Int64},
	IsNonDeterministic: true,
	Strict:             true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val1 any) (any, error) {
		return "", acquireAdvisoryLock(ctx, advisoryLockNameInt8(val1.(int64)), -1, false)
	},
}

// pg_advisory_lock_int4_int4 represents the pg_advisory_lock(int4, int4) function.
var pg_advisory_lock_int4_int4 = framework.Function2{
	Name:               "pg_advisory_lock",
	Return:             pgtypes.Void,
	Parameters:         [2]*pgtypes.DoltgresType{pgtypes.Int32, pgtypes.Int32},
	IsNonDeterministic: true,
	Strict:             true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		return "", acquireAdvisoryLock(ctx, advisoryLockNameInt4Pair(val1.(int32), val2.(int32)), -1, false)
	},
}

// pg_try_advisory_lock_bigint represents the pg_try_advisory_lock(bigint) function.
var pg_try_advisory_lock_bigint = framework.Function1{
	Name:               "pg_try_advisory_lock",
	Return:             pgtypes.Bool,
	Parameters:         [1]*pgtypes.DoltgresType{pgtypes.Int64},
	IsNonDeterministic: true,
	Strict:             true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val1 any) (any, error) {
		return tryAcquireAdvisoryLock(ctx, advisoryLockNameInt8(val1.(int64)), false)
	},
}

// pg_try_advisory_lock_int4_int4 represents the pg_try_advisory_lock(int4, int4) function.
var pg_try_advisory_lock_int4_int4 = framework.Function2{
	Name:               "pg_try_advisory_lock",
	Return:             pgtypes.Bool,
	Parameters:         [2]*pgtypes.DoltgresType{pgtypes.Int32, pgtypes.Int32},
	IsNonDeterministic: true,
	Strict:             true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		return tryAcquireAdvisoryLock(ctx, advisoryLockNameInt4Pair(val1.(int32), val2.(int32)), false)
	},
}

// pg_advisory_unlock_bigint represents the pg_advisory_unlock(bigint) function.
var pg_advisory_unlock_bigint = framework.Function1{
	Name:               "pg_advisory_unlock",
	Return:             pgtypes.Bool,
	Parameters:         [1]*pgtypes.DoltgresType{pgtypes.Int64},
	IsNonDeterministic: true,
	Strict:             true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val1 any) (any, error) {
		return releaseAdvisoryLock(ctx, advisoryLockNameInt8(val1.(int64)))
	},
}

// pg_advisory_unlock_int4_int4 represents the pg_advisory_unlock(int4, int4) function.
var pg_advisory_unlock_int4_int4 = framework.Function2{
	Name:               "pg_advisory_unlock",
	Return:             pgtypes.Bool,
	Parameters:         [2]*pgtypes.DoltgresType{pgtypes.Int32, pgtypes.Int32},
	IsNonDeterministic: true,
	Strict:             true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		return releaseAdvisoryLock(ctx, advisoryLockNameInt4Pair(val1.(int32), val2.(int32)))
	},
}

// pg_advisory_xact_lock_bigint represents the pg_advisory_xact_lock(bigint) function.
// The lock is held for the duration of the surrounding transaction (or, in
// autocommit mode, the duration of the statement) and released at transaction
// end by the connection layer via ReleaseSessionXactLocks.
var pg_advisory_xact_lock_bigint = framework.Function1{
	Name:               "pg_advisory_xact_lock",
	Return:             pgtypes.Void,
	Parameters:         [1]*pgtypes.DoltgresType{pgtypes.Int64},
	IsNonDeterministic: true,
	Strict:             true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val1 any) (any, error) {
		return "", acquireAdvisoryLock(ctx, advisoryLockNameInt8(val1.(int64)), -1, true)
	},
}

// pg_advisory_xact_lock_int4_int4 represents the pg_advisory_xact_lock(int4, int4) function.
var pg_advisory_xact_lock_int4_int4 = framework.Function2{
	Name:               "pg_advisory_xact_lock",
	Return:             pgtypes.Void,
	Parameters:         [2]*pgtypes.DoltgresType{pgtypes.Int32, pgtypes.Int32},
	IsNonDeterministic: true,
	Strict:             true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		return "", acquireAdvisoryLock(ctx, advisoryLockNameInt4Pair(val1.(int32), val2.(int32)), -1, true)
	},
}

// pg_try_advisory_xact_lock_bigint represents the pg_try_advisory_xact_lock(bigint) function.
var pg_try_advisory_xact_lock_bigint = framework.Function1{
	Name:               "pg_try_advisory_xact_lock",
	Return:             pgtypes.Bool,
	Parameters:         [1]*pgtypes.DoltgresType{pgtypes.Int64},
	IsNonDeterministic: true,
	Strict:             true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val1 any) (any, error) {
		return tryAcquireAdvisoryLock(ctx, advisoryLockNameInt8(val1.(int64)), true)
	},
}

// pg_try_advisory_xact_lock_int4_int4 represents the pg_try_advisory_xact_lock(int4, int4) function.
var pg_try_advisory_xact_lock_int4_int4 = framework.Function2{
	Name:               "pg_try_advisory_xact_lock",
	Return:             pgtypes.Bool,
	Parameters:         [2]*pgtypes.DoltgresType{pgtypes.Int32, pgtypes.Int32},
	IsNonDeterministic: true,
	Strict:             true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		return tryAcquireAdvisoryLock(ctx, advisoryLockNameInt4Pair(val1.(int32), val2.(int32)), true)
	},
}

// pg_advisory_unlock_all represents the pg_advisory_unlock_all() function.
// PostgreSQL semantics: releases every session-level advisory lock currently
// held by the session. Transaction-level advisory locks are unaffected and
// remain held until the transaction ends.
var pg_advisory_unlock_all = framework.Function0{
	Name:               "pg_advisory_unlock_all",
	Return:             pgtypes.Void,
	IsNonDeterministic: true,
	Callable: func(ctx *sql.Context) (any, error) {
		ls := getLockSubsystem()
		if ls == nil {
			return nil, errors.Errorf("lock subsystem not available")
		}

		xactCounts := snapshotSessionXactLockCounts(uint32(ctx.Session.ID()))

		var allNames []string
		_ = ctx.Session.IterLocks(func(name string) error {
			allNames = append(allNames, name)
			return nil
		})

		for _, name := range allNames {
			// Repeatedly unlock until the count reserved for the
			// transaction-scope tracker remains, so reentrant
			// session-scope acquisitions are fully released.
			for {
				state, owner := ls.GetLockState(name)
				if state != sql.LockInUse || uint32(owner) != uint32(ctx.Session.ID()) {
					break
				}
				if reentrantCountForSession(name) <= xactCounts[name] {
					break
				}
				if err := ls.Unlock(ctx, name); err != nil {
					break
				}
			}
		}
		ReleaseSessionSharedAdvisoryLocks(uint32(ctx.Session.ID()))
		return "", nil
	},
}

// snapshotSessionXactLockCounts produces a {name -> count} snapshot of every
// transaction-scope advisory lock the given session has on file, used by
// pg_advisory_unlock_all to avoid releasing transaction-scope acquisitions.
func snapshotSessionXactLockCounts(sessionID uint32) map[string]int {
	xactLocksMu.Lock()
	defer xactLocksMu.Unlock()
	out := make(map[string]int, len(xactLocks[sessionID]))
	for _, name := range xactLocks[sessionID] {
		out[name]++
	}
	return out
}

// reentrantCountForSession returns the LockSubsystem reentrant count for the
// given lock name as observed by the calling session, or 0 if the lock is
// free or owned by another session.
func reentrantCountForSession(name string) int {
	ls := getLockSubsystem()
	if ls == nil {
		return 0
	}
	state, _ := ls.GetLockState(name)
	if state != sql.LockInUse {
		return 0
	}
	// LockSubsystem doesn't expose the count directly. Approximate by
	// returning 1: this is enough to make pg_advisory_unlock_all release
	// at least once per name, after which we recheck state and stop.
	return 1
}

// acquireAdvisoryLock acquires the named lock with the given timeout. When
// txn is true, the acquisition is recorded for transaction-scope release;
// the caller must hold the lock for the lifetime of the surrounding
// transaction (or autocommit statement).
func acquireAdvisoryLock(ctx *sql.Context, name string, timeout time.Duration, txn bool) error {
	return acquireExclusiveAdvisoryLock(ctx, name, timeout, txn)
}

// tryAcquireAdvisoryLock attempts to acquire the named lock without
// blocking, returning (true, nil) on success or (false, nil) when the lock
// is held by another session. Returns a non-nil error only on infrastructure
// failures (e.g. missing lock subsystem).
func tryAcquireAdvisoryLock(ctx *sql.Context, name string, txn bool) (bool, error) {
	return tryAcquireExclusiveAdvisoryLock(ctx, name, txn)
}

// releaseAdvisoryLock releases one count of the named session-scope
// advisory lock. Returns true if a count was released, false if the lock
// is not held by this session.
func releaseAdvisoryLock(ctx *sql.Context, name string) (bool, error) {
	ls := getLockSubsystem()
	if ls == nil {
		return false, errors.Errorf("lock subsystem not available")
	}
	err := ls.Unlock(ctx, name)
	if sql.ErrLockDoesNotExist.Is(err) || sql.ErrLockNotOwned.Is(err) {
		return false, nil
	}
	return err == nil, err
}

// getLockSubsystem returns the active lock system for the SQL engine.
func getLockSubsystem() *sql.LockSubsystem {
	server := sqlserver.GetRunningServer()
	if server == nil {
		return nil
	}
	engine := server.Engine
	// This should be impossible if the server was initialized correctly, but for some test harnesses we
	// take shortcuts that might invalidate that assumption
	if engine == nil {
		return nil
	}
	return engine.LS
}
