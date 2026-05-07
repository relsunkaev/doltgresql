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

	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// PostgreSQL's SET LOCAL and set_config(..., is_local=true) take effect
// only for the duration of the surrounding transaction (or, in
// autocommit mode, the surrounding statement). At transaction end the
// previous value must be restored.
//
// We implement this by snapshotting the original value of every
// transaction-local variable the session writes, replaying those
// snapshots in reverse at transaction end. Snapshots are keyed by
// session id and variable name; the first SET LOCAL of a given variable
// inside a transaction wins, so that multiple SET LOCALs within the
// same transaction restore back to the pre-transaction value rather
// than to an intermediate one.

type xactVarSnapshot struct {
	// isUser is true for namespaced PostgreSQL custom GUCs (e.g.
	// "app.user_id"), which Doltgres exposes through user variables.
	// false for session variables.
	isUser bool

	// existed records whether the variable held a value before the
	// current transaction started. When false, restoring sets the
	// variable to its zero/empty value (Doltgres has no native "unset"
	// for user variables, so this is the closest reasonable behavior).
	existed bool

	value any
}

var xactVarsMu sync.Mutex
var xactVars = map[uint32]map[string]xactVarSnapshot{}

// SnapshotSessionVarBeforeLocalSet records the current value of the
// named variable so it can be restored at end of transaction. Only the
// first call for a given (session, name) pair takes effect; subsequent
// calls are no-ops, matching PostgreSQL's behavior of restoring all
// SET LOCALs to the pre-transaction value rather than to intermediate
// SET LOCAL values.
func SnapshotSessionVarBeforeLocalSet(ctx *sql.Context, name string, isUserVar bool) error {
	if ctx == nil || ctx.Session == nil {
		return nil
	}
	id := uint32(ctx.Session.ID())

	xactVarsMu.Lock()
	if _, ok := xactVars[id][name]; ok {
		xactVarsMu.Unlock()
		return nil
	}
	xactVarsMu.Unlock()

	snap := xactVarSnapshot{isUser: isUserVar}
	if isUserVar {
		_, val, err := ctx.GetUserVariable(ctx, name)
		if err != nil {
			return err
		}
		snap.existed = val != nil
		snap.value = val
	} else {
		val, err := ctx.GetSessionVariable(ctx, name)
		if err != nil {
			// Variable didn't exist; restore will set it to empty
			// when the transaction ends.
			snap.existed = false
		} else {
			snap.existed = true
			snap.value = val
		}
	}

	xactVarsMu.Lock()
	if xactVars[id] == nil {
		xactVars[id] = map[string]xactVarSnapshot{}
	}
	if _, exists := xactVars[id][name]; !exists {
		xactVars[id][name] = snap
	}
	xactVarsMu.Unlock()
	return nil
}

// ReleaseSessionXactVars restores every variable snapshotted by SET
// LOCAL / set_config(..., true) within the current transaction back to
// its pre-transaction value, then clears the snapshot table for this
// session. Called by the connection layer at transaction end.
func ReleaseSessionXactVars(ctx *sql.Context) error {
	if ctx == nil || ctx.Session == nil {
		return nil
	}
	id := uint32(ctx.Session.ID())

	xactVarsMu.Lock()
	snapshots := xactVars[id]
	delete(xactVars, id)
	xactVarsMu.Unlock()

	if len(snapshots) == 0 {
		return nil
	}

	var firstErr error
	for name, snap := range snapshots {
		if err := restoreXactVar(ctx, name, snap); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func restoreXactVar(ctx *sql.Context, name string, snap xactVarSnapshot) error {
	if snap.isUser {
		// User variables are PostgreSQL-style namespaced GUCs
		// (e.g. "app.user_id"). Doltgres has no public "unset"
		// API for them, but storing the nil value makes
		// current_setting()'s lookup fall through to the session-
		// variable layer and return NULL with missing_ok=true,
		// which matches what an unset GUC returns in PostgreSQL.
		var val any
		if snap.existed {
			val = snap.value
		}
		return ctx.SetUserVariable(ctx, name, val, pgtypes.Text)
	}
	if !snap.existed {
		// Session variables in Doltgres always have a default; we
		// restore an unset variable by clearing it to empty, which
		// matches PostgreSQL's reset-on-rollback semantics for
		// PostgreSQL-defined GUCs.
		return ctx.SetSessionVariable(ctx, name, "")
	}
	return ctx.SetSessionVariable(ctx, name, snap.value)
}

// HasSessionXactVars reports whether the session has any outstanding
// transaction-local variable snapshots.
func HasSessionXactVars(sessionID uint32) bool {
	xactVarsMu.Lock()
	defer xactVarsMu.Unlock()
	return len(xactVars[sessionID]) > 0
}
