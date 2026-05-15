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

type xactVarSavepoint struct {
	name      string
	snapshots map[string]xactVarSnapshot
}

var xactVarSavepoints = map[uint32][]xactVarSavepoint{}

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

	if err := snapshotSavepointVarsBeforeLocalSet(ctx, id, name, isUserVar); err != nil {
		return err
	}

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

// PushSessionXactVarSavepoint snapshots all currently transaction-local
// variables for a new SAVEPOINT. Variables first written after the
// savepoint are captured lazily by SnapshotSessionVarBeforeLocalSet.
func PushSessionXactVarSavepoint(ctx *sql.Context, name string) error {
	if ctx == nil || ctx.Session == nil {
		return nil
	}
	id := uint32(ctx.Session.ID())

	xactVarsMu.Lock()
	varNames := make(map[string]bool, len(xactVars[id]))
	for varName, snap := range xactVars[id] {
		varNames[varName] = snap.isUser
	}
	xactVarsMu.Unlock()

	snapshots := make(map[string]xactVarSnapshot, len(varNames))
	for varName, isUser := range varNames {
		snap, err := currentXactVarSnapshot(ctx, varName, isUser)
		if err != nil {
			return err
		}
		snapshots[varName] = snap
	}

	xactVarsMu.Lock()
	xactVarSavepoints[id] = append(xactVarSavepoints[id], xactVarSavepoint{
		name:      name,
		snapshots: snapshots,
	})
	xactVarsMu.Unlock()
	return nil
}

// RollbackSessionXactVarsToSavepoint restores transaction-local
// variables to the state they had when the named SAVEPOINT was made.
// The target savepoint remains active; later savepoints are discarded,
// matching PostgreSQL's ROLLBACK TO SAVEPOINT semantics.
func RollbackSessionXactVarsToSavepoint(ctx *sql.Context, name string) error {
	if ctx == nil || ctx.Session == nil {
		return nil
	}
	id := uint32(ctx.Session.ID())

	xactVarsMu.Lock()
	stack := xactVarSavepoints[id]
	idx := findXactVarSavepoint(stack, name)
	if idx < 0 {
		xactVarsMu.Unlock()
		return nil
	}
	frame := stack[idx]
	xactVarSavepoints[id] = stack[:idx+1]
	snapshots := make(map[string]xactVarSnapshot, len(frame.snapshots))
	for varName, snap := range frame.snapshots {
		snapshots[varName] = snap
	}
	xactVarsMu.Unlock()

	var firstErr error
	for varName, snap := range snapshots {
		if err := restoreXactVar(ctx, varName, snap); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// ReleaseSessionXactVarSavepoint releases the named savepoint and all
// savepoints created after it. The variable values themselves remain as
// they are, matching PostgreSQL RELEASE SAVEPOINT semantics.
func ReleaseSessionXactVarSavepoint(ctx *sql.Context, name string) {
	if ctx == nil || ctx.Session == nil {
		return
	}
	id := uint32(ctx.Session.ID())

	xactVarsMu.Lock()
	defer xactVarsMu.Unlock()
	stack := xactVarSavepoints[id]
	idx := findXactVarSavepoint(stack, name)
	if idx < 0 {
		return
	}
	if idx == 0 {
		delete(xactVarSavepoints, id)
		return
	}
	xactVarSavepoints[id] = stack[:idx]
}

func snapshotSavepointVarsBeforeLocalSet(ctx *sql.Context, sessionID uint32, name string, isUserVar bool) error {
	xactVarsMu.Lock()
	needsSnapshot := false
	for _, frame := range xactVarSavepoints[sessionID] {
		if _, ok := frame.snapshots[name]; !ok {
			needsSnapshot = true
			break
		}
	}
	xactVarsMu.Unlock()
	if !needsSnapshot {
		return nil
	}

	snap, err := currentXactVarSnapshot(ctx, name, isUserVar)
	if err != nil {
		return err
	}

	xactVarsMu.Lock()
	stack := xactVarSavepoints[sessionID]
	for i := range stack {
		if stack[i].snapshots == nil {
			stack[i].snapshots = map[string]xactVarSnapshot{}
		}
		if _, ok := stack[i].snapshots[name]; !ok {
			stack[i].snapshots[name] = snap
		}
	}
	xactVarSavepoints[sessionID] = stack
	xactVarsMu.Unlock()
	return nil
}

func currentXactVarSnapshot(ctx *sql.Context, name string, isUserVar bool) (xactVarSnapshot, error) {
	snap := xactVarSnapshot{isUser: isUserVar}
	if isUserVar {
		_, val, err := ctx.GetUserVariable(ctx, name)
		if err != nil {
			return snap, err
		}
		snap.existed = val != nil
		snap.value = val
		return snap, nil
	}
	val, err := ctx.GetSessionVariable(ctx, name)
	if err != nil {
		snap.existed = false
		return snap, nil
	}
	snap.existed = true
	snap.value = val
	return snap, nil
}

func findXactVarSavepoint(stack []xactVarSavepoint, name string) int {
	for i := len(stack) - 1; i >= 0; i-- {
		if stack[i].name == name {
			return i
		}
	}
	return -1
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
	delete(xactVarSavepoints, id)
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
		// User variables are PostgreSQL-style namespaced GUCs (e.g.
		// "app.user_id"). PostgreSQL keeps a custom GUC placeholder after a
		// transaction-local assignment has been reverted, so an absent prior
		// value restores to the empty string rather than disappearing.
		val := any("")
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
	return len(xactVars[sessionID]) > 0 || len(xactVarSavepoints[sessionID]) > 0
}
