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

package sessionstate

import "sync"

// CommitAction is run after the wire-protocol transaction commits. Returning
// false unregisters the action for future commits.
type CommitAction func() (bool, error)
type RollbackAction func() error

var transactionCommitActions = struct {
	sync.Mutex
	actions map[uint32]map[string]CommitAction
}{
	actions: make(map[uint32]map[string]CommitAction),
}

var transactionRollbackActions = struct {
	sync.Mutex
	actions map[uint32]map[string]RollbackAction
}{
	actions: make(map[uint32]map[string]RollbackAction),
}

// RegisterCommitAction registers or replaces a commit action for a connection.
func RegisterCommitAction(connectionID uint32, key string, action CommitAction) {
	transactionCommitActions.Lock()
	defer transactionCommitActions.Unlock()
	if transactionCommitActions.actions[connectionID] == nil {
		transactionCommitActions.actions[connectionID] = make(map[string]CommitAction)
	}
	transactionCommitActions.actions[connectionID][key] = action
}

// RegisterRollbackAction registers or replaces a rollback action for a connection.
func RegisterRollbackAction(connectionID uint32, key string, action RollbackAction) {
	transactionRollbackActions.Lock()
	defer transactionRollbackActions.Unlock()
	if transactionRollbackActions.actions[connectionID] == nil {
		transactionRollbackActions.actions[connectionID] = make(map[string]RollbackAction)
	}
	transactionRollbackActions.actions[connectionID][key] = action
}

// RunCommitActions runs all registered commit actions for a connection.
func RunCommitActions(connectionID uint32) error {
	transactionCommitActions.Lock()
	actionsByKey := transactionCommitActions.actions[connectionID]
	actions := make([]commitActionEntry, 0, len(actionsByKey))
	for key, action := range actionsByKey {
		actions = append(actions, commitActionEntry{key: key, action: action})
	}
	transactionCommitActions.Unlock()

	if len(actions) == 0 {
		return nil
	}

	removeKeys := make([]string, 0, len(actions))
	for _, entry := range actions {
		keep, err := entry.action()
		if err != nil {
			return err
		}
		if !keep {
			removeKeys = append(removeKeys, entry.key)
		}
	}
	if len(removeKeys) == 0 {
		return nil
	}

	transactionCommitActions.Lock()
	defer transactionCommitActions.Unlock()
	actionsByKey = transactionCommitActions.actions[connectionID]
	for _, key := range removeKeys {
		delete(actionsByKey, key)
	}
	if len(actionsByKey) == 0 {
		delete(transactionCommitActions.actions, connectionID)
	}
	return nil
}

// ClearRollbackActions clears all registered rollback actions for a connection.
func ClearRollbackActions(connectionID uint32) {
	transactionRollbackActions.Lock()
	defer transactionRollbackActions.Unlock()
	delete(transactionRollbackActions.actions, connectionID)
}

// RunRollbackActions runs and clears all registered rollback actions for a connection.
func RunRollbackActions(connectionID uint32) error {
	transactionRollbackActions.Lock()
	actionsByKey := transactionRollbackActions.actions[connectionID]
	actions := make([]rollbackActionEntry, 0, len(actionsByKey))
	for key, action := range actionsByKey {
		actions = append(actions, rollbackActionEntry{key: key, action: action})
	}
	delete(transactionRollbackActions.actions, connectionID)
	transactionRollbackActions.Unlock()

	for _, entry := range actions {
		if err := entry.action(); err != nil {
			return err
		}
	}
	return nil
}

type commitActionEntry struct {
	key    string
	action CommitAction
}

type rollbackActionEntry struct {
	key    string
	action RollbackAction
}
