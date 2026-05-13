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

package sequences

import (
	"strings"
	"sync"

	"github.com/dolthub/doltgresql/core/id"
)

type runtimeStateKey struct {
	database string
	sequence id.Sequence
}

type runtimeState struct {
	current  int64
	isAtEnd  bool
	isCalled bool
}

type structuralState struct {
	dataTypeID  id.Type
	persistence Persistence
	start       int64
	increment   int64
	minimum     int64
	maximum     int64
	cache       int64
	cycle       bool
	owner       string
	ownerTable  id.Table
	ownerColumn string
}

type runtimeStateEntry struct {
	state     runtimeState
	structure structuralState
}

var sequenceRuntimeState = struct {
	sync.Mutex
	values map[runtimeStateKey]runtimeStateEntry
}{
	values: make(map[runtimeStateKey]runtimeStateEntry),
}

func newRuntimeStateKey(database string, sequence id.Sequence) runtimeStateKey {
	return runtimeStateKey{
		database: strings.ToLower(database),
		sequence: sequence,
	}
}

func snapshotRuntimeState(seq *Sequence) runtimeState {
	return runtimeState{
		current:  seq.Current,
		isAtEnd:  seq.IsAtEnd,
		isCalled: seq.IsCalled,
	}
}

func snapshotStructuralState(seq *Sequence) structuralState {
	return structuralState{
		dataTypeID:  seq.DataTypeID,
		persistence: seq.Persistence,
		start:       seq.Start,
		increment:   seq.Increment,
		minimum:     seq.Minimum,
		maximum:     seq.Maximum,
		cache:       seq.Cache,
		cycle:       seq.Cycle,
		owner:       seq.Owner,
		ownerTable:  seq.OwnerTable,
		ownerColumn: seq.OwnerColumn,
	}
}

func applyRuntimeState(seq *Sequence, state runtimeState) {
	seq.Current = state.current
	seq.IsAtEnd = state.isAtEnd
	seq.IsCalled = state.isCalled
}

func applySharedRuntimeState(database string, seq *Sequence) {
	if seq == nil {
		return
	}
	key := newRuntimeStateKey(database, seq.Id)
	structure := snapshotStructuralState(seq)

	sequenceRuntimeState.Lock()
	entry, ok := sequenceRuntimeState.values[key]
	sequenceRuntimeState.Unlock()
	if !ok || entry.structure != structure {
		return
	}
	applyRuntimeState(seq, entry.state)
}

func rememberSharedRuntimeState(database string, seq *Sequence) {
	if seq == nil {
		return
	}
	key := newRuntimeStateKey(database, seq.Id)
	entry := runtimeStateEntry{
		state:     snapshotRuntimeState(seq),
		structure: snapshotStructuralState(seq),
	}

	sequenceRuntimeState.Lock()
	sequenceRuntimeState.values[key] = entry
	sequenceRuntimeState.Unlock()
}

func clearSharedRuntimeState(database string, sequence id.Sequence) {
	key := newRuntimeStateKey(database, sequence)
	sequenceRuntimeState.Lock()
	delete(sequenceRuntimeState.values, key)
	sequenceRuntimeState.Unlock()
}

// ClearSharedRuntimeStateForDatabase clears all process-local runtime sequence state for a database scope.
func ClearSharedRuntimeStateForDatabase(database string) {
	scope := strings.ToLower(database)
	baseScope, _, _ := strings.Cut(scope, "/")
	sequenceRuntimeState.Lock()
	for key := range sequenceRuntimeState.values {
		if key.database == scope || key.database == baseScope || strings.HasPrefix(key.database, baseScope+"/") {
			delete(sequenceRuntimeState.values, key)
		}
	}
	sequenceRuntimeState.Unlock()
}

// ClearAllSharedRuntimeState clears all process-local runtime sequence state.
func ClearAllSharedRuntimeState() {
	sequenceRuntimeState.Lock()
	clear(sequenceRuntimeState.values)
	sequenceRuntimeState.Unlock()
}
