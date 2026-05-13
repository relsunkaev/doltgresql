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

	"github.com/dolthub/doltgresql/core/id"
)

type sessionSequenceValue struct {
	sequence id.Sequence
	value    int64
}

var sequenceSessionState = struct {
	sync.Mutex
	bySequence map[uint32]map[id.Sequence]int64
	last       map[uint32]sessionSequenceValue
}{
	bySequence: make(map[uint32]map[id.Sequence]int64),
	last:       make(map[uint32]sessionSequenceValue),
}

func recordSessionSequenceValue(ctx *sql.Context, sequence id.Sequence, value int64) {
	if ctx == nil || ctx.Session == nil {
		return
	}
	sessionID := ctx.Session.ID()

	sequenceSessionState.Lock()
	defer sequenceSessionState.Unlock()
	if sequenceSessionState.bySequence[sessionID] == nil {
		sequenceSessionState.bySequence[sessionID] = make(map[id.Sequence]int64)
	}
	sequenceSessionState.bySequence[sessionID][sequence] = value
	sequenceSessionState.last[sessionID] = sessionSequenceValue{
		sequence: sequence,
		value:    value,
	}
}

func sessionSequenceCurrentValue(ctx *sql.Context, sequence id.Sequence) (int64, bool) {
	if ctx == nil || ctx.Session == nil {
		return 0, false
	}
	sequenceSessionState.Lock()
	defer sequenceSessionState.Unlock()
	value, ok := sequenceSessionState.bySequence[ctx.Session.ID()][sequence]
	return value, ok
}

func sessionSequenceLastValue(ctx *sql.Context) (sessionSequenceValue, bool) {
	if ctx == nil || ctx.Session == nil {
		return sessionSequenceValue{}, false
	}
	sequenceSessionState.Lock()
	defer sequenceSessionState.Unlock()
	value, ok := sequenceSessionState.last[ctx.Session.ID()]
	return value, ok
}
