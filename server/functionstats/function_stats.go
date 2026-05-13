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

package functionstats

import (
	"sort"
	"sync"
	"time"

	"github.com/dolthub/doltgresql/core/id"
)

// FunctionStat describes the function-call statistics visible in pg_stat_user_functions.
type FunctionStat struct {
	FuncID      id.Function
	Calls       int64
	TotalTimeMS float64
	SelfTimeMS  float64
}

var registry = struct {
	sync.RWMutex
	bySession map[uint32]map[id.Function]FunctionStat
}{
	bySession: make(map[uint32]map[id.Function]FunctionStat),
}

// Record records a completed user-defined function call for the session.
func Record(sessionID uint32, functionID id.Function, elapsed time.Duration) {
	if !functionID.AsId().IsValid() {
		return
	}

	registry.Lock()
	defer registry.Unlock()

	sessionStats := registry.bySession[sessionID]
	if sessionStats == nil {
		sessionStats = make(map[id.Function]FunctionStat)
		registry.bySession[sessionID] = sessionStats
	}
	stat := sessionStats[functionID]
	stat.FuncID = functionID
	stat.Calls++
	elapsedMS := float64(elapsed) / float64(time.Millisecond)
	stat.TotalTimeMS += elapsedMS
	stat.SelfTimeMS += elapsedMS
	sessionStats[functionID] = stat
}

// List returns a stable snapshot of function-call stats for a session.
func List(sessionID uint32) []FunctionStat {
	registry.RLock()
	defer registry.RUnlock()

	sessionStats := registry.bySession[sessionID]
	if len(sessionStats) == 0 {
		return nil
	}
	stats := make([]FunctionStat, 0, len(sessionStats))
	for _, stat := range sessionStats {
		stats = append(stats, stat)
	}
	sort.Slice(stats, func(i, j int) bool {
		return string(stats[i].FuncID) < string(stats[j].FuncID)
	})
	return stats
}

// DeleteAll removes all function-call stats for a session.
func DeleteAll(sessionID uint32) {
	registry.Lock()
	defer registry.Unlock()

	delete(registry.bySession, sessionID)
}
