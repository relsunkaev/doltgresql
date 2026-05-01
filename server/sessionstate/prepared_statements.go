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

import (
	"sort"
	"sync"
	"time"
)

// PreparedStatement describes a prepared statement visible in pg_prepared_statements.
type PreparedStatement struct {
	Name          string
	Statement     string
	PrepareTime   time.Time
	ParameterOIDs []uint32
	ResultOIDs    []uint32
	FromSQL       bool
	GenericPlans  int64
	CustomPlans   int64
}

var preparedStatements = struct {
	sync.RWMutex
	bySession map[uint32]map[string]PreparedStatement
}{
	bySession: make(map[uint32]map[string]PreparedStatement),
}

// UpsertPreparedStatement records a prepared statement for a session.
func UpsertPreparedStatement(sessionID uint32, statement PreparedStatement) {
	preparedStatements.Lock()
	defer preparedStatements.Unlock()

	if preparedStatements.bySession[sessionID] == nil {
		preparedStatements.bySession[sessionID] = make(map[string]PreparedStatement)
	}
	preparedStatements.bySession[sessionID][statement.Name] = statement
}

// DeletePreparedStatement removes one prepared statement from a session.
func DeletePreparedStatement(sessionID uint32, name string) {
	preparedStatements.Lock()
	defer preparedStatements.Unlock()

	delete(preparedStatements.bySession[sessionID], name)
	if len(preparedStatements.bySession[sessionID]) == 0 {
		delete(preparedStatements.bySession, sessionID)
	}
}

// DeleteAllPreparedStatements removes all prepared statements for a session.
func DeleteAllPreparedStatements(sessionID uint32) {
	preparedStatements.Lock()
	defer preparedStatements.Unlock()

	delete(preparedStatements.bySession, sessionID)
}

// IncrementPreparedStatementPlanCount updates the generic/custom plan counters for a prepared statement.
func IncrementPreparedStatementPlanCount(sessionID uint32, name string, generic bool) {
	preparedStatements.Lock()
	defer preparedStatements.Unlock()

	statement, ok := preparedStatements.bySession[sessionID][name]
	if !ok {
		return
	}
	if generic {
		statement.GenericPlans++
	} else {
		statement.CustomPlans++
	}
	preparedStatements.bySession[sessionID][name] = statement
}

// ListPreparedStatements returns a stable snapshot of the prepared statements for a session.
func ListPreparedStatements(sessionID uint32) []PreparedStatement {
	preparedStatements.RLock()
	defer preparedStatements.RUnlock()

	sessionStatements := preparedStatements.bySession[sessionID]
	if len(sessionStatements) == 0 {
		return nil
	}
	statements := make([]PreparedStatement, 0, len(sessionStatements))
	for _, statement := range sessionStatements {
		statement.ParameterOIDs = append([]uint32(nil), statement.ParameterOIDs...)
		statement.ResultOIDs = append([]uint32(nil), statement.ResultOIDs...)
		statements = append(statements, statement)
	}
	sort.Slice(statements, func(i, j int) bool {
		return statements[i].Name < statements[j].Name
	})
	return statements
}
