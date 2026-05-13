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

package rowsecurity

import (
	"strings"
	"sync"
)

const defaultSchemaName = "public"
const defaultDatabaseName = "postgres"

// Policy is the supported subset of a PostgreSQL row-level security policy.
type Policy struct {
	Name        string
	Command     string
	UsingColumn string
	CheckColumn string
}

// State is row-level security metadata for one table.
type State struct {
	Enabled  bool
	Forced   bool
	Policies []Policy
}

type tableKey struct {
	database string
	schema   string
	table    string
}

var registry = struct {
	sync.RWMutex
	tables map[tableKey]State
}{
	tables: map[tableKey]State{},
}

// ResetForTests clears all in-memory row-level security state.
func ResetForTests() {
	registry.Lock()
	defer registry.Unlock()
	registry.tables = map[tableKey]State{}
}

// SetTableMode updates the row-level security mode for a table.
func SetTableMode(database, schema, table string, enabled *bool, forced *bool) {
	registry.Lock()
	defer registry.Unlock()
	key := makeKey(database, schema, table)
	state := registry.tables[key]
	if enabled != nil {
		state.Enabled = *enabled
	}
	if forced != nil {
		state.Forced = *forced
	}
	registry.tables[key] = state
}

// AddPolicy adds or replaces a row-level security policy.
func AddPolicy(database, schema, table string, policy Policy) {
	registry.Lock()
	defer registry.Unlock()
	key := makeKey(database, schema, table)
	state := registry.tables[key]
	policy.Name = normalizeIdentifier(policy.Name)
	policy.Command = strings.ToLower(strings.TrimSpace(policy.Command))
	for i, existing := range state.Policies {
		if existing.Name == policy.Name {
			state.Policies[i] = policy
			registry.tables[key] = state
			return
		}
	}
	state.Policies = append(state.Policies, policy)
	registry.tables[key] = state
}

// RenameTable moves row-level security state to a renamed table.
func RenameTable(database, oldSchema, oldTable, newSchema, newTable string) {
	registry.Lock()
	defer registry.Unlock()
	oldKey := makeKey(database, oldSchema, oldTable)
	state, ok := registry.tables[oldKey]
	if !ok {
		return
	}
	delete(registry.tables, oldKey)
	registry.tables[makeKey(database, newSchema, newTable)] = state
}

// RenameColumn rewrites policy column references for a renamed table column.
func RenameColumn(database, schema, table, oldColumn, newColumn string) {
	registry.Lock()
	defer registry.Unlock()
	key := makeKey(database, schema, table)
	state, ok := registry.tables[key]
	if !ok {
		return
	}
	oldColumn = normalizeIdentifier(oldColumn)
	newColumn = normalizeIdentifier(newColumn)
	for i := range state.Policies {
		if state.Policies[i].UsingColumn == oldColumn {
			state.Policies[i].UsingColumn = newColumn
		}
		if state.Policies[i].CheckColumn == oldColumn {
			state.Policies[i].CheckColumn = newColumn
		}
	}
	registry.tables[key] = state
}

// Get returns the row-level security state for a table.
func Get(database, schema, table string) (State, bool) {
	registry.RLock()
	defer registry.RUnlock()
	state, ok := registry.tables[makeKey(database, schema, table)]
	if !ok {
		return State{}, false
	}
	state.Policies = append([]Policy(nil), state.Policies...)
	return state, true
}

// PolicyForCommand returns the first policy that applies to command.
func (s State) PolicyForCommand(command string) (Policy, bool) {
	command = strings.ToLower(command)
	for _, policy := range s.Policies {
		if policy.Command == command || policy.Command == "all" {
			return policy, true
		}
	}
	return Policy{}, false
}

// NormalizeName normalizes an SQL identifier for registry lookup.
func NormalizeName(name string) string {
	return normalizeIdentifier(name)
}

func makeKey(database, schema, table string) tableKey {
	if database == "" {
		database = defaultDatabaseName
	}
	if schema == "" {
		schema = defaultSchemaName
	}
	return tableKey{
		database: normalizeIdentifier(database),
		schema:   normalizeIdentifier(schema),
		table:    normalizeIdentifier(table),
	}
}

func normalizeIdentifier(identifier string) string {
	identifier = strings.TrimSpace(identifier)
	if len(identifier) >= 2 && identifier[0] == '"' && identifier[len(identifier)-1] == '"' {
		return strings.ReplaceAll(identifier[1:len(identifier)-1], `""`, `"`)
	}
	return strings.ToLower(identifier)
}
