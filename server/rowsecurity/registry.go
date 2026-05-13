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
	UsingAll    bool
	CheckColumn string
	CheckAll    bool
	Roles       []string
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

type transactionSnapshot struct {
	captured bool
	tables   map[tableKey]State
}

var registry = struct {
	sync.RWMutex
	tables       map[tableKey]State
	transactions map[uint32]transactionSnapshot
}{
	tables:       map[tableKey]State{},
	transactions: map[uint32]transactionSnapshot{},
}

// ResetForTests clears all in-memory row-level security state.
func ResetForTests() {
	registry.Lock()
	defer registry.Unlock()
	registry.tables = map[tableKey]State{}
	registry.transactions = map[uint32]transactionSnapshot{}
}

// BeginTransaction starts tracking row-level security metadata mutations for a connection.
func BeginTransaction(connectionID uint32) {
	registry.Lock()
	defer registry.Unlock()
	registry.transactions[connectionID] = transactionSnapshot{}
}

// CommitTransaction persists any row-level security metadata mutations for a connection.
func CommitTransaction(connectionID uint32) {
	registry.Lock()
	defer registry.Unlock()
	delete(registry.transactions, connectionID)
}

// RollbackTransaction restores row-level security metadata to its transaction-start state.
func RollbackTransaction(connectionID uint32) {
	registry.Lock()
	defer registry.Unlock()
	snapshot, ok := registry.transactions[connectionID]
	if !ok {
		return
	}
	if snapshot.captured {
		registry.tables = cloneStateMap(snapshot.tables)
	}
	delete(registry.transactions, connectionID)
}

// SetTableMode updates the row-level security mode for a table.
func SetTableMode(connectionID uint32, database, schema, table string, enabled *bool, forced *bool) {
	registry.Lock()
	defer registry.Unlock()
	trackMutationLocked(connectionID)
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

// AddPolicy adds a row-level security policy. It returns false when a policy
// with the same name already exists for the table.
func AddPolicy(connectionID uint32, database, schema, table string, policy Policy) bool {
	registry.Lock()
	defer registry.Unlock()
	key := makeKey(database, schema, table)
	state := registry.tables[key]
	policy.Name = normalizeIdentifier(policy.Name)
	policy.Command = strings.ToLower(strings.TrimSpace(policy.Command))
	for i := range policy.Roles {
		policy.Roles[i] = normalizeIdentifier(policy.Roles[i])
	}
	for _, existing := range state.Policies {
		if existing.Name == policy.Name {
			return false
		}
	}
	trackMutationLocked(connectionID)
	state.Policies = append(state.Policies, policy)
	registry.tables[key] = state
	return true
}

// DropPolicy removes a row-level security policy. It returns false when the
// policy does not exist for the table.
func DropPolicy(connectionID uint32, database, schema, table string, policyName string) bool {
	registry.Lock()
	defer registry.Unlock()
	key := makeKey(database, schema, table)
	state, ok := registry.tables[key]
	if !ok {
		return false
	}
	policyName = normalizeIdentifier(policyName)
	for i, existing := range state.Policies {
		if existing.Name != policyName {
			continue
		}
		trackMutationLocked(connectionID)
		state.Policies = append(state.Policies[:i], state.Policies[i+1:]...)
		registry.tables[key] = state
		return true
	}
	return false
}

// DropTable removes all row-level security state for a dropped table.
func DropTable(connectionID uint32, database, schema, table string) {
	registry.Lock()
	defer registry.Unlock()
	key := makeKey(database, schema, table)
	if _, ok := registry.tables[key]; !ok {
		return
	}
	trackMutationLocked(connectionID)
	delete(registry.tables, key)
}

// RenameTable moves row-level security state to a renamed table.
func RenameTable(connectionID uint32, database, oldSchema, oldTable, newSchema, newTable string) {
	registry.Lock()
	defer registry.Unlock()
	oldKey := makeKey(database, oldSchema, oldTable)
	state, ok := registry.tables[oldKey]
	if !ok {
		return
	}
	trackMutationLocked(connectionID)
	delete(registry.tables, oldKey)
	registry.tables[makeKey(database, newSchema, newTable)] = state
}

// RenameColumn rewrites policy column references for a renamed table column.
func RenameColumn(connectionID uint32, database, schema, table, oldColumn, newColumn string) {
	registry.Lock()
	defer registry.Unlock()
	key := makeKey(database, schema, table)
	state, ok := registry.tables[key]
	if !ok {
		return
	}
	trackMutationLocked(connectionID)
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
	return cloneState(state), true
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

// PoliciesForCommand returns every permissive policy that applies to the given
// command and role. PostgreSQL ORs permissive policies together.
func (s State) PoliciesForCommand(command string, user string) []Policy {
	command = strings.ToLower(command)
	user = normalizeIdentifier(user)
	var policies []Policy
	for _, policy := range s.Policies {
		if policy.Command != command && policy.Command != "all" {
			continue
		}
		if !policyAppliesToRole(policy, user) {
			continue
		}
		policies = append(policies, policy)
	}
	return policies
}

func policyAppliesToRole(policy Policy, user string) bool {
	if len(policy.Roles) == 0 {
		return true
	}
	for _, role := range policy.Roles {
		role = normalizeIdentifier(role)
		if role == "public" || role == user {
			return true
		}
	}
	return false
}

func trackMutationLocked(connectionID uint32) {
	snapshot, ok := registry.transactions[connectionID]
	if !ok || snapshot.captured {
		return
	}
	snapshot.captured = true
	snapshot.tables = cloneStateMap(registry.tables)
	registry.transactions[connectionID] = snapshot
}

func cloneStateMap(tables map[tableKey]State) map[tableKey]State {
	clone := make(map[tableKey]State, len(tables))
	for key, state := range tables {
		clone[key] = cloneState(state)
	}
	return clone
}

func cloneState(state State) State {
	state.Policies = clonePolicies(state.Policies)
	return state
}

func clonePolicies(policies []Policy) []Policy {
	if len(policies) == 0 {
		return nil
	}
	clone := make([]Policy, len(policies))
	for i, policy := range policies {
		clone[i] = policy
		clone[i].Roles = append([]string(nil), policy.Roles...)
	}
	return clone
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
