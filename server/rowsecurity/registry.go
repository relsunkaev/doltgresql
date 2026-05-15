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
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

const defaultSchemaName = "public"
const defaultDatabaseName = "postgres"
const stateVersion = 1

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

type persistentState struct {
	Version int               `json:"version"`
	Tables  []persistentTable `json:"tables"`
}

type persistentTable struct {
	Database string   `json:"database"`
	Schema   string   `json:"schema"`
	Table    string   `json:"table"`
	Enabled  bool     `json:"enabled,omitempty"`
	Forced   bool     `json:"forced,omitempty"`
	Policies []Policy `json:"policies,omitempty"`
}

var registry = struct {
	sync.RWMutex
	tables       map[tableKey]State
	transactions map[uint32]transactionSnapshot
	storageFS    filesys.Filesys
	storagePath  string
}{
	tables:       map[tableKey]State{},
	transactions: map[uint32]transactionSnapshot{},
}

// ConfigureStorage loads and persists row-level security state in the supplied filesystem.
func ConfigureStorage(fs filesys.Filesys, storagePath string) error {
	registry.Lock()
	defer registry.Unlock()
	registry.storageFS = fs
	registry.storagePath = storagePath
	registry.tables = map[tableKey]State{}
	registry.transactions = map[uint32]transactionSnapshot{}
	if fs == nil || storagePath == "" {
		return nil
	}
	return loadLocked()
}

// ResetForTests clears all in-memory row-level security state.
func ResetForTests() {
	registry.Lock()
	defer registry.Unlock()
	registry.tables = map[tableKey]State{}
	registry.transactions = map[uint32]transactionSnapshot{}
	registry.storageFS = nil
	registry.storagePath = ""
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
	snapshot, ok := registry.transactions[connectionID]
	delete(registry.transactions, connectionID)
	if ok && snapshot.captured {
		_ = persistLocked()
	}
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
		_ = persistLocked()
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
	setStateLocked(key, state)
	persistMutationLocked(connectionID)
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
	setStateLocked(key, state)
	persistMutationLocked(connectionID)
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
		setStateLocked(key, state)
		persistMutationLocked(connectionID)
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
	persistMutationLocked(connectionID)
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
	setStateLocked(makeKey(database, newSchema, newTable), state)
	persistMutationLocked(connectionID)
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
	setStateLocked(key, state)
	persistMutationLocked(connectionID)
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

func persistMutationLocked(connectionID uint32) {
	if _, inTransaction := registry.transactions[connectionID]; inTransaction {
		return
	}
	_ = persistLocked()
}

func setStateLocked(key tableKey, state State) {
	if !state.Enabled && !state.Forced && len(state.Policies) == 0 {
		delete(registry.tables, key)
		return
	}
	registry.tables[key] = state
}

func loadLocked() error {
	exists, isDir := registry.storageFS.Exists(registry.storagePath)
	if !exists {
		return nil
	}
	if isDir {
		return errors.Errorf("row-level security state path %q is a directory", registry.storagePath)
	}
	data, err := registry.storageFS.ReadFile(registry.storagePath)
	if err != nil {
		return err
	}
	var state persistentState
	if err = json.Unmarshal(data, &state); err != nil {
		return err
	}
	for _, stored := range state.Tables {
		for i := range stored.Policies {
			stored.Policies[i].Name = normalizeIdentifier(stored.Policies[i].Name)
			stored.Policies[i].Command = strings.ToLower(strings.TrimSpace(stored.Policies[i].Command))
			for j := range stored.Policies[i].Roles {
				stored.Policies[i].Roles[j] = normalizeIdentifier(stored.Policies[i].Roles[j])
			}
		}
		setStateLocked(makeKey(stored.Database, stored.Schema, stored.Table), State{
			Enabled:  stored.Enabled,
			Forced:   stored.Forced,
			Policies: clonePolicies(stored.Policies),
		})
	}
	return nil
}

func persistLocked() error {
	if registry.storageFS == nil || registry.storagePath == "" {
		return nil
	}
	if len(registry.tables) == 0 {
		if exists, isDir := registry.storageFS.Exists(registry.storagePath); exists && !isDir {
			return registry.storageFS.DeleteFile(registry.storagePath)
		}
		return nil
	}
	dir := filepath.Dir(registry.storagePath)
	if dir != "." && dir != "" {
		if err := registry.storageFS.MkDirs(dir); err != nil {
			return err
		}
	}
	data, err := json.MarshalIndent(toPersistentStateLocked(), "", "  ")
	if err != nil {
		return err
	}
	return registry.storageFS.WriteFile(registry.storagePath, data, os.ModePerm)
}

func toPersistentStateLocked() persistentState {
	keys := make([]tableKey, 0, len(registry.tables))
	for key := range registry.tables {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].database != keys[j].database {
			return keys[i].database < keys[j].database
		}
		if keys[i].schema != keys[j].schema {
			return keys[i].schema < keys[j].schema
		}
		return keys[i].table < keys[j].table
	})
	tables := make([]persistentTable, 0, len(keys))
	for _, key := range keys {
		state := registry.tables[key]
		tables = append(tables, persistentTable{
			Database: key.database,
			Schema:   key.schema,
			Table:    key.table,
			Enabled:  state.Enabled,
			Forced:   state.Forced,
			Policies: clonePolicies(state.Policies),
		})
	}
	return persistentState{
		Version: stateVersion,
		Tables:  tables,
	}
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
