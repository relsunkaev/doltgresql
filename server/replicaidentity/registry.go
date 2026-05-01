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

package replicaidentity

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

// Identity is PostgreSQL's pg_class.relreplident value for a table.
type Identity byte

const (
	IdentityDefault    Identity = 'd'
	IdentityUsingIndex Identity = 'i'
	IdentityFull       Identity = 'f'
	IdentityNothing    Identity = 'n'
)

const stateVersion = 1

// Setting describes one table's selected replica identity.
type Setting struct {
	Identity  Identity
	IndexName string
}

type registryKey struct {
	Database string
	Schema   string
	Table    string
}

type persistentState struct {
	Version int                 `json:"version"`
	Tables  []persistentSetting `json:"tables"`
}

type persistentSetting struct {
	Database  string `json:"database"`
	Schema    string `json:"schema"`
	Table     string `json:"table"`
	Identity  string `json:"identity"`
	IndexName string `json:"index_name,omitempty"`
}

var defaultRegistry = struct {
	sync.RWMutex
	settings    map[registryKey]Setting
	storageFS   filesys.Filesys
	storagePath string
}{
	settings: make(map[registryKey]Setting),
}

// Byte returns the pg_class.relreplident byte for this identity.
func (i Identity) Byte() byte {
	if !i.Valid() {
		return byte(IdentityDefault)
	}
	return byte(i)
}

// String returns the pg_class.relreplident string for this identity.
func (i Identity) String() string {
	return string(i.Byte())
}

// Valid returns whether this identity is one of PostgreSQL's relreplident values.
func (i Identity) Valid() bool {
	switch i {
	case IdentityDefault, IdentityUsingIndex, IdentityFull, IdentityNothing:
		return true
	default:
		return false
	}
}

// ConfigureStorage loads and persists replica identity state in the supplied filesystem.
func ConfigureStorage(fs filesys.Filesys, storagePath string) error {
	defaultRegistry.Lock()
	defer defaultRegistry.Unlock()
	defaultRegistry.storageFS = fs
	defaultRegistry.storagePath = storagePath
	defaultRegistry.settings = make(map[registryKey]Setting)
	if fs == nil || storagePath == "" {
		return nil
	}
	return loadLocked()
}

// ResetForTests clears all in-memory replica identity state.
func ResetForTests() {
	defaultRegistry.Lock()
	defer defaultRegistry.Unlock()
	defaultRegistry.settings = make(map[registryKey]Setting)
	defaultRegistry.storageFS = nil
	defaultRegistry.storagePath = ""
}

// Get returns the replica identity setting for a table, defaulting to DEFAULT.
func Get(database string, schema string, table string) Setting {
	defaultRegistry.RLock()
	defer defaultRegistry.RUnlock()
	setting, ok := defaultRegistry.settings[registryKey{
		Database: database,
		Schema:   schema,
		Table:    table,
	}]
	if !ok || !setting.Identity.Valid() {
		return Setting{Identity: IdentityDefault}
	}
	return setting
}

// Set records a table's replica identity. DEFAULT is represented by removing any explicit setting.
func Set(database string, schema string, table string, identity Identity, indexName string) error {
	if database == "" {
		return errors.New("database is required for replica identity")
	}
	if schema == "" {
		return errors.New("schema is required for replica identity")
	}
	if table == "" {
		return errors.New("table is required for replica identity")
	}
	if !identity.Valid() {
		return errors.Errorf("invalid replica identity %q", byte(identity))
	}
	if identity != IdentityUsingIndex {
		indexName = ""
	} else if indexName == "" {
		return errors.New("index name is required for replica identity using index")
	}

	defaultRegistry.Lock()
	defer defaultRegistry.Unlock()
	key := registryKey{
		Database: database,
		Schema:   schema,
		Table:    table,
	}
	if identity == IdentityDefault {
		delete(defaultRegistry.settings, key)
	} else {
		defaultRegistry.settings[key] = Setting{
			Identity:  identity,
			IndexName: indexName,
		}
	}
	return persistLocked()
}

func loadLocked() error {
	exists, isDir := defaultRegistry.storageFS.Exists(defaultRegistry.storagePath)
	if !exists {
		return nil
	}
	if isDir {
		return errors.Errorf("replica identity state path %q is a directory", defaultRegistry.storagePath)
	}
	data, err := defaultRegistry.storageFS.ReadFile(defaultRegistry.storagePath)
	if err != nil {
		return err
	}
	var state persistentState
	if err = json.Unmarshal(data, &state); err != nil {
		return err
	}
	for _, stored := range state.Tables {
		if len(stored.Identity) != 1 {
			return errors.Errorf("invalid replica identity for %s.%s.%s", stored.Database, stored.Schema, stored.Table)
		}
		identity := Identity(stored.Identity[0])
		if !identity.Valid() {
			return errors.Errorf("invalid replica identity %q for %s.%s.%s", stored.Identity, stored.Database, stored.Schema, stored.Table)
		}
		if identity == IdentityDefault {
			continue
		}
		defaultRegistry.settings[registryKey{
			Database: stored.Database,
			Schema:   stored.Schema,
			Table:    stored.Table,
		}] = Setting{
			Identity:  identity,
			IndexName: stored.IndexName,
		}
	}
	return nil
}

func persistLocked() error {
	if defaultRegistry.storageFS == nil || defaultRegistry.storagePath == "" {
		return nil
	}
	if len(defaultRegistry.settings) == 0 {
		if exists, isDir := defaultRegistry.storageFS.Exists(defaultRegistry.storagePath); exists && !isDir {
			return defaultRegistry.storageFS.DeleteFile(defaultRegistry.storagePath)
		}
		return nil
	}
	dir := filepath.Dir(defaultRegistry.storagePath)
	if dir != "." && dir != "" {
		if err := defaultRegistry.storageFS.MkDirs(dir); err != nil {
			return err
		}
	}
	data, err := json.MarshalIndent(toPersistentStateLocked(), "", "  ")
	if err != nil {
		return err
	}
	return defaultRegistry.storageFS.WriteFile(defaultRegistry.storagePath, data, os.ModePerm)
}

func toPersistentStateLocked() persistentState {
	keys := make([]registryKey, 0, len(defaultRegistry.settings))
	for key := range defaultRegistry.settings {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Database != keys[j].Database {
			return keys[i].Database < keys[j].Database
		}
		if keys[i].Schema != keys[j].Schema {
			return keys[i].Schema < keys[j].Schema
		}
		return keys[i].Table < keys[j].Table
	})

	state := persistentState{
		Version: stateVersion,
		Tables:  make([]persistentSetting, 0, len(keys)),
	}
	for _, key := range keys {
		setting := defaultRegistry.settings[key]
		state.Tables = append(state.Tables, persistentSetting{
			Database:  key.Database,
			Schema:    key.Schema,
			Table:     key.Table,
			Identity:  setting.Identity.String(),
			IndexName: setting.IndexName,
		})
	}
	return state
}
