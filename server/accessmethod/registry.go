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

package accessmethod

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

const persistentStateVersion = 1

// Entry is a user-defined access method catalog entry.
type Entry struct {
	Name    string `json:"name"`
	Handler string `json:"handler"`
	Type    string `json:"type"`
}

type persistentState struct {
	Version int     `json:"version"`
	Entries []Entry `json:"entries"`
}

var registry = struct {
	sync.Mutex
	entries     map[string]Entry
	storageFS   filesys.Filesys
	storagePath string
}{
	entries: make(map[string]Entry),
}

var builtInNames = map[string]struct{}{
	"heap":   {},
	"btree":  {},
	"hash":   {},
	"gist":   {},
	"gin":    {},
	"spgist": {},
	"brin":   {},
}

// Register adds a user-defined access method entry.
func Register(name string, handlerName string, typ string) error {
	registry.Lock()
	defer registry.Unlock()
	if _, ok := builtInNames[name]; ok {
		return errors.Errorf(`access method "%s" already exists`, name)
	}
	if _, ok := registry.entries[name]; ok {
		return errors.Errorf(`access method "%s" already exists`, name)
	}
	registry.entries[name] = Entry{
		Name:    name,
		Handler: handlerName,
		Type:    typ,
	}
	return persistLocked()
}

// Drop removes a user-defined access method entry.
func Drop(name string) (bool, error) {
	registry.Lock()
	defer registry.Unlock()
	if _, ok := builtInNames[name]; ok {
		return false, errors.Errorf(`cannot drop built-in access method "%s"`, name)
	}
	if _, ok := registry.entries[name]; !ok {
		return false, nil
	}
	delete(registry.entries, name)
	return true, persistLocked()
}

// Snapshot returns the current user-defined access method entries.
func Snapshot() []Entry {
	registry.Lock()
	defer registry.Unlock()
	return entriesLocked()
}

func entriesLocked() []Entry {
	entries := make([]Entry, 0, len(registry.entries))
	for _, entry := range registry.entries {
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name < entries[j].Name
	})
	return entries
}

// ConfigureStorage loads and persists user-defined access method state in the supplied filesystem.
func ConfigureStorage(fs filesys.Filesys, storagePath string) error {
	registry.Lock()
	defer registry.Unlock()
	registry.entries = make(map[string]Entry)
	registry.storageFS = fs
	registry.storagePath = storagePath
	if fs == nil || storagePath == "" {
		return nil
	}
	return loadLocked()
}

// ResetForTests clears all in-memory access method state.
func ResetForTests() {
	registry.Lock()
	defer registry.Unlock()
	registry.entries = make(map[string]Entry)
	registry.storageFS = nil
	registry.storagePath = ""
}

func loadLocked() error {
	exists, isDir := registry.storageFS.Exists(registry.storagePath)
	if !exists {
		return nil
	}
	if isDir {
		return errors.Errorf("access method state path %q is a directory", registry.storagePath)
	}
	data, err := registry.storageFS.ReadFile(registry.storagePath)
	if err != nil {
		return err
	}
	var state persistentState
	if err = json.Unmarshal(data, &state); err != nil {
		return err
	}
	if state.Version != persistentStateVersion {
		return errors.Errorf("unsupported access method state version %d", state.Version)
	}
	for _, entry := range state.Entries {
		if entry.Name == "" {
			return errors.New("access method state contains entry with empty name")
		}
		if _, ok := builtInNames[entry.Name]; ok {
			return errors.Errorf(`access method state cannot redefine built-in access method "%s"`, entry.Name)
		}
		if _, ok := registry.entries[entry.Name]; ok {
			return errors.Errorf(`access method state contains duplicate entry "%s"`, entry.Name)
		}
		registry.entries[entry.Name] = entry
	}
	return nil
}

func persistLocked() error {
	if registry.storageFS == nil || registry.storagePath == "" {
		return nil
	}
	if len(registry.entries) == 0 {
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
	data, err := json.MarshalIndent(persistentState{
		Version: persistentStateVersion,
		Entries: entriesLocked(),
	}, "", "  ")
	if err != nil {
		return err
	}
	return registry.storageFS.WriteFile(registry.storagePath, data, os.ModePerm)
}
