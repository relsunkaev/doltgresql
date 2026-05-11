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
	"sort"
	"sync"

	"github.com/cockroachdb/errors"
)

// Entry is a user-defined access method catalog entry.
type Entry struct {
	Name    string
	Handler string
	Type    string
}

var registry = struct {
	sync.Mutex
	entries map[string]Entry
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
	return nil
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
	return true, nil
}

// Snapshot returns the current user-defined access method entries.
func Snapshot() []Entry {
	registry.Lock()
	defer registry.Unlock()
	entries := make([]Entry, 0, len(registry.entries))
	for _, entry := range registry.entries {
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name < entries[j].Name
	})
	return entries
}
