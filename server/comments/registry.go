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

package comments

import (
	"sync"

	"github.com/dolthub/doltgresql/core/id"
)

const (
	pgCatalogName = "pg_catalog"
	pgClassName   = "pg_class"
)

type Key struct {
	ObjOID   uint32
	ClassOID uint32
	ObjSubID int32
}

type Entry struct {
	Key
	Description string
}

var registry = struct {
	sync.RWMutex
	entries map[Key]string
}{
	entries: make(map[Key]string),
}

func Set(key Key, description *string) {
	registry.Lock()
	defer registry.Unlock()
	if description == nil {
		delete(registry.entries, key)
		return
	}
	registry.entries[key] = *description
}

func Get(key Key) (string, bool) {
	registry.RLock()
	defer registry.RUnlock()
	description, ok := registry.entries[key]
	return description, ok
}

func GetForObject(objOID uint32) (string, bool) {
	registry.RLock()
	defer registry.RUnlock()
	for key, description := range registry.entries {
		if key.ObjOID == objOID && key.ObjSubID == 0 {
			return description, true
		}
	}
	return "", false
}

func Entries() []Entry {
	registry.RLock()
	defer registry.RUnlock()
	entries := make([]Entry, 0, len(registry.entries))
	for key, description := range registry.entries {
		entries = append(entries, Entry{Key: key, Description: description})
	}
	return entries
}

func PgClassOID() uint32 {
	return id.Cache().ToOID(id.NewTable(pgCatalogName, pgClassName).AsId())
}

func IDFromOID(oid uint32) id.Id {
	if internal := id.Cache().ToInternal(oid); internal.IsValid() {
		return internal
	}
	return id.NewOID(oid).AsId()
}
