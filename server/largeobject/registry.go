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

package largeobject

import (
	"sort"
	"sync"

	"github.com/cockroachdb/errors"
)

const largeObjectPageSize = 2048

// Object is a PostgreSQL large object.
type Object struct {
	OID   uint32
	Owner string
	Data  []byte
}

var registry = struct {
	sync.RWMutex
	nextOID uint32
	objects map[uint32]Object
}{
	nextOID: 100000,
	objects: make(map[uint32]Object),
}

// ResetForTests clears the in-memory large-object registry.
func ResetForTests() {
	registry.Lock()
	defer registry.Unlock()
	registry.nextOID = 100000
	registry.objects = make(map[uint32]Object)
}

// Create creates a large object with the requested OID. A requested OID of 0 allocates the next free OID.
func Create(requestedOID uint32, owner string, data []byte) (uint32, error) {
	registry.Lock()
	defer registry.Unlock()
	oid := requestedOID
	if oid == 0 {
		for {
			oid = registry.nextOID
			registry.nextOID++
			if _, ok := registry.objects[oid]; !ok {
				break
			}
		}
	}
	if _, ok := registry.objects[oid]; ok {
		return 0, errors.Errorf("large object %d already exists", oid)
	}
	registry.objects[oid] = Object{OID: oid, Owner: owner, Data: append([]byte(nil), data...)}
	if oid >= registry.nextOID {
		registry.nextOID = oid + 1
	}
	return oid, nil
}

// Unlink removes a large object.
func Unlink(oid uint32) int32 {
	registry.Lock()
	defer registry.Unlock()
	if _, ok := registry.objects[oid]; !ok {
		return 0
	}
	delete(registry.objects, oid)
	return 1
}

// Get returns a copy of the large object's data.
func Get(oid uint32) ([]byte, bool) {
	registry.RLock()
	defer registry.RUnlock()
	obj, ok := registry.objects[oid]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), obj.Data...), true
}

// Exists returns whether a large object exists.
func Exists(oid uint32) bool {
	registry.RLock()
	defer registry.RUnlock()
	_, ok := registry.objects[oid]
	return ok
}

// AlterOwner updates a large object's owner.
func AlterOwner(oid uint32, owner string) error {
	registry.Lock()
	defer registry.Unlock()
	obj, ok := registry.objects[oid]
	if !ok {
		return errors.Errorf("large object %d does not exist", oid)
	}
	obj.Owner = owner
	registry.objects[oid] = obj
	return nil
}

// Objects returns a stable snapshot of all large objects.
func Objects() []Object {
	registry.RLock()
	defer registry.RUnlock()
	objects := make([]Object, 0, len(registry.objects))
	for _, obj := range registry.objects {
		obj.Data = append([]byte(nil), obj.Data...)
		objects = append(objects, obj)
	}
	sort.Slice(objects, func(i, j int) bool {
		return objects[i].OID < objects[j].OID
	})
	return objects
}

// Pages returns a large object's data split into PostgreSQL large-object pages.
func Pages(obj Object) [][]byte {
	if len(obj.Data) == 0 {
		return nil
	}
	pages := make([][]byte, 0, (len(obj.Data)+largeObjectPageSize-1)/largeObjectPageSize)
	for start := 0; start < len(obj.Data); start += largeObjectPageSize {
		end := start + largeObjectPageSize
		if end > len(obj.Data) {
			end = len(obj.Data)
		}
		pages = append(pages, append([]byte(nil), obj.Data[start:end]...))
	}
	return pages
}
