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
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

const largeObjectPageSize = 2048
const persistentStateVersion = 1
const defaultDatabaseName = "postgres"

// Object is a PostgreSQL large object.
type Object struct {
	OID      uint32
	Database string
	Owner    string
	Data     []byte
	ACL      []string
}

type objectKey struct {
	database string
	oid      uint32
}

type transactionSnapshot struct {
	captured   bool
	nextOID    uint32
	objects    map[objectKey]Object
	savepoints []transactionSavepoint
}

type transactionSavepoint struct {
	name    string
	nextOID uint32
	objects map[objectKey]Object
}

type persistentState struct {
	Version int      `json:"version"`
	NextOID uint32   `json:"next_oid"`
	Objects []Object `json:"objects"`
}

var registry = struct {
	sync.RWMutex
	nextOID      uint32
	objects      map[objectKey]Object
	transactions map[uint32]transactionSnapshot
	storageFS    filesys.Filesys
	storagePath  string
}{
	nextOID:      100000,
	objects:      make(map[objectKey]Object),
	transactions: make(map[uint32]transactionSnapshot),
}

// ResetForTests clears the in-memory large-object registry.
func ResetForTests() {
	registry.Lock()
	defer registry.Unlock()
	registry.nextOID = 100000
	registry.objects = make(map[objectKey]Object)
	registry.transactions = make(map[uint32]transactionSnapshot)
	registry.storageFS = nil
	registry.storagePath = ""
}

// ConfigureStorage loads and persists large-object state in the supplied filesystem.
func ConfigureStorage(fs filesys.Filesys, storagePath string) error {
	registry.Lock()
	defer registry.Unlock()
	registry.nextOID = 100000
	registry.objects = make(map[objectKey]Object)
	registry.transactions = make(map[uint32]transactionSnapshot)
	registry.storageFS = fs
	registry.storagePath = storagePath
	if fs == nil || storagePath == "" {
		return nil
	}
	return loadLocked()
}

// BeginTransaction starts transaction-scoped large-object bookkeeping.
func BeginTransaction(connectionID uint32) {
	registry.Lock()
	defer registry.Unlock()
	registry.transactions[connectionID] = transactionSnapshot{}
}

// TrackMutation captures the pre-transaction state before the first mutation.
func TrackMutation(connectionID uint32) {
	registry.Lock()
	defer registry.Unlock()
	snapshot, ok := registry.transactions[connectionID]
	if !ok || snapshot.captured {
		return
	}
	snapshot.captured = true
	snapshot.nextOID = registry.nextOID
	snapshot.objects = cloneObjectsLocked()
	registry.transactions[connectionID] = snapshot
}

// PushSavepoint records the current state for a transaction savepoint.
func PushSavepoint(connectionID uint32, name string) {
	registry.Lock()
	defer registry.Unlock()
	snapshot := registry.transactions[connectionID]
	snapshot.savepoints = append(snapshot.savepoints, transactionSavepoint{
		name:    name,
		nextOID: registry.nextOID,
		objects: cloneObjectsLocked(),
	})
	registry.transactions[connectionID] = snapshot
}

// RollbackToSavepoint restores the state captured by the named savepoint.
func RollbackToSavepoint(connectionID uint32, name string) error {
	registry.Lock()
	defer registry.Unlock()
	snapshot, ok := registry.transactions[connectionID]
	if !ok {
		return nil
	}
	idx := findSavepoint(snapshot.savepoints, name)
	if idx < 0 {
		return nil
	}
	frame := snapshot.savepoints[idx]
	registry.nextOID = frame.nextOID
	registry.objects = cloneObjectMap(frame.objects)
	snapshot.savepoints = snapshot.savepoints[:idx+1]
	registry.transactions[connectionID] = snapshot
	return persistLocked()
}

// ReleaseSavepoint releases the named savepoint and savepoints created after it.
func ReleaseSavepoint(connectionID uint32, name string) {
	registry.Lock()
	defer registry.Unlock()
	snapshot, ok := registry.transactions[connectionID]
	if !ok {
		return
	}
	idx := findSavepoint(snapshot.savepoints, name)
	if idx < 0 {
		return
	}
	if idx == 0 {
		snapshot.savepoints = nil
	} else {
		snapshot.savepoints = snapshot.savepoints[:idx]
	}
	registry.transactions[connectionID] = snapshot
}

// CommitTransaction discards transaction-scoped rollback state.
func CommitTransaction(connectionID uint32) error {
	registry.Lock()
	defer registry.Unlock()
	delete(registry.transactions, connectionID)
	return persistLocked()
}

// RollbackTransaction restores the pre-transaction large-object state.
func RollbackTransaction(connectionID uint32) {
	registry.Lock()
	defer registry.Unlock()
	snapshot, ok := registry.transactions[connectionID]
	delete(registry.transactions, connectionID)
	if !ok || !snapshot.captured {
		return
	}
	registry.nextOID = snapshot.nextOID
	registry.objects = cloneObjectMap(snapshot.objects)
	_ = persistLocked()
}

// Create creates a large object with the requested OID. A requested OID of 0 allocates the next free OID.
func Create(database string, requestedOID uint32, owner string, data []byte) (uint32, error) {
	registry.Lock()
	defer registry.Unlock()
	database = normalizeDatabase(database)
	oid := requestedOID
	if oid == 0 {
		for {
			oid = registry.nextOID
			registry.nextOID++
			if _, ok := registry.objects[objectKey{database: database, oid: oid}]; !ok {
				break
			}
		}
	}
	key := objectKey{database: database, oid: oid}
	if _, ok := registry.objects[key]; ok {
		return 0, errors.Errorf("large object %d already exists", oid)
	}
	registry.objects[key] = Object{OID: oid, Database: database, Owner: owner, Data: append([]byte(nil), data...)}
	if oid >= registry.nextOID {
		registry.nextOID = oid + 1
	}
	if err := persistLocked(); err != nil {
		delete(registry.objects, key)
		return 0, err
	}
	return oid, nil
}

// Unlink removes a large object.
func Unlink(database string, oid uint32) (int32, error) {
	registry.Lock()
	defer registry.Unlock()
	key := objectKey{database: normalizeDatabase(database), oid: oid}
	if _, ok := registry.objects[key]; !ok {
		return 0, nil
	}
	delete(registry.objects, key)
	if err := persistLocked(); err != nil {
		return 0, err
	}
	return 1, nil
}

// Put writes data into an existing large object at the given zero-based offset.
func Put(database string, oid uint32, offset int64, data []byte) error {
	if offset < 0 {
		return errors.Errorf("large object offset must not be negative")
	}
	registry.Lock()
	defer registry.Unlock()
	key := objectKey{database: normalizeDatabase(database), oid: oid}
	obj, ok := registry.objects[key]
	if !ok {
		return errors.Errorf("large object %d does not exist", oid)
	}
	end := offset + int64(len(data))
	if end < offset {
		return errors.Errorf("large object write offset overflow")
	}
	if end > int64(len(obj.Data)) {
		resized := make([]byte, end)
		copy(resized, obj.Data)
		obj.Data = resized
	}
	copy(obj.Data[offset:end], data)
	registry.objects[key] = obj
	return persistLocked()
}

// Get returns a copy of the large object's data.
func Get(database string, oid uint32) ([]byte, bool) {
	registry.RLock()
	defer registry.RUnlock()
	obj, ok := registry.objects[objectKey{database: normalizeDatabase(database), oid: oid}]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), obj.Data...), true
}

// GetSlice returns a copy of a byte range from the large object.
func GetSlice(database string, oid uint32, offset int64, length int32) ([]byte, bool, error) {
	if offset < 0 {
		return nil, false, errors.Errorf("large object offset must not be negative")
	}
	if length < 0 {
		return nil, false, errors.Errorf("large object length must not be negative")
	}
	registry.RLock()
	defer registry.RUnlock()
	obj, ok := registry.objects[objectKey{database: normalizeDatabase(database), oid: oid}]
	if !ok {
		return nil, false, nil
	}
	if offset >= int64(len(obj.Data)) || length == 0 {
		return []byte{}, true, nil
	}
	end := offset + int64(length)
	if end < offset {
		return nil, false, errors.Errorf("large object read offset overflow")
	}
	if end > int64(len(obj.Data)) {
		end = int64(len(obj.Data))
	}
	return append([]byte(nil), obj.Data[offset:end]...), true, nil
}

// Exists returns whether a large object exists.
func Exists(database string, oid uint32) bool {
	registry.RLock()
	defer registry.RUnlock()
	_, ok := registry.objects[objectKey{database: normalizeDatabase(database), oid: oid}]
	return ok
}

// Owner returns the owner of a large object.
func Owner(database string, oid uint32) (string, bool) {
	registry.RLock()
	defer registry.RUnlock()
	obj, ok := registry.objects[objectKey{database: normalizeDatabase(database), oid: oid}]
	if !ok {
		return "", false
	}
	return obj.Owner, true
}

// AlterOwner updates a large object's owner.
func AlterOwner(database string, oid uint32, owner string) error {
	registry.Lock()
	defer registry.Unlock()
	key := objectKey{database: normalizeDatabase(database), oid: oid}
	obj, ok := registry.objects[key]
	if !ok {
		return errors.Errorf("large object %d does not exist", oid)
	}
	obj.Owner = owner
	registry.objects[key] = obj
	return persistLocked()
}

// AddACLItem records an ACL item for the large object.
func AddACLItem(database string, oid uint32, item string) error {
	registry.Lock()
	defer registry.Unlock()
	key := objectKey{database: normalizeDatabase(database), oid: oid}
	obj, ok := registry.objects[key]
	if !ok {
		return errors.Errorf("large object %d does not exist", oid)
	}
	for _, existing := range obj.ACL {
		if existing == item {
			return nil
		}
	}
	obj.ACL = append(obj.ACL, item)
	sort.Strings(obj.ACL)
	registry.objects[key] = obj
	return persistLocked()
}

// Objects returns a stable snapshot of all large objects in the database.
func Objects(database string) []Object {
	registry.RLock()
	defer registry.RUnlock()
	database = normalizeDatabase(database)
	objects := make([]Object, 0, len(registry.objects))
	for _, obj := range registry.objects {
		if obj.Database == database {
			objects = append(objects, cloneObject(obj))
		}
	}
	sort.Slice(objects, func(i, j int) bool {
		if objects[i].Database != objects[j].Database {
			return objects[i].Database < objects[j].Database
		}
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

func loadLocked() error {
	exists, isDir := registry.storageFS.Exists(registry.storagePath)
	if !exists {
		return nil
	}
	if isDir {
		return errors.Errorf("large object state path %q is a directory", registry.storagePath)
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
		return errors.Errorf("unsupported large object state version %d", state.Version)
	}
	maxOID := uint32(0)
	for _, object := range state.Objects {
		object.Database = normalizeDatabase(object.Database)
		registry.objects[objectKey{database: object.Database, oid: object.OID}] = cloneObject(object)
		if object.OID > maxOID {
			maxOID = object.OID
		}
	}
	registry.nextOID = state.NextOID
	if registry.nextOID <= maxOID {
		registry.nextOID = maxOID + 1
	}
	if registry.nextOID == 0 {
		registry.nextOID = 100000
	}
	return nil
}

func persistLocked() error {
	if registry.storageFS == nil || registry.storagePath == "" {
		return nil
	}
	if len(registry.objects) == 0 {
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
	objects := objectsLocked()
	state := persistentState{
		Version: persistentStateVersion,
		NextOID: registry.nextOID,
		Objects: objects,
	}
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	return registry.storageFS.WriteFile(registry.storagePath, data, os.ModePerm)
}

func cloneObjectsLocked() map[objectKey]Object {
	return cloneObjectMap(registry.objects)
}

func objectsLocked() []Object {
	objects := make([]Object, 0, len(registry.objects))
	for _, obj := range registry.objects {
		objects = append(objects, cloneObject(obj))
	}
	sort.Slice(objects, func(i, j int) bool {
		return objects[i].OID < objects[j].OID
	})
	return objects
}

func cloneObjectMap(objects map[objectKey]Object) map[objectKey]Object {
	cloned := make(map[objectKey]Object, len(objects))
	for key, object := range objects {
		cloned[key] = cloneObject(object)
	}
	return cloned
}

func cloneObject(object Object) Object {
	object.Database = normalizeDatabase(object.Database)
	object.Data = append([]byte(nil), object.Data...)
	object.ACL = append([]string(nil), object.ACL...)
	return object
}

func findSavepoint(stack []transactionSavepoint, name string) int {
	for i := len(stack) - 1; i >= 0; i-- {
		if stack[i].name == name {
			return i
		}
	}
	return -1
}

func normalizeDatabase(database string) string {
	database = strings.TrimSpace(database)
	if database == "" {
		return defaultDatabaseName
	}
	return strings.ToLower(database)
}
