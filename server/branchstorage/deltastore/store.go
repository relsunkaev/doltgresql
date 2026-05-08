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

// Package deltastore is the durable home for per-commit delta metadata records
// produced by generated batch branch commits. Each record is keyed by the
// branch commit hash it describes; the bytes are the canonical deltameta
// encoding produced by deltameta.Encode (or deltameta.Builder.Build →
// deltameta.Encode).
//
// The store is intentionally narrow: Put, Get, Delete, Range. Retention and
// garbage collection live in retention.go and consume the same interface so
// that operators can swap implementations (in-memory for tests, file-backed
// for single-process deployments, future store-backed for horizontally-scaled
// deployments) without changing the GC code path.
//
// See docs/customer-branch-storage-architecture.md "Delta Metadata" and
// "Retention and Garbage Collection".
package deltastore

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/store/hash"

	"github.com/dolthub/doltgresql/server/branchstorage/deltameta"
)

// ErrNotFound is the sentinel returned by Get and Delete when a target commit
// has no recorded delta. Callers compare with errors.Is.
var ErrNotFound = errors.New("deltastore: record not found")

// Record is one persisted delta. EncodedDelta is the canonical bytes form a
// fast-path driver hands to deltameta.Decode. RecordedAt drives retention
// windows; if zero, Put fills it with time.Now().UTC().
type Record struct {
	TargetCommit hash.Hash
	EncodedDelta []byte
	RecordedAt   time.Time
}

// Store is the durable per-commit delta record interface.
//
// Concurrency: implementations must be safe for concurrent calls. Put after
// Put on the same TargetCommit is idempotent-with-overwrite (latest write
// wins). Delete after Delete returns ErrNotFound (typed so retries are
// distinguishable from successful first-time removals).
type Store interface {
	// Put writes the record durably. The bytes must be a valid deltameta
	// payload (deltameta.Decode succeeds); implementations enforce this so
	// a stale or malformed encoding cannot land in the store.
	Put(ctx context.Context, rec Record) error

	// Get fetches the record for the given target commit. Returns
	// ErrNotFound if the commit has no recorded delta.
	Get(ctx context.Context, targetCommit hash.Hash) (Record, error)

	// Delete removes the record for the given target commit. Returns
	// ErrNotFound if the commit has no recorded delta.
	Delete(ctx context.Context, targetCommit hash.Hash) error

	// Range invokes fn for each record in deterministic order
	// (target-commit hash ascending). fn returning false stops iteration.
	Range(ctx context.Context, fn func(Record) bool) error
}

// notFound returns an error wrapping ErrNotFound that names the missing
// commit hash. Used by all Store implementations so error surfaces are
// uniform.
func notFound(targetCommit hash.Hash) error {
	return errors.Wrapf(ErrNotFound, "commit %s", targetCommit)
}

// validateEncoded ensures the record's payload is a well-formed delta. The
// store is the last line of defense against malformed records: producers may
// skip Builder, but they cannot skip the store's Put.
func validateEncoded(encoded []byte) error {
	if _, err := deltameta.Decode(encoded); err != nil {
		return errors.Wrap(err, "deltastore: invalid encoded delta")
	}
	return nil
}

// MemoryStore is an in-process Store. Useful for tests and for the GC safety
// suite (which exercises the store with no filesystem state).
type MemoryStore struct {
	mu      sync.RWMutex
	records map[hash.Hash]Record
}

// NewMemoryStore returns an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{records: make(map[hash.Hash]Record)}
}

// Put writes the record into the in-memory map. Validates the encoding first.
func (s *MemoryStore) Put(_ context.Context, rec Record) error {
	if err := validateEncoded(rec.EncodedDelta); err != nil {
		return err
	}
	if rec.RecordedAt.IsZero() {
		rec.RecordedAt = time.Now().UTC()
	}
	rec.EncodedDelta = append([]byte(nil), rec.EncodedDelta...)
	s.mu.Lock()
	s.records[rec.TargetCommit] = rec
	s.mu.Unlock()
	return nil
}

// Get returns a copy of the stored record. The returned bytes are a defensive
// copy so callers can mutate them without affecting the store.
func (s *MemoryStore) Get(_ context.Context, targetCommit hash.Hash) (Record, error) {
	s.mu.RLock()
	rec, ok := s.records[targetCommit]
	s.mu.RUnlock()
	if !ok {
		return Record{}, notFound(targetCommit)
	}
	rec.EncodedDelta = append([]byte(nil), rec.EncodedDelta...)
	return rec, nil
}

// Delete removes the record. Returns ErrNotFound if no record exists.
func (s *MemoryStore) Delete(_ context.Context, targetCommit hash.Hash) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.records[targetCommit]; !ok {
		return notFound(targetCommit)
	}
	delete(s.records, targetCommit)
	return nil
}

// Range iterates in target-commit-hash ascending order so callers (notably
// the retention GC pass) get a deterministic walk regardless of insert order.
func (s *MemoryStore) Range(_ context.Context, fn func(Record) bool) error {
	s.mu.RLock()
	keys := make([]hash.Hash, 0, len(s.records))
	for k := range s.records {
		keys = append(keys, k)
	}
	s.mu.RUnlock()
	sort.Slice(keys, func(i, j int) bool { return keys[i].Less(keys[j]) })
	for _, k := range keys {
		s.mu.RLock()
		rec, ok := s.records[k]
		s.mu.RUnlock()
		if !ok {
			// concurrently deleted; skip
			continue
		}
		rec.EncodedDelta = append([]byte(nil), rec.EncodedDelta...)
		if !fn(rec) {
			return nil
		}
	}
	return nil
}
