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

package deltastore

import (
	"context"
	"errors"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dolthub/dolt/go/store/hash"

	"github.com/dolthub/doltgresql/server/branchstorage/deltameta"
)

func mkHash(seed byte) hash.Hash {
	var h hash.Hash
	for i := range h {
		h[i] = seed + byte(i)
	}
	return h
}

func ptrHash(h hash.Hash) *hash.Hash { return &h }

func sampleEncoded(t *testing.T, base, target hash.Hash) []byte {
	t.Helper()
	d := deltameta.Delta{
		Format:       deltameta.FormatVersion1,
		BaseRoot:     base,
		TargetRef:    "refs/heads/sync",
		TargetCommit: target,
		Tables: []deltameta.TableDelta{{
			Name: "t",
			Rows: []deltameta.RowChange{{
				PrimaryKey:     []byte{1},
				OldRowHash:     ptrHash(mkHash(0xA0)),
				NewRowHash:     ptrHash(mkHash(0xA1)),
				ChangedScalars: []string{"col"},
			}},
		}},
	}
	encoded, err := deltameta.Encode(d)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return encoded
}

// runStoreContract exercises the contract that every Store implementation
// must satisfy. Called from per-implementation TestX_Store tests below.
func runStoreContract(t *testing.T, s Store) {
	ctx := context.Background()
	target := mkHash(0x20)
	encoded := sampleEncoded(t, mkHash(0x10), target)

	// Get on a missing key returns ErrNotFound.
	if _, err := s.Get(ctx, target); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound on empty store, got %v", err)
	}

	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	if err := s.Put(ctx, Record{TargetCommit: target, EncodedDelta: encoded, RecordedAt: now}); err != nil {
		t.Fatalf("put: %v", err)
	}

	// Get returns the encoded bytes verbatim.
	got, err := s.Get(ctx, target)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(got.EncodedDelta) != string(encoded) {
		t.Fatalf("get bytes differ:\n want %s\n got  %s", encoded, got.EncodedDelta)
	}
	if !got.RecordedAt.Equal(now) {
		t.Fatalf("recordedAt round-trip: want %v got %v", now, got.RecordedAt)
	}

	// Range over all records returns this one.
	var seen []hash.Hash
	if err := s.Range(ctx, func(r Record) bool {
		seen = append(seen, r.TargetCommit)
		return true
	}); err != nil {
		t.Fatalf("range: %v", err)
	}
	if len(seen) != 1 || seen[0] != target {
		t.Fatalf("range saw: %v", seen)
	}

	// Put on the same key overwrites (idempotent for same record, latest
	// wins for retries).
	encoded2 := sampleEncoded(t, mkHash(0x10), target) // same shape
	if err := s.Put(ctx, Record{TargetCommit: target, EncodedDelta: encoded2, RecordedAt: now.Add(time.Hour)}); err != nil {
		t.Fatalf("put overwrite: %v", err)
	}
	got, err = s.Get(ctx, target)
	if err != nil {
		t.Fatalf("get after overwrite: %v", err)
	}
	if !got.RecordedAt.Equal(now.Add(time.Hour)) {
		t.Fatalf("overwrite did not update RecordedAt: %v", got.RecordedAt)
	}

	// Delete removes the record.
	if err := s.Delete(ctx, target); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := s.Get(ctx, target); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}

	// Delete on missing is ErrNotFound (idempotent-but-typed).
	if err := s.Delete(ctx, target); !errors.Is(err, ErrNotFound) {
		t.Fatalf("delete on missing: %v", err)
	}
}

func TestMemoryStore_Contract(t *testing.T) {
	runStoreContract(t, NewMemoryStore())
}

func TestFileStore_Contract(t *testing.T) {
	dir := t.TempDir()
	s, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	runStoreContract(t, s)
}

func TestFileStore_DurabilityAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s1, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("open 1: %v", err)
	}
	target := mkHash(0x40)
	encoded := sampleEncoded(t, mkHash(0x10), target)
	if err := s1.Put(ctx, Record{TargetCommit: target, EncodedDelta: encoded, RecordedAt: time.Now().UTC()}); err != nil {
		t.Fatalf("put: %v", err)
	}
	// Reopen the store.
	s2, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("open 2: %v", err)
	}
	got, err := s2.Get(ctx, target)
	if err != nil {
		t.Fatalf("get after reopen: %v", err)
	}
	if string(got.EncodedDelta) != string(encoded) {
		t.Fatalf("post-reopen bytes drift")
	}
}

func TestFileStore_RejectsMalformedRecord(t *testing.T) {
	dir := t.TempDir()
	s, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ctx := context.Background()
	target := mkHash(0x40)
	if err := s.Put(ctx, Record{TargetCommit: target, EncodedDelta: []byte("not-json")}); err == nil {
		t.Fatalf("expected put of malformed delta to fail")
	}
}

func TestFileStore_KeyIsTargetCommit(t *testing.T) {
	dir := t.TempDir()
	s, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ctx := context.Background()
	target := mkHash(0xC0)
	encoded := sampleEncoded(t, mkHash(0x10), target)
	if err := s.Put(ctx, Record{TargetCommit: target, EncodedDelta: encoded, RecordedAt: time.Now().UTC()}); err != nil {
		t.Fatalf("put: %v", err)
	}
	// Path includes the hex-encoded target commit so an operator can grep
	// for a specific commit's delta record by hand.
	matches, err := filepath.Glob(filepath.Join(dir, "*"+target.String()+"*"))
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	if len(matches) == 0 {
		t.Fatalf("expected a file naming the target commit; dir contents under %s", dir)
	}
}

// TestFileStore_RecordedAtDefaultsWhenZero ensures Put fills in a
// reasonable RecordedAt if the caller forgot. Without this, GC windows can
// be impossible to reason about (zero-time records would always look stale).
func TestFileStore_RecordedAtDefaultsWhenZero(t *testing.T) {
	dir := t.TempDir()
	s, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ctx := context.Background()
	target := mkHash(0x40)
	encoded := sampleEncoded(t, mkHash(0x10), target)
	before := time.Now().UTC().Add(-time.Second)
	if err := s.Put(ctx, Record{TargetCommit: target, EncodedDelta: encoded}); err != nil {
		t.Fatalf("put: %v", err)
	}
	got, err := s.Get(ctx, target)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.RecordedAt.Before(before) {
		t.Fatalf("RecordedAt should default to roughly now, got %v", got.RecordedAt)
	}
}

func TestMemoryStore_ConcurrentPutsAreSafe(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()
	const n = 64
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			target := mkHash(byte(i))
			encoded := sampleEncoded(t, mkHash(0x10), target)
			if err := s.Put(ctx, Record{TargetCommit: target, EncodedDelta: encoded, RecordedAt: time.Now().UTC()}); err != nil {
				t.Errorf("put %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()
	count := 0
	if err := s.Range(ctx, func(r Record) bool {
		count++
		return true
	}); err != nil {
		t.Fatalf("range: %v", err)
	}
	if count != n {
		t.Fatalf("expected %d records, got %d", n, count)
	}
}

func TestMemoryStore_RangeStopsWhenCallbackFalse(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		target := mkHash(byte(i))
		encoded := sampleEncoded(t, mkHash(0x10), target)
		if err := s.Put(ctx, Record{TargetCommit: target, EncodedDelta: encoded, RecordedAt: time.Now().UTC()}); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	var seen []string
	if err := s.Range(ctx, func(r Record) bool {
		seen = append(seen, r.TargetCommit.String())
		return len(seen) < 2
	}); err != nil {
		t.Fatalf("range: %v", err)
	}
	if len(seen) != 2 {
		t.Fatalf("range did not stop at false: saw %v", seen)
	}
}

func TestErrNotFoundFormatsCommit(t *testing.T) {
	target := mkHash(0xCD)
	err := notFound(target)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("notFound result must wrap ErrNotFound: %v", err)
	}
	if !strings.Contains(err.Error(), target.String()) {
		t.Fatalf("notFound error should include the commit hash: %v", err)
	}
}

// TestFileStore_PutSurvivesPartialDirectory ensures the store creates its
// destination directory if it doesn't exist.
func TestFileStore_PutSurvivesPartialDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "deltas")
	s, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ctx := context.Background()
	target := mkHash(0x40)
	encoded := sampleEncoded(t, mkHash(0x10), target)
	if err := s.Put(ctx, Record{TargetCommit: target, EncodedDelta: encoded, RecordedAt: time.Now().UTC()}); err != nil {
		t.Fatalf("put: %v", err)
	}
}

// TestFileStore_SortableRange returns records in deterministic order so GC
// passes can reason about retention by recorded time without re-sorting.
func TestFileStore_SortableRange(t *testing.T) {
	dir := t.TempDir()
	s, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ctx := context.Background()
	for _, b := range []byte{0x40, 0x20, 0x60, 0x10} {
		target := mkHash(b)
		encoded := sampleEncoded(t, mkHash(0x10), target)
		if err := s.Put(ctx, Record{TargetCommit: target, EncodedDelta: encoded, RecordedAt: time.Now().UTC()}); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	var seen []string
	if err := s.Range(ctx, func(r Record) bool {
		seen = append(seen, r.TargetCommit.String())
		return true
	}); err != nil {
		t.Fatalf("range: %v", err)
	}
	sorted := append([]string(nil), seen...)
	sort.Strings(sorted)
	for i := range seen {
		if seen[i] != sorted[i] {
			t.Fatalf("range order non-deterministic: %v", seen)
		}
	}
}
