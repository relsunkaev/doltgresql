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
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dolthub/dolt/go/store/hash"
)

// fileStoreSuffix is the on-disk extension for delta records. Plain .json
// makes records inspectable and grep-able by an operator looking at a single
// commit's metadata.
const fileStoreSuffix = ".delta.json"

// fileRecord is the on-disk envelope. EncodedDelta is the canonical deltameta
// bytes produced by deltameta.Encode; we wrap it so the per-record
// RecordedAt timestamp lives alongside it durably.
type fileRecord struct {
	TargetCommit string          `json:"targetCommit"`
	RecordedAt   time.Time       `json:"recordedAt"`
	EncodedDelta json.RawMessage `json:"encodedDelta"`
}

// FileStore is a file-backed Store. Each record is one JSON file named after
// its target commit hex; writes are atomic via write-temp + rename + fsync.
//
// Trade-offs:
//   - Single-node only. Multi-node deployments need a different backing store
//     (likely a Dolt-core delta-table; tracked separately).
//   - O(N) Range over directory entries. Acceptable: at GC time the store
//     walks the full directory anyway.
type FileStore struct {
	dir string
	// mu serializes Put/Delete on the same key. Directory operations
	// (rename, sync) are atomic at the OS level, so the lock just keeps
	// concurrent writers from racing the same file.
	mu sync.Mutex
}

// NewFileStore opens (or creates) a file-backed store under dir. The directory
// is created with os.MkdirAll if it does not exist.
func NewFileStore(dir string) (*FileStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &FileStore{dir: dir}, nil
}

// pathFor returns the on-disk path for a target commit.
func (s *FileStore) pathFor(targetCommit hash.Hash) string {
	return filepath.Join(s.dir, targetCommit.String()+fileStoreSuffix)
}

// Put atomically writes the record. The encoded delta must be a valid
// deltameta payload; malformed bytes are rejected before any I/O.
func (s *FileStore) Put(_ context.Context, rec Record) error {
	if err := validateEncoded(rec.EncodedDelta); err != nil {
		return err
	}
	if rec.RecordedAt.IsZero() {
		rec.RecordedAt = time.Now().UTC()
	}
	envelope, err := json.Marshal(fileRecord{
		TargetCommit: rec.TargetCommit.String(),
		RecordedAt:   rec.RecordedAt.UTC(),
		EncodedDelta: rec.EncodedDelta,
	})
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	final := s.pathFor(rec.TargetCommit)
	tmp, err := os.CreateTemp(s.dir, "deltastore-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() {
		// Best-effort cleanup on any error path.
		if _, statErr := os.Stat(tmpName); statErr == nil {
			_ = os.Remove(tmpName)
		}
	}()
	if _, err := tmp.Write(envelope); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, final); err != nil {
		return err
	}
	// fsync the directory so the rename is durable.
	return fsyncDir(s.dir)
}

// Get reads the record off disk. Returns ErrNotFound for a missing target
// commit.
func (s *FileStore) Get(_ context.Context, targetCommit hash.Hash) (Record, error) {
	bytes, err := os.ReadFile(s.pathFor(targetCommit))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Record{}, notFound(targetCommit)
		}
		return Record{}, err
	}
	var env fileRecord
	if err := json.Unmarshal(bytes, &env); err != nil {
		return Record{}, err
	}
	return Record{
		TargetCommit: targetCommit,
		RecordedAt:   env.RecordedAt,
		EncodedDelta: []byte(env.EncodedDelta),
	}, nil
}

// Delete removes the record file. Returns ErrNotFound if no record exists.
func (s *FileStore) Delete(_ context.Context, targetCommit hash.Hash) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := os.Remove(s.pathFor(targetCommit))
	if errors.Is(err, os.ErrNotExist) {
		return notFound(targetCommit)
	}
	if err != nil {
		return err
	}
	return fsyncDir(s.dir)
}

// Range walks the directory in commit-hash-ascending order.
func (s *FileStore) Range(ctx context.Context, fn func(Record) bool) error {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return err
	}
	hashes := make([]hash.Hash, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(name, fileStoreSuffix) {
			continue
		}
		hashStr := strings.TrimSuffix(name, fileStoreSuffix)
		h, ok := hash.MaybeParse(hashStr)
		if !ok {
			continue
		}
		hashes = append(hashes, h)
	}
	sort.Slice(hashes, func(i, j int) bool { return hashes[i].Less(hashes[j]) })
	for _, h := range hashes {
		rec, err := s.Get(ctx, h)
		if errors.Is(err, ErrNotFound) {
			// Concurrently deleted; skip.
			continue
		}
		if err != nil {
			return err
		}
		if !fn(rec) {
			return nil
		}
	}
	return nil
}

// fsyncDir fsyncs the directory so file-creation/rename metadata is durable.
// On platforms where this is a no-op (Windows in some versions), the OS
// short-circuits the call.
func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		// Some filesystems return EINVAL for directory fsync. Treat as
		// non-fatal: the subsequent rename is still durable on the
		// majority of POSIX filesystems we care about.
		var pathErr *os.PathError
		if errors.As(err, &pathErr) && errors.Is(pathErr.Err, os.ErrInvalid) {
			return nil
		}
		// EINVAL on some FSes (e.g. tmpfs on certain configs) maps to a
		// raw syscall error rather than os.ErrInvalid. The common
		// upstream treatment is to ignore.
		if isInvalidDirSync(err) {
			return nil
		}
		return err
	}
	return nil
}
