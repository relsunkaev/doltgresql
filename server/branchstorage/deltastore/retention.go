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
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/store/hash"
)

// Default retention windows. The diagnostic window is the upper bound on how
// long a delta record stays available for review/replay/audit-bound
// diagnostics; the fast-merge window is the shorter span during which the
// constrained-merge fast path expects to find the record live.
//
// These defaults are conservative for an interactive customer deployment: 30
// days of diagnostic retention and 7 days of fast-merge retention. Operators
// override per deployment via Policy.
const (
	DefaultDiagnosticWindow = 30 * 24 * time.Hour
	DefaultFastMergeWindow  = 7 * 24 * time.Hour
)

// Policy is the operator-tunable retention configuration. DiagnosticWindow is
// the larger window: a record older than DiagnosticWindow with no pin is
// eligible for GC. FastMergeWindow is informational at this layer (it tells
// the fast-path driver when to stop expecting the record to be live); it is
// captured here so config and code stay co-located.
//
// Both windows must be positive. Zero policies are rejected by RunGC so a
// misconfigured caller cannot silently delete every record.
type Policy struct {
	DiagnosticWindow time.Duration
	FastMergeWindow  time.Duration
}

// DefaultPolicy returns the architecture-default retention windows.
func DefaultPolicy() Policy {
	return Policy{
		DiagnosticWindow: DefaultDiagnosticWindow,
		FastMergeWindow:  DefaultFastMergeWindow,
	}
}

// PinSet tracks target-commit hashes the application requires GC to keep,
// regardless of age. Examples: visible ref tips, pending review branches,
// in-flight fallback merges, audit-locked commits. Population is the caller's
// responsibility — the delta store does not see Dolt's ref topology.
//
// Concurrency: the type is safe for concurrent calls.
type PinSet struct {
	mu    sync.RWMutex
	notes map[hash.Hash]string
}

// NewPinSet returns an empty pin set.
func NewPinSet() *PinSet {
	return &PinSet{notes: make(map[hash.Hash]string)}
}

// Pin records that targetCommit must be retained. note is recorded for
// diagnostics ("active review #42", "visible ref refs/heads/sync", etc.).
// Pinning the same hash twice keeps the latest note.
func (p *PinSet) Pin(targetCommit hash.Hash, note string) {
	p.mu.Lock()
	p.notes[targetCommit] = note
	p.mu.Unlock()
}

// Unpin removes the pin if any. Idempotent.
func (p *PinSet) Unpin(targetCommit hash.Hash) {
	p.mu.Lock()
	delete(p.notes, targetCommit)
	p.mu.Unlock()
}

// IsPinned reports whether targetCommit is in the pin set.
func (p *PinSet) IsPinned(targetCommit hash.Hash) bool {
	if p == nil {
		return false
	}
	p.mu.RLock()
	_, ok := p.notes[targetCommit]
	p.mu.RUnlock()
	return ok
}

// Note returns the diagnostic note attached to a pin, or empty if not pinned.
func (p *PinSet) Note(targetCommit hash.Hash) string {
	if p == nil {
		return ""
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.notes[targetCommit]
}

// GCOptions tunes a single GC pass. Now is normally time.Now().UTC() but
// tests inject a fixed clock. DryRun returns the report without performing
// any deletes.
type GCOptions struct {
	Policy Policy
	Now    time.Time
	Pins   *PinSet
	DryRun bool
}

// GCReport summarizes a GC pass. WouldRemove is populated for both real and
// dry-run passes; Removed is zero on dry runs.
type GCReport struct {
	Scanned     int
	Retained    int
	Removed     int
	WouldRemove int
}

// RunGC walks every record in the store and deletes records that are
// out-of-window AND not pinned. Records with a zero RecordedAt are retained
// (treated as "unknown, do not GC") so an upgrade that introduces RecordedAt
// after-the-fact does not cause data loss.
//
// Architecture invariants enforced here:
//
//   - "GC must not remove data needed to resolve visible refs, compute Dolt
//     diffs, reproduce audit commits, or complete fallback merge/review
//     flows": the caller pins those commits; RunGC never bypasses pins.
//   - "retain delta metadata while it can support diagnostics, review,
//     rollback, or replay for configured windows": records younger than
//     DiagnosticWindow are retained.
//   - Determinism: RunGC visits records in commit-hash-ascending order
//     (driven by Store.Range).
func RunGC(ctx context.Context, s Store, opts GCOptions) (GCReport, error) {
	if opts.Policy.DiagnosticWindow <= 0 || opts.Policy.FastMergeWindow <= 0 {
		return GCReport{}, errors.New("deltastore: gc policy must have positive windows")
	}
	if opts.Now.IsZero() {
		opts.Now = time.Now().UTC()
	}
	report := GCReport{}
	cutoff := opts.Now.Add(-opts.Policy.DiagnosticWindow)

	type doomed struct {
		commit hash.Hash
	}
	var toDelete []doomed

	err := s.Range(ctx, func(rec Record) bool {
		report.Scanned++
		if opts.Pins.IsPinned(rec.TargetCommit) {
			report.Retained++
			return true
		}
		if rec.RecordedAt.IsZero() {
			// Treat unknown-age records as live until an operator
			// re-stamps them. Better to retain than to silently
			// erase.
			report.Retained++
			return true
		}
		if rec.RecordedAt.After(cutoff) {
			report.Retained++
			return true
		}
		report.WouldRemove++
		toDelete = append(toDelete, doomed{commit: rec.TargetCommit})
		return true
	})
	if err != nil {
		return report, err
	}
	if opts.DryRun {
		return report, nil
	}
	for _, d := range toDelete {
		if err := s.Delete(ctx, d.commit); err != nil {
			return report, errors.Wrapf(err, "gc delete %s", d.commit)
		}
		report.Removed++
	}
	return report, nil
}
