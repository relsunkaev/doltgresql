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
	"testing"
	"time"

	"github.com/dolthub/dolt/go/store/hash"
)

func TestDefaultPolicy_PinsArchitectureWindows(t *testing.T) {
	p := DefaultPolicy()
	if p.DiagnosticWindow <= 0 {
		t.Fatalf("DiagnosticWindow must be positive: %v", p.DiagnosticWindow)
	}
	if p.FastMergeWindow <= 0 {
		t.Fatalf("FastMergeWindow must be positive: %v", p.FastMergeWindow)
	}
	// Diagnostic window is the upper bound (review/diagnostic spans more
	// than the active fast-merge window).
	if p.FastMergeWindow > p.DiagnosticWindow {
		t.Fatalf("FastMergeWindow (%v) must not exceed DiagnosticWindow (%v)", p.FastMergeWindow, p.DiagnosticWindow)
	}
}

// TestRunGC_RemovesOutOfWindowRecord verifies the basic policy: a record
// older than the diagnostic window with no pin is GC'd.
func TestRunGC_RemovesOutOfWindowRecord(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()
	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)

	old := mkHash(0xA1)
	if err := s.Put(ctx, Record{TargetCommit: old, EncodedDelta: sampleEncoded(t, mkHash(0x10), old), RecordedAt: now.Add(-100 * 24 * time.Hour)}); err != nil {
		t.Fatalf("put: %v", err)
	}
	report, err := RunGC(ctx, s, GCOptions{
		Policy: Policy{DiagnosticWindow: 30 * 24 * time.Hour, FastMergeWindow: 7 * 24 * time.Hour},
		Now:    now,
	})
	if err != nil {
		t.Fatalf("gc: %v", err)
	}
	if report.Scanned != 1 || report.Removed != 1 || report.Retained != 0 {
		t.Fatalf("report: %+v", report)
	}
	if _, err := s.Get(ctx, old); !errors.Is(err, ErrNotFound) {
		t.Fatalf("record should have been GC'd: %v", err)
	}
}

// TestRunGC_KeepsInWindowRecord verifies records within the diagnostic
// window are retained even without a pin.
func TestRunGC_KeepsInWindowRecord(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()
	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)

	target := mkHash(0xB1)
	if err := s.Put(ctx, Record{TargetCommit: target, EncodedDelta: sampleEncoded(t, mkHash(0x10), target), RecordedAt: now.Add(-3 * 24 * time.Hour)}); err != nil {
		t.Fatalf("put: %v", err)
	}
	report, err := RunGC(ctx, s, GCOptions{
		Policy: Policy{DiagnosticWindow: 30 * 24 * time.Hour, FastMergeWindow: 7 * 24 * time.Hour},
		Now:    now,
	})
	if err != nil {
		t.Fatalf("gc: %v", err)
	}
	if report.Removed != 0 {
		t.Fatalf("in-window record removed: %+v", report)
	}
	if _, err := s.Get(ctx, target); err != nil {
		t.Fatalf("get: %v", err)
	}
}

// TestRunGC_HonorsPinsForOutOfWindowRecord is the architecture's central
// safety guarantee: a record outside the window but still pinned (e.g. by
// an active review or visible ref) MUST NOT be GC'd.
func TestRunGC_HonorsPinsForOutOfWindowRecord(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()
	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)

	pinned := mkHash(0xA1)
	if err := s.Put(ctx, Record{TargetCommit: pinned, EncodedDelta: sampleEncoded(t, mkHash(0x10), pinned), RecordedAt: now.Add(-100 * 24 * time.Hour)}); err != nil {
		t.Fatalf("put: %v", err)
	}
	pins := NewPinSet()
	pins.Pin(pinned, "active review #42")

	report, err := RunGC(ctx, s, GCOptions{
		Policy: Policy{DiagnosticWindow: 30 * 24 * time.Hour, FastMergeWindow: 7 * 24 * time.Hour},
		Now:    now,
		Pins:   pins,
	})
	if err != nil {
		t.Fatalf("gc: %v", err)
	}
	if report.Removed != 0 {
		t.Fatalf("pinned record was GC'd: %+v", report)
	}
	if _, err := s.Get(ctx, pinned); err != nil {
		t.Fatalf("pinned record should still be present: %v", err)
	}
}

// TestRunGC_DryRunDoesNotMutate verifies the read-only mode operators use to
// preview GC effects before deleting anything.
func TestRunGC_DryRunDoesNotMutate(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()
	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	old := mkHash(0xA1)
	if err := s.Put(ctx, Record{TargetCommit: old, EncodedDelta: sampleEncoded(t, mkHash(0x10), old), RecordedAt: now.Add(-100 * 24 * time.Hour)}); err != nil {
		t.Fatalf("put: %v", err)
	}
	report, err := RunGC(ctx, s, GCOptions{
		Policy: Policy{DiagnosticWindow: 30 * 24 * time.Hour, FastMergeWindow: 7 * 24 * time.Hour},
		Now:    now,
		DryRun: true,
	})
	if err != nil {
		t.Fatalf("gc: %v", err)
	}
	if report.WouldRemove != 1 {
		t.Fatalf("dry-run report: %+v", report)
	}
	if _, err := s.Get(ctx, old); err != nil {
		t.Fatalf("dry-run mutated store: %v", err)
	}
}

// TestRunGC_RejectsZeroPolicy is a guard against a zero-value Policy
// silently deleting all records (zero windows would mean "everything is out
// of window").
func TestRunGC_RejectsZeroPolicy(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()
	old := mkHash(0xA1)
	if err := s.Put(ctx, Record{TargetCommit: old, EncodedDelta: sampleEncoded(t, mkHash(0x10), old)}); err != nil {
		t.Fatalf("put: %v", err)
	}
	_, err := RunGC(ctx, s, GCOptions{Policy: Policy{}, Now: time.Now()})
	if err == nil {
		t.Fatalf("expected zero policy to be rejected")
	}
}

// TestRunGC_RemovesMixed walks a mixed set: in-window, out-of-window, pinned
// out-of-window, and verifies GC removes only the unpinned out-of-window
// record. This is the architecture's "GC must not remove data needed by
// visible refs / pending review / fallback flows" coverage at the unit
// level.
func TestRunGC_AdversarialMixed(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()
	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)

	type rec struct {
		hash    hash.Hash
		age     time.Duration
		pinNote string
	}
	records := []rec{
		{mkHash(0x01), 1 * 24 * time.Hour, ""},                     // fresh
		{mkHash(0x02), 90 * 24 * time.Hour, ""},                    // out of window, no pin
		{mkHash(0x03), 90 * 24 * time.Hour, "active review"},       // out of window, pinned by review
		{mkHash(0x04), 90 * 24 * time.Hour, "visible ref"},         // out of window, pinned by ref
		{mkHash(0x05), 90 * 24 * time.Hour, "fallback flight"},     // pinned for in-flight fallback
		{mkHash(0x06), 6 * 24 * time.Hour, ""},                     // within fast-merge window
		{mkHash(0x07), 25 * 24 * time.Hour, ""},                    // within diagnostic, outside fast-merge
	}
	pins := NewPinSet()
	for _, r := range records {
		if err := s.Put(ctx, Record{
			TargetCommit: r.hash,
			EncodedDelta: sampleEncoded(t, mkHash(0x10), r.hash),
			RecordedAt:   now.Add(-r.age),
		}); err != nil {
			t.Fatalf("put %x: %v", r.hash[0], err)
		}
		if r.pinNote != "" {
			pins.Pin(r.hash, r.pinNote)
		}
	}

	report, err := RunGC(ctx, s, GCOptions{
		Policy: Policy{DiagnosticWindow: 30 * 24 * time.Hour, FastMergeWindow: 7 * 24 * time.Hour},
		Now:    now,
		Pins:   pins,
	})
	if err != nil {
		t.Fatalf("gc: %v", err)
	}
	if report.Removed != 1 {
		t.Fatalf("expected exactly one removal (the unpinned out-of-window record), report=%+v", report)
	}
	// The unpinned, out-of-window record (0x02) must be the one removed.
	if _, err := s.Get(ctx, mkHash(0x02)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("0x02 should be GC'd: %v", err)
	}
	// All others must remain.
	for _, r := range records {
		if r.hash == mkHash(0x02) {
			continue
		}
		if _, err := s.Get(ctx, r.hash); err != nil {
			t.Fatalf("record %x should remain (note=%q): %v", r.hash[0], r.pinNote, err)
		}
	}
}

func TestPinSet_PinUnpinIsPresent(t *testing.T) {
	p := NewPinSet()
	h := mkHash(0xCC)
	if p.IsPinned(h) {
		t.Fatalf("empty pin set claims pinned")
	}
	p.Pin(h, "review")
	if !p.IsPinned(h) {
		t.Fatalf("pinned not detected")
	}
	if note := p.Note(h); note != "review" {
		t.Fatalf("note round-trip: %q", note)
	}
	p.Unpin(h)
	if p.IsPinned(h) {
		t.Fatalf("unpinned still pinned")
	}
}

// TestRunGC_RangeFailureAborts ensures GC does not silently swallow errors
// from Range or Delete.
func TestRunGC_DeleteFailureSurfaces(t *testing.T) {
	s := &erroringStore{base: NewMemoryStore(), errOnDelete: true}
	ctx := context.Background()
	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	old := mkHash(0xA1)
	if err := s.base.Put(ctx, Record{TargetCommit: old, EncodedDelta: sampleEncoded(t, mkHash(0x10), old), RecordedAt: now.Add(-100 * 24 * time.Hour)}); err != nil {
		t.Fatalf("put: %v", err)
	}
	_, err := RunGC(ctx, s, GCOptions{
		Policy: Policy{DiagnosticWindow: 30 * 24 * time.Hour, FastMergeWindow: 7 * 24 * time.Hour},
		Now:    now,
	})
	if err == nil {
		t.Fatalf("expected GC to surface delete error")
	}
}

// TestRunGC_IgnoresZeroRecordedAt protects against records whose RecordedAt
// is missing: they should be retained until manually cleaned (treating zero
// time as "unknown, do not GC").
func TestRunGC_IgnoresZeroRecordedAt(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()
	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	target := mkHash(0xA1)
	rec := Record{TargetCommit: target, EncodedDelta: sampleEncoded(t, mkHash(0x10), target)}
	if err := s.Put(ctx, rec); err != nil {
		t.Fatalf("put: %v", err)
	}
	// MemoryStore.Put fills in RecordedAt, so we have to surgically clear
	// it to simulate an upgrade where older records came in without one.
	s.records[target] = Record{TargetCommit: target, EncodedDelta: rec.EncodedDelta, RecordedAt: time.Time{}}

	report, err := RunGC(ctx, s, GCOptions{
		Policy: Policy{DiagnosticWindow: 30 * 24 * time.Hour, FastMergeWindow: 7 * 24 * time.Hour},
		Now:    now,
	})
	if err != nil {
		t.Fatalf("gc: %v", err)
	}
	if report.Removed != 0 {
		t.Fatalf("zero-RecordedAt should be retained, report=%+v", report)
	}
}

// erroringStore is a Store decorator that fails on Delete. Used to test GC
// error surfacing.
type erroringStore struct {
	base        *MemoryStore
	errOnDelete bool
}

func (e *erroringStore) Put(ctx context.Context, rec Record) error {
	return e.base.Put(ctx, rec)
}
func (e *erroringStore) Get(ctx context.Context, c hash.Hash) (Record, error) {
	return e.base.Get(ctx, c)
}
func (e *erroringStore) Delete(ctx context.Context, c hash.Hash) error {
	if e.errOnDelete {
		return errors.New("synthetic delete failure")
	}
	return e.base.Delete(ctx, c)
}
func (e *erroringStore) Range(ctx context.Context, fn func(Record) bool) error {
	return e.base.Range(ctx, fn)
}
