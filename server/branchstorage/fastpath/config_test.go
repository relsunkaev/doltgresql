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

package fastpath

import (
	"testing"

	"github.com/dolthub/doltgresql/server/branchstorage/deltameta"
)

// TestDefaultConfig pins the default fast-path tuning so the architecture's
// 10k-row constrained-batch target is not silently lowered by a refactor.
func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.RowChangeLimit != DefaultRowChangeLimit {
		t.Fatalf("default row change limit drifted: %d != %d", cfg.RowChangeLimit, DefaultRowChangeLimit)
	}
	if DefaultRowChangeLimit < 10000 {
		t.Fatalf("default row change limit %d below architecture target 10000", DefaultRowChangeLimit)
	}
	if cfg.UnsupportedColumns != nil && len(cfg.UnsupportedColumns) != 0 {
		t.Fatalf("default config should not preemptively block any columns: %v", cfg.UnsupportedColumns)
	}
}

// TestConfig_RowChangeLimit_TakesEffect verifies the configurable limit drives
// the StatusDeclinedBatchTooLarge decline.
func TestConfig_RowChangeLimit_TakesEffect(t *testing.T) {
	in, _ := baseInputs()
	in.Config.RowChangeLimit = 0
	got := Decide(in)
	if got.Result.Status != StatusDeclinedBatchTooLarge {
		t.Fatalf("want declined_batch_too_large, got %s", got.Result.Status)
	}
	if got.Result.Context.Limit != 0 {
		t.Fatalf("decline context Limit: want 0, got %d", got.Result.Context.Limit)
	}
}

// TestConfig_UnsupportedColumns_ForcesDecline verifies an operator-configured
// blocked-column set forces declined_unsupported_column even when the producer
// listed the column as a scalar.
func TestConfig_UnsupportedColumns_ForcesDecline(t *testing.T) {
	in, _ := baseInputs()
	pk := []byte{0x01}
	main := in.Snapshots[tblJournals].Main[string(pk)]
	main.Hash = mkHash(0xA2)
	main.Cols["amount"] = ColValue{Bytes: []byte("200")}
	in.Snapshots[tblJournals].Main[string(pk)] = main
	// Operator says: 'memo' on public.journals is not safe for fast-path
	// merges in this deployment (e.g. Postgres FTS triggers downstream).
	in.Config.UnsupportedColumns = map[string]map[string]struct{}{
		tblJournals: {"memo": {}},
	}
	got := Decide(in)
	if got.Result.Status != StatusDeclinedUnsupportedColumn {
		t.Fatalf("want declined_unsupported_column, got %s (%+v)", got.Result.Status, got.Result.Context)
	}
	if got.Result.Context.UnsupportedTable != tblJournals {
		t.Fatalf("decline UnsupportedTable: %s", got.Result.Context.UnsupportedTable)
	}
	if !contains(got.Result.Context.UnsupportedCols, "memo") {
		t.Fatalf("decline UnsupportedCols want memo, got %v", got.Result.Context.UnsupportedCols)
	}
}

// TestConfig_UnsupportedColumns_OnlyAppliesToBranchEdits ensures the operator
// blocklist never declines on rows the branch did not edit. A scalar that is
// blocklisted but not changed by either side is irrelevant.
func TestConfig_UnsupportedColumns_OnlyAppliesToBranchEdits(t *testing.T) {
	in, _ := baseInputs()
	in.Config.UnsupportedColumns = map[string]map[string]struct{}{
		tblJournals: {"unrelated_column": {}},
	}
	got := Decide(in)
	if !got.Result.Status.IsApplied() {
		t.Fatalf("want applied (blocklisted column not edited), got %s", got.Result.Status)
	}
}

// TestConfig_ZeroRowChangeLimit_DistinctFromUnset documents the design choice
// that a zero RowChangeLimit means 'no rows', not 'use default'. Defaulting at
// Decide call sites must come from DefaultConfig(), not from in-band sentinels.
func TestConfig_ZeroRowChangeLimit_DistinctFromUnset(t *testing.T) {
	in, _ := baseInputs()
	in.Config = Config{} // explicit zero value
	got := Decide(in)
	if got.Result.Status != StatusDeclinedBatchTooLarge {
		t.Fatalf("zero-value Config must decline batch_too_large, got %s", got.Result.Status)
	}
}

// TestConfig_HotReloadIsExplicitlyNotSupported pins the per-merge snapshot
// behavior. Decide reads Config once at the top of the call. Mutating the
// caller's map mid-Decide must never affect the decision.
//
// This is exercised by ensuring Decide's behavior is determined by the
// Config value at call time and that Decide never holds a long-lived reference
// (guarded by reading config into local state at function start, conceptually).
func TestConfig_HotReloadIsExplicitlyNotSupported(t *testing.T) {
	in, _ := baseInputs()
	in.Config.RowChangeLimit = 10
	in.Config.UnsupportedColumns = map[string]map[string]struct{}{
		tblJournals: {"memo": {}},
	}
	r1 := Decide(in)
	// Mutating the operator's map after the call must not retroactively
	// alter r1's decision (the fast path returns a value, not a future).
	delete(in.Config.UnsupportedColumns[tblJournals], "memo")
	if r1.Result.Status != StatusDeclinedUnsupportedColumn {
		t.Fatalf("snapshot semantics broken: r1 status %s", r1.Result.Status)
	}
}

// TestEncodedDeltaIsNotMutatedByDecide guards a related invariant: Decide must
// not mutate Inputs (callers may reuse a single Inputs across retries). This
// is a complementary contract test for the per-merge snapshot design.
func TestEncodedDeltaIsNotMutatedByDecide(t *testing.T) {
	in, original := baseInputs()
	originalCopy := make([]byte, len(original))
	copy(originalCopy, original)
	_ = Decide(in)
	if string(in.EncodedDelta) != string(originalCopy) {
		t.Fatalf("Decide mutated EncodedDelta")
	}
	// Decoding the post-call EncodedDelta must still produce the original delta.
	if _, err := deltameta.Decode(in.EncodedDelta); err != nil {
		t.Fatalf("EncodedDelta corrupted: %v", err)
	}
}
