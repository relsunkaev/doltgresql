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
	"encoding/binary"
	"testing"
	"time"

	"github.com/dolthub/dolt/go/store/hash"

	"github.com/dolthub/doltgresql/server/branchstorage/deltameta"
)

// makeBenchInputs builds an Inputs with `n` UPDATE rows describing disjoint
// scalar edits in one table. Each row's main side has a different scalar
// edit so the disjoint-scalar merge path runs end-to-end.
func makeBenchInputs(b *testing.B, n int) Inputs {
	b.Helper()
	base := mkHash(0x10)
	target := mkHash(0x20)
	rows := make([]deltameta.RowChange, n)
	baseSnap := make(map[string]RowSnapshot, n)
	mainSnap := make(map[string]RowSnapshot, n)
	targetSnap := make(map[string]RowSnapshot, n)
	for i := 0; i < n; i++ {
		pk := make([]byte, 8)
		binary.BigEndian.PutUint64(pk, uint64(i))
		oldH := hashFromInt(uint64(i)<<8 | 0x01)
		newH := hashFromInt(uint64(i)<<8 | 0x02)
		mainH := hashFromInt(uint64(i)<<8 | 0x03)
		rows[i] = deltameta.RowChange{
			PrimaryKey:     pk,
			OldRowHash:     ptrHashLocal(oldH),
			NewRowHash:     ptrHashLocal(newH),
			ChangedScalars: []string{"memo"}, // branch edits memo
		}
		baseSnap[string(pk)] = RowSnapshot{Hash: oldH, Cols: map[string]ColValue{
			"memo":   {Bytes: []byte("base-memo")},
			"amount": {Bytes: []byte("100")},
		}}
		// Main edits a different scalar (amount) so the row-side merge
		// must run rather than short-circuiting.
		mainSnap[string(pk)] = RowSnapshot{Hash: mainH, Cols: map[string]ColValue{
			"memo":   {Bytes: []byte("base-memo")},
			"amount": {Bytes: []byte("999")},
		}}
		targetSnap[string(pk)] = RowSnapshot{Hash: newH, Cols: map[string]ColValue{
			"memo":   {Bytes: []byte("branch-memo")},
			"amount": {Bytes: []byte("100")},
		}}
	}
	d := deltameta.Delta{
		Format:       deltameta.FormatVersion1,
		BaseRoot:     base,
		TargetRef:    "refs/heads/sync",
		TargetCommit: target,
		Tables: []deltameta.TableDelta{{
			Name: "public.journals",
			Rows: rows,
		}},
	}
	encoded, err := deltameta.Encode(d)
	if err != nil {
		b.Fatalf("encode: %v", err)
	}
	return Inputs{
		Delta:                d,
		EncodedDelta:         encoded,
		ExpectedBaseRoot:     base,
		ExpectedTargetCommit: target,
		Config:               DefaultConfig(),
		Snapshots: map[string]TableSnapshots{
			"public.journals": {Base: baseSnap, Main: mainSnap, Target: targetSnap},
		},
	}
}

func hashFromInt(v uint64) hash.Hash {
	var h hash.Hash
	binary.BigEndian.PutUint64(h[:8], v)
	for i := 8; i < len(h); i++ {
		h[i] = byte(i)
	}
	return h
}

func ptrHashLocal(h hash.Hash) *hash.Hash { return &h }

func BenchmarkDecide_1k_DisjointScalars(b *testing.B) {
	in := makeBenchInputs(b, 1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got := Decide(in)
		if !got.Result.Status.IsApplied() {
			b.Fatalf("status %s", got.Result.Status)
		}
	}
}

func BenchmarkDecide_10k_DisjointScalars(b *testing.B) {
	in := makeBenchInputs(b, 10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got := Decide(in)
		if !got.Result.Status.IsApplied() {
			b.Fatalf("status %s", got.Result.Status)
		}
	}
}

// TestDecide_10k_SubsecondTarget asserts the architecture's typical-case
// subsecond contract for the constrained-merge fast path at 10k rows. The
// budget is conservative (250ms) so a future regression that doubles
// per-row cost is caught.
//
// This test runs once (no benchmark loop) so CI sees a hard pass/fail
// rather than a noisy benchmark number. The 250ms budget is well under
// the architecture's "subsecond" target and leaves headroom for the
// merge driver to write the materialized root + indexes.
func TestDecide_10k_SubsecondTarget(t *testing.T) {
	in := makeBenchInputsT(t, 10000)
	start := time.Now()
	got := Decide(in)
	elapsed := time.Since(start)
	if !got.Result.Status.IsApplied() {
		t.Fatalf("decide must apply for the bench input, got %s (%+v)", got.Result.Status, got.Result.Context)
	}
	const budget = 250 * time.Millisecond
	if elapsed > budget {
		t.Fatalf("Decide on 10k rows took %v, budget %v", elapsed, budget)
	}
	t.Logf("Decide(10k) elapsed=%v", elapsed)
}

// makeBenchInputsT mirrors makeBenchInputs for a *testing.T (the assertion
// test reuses the bench builder by construction).
func makeBenchInputsT(t *testing.T, n int) Inputs {
	t.Helper()
	base := mkHash(0x10)
	target := mkHash(0x20)
	rows := make([]deltameta.RowChange, n)
	baseSnap := make(map[string]RowSnapshot, n)
	mainSnap := make(map[string]RowSnapshot, n)
	targetSnap := make(map[string]RowSnapshot, n)
	for i := 0; i < n; i++ {
		pk := make([]byte, 8)
		binary.BigEndian.PutUint64(pk, uint64(i))
		oldH := hashFromInt(uint64(i)<<8 | 0x01)
		newH := hashFromInt(uint64(i)<<8 | 0x02)
		mainH := hashFromInt(uint64(i)<<8 | 0x03)
		rows[i] = deltameta.RowChange{
			PrimaryKey:     pk,
			OldRowHash:     ptrHashLocal(oldH),
			NewRowHash:     ptrHashLocal(newH),
			ChangedScalars: []string{"memo"},
		}
		baseSnap[string(pk)] = RowSnapshot{Hash: oldH, Cols: map[string]ColValue{"memo": {Bytes: []byte("base-memo")}, "amount": {Bytes: []byte("100")}}}
		mainSnap[string(pk)] = RowSnapshot{Hash: mainH, Cols: map[string]ColValue{"memo": {Bytes: []byte("base-memo")}, "amount": {Bytes: []byte("999")}}}
		targetSnap[string(pk)] = RowSnapshot{Hash: newH, Cols: map[string]ColValue{"memo": {Bytes: []byte("branch-memo")}, "amount": {Bytes: []byte("100")}}}
	}
	d := deltameta.Delta{
		Format:       deltameta.FormatVersion1,
		BaseRoot:     base,
		TargetRef:    "refs/heads/sync",
		TargetCommit: target,
		Tables:       []deltameta.TableDelta{{Name: "public.journals", Rows: rows}},
	}
	encoded, err := deltameta.Encode(d)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return Inputs{
		Delta:                d,
		EncodedDelta:         encoded,
		ExpectedBaseRoot:     base,
		ExpectedTargetCommit: target,
		Config:               DefaultConfig(),
		Snapshots: map[string]TableSnapshots{
			"public.journals": {Base: baseSnap, Main: mainSnap, Target: targetSnap},
		},
	}
}
