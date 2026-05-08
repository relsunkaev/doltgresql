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
	"reflect"
	"testing"

	"github.com/dolthub/dolt/go/store/hash"

	"github.com/dolthub/doltgresql/server/branchstorage/deltameta"
)

const (
	tblJournals = "public.journals"
	tblLines    = "public.journal_lines"
)

func mkHash(seed byte) hash.Hash {
	var h hash.Hash
	for i := range h {
		h[i] = seed + byte(i)
	}
	return h
}

func ptrHash(h hash.Hash) *hash.Hash { return &h }

func cols(kv ...string) map[string]ColValue {
	if len(kv)%2 != 0 {
		panic("cols: expected even number of args")
	}
	out := make(map[string]ColValue, len(kv)/2)
	for i := 0; i < len(kv); i += 2 {
		out[kv[i]] = ColValue{Bytes: []byte(kv[i+1])}
	}
	return out
}

func snap(h byte, c map[string]ColValue) RowSnapshot {
	return RowSnapshot{Hash: mkHash(h), Cols: c}
}

// baseInputs builds an Inputs scaffold whose tests can override fields. It
// covers the simplest single-row UPDATE scenario with a passing delta; tests
// then mutate the bits that drive the decision under test.
func baseInputs() (Inputs, []byte) {
	pk := []byte{0x01}
	d := deltameta.Delta{
		Format:       deltameta.FormatVersion1,
		BaseRoot:     mkHash(0x10),
		TargetRef:    "refs/heads/sync",
		TargetCommit: mkHash(0x20),
		Tables: []deltameta.TableDelta{{
			Name: tblJournals,
			Rows: []deltameta.RowChange{{
				PrimaryKey:     pk,
				OldRowHash:     ptrHash(mkHash(0xA0)),
				NewRowHash:     ptrHash(mkHash(0xA1)),
				ChangedScalars: []string{"memo"},
			}},
		}},
	}
	encoded, err := deltameta.Encode(d)
	if err != nil {
		panic(err)
	}
	return Inputs{
		Delta:                d,
		EncodedDelta:         encoded,
		ExpectedBaseRoot:     mkHash(0x10),
		ExpectedTargetCommit: mkHash(0x20),
		RowChangeLimit:       10000,
		Snapshots: map[string]TableSnapshots{
			tblJournals: {
				Base:   map[string]RowSnapshot{string(pk): snap(0xA0, cols("memo", "old", "amount", "100"))},
				Main:   map[string]RowSnapshot{string(pk): snap(0xA0, cols("memo", "old", "amount", "100"))},
				Target: map[string]RowSnapshot{string(pk): snap(0xA1, cols("memo", "new", "amount", "100"))},
			},
		},
	}, encoded
}

// 1) Clean update: target updated memo, main unchanged. Fast path applies.
func TestCleanUpdate_Applies(t *testing.T) {
	in, _ := baseInputs()
	got := Decide(in)
	if !got.Result.Status.IsApplied() {
		t.Fatalf("want applied, got %s (%+v)", got.Result.Status, got.Result.Context)
	}
	if got.Plan == nil || len(got.Plan.Tables) != 1 {
		t.Fatalf("expected one-table plan, got %+v", got.Plan)
	}
	tp := got.Plan.Tables[0]
	if tp.Name != tblJournals {
		t.Fatalf("plan table: %s", tp.Name)
	}
	if len(tp.Inserts) != 0 || len(tp.Deletes) != 0 || len(tp.Updates) != 1 {
		t.Fatalf("expected 1 update, got %+v", tp)
	}
	upd := tp.Updates[0]
	if string(upd.PrimaryKey) != string([]byte{0x01}) {
		t.Fatalf("update pk: %v", upd.PrimaryKey)
	}
	if string(upd.NewRow["memo"].Bytes) != "new" {
		t.Fatalf("expected merged memo=new, got %q", upd.NewRow["memo"].Bytes)
	}
}

// 2) Same-column conflict: both main and branch edited memo. Fast path declines.
func TestSameColumnConflict_Declines(t *testing.T) {
	in, _ := baseInputs()
	pk := []byte{0x01}
	main := in.Snapshots[tblJournals].Main[string(pk)]
	main.Hash = mkHash(0xA2) // main has its own change
	main.Cols["memo"] = ColValue{Bytes: []byte("main-edit")}
	in.Snapshots[tblJournals].Main[string(pk)] = main
	got := Decide(in)
	if got.Result.Status != StatusDeclinedConflict {
		t.Fatalf("want declined_conflict, got %s", got.Result.Status)
	}
	if got.Result.Context.ConflictingTable != tblJournals {
		t.Fatalf("decline table: %s", got.Result.Context.ConflictingTable)
	}
	if !contains(got.Result.Context.ConflictColumns, "memo") {
		t.Fatalf("expected memo in conflict columns, got %v", got.Result.Context.ConflictColumns)
	}
	if got.Plan != nil {
		t.Fatalf("decline must not produce a plan")
	}
}

// 3) Disjoint-column merge: main edited amount, branch edited memo, same row.
// Fast path applies and produces a merged row.
func TestDisjointColumnMerge_Applies(t *testing.T) {
	in, _ := baseInputs()
	pk := []byte{0x01}
	main := in.Snapshots[tblJournals].Main[string(pk)]
	main.Hash = mkHash(0xA2)
	main.Cols["amount"] = ColValue{Bytes: []byte("200")}
	in.Snapshots[tblJournals].Main[string(pk)] = main

	got := Decide(in)
	if !got.Result.Status.IsApplied() {
		t.Fatalf("want applied, got %s (%+v)", got.Result.Status, got.Result.Context)
	}
	upd := got.Plan.Tables[0].Updates[0]
	if string(upd.NewRow["memo"].Bytes) != "new" {
		t.Fatalf("memo should be branch's edit, got %q", upd.NewRow["memo"].Bytes)
	}
	if string(upd.NewRow["amount"].Bytes) != "200" {
		t.Fatalf("amount should be main's edit, got %q", upd.NewRow["amount"].Bytes)
	}
}

// 4) Complex-column conflict: branch touched a json/blob column on a row main
// also changed. Fast path declines conservatively.
func TestComplexColumnConflict_Declines(t *testing.T) {
	in, _ := baseInputs()
	pk := []byte{0x01}
	in.Delta.Tables[0].Rows[0].TouchedComplex = []string{"doc"}
	// Main also changed the row (different hash) — both sides touched the
	// row that has a complex column on the branch side.
	main := in.Snapshots[tblJournals].Main[string(pk)]
	main.Hash = mkHash(0xA3)
	in.Snapshots[tblJournals].Main[string(pk)] = main
	// Re-encode to keep EncodedDelta consistent with Delta.
	encoded, err := deltameta.Encode(in.Delta)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	in.EncodedDelta = encoded

	got := Decide(in)
	if got.Result.Status != StatusDeclinedUnsupportedColumn {
		t.Fatalf("want declined_unsupported_column, got %s (%+v)", got.Result.Status, got.Result.Context)
	}
	if !contains(got.Result.Context.UnsupportedCols, "doc") {
		t.Fatalf("expected doc in unsupported cols, got %v", got.Result.Context.UnsupportedCols)
	}
}

// 4b) Complex-column on branch only (main unchanged) is still acceptable: no
// merge decision needed because main has no edit to combine with.
func TestComplexColumnBranchOnly_Applies(t *testing.T) {
	in, _ := baseInputs()
	in.Delta.Tables[0].Rows[0].TouchedComplex = []string{"doc"}
	encoded, err := deltameta.Encode(in.Delta)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	in.EncodedDelta = encoded
	got := Decide(in)
	if !got.Result.Status.IsApplied() {
		t.Fatalf("want applied, got %s (%+v)", got.Result.Status, got.Result.Context)
	}
}

// 5) Missing delta: fast path declines without inspecting rows.
func TestMissingDelta_Declines(t *testing.T) {
	in, _ := baseInputs()
	in.EncodedDelta = nil
	got := Decide(in)
	if got.Result.Status != StatusDeclinedMissingDeltaMetadata {
		t.Fatalf("want declined_missing_delta_metadata, got %s", got.Result.Status)
	}
}

// 5b) Stale delta: encoded payload is bound to a base root that does not
// match what the merge driver expects. Decline.
func TestStaleDelta_BaseRootMismatch_Declines(t *testing.T) {
	in, _ := baseInputs()
	in.ExpectedBaseRoot = mkHash(0x77) // changed from 0x10
	got := Decide(in)
	if got.Result.Status != StatusDeclinedMissingDeltaMetadata {
		t.Fatalf("want declined_missing_delta_metadata, got %s", got.Result.Status)
	}
}

// 5c) Stale delta: declared old row hash does not match observed base hash.
func TestStaleDelta_BaseRowHashMismatch_Declines(t *testing.T) {
	in, _ := baseInputs()
	pk := []byte{0x01}
	base := in.Snapshots[tblJournals].Base[string(pk)]
	base.Hash = mkHash(0xCC) // different from delta's recorded old hash
	in.Snapshots[tblJournals].Base[string(pk)] = base
	// Main matches base for cleanness of test.
	main := in.Snapshots[tblJournals].Main[string(pk)]
	main.Hash = base.Hash
	in.Snapshots[tblJournals].Main[string(pk)] = main

	got := Decide(in)
	if got.Result.Status != StatusDeclinedMissingDeltaMetadata {
		t.Fatalf("want declined_missing_delta_metadata, got %s", got.Result.Status)
	}
}

// 5d) Malformed delta payload: decline.
func TestMalformedDelta_Declines(t *testing.T) {
	in, _ := baseInputs()
	in.EncodedDelta = []byte("not-json")
	got := Decide(in)
	if got.Result.Status != StatusDeclinedMissingDeltaMetadata {
		t.Fatalf("want declined_missing_delta_metadata, got %s", got.Result.Status)
	}
}

// 6) Schema change: fast path declines.
func TestSchemaChange_Declines(t *testing.T) {
	in, _ := baseInputs()
	in.SchemaChanged = true
	in.SchemaChangeNote = "added column public.journals.tag"
	got := Decide(in)
	if got.Result.Status != StatusDeclinedSchemaChange {
		t.Fatalf("want declined_schema_change, got %s", got.Result.Status)
	}
	if got.Result.Context.SchemaChangeNote == "" {
		t.Fatalf("expected schema change note in context")
	}
}

// 7) Batch too large: fast path declines.
func TestBatchTooLarge_Declines(t *testing.T) {
	in, _ := baseInputs()
	in.RowChangeLimit = 0 // any non-zero delta exceeds
	got := Decide(in)
	if got.Result.Status != StatusDeclinedBatchTooLarge {
		t.Fatalf("want declined_batch_too_large, got %s", got.Result.Status)
	}
	if got.Result.Context.RowCount != 1 || got.Result.Context.Limit != 0 {
		t.Fatalf("decline context: %+v", got.Result.Context)
	}
}

// 8) Clean insert: target inserted a new row, main does not have it. Apply.
func TestCleanInsert_Applies(t *testing.T) {
	pk := []byte{0x02}
	in := Inputs{
		Delta: deltameta.Delta{
			Format:       deltameta.FormatVersion1,
			BaseRoot:     mkHash(0x10),
			TargetRef:    "refs/heads/sync",
			TargetCommit: mkHash(0x20),
			Tables: []deltameta.TableDelta{{
				Name: tblJournals,
				Rows: []deltameta.RowChange{{
					PrimaryKey: pk,
					NewRowHash: ptrHash(mkHash(0xB1)),
				}},
			}},
		},
		ExpectedBaseRoot:     mkHash(0x10),
		ExpectedTargetCommit: mkHash(0x20),
		RowChangeLimit:       10000,
		Snapshots: map[string]TableSnapshots{
			tblJournals: {
				Base:   map[string]RowSnapshot{},
				Main:   map[string]RowSnapshot{},
				Target: map[string]RowSnapshot{string(pk): snap(0xB1, cols("memo", "fresh"))},
			},
		},
	}
	enc, err := deltameta.Encode(in.Delta)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	in.EncodedDelta = enc

	got := Decide(in)
	if !got.Result.Status.IsApplied() {
		t.Fatalf("want applied, got %s (%+v)", got.Result.Status, got.Result.Context)
	}
	if len(got.Plan.Tables[0].Inserts) != 1 {
		t.Fatalf("expected 1 insert, got %+v", got.Plan.Tables[0])
	}
}

// 8b) Insert/insert collision: main already has the row at this PK. Decline.
func TestInsertCollision_Declines(t *testing.T) {
	pk := []byte{0x02}
	in, _ := baseInputs()
	in.Delta.Tables[0].Rows = []deltameta.RowChange{{
		PrimaryKey: pk,
		NewRowHash: ptrHash(mkHash(0xB1)),
	}}
	enc, err := deltameta.Encode(in.Delta)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	in.EncodedDelta = enc

	in.Snapshots[tblJournals] = TableSnapshots{
		Base:   map[string]RowSnapshot{},
		Main:   map[string]RowSnapshot{string(pk): snap(0xC1, cols("memo", "main-inserted"))},
		Target: map[string]RowSnapshot{string(pk): snap(0xB1, cols("memo", "branch-inserted"))},
	}

	got := Decide(in)
	if got.Result.Status != StatusDeclinedConflict {
		t.Fatalf("want declined_conflict, got %s", got.Result.Status)
	}
}

// 9) Clean delete: branch deleted, main unchanged.
func TestCleanDelete_Applies(t *testing.T) {
	pk := []byte{0x03}
	in, _ := baseInputs()
	in.Delta.Tables[0].Rows = []deltameta.RowChange{{
		PrimaryKey: pk,
		OldRowHash: ptrHash(mkHash(0xD0)),
	}}
	enc, err := deltameta.Encode(in.Delta)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	in.EncodedDelta = enc

	in.Snapshots[tblJournals] = TableSnapshots{
		Base:   map[string]RowSnapshot{string(pk): snap(0xD0, cols("memo", "doomed"))},
		Main:   map[string]RowSnapshot{string(pk): snap(0xD0, cols("memo", "doomed"))},
		Target: map[string]RowSnapshot{},
	}

	got := Decide(in)
	if !got.Result.Status.IsApplied() {
		t.Fatalf("want applied, got %s (%+v)", got.Result.Status, got.Result.Context)
	}
	if len(got.Plan.Tables[0].Deletes) != 1 || string(got.Plan.Tables[0].Deletes[0]) != string(pk) {
		t.Fatalf("expected 1 delete of pk %v, got %+v", pk, got.Plan.Tables[0])
	}
}

// 9b) Delete/update conflict: branch deleted, main updated. Decline.
func TestDeleteUpdateConflict_Declines(t *testing.T) {
	pk := []byte{0x03}
	in, _ := baseInputs()
	in.Delta.Tables[0].Rows = []deltameta.RowChange{{
		PrimaryKey: pk,
		OldRowHash: ptrHash(mkHash(0xD0)),
	}}
	enc, err := deltameta.Encode(in.Delta)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	in.EncodedDelta = enc

	in.Snapshots[tblJournals] = TableSnapshots{
		Base:   map[string]RowSnapshot{string(pk): snap(0xD0, cols("memo", "doomed"))},
		Main:   map[string]RowSnapshot{string(pk): snap(0xD2, cols("memo", "main-saved"))},
		Target: map[string]RowSnapshot{},
	}

	got := Decide(in)
	if got.Result.Status != StatusDeclinedConflict {
		t.Fatalf("want declined_conflict, got %s", got.Result.Status)
	}
}

// 9c) Delete on a row main also already deleted: still applies cleanly.
func TestDeleteAlreadyDeleted_Applies(t *testing.T) {
	pk := []byte{0x03}
	in, _ := baseInputs()
	in.Delta.Tables[0].Rows = []deltameta.RowChange{{
		PrimaryKey: pk,
		OldRowHash: ptrHash(mkHash(0xD0)),
	}}
	enc, err := deltameta.Encode(in.Delta)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	in.EncodedDelta = enc

	in.Snapshots[tblJournals] = TableSnapshots{
		Base:   map[string]RowSnapshot{string(pk): snap(0xD0, cols("memo", "doomed"))},
		Main:   map[string]RowSnapshot{},
		Target: map[string]RowSnapshot{},
	}

	got := Decide(in)
	if !got.Result.Status.IsApplied() {
		t.Fatalf("want applied (delete idempotent), got %s", got.Result.Status)
	}
	// No-op delete: nothing scheduled.
	if len(got.Plan.Tables[0].Deletes) != 0 {
		t.Fatalf("expected no scheduled deletes when row already gone, got %+v", got.Plan.Tables[0].Deletes)
	}
}

// 10) Determinism: same Inputs run twice produces equal Decisions.
func TestDeterminism_SameInputsSameDecision(t *testing.T) {
	in1, _ := baseInputs()
	in2, _ := baseInputs()
	d1 := Decide(in1)
	d2 := Decide(in2)
	if !reflect.DeepEqual(d1, d2) {
		t.Fatalf("non-deterministic decision:\n a=%+v\n b=%+v", d1, d2)
	}
}

// 11) Multi-row merge: mix of insert, update, delete in one delta.
func TestMultiRowMix_Applies(t *testing.T) {
	pkUpd := []byte{0x01}
	pkIns := []byte{0x02}
	pkDel := []byte{0x03}
	d := deltameta.Delta{
		Format:       deltameta.FormatVersion1,
		BaseRoot:     mkHash(0x10),
		TargetRef:    "refs/heads/sync",
		TargetCommit: mkHash(0x20),
		Tables: []deltameta.TableDelta{{
			Name: tblJournals,
			Rows: []deltameta.RowChange{
				{
					PrimaryKey:     pkUpd,
					OldRowHash:     ptrHash(mkHash(0xA0)),
					NewRowHash:     ptrHash(mkHash(0xA1)),
					ChangedScalars: []string{"memo"},
				},
				{
					PrimaryKey: pkIns,
					NewRowHash: ptrHash(mkHash(0xB1)),
				},
				{
					PrimaryKey: pkDel,
					OldRowHash: ptrHash(mkHash(0xD0)),
				},
			},
		}},
	}
	enc, err := deltameta.Encode(d)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	in := Inputs{
		Delta:                d,
		EncodedDelta:         enc,
		ExpectedBaseRoot:     mkHash(0x10),
		ExpectedTargetCommit: mkHash(0x20),
		RowChangeLimit:       10000,
		Snapshots: map[string]TableSnapshots{
			tblJournals: {
				Base: map[string]RowSnapshot{
					string(pkUpd): snap(0xA0, cols("memo", "old", "amount", "100")),
					string(pkDel): snap(0xD0, cols("memo", "doomed")),
				},
				Main: map[string]RowSnapshot{
					string(pkUpd): snap(0xA0, cols("memo", "old", "amount", "100")),
					string(pkDel): snap(0xD0, cols("memo", "doomed")),
				},
				Target: map[string]RowSnapshot{
					string(pkUpd): snap(0xA1, cols("memo", "new", "amount", "100")),
					string(pkIns): snap(0xB1, cols("memo", "fresh")),
				},
			},
		},
	}
	got := Decide(in)
	if !got.Result.Status.IsApplied() {
		t.Fatalf("want applied, got %s (%+v)", got.Result.Status, got.Result.Context)
	}
	tp := got.Plan.Tables[0]
	if len(tp.Inserts) != 1 || len(tp.Updates) != 1 || len(tp.Deletes) != 1 {
		t.Fatalf("expected 1/1/1 ins/upd/del, got %+v", tp)
	}
}

// 12) Multi-table affected list shows up in Tables context on decline.
func TestDeclineContextLists_Tables(t *testing.T) {
	in, _ := baseInputs()
	in.SchemaChanged = true
	in.SchemaChangeNote = "added column"
	got := Decide(in)
	if got.Result.Status != StatusDeclinedSchemaChange {
		t.Fatalf("want declined_schema_change, got %s", got.Result.Status)
	}
	if !contains(got.Result.Context.Tables, tblJournals) {
		t.Fatalf("expected affected tables in context, got %v", got.Result.Context.Tables)
	}
}

// 13) Update on a row main side has deleted is a delete/update conflict from
// the branch's update perspective. Decline.
func TestUpdateAgainstMainDeleted_Declines(t *testing.T) {
	in, _ := baseInputs()
	pk := []byte{0x01}
	delete(in.Snapshots[tblJournals].Main, string(pk)) // main deleted the row
	got := Decide(in)
	if got.Result.Status != StatusDeclinedConflict {
		t.Fatalf("want declined_conflict, got %s", got.Result.Status)
	}
}

// 14) Update where main is unchanged from base produces a clean apply (no
// per-column merge needed — write the target row).
func TestUpdate_MainUnchanged_AppliesAsClean(t *testing.T) {
	in, _ := baseInputs() // main hash already matches base
	got := Decide(in)
	if !got.Result.Status.IsApplied() {
		t.Fatalf("want applied")
	}
	upd := got.Plan.Tables[0].Updates[0]
	if string(upd.NewRow["memo"].Bytes) != "new" {
		t.Fatalf("expected memo=new, got %q", upd.NewRow["memo"].Bytes)
	}
	if string(upd.NewRow["amount"].Bytes) != "100" {
		t.Fatalf("expected amount unchanged, got %q", upd.NewRow["amount"].Bytes)
	}
}

func contains(ss []string, want string) bool {
	for _, s := range ss {
		if s == want {
			return true
		}
	}
	return false
}
