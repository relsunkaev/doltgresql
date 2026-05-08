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

	"github.com/dolthub/doltgresql/server/branchstorage/deltameta"
)

// TestArchitectureCorrectnessCoverage maps the seven correctness cases listed
// in dg-93u.8 to specific test fixtures in this package and asserts the
// expected outcome for each. Cases 6 and 7 (crash recovery; fallback parity
// with full Dolt semantics) require Dolt-core integration and live in the
// integration suite — see the bd subissues filed under dg-93u.8.
func TestArchitectureCorrectnessCoverage(t *testing.T) {
	type expectation struct {
		name        string
		wantApplied bool
		wantStatus  Status
		setup       func() Inputs
	}

	cases := []expectation{
		{
			name:        "1: same-row different-column merge succeeds",
			wantApplied: true,
			setup: func() Inputs {
				in, _ := baseInputs()
				pk := []byte{0x01}
				main := in.Snapshots[tblJournals].Main[string(pk)]
				main.Hash = mkHash(0xA2)
				main.Cols["amount"] = ColValue{Bytes: []byte("200")}
				in.Snapshots[tblJournals].Main[string(pk)] = main
				return in
			},
		},
		{
			name:       "2: same-column conflict falls back",
			wantStatus: StatusDeclinedConflict,
			setup: func() Inputs {
				in, _ := baseInputs()
				pk := []byte{0x01}
				main := in.Snapshots[tblJournals].Main[string(pk)]
				main.Hash = mkHash(0xA2)
				main.Cols["memo"] = ColValue{Bytes: []byte("main-edit")}
				in.Snapshots[tblJournals].Main[string(pk)] = main
				return in
			},
		},
		{
			name:       "3: complex-column conflict falls back",
			wantStatus: StatusDeclinedUnsupportedColumn,
			setup: func() Inputs {
				in, _ := baseInputs()
				pk := []byte{0x01}
				in.Delta.Tables[0].Rows[0].TouchedComplex = []string{"doc"}
				main := in.Snapshots[tblJournals].Main[string(pk)]
				main.Hash = mkHash(0xA3)
				in.Snapshots[tblJournals].Main[string(pk)] = main
				enc, err := deltameta.Encode(in.Delta)
				if err != nil {
					t.Fatalf("encode: %v", err)
				}
				in.EncodedDelta = enc
				return in
			},
		},
		{
			name:       "4a: missing delta metadata falls back",
			wantStatus: StatusDeclinedMissingDeltaMetadata,
			setup: func() Inputs {
				in, _ := baseInputs()
				in.EncodedDelta = nil
				return in
			},
		},
		{
			name:       "4b: stale delta metadata falls back",
			wantStatus: StatusDeclinedMissingDeltaMetadata,
			setup: func() Inputs {
				in, _ := baseInputs()
				in.ExpectedBaseRoot = mkHash(0x77)
				return in
			},
		},
		{
			name:       "5: schema changes fall back",
			wantStatus: StatusDeclinedSchemaChange,
			setup: func() Inputs {
				in, _ := baseInputs()
				in.SchemaChanged = true
				in.SchemaChangeNote = "added public.journals.tag"
				return in
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := Decide(c.setup())
			if c.wantApplied {
				if !got.Result.Status.IsApplied() {
					t.Fatalf("want applied, got %s (%+v)", got.Result.Status, got.Result.Context)
				}
				return
			}
			if got.Result.Status != c.wantStatus {
				t.Fatalf("want %s, got %s (%+v)", c.wantStatus, got.Result.Status, got.Result.Context)
			}
			if !got.Result.Status.IsDecline() {
				t.Fatalf("expected decline status to report IsDecline")
			}
			if got.Plan != nil {
				t.Fatalf("declines must not produce a plan")
			}
		})
	}
}

// TestPlanIsCanonicalRegardlessOfInputOrder pins the determinism guarantee:
// shuffled rows in the same delta produce a plan that is equal to the plan
// from a canonically-ordered delta. This protects callers (and any future
// content-addressed merge log) from drift caused by producer ordering.
func TestPlanIsCanonicalRegardlessOfInputOrder(t *testing.T) {
	pkA := []byte{0x01}
	pkB := []byte{0x02}
	pkC := []byte{0x03}

	canonical := func() deltameta.Delta {
		return deltameta.Delta{
			Format:       deltameta.FormatVersion1,
			BaseRoot:     mkHash(0x10),
			TargetRef:    "refs/heads/sync",
			TargetCommit: mkHash(0x20),
			Tables: []deltameta.TableDelta{{
				Name: tblJournals,
				Rows: []deltameta.RowChange{
					{PrimaryKey: pkA, OldRowHash: ptrHash(mkHash(0xA0)), NewRowHash: ptrHash(mkHash(0xA1)), ChangedScalars: []string{"memo"}},
					{PrimaryKey: pkB, NewRowHash: ptrHash(mkHash(0xB1))},
					{PrimaryKey: pkC, OldRowHash: ptrHash(mkHash(0xD0))},
				},
			}},
		}
	}

	snaps := map[string]TableSnapshots{
		tblJournals: {
			Base: map[string]RowSnapshot{
				string(pkA): snap(0xA0, cols("memo", "old", "amount", "100")),
				string(pkC): snap(0xD0, cols("memo", "doomed")),
			},
			Main: map[string]RowSnapshot{
				string(pkA): snap(0xA0, cols("memo", "old", "amount", "100")),
				string(pkC): snap(0xD0, cols("memo", "doomed")),
			},
			Target: map[string]RowSnapshot{
				string(pkA): snap(0xA1, cols("memo", "new", "amount", "100")),
				string(pkB): snap(0xB1, cols("memo", "fresh")),
			},
		},
	}

	d1 := canonical()
	d2 := canonical()
	// Shuffle d2's rows.
	d2.Tables[0].Rows[0], d2.Tables[0].Rows[2] = d2.Tables[0].Rows[2], d2.Tables[0].Rows[0]

	enc1, err := deltameta.Encode(d1)
	if err != nil {
		t.Fatalf("encode 1: %v", err)
	}
	enc2, err := deltameta.Encode(d2)
	if err != nil {
		t.Fatalf("encode 2: %v", err)
	}
	in1 := Inputs{Delta: d1, EncodedDelta: enc1, ExpectedBaseRoot: mkHash(0x10), ExpectedTargetCommit: mkHash(0x20), Config: DefaultConfig(), Snapshots: snaps}
	in2 := Inputs{Delta: d2, EncodedDelta: enc2, ExpectedBaseRoot: mkHash(0x10), ExpectedTargetCommit: mkHash(0x20), Config: DefaultConfig(), Snapshots: snaps}

	r1 := Decide(in1)
	r2 := Decide(in2)
	if !reflect.DeepEqual(r1, r2) {
		t.Fatalf("plan differs between canonical and shuffled inputs:\n c=%+v\n s=%+v", r1, r2)
	}
}

// TestEveryDeclineCarriesContextForTriage ensures decline results never come
// back empty. Operators reading metrics or audit logs need at minimum a
// status; ideally they get a table or a primary key too. This guard catches
// future declines added without diagnostic context.
func TestEveryDeclineCarriesContextForTriage(t *testing.T) {
	scenarios := []struct {
		name  string
		setup func() Inputs
	}{
		{"missing delta", func() Inputs {
			in, _ := baseInputs()
			in.EncodedDelta = nil
			return in
		}},
		{"schema change", func() Inputs {
			in, _ := baseInputs()
			in.SchemaChanged = true
			return in
		}},
		{"batch too large", func() Inputs {
			in, _ := baseInputs()
			in.Config.RowChangeLimit = 0
			return in
		}},
		{"same-column conflict", func() Inputs {
			in, _ := baseInputs()
			pk := []byte{0x01}
			main := in.Snapshots[tblJournals].Main[string(pk)]
			main.Hash = mkHash(0xA2)
			main.Cols["memo"] = ColValue{Bytes: []byte("main-edit")}
			in.Snapshots[tblJournals].Main[string(pk)] = main
			return in
		}},
		{"unsupported column", func() Inputs {
			in, _ := baseInputs()
			pk := []byte{0x01}
			in.Delta.Tables[0].Rows[0].TouchedComplex = []string{"doc"}
			main := in.Snapshots[tblJournals].Main[string(pk)]
			main.Hash = mkHash(0xA3)
			in.Snapshots[tblJournals].Main[string(pk)] = main
			enc, err := deltameta.Encode(in.Delta)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			in.EncodedDelta = enc
			return in
		}},
	}
	for _, s := range scenarios {
		t.Run(s.name, func(t *testing.T) {
			got := Decide(s.setup())
			if !got.Result.Status.IsDecline() {
				t.Fatalf("expected decline, got %s", got.Result.Status)
			}
			ctx := got.Result.Context
			empty := len(ctx.Tables) == 0 &&
				ctx.RowCount == 0 &&
				ctx.Limit == 0 &&
				ctx.ConflictingTable == "" &&
				len(ctx.ConflictingPK) == 0 &&
				len(ctx.ConflictColumns) == 0 &&
				ctx.UnsupportedTable == "" &&
				len(ctx.UnsupportedPK) == 0 &&
				len(ctx.UnsupportedCols) == 0 &&
				ctx.SchemaChangeNote == "" &&
				ctx.Detail == ""
			if empty {
				t.Fatalf("decline %s carries no diagnostic context", got.Result.Status)
			}
		})
	}
}
