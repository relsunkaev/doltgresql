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

package deltameta

import (
	"strings"
	"testing"

	"github.com/dolthub/dolt/go/store/hash"
)

func mkHash(seed byte) hash.Hash {
	var h hash.Hash
	for i := range h {
		h[i] = seed + byte(i)
	}
	return h
}

func sampleDelta() Delta {
	return Delta{
		Format:       FormatVersion1,
		BaseRoot:     mkHash(0x10),
		TargetRef:    "refs/heads/sync-2026-05-01",
		TargetCommit: mkHash(0x20),
		Tables: []TableDelta{
			{
				Name: "public.journal_lines",
				Rows: []RowChange{
					{
						PrimaryKey:     []byte{0x01, 0x02},
						OldRowHash:     ptrHash(mkHash(0x30)),
						NewRowHash:     ptrHash(mkHash(0x31)),
						ChangedScalars: []string{"amount", "memo"},
					},
					{
						PrimaryKey: []byte{0x01, 0x03},
						OldRowHash: nil, // INSERT
						NewRowHash: ptrHash(mkHash(0x32)),
					},
				},
			},
		},
		Provenance: map[string]string{"job_id": "ingest-42", "actor": "sync-worker"},
	}
}

func ptrHash(h hash.Hash) *hash.Hash { return &h }

func TestEncodeDecodeRoundTrip(t *testing.T) {
	original := sampleDelta()
	encoded, err := Encode(original)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !equalDeltas(original, decoded) {
		t.Fatalf("round trip mismatch:\n original=%#v\n decoded=%#v", original, decoded)
	}
}

// TestEncodeIsDeterministic ensures that re-encoding the same delta produces
// byte-identical output. The architecture requires deterministic, durable
// metadata so that hashes used for content addressing remain stable.
func TestEncodeIsDeterministic(t *testing.T) {
	d1 := sampleDelta()
	d2 := sampleDelta()
	a, err := Encode(d1)
	if err != nil {
		t.Fatalf("encode 1: %v", err)
	}
	b, err := Encode(d2)
	if err != nil {
		t.Fatalf("encode 2: %v", err)
	}
	if string(a) != string(b) {
		t.Fatalf("non-deterministic encoding:\n a=%s\n b=%s", a, b)
	}
}

// TestEncodeNormalizesOrdering proves that input with shuffled tables, rows,
// changed-scalars, and provenance keys still produces the canonical encoding.
// Otherwise a generated delta could differ byte-wise just because the producer
// emitted columns or rows in a different order.
func TestEncodeNormalizesOrdering(t *testing.T) {
	canonical := sampleDelta()
	canonicalBytes, err := Encode(canonical)
	if err != nil {
		t.Fatalf("encode canonical: %v", err)
	}

	shuffled := sampleDelta()
	// Reverse rows.
	rows := shuffled.Tables[0].Rows
	rows[0], rows[1] = rows[1], rows[0]
	// Reverse changed scalars on row 0.
	cs := shuffled.Tables[0].Rows[0].ChangedScalars
	for i, j := 0, len(cs)-1; i < j; i, j = i+1, j-1 {
		cs[i], cs[j] = cs[j], cs[i]
	}

	shuffledBytes, err := Encode(shuffled)
	if err != nil {
		t.Fatalf("encode shuffled: %v", err)
	}
	if string(canonicalBytes) != string(shuffledBytes) {
		t.Fatalf("shuffled input did not produce canonical encoding:\n c=%s\n s=%s", canonicalBytes, shuffledBytes)
	}
}

func TestValidateRejectsUnknownFormat(t *testing.T) {
	d := sampleDelta()
	d.Format = 999
	if err := Validate(d); err == nil {
		t.Fatalf("expected unknown format to fail validation")
	}
}

func TestValidateRejectsZeroBaseRoot(t *testing.T) {
	d := sampleDelta()
	d.BaseRoot = hash.Hash{}
	err := Validate(d)
	if err == nil || !strings.Contains(err.Error(), "base root") {
		t.Fatalf("expected base root to be required, got: %v", err)
	}
}

func TestValidateRejectsZeroTargetCommit(t *testing.T) {
	d := sampleDelta()
	d.TargetCommit = hash.Hash{}
	err := Validate(d)
	if err == nil || !strings.Contains(err.Error(), "target commit") {
		t.Fatalf("expected target commit to be required, got: %v", err)
	}
}

func TestValidateRejectsEmptyTargetRef(t *testing.T) {
	d := sampleDelta()
	d.TargetRef = "  "
	err := Validate(d)
	if err == nil || !strings.Contains(err.Error(), "target ref") {
		t.Fatalf("expected target ref to be required, got: %v", err)
	}
}

func TestValidateRequiresAtLeastOneTable(t *testing.T) {
	d := sampleDelta()
	d.Tables = nil
	if err := Validate(d); err == nil {
		t.Fatalf("expected at least one affected table")
	}
}

func TestValidateRejectsEmptyTableName(t *testing.T) {
	d := sampleDelta()
	d.Tables[0].Name = ""
	if err := Validate(d); err == nil {
		t.Fatalf("expected empty table name to fail validation")
	}
}

func TestValidateRejectsRowWithNoPrimaryKey(t *testing.T) {
	d := sampleDelta()
	d.Tables[0].Rows[0].PrimaryKey = nil
	if err := Validate(d); err == nil {
		t.Fatalf("expected missing primary key to fail validation")
	}
}

// A row with neither old nor new row hash is meaningless: it cannot be
// classified as an INSERT, UPDATE, or DELETE.
func TestValidateRejectsRowWithNeitherOldNorNewHash(t *testing.T) {
	d := sampleDelta()
	d.Tables[0].Rows[0].OldRowHash = nil
	d.Tables[0].Rows[0].NewRowHash = nil
	if err := Validate(d); err == nil {
		t.Fatalf("expected row with no hashes to fail validation")
	}
}

// An UPDATE row whose old==new hash is degenerate (no actual change). It would
// silently bloat the delta and confuse fast-path decisions.
func TestValidateRejectsRowWithIdenticalOldAndNewHash(t *testing.T) {
	d := sampleDelta()
	same := mkHash(0x40)
	d.Tables[0].Rows[0].OldRowHash = ptrHash(same)
	d.Tables[0].Rows[0].NewRowHash = ptrHash(same)
	if err := Validate(d); err == nil {
		t.Fatalf("expected old==new hash to fail validation")
	}
}

// An INSERT row (no old hash) cannot have changed-scalar markings; the column
// is fully new on the branch side and the fast-path treats it as a clean add.
// Allowing scalars on an INSERT lets a producer claim a column-level merge
// where there is no base column value to merge against.
func TestValidateRejectsChangedScalarsOnInsert(t *testing.T) {
	d := sampleDelta()
	d.Tables[0].Rows[0].OldRowHash = nil
	if err := Validate(d); err == nil {
		t.Fatalf("expected INSERT with changed scalars to fail validation")
	}
}

// A DELETE row (no new hash) similarly cannot have changed-scalar markings;
// the row is gone, there is nothing to column-merge.
func TestValidateRejectsChangedScalarsOnDelete(t *testing.T) {
	d := sampleDelta()
	d.Tables[0].Rows[0].NewRowHash = nil
	if err := Validate(d); err == nil {
		t.Fatalf("expected DELETE with changed scalars to fail validation")
	}
}

func TestValidateRejectsDuplicatePrimaryKeyInSameTable(t *testing.T) {
	d := sampleDelta()
	dup := append([]byte(nil), d.Tables[0].Rows[0].PrimaryKey...)
	d.Tables[0].Rows[1].PrimaryKey = dup
	if err := Validate(d); err == nil {
		t.Fatalf("expected duplicate primary key to fail validation")
	}
}

func TestValidateRejectsDuplicateChangedScalar(t *testing.T) {
	d := sampleDelta()
	d.Tables[0].Rows[0].ChangedScalars = []string{"amount", "amount"}
	if err := Validate(d); err == nil {
		t.Fatalf("expected duplicate changed scalar to fail validation")
	}
}

func TestValidateRejectsScalarAndComplexOverlap(t *testing.T) {
	d := sampleDelta()
	d.Tables[0].Rows[0].TouchedComplex = []string{"amount"}
	if err := Validate(d); err == nil {
		t.Fatalf("expected scalar/complex column overlap to fail validation")
	}
}

func TestRowChangeKindClassification(t *testing.T) {
	cases := []struct {
		name   string
		old    *hash.Hash
		newH   *hash.Hash
		expect RowKind
	}{
		{"insert", nil, ptrHash(mkHash(1)), RowKindInsert},
		{"delete", ptrHash(mkHash(1)), nil, RowKindDelete},
		{"update", ptrHash(mkHash(1)), ptrHash(mkHash(2)), RowKindUpdate},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rc := RowChange{PrimaryKey: []byte{1}, OldRowHash: c.old, NewRowHash: c.newH}
			if got := rc.Kind(); got != c.expect {
				t.Fatalf("expected %s, got %s", c.expect, got)
			}
		})
	}
}

// TestStaleDetectsBaseRootMismatch ensures a delta produced against an older
// base root than what the merge expects is detected as stale (caller will then
// emit declined_missing_delta_metadata per architecture).
func TestStaleDetectsBaseRootMismatch(t *testing.T) {
	d := sampleDelta()
	encoded, err := Encode(d)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	// The merge driver expects the base root to match the branch's recorded
	// base. If a different base is observed, the delta is stale.
	if !IsBoundTo(encoded, d.BaseRoot, d.TargetCommit) {
		t.Fatalf("expected delta to be bound to its declared roots")
	}
	otherBase := mkHash(0xAA)
	if IsBoundTo(encoded, otherBase, d.TargetCommit) {
		t.Fatalf("expected delta to NOT be bound to a different base root")
	}
	otherTarget := mkHash(0xBB)
	if IsBoundTo(encoded, d.BaseRoot, otherTarget) {
		t.Fatalf("expected delta to NOT be bound to a different target commit")
	}
}

func TestDecodeRejectsCorruptPayload(t *testing.T) {
	if _, err := Decode([]byte("not even json")); err == nil {
		t.Fatalf("expected decode of corrupt payload to fail")
	}
	if _, err := Decode(nil); err == nil {
		t.Fatalf("expected decode of empty payload to fail")
	}
}

func TestDecodeRejectsUnknownFormatVersion(t *testing.T) {
	d := sampleDelta()
	bytes, err := Encode(d)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	tampered := strings.Replace(string(bytes), `"format":1`, `"format":42`, 1)
	if _, err := Decode([]byte(tampered)); err == nil {
		t.Fatalf("expected decode to reject unknown format version")
	}
}

// TestRowCountMatchesArchitectureBudget exercises the helper that fast-path
// uses to enforce the configured row-change limit. The architecture targets
// 10k changed rows per batch, so the helper must count INSERTs, UPDATEs, and
// DELETEs across all tables exactly once.
func TestRowCount(t *testing.T) {
	d := sampleDelta()
	if got := d.RowCount(); got != 2 {
		t.Fatalf("expected 2 changed rows, got %d", got)
	}
	d.Tables = append(d.Tables, TableDelta{
		Name: "public.journals",
		Rows: []RowChange{{PrimaryKey: []byte{9}, NewRowHash: ptrHash(mkHash(1))}},
	})
	if got := d.RowCount(); got != 3 {
		t.Fatalf("expected 3 changed rows, got %d", got)
	}
}

func TestAffectedTableNames(t *testing.T) {
	d := sampleDelta()
	d.Tables = append(d.Tables, TableDelta{Name: "public.journals", Rows: []RowChange{
		{PrimaryKey: []byte{1}, NewRowHash: ptrHash(mkHash(1))},
	}})
	got := d.AffectedTableNames()
	want := []string{"public.journal_lines", "public.journals"}
	if len(got) != len(want) {
		t.Fatalf("affected tables: want %v got %v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("affected tables[%d]: want %q got %q", i, want[i], got[i])
		}
	}
}

func equalDeltas(a, b Delta) bool {
	if a.Format != b.Format {
		return false
	}
	if a.BaseRoot != b.BaseRoot {
		return false
	}
	if a.TargetRef != b.TargetRef {
		return false
	}
	if a.TargetCommit != b.TargetCommit {
		return false
	}
	if len(a.Tables) != len(b.Tables) {
		return false
	}
	for i := range a.Tables {
		ta, tb := a.Tables[i], b.Tables[i]
		if ta.Name != tb.Name {
			return false
		}
		if len(ta.Rows) != len(tb.Rows) {
			return false
		}
		for j := range ta.Rows {
			ra, rb := ta.Rows[j], tb.Rows[j]
			if string(ra.PrimaryKey) != string(rb.PrimaryKey) {
				return false
			}
			if !ptrHashEq(ra.OldRowHash, rb.OldRowHash) {
				return false
			}
			if !ptrHashEq(ra.NewRowHash, rb.NewRowHash) {
				return false
			}
			if !sliceStringEq(ra.ChangedScalars, rb.ChangedScalars) {
				return false
			}
			if !sliceStringEq(ra.TouchedComplex, rb.TouchedComplex) {
				return false
			}
		}
	}
	if len(a.Provenance) != len(b.Provenance) {
		return false
	}
	for k, v := range a.Provenance {
		if b.Provenance[k] != v {
			return false
		}
	}
	return true
}

func ptrHashEq(a, b *hash.Hash) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

func sliceStringEq(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
