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
)

func TestBuilder_HappyPath(t *testing.T) {
	b := NewBuilder(mkHash(0x10), "refs/heads/sync", mkHash(0x20))
	b.Provenance("job", "ingest-42")
	b.AddInsert("public.journals", []byte{0x01}, mkHash(0xA1))
	b.AddUpdate("public.journals", []byte{0x02}, mkHash(0xB0), mkHash(0xB1), []string{"memo", "amount"}, nil)
	b.AddDelete("public.journals", []byte{0x03}, mkHash(0xC0))

	d, err := b.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if d.Format != FormatVersion1 {
		t.Fatalf("format: %d", d.Format)
	}
	if d.RowCount() != 3 {
		t.Fatalf("row count: %d", d.RowCount())
	}
	if len(d.Tables) != 1 || d.Tables[0].Name != "public.journals" {
		t.Fatalf("tables: %+v", d.Tables)
	}
	if d.Provenance["job"] != "ingest-42" {
		t.Fatalf("provenance: %+v", d.Provenance)
	}
}

func TestBuilder_BuildIsValidatedAndCanonical(t *testing.T) {
	b := NewBuilder(mkHash(0x10), "refs/heads/sync", mkHash(0x20))
	// Add rows in non-canonical order.
	b.AddUpdate("public.journals", []byte{0x02}, mkHash(0xB0), mkHash(0xB1), []string{"memo"}, nil)
	b.AddInsert("public.alpha", []byte{0x01}, mkHash(0xA1))
	d, err := b.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	encoded, err := Encode(d)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	round, err := Decode(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	// Builder.Build returns canonical (sorted) output, byte-for-byte equal
	// to Encode/Decode round-trip.
	encoded2, err := Encode(round)
	if err != nil {
		t.Fatalf("re-encode: %v", err)
	}
	if string(encoded) != string(encoded2) {
		t.Fatalf("Builder did not produce canonical output:\n a=%s\n b=%s", encoded, encoded2)
	}
}

func TestBuilder_RejectsInsertWithChangedScalars(t *testing.T) {
	// AddInsert exposes no ChangedScalars argument, so producers can't
	// supply them. This test guards against accidental future shape drift.
	b := NewBuilder(mkHash(0x10), "refs/heads/sync", mkHash(0x20))
	b.AddInsert("t", []byte{1}, mkHash(0xA1))
	d, err := b.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if len(d.Tables[0].Rows[0].ChangedScalars) != 0 {
		t.Fatalf("INSERT should never carry ChangedScalars: %+v", d.Tables[0].Rows[0])
	}
}

func TestBuilder_RejectsDeleteWithChangedScalars(t *testing.T) {
	b := NewBuilder(mkHash(0x10), "refs/heads/sync", mkHash(0x20))
	b.AddDelete("t", []byte{1}, mkHash(0xA0))
	d, err := b.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if len(d.Tables[0].Rows[0].ChangedScalars) != 0 {
		t.Fatalf("DELETE should never carry ChangedScalars: %+v", d.Tables[0].Rows[0])
	}
}

func TestBuilder_RejectsDuplicatePrimaryKey(t *testing.T) {
	b := NewBuilder(mkHash(0x10), "refs/heads/sync", mkHash(0x20))
	b.AddInsert("t", []byte{1}, mkHash(0xA1))
	b.AddInsert("t", []byte{1}, mkHash(0xA2))
	if _, err := b.Build(); err == nil {
		t.Fatalf("expected duplicate PK to fail")
	}
}

func TestBuilder_RejectsScalarComplexOverlap(t *testing.T) {
	b := NewBuilder(mkHash(0x10), "refs/heads/sync", mkHash(0x20))
	b.AddUpdate("t", []byte{1}, mkHash(0xA0), mkHash(0xA1), []string{"col"}, []string{"col"})
	if _, err := b.Build(); err == nil {
		t.Fatalf("expected overlap to fail")
	}
}

func TestBuilder_RejectsIdenticalUpdateHashes(t *testing.T) {
	b := NewBuilder(mkHash(0x10), "refs/heads/sync", mkHash(0x20))
	b.AddUpdate("t", []byte{1}, mkHash(0xA0), mkHash(0xA0), nil, nil)
	if _, err := b.Build(); err == nil {
		t.Fatalf("expected old==new to fail")
	}
}

func TestBuilder_RejectsZeroBaseOrTarget(t *testing.T) {
	cases := []struct {
		name string
		mut  func(b *Builder)
	}{
		{"zero base", func(b *Builder) { b.delta.BaseRoot = zeroHash() }},
		{"zero target", func(b *Builder) { b.delta.TargetCommit = zeroHash() }},
		{"empty ref", func(b *Builder) { b.delta.TargetRef = "  " }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b := NewBuilder(mkHash(0x10), "refs/heads/sync", mkHash(0x20))
			b.AddInsert("t", []byte{1}, mkHash(0xA1))
			c.mut(b)
			if _, err := b.Build(); err == nil {
				t.Fatalf("expected build to fail")
			}
		})
	}
}

func TestBuilder_RejectsEmptyDelta(t *testing.T) {
	b := NewBuilder(mkHash(0x10), "refs/heads/sync", mkHash(0x20))
	_, err := b.Build()
	if err == nil {
		t.Fatalf("expected build with no rows to fail")
	}
	if !strings.Contains(err.Error(), "table") {
		t.Fatalf("error should mention table requirement: %v", err)
	}
}

func TestBuilder_AccumulatesBranchTouchedComplex(t *testing.T) {
	b := NewBuilder(mkHash(0x10), "refs/heads/sync", mkHash(0x20))
	b.AddUpdate("t", []byte{1}, mkHash(0xA0), mkHash(0xA1), []string{"scalar1"}, []string{"complex1", "complex2"})
	d, err := b.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	r := d.Tables[0].Rows[0]
	if len(r.TouchedComplex) != 2 {
		t.Fatalf("touched complex: %v", r.TouchedComplex)
	}
}

func TestBuilder_PersistsAcrossEncodeDecode(t *testing.T) {
	b := NewBuilder(mkHash(0x10), "refs/heads/sync", mkHash(0x20))
	b.AddInsert("t", []byte{1}, mkHash(0xA1))
	d, err := b.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	encoded, err := Encode(d)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if !IsBoundTo(encoded, mkHash(0x10), mkHash(0x20)) {
		t.Fatalf("encoded delta not bound to declared roots")
	}
}
