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
	"encoding/binary"
	"testing"

	"github.com/dolthub/dolt/go/store/hash"
)

func makeBenchDelta(n int) Delta {
	rows := make([]RowChange, n)
	for i := 0; i < n; i++ {
		pk := make([]byte, 8)
		binary.BigEndian.PutUint64(pk, uint64(i))
		oh := hashFromIntDM(uint64(i)<<8 | 0x01)
		nh := hashFromIntDM(uint64(i)<<8 | 0x02)
		rows[i] = RowChange{
			PrimaryKey:     pk,
			OldRowHash:     &oh,
			NewRowHash:     &nh,
			ChangedScalars: []string{"memo"},
		}
	}
	return Delta{
		Format:       FormatVersion1,
		BaseRoot:     hashFromIntDM(0x10),
		TargetRef:    "refs/heads/sync",
		TargetCommit: hashFromIntDM(0x20),
		Tables:       []TableDelta{{Name: "public.journals", Rows: rows}},
	}
}

func hashFromIntDM(v uint64) hash.Hash {
	var h hash.Hash
	binary.BigEndian.PutUint64(h[:8], v)
	for i := 8; i < len(h); i++ {
		h[i] = byte(i)
	}
	return h
}

func BenchmarkEncode_10k(b *testing.B) {
	d := makeBenchDelta(10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := Encode(d)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecode_10k(b *testing.B) {
	d := makeBenchDelta(10000)
	encoded, err := Encode(d)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := Decode(encoded)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// TestEncodingSize_10kRowBudget pins the storage-overhead-per-delta-entry
// dimension architectured by dg-93u.7.5. The encoded payload for a 10k-row
// disjoint-scalar batch must be small enough that durably attaching it to
// the commit is acceptable for the 10k-row constrained-batch target.
//
// The budget is generous: 4 MiB encoded for 10k rows = ~400 bytes per row,
// which comfortably accommodates the JSON envelope, hash hex, primary-key
// hex, and column-name strings. A regression that doubles per-row size will
// trip this.
func TestEncodingSize_10kRowBudget(t *testing.T) {
	d := makeBenchDelta(10000)
	encoded, err := Encode(d)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	const budget = 4 * 1024 * 1024
	if len(encoded) > budget {
		t.Fatalf("encoded 10k-row delta = %d bytes, budget %d", len(encoded), budget)
	}
	t.Logf("encoded 10k-row delta = %d bytes (%d bytes/row)", len(encoded), len(encoded)/10000)
}
