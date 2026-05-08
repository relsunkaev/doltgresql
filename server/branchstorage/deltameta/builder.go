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
	"github.com/dolthub/dolt/go/store/hash"
)

// Builder constructs a well-formed Delta from per-row producer events. It is
// the recommended entry point for sync, agent, and background-batch workers
// that emit batch branch commits: each AddInsert/AddUpdate/AddDelete enforces
// the architecture's row-shape invariants so producers cannot accidentally
// construct a delta that Validate would later reject.
//
// The Builder is single-use, not safe for concurrent calls. Call Build() once
// to canonicalize and validate the accumulated rows.
type Builder struct {
	delta     Delta
	tableIdx  map[string]int            // table name → index into delta.Tables
	rowSeen   map[string]map[string]int // table name → primary key string → row index
}

// NewBuilder returns a Builder bound to the given base root, target ref, and
// target commit. These three values bind the resulting delta to the branch
// commit it describes; they are recorded once and can not be changed by row
// helpers.
func NewBuilder(baseRoot hash.Hash, targetRef string, targetCommit hash.Hash) *Builder {
	return &Builder{
		delta: Delta{
			Format:       FormatVersion1,
			BaseRoot:     baseRoot,
			TargetRef:    targetRef,
			TargetCommit: targetCommit,
		},
		tableIdx: make(map[string]int),
		rowSeen:  make(map[string]map[string]int),
	}
}

// Provenance attaches a key/value pair to the optional provenance map. Useful
// for job ID, agent name, request correlation, etc. Repeated keys overwrite
// (last write wins).
func (b *Builder) Provenance(key, value string) {
	if b.delta.Provenance == nil {
		b.delta.Provenance = make(map[string]string)
	}
	b.delta.Provenance[key] = value
}

// AddInsert records a new branch-side row. INSERTs never carry a base hash or
// changed-scalar list; the row is wholly new.
func (b *Builder) AddInsert(table string, primaryKey []byte, newRowHash hash.Hash) {
	pk := append([]byte(nil), primaryKey...) // defensive copy
	nh := newRowHash
	b.appendRow(table, RowChange{
		PrimaryKey: pk,
		NewRowHash: &nh,
	})
}

// AddUpdate records an UPDATE: the same primary key existed at the recorded
// base row hash and now holds the recorded new row hash. ChangedScalars lists
// the scalar columns whose values differ between base and target;
// TouchedComplex lists non-scalar columns the row touched (the fast path
// declines if both sides touched any complex column on the same row).
func (b *Builder) AddUpdate(
	table string,
	primaryKey []byte,
	oldRowHash, newRowHash hash.Hash,
	changedScalars []string,
	touchedComplex []string,
) {
	pk := append([]byte(nil), primaryKey...)
	oh := oldRowHash
	nh := newRowHash
	b.appendRow(table, RowChange{
		PrimaryKey:     pk,
		OldRowHash:     &oh,
		NewRowHash:     &nh,
		ChangedScalars: append([]string(nil), changedScalars...),
		TouchedComplex: append([]string(nil), touchedComplex...),
	})
}

// AddDelete records a DELETE: the row at the recorded base row hash is gone
// on the branch side. DELETEs never carry a new hash or changed-scalar list.
func (b *Builder) AddDelete(table string, primaryKey []byte, oldRowHash hash.Hash) {
	pk := append([]byte(nil), primaryKey...)
	oh := oldRowHash
	b.appendRow(table, RowChange{
		PrimaryKey: pk,
		OldRowHash: &oh,
	})
}

// Build canonicalizes and validates the accumulated rows and returns a Delta
// ready to encode. Callers should treat any returned error as a producer bug
// (the producer constructed an unrepresentable shape) — the architecture's
// commit pipeline calls this synchronously and fails fast on error rather
// than persisting an unverifiable record.
func (b *Builder) Build() (Delta, error) {
	d := b.delta
	if err := Validate(d); err != nil {
		return Delta{}, err
	}
	// Re-canonicalize via the wire round-trip so callers get the same byte
	// shape Encode would emit, without forcing them to call Encode/Decode.
	encoded, err := Encode(d)
	if err != nil {
		return Delta{}, err
	}
	return Decode(encoded)
}

// appendRow inserts a row into the per-table slice, allocating the table on
// first use. Duplicate primary keys within the same table cause the row to
// land twice; Validate will then reject the build. We do not preempt here so
// the error surface is unified at Build() time.
func (b *Builder) appendRow(table string, row RowChange) {
	idx, ok := b.tableIdx[table]
	if !ok {
		idx = len(b.delta.Tables)
		b.tableIdx[table] = idx
		b.delta.Tables = append(b.delta.Tables, TableDelta{Name: table})
		b.rowSeen[table] = make(map[string]int)
	}
	b.delta.Tables[idx].Rows = append(b.delta.Tables[idx].Rows, row)
}

// zeroHash is a tiny test helper that returns the all-zero hash. It lives in
// the production file (not _test.go) so cross-test assertions can use it
// without leaking into test-only build tags.
func zeroHash() hash.Hash { return hash.Hash{} }
