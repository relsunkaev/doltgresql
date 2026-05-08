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

// Package deltameta defines the per-commit delta metadata format used by the
// constrained-merge fast path. A delta is durable optimization metadata that
// rides alongside a generated batch branch commit. The fast-path merge driver
// reads it to decide whether the merge can be applied without running the full
// Dolt three-way merge engine.
//
// The metadata is not a source of truth: if it is missing, stale, malformed,
// or not bound to the expected base/target roots, the merge driver declines
// the fast path and falls back to the existing Dolt merge/review path. See
// docs/customer-branch-storage-architecture.md for the full architecture.
package deltameta

import (
	"github.com/dolthub/dolt/go/store/hash"
)

// FormatVersion1 is the only currently-recognized delta format. Bumping this
// is the way new fields are introduced; older readers refuse unknown formats
// and the merge driver declines the fast path.
const FormatVersion1 uint16 = 1

// Delta is the per-commit metadata attached to a generated batch branch
// commit. It binds the commit to a base root and target commit and enumerates
// the row-level effects in a form the constrained-merge fast path can consume.
//
// Field choices follow the architecture document's required fields list:
//   - affected tables: TableDelta entries
//   - primary keys, old/new row hash, changed scalar columns: RowChange
//   - base root: BaseRoot
//   - target branch metadata: TargetRef + TargetCommit
//   - job/provenance metadata: Provenance
type Delta struct {
	Format       uint16
	BaseRoot     hash.Hash
	TargetRef    string
	TargetCommit hash.Hash
	Tables       []TableDelta
	Provenance   map[string]string
}

// TableDelta is the per-table slice of the commit's row-level effects.
type TableDelta struct {
	Name string
	Rows []RowChange
}

// RowChange describes one row's transition from base to target. Hash pointers
// distinguish the kinds of change:
//
//   - OldRowHash == nil, NewRowHash != nil: INSERT
//   - OldRowHash != nil, NewRowHash == nil: DELETE
//   - both non-nil and != : UPDATE
//
// ChangedScalars lists scalar columns whose values differ between old and new
// row state. The fast path uses this list to permit same-row, disjoint-column
// merges. TouchedComplex lists non-scalar columns (blob/json/generated/etc.)
// that the row touched; if either side touches any complex column for the
// same row, the fast path declines conservatively.
type RowChange struct {
	PrimaryKey     []byte
	OldRowHash     *hash.Hash
	NewRowHash     *hash.Hash
	ChangedScalars []string
	TouchedComplex []string
}

// RowKind enumerates the three kinds of row change a delta entry can encode.
type RowKind uint8

const (
	RowKindInvalid RowKind = iota
	RowKindInsert
	RowKindDelete
	RowKindUpdate
)

// String returns a stable name for the row kind. Stable strings let decline
// reasons embed the kind in diagnostics without leaking enum integers.
func (k RowKind) String() string {
	switch k {
	case RowKindInsert:
		return "insert"
	case RowKindDelete:
		return "delete"
	case RowKindUpdate:
		return "update"
	default:
		return "invalid"
	}
}

// Kind classifies a RowChange by which of its hashes are present.
func (r RowChange) Kind() RowKind {
	switch {
	case r.OldRowHash == nil && r.NewRowHash != nil:
		return RowKindInsert
	case r.OldRowHash != nil && r.NewRowHash == nil:
		return RowKindDelete
	case r.OldRowHash != nil && r.NewRowHash != nil:
		return RowKindUpdate
	default:
		return RowKindInvalid
	}
}

// RowCount returns the total number of changed rows across all tables. The
// fast path compares this against the configured row-change limit before
// doing any per-row work.
func (d Delta) RowCount() int {
	n := 0
	for _, t := range d.Tables {
		n += len(t.Rows)
	}
	return n
}

// AffectedTableNames returns the names of tables this delta touches, in
// canonical (sorted) order. Useful for restricting validation/merge work to
// the changed tables and for diagnostics.
func (d Delta) AffectedTableNames() []string {
	names := make([]string, 0, len(d.Tables))
	for _, t := range d.Tables {
		names = append(names, t.Name)
	}
	// Defensive copy + sort, in case the caller's Delta was mutated after
	// canonicalization.
	sortStrings(names)
	return names
}
