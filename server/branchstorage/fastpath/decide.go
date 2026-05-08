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
	"bytes"
	"sort"

	"github.com/dolthub/dolt/go/store/hash"

	"github.com/dolthub/doltgresql/server/branchstorage/deltameta"
)

// ColValue is the byte-level encoding of a single column's value as observed
// by the merge driver. Bytes is nil for SQL NULL.
type ColValue struct {
	Bytes []byte
}

// equal compares two ColValues for byte equality. nil is considered distinct
// from an empty (but non-nil) slice for symmetry with SQL NULL semantics.
func (c ColValue) equal(other ColValue) bool {
	if c.Bytes == nil || other.Bytes == nil {
		return c.Bytes == nil && other.Bytes == nil
	}
	return bytes.Equal(c.Bytes, other.Bytes)
}

// RowSnapshot is a row's content-addressed hash plus its column-level values
// at one of the three merge sides (base, current main, target branch).
//
// Cols is keyed by column name. Only scalar columns the merge driver may
// need to decide column-disjointness or to assemble a merged row need to
// be present; complex columns are tracked in Delta.TouchedComplex and the
// fast path declines if both sides touched them.
type RowSnapshot struct {
	Hash hash.Hash
	Cols map[string]ColValue
}

// TableSnapshots holds the three merge-side row maps for a single table,
// keyed by string(primary key bytes).
type TableSnapshots struct {
	Base   map[string]RowSnapshot
	Main   map[string]RowSnapshot
	Target map[string]RowSnapshot
}

// Inputs collects everything Decide needs.
type Inputs struct {
	// Delta is the parsed delta record.
	Delta deltameta.Delta

	// EncodedDelta is the canonical bytes form of Delta. Decide uses it to
	// re-verify base/target binding (the "delta is durable with the commit
	// it describes" architecture invariant). nil or malformed bytes cause a
	// declined_missing_delta_metadata decline.
	EncodedDelta []byte

	// ExpectedBaseRoot is the root the merge driver expects the delta to
	// be bound to (recorded at branch creation time).
	ExpectedBaseRoot hash.Hash

	// ExpectedTargetCommit is the branch commit the merge driver expects
	// the delta to describe (recorded at commit time).
	ExpectedTargetCommit hash.Hash

	// SchemaChanged is precomputed by the caller from a cheap branch-vs-base
	// schema comparison. true forces declined_schema_change.
	SchemaChanged    bool
	SchemaChangeNote string

	// RowChangeLimit is the maximum number of changed rows the fast path
	// will attempt. Above this it declines as declined_batch_too_large.
	RowChangeLimit int

	// Snapshots is the per-table base/main/target row maps the merge
	// driver loaded for the rows the delta lists.
	Snapshots map[string]TableSnapshots
}

// RowOp is one row-level operation in a merge plan.
type RowOp struct {
	PrimaryKey []byte
	NewRow     map[string]ColValue
}

// TablePlan groups row-level ops for a single table.
type TablePlan struct {
	Name    string
	Inserts []RowOp
	Updates []RowOp
	Deletes [][]byte
}

// Plan is the merge driver's instruction set: write these ops to main, then
// commit, then update materialized indexes per the architecture.
type Plan struct {
	Tables []TablePlan
}

// Decision is the outcome of Decide: a Result describing the status and
// diagnostic context, plus a Plan for accepted cases.
type Decision struct {
	Result Result
	Plan   *Plan
}

// Decide runs the constrained-merge fast-path decision logic. It implements
// steps 1-6 of the architecture's seven-step list (step 7, the actual write
// of main's materialized root and indexes, is the merge driver's job and
// consumes Decision.Plan).
//
// The function is pure: same Inputs always return the same Decision. It
// never mutates Inputs, never reads from disk, and never returns nil
// pointers from accepted decisions.
func Decide(in Inputs) Decision {
	// Step 1: delta presence + binding.
	if len(in.EncodedDelta) == 0 {
		return Decision{Result: Decline(StatusDeclinedMissingDeltaMetadata, Context{
			Detail: "encoded delta payload is empty",
		})}
	}
	parsed, err := deltameta.Decode(in.EncodedDelta)
	if err != nil {
		return Decision{Result: Decline(StatusDeclinedMissingDeltaMetadata, Context{
			Detail: "delta decode: " + err.Error(),
		})}
	}
	if parsed.BaseRoot != in.ExpectedBaseRoot || parsed.TargetCommit != in.ExpectedTargetCommit {
		return Decision{Result: Decline(StatusDeclinedMissingDeltaMetadata, Context{
			Detail: "delta is not bound to the expected base/target roots",
		})}
	}

	// Step 2: schema change.
	if in.SchemaChanged {
		note := in.SchemaChangeNote
		if note == "" {
			note = "branch commit changed schema"
		}
		return Decision{Result: Decline(StatusDeclinedSchemaChange, Context{
			Tables:           parsed.AffectedTableNames(),
			SchemaChangeNote: note,
		})}
	}

	// Step 3: batch size.
	rowCount := parsed.RowCount()
	if rowCount > in.RowChangeLimit {
		return Decision{Result: Decline(StatusDeclinedBatchTooLarge, Context{
			Tables:   parsed.AffectedTableNames(),
			RowCount: rowCount,
			Limit:    in.RowChangeLimit,
		})}
	}

	// Steps 4-6: per-row 3-way analysis and plan assembly.
	plan := &Plan{Tables: make([]TablePlan, 0, len(parsed.Tables))}
	for _, table := range parsed.Tables {
		snaps := in.Snapshots[table.Name] // zero-value works fine if missing
		tp := TablePlan{Name: table.Name}
		for _, row := range table.Rows {
			res, op, kind := classifyRow(table.Name, row, snaps)
			if res.Status.IsDecline() {
				return Decision{Result: res}
			}
			switch kind {
			case opInsert:
				tp.Inserts = append(tp.Inserts, op)
			case opUpdate:
				tp.Updates = append(tp.Updates, op)
			case opDelete:
				tp.Deletes = append(tp.Deletes, op.PrimaryKey)
			case opNoop:
				// row already in target state on main; nothing to do
			}
		}
		// Sort each op slice by primary key so plan output is deterministic.
		sort.SliceStable(tp.Inserts, func(i, j int) bool { return bytes.Compare(tp.Inserts[i].PrimaryKey, tp.Inserts[j].PrimaryKey) < 0 })
		sort.SliceStable(tp.Updates, func(i, j int) bool { return bytes.Compare(tp.Updates[i].PrimaryKey, tp.Updates[j].PrimaryKey) < 0 })
		sort.SliceStable(tp.Deletes, func(i, j int) bool { return bytes.Compare(tp.Deletes[i], tp.Deletes[j]) < 0 })
		plan.Tables = append(plan.Tables, tp)
	}
	sort.SliceStable(plan.Tables, func(i, j int) bool { return plan.Tables[i].Name < plan.Tables[j].Name })

	return Decision{Result: Applied(), Plan: plan}
}

type planOpKind uint8

const (
	opNoop planOpKind = iota
	opInsert
	opUpdate
	opDelete
)

// classifyRow runs the per-row 3-way decision for one delta entry. It returns
// either a Result (decline) with no plan, or a Result-applied with a single
// op kind and the row op.
func classifyRow(table string, row deltameta.RowChange, snaps TableSnapshots) (Result, RowOp, planOpKind) {
	pk := string(row.PrimaryKey)
	main, mainHas := snaps.Main[pk]
	target, targetHas := snaps.Target[pk]

	switch row.Kind() {
	case deltameta.RowKindInsert:
		if mainHas {
			// Insert/insert collision (or main has the row already): conflict.
			return Decline(StatusDeclinedConflict, Context{
				ConflictingTable: table,
				ConflictingPK:    row.PrimaryKey,
				Detail:           "main already has the inserted primary key",
			}), RowOp{}, opNoop
		}
		if !targetHas {
			return Decline(StatusDeclinedMissingDeltaMetadata, Context{
				Detail: "target snapshot missing for an INSERT row",
			}), RowOp{}, opNoop
		}
		return Applied(), RowOp{PrimaryKey: row.PrimaryKey, NewRow: copyCols(target.Cols)}, opInsert

	case deltameta.RowKindDelete:
		base, baseHas := snaps.Base[pk]
		if !baseHas {
			return Decline(StatusDeclinedMissingDeltaMetadata, Context{
				Detail: "base snapshot missing for a DELETE row",
			}), RowOp{}, opNoop
		}
		// The delta declared the old hash; assert it matches the observed
		// base. A mismatch means the recorded delta is stale.
		if row.OldRowHash == nil || base.Hash != *row.OldRowHash {
			return Decline(StatusDeclinedMissingDeltaMetadata, Context{
				ConflictingTable: table,
				ConflictingPK:    row.PrimaryKey,
				Detail:           "DELETE row's recorded base hash does not match observed base",
			}), RowOp{}, opNoop
		}
		if !mainHas {
			// Already gone on main; clean apply, but no scheduled op.
			return Applied(), RowOp{}, opNoop
		}
		if main.Hash != base.Hash {
			// Main edited the row; deleting it would lose those edits.
			return Decline(StatusDeclinedConflict, Context{
				ConflictingTable: table,
				ConflictingPK:    row.PrimaryKey,
				Detail:           "main updated a row the branch deleted",
			}), RowOp{}, opNoop
		}
		return Applied(), RowOp{PrimaryKey: row.PrimaryKey}, opDelete

	case deltameta.RowKindUpdate:
		base, baseHas := snaps.Base[pk]
		if !baseHas {
			return Decline(StatusDeclinedMissingDeltaMetadata, Context{
				Detail: "base snapshot missing for an UPDATE row",
			}), RowOp{}, opNoop
		}
		if row.OldRowHash == nil || base.Hash != *row.OldRowHash {
			return Decline(StatusDeclinedMissingDeltaMetadata, Context{
				ConflictingTable: table,
				ConflictingPK:    row.PrimaryKey,
				Detail:           "UPDATE row's recorded base hash does not match observed base",
			}), RowOp{}, opNoop
		}
		if !mainHas {
			// Main deleted a row the branch updated.
			return Decline(StatusDeclinedConflict, Context{
				ConflictingTable: table,
				ConflictingPK:    row.PrimaryKey,
				Detail:           "main deleted a row the branch updated",
			}), RowOp{}, opNoop
		}
		if !targetHas {
			return Decline(StatusDeclinedMissingDeltaMetadata, Context{
				Detail: "target snapshot missing for an UPDATE row",
			}), RowOp{}, opNoop
		}

		if main.Hash == base.Hash {
			// Main has not edited the row; write target's full row.
			return Applied(), RowOp{PrimaryKey: row.PrimaryKey, NewRow: copyCols(target.Cols)}, opUpdate
		}

		// Both sides edited the row. Disjoint-column merge is allowed if
		// (a) no complex column was touched on either side for this row,
		// and (b) main's changed scalars and branch's changed scalars do
		// not overlap. Otherwise: conflict.
		if len(row.TouchedComplex) > 0 {
			return Decline(StatusDeclinedUnsupportedColumn, Context{
				UnsupportedTable: table,
				UnsupportedPK:    row.PrimaryKey,
				UnsupportedCols:  append([]string(nil), row.TouchedComplex...),
				Detail:           "branch and main both touched a row with non-scalar columns",
			}), RowOp{}, opNoop
		}

		mainChanged := changedScalars(base.Cols, main.Cols)
		overlap := overlappingColumns(mainChanged, row.ChangedScalars)
		if len(overlap) > 0 {
			return Decline(StatusDeclinedConflict, Context{
				ConflictingTable: table,
				ConflictingPK:    row.PrimaryKey,
				ConflictColumns:  overlap,
				Detail:           "main and branch edited the same scalar column(s)",
			}), RowOp{}, opNoop
		}

		merged := make(map[string]ColValue, len(base.Cols))
		for k, v := range base.Cols {
			merged[k] = v
		}
		// Apply main's edits.
		for _, c := range mainChanged {
			merged[c] = main.Cols[c]
		}
		// Apply branch's edits (declared changed scalars from target).
		for _, c := range row.ChangedScalars {
			merged[c] = target.Cols[c]
		}
		return Applied(), RowOp{PrimaryKey: row.PrimaryKey, NewRow: merged}, opUpdate

	default:
		return Decline(StatusDeclinedMissingDeltaMetadata, Context{
			ConflictingTable: table,
			ConflictingPK:    row.PrimaryKey,
			Detail:           "delta row has neither old nor new row hash",
		}), RowOp{}, opNoop
	}
}

// changedScalars returns the set of column names whose values differ between
// base and main. The caller treats this as "what main changed". Order is
// canonical (sorted) so call sites (and overlap detection) are deterministic.
func changedScalars(base, main map[string]ColValue) []string {
	keys := make(map[string]struct{}, len(base)+len(main))
	for k := range base {
		keys[k] = struct{}{}
	}
	for k := range main {
		keys[k] = struct{}{}
	}
	out := make([]string, 0, len(keys))
	for k := range keys {
		bv := base[k]
		mv := main[k]
		if !bv.equal(mv) {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

func overlappingColumns(a, b []string) []string {
	set := make(map[string]struct{}, len(a))
	for _, c := range a {
		set[c] = struct{}{}
	}
	var out []string
	for _, c := range b {
		if _, ok := set[c]; ok {
			out = append(out, c)
		}
	}
	sort.Strings(out)
	return out
}

func copyCols(in map[string]ColValue) map[string]ColValue {
	if in == nil {
		return nil
	}
	out := make(map[string]ColValue, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
