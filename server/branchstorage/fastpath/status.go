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

// Package fastpath implements the constrained-merge fast path for generated
// batch branch commits. The status vocabulary, decision logic, and decline
// reasons follow the architecture spelled out in
// docs/customer-branch-storage-architecture.md.
//
// The fast path is a correctness-preserving optimization: every accepted case
// must produce the same visible committed result as a full Dolt merge would
// produce, and every unsupported case must decline (not partially apply).
package fastpath

import "fmt"

// Status is the architecture-fixed vocabulary describing the outcome of a
// fast-path merge attempt. The string forms are part of the diagnostics
// contract; see docs/customer-branch-storage-architecture.md.
type Status uint8

const (
	// StatusFastPathApplied means the constrained merge ran and the new
	// main commit (with its materialized root and eager indexes) is
	// published.
	StatusFastPathApplied Status = iota + 1

	// StatusDeclinedConflict means the per-row 3-way comparison found a
	// shape Dolt's merge engine would call a conflict: same-column edits
	// on both sides, an update against a deleted row, etc.
	StatusDeclinedConflict

	// StatusDeclinedUnsupportedColumn means at least one row touched a
	// non-scalar column (blob, json-like document, generated value) on
	// both sides. The fast path is conservative and declines so the full
	// Dolt merge can apply its type-aware semantics.
	StatusDeclinedUnsupportedColumn

	// StatusDeclinedMissingDeltaMetadata means the delta record is
	// missing, malformed, stale, or not bound to the expected base/target
	// roots. The fast path declines without reading row data.
	StatusDeclinedMissingDeltaMetadata

	// StatusDeclinedBatchTooLarge means the delta's row count exceeds the
	// configured fast-path threshold.
	StatusDeclinedBatchTooLarge

	// StatusDeclinedSchemaChange means the branch commit modified table
	// schemas. The fast path applies only to row-level effects; schema
	// shaping uses the full Dolt merge path.
	StatusDeclinedSchemaChange
)

// String returns the architecture-fixed wire form of the status.
func (s Status) String() string {
	switch s {
	case StatusFastPathApplied:
		return "fast_path_applied"
	case StatusDeclinedConflict:
		return "declined_conflict"
	case StatusDeclinedUnsupportedColumn:
		return "declined_unsupported_column"
	case StatusDeclinedMissingDeltaMetadata:
		return "declined_missing_delta_metadata"
	case StatusDeclinedBatchTooLarge:
		return "declined_batch_too_large"
	case StatusDeclinedSchemaChange:
		return "declined_schema_change"
	default:
		return fmt.Sprintf("unknown_status(%d)", uint8(s))
	}
}

// IsApplied reports whether the fast path published a new main commit.
func (s Status) IsApplied() bool { return s == StatusFastPathApplied }

// IsDecline reports whether the fast path declined and handed control back
// to the full Dolt merge/review path.
func (s Status) IsDecline() bool {
	return s != StatusFastPathApplied && s != 0
}

// Context carries diagnostic context attached to a fast-path decision. It is
// intentionally a flat struct so it embeds cleanly in metrics, logs, and
// review-path inputs.
//
// Fields are best-effort: a Decline may populate only the fields relevant
// to its reason. For example, StatusDeclinedBatchTooLarge sets RowCount
// and Limit; StatusDeclinedSchemaChange sets Tables; StatusDeclinedConflict
// sets ConflictingTable, ConflictingPK, and ConflictColumns.
type Context struct {
	Tables           []string
	RowCount         int
	Limit            int
	ConflictingTable string
	ConflictingPK    []byte
	ConflictColumns  []string
	UnsupportedTable string
	UnsupportedPK    []byte
	UnsupportedCols  []string
	SchemaChangeNote string
	Detail           string
}

// Result is the outcome of a fast-path attempt, status plus context.
type Result struct {
	Status  Status
	Context Context
}

// Applied returns a successful fast-path result with no decline context.
func Applied() Result {
	return Result{Status: StatusFastPathApplied}
}

// Decline returns a decline result with the given status and context.
func Decline(status Status, ctx Context) Result {
	return Result{Status: status, Context: ctx}
}
