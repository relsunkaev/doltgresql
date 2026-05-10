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

package node

import (
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/doltgresql/server/indexmetadata"
)

// TestHidePlannerIndexBuildState pins the planner-visibility contract for
// PostgreSQL's pg_index.indisready and pg_index.indisvalid bits: an index
// in either non-default state must be invisible to query planning, while
// a steady-state index must remain visible. This is the core invariant
// CREATE INDEX CONCURRENTLY relies on — concurrent builds park the index
// in the catalog under (notReady=true, invalid=true), then flip both off
// once the build is complete.
func TestHidePlannerIndexBuildState(t *testing.T) {
	tests := []struct {
		name     string
		metadata indexmetadata.Metadata
		want     bool
	}{
		{
			name:     "default_visible",
			metadata: indexmetadata.Metadata{AccessMethod: "btree"},
			want:     false,
		},
		{
			name:     "invalid_hidden",
			metadata: indexmetadata.Metadata{AccessMethod: "btree", Invalid: true},
			want:     true,
		},
		{
			name:     "not_ready_hidden",
			metadata: indexmetadata.Metadata{AccessMethod: "btree", NotReady: true},
			want:     true,
		},
		{
			name:     "concurrent_build_in_progress_hidden",
			metadata: indexmetadata.Metadata{AccessMethod: "btree", NotReady: true, Invalid: true},
			want:     true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx := stubIndex{comment: indexmetadata.EncodeComment(tt.metadata)}
			if got := hidePlannerIndex(idx); got != tt.want {
				t.Fatalf("hidePlannerIndex got %v, want %v", got, tt.want)
			}
		})
	}
}

// TestHidePlannerIndexLegacyComment guards against a regression where
// indexes created before the build-state bits existed (their comment is
// either empty or carries no metadata payload) get hidden by accident.
// Every pre-existing index must remain visible to the planner.
func TestHidePlannerIndexLegacyComment(t *testing.T) {
	for _, comment := range []string{"", "ordinary user comment"} {
		if hidePlannerIndex(stubIndex{comment: comment}) {
			t.Fatalf("legacy comment %q must remain planner-visible", comment)
		}
	}
}

// TestBtreePlannerBoundaryFiltersInvalidIndex verifies the wrapper hides
// invalid/not-ready indexes from the planner-facing GetIndexes call so
// that a CREATE INDEX CONCURRENTLY build cannot accidentally serve a
// query against a half-built index.
func TestBtreePlannerBoundaryFiltersInvalidIndex(t *testing.T) {
	validComment := indexmetadata.EncodeComment(indexmetadata.Metadata{AccessMethod: "btree"})
	buildingComment := indexmetadata.EncodeComment(indexmetadata.Metadata{
		AccessMethod: "btree",
		NotReady:     true,
		Invalid:      true,
	})
	table := &stubIndexedTable{
		indexes: []sql.Index{
			stubIndex{comment: validComment},
			stubIndex{comment: buildingComment},
		},
	}
	wrapped, didWrap, err := WrapBtreePlannerBoundaryTable(nil, table)
	if err != nil {
		t.Fatalf("WrapBtreePlannerBoundaryTable: %v", err)
	}
	if !didWrap {
		t.Fatal("expected wrapping when an index is mid-CONCURRENTLY")
	}
	boundary, ok := wrapped.(*BtreePlannerBoundaryTable)
	if !ok {
		t.Fatalf("expected BtreePlannerBoundaryTable, got %T", wrapped)
	}
	got, err := boundary.GetIndexes(nil)
	if err != nil {
		t.Fatalf("GetIndexes: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 visible index, got %d", len(got))
	}
	if got[0].Comment() != validComment {
		t.Fatal("expected only the steady-state index to be visible")
	}
}

func TestPlannerSafeSortOptionIndex(t *testing.T) {
	tableSchema := sql.Schema{
		{Name: "nullable_score", Type: types.Int32, Nullable: true},
		{Name: "required_score", Type: types.Int32, Nullable: false},
	}
	tests := []struct {
		name     string
		metadata indexmetadata.Metadata
		want     bool
	}{
		{
			name: "nullable_asc_nulls_first_matches_native_order",
			metadata: sortOptionTestMetadata("nullable_score", false, indexmetadata.IndexColumnOption{
				NullsOrder: indexmetadata.NullsOrderFirst,
			}),
			want: true,
		},
		{
			name: "nullable_desc_nulls_last_matches_reverse_native_order",
			metadata: sortOptionTestMetadata("nullable_score", false, indexmetadata.IndexColumnOption{
				Direction:  indexmetadata.SortDirectionDesc,
				NullsOrder: indexmetadata.NullsOrderLast,
			}),
			want: true,
		},
		{
			name: "nullable_desc_default_nulls_first_stays_fenced",
			metadata: sortOptionTestMetadata("nullable_score", false, indexmetadata.IndexColumnOption{
				Direction: indexmetadata.SortDirectionDesc,
			}),
			want: false,
		},
		{
			name: "nullable_asc_nulls_last_stays_fenced",
			metadata: sortOptionTestMetadata("nullable_score", false, indexmetadata.IndexColumnOption{
				NullsOrder: indexmetadata.NullsOrderLast,
			}),
			want: false,
		},
		{
			name: "not_null_desc_default_is_safe",
			metadata: sortOptionTestMetadata("required_score", false, indexmetadata.IndexColumnOption{
				Direction: indexmetadata.SortDirectionDesc,
			}),
			want: true,
		},
		{
			name: "expression_index_stays_fenced",
			metadata: sortOptionTestMetadata("lower(nullable_score::text)", true, indexmetadata.IndexColumnOption{
				NullsOrder: indexmetadata.NullsOrderFirst,
			}),
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx := stubIndex{comment: indexmetadata.EncodeComment(tt.metadata)}
			if got := plannerSafeSortOptionIndex(idx, tableSchema); got != tt.want {
				t.Fatalf("plannerSafeSortOptionIndex got %v, want %v", got, tt.want)
			}
		})
	}
}

func sortOptionTestMetadata(column string, expression bool, option indexmetadata.IndexColumnOption) indexmetadata.Metadata {
	return indexmetadata.Metadata{
		AccessMethod:      indexmetadata.AccessMethodBtree,
		Columns:           []string{column},
		StorageColumns:    []string{column},
		ExpressionColumns: []bool{expression},
		SortOptions:       []indexmetadata.IndexColumnOption{option},
	}
}

// stubIndexedTable is a sql.Table + sql.IndexAddressable backed by a
// fixed slice of sql.Index, used for unit-testing the planner-boundary
// wrapper without standing up real Dolt storage.
type stubIndexedTable struct {
	indexes []sql.Index
}

var _ sql.Table = (*stubIndexedTable)(nil)
var _ sql.IndexAddressable = (*stubIndexedTable)(nil)

func (t *stubIndexedTable) Name() string                                       { return "stub" }
func (t *stubIndexedTable) String() string                                     { return "stub" }
func (t *stubIndexedTable) Schema(*sql.Context) sql.Schema                     { return nil }
func (t *stubIndexedTable) Collation() sql.CollationID                         { return sql.Collation_Default }
func (t *stubIndexedTable) Partitions(*sql.Context) (sql.PartitionIter, error) { return nil, nil }
func (t *stubIndexedTable) PartitionRows(*sql.Context, sql.Partition) (sql.RowIter, error) {
	return nil, nil
}
func (t *stubIndexedTable) GetIndexes(*sql.Context) ([]sql.Index, error)                 { return t.indexes, nil }
func (t *stubIndexedTable) IndexedAccess(*sql.Context, sql.IndexLookup) sql.IndexedTable { return nil }
func (t *stubIndexedTable) PreciseMatch() bool                                           { return false }

// stubIndex is a minimal sql.Index used by build-state filter tests.
// Only Comment is meaningful; the other accessors return zero values
// because the filter never inspects them.
type stubIndex struct {
	comment string
}

var _ sql.Index = stubIndex{}

func (s stubIndex) ID() string                                                    { return "stub" }
func (s stubIndex) Database() string                                              { return "" }
func (s stubIndex) Table() string                                                 { return "" }
func (s stubIndex) Expressions() []string                                         { return nil }
func (s stubIndex) IsUnique() bool                                                { return false }
func (s stubIndex) IsSpatial() bool                                               { return false }
func (s stubIndex) IsFullText() bool                                              { return false }
func (s stubIndex) IsVector() bool                                                { return false }
func (s stubIndex) Comment() string                                               { return s.comment }
func (s stubIndex) IndexType() string                                             { return "BTREE" }
func (s stubIndex) IsGenerated() bool                                             { return false }
func (s stubIndex) ColumnExpressionTypes(*sql.Context) []sql.ColumnExpressionType { return nil }
func (s stubIndex) CanSupport(*sql.Context, ...sql.Range) bool                    { return false }
func (s stubIndex) CanSupportOrderBy(sql.Expression) bool                         { return false }
func (s stubIndex) PrefixLengths() []uint16                                       { return nil }
