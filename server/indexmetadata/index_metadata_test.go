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

package indexmetadata

import (
	"testing"

	"github.com/dolthub/go-mysql-server/sql"

	pgtypes "github.com/dolthub/doltgresql/server/types"
)

func TestEncodeDecodeComment(t *testing.T) {
	comment := EncodeComment(Metadata{
		AccessMethod:      "GIN",
		Columns:           []string{" lower(doc) "},
		StorageColumns:    []string{" doc "},
		ExpressionColumns: []bool{true},
		IncludeColumns:    []string{" doc_id ", " title "},
		Predicate:         " doc_id > 10 ",
		PredicateColumns:  []string{" doc_id "},
		Collations:        []string{` "C" `, "und-x-icu"},
		OpClasses:         []string{" JSONB_OPS ", "jsonb_path_ops"},
		RelOptions:        []string{" FILLFACTOR = 70 "},
		StatisticsTargets: []int16{100, -1},
		Unique:            true,
		NullsNotDistinct:  true,
		Deferrable:        true,
		InitiallyDeferred: true,
		SortOptions: []IndexColumnOption{
			{Direction: " DESC "},
			{NullsOrder: " FIRST "},
			{NullsOrder: " LAST "},
		},
		Constraint: " NONE ",
		Gin: &GinMetadata{
			PostingTable:      " dg_gin_docs_doc_idx_postings ",
			PostingChunkTable: " dg_gin_docs_doc_idx_posting_chunks ",
		},
	})

	metadata, ok := DecodeComment(comment)
	if !ok {
		t.Fatal("expected metadata comment to decode")
	}
	if metadata.AccessMethod != AccessMethodGin {
		t.Fatalf("expected access method %q, got %q", AccessMethodGin, metadata.AccessMethod)
	}
	if len(metadata.Columns) != 1 || metadata.Columns[0] != "lower(doc)" {
		t.Fatalf("unexpected columns: %#v", metadata.Columns)
	}
	if len(metadata.StorageColumns) != 1 || metadata.StorageColumns[0] != "doc" {
		t.Fatalf("unexpected storage columns: %#v", metadata.StorageColumns)
	}
	if len(metadata.ExpressionColumns) != 1 || !metadata.ExpressionColumns[0] {
		t.Fatalf("unexpected expression column flags: %#v", metadata.ExpressionColumns)
	}
	if len(metadata.IncludeColumns) != 2 || metadata.IncludeColumns[0] != "doc_id" || metadata.IncludeColumns[1] != "title" {
		t.Fatalf("unexpected include columns: %#v", metadata.IncludeColumns)
	}
	if metadata.Predicate != "doc_id > 10" {
		t.Fatalf("unexpected predicate: %q", metadata.Predicate)
	}
	if len(metadata.PredicateColumns) != 1 || metadata.PredicateColumns[0] != "doc_id" {
		t.Fatalf("unexpected predicate columns: %#v", metadata.PredicateColumns)
	}
	if len(metadata.Collations) != 2 || metadata.Collations[0] != CollationC || metadata.Collations[1] != CollationUndIcu {
		t.Fatalf("unexpected collations: %#v", metadata.Collations)
	}
	if len(metadata.OpClasses) != 2 {
		t.Fatalf("expected 2 opclasses, got %d", len(metadata.OpClasses))
	}
	if metadata.OpClasses[0] != OpClassJsonbOps || metadata.OpClasses[1] != OpClassJsonbPathOps {
		t.Fatalf("unexpected opclasses: %#v", metadata.OpClasses)
	}
	if len(metadata.RelOptions) != 1 || metadata.RelOptions[0] != "fillfactor=70" {
		t.Fatalf("unexpected reloptions: %#v", metadata.RelOptions)
	}
	if len(metadata.StatisticsTargets) != 2 || metadata.StatisticsTargets[0] != 100 || metadata.StatisticsTargets[1] != -1 {
		t.Fatalf("unexpected statistics targets: %#v", metadata.StatisticsTargets)
	}
	if !metadata.NullsNotDistinct {
		t.Fatal("expected NULLS NOT DISTINCT metadata to round-trip")
	}
	if !metadata.Unique {
		t.Fatal("expected unique metadata to round-trip")
	}
	if !MetadataUnique(comment) {
		t.Fatal("expected unique metadata accessor to decode metadata")
	}
	if !NullsNotDistinct(comment) {
		t.Fatal("expected NULLS NOT DISTINCT accessor to decode metadata")
	}
	if !Deferrable(comment) {
		t.Fatal("expected deferrable metadata accessor to decode metadata")
	}
	if !InitiallyDeferred(comment) {
		t.Fatal("expected initially deferred metadata accessor to decode metadata")
	}
	if len(metadata.SortOptions) != 3 {
		t.Fatalf("expected 3 sort options, got %d", len(metadata.SortOptions))
	}
	if metadata.SortOptions[0].Direction != SortDirectionDesc {
		t.Fatalf("unexpected first sort option: %#v", metadata.SortOptions[0])
	}
	if metadata.SortOptions[1].NullsOrder != NullsOrderFirst {
		t.Fatalf("unexpected second sort option: %#v", metadata.SortOptions[1])
	}
	if metadata.SortOptions[2].NullsOrder != NullsOrderLast {
		t.Fatalf("unexpected third sort option: %#v", metadata.SortOptions[2])
	}
	if metadata.Gin == nil || metadata.Gin.PostingTable != "dg_gin_docs_doc_idx_postings" {
		t.Fatalf("unexpected gin metadata: %#v", metadata.Gin)
	}
	if metadata.Gin.PostingChunkTable != "dg_gin_docs_doc_idx_posting_chunks" {
		t.Fatalf("unexpected gin posting chunk table: %#v", metadata.Gin)
	}
	if metadata.Constraint != ConstraintNone {
		t.Fatalf("unexpected constraint marker: %q", metadata.Constraint)
	}
	if Constraint(comment) != ConstraintNone {
		t.Fatalf("unexpected constraint accessor value: %q", Constraint(comment))
	}
	if !IsStandaloneIndex(comment) {
		t.Fatal("expected standalone index marker")
	}
}

func TestGinPostingMetadataTrimsChunkTable(t *testing.T) {
	comment := commentPrefix + `{"accessMethod":"gin","gin":{"postingChunkTable":"dg_gin_docs_doc_idx_posting_chunks"}}`

	metadata, ok := DecodeComment(comment)
	if !ok {
		t.Fatal("expected metadata comment to decode")
	}
	if metadata.Gin == nil {
		t.Fatal("expected gin metadata")
	}
	if got := metadata.Gin.PostingChunkTable; got != "dg_gin_docs_doc_idx_posting_chunks" {
		t.Fatalf("expected gin posting chunk table, got %q", got)
	}
}

func TestDecodeCommentRejectsPlainComments(t *testing.T) {
	if _, ok := DecodeComment("ordinary user comment"); ok {
		t.Fatal("expected ordinary comments to be ignored")
	}
	if IsStandaloneIndex("ordinary user comment") {
		t.Fatal("expected ordinary comments to not mark standalone indexes")
	}
}

func TestAccessMethodFallbacks(t *testing.T) {
	if got := NormalizeAccessMethod(""); got != AccessMethodBtree {
		t.Fatalf("expected omitted access method to default to btree, got %q", got)
	}
	if got := AccessMethod("BTREE", "ordinary user comment"); got != AccessMethodBtree {
		t.Fatalf("expected native index type fallback to normalize to btree, got %q", got)
	}
}

func TestIsSupportedGinJsonbOpClass(t *testing.T) {
	for _, opClass := range []string{OpClassJsonbOps, OpClassJsonbPathOps, "JSONB_OPS"} {
		if !IsSupportedGinJsonbOpClass(opClass) {
			t.Fatalf("expected %q to be supported", opClass)
		}
	}
	if IsSupportedGinJsonbOpClass("text_ops") {
		t.Fatal("expected text_ops to be unsupported")
	}
}

func TestIsSupportedBtreeOpClass(t *testing.T) {
	for _, opClass := range []string{"int4_ops", "TEXT_OPS", " uuid_ops ", "time_ops", "pg_lsn_ops", OpClassJsonbOps} {
		if !IsSupportedBtreeOpClass(opClass) {
			t.Fatalf("expected %q to be supported", opClass)
		}
	}
	if IsSupportedBtreeOpClass(OpClassJsonbPathOps) {
		t.Fatal("expected jsonb_path_ops to be unsupported for btree")
	}
}

func TestBtreeOpClassAcceptsType(t *testing.T) {
	tests := []struct {
		name        string
		opClass     string
		typ         *pgtypes.DoltgresType
		displayName string
		accepted    bool
	}{
		{
			name:        "int4_ops_accepts_int4",
			opClass:     "int4_ops",
			typ:         pgtypes.Int32,
			displayName: "integer",
			accepted:    true,
		},
		{
			name:        "text_ops_rejects_int4",
			opClass:     "text_ops",
			typ:         pgtypes.Int32,
			displayName: "integer",
		},
		{
			name:        "int4_ops_rejects_text",
			opClass:     "int4_ops",
			typ:         pgtypes.Text,
			displayName: "text",
		},
		{
			name:        "jsonb_ops_accepts_jsonb",
			opClass:     OpClassJsonbOps,
			typ:         pgtypes.JsonB,
			displayName: "jsonb",
			accepted:    true,
		},
		{
			name:        "text_ops_accepts_varchar",
			opClass:     "text_ops",
			typ:         pgtypes.VarChar,
			displayName: "character varying",
			accepted:    true,
		},
		{
			name:        "bpchar_pattern_ops_accepts_varchar",
			opClass:     OpClassBpcharPatternOps,
			typ:         pgtypes.VarChar,
			displayName: "character varying",
			accepted:    true,
		},
		{
			name:        "varchar_pattern_ops_rejects_bpchar",
			opClass:     OpClassVarcharPatternOps,
			typ:         pgtypes.BpChar,
			displayName: "character",
		},
		{
			name:        "bit_ops_accepts_varbit",
			opClass:     "bit_ops",
			typ:         pgtypes.VarBit,
			displayName: "varbit",
			accepted:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			displayName, accepted := BtreeOpClassAcceptsType(tt.opClass, tt.typ)
			if displayName != tt.displayName {
				t.Fatalf("expected display name %q, got %q", tt.displayName, displayName)
			}
			if accepted != tt.accepted {
				t.Fatalf("expected accepted=%v, got %v", tt.accepted, accepted)
			}
		})
	}
}

func TestDefaultBtreeOpClassForType(t *testing.T) {
	tests := []struct {
		name    string
		typ     *pgtypes.DoltgresType
		opClass string
	}{
		{name: "int4", typ: pgtypes.Int32, opClass: "int4_ops"},
		{name: "text", typ: pgtypes.Text, opClass: "text_ops"},
		{name: "varchar", typ: pgtypes.VarChar, opClass: "varchar_ops"},
		{name: "bpchar", typ: pgtypes.BpChar, opClass: "bpchar_ops"},
		{name: "jsonb", typ: pgtypes.JsonB, opClass: OpClassJsonbOps},
		{name: "varbit", typ: pgtypes.VarBit, opClass: "varbit_ops"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opClass, ok := DefaultBtreeOpClassForType(tt.typ)
			if !ok {
				t.Fatal("expected default opclass")
			}
			if opClass != tt.opClass {
				t.Fatalf("expected default opclass %q, got %q", tt.opClass, opClass)
			}
		})
	}
}

func TestColumnOpClassDefinitionOmitsDefaults(t *testing.T) {
	tests := []struct {
		name          string
		accessMethod  string
		opClass       string
		logicalColumn LogicalColumn
		tableSchema   sql.Schema
		want          string
	}{
		{
			name:         "default_gin_jsonb_ops",
			accessMethod: AccessMethodGin,
			opClass:      OpClassJsonbOps,
			logicalColumn: LogicalColumn{
				Definition:  "doc",
				StorageName: "doc",
			},
			tableSchema: sql.Schema{{Name: "doc", Type: pgtypes.JsonB}},
		},
		{
			name:         "nondefault_gin_jsonb_path_ops",
			accessMethod: AccessMethodGin,
			opClass:      OpClassJsonbPathOps,
			logicalColumn: LogicalColumn{
				Definition:  "doc",
				StorageName: "doc",
			},
			tableSchema: sql.Schema{{Name: "doc", Type: pgtypes.JsonB}},
			want:        OpClassJsonbPathOps,
		},
		{
			name:         "default_btree_int4_ops",
			accessMethod: AccessMethodBtree,
			opClass:      "INT4_OPS",
			logicalColumn: LogicalColumn{
				Definition:  "id",
				StorageName: "id",
			},
			tableSchema: sql.Schema{{Name: "id", Type: pgtypes.Int32}},
		},
		{
			name:         "nondefault_btree_text_pattern_ops",
			accessMethod: AccessMethodBtree,
			opClass:      OpClassTextPatternOps,
			logicalColumn: LogicalColumn{
				Definition:  "label",
				StorageName: "label",
			},
			tableSchema: sql.Schema{{Name: "label", Type: pgtypes.Text}},
			want:        OpClassTextPatternOps,
		},
		{
			name:         "expression_preserves_opclass",
			accessMethod: AccessMethodBtree,
			opClass:      "TEXT_OPS",
			logicalColumn: LogicalColumn{
				Definition: "lower(label)",
				Expression: true,
			},
			tableSchema: sql.Schema{{Name: "label", Type: pgtypes.Text}},
			want:        "text_ops",
		},
		{
			name:         "unknown_column_preserves_opclass",
			accessMethod: AccessMethodGin,
			opClass:      OpClassJsonbOps,
			logicalColumn: LogicalColumn{
				Definition:  "doc",
				StorageName: "doc",
			},
			want: OpClassJsonbOps,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := columnOpClassDefinition(tt.accessMethod, tt.opClass, tt.logicalColumn, tt.tableSchema)
			if got != tt.want {
				t.Fatalf("expected opclass definition %q, got %q", tt.want, got)
			}
		})
	}
}

func TestIsSupportedCollation(t *testing.T) {
	for _, collation := range []string{"default", `"C"`, "und-x-icu"} {
		if !IsSupportedCollation(collation) {
			t.Fatalf("expected %q to be supported", collation)
		}
	}
	if IsSupportedCollation("definitely-not-a-collation") {
		t.Fatal("expected unknown collation to be unsupported")
	}
}

func TestBuildStateDefaultsToReadyAndValid(t *testing.T) {
	// A fresh metadata with no build flags should report as ready+valid
	// — that is the steady state for every normal CREATE INDEX.
	comment := EncodeComment(Metadata{AccessMethod: "btree"})
	if !IsReady(comment) {
		t.Fatal("default-state index should report as ready (writers maintain it)")
	}
	if !IsValid(comment) {
		t.Fatal("default-state index should report as valid (planner may use it)")
	}

	// An index whose author never wrote build-state bits at all (the
	// pre-CONCURRENTLY format) must keep deserializing as ready+valid.
	legacy := commentPrefix + `{"accessMethod":"btree"}`
	if !IsReady(legacy) {
		t.Fatal("legacy comment should default to ready")
	}
	if !IsValid(legacy) {
		t.Fatal("legacy comment should default to valid")
	}
}

func TestBuildStateRoundTrip(t *testing.T) {
	tests := []struct {
		name      string
		notReady  bool
		invalid   bool
		wantReady bool
		wantValid bool
	}{
		{name: "ready_valid", notReady: false, invalid: false, wantReady: true, wantValid: true},
		{name: "ready_invalid", notReady: false, invalid: true, wantReady: true, wantValid: false},
		{name: "notready_invalid", notReady: true, invalid: true, wantReady: false, wantValid: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			comment := EncodeComment(Metadata{
				AccessMethod: "btree",
				NotReady:     tt.notReady,
				Invalid:      tt.invalid,
			})
			metadata, ok := DecodeComment(comment)
			if !ok {
				t.Fatal("expected metadata to decode")
			}
			if metadata.NotReady != tt.notReady {
				t.Fatalf("expected NotReady=%v, got %v", tt.notReady, metadata.NotReady)
			}
			if metadata.Invalid != tt.invalid {
				t.Fatalf("expected Invalid=%v, got %v", tt.invalid, metadata.Invalid)
			}
			if got := IsReady(comment); got != tt.wantReady {
				t.Fatalf("expected IsReady=%v, got %v", tt.wantReady, got)
			}
			if got := IsValid(comment); got != tt.wantValid {
				t.Fatalf("expected IsValid=%v, got %v", tt.wantValid, got)
			}
		})
	}
}

func TestIndOptionValue(t *testing.T) {
	tests := []struct {
		name   string
		option IndexColumnOption
		want   int16
	}{
		{
			name: "default",
			want: 0,
		},
		{
			name:   "desc_defaults_to_nulls_first",
			option: IndexColumnOption{Direction: SortDirectionDesc},
			want:   IndOptionDesc | IndOptionNullsFirst,
		},
		{
			name:   "asc_nulls_first",
			option: IndexColumnOption{NullsOrder: NullsOrderFirst},
			want:   IndOptionNullsFirst,
		},
		{
			name:   "desc_nulls_last",
			option: IndexColumnOption{Direction: SortDirectionDesc, NullsOrder: NullsOrderLast},
			want:   IndOptionDesc,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IndOptionValue(tt.option); got != tt.want {
				t.Fatalf("expected indoption %d, got %d", tt.want, got)
			}
		})
	}
}
