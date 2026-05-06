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

import "testing"

func TestEncodeDecodeComment(t *testing.T) {
	comment := EncodeComment(Metadata{
		AccessMethod:      "GIN",
		Columns:           []string{" lower(doc) "},
		StorageColumns:    []string{" doc "},
		ExpressionColumns: []bool{true},
		IncludeColumns:    []string{" doc_id ", " title "},
		Collations:        []string{` "C" `, "und-x-icu"},
		OpClasses:         []string{" JSONB_OPS ", "jsonb_path_ops"},
		RelOptions:        []string{" FILLFACTOR = 70 "},
		SortOptions: []IndexColumnOption{
			{Direction: " DESC "},
			{NullsOrder: " FIRST "},
			{NullsOrder: " LAST "},
		},
		Constraint: " NONE ",
		Gin: &GinMetadata{
			PostingTable: "dg_gin_docs_doc_idx_postings",
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
	if metadata.Constraint != ConstraintNone {
		t.Fatalf("unexpected constraint marker: %q", metadata.Constraint)
	}
	if !IsStandaloneIndex(comment) {
		t.Fatal("expected standalone index marker")
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
	for _, opClass := range []string{"int4_ops", "TEXT_OPS", " uuid_ops "} {
		if !IsSupportedBtreeOpClass(opClass) {
			t.Fatalf("expected %q to be supported", opClass)
		}
	}
	if IsSupportedBtreeOpClass(OpClassJsonbOps) {
		t.Fatal("expected jsonb_ops to be unsupported for btree")
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
