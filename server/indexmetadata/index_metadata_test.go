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
		AccessMethod: "GIN",
		Columns:      []string{" doc "},
		OpClasses:    []string{" JSONB_OPS ", "jsonb_path_ops"},
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
	if len(metadata.Columns) != 1 || metadata.Columns[0] != "doc" {
		t.Fatalf("unexpected columns: %#v", metadata.Columns)
	}
	if len(metadata.OpClasses) != 2 {
		t.Fatalf("expected 2 opclasses, got %d", len(metadata.OpClasses))
	}
	if metadata.OpClasses[0] != OpClassJsonbOps || metadata.OpClasses[1] != OpClassJsonbPathOps {
		t.Fatalf("unexpected opclasses: %#v", metadata.OpClasses)
	}
	if metadata.Gin == nil || metadata.Gin.PostingTable != "dg_gin_docs_doc_idx_postings" {
		t.Fatalf("unexpected gin metadata: %#v", metadata.Gin)
	}
}

func TestDecodeCommentRejectsPlainComments(t *testing.T) {
	if _, ok := DecodeComment("ordinary user comment"); ok {
		t.Fatal("expected ordinary comments to be ignored")
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
