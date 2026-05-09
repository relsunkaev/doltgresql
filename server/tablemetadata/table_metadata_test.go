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

package tablemetadata

import "testing"

func TestPrimaryKeyConstraintNameMetadata(t *testing.T) {
	comment := SetPrimaryKeyConstraintName("", " custom_pkey ")
	if got := PrimaryKeyConstraintName(comment); got != "custom_pkey" {
		t.Fatalf("expected custom_pkey, got %q", got)
	}

	metadata, ok := DecodeComment(comment)
	if !ok {
		t.Fatalf("expected encoded metadata comment to decode")
	}
	if metadata.PrimaryKeyConstraint != "custom_pkey" {
		t.Fatalf("expected decoded constraint name custom_pkey, got %q", metadata.PrimaryKeyConstraint)
	}

	if got := SetPrimaryKeyConstraintName(comment, ""); got != "" {
		t.Fatalf("expected clearing the only metadata value to clear the comment, got %q", got)
	}
}

func TestMaterializedViewMetadata(t *testing.T) {
	comment := SetMaterializedViewDefinition("", " SELECT id FROM source ")
	if !IsMaterializedView(comment) {
		t.Fatalf("expected materialized view metadata")
	}
	if got := MaterializedViewDefinition(comment); got != "SELECT id FROM source" {
		t.Fatalf("expected trimmed materialized view definition, got %q", got)
	}

	comment = SetPrimaryKeyConstraintName(comment, "custom_pkey")
	if got := PrimaryKeyConstraintName(comment); got != "custom_pkey" {
		t.Fatalf("expected primary key metadata to be preserved, got %q", got)
	}
	comment = SetPrimaryKeyConstraintName(comment, "")
	if !IsMaterializedView(comment) {
		t.Fatalf("expected clearing primary-key metadata to preserve materialized view metadata")
	}
}

func TestDecodeCommentRejectsPlainComments(t *testing.T) {
	if _, ok := DecodeComment("plain table comment"); ok {
		t.Fatalf("expected plain comments to be ignored")
	}
	if got := PrimaryKeyConstraintName("plain table comment"); got != "" {
		t.Fatalf("expected no primary key constraint name for plain comments, got %q", got)
	}
	if IsMaterializedView("plain table comment") {
		t.Fatalf("expected no materialized view metadata for plain comments")
	}
}
