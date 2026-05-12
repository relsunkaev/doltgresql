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

import (
	"testing"

	"github.com/dolthub/doltgresql/core/id"
)

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
	if !IsMaterializedViewPopulated(comment) {
		t.Fatalf("expected materialized view metadata to default to populated")
	}

	comment = SetMaterializedViewDefinitionWithPopulated(comment, " SELECT id FROM source ", false)
	if !IsMaterializedView(comment) {
		t.Fatalf("expected unpopulated materialized view metadata")
	}
	if IsMaterializedViewPopulated(comment) {
		t.Fatalf("expected materialized view metadata to be unpopulated")
	}
	if got := MaterializedViewDefinition(comment); got != "SELECT id FROM source" {
		t.Fatalf("expected unpopulated materialized view definition to be preserved, got %q", got)
	}

	comment = SetMaterializedViewDefinitionWithPopulated(comment, "SELECT id FROM source", true)
	if !IsMaterializedViewPopulated(comment) {
		t.Fatalf("expected materialized view metadata to be populated")
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

func TestOfTypeMetadata(t *testing.T) {
	typeID := id.NewType("public", "typed_person")
	comment := SetOfType("", typeID)

	got, ok := OfType(comment)
	if !ok {
		t.Fatalf("expected typed-table metadata")
	}
	if got != typeID {
		t.Fatalf("expected type ID %q, got %q", typeID, got)
	}

	comment = SetPrimaryKeyConstraintName(comment, "typed_people_pkey")
	got, ok = OfType(comment)
	if !ok || got != typeID {
		t.Fatalf("expected typed-table metadata to be preserved, got %q, %v", got, ok)
	}
	if got := PrimaryKeyConstraintName(comment); got != "typed_people_pkey" {
		t.Fatalf("expected primary-key metadata to be preserved, got %q", got)
	}
}

func TestRelOptionsMetadata(t *testing.T) {
	comment := SetRelOptions("", []string{" FILLFACTOR = 40 ", "autovacuum_enabled=false"})
	got := RelOptions(comment)
	if len(got) != 2 || got[0] != "fillfactor=40" || got[1] != "autovacuum_enabled=false" {
		t.Fatalf("unexpected reloptions: %#v", got)
	}

	comment = SetPrimaryKeyConstraintName(comment, "items_pkey")
	merged := MergeRelOptions(RelOptions(comment), []string{"fillfactor=70", "autovacuum_analyze_scale_factor=0.2"})
	comment = SetRelOptions(comment, merged)
	got = RelOptions(comment)
	if len(got) != 3 ||
		got[0] != "fillfactor=70" ||
		got[1] != "autovacuum_enabled=false" ||
		got[2] != "autovacuum_analyze_scale_factor=0.2" {
		t.Fatalf("unexpected merged reloptions: %#v", got)
	}
	if gotName := PrimaryKeyConstraintName(comment); gotName != "items_pkey" {
		t.Fatalf("expected primary key metadata to be preserved, got %q", gotName)
	}

	comment = SetRelOptions(comment, ResetRelOptions(RelOptions(comment), []string{"fillfactor"}))
	got = RelOptions(comment)
	if len(got) != 2 || got[0] != "autovacuum_enabled=false" || got[1] != "autovacuum_analyze_scale_factor=0.2" {
		t.Fatalf("unexpected reset reloptions: %#v", got)
	}

	comment = SetPrimaryKeyConstraintName(comment, "")
	comment = SetRelOptions(comment, nil)
	if comment != "" {
		t.Fatalf("expected clearing only metadata to clear the comment, got %q", comment)
	}
}

func TestRelPersistenceMetadata(t *testing.T) {
	comment := SetRelPersistence("", "u")
	if got := RelPersistence(comment); got != "u" {
		t.Fatalf("unexpected relpersistence: %q", got)
	}

	comment = SetPrimaryKeyConstraintName(comment, "items_pkey")
	if got := RelPersistence(comment); got != "u" {
		t.Fatalf("expected relpersistence metadata to be preserved, got %q", got)
	}
	if got := PrimaryKeyConstraintName(comment); got != "items_pkey" {
		t.Fatalf("expected primary key metadata to be preserved, got %q", got)
	}

	comment = SetPrimaryKeyConstraintName(comment, "")
	comment = SetRelPersistence(comment, "p")
	if comment != "" {
		t.Fatalf("expected clearing only metadata to clear the comment, got %q", comment)
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
