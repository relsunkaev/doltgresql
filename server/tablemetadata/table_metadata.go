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
	"encoding/json"
	"strings"
)

const commentPrefix = "doltgres:table-metadata:v1:"

// Metadata stores PostgreSQL table metadata that Dolt's native schema metadata
// does not currently expose.
type Metadata struct {
	PrimaryKeyConstraint        string `json:"primaryKeyConstraint,omitempty"`
	MaterializedView            bool   `json:"materializedView,omitempty"`
	MaterializedViewDefinition  string `json:"materializedViewDefinition,omitempty"`
	MaterializedViewUnpopulated bool   `json:"materializedViewUnpopulated,omitempty"`
}

// EncodeComment returns a durable table comment containing PostgreSQL metadata.
func EncodeComment(metadata Metadata) string {
	metadata.PrimaryKeyConstraint = strings.TrimSpace(metadata.PrimaryKeyConstraint)
	encoded, _ := json.Marshal(metadata)
	return commentPrefix + string(encoded)
}

// DecodeComment returns metadata encoded by EncodeComment.
func DecodeComment(comment string) (Metadata, bool) {
	if !strings.HasPrefix(comment, commentPrefix) {
		return Metadata{}, false
	}
	var metadata Metadata
	if err := json.Unmarshal([]byte(strings.TrimPrefix(comment, commentPrefix)), &metadata); err != nil {
		return Metadata{}, false
	}
	metadata.PrimaryKeyConstraint = strings.TrimSpace(metadata.PrimaryKeyConstraint)
	metadata.MaterializedViewDefinition = strings.TrimSpace(metadata.MaterializedViewDefinition)
	return metadata, true
}

// PrimaryKeyConstraintName returns the PostgreSQL primary-key constraint name
// encoded in a Doltgres table comment.
func PrimaryKeyConstraintName(comment string) string {
	metadata, ok := DecodeComment(comment)
	if !ok {
		return ""
	}
	return metadata.PrimaryKeyConstraint
}

// SetPrimaryKeyConstraintName returns a table metadata comment with the given
// PostgreSQL primary-key constraint name. An empty name clears Doltgres table
// metadata when no other table metadata is present.
func SetPrimaryKeyConstraintName(comment string, name string) string {
	metadata, _ := DecodeComment(comment)
	metadata.PrimaryKeyConstraint = strings.TrimSpace(name)
	if metadata.empty() {
		return ""
	}
	return EncodeComment(metadata)
}

// SetMaterializedViewDefinition marks the table metadata comment as a
// table-backed materialized view and records the view query definition.
func SetMaterializedViewDefinition(comment string, definition string) string {
	return SetMaterializedViewDefinitionWithPopulated(comment, definition, true)
}

// SetMaterializedViewDefinitionWithPopulated marks the table metadata comment
// as a table-backed materialized view, records the view query definition, and
// stores whether the snapshot is currently populated.
func SetMaterializedViewDefinitionWithPopulated(comment string, definition string, populated bool) string {
	metadata, _ := DecodeComment(comment)
	metadata.MaterializedView = true
	metadata.MaterializedViewDefinition = strings.TrimSpace(definition)
	metadata.MaterializedViewUnpopulated = !populated
	return EncodeComment(metadata)
}

// IsMaterializedView returns whether the comment marks a table-backed
// materialized view.
func IsMaterializedView(comment string) bool {
	metadata, ok := DecodeComment(comment)
	return ok && metadata.MaterializedView
}

// MaterializedViewDefinition returns the stored materialized view query
// definition, if any.
func MaterializedViewDefinition(comment string) string {
	metadata, ok := DecodeComment(comment)
	if !ok || !metadata.MaterializedView {
		return ""
	}
	return metadata.MaterializedViewDefinition
}

// IsMaterializedViewPopulated returns whether the metadata marks the
// materialized view as scan-ready. Legacy materialized-view comments predate
// this flag and are treated as populated.
func IsMaterializedViewPopulated(comment string) bool {
	metadata, ok := DecodeComment(comment)
	return ok && metadata.MaterializedView && !metadata.MaterializedViewUnpopulated
}

func (metadata Metadata) empty() bool {
	return metadata.PrimaryKeyConstraint == "" &&
		!metadata.MaterializedView &&
		metadata.MaterializedViewDefinition == "" &&
		!metadata.MaterializedViewUnpopulated
}
