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

	"github.com/dolthub/doltgresql/core/id"
)

const commentPrefix = "doltgres:table-metadata:v1:"

// Metadata stores PostgreSQL table metadata that Dolt's native schema metadata
// does not currently expose.
type Metadata struct {
	PrimaryKeyConstraint        string   `json:"primaryKeyConstraint,omitempty"`
	MaterializedView            bool     `json:"materializedView,omitempty"`
	MaterializedViewDefinition  string   `json:"materializedViewDefinition,omitempty"`
	MaterializedViewUnpopulated bool     `json:"materializedViewUnpopulated,omitempty"`
	OfTypeSchema                string   `json:"ofTypeSchema,omitempty"`
	OfTypeName                  string   `json:"ofTypeName,omitempty"`
	RelOptions                  []string `json:"relOptions,omitempty"`
	RelPersistence              string   `json:"relPersistence,omitempty"`
}

// EncodeComment returns a durable table comment containing PostgreSQL metadata.
func EncodeComment(metadata Metadata) string {
	metadata.PrimaryKeyConstraint = strings.TrimSpace(metadata.PrimaryKeyConstraint)
	NormalizeRelOptions(metadata.RelOptions)
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
	NormalizeRelOptions(metadata.RelOptions)
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

// SetOfType records the composite type referenced by CREATE TABLE ... OF.
func SetOfType(comment string, typeID id.Type) string {
	metadata, _ := DecodeComment(comment)
	metadata.OfTypeSchema = strings.TrimSpace(typeID.SchemaName())
	metadata.OfTypeName = strings.TrimSpace(typeID.TypeName())
	if metadata.empty() {
		return ""
	}
	return EncodeComment(metadata)
}

// OfType returns the composite type referenced by CREATE TABLE ... OF.
func OfType(comment string) (id.Type, bool) {
	metadata, ok := DecodeComment(comment)
	if !ok || metadata.OfTypeName == "" {
		return id.NullType, false
	}
	return id.NewType(metadata.OfTypeSchema, metadata.OfTypeName), true
}

// RelOptions returns the PostgreSQL reloptions encoded in a Doltgres table
// metadata comment.
func RelOptions(comment string) []string {
	metadata, ok := DecodeComment(comment)
	if !ok {
		return nil
	}
	return metadata.RelOptions
}

// RelPersistence returns the PostgreSQL relpersistence value encoded in a
// Doltgres table metadata comment.
func RelPersistence(comment string) string {
	metadata, ok := DecodeComment(comment)
	if !ok {
		return ""
	}
	return metadata.RelPersistence
}

// SetRelPersistence returns a table metadata comment with the given
// PostgreSQL relpersistence value. Empty or permanent persistence clears only
// the relpersistence metadata.
func SetRelPersistence(comment string, relPersistence string) string {
	metadata, _ := DecodeComment(comment)
	if relPersistence == "p" {
		relPersistence = ""
	}
	metadata.RelPersistence = strings.TrimSpace(relPersistence)
	if metadata.empty() {
		return ""
	}
	return EncodeComment(metadata)
}

// SetRelOptions returns a table metadata comment with the given PostgreSQL
// reloptions. An empty reloptions slice clears only the reloptions metadata.
func SetRelOptions(comment string, relOptions []string) string {
	metadata, _ := DecodeComment(comment)
	metadata.RelOptions = append([]string(nil), relOptions...)
	NormalizeRelOptions(metadata.RelOptions)
	if metadata.empty() {
		return ""
	}
	return EncodeComment(metadata)
}

// MergeRelOptions applies updates to an existing reloptions slice while
// preserving first-seen key order.
func MergeRelOptions(existing []string, updates []string) []string {
	values := make(map[string]string, len(existing)+len(updates))
	order := make([]string, 0, len(existing)+len(updates))
	for _, option := range existing {
		key, value, ok := SplitRelOption(option)
		if !ok {
			continue
		}
		if _, exists := values[key]; !exists {
			order = append(order, key)
		}
		values[key] = value
	}
	for _, option := range updates {
		key, value, ok := SplitRelOption(option)
		if !ok {
			continue
		}
		if _, exists := values[key]; !exists {
			order = append(order, key)
		}
		values[key] = value
	}
	ret := make([]string, 0, len(order))
	for _, key := range order {
		ret = append(ret, key+"="+values[key])
	}
	return ret
}

// ResetRelOptions removes the named keys from an existing reloptions slice.
func ResetRelOptions(existing []string, resetKeys []string) []string {
	reset := make(map[string]struct{}, len(resetKeys))
	for _, key := range resetKeys {
		reset[strings.ToLower(strings.TrimSpace(key))] = struct{}{}
	}
	ret := make([]string, 0, len(existing))
	for _, option := range existing {
		key, _, ok := SplitRelOption(option)
		if !ok {
			continue
		}
		if _, remove := reset[key]; remove {
			continue
		}
		ret = append(ret, option)
	}
	return ret
}

// SplitRelOption returns a normalized key and trimmed value from a reloption
// string in key=value form.
func SplitRelOption(option string) (key string, value string, ok bool) {
	key, value, ok = strings.Cut(strings.TrimSpace(option), "=")
	if !ok {
		return "", "", false
	}
	return strings.ToLower(strings.TrimSpace(key)), strings.TrimSpace(value), true
}

// NormalizeRelOptions normalizes reloptions in place.
func NormalizeRelOptions(relOptions []string) {
	for i, option := range relOptions {
		key, value, ok := SplitRelOption(option)
		if !ok {
			continue
		}
		relOptions[i] = key + "=" + value
	}
}

func (metadata Metadata) empty() bool {
	return metadata.PrimaryKeyConstraint == "" &&
		!metadata.MaterializedView &&
		metadata.MaterializedViewDefinition == "" &&
		!metadata.MaterializedViewUnpopulated &&
		metadata.OfTypeSchema == "" &&
		metadata.OfTypeName == "" &&
		metadata.RelPersistence == "" &&
		len(metadata.RelOptions) == 0
}
