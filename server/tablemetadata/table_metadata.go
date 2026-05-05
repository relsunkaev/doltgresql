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
	PrimaryKeyConstraint string `json:"primaryKeyConstraint,omitempty"`
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
	if metadata.PrimaryKeyConstraint == "" {
		return ""
	}
	return EncodeComment(metadata)
}
