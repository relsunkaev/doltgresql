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
	"encoding/json"
	"strings"
)

const (
	AccessMethodBtree = "btree"
	AccessMethodGin   = "gin"

	OpClassJsonbOps     = "jsonb_ops"
	OpClassJsonbPathOps = "jsonb_path_ops"

	commentPrefix = "doltgres:index-metadata:v1:"
)

// Metadata stores PostgreSQL index metadata that Dolt's native index metadata
// does not currently expose.
type Metadata struct {
	AccessMethod string   `json:"accessMethod,omitempty"`
	OpClasses    []string `json:"opClasses,omitempty"`
}

// EncodeComment returns a durable index comment containing PostgreSQL metadata.
func EncodeComment(metadata Metadata) string {
	metadata.AccessMethod = NormalizeAccessMethod(metadata.AccessMethod)
	for i := range metadata.OpClasses {
		metadata.OpClasses[i] = NormalizeOpClass(metadata.OpClasses[i])
	}
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
	metadata.AccessMethod = NormalizeAccessMethod(metadata.AccessMethod)
	for i := range metadata.OpClasses {
		metadata.OpClasses[i] = NormalizeOpClass(metadata.OpClasses[i])
	}
	return metadata, true
}

// NormalizeAccessMethod lower-cases PostgreSQL access method names and applies
// the PostgreSQL default for omitted access methods.
func NormalizeAccessMethod(method string) string {
	method = strings.ToLower(strings.TrimSpace(method))
	if method == "" {
		return AccessMethodBtree
	}
	return method
}

// NormalizeOpClass lower-cases PostgreSQL opclass names.
func NormalizeOpClass(opClass string) string {
	return strings.ToLower(strings.TrimSpace(opClass))
}

// AccessMethod returns the PostgreSQL access method for an index. If no
// Doltgres metadata is present, the index's native type is used.
func AccessMethod(indexType string, comment string) string {
	if metadata, ok := DecodeComment(comment); ok && metadata.AccessMethod != "" {
		return metadata.AccessMethod
	}
	return NormalizeAccessMethod(indexType)
}

// OpClasses returns the PostgreSQL opclasses encoded for an index.
func OpClasses(comment string) []string {
	metadata, ok := DecodeComment(comment)
	if !ok {
		return nil
	}
	return metadata.OpClasses
}

// IsSupportedGinJsonbOpClass returns whether opClass is a supported JSONB GIN
// opclass in the current first-pass metadata implementation.
func IsSupportedGinJsonbOpClass(opClass string) bool {
	switch NormalizeOpClass(opClass) {
	case OpClassJsonbOps, OpClassJsonbPathOps:
		return true
	default:
		return false
	}
}
