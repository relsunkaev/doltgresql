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

	CollationDefault  = "default"
	CollationC        = "C"
	CollationPOSIX    = "POSIX"
	CollationUcsBasic = "ucs_basic"
	CollationUndIcu   = "und-x-icu"

	SortDirectionDesc = "desc"
	NullsOrderFirst   = "first"
	NullsOrderLast    = "last"

	ConstraintNone = "none"

	IndOptionDesc       int16 = 1
	IndOptionNullsFirst int16 = 2

	commentPrefix = "doltgres:index-metadata:v1:"
)

var supportedBtreeOpClasses = map[string]struct{}{
	"bool_ops":        {},
	"int2_ops":        {},
	"int4_ops":        {},
	"int8_ops":        {},
	"float4_ops":      {},
	"float8_ops":      {},
	"numeric_ops":     {},
	"text_ops":        {},
	"varchar_ops":     {},
	"bpchar_ops":      {},
	"date_ops":        {},
	"timestamp_ops":   {},
	"timestamptz_ops": {},
	"uuid_ops":        {},
}

var supportedCollations = map[string]struct{}{
	CollationDefault:  {},
	CollationC:        {},
	CollationPOSIX:    {},
	CollationUcsBasic: {},
	CollationUndIcu:   {},
}

// Metadata stores PostgreSQL index metadata that Dolt's native index metadata
// does not currently expose.
type Metadata struct {
	AccessMethod string              `json:"accessMethod,omitempty"`
	Columns      []string            `json:"columns,omitempty"`
	Collations   []string            `json:"collations,omitempty"`
	OpClasses    []string            `json:"opClasses,omitempty"`
	SortOptions  []IndexColumnOption `json:"sortOptions,omitempty"`
	Constraint   string              `json:"constraint,omitempty"`
	Gin          *GinMetadata        `json:"gin,omitempty"`
}

// IndexColumnOption stores PostgreSQL per-column index options.
type IndexColumnOption struct {
	Direction  string `json:"direction,omitempty"`
	NullsOrder string `json:"nullsOrder,omitempty"`
}

// GinMetadata stores durable metadata for PostgreSQL GIN indexes.
type GinMetadata struct {
	PostingTable string `json:"postingTable,omitempty"`
}

// EncodeComment returns a durable index comment containing PostgreSQL metadata.
func EncodeComment(metadata Metadata) string {
	metadata.AccessMethod = NormalizeAccessMethod(metadata.AccessMethod)
	for i := range metadata.Columns {
		metadata.Columns[i] = strings.TrimSpace(metadata.Columns[i])
	}
	for i := range metadata.Collations {
		metadata.Collations[i] = NormalizeCollation(metadata.Collations[i])
	}
	for i := range metadata.OpClasses {
		metadata.OpClasses[i] = NormalizeOpClass(metadata.OpClasses[i])
	}
	normalizeSortOptions(metadata.SortOptions)
	metadata.Constraint = NormalizeConstraint(metadata.Constraint)
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
	for i := range metadata.Columns {
		metadata.Columns[i] = strings.TrimSpace(metadata.Columns[i])
	}
	for i := range metadata.Collations {
		metadata.Collations[i] = NormalizeCollation(metadata.Collations[i])
	}
	for i := range metadata.OpClasses {
		metadata.OpClasses[i] = NormalizeOpClass(metadata.OpClasses[i])
	}
	normalizeSortOptions(metadata.SortOptions)
	metadata.Constraint = NormalizeConstraint(metadata.Constraint)
	return metadata, true
}

// IsStandaloneIndex returns whether the comment marks the index as not owned by
// a PostgreSQL constraint.
func IsStandaloneIndex(comment string) bool {
	metadata, ok := DecodeComment(comment)
	return ok && metadata.Constraint == ConstraintNone
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

// NormalizeConstraint normalizes the PostgreSQL constraint ownership marker.
func NormalizeConstraint(constraint string) string {
	return strings.ToLower(strings.TrimSpace(constraint))
}

// NormalizeOpClass lower-cases PostgreSQL opclass names.
func NormalizeOpClass(opClass string) string {
	return strings.ToLower(strings.TrimSpace(opClass))
}

// NormalizeCollation trims PostgreSQL collation names and preserves the
// canonical spelling for built-in names whose OIDs are case-sensitive.
func NormalizeCollation(collation string) string {
	collation = strings.Trim(strings.TrimSpace(collation), `"`)
	switch strings.ToLower(collation) {
	case "":
		return ""
	case "default":
		return CollationDefault
	case "c":
		return CollationC
	case "posix":
		return CollationPOSIX
	case "ucs_basic":
		return CollationUcsBasic
	case "und-x-icu":
		return CollationUndIcu
	default:
		return strings.TrimSpace(collation)
	}
}

func normalizeSortOptions(sortOptions []IndexColumnOption) {
	for i := range sortOptions {
		sortOptions[i].Direction = strings.ToLower(strings.TrimSpace(sortOptions[i].Direction))
		sortOptions[i].NullsOrder = strings.ToLower(strings.TrimSpace(sortOptions[i].NullsOrder))
	}
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

// Collations returns the PostgreSQL collations encoded for an index.
func Collations(comment string) []string {
	metadata, ok := DecodeComment(comment)
	if !ok {
		return nil
	}
	return metadata.Collations
}

// Columns returns the PostgreSQL logical columns encoded for an index.
func Columns(comment string) []string {
	metadata, ok := DecodeComment(comment)
	if !ok {
		return nil
	}
	return metadata.Columns
}

// SortOptions returns the PostgreSQL sort/null options encoded for an index.
func SortOptions(comment string) []IndexColumnOption {
	metadata, ok := DecodeComment(comment)
	if !ok {
		return nil
	}
	return metadata.SortOptions
}

// IndOptionValues returns pg_index.indoption-compatible bit values.
func IndOptionValues(comment string, columnCount int) []any {
	values := make([]any, columnCount)
	for i := range values {
		values[i] = int16(0)
	}
	sortOptions := SortOptions(comment)
	for i := 0; i < len(sortOptions) && i < len(values); i++ {
		values[i] = IndOptionValue(sortOptions[i])
	}
	return values
}

// IndOptionValue returns PostgreSQL's pg_index.indoption bit flags for a column.
func IndOptionValue(option IndexColumnOption) int16 {
	option.Direction = strings.ToLower(strings.TrimSpace(option.Direction))
	option.NullsOrder = strings.ToLower(strings.TrimSpace(option.NullsOrder))
	var value int16
	if option.Direction == SortDirectionDesc {
		value |= IndOptionDesc
	}
	if option.NullsOrder == NullsOrderFirst || (option.NullsOrder == "" && option.Direction == SortDirectionDesc) {
		value |= IndOptionNullsFirst
	}
	return value
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

// IsSupportedBtreeOpClass returns whether opClass is a catalog-visible btree
// opclass whose metadata Doltgres can preserve for ordinary btree indexes.
func IsSupportedBtreeOpClass(opClass string) bool {
	_, ok := supportedBtreeOpClasses[NormalizeOpClass(opClass)]
	return ok
}

// IsSupportedCollation returns whether Doltgres can preserve this built-in
// PostgreSQL collation in index metadata.
func IsSupportedCollation(collation string) bool {
	_, ok := supportedCollations[NormalizeCollation(collation)]
	return ok
}
