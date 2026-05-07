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
	"reflect"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	pgtypes "github.com/dolthub/doltgresql/server/types"
)

const (
	AccessMethodBtree = "btree"
	AccessMethodGin   = "gin"

	OpClassJsonbOps     = "jsonb_ops"
	OpClassJsonbPathOps = "jsonb_path_ops"

	OpClassTextPatternOps    = "text_pattern_ops"
	OpClassVarcharPatternOps = "varchar_pattern_ops"
	OpClassBpcharPatternOps  = "bpchar_pattern_ops"

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
	"bit_ops":                {},
	"bool_ops":               {},
	"int2_ops":               {},
	"int4_ops":               {},
	"int8_ops":               {},
	"float4_ops":             {},
	"float8_ops":             {},
	"numeric_ops":            {},
	"char_ops":               {},
	"name_ops":               {},
	"text_ops":               {},
	"varchar_ops":            {},
	"bpchar_ops":             {},
	"bytea_ops":              {},
	OpClassTextPatternOps:    {},
	OpClassVarcharPatternOps: {},
	OpClassBpcharPatternOps:  {},
	"date_ops":               {},
	"interval_ops":           {},
	OpClassJsonbOps:          {},
	"oid_ops":                {},
	"oidvector_ops":          {},
	"pg_lsn_ops":             {},
	"time_ops":               {},
	"timestamp_ops":          {},
	"timestamptz_ops":        {},
	"timetz_ops":             {},
	"uuid_ops":               {},
	"varbit_ops":             {},
}

var btreeOpClassInputTypes = map[string]map[string]struct{}{
	"bit_ops":                set("bit", "varbit"),
	"bool_ops":               set("bool"),
	"int2_ops":               set("int2"),
	"int4_ops":               set("int4"),
	"int8_ops":               set("int8"),
	"float4_ops":             set("float4"),
	"float8_ops":             set("float8"),
	"numeric_ops":            set("numeric"),
	"char_ops":               set("char"),
	"name_ops":               set("name"),
	"text_ops":               set("text", "varchar"),
	"varchar_ops":            set("text", "varchar"),
	"bpchar_ops":             set("text", "varchar", "bpchar"),
	"bytea_ops":              set("bytea"),
	OpClassTextPatternOps:    set("text", "varchar"),
	OpClassVarcharPatternOps: set("text", "varchar"),
	OpClassBpcharPatternOps:  set("text", "varchar", "bpchar"),
	"date_ops":               set("date"),
	"interval_ops":           set("interval"),
	OpClassJsonbOps:          set("jsonb"),
	"oid_ops":                set("oid", "int4"),
	"oidvector_ops":          set("oidvector"),
	"pg_lsn_ops":             set("pg_lsn"),
	"time_ops":               set("time"),
	"timestamp_ops":          set("timestamp"),
	"timestamptz_ops":        set("timestamptz"),
	"timetz_ops":             set("timetz"),
	"uuid_ops":               set("uuid"),
	"varbit_ops":             set("bit", "varbit"),
}

var supportedCollations = map[string]struct{}{
	CollationDefault:  {},
	CollationC:        {},
	CollationPOSIX:    {},
	CollationUcsBasic: {},
	CollationUndIcu:   {},
}

func set(values ...string) map[string]struct{} {
	ret := make(map[string]struct{}, len(values))
	for _, value := range values {
		ret[value] = struct{}{}
	}
	return ret
}

// Metadata stores PostgreSQL index metadata that Dolt's native index metadata
// does not currently expose.
type Metadata struct {
	AccessMethod      string              `json:"accessMethod,omitempty"`
	Columns           []string            `json:"columns,omitempty"`
	StorageColumns    []string            `json:"storageColumns,omitempty"`
	ExpressionColumns []bool              `json:"expressionColumns,omitempty"`
	IncludeColumns    []string            `json:"includeColumns,omitempty"`
	Predicate         string              `json:"predicate,omitempty"`
	PredicateColumns  []string            `json:"predicateColumns,omitempty"`
	Collations        []string            `json:"collations,omitempty"`
	OpClasses         []string            `json:"opClasses,omitempty"`
	RelOptions        []string            `json:"relOptions,omitempty"`
	StatisticsTargets []int16             `json:"statisticsTargets,omitempty"`
	SortOptions       []IndexColumnOption `json:"sortOptions,omitempty"`
	NullsNotDistinct  bool                `json:"nullsNotDistinct,omitempty"`
	Constraint        string              `json:"constraint,omitempty"`
	Gin               *GinMetadata        `json:"gin,omitempty"`
}

// IndexColumnOption stores PostgreSQL per-column index options.
type IndexColumnOption struct {
	Direction  string `json:"direction,omitempty"`
	NullsOrder string `json:"nullsOrder,omitempty"`
}

// GinMetadata stores durable metadata for PostgreSQL GIN indexes.
type GinMetadata struct {
	PostingTable      string `json:"postingTable,omitempty"`
	PostingChunkTable string `json:"postingChunkTable,omitempty"`
}

// EncodeComment returns a durable index comment containing PostgreSQL metadata.
func EncodeComment(metadata Metadata) string {
	metadata.AccessMethod = NormalizeAccessMethod(metadata.AccessMethod)
	for i := range metadata.Columns {
		metadata.Columns[i] = strings.TrimSpace(metadata.Columns[i])
	}
	for i := range metadata.StorageColumns {
		metadata.StorageColumns[i] = strings.TrimSpace(metadata.StorageColumns[i])
	}
	for i := range metadata.IncludeColumns {
		metadata.IncludeColumns[i] = strings.TrimSpace(metadata.IncludeColumns[i])
	}
	metadata.Predicate = strings.TrimSpace(metadata.Predicate)
	for i := range metadata.PredicateColumns {
		metadata.PredicateColumns[i] = strings.TrimSpace(metadata.PredicateColumns[i])
	}
	for i := range metadata.Collations {
		metadata.Collations[i] = NormalizeCollation(metadata.Collations[i])
	}
	for i := range metadata.OpClasses {
		metadata.OpClasses[i] = NormalizeOpClass(metadata.OpClasses[i])
	}
	normalizeRelOptions(metadata.RelOptions)
	normalizeSortOptions(metadata.SortOptions)
	metadata.Constraint = NormalizeConstraint(metadata.Constraint)
	normalizeGinMetadata(metadata.Gin)
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
	for i := range metadata.StorageColumns {
		metadata.StorageColumns[i] = strings.TrimSpace(metadata.StorageColumns[i])
	}
	for i := range metadata.IncludeColumns {
		metadata.IncludeColumns[i] = strings.TrimSpace(metadata.IncludeColumns[i])
	}
	metadata.Predicate = strings.TrimSpace(metadata.Predicate)
	for i := range metadata.PredicateColumns {
		metadata.PredicateColumns[i] = strings.TrimSpace(metadata.PredicateColumns[i])
	}
	for i := range metadata.Collations {
		metadata.Collations[i] = NormalizeCollation(metadata.Collations[i])
	}
	for i := range metadata.OpClasses {
		metadata.OpClasses[i] = NormalizeOpClass(metadata.OpClasses[i])
	}
	normalizeRelOptions(metadata.RelOptions)
	normalizeSortOptions(metadata.SortOptions)
	metadata.Constraint = NormalizeConstraint(metadata.Constraint)
	normalizeGinMetadata(metadata.Gin)
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

func normalizeRelOptions(relOptions []string) {
	for i := range relOptions {
		key, value, ok := strings.Cut(strings.TrimSpace(relOptions[i]), "=")
		if !ok {
			relOptions[i] = strings.TrimSpace(relOptions[i])
			continue
		}
		relOptions[i] = strings.ToLower(strings.TrimSpace(key)) + "=" + strings.TrimSpace(value)
	}
}

func normalizeSortOptions(sortOptions []IndexColumnOption) {
	for i := range sortOptions {
		sortOptions[i].Direction = strings.ToLower(strings.TrimSpace(sortOptions[i].Direction))
		sortOptions[i].NullsOrder = strings.ToLower(strings.TrimSpace(sortOptions[i].NullsOrder))
	}
}

func normalizeGinMetadata(gin *GinMetadata) {
	if gin == nil {
		return
	}
	gin.PostingTable = strings.TrimSpace(gin.PostingTable)
	gin.PostingChunkTable = strings.TrimSpace(gin.PostingChunkTable)
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

// RelOptions returns the PostgreSQL reloptions encoded for an index.
func RelOptions(comment string) []string {
	metadata, ok := DecodeComment(comment)
	if !ok {
		return nil
	}
	return metadata.RelOptions
}

// NullsNotDistinct returns whether this unique index treats NULL values as
// equal for uniqueness checks.
func NullsNotDistinct(comment string) bool {
	metadata, ok := DecodeComment(comment)
	return ok && metadata.NullsNotDistinct
}

// StatisticsTargets returns the PostgreSQL per-attribute statistics targets encoded for an index.
func StatisticsTargets(comment string) []int16 {
	metadata, ok := DecodeComment(comment)
	if !ok {
		return nil
	}
	return metadata.StatisticsTargets
}

// Columns returns the PostgreSQL logical columns encoded for an index.
func Columns(comment string) []string {
	metadata, ok := DecodeComment(comment)
	if !ok {
		return nil
	}
	return metadata.Columns
}

// StorageColumns returns the physical columns backing PostgreSQL-facing
// logical index columns, when they differ from the display definitions.
func StorageColumns(comment string) []string {
	metadata, ok := DecodeComment(comment)
	if !ok {
		return nil
	}
	return metadata.StorageColumns
}

// ExpressionColumns returns whether each PostgreSQL-facing index column is an
// expression slot.
func ExpressionColumns(comment string) []bool {
	metadata, ok := DecodeComment(comment)
	if !ok {
		return nil
	}
	return metadata.ExpressionColumns
}

// IncludeColumns returns PostgreSQL INCLUDE columns encoded for an index.
func IncludeColumns(comment string) []string {
	metadata, ok := DecodeComment(comment)
	if !ok {
		return nil
	}
	return metadata.IncludeColumns
}

// Predicate returns the PostgreSQL partial-index predicate encoded for an index.
func Predicate(comment string) string {
	metadata, ok := DecodeComment(comment)
	if !ok {
		return ""
	}
	return metadata.Predicate
}

// PredicateColumns returns physical table columns referenced by an encoded
// partial-index predicate.
func PredicateColumns(comment string) []string {
	metadata, ok := DecodeComment(comment)
	if !ok {
		return nil
	}
	return metadata.PredicateColumns
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

// BtreeOpClassAcceptsType returns PostgreSQL's built-in btree opclass/type
// compatibility for opclasses whose metadata Doltgres currently preserves.
func BtreeOpClassAcceptsType(opClass string, typ sql.Type) (string, bool) {
	typeName, displayName, ok := doltgresTypeNames(typ)
	if !ok {
		return typeDisplayName(typ), false
	}
	acceptedTypes, ok := btreeOpClassInputTypes[NormalizeOpClass(opClass)]
	if !ok {
		return displayName, false
	}
	_, ok = acceptedTypes[typeName]
	return displayName, ok
}

// DefaultBtreeOpClassForType returns PostgreSQL's built-in default btree
// opclass for the given Doltgres column type.
func DefaultBtreeOpClassForType(typ sql.Type) (string, bool) {
	typeName, _, ok := doltgresTypeNames(typ)
	if !ok {
		return "", false
	}

	switch typeName {
	case "bit":
		return "bit_ops", true
	case "bool":
		return "bool_ops", true
	case "int2":
		return "int2_ops", true
	case "int4":
		return "int4_ops", true
	case "int8":
		return "int8_ops", true
	case "float4":
		return "float4_ops", true
	case "float8":
		return "float8_ops", true
	case "numeric":
		return "numeric_ops", true
	case "char":
		return "char_ops", true
	case "name":
		return "name_ops", true
	case "text":
		return "text_ops", true
	case "varchar":
		return "varchar_ops", true
	case "bpchar":
		return "bpchar_ops", true
	case "bytea":
		return "bytea_ops", true
	case "date":
		return "date_ops", true
	case "interval":
		return "interval_ops", true
	case "jsonb":
		return "jsonb_ops", true
	case "oid":
		return "oid_ops", true
	case "oidvector":
		return "oidvector_ops", true
	case "pg_lsn":
		return "pg_lsn_ops", true
	case "time":
		return "time_ops", true
	case "timestamp":
		return "timestamp_ops", true
	case "timestamptz":
		return "timestamptz_ops", true
	case "timetz":
		return "timetz_ops", true
	case "uuid":
		return "uuid_ops", true
	case "varbit":
		return "varbit_ops", true
	default:
		return "", false
	}
}

func doltgresTypeNames(typ sql.Type) (string, string, bool) {
	doltgresType, ok := doltgresType(typ)
	if !ok {
		return "", "", false
	}
	typeName := doltgresType.ID.TypeName()
	return typeName, postgresTypeDisplayName(typeName, doltgresType), true
}

func doltgresType(typ sql.Type) (*pgtypes.DoltgresType, bool) {
	if typ == nil || isNilType(typ) {
		return nil, false
	}
	if typ, ok := typ.(*pgtypes.DoltgresType); ok {
		return typ, true
	}
	doltgresType, ok := doltgresTypeFromGmsType(typ)
	if !ok {
		return nil, false
	}
	return doltgresType, true
}

func doltgresTypeFromGmsType(typ sql.Type) (doltgresType *pgtypes.DoltgresType, ok bool) {
	defer func() {
		if recover() != nil {
			doltgresType = nil
			ok = false
		}
	}()
	doltgresType, err := pgtypes.FromGmsTypeToDoltgresType(typ)
	return doltgresType, err == nil && doltgresType != nil
}

func isNilType(typ sql.Type) bool {
	value := reflect.ValueOf(typ)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func postgresTypeDisplayName(typeName string, typ *pgtypes.DoltgresType) string {
	switch typeName {
	case "bool":
		return "boolean"
	case "int2":
		return "smallint"
	case "int4":
		return "integer"
	case "int8":
		return "bigint"
	case "float4":
		return "real"
	case "float8":
		return "double precision"
	case "varchar":
		return "character varying"
	case "bpchar":
		return "character"
	}
	return strings.TrimSpace(typ.String())
}

func typeDisplayName(typ sql.Type) string {
	if typ == nil || isNilType(typ) {
		return "unknown"
	}
	return strings.TrimSpace(typ.String())
}

// IsSupportedCollation returns whether Doltgres can preserve this built-in
// PostgreSQL collation in index metadata.
func IsSupportedCollation(collation string) bool {
	_, ok := supportedCollations[NormalizeCollation(collation)]
	return ok
}
