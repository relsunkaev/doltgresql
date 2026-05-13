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
	"sort"
	"strings"

	"github.com/dolthub/doltgresql/core/id"
)

const commentPrefix = "doltgres:table-metadata:v1:"

// Metadata stores PostgreSQL table metadata that Dolt's native schema metadata
// does not currently expose.
type Metadata struct {
	PrimaryKeyConstraint        string                       `json:"primaryKeyConstraint,omitempty"`
	PrimaryKeyIndexComment      string                       `json:"primaryKeyIndexComment,omitempty"`
	MaterializedView            bool                         `json:"materializedView,omitempty"`
	MaterializedViewDefinition  string                       `json:"materializedViewDefinition,omitempty"`
	MaterializedViewUnpopulated bool                         `json:"materializedViewUnpopulated,omitempty"`
	OfTypeSchema                string                       `json:"ofTypeSchema,omitempty"`
	OfTypeName                  string                       `json:"ofTypeName,omitempty"`
	Owner                       string                       `json:"owner,omitempty"`
	RelOptions                  []string                     `json:"relOptions,omitempty"`
	RelPersistence              string                       `json:"relPersistence,omitempty"`
	PartitionKeyDef             string                       `json:"partitionKeyDef,omitempty"`
	ForeignTable                bool                         `json:"foreignTable,omitempty"`
	ForeignServer               string                       `json:"foreignServer,omitempty"`
	ForeignOptions              []string                     `json:"foreignOptions,omitempty"`
	ColumnOptions               map[string][]string          `json:"columnOptions,omitempty"`
	ColumnStorage               map[string]string            `json:"columnStorage,omitempty"`
	ColumnCompression           map[string]string            `json:"columnCompression,omitempty"`
	ColumnStatisticsTargets     map[string]int16             `json:"columnStatisticsTargets,omitempty"`
	ColumnIdentity              map[string]string            `json:"columnIdentity,omitempty"`
	ColumnMissingValues         map[string]string            `json:"columnMissingValues,omitempty"`
	NotNullConstraints          map[string]NotNullConstraint `json:"notNullConstraints,omitempty"`
	Inherits                    []InheritedTable             `json:"inherits,omitempty"`
	DroppedColumns              []DroppedColumn              `json:"droppedColumns,omitempty"`
	ExtendedStatistics          []ExtendedStatistic          `json:"extendedStatistics,omitempty"`
}

// InheritedTable stores one parent relation from CREATE TABLE ... INHERITS.
type InheritedTable struct {
	Schema string `json:"schema,omitempty"`
	Name   string `json:"name,omitempty"`
}

// DroppedColumn stores the original attribute slot for a dropped table column.
type DroppedColumn struct {
	Name   string `json:"name,omitempty"`
	AttNum int16  `json:"attNum,omitempty"`
}

// ExtendedStatistic stores the metadata for a CREATE STATISTICS object.
type ExtendedStatistic struct {
	Name    string   `json:"name,omitempty"`
	Columns []string `json:"columns,omitempty"`
	Kinds   []string `json:"kinds,omitempty"`
}

// NotNullConstraint stores PostgreSQL metadata for a column NOT NULL
// constraint that Dolt's native schema only represents as column nullability.
type NotNullConstraint struct {
	Name      string `json:"name,omitempty"`
	NoInherit bool   `json:"noInherit,omitempty"`
}

// EncodeComment returns a durable table comment containing PostgreSQL metadata.
func EncodeComment(metadata Metadata) string {
	metadata.PrimaryKeyConstraint = strings.TrimSpace(metadata.PrimaryKeyConstraint)
	metadata.PrimaryKeyIndexComment = strings.TrimSpace(metadata.PrimaryKeyIndexComment)
	metadata.Owner = strings.TrimSpace(metadata.Owner)
	metadata.PartitionKeyDef = strings.TrimSpace(metadata.PartitionKeyDef)
	metadata.ForeignServer = strings.TrimSpace(metadata.ForeignServer)
	NormalizeRelOptions(metadata.RelOptions)
	NormalizeRelOptions(metadata.ForeignOptions)
	NormalizeColumnOptions(metadata.ColumnOptions)
	normalizeColumnStringMetadata(metadata.ColumnStorage)
	normalizeColumnStringMetadata(metadata.ColumnCompression)
	normalizeColumnStatisticsTargets(metadata.ColumnStatisticsTargets)
	normalizeColumnIdentity(metadata.ColumnIdentity)
	normalizeColumnMissingValues(metadata.ColumnMissingValues)
	normalizeNotNullConstraints(metadata.NotNullConstraints)
	normalizeInheritedTables(&metadata.Inherits)
	normalizeDroppedColumns(&metadata.DroppedColumns)
	normalizeExtendedStatistics(&metadata.ExtendedStatistics)
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
	metadata.PrimaryKeyIndexComment = strings.TrimSpace(metadata.PrimaryKeyIndexComment)
	metadata.MaterializedViewDefinition = strings.TrimSpace(metadata.MaterializedViewDefinition)
	metadata.Owner = strings.TrimSpace(metadata.Owner)
	metadata.PartitionKeyDef = strings.TrimSpace(metadata.PartitionKeyDef)
	NormalizeRelOptions(metadata.RelOptions)
	metadata.ForeignServer = strings.TrimSpace(metadata.ForeignServer)
	NormalizeRelOptions(metadata.ForeignOptions)
	NormalizeColumnOptions(metadata.ColumnOptions)
	normalizeColumnStringMetadata(metadata.ColumnStorage)
	normalizeColumnStringMetadata(metadata.ColumnCompression)
	normalizeColumnStatisticsTargets(metadata.ColumnStatisticsTargets)
	normalizeColumnIdentity(metadata.ColumnIdentity)
	normalizeColumnMissingValues(metadata.ColumnMissingValues)
	normalizeNotNullConstraints(metadata.NotNullConstraints)
	normalizeInheritedTables(&metadata.Inherits)
	normalizeDroppedColumns(&metadata.DroppedColumns)
	normalizeExtendedStatistics(&metadata.ExtendedStatistics)
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

// Inherits returns the parent relations encoded for CREATE TABLE ... INHERITS.
func Inherits(comment string) []InheritedTable {
	metadata, ok := DecodeComment(comment)
	if !ok || len(metadata.Inherits) == 0 {
		return nil
	}
	return append([]InheritedTable(nil), metadata.Inherits...)
}

// SetInherits returns a table metadata comment with the given inherited parent
// relations. An empty list clears Doltgres table metadata when no other table
// metadata is present.
func SetInherits(comment string, inheritedTables []InheritedTable) string {
	metadata, _ := DecodeComment(comment)
	metadata.Inherits = append([]InheritedTable(nil), inheritedTables...)
	normalizeInheritedTables(&metadata.Inherits)
	if metadata.empty() {
		return ""
	}
	return EncodeComment(metadata)
}

// PrimaryKeyIndexComment returns PostgreSQL index metadata for the native
// primary-key index, which cannot carry a Dolt index comment directly.
func PrimaryKeyIndexComment(comment string) string {
	metadata, ok := DecodeComment(comment)
	if !ok {
		return ""
	}
	return metadata.PrimaryKeyIndexComment
}

// SetPrimaryKeyIndexComment returns a table metadata comment carrying
// PostgreSQL index metadata for the native primary-key index.
func SetPrimaryKeyIndexComment(comment string, indexComment string) string {
	metadata, _ := DecodeComment(comment)
	metadata.PrimaryKeyIndexComment = strings.TrimSpace(indexComment)
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

// Owner returns the PostgreSQL owner encoded in a Doltgres table metadata comment.
func Owner(comment string) string {
	metadata, ok := DecodeComment(comment)
	if !ok {
		return ""
	}
	return metadata.Owner
}

// SetOwner returns a table metadata comment with the given PostgreSQL owner.
func SetOwner(comment string, owner string) string {
	metadata, _ := DecodeComment(comment)
	metadata.Owner = strings.TrimSpace(owner)
	if metadata.empty() {
		return ""
	}
	return EncodeComment(metadata)
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

// ColumnOptions returns the PostgreSQL attoptions encoded for a column in a
// Doltgres table metadata comment.
func ColumnOptions(comment string, column string) []string {
	metadata, ok := DecodeComment(comment)
	if !ok || len(metadata.ColumnOptions) == 0 {
		return nil
	}
	return append([]string(nil), metadata.ColumnOptions[strings.TrimSpace(column)]...)
}

// ColumnStorage returns the PostgreSQL attstorage value encoded for a column.
func ColumnStorage(comment string, column string) string {
	metadata, ok := DecodeComment(comment)
	if !ok || len(metadata.ColumnStorage) == 0 {
		return ""
	}
	return metadata.ColumnStorage[strings.TrimSpace(column)]
}

// ColumnCompression returns the PostgreSQL attcompression value encoded for a
// column.
func ColumnCompression(comment string, column string) string {
	metadata, ok := DecodeComment(comment)
	if !ok || len(metadata.ColumnCompression) == 0 {
		return ""
	}
	return metadata.ColumnCompression[strings.TrimSpace(column)]
}

// ColumnStatisticsTarget returns the PostgreSQL attstattarget value encoded for
// a column.
func ColumnStatisticsTarget(comment string, column string) (int16, bool) {
	metadata, ok := DecodeComment(comment)
	if !ok || len(metadata.ColumnStatisticsTargets) == 0 {
		return -1, false
	}
	target, ok := metadata.ColumnStatisticsTargets[strings.TrimSpace(column)]
	return target, ok
}

// ColumnIdentity returns the PostgreSQL attidentity value encoded for a column.
func ColumnIdentity(comment string, column string) string {
	metadata, ok := DecodeComment(comment)
	if !ok || len(metadata.ColumnIdentity) == 0 {
		return ""
	}
	return metadata.ColumnIdentity[strings.TrimSpace(column)]
}

// NotNullConstraintMetadata returns PostgreSQL NOT NULL constraint metadata for
// a column, if the table comment carries explicit metadata for it.
func NotNullConstraintMetadata(comment string, column string) (NotNullConstraint, bool) {
	metadata, ok := DecodeComment(comment)
	if !ok || len(metadata.NotNullConstraints) == 0 {
		return NotNullConstraint{}, false
	}
	constraint, ok := metadata.NotNullConstraints[strings.TrimSpace(column)]
	return constraint, ok
}

// ColumnMissingValue returns the PostgreSQL attmissingval element encoded for a
// column, if one exists.
func ColumnMissingValue(comment string, column string) (string, bool) {
	metadata, ok := DecodeComment(comment)
	if !ok || len(metadata.ColumnMissingValues) == 0 {
		return "", false
	}
	value, ok := metadata.ColumnMissingValues[strings.TrimSpace(column)]
	return value, ok
}

// DroppedColumns returns dropped columns sorted by their original attnum.
func DroppedColumns(comment string) []DroppedColumn {
	metadata, ok := DecodeComment(comment)
	if !ok || len(metadata.DroppedColumns) == 0 {
		return nil
	}
	return append([]DroppedColumn(nil), metadata.DroppedColumns...)
}

// ExtendedStatistics returns CREATE STATISTICS metadata for a table.
func ExtendedStatistics(comment string) []ExtendedStatistic {
	metadata, ok := DecodeComment(comment)
	if !ok || len(metadata.ExtendedStatistics) == 0 {
		return nil
	}
	return append([]ExtendedStatistic(nil), metadata.ExtendedStatistics...)
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

// PartitionKeyDef returns the PostgreSQL partition key definition encoded in a
// Doltgres table metadata comment.
func PartitionKeyDef(comment string) string {
	metadata, ok := DecodeComment(comment)
	if !ok {
		return ""
	}
	return metadata.PartitionKeyDef
}

// SetPartitionKeyDef returns a table metadata comment with the given
// PostgreSQL partition key definition. An empty definition clears only the
// partition metadata.
func SetPartitionKeyDef(comment string, definition string) string {
	metadata, _ := DecodeComment(comment)
	metadata.PartitionKeyDef = strings.TrimSpace(definition)
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

// SetForeignTable returns a table metadata comment marking the table as a
// foreign table backed by the named foreign server.
func SetForeignTable(comment string, server string, options []string) string {
	metadata, _ := DecodeComment(comment)
	metadata.ForeignTable = true
	metadata.ForeignServer = strings.TrimSpace(server)
	metadata.ForeignOptions = append([]string(nil), options...)
	NormalizeRelOptions(metadata.ForeignOptions)
	return EncodeComment(metadata)
}

// IsForeignTable returns whether the comment marks a table as a foreign table.
func IsForeignTable(comment string) bool {
	metadata, ok := DecodeComment(comment)
	return ok && metadata.ForeignTable
}

// ForeignServer returns the stored foreign server name, if any.
func ForeignServer(comment string) string {
	metadata, ok := DecodeComment(comment)
	if !ok || !metadata.ForeignTable {
		return ""
	}
	return metadata.ForeignServer
}

// ForeignOptions returns the stored foreign table options, if any.
func ForeignOptions(comment string) []string {
	metadata, ok := DecodeComment(comment)
	if !ok || !metadata.ForeignTable {
		return nil
	}
	return append([]string(nil), metadata.ForeignOptions...)
}

// SetColumnOptions returns a table metadata comment with PostgreSQL attoptions
// for a single column. An empty options slice clears only that column's metadata.
func SetColumnOptions(comment string, column string, options []string) string {
	column = strings.TrimSpace(column)
	metadata, _ := DecodeComment(comment)
	if column == "" {
		if metadata.empty() {
			return ""
		}
		return EncodeComment(metadata)
	}
	if metadata.ColumnOptions == nil {
		metadata.ColumnOptions = make(map[string][]string)
	}
	if len(options) == 0 {
		delete(metadata.ColumnOptions, column)
	} else {
		metadata.ColumnOptions[column] = append([]string(nil), options...)
		NormalizeRelOptions(metadata.ColumnOptions[column])
	}
	if len(metadata.ColumnOptions) == 0 {
		metadata.ColumnOptions = nil
	}
	if metadata.empty() {
		return ""
	}
	return EncodeComment(metadata)
}

// SetColumnStorage returns a table metadata comment with PostgreSQL attstorage
// for a single column.
func SetColumnStorage(comment string, column string, storage string) string {
	column = strings.TrimSpace(column)
	storage = strings.TrimSpace(storage)
	metadata, _ := DecodeComment(comment)
	if column == "" {
		if metadata.empty() {
			return ""
		}
		return EncodeComment(metadata)
	}
	if metadata.ColumnStorage == nil {
		metadata.ColumnStorage = make(map[string]string)
	}
	if storage == "" {
		delete(metadata.ColumnStorage, column)
	} else {
		metadata.ColumnStorage[column] = storage
	}
	if len(metadata.ColumnStorage) == 0 {
		metadata.ColumnStorage = nil
	}
	if metadata.empty() {
		return ""
	}
	return EncodeComment(metadata)
}

// SetColumnCompression returns a table metadata comment with PostgreSQL
// attcompression for a single column.
func SetColumnCompression(comment string, column string, compression string) string {
	column = strings.TrimSpace(column)
	compression = strings.TrimSpace(compression)
	metadata, _ := DecodeComment(comment)
	if column == "" {
		if metadata.empty() {
			return ""
		}
		return EncodeComment(metadata)
	}
	if metadata.ColumnCompression == nil {
		metadata.ColumnCompression = make(map[string]string)
	}
	if compression == "" {
		delete(metadata.ColumnCompression, column)
	} else {
		metadata.ColumnCompression[column] = compression
	}
	if len(metadata.ColumnCompression) == 0 {
		metadata.ColumnCompression = nil
	}
	if metadata.empty() {
		return ""
	}
	return EncodeComment(metadata)
}

// SetColumnStatisticsTarget returns a table metadata comment with PostgreSQL
// attstattarget for a single column. A target of -1 clears the explicit value.
func SetColumnStatisticsTarget(comment string, column string, target int16) string {
	column = strings.TrimSpace(column)
	metadata, _ := DecodeComment(comment)
	if column == "" {
		if metadata.empty() {
			return ""
		}
		return EncodeComment(metadata)
	}
	if metadata.ColumnStatisticsTargets == nil {
		metadata.ColumnStatisticsTargets = make(map[string]int16)
	}
	if target == -1 {
		delete(metadata.ColumnStatisticsTargets, column)
	} else {
		metadata.ColumnStatisticsTargets[column] = target
	}
	if len(metadata.ColumnStatisticsTargets) == 0 {
		metadata.ColumnStatisticsTargets = nil
	}
	if metadata.empty() {
		return ""
	}
	return EncodeComment(metadata)
}

// SetColumnIdentity returns a table metadata comment with the PostgreSQL
// attidentity value for a single column. Valid values are "a" for GENERATED
// ALWAYS and "d" for GENERATED BY DEFAULT; any other value clears the metadata.
func SetColumnIdentity(comment string, column string, identity string) string {
	column = strings.TrimSpace(column)
	identity = strings.TrimSpace(identity)
	metadata, _ := DecodeComment(comment)
	if column == "" {
		if metadata.empty() {
			return ""
		}
		return EncodeComment(metadata)
	}
	if metadata.ColumnIdentity == nil {
		metadata.ColumnIdentity = make(map[string]string)
	}
	switch identity {
	case "a", "d":
		metadata.ColumnIdentity[column] = identity
	default:
		delete(metadata.ColumnIdentity, column)
	}
	if len(metadata.ColumnIdentity) == 0 {
		metadata.ColumnIdentity = nil
	}
	if metadata.empty() {
		return ""
	}
	return EncodeComment(metadata)
}

// SetNotNullConstraint returns a table metadata comment with PostgreSQL
// constraint metadata for a column NOT NULL constraint.
func SetNotNullConstraint(comment string, column string, constraint NotNullConstraint) string {
	column = strings.TrimSpace(column)
	constraint.Name = strings.TrimSpace(constraint.Name)
	metadata, _ := DecodeComment(comment)
	if column == "" {
		if metadata.empty() {
			return ""
		}
		return EncodeComment(metadata)
	}
	if metadata.NotNullConstraints == nil {
		metadata.NotNullConstraints = make(map[string]NotNullConstraint)
	}
	if constraint.Name == "" && !constraint.NoInherit {
		delete(metadata.NotNullConstraints, column)
	} else {
		metadata.NotNullConstraints[column] = constraint
	}
	if len(metadata.NotNullConstraints) == 0 {
		metadata.NotNullConstraints = nil
	}
	if metadata.empty() {
		return ""
	}
	return EncodeComment(metadata)
}

// SetColumnMissingValue returns a table metadata comment with PostgreSQL
// attmissingval metadata for a single column.
func SetColumnMissingValue(comment string, column string, value string) string {
	column = strings.TrimSpace(column)
	metadata, _ := DecodeComment(comment)
	if column == "" {
		if metadata.empty() {
			return ""
		}
		return EncodeComment(metadata)
	}
	if metadata.ColumnMissingValues == nil {
		metadata.ColumnMissingValues = make(map[string]string)
	}
	metadata.ColumnMissingValues[column] = value
	if metadata.empty() {
		return ""
	}
	return EncodeComment(metadata)
}

// AddDroppedColumn returns a table metadata comment that preserves the original
// pg_attribute slot for a dropped column.
func AddDroppedColumn(comment string, column string, attnum int16) string {
	column = strings.TrimSpace(column)
	metadata, _ := DecodeComment(comment)
	if column == "" || attnum <= 0 {
		if metadata.empty() {
			return ""
		}
		return EncodeComment(metadata)
	}
	metadata.DroppedColumns = append(metadata.DroppedColumns, DroppedColumn{Name: column, AttNum: attnum})
	delete(metadata.ColumnOptions, column)
	delete(metadata.ColumnStorage, column)
	delete(metadata.ColumnCompression, column)
	delete(metadata.ColumnStatisticsTargets, column)
	delete(metadata.ColumnIdentity, column)
	delete(metadata.ColumnMissingValues, column)
	delete(metadata.NotNullConstraints, column)
	normalizeDroppedColumns(&metadata.DroppedColumns)
	if len(metadata.ColumnOptions) == 0 {
		metadata.ColumnOptions = nil
	}
	if len(metadata.ColumnStorage) == 0 {
		metadata.ColumnStorage = nil
	}
	if len(metadata.ColumnCompression) == 0 {
		metadata.ColumnCompression = nil
	}
	if len(metadata.ColumnStatisticsTargets) == 0 {
		metadata.ColumnStatisticsTargets = nil
	}
	if len(metadata.ColumnIdentity) == 0 {
		metadata.ColumnIdentity = nil
	}
	if len(metadata.ColumnMissingValues) == 0 {
		metadata.ColumnMissingValues = nil
	}
	if len(metadata.NotNullConstraints) == 0 {
		metadata.NotNullConstraints = nil
	}
	if metadata.empty() {
		return ""
	}
	return EncodeComment(metadata)
}

// AddExtendedStatistic records metadata for a CREATE STATISTICS object on a
// table. Reusing a statistic name replaces the previous metadata.
func AddExtendedStatistic(comment string, statistic ExtendedStatistic) string {
	metadata, _ := DecodeComment(comment)
	statistic.Name = strings.TrimSpace(statistic.Name)
	normalizeExtendedStatistic(&statistic)
	if statistic.Name == "" || len(statistic.Columns) == 0 || len(statistic.Kinds) == 0 {
		if metadata.empty() {
			return ""
		}
		return EncodeComment(metadata)
	}
	replaced := false
	for i, existing := range metadata.ExtendedStatistics {
		if existing.Name == statistic.Name {
			metadata.ExtendedStatistics[i] = statistic
			replaced = true
			break
		}
	}
	if !replaced {
		metadata.ExtendedStatistics = append(metadata.ExtendedStatistics, statistic)
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

// NormalizeColumnOptions normalizes all column option values in place and
// removes empty column names.
func NormalizeColumnOptions(columnOptions map[string][]string) {
	for column, options := range columnOptions {
		if strings.TrimSpace(column) == "" || len(options) == 0 {
			delete(columnOptions, column)
			continue
		}
		NormalizeRelOptions(options)
	}
}

func normalizeColumnStringMetadata(values map[string]string) {
	for column, value := range values {
		trimmedColumn := strings.TrimSpace(column)
		trimmedValue := strings.TrimSpace(value)
		if trimmedColumn == "" || trimmedValue == "" {
			delete(values, column)
			continue
		}
		if trimmedColumn != column {
			delete(values, column)
		}
		values[trimmedColumn] = trimmedValue
	}
}

func normalizeColumnStatisticsTargets(values map[string]int16) {
	for column, value := range values {
		trimmedColumn := strings.TrimSpace(column)
		if trimmedColumn == "" || value == -1 {
			delete(values, column)
			continue
		}
		if trimmedColumn != column {
			delete(values, column)
		}
		values[trimmedColumn] = value
	}
}

func normalizeColumnIdentity(values map[string]string) {
	for column, value := range values {
		trimmedColumn := strings.TrimSpace(column)
		trimmedValue := strings.TrimSpace(value)
		if trimmedColumn == "" || (trimmedValue != "a" && trimmedValue != "d") {
			delete(values, column)
			continue
		}
		if trimmedColumn != column || trimmedValue != value {
			delete(values, column)
		}
		values[trimmedColumn] = trimmedValue
	}
}

func normalizeNotNullConstraints(values map[string]NotNullConstraint) {
	for column, constraint := range values {
		trimmedColumn := strings.TrimSpace(column)
		constraint.Name = strings.TrimSpace(constraint.Name)
		if trimmedColumn == "" || (constraint.Name == "" && !constraint.NoInherit) {
			delete(values, column)
			continue
		}
		if trimmedColumn != column {
			delete(values, column)
		}
		values[trimmedColumn] = constraint
	}
}

func normalizeInheritedTables(values *[]InheritedTable) {
	if len(*values) == 0 {
		return
	}
	ret := (*values)[:0]
	seen := make(map[InheritedTable]struct{}, len(*values))
	for _, value := range *values {
		value.Schema = strings.TrimSpace(value.Schema)
		value.Name = strings.TrimSpace(value.Name)
		if value.Name == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		ret = append(ret, value)
	}
	*values = ret
}

func normalizeColumnMissingValues(values map[string]string) {
	for column, value := range values {
		trimmedColumn := strings.TrimSpace(column)
		if trimmedColumn == "" {
			delete(values, column)
			continue
		}
		if trimmedColumn != column {
			delete(values, column)
		}
		values[trimmedColumn] = value
	}
}

func normalizeDroppedColumns(values *[]DroppedColumn) {
	if len(*values) == 0 {
		return
	}
	byAttNum := make(map[int16]DroppedColumn, len(*values))
	for _, value := range *values {
		value.Name = strings.TrimSpace(value.Name)
		if value.Name == "" || value.AttNum <= 0 {
			continue
		}
		byAttNum[value.AttNum] = value
	}
	*values = (*values)[:0]
	for _, value := range byAttNum {
		*values = append(*values, value)
	}
	sort.Slice(*values, func(i, j int) bool {
		return (*values)[i].AttNum < (*values)[j].AttNum
	})
}

func normalizeExtendedStatistics(values *[]ExtendedStatistic) {
	if len(*values) == 0 {
		return
	}
	byName := make(map[string]ExtendedStatistic, len(*values))
	for _, value := range *values {
		normalizeExtendedStatistic(&value)
		if value.Name == "" || len(value.Columns) == 0 || len(value.Kinds) == 0 {
			continue
		}
		byName[value.Name] = value
	}
	*values = (*values)[:0]
	for _, value := range byName {
		*values = append(*values, value)
	}
	sort.Slice(*values, func(i, j int) bool {
		return (*values)[i].Name < (*values)[j].Name
	})
}

func normalizeExtendedStatistic(value *ExtendedStatistic) {
	value.Name = strings.TrimSpace(value.Name)
	value.Columns = normalizeStringList(value.Columns)
	value.Kinds = normalizeStringList(value.Kinds)
}

func normalizeStringList(values []string) []string {
	ret := values[:0]
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		ret = append(ret, value)
	}
	if len(ret) == 0 {
		return nil
	}
	return ret
}

func (metadata Metadata) empty() bool {
	return metadata.PrimaryKeyConstraint == "" &&
		metadata.PrimaryKeyIndexComment == "" &&
		!metadata.MaterializedView &&
		metadata.MaterializedViewDefinition == "" &&
		!metadata.MaterializedViewUnpopulated &&
		metadata.OfTypeSchema == "" &&
		metadata.OfTypeName == "" &&
		metadata.Owner == "" &&
		metadata.RelPersistence == "" &&
		metadata.PartitionKeyDef == "" &&
		len(metadata.RelOptions) == 0 &&
		!metadata.ForeignTable &&
		metadata.ForeignServer == "" &&
		len(metadata.ForeignOptions) == 0 &&
		len(metadata.ColumnOptions) == 0 &&
		len(metadata.ColumnStorage) == 0 &&
		len(metadata.ColumnCompression) == 0 &&
		len(metadata.ColumnStatisticsTargets) == 0 &&
		len(metadata.ColumnIdentity) == 0 &&
		len(metadata.ColumnMissingValues) == 0 &&
		len(metadata.NotNullConstraints) == 0 &&
		len(metadata.Inherits) == 0 &&
		len(metadata.DroppedColumns) == 0 &&
		len(metadata.ExtendedStatistics) == 0
}
