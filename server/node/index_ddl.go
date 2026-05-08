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

package node

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/server/indexmetadata"
)

type locatedIndex struct {
	db        sql.Database
	table     sql.Table
	alterable sql.IndexAlterableTable
	index     sql.Index
}

// DropIndexTarget identifies one index named in a PostgreSQL DROP INDEX statement.
type DropIndexTarget struct {
	Schema string
	Table  string
	Index  string
}

// DropIndex handles PostgreSQL DROP INDEX, including table-less index names.
type DropIndex struct {
	ifExists bool
	targets  []DropIndexTarget
}

var _ sql.ExecSourceRel = (*DropIndex)(nil)
var _ vitess.Injectable = (*DropIndex)(nil)

// NewDropIndex returns a new *DropIndex.
func NewDropIndex(ifExists bool, schema string, table string, index string) *DropIndex {
	return NewDropIndexes(ifExists, []DropIndexTarget{{
		Schema: schema,
		Table:  table,
		Index:  index,
	}})
}

// NewDropIndexes returns a new *DropIndex for one or more indexes.
func NewDropIndexes(ifExists bool, targets []DropIndexTarget) *DropIndex {
	targets = append([]DropIndexTarget(nil), targets...)
	return &DropIndex{
		ifExists: ifExists,
		targets:  targets,
	}
}

// Children implements the interface sql.ExecSourceRel.
func (d *DropIndex) Children() []sql.Node {
	return nil
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (d *DropIndex) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (d *DropIndex) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (d *DropIndex) RowIter(ctx *sql.Context, r sql.Row) (sql.RowIter, error) {
	type targetIndex struct {
		located     *locatedIndex
		metadata    indexmetadata.Metadata
		hasMetadata bool
	}
	locatedIndexes := make([]targetIndex, 0, len(d.targets))
	for _, target := range d.targets {
		located, ok, err := locateIndex(ctx, target.Schema, target.Table, target.Index, d.ifExists)
		if err != nil {
			return nil, err
		}
		if !ok {
			if d.ifExists {
				continue
			}
			return nil, sql.ErrIndexNotFound.New(target.Index)
		}
		if isConstraintBackedIndex(located.index) {
			indexName := indexmetadata.DisplayNameForTable(located.index, located.table)
			return nil, errors.Errorf(`cannot drop index "%s" because constraint "%s" on table "%s" requires it`,
				indexName, indexName, located.index.Table())
		}

		metadata, hasMetadata := indexmetadata.DecodeComment(located.index.Comment())
		locatedIndexes = append(locatedIndexes, targetIndex{
			located:     located,
			metadata:    metadata,
			hasMetadata: hasMetadata,
		})
	}

	for _, target := range locatedIndexes {
		if err := target.located.alterable.DropIndex(ctx, target.located.index.ID()); err != nil {
			if sql.ErrIndexNotFound.Is(err) && d.ifExists {
				continue
			}
			return nil, err
		}
		if target.hasMetadata && target.metadata.Gin != nil && target.metadata.Gin.PostingTable != "" {
			if err := dropTable(ctx, target.located.db, target.metadata.Gin.PostingTable); err != nil && !sql.ErrTableNotFound.Is(err) {
				return nil, err
			}
		}
		if target.hasMetadata && target.metadata.Gin != nil && target.metadata.Gin.PostingChunkTable != "" {
			if err := dropTable(ctx, target.located.db, target.metadata.Gin.PostingChunkTable); err != nil && !sql.ErrTableNotFound.Is(err) {
				return nil, err
			}
		}
	}
	return sql.RowsToRowIter(), nil
}

// Schema implements the interface sql.ExecSourceRel.
func (d *DropIndex) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (d *DropIndex) String() string {
	return "DROP INDEX"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (d *DropIndex) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(d, children...)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (d *DropIndex) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return d, nil
}

// RenameIndex handles PostgreSQL ALTER INDEX RENAME, including table-less index names.
type RenameIndex struct {
	ifExists bool
	schema   string
	table    string
	from     string
	to       string
}

var _ sql.ExecSourceRel = (*RenameIndex)(nil)
var _ vitess.Injectable = (*RenameIndex)(nil)

// NewRenameIndex returns a new *RenameIndex.
func NewRenameIndex(ifExists bool, schema string, table string, from string, to string) *RenameIndex {
	return &RenameIndex{
		ifExists: ifExists,
		schema:   schema,
		table:    table,
		from:     from,
		to:       to,
	}
}

// Children implements the interface sql.ExecSourceRel.
func (r *RenameIndex) Children() []sql.Node {
	return nil
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (r *RenameIndex) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (r *RenameIndex) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (r *RenameIndex) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	located, ok, err := locateIndex(ctx, r.schema, r.table, r.from, r.ifExists)
	if err != nil {
		return nil, err
	}
	if !ok {
		if r.ifExists {
			return sql.RowsToRowIter(), nil
		}
		return nil, sql.ErrIndexNotFound.New(r.from)
	}
	if isPrimaryKeyIndex(located.index) {
		return nil, errors.Errorf("renaming primary key indexes is not yet supported")
	}
	if err = located.alterable.RenameIndex(ctx, located.index.ID(), r.to); err != nil {
		if sql.ErrIndexNotFound.Is(err) && r.ifExists {
			return sql.RowsToRowIter(), nil
		}
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

// Schema implements the interface sql.ExecSourceRel.
func (r *RenameIndex) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (r *RenameIndex) String() string {
	return "ALTER INDEX RENAME"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (r *RenameIndex) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(r, children...)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (r *RenameIndex) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return r, nil
}

// AlterIndexSetDefaultTablespace handles ALTER INDEX ... SET TABLESPACE pg_default.
// Doltgres currently only supports the default tablespace, which is already the
// stored state for every index, so the execution step validates the target and
// otherwise leaves catalog metadata unchanged.
type AlterIndexSetDefaultTablespace struct {
	ifExists bool
	schema   string
	table    string
	index    string
}

var _ sql.ExecSourceRel = (*AlterIndexSetDefaultTablespace)(nil)
var _ vitess.Injectable = (*AlterIndexSetDefaultTablespace)(nil)

// NewAlterIndexSetDefaultTablespace returns a new *AlterIndexSetDefaultTablespace.
func NewAlterIndexSetDefaultTablespace(ifExists bool, schema string, table string, index string) *AlterIndexSetDefaultTablespace {
	return &AlterIndexSetDefaultTablespace{
		ifExists: ifExists,
		schema:   schema,
		table:    table,
		index:    index,
	}
}

// Children implements the interface sql.ExecSourceRel.
func (a *AlterIndexSetDefaultTablespace) Children() []sql.Node {
	return nil
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (a *AlterIndexSetDefaultTablespace) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (a *AlterIndexSetDefaultTablespace) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (a *AlterIndexSetDefaultTablespace) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	_, ok, err := locateIndex(ctx, a.schema, a.table, a.index, a.ifExists)
	if err != nil {
		return nil, err
	}
	if !ok {
		if a.ifExists {
			return sql.RowsToRowIter(), nil
		}
		return nil, sql.ErrIndexNotFound.New(a.index)
	}
	return sql.RowsToRowIter(), nil
}

// Schema implements the interface sql.ExecSourceRel.
func (a *AlterIndexSetDefaultTablespace) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (a *AlterIndexSetDefaultTablespace) String() string {
	return "ALTER INDEX SET TABLESPACE"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (a *AlterIndexSetDefaultTablespace) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(a, children...)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (a *AlterIndexSetDefaultTablespace) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return a, nil
}

// AlterIndexSetStorage handles ALTER INDEX ... SET/RESET storage parameters.
type AlterIndexSetStorage struct {
	ifExists   bool
	schema     string
	table      string
	index      string
	relOptions []string
	resetKeys  []string
}

var _ sql.ExecSourceRel = (*AlterIndexSetStorage)(nil)
var _ vitess.Injectable = (*AlterIndexSetStorage)(nil)

// NewAlterIndexSetStorage returns a new *AlterIndexSetStorage.
func NewAlterIndexSetStorage(ifExists bool, schema string, table string, index string, relOptions []string, resetKeys []string) *AlterIndexSetStorage {
	return &AlterIndexSetStorage{
		ifExists:   ifExists,
		schema:     schema,
		table:      table,
		index:      index,
		relOptions: append([]string(nil), relOptions...),
		resetKeys:  append([]string(nil), resetKeys...),
	}
}

// Children implements the interface sql.ExecSourceRel.
func (a *AlterIndexSetStorage) Children() []sql.Node {
	return nil
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (a *AlterIndexSetStorage) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (a *AlterIndexSetStorage) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (a *AlterIndexSetStorage) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	located, ok, err := locateIndex(ctx, a.schema, a.table, a.index, a.ifExists)
	if err != nil {
		return nil, err
	}
	if !ok {
		if a.ifExists {
			return sql.RowsToRowIter(), nil
		}
		return nil, sql.ErrIndexNotFound.New(a.index)
	}
	if isConstraintBackedIndex(located.index) {
		return nil, errors.Errorf("ALTER INDEX storage parameters for constraint-backed indexes are not yet supported")
	}

	accessMethod := indexmetadata.AccessMethod(located.index.IndexType(), located.index.Comment())
	if accessMethod != indexmetadata.AccessMethodBtree {
		return nil, errors.Errorf("ALTER INDEX storage parameters for %s indexes are not yet supported", accessMethod)
	}

	metadata, hasMetadata := indexmetadata.DecodeComment(located.index.Comment())
	if !hasMetadata {
		metadata = indexmetadata.Metadata{AccessMethod: indexmetadata.AccessMethodBtree}
	}
	if len(a.resetKeys) > 0 {
		metadata.RelOptions = resetRelOptions(metadata.RelOptions, a.resetKeys)
	} else {
		metadata.RelOptions = mergeRelOptions(metadata.RelOptions, a.relOptions)
	}

	if err = rebuildIndexWithMetadata(ctx, located, metadata); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

// Schema implements the interface sql.ExecSourceRel.
func (a *AlterIndexSetStorage) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (a *AlterIndexSetStorage) String() string {
	return "ALTER INDEX SET STORAGE"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (a *AlterIndexSetStorage) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(a, children...)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (a *AlterIndexSetStorage) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return a, nil
}

// AlterIndexSetStatistics handles ALTER INDEX ... ALTER COLUMN ... SET STATISTICS.
type AlterIndexSetStatistics struct {
	ifExists     bool
	schema       string
	table        string
	index        string
	columnNumber int16
	statsTarget  int16
}

var _ sql.ExecSourceRel = (*AlterIndexSetStatistics)(nil)
var _ vitess.Injectable = (*AlterIndexSetStatistics)(nil)

// NewAlterIndexSetStatistics returns a new *AlterIndexSetStatistics.
func NewAlterIndexSetStatistics(ifExists bool, schema string, table string, index string, columnNumber int16, statsTarget int16) *AlterIndexSetStatistics {
	return &AlterIndexSetStatistics{
		ifExists:     ifExists,
		schema:       schema,
		table:        table,
		index:        index,
		columnNumber: columnNumber,
		statsTarget:  statsTarget,
	}
}

// Children implements the interface sql.ExecSourceRel.
func (a *AlterIndexSetStatistics) Children() []sql.Node {
	return nil
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (a *AlterIndexSetStatistics) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (a *AlterIndexSetStatistics) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (a *AlterIndexSetStatistics) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	located, ok, err := locateIndex(ctx, a.schema, a.table, a.index, a.ifExists)
	if err != nil {
		return nil, err
	}
	if !ok {
		if a.ifExists {
			return sql.RowsToRowIter(), nil
		}
		return nil, sql.ErrIndexNotFound.New(a.index)
	}
	if isConstraintBackedIndex(located.index) {
		return nil, errors.Errorf("ALTER INDEX statistics targets for constraint-backed indexes are not yet supported")
	}

	accessMethod := indexmetadata.AccessMethod(located.index.IndexType(), located.index.Comment())
	if accessMethod != indexmetadata.AccessMethodBtree {
		return nil, errors.Errorf("ALTER INDEX statistics targets for %s indexes are not yet supported", accessMethod)
	}

	logicalColumns := indexmetadata.LogicalColumns(located.index, located.table.Schema(ctx))
	indexName := indexmetadata.DisplayNameForTable(located.index, located.table)
	columnIdx := int(a.columnNumber) - 1
	if columnIdx < 0 || columnIdx >= len(logicalColumns) {
		return nil, errors.Errorf(`column number %d of relation "%s" does not exist`, a.columnNumber, indexName)
	}
	logicalColumn := logicalColumns[columnIdx]
	if !logicalColumn.Expression {
		return nil, errors.Errorf(`cannot alter statistics on non-expression column "%s" of index "%s"`, logicalColumn.Definition, indexName)
	}

	metadata, hasMetadata := indexmetadata.DecodeComment(located.index.Comment())
	if !hasMetadata {
		metadata = indexmetadata.Metadata{AccessMethod: indexmetadata.AccessMethodBtree}
	}
	if len(metadata.StatisticsTargets) < len(logicalColumns) {
		targets := make([]int16, len(logicalColumns))
		for i := range targets {
			targets[i] = -1
		}
		copy(targets, metadata.StatisticsTargets)
		metadata.StatisticsTargets = targets
	}
	metadata.StatisticsTargets[columnIdx] = a.statsTarget

	if err = rebuildIndexWithMetadata(ctx, located, metadata); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

// Schema implements the interface sql.ExecSourceRel.
func (a *AlterIndexSetStatistics) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (a *AlterIndexSetStatistics) String() string {
	return "ALTER INDEX SET STATISTICS"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (a *AlterIndexSetStatistics) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(a, children...)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (a *AlterIndexSetStatistics) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return a, nil
}

func locateIndex(ctx *sql.Context, schemaName string, tableName string, indexName string, missingOK bool) (*locatedIndex, bool, error) {
	schemaName, err := core.GetSchemaName(ctx, nil, schemaName)
	if err != nil {
		return nil, false, err
	}
	db, err := indexDDLDatabase(ctx, schemaName, missingOK)
	if err != nil {
		return nil, false, err
	}
	if db == nil {
		return nil, false, nil
	}
	if tableName != "" {
		table, ok, err := db.GetTableInsensitive(ctx, tableName)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			if missingOK {
				return nil, false, nil
			}
			return nil, false, sql.ErrTableNotFound.New(tableName)
		}
		return locateIndexOnTable(ctx, db, tableName, table, indexName)
	}

	tableNames, err := db.GetTableNames(ctx)
	if err != nil {
		return nil, false, err
	}
	var match *locatedIndex
	for _, nextTableName := range tableNames {
		table, ok, err := db.GetTableInsensitive(ctx, nextTableName)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			continue
		}
		located, ok, err := locateIndexOnTable(ctx, db, nextTableName, table, indexName)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			continue
		}
		if match != nil {
			return nil, false, errors.Errorf(`index "%s" is ambiguous`, indexName)
		}
		match = located
	}
	if match == nil {
		return nil, false, nil
	}
	return match, true, nil
}

func indexDDLDatabase(ctx *sql.Context, schemaName string, missingOK bool) (sql.Database, error) {
	db, err := core.GetSqlDatabaseFromContext(ctx, "")
	if err != nil {
		return nil, err
	}
	if db == nil {
		if missingOK {
			return nil, nil
		}
		return nil, errors.Errorf("database not found")
	}
	schemaDb, ok := db.(sql.SchemaDatabase)
	if !ok {
		return db, nil
	}
	db, ok, err = schemaDb.GetSchema(ctx, schemaName)
	if err != nil {
		return nil, err
	}
	if !ok {
		if missingOK {
			return nil, nil
		}
		return nil, errors.Errorf(`schema "%s" does not exist`, schemaName)
	}
	return db, nil
}

func locateIndexOnTable(ctx *sql.Context, db sql.Database, tableName string, table sql.Table, indexName string) (*locatedIndex, bool, error) {
	indexAddressable, ok := table.(sql.IndexAddressable)
	if !ok {
		return nil, false, nil
	}
	indexes, err := indexAddressable.GetIndexes(ctx)
	if err != nil {
		return nil, false, err
	}
	for _, index := range indexes {
		if !indexNameMatches(index, table, indexName) {
			continue
		}
		alterable, ok := table.(sql.IndexAlterableTable)
		if !ok {
			return nil, false, errors.Errorf(`relation "%s" does not support index alteration`, tableName)
		}
		return &locatedIndex{
			db:        db,
			table:     table,
			alterable: alterable,
			index:     index,
		}, true, nil
	}
	return nil, false, nil
}

func indexNameMatches(index sql.Index, table sql.Table, name string) bool {
	return strings.EqualFold(index.ID(), name) || strings.EqualFold(indexmetadata.DisplayNameForTable(index, table), name)
}

func isPrimaryKeyIndex(index sql.Index) bool {
	if strings.EqualFold(index.ID(), "PRIMARY") {
		return true
	}
	primaryKeyIndex, ok := index.(interface {
		IsPrimaryKey() bool
	})
	return ok && primaryKeyIndex.IsPrimaryKey()
}

func isConstraintBackedIndex(index sql.Index) bool {
	return isPrimaryKeyIndex(index) || (index.IsUnique() && !indexmetadata.IsStandaloneIndex(index.Comment()))
}

func indexColumnsForRebuild(ctx *sql.Context, index sql.Index, table sql.Table) ([]sql.IndexColumn, error) {
	logicalColumns := indexmetadata.LogicalColumns(index, table.Schema(ctx))
	prefixLengths := index.PrefixLengths()
	columns := make([]sql.IndexColumn, 0, len(logicalColumns))
	seen := make(map[string]struct{}, len(logicalColumns))
	for i, logicalColumn := range logicalColumns {
		columnName := strings.TrimSpace(logicalColumn.StorageName)
		if columnName == "" {
			return nil, errors.Errorf("ALTER INDEX storage parameters for expression indexes are not yet supported")
		}
		key := strings.ToLower(columnName)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		column := sql.IndexColumn{Name: columnName}
		if i < len(prefixLengths) {
			column.Length = int64(prefixLengths[i])
		}
		columns = append(columns, column)
	}
	if len(columns) == 0 {
		return nil, errors.Errorf("ALTER INDEX storage parameters require at least one index column")
	}
	return columns, nil
}

func rebuildIndexWithMetadata(ctx *sql.Context, located *locatedIndex, metadata indexmetadata.Metadata) error {
	columns, err := indexColumnsForRebuild(ctx, located.index, located.table)
	if err != nil {
		return err
	}
	constraint := sql.IndexConstraint_None
	if located.index.IsUnique() {
		constraint = sql.IndexConstraint_Unique
		metadata.Constraint = indexmetadata.ConstraintNone
	}

	comment := alteredIndexComment(metadata)
	indexDef := sql.IndexDef{
		Name:       located.index.ID(),
		Columns:    columns,
		Constraint: constraint,
		Storage:    sql.IndexUsing_BTree,
		Comment:    comment,
	}
	if err = located.alterable.DropIndex(ctx, located.index.ID()); err != nil {
		return err
	}
	if err = located.alterable.CreateIndex(ctx, indexDef); err != nil {
		return err
	}
	return nil
}

func mergeRelOptions(existing []string, updates []string) []string {
	values := make(map[string]string, len(existing)+len(updates))
	order := make([]string, 0, len(existing)+len(updates))
	for _, option := range existing {
		key, value, ok := splitRelOption(option)
		if !ok {
			continue
		}
		if _, exists := values[key]; !exists {
			order = append(order, key)
		}
		values[key] = value
	}
	for _, option := range updates {
		key, value, ok := splitRelOption(option)
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
		ret = append(ret, fmt.Sprintf("%s=%s", key, values[key]))
	}
	return ret
}

func resetRelOptions(existing []string, resetKeys []string) []string {
	reset := make(map[string]struct{}, len(resetKeys))
	for _, key := range resetKeys {
		reset[strings.ToLower(strings.TrimSpace(key))] = struct{}{}
	}
	ret := make([]string, 0, len(existing))
	for _, option := range existing {
		key, _, ok := splitRelOption(option)
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

func splitRelOption(option string) (key string, value string, ok bool) {
	key, value, ok = strings.Cut(strings.TrimSpace(option), "=")
	if !ok {
		return "", "", false
	}
	return strings.ToLower(strings.TrimSpace(key)), strings.TrimSpace(value), true
}

func alteredIndexComment(metadata indexmetadata.Metadata) string {
	if !hasAlteredIndexMetadata(metadata) {
		return ""
	}
	if metadata.AccessMethod == "" {
		metadata.AccessMethod = indexmetadata.AccessMethodBtree
	}
	return indexmetadata.EncodeComment(metadata)
}

func hasAlteredIndexMetadata(metadata indexmetadata.Metadata) bool {
	if metadata.AccessMethod != "" && metadata.AccessMethod != indexmetadata.AccessMethodBtree {
		return true
	}
	if hasNonEmptyString(metadata.Columns) ||
		hasNonEmptyString(metadata.StorageColumns) ||
		hasTrueBool(metadata.ExpressionColumns) ||
		hasNonEmptyString(metadata.IncludeColumns) ||
		strings.TrimSpace(metadata.Predicate) != "" ||
		hasNonEmptyString(metadata.PredicateColumns) ||
		hasNonEmptyString(metadata.Collations) ||
		hasNonEmptyString(metadata.OpClasses) ||
		hasNonEmptyString(metadata.RelOptions) ||
		hasNonDefaultStatisticsTarget(metadata.StatisticsTargets) ||
		hasNonEmptyIndexColumnOption(metadata.SortOptions) ||
		strings.TrimSpace(metadata.Constraint) != "" ||
		metadata.NotReady ||
		metadata.Invalid ||
		metadata.Gin != nil {
		return true
	}
	return false
}

func hasNonDefaultStatisticsTarget(values []int16) bool {
	for _, value := range values {
		if value != -1 {
			return true
		}
	}
	return false
}

func hasNonEmptyString(values []string) bool {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return true
		}
	}
	return false
}

func hasTrueBool(values []bool) bool {
	for _, value := range values {
		if value {
			return true
		}
	}
	return false
}

func hasNonEmptyIndexColumnOption(values []indexmetadata.IndexColumnOption) bool {
	for _, value := range values {
		if strings.TrimSpace(value.Direction) != "" || strings.TrimSpace(value.NullsOrder) != "" {
			return true
		}
	}
	return false
}
