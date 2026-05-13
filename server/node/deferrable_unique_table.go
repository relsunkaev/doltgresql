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
	"fmt"
	"io"
	"reflect"

	"github.com/cockroachdb/errors"
	doltsqle "github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/server/deferrable"
	"github.com/dolthub/doltgresql/server/indexmetadata"
)

type deferrableUniqueIndex struct {
	index            sql.Index
	constraint       deferrable.UniqueConstraint
	columnIndexes    []int
	columnTypes      []sql.Type
	nullsNotDistinct bool
}

// DeferrableUniqueTable enforces PostgreSQL DEFERRABLE unique and primary-key
// constraints that are stored as non-unique Dolt indexes.
type DeferrableUniqueTable struct {
	underlying sql.Table
	indexes    []deferrableUniqueIndex
}

var _ sql.TableWrapper = (*DeferrableUniqueTable)(nil)
var _ sql.MutableTableWrapper = (*DeferrableUniqueTable)(nil)
var _ sql.InsertableTable = (*DeferrableUniqueTable)(nil)
var _ sql.ReplaceableTable = (*DeferrableUniqueTable)(nil)
var _ sql.UpdatableTable = (*DeferrableUniqueTable)(nil)
var _ sql.IndexAddressable = (*DeferrableUniqueTable)(nil)
var _ sql.IndexedTable = (*DeferrableUniqueTable)(nil)

// WrapDeferrableUniqueTable wraps table when it has DEFERRABLE unique metadata.
func WrapDeferrableUniqueTable(ctx *sql.Context, table sql.Table) (sql.Table, bool, error) {
	if _, ok := table.(*DeferrableUniqueTable); ok {
		return table, false, nil
	}
	indexAddressable, ok := table.(sql.IndexAddressable)
	if !ok {
		return table, false, nil
	}
	indexes, err := indexAddressable.GetIndexes(ctx)
	if err != nil {
		return nil, false, err
	}
	tableSchema := table.Schema(ctx)
	checks := make([]deferrableUniqueIndex, 0)
	for _, index := range indexes {
		metadata, ok := indexmetadata.DecodeComment(index.Comment())
		if !ok || !metadata.Unique || !metadata.Deferrable {
			continue
		}
		if indexmetadata.AccessMethod(index.IndexType(), index.Comment()) != indexmetadata.AccessMethodBtree {
			return nil, false, errors.Errorf("DEFERRABLE unique constraints are not yet supported for %s indexes", index.IndexType())
		}
		check, err := deferrableUniqueIndexFromIndex(ctx, table, index, metadata, tableSchema)
		if err != nil {
			return nil, false, err
		}
		checks = append(checks, check)
	}
	if len(checks) == 0 {
		return table, false, nil
	}
	return &DeferrableUniqueTable{
		underlying: table,
		indexes:    checks,
	}, true, nil
}

func deferrableUniqueIndexFromIndex(ctx *sql.Context, table sql.Table, index sql.Index, metadata indexmetadata.Metadata, schema sql.Schema) (deferrableUniqueIndex, error) {
	logicalColumns := indexmetadata.LogicalColumns(index, schema)
	columnTypes := index.ColumnExpressionTypes(ctx)
	check := deferrableUniqueIndex{
		index:            index,
		columnIndexes:    make([]int, len(logicalColumns)),
		columnTypes:      make([]sql.Type, len(logicalColumns)),
		nullsNotDistinct: metadata.NullsNotDistinct,
	}
	columnNames := make([]string, len(logicalColumns))
	for i, column := range logicalColumns {
		if column.Expression {
			return deferrableUniqueIndex{}, errors.Errorf("DEFERRABLE unique expression indexes are not yet supported")
		}
		columnIndex := schema.IndexOfColName(column.StorageName)
		if columnIndex < 0 {
			return deferrableUniqueIndex{}, sql.ErrKeyColumnDoesNotExist.New(column.StorageName)
		}
		check.columnIndexes[i] = columnIndex
		columnNames[i] = schema[columnIndex].Name
		if i < len(columnTypes) {
			check.columnTypes[i] = columnTypes[i].Type
		} else {
			check.columnTypes[i] = schema[columnIndex].Type
		}
	}
	check.constraint = deferrable.UniqueConstraint{
		Database:          ctx.GetCurrentDatabase(),
		SchemaName:        tableSchemaName(table),
		Table:             table.Name(),
		Name:              indexmetadata.DisplayNameForTable(index, table),
		Columns:           columnNames,
		Primary:           indexmetadata.IsPrimaryConstraint(index),
		NullsNotDistinct:  metadata.NullsNotDistinct,
		InitiallyDeferred: metadata.InitiallyDeferred,
	}
	return check, nil
}

func tableSchemaName(table sql.Table) string {
	if schemaTable, ok := table.(sql.DatabaseSchemaTable); ok && schemaTable.DatabaseSchema() != nil {
		return schemaTable.DatabaseSchema().SchemaName()
	}
	return ""
}

func (t *DeferrableUniqueTable) Underlying() sql.Table {
	return t.underlying
}

func (t *DeferrableUniqueTable) WithUnderlying(table sql.Table) sql.Table {
	return &DeferrableUniqueTable{
		underlying: table,
		indexes:    t.indexes,
	}
}

func (t *DeferrableUniqueTable) Name() string {
	return t.underlying.Name()
}

func (t *DeferrableUniqueTable) String() string {
	return t.underlying.String()
}

func (t *DeferrableUniqueTable) Schema(ctx *sql.Context) sql.Schema {
	return t.underlying.Schema(ctx)
}

func (t *DeferrableUniqueTable) Collation() sql.CollationID {
	return t.underlying.Collation()
}

func (t *DeferrableUniqueTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return t.underlying.Partitions(ctx)
}

func (t *DeferrableUniqueTable) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	return t.underlying.PartitionRows(ctx, partition)
}

func (t *DeferrableUniqueTable) DatabaseSchema() sql.DatabaseSchema {
	if schemaTable, ok := t.underlying.(sql.DatabaseSchemaTable); ok {
		return schemaTable.DatabaseSchema()
	}
	return nil
}

func (t *DeferrableUniqueTable) IndexedAccess(ctx *sql.Context, lookup sql.IndexLookup) sql.IndexedTable {
	if indexAddressable, ok := t.underlying.(sql.IndexAddressable); ok {
		return indexAddressable.IndexedAccess(ctx, lookup)
	}
	return nil
}

func (t *DeferrableUniqueTable) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	if indexAddressable, ok := t.underlying.(sql.IndexAddressable); ok {
		return indexAddressable.GetIndexes(ctx)
	}
	return nil, nil
}

func (t *DeferrableUniqueTable) LookupPartitions(ctx *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	if indexedTable, ok := t.underlying.(sql.IndexedTable); ok {
		return indexedTable.LookupPartitions(ctx, lookup)
	}
	return nil, errors.Errorf("table %s is not indexed", t.Name())
}

func (t *DeferrableUniqueTable) PreciseMatch() bool {
	if indexAddressable, ok := t.underlying.(sql.IndexAddressable); ok {
		return indexAddressable.PreciseMatch()
	}
	return false
}

func (t *DeferrableUniqueTable) Inserter(ctx *sql.Context) sql.RowInserter {
	insertable, ok := t.underlying.(sql.InsertableTable)
	if !ok {
		return sqlutil.NewStaticErrorEditor(planErr("table %s is not insertable", t.Name()))
	}
	return &deferrableUniqueEditor{
		table:   t,
		primary: insertable.Inserter(ctx),
	}
}

func (t *DeferrableUniqueTable) Replacer(ctx *sql.Context) sql.RowReplacer {
	replaceable, ok := t.underlying.(sql.ReplaceableTable)
	if !ok {
		return sqlutil.NewStaticErrorEditor(planErr("table %s is not replaceable", t.Name()))
	}
	return &deferrableUniqueEditor{
		table:   t,
		primary: replaceable.Replacer(ctx),
	}
}

func (t *DeferrableUniqueTable) Updater(ctx *sql.Context) sql.RowUpdater {
	updatable, ok := t.underlying.(sql.UpdatableTable)
	if !ok {
		return sqlutil.NewStaticErrorEditor(planErr("table %s is not updatable", t.Name()))
	}
	return &deferrableUniqueEditor{
		table:   t,
		primary: updatable.Updater(ctx),
	}
}

func (t *DeferrableUniqueTable) findDuplicate(ctx *sql.Context, index deferrableUniqueIndex, key sql.Row, oldRow sql.Row, ignoreRows []sql.Row) (sql.Row, error) {
	table := scanTableForDeferrableUniqueCheck(t.underlying)
	duplicate, usedLookup, err := firstMatchingDeferrableUniqueIndexedRow(ctx, table, index, key, oldRow, ignoreRows)
	if err != nil || usedLookup {
		return duplicate, err
	}
	return firstMatchingDeferrableUniqueRow(ctx, table, index, key, oldRow, ignoreRows)
}

func scanTableForDeferrableUniqueCheck(table sql.Table) sql.Table {
	table = sql.GetUnderlyingTable(table)
	switch table := table.(type) {
	case *doltsqle.IndexedDoltTable:
		return table.DoltTable
	case *doltsqle.WritableIndexedDoltTable:
		return table.WritableDoltTable
	default:
		return table
	}
}

func firstMatchingDeferrableUniqueIndexedRow(ctx *sql.Context, table sql.Table, index deferrableUniqueIndex, key sql.Row, oldRow sql.Row, ignoreRows []sql.Row) (sql.Row, bool, error) {
	indexAddressable, ok := table.(sql.IndexAddressable)
	if !ok {
		return nil, false, nil
	}
	lookup, err := index.lookup(ctx, key)
	if err != nil {
		return nil, true, err
	}
	indexedTable := indexAddressable.IndexedAccess(ctx, lookup)
	if indexedTable == nil {
		return nil, false, nil
	}
	duplicate, err := firstMatchingDeferrableUniqueLookupRow(ctx, indexedTable, lookup, index, key, oldRow, ignoreRows)
	return duplicate, true, err
}

func firstMatchingDeferrableUniqueLookupRow(ctx *sql.Context, table sql.IndexedTable, lookup sql.IndexLookup, index deferrableUniqueIndex, key sql.Row, oldRow sql.Row, ignoreRows []sql.Row) (sql.Row, error) {
	partitions, err := table.LookupPartitions(ctx, lookup)
	if err != nil {
		return nil, err
	}
	defer partitions.Close(ctx)

	for {
		partition, err := partitions.Next(ctx)
		if err == io.EOF {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		rows, err := table.PartitionRows(ctx, partition)
		if err != nil {
			return nil, err
		}
		duplicate, err := nextMatchingDeferrableUniqueRow(ctx, rows, index, key, oldRow, ignoreRows)
		if closeErr := rows.Close(ctx); err == nil {
			err = closeErr
		}
		if err != nil || duplicate != nil {
			return duplicate, err
		}
	}
}

func firstMatchingDeferrableUniqueRow(ctx *sql.Context, table sql.Table, index deferrableUniqueIndex, key sql.Row, oldRow sql.Row, ignoreRows []sql.Row) (sql.Row, error) {
	partitions, err := table.Partitions(ctx)
	if err != nil {
		return nil, err
	}
	defer partitions.Close(ctx)

	for {
		partition, err := partitions.Next(ctx)
		if err == io.EOF {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		rows, err := table.PartitionRows(ctx, partition)
		if err != nil {
			return nil, err
		}
		duplicate, err := nextMatchingDeferrableUniqueRow(ctx, rows, index, key, oldRow, ignoreRows)
		if closeErr := rows.Close(ctx); err == nil {
			err = closeErr
		}
		if err != nil || duplicate != nil {
			return duplicate, err
		}
	}
}

func nextMatchingDeferrableUniqueRow(ctx *sql.Context, rows sql.RowIter, index deferrableUniqueIndex, key sql.Row, oldRow sql.Row, ignoreRows []sql.Row) (sql.Row, error) {
	for {
		row, err := rows.Next(ctx)
		if err == io.EOF {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		if oldRow != nil && reflect.DeepEqual(row, oldRow) {
			continue
		}
		if shouldIgnoreDeferrableUniqueRow(row, ignoreRows) {
			continue
		}
		matches, err := index.rowMatchesKey(ctx, row, key)
		if err != nil {
			return nil, err
		}
		if matches {
			return row, nil
		}
	}
}

func shouldIgnoreDeferrableUniqueRow(row sql.Row, ignoreRows []sql.Row) bool {
	for _, ignoreRow := range ignoreRows {
		if reflect.DeepEqual(row, ignoreRow) {
			return true
		}
	}
	return false
}

func (i deferrableUniqueIndex) key(row sql.Row) (sql.Row, bool) {
	key := make(sql.Row, len(i.columnIndexes))
	hasNull := false
	for n, columnIndex := range i.columnIndexes {
		if columnIndex >= len(row) {
			hasNull = true
			continue
		}
		key[n] = row[columnIndex]
		if key[n] == nil {
			hasNull = true
		}
	}
	return key, hasNull
}

func (i deferrableUniqueIndex) lookup(ctx *sql.Context, key sql.Row) (sql.IndexLookup, error) {
	ranges := make(sql.MySQLRange, len(key))
	for n, value := range key {
		if n >= len(i.columnTypes) || i.columnTypes[n] == nil {
			return sql.IndexLookup{}, errors.Errorf("missing type for DEFERRABLE unique index column %d", n)
		}
		if value == nil {
			ranges[n] = sql.NullRangeColumnExpr(i.columnTypes[n])
		} else {
			ranges[n] = sql.ClosedRangeColumnExpr(value, value, i.columnTypes[n])
		}
	}
	return sql.NewIndexLookup(i.index, sql.MySQLRangeCollection{ranges}, true, false, false, false), nil
}

func (i deferrableUniqueIndex) rowMatchesKey(ctx *sql.Context, row sql.Row, key sql.Row) (bool, error) {
	if len(key) != len(i.columnIndexes) {
		return false, nil
	}
	for n, columnIndex := range i.columnIndexes {
		if columnIndex >= len(row) {
			return false, nil
		}
		matches, err := i.valuesMatch(ctx, n, row[columnIndex], key[n])
		if err != nil || !matches {
			return matches, err
		}
	}
	return true, nil
}

func (i deferrableUniqueIndex) keyMatches(ctx *sql.Context, left sql.Row, right sql.Row) (bool, error) {
	if len(left) != len(right) {
		return false, nil
	}
	for n := range left {
		matches, err := i.valuesMatch(ctx, n, left[n], right[n])
		if err != nil || !matches {
			return matches, err
		}
	}
	return true, nil
}

func (i deferrableUniqueIndex) valuesMatch(ctx *sql.Context, columnIndex int, left any, right any) (bool, error) {
	if left == nil || right == nil {
		return i.nullsNotDistinct && left == nil && right == nil, nil
	}
	if columnIndex < len(i.columnTypes) && i.columnTypes[columnIndex] != nil {
		cmp, err := i.columnTypes[columnIndex].Compare(ctx, left, right)
		if err != nil {
			return false, err
		}
		return cmp == 0, nil
	}
	return reflect.DeepEqual(left, right), nil
}

type deferrableUniquePrimaryEditor interface {
	sql.EditOpenerCloser
	Close(*sql.Context) error
}

type deferrableUniqueEditor struct {
	table       *DeferrableUniqueTable
	primary     deferrableUniquePrimaryEditor
	pendingRows map[int][]pendingDeferrableUniqueRow
	removedRows []sql.Row
}

type pendingDeferrableUniqueRow struct {
	key sql.Row
	row sql.Row
}

var _ sql.TableEditor = (*deferrableUniqueEditor)(nil)

func (e *deferrableUniqueEditor) StatementBegin(ctx *sql.Context) {
	e.pendingRows = nil
	e.removedRows = nil
	e.primary.StatementBegin(ctx)
}

func (e *deferrableUniqueEditor) DiscardChanges(ctx *sql.Context, err error) error {
	e.pendingRows = nil
	e.removedRows = nil
	return e.primary.DiscardChanges(ctx, err)
}

func (e *deferrableUniqueEditor) StatementComplete(ctx *sql.Context) error {
	err := e.primary.StatementComplete(ctx)
	e.pendingRows = nil
	e.removedRows = nil
	return err
}

func (e *deferrableUniqueEditor) Insert(ctx *sql.Context, row sql.Row) error {
	if err := e.validateRow(ctx, row, nil); err != nil {
		return err
	}
	inserter, ok := e.primary.(sql.RowInserter)
	if !ok {
		return errors.Errorf("primary table editor does not support inserts")
	}
	if err := inserter.Insert(ctx, row); err != nil {
		return err
	}
	e.recordPendingRow(ctx, row)
	e.markDeferredDirty(ctx)
	return nil
}

func (e *deferrableUniqueEditor) Delete(ctx *sql.Context, row sql.Row) error {
	deleter, ok := e.primary.(sql.RowDeleter)
	if !ok {
		return errors.Errorf("primary table editor does not support deletes")
	}
	if err := deleter.Delete(ctx, row); err != nil {
		return err
	}
	e.removedRows = append(e.removedRows, row)
	e.markDeferredDirty(ctx)
	return nil
}

func (e *deferrableUniqueEditor) Update(ctx *sql.Context, oldRow sql.Row, newRow sql.Row) error {
	if err := e.validateRow(ctx, newRow, oldRow); err != nil {
		return err
	}
	updater, ok := e.primary.(sql.RowUpdater)
	if !ok {
		return errors.Errorf("primary table editor does not support updates")
	}
	if err := updater.Update(ctx, oldRow, newRow); err != nil {
		return err
	}
	e.removedRows = append(e.removedRows, oldRow)
	e.recordPendingRow(ctx, newRow)
	e.markDeferredDirty(ctx)
	return nil
}

func (e *deferrableUniqueEditor) Close(ctx *sql.Context) error {
	err := e.primary.Close(ctx)
	e.pendingRows = nil
	e.removedRows = nil
	return err
}

func (e *deferrableUniqueEditor) validateRow(ctx *sql.Context, row sql.Row, oldRow sql.Row) error {
	for indexOffset, index := range e.table.indexes {
		if deferrable.ShouldDeferUnique(ctx.Session.ID(), index.constraint) {
			continue
		}
		key, hasNull := index.key(row)
		if hasNull && !index.nullsNotDistinct {
			continue
		}
		if duplicate, err := e.pendingDuplicate(ctx, indexOffset, index, key); err != nil || duplicate != nil {
			if err != nil {
				return err
			}
			return sql.NewUniqueKeyErr(fmt.Sprintf("%v", key), index.constraint.Primary, duplicate)
		}
		duplicate, err := e.table.findDuplicate(ctx, index, key, oldRow, e.removedRows)
		if err != nil {
			return err
		}
		if duplicate != nil {
			return sql.NewUniqueKeyErr(fmt.Sprintf("%v", key), index.constraint.Primary, duplicate)
		}
	}
	return nil
}

func (e *deferrableUniqueEditor) pendingDuplicate(ctx *sql.Context, indexOffset int, index deferrableUniqueIndex, key sql.Row) (sql.Row, error) {
	for _, pending := range e.pendingRows[indexOffset] {
		matches, err := index.keyMatches(ctx, pending.key, key)
		if err != nil || matches {
			return pending.row, err
		}
	}
	return nil, nil
}

func (e *deferrableUniqueEditor) recordPendingRow(ctx *sql.Context, row sql.Row) {
	for indexOffset, index := range e.table.indexes {
		if deferrable.ShouldDeferUnique(ctx.Session.ID(), index.constraint) {
			continue
		}
		key, hasNull := index.key(row)
		if hasNull && !index.nullsNotDistinct {
			continue
		}
		if e.pendingRows == nil {
			e.pendingRows = make(map[int][]pendingDeferrableUniqueRow, len(e.table.indexes))
		}
		e.pendingRows[indexOffset] = append(e.pendingRows[indexOffset], pendingDeferrableUniqueRow{
			key: key,
			row: row,
		})
	}
}

func (e *deferrableUniqueEditor) markDeferredDirty(ctx *sql.Context) {
	for _, index := range e.table.indexes {
		if deferrable.ShouldDeferUnique(ctx.Session.ID(), index.constraint) {
			deferrable.MarkUniqueDirty(ctx.Session.ID(), index.constraint)
		}
	}
}
