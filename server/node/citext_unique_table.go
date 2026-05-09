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

	"github.com/dolthub/doltgresql/server/indexmetadata"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

type citextUniqueIndex struct {
	index            sql.Index
	columnIndexes    []int
	columnTypes      []sql.Type
	nullsNotDistinct bool
}

// CitextUniqueTable enforces PostgreSQL citext equality for unique indexes.
// Native Dolt unique indexes compare the stored string bytes, while citext
// uniqueness must compare case-insensitively and still preserve the original
// text value in the table row.
type CitextUniqueTable struct {
	underlying sql.Table
	indexes    []citextUniqueIndex
}

var _ sql.TableWrapper = (*CitextUniqueTable)(nil)
var _ sql.MutableTableWrapper = (*CitextUniqueTable)(nil)
var _ sql.InsertableTable = (*CitextUniqueTable)(nil)
var _ sql.ReplaceableTable = (*CitextUniqueTable)(nil)
var _ sql.UpdatableTable = (*CitextUniqueTable)(nil)
var _ sql.IndexAddressable = (*CitextUniqueTable)(nil)
var _ sql.IndexedTable = (*CitextUniqueTable)(nil)

// WrapCitextUniqueTable wraps table when it has a unique btree index that
// includes at least one citext column.
func WrapCitextUniqueTable(ctx *sql.Context, table sql.Table) (sql.Table, bool, error) {
	if _, ok := table.(*CitextUniqueTable); ok {
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
	checks := make([]citextUniqueIndex, 0)
	for _, index := range indexes {
		if !index.IsUnique() {
			continue
		}
		logicalColumns := indexmetadata.LogicalColumns(index, tableSchema)
		if len(logicalColumns) == 0 {
			continue
		}
		columnTypes := index.ColumnExpressionTypes(ctx)
		hasCitext := false
		for i, column := range logicalColumns {
			if column.Expression {
				if i < len(columnTypes) && isCitextType(columnTypes[i].Type) {
					hasCitext = true
				}
				continue
			}
			columnIndex := tableSchema.IndexOfColName(column.StorageName)
			if columnIndex < 0 {
				return nil, false, sql.ErrKeyColumnDoesNotExist.New(column.StorageName)
			}
			if isCitextType(tableSchema[columnIndex].Type) {
				hasCitext = true
			}
		}
		if !hasCitext {
			continue
		}
		if indexmetadata.AccessMethod(index.IndexType(), index.Comment()) != indexmetadata.AccessMethodBtree {
			return nil, false, errors.Errorf("citext unique indexes are not yet supported for %s indexes", index.IndexType())
		}
		check := citextUniqueIndex{
			index:            index,
			columnIndexes:    make([]int, len(logicalColumns)),
			columnTypes:      make([]sql.Type, len(logicalColumns)),
			nullsNotDistinct: indexmetadata.NullsNotDistinct(index.Comment()),
		}
		for i, column := range logicalColumns {
			if column.Expression {
				return nil, false, errors.Errorf("citext unique expression indexes are not yet supported")
			}
			columnIndex := tableSchema.IndexOfColName(column.StorageName)
			if columnIndex < 0 {
				return nil, false, sql.ErrKeyColumnDoesNotExist.New(column.StorageName)
			}
			check.columnIndexes[i] = columnIndex
			check.columnTypes[i] = tableSchema[columnIndex].Type
		}
		checks = append(checks, check)
	}
	if len(checks) == 0 {
		return table, false, nil
	}
	return &CitextUniqueTable{
		underlying: table,
		indexes:    checks,
	}, true, nil
}

func isCitextType(typ sql.Type) bool {
	doltgresType, ok := typ.(*pgtypes.DoltgresType)
	return ok && doltgresType.ID.TypeName() == "citext"
}

func (t *CitextUniqueTable) Underlying() sql.Table {
	return t.underlying
}

func (t *CitextUniqueTable) WithUnderlying(table sql.Table) sql.Table {
	return &CitextUniqueTable{
		underlying: table,
		indexes:    t.indexes,
	}
}

func (t *CitextUniqueTable) Name() string {
	return t.underlying.Name()
}

func (t *CitextUniqueTable) String() string {
	return t.underlying.String()
}

func (t *CitextUniqueTable) Schema(ctx *sql.Context) sql.Schema {
	return t.underlying.Schema(ctx)
}

func (t *CitextUniqueTable) Collation() sql.CollationID {
	return t.underlying.Collation()
}

func (t *CitextUniqueTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return t.underlying.Partitions(ctx)
}

func (t *CitextUniqueTable) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	return t.underlying.PartitionRows(ctx, partition)
}

func (t *CitextUniqueTable) DatabaseSchema() sql.DatabaseSchema {
	if schemaTable, ok := t.underlying.(sql.DatabaseSchemaTable); ok {
		return schemaTable.DatabaseSchema()
	}
	return nil
}

func (t *CitextUniqueTable) IndexedAccess(ctx *sql.Context, lookup sql.IndexLookup) sql.IndexedTable {
	if indexAddressable, ok := t.underlying.(sql.IndexAddressable); ok {
		return indexAddressable.IndexedAccess(ctx, lookup)
	}
	return nil
}

func (t *CitextUniqueTable) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	if indexAddressable, ok := t.underlying.(sql.IndexAddressable); ok {
		return indexAddressable.GetIndexes(ctx)
	}
	return nil, nil
}

func (t *CitextUniqueTable) LookupPartitions(ctx *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	if indexedTable, ok := t.underlying.(sql.IndexedTable); ok {
		return indexedTable.LookupPartitions(ctx, lookup)
	}
	return nil, errors.Errorf("table %s is not indexed", t.Name())
}

func (t *CitextUniqueTable) PreciseMatch() bool {
	if indexAddressable, ok := t.underlying.(sql.IndexAddressable); ok {
		return indexAddressable.PreciseMatch()
	}
	return false
}

func (t *CitextUniqueTable) Inserter(ctx *sql.Context) sql.RowInserter {
	insertable, ok := t.underlying.(sql.InsertableTable)
	if !ok {
		return sqlutil.NewStaticErrorEditor(planErr("table %s is not insertable", t.Name()))
	}
	return &citextUniqueEditor{
		table:   t,
		primary: insertable.Inserter(ctx),
	}
}

func (t *CitextUniqueTable) Replacer(ctx *sql.Context) sql.RowReplacer {
	replaceable, ok := t.underlying.(sql.ReplaceableTable)
	if !ok {
		return sqlutil.NewStaticErrorEditor(planErr("table %s is not replaceable", t.Name()))
	}
	return &citextUniqueEditor{
		table:   t,
		primary: replaceable.Replacer(ctx),
	}
}

func (t *CitextUniqueTable) Updater(ctx *sql.Context) sql.RowUpdater {
	updatable, ok := t.underlying.(sql.UpdatableTable)
	if !ok {
		return sqlutil.NewStaticErrorEditor(planErr("table %s is not updatable", t.Name()))
	}
	return &citextUniqueEditor{
		table:   t,
		primary: updatable.Updater(ctx),
	}
}

func (t *CitextUniqueTable) findDuplicate(ctx *sql.Context, index citextUniqueIndex, key sql.Row, oldRow sql.Row, ignoreRows []sql.Row) (sql.Row, error) {
	table := scanTableForCitextUniqueCheck(t.underlying)
	return firstMatchingCitextUniqueRow(ctx, table, index, key, oldRow, ignoreRows)
}

func scanTableForCitextUniqueCheck(table sql.Table) sql.Table {
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

func firstMatchingCitextUniqueRow(ctx *sql.Context, table sql.Table, index citextUniqueIndex, key sql.Row, oldRow sql.Row, ignoreRows []sql.Row) (sql.Row, error) {
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
		duplicate, err := nextMatchingCitextUniqueRow(ctx, rows, index, key, oldRow, ignoreRows)
		if closeErr := rows.Close(ctx); err == nil {
			err = closeErr
		}
		if err != nil || duplicate != nil {
			return duplicate, err
		}
	}
}

func nextMatchingCitextUniqueRow(ctx *sql.Context, rows sql.RowIter, index citextUniqueIndex, key sql.Row, oldRow sql.Row, ignoreRows []sql.Row) (sql.Row, error) {
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
		if shouldIgnoreCitextUniqueRow(row, ignoreRows) {
			continue
		}
		matches, err := index.rowMatchesKey(ctx, row, key)
		if err != nil {
			return nil, err
		}
		if !matches {
			continue
		}
		return row, nil
	}
}

func shouldIgnoreCitextUniqueRow(row sql.Row, ignoreRows []sql.Row) bool {
	for _, ignoreRow := range ignoreRows {
		if reflect.DeepEqual(row, ignoreRow) {
			return true
		}
	}
	return false
}

func (i citextUniqueIndex) key(row sql.Row) (sql.Row, bool) {
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

func (i citextUniqueIndex) rowMatchesKey(ctx *sql.Context, row sql.Row, key sql.Row) (bool, error) {
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

func (i citextUniqueIndex) keyMatches(ctx *sql.Context, left sql.Row, right sql.Row) (bool, error) {
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

func (i citextUniqueIndex) valuesMatch(ctx *sql.Context, columnIndex int, left any, right any) (bool, error) {
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

type citextUniquePrimaryEditor interface {
	sql.EditOpenerCloser
	Close(*sql.Context) error
}

type citextUniqueEditor struct {
	table       *CitextUniqueTable
	primary     citextUniquePrimaryEditor
	pendingRows map[int][]pendingCitextUniqueRow
	removedRows []sql.Row
}

type pendingCitextUniqueRow struct {
	key sql.Row
	row sql.Row
}

var _ sql.TableEditor = (*citextUniqueEditor)(nil)

func (e *citextUniqueEditor) StatementBegin(ctx *sql.Context) {
	e.pendingRows = nil
	e.removedRows = nil
	e.primary.StatementBegin(ctx)
}

func (e *citextUniqueEditor) DiscardChanges(ctx *sql.Context, err error) error {
	e.pendingRows = nil
	e.removedRows = nil
	return e.primary.DiscardChanges(ctx, err)
}

func (e *citextUniqueEditor) StatementComplete(ctx *sql.Context) error {
	err := e.primary.StatementComplete(ctx)
	e.pendingRows = nil
	e.removedRows = nil
	return err
}

func (e *citextUniqueEditor) Insert(ctx *sql.Context, row sql.Row) error {
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
	e.recordPendingRow(row)
	return nil
}

func (e *citextUniqueEditor) Delete(ctx *sql.Context, row sql.Row) error {
	deleter, ok := e.primary.(sql.RowDeleter)
	if !ok {
		return errors.Errorf("primary table editor does not support deletes")
	}
	if err := deleter.Delete(ctx, row); err != nil {
		return err
	}
	e.removedRows = append(e.removedRows, row)
	return nil
}

func (e *citextUniqueEditor) Update(ctx *sql.Context, oldRow sql.Row, newRow sql.Row) error {
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
	e.recordPendingRow(newRow)
	return nil
}

func (e *citextUniqueEditor) Close(ctx *sql.Context) error {
	err := e.primary.Close(ctx)
	e.pendingRows = nil
	e.removedRows = nil
	return err
}

func (e *citextUniqueEditor) validateRow(ctx *sql.Context, row sql.Row, oldRow sql.Row) error {
	for indexOffset, index := range e.table.indexes {
		key, hasNull := index.key(row)
		if hasNull && !index.nullsNotDistinct {
			continue
		}
		if duplicate, err := e.pendingDuplicate(ctx, indexOffset, index, key); err != nil || duplicate != nil {
			if err != nil {
				return err
			}
			return sql.NewUniqueKeyErr(fmt.Sprintf("%v", key), false, duplicate)
		}
		duplicate, err := e.table.findDuplicate(ctx, index, key, oldRow, e.removedRows)
		if err != nil {
			return err
		}
		if duplicate != nil {
			return sql.NewUniqueKeyErr(fmt.Sprintf("%v", key), false, duplicate)
		}
	}
	return nil
}

func (e *citextUniqueEditor) pendingDuplicate(ctx *sql.Context, indexOffset int, index citextUniqueIndex, key sql.Row) (sql.Row, error) {
	for _, pending := range e.pendingRows[indexOffset] {
		matches, err := index.keyMatches(ctx, pending.key, key)
		if err != nil || matches {
			return pending.row, err
		}
	}
	return nil, nil
}

func (e *citextUniqueEditor) recordPendingRow(row sql.Row) {
	for indexOffset, index := range e.table.indexes {
		key, hasNull := index.key(row)
		if hasNull && !index.nullsNotDistinct {
			continue
		}
		if e.pendingRows == nil {
			e.pendingRows = make(map[int][]pendingCitextUniqueRow, len(e.table.indexes))
		}
		e.pendingRows[indexOffset] = append(e.pendingRows[indexOffset], pendingCitextUniqueRow{
			key: key,
			row: row,
		})
	}
}
