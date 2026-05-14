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
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	goerrors "gopkg.in/src-d/go-errors.v1"
)

// VirtualColumnUpdateTable pads rows with virtual column values before update
// and replace/delete editors see them. INSERT ... ON DUPLICATE KEY UPDATE uses
// the duplicate-key error's stored row as the old row; stored rows do not carry
// virtual expression-index columns, but Dolt secondary index writers expect
// those values when maintaining the index.
type VirtualColumnUpdateTable struct {
	underlying  sql.Table
	schema      sql.Schema
	projections []sql.Expression
}

var _ sql.TableWrapper = (*VirtualColumnUpdateTable)(nil)
var _ sql.MutableTableWrapper = (*VirtualColumnUpdateTable)(nil)
var _ sql.InsertableTable = (*VirtualColumnUpdateTable)(nil)
var _ sql.ReplaceableTable = (*VirtualColumnUpdateTable)(nil)
var _ sql.UpdatableTable = (*VirtualColumnUpdateTable)(nil)
var _ sql.IndexAddressable = (*VirtualColumnUpdateTable)(nil)
var _ sql.IndexedTable = (*VirtualColumnUpdateTable)(nil)

// WrapVirtualColumnUpdateTable wraps tables that contain a VirtualColumnTable.
func WrapVirtualColumnUpdateTable(ctx *sql.Context, table sql.Table) (sql.Table, bool) {
	if _, ok := table.(*VirtualColumnUpdateTable); ok {
		return table, false
	}
	virtualTable, ok := plan.FindVirtualColumnTable(table)
	if !ok || !virtualTable.Schema(ctx).HasVirtualColumns() {
		return table, false
	}
	return &VirtualColumnUpdateTable{
		underlying:  table,
		schema:      virtualTable.Schema(ctx),
		projections: virtualTable.Projections,
	}, true
}

func (t *VirtualColumnUpdateTable) Underlying() sql.Table {
	return t.underlying
}

func (t *VirtualColumnUpdateTable) WithUnderlying(table sql.Table) sql.Table {
	return &VirtualColumnUpdateTable{
		underlying:  table,
		schema:      t.schema,
		projections: t.projections,
	}
}

func (t *VirtualColumnUpdateTable) Name() string {
	return t.underlying.Name()
}

func (t *VirtualColumnUpdateTable) String() string {
	return t.underlying.String()
}

func (t *VirtualColumnUpdateTable) Schema(ctx *sql.Context) sql.Schema {
	return t.underlying.Schema(ctx)
}

func (t *VirtualColumnUpdateTable) Collation() sql.CollationID {
	return t.underlying.Collation()
}

func (t *VirtualColumnUpdateTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return t.underlying.Partitions(ctx)
}

func (t *VirtualColumnUpdateTable) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	return t.underlying.PartitionRows(ctx, partition)
}

func (t *VirtualColumnUpdateTable) DatabaseSchema() sql.DatabaseSchema {
	for table := t.underlying; table != nil; {
		if schemaTable, ok := table.(sql.DatabaseSchemaTable); ok {
			if databaseSchema := schemaTable.DatabaseSchema(); databaseSchema != nil {
				return databaseSchema
			}
		}
		wrapper, ok := table.(sql.TableWrapper)
		if !ok {
			return nil
		}
		table = wrapper.Underlying()
	}
	return nil
}

func (t *VirtualColumnUpdateTable) IndexedAccess(ctx *sql.Context, lookup sql.IndexLookup) sql.IndexedTable {
	if indexAddressable, ok := findIndexAddressableTable(t.underlying); ok {
		return indexAddressable.IndexedAccess(ctx, lookup)
	}
	return nil
}

func (t *VirtualColumnUpdateTable) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	if indexAddressable, ok := findIndexAddressableTable(t.underlying); ok {
		return indexAddressable.GetIndexes(ctx)
	}
	return nil, nil
}

func (t *VirtualColumnUpdateTable) LookupPartitions(ctx *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	if indexedTable, ok := findIndexedTable(t.underlying); ok {
		return indexedTable.LookupPartitions(ctx, lookup)
	}
	return nil, planErr("table %s is not indexed", t.Name())
}

func (t *VirtualColumnUpdateTable) PreciseMatch() bool {
	if indexAddressable, ok := findIndexAddressableTable(t.underlying); ok {
		return indexAddressable.PreciseMatch()
	}
	return false
}

func (t *VirtualColumnUpdateTable) Inserter(ctx *sql.Context) sql.RowInserter {
	insertable, ok := findInsertableTable(t.underlying)
	if !ok {
		return sqlutil.NewStaticErrorEditor(planErr("table %s is not insertable", t.Name()))
	}
	return &virtualColumnRowInserter{
		padder:  virtualColumnRowPadder{schema: t.schema, projections: t.projections},
		primary: insertable.Inserter(ctx),
	}
}

func (t *VirtualColumnUpdateTable) Replacer(ctx *sql.Context) sql.RowReplacer {
	replaceable, ok := findReplaceableTable(t.underlying)
	if !ok {
		return sqlutil.NewStaticErrorEditor(planErr("table %s is not replaceable", t.Name()))
	}
	return &virtualColumnRowReplacer{
		padder:  virtualColumnRowPadder{schema: t.schema, projections: t.projections},
		primary: replaceable.Replacer(ctx),
	}
}

func (t *VirtualColumnUpdateTable) Updater(ctx *sql.Context) sql.RowUpdater {
	updatable, ok := findUpdatableTable(t.underlying)
	if !ok {
		return sqlutil.NewStaticErrorEditor(planErr("table %s is not updatable", t.Name()))
	}
	return &virtualColumnRowUpdater{
		padder:  virtualColumnRowPadder{schema: t.schema, projections: t.projections},
		primary: updatable.Updater(ctx),
	}
}

func findInsertableTable(table sql.Table) (sql.InsertableTable, bool) {
	switch table := table.(type) {
	case sql.InsertableTable:
		return table, true
	case sql.TableWrapper:
		return findInsertableTable(table.Underlying())
	default:
		return nil, false
	}
}

func findReplaceableTable(table sql.Table) (sql.ReplaceableTable, bool) {
	switch table := table.(type) {
	case sql.ReplaceableTable:
		return table, true
	case sql.TableWrapper:
		return findReplaceableTable(table.Underlying())
	default:
		return nil, false
	}
}

func findUpdatableTable(table sql.Table) (sql.UpdatableTable, bool) {
	switch table := table.(type) {
	case sql.UpdatableTable:
		return table, true
	case sql.TableWrapper:
		return findUpdatableTable(table.Underlying())
	default:
		return nil, false
	}
}

func findIndexAddressableTable(table sql.Table) (sql.IndexAddressable, bool) {
	switch table := table.(type) {
	case sql.IndexAddressable:
		return table, true
	case sql.TableWrapper:
		return findIndexAddressableTable(table.Underlying())
	default:
		return nil, false
	}
}

func findIndexedTable(table sql.Table) (sql.IndexedTable, bool) {
	switch table := table.(type) {
	case sql.IndexedTable:
		return table, true
	case sql.TableWrapper:
		return findIndexedTable(table.Underlying())
	default:
		return nil, false
	}
}

type virtualColumnRowPadder struct {
	schema      sql.Schema
	projections []sql.Expression
}

func (p virtualColumnRowPadder) pad(ctx *sql.Context, row sql.Row) (sql.Row, error) {
	if row == nil || len(row) >= len(p.schema) {
		return row, nil
	}
	fullRow := make(sql.Row, len(p.schema))
	sourceIndex := 0
	for targetIndex, column := range p.schema {
		if column.Virtual {
			continue
		}
		if sourceIndex < len(row) {
			fullRow[targetIndex] = row[sourceIndex]
		}
		sourceIndex++
	}
	for targetIndex, column := range p.schema {
		if !column.Virtual {
			continue
		}
		if targetIndex >= len(p.projections) || p.projections[targetIndex] == nil {
			continue
		}
		value, err := p.projections[targetIndex].Eval(ctx, fullRow)
		if err != nil {
			return nil, err
		}
		fullRow[targetIndex] = value
	}
	return fullRow, nil
}

func (p virtualColumnRowPadder) padUniqueKeyError(ctx *sql.Context, err error) error {
	wrapped, ok := err.(*goerrors.Error)
	if !ok {
		return err
	}
	uniqueKeyError, ok := wrapped.Cause().(sql.UniqueKeyError)
	if !ok || uniqueKeyError.Existing == nil {
		return err
	}
	existing, padErr := p.pad(ctx, uniqueKeyError.Existing)
	if padErr != nil {
		return padErr
	}
	return sql.NewUniqueKeyErr(uniqueKeyError.Error(), uniqueKeyError.IsPK, existing)
}

type virtualColumnRowInserter struct {
	padder  virtualColumnRowPadder
	primary sql.RowInserter
}

var _ sql.RowInserter = (*virtualColumnRowInserter)(nil)

func (e *virtualColumnRowInserter) StatementBegin(ctx *sql.Context) {
	e.primary.StatementBegin(ctx)
}

func (e *virtualColumnRowInserter) DiscardChanges(ctx *sql.Context, err error) error {
	return e.primary.DiscardChanges(ctx, err)
}

func (e *virtualColumnRowInserter) StatementComplete(ctx *sql.Context) error {
	return e.primary.StatementComplete(ctx)
}

func (e *virtualColumnRowInserter) Insert(ctx *sql.Context, row sql.Row) error {
	if err := e.primary.Insert(ctx, row); err != nil {
		return e.padder.padUniqueKeyError(ctx, err)
	}
	return nil
}

func (e *virtualColumnRowInserter) Close(ctx *sql.Context) error {
	return e.primary.Close(ctx)
}

type virtualColumnRowUpdater struct {
	padder  virtualColumnRowPadder
	primary sql.RowUpdater
}

var _ sql.RowUpdater = (*virtualColumnRowUpdater)(nil)

func (e *virtualColumnRowUpdater) StatementBegin(ctx *sql.Context) {
	e.primary.StatementBegin(ctx)
}

func (e *virtualColumnRowUpdater) DiscardChanges(ctx *sql.Context, err error) error {
	return e.primary.DiscardChanges(ctx, err)
}

func (e *virtualColumnRowUpdater) StatementComplete(ctx *sql.Context) error {
	return e.primary.StatementComplete(ctx)
}

func (e *virtualColumnRowUpdater) Update(ctx *sql.Context, oldRow sql.Row, newRow sql.Row) error {
	var err error
	oldRow, err = e.padder.pad(ctx, oldRow)
	if err != nil {
		return err
	}
	newRow, err = e.padder.pad(ctx, newRow)
	if err != nil {
		return err
	}
	return e.primary.Update(ctx, oldRow, newRow)
}

func (e *virtualColumnRowUpdater) Close(ctx *sql.Context) error {
	return e.primary.Close(ctx)
}

type virtualColumnRowReplacer struct {
	padder  virtualColumnRowPadder
	primary sql.RowReplacer
}

var _ sql.RowReplacer = (*virtualColumnRowReplacer)(nil)

func (e *virtualColumnRowReplacer) StatementBegin(ctx *sql.Context) {
	e.primary.StatementBegin(ctx)
}

func (e *virtualColumnRowReplacer) DiscardChanges(ctx *sql.Context, err error) error {
	return e.primary.DiscardChanges(ctx, err)
}

func (e *virtualColumnRowReplacer) StatementComplete(ctx *sql.Context) error {
	return e.primary.StatementComplete(ctx)
}

func (e *virtualColumnRowReplacer) Insert(ctx *sql.Context, row sql.Row) error {
	if err := e.primary.Insert(ctx, row); err != nil {
		return e.padder.padUniqueKeyError(ctx, err)
	}
	return nil
}

func (e *virtualColumnRowReplacer) Delete(ctx *sql.Context, row sql.Row) error {
	var err error
	row, err = e.padder.pad(ctx, row)
	if err != nil {
		return err
	}
	return e.primary.Delete(ctx, row)
}

func (e *virtualColumnRowReplacer) Close(ctx *sql.Context) error {
	return e.primary.Close(ctx)
}
