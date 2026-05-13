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
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/go-mysql-server/sql"
)

// BeforeInsertVisibleTable makes row edits visible to trigger-side SQL between
// row iterations for INSERTs that have row-level BEFORE triggers.
type BeforeInsertVisibleTable struct {
	underlying sql.Table
	dbName     string
}

var _ sql.TableWrapper = (*BeforeInsertVisibleTable)(nil)
var _ sql.MutableTableWrapper = (*BeforeInsertVisibleTable)(nil)
var _ sql.InsertableTable = (*BeforeInsertVisibleTable)(nil)
var _ sql.ReplaceableTable = (*BeforeInsertVisibleTable)(nil)
var _ sql.UpdatableTable = (*BeforeInsertVisibleTable)(nil)
var _ sql.IndexAddressable = (*BeforeInsertVisibleTable)(nil)
var _ sql.IndexedTable = (*BeforeInsertVisibleTable)(nil)

// WrapBeforeInsertVisibleTable wraps INSERT targets that need PostgreSQL-style
// row visibility during row-level BEFORE INSERT trigger execution.
func WrapBeforeInsertVisibleTable(dbName string, table sql.Table) (sql.Table, bool) {
	if _, ok := table.(*BeforeInsertVisibleTable); ok {
		return table, false
	}
	if _, ok := table.(sql.InsertableTable); !ok {
		return table, false
	}
	return &BeforeInsertVisibleTable{
		underlying: table,
		dbName:     dbName,
	}, true
}

func (t *BeforeInsertVisibleTable) Underlying() sql.Table {
	return t.underlying
}

func (t *BeforeInsertVisibleTable) WithUnderlying(table sql.Table) sql.Table {
	return &BeforeInsertVisibleTable{
		underlying: table,
		dbName:     t.dbName,
	}
}

func (t *BeforeInsertVisibleTable) Name() string {
	return t.underlying.Name()
}

func (t *BeforeInsertVisibleTable) String() string {
	return t.underlying.String()
}

func (t *BeforeInsertVisibleTable) Schema(ctx *sql.Context) sql.Schema {
	return t.underlying.Schema(ctx)
}

func (t *BeforeInsertVisibleTable) Collation() sql.CollationID {
	return t.underlying.Collation()
}

func (t *BeforeInsertVisibleTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return t.underlying.Partitions(ctx)
}

func (t *BeforeInsertVisibleTable) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	return t.underlying.PartitionRows(ctx, partition)
}

func (t *BeforeInsertVisibleTable) DatabaseSchema() sql.DatabaseSchema {
	if schemaTable, ok := t.underlying.(sql.DatabaseSchemaTable); ok {
		return schemaTable.DatabaseSchema()
	}
	return nil
}

func (t *BeforeInsertVisibleTable) IndexedAccess(ctx *sql.Context, lookup sql.IndexLookup) sql.IndexedTable {
	if indexAddressable, ok := t.underlying.(sql.IndexAddressable); ok {
		return indexAddressable.IndexedAccess(ctx, lookup)
	}
	return nil
}

func (t *BeforeInsertVisibleTable) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	if indexAddressable, ok := t.underlying.(sql.IndexAddressable); ok {
		return indexAddressable.GetIndexes(ctx)
	}
	return nil, nil
}

func (t *BeforeInsertVisibleTable) LookupPartitions(ctx *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	if indexedTable, ok := t.underlying.(sql.IndexedTable); ok {
		return indexedTable.LookupPartitions(ctx, lookup)
	}
	return nil, planErr("table %s is not indexed", t.Name())
}

func (t *BeforeInsertVisibleTable) PreciseMatch() bool {
	if indexAddressable, ok := t.underlying.(sql.IndexAddressable); ok {
		return indexAddressable.PreciseMatch()
	}
	return false
}

func (t *BeforeInsertVisibleTable) Inserter(ctx *sql.Context) sql.RowInserter {
	insertable, ok := t.underlying.(sql.InsertableTable)
	if !ok {
		return sqlutil.NewStaticErrorEditor(planErr("table %s is not insertable", t.Name()))
	}
	editor := &beforeInsertVisibleInserter{
		beforeInsertVisibleEditor: beforeInsertVisibleEditor{table: t},
		insertable:                insertable,
	}
	editor.primary = insertable.Inserter(ctx)
	return editor
}

func (t *BeforeInsertVisibleTable) Replacer(ctx *sql.Context) sql.RowReplacer {
	replaceable, ok := t.underlying.(sql.ReplaceableTable)
	if !ok {
		return sqlutil.NewStaticErrorEditor(planErr("table %s is not replaceable", t.Name()))
	}
	editor := &beforeInsertVisibleReplacer{
		beforeInsertVisibleEditor: beforeInsertVisibleEditor{table: t},
		replaceable:               replaceable,
	}
	editor.primary = replaceable.Replacer(ctx)
	return editor
}

func (t *BeforeInsertVisibleTable) Updater(ctx *sql.Context) sql.RowUpdater {
	updatable, ok := t.underlying.(sql.UpdatableTable)
	if !ok {
		return sqlutil.NewStaticErrorEditor(planErr("table %s is not updatable", t.Name()))
	}
	editor := &beforeInsertVisibleUpdater{
		beforeInsertVisibleEditor: beforeInsertVisibleEditor{table: t},
		updatable:                 updatable,
	}
	editor.primary = updatable.Updater(ctx)
	return editor
}

type beforeInsertVisibleEditor struct {
	table        *BeforeInsertVisibleTable
	primary      beforeInsertVisiblePrimary
	statement    bool
	startRoot    doltdb.RootValue
	hasStartRoot bool
}

type beforeInsertVisiblePrimary interface {
	sql.EditOpenerCloser
	sql.Closer
}

func (e *beforeInsertVisibleEditor) statementBegin(ctx *sql.Context) {
	if !e.statement {
		e.statement = true
		roots, ok := dsess.DSessFromSess(ctx.Session).GetRoots(ctx, e.dbName(ctx))
		if ok {
			e.startRoot = roots.Working
			e.hasStartRoot = true
		}
	}
	if e.primary != nil {
		e.primary.StatementBegin(ctx)
	}
}

func (e *beforeInsertVisibleEditor) discardChanges(ctx *sql.Context, err error) error {
	var ret error
	if e.primary != nil {
		ret = e.primary.DiscardChanges(ctx, err)
	}
	if e.hasStartRoot {
		if restoreErr := dsess.DSessFromSess(ctx.Session).SetWorkingRoot(ctx, e.dbName(ctx), e.startRoot); ret == nil {
			ret = restoreErr
		}
	}
	return ret
}

func (e *beforeInsertVisibleEditor) statementComplete(ctx *sql.Context) error {
	if e.primary == nil {
		return nil
	}
	return e.primary.StatementComplete(ctx)
}

func (e *beforeInsertVisibleEditor) close(ctx *sql.Context) error {
	if e.primary == nil {
		return nil
	}
	return e.primary.Close(ctx)
}

func (e *beforeInsertVisibleEditor) flushRow(ctx *sql.Context) error {
	if e.primary == nil {
		return nil
	}
	if err := e.primary.StatementComplete(ctx); err != nil {
		return err
	}
	if err := e.primary.Close(ctx); err != nil {
		return err
	}
	e.primary = nil
	return nil
}

func (e *beforeInsertVisibleEditor) dbName(ctx *sql.Context) string {
	if e.table.dbName != "" {
		return e.table.dbName
	}
	return ctx.GetCurrentDatabase()
}

type beforeInsertVisibleInserter struct {
	beforeInsertVisibleEditor
	insertable sql.InsertableTable
}

var _ sql.RowInserter = (*beforeInsertVisibleInserter)(nil)

func (e *beforeInsertVisibleInserter) StatementBegin(ctx *sql.Context) {
	e.statementBegin(ctx)
}

func (e *beforeInsertVisibleInserter) DiscardChanges(ctx *sql.Context, err error) error {
	return e.discardChanges(ctx, err)
}

func (e *beforeInsertVisibleInserter) StatementComplete(ctx *sql.Context) error {
	return e.statementComplete(ctx)
}

func (e *beforeInsertVisibleInserter) Insert(ctx *sql.Context, row sql.Row) error {
	if e.primary == nil {
		e.primary = e.insertable.Inserter(ctx)
		if e.statement {
			e.primary.StatementBegin(ctx)
		}
	}
	inserter, ok := e.primary.(sql.RowInserter)
	if !ok {
		return planErr("primary table editor does not support inserts")
	}
	if err := inserter.Insert(ctx, row); err != nil {
		return err
	}
	return e.flushRow(ctx)
}

func (e *beforeInsertVisibleInserter) Close(ctx *sql.Context) error {
	return e.close(ctx)
}

type beforeInsertVisibleUpdater struct {
	beforeInsertVisibleEditor
	updatable sql.UpdatableTable
}

var _ sql.RowUpdater = (*beforeInsertVisibleUpdater)(nil)

func (e *beforeInsertVisibleUpdater) StatementBegin(ctx *sql.Context) {
	e.statementBegin(ctx)
}

func (e *beforeInsertVisibleUpdater) DiscardChanges(ctx *sql.Context, err error) error {
	return e.discardChanges(ctx, err)
}

func (e *beforeInsertVisibleUpdater) StatementComplete(ctx *sql.Context) error {
	return e.statementComplete(ctx)
}

func (e *beforeInsertVisibleUpdater) Update(ctx *sql.Context, oldRow sql.Row, newRow sql.Row) error {
	if e.primary == nil {
		e.primary = e.updatable.Updater(ctx)
		if e.statement {
			e.primary.StatementBegin(ctx)
		}
	}
	updater, ok := e.primary.(sql.RowUpdater)
	if !ok {
		return planErr("primary table editor does not support updates")
	}
	if err := updater.Update(ctx, oldRow, newRow); err != nil {
		return err
	}
	return e.flushRow(ctx)
}

func (e *beforeInsertVisibleUpdater) Close(ctx *sql.Context) error {
	return e.close(ctx)
}

type beforeInsertVisibleReplacer struct {
	beforeInsertVisibleEditor
	replaceable sql.ReplaceableTable
}

var _ sql.RowReplacer = (*beforeInsertVisibleReplacer)(nil)

func (e *beforeInsertVisibleReplacer) StatementBegin(ctx *sql.Context) {
	e.statementBegin(ctx)
}

func (e *beforeInsertVisibleReplacer) DiscardChanges(ctx *sql.Context, err error) error {
	return e.discardChanges(ctx, err)
}

func (e *beforeInsertVisibleReplacer) StatementComplete(ctx *sql.Context) error {
	return e.statementComplete(ctx)
}

func (e *beforeInsertVisibleReplacer) Insert(ctx *sql.Context, row sql.Row) error {
	if e.primary == nil {
		e.primary = e.replaceable.Replacer(ctx)
		if e.statement {
			e.primary.StatementBegin(ctx)
		}
	}
	replacer, ok := e.primary.(sql.RowReplacer)
	if !ok {
		return planErr("primary table editor does not support replacements")
	}
	if err := replacer.Insert(ctx, row); err != nil {
		return err
	}
	return e.flushRow(ctx)
}

func (e *beforeInsertVisibleReplacer) Delete(ctx *sql.Context, row sql.Row) error {
	if e.primary == nil {
		e.primary = e.replaceable.Replacer(ctx)
		if e.statement {
			e.primary.StatementBegin(ctx)
		}
	}
	replacer, ok := e.primary.(sql.RowReplacer)
	if !ok {
		return planErr("primary table editor does not support replacements")
	}
	return replacer.Delete(ctx, row)
}

func (e *beforeInsertVisibleReplacer) Close(ctx *sql.Context) error {
	return e.close(ctx)
}
