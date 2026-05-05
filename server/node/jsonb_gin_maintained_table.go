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
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/server/indexmetadata"
	"github.com/dolthub/doltgresql/server/jsonbgin"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

type JsonbGinMaintainedIndex struct {
	Name         string
	ColumnName   string
	ColumnIndex  int
	OpClass      string
	PostingTable string
}

type JsonbGinMaintainedTable struct {
	underlying sql.Table
	schemaName string
	indexes    []JsonbGinMaintainedIndex
}

var _ sql.TableWrapper = (*JsonbGinMaintainedTable)(nil)
var _ sql.MutableTableWrapper = (*JsonbGinMaintainedTable)(nil)
var _ sql.InsertableTable = (*JsonbGinMaintainedTable)(nil)
var _ sql.ReplaceableTable = (*JsonbGinMaintainedTable)(nil)
var _ sql.UpdatableTable = (*JsonbGinMaintainedTable)(nil)
var _ sql.DeletableTable = (*JsonbGinMaintainedTable)(nil)
var _ sql.IndexAddressable = (*JsonbGinMaintainedTable)(nil)
var _ sql.IndexedTable = (*JsonbGinMaintainedTable)(nil)

func WrapJsonbGinMaintainedTable(ctx *sql.Context, schemaName string, table sql.Table) (sql.Table, bool, error) {
	if _, ok := table.(*JsonbGinMaintainedTable); ok {
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
	ginIndexes := make([]JsonbGinMaintainedIndex, 0)
	for _, index := range indexes {
		metadata, ok := indexmetadata.DecodeComment(index.Comment())
		if !ok || metadata.AccessMethod != indexmetadata.AccessMethodGin || metadata.Gin == nil || metadata.Gin.PostingTable == "" {
			continue
		}
		columns := metadata.Columns
		if len(columns) == 0 {
			columns = columnsFromIndexExpressions(index.Expressions())
		}
		if len(columns) != 1 {
			continue
		}
		columnIndex := tableSchema.IndexOfColName(columns[0])
		if columnIndex < 0 {
			return nil, false, errors.Errorf(`column "%s" for gin index "%s" does not exist`, columns[0], index.ID())
		}
		opClass := indexmetadata.OpClassJsonbOps
		if len(metadata.OpClasses) > 0 && metadata.OpClasses[0] != "" {
			opClass = metadata.OpClasses[0]
		}
		ginIndexes = append(ginIndexes, JsonbGinMaintainedIndex{
			Name:         index.ID(),
			ColumnName:   columns[0],
			ColumnIndex:  columnIndex,
			OpClass:      opClass,
			PostingTable: metadata.Gin.PostingTable,
		})
	}
	if len(ginIndexes) == 0 {
		return table, false, nil
	}
	return &JsonbGinMaintainedTable{
		underlying: table,
		schemaName: schemaName,
		indexes:    ginIndexes,
	}, true, nil
}

func columnsFromIndexExpressions(expressions []string) []string {
	columns := make([]string, len(expressions))
	for i, expr := range expressions {
		lastDot := strings.LastIndex(expr, ".")
		columns[i] = expr[lastDot+1:]
	}
	return columns
}

func (t *JsonbGinMaintainedTable) Underlying() sql.Table {
	return t.underlying
}

func (t *JsonbGinMaintainedTable) WithUnderlying(table sql.Table) sql.Table {
	return &JsonbGinMaintainedTable{
		underlying: table,
		schemaName: t.schemaName,
		indexes:    t.indexes,
	}
}

func (t *JsonbGinMaintainedTable) Name() string {
	return t.underlying.Name()
}

func (t *JsonbGinMaintainedTable) String() string {
	return t.underlying.String()
}

func (t *JsonbGinMaintainedTable) Schema(ctx *sql.Context) sql.Schema {
	return t.underlying.Schema(ctx)
}

func (t *JsonbGinMaintainedTable) Collation() sql.CollationID {
	return t.underlying.Collation()
}

func (t *JsonbGinMaintainedTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return t.underlying.Partitions(ctx)
}

func (t *JsonbGinMaintainedTable) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	return t.underlying.PartitionRows(ctx, partition)
}

func (t *JsonbGinMaintainedTable) DatabaseSchema() sql.DatabaseSchema {
	if schemaTable, ok := t.underlying.(sql.DatabaseSchemaTable); ok {
		return schemaTable.DatabaseSchema()
	}
	return nil
}

func (t *JsonbGinMaintainedTable) IndexedAccess(ctx *sql.Context, lookup sql.IndexLookup) sql.IndexedTable {
	if indexAddressable, ok := t.underlying.(sql.IndexAddressable); ok {
		return indexAddressable.IndexedAccess(ctx, lookup)
	}
	return nil
}

func (t *JsonbGinMaintainedTable) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	if indexAddressable, ok := t.underlying.(sql.IndexAddressable); ok {
		return indexAddressable.GetIndexes(ctx)
	}
	return nil, nil
}

func (t *JsonbGinMaintainedTable) LookupPartitions(ctx *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	if indexedTable, ok := t.underlying.(sql.IndexedTable); ok {
		return indexedTable.LookupPartitions(ctx, lookup)
	}
	return nil, errors.Errorf("table %s is not indexed", t.Name())
}

func (t *JsonbGinMaintainedTable) PreciseMatch() bool {
	if indexAddressable, ok := t.underlying.(sql.IndexAddressable); ok {
		return indexAddressable.PreciseMatch()
	}
	return false
}

func (t *JsonbGinMaintainedTable) Inserter(ctx *sql.Context) sql.RowInserter {
	insertable, ok := t.underlying.(sql.InsertableTable)
	if !ok {
		return sqlutil.NewStaticErrorEditor(planErr("table %s is not insertable", t.Name()))
	}
	editor, err := t.newEditor(ctx, insertable.Inserter(ctx))
	if err != nil {
		return sqlutil.NewStaticErrorEditor(err)
	}
	return editor
}

func (t *JsonbGinMaintainedTable) Replacer(ctx *sql.Context) sql.RowReplacer {
	replaceable, ok := t.underlying.(sql.ReplaceableTable)
	if !ok {
		return sqlutil.NewStaticErrorEditor(planErr("table %s is not replaceable", t.Name()))
	}
	editor, err := t.newEditor(ctx, replaceable.Replacer(ctx))
	if err != nil {
		return sqlutil.NewStaticErrorEditor(err)
	}
	return editor
}

func (t *JsonbGinMaintainedTable) Updater(ctx *sql.Context) sql.RowUpdater {
	updatable, ok := t.underlying.(sql.UpdatableTable)
	if !ok {
		return sqlutil.NewStaticErrorEditor(planErr("table %s is not updatable", t.Name()))
	}
	editor, err := t.newEditor(ctx, updatable.Updater(ctx))
	if err != nil {
		return sqlutil.NewStaticErrorEditor(err)
	}
	return editor
}

func (t *JsonbGinMaintainedTable) Deleter(ctx *sql.Context) sql.RowDeleter {
	deletable, ok := t.underlying.(sql.DeletableTable)
	if !ok {
		return sqlutil.NewStaticErrorEditor(planErr("table %s is not deletable", t.Name()))
	}
	editor, err := t.newEditor(ctx, deletable.Deleter(ctx))
	if err != nil {
		return sqlutil.NewStaticErrorEditor(err)
	}
	return editor
}

func planErr(format string, args ...any) error {
	return errors.Errorf(format, args...)
}

func (t *JsonbGinMaintainedTable) newEditor(ctx *sql.Context, primary jsonbGinPrimaryEditor) (*jsonbGinMaintainingEditor, error) {
	postingEditors := make([]jsonbGinPostingEditor, len(t.indexes))
	for i, ginIndex := range t.indexes {
		postingTable, err := core.GetSqlTableFromContext(ctx, "", doltdb.TableName{Name: ginIndex.PostingTable, Schema: t.schemaName})
		if err != nil {
			return nil, err
		}
		if postingTable == nil {
			return nil, errors.Errorf(`posting table "%s" for gin index "%s" does not exist`, ginIndex.PostingTable, ginIndex.Name)
		}
		replaceable, ok := postingTable.(sql.ReplaceableTable)
		if !ok {
			return nil, errors.Errorf(`posting table "%s" for gin index "%s" is not editable`, ginIndex.PostingTable, ginIndex.Name)
		}
		postingEditors[i] = jsonbGinPostingEditor{
			index:  ginIndex,
			editor: replaceable.Replacer(ctx),
		}
	}
	return &jsonbGinMaintainingEditor{
		tableSchema: t.Schema(ctx),
		primary:     primary,
		postings:    postingEditors,
	}, nil
}

type jsonbGinPrimaryEditor interface {
	sql.EditOpenerCloser
	Close(*sql.Context) error
}

type jsonbGinPostingEditor struct {
	index  JsonbGinMaintainedIndex
	editor sql.RowReplacer
}

type jsonbGinMaintainingEditor struct {
	tableSchema sql.Schema
	primary     jsonbGinPrimaryEditor
	postings    []jsonbGinPostingEditor
}

var _ sql.TableEditor = (*jsonbGinMaintainingEditor)(nil)

func (e *jsonbGinMaintainingEditor) StatementBegin(ctx *sql.Context) {
	for _, posting := range e.postings {
		posting.editor.StatementBegin(ctx)
	}
	e.primary.StatementBegin(ctx)
}

func (e *jsonbGinMaintainingEditor) DiscardChanges(ctx *sql.Context, err error) error {
	var ret error
	for _, posting := range e.postings {
		if nextErr := posting.editor.DiscardChanges(ctx, err); ret == nil {
			ret = nextErr
		}
	}
	if nextErr := e.primary.DiscardChanges(ctx, err); ret == nil {
		ret = nextErr
	}
	return ret
}

func (e *jsonbGinMaintainingEditor) StatementComplete(ctx *sql.Context) error {
	var ret error
	for _, posting := range e.postings {
		if nextErr := posting.editor.StatementComplete(ctx); ret == nil {
			ret = nextErr
		}
	}
	if nextErr := e.primary.StatementComplete(ctx); ret == nil {
		ret = nextErr
	}
	return ret
}

func (e *jsonbGinMaintainingEditor) Insert(ctx *sql.Context, row sql.Row) error {
	if err := e.insertPostings(ctx, row); err != nil {
		return err
	}
	inserter, ok := e.primary.(sql.RowInserter)
	if !ok {
		return errors.Errorf("primary table editor does not support inserts")
	}
	return inserter.Insert(ctx, row)
}

func (e *jsonbGinMaintainingEditor) Delete(ctx *sql.Context, row sql.Row) error {
	if err := e.deletePostings(ctx, row); err != nil {
		return err
	}
	deleter, ok := e.primary.(sql.RowDeleter)
	if !ok {
		return errors.Errorf("primary table editor does not support deletes")
	}
	return deleter.Delete(ctx, row)
}

func (e *jsonbGinMaintainingEditor) Update(ctx *sql.Context, oldRow sql.Row, newRow sql.Row) error {
	if err := e.deletePostings(ctx, oldRow); err != nil {
		return err
	}
	if err := e.insertPostings(ctx, newRow); err != nil {
		return err
	}
	updater, ok := e.primary.(sql.RowUpdater)
	if !ok {
		return errors.Errorf("primary table editor does not support updates")
	}
	return updater.Update(ctx, oldRow, newRow)
}

func (e *jsonbGinMaintainingEditor) Close(ctx *sql.Context) error {
	var ret error
	for _, posting := range e.postings {
		if nextErr := posting.editor.Close(ctx); ret == nil {
			ret = nextErr
		}
	}
	if nextErr := e.primary.Close(ctx); ret == nil {
		ret = nextErr
	}
	return ret
}

func (e *jsonbGinMaintainingEditor) insertPostings(ctx *sql.Context, row sql.Row) error {
	for _, posting := range e.postings {
		postingRows, err := e.postingRows(ctx, posting.index, row)
		if err != nil {
			return err
		}
		for _, postingRow := range postingRows {
			if err = posting.editor.Insert(ctx, postingRow); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *jsonbGinMaintainingEditor) deletePostings(ctx *sql.Context, row sql.Row) error {
	for _, posting := range e.postings {
		postingRows, err := e.postingRows(ctx, posting.index, row)
		if err != nil {
			return err
		}
		for _, postingRow := range postingRows {
			if err = posting.editor.Delete(ctx, postingRow); err != nil && !sql.ErrDeleteRowNotFound.Is(err) {
				return err
			}
		}
	}
	return nil
}

func (e *jsonbGinMaintainingEditor) postingRows(ctx *sql.Context, index JsonbGinMaintainedIndex, row sql.Row) ([]sql.Row, error) {
	if index.ColumnIndex >= len(row) || row[index.ColumnIndex] == nil {
		return nil, nil
	}
	doc, err := pgtypes.JsonDocumentFromSQLValue(ctx, pgtypes.JsonB, row[index.ColumnIndex])
	if err != nil {
		return nil, err
	}
	tokens, err := jsonbgin.Extract(doc, index.OpClass)
	if err != nil {
		return nil, err
	}
	rowID := rowIdentity(e.tableSchema, row)
	postingRows := make([]sql.Row, len(tokens))
	for i, token := range tokens {
		postingRows[i] = sql.Row{jsonbgin.EncodeToken(token), rowID}
	}
	return postingRows, nil
}

func (e *jsonbGinMaintainingEditor) String() string {
	return fmt.Sprintf("jsonbGinMaintainingEditor(%d)", len(e.postings))
}
