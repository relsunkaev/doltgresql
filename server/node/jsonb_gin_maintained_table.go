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
	"sort"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/go-mysql-server/sql"
	gmsexpression "github.com/dolthub/go-mysql-server/sql/expression"

	"github.com/dolthub/doltgresql/core"
	pgexpression "github.com/dolthub/doltgresql/server/expression"
	"github.com/dolthub/doltgresql/server/functions/framework"
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
var _ sql.IndexSearchableTable = (*JsonbGinMaintainedTable)(nil)

type jsonbGinLookupMode string

const (
	jsonbGinLookupIntersect jsonbGinLookupMode = "intersect"
	jsonbGinLookupUnion     jsonbGinLookupMode = "union"
)

type jsonbGinLookupSpec struct {
	index  JsonbGinMaintainedIndex
	tokens []jsonbgin.Token
	mode   jsonbGinLookupMode
	debug  string
}

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

type JsonbGinSearchableTable struct {
	maintained *JsonbGinMaintainedTable
}

var _ sql.Table = (*JsonbGinSearchableTable)(nil)
var _ sql.DatabaseSchemaTable = (*JsonbGinSearchableTable)(nil)
var _ sql.IndexAddressableTable = (*JsonbGinSearchableTable)(nil)
var _ sql.IndexedTable = (*JsonbGinSearchableTable)(nil)
var _ sql.IndexSearchableTable = (*JsonbGinSearchableTable)(nil)

func WrapJsonbGinSearchableTable(ctx *sql.Context, schemaName string, table sql.Table) (sql.Table, bool, error) {
	if _, ok := table.(*JsonbGinSearchableTable); ok {
		return table, false, nil
	}
	if _, ok := table.(*JsonbGinMaintainedTable); ok {
		return table, false, nil
	}
	maintainedTable, wrapped, err := WrapJsonbGinMaintainedTable(ctx, schemaName, table)
	if err != nil || !wrapped {
		return table, wrapped, err
	}
	return &JsonbGinSearchableTable{
		maintained: maintainedTable.(*JsonbGinMaintainedTable),
	}, true, nil
}

func (t *JsonbGinSearchableTable) Name() string {
	return t.maintained.Name()
}

func (t *JsonbGinSearchableTable) String() string {
	return t.maintained.String()
}

func (t *JsonbGinSearchableTable) Schema(ctx *sql.Context) sql.Schema {
	return t.maintained.Schema(ctx)
}

func (t *JsonbGinSearchableTable) Collation() sql.CollationID {
	return t.maintained.Collation()
}

func (t *JsonbGinSearchableTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return t.maintained.Partitions(ctx)
}

func (t *JsonbGinSearchableTable) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	return t.maintained.PartitionRows(ctx, partition)
}

func (t *JsonbGinSearchableTable) DatabaseSchema() sql.DatabaseSchema {
	return t.maintained.DatabaseSchema()
}

func (t *JsonbGinSearchableTable) IndexedAccess(ctx *sql.Context, lookup sql.IndexLookup) sql.IndexedTable {
	return t.maintained.IndexedAccess(ctx, lookup)
}

func (t *JsonbGinSearchableTable) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	return t.maintained.GetIndexes(ctx)
}

func (t *JsonbGinSearchableTable) LookupPartitions(ctx *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	return t.maintained.LookupPartitions(ctx, lookup)
}

func (t *JsonbGinSearchableTable) PreciseMatch() bool {
	return t.maintained.PreciseMatch()
}

func (t *JsonbGinSearchableTable) SkipIndexCosting() bool {
	return false
}

func (t *JsonbGinSearchableTable) LookupForExpressions(ctx *sql.Context, exprs ...sql.Expression) (sql.IndexLookup, *sql.FuncDepSet, sql.Expression, bool, error) {
	return t.maintained.LookupForExpressions(ctx, exprs...)
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
	if index, ok := lookup.Index.(*jsonbGinLookupIndex); ok {
		return &jsonbGinIndexedTable{
			JsonbGinMaintainedTable: t,
			lookup:                  index.lookup,
		}
	}
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

func (t *JsonbGinMaintainedTable) SkipIndexCosting() bool {
	return false
}

func (t *JsonbGinMaintainedTable) LookupForExpressions(ctx *sql.Context, exprs ...sql.Expression) (sql.IndexLookup, *sql.FuncDepSet, sql.Expression, bool, error) {
	for _, expr := range exprs {
		lookupSpec, ok, err := t.lookupSpecForExpression(ctx, expr)
		if err != nil || !ok {
			if err != nil {
				return sql.IndexLookup{}, nil, nil, false, err
			}
			continue
		}
		lookupIndex := &jsonbGinLookupIndex{
			tableName: t.Name(),
			lookup:    lookupSpec,
		}
		debugRange := sql.MySQLRangeCollection{{
			sql.ClosedRangeColumnExpr(lookupSpec.debug, lookupSpec.debug, pgtypes.Text),
		}}
		lookup := sql.NewIndexLookup(lookupIndex, debugRange, false, false, false, false)
		return lookup, nil, gmsexpression.JoinAnd(exprs...), true, nil
	}
	return sql.IndexLookup{}, nil, nil, false, nil
}

func (t *JsonbGinMaintainedTable) lookupSpecForExpression(ctx *sql.Context, expr sql.Expression) (jsonbGinLookupSpec, bool, error) {
	binary, ok := expr.(*pgexpression.BinaryOperator)
	if !ok {
		return jsonbGinLookupSpec{}, false, nil
	}
	field, ok := binary.Left().(*gmsexpression.GetField)
	if !ok {
		return jsonbGinLookupSpec{}, false, nil
	}
	for _, index := range t.indexes {
		if !strings.EqualFold(field.Name(), index.ColumnName) {
			continue
		}
		tokens, mode, ok, err := jsonbGinLookupTokens(ctx, index.OpClass, binary.Operator(), binary.Right())
		if err != nil || !ok {
			return jsonbGinLookupSpec{}, ok, err
		}
		return jsonbGinLookupSpec{
			index:  index,
			tokens: tokens,
			mode:   mode,
			debug:  fmt.Sprintf("%s %s %d token(s)", index.Name, mode, len(tokens)),
		}, true, nil
	}
	return jsonbGinLookupSpec{}, false, nil
}

func jsonbGinLookupTokens(ctx *sql.Context, opClass string, operator framework.Operator, right sql.Expression) ([]jsonbgin.Token, jsonbGinLookupMode, bool, error) {
	value, err := right.Eval(ctx, nil)
	if err != nil {
		return nil, "", false, nil
	}
	opClass = indexmetadata.NormalizeOpClass(opClass)
	switch operator {
	case framework.Operator_BinaryJSONContainsRight:
		doc, err := pgtypes.JsonDocumentFromSQLValue(ctx, pgtypes.JsonB, value)
		if err != nil {
			return nil, "", false, err
		}
		tokens, err := jsonbgin.Extract(doc, opClass)
		if err != nil || len(tokens) == 0 {
			return nil, "", false, err
		}
		return tokens, jsonbGinLookupIntersect, true, nil
	case framework.Operator_BinaryJSONTopLevel:
		if opClass != indexmetadata.OpClassJsonbOps {
			return nil, "", false, nil
		}
		key, ok := value.(string)
		if !ok {
			return nil, "", false, nil
		}
		return []jsonbgin.Token{{OpClass: opClass, Kind: jsonbgin.TokenKindKey, Value: key}}, jsonbGinLookupIntersect, true, nil
	case framework.Operator_BinaryJSONTopLevelAny, framework.Operator_BinaryJSONTopLevelAll:
		if opClass != indexmetadata.OpClassJsonbOps {
			return nil, "", false, nil
		}
		keys, ok := textArrayStrings(value)
		if !ok || len(keys) == 0 {
			return nil, "", false, nil
		}
		tokens := make([]jsonbgin.Token, len(keys))
		for i, key := range keys {
			tokens[i] = jsonbgin.Token{OpClass: opClass, Kind: jsonbgin.TokenKindKey, Value: key}
		}
		if operator == framework.Operator_BinaryJSONTopLevelAny {
			return tokens, jsonbGinLookupUnion, true, nil
		}
		return tokens, jsonbGinLookupIntersect, true, nil
	default:
		return nil, "", false, nil
	}
}

func textArrayStrings(value any) ([]string, bool) {
	switch value := value.(type) {
	case []string:
		return value, true
	case []any:
		keys := make([]string, len(value))
		for i, item := range value {
			key, ok := item.(string)
			if !ok {
				return nil, false
			}
			keys[i] = key
		}
		return keys, true
	default:
		return nil, false
	}
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

type jsonbGinLookupIndex struct {
	tableName string
	lookup    jsonbGinLookupSpec
}

var _ sql.Index = (*jsonbGinLookupIndex)(nil)

func (i *jsonbGinLookupIndex) ID() string {
	return i.lookup.index.Name
}

func (i *jsonbGinLookupIndex) Database() string {
	return ""
}

func (i *jsonbGinLookupIndex) Table() string {
	return i.tableName
}

func (i *jsonbGinLookupIndex) Expressions() []string {
	return []string{fmt.Sprintf("jsonb_gin(%s)", i.lookup.index.ColumnName)}
}

func (i *jsonbGinLookupIndex) IsUnique() bool {
	return false
}

func (i *jsonbGinLookupIndex) IsSpatial() bool {
	return false
}

func (i *jsonbGinLookupIndex) IsFullText() bool {
	return false
}

func (i *jsonbGinLookupIndex) IsVector() bool {
	return false
}

func (i *jsonbGinLookupIndex) Comment() string {
	return ""
}

func (i *jsonbGinLookupIndex) IndexType() string {
	return "GIN"
}

func (i *jsonbGinLookupIndex) IsGenerated() bool {
	return true
}

func (i *jsonbGinLookupIndex) ColumnExpressionTypes(ctx *sql.Context) []sql.ColumnExpressionType {
	return []sql.ColumnExpressionType{{
		Type:       pgtypes.Text,
		Expression: i.Expressions()[0],
	}}
}

func (i *jsonbGinLookupIndex) CanSupport(ctx *sql.Context, ranges ...sql.Range) bool {
	return true
}

func (i *jsonbGinLookupIndex) CanSupportOrderBy(expr sql.Expression) bool {
	return false
}

func (i *jsonbGinLookupIndex) PrefixLengths() []uint16 {
	return nil
}

type jsonbGinIndexedTable struct {
	*JsonbGinMaintainedTable
	lookup jsonbGinLookupSpec
}

var _ sql.IndexedTable = (*jsonbGinIndexedTable)(nil)

func (t *jsonbGinIndexedTable) WithUnderlying(table sql.Table) sql.Table {
	maintained := t.JsonbGinMaintainedTable.WithUnderlying(table).(*JsonbGinMaintainedTable)
	return &jsonbGinIndexedTable{
		JsonbGinMaintainedTable: maintained,
		lookup:                  t.lookup,
	}
}

func (t *jsonbGinIndexedTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return t.LookupPartitions(ctx, sql.IndexLookup{})
}

func (t *jsonbGinIndexedTable) LookupPartitions(ctx *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	rowIDs, err := t.lookupPostingRowIDs(ctx)
	if err != nil {
		return nil, err
	}
	return sql.PartitionsToPartitionIter(jsonbGinLookupPartition{rowIDs: rowIDs}), nil
}

func (t *jsonbGinIndexedTable) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	ginPartition, ok := partition.(jsonbGinLookupPartition)
	if !ok {
		return nil, errors.Errorf("unexpected JSONB GIN lookup partition %T", partition)
	}
	if len(ginPartition.rowIDs) == 0 {
		return sql.RowsToRowIter(), nil
	}
	partitions, err := t.underlying.Partitions(ctx)
	if err != nil {
		return nil, err
	}
	return &jsonbGinCandidateRowIter{
		table:       t.underlying,
		tableParts:  partitions,
		tableSchema: t.underlying.Schema(ctx),
		rowIDs:      ginPartition.rowIDs,
	}, nil
}

func (t *jsonbGinIndexedTable) lookupPostingRowIDs(ctx *sql.Context) (map[string]struct{}, error) {
	postingTable, err := core.GetSqlTableFromContext(ctx, "", doltdb.TableName{Name: t.lookup.index.PostingTable, Schema: t.schemaName})
	if err != nil {
		return nil, err
	}
	if postingTable == nil {
		return nil, errors.Errorf(`posting table "%s" for gin index "%s" does not exist`, t.lookup.index.PostingTable, t.lookup.index.Name)
	}

	tokenRows := make([]map[string]struct{}, len(t.lookup.tokens))
	for i, token := range t.lookup.tokens {
		tokenRows[i], err = lookupPostingTokenRowIDs(ctx, postingTable, jsonbgin.EncodeToken(token))
		if err != nil {
			return nil, err
		}
	}

	switch t.lookup.mode {
	case jsonbGinLookupUnion:
		return unionPostingRowIDs(tokenRows), nil
	case jsonbGinLookupIntersect:
		return intersectPostingRowIDs(tokenRows), nil
	default:
		return nil, errors.Errorf("unknown JSONB GIN lookup mode %q", t.lookup.mode)
	}
}

func lookupPostingTokenRowIDs(ctx *sql.Context, postingTable sql.Table, encodedToken string) (map[string]struct{}, error) {
	if indexAddressable, ok := postingTable.(sql.IndexAddressable); ok {
		rowIDs, indexed, err := lookupPostingTokenRowIDsFromIndex(ctx, indexAddressable, encodedToken)
		if err != nil || indexed {
			return rowIDs, err
		}
	}
	return scanPostingTokenRowIDs(ctx, postingTable, encodedToken)
}

func lookupPostingTokenRowIDsFromIndex(ctx *sql.Context, indexAddressable sql.IndexAddressable, encodedToken string) (map[string]struct{}, bool, error) {
	indexes, err := indexAddressable.GetIndexes(ctx)
	if err != nil {
		return nil, false, err
	}
	postingIndex := jsonbGinPostingTokenIndex(ctx, indexes)
	if postingIndex == nil {
		return nil, false, nil
	}
	columnTypes := postingIndex.ColumnExpressionTypes(ctx)
	if len(columnTypes) == 0 {
		return nil, false, nil
	}
	lookup := sql.NewIndexLookup(postingIndex, sql.MySQLRangeCollection{{
		sql.ClosedRangeColumnExpr(encodedToken, encodedToken, columnTypes[0].Type),
	}}, false, false, false, false)
	indexedTable := indexAddressable.IndexedAccess(ctx, lookup)
	if indexedTable == nil {
		return nil, false, nil
	}
	rowIDs, err := readPostingIndexedRows(ctx, indexedTable, lookup, encodedToken)
	return rowIDs, true, err
}

func jsonbGinPostingTokenIndex(ctx *sql.Context, indexes []sql.Index) sql.Index {
	for _, index := range indexes {
		expressions := index.Expressions()
		if len(expressions) == 0 || !strings.EqualFold(indexExpressionColumnName(expressions[0]), "token") {
			continue
		}
		if len(index.ColumnExpressionTypes(ctx)) == 0 {
			continue
		}
		return index
	}
	return nil
}

func indexExpressionColumnName(expression string) string {
	if lastDot := strings.LastIndex(expression, "."); lastDot >= 0 {
		expression = expression[lastDot+1:]
	}
	return strings.Trim(expression, "`\"")
}

func readPostingIndexedRows(ctx *sql.Context, indexedTable sql.IndexedTable, lookup sql.IndexLookup, encodedToken string) (map[string]struct{}, error) {
	rowIDs := make(map[string]struct{})
	partitions, err := indexedTable.LookupPartitions(ctx, lookup)
	if err != nil {
		return nil, err
	}
	defer partitions.Close(ctx)
	for {
		partition, err := partitions.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		rows, err := indexedTable.PartitionRows(ctx, partition)
		if err != nil {
			return nil, err
		}
		if err = readPostingTokenRows(ctx, rows, encodedToken, rowIDs); err != nil {
			_ = rows.Close(ctx)
			return nil, err
		}
		if err = rows.Close(ctx); err != nil {
			return nil, err
		}
	}
	return rowIDs, nil
}

func scanPostingTokenRowIDs(ctx *sql.Context, postingTable sql.Table, encodedToken string) (map[string]struct{}, error) {
	rowIDs := make(map[string]struct{})
	partitions, err := postingTable.Partitions(ctx)
	if err != nil {
		return nil, err
	}
	defer partitions.Close(ctx)
	for {
		partition, err := partitions.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		rows, err := postingTable.PartitionRows(ctx, partition)
		if err != nil {
			return nil, err
		}
		if err = readPostingTokenRows(ctx, rows, encodedToken, rowIDs); err != nil {
			_ = rows.Close(ctx)
			return nil, err
		}
		if err = rows.Close(ctx); err != nil {
			return nil, err
		}
	}
	return rowIDs, nil
}

func readPostingTokenRows(ctx *sql.Context, rows sql.RowIter, encodedToken string, rowIDs map[string]struct{}) error {
	for {
		row, err := rows.Next(ctx)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if len(row) < 2 || row[0] == nil || row[1] == nil {
			continue
		}
		tokenText, ok := row[0].(string)
		if !ok {
			return errors.Errorf("unexpected JSONB GIN posting token type %T", row[0])
		}
		if tokenText != encodedToken {
			continue
		}
		rowID, ok := row[1].(string)
		if !ok {
			return errors.Errorf("unexpected JSONB GIN posting row identity type %T", row[1])
		}
		rowIDs[rowID] = struct{}{}
	}
}

func unionPostingRowIDs(tokenRows []map[string]struct{}) map[string]struct{} {
	rowIDs := make(map[string]struct{})
	for _, rows := range tokenRows {
		for rowID := range rows {
			rowIDs[rowID] = struct{}{}
		}
	}
	return rowIDs
}

func intersectPostingRowIDs(tokenRows []map[string]struct{}) map[string]struct{} {
	if len(tokenRows) == 0 {
		return nil
	}
	sort.Slice(tokenRows, func(i, j int) bool {
		return len(tokenRows[i]) < len(tokenRows[j])
	})
	if len(tokenRows[0]) == 0 {
		return nil
	}
	rowIDs := make(map[string]struct{})
	for rowID := range tokenRows[0] {
		found := true
		for _, rows := range tokenRows[1:] {
			if _, ok := rows[rowID]; !ok {
				found = false
				break
			}
		}
		if found {
			rowIDs[rowID] = struct{}{}
		}
	}
	return rowIDs
}

type jsonbGinLookupPartition struct {
	rowIDs map[string]struct{}
}

func (p jsonbGinLookupPartition) Key() []byte {
	return []byte("jsonb-gin-lookup")
}

type jsonbGinCandidateRowIter struct {
	table       sql.Table
	tableParts  sql.PartitionIter
	currentRows sql.RowIter
	tableSchema sql.Schema
	rowIDs      map[string]struct{}
}

var _ sql.RowIter = (*jsonbGinCandidateRowIter)(nil)

func (i *jsonbGinCandidateRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	for {
		if i.currentRows == nil {
			partition, err := i.tableParts.Next(ctx)
			if err == io.EOF {
				_ = i.Close(ctx)
				return nil, io.EOF
			}
			if err != nil {
				return nil, err
			}
			i.currentRows, err = i.table.PartitionRows(ctx, partition)
			if err != nil {
				return nil, err
			}
		}
		row, err := i.currentRows.Next(ctx)
		if err == io.EOF {
			if closeErr := i.currentRows.Close(ctx); closeErr != nil {
				return nil, closeErr
			}
			i.currentRows = nil
			continue
		}
		if err != nil {
			return nil, err
		}
		rowID, ok := primaryKeyRowIdentity(i.tableSchema, row)
		if !ok {
			// Aggregate plans such as count(*) may prune the primary key while keeping
			// the JSONB column needed by the retained recheck filter.
			return row, nil
		}
		if _, ok := i.rowIDs[rowID]; ok {
			return row, nil
		}
	}
}

func (i *jsonbGinCandidateRowIter) Close(ctx *sql.Context) error {
	var ret error
	if i.currentRows != nil {
		ret = i.currentRows.Close(ctx)
		i.currentRows = nil
	}
	if i.tableParts != nil {
		if err := i.tableParts.Close(ctx); ret == nil {
			ret = err
		}
		i.tableParts = nil
	}
	return ret
}
