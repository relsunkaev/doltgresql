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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"

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
	index               JsonbGinMaintainedIndex
	encodedTokens       []string
	mode                jsonbGinLookupMode
	broadTokenSensitive bool
	debug               string
}

type jsonbGinPostingCandidate struct {
	rowID string
	key   sql.Row
}

const (
	jsonbGinMaxBroadKeyPostingRowsForIndexedLookup = 128
	jsonbGinMaxCandidateRowsForIndexedLookup       = 512
)

var jsonbGinLiteralTokenCache sync.Map

type jsonbGinCachedLookupTokens struct {
	encodedTokens []string
	mode          jsonbGinLookupMode
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
		if tooBroad, err := t.lookupSpecTooBroadForIndex(ctx, lookupSpec); err != nil || tooBroad {
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
		encodedTokens, mode, ok, err := jsonbGinLookupTokens(ctx, index.OpClass, binary.Operator(), binary.Right())
		if err != nil || !ok {
			return jsonbGinLookupSpec{}, ok, err
		}
		return jsonbGinLookupSpec{
			index:               index,
			encodedTokens:       encodedTokens,
			mode:                mode,
			broadTokenSensitive: jsonbGinBroadTokenSensitiveOperator(binary.Operator()),
			debug:               fmt.Sprintf("%s %s %d token(s)", index.Name, mode, len(encodedTokens)),
		}, true, nil
	}
	return jsonbGinLookupSpec{}, false, nil
}

func (t *JsonbGinMaintainedTable) lookupSpecTooBroadForIndex(ctx *sql.Context, lookupSpec jsonbGinLookupSpec) (bool, error) {
	postingTable, err := core.GetSqlTableFromContext(ctx, "", doltdb.TableName{Name: lookupSpec.index.PostingTable, Schema: t.schemaName})
	if err != nil {
		return false, err
	}
	if postingTable == nil {
		return false, errors.Errorf(`posting table "%s" for gin index "%s" does not exist`, lookupSpec.index.PostingTable, lookupSpec.index.Name)
	}
	tokenRows := make([]map[string]struct{}, len(lookupSpec.encodedTokens))
	for i, encodedToken := range lookupSpec.encodedTokens {
		if lookupSpec.broadTokenSensitive {
			exceeds, err := postingTokenRowIDCountExceeds(ctx, postingTable, encodedToken, jsonbGinMaxBroadKeyPostingRowsForIndexedLookup)
			if err != nil || exceeds {
				return exceeds, err
			}
		}
		rowIDs, err := lookupPostingTokenRowIDs(ctx, postingTable, encodedToken)
		if err != nil {
			return false, err
		}
		tokenRows[i] = rowIDs
	}
	var candidateRows map[string]struct{}
	switch lookupSpec.mode {
	case jsonbGinLookupUnion:
		candidateRows = unionPostingRowIDs(tokenRows)
	case jsonbGinLookupIntersect:
		candidateRows = intersectPostingRowIDs(tokenRows)
	default:
		return false, errors.Errorf("unknown JSONB GIN lookup mode %q", lookupSpec.mode)
	}
	if len(candidateRows) > jsonbGinMaxCandidateRowsForIndexedLookup {
		return true, nil
	}
	return false, nil
}

func jsonbGinBroadTokenSensitiveOperator(operator framework.Operator) bool {
	switch operator {
	case framework.Operator_BinaryJSONTopLevel, framework.Operator_BinaryJSONTopLevelAny, framework.Operator_BinaryJSONTopLevelAll:
		return true
	default:
		return false
	}
}

func postingTokenRowIDCountExceeds(ctx *sql.Context, postingTable sql.Table, encodedToken string, limit int) (bool, error) {
	if indexAddressable, ok := postingTable.(sql.IndexAddressable); ok {
		exceeds, indexed, err := postingTokenRowIDIndexCountExceeds(ctx, indexAddressable, encodedToken, limit)
		if err != nil || indexed {
			return exceeds, err
		}
	}
	rowIDs := make(map[string]struct{})
	partitions, err := postingTable.Partitions(ctx)
	if err != nil {
		return false, err
	}
	defer partitions.Close(ctx)
	for {
		partition, err := partitions.Next(ctx)
		if err == io.EOF {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		rows, err := postingTable.PartitionRows(ctx, partition)
		if err != nil {
			return false, err
		}
		exceeds, err := readPostingTokenRowsUntilLimit(ctx, rows, encodedToken, rowIDs, limit)
		closeErr := rows.Close(ctx)
		if err != nil {
			return false, err
		}
		if closeErr != nil {
			return false, closeErr
		}
		if exceeds {
			return true, nil
		}
	}
}

func postingTokenRowIDIndexCountExceeds(ctx *sql.Context, indexAddressable sql.IndexAddressable, encodedToken string, limit int) (bool, bool, error) {
	indexes, err := indexAddressable.GetIndexes(ctx)
	if err != nil {
		return false, false, err
	}
	postingIndex := jsonbGinPostingTokenIndex(ctx, indexes)
	if postingIndex == nil {
		return false, false, nil
	}
	columnTypes := postingIndex.ColumnExpressionTypes(ctx)
	if len(columnTypes) == 0 {
		return false, false, nil
	}
	lookup := sql.NewIndexLookup(postingIndex, sql.MySQLRangeCollection{{
		sql.ClosedRangeColumnExpr(encodedToken, encodedToken, columnTypes[0].Type),
	}}, false, false, false, false)
	indexedTable := indexAddressable.IndexedAccess(ctx, lookup)
	if indexedTable == nil {
		return false, false, nil
	}
	exceeds, err := readPostingIndexedRowsUntilLimit(ctx, indexedTable, lookup, encodedToken, limit)
	return exceeds, true, err
}

func jsonbGinLookupTokens(ctx *sql.Context, opClass string, operator framework.Operator, right sql.Expression) ([]string, jsonbGinLookupMode, bool, error) {
	value, err := right.Eval(ctx, nil)
	if err != nil {
		return nil, "", false, nil
	}
	if key, ok := jsonbGinLookupTokenCacheKey(opClass, operator, value); ok {
		if cached, ok := jsonbGinLiteralTokenCache.Load(key); ok {
			tokens := cached.(jsonbGinCachedLookupTokens)
			return cloneStrings(tokens.encodedTokens), tokens.mode, true, nil
		}
		tokens, mode, ok, err := jsonbGinLookupTokensFromValue(ctx, opClass, operator, value)
		if err != nil || !ok {
			return tokens, mode, ok, err
		}
		jsonbGinLiteralTokenCache.Store(key, jsonbGinCachedLookupTokens{
			encodedTokens: cloneStrings(tokens),
			mode:          mode,
		})
		return tokens, mode, true, nil
	}
	return jsonbGinLookupTokensFromValue(ctx, opClass, operator, value)
}

func jsonbGinLookupTokensFromValue(ctx *sql.Context, opClass string, operator framework.Operator, value any) ([]string, jsonbGinLookupMode, bool, error) {
	opClass = indexmetadata.NormalizeOpClass(opClass)
	switch operator {
	case framework.Operator_BinaryJSONContainsRight:
		doc, err := pgtypes.JsonDocumentFromSQLValue(ctx, pgtypes.JsonB, value)
		if err != nil {
			return nil, "", false, err
		}
		tokens, err := jsonbgin.ExtractEncoded(doc, opClass)
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
		return []string{jsonbgin.EncodeToken(jsonbgin.Token{OpClass: opClass, Kind: jsonbgin.TokenKindKey, Value: key})}, jsonbGinLookupIntersect, true, nil
	case framework.Operator_BinaryJSONTopLevelAny, framework.Operator_BinaryJSONTopLevelAll:
		if opClass != indexmetadata.OpClassJsonbOps {
			return nil, "", false, nil
		}
		keys, ok := textArrayStrings(value)
		if !ok || len(keys) == 0 {
			return nil, "", false, nil
		}
		tokens := make([]string, len(keys))
		for i, key := range keys {
			tokens[i] = jsonbgin.EncodeToken(jsonbgin.Token{OpClass: opClass, Kind: jsonbgin.TokenKindKey, Value: key})
		}
		if operator == framework.Operator_BinaryJSONTopLevelAny {
			return tokens, jsonbGinLookupUnion, true, nil
		}
		return tokens, jsonbGinLookupIntersect, true, nil
	default:
		return nil, "", false, nil
	}
}

func jsonbGinLookupTokenCacheKey(opClass string, operator framework.Operator, value any) (string, bool) {
	switch value.(type) {
	case string, []string, []any, bool, nil:
	default:
		// JSON/JSONB literals generally arrive as strings in this planner path.
		// Avoid caching arbitrary runtime values unless their representation is
		// explicitly stable here.
		return "", false
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", false
	}
	return fmt.Sprintf("%s\x00%s\x00%s", indexmetadata.NormalizeOpClass(opClass), operator, encoded), true
}

func cloneStrings(tokens []string) []string {
	if len(tokens) == 0 {
		return nil
	}
	cloned := make([]string, len(tokens))
	copy(cloned, tokens)
	return cloned
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
	tableSchema := t.Schema(ctx)
	return &jsonbGinMaintainingEditor{
		tableSchema:        tableSchema,
		primaryKeyOrdinals: primaryKeyOrdinals(tableSchema),
		primary:            primary,
		postings:           postingEditors,
	}, nil
}

type jsonbGinPrimaryEditor interface {
	sql.EditOpenerCloser
	Close(*sql.Context) error
}

type jsonbGinPostingEditor struct {
	index   JsonbGinMaintainedIndex
	editor  sql.RowReplacer
	pending map[string]jsonbGinPendingPosting
}

type jsonbGinPendingPosting struct {
	row    sql.Row
	insert bool
	delete bool
}

type jsonbGinMaintainingEditor struct {
	tableSchema        sql.Schema
	primaryKeyOrdinals []int
	primary            jsonbGinPrimaryEditor
	postings           []jsonbGinPostingEditor
}

var _ sql.TableEditor = (*jsonbGinMaintainingEditor)(nil)

func (e *jsonbGinMaintainingEditor) StatementBegin(ctx *sql.Context) {
	for i := range e.postings {
		e.postings[i].pending = make(map[string]jsonbGinPendingPosting)
		e.postings[i].editor.StatementBegin(ctx)
	}
	e.primary.StatementBegin(ctx)
}

func (e *jsonbGinMaintainingEditor) DiscardChanges(ctx *sql.Context, err error) error {
	var ret error
	for i := range e.postings {
		clear(e.postings[i].pending)
		if nextErr := e.postings[i].editor.DiscardChanges(ctx, err); ret == nil {
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
	for i := range e.postings {
		if nextErr := e.postings[i].flush(ctx); ret == nil {
			ret = nextErr
		}
		if ret != nil {
			continue
		}
		if nextErr := e.postings[i].editor.StatementComplete(ctx); ret == nil {
			ret = nextErr
		}
	}
	if ret != nil {
		return e.discardAfterStatementCompleteError(ctx, ret)
	}
	if nextErr := e.primary.StatementComplete(ctx); ret == nil {
		ret = nextErr
	}
	return ret
}

func (e *jsonbGinMaintainingEditor) discardAfterStatementCompleteError(ctx *sql.Context, err error) error {
	ret := err
	for i := range e.postings {
		clear(e.postings[i].pending)
		if nextErr := e.postings[i].editor.DiscardChanges(ctx, err); ret == nil {
			ret = nextErr
		}
	}
	if nextErr := e.primary.DiscardChanges(ctx, err); ret == nil {
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
	if err := e.updatePostings(ctx, oldRow, newRow); err != nil {
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
	for i := range e.postings {
		if nextErr := e.postings[i].editor.Close(ctx); ret == nil {
			ret = nextErr
		}
	}
	if nextErr := e.primary.Close(ctx); ret == nil {
		ret = nextErr
	}
	return ret
}

func (e *jsonbGinMaintainingEditor) insertPostings(ctx *sql.Context, row sql.Row) error {
	for i := range e.postings {
		postingRows, err := e.postingRows(ctx, e.postings[i].index, row)
		if err != nil {
			return err
		}
		for _, postingRow := range postingRows {
			e.postings[i].stageInsert(postingRow)
		}
	}
	return nil
}

func (e *jsonbGinMaintainingEditor) deletePostings(ctx *sql.Context, row sql.Row) error {
	for i := range e.postings {
		postingRows, err := e.postingRows(ctx, e.postings[i].index, row)
		if err != nil {
			return err
		}
		for _, postingRow := range postingRows {
			e.postings[i].stageDelete(postingRow)
		}
	}
	return nil
}

func (e *jsonbGinMaintainingEditor) updatePostings(ctx *sql.Context, oldRow sql.Row, newRow sql.Row) error {
	for i := range e.postings {
		oldPostingRows, err := e.postingRows(ctx, e.postings[i].index, oldRow)
		if err != nil {
			return err
		}
		newPostingRows, err := e.postingRows(ctx, e.postings[i].index, newRow)
		if err != nil {
			return err
		}
		for _, postingRow := range compactPostingRowsToDelete(oldPostingRows, newPostingRows) {
			e.postings[i].stageDelete(postingRow)
		}
		for _, postingRow := range compactPostingRowsToInsert(oldPostingRows, newPostingRows) {
			e.postings[i].stageInsert(postingRow)
		}
	}
	return nil
}

func (p *jsonbGinPostingEditor) stageInsert(row sql.Row) {
	p.ensurePending()
	key := postingRowKey(row)
	pending := p.pending[key]
	if pending.delete {
		pending.delete = false
	} else {
		pending.insert = true
	}
	pending.row = row
	if !pending.insert && !pending.delete {
		delete(p.pending, key)
		return
	}
	p.pending[key] = pending
}

func (p *jsonbGinPostingEditor) stageDelete(row sql.Row) {
	p.ensurePending()
	key := postingRowKey(row)
	pending := p.pending[key]
	if pending.insert {
		pending.insert = false
	} else {
		pending.delete = true
	}
	pending.row = row
	if !pending.insert && !pending.delete {
		delete(p.pending, key)
		return
	}
	p.pending[key] = pending
}

func (p *jsonbGinPostingEditor) ensurePending() {
	if p.pending == nil {
		p.pending = make(map[string]jsonbGinPendingPosting)
	}
}

func (p *jsonbGinPostingEditor) flush(ctx *sql.Context) error {
	if len(p.pending) == 0 {
		return nil
	}
	for _, pending := range p.pending {
		if !pending.delete {
			continue
		}
		if err := p.editor.Delete(ctx, pending.row); err != nil && !sql.ErrDeleteRowNotFound.Is(err) {
			return err
		}
	}
	for _, pending := range p.pending {
		if !pending.insert {
			continue
		}
		if err := p.editor.Insert(ctx, pending.row); err != nil {
			return err
		}
	}
	clear(p.pending)
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
	encodedTokens, err := jsonbgin.ExtractEncoded(doc, index.OpClass)
	if err != nil {
		return nil, err
	}
	rowID := e.rowIdentity(row)
	postingRows := make([]sql.Row, len(encodedTokens))
	keyValues := e.primaryKeyRowValues(row)
	for i, encodedToken := range encodedTokens {
		postingRow := sql.Row{encodedToken, rowID}
		postingRow = append(postingRow, keyValues...)
		postingRows[i] = postingRow
	}
	return postingRows, nil
}

func (e *jsonbGinMaintainingEditor) rowIdentity(row sql.Row) string {
	if len(e.primaryKeyOrdinals) == 0 {
		return rowIdentity(e.tableSchema, row)
	}
	hash := sha256.New()
	for i, ordinal := range e.primaryKeyOrdinals {
		if i > 0 {
			_, _ = hash.Write([]byte{0})
		}
		if ordinal < len(row) {
			_, _ = hash.Write([]byte(fmt.Sprintf("%T=%v", row[ordinal], row[ordinal])))
		}
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func (e *jsonbGinMaintainingEditor) primaryKeyRowValues(row sql.Row) sql.Row {
	if len(e.primaryKeyOrdinals) == 0 {
		return nil
	}
	keyValues := make(sql.Row, len(e.primaryKeyOrdinals))
	for i, ordinal := range e.primaryKeyOrdinals {
		if ordinal < len(row) {
			keyValues[i] = row[ordinal]
		}
	}
	return keyValues
}

func primaryKeyOrdinals(sch sql.Schema) []int {
	ordinals := make([]int, 0)
	for i, column := range sch {
		if column.PrimaryKey {
			ordinals = append(ordinals, i)
		}
	}
	return ordinals
}

func compactPostingRowsToDelete(oldRows []sql.Row, newRows []sql.Row) []sql.Row {
	newKeys := postingRowKeySet(newRows)
	rows := make([]sql.Row, 0)
	for _, row := range oldRows {
		if _, ok := newKeys[postingRowKey(row)]; !ok {
			rows = append(rows, row)
		}
	}
	return rows
}

func compactPostingRowsToInsert(oldRows []sql.Row, newRows []sql.Row) []sql.Row {
	oldKeys := postingRowKeySet(oldRows)
	rows := make([]sql.Row, 0)
	for _, row := range newRows {
		if _, ok := oldKeys[postingRowKey(row)]; !ok {
			rows = append(rows, row)
		}
	}
	return rows
}

func postingRowKeySet(rows []sql.Row) map[string]struct{} {
	keys := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		keys[postingRowKey(row)] = struct{}{}
	}
	return keys
}

func postingRowKey(row sql.Row) string {
	if len(row) < 2 {
		return ""
	}
	return fmt.Sprintf("%s\x00%s", row[0], row[1])
}

func primaryKeyRowValues(sch sql.Schema, row sql.Row) sql.Row {
	keyValues := make(sql.Row, 0)
	for i, value := range row {
		if i < len(sch) && sch[i].PrimaryKey {
			keyValues = append(keyValues, value)
		}
	}
	return keyValues
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
	rowIDs, candidates, err := t.lookupPostingRowIDs(ctx)
	if err != nil {
		return nil, err
	}
	return sql.PartitionsToPartitionIter(jsonbGinLookupPartition{rowIDs: rowIDs, candidates: candidates}), nil
}

func (t *jsonbGinIndexedTable) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	ginPartition, ok := partition.(jsonbGinLookupPartition)
	if !ok {
		return nil, errors.Errorf("unexpected JSONB GIN lookup partition %T", partition)
	}
	if len(ginPartition.rowIDs) == 0 {
		return sql.RowsToRowIter(), nil
	}
	if len(ginPartition.candidates) > 0 {
		if iter, ok, err := t.directCandidateRowIter(ctx, ginPartition.candidates); err != nil || ok {
			return iter, err
		}
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

func (t *jsonbGinIndexedTable) directCandidateRowIter(ctx *sql.Context, candidates []jsonbGinPostingCandidate) (sql.RowIter, bool, error) {
	indexAddressable, ok := t.underlying.(sql.IndexAddressable)
	if !ok {
		return nil, false, nil
	}
	primaryIndex, ok, err := primaryLookupIndex(ctx, indexAddressable)
	if err != nil || !ok {
		return nil, ok, err
	}
	columnTypes := primaryIndex.ColumnExpressionTypes(ctx)
	if len(columnTypes) == 0 {
		return nil, false, nil
	}
	return &jsonbGinDirectCandidateRowIter{
		table:      indexAddressable,
		index:      primaryIndex,
		keyTypes:   columnTypes,
		candidates: candidates,
	}, true, nil
}

func (t *jsonbGinIndexedTable) lookupPostingRowIDs(ctx *sql.Context) (map[string]struct{}, []jsonbGinPostingCandidate, error) {
	postingTable, err := core.GetSqlTableFromContext(ctx, "", doltdb.TableName{Name: t.lookup.index.PostingTable, Schema: t.schemaName})
	if err != nil {
		return nil, nil, err
	}
	if postingTable == nil {
		return nil, nil, errors.Errorf(`posting table "%s" for gin index "%s" does not exist`, t.lookup.index.PostingTable, t.lookup.index.Name)
	}

	tokenRows := make([]map[string]struct{}, len(t.lookup.encodedTokens))
	tokenCandidates := make([]map[string]jsonbGinPostingCandidate, len(t.lookup.encodedTokens))
	for i, encodedToken := range t.lookup.encodedTokens {
		tokenRows[i], tokenCandidates[i], err = lookupPostingTokenRowIDsAndCandidates(ctx, postingTable, encodedToken)
		if err != nil {
			return nil, nil, err
		}
	}

	var rowIDs map[string]struct{}
	switch t.lookup.mode {
	case jsonbGinLookupUnion:
		rowIDs = unionPostingRowIDs(tokenRows)
	case jsonbGinLookupIntersect:
		rowIDs = intersectPostingRowIDs(tokenRows)
	default:
		return nil, nil, errors.Errorf("unknown JSONB GIN lookup mode %q", t.lookup.mode)
	}
	return rowIDs, postingCandidatesForRowIDs(rowIDs, tokenCandidates), nil
}

func lookupPostingTokenRowIDs(ctx *sql.Context, postingTable sql.Table, encodedToken string) (map[string]struct{}, error) {
	rowIDs, _, err := lookupPostingTokenRowIDsAndCandidates(ctx, postingTable, encodedToken)
	return rowIDs, err
}

func lookupPostingTokenRowIDsAndCandidates(ctx *sql.Context, postingTable sql.Table, encodedToken string) (map[string]struct{}, map[string]jsonbGinPostingCandidate, error) {
	if indexAddressable, ok := postingTable.(sql.IndexAddressable); ok {
		rowIDs, candidates, indexed, err := lookupPostingTokenRowIDsFromIndex(ctx, indexAddressable, encodedToken)
		if err != nil || indexed {
			return rowIDs, candidates, err
		}
	}
	return scanPostingTokenRowIDs(ctx, postingTable, encodedToken)
}

func lookupPostingTokenRowIDsFromIndex(ctx *sql.Context, indexAddressable sql.IndexAddressable, encodedToken string) (map[string]struct{}, map[string]jsonbGinPostingCandidate, bool, error) {
	indexes, err := indexAddressable.GetIndexes(ctx)
	if err != nil {
		return nil, nil, false, err
	}
	postingIndex := jsonbGinPostingTokenIndex(ctx, indexes)
	if postingIndex == nil {
		return nil, nil, false, nil
	}
	columnTypes := postingIndex.ColumnExpressionTypes(ctx)
	if len(columnTypes) == 0 {
		return nil, nil, false, nil
	}
	lookup := sql.NewIndexLookup(postingIndex, sql.MySQLRangeCollection{{
		sql.ClosedRangeColumnExpr(encodedToken, encodedToken, columnTypes[0].Type),
	}}, false, false, false, false)
	indexedTable := indexAddressable.IndexedAccess(ctx, lookup)
	if indexedTable == nil {
		return nil, nil, false, nil
	}
	rowIDs, candidates, err := readPostingIndexedRows(ctx, indexedTable, lookup, encodedToken)
	return rowIDs, candidates, true, err
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

func readPostingIndexedRows(ctx *sql.Context, indexedTable sql.IndexedTable, lookup sql.IndexLookup, encodedToken string) (map[string]struct{}, map[string]jsonbGinPostingCandidate, error) {
	rowIDs := make(map[string]struct{})
	candidates := make(map[string]jsonbGinPostingCandidate)
	partitions, err := indexedTable.LookupPartitions(ctx, lookup)
	if err != nil {
		return nil, nil, err
	}
	defer partitions.Close(ctx)
	for {
		partition, err := partitions.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, err
		}
		rows, err := indexedTable.PartitionRows(ctx, partition)
		if err != nil {
			return nil, nil, err
		}
		if err = readPostingTokenRows(ctx, rows, encodedToken, rowIDs, candidates); err != nil {
			_ = rows.Close(ctx)
			return nil, nil, err
		}
		if err = rows.Close(ctx); err != nil {
			return nil, nil, err
		}
	}
	return rowIDs, candidates, nil
}

func readPostingIndexedRowsUntilLimit(ctx *sql.Context, indexedTable sql.IndexedTable, lookup sql.IndexLookup, encodedToken string, limit int) (bool, error) {
	rowIDs := make(map[string]struct{})
	partitions, err := indexedTable.LookupPartitions(ctx, lookup)
	if err != nil {
		return false, err
	}
	defer partitions.Close(ctx)
	for {
		partition, err := partitions.Next(ctx)
		if err == io.EOF {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		rows, err := indexedTable.PartitionRows(ctx, partition)
		if err != nil {
			return false, err
		}
		exceeds, err := readPostingTokenRowsUntilLimit(ctx, rows, encodedToken, rowIDs, limit)
		closeErr := rows.Close(ctx)
		if err != nil {
			return false, err
		}
		if closeErr != nil {
			return false, closeErr
		}
		if exceeds {
			return true, nil
		}
	}
}

func scanPostingTokenRowIDs(ctx *sql.Context, postingTable sql.Table, encodedToken string) (map[string]struct{}, map[string]jsonbGinPostingCandidate, error) {
	rowIDs := make(map[string]struct{})
	candidates := make(map[string]jsonbGinPostingCandidate)
	partitions, err := postingTable.Partitions(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer partitions.Close(ctx)
	for {
		partition, err := partitions.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, err
		}
		rows, err := postingTable.PartitionRows(ctx, partition)
		if err != nil {
			return nil, nil, err
		}
		if err = readPostingTokenRows(ctx, rows, encodedToken, rowIDs, candidates); err != nil {
			_ = rows.Close(ctx)
			return nil, nil, err
		}
		if err = rows.Close(ctx); err != nil {
			return nil, nil, err
		}
	}
	return rowIDs, candidates, nil
}

func readPostingTokenRows(ctx *sql.Context, rows sql.RowIter, encodedToken string, rowIDs map[string]struct{}, candidates map[string]jsonbGinPostingCandidate) error {
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
		if len(row) > 2 {
			candidates[rowID] = jsonbGinPostingCandidate{rowID: rowID, key: append(sql.Row(nil), row[2:]...)}
		}
	}
}

func readPostingTokenRowsUntilLimit(ctx *sql.Context, rows sql.RowIter, encodedToken string, rowIDs map[string]struct{}, limit int) (bool, error) {
	for {
		row, err := rows.Next(ctx)
		if err == io.EOF {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		if len(row) < 2 || row[0] == nil || row[1] == nil {
			continue
		}
		tokenText, ok := row[0].(string)
		if !ok {
			return false, errors.Errorf("unexpected JSONB GIN posting token type %T", row[0])
		}
		if tokenText != encodedToken {
			continue
		}
		rowID, ok := row[1].(string)
		if !ok {
			return false, errors.Errorf("unexpected JSONB GIN posting row identity type %T", row[1])
		}
		rowIDs[rowID] = struct{}{}
		if len(rowIDs) > limit {
			return true, nil
		}
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

func postingCandidatesForRowIDs(rowIDs map[string]struct{}, tokenCandidates []map[string]jsonbGinPostingCandidate) []jsonbGinPostingCandidate {
	if len(rowIDs) == 0 || len(tokenCandidates) == 0 {
		return nil
	}
	candidates := make([]jsonbGinPostingCandidate, 0, len(rowIDs))
	for rowID := range rowIDs {
		found := false
		var candidate jsonbGinPostingCandidate
		for _, candidatesByID := range tokenCandidates {
			if candidate, found = candidatesByID[rowID]; found {
				break
			}
		}
		if !found || len(candidate.key) == 0 {
			return nil
		}
		candidates = append(candidates, candidate)
	}
	return candidates
}

type jsonbGinLookupPartition struct {
	rowIDs     map[string]struct{}
	candidates []jsonbGinPostingCandidate
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

type jsonbGinDirectCandidateRowIter struct {
	table      sql.IndexAddressable
	index      sql.Index
	keyTypes   []sql.ColumnExpressionType
	candidates []jsonbGinPostingCandidate
	idx        int
	current    sql.RowIter
}

var _ sql.RowIter = (*jsonbGinDirectCandidateRowIter)(nil)

func (i *jsonbGinDirectCandidateRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	for {
		if i.current == nil {
			if i.idx >= len(i.candidates) {
				return nil, io.EOF
			}
			candidate := i.candidates[i.idx]
			i.idx++
			if len(candidate.key) != len(i.keyTypes) {
				continue
			}
			indexedTable := i.table.IndexedAccess(ctx, sql.NewIndexLookup(i.index, sql.MySQLRangeCollection{primaryKeyRanges(candidate.key, i.keyTypes)}, false, false, false, false))
			if indexedTable == nil {
				continue
			}
			partitions, err := indexedTable.LookupPartitions(ctx, sql.NewIndexLookup(i.index, sql.MySQLRangeCollection{primaryKeyRanges(candidate.key, i.keyTypes)}, false, false, false, false))
			if err != nil {
				return nil, err
			}
			partition, err := partitions.Next(ctx)
			closeErr := partitions.Close(ctx)
			if err == io.EOF {
				if closeErr != nil {
					return nil, closeErr
				}
				continue
			}
			if err != nil {
				return nil, err
			}
			if closeErr != nil {
				return nil, closeErr
			}
			i.current, err = indexedTable.PartitionRows(ctx, partition)
			if err != nil {
				return nil, err
			}
		}
		row, err := i.current.Next(ctx)
		if err == io.EOF {
			if closeErr := i.current.Close(ctx); closeErr != nil {
				return nil, closeErr
			}
			i.current = nil
			continue
		}
		return row, err
	}
}

func (i *jsonbGinDirectCandidateRowIter) Close(ctx *sql.Context) error {
	if i.current != nil {
		err := i.current.Close(ctx)
		i.current = nil
		return err
	}
	return nil
}

func primaryLookupIndex(ctx *sql.Context, table sql.IndexAddressable) (sql.Index, bool, error) {
	indexes, err := table.GetIndexes(ctx)
	if err != nil {
		return nil, false, err
	}
	for _, index := range indexes {
		if strings.EqualFold(index.ID(), "PRIMARY") {
			return index, true, nil
		}
	}
	return nil, false, nil
}

func primaryKeyRanges(key sql.Row, keyTypes []sql.ColumnExpressionType) sql.MySQLRange {
	ranges := make(sql.MySQLRange, len(key))
	for i, value := range key {
		ranges[i] = sql.ClosedRangeColumnExpr(value, value, keyTypes[i].Type)
	}
	return ranges
}
