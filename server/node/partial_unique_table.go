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
	"crypto/md5"
	"fmt"
	"go/constant"
	"io"
	"reflect"
	"strings"
	"unicode/utf8"

	"github.com/cockroachdb/errors"
	doltsqle "github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/postgres/parser/parser"
	"github.com/dolthub/doltgresql/postgres/parser/sem/tree"
	"github.com/dolthub/doltgresql/server/indexmetadata"
)

type partialUniqueIndex struct {
	index         sql.Index
	name          string
	columnIndexes []int
	columnTypes   []sql.Type
	predicate     *partialIndexPredicate
}

// PartialUniqueTable enforces PostgreSQL partial unique indexes around a
// non-unique physical index. Native Dolt uniqueness would reject rows outside
// the predicate, so Doltgres records the index as PostgreSQL-unique in metadata
// and applies the predicate-scoped duplicate check here.
type PartialUniqueTable struct {
	underlying sql.Table
	indexes    []partialUniqueIndex
}

var _ sql.TableWrapper = (*PartialUniqueTable)(nil)
var _ sql.MutableTableWrapper = (*PartialUniqueTable)(nil)
var _ sql.InsertableTable = (*PartialUniqueTable)(nil)
var _ sql.ReplaceableTable = (*PartialUniqueTable)(nil)
var _ sql.UpdatableTable = (*PartialUniqueTable)(nil)
var _ sql.IndexAddressable = (*PartialUniqueTable)(nil)
var _ sql.IndexedTable = (*PartialUniqueTable)(nil)

// WrapPartialUniqueTable wraps table when it has partial unique index metadata.
func WrapPartialUniqueTable(ctx *sql.Context, table sql.Table) (sql.Table, bool, error) {
	if _, ok := table.(*PartialUniqueTable); ok {
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
	checks := make([]partialUniqueIndex, 0)
	for _, index := range indexes {
		if !indexmetadata.IsPartialUnique(index) {
			continue
		}
		check, err := partialUniqueIndexFromIndex(ctx, index, table.Name(), tableSchema)
		if err != nil {
			return nil, false, err
		}
		checks = append(checks, check)
	}
	if len(checks) == 0 {
		return table, false, nil
	}
	return &PartialUniqueTable{
		underlying: table,
		indexes:    checks,
	}, true, nil
}

func partialUniqueIndexFromIndex(ctx *sql.Context, index sql.Index, tableName string, schema sql.Schema) (partialUniqueIndex, error) {
	if indexmetadata.AccessMethod(index.IndexType(), index.Comment()) != indexmetadata.AccessMethodBtree {
		return partialUniqueIndex{}, errors.Errorf("partial unique indexes are not yet supported for %s indexes", index.IndexType())
	}
	logicalColumns := indexmetadata.LogicalColumns(index, schema)
	columnTypes := index.ColumnExpressionTypes(ctx)
	check := partialUniqueIndex{
		index:         index,
		name:          index.ID(),
		columnIndexes: make([]int, len(logicalColumns)),
		columnTypes:   make([]sql.Type, len(logicalColumns)),
	}
	for i, column := range logicalColumns {
		if column.Expression {
			return partialUniqueIndex{}, errors.Errorf("partial unique expression indexes are not yet supported")
		}
		columnIndex := schema.IndexOfColName(column.StorageName)
		if columnIndex < 0 {
			return partialUniqueIndex{}, sql.ErrKeyColumnDoesNotExist.New(column.StorageName)
		}
		check.columnIndexes[i] = columnIndex
		if i < len(columnTypes) {
			check.columnTypes[i] = columnTypes[i].Type
		} else {
			check.columnTypes[i] = schema[columnIndex].Type
		}
	}
	predicate, err := parsePartialUniquePredicate(indexmetadata.Predicate(index.Comment()), tableName, schema)
	if err != nil {
		return partialUniqueIndex{}, err
	}
	check.predicate = predicate
	return check, nil
}

func partialUniqueIndexFromColumns(indexName string, tableName string, schema sql.Schema, columns []sql.IndexColumn, metadata indexmetadata.Metadata) (partialUniqueIndex, error) {
	check := partialUniqueIndex{
		name:          indexName,
		columnIndexes: make([]int, len(columns)),
		columnTypes:   make([]sql.Type, len(columns)),
	}
	for i, column := range columns {
		if column.Expression != nil {
			return partialUniqueIndex{}, errors.Errorf("partial unique expression indexes are not yet supported")
		}
		columnIndex := schema.IndexOfColName(column.Name)
		if columnIndex < 0 {
			return partialUniqueIndex{}, sql.ErrKeyColumnDoesNotExist.New(column.Name)
		}
		check.columnIndexes[i] = columnIndex
		check.columnTypes[i] = schema[columnIndex].Type
	}
	predicate, err := parsePartialUniquePredicate(metadata.Predicate, tableName, schema)
	if err != nil {
		return partialUniqueIndex{}, err
	}
	check.predicate = predicate
	return check, nil
}

func (t *PartialUniqueTable) Underlying() sql.Table {
	return t.underlying
}

func (t *PartialUniqueTable) WithUnderlying(table sql.Table) sql.Table {
	return &PartialUniqueTable{
		underlying: table,
		indexes:    t.indexes,
	}
}

func (t *PartialUniqueTable) Name() string {
	return t.underlying.Name()
}

func (t *PartialUniqueTable) String() string {
	return t.underlying.String()
}

func (t *PartialUniqueTable) Schema(ctx *sql.Context) sql.Schema {
	return t.underlying.Schema(ctx)
}

func (t *PartialUniqueTable) Collation() sql.CollationID {
	return t.underlying.Collation()
}

func (t *PartialUniqueTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return t.underlying.Partitions(ctx)
}

func (t *PartialUniqueTable) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	return t.underlying.PartitionRows(ctx, partition)
}

func (t *PartialUniqueTable) DatabaseSchema() sql.DatabaseSchema {
	if schemaTable, ok := t.underlying.(sql.DatabaseSchemaTable); ok {
		return schemaTable.DatabaseSchema()
	}
	return nil
}

func (t *PartialUniqueTable) IndexedAccess(ctx *sql.Context, lookup sql.IndexLookup) sql.IndexedTable {
	if indexAddressable, ok := t.underlying.(sql.IndexAddressable); ok {
		return indexAddressable.IndexedAccess(ctx, lookup)
	}
	return nil
}

func (t *PartialUniqueTable) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	if indexAddressable, ok := t.underlying.(sql.IndexAddressable); ok {
		return indexAddressable.GetIndexes(ctx)
	}
	return nil, nil
}

func (t *PartialUniqueTable) LookupPartitions(ctx *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	if indexedTable, ok := t.underlying.(sql.IndexedTable); ok {
		return indexedTable.LookupPartitions(ctx, lookup)
	}
	return nil, errors.Errorf("table %s is not indexed", t.Name())
}

func (t *PartialUniqueTable) PreciseMatch() bool {
	if indexAddressable, ok := t.underlying.(sql.IndexAddressable); ok {
		return indexAddressable.PreciseMatch()
	}
	return false
}

func (t *PartialUniqueTable) Inserter(ctx *sql.Context) sql.RowInserter {
	insertable, ok := t.underlying.(sql.InsertableTable)
	if !ok {
		return sqlutil.NewStaticErrorEditor(planErr("table %s is not insertable", t.Name()))
	}
	return &partialUniqueEditor{
		table:   t,
		primary: insertable.Inserter(ctx),
	}
}

func (t *PartialUniqueTable) Replacer(ctx *sql.Context) sql.RowReplacer {
	replaceable, ok := t.underlying.(sql.ReplaceableTable)
	if !ok {
		return sqlutil.NewStaticErrorEditor(planErr("table %s is not replaceable", t.Name()))
	}
	return &partialUniqueEditor{
		table:   t,
		primary: replaceable.Replacer(ctx),
	}
}

func (t *PartialUniqueTable) Updater(ctx *sql.Context) sql.RowUpdater {
	updatable, ok := t.underlying.(sql.UpdatableTable)
	if !ok {
		return sqlutil.NewStaticErrorEditor(planErr("table %s is not updatable", t.Name()))
	}
	return &partialUniqueEditor{
		table:   t,
		primary: updatable.Updater(ctx),
	}
}

func (t *PartialUniqueTable) findDuplicate(ctx *sql.Context, index partialUniqueIndex, key sql.Row, oldRow sql.Row, ignoreRows []sql.Row) (sql.Row, error) {
	table := scanTableForPartialUniqueCheck(t.underlying)
	duplicate, _, err := firstMatchingPartialUniqueIndexedRow(ctx, table, index, key, oldRow, ignoreRows)
	if err != nil || duplicate != nil {
		return duplicate, err
	}
	return firstMatchingPartialUniqueRow(ctx, table, index, key, oldRow, ignoreRows)
}

func scanTableForPartialUniqueCheck(table sql.Table) sql.Table {
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

func firstMatchingPartialUniqueIndexedRow(ctx *sql.Context, table sql.Table, index partialUniqueIndex, key sql.Row, oldRow sql.Row, ignoreRows []sql.Row) (sql.Row, bool, error) {
	if index.index == nil {
		return nil, false, nil
	}
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
	duplicate, err := firstMatchingPartialUniqueLookupRow(ctx, indexedTable, lookup, index, key, oldRow, ignoreRows)
	return duplicate, true, err
}

func firstMatchingPartialUniqueLookupRow(ctx *sql.Context, table sql.IndexedTable, lookup sql.IndexLookup, index partialUniqueIndex, key sql.Row, oldRow sql.Row, ignoreRows []sql.Row) (sql.Row, error) {
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
		duplicate, err := nextMatchingPartialUniqueRow(ctx, rows, index, key, oldRow, ignoreRows)
		if closeErr := rows.Close(ctx); err == nil {
			err = closeErr
		}
		if err != nil || duplicate != nil {
			return duplicate, err
		}
	}
}

func firstMatchingPartialUniqueRow(ctx *sql.Context, table sql.Table, index partialUniqueIndex, key sql.Row, oldRow sql.Row, ignoreRows []sql.Row) (sql.Row, error) {
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
		duplicate, err := nextMatchingPartialUniqueRow(ctx, rows, index, key, oldRow, ignoreRows)
		if closeErr := rows.Close(ctx); err == nil {
			err = closeErr
		}
		if err != nil || duplicate != nil {
			return duplicate, err
		}
	}
}

func nextMatchingPartialUniqueRow(ctx *sql.Context, rows sql.RowIter, index partialUniqueIndex, key sql.Row, oldRow sql.Row, ignoreRows []sql.Row) (sql.Row, error) {
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
		if shouldIgnorePartialUniqueRow(row, ignoreRows) {
			continue
		}
		predicateMatches, err := index.predicate.matches(ctx, row)
		if err != nil {
			return nil, err
		}
		if !predicateMatches {
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

func shouldIgnorePartialUniqueRow(row sql.Row, ignoreRows []sql.Row) bool {
	for _, ignoreRow := range ignoreRows {
		if reflect.DeepEqual(row, ignoreRow) {
			return true
		}
	}
	return false
}

func (i partialUniqueIndex) key(row sql.Row) (sql.Row, bool) {
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

func (i partialUniqueIndex) lookup(ctx *sql.Context, key sql.Row) (sql.IndexLookup, error) {
	ranges := make(sql.MySQLRange, len(key))
	for n, value := range key {
		if n >= len(i.columnTypes) || i.columnTypes[n] == nil {
			return sql.IndexLookup{}, errors.Errorf("missing type for partial unique index column %d", n)
		}
		ranges[n] = sql.ClosedRangeColumnExpr(value, value, i.columnTypes[n])
	}
	return sql.NewIndexLookup(i.index, sql.MySQLRangeCollection{ranges}, true, false, false, false), nil
}

func (i partialUniqueIndex) rowMatchesKey(ctx *sql.Context, row sql.Row, key sql.Row) (bool, error) {
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

func (i partialUniqueIndex) keyMatches(ctx *sql.Context, left sql.Row, right sql.Row) (bool, error) {
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

func (i partialUniqueIndex) valuesMatch(ctx *sql.Context, columnIndex int, left any, right any) (bool, error) {
	if left == nil || right == nil {
		return false, nil
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

type partialUniquePrimaryEditor interface {
	sql.EditOpenerCloser
	Close(*sql.Context) error
}

type partialUniqueEditor struct {
	table       *PartialUniqueTable
	primary     partialUniquePrimaryEditor
	pendingRows map[int][]pendingPartialUniqueRow
	removedRows []sql.Row
}

type pendingPartialUniqueRow struct {
	key sql.Row
	row sql.Row
}

var _ sql.TableEditor = (*partialUniqueEditor)(nil)

func (e *partialUniqueEditor) StatementBegin(ctx *sql.Context) {
	e.pendingRows = nil
	e.removedRows = nil
	e.primary.StatementBegin(ctx)
}

func (e *partialUniqueEditor) DiscardChanges(ctx *sql.Context, err error) error {
	e.pendingRows = nil
	e.removedRows = nil
	return e.primary.DiscardChanges(ctx, err)
}

func (e *partialUniqueEditor) StatementComplete(ctx *sql.Context) error {
	err := e.primary.StatementComplete(ctx)
	e.pendingRows = nil
	e.removedRows = nil
	return err
}

func (e *partialUniqueEditor) Insert(ctx *sql.Context, row sql.Row) error {
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
	return nil
}

func (e *partialUniqueEditor) Delete(ctx *sql.Context, row sql.Row) error {
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

func (e *partialUniqueEditor) Update(ctx *sql.Context, oldRow sql.Row, newRow sql.Row) error {
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
	return nil
}

func (e *partialUniqueEditor) Close(ctx *sql.Context) error {
	err := e.primary.Close(ctx)
	e.pendingRows = nil
	e.removedRows = nil
	return err
}

func (e *partialUniqueEditor) validateRow(ctx *sql.Context, row sql.Row, oldRow sql.Row) error {
	for indexOffset, index := range e.table.indexes {
		predicateMatches, err := index.predicate.matches(ctx, row)
		if err != nil {
			return err
		}
		if !predicateMatches {
			continue
		}
		key, hasNull := index.key(row)
		if hasNull {
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

func (e *partialUniqueEditor) pendingDuplicate(ctx *sql.Context, indexOffset int, index partialUniqueIndex, key sql.Row) (sql.Row, error) {
	for _, pending := range e.pendingRows[indexOffset] {
		matches, err := index.keyMatches(ctx, pending.key, key)
		if err != nil || matches {
			return pending.row, err
		}
	}
	return nil, nil
}

func (e *partialUniqueEditor) recordPendingRow(ctx *sql.Context, row sql.Row) {
	for indexOffset, index := range e.table.indexes {
		predicateMatches, err := index.predicate.matches(ctx, row)
		if err != nil || !predicateMatches {
			continue
		}
		key, hasNull := index.key(row)
		if hasNull {
			continue
		}
		if e.pendingRows == nil {
			e.pendingRows = make(map[int][]pendingPartialUniqueRow, len(e.table.indexes))
		}
		e.pendingRows[indexOffset] = append(e.pendingRows[indexOffset], pendingPartialUniqueRow{
			key: key,
			row: row,
		})
	}
}

func validateNoPartialUniqueDuplicates(ctx *sql.Context, table sql.Table, index partialUniqueIndex) error {
	seen := make([]pendingPartialUniqueRow, 0)
	partitions, err := table.Partitions(ctx)
	if err != nil {
		return err
	}
	defer partitions.Close(ctx)
	for {
		partition, err := partitions.Next(ctx)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		rows, err := table.PartitionRows(ctx, partition)
		if err != nil {
			return err
		}
		for {
			row, err := rows.Next(ctx)
			if err == io.EOF {
				break
			}
			if err != nil {
				_ = rows.Close(ctx)
				return err
			}
			predicateMatches, err := index.predicate.matches(ctx, row)
			if err != nil {
				_ = rows.Close(ctx)
				return err
			}
			if !predicateMatches {
				continue
			}
			key, hasNull := index.key(row)
			if hasNull {
				continue
			}
			for _, pending := range seen {
				matches, err := index.keyMatches(ctx, pending.key, key)
				if err != nil {
					_ = rows.Close(ctx)
					return err
				}
				if matches {
					_ = rows.Close(ctx)
					return sql.NewUniqueKeyErr(fmt.Sprintf("%v", key), false, pending.row)
				}
			}
			seen = append(seen, pendingPartialUniqueRow{key: key, row: row})
		}
		if err := rows.Close(ctx); err != nil {
			return err
		}
	}
}

type partialIndexPredicate struct {
	expr      tree.Expr
	tableName string
	schema    sql.Schema
}

type predicateTruth uint8

const (
	predicateUnknown predicateTruth = iota
	predicateFalse
	predicateTrue
)

type predicateValue struct {
	value any
	typ   sql.Type
}

func parsePartialUniquePredicate(predicate string, tableName string, schema sql.Schema) (*partialIndexPredicate, error) {
	statements, err := parser.Parse("SELECT 1 WHERE " + predicate)
	if err != nil {
		return nil, err
	}
	if len(statements) != 1 {
		return nil, errors.Errorf("partial index predicate must parse as a single expression")
	}
	selectStatement, ok := statements[0].AST.(*tree.Select)
	if !ok {
		return nil, errors.Errorf("partial index predicate parse did not produce SELECT")
	}
	selectClause, ok := selectStatement.Select.(*tree.SelectClause)
	if !ok || selectClause.Where == nil {
		return nil, errors.Errorf("partial index predicate parse did not produce WHERE")
	}
	predicateExpr := selectClause.Where.Expr
	compiled := &partialIndexPredicate{
		expr:      predicateExpr,
		tableName: tableName,
		schema:    schema,
	}
	if err = compiled.validateColumns(predicateExpr); err != nil {
		return nil, err
	}
	return compiled, nil
}

func (p *partialIndexPredicate) matches(ctx *sql.Context, row sql.Row) (bool, error) {
	truth, err := p.evalBool(ctx, row, p.expr)
	return truth == predicateTrue, err
}

func (p *partialIndexPredicate) validateColumns(expr tree.Expr) error {
	switch expr := expr.(type) {
	case *tree.AndExpr:
		if err := p.validateColumns(expr.Left); err != nil {
			return err
		}
		return p.validateColumns(expr.Right)
	case *tree.ComparisonExpr:
		if err := p.validateColumns(expr.Left); err != nil {
			return err
		}
		return p.validateColumns(expr.Right)
	case *tree.FuncExpr:
		for _, fnExpr := range expr.Exprs {
			if err := p.validateColumns(fnExpr); err != nil {
				return err
			}
		}
	case *tree.IsNotNullExpr:
		return p.validateColumns(expr.Expr)
	case *tree.IsNullExpr:
		return p.validateColumns(expr.Expr)
	case *tree.NotExpr:
		return p.validateColumns(expr.Expr)
	case *tree.OrExpr:
		if err := p.validateColumns(expr.Left); err != nil {
			return err
		}
		return p.validateColumns(expr.Right)
	case *tree.ParenExpr:
		return p.validateColumns(expr.Expr)
	case *tree.RangeCond:
		if err := p.validateColumns(expr.Left); err != nil {
			return err
		}
		if err := p.validateColumns(expr.From); err != nil {
			return err
		}
		return p.validateColumns(expr.To)
	case *tree.Tuple:
		for _, tupleExpr := range expr.Exprs {
			if err := p.validateColumns(tupleExpr); err != nil {
				return err
			}
		}
	case *tree.UnresolvedName:
		name, err := p.columnName(expr)
		if err != nil {
			return err
		}
		if p.schema.IndexOfColName(name) < 0 {
			return sql.ErrKeyColumnDoesNotExist.New(name)
		}
	}
	return nil
}

func (p *partialIndexPredicate) evalBool(ctx *sql.Context, row sql.Row, expr tree.Expr) (predicateTruth, error) {
	switch expr := expr.(type) {
	case *tree.AndExpr:
		left, err := p.evalBool(ctx, row, expr.Left)
		if err != nil || left == predicateFalse {
			return left, err
		}
		right, err := p.evalBool(ctx, row, expr.Right)
		if err != nil || right == predicateFalse {
			return right, err
		}
		if left == predicateTrue && right == predicateTrue {
			return predicateTrue, nil
		}
		return predicateUnknown, nil
	case *tree.ComparisonExpr:
		return p.evalComparison(ctx, row, expr)
	case *tree.IsNotNullExpr:
		value, err := p.evalValue(ctx, row, expr.Expr)
		if err != nil {
			return predicateUnknown, err
		}
		if value.value != nil {
			return predicateTrue, nil
		}
		return predicateFalse, nil
	case *tree.IsNullExpr:
		value, err := p.evalValue(ctx, row, expr.Expr)
		if err != nil {
			return predicateUnknown, err
		}
		if value.value == nil {
			return predicateTrue, nil
		}
		return predicateFalse, nil
	case *tree.NotExpr:
		truth, err := p.evalBool(ctx, row, expr.Expr)
		if err != nil || truth == predicateUnknown {
			return truth, err
		}
		if truth == predicateTrue {
			return predicateFalse, nil
		}
		return predicateTrue, nil
	case *tree.OrExpr:
		left, err := p.evalBool(ctx, row, expr.Left)
		if err != nil || left == predicateTrue {
			return left, err
		}
		right, err := p.evalBool(ctx, row, expr.Right)
		if err != nil || right == predicateTrue {
			return right, err
		}
		if left == predicateFalse && right == predicateFalse {
			return predicateFalse, nil
		}
		return predicateUnknown, nil
	case *tree.ParenExpr:
		return p.evalBool(ctx, row, expr.Expr)
	case *tree.RangeCond:
		return p.evalRangeCond(ctx, row, expr)
	default:
		value, err := p.evalValue(ctx, row, expr)
		if err != nil {
			return predicateUnknown, err
		}
		return predicateTruthFromValue(value.value)
	}
}

func (p *partialIndexPredicate) evalRangeCond(ctx *sql.Context, row sql.Row, expr *tree.RangeCond) (predicateTruth, error) {
	left, err := p.evalValue(ctx, row, expr.Left)
	if err != nil {
		return predicateUnknown, err
	}
	from, err := p.evalValue(ctx, row, expr.From)
	if err != nil {
		return predicateUnknown, err
	}
	to, err := p.evalValue(ctx, row, expr.To)
	if err != nil {
		return predicateUnknown, err
	}

	truth, err := predicateBetween(ctx, left, from, to)
	if err != nil {
		return predicateUnknown, err
	}
	if expr.Symmetric {
		reverse, err := predicateBetween(ctx, left, to, from)
		if err != nil {
			return predicateUnknown, err
		}
		truth = predicateOr(truth, reverse)
	}
	if expr.Not {
		truth = predicateNot(truth)
	}
	return truth, nil
}

func predicateBetween(ctx *sql.Context, left predicateValue, from predicateValue, to predicateValue) (predicateTruth, error) {
	if left.value == nil || from.value == nil || to.value == nil {
		return predicateUnknown, nil
	}
	lowerCmp, err := comparePredicateValues(ctx, left, from)
	if err != nil {
		return predicateUnknown, err
	}
	upperCmp, err := comparePredicateValues(ctx, left, to)
	if err != nil {
		return predicateUnknown, err
	}
	return predicateTruthFromBool(lowerCmp >= 0 && upperCmp <= 0), nil
}

func predicateOr(left predicateTruth, right predicateTruth) predicateTruth {
	if left == predicateTrue || right == predicateTrue {
		return predicateTrue
	}
	if left == predicateFalse && right == predicateFalse {
		return predicateFalse
	}
	return predicateUnknown
}

func predicateNot(truth predicateTruth) predicateTruth {
	switch truth {
	case predicateTrue:
		return predicateFalse
	case predicateFalse:
		return predicateTrue
	default:
		return predicateUnknown
	}
}

func (p *partialIndexPredicate) evalComparison(ctx *sql.Context, row sql.Row, expr *tree.ComparisonExpr) (predicateTruth, error) {
	if expr.Operator == tree.In || expr.Operator == tree.NotIn {
		return p.evalInComparison(ctx, row, expr)
	}
	if expr.Operator == tree.Like || expr.Operator == tree.NotLike {
		return p.evalLikeComparison(ctx, row, expr)
	}

	left, err := p.evalValue(ctx, row, expr.Left)
	if err != nil {
		return predicateUnknown, err
	}
	right, err := p.evalValue(ctx, row, expr.Right)
	if err != nil {
		return predicateUnknown, err
	}
	if expr.Operator == tree.IsDistinctFrom || expr.Operator == tree.IsNotDistinctFrom {
		distinct, err := comparePredicateDistinct(ctx, left, right)
		if err != nil {
			return predicateUnknown, err
		}
		if expr.Operator == tree.IsNotDistinctFrom {
			distinct = !distinct
		}
		return predicateTruthFromBool(distinct), nil
	}
	if left.value == nil || right.value == nil {
		return predicateUnknown, nil
	}
	cmp, err := comparePredicateValues(ctx, left, right)
	if err != nil {
		return predicateUnknown, err
	}
	switch expr.Operator {
	case tree.EQ:
		return predicateTruthFromBool(cmp == 0), nil
	case tree.NE:
		return predicateTruthFromBool(cmp != 0), nil
	case tree.LT:
		return predicateTruthFromBool(cmp < 0), nil
	case tree.LE:
		return predicateTruthFromBool(cmp <= 0), nil
	case tree.GT:
		return predicateTruthFromBool(cmp > 0), nil
	case tree.GE:
		return predicateTruthFromBool(cmp >= 0), nil
	default:
		return predicateUnknown, errors.Errorf("partial unique index predicate operator %s is not yet supported", expr.Operator.String())
	}
}

func (p *partialIndexPredicate) evalInComparison(ctx *sql.Context, row sql.Row, expr *tree.ComparisonExpr) (predicateTruth, error) {
	left, err := p.evalValue(ctx, row, expr.Left)
	if err != nil {
		return predicateUnknown, err
	}
	if left.value == nil {
		return predicateUnknown, nil
	}
	tuple, ok := tree.StripParens(expr.Right).(*tree.Tuple)
	if !ok {
		return predicateUnknown, errors.Errorf("partial unique index predicate IN expression %T is not yet supported", expr.Right)
	}

	sawUnknown := false
	for _, tupleExpr := range tuple.Exprs {
		right, err := p.evalValue(ctx, row, tupleExpr)
		if err != nil {
			return predicateUnknown, err
		}
		if right.value == nil {
			sawUnknown = true
			continue
		}
		cmp, err := comparePredicateValues(ctx, left, right)
		if err != nil {
			return predicateUnknown, err
		}
		if cmp == 0 {
			truth := predicateTrue
			if expr.Operator == tree.NotIn {
				truth = predicateFalse
			}
			return truth, nil
		}
	}
	if sawUnknown {
		return predicateUnknown, nil
	}
	truth := predicateFalse
	if expr.Operator == tree.NotIn {
		truth = predicateTrue
	}
	return truth, nil
}

func (p *partialIndexPredicate) evalLikeComparison(ctx *sql.Context, row sql.Row, expr *tree.ComparisonExpr) (predicateTruth, error) {
	left, err := p.evalValue(ctx, row, expr.Left)
	if err != nil {
		return predicateUnknown, err
	}
	right, err := p.evalValue(ctx, row, expr.Right)
	if err != nil {
		return predicateUnknown, err
	}
	if left.value == nil || right.value == nil {
		return predicateUnknown, nil
	}
	text, ok := left.value.(string)
	if !ok {
		return predicateUnknown, errors.Errorf("partial unique index predicate operator LIKE does not support %T", left.value)
	}
	pattern, ok := right.value.(string)
	if !ok {
		return predicateUnknown, errors.Errorf("partial unique index predicate operator LIKE does not support %T", right.value)
	}
	prefix, ok := textPatternPrefix(pattern, '\\')
	if !ok {
		return predicateUnknown, errors.Errorf("partial unique index predicate operator LIKE only supports literal prefix patterns")
	}
	matches := strings.HasPrefix(text, prefix)
	if expr.Operator == tree.NotLike {
		matches = !matches
	}
	return predicateTruthFromBool(matches), nil
}

func comparePredicateDistinct(ctx *sql.Context, left predicateValue, right predicateValue) (bool, error) {
	if left.value == nil || right.value == nil {
		return left.value != nil || right.value != nil, nil
	}
	cmp, err := comparePredicateValues(ctx, left, right)
	if err != nil {
		return false, err
	}
	return cmp != 0, nil
}

func comparePredicateValues(ctx *sql.Context, left predicateValue, right predicateValue) (int, error) {
	if left.typ != nil {
		rightValue, _, err := left.typ.Convert(ctx, right.value)
		if err != nil {
			return 0, err
		}
		return left.typ.Compare(ctx, left.value, rightValue)
	}
	if right.typ != nil {
		leftValue, _, err := right.typ.Convert(ctx, left.value)
		if err != nil {
			return 0, err
		}
		cmp, err := right.typ.Compare(ctx, leftValue, right.value)
		return -cmp, err
	}
	switch leftValue := left.value.(type) {
	case bool:
		rightValue, ok := right.value.(bool)
		if !ok {
			return 0, errors.Errorf("cannot compare bool to %T", right.value)
		}
		return compareOrdered(leftValue, rightValue), nil
	case int64:
		rightValue, ok := right.value.(int64)
		if !ok {
			return 0, errors.Errorf("cannot compare int to %T", right.value)
		}
		return compareOrdered(leftValue, rightValue), nil
	case string:
		rightValue, ok := right.value.(string)
		if !ok {
			return 0, errors.Errorf("cannot compare string to %T", right.value)
		}
		return compareOrdered(leftValue, rightValue), nil
	default:
		if reflect.DeepEqual(left.value, right.value) {
			return 0, nil
		}
		return 0, errors.Errorf("cannot compare %T to %T", left.value, right.value)
	}
}

func compareOrdered[T ~bool | ~int64 | ~string](left T, right T) int {
	if left == right {
		return 0
	}
	if !leftLess(left, right) {
		return 1
	}
	return -1
}

func leftLess[T ~bool | ~int64 | ~string](left T, right T) bool {
	switch left := any(left).(type) {
	case bool:
		return !left && any(right).(bool)
	case int64:
		return left < any(right).(int64)
	case string:
		return left < any(right).(string)
	default:
		panic("unsupported ordered comparison")
	}
}

func (p *partialIndexPredicate) evalValue(ctx *sql.Context, row sql.Row, expr tree.Expr) (predicateValue, error) {
	switch expr := expr.(type) {
	case *tree.DBool:
		return predicateValue{value: bool(*expr)}, nil
	case *tree.DInt:
		return predicateValue{value: int64(*expr)}, nil
	case *tree.DString:
		return predicateValue{value: string(*expr)}, nil
	case *tree.NumVal:
		if expr.Kind() == constant.Int {
			value, err := expr.AsInt64()
			if err != nil {
				return predicateValue{}, err
			}
			return predicateValue{value: value}, nil
		}
		return predicateValue{value: expr.FormattedString()}, nil
	case *tree.ParenExpr:
		return p.evalValue(ctx, row, expr.Expr)
	case *tree.UnaryExpr:
		return p.evalUnary(ctx, row, expr)
	case *tree.CoalesceExpr:
		return p.evalCoalesce(ctx, row, expr)
	case *tree.FuncExpr:
		return p.evalFunction(ctx, row, expr)
	case *tree.StrVal:
		return predicateValue{value: expr.RawString()}, nil
	case *tree.UnresolvedName:
		name, err := p.columnName(expr)
		if err != nil {
			return predicateValue{}, err
		}
		columnIndex := p.schema.IndexOfColName(name)
		if columnIndex < 0 {
			return predicateValue{}, sql.ErrKeyColumnDoesNotExist.New(name)
		}
		if columnIndex >= len(row) {
			return predicateValue{}, errors.Errorf("row is missing predicate column %q", name)
		}
		return predicateValue{
			value: row[columnIndex],
			typ:   p.schema[columnIndex].Type,
		}, nil
	case tree.NullLiteral:
		return predicateValue{}, nil
	default:
		return predicateValue{}, errors.Errorf("partial unique index predicate expression %T is not yet supported", expr)
	}
}

func (p *partialIndexPredicate) evalUnary(ctx *sql.Context, row sql.Row, expr *tree.UnaryExpr) (predicateValue, error) {
	if expr.Operator != tree.UnaryMinus {
		return predicateValue{}, errors.Errorf("partial unique index predicate unary operator %s is not yet supported", expr.Operator.String())
	}
	value, err := p.evalValue(ctx, row, expr.Expr)
	if err != nil || value.value == nil {
		return predicateValue{}, err
	}
	intValue, ok := predicateSignedIntegerValue(value.value)
	if !ok {
		return predicateValue{}, errors.Errorf("partial unique index predicate unary minus does not support %T", value.value)
	}
	if intValue == -1<<63 {
		return predicateValue{}, errors.New("partial unique index predicate unary minus overflowed int64")
	}
	return predicateValue{value: -intValue}, nil
}

func (p *partialIndexPredicate) evalCoalesce(ctx *sql.Context, row sql.Row, expr *tree.CoalesceExpr) (predicateValue, error) {
	for _, child := range expr.Exprs {
		value, err := p.evalValue(ctx, row, child)
		if err != nil {
			return predicateValue{}, err
		}
		if value.value != nil {
			return value, nil
		}
	}
	return predicateValue{}, nil
}

func (p *partialIndexPredicate) evalFunction(ctx *sql.Context, row sql.Row, expr *tree.FuncExpr) (predicateValue, error) {
	name, ok := partialPredicateFunctionName(expr.Func)
	if !ok {
		return predicateValue{}, errors.Errorf("partial unique index predicate function %s is not yet supported", expr.Func.String())
	}
	if name == "strpos" {
		return p.evalStrpos(ctx, row, expr)
	}
	if name == "starts_with" {
		return p.evalStartsWith(ctx, row, expr)
	}
	if name == "left" || name == "right" {
		return p.evalLeftRight(ctx, row, expr, name)
	}
	if name == "replace" {
		return p.evalReplace(ctx, row, expr)
	}
	if name == "translate" {
		return p.evalTranslate(ctx, row, expr)
	}
	if len(expr.Exprs) != 1 {
		return predicateValue{}, errors.Errorf("partial unique index predicate function %s expects one argument", name)
	}
	arg, err := p.evalValue(ctx, row, expr.Exprs[0])
	if err != nil {
		return predicateValue{}, err
	}
	if arg.value == nil {
		return predicateValue{}, nil
	}
	if name == "abs" {
		return predicateAbsValue(arg.value)
	}
	if name == "bit_length" {
		return predicateBitLengthValue(arg.value)
	}
	if name == "octet_length" {
		return predicateOctetLengthValue(arg.value)
	}
	text, ok := arg.value.(string)
	if !ok {
		return predicateValue{}, errors.Errorf("partial unique index predicate function %s does not support %T", name, arg.value)
	}
	switch name {
	case "char_length", "character_length", "length":
		return predicateValue{value: int64(utf8.RuneCountInString(text))}, nil
	case "lower":
		return predicateValue{value: strings.ToLower(text)}, nil
	case "upper":
		return predicateValue{value: strings.ToUpper(text)}, nil
	case "btrim":
		return predicateValue{value: strings.TrimFunc(text, func(r rune) bool { return r == ' ' })}, nil
	case "ltrim":
		return predicateValue{value: strings.TrimLeftFunc(text, func(r rune) bool { return r == ' ' })}, nil
	case "rtrim":
		return predicateValue{value: strings.TrimRightFunc(text, func(r rune) bool { return r == ' ' })}, nil
	case "md5":
		return predicateValue{value: fmt.Sprintf("%x", md5.Sum([]byte(text)))}, nil
	default:
		return predicateValue{}, errors.Errorf("partial unique index predicate function %s is not yet supported", name)
	}
}

func (p *partialIndexPredicate) evalStrpos(ctx *sql.Context, row sql.Row, expr *tree.FuncExpr) (predicateValue, error) {
	if len(expr.Exprs) != 2 {
		return predicateValue{}, errors.Errorf("partial unique index predicate function strpos expects two arguments")
	}
	str, err := p.evalValue(ctx, row, expr.Exprs[0])
	if err != nil {
		return predicateValue{}, err
	}
	substring, err := p.evalValue(ctx, row, expr.Exprs[1])
	if err != nil {
		return predicateValue{}, err
	}
	if str.value == nil || substring.value == nil {
		return predicateValue{}, nil
	}
	strText, ok := str.value.(string)
	if !ok {
		return predicateValue{}, errors.Errorf("partial unique index predicate function strpos does not support %T", str.value)
	}
	substringText, ok := substring.value.(string)
	if !ok {
		return predicateValue{}, errors.Errorf("partial unique index predicate function strpos does not support %T", substring.value)
	}
	idx := strings.Index(strText, substringText)
	if idx < 0 {
		return predicateValue{value: int64(0)}, nil
	}
	return predicateValue{value: int64(idx + 1)}, nil
}

func (p *partialIndexPredicate) evalStartsWith(ctx *sql.Context, row sql.Row, expr *tree.FuncExpr) (predicateValue, error) {
	if len(expr.Exprs) != 2 {
		return predicateValue{}, errors.Errorf("partial unique index predicate function starts_with expects two arguments")
	}
	str, err := p.evalValue(ctx, row, expr.Exprs[0])
	if err != nil {
		return predicateValue{}, err
	}
	prefix, err := p.evalValue(ctx, row, expr.Exprs[1])
	if err != nil {
		return predicateValue{}, err
	}
	if str.value == nil || prefix.value == nil {
		return predicateValue{}, nil
	}
	strText, ok := str.value.(string)
	if !ok {
		return predicateValue{}, errors.Errorf("partial unique index predicate function starts_with does not support %T", str.value)
	}
	prefixText, ok := prefix.value.(string)
	if !ok {
		return predicateValue{}, errors.Errorf("partial unique index predicate function starts_with does not support %T", prefix.value)
	}
	return predicateValue{value: strings.HasPrefix(strText, prefixText)}, nil
}

func (p *partialIndexPredicate) evalLeftRight(ctx *sql.Context, row sql.Row, expr *tree.FuncExpr, name string) (predicateValue, error) {
	if len(expr.Exprs) != 2 {
		return predicateValue{}, errors.Errorf("partial unique index predicate function %s expects two arguments", name)
	}
	str, err := p.evalValue(ctx, row, expr.Exprs[0])
	if err != nil {
		return predicateValue{}, err
	}
	count, err := p.evalValue(ctx, row, expr.Exprs[1])
	if err != nil {
		return predicateValue{}, err
	}
	if str.value == nil || count.value == nil {
		return predicateValue{}, nil
	}
	text, ok := str.value.(string)
	if !ok {
		return predicateValue{}, errors.Errorf("partial unique index predicate function %s does not support %T", name, str.value)
	}
	n, ok := predicateSignedIntegerValue(count.value)
	if !ok {
		return predicateValue{}, errors.Errorf("partial unique index predicate function %s does not support %T", name, count.value)
	}
	switch name {
	case "left":
		return predicateValue{value: predicateLeftText(text, n)}, nil
	case "right":
		return predicateValue{value: predicateRightText(text, n)}, nil
	default:
		return predicateValue{}, errors.Errorf("partial unique index predicate function %s is not yet supported", name)
	}
}

func predicateLeftText(text string, n int64) string {
	runeCount := int64(utf8.RuneCountInString(text))
	if n >= 0 {
		if n >= runeCount {
			return text
		}
		return text[:predicateByteIndexAfterRunes(text, n)]
	}
	keep := runeCount + n
	if keep <= 0 {
		return ""
	}
	return text[:predicateByteIndexAfterRunes(text, keep)]
}

func predicateRightText(text string, n int64) string {
	runeCount := int64(utf8.RuneCountInString(text))
	if n >= 0 {
		if n >= runeCount {
			return text
		}
		return text[predicateByteIndexAfterRunes(text, runeCount-n):]
	}
	if n == -1<<63 {
		return ""
	}
	skip := -n
	if skip >= runeCount {
		return ""
	}
	return text[predicateByteIndexAfterRunes(text, skip):]
}

func predicateByteIndexAfterRunes(text string, count int64) int {
	if count <= 0 {
		return 0
	}
	seen := int64(0)
	for idx := range text {
		if seen == count {
			return idx
		}
		seen++
	}
	return len(text)
}

func (p *partialIndexPredicate) evalReplace(ctx *sql.Context, row sql.Row, expr *tree.FuncExpr) (predicateValue, error) {
	if len(expr.Exprs) != 3 {
		return predicateValue{}, errors.Errorf("partial unique index predicate function replace expects three arguments")
	}
	str, err := p.evalValue(ctx, row, expr.Exprs[0])
	if err != nil {
		return predicateValue{}, err
	}
	from, err := p.evalValue(ctx, row, expr.Exprs[1])
	if err != nil {
		return predicateValue{}, err
	}
	to, err := p.evalValue(ctx, row, expr.Exprs[2])
	if err != nil {
		return predicateValue{}, err
	}
	if str.value == nil || from.value == nil || to.value == nil {
		return predicateValue{}, nil
	}
	text, ok := str.value.(string)
	if !ok {
		return predicateValue{}, errors.Errorf("partial unique index predicate function replace does not support %T", str.value)
	}
	fromText, ok := from.value.(string)
	if !ok {
		return predicateValue{}, errors.Errorf("partial unique index predicate function replace does not support %T", from.value)
	}
	toText, ok := to.value.(string)
	if !ok {
		return predicateValue{}, errors.Errorf("partial unique index predicate function replace does not support %T", to.value)
	}
	if fromText == "" {
		return predicateValue{value: text}, nil
	}
	return predicateValue{value: strings.ReplaceAll(text, fromText, toText)}, nil
}

func (p *partialIndexPredicate) evalTranslate(ctx *sql.Context, row sql.Row, expr *tree.FuncExpr) (predicateValue, error) {
	if len(expr.Exprs) != 3 {
		return predicateValue{}, errors.Errorf("partial unique index predicate function translate expects three arguments")
	}
	str, err := p.evalValue(ctx, row, expr.Exprs[0])
	if err != nil {
		return predicateValue{}, err
	}
	from, err := p.evalValue(ctx, row, expr.Exprs[1])
	if err != nil {
		return predicateValue{}, err
	}
	to, err := p.evalValue(ctx, row, expr.Exprs[2])
	if err != nil {
		return predicateValue{}, err
	}
	if str.value == nil || from.value == nil || to.value == nil {
		return predicateValue{}, nil
	}
	text, ok := str.value.(string)
	if !ok {
		return predicateValue{}, errors.Errorf("partial unique index predicate function translate does not support %T", str.value)
	}
	fromText, ok := from.value.(string)
	if !ok {
		return predicateValue{}, errors.Errorf("partial unique index predicate function translate does not support %T", from.value)
	}
	toText, ok := to.value.(string)
	if !ok {
		return predicateValue{}, errors.Errorf("partial unique index predicate function translate does not support %T", to.value)
	}
	return predicateValue{value: predicateTranslateText(text, fromText, toText)}, nil
}

func predicateTranslateText(text string, fromText string, toText string) string {
	if fromText == "" {
		return text
	}
	from := []rune(fromText)
	to := []rune(toText)
	toLen := len(to)
	fromMap := make(map[rune]int, len(from))
	for i, r := range from {
		fromMap[r] = i
	}
	translated := make([]rune, 0, utf8.RuneCountInString(text))
	for _, r := range text {
		if idx, ok := fromMap[r]; ok {
			if idx < toLen {
				translated = append(translated, to[idx])
			}
			continue
		}
		translated = append(translated, r)
	}
	return string(translated)
}

func predicateBitLengthValue(value any) (predicateValue, error) {
	switch value := value.(type) {
	case string:
		return predicateValue{value: int64(len(value)) * 8}, nil
	case []byte:
		return predicateValue{value: int64(len(value)) * 8}, nil
	default:
		return predicateValue{}, errors.Errorf("partial unique index predicate function bit_length does not support %T", value)
	}
}

func predicateOctetLengthValue(value any) (predicateValue, error) {
	switch value := value.(type) {
	case string:
		return predicateValue{value: int64(len(value))}, nil
	case []byte:
		return predicateValue{value: int64(len(value))}, nil
	default:
		return predicateValue{}, errors.Errorf("partial unique index predicate function octet_length does not support %T", value)
	}
}

func predicateAbsValue(value any) (predicateValue, error) {
	intValue, ok := predicateSignedIntegerValue(value)
	if !ok {
		return predicateValue{}, errors.Errorf("partial unique index predicate function abs does not support %T", value)
	}
	if intValue == -1<<63 {
		return predicateValue{}, errors.New("partial unique index predicate function abs overflowed int64")
	}
	if intValue < 0 {
		intValue = -intValue
	}
	return predicateValue{value: intValue}, nil
}

func predicateSignedIntegerValue(value any) (int64, bool) {
	switch value := value.(type) {
	case int:
		return int64(value), true
	case int8:
		return int64(value), true
	case int16:
		return int64(value), true
	case int32:
		return int64(value), true
	case int64:
		return value, true
	default:
		return 0, false
	}
}

func partialPredicateFunctionName(ref tree.ResolvableFunctionReference) (string, bool) {
	switch fn := ref.FunctionReference.(type) {
	case *tree.UnresolvedName:
		if fn.Star || fn.NumParts == 0 {
			return "", false
		}
		return strings.ToLower(strings.Trim(fn.Parts[0], `"`)), true
	case *tree.FunctionDefinition:
		return strings.ToLower(strings.Trim(fn.Name, `"`)), true
	default:
		return "", false
	}
}

func (p *partialIndexPredicate) columnName(name *tree.UnresolvedName) (string, error) {
	if name.Star || name.NumParts == 0 {
		return "", errors.Errorf("partial unique index predicate does not support *")
	}
	if name.NumParts > 1 && p.tableName != "" && name.Parts[1] != "" && !strings.EqualFold(name.Parts[1], p.tableName) {
		return "", errors.Errorf("partial unique index predicate table %q does not match %q", name.Parts[1], p.tableName)
	}
	return name.Parts[0], nil
}

func predicateTruthFromBool(value bool) predicateTruth {
	if value {
		return predicateTrue
	}
	return predicateFalse
}

func predicateTruthFromValue(value any) (predicateTruth, error) {
	if value == nil {
		return predicateUnknown, nil
	}
	boolValue, ok := value.(bool)
	if !ok {
		return predicateUnknown, errors.Errorf("partial unique index predicate evaluated to %T, not bool", value)
	}
	return predicateTruthFromBool(boolValue), nil
}
