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
	"sort"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
	"github.com/dolthub/doltgresql/server/indexmetadata"
	"github.com/dolthub/doltgresql/server/tablemetadata"
)

type temporalWithoutOverlapsCheck struct {
	name                  string
	equalityColumnIndexes []int
	equalityColumnTypes   []sql.Type
	periodColumnIndex     int
}

type temporalForeignKeyCheck struct {
	name               string
	columnIndexes      []int
	columnTypes        []sql.Type
	periodColumnIndex  int
	parentSchema       string
	parentTable        string
	parentColumns      []string
	parentPeriodColumn string
}

// TemporalConstraintTable enforces PostgreSQL temporal UNIQUE WITHOUT
// OVERLAPS and PERIOD foreign-key constraints around native Dolt writes.
type TemporalConstraintTable struct {
	underlying  sql.Table
	uniques     []temporalWithoutOverlapsCheck
	foreignKeys []temporalForeignKeyCheck
}

var _ sql.TableWrapper = (*TemporalConstraintTable)(nil)
var _ sql.MutableTableWrapper = (*TemporalConstraintTable)(nil)
var _ sql.InsertableTable = (*TemporalConstraintTable)(nil)
var _ sql.ReplaceableTable = (*TemporalConstraintTable)(nil)
var _ sql.UpdatableTable = (*TemporalConstraintTable)(nil)
var _ sql.IndexAddressable = (*TemporalConstraintTable)(nil)
var _ sql.IndexedTable = (*TemporalConstraintTable)(nil)

// WrapTemporalConstraintTable wraps table when Doltgres metadata describes
// temporal constraints that native Dolt/GMS cannot enforce directly.
func WrapTemporalConstraintTable(ctx *sql.Context, table sql.Table) (sql.Table, bool, error) {
	if _, ok := table.(*TemporalConstraintTable); ok {
		return table, false, nil
	}
	uniques, err := temporalWithoutOverlapsChecks(ctx, table)
	if err != nil {
		return nil, false, err
	}
	foreignKeys, err := temporalForeignKeyChecks(ctx, table)
	if err != nil {
		return nil, false, err
	}
	if len(uniques) == 0 && len(foreignKeys) == 0 {
		return table, false, nil
	}
	return &TemporalConstraintTable{
		underlying:  table,
		uniques:     uniques,
		foreignKeys: foreignKeys,
	}, true, nil
}

func temporalWithoutOverlapsChecks(ctx *sql.Context, table sql.Table) ([]temporalWithoutOverlapsCheck, error) {
	indexAddressable, ok := table.(sql.IndexAddressable)
	if !ok {
		return nil, nil
	}
	indexes, err := indexAddressable.GetIndexes(ctx)
	if err != nil {
		return nil, err
	}
	tableSchema := table.Schema(ctx)
	commentTable, _ := temporalCommentedTable(table)
	checks := make([]temporalWithoutOverlapsCheck, 0)
	for _, index := range indexes {
		comment := indexmetadata.CommentForTable(index, commentTable)
		periodColumn := indexmetadata.WithoutOverlapsColumn(comment)
		if periodColumn == "" {
			continue
		}
		periodIdx := tableSchema.IndexOfColName(periodColumn)
		if periodIdx < 0 {
			return nil, sql.ErrKeyColumnDoesNotExist.New(periodColumn)
		}
		logicalColumns := indexmetadata.LogicalColumns(index, tableSchema)
		check := temporalWithoutOverlapsCheck{
			name:              indexmetadata.DisplayNameForTable(index, commentTable),
			periodColumnIndex: periodIdx,
		}
		columnTypes := index.ColumnExpressionTypes(ctx)
		for i, column := range logicalColumns {
			if column.Expression {
				return nil, errors.Errorf("WITHOUT OVERLAPS expression indexes are not yet supported")
			}
			if strings.EqualFold(column.StorageName, periodColumn) {
				continue
			}
			colIdx := tableSchema.IndexOfColName(column.StorageName)
			if colIdx < 0 {
				return nil, sql.ErrKeyColumnDoesNotExist.New(column.StorageName)
			}
			check.equalityColumnIndexes = append(check.equalityColumnIndexes, colIdx)
			if i < len(columnTypes) && columnTypes[i].Type != nil {
				check.equalityColumnTypes = append(check.equalityColumnTypes, columnTypes[i].Type)
			} else {
				check.equalityColumnTypes = append(check.equalityColumnTypes, tableSchema[colIdx].Type)
			}
		}
		checks = append(checks, check)
	}
	return checks, nil
}

func temporalForeignKeyChecks(ctx *sql.Context, table sql.Table) ([]temporalForeignKeyCheck, error) {
	_, comment := temporalCommentedTable(table)
	if comment == "" {
		return nil, nil
	}
	foreignKeys := tablemetadata.TemporalForeignKeys(comment)
	if len(foreignKeys) == 0 {
		return nil, nil
	}
	tableSchema := table.Schema(ctx)
	checks := make([]temporalForeignKeyCheck, 0, len(foreignKeys))
	for _, fk := range foreignKeys {
		periodIdx := tableSchema.IndexOfColName(fk.PeriodColumn)
		if periodIdx < 0 {
			return nil, sql.ErrKeyColumnDoesNotExist.New(fk.PeriodColumn)
		}
		check := temporalForeignKeyCheck{
			name:               fk.Name,
			periodColumnIndex:  periodIdx,
			parentSchema:       fk.ParentSchema,
			parentTable:        fk.ParentTable,
			parentColumns:      append([]string(nil), fk.ParentColumns...),
			parentPeriodColumn: fk.ParentPeriodColumn,
		}
		for _, column := range fk.Columns {
			colIdx := tableSchema.IndexOfColName(column)
			if colIdx < 0 {
				return nil, sql.ErrKeyColumnDoesNotExist.New(column)
			}
			check.columnIndexes = append(check.columnIndexes, colIdx)
			check.columnTypes = append(check.columnTypes, tableSchema[colIdx].Type)
		}
		checks = append(checks, check)
	}
	return checks, nil
}

func temporalCommentedTable(table sql.Table) (sql.Table, string) {
	for table != nil {
		if commented, ok := table.(sql.CommentedTable); ok {
			return table, commented.Comment()
		}
		wrapper, ok := table.(sql.TableWrapper)
		if !ok {
			break
		}
		table = wrapper.Underlying()
	}
	return table, ""
}

func (t *TemporalConstraintTable) Underlying() sql.Table {
	return t.underlying
}

func (t *TemporalConstraintTable) WithUnderlying(table sql.Table) sql.Table {
	return &TemporalConstraintTable{
		underlying:  table,
		uniques:     t.uniques,
		foreignKeys: t.foreignKeys,
	}
}

func (t *TemporalConstraintTable) Name() string {
	return t.underlying.Name()
}

func (t *TemporalConstraintTable) String() string {
	return t.underlying.String()
}

func (t *TemporalConstraintTable) Schema(ctx *sql.Context) sql.Schema {
	return t.underlying.Schema(ctx)
}

func (t *TemporalConstraintTable) Collation() sql.CollationID {
	return t.underlying.Collation()
}

func (t *TemporalConstraintTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return t.underlying.Partitions(ctx)
}

func (t *TemporalConstraintTable) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	return t.underlying.PartitionRows(ctx, partition)
}

func (t *TemporalConstraintTable) DatabaseSchema() sql.DatabaseSchema {
	if schemaTable, ok := t.underlying.(sql.DatabaseSchemaTable); ok {
		return schemaTable.DatabaseSchema()
	}
	return nil
}

func (t *TemporalConstraintTable) IndexedAccess(ctx *sql.Context, lookup sql.IndexLookup) sql.IndexedTable {
	if indexAddressable, ok := t.underlying.(sql.IndexAddressable); ok {
		return indexAddressable.IndexedAccess(ctx, lookup)
	}
	return nil
}

func (t *TemporalConstraintTable) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	if indexAddressable, ok := t.underlying.(sql.IndexAddressable); ok {
		return indexAddressable.GetIndexes(ctx)
	}
	return nil, nil
}

func (t *TemporalConstraintTable) LookupPartitions(ctx *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	if indexedTable, ok := t.underlying.(sql.IndexedTable); ok {
		return indexedTable.LookupPartitions(ctx, lookup)
	}
	return nil, errors.Errorf("table %s is not indexed", t.Name())
}

func (t *TemporalConstraintTable) PreciseMatch() bool {
	if indexAddressable, ok := t.underlying.(sql.IndexAddressable); ok {
		return indexAddressable.PreciseMatch()
	}
	return false
}

func (t *TemporalConstraintTable) Inserter(ctx *sql.Context) sql.RowInserter {
	insertable, ok := t.underlying.(sql.InsertableTable)
	if !ok {
		return sqlutil.NewStaticErrorEditor(planErr("table %s is not insertable", t.Name()))
	}
	return &temporalConstraintEditor{table: t, primary: insertable.Inserter(ctx)}
}

func (t *TemporalConstraintTable) Replacer(ctx *sql.Context) sql.RowReplacer {
	replaceable, ok := t.underlying.(sql.ReplaceableTable)
	if !ok {
		return sqlutil.NewStaticErrorEditor(planErr("table %s is not replaceable", t.Name()))
	}
	return &temporalConstraintEditor{table: t, primary: replaceable.Replacer(ctx)}
}

func (t *TemporalConstraintTable) Updater(ctx *sql.Context) sql.RowUpdater {
	updatable, ok := t.underlying.(sql.UpdatableTable)
	if !ok {
		return sqlutil.NewStaticErrorEditor(planErr("table %s is not updatable", t.Name()))
	}
	return &temporalConstraintEditor{table: t, primary: updatable.Updater(ctx)}
}

type temporalPrimaryEditor interface {
	sql.EditOpenerCloser
	Close(*sql.Context) error
}

type temporalConstraintEditor struct {
	table       *TemporalConstraintTable
	primary     temporalPrimaryEditor
	pendingRows []sql.Row
	removedRows []sql.Row
}

var _ sql.TableEditor = (*temporalConstraintEditor)(nil)

func (e *temporalConstraintEditor) StatementBegin(ctx *sql.Context) {
	e.pendingRows = nil
	e.removedRows = nil
	e.primary.StatementBegin(ctx)
}

func (e *temporalConstraintEditor) DiscardChanges(ctx *sql.Context, err error) error {
	e.pendingRows = nil
	e.removedRows = nil
	return e.primary.DiscardChanges(ctx, err)
}

func (e *temporalConstraintEditor) StatementComplete(ctx *sql.Context) error {
	err := e.primary.StatementComplete(ctx)
	e.pendingRows = nil
	e.removedRows = nil
	return err
}

func (e *temporalConstraintEditor) Insert(ctx *sql.Context, row sql.Row) error {
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
	e.pendingRows = append(e.pendingRows, row)
	return nil
}

func (e *temporalConstraintEditor) Update(ctx *sql.Context, oldRow sql.Row, newRow sql.Row) error {
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
	e.pendingRows = append(e.pendingRows, newRow)
	return nil
}

func (e *temporalConstraintEditor) Delete(ctx *sql.Context, row sql.Row) error {
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

func (e *temporalConstraintEditor) Close(ctx *sql.Context) error {
	err := e.primary.Close(ctx)
	e.pendingRows = nil
	e.removedRows = nil
	return err
}

func (e *temporalConstraintEditor) validateRow(ctx *sql.Context, row sql.Row, oldRow sql.Row) error {
	for _, check := range e.table.uniques {
		if err := e.validateWithoutOverlaps(ctx, check, row, oldRow); err != nil {
			return err
		}
	}
	for _, check := range e.table.foreignKeys {
		if err := check.validate(ctx, e.table.Name(), row); err != nil {
			return err
		}
	}
	return nil
}

func (e *temporalConstraintEditor) validateWithoutOverlaps(ctx *sql.Context, check temporalWithoutOverlapsCheck, row sql.Row, oldRow sql.Row) error {
	key, hasNull := check.key(row)
	if hasNull {
		return nil
	}
	candidate, ok, err := parseTemporalRangeValue(rowValue(row, check.periodColumnIndex))
	if err != nil || !ok || candidate.empty {
		return err
	}
	for _, pending := range e.pendingRows {
		matches, err := check.rowMatchesKey(ctx, pending, key)
		if err != nil {
			return err
		}
		if !matches {
			continue
		}
		pendingRange, ok, err := parseTemporalRangeValue(rowValue(pending, check.periodColumnIndex))
		if err != nil {
			return err
		}
		if ok && temporalRangesOverlap(candidate, pendingRange) {
			return temporalExclusionViolation(check.name)
		}
	}
	duplicate, err := firstOverlappingTemporalRow(ctx, scanTableForNullsNotDistinctCheck(e.table.underlying), check, key, candidate, oldRow, e.removedRows)
	if err != nil {
		return err
	}
	if duplicate != nil {
		return temporalExclusionViolation(check.name)
	}
	return nil
}

func (c temporalWithoutOverlapsCheck) key(row sql.Row) (sql.Row, bool) {
	key := make(sql.Row, len(c.equalityColumnIndexes))
	for i, columnIdx := range c.equalityColumnIndexes {
		value := rowValue(row, columnIdx)
		if value == nil {
			return nil, true
		}
		key[i] = value
	}
	return key, false
}

func (c temporalWithoutOverlapsCheck) rowMatchesKey(ctx *sql.Context, row sql.Row, key sql.Row) (bool, error) {
	if len(key) != len(c.equalityColumnIndexes) {
		return false, nil
	}
	for i, columnIdx := range c.equalityColumnIndexes {
		value := rowValue(row, columnIdx)
		if value == nil {
			return false, nil
		}
		if i < len(c.equalityColumnTypes) && c.equalityColumnTypes[i] != nil {
			cmp, err := c.equalityColumnTypes[i].Compare(ctx, value, key[i])
			if err != nil || cmp != 0 {
				return false, err
			}
			continue
		}
		if !reflect.DeepEqual(value, key[i]) {
			return false, nil
		}
	}
	return true, nil
}

func firstOverlappingTemporalRow(ctx *sql.Context, table sql.Table, check temporalWithoutOverlapsCheck, key sql.Row, candidate temporalRange, oldRow sql.Row, ignoreRows []sql.Row) (sql.Row, error) {
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
		duplicate, err := nextOverlappingTemporalRow(ctx, rows, check, key, candidate, oldRow, ignoreRows)
		if closeErr := rows.Close(ctx); err == nil {
			err = closeErr
		}
		if err != nil || duplicate != nil {
			return duplicate, err
		}
	}
}

func nextOverlappingTemporalRow(ctx *sql.Context, rows sql.RowIter, check temporalWithoutOverlapsCheck, key sql.Row, candidate temporalRange, oldRow sql.Row, ignoreRows []sql.Row) (sql.Row, error) {
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
		if shouldIgnoreNullsNotDistinctRow(row, ignoreRows) {
			continue
		}
		matches, err := check.rowMatchesKey(ctx, row, key)
		if err != nil {
			return nil, err
		}
		if !matches {
			continue
		}
		existingRange, ok, err := parseTemporalRangeValue(rowValue(row, check.periodColumnIndex))
		if err != nil {
			return nil, err
		}
		if ok && temporalRangesOverlap(candidate, existingRange) {
			return row, nil
		}
	}
}

func (c temporalForeignKeyCheck) validate(ctx *sql.Context, childTable string, row sql.Row) error {
	key, hasNull := c.key(row)
	if hasNull {
		return nil
	}
	childPeriod, ok, err := parseTemporalRangeValue(rowValue(row, c.periodColumnIndex))
	if err != nil || !ok || childPeriod.empty {
		return err
	}
	parent, err := core.GetSqlTableFromContext(ctx, "", doltdb.TableName{Name: c.parentTable, Schema: c.parentSchema})
	if err != nil {
		return err
	}
	if parent == nil {
		return c.violation(childTable)
	}
	covered, err := c.parentCovers(ctx, parent, key, childPeriod)
	if err != nil {
		return err
	}
	if !covered {
		return c.violation(childTable)
	}
	return nil
}

func (c temporalForeignKeyCheck) key(row sql.Row) (sql.Row, bool) {
	key := make(sql.Row, len(c.columnIndexes))
	for i, columnIdx := range c.columnIndexes {
		value := rowValue(row, columnIdx)
		if value == nil {
			return nil, true
		}
		key[i] = value
	}
	if rowValue(row, c.periodColumnIndex) == nil {
		return nil, true
	}
	return key, false
}

func (c temporalForeignKeyCheck) parentCovers(ctx *sql.Context, parent sql.Table, key sql.Row, childPeriod temporalRange) (bool, error) {
	parentSchema := parent.Schema(ctx)
	parentPeriodIdx := parentSchema.IndexOfColName(c.parentPeriodColumn)
	if parentPeriodIdx < 0 {
		return false, sql.ErrKeyColumnDoesNotExist.New(c.parentPeriodColumn)
	}
	parentColumnIndexes := make([]int, len(c.parentColumns))
	parentColumnTypes := make([]sql.Type, len(c.parentColumns))
	for i, column := range c.parentColumns {
		colIdx := parentSchema.IndexOfColName(column)
		if colIdx < 0 {
			return false, sql.ErrKeyColumnDoesNotExist.New(column)
		}
		parentColumnIndexes[i] = colIdx
		parentColumnTypes[i] = parentSchema[colIdx].Type
	}
	ranges, err := matchingParentTemporalRanges(ctx, scanTableForNullsNotDistinctCheck(parent), parentColumnIndexes, parentColumnTypes, parentPeriodIdx, key)
	if err != nil {
		return false, err
	}
	return temporalRangesCover(childPeriod, ranges), nil
}

func (c temporalForeignKeyCheck) violation(childTable string) error {
	return pgerror.Newf(pgcode.ForeignKeyViolation,
		`insert or update on table "%s" violates foreign key constraint "%s"`, childTable, c.name)
}

func matchingParentTemporalRanges(ctx *sql.Context, parent sql.Table, columnIndexes []int, columnTypes []sql.Type, periodIdx int, key sql.Row) ([]temporalRange, error) {
	partitions, err := parent.Partitions(ctx)
	if err != nil {
		return nil, err
	}
	defer partitions.Close(ctx)
	ranges := make([]temporalRange, 0)
	for {
		partition, err := partitions.Next(ctx)
		if err == io.EOF {
			return ranges, nil
		}
		if err != nil {
			return nil, err
		}
		rows, err := parent.PartitionRows(ctx, partition)
		if err != nil {
			return nil, err
		}
		for {
			row, err := rows.Next(ctx)
			if err == io.EOF {
				break
			}
			if err != nil {
				_ = rows.Close(ctx)
				return nil, err
			}
			matches, err := temporalRowMatchesKey(ctx, row, columnIndexes, columnTypes, key)
			if err != nil {
				_ = rows.Close(ctx)
				return nil, err
			}
			if !matches {
				continue
			}
			period, ok, err := parseTemporalRangeValue(rowValue(row, periodIdx))
			if err != nil {
				_ = rows.Close(ctx)
				return nil, err
			}
			if ok && !period.empty {
				ranges = append(ranges, period)
			}
		}
		if err := rows.Close(ctx); err != nil {
			return nil, err
		}
	}
}

func temporalRowMatchesKey(ctx *sql.Context, row sql.Row, columnIndexes []int, columnTypes []sql.Type, key sql.Row) (bool, error) {
	if len(columnIndexes) != len(key) {
		return false, nil
	}
	for i, columnIdx := range columnIndexes {
		value := rowValue(row, columnIdx)
		if value == nil {
			return false, nil
		}
		if i < len(columnTypes) && columnTypes[i] != nil {
			cmp, err := columnTypes[i].Compare(ctx, value, key[i])
			if err != nil || cmp != 0 {
				return false, err
			}
			continue
		}
		if !reflect.DeepEqual(value, key[i]) {
			return false, nil
		}
	}
	return true, nil
}

type temporalRange struct {
	lower          string
	upper          string
	hasLower       bool
	hasUpper       bool
	lowerInclusive bool
	upperInclusive bool
	empty          bool
}

func parseTemporalRangeValue(value any) (temporalRange, bool, error) {
	if value == nil {
		return temporalRange{}, false, nil
	}
	raw := strings.TrimSpace(fmt.Sprint(value))
	if raw == "" {
		return temporalRange{}, false, nil
	}
	if strings.EqualFold(raw, "empty") {
		return temporalRange{empty: true}, true, nil
	}
	if len(raw) < 3 {
		return temporalRange{}, false, errors.Errorf("invalid range value %q", raw)
	}
	lowerBound := raw[0]
	upperBound := raw[len(raw)-1]
	if (lowerBound != '[' && lowerBound != '(') || (upperBound != ']' && upperBound != ')') {
		return temporalRange{}, false, errors.Errorf("invalid range value %q", raw)
	}
	body := raw[1 : len(raw)-1]
	parts := strings.SplitN(body, ",", 2)
	if len(parts) != 2 {
		return temporalRange{}, false, errors.Errorf("invalid range value %q", raw)
	}
	lower := cleanTemporalRangeBound(parts[0])
	upper := cleanTemporalRangeBound(parts[1])
	return temporalRange{
		lower:          lower,
		upper:          upper,
		hasLower:       lower != "",
		hasUpper:       upper != "",
		lowerInclusive: lowerBound == '[',
		upperInclusive: upperBound == ']',
	}, true, nil
}

func cleanTemporalRangeBound(bound string) string {
	return strings.Trim(strings.TrimSpace(bound), `"`)
}

func temporalRangesOverlap(left temporalRange, right temporalRange) bool {
	if left.empty || right.empty {
		return false
	}
	return temporalLowerBeforeUpper(left, right) && temporalLowerBeforeUpper(right, left)
}

func temporalLowerBeforeUpper(left temporalRange, right temporalRange) bool {
	if !left.hasLower || !right.hasUpper {
		return true
	}
	cmp := strings.Compare(left.lower, right.upper)
	if cmp < 0 {
		return true
	}
	if cmp > 0 {
		return false
	}
	return left.lowerInclusive && right.upperInclusive
}

func temporalRangesCover(target temporalRange, ranges []temporalRange) bool {
	if target.empty {
		return true
	}
	if len(ranges) == 0 {
		return false
	}
	sort.Slice(ranges, func(i, j int) bool {
		return temporalLowerLess(ranges[i], ranges[j])
	})
	started := false
	var covered temporalRange
	for _, candidate := range ranges {
		if candidate.empty {
			continue
		}
		if !started {
			if temporalRangeStartsAfter(candidate, target.lower, target.hasLower) {
				continue
			}
			covered = candidate
			started = true
		} else {
			if temporalRangeStartsAfter(candidate, covered.upper, covered.hasUpper) {
				return false
			}
			if temporalUpperGreater(candidate, covered) {
				covered.upper = candidate.upper
				covered.hasUpper = candidate.hasUpper
				covered.upperInclusive = candidate.upperInclusive
			}
		}
		if temporalUpperCovers(covered, target) {
			return true
		}
	}
	return !target.hasUpper && started && !covered.hasUpper
}

func temporalLowerLess(left temporalRange, right temporalRange) bool {
	if !left.hasLower {
		return right.hasLower
	}
	if !right.hasLower {
		return false
	}
	cmp := strings.Compare(left.lower, right.lower)
	if cmp != 0 {
		return cmp < 0
	}
	return left.lowerInclusive && !right.lowerInclusive
}

func temporalRangeStartsAfter(candidate temporalRange, point string, hasPoint bool) bool {
	if !hasPoint {
		return candidate.hasLower
	}
	if !candidate.hasLower {
		return false
	}
	return strings.Compare(candidate.lower, point) > 0
}

func temporalUpperGreater(candidate temporalRange, covered temporalRange) bool {
	if !candidate.hasUpper {
		return covered.hasUpper
	}
	if !covered.hasUpper {
		return false
	}
	cmp := strings.Compare(candidate.upper, covered.upper)
	if cmp != 0 {
		return cmp > 0
	}
	return candidate.upperInclusive && !covered.upperInclusive
}

func temporalUpperCovers(covered temporalRange, target temporalRange) bool {
	if !target.hasUpper {
		return !covered.hasUpper
	}
	if !covered.hasUpper {
		return true
	}
	cmp := strings.Compare(covered.upper, target.upper)
	if cmp != 0 {
		return cmp > 0
	}
	return covered.upperInclusive || !target.upperInclusive
}

func temporalExclusionViolation(name string) error {
	return pgerror.Newf(pgcode.ExclusionViolation,
		`conflicting key value violates exclusion constraint "%s"`, name)
}

func rowValue(row sql.Row, index int) any {
	if index < 0 || index >= len(row) {
		return nil
	}
	return row[index]
}
