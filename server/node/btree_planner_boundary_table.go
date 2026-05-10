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
	"strings"
	"unicode/utf8"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/go-mysql-server/sql"
	gmsexpression "github.com/dolthub/go-mysql-server/sql/expression"

	pgexpression "github.com/dolthub/doltgresql/server/expression"
	"github.com/dolthub/doltgresql/server/indexmetadata"
)

type BtreePlannerBoundaryTable struct {
	underlying       sql.Table
	patternOpLookup  []btreePatternOpLookup
	citextLookup     []citextBtreeLookup
	partialLookup    []partialBtreeLookup
	partialPattern   []partialBtreePatternLookup
	preserveFullRows bool
}

type btreePatternOpLookup struct {
	index      sql.Index
	columnName string
	expression string
	typ        sql.Type
}

type partialBtreePatternLookup struct {
	btreePatternOpLookup
	predicate string
}

type citextBtreeLookup struct {
	index      sql.Index
	columnName string
	expression string
	typ        sql.Type
}

var _ sql.Table = (*BtreePlannerBoundaryTable)(nil)
var _ sql.DatabaseSchemaTable = (*BtreePlannerBoundaryTable)(nil)
var _ sql.InsertableTable = (*BtreePlannerBoundaryTable)(nil)
var _ sql.ReplaceableTable = (*BtreePlannerBoundaryTable)(nil)
var _ sql.UpdatableTable = (*BtreePlannerBoundaryTable)(nil)
var _ sql.DeletableTable = (*BtreePlannerBoundaryTable)(nil)
var _ sql.IndexAddressableTable = (*BtreePlannerBoundaryTable)(nil)
var _ sql.IndexedTable = (*BtreePlannerBoundaryTable)(nil)
var _ sql.IndexSearchableTable = (*BtreePlannerBoundaryTable)(nil)
var _ sql.ProjectedTable = (*BtreePlannerBoundaryTable)(nil)

func WrapBtreePlannerBoundaryTable(ctx *sql.Context, table sql.Table) (sql.Table, bool, error) {
	if _, ok := table.(*BtreePlannerBoundaryTable); ok {
		return table, false, nil
	}
	if wrapper, ok := table.(sql.MutableTableWrapper); ok {
		wrappedUnderlying, wrapped, err := WrapBtreePlannerBoundaryTable(ctx, wrapper.Underlying())
		if err != nil || !wrapped {
			return table, wrapped, err
		}
		return wrapper.WithUnderlying(wrappedUnderlying), true, nil
	}
	indexAddressable, ok := table.(sql.IndexAddressable)
	if !ok {
		return table, false, nil
	}
	indexes, err := indexAddressable.GetIndexes(ctx)
	if err != nil {
		return table, false, err
	}
	needsWrap := false
	patternOpLookups := make([]btreePatternOpLookup, 0)
	citextLookups := make([]citextBtreeLookup, 0)
	partialLookups := make([]partialBtreeLookup, 0)
	partialPatternLookups := make([]partialBtreePatternLookup, 0)
	preserveFullRows := false
	tableSchema := table.Schema(ctx)
	for _, index := range indexes {
		if hidePlannerIndex(index) {
			needsWrap = true
			continue
		}
		if predicate := indexmetadata.Predicate(index.Comment()); predicate != "" {
			needsWrap = true
			if lookup, ok := partialBtreeIndexLookup(ctx, index, tableSchema); ok {
				partialLookups = append(partialLookups, lookup)
			}
			if lookup, ok := patternOpClassLookup(ctx, index, tableSchema); ok {
				partialPatternLookups = append(partialPatternLookups, partialBtreePatternLookup{
					btreePatternOpLookup: lookup,
					predicate:            predicate,
				})
			}
			continue
		}
		if btreeSortOptionIndex(index) {
			needsWrap = true
			preserveFullRows = true
		}
		if unsafeBtreePlannerIndex(index, tableSchema) {
			needsWrap = true
			if lookup, ok := patternOpClassLookup(ctx, index, tableSchema); ok {
				patternOpLookups = append(patternOpLookups, lookup)
			}
			if lookup, ok := citextBtreeIndexLookup(ctx, index, tableSchema); ok {
				citextLookups = append(citextLookups, lookup)
			}
			continue
		}
	}
	if !needsWrap {
		return table, false, nil
	}
	if len(patternOpLookups) == 0 && len(citextLookups) == 0 && len(partialLookups) == 0 && len(partialPatternLookups) == 0 {
		return &BtreePlannerBoundaryTable{underlying: table, preserveFullRows: preserveFullRows}, true, nil
	}
	return &BtreePlannerBoundaryTable{
		underlying:       table,
		patternOpLookup:  patternOpLookups,
		citextLookup:     citextLookups,
		partialLookup:    partialLookups,
		partialPattern:   partialPatternLookups,
		preserveFullRows: preserveFullRows,
	}, true, nil
}

func (t *BtreePlannerBoundaryTable) Name() string {
	return t.underlying.Name()
}

func (t *BtreePlannerBoundaryTable) String() string {
	return t.underlying.String()
}

func (t *BtreePlannerBoundaryTable) Schema(ctx *sql.Context) sql.Schema {
	return t.underlying.Schema(ctx)
}

func (t *BtreePlannerBoundaryTable) Collation() sql.CollationID {
	return t.underlying.Collation()
}

func (t *BtreePlannerBoundaryTable) WithProjections(ctx *sql.Context, colNames []string) (sql.Table, error) {
	if t.preserveFullRows {
		return &BtreePlannerBoundaryTable{
			underlying:       t.underlying,
			patternOpLookup:  t.patternOpLookup,
			citextLookup:     t.citextLookup,
			partialLookup:    t.partialLookup,
			partialPattern:   t.partialPattern,
			preserveFullRows: t.preserveFullRows,
		}, nil
	}
	projected, ok := t.underlying.(sql.ProjectedTable)
	if !ok {
		return nil, errors.Errorf("table %s does not support projections", t.Name())
	}
	table, err := projected.WithProjections(ctx, colNames)
	if err != nil {
		return nil, err
	}
	return &BtreePlannerBoundaryTable{
		underlying:       table,
		patternOpLookup:  t.patternOpLookup,
		citextLookup:     t.citextLookup,
		partialLookup:    t.partialLookup,
		partialPattern:   t.partialPattern,
		preserveFullRows: t.preserveFullRows,
	}, nil
}

func (t *BtreePlannerBoundaryTable) Projections() []string {
	if projected, ok := t.underlying.(sql.ProjectedTable); ok {
		return projected.Projections()
	}
	return nil
}

func (t *BtreePlannerBoundaryTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return t.underlying.Partitions(ctx)
}

func (t *BtreePlannerBoundaryTable) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	return t.underlying.PartitionRows(ctx, partition)
}

func (t *BtreePlannerBoundaryTable) DatabaseSchema() sql.DatabaseSchema {
	if schemaTable, ok := t.underlying.(sql.DatabaseSchemaTable); ok {
		return schemaTable.DatabaseSchema()
	}
	return nil
}

func (t *BtreePlannerBoundaryTable) Inserter(ctx *sql.Context) sql.RowInserter {
	insertable, ok := t.underlying.(sql.InsertableTable)
	if !ok {
		return sqlutil.NewStaticErrorEditor(planErr("table %s is not insertable", t.Name()))
	}
	return insertable.Inserter(ctx)
}

func (t *BtreePlannerBoundaryTable) Replacer(ctx *sql.Context) sql.RowReplacer {
	replaceable, ok := t.underlying.(sql.ReplaceableTable)
	if !ok {
		return sqlutil.NewStaticErrorEditor(planErr("table %s is not replaceable", t.Name()))
	}
	return replaceable.Replacer(ctx)
}

func (t *BtreePlannerBoundaryTable) Updater(ctx *sql.Context) sql.RowUpdater {
	updatable, ok := t.underlying.(sql.UpdatableTable)
	if !ok {
		return sqlutil.NewStaticErrorEditor(planErr("table %s is not updatable", t.Name()))
	}
	return updatable.Updater(ctx)
}

func (t *BtreePlannerBoundaryTable) Deleter(ctx *sql.Context) sql.RowDeleter {
	deletable, ok := t.underlying.(sql.DeletableTable)
	if !ok {
		return sqlutil.NewStaticErrorEditor(planErr("table %s is not deletable", t.Name()))
	}
	return deletable.Deleter(ctx)
}

func (t *BtreePlannerBoundaryTable) IndexedAccess(ctx *sql.Context, lookup sql.IndexLookup) sql.IndexedTable {
	unwrapped, wrappedIndex := unwrapPlannerIndexLookup(lookup)
	indexed := indexedAccessWithMutableWrappers(ctx, t.underlying, unwrapped)
	if indexed == nil {
		return nil
	}
	if !wrappedIndex {
		return indexed
	}
	return &BtreePlannerBoundaryTable{underlying: indexed}
}

func (t *BtreePlannerBoundaryTable) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	indexAddressable, ok := t.underlying.(sql.IndexAddressable)
	if !ok {
		return nil, nil
	}
	indexes, err := indexAddressable.GetIndexes(ctx)
	if err != nil {
		return nil, err
	}
	filtered := make([]sql.Index, 0, len(indexes))
	for _, index := range indexes {
		if hidePlannerIndex(index) {
			continue
		}
		if indexmetadata.Predicate(index.Comment()) != "" {
			filtered = append(filtered, partialPlannerHiddenIndex{Index: index})
			continue
		}
		if unsafeBtreePlannerIndex(index, t.Schema(ctx)) {
			continue
		}
		if nullableNullProbeIndex, ok := nullableNullProbeSortOptionPlannerIndex(ctx, index, t.Schema(ctx)); ok {
			filtered = append(filtered, nullableNullProbeIndex)
			continue
		}
		if metadataOnlySortOptionIndex(index, t.Schema(ctx)) {
			if !metadataOnlySortOptionIndexColumnsAvailable(index, t.Schema(ctx)) {
				continue
			}
			filtered = append(filtered, metadataOnlyOrderedIndex{Index: index})
			continue
		}
		filtered = append(filtered, index)
	}
	return filtered, nil
}

// hidePlannerIndex reports whether an index must not be considered by
// the planner. This covers PostgreSQL's pg_index.indisvalid=false and
// indisready=false states: an index that is mid-build (CREATE INDEX
// CONCURRENTLY) or whose backfill failed must remain in the catalog
// (so writers can keep maintaining it) but must not be used to satisfy
// query plans.
func hidePlannerIndex(index sql.Index) bool {
	comment := index.Comment()
	return !indexmetadata.IsValid(comment) || !indexmetadata.IsReady(comment)
}

func (t *BtreePlannerBoundaryTable) LookupPartitions(ctx *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	if indexedTable, ok := t.underlying.(sql.IndexedTable); ok {
		unwrapped, _ := unwrapPlannerIndexLookup(lookup)
		return indexedTable.LookupPartitions(ctx, unwrapped)
	}
	return nil, errors.Errorf("table %s is not indexed", t.Name())
}

func (t *BtreePlannerBoundaryTable) PreciseMatch() bool {
	if indexAddressable, ok := t.underlying.(sql.IndexAddressable); ok {
		return indexAddressable.PreciseMatch()
	}
	return false
}

func indexedAccessWithMutableWrappers(ctx *sql.Context, table sql.Table, lookup sql.IndexLookup) sql.IndexedTable {
	if wrapper, ok := table.(sql.MutableTableWrapper); ok {
		indexed := indexedAccessWithMutableWrappers(ctx, wrapper.Underlying(), lookup)
		if indexed == nil {
			return nil
		}
		wrapped := wrapper.WithUnderlying(indexed)
		indexedWrapped, ok := wrapped.(sql.IndexedTable)
		if !ok {
			return nil
		}
		return indexedWrapped
	}
	if indexAddressable, ok := table.(sql.IndexAddressable); ok {
		return indexAddressable.IndexedAccess(ctx, lookup)
	}
	return nil
}

func (t *BtreePlannerBoundaryTable) SkipIndexCosting() bool {
	return false
}

func (t *BtreePlannerBoundaryTable) LookupForExpressions(ctx *sql.Context, exprs ...sql.Expression) (sql.IndexLookup, *sql.FuncDepSet, sql.Expression, bool, error) {
	lookup, ok, err := t.lookupForPartialIndexes(ctx, exprs...)
	if err != nil || ok {
		return lookup, nil, gmsexpression.JoinAnd(exprs...), ok, err
	}
	lookup, ok, err = t.lookupForPartialPatternLike(ctx, exprs...)
	if err != nil || ok {
		return lookup, nil, gmsexpression.JoinAnd(exprs...), ok, err
	}
	lookup, ok, err = t.lookupForCitextComparisons(ctx, exprs...)
	if err != nil || ok {
		return lookup, nil, gmsexpression.JoinAnd(exprs...), ok, err
	}
	for _, expr := range exprs {
		lookup, ok, err := t.lookupForPatternLike(ctx, expr)
		if err != nil || !ok {
			if err != nil {
				return sql.IndexLookup{}, nil, nil, false, err
			}
			continue
		}
		return lookup, nil, gmsexpression.JoinAnd(exprs...), true, nil
	}
	return sql.IndexLookup{}, nil, nil, false, nil
}

func (t *BtreePlannerBoundaryTable) lookupForCitextComparisons(ctx *sql.Context, exprs ...sql.Expression) (sql.IndexLookup, bool, error) {
	for _, cached := range t.citextLookup {
		builder := sql.NewMySQLIndexBuilder(ctx, cached.index)
		matched := false
		for _, expr := range exprs {
			comparison, ok, err := citextLookupComparison(ctx, expr, cached.columnName)
			if err != nil {
				return sql.IndexLookup{}, false, err
			}
			if !ok {
				continue
			}
			matched = true
			switch comparison.op {
			case sql.IndexScanOpEq:
				builder.Equals(ctx, cached.expression, cached.typ, comparison.value)
			case sql.IndexScanOpGt:
				builder.GreaterThan(ctx, cached.expression, cached.typ, comparison.value)
			case sql.IndexScanOpGte:
				builder.GreaterOrEqual(ctx, cached.expression, cached.typ, comparison.value)
			case sql.IndexScanOpLt:
				builder.LessThan(ctx, cached.expression, cached.typ, comparison.value)
			case sql.IndexScanOpLte:
				builder.LessOrEqual(ctx, cached.expression, cached.typ, comparison.value)
			default:
				matched = false
			}
		}
		if !matched {
			continue
		}
		lookup, err := builder.Build(ctx)
		if err != nil {
			return sql.IndexLookup{}, false, err
		}
		if lookup.IsEmpty() {
			continue
		}
		return lookup, true, nil
	}
	return sql.IndexLookup{}, false, nil
}

func (t *BtreePlannerBoundaryTable) lookupForPatternLike(ctx *sql.Context, expr sql.Expression) (sql.IndexLookup, bool, error) {
	fieldName, prefix, upper, ok := prefixLikeLookupBounds(expr)
	if !ok {
		return sql.IndexLookup{}, false, nil
	}

	for _, cached := range t.patternOpLookup {
		if !strings.EqualFold(cached.columnName, fieldName) {
			continue
		}
		lookup, err := buildPatternLookup(ctx, cached, prefix, upper)
		if err != nil {
			return sql.IndexLookup{}, false, err
		}
		if lookup.IsEmpty() {
			continue
		}
		return lookup, true, nil
	}
	return sql.IndexLookup{}, false, nil
}

func prefixLikeLookupBounds(expr sql.Expression) (fieldName string, prefix string, upper string, ok bool) {
	expr = unwrapGMSCast(expr)
	like, ok := expr.(*gmsexpression.Like)
	if !ok {
		return "", "", "", false
	}
	field, ok := unwrapGMSCast(like.Left()).(*gmsexpression.GetField)
	if !ok {
		return "", "", "", false
	}
	patternLiteral, ok := unwrapGMSCast(like.Right()).(*gmsexpression.Literal)
	if !ok {
		return "", "", "", false
	}
	pattern, ok := patternLiteral.Value().(string)
	if !ok {
		return "", "", "", false
	}
	escape, ok := literalLikeEscape(like)
	if !ok {
		return "", "", "", false
	}
	prefix, ok = textPatternPrefix(pattern, escape)
	if !ok {
		return "", "", "", false
	}
	upper, ok = nextTextPatternPrefix(prefix)
	if !ok {
		return "", "", "", false
	}
	return field.Name(), prefix, upper, true
}

func buildPatternLookup(ctx *sql.Context, cached btreePatternOpLookup, prefix string, upper string) (sql.IndexLookup, error) {
	builder := sql.NewMySQLIndexBuilder(ctx, cached.index)
	builder.GreaterOrEqual(ctx, cached.expression, cached.typ, prefix)
	builder.LessThan(ctx, cached.expression, cached.typ, upper)
	return builder.Build(ctx)
}

type citextComparisonLookup struct {
	op    sql.IndexScanOp
	value string
}

func citextLookupComparison(ctx *sql.Context, expr sql.Expression, columnName string) (citextComparisonLookup, bool, error) {
	indexComparison, ok := unwrapGMSCast(expr).(sql.IndexComparisonExpression)
	if !ok {
		return citextComparisonLookup{}, false, nil
	}
	op, left, right, ok := indexComparison.IndexScanOperation()
	if !ok || op == sql.IndexScanOpNotEq {
		return citextComparisonLookup{}, false, nil
	}

	valueExpr := right
	if !citextLookupFieldMatches(left, columnName) {
		if !citextLookupFieldMatches(right, columnName) {
			return citextComparisonLookup{}, false, nil
		}
		op = invertIndexScanOp(op)
		valueExpr = left
	}
	value, ok, err := constantStringValue(ctx, valueExpr)
	if err != nil || !ok {
		return citextComparisonLookup{}, false, err
	}
	return citextComparisonLookup{
		op:    op,
		value: strings.ToLower(value),
	}, true, nil
}

func citextLookupFieldMatches(expr sql.Expression, columnName string) bool {
	field, ok := unwrapGMSCast(expr).(*gmsexpression.GetField)
	return ok && strings.EqualFold(field.Name(), columnName)
}

func invertIndexScanOp(op sql.IndexScanOp) sql.IndexScanOp {
	switch op {
	case sql.IndexScanOpGt:
		return sql.IndexScanOpLt
	case sql.IndexScanOpGte:
		return sql.IndexScanOpLte
	case sql.IndexScanOpLt:
		return sql.IndexScanOpGt
	case sql.IndexScanOpLte:
		return sql.IndexScanOpGte
	default:
		return op
	}
}

func constantStringValue(ctx *sql.Context, expr sql.Expression) (string, bool, error) {
	if !expr.Resolved() {
		return "", false, nil
	}
	value, err := expr.Eval(ctx, nil)
	if err != nil || value == nil {
		return "", false, err
	}
	value, err = sql.UnwrapAny(ctx, value)
	if err != nil || value == nil {
		return "", false, err
	}
	str, ok := value.(string)
	return str, ok, nil
}

func patternOpClassLookup(ctx *sql.Context, index sql.Index, tableSchema sql.Schema) (btreePatternOpLookup, bool) {
	if indexmetadata.AccessMethod(index.IndexType(), index.Comment()) != indexmetadata.AccessMethodBtree {
		return btreePatternOpLookup{}, false
	}
	logicalColumns := indexmetadata.LogicalColumns(index, tableSchema)
	columnTypes := index.ColumnExpressionTypes(ctx)
	opClasses := indexmetadata.OpClassesForSchema(index, tableSchema)
	for i, opClass := range opClasses {
		if i != 0 || !isBtreePatternOpClass(opClass) || i >= len(logicalColumns) || i >= len(columnTypes) {
			continue
		}
		logicalColumn := logicalColumns[i]
		if logicalColumn.Expression {
			continue
		}
		return btreePatternOpLookup{
			index:      index,
			columnName: logicalColumn.StorageName,
			expression: columnTypes[i].Expression,
			typ:        columnTypes[i].Type,
		}, true
	}
	return btreePatternOpLookup{}, false
}

func citextBtreeIndexLookup(ctx *sql.Context, index sql.Index, tableSchema sql.Schema) (citextBtreeLookup, bool) {
	if indexmetadata.AccessMethod(index.IndexType(), index.Comment()) != indexmetadata.AccessMethodBtree {
		return citextBtreeLookup{}, false
	}
	logicalColumns := indexmetadata.LogicalColumns(index, tableSchema)
	columnTypes := index.ColumnExpressionTypes(ctx)
	opClasses := indexmetadata.OpClassesForSchema(index, tableSchema)
	for i, opClass := range opClasses {
		if indexmetadata.NormalizeOpClass(opClass) != indexmetadata.OpClassCitextOps ||
			i >= len(logicalColumns) ||
			i >= len(columnTypes) {
			continue
		}
		logicalColumn := logicalColumns[i]
		if logicalColumn.Expression {
			continue
		}
		columnIndex := tableSchema.IndexOfColName(logicalColumn.StorageName)
		if columnIndex < 0 || !isCitextType(tableSchema[columnIndex].Type) {
			continue
		}
		expression := columnTypes[i].Expression
		if !strings.Contains(strings.ToLower(citextPhysicalExpression(expression, tableSchema)), "lower(") {
			continue
		}
		return citextBtreeLookup{
			index:      index,
			columnName: logicalColumn.StorageName,
			expression: expression,
			typ:        columnTypes[i].Type,
		}, true
	}
	return citextBtreeLookup{}, false
}

func citextPhysicalExpression(indexExpression string, tableSchema sql.Schema) string {
	columnName := strings.Trim(strings.TrimSpace(indexExpression), "`\"")
	columnIndex := tableSchema.IndexOfColName(columnName)
	if columnIndex < 0 {
		if dot := strings.LastIndex(columnName, "."); dot >= 0 {
			columnIndex = tableSchema.IndexOfColName(columnName[dot+1:])
		}
	}
	if columnIndex < 0 {
		return indexExpression
	}
	column := tableSchema[columnIndex]
	if !column.HiddenSystem || column.Generated == nil || column.Generated.Expr == nil {
		return indexExpression
	}
	return column.Generated.Expr.String()
}

func unsafeBtreePlannerIndex(index sql.Index, tableSchema sql.Schema) bool {
	metadata, ok := indexmetadata.DecodeComment(index.Comment())
	if !ok {
		metadata = indexmetadata.Metadata{}
	}
	if indexmetadata.AccessMethod(index.IndexType(), index.Comment()) != indexmetadata.AccessMethodBtree {
		return false
	}
	for _, column := range indexmetadata.LogicalColumns(index, tableSchema) {
		if column.Expression {
			continue
		}
		columnIndex := tableSchema.IndexOfColName(column.StorageName)
		if columnIndex < 0 {
			continue
		}
		if isCitextType(tableSchema[columnIndex].Type) {
			return true
		}
	}
	for _, collation := range metadata.Collations {
		if strings.TrimSpace(collation) != "" {
			return true
		}
	}
	for _, opClass := range metadata.OpClasses {
		if isBtreePatternOpClass(opClass) {
			return true
		}
	}
	return false
}

func isBtreePatternOpClass(opClass string) bool {
	switch indexmetadata.NormalizeOpClass(opClass) {
	case indexmetadata.OpClassTextPatternOps, indexmetadata.OpClassVarcharPatternOps, indexmetadata.OpClassBpcharPatternOps:
		return true
	default:
		return false
	}
}

func unwrapGMSCast(expr sql.Expression) sql.Expression {
	for {
		cast, ok := expr.(*pgexpression.GMSCast)
		if !ok {
			return expr
		}
		expr = cast.Child()
	}
}

func literalLikeEscape(like *gmsexpression.Like) (byte, bool) {
	if like.Escape == nil {
		return '\\', true
	}
	escapeLiteral, ok := unwrapGMSCast(like.Escape).(*gmsexpression.Literal)
	if !ok {
		return 0, false
	}
	escape, ok := escapeLiteral.Value().(string)
	if !ok || len(escape) != 1 || escape[0] >= utf8.RuneSelf {
		return 0, false
	}
	return escape[0], true
}

func textPatternPrefix(pattern string, escape byte) (string, bool) {
	var prefix strings.Builder
	escaped := false
	for i := 0; i < len(pattern); i++ {
		ch := pattern[i]
		if escaped {
			prefix.WriteByte(ch)
			escaped = false
			continue
		}
		if ch == escape {
			escaped = true
			continue
		}
		switch ch {
		case '%':
			if i != len(pattern)-1 || prefix.Len() == 0 {
				return "", false
			}
			return prefix.String(), true
		case '_':
			return "", false
		default:
			if ch >= utf8.RuneSelf {
				return "", false
			}
			prefix.WriteByte(ch)
		}
	}
	return "", false
}

func nextTextPatternPrefix(prefix string) (string, bool) {
	if prefix == "" {
		return "", false
	}
	upper := []byte(prefix)
	for i := len(upper) - 1; i >= 0; i-- {
		if upper[i] < utf8.RuneSelf-1 {
			upper[i]++
			return string(upper[:i+1]), true
		}
	}
	return "", false
}
