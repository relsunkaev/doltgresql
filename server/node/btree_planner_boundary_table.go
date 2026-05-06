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
	"github.com/dolthub/go-mysql-server/sql"
	gmsexpression "github.com/dolthub/go-mysql-server/sql/expression"

	pgexpression "github.com/dolthub/doltgresql/server/expression"
	"github.com/dolthub/doltgresql/server/indexmetadata"
)

type BtreePlannerBoundaryTable struct {
	underlying      sql.Table
	patternOpLookup []btreePatternOpLookup
}

type btreePatternOpLookup struct {
	index      sql.Index
	columnName string
	expression string
	typ        sql.Type
}

var _ sql.Table = (*BtreePlannerBoundaryTable)(nil)
var _ sql.DatabaseSchemaTable = (*BtreePlannerBoundaryTable)(nil)
var _ sql.IndexAddressableTable = (*BtreePlannerBoundaryTable)(nil)
var _ sql.IndexedTable = (*BtreePlannerBoundaryTable)(nil)
var _ sql.IndexSearchableTable = (*BtreePlannerBoundaryTable)(nil)
var _ sql.ProjectedTable = (*BtreePlannerBoundaryTable)(nil)

func WrapBtreePlannerBoundaryTable(ctx *sql.Context, table sql.Table) (sql.Table, bool, error) {
	if _, ok := table.(*BtreePlannerBoundaryTable); ok {
		return table, false, nil
	}
	indexAddressable, ok := table.(sql.IndexAddressable)
	if !ok {
		return table, false, nil
	}
	indexes, err := indexAddressable.GetIndexes(ctx)
	if err != nil {
		return table, false, err
	}
	hasUnsafeIndex := false
	patternOpLookups := make([]btreePatternOpLookup, 0)
	for _, index := range indexes {
		if unsafeBtreePlannerIndex(index) {
			hasUnsafeIndex = true
			if lookup, ok := patternOpClassLookup(ctx, index, table.Schema(ctx)); ok {
				patternOpLookups = append(patternOpLookups, lookup)
			}
			continue
		}
	}
	if !hasUnsafeIndex {
		return table, false, nil
	}
	if len(patternOpLookups) == 0 {
		return &BtreePlannerBoundaryTable{underlying: table}, true, nil
	}
	return &BtreePlannerBoundaryTable{underlying: table, patternOpLookup: patternOpLookups}, true, nil
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
	projected, ok := t.underlying.(sql.ProjectedTable)
	if !ok {
		return nil, errors.Errorf("table %s does not support projections", t.Name())
	}
	table, err := projected.WithProjections(ctx, colNames)
	if err != nil {
		return nil, err
	}
	return &BtreePlannerBoundaryTable{underlying: table, patternOpLookup: t.patternOpLookup}, nil
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

func (t *BtreePlannerBoundaryTable) IndexedAccess(ctx *sql.Context, lookup sql.IndexLookup) sql.IndexedTable {
	if indexAddressable, ok := t.underlying.(sql.IndexAddressable); ok {
		return indexAddressable.IndexedAccess(ctx, lookup)
	}
	return nil
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
		if !unsafeBtreePlannerIndex(index) {
			filtered = append(filtered, index)
		}
	}
	return filtered, nil
}

func (t *BtreePlannerBoundaryTable) LookupPartitions(ctx *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	if indexedTable, ok := t.underlying.(sql.IndexedTable); ok {
		return indexedTable.LookupPartitions(ctx, lookup)
	}
	return nil, errors.Errorf("table %s is not indexed", t.Name())
}

func (t *BtreePlannerBoundaryTable) PreciseMatch() bool {
	if indexAddressable, ok := t.underlying.(sql.IndexAddressable); ok {
		return indexAddressable.PreciseMatch()
	}
	return false
}

func (t *BtreePlannerBoundaryTable) SkipIndexCosting() bool {
	return false
}

func (t *BtreePlannerBoundaryTable) LookupForExpressions(ctx *sql.Context, exprs ...sql.Expression) (sql.IndexLookup, *sql.FuncDepSet, sql.Expression, bool, error) {
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

func (t *BtreePlannerBoundaryTable) lookupForPatternLike(ctx *sql.Context, expr sql.Expression) (sql.IndexLookup, bool, error) {
	expr = unwrapGMSCast(expr)
	like, ok := expr.(*gmsexpression.Like)
	if !ok {
		return sql.IndexLookup{}, false, nil
	}
	field, ok := unwrapGMSCast(like.Left()).(*gmsexpression.GetField)
	if !ok {
		return sql.IndexLookup{}, false, nil
	}
	patternLiteral, ok := unwrapGMSCast(like.Right()).(*gmsexpression.Literal)
	if !ok {
		return sql.IndexLookup{}, false, nil
	}
	pattern, ok := patternLiteral.Value().(string)
	if !ok {
		return sql.IndexLookup{}, false, nil
	}
	escape, ok := literalLikeEscape(like)
	if !ok {
		return sql.IndexLookup{}, false, nil
	}
	prefix, ok := textPatternPrefix(pattern, escape)
	if !ok {
		return sql.IndexLookup{}, false, nil
	}
	upper, ok := nextTextPatternPrefix(prefix)
	if !ok {
		return sql.IndexLookup{}, false, nil
	}

	for _, cached := range t.patternOpLookup {
		if !strings.EqualFold(cached.columnName, field.Name()) {
			continue
		}
		builder := sql.NewMySQLIndexBuilder(ctx, cached.index)
		builder.GreaterOrEqual(ctx, cached.expression, cached.typ, prefix)
		builder.LessThan(ctx, cached.expression, cached.typ, upper)
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

func patternOpClassLookup(ctx *sql.Context, index sql.Index, tableSchema sql.Schema) (btreePatternOpLookup, bool) {
	metadata, ok := indexmetadata.DecodeComment(index.Comment())
	if !ok {
		return btreePatternOpLookup{}, false
	}
	if indexmetadata.AccessMethod(index.IndexType(), index.Comment()) != indexmetadata.AccessMethodBtree {
		return btreePatternOpLookup{}, false
	}
	logicalColumns := indexmetadata.LogicalColumns(index, tableSchema)
	columnTypes := index.ColumnExpressionTypes(ctx)
	for i, opClass := range metadata.OpClasses {
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

func unsafeBtreePlannerIndex(index sql.Index) bool {
	metadata, ok := indexmetadata.DecodeComment(index.Comment())
	if !ok {
		return false
	}
	if indexmetadata.AccessMethod(index.IndexType(), index.Comment()) != indexmetadata.AccessMethodBtree {
		return false
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
