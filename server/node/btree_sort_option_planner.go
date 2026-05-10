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

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/server/indexmetadata"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

type metadataOnlyOrderedIndex struct {
	sql.Index
}

func (i metadataOnlyOrderedIndex) Order(*sql.Context) sql.IndexOrder {
	return sql.IndexOrderNone
}

func (i metadataOnlyOrderedIndex) Reversible(*sql.Context) bool {
	return false
}

func (i metadataOnlyOrderedIndex) ExtendedExpressions(ctx *sql.Context) []string {
	if extended, ok := i.Index.(sql.ExtendedIndex); ok {
		return extended.ExtendedExpressions(ctx)
	}
	return i.Expressions()
}

func (i metadataOnlyOrderedIndex) ExtendedColumnExpressionTypes(ctx *sql.Context) []sql.ColumnExpressionType {
	if extended, ok := i.Index.(sql.ExtendedIndex); ok {
		return extended.ExtendedColumnExpressionTypes(ctx)
	}
	return i.ColumnExpressionTypes(ctx)
}

func metadataOnlySortOptionIndex(index sql.Index, tableSchema sql.Schema) bool {
	return btreeSortOptionIndex(index) && !plannerSafeSortOptionIndex(index, tableSchema)
}

type nullableNullProbeOrderedIndex struct {
	sql.Index
	expressions           []string
	columnExpressionTypes []sql.ColumnExpressionType
}

func (i nullableNullProbeOrderedIndex) Expressions() []string {
	return append([]string(nil), i.expressions...)
}

func (i nullableNullProbeOrderedIndex) ColumnExpressionTypes(*sql.Context) []sql.ColumnExpressionType {
	return append([]sql.ColumnExpressionType(nil), i.columnExpressionTypes...)
}

func (i nullableNullProbeOrderedIndex) Order(ctx *sql.Context) sql.IndexOrder {
	if ordered, ok := i.Index.(sql.OrderedIndex); ok {
		return ordered.Order(ctx)
	}
	return sql.IndexOrderNone
}

func (i nullableNullProbeOrderedIndex) Reversible(ctx *sql.Context) bool {
	if ordered, ok := i.Index.(sql.OrderedIndex); ok {
		return ordered.Reversible(ctx)
	}
	return false
}

func (i nullableNullProbeOrderedIndex) ExtendedExpressions(ctx *sql.Context) []string {
	expressions := append([]string(nil), i.expressions...)
	if extended, ok := i.Index.(sql.ExtendedIndex); ok {
		underlyingExpressions := i.Index.Expressions()
		underlyingExtended := extended.ExtendedExpressions(ctx)
		if len(underlyingExtended) > len(underlyingExpressions) {
			return append(expressions, underlyingExtended[len(underlyingExpressions):]...)
		}
		return append(expressions, underlyingExtended...)
	}
	return expressions
}

func (i nullableNullProbeOrderedIndex) ExtendedColumnExpressionTypes(ctx *sql.Context) []sql.ColumnExpressionType {
	columnExpressionTypes := append([]sql.ColumnExpressionType(nil), i.columnExpressionTypes...)
	if extended, ok := i.Index.(sql.ExtendedIndex); ok {
		underlyingTypes := i.Index.ColumnExpressionTypes(ctx)
		underlyingExtended := extended.ExtendedColumnExpressionTypes(ctx)
		if len(underlyingExtended) > len(underlyingTypes) {
			return append(columnExpressionTypes, underlyingExtended[len(underlyingTypes):]...)
		}
		return append(columnExpressionTypes, underlyingExtended...)
	}
	return columnExpressionTypes
}

func nullableNullProbeSortOptionPlannerIndex(ctx *sql.Context, index sql.Index, tableSchema sql.Schema) (sql.Index, bool) {
	if !btreeSortOptionIndex(index) || tableSchema == nil {
		return nil, false
	}
	sortOptions := indexmetadata.SortOptions(index.Comment())
	logicalColumns := indexmetadata.LogicalColumns(index, tableSchema)
	if len(logicalColumns) == 0 || len(sortOptions) == 0 {
		return nil, false
	}

	physicalExpressions := index.Expressions()
	physicalTypes := index.ColumnExpressionTypes(ctx)
	expressions := make([]string, 0, len(physicalExpressions))
	columnExpressionTypes := make([]sql.ColumnExpressionType, 0, len(physicalTypes))
	physicalIndex := 0
	insertedProbe := false
	for logicalIndex, logicalColumn := range logicalColumns {
		if logicalIndex >= len(sortOptions) || logicalColumn.Expression {
			return nil, false
		}
		schemaIndex := tableSchema.IndexOfColName(logicalColumn.StorageName)
		if schemaIndex < 0 {
			return nil, false
		}
		schemaColumn := tableSchema[schemaIndex]
		qualifiedColumn := index.Table() + "." + schemaColumn.Name
		if schemaColumn.Nullable && nullableSortOptionNeedsNullProbeForPlanner(sortOptions[logicalIndex]) {
			if physicalIndex >= len(physicalExpressions) ||
				!isHiddenNullableSortProbeColumn(physicalExpressions[physicalIndex], tableSchema, schemaColumn.Name) {
				return nil, false
			}
			probeExpression := qualifiedColumn + " IS NULL"
			expressions = append(expressions, probeExpression)
			columnExpressionTypes = append(columnExpressionTypes, sql.ColumnExpressionType{
				Expression: probeExpression,
				Type:       pgtypes.Bool,
			})
			physicalIndex++
			insertedProbe = true
		}
		if physicalIndex >= len(physicalExpressions) ||
			!strings.EqualFold(unqualifiedPlannerIndexColumn(physicalExpressions[physicalIndex]), schemaColumn.Name) {
			return nil, false
		}
		expressions = append(expressions, qualifiedColumn)
		columnExpressionTypes = append(columnExpressionTypes, sql.ColumnExpressionType{
			Expression: qualifiedColumn,
			Type:       schemaColumn.Type,
		})
		physicalIndex++
	}
	if !insertedProbe || physicalIndex != len(physicalExpressions) {
		return nil, false
	}
	return nullableNullProbeOrderedIndex{
		Index:                 index,
		expressions:           expressions,
		columnExpressionTypes: columnExpressionTypes,
	}, true
}

func nullableSortOptionNeedsNullProbeForPlanner(option indexmetadata.IndexColumnOption) bool {
	direction := strings.ToLower(strings.TrimSpace(option.Direction))
	nullsOrder := strings.ToLower(strings.TrimSpace(option.NullsOrder))
	if direction == "" && nullsOrder == "" {
		return false
	}
	return !nativeNullableSortOption(option)
}

func isHiddenNullableSortProbeColumn(physicalExpression string, tableSchema sql.Schema, columnName string) bool {
	hiddenColumnName := unqualifiedPlannerIndexColumn(physicalExpression)
	hiddenColumnIndex := tableSchema.IndexOfColName(hiddenColumnName)
	if hiddenColumnIndex < 0 {
		return false
	}
	hiddenColumn := tableSchema[hiddenColumnIndex]
	if !hiddenColumn.HiddenSystem || hiddenColumn.Generated == nil || hiddenColumn.Generated.Expr == nil {
		return false
	}
	return nullableSortProbeExpressionKey(hiddenColumn.Generated.Expr.String()) == nullableSortProbeExpressionKey(columnName+" IS NULL")
}

func nullableSortProbeExpressionKey(expr string) string {
	expr = strings.ToLower(strings.TrimSpace(expr))
	for strings.HasPrefix(expr, "(") && strings.HasSuffix(expr, ")") {
		expr = strings.TrimSpace(expr[1 : len(expr)-1])
	}
	expr = strings.ReplaceAll(expr, `"`, "")
	expr = strings.ReplaceAll(expr, "`", "")
	return strings.Join(strings.Fields(expr), " ")
}

func btreeSortOptionIndex(index sql.Index) bool {
	if indexmetadata.AccessMethod(index.IndexType(), index.Comment()) != indexmetadata.AccessMethodBtree {
		return false
	}
	return hasSortOption(indexmetadata.SortOptions(index.Comment()))
}

func hasSortOption(sortOptions []indexmetadata.IndexColumnOption) bool {
	for _, option := range sortOptions {
		if strings.TrimSpace(option.Direction) != "" || strings.TrimSpace(option.NullsOrder) != "" {
			return true
		}
	}
	return false
}

func plannerSafeSortOptionIndex(index sql.Index, tableSchema sql.Schema) bool {
	sortOptions := indexmetadata.SortOptions(index.Comment())
	if !hasSortOption(sortOptions) || tableSchema == nil {
		return false
	}
	logicalColumns := indexmetadata.LogicalColumns(index, tableSchema)
	if len(logicalColumns) == 0 {
		return false
	}
	for i, option := range sortOptions {
		if strings.TrimSpace(option.Direction) == "" && strings.TrimSpace(option.NullsOrder) == "" {
			continue
		}
		if i >= len(logicalColumns) {
			return false
		}
		column := logicalColumns[i]
		if column.Expression {
			return false
		}
		schemaIndex := tableSchema.IndexOfColName(column.StorageName)
		if schemaIndex < 0 {
			return false
		}
		if !tableSchema[schemaIndex].Nullable {
			continue
		}
		if nativeNullableSortOption(option) {
			continue
		}
		return false
	}
	return true
}

func nativeNullableSortOption(option indexmetadata.IndexColumnOption) bool {
	direction := strings.ToLower(strings.TrimSpace(option.Direction))
	nullsOrder := strings.ToLower(strings.TrimSpace(option.NullsOrder))
	return (direction == "" && nullsOrder == indexmetadata.NullsOrderFirst) ||
		(direction == indexmetadata.SortDirectionDesc && nullsOrder == indexmetadata.NullsOrderLast)
}

func metadataOnlySortOptionIndexColumnsAvailable(index sql.Index, tableSchema sql.Schema) bool {
	for _, column := range indexmetadata.LogicalColumns(index, tableSchema) {
		if column.StorageName == "" || tableSchema.IndexOfColName(column.StorageName) < 0 {
			return false
		}
	}
	return true
}
