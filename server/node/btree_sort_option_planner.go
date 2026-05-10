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
	if indexmetadata.AccessMethod(index.IndexType(), index.Comment()) != indexmetadata.AccessMethodBtree {
		return false
	}
	if plannerSafeSortOptionIndex(index, tableSchema) {
		return false
	}
	for _, option := range indexmetadata.SortOptions(index.Comment()) {
		if strings.TrimSpace(option.Direction) != "" || strings.TrimSpace(option.NullsOrder) != "" {
			return true
		}
	}
	return false
}

func plannerSafeSortOptionIndex(index sql.Index, tableSchema sql.Schema) bool {
	sortOptions := indexmetadata.SortOptions(index.Comment())
	if len(sortOptions) == 0 || tableSchema == nil {
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
		if schemaIndex < 0 || tableSchema[schemaIndex].Nullable {
			return false
		}
	}
	return true
}

func metadataOnlySortOptionIndexColumnsAvailable(index sql.Index, tableSchema sql.Schema) bool {
	for _, column := range indexmetadata.LogicalColumns(index, tableSchema) {
		if column.StorageName == "" || tableSchema.IndexOfColName(column.StorageName) < 0 {
			return false
		}
	}
	return true
}
