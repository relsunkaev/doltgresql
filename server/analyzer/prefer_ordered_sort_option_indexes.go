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

package analyzer

import (
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"

	"github.com/dolthub/doltgresql/server/indexmetadata"
)

// PreferOrderedSortOptionIndexes applies PostgreSQL sort-option btree indexes
// before filter index pushdown can consume the table. GMS' later replaceIdxSort
// rule already removes the Sort once this ordered access is in place, while any
// existing Filter node remains available above the ordered scan.
func PreferOrderedSortOptionIndexes(ctx *sql.Context, a *analyzer.Analyzer, n sql.Node, scope *plan.Scope, sel analyzer.RuleSelector, qFlags *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	return preferOrderedSortOptionIndexes(ctx, n, nil)
}

func preferOrderedSortOptionIndexes(ctx *sql.Context, node sql.Node, sortFields sql.SortFields) (sql.Node, transform.TreeIdentity, error) {
	switch n := node.(type) {
	case *plan.Sort:
		if !sameSortFieldOrder(n.SortFields) {
			return n, transform.SameTree, nil
		}
		sortFields = n.SortFields
	case *plan.TopN:
		if !sameSortFieldOrder(n.Fields) {
			return n, transform.SameTree, nil
		}
		sortFields = n.Fields
	case *plan.IndexedTableAccess:
		return n, transform.SameTree, nil
	case *plan.ResolvedTable:
		if len(sortFields) == 0 {
			return n, transform.SameTree, nil
		}
		return orderedSortOptionIndexedAccess(ctx, n, sortFields)
	}

	children := node.Children()
	if len(children) == 0 {
		return node, transform.SameTree, nil
	}
	newChildren := make([]sql.Node, len(children))
	copy(newChildren, children)
	allSame := transform.SameTree
	for i, child := range children {
		switch child.(type) {
		case *plan.Sort, *plan.TopN, *plan.Project, *plan.Filter, *plan.Limit, *plan.Offset, *plan.Distinct, *plan.TableAlias, *plan.ResolvedTable:
			newChild, same, err := preferOrderedSortOptionIndexes(ctx, child, sortFields)
			if err != nil {
				return nil, transform.SameTree, err
			}
			if !same {
				newChildren[i] = newChild
				allSame = transform.NewTree
			}
		}
	}
	if allSame {
		return node, transform.SameTree, nil
	}
	ret, err := node.WithChildren(ctx, newChildren...)
	if err != nil {
		return nil, transform.SameTree, err
	}
	return ret, transform.NewTree, nil
}

func orderedSortOptionIndexedAccess(ctx *sql.Context, table *plan.ResolvedTable, sortFields sql.SortFields) (sql.Node, transform.TreeIdentity, error) {
	indexedTable, ok := table.UnderlyingTable().(sql.IndexAddressableTable)
	if !ok {
		return table, transform.SameTree, nil
	}
	if indexSearchable, ok := table.UnderlyingTable().(sql.IndexSearchableTable); ok && indexSearchable.SkipIndexCosting() {
		return table, transform.SameTree, nil
	}
	indexes, err := indexedTable.GetIndexes(ctx)
	if err != nil {
		return nil, transform.SameTree, err
	}
	for _, index := range indexes {
		if !sortOptionIndexHasOptions(index) || index.IsSpatial() || index.IsVector() {
			continue
		}
		if !sortFieldsMatchIndex(sortFields, index.Expressions()) {
			continue
		}
		ordered, ok := index.(sql.OrderedIndex)
		if !ok || ordered.Order(ctx) == sql.IndexOrderNone {
			continue
		}
		isReverse := sortFields[0].Order == sql.Descending
		if isReverse && !ordered.Reversible(ctx) {
			continue
		}
		indexBuilder := sql.NewMySQLIndexBuilder(ctx, index)
		lookup, err := indexBuilder.Build(ctx)
		if err != nil {
			return nil, transform.SameTree, err
		}
		mysqlRanges := lookup.Ranges.(sql.MySQLRangeCollection)
		if !index.CanSupport(ctx, mysqlRanges.ToRanges()...) {
			continue
		}
		if isReverse {
			lookup = sql.NewIndexLookup(
				lookup.Index,
				mysqlRanges,
				lookup.IsPointLookup,
				lookup.IsEmptyRange,
				lookup.IsSpatialLookup,
				true,
			)
		}
		indexedAccess, err := plan.NewStaticIndexedAccessForTableNode(ctx, table, lookup)
		if err != nil {
			return nil, transform.SameTree, err
		}
		return indexedAccess, transform.NewTree, nil
	}
	return table, transform.SameTree, nil
}

func sortOptionIndexHasOptions(index sql.Index) bool {
	if indexmetadata.AccessMethod(index.IndexType(), index.Comment()) != indexmetadata.AccessMethodBtree {
		return false
	}
	for _, option := range indexmetadata.SortOptions(index.Comment()) {
		if strings.TrimSpace(option.Direction) != "" || strings.TrimSpace(option.NullsOrder) != "" {
			return true
		}
	}
	return false
}

func sortFieldsMatchIndex(sortFields sql.SortFields, indexExpressions []string) bool {
	if len(sortFields) > len(indexExpressions) {
		return false
	}
	for i, sortField := range sortFields {
		if !strings.EqualFold(sortExpressionKey(sortField.Column.String()), sortExpressionKey(indexExpressions[i])) {
			return false
		}
	}
	return true
}

func sortExpressionKey(expr string) string {
	expr = strings.ToLower(strings.TrimSpace(expr))
	expr = strings.ReplaceAll(expr, `"`, "")
	expr = strings.ReplaceAll(expr, "`", "")
	return strings.Join(strings.Fields(expr), " ")
}

func sameSortFieldOrder(sortFields sql.SortFields) bool {
	if len(sortFields) == 0 {
		return false
	}
	for _, sortField := range sortFields {
		if sortFields[0].Order != sortField.Order {
			return false
		}
	}
	return true
}
