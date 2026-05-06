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
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	gmsanalyzer "github.com/dolthub/go-mysql-server/sql/analyzer"
	gmsexpression "github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"

	pgtransform "github.com/dolthub/doltgresql/server/transform"
)

// AssignSelectiveLookupJoinHints nudges GMS' join optimizer toward lookup joins
// for the narrow Doltgres index benchmark shape: an inner join whose left side
// has already been filtered by a selective equality predicate and whose right
// side is a directly indexed table.
func AssignSelectiveLookupJoinHints(ctx *sql.Context, _ *gmsanalyzer.Analyzer, node sql.Node, _ *plan.Scope, _ gmsanalyzer.RuleSelector, _ *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	return pgtransform.NodeWithOpaque(ctx, node, func(ctx *sql.Context, node sql.Node) (sql.Node, transform.TreeIdentity, error) {
		join, ok := node.(*plan.JoinNode)
		if !ok || !join.JoinType().IsInner() || join.Comment() != "" {
			return node, transform.SameTree, nil
		}
		if !hasSelectiveEqualityFilter(ctx, join.Left()) {
			return node, transform.SameTree, nil
		}
		leftName, ok := lookupJoinTableName(join.Left())
		if !ok {
			return node, transform.SameTree, nil
		}
		rightName, rightTable, ok := lookupJoinIndexedTable(join.Right())
		if !ok || !hasUsableLookupJoinIndex(ctx, rightTable) {
			return node, transform.SameTree, nil
		}
		comment := fmt.Sprintf("/*+ lookup_join(%s,%s) */", leftName, rightName)
		return join.WithComment(comment), transform.NewTree, nil
	})
}

func hasSelectiveEqualityFilter(ctx *sql.Context, node sql.Node) bool {
	filter, ok := node.(*plan.Filter)
	if !ok {
		return false
	}
	for _, expr := range SplitConjunction(ctx, filter.Expression) {
		equals, ok := expr.(*gmsexpression.Equals)
		if !ok {
			continue
		}
		if equalityComparesFieldToConstant(equals) {
			return true
		}
	}
	return false
}

func equalityComparesFieldToConstant(equals *gmsexpression.Equals) bool {
	leftField := expressionIsGetField(equals.Left())
	rightField := expressionIsGetField(equals.Right())
	return leftField != rightField
}

func expressionIsGetField(expr sql.Expression) bool {
	_, ok := expr.(*gmsexpression.GetField)
	return ok
}

func lookupJoinTableName(node sql.Node) (string, bool) {
	switch node := node.(type) {
	case *plan.Filter:
		return lookupJoinTableName(node.Child)
	case *plan.Project:
		return lookupJoinTableName(node.Child)
	case *plan.TableAlias:
		return node.Name(), true
	case sql.TableNode:
		return node.Name(), true
	default:
		return "", false
	}
}

func lookupJoinIndexedTable(node sql.Node) (string, sql.Table, bool) {
	switch node := node.(type) {
	case *plan.Filter:
		return lookupJoinIndexedTable(node.Child)
	case *plan.Project:
		return lookupJoinIndexedTable(node.Child)
	case *plan.TableAlias:
		_, table, ok := lookupJoinIndexedTable(node.Child)
		return node.Name(), table, ok
	case *plan.ResolvedTable:
		return node.Name(), node.Table, true
	case sql.TableNode:
		return node.Name(), node.UnderlyingTable(), true
	default:
		return "", nil, false
	}
}

func hasUsableLookupJoinIndex(ctx *sql.Context, table sql.Table) bool {
	indexAddressable, ok := table.(sql.IndexAddressable)
	if !ok {
		return false
	}
	indexes, err := indexAddressable.GetIndexes(ctx)
	if err != nil {
		return false
	}
	for _, index := range indexes {
		if strings.EqualFold(index.IndexType(), "BTREE") && len(index.Expressions()) > 0 {
			return true
		}
	}
	return false
}
