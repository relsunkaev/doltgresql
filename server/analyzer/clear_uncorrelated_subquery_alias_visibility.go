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
	"github.com/dolthub/go-mysql-server/sql"
	gmsanalyzer "github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"

	pgtransform "github.com/dolthub/doltgresql/server/transform"
)

// ClearUncorrelatedSubqueryAliasVisibility normalizes derived-table visibility
// inside scalar subqueries after GMS has proved that the scalar subquery is
// uncorrelated. GMS may leave OuterScopeVisibility set from the earlier
// analysis scope, and may also miss lateral propagation for nested correlated
// aliases inside a lateral subquery.
func ClearUncorrelatedSubqueryAliasVisibility(ctx *sql.Context, a *gmsanalyzer.Analyzer, node sql.Node, scope *plan.Scope, selector gmsanalyzer.RuleSelector, qFlags *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	return pgtransform.NodeExprsWithOpaque(ctx, node, func(ctx *sql.Context, expr sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		subquery, ok := expr.(*plan.Subquery)
		if !ok || !subquery.Correlated().Empty() {
			return expr, transform.SameTree, nil
		}

		query, same, err := normalizeUncorrelatedSubqueryAliases(ctx, subquery.Query)
		if err != nil || same {
			return expr, same, err
		}
		return subquery.WithQuery(query), transform.NewTree, nil
	})
}

func normalizeUncorrelatedSubqueryAliases(ctx *sql.Context, node sql.Node) (sql.Node, transform.TreeIdentity, error) {
	return pgtransform.NodeWithOpaque(ctx, node, func(ctx *sql.Context, node sql.Node) (sql.Node, transform.TreeIdentity, error) {
		subqueryAlias, ok := node.(*plan.SubqueryAlias)
		if !ok {
			return node, transform.SameTree, nil
		}

		ret := *subqueryAlias
		changed := false
		if ret.OuterScopeVisibility && !ret.IsLateral && ret.Correlated.Empty() {
			ret.OuterScopeVisibility = false
			changed = true
		}
		if !ret.OuterScopeVisibility && !ret.IsLateral && !ret.Correlated.Empty() {
			ret.IsLateral = true
			changed = true
		}
		if !changed {
			return node, transform.SameTree, nil
		}
		return &ret, transform.NewTree, nil
	})
}
