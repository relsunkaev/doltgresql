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
	gmsexpression "github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"

	pgexprs "github.com/dolthub/doltgresql/server/expression"
)

// PreserveLateralLeftJoin keeps LEFT JOIN LATERAL ... ON true from being
// lowered to LATERAL CROSS JOIN by the GMS memo builder when the true filter
// is simplified away.
func PreserveLateralLeftJoin(ctx *sql.Context, _ *gmsanalyzer.Analyzer, node sql.Node, _ *plan.Scope, _ gmsanalyzer.RuleSelector, _ *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	return transform.NodeWithOpaque(ctx, node, func(ctx *sql.Context, n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		join, ok := n.(*plan.JoinNode)
		if !ok || join.JoinType() != plan.JoinTypeLateralLeft || !isTrueOrSimplifiedAway(join.JoinCond()) {
			return n, transform.SameTree, nil
		}
		return join.WithFilter(pgexprs.NewNonFoldableTrue()), transform.NewTree, nil
	})
}

func isTrueOrSimplifiedAway(expr sql.Expression) bool {
	if expr == nil {
		return true
	}
	lit, ok := expr.(*gmsexpression.Literal)
	return ok && lit.Val == true
}
