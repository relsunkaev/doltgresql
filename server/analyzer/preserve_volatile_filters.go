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
)

func skipRuleForNonDeterministicFilters(original gmsanalyzer.RuleFunc) gmsanalyzer.RuleFunc {
	return func(
		ctx *sql.Context,
		a *gmsanalyzer.Analyzer,
		node sql.Node,
		scope *plan.Scope,
		selector gmsanalyzer.RuleSelector,
		qFlags *sql.QueryFlags,
	) (sql.Node, transform.TreeIdentity, error) {
		if nodeHasNonDeterministicFilter(ctx, node) {
			return node, transform.SameTree, nil
		}
		return original(ctx, a, node, scope, selector, qFlags)
	}
}

func nodeHasNonDeterministicFilter(ctx *sql.Context, node sql.Node) bool {
	found := false
	_, _, _ = transform.NodeWithOpaque(ctx, node, func(ctx *sql.Context, n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		if found {
			return n, transform.SameTree, nil
		}
		switch n := n.(type) {
		case *plan.Filter:
			found = expressionHasNonDeterministicCall(ctx, n.Expression)
		case *plan.JoinNode:
			if n.Filter != nil {
				found = expressionHasNonDeterministicCall(ctx, n.Filter)
			}
		}
		return n, transform.SameTree, nil
	})
	return found
}

func expressionHasNonDeterministicCall(ctx *sql.Context, expr sql.Expression) bool {
	return transform.InspectExpr(ctx, expr, func(ctx *sql.Context, expr sql.Expression) bool {
		nonDeterministic, ok := expr.(sql.NonDeterministicExpression)
		return ok && nonDeterministic.IsNonDeterministic()
	})
}

func wrapAnalyzerRuleByName(
	rules []gmsanalyzer.Rule,
	name string,
	wrap func(gmsanalyzer.RuleFunc) gmsanalyzer.RuleFunc,
) []gmsanalyzer.Rule {
	wrapped := false
	newRules := make([]gmsanalyzer.Rule, len(rules))
	for i, rule := range rules {
		if rule.Id.String() == name {
			wrapped = true
			rule.Apply = wrap(rule.Apply)
		}
		newRules[i] = rule
	}

	if !wrapped {
		panic("one or more rules were not wrapped, this is a bug")
	}

	return newRules
}
