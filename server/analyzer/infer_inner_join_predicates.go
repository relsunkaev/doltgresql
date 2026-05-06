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

	pgexpression "github.com/dolthub/doltgresql/server/expression"
	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtransform "github.com/dolthub/doltgresql/server/transform"
)

// InferInnerJoinPredicates derives conservative single-table predicates from
// inner-join equalities before the GMS join optimizer chooses a join strategy.
func InferInnerJoinPredicates(ctx *sql.Context, _ *gmsanalyzer.Analyzer, node sql.Node, _ *plan.Scope, _ gmsanalyzer.RuleSelector, _ *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	return pgtransform.NodeWithOpaque(ctx, node, func(ctx *sql.Context, node sql.Node) (sql.Node, transform.TreeIdentity, error) {
		join, ok := node.(*plan.JoinNode)
		if !ok || !join.JoinType().IsInner() || join.JoinCond() == nil {
			return node, transform.SameTree, nil
		}

		leftConstants := collectEqualityConstants(ctx, join.Left())
		rightConstants := collectEqualityConstants(ctx, join.Right())
		leftComparisons := collectRangeComparisons(ctx, join.Left())
		rightComparisons := collectRangeComparisons(ctx, join.Right())
		if len(leftConstants) == 0 && len(rightConstants) == 0 && len(leftComparisons) == 0 && len(rightComparisons) == 0 {
			return node, transform.SameTree, nil
		}

		var leftPredicates []sql.Expression
		var rightPredicates []sql.Expression
		var joinPredicates []sql.Expression
		var recheckPredicates []sql.Expression
		for _, expr := range SplitConjunction(ctx, join.JoinCond()) {
			joinPredicate := expr
			leftExpr, rightExpr, ok := equalityExpressionSides(expr)
			if !ok {
				joinPredicates = append(joinPredicates, joinPredicate)
				continue
			}
			leftField, leftOk := leftExpr.(*gmsexpression.GetField)
			rightField, rightOk := rightExpr.(*gmsexpression.GetField)
			if !leftOk || !rightOk {
				joinPredicates = append(joinPredicates, joinPredicate)
				continue
			}
			leftSide, rightSide, ok := joinEqualitySides(ctx, join, leftField, rightField)
			if !ok {
				joinPredicates = append(joinPredicates, joinPredicate)
				continue
			}
			if _, ok := expr.(*gmsexpression.Equals); !ok {
				joinPredicate = gmsexpression.NewEquals(leftExpr, rightExpr)
				recheckPredicates = append(recheckPredicates, expr)
			}
			joinPredicates = append(joinPredicates, joinPredicate)
			if literal, ok := leftConstants[leftSide.key]; ok {
				predicate, ok := inferredEqualityPredicate(ctx, join.Right(), rightSide.field, literal)
				if ok && !hasEqualityConstant(rightConstants, rightSide.key) {
					rightPredicates = append(rightPredicates, predicate)
				}
			}
			for _, comparison := range leftComparisons[leftSide.key] {
				predicate, ok := inferredRangePredicate(ctx, join.Right(), rightSide.field, comparison)
				if ok && !hasEqualityConstant(rightConstants, rightSide.key) && !hasRangeComparison(rightComparisons, rightSide.key, comparison) {
					rightPredicates = append(rightPredicates, predicate)
				}
			}
			if literal, ok := rightConstants[rightSide.key]; ok {
				predicate, ok := inferredEqualityPredicate(ctx, join.Left(), leftSide.field, literal)
				if ok && !hasEqualityConstant(leftConstants, leftSide.key) {
					leftPredicates = append(leftPredicates, predicate)
				}
			}
			for _, comparison := range rightComparisons[rightSide.key] {
				predicate, ok := inferredRangePredicate(ctx, join.Left(), leftSide.field, comparison)
				if ok && !hasEqualityConstant(leftConstants, leftSide.key) && !hasRangeComparison(leftComparisons, leftSide.key, comparison) {
					leftPredicates = append(leftPredicates, predicate)
				}
			}
		}

		if len(leftPredicates) == 0 && len(rightPredicates) == 0 {
			return node, transform.SameTree, nil
		}
		left := addFilterPredicates(join.Left(), leftPredicates)
		right := addFilterPredicates(join.Right(), rightPredicates)
		replacementNode, err := join.WithExpressions(ctx, gmsexpression.JoinAnd(joinPredicates...))
		if err != nil {
			return nil, transform.NewTree, err
		}
		replacement := replacementNode.(*plan.JoinNode)
		if shouldAssignInferredHashJoinHint(join.Comment()) {
			if comment := inferredHashJoinComment(left, right); comment != "" {
				replacement = replacement.WithComment(comment).(*plan.JoinNode)
			}
		}
		newNode, err := replacement.WithChildren(ctx, left, right)
		if err != nil {
			return nil, transform.NewTree, err
		}
		if len(recheckPredicates) > 0 {
			newNode = plan.NewFilter(gmsexpression.JoinAnd(recheckPredicates...), newNode)
		}
		return newNode, transform.NewTree, nil
	})
}

type joinFieldSide struct {
	field *gmsexpression.GetField
	key   equalityFieldKey
}

type equalityFieldKey struct {
	tableID sql.TableId
	table   string
	name    string
}

type rangeComparison struct {
	op      sql.IndexScanOp
	literal *gmsexpression.Literal
}

func collectEqualityConstants(ctx *sql.Context, node sql.Node) map[equalityFieldKey]*gmsexpression.Literal {
	constants := make(map[equalityFieldKey]*gmsexpression.Literal)
	for {
		filter, ok := node.(*plan.Filter)
		if !ok {
			return constants
		}
		for _, expr := range SplitConjunction(ctx, filter.Expression) {
			field, literal, ok := equalityFieldAndLiteral(expr)
			if !ok || literal.Value() == nil {
				continue
			}
			constants[fieldKey(field)] = literal
		}
		node = filter.Child
	}
}

func collectRangeComparisons(ctx *sql.Context, node sql.Node) map[equalityFieldKey][]rangeComparison {
	comparisons := make(map[equalityFieldKey][]rangeComparison)
	for {
		filter, ok := node.(*plan.Filter)
		if !ok {
			return comparisons
		}
		for _, expr := range SplitConjunction(ctx, filter.Expression) {
			field, comparison, ok := rangeFieldAndLiteral(expr)
			if !ok || comparison.literal.Value() == nil {
				continue
			}
			key := fieldKey(field)
			if hasRangeComparison(comparisons, key, comparison) {
				continue
			}
			comparisons[key] = append(comparisons[key], comparison)
		}
		node = filter.Child
	}
}

func equalityFieldAndLiteral(expr sql.Expression) (*gmsexpression.GetField, *gmsexpression.Literal, bool) {
	left, right, ok := equalityExpressionSides(expr)
	if !ok {
		return nil, nil, false
	}
	if field, ok := left.(*gmsexpression.GetField); ok {
		if literal, ok := right.(*gmsexpression.Literal); ok {
			return field, literal, true
		}
	}
	if field, ok := right.(*gmsexpression.GetField); ok {
		if literal, ok := left.(*gmsexpression.Literal); ok {
			return field, literal, true
		}
	}
	return nil, nil, false
}

func equalityExpressionSides(expr sql.Expression) (sql.Expression, sql.Expression, bool) {
	equality, ok := expr.(gmsexpression.Equality)
	if !ok || !equality.RepresentsEquality() {
		return nil, nil, false
	}
	return equality.Left(), equality.Right(), true
}

func rangeFieldAndLiteral(expr sql.Expression) (*gmsexpression.GetField, rangeComparison, bool) {
	comparison, ok := expr.(sql.IndexComparisonExpression)
	if !ok {
		return nil, rangeComparison{}, false
	}
	op, left, right, ok := comparison.IndexScanOperation()
	if !ok || !isRangeScanOp(op) {
		return nil, rangeComparison{}, false
	}
	if field, ok := left.(*gmsexpression.GetField); ok {
		if literal, ok := right.(*gmsexpression.Literal); ok {
			return field, rangeComparison{op: op, literal: literal}, true
		}
	}
	if literal, ok := left.(*gmsexpression.Literal); ok {
		if field, ok := right.(*gmsexpression.GetField); ok {
			return field, rangeComparison{op: op.Swap(), literal: literal}, true
		}
	}
	return nil, rangeComparison{}, false
}

func isRangeScanOp(op sql.IndexScanOp) bool {
	switch op {
	case sql.IndexScanOpGt, sql.IndexScanOpGte, sql.IndexScanOpLt, sql.IndexScanOpLte:
		return true
	default:
		return false
	}
}

func joinEqualitySides(ctx *sql.Context, join *plan.JoinNode, first *gmsexpression.GetField, second *gmsexpression.GetField) (joinFieldSide, joinFieldSide, bool) {
	firstLeft, firstLeftOk := fieldForChild(ctx, join.Left(), first)
	firstRight, firstRightOk := fieldForChild(ctx, join.Right(), first)
	secondLeft, secondLeftOk := fieldForChild(ctx, join.Left(), second)
	secondRight, secondRightOk := fieldForChild(ctx, join.Right(), second)
	switch {
	case firstLeftOk && !firstRightOk && secondRightOk && !secondLeftOk:
		return joinFieldSide{field: firstLeft, key: fieldKey(firstLeft)}, joinFieldSide{field: secondRight, key: fieldKey(secondRight)}, true
	case firstRightOk && !firstLeftOk && secondLeftOk && !secondRightOk:
		return joinFieldSide{field: secondLeft, key: fieldKey(secondLeft)}, joinFieldSide{field: firstRight, key: fieldKey(firstRight)}, true
	default:
		return joinFieldSide{}, joinFieldSide{}, false
	}
}

func fieldForChild(ctx *sql.Context, child sql.Node, field *gmsexpression.GetField) (*gmsexpression.GetField, bool) {
	schema := child.Schema(ctx)
	for i, column := range schema {
		if !sameFieldName(column, field) {
			continue
		}
		childField, ok := field.WithIndex(i).(*gmsexpression.GetField)
		return childField, ok
	}
	return nil, false
}

func sameFieldName(column *sql.Column, field *gmsexpression.GetField) bool {
	if !strings.EqualFold(column.Name, field.Name()) {
		return false
	}
	if field.Table() == "" {
		return true
	}
	return strings.EqualFold(column.Source, field.Table())
}

func fieldKey(field *gmsexpression.GetField) equalityFieldKey {
	return equalityFieldKey{
		tableID: field.TableId(),
		table:   strings.ToLower(field.Table()),
		name:    strings.ToLower(field.Name()),
	}
}

func hasEqualityConstant(constants map[equalityFieldKey]*gmsexpression.Literal, key equalityFieldKey) bool {
	_, ok := constants[key]
	return ok
}

func hasRangeComparison(comparisons map[equalityFieldKey][]rangeComparison, key equalityFieldKey, candidate rangeComparison) bool {
	for _, existing := range comparisons[key] {
		if existing.op == candidate.op && existing.literal.String() == candidate.literal.String() {
			return true
		}
	}
	return false
}

func inferredEqualityPredicate(ctx *sql.Context, child sql.Node, field *gmsexpression.GetField, literal *gmsexpression.Literal) (sql.Expression, bool) {
	childField, ok := fieldForChild(ctx, child, field)
	if !ok {
		return nil, false
	}
	predicate, err := pgexpression.NewBinaryOperator(framework.Operator_BinaryEqual).WithChildren(ctx, childField, literal)
	if err != nil {
		return nil, false
	}
	return predicate, true
}

func inferredRangePredicate(ctx *sql.Context, child sql.Node, field *gmsexpression.GetField, comparison rangeComparison) (sql.Expression, bool) {
	childField, ok := fieldForChild(ctx, child, field)
	if !ok {
		return nil, false
	}
	operator, ok := rangeOperatorForIndexScanOp(comparison.op)
	if !ok {
		return nil, false
	}
	predicate, err := pgexpression.NewBinaryOperator(operator).WithChildren(ctx, childField, comparison.literal)
	if err != nil {
		return nil, false
	}
	return predicate, true
}

func rangeOperatorForIndexScanOp(op sql.IndexScanOp) (framework.Operator, bool) {
	switch op {
	case sql.IndexScanOpGt:
		return framework.Operator_BinaryGreaterThan, true
	case sql.IndexScanOpGte:
		return framework.Operator_BinaryGreaterOrEqual, true
	case sql.IndexScanOpLt:
		return framework.Operator_BinaryLessThan, true
	case sql.IndexScanOpLte:
		return framework.Operator_BinaryLessOrEqual, true
	default:
		return 0, false
	}
}

func addFilterPredicates(node sql.Node, predicates []sql.Expression) sql.Node {
	if len(predicates) == 0 {
		return node
	}
	expr := gmsexpression.JoinAnd(predicates...)
	if filter, ok := node.(*plan.Filter); ok {
		return plan.NewFilter(gmsexpression.JoinAnd(filter.Expression, expr), filter.Child)
	}
	return plan.NewFilter(expr, node)
}

func lookupJoinBaseTableName(node sql.Node) (string, bool) {
	switch node := node.(type) {
	case *plan.Filter:
		return lookupJoinBaseTableName(node.Child)
	case *plan.Project:
		return lookupJoinBaseTableName(node.Child)
	case *plan.TableAlias:
		return lookupJoinBaseTableName(node.Child)
	case *plan.ResolvedTable:
		return node.Name(), true
	case sql.TableNode:
		return node.Name(), true
	default:
		return "", false
	}
}

func inferredHashJoinComment(left sql.Node, right sql.Node) string {
	seen := make(map[string]struct{})
	var hints []string
	if leftName, ok := lookupJoinTableName(left); ok {
		if rightName, _, ok := lookupJoinIndexedTable(right); ok {
			hint := fmt.Sprintf("hash_join(%s,%s)", leftName, rightName)
			hints = append(hints, hint)
			seen[strings.ToLower(hint)] = struct{}{}
		}
	}
	if leftName, ok := lookupJoinBaseTableName(left); ok {
		if rightName, ok := lookupJoinBaseTableName(right); ok {
			hint := fmt.Sprintf("hash_join(%s,%s)", leftName, rightName)
			if _, ok := seen[strings.ToLower(hint)]; !ok {
				hints = append(hints, hint)
			}
		}
	}
	if len(hints) == 0 {
		return ""
	}
	return "/*+ " + strings.Join(hints, " ") + " */"
}

func shouldAssignInferredHashJoinHint(comment string) bool {
	return comment == ""
}
