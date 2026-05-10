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

package indexpredicate

import (
	"math"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/dolthub/doltgresql/postgres/parser/parser"
	"github.com/dolthub/doltgresql/postgres/parser/sem/tree"
)

// Implies reports whether queryPredicate is strong enough to satisfy
// indexPredicate. It intentionally handles a conservative subset of PostgreSQL
// predicate implication shapes; unsupported expressions return false.
func Implies(indexPredicate string, queryPredicate string) bool {
	if indexPredicate == queryPredicate {
		return true
	}

	indexTerms, ok := predicateConjuncts(indexPredicate)
	if !ok {
		return false
	}
	queryTerms, ok := predicateConjuncts(queryPredicate)
	if !ok {
		return false
	}
	for term, indexExpr := range indexTerms {
		if _, ok = queryTerms[term]; ok {
			continue
		}
		if !predicateTermImpliedByQueryTerms(indexExpr, queryTerms) {
			return false
		}
	}
	return true
}

// Definition returns a normalized predicate expression string.
func Definition(predicate tree.Expr) string {
	if predicate == nil {
		return ""
	}
	return strings.TrimSpace(tree.AsString(predicate))
}

func predicateConjuncts(predicate string) (map[string]tree.Expr, bool) {
	expr, ok := parsePredicateExpr(predicate)
	if !ok {
		return nil, false
	}
	terms := make(map[string]tree.Expr)
	collectPredicateConjuncts(expr, terms)
	return terms, true
}

func parsePredicateExpr(predicate string) (tree.Expr, bool) {
	statements, err := parser.Parse("SELECT 1 WHERE " + predicate)
	if err != nil || len(statements) != 1 {
		return nil, false
	}
	selectStatement, ok := statements[0].AST.(*tree.Select)
	if !ok {
		return nil, false
	}
	selectClause, ok := selectStatement.Select.(*tree.SelectClause)
	if !ok || selectClause.Where == nil {
		return nil, false
	}
	return selectClause.Where.Expr, true
}

func collectPredicateConjuncts(expr tree.Expr, terms map[string]tree.Expr) {
	switch expr := expr.(type) {
	case *tree.AndExpr:
		collectPredicateConjuncts(expr.Left, terms)
		collectPredicateConjuncts(expr.Right, terms)
	case *tree.ParenExpr:
		collectPredicateConjuncts(expr.Expr, terms)
	default:
		terms[strings.TrimSpace(tree.AsString(expr))] = expr
	}
}

func anyPredicateTermImplies(indexExpr tree.Expr, queryTerms map[string]tree.Expr) bool {
	for _, queryExpr := range queryTerms {
		if predicateTermImplies(indexExpr, queryExpr) {
			return true
		}
	}
	return false
}

func predicateTermImpliedByQueryTerms(indexExpr tree.Expr, queryTerms map[string]tree.Expr) bool {
	if anyPredicateTermImplies(indexExpr, queryTerms) {
		return true
	}
	return queryTermsImplyEquality(indexExpr, queryTerms)
}

func predicateTermImplies(indexExpr tree.Expr, queryExpr tree.Expr) bool {
	indexExpr = unwrapPredicateParens(indexExpr)
	queryExpr = unwrapPredicateParens(queryExpr)
	if strings.TrimSpace(tree.AsString(indexExpr)) == strings.TrimSpace(tree.AsString(queryExpr)) {
		return true
	}

	if queryOr, ok := queryExpr.(*tree.OrExpr); ok {
		return predicateTermImplies(indexExpr, queryOr.Left) && predicateTermImplies(indexExpr, queryOr.Right)
	}
	if indexOr, ok := indexExpr.(*tree.OrExpr); ok {
		return predicateTermImplies(indexOr.Left, queryExpr) || predicateTermImplies(indexOr.Right, queryExpr)
	}
	if indexAnd, ok := indexExpr.(*tree.AndExpr); ok {
		return predicateTermImplies(indexAnd.Left, queryExpr) && predicateTermImplies(indexAnd.Right, queryExpr)
	}
	if queryAnd, ok := queryExpr.(*tree.AndExpr); ok {
		return predicateTermImplies(indexExpr, queryAnd.Left) || predicateTermImplies(indexExpr, queryAnd.Right)
	}

	if indexNotNull, ok := notNullPredicateExprKey(indexExpr); ok {
		return queryPredicateImpliesNotNull(indexNotNull, queryExpr)
	}
	if indexNull, ok := nullPredicateExprKey(indexExpr); ok {
		queryNull, ok := nullPredicateExprKey(queryExpr)
		return ok && queryNull == indexNull
	}

	indexPrefix, ok := predicatePrefixLikeFromExpr(indexExpr)
	if ok {
		if queryPrefix, ok := predicatePrefixLikeFromExpr(queryExpr); ok {
			return indexPrefix.exprKey == queryPrefix.exprKey && strings.HasPrefix(queryPrefix.prefix, indexPrefix.prefix)
		}
		if queryValues, ok := predicateValueSetFromExpr(queryExpr); ok {
			return queryValues.stringsHavePrefix(indexPrefix.exprKey, indexPrefix.prefix)
		}
		return false
	}

	indexValues, ok := predicateValueSetFromExpr(indexExpr)
	if ok {
		queryValues, ok := predicateValueSetFromExpr(queryExpr)
		if ok {
			return indexValues.exprKey == queryValues.exprKey && queryValues.subsetOf(indexValues)
		}
		queryBool, ok := booleanPredicateComparisonFromExpr(queryExpr)
		if !ok || indexValues.exprKey != queryBool.exprKey {
			return false
		}
		indexBool, ok := indexValues.singleBoolValue()
		return ok && indexBool == queryBool.value
	}

	indexExclusions, ok := predicateExclusionSetFromExpr(indexExpr)
	if ok {
		queryValues, ok := predicateValueSetFromExpr(queryExpr)
		if ok {
			return indexExclusions.exprKey == queryValues.exprKey && queryValues.disjointFrom(indexExclusions)
		}
		queryExclusions, ok := predicateExclusionSetFromExpr(queryExpr)
		if ok {
			return queryExclusions.implies(indexExclusions)
		}
		if !indexExclusions.nullsIncluded {
			return false
		}
		queryNull, ok := nullPredicateExprKey(queryExpr)
		return ok && queryNull == indexExclusions.exprKey
	}

	indexRange, ok := numericPredicateRangeFromExpr(indexExpr)
	if ok {
		queryRange, ok := numericPredicateRangeFromExpr(queryExpr)
		if !ok || !strings.EqualFold(indexRange.column, queryRange.column) {
			return false
		}
		return queryRange.bounds.subsetOf(indexRange.bounds)
	}
	indexBool, ok := booleanPredicateComparisonFromExpr(indexExpr)
	if !ok {
		return false
	}
	queryBool, ok := booleanPredicateComparisonFromExpr(queryExpr)
	return ok && indexBool.exprKey == queryBool.exprKey && indexBool.value == queryBool.value
}

type predicateEquality struct {
	leftKey  string
	rightKey string
	nullSafe bool
}

type predicateQueryFacts struct {
	values             map[string]string
	nulls              map[string]struct{}
	strongEqualities   map[string]map[string]struct{}
	nullSafeEqualities map[string]map[string]struct{}
}

func queryTermsImplyEquality(indexExpr tree.Expr, queryTerms map[string]tree.Expr) bool {
	equality, ok := predicateEqualityFromExpr(indexExpr)
	if !ok {
		return false
	}
	facts := predicateQueryFactsFromTerms(queryTerms)
	if facts.implyStrongEquality(equality.leftKey, equality.rightKey) {
		return true
	}
	if !equality.nullSafe {
		return false
	}
	return facts.implyNullSafeEquality(equality.leftKey, equality.rightKey)
}

func (f predicateQueryFacts) implyStrongEquality(leftKey string, rightKey string) bool {
	if leftValue, ok := f.values[leftKey]; ok {
		if rightValue, ok := f.values[rightKey]; ok && leftValue == rightValue {
			return true
		}
	}
	return predicateEqualityReachable(f.strongEqualities, leftKey, rightKey)
}

func (f predicateQueryFacts) implyNullSafeEquality(leftKey string, rightKey string) bool {
	_, leftNull := f.nulls[leftKey]
	_, rightNull := f.nulls[rightKey]
	if leftNull && rightNull {
		return true
	}
	return predicateEqualityReachable(f.nullSafeEqualities, leftKey, rightKey)
}

func predicateEqualityFromExpr(expr tree.Expr) (predicateEquality, bool) {
	comparison, ok := unwrapPredicateParens(expr).(*tree.ComparisonExpr)
	if !ok || (comparison.Operator != tree.EQ && comparison.Operator != tree.IsNotDistinctFrom) {
		return predicateEquality{}, false
	}
	leftKey, ok := predicateComparableExprKey(comparison.Left)
	if !ok {
		return predicateEquality{}, false
	}
	rightKey, ok := predicateComparableExprKey(comparison.Right)
	if !ok || leftKey == rightKey {
		return predicateEquality{}, false
	}
	return predicateEquality{
		leftKey:  leftKey,
		rightKey: rightKey,
		nullSafe: comparison.Operator == tree.IsNotDistinctFrom,
	}, true
}

func predicateQueryFactsFromTerms(queryTerms map[string]tree.Expr) predicateQueryFacts {
	facts := predicateQueryFacts{
		values:             make(map[string]string),
		nulls:              make(map[string]struct{}),
		strongEqualities:   make(map[string]map[string]struct{}),
		nullSafeEqualities: make(map[string]map[string]struct{}),
	}
	for _, queryExpr := range queryTerms {
		if values, ok := predicateValueSetFromExpr(queryExpr); ok && len(values.values) == 1 {
			for value := range values.values {
				if existingValue, ok := facts.values[values.exprKey]; !ok || existingValue == value {
					facts.values[values.exprKey] = value
				} else {
					delete(facts.values, values.exprKey)
				}
			}
		}
		if exprKey, ok := nullPredicateExprKey(queryExpr); ok {
			facts.nulls[exprKey] = struct{}{}
		}
		if equality, ok := predicateEqualityFromExpr(queryExpr); ok {
			if !equality.nullSafe {
				predicateAddEqualityEdge(facts.strongEqualities, equality.leftKey, equality.rightKey)
			}
			predicateAddEqualityEdge(facts.nullSafeEqualities, equality.leftKey, equality.rightKey)
		}
	}
	return facts
}

func predicateAddEqualityEdge(graph map[string]map[string]struct{}, leftKey string, rightKey string) {
	if graph[leftKey] == nil {
		graph[leftKey] = make(map[string]struct{})
	}
	if graph[rightKey] == nil {
		graph[rightKey] = make(map[string]struct{})
	}
	graph[leftKey][rightKey] = struct{}{}
	graph[rightKey][leftKey] = struct{}{}
}

func predicateEqualityReachable(graph map[string]map[string]struct{}, leftKey string, rightKey string) bool {
	if leftKey == rightKey {
		return true
	}
	if len(graph[leftKey]) == 0 || len(graph[rightKey]) == 0 {
		return false
	}
	seen := map[string]struct{}{leftKey: {}}
	queue := []string{leftKey}
	for len(queue) > 0 {
		key := queue[0]
		queue = queue[1:]
		for next := range graph[key] {
			if next == rightKey {
				return true
			}
			if _, ok := seen[next]; ok {
				continue
			}
			seen[next] = struct{}{}
			queue = append(queue, next)
		}
	}
	return false
}

func notNullPredicateExprKey(expr tree.Expr) (string, bool) {
	expr = unwrapPredicateParens(expr)
	if isNotNull, ok := expr.(*tree.IsNotNullExpr); ok {
		return predicateComparableExprKey(isNotNull.Expr)
	}
	comparison, ok := expr.(*tree.ComparisonExpr)
	if !ok || comparison.Operator != tree.IsDistinctFrom {
		return "", false
	}
	if predicateIsNullLiteral(comparison.Right) {
		return predicateComparableExprKey(comparison.Left)
	}
	if predicateIsNullLiteral(comparison.Left) {
		return predicateComparableExprKey(comparison.Right)
	}
	return "", false
}

func nullPredicateExprKey(expr tree.Expr) (string, bool) {
	expr = unwrapPredicateParens(expr)
	if isNull, ok := expr.(*tree.IsNullExpr); ok {
		return predicateComparableExprKey(isNull.Expr)
	}
	comparison, ok := expr.(*tree.ComparisonExpr)
	if !ok || comparison.Operator != tree.IsNotDistinctFrom {
		return "", false
	}
	if predicateIsNullLiteral(comparison.Right) {
		if exprKey, ok := predicateComparableExprKey(comparison.Left); ok {
			return exprKey, true
		}
	}
	if predicateIsNullLiteral(comparison.Left) {
		if exprKey, ok := predicateComparableExprKey(comparison.Right); ok {
			return exprKey, true
		}
	}
	return "", false
}

func predicateIsNullLiteral(expr tree.Expr) bool {
	expr = unwrapPredicateParens(expr)
	return expr == tree.DNull
}

func queryPredicateImpliesNotNull(indexExprKey string, queryExpr tree.Expr) bool {
	if queryNotNull, ok := notNullPredicateExprKey(queryExpr); ok {
		return queryNotNull == indexExprKey
	}
	if queryValues, ok := predicateValueSetFromExpr(queryExpr); ok {
		return queryValues.exprKey == indexExprKey
	}
	if queryRange, ok := numericPredicateRangeFromExpr(queryExpr); ok {
		return queryRange.bounds.valid() && "column:"+queryRange.column == indexExprKey
	}
	if queryBool, ok := booleanPredicateComparisonFromExpr(queryExpr); ok {
		return queryBool.exprKey == indexExprKey
	}
	if queryPrefix, ok := predicatePrefixLikeFromExpr(queryExpr); ok {
		return queryPrefix.exprKey == indexExprKey
	}
	return false
}

func unwrapPredicateParens(expr tree.Expr) tree.Expr {
	for {
		paren, ok := expr.(*tree.ParenExpr)
		if !ok {
			return expr
		}
		expr = paren.Expr
	}
}

type numericPredicateComparison struct {
	column string
	op     tree.ComparisonOperator
	value  float64
}

type numericPredicateRange struct {
	hasLower       bool
	lower          float64
	lowerInclusive bool
	hasUpper       bool
	upper          float64
	upperInclusive bool
}

type booleanPredicateComparison struct {
	exprKey string
	value   bool
}

type predicatePrefixLike struct {
	exprKey string
	prefix  string
}

type predicateValueSet struct {
	exprKey string
	values  map[string]struct{}
}

type predicateExclusionSet struct {
	exprKey       string
	values        map[string]struct{}
	nullsIncluded bool
}

func (s predicateValueSet) subsetOf(other predicateValueSet) bool {
	for value := range s.values {
		if _, ok := other.values[value]; !ok {
			return false
		}
	}
	return true
}

func (s predicateValueSet) singleBoolValue() (bool, bool) {
	if len(s.values) != 1 {
		return false, false
	}
	for value := range s.values {
		switch value {
		case "b:true":
			return true, true
		case "b:false":
			return false, true
		}
	}
	return false, false
}

func (s predicateValueSet) stringsHavePrefix(exprKey string, prefix string) bool {
	if s.exprKey != exprKey || len(s.values) == 0 {
		return false
	}
	for value := range s.values {
		if !strings.HasPrefix(value, "s:") || !strings.HasPrefix(strings.TrimPrefix(value, "s:"), prefix) {
			return false
		}
	}
	return true
}

func (s predicateValueSet) disjointFrom(other predicateExclusionSet) bool {
	for value := range s.values {
		if _, ok := other.values[value]; ok {
			return false
		}
	}
	return true
}

func (s predicateExclusionSet) implies(other predicateExclusionSet) bool {
	if s.exprKey != other.exprKey {
		return false
	}
	if s.nullsIncluded && !other.nullsIncluded {
		return false
	}
	for value := range other.values {
		if _, ok := s.values[value]; !ok {
			return false
		}
	}
	return true
}

func predicateValueSetFromExpr(expr tree.Expr) (predicateValueSet, bool) {
	comparison, ok := unwrapPredicateParens(expr).(*tree.ComparisonExpr)
	if !ok {
		return predicateValueSet{}, false
	}
	switch comparison.Operator {
	case tree.EQ, tree.IsNotDistinctFrom:
		if exprKey, ok := predicateComparableExprKey(comparison.Left); ok {
			if value, ok := predicateLiteralKey(comparison.Right); ok {
				return predicateValueSet{exprKey: exprKey, values: map[string]struct{}{value: {}}}, true
			}
		}
		if exprKey, ok := predicateComparableExprKey(comparison.Right); ok {
			if value, ok := predicateLiteralKey(comparison.Left); ok {
				return predicateValueSet{exprKey: exprKey, values: map[string]struct{}{value: {}}}, true
			}
		}
	case tree.In:
		exprKey, ok := predicateComparableExprKey(comparison.Left)
		if !ok {
			return predicateValueSet{}, false
		}
		tuple, ok := unwrapPredicateParens(comparison.Right).(*tree.Tuple)
		if !ok || len(tuple.Exprs) == 0 {
			return predicateValueSet{}, false
		}
		values := make(map[string]struct{}, len(tuple.Exprs))
		for _, expr := range tuple.Exprs {
			value, ok := predicateLiteralKey(expr)
			if !ok {
				return predicateValueSet{}, false
			}
			values[value] = struct{}{}
		}
		return predicateValueSet{exprKey: exprKey, values: values}, true
	}
	return predicateValueSet{}, false
}

func predicateExclusionSetFromExpr(expr tree.Expr) (predicateExclusionSet, bool) {
	expr = unwrapPredicateParens(expr)
	if notExpr, ok := expr.(*tree.NotExpr); ok {
		comparison, ok := unwrapPredicateParens(notExpr.Expr).(*tree.ComparisonExpr)
		if !ok || comparison.Operator != tree.In {
			return predicateExclusionSet{}, false
		}
		return predicateExclusionSetFromInComparison(comparison.Left, comparison.Right)
	}
	comparison, ok := expr.(*tree.ComparisonExpr)
	if !ok {
		return predicateExclusionSet{}, false
	}
	switch comparison.Operator {
	case tree.NE, tree.IsDistinctFrom:
		nullsIncluded := comparison.Operator == tree.IsDistinctFrom
		if nullsIncluded && (predicateIsNullLiteral(comparison.Left) || predicateIsNullLiteral(comparison.Right)) {
			return predicateExclusionSet{}, false
		}
		if exprKey, ok := predicateComparableExprKey(comparison.Left); ok {
			if value, ok := predicateLiteralKey(comparison.Right); ok {
				return predicateExclusionSet{exprKey: exprKey, values: map[string]struct{}{value: {}}, nullsIncluded: nullsIncluded}, true
			}
		}
		if exprKey, ok := predicateComparableExprKey(comparison.Right); ok {
			if value, ok := predicateLiteralKey(comparison.Left); ok {
				return predicateExclusionSet{exprKey: exprKey, values: map[string]struct{}{value: {}}, nullsIncluded: nullsIncluded}, true
			}
		}
	case tree.NotIn:
		return predicateExclusionSetFromInComparison(comparison.Left, comparison.Right)
	}
	return predicateExclusionSet{}, false
}

func predicateExclusionSetFromInComparison(left tree.Expr, right tree.Expr) (predicateExclusionSet, bool) {
	exprKey, ok := predicateComparableExprKey(left)
	if !ok {
		return predicateExclusionSet{}, false
	}
	tuple, ok := unwrapPredicateParens(right).(*tree.Tuple)
	if !ok || len(tuple.Exprs) == 0 {
		return predicateExclusionSet{}, false
	}
	values := make(map[string]struct{}, len(tuple.Exprs))
	for _, expr := range tuple.Exprs {
		value, ok := predicateLiteralKey(expr)
		if !ok {
			return predicateExclusionSet{}, false
		}
		values[value] = struct{}{}
	}
	return predicateExclusionSet{exprKey: exprKey, values: values}, true
}

func predicatePrefixLikeFromExpr(expr tree.Expr) (predicatePrefixLike, bool) {
	comparison, ok := unwrapPredicateParens(expr).(*tree.ComparisonExpr)
	if !ok || comparison.Operator != tree.Like {
		return predicatePrefixLike{}, false
	}
	exprKey, ok := predicateComparableExprKey(comparison.Left)
	if !ok {
		return predicatePrefixLike{}, false
	}
	pattern, ok := predicateStringLiteral(comparison.Right)
	if !ok {
		return predicatePrefixLike{}, false
	}
	prefix, ok := predicateLikePrefix(pattern, '\\')
	if !ok {
		return predicatePrefixLike{}, false
	}
	return predicatePrefixLike{exprKey: exprKey, prefix: prefix}, true
}

func predicateComparableExprKey(expr tree.Expr) (string, bool) {
	expr = unwrapPredicateParens(expr)
	if column, ok := predicateColumnName(expr); ok {
		return "column:" + column, true
	}
	if coalesce, ok := expr.(*tree.CoalesceExpr); ok {
		return predicateCoalesceExprKey(coalesce)
	}
	if nullif, ok := expr.(*tree.NullIfExpr); ok {
		return predicateNullIfExprKey(nullif)
	}
	if binary, ok := expr.(*tree.BinaryExpr); ok {
		return predicateBinaryExprKey(binary)
	}
	fn, ok := expr.(*tree.FuncExpr)
	if !ok {
		return "", false
	}
	name, ok := predicateFunctionName(fn.Func)
	if !ok {
		return "", false
	}
	if name == "strpos" {
		return predicateFunctionCallExprKey(name, fn.Exprs, 2)
	}
	if name == "starts_with" {
		return predicateFunctionCallExprKey(name, fn.Exprs, 2)
	}
	if name == "btrim" || name == "ltrim" || name == "rtrim" {
		return predicateVariableArityFunctionCallExprKey(name, fn.Exprs, 1, 2)
	}
	if name == "left" || name == "right" {
		return predicateFunctionCallExprKey(name, fn.Exprs, 2)
	}
	if name == "lpad" || name == "rpad" {
		return predicateVariableArityFunctionCallExprKey(name, fn.Exprs, 2, 3)
	}
	if name == "repeat" {
		return predicateFunctionCallExprKey(name, fn.Exprs, 2)
	}
	if name == "gcd" || name == "lcm" {
		return predicateCommutativeFunctionCallExprKey(name, fn.Exprs, 2)
	}
	if name == "mod" {
		return predicateFunctionCallExprKey(name, fn.Exprs, 2)
	}
	if name == "concat" {
		return predicateVariadicFunctionCallExprKey(name, fn.Exprs, 1)
	}
	if name == "replace" {
		return predicateFunctionCallExprKey(name, fn.Exprs, 3)
	}
	if name == "translate" {
		return predicateFunctionCallExprKey(name, fn.Exprs, 3)
	}
	if name == "split_part" {
		return predicateFunctionCallExprKey(name, fn.Exprs, 3)
	}
	if name == "substr" || name == "substring" {
		return predicateSubstringExprKey(name, fn.Exprs)
	}
	if len(fn.Exprs) != 1 {
		return "", false
	}
	name, ok = predicateCanonicalUnaryFunction(name)
	if !ok {
		return "", false
	}
	argKey, ok := predicateComparableExprKey(fn.Exprs[0])
	if !ok {
		return "", false
	}
	return "func:" + name + "(" + argKey + ")", true
}

func predicateCoalesceExprKey(expr *tree.CoalesceExpr) (string, bool) {
	if len(expr.Exprs) == 0 {
		return "", false
	}
	parts := make([]string, 0, len(expr.Exprs))
	for _, child := range expr.Exprs {
		childKey, ok := predicateFunctionArgumentExprKey(child)
		if !ok {
			return "", false
		}
		parts = append(parts, childKey)
	}
	return "func:coalesce(" + strings.Join(parts, ",") + ")", true
}

func predicateNullIfExprKey(expr *tree.NullIfExpr) (string, bool) {
	leftKey, ok := predicateFunctionArgumentExprKey(expr.Expr1)
	if !ok {
		return "", false
	}
	rightKey, ok := predicateFunctionArgumentExprKey(expr.Expr2)
	if !ok {
		return "", false
	}
	return "func:nullif(" + leftKey + "," + rightKey + ")", true
}

func predicateBinaryExprKey(expr *tree.BinaryExpr) (string, bool) {
	op, ok := predicateArithmeticBinaryOperator(expr.Operator)
	if !ok {
		return "", false
	}
	leftKey, ok := predicateFunctionArgumentExprKey(expr.Left)
	if !ok {
		return "", false
	}
	rightKey, ok := predicateFunctionArgumentExprKey(expr.Right)
	if !ok {
		return "", false
	}
	if predicateCommutativeArithmeticOperator(op) && rightKey < leftKey {
		leftKey, rightKey = rightKey, leftKey
	}
	return "binary:" + op + "(" + leftKey + "," + rightKey + ")", true
}

func predicateCommutativeArithmeticOperator(op string) bool {
	return op == "+" || op == "*"
}

func predicateArithmeticBinaryOperator(op tree.BinaryOperator) (string, bool) {
	switch op {
	case tree.Plus:
		return "+", true
	case tree.Minus:
		return "-", true
	case tree.Mult:
		return "*", true
	default:
		return "", false
	}
}

func predicateFunctionCallExprKey(name string, exprs tree.Exprs, expectedArgs int) (string, bool) {
	if len(exprs) != expectedArgs {
		return "", false
	}
	parts := make([]string, 0, len(exprs))
	for _, child := range exprs {
		childKey, ok := predicateFunctionArgumentExprKey(child)
		if !ok {
			return "", false
		}
		parts = append(parts, childKey)
	}
	return "func:" + name + "(" + strings.Join(parts, ",") + ")", true
}

func predicateCommutativeFunctionCallExprKey(name string, exprs tree.Exprs, expectedArgs int) (string, bool) {
	if len(exprs) != expectedArgs {
		return "", false
	}
	parts := make([]string, 0, len(exprs))
	for _, child := range exprs {
		childKey, ok := predicateFunctionArgumentExprKey(child)
		if !ok {
			return "", false
		}
		parts = append(parts, childKey)
	}
	if len(parts) == 2 && parts[1] < parts[0] {
		parts[0], parts[1] = parts[1], parts[0]
	}
	return "func:" + name + "(" + strings.Join(parts, ",") + ")", true
}

func predicateVariableArityFunctionCallExprKey(name string, exprs tree.Exprs, minArgs int, maxArgs int) (string, bool) {
	if len(exprs) < minArgs || len(exprs) > maxArgs {
		return "", false
	}
	return predicateFunctionCallExprKeyParts(name, exprs)
}

func predicateVariadicFunctionCallExprKey(name string, exprs tree.Exprs, minArgs int) (string, bool) {
	if len(exprs) < minArgs {
		return "", false
	}
	return predicateFunctionCallExprKeyParts(name, exprs)
}

func predicateFunctionCallExprKeyParts(name string, exprs tree.Exprs) (string, bool) {
	parts := make([]string, 0, len(exprs))
	for _, child := range exprs {
		childKey, ok := predicateFunctionArgumentExprKey(child)
		if !ok {
			return "", false
		}
		parts = append(parts, childKey)
	}
	return "func:" + name + "(" + strings.Join(parts, ",") + ")", true
}

func predicateSubstringExprKey(name string, exprs tree.Exprs) (string, bool) {
	if len(exprs) != 2 && len(exprs) != 3 {
		return "", false
	}
	parts := make([]string, 0, len(exprs))
	for i, child := range exprs {
		if i > 0 {
			if literalKey, ok := predicateLiteralKey(child); ok && strings.HasPrefix(literalKey, "s:") {
				return "", false
			}
		}
		childKey, ok := predicateFunctionArgumentExprKey(child)
		if !ok {
			return "", false
		}
		parts = append(parts, childKey)
	}
	return "func:substring(" + strings.Join(parts, ",") + ")", true
}

func predicateFunctionArgumentExprKey(expr tree.Expr) (string, bool) {
	if exprKey, ok := predicateComparableExprKey(expr); ok {
		return exprKey, true
	}
	if literalKey, ok := predicateLiteralKey(expr); ok {
		return "literal:" + literalKey, true
	}
	if predicateIsNullLiteral(expr) {
		return "literal:null", true
	}
	return "", false
}

func predicateCanonicalUnaryFunction(name string) (string, bool) {
	switch name {
	case "abs":
		return name, true
	case "floor":
		return name, true
	case "ceil", "ceiling":
		return "ceil", true
	case "round":
		return name, true
	case "trunc":
		return name, true
	case "sign":
		return name, true
	case "bit_length":
		return name, true
	case "octet_length":
		return name, true
	case "ascii", "lower", "upper", "btrim", "ltrim", "rtrim", "md5", "hashtext", "reverse", "to_hex", "initcap", "quote_literal", "quote_ident", "chr":
		return name, true
	case "char_length", "character_length", "length":
		return "length", true
	default:
		return "", false
	}
}

func predicateFunctionName(ref tree.ResolvableFunctionReference) (string, bool) {
	switch fn := ref.FunctionReference.(type) {
	case *tree.UnresolvedName:
		if fn.Star || fn.NumParts == 0 {
			return "", false
		}
		return strings.ToLower(strings.Trim(fn.Parts[0], `"`)), true
	case *tree.FunctionDefinition:
		return strings.ToLower(strings.Trim(fn.Name, `"`)), true
	default:
		return "", false
	}
}

func predicateLikePrefix(pattern string, escape byte) (string, bool) {
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

func predicateLiteralKey(expr tree.Expr) (string, bool) {
	if value, ok := predicateNumericConstant(expr); ok {
		return "n:" + strconv.FormatFloat(value, 'g', -1, 64), true
	}
	if value, ok := predicateBoolConstant(expr); ok {
		return "b:" + strconv.FormatBool(value), true
	}
	switch expr := unwrapPredicateParens(expr).(type) {
	case *tree.DString:
		return "s:" + string(*expr), true
	case *tree.StrVal:
		return "s:" + expr.RawString(), true
	}
	return "", false
}

func predicateStringLiteral(expr tree.Expr) (string, bool) {
	switch expr := unwrapPredicateParens(expr).(type) {
	case *tree.DString:
		return string(*expr), true
	case *tree.StrVal:
		return expr.RawString(), true
	default:
		return "", false
	}
}

type numericPredicateRangeWithColumn struct {
	column string
	bounds numericPredicateRange
}

func numericPredicateRangeFromExpr(expr tree.Expr) (numericPredicateRangeWithColumn, bool) {
	switch expr := unwrapPredicateParens(expr).(type) {
	case *tree.RangeCond:
		if expr.Not || expr.Symmetric {
			return numericPredicateRangeWithColumn{}, false
		}
		column, ok := predicateColumnName(expr.Left)
		if !ok {
			return numericPredicateRangeWithColumn{}, false
		}
		from, ok := predicateNumericConstant(expr.From)
		if !ok {
			return numericPredicateRangeWithColumn{}, false
		}
		to, ok := predicateNumericConstant(expr.To)
		if !ok || from > to {
			return numericPredicateRangeWithColumn{}, false
		}
		return numericPredicateRangeWithColumn{
			column: column,
			bounds: numericPredicateRange{
				hasLower:       true,
				lower:          from,
				lowerInclusive: true,
				hasUpper:       true,
				upper:          to,
				upperInclusive: true,
			},
		}, true
	default:
		comparison, ok := numericPredicateComparisonFromExpr(expr)
		if !ok {
			return numericPredicateRangeWithColumn{}, false
		}
		return numericPredicateRangeWithColumn{
			column: comparison.column,
			bounds: numericComparisonRange(comparison),
		}, true
	}
}

func numericPredicateComparisonFromExpr(expr tree.Expr) (numericPredicateComparison, bool) {
	switch expr := expr.(type) {
	case *tree.ParenExpr:
		return numericPredicateComparisonFromExpr(expr.Expr)
	case *tree.ComparisonExpr:
		if column, ok := predicateColumnName(expr.Left); ok {
			if value, ok := predicateNumericConstant(expr.Right); ok {
				return numericPredicateComparison{column: column, op: expr.Operator, value: value}, true
			}
		}
		if column, ok := predicateColumnName(expr.Right); ok {
			if value, ok := predicateNumericConstant(expr.Left); ok {
				return numericPredicateComparison{column: column, op: reverseComparisonOperator(expr.Operator), value: value}, true
			}
		}
	}
	return numericPredicateComparison{}, false
}

func booleanPredicateComparisonFromExpr(expr tree.Expr) (booleanPredicateComparison, bool) {
	switch expr := expr.(type) {
	case *tree.ParenExpr:
		return booleanPredicateComparisonFromExpr(expr.Expr)
	case *tree.UnresolvedName:
		exprKey, ok := predicateBooleanExprKey(expr)
		return booleanPredicateComparison{exprKey: exprKey, value: true}, ok
	case *tree.FuncExpr:
		exprKey, ok := predicateBooleanExprKey(expr)
		return booleanPredicateComparison{exprKey: exprKey, value: true}, ok
	case *tree.NotExpr:
		if exprKey, ok := predicateBooleanExprKey(expr.Expr); ok {
			return booleanPredicateComparison{exprKey: exprKey, value: false}, true
		}
	case *tree.ComparisonExpr:
		if expr.Operator != tree.EQ && expr.Operator != tree.IsNotDistinctFrom {
			return booleanPredicateComparison{}, false
		}
		if exprKey, ok := predicateBooleanExprKey(expr.Left); ok {
			if value, ok := predicateBoolConstant(expr.Right); ok {
				return booleanPredicateComparison{exprKey: exprKey, value: value}, true
			}
		}
		if exprKey, ok := predicateBooleanExprKey(expr.Right); ok {
			if value, ok := predicateBoolConstant(expr.Left); ok {
				return booleanPredicateComparison{exprKey: exprKey, value: value}, true
			}
		}
	}
	return booleanPredicateComparison{}, false
}

func predicateBooleanExprKey(expr tree.Expr) (string, bool) {
	expr = unwrapPredicateParens(expr)
	if column, ok := predicateColumnName(expr); ok {
		return "column:" + column, true
	}
	fn, ok := expr.(*tree.FuncExpr)
	if !ok {
		return "", false
	}
	name, ok := predicateFunctionName(fn.Func)
	if !ok || name != "starts_with" {
		return "", false
	}
	return predicateFunctionCallExprKey(name, fn.Exprs, 2)
}

func predicateColumnName(expr tree.Expr) (string, bool) {
	switch expr := expr.(type) {
	case *tree.ParenExpr:
		return predicateColumnName(expr.Expr)
	case *tree.UnresolvedName:
		if expr.Star || expr.NumParts == 0 {
			return "", false
		}
		return strings.ToLower(strings.Trim(expr.Parts[0], `"`)), true
	}
	return "", false
}

func predicateBoolConstant(expr tree.Expr) (bool, bool) {
	switch expr := expr.(type) {
	case *tree.ParenExpr:
		return predicateBoolConstant(expr.Expr)
	case *tree.DBool:
		return bool(*expr), true
	}
	return false, false
}

func predicateNumericConstant(expr tree.Expr) (float64, bool) {
	switch expr := expr.(type) {
	case *tree.ParenExpr:
		return predicateNumericConstant(expr.Expr)
	case *tree.UnaryExpr:
		if expr.Operator != tree.UnaryMinus {
			return 0, false
		}
		value, ok := predicateNumericConstant(expr.Expr)
		if !ok {
			return 0, false
		}
		return -value, true
	case *tree.DInt:
		return float64(*expr), true
	case *tree.NumVal:
		value, err := strconv.ParseFloat(expr.FormattedString(), 64)
		return value, err == nil
	}
	return 0, false
}

func reverseComparisonOperator(op tree.ComparisonOperator) tree.ComparisonOperator {
	switch op {
	case tree.LT:
		return tree.GT
	case tree.LE:
		return tree.GE
	case tree.GT:
		return tree.LT
	case tree.GE:
		return tree.LE
	default:
		return op
	}
}

func numericComparisonRange(comparison numericPredicateComparison) numericPredicateRange {
	switch comparison.op {
	case tree.EQ:
		return numericPredicateRange{
			hasLower:       true,
			lower:          comparison.value,
			lowerInclusive: true,
			hasUpper:       true,
			upper:          comparison.value,
			upperInclusive: true,
		}
	case tree.GT:
		return numericPredicateRange{hasLower: true, lower: comparison.value}
	case tree.GE:
		return numericPredicateRange{hasLower: true, lower: comparison.value, lowerInclusive: true}
	case tree.LT:
		return numericPredicateRange{hasUpper: true, upper: comparison.value}
	case tree.LE:
		return numericPredicateRange{hasUpper: true, upper: comparison.value, upperInclusive: true}
	default:
		return numericPredicateRange{}
	}
}

func (r numericPredicateRange) subsetOf(other numericPredicateRange) bool {
	if !r.valid() || !other.valid() {
		return false
	}
	if other.hasLower {
		if !r.hasLower {
			return false
		}
		if r.lower < other.lower {
			return false
		}
		if r.lower == other.lower && r.lowerInclusive && !other.lowerInclusive {
			return false
		}
	}
	if other.hasUpper {
		if !r.hasUpper {
			return false
		}
		if r.upper > other.upper {
			return false
		}
		if r.upper == other.upper && r.upperInclusive && !other.upperInclusive {
			return false
		}
	}
	return true
}

func (r numericPredicateRange) valid() bool {
	if r.hasLower && math.IsNaN(r.lower) {
		return false
	}
	if r.hasUpper && math.IsNaN(r.upper) {
		return false
	}
	return r.hasLower || r.hasUpper
}
