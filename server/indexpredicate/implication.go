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
			if indexValues.exprKey == queryValues.exprKey {
				return queryValues.subsetOf(indexValues)
			}
			return predicateTransformedArgumentValueSetImplies(indexValues, queryValues)
		}
		if queryRange, ok := numericPredicateRangeFromExpr(queryExpr); ok {
			return predicateSignArgumentRangeImplies(indexValues, queryRange)
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

func predicateTransformedArgumentValueSetImplies(indexValues predicateValueSet, queryValues predicateValueSet) bool {
	return predicateAbsArgumentValueSetImplies(indexValues, queryValues) ||
		predicateCaseFoldArgumentValueSetImplies(indexValues, queryValues) ||
		predicateReverseArgumentValueSetImplies(indexValues, queryValues) ||
		predicateAsciiArgumentValueSetImplies(indexValues, queryValues) ||
		predicateSubstringArgumentValueSetImplies(indexValues, queryValues) ||
		predicateLeftRightArgumentValueSetImplies(indexValues, queryValues) ||
		predicateSignArgumentValueSetImplies(indexValues, queryValues)
}

func predicateAbsArgumentValueSetImplies(indexValues predicateValueSet, queryValues predicateValueSet) bool {
	argumentKey, ok := predicateUnaryFunctionArgumentExprKey(indexValues.exprKey, "abs")
	if !ok || queryValues.exprKey != argumentKey {
		return false
	}
	absoluteValues := make(map[string]struct{}, len(queryValues.values))
	for value := range queryValues.values {
		absoluteValue, ok := predicateAbsNumericLiteralKey(value)
		if !ok {
			return false
		}
		absoluteValues[absoluteValue] = struct{}{}
	}
	return predicateValueSet{exprKey: indexValues.exprKey, values: absoluteValues}.subsetOf(indexValues)
}

func predicateCaseFoldArgumentValueSetImplies(indexValues predicateValueSet, queryValues predicateValueSet) bool {
	functionName, argumentKey, ok := predicateCaseFoldArgumentExprKey(indexValues.exprKey)
	if !ok || queryValues.exprKey != argumentKey {
		return false
	}
	foldedValues := make(map[string]struct{}, len(queryValues.values))
	for value := range queryValues.values {
		foldedValue, ok := predicateCaseFoldStringLiteralKey(functionName, value)
		if !ok {
			return false
		}
		foldedValues[foldedValue] = struct{}{}
	}
	return predicateValueSet{exprKey: indexValues.exprKey, values: foldedValues}.subsetOf(indexValues)
}

func predicateCaseFoldArgumentExprKey(exprKey string) (string, string, bool) {
	for _, functionName := range []string{"lower", "upper"} {
		if argumentKey, ok := predicateUnaryFunctionArgumentExprKey(exprKey, functionName); ok {
			return functionName, argumentKey, true
		}
	}
	return "", "", false
}

func predicateReverseArgumentValueSetImplies(indexValues predicateValueSet, queryValues predicateValueSet) bool {
	argumentKey, ok := predicateUnaryFunctionArgumentExprKey(indexValues.exprKey, "reverse")
	if !ok || queryValues.exprKey != argumentKey {
		return false
	}
	reversedValues := make(map[string]struct{}, len(queryValues.values))
	for value := range queryValues.values {
		reversedValue, ok := predicateReverseStringLiteralKey(value)
		if !ok {
			return false
		}
		reversedValues[reversedValue] = struct{}{}
	}
	return predicateValueSet{exprKey: indexValues.exprKey, values: reversedValues}.subsetOf(indexValues)
}

func predicateUnaryFunctionArgumentExprKey(exprKey string, functionName string) (string, bool) {
	prefix := "func:" + functionName + "("
	if !strings.HasPrefix(exprKey, prefix) || !strings.HasSuffix(exprKey, ")") {
		return "", false
	}
	return strings.TrimSuffix(strings.TrimPrefix(exprKey, prefix), ")"), true
}

func predicateAbsNumericLiteralKey(value string) (string, bool) {
	const prefix = "n:"
	if !strings.HasPrefix(value, prefix) {
		return "", false
	}
	number, err := strconv.ParseFloat(strings.TrimPrefix(value, prefix), 64)
	if err != nil {
		return "", false
	}
	return prefix + strconv.FormatFloat(math.Abs(number), 'g', -1, 64), true
}

func predicateCaseFoldStringLiteralKey(functionName string, value string) (string, bool) {
	const prefix = "s:"
	if !strings.HasPrefix(value, prefix) {
		return "", false
	}
	value = strings.TrimPrefix(value, prefix)
	switch functionName {
	case "lower":
		value = strings.ToLower(value)
	case "upper":
		value = strings.ToUpper(value)
	default:
		return "", false
	}
	return prefix + value, true
}

func predicateReverseStringLiteralKey(value string) (string, bool) {
	const prefix = "s:"
	if !strings.HasPrefix(value, prefix) {
		return "", false
	}
	return prefix + predicateReverseText(strings.TrimPrefix(value, prefix)), true
}

func predicateReverseText(text string) string {
	runes := []rune(text)
	for left, right := 0, len(runes)-1; left < right; left, right = left+1, right-1 {
		runes[left], runes[right] = runes[right], runes[left]
	}
	return string(runes)
}

func predicateAsciiArgumentValueSetImplies(indexValues predicateValueSet, queryValues predicateValueSet) bool {
	argumentKey, ok := predicateUnaryFunctionArgumentExprKey(indexValues.exprKey, "ascii")
	if !ok || queryValues.exprKey != argumentKey {
		return false
	}
	asciiValues := make(map[string]struct{}, len(queryValues.values))
	for value := range queryValues.values {
		asciiValue, ok := predicateAsciiStringLiteralKey(value)
		if !ok {
			return false
		}
		asciiValues[asciiValue] = struct{}{}
	}
	return predicateValueSet{exprKey: indexValues.exprKey, values: asciiValues}.subsetOf(indexValues)
}

func predicateAsciiStringLiteralKey(value string) (string, bool) {
	const prefix = "s:"
	if !strings.HasPrefix(value, prefix) {
		return "", false
	}
	text := strings.TrimPrefix(value, prefix)
	for _, r := range text {
		return "n:" + strconv.FormatInt(int64(r), 10), true
	}
	return "n:0", true
}

func predicateSubstringArgumentValueSetImplies(indexValues predicateValueSet, queryValues predicateValueSet) bool {
	argumentKey, start, count, hasCount, ok := predicateSubstringArgumentExprKey(indexValues.exprKey)
	if !ok || queryValues.exprKey != argumentKey {
		return false
	}
	substringValues := make(map[string]struct{}, len(queryValues.values))
	for value := range queryValues.values {
		substringValue, ok := predicateSubstringStringLiteralKey(value, start, count, hasCount)
		if !ok {
			return false
		}
		substringValues[substringValue] = struct{}{}
	}
	return predicateValueSet{exprKey: indexValues.exprKey, values: substringValues}.subsetOf(indexValues)
}

func predicateSubstringArgumentExprKey(exprKey string) (string, int64, int64, bool, bool) {
	const prefix = "func:substring("
	if !strings.HasPrefix(exprKey, prefix) || !strings.HasSuffix(exprKey, ")") {
		return "", 0, 0, false, false
	}
	parts := strings.Split(strings.TrimSuffix(strings.TrimPrefix(exprKey, prefix), ")"), ",")
	if len(parts) != 2 && len(parts) != 3 {
		return "", 0, 0, false, false
	}
	start, ok := predicateIntegerFunctionArgumentKey(parts[1])
	if !ok {
		return "", 0, 0, false, false
	}
	if len(parts) == 2 {
		return parts[0], start, 0, false, true
	}
	count, ok := predicateIntegerFunctionArgumentKey(parts[2])
	if !ok {
		return "", 0, 0, false, false
	}
	return parts[0], start, count, true, true
}

func predicateIntegerFunctionArgumentKey(exprKey string) (int64, bool) {
	const prefix = "literal:n:"
	if !strings.HasPrefix(exprKey, prefix) {
		return 0, false
	}
	value, err := strconv.ParseInt(strings.TrimPrefix(exprKey, prefix), 10, 64)
	return value, err == nil
}

func predicateSubstringStringLiteralKey(value string, start int64, count int64, hasCount bool) (string, bool) {
	const prefix = "s:"
	if !strings.HasPrefix(value, prefix) {
		return "", false
	}
	text, ok := predicateSubstringText(strings.TrimPrefix(value, prefix), start, count, hasCount)
	if !ok {
		return "", false
	}
	return prefix + text, true
}

func predicateSubstringText(text string, start int64, count int64, hasCount bool) (string, bool) {
	runes := []rune(text)
	runeCount := int64(len(runes))
	if !hasCount {
		if start < 1 {
			start = 1
		}
		start--
		if start >= runeCount {
			return "", true
		}
		return string(runes[start:]), true
	}
	if count < 0 {
		return "", false
	}
	if count == 0 {
		return "", true
	}
	if start < 1 {
		if start <= 1-count {
			return "", true
		}
		count -= 1 - start
		start = 0
	} else {
		start--
	}
	if count <= 0 {
		return "", true
	}
	if start >= runeCount {
		return "", true
	}
	if count > runeCount-start {
		return string(runes[start:]), true
	}
	return string(runes[start : start+count]), true
}

func predicateLeftRightArgumentValueSetImplies(indexValues predicateValueSet, queryValues predicateValueSet) bool {
	functionName, argumentKey, count, ok := predicateLeftRightArgumentExprKey(indexValues.exprKey)
	if !ok || queryValues.exprKey != argumentKey {
		return false
	}
	transformedValues := make(map[string]struct{}, len(queryValues.values))
	for value := range queryValues.values {
		transformedValue, ok := predicateLeftRightStringLiteralKey(functionName, value, count)
		if !ok {
			return false
		}
		transformedValues[transformedValue] = struct{}{}
	}
	return predicateValueSet{exprKey: indexValues.exprKey, values: transformedValues}.subsetOf(indexValues)
}

func predicateLeftRightArgumentExprKey(exprKey string) (string, string, int64, bool) {
	for _, functionName := range []string{"left", "right"} {
		if argumentKey, count, ok := predicateIntegerArgumentFunctionExprKey(exprKey, functionName); ok {
			return functionName, argumentKey, count, true
		}
	}
	return "", "", 0, false
}

func predicateIntegerArgumentFunctionExprKey(exprKey string, functionName string) (string, int64, bool) {
	prefix := "func:" + functionName + "("
	if !strings.HasPrefix(exprKey, prefix) || !strings.HasSuffix(exprKey, ")") {
		return "", 0, false
	}
	inner := strings.TrimSuffix(strings.TrimPrefix(exprKey, prefix), ")")
	const marker = ",literal:n:"
	idx := strings.LastIndex(inner, marker)
	if idx < 0 {
		return "", 0, false
	}
	count, ok := predicateIntegerFunctionArgumentKey("literal:n:" + inner[idx+len(marker):])
	if !ok {
		return "", 0, false
	}
	return inner[:idx], count, true
}

func predicateLeftRightStringLiteralKey(functionName string, value string, count int64) (string, bool) {
	const prefix = "s:"
	if !strings.HasPrefix(value, prefix) {
		return "", false
	}
	text := strings.TrimPrefix(value, prefix)
	switch functionName {
	case "left":
		text = predicateLeftText(text, count)
	case "right":
		text = predicateRightText(text, count)
	default:
		return "", false
	}
	return prefix + text, true
}

func predicateLeftText(text string, count int64) string {
	runes := []rune(text)
	runeCount := int64(len(runes))
	if count >= 0 {
		if count >= runeCount {
			return text
		}
		return string(runes[:count])
	}
	keep := runeCount + count
	if keep <= 0 {
		return ""
	}
	return string(runes[:keep])
}

func predicateRightText(text string, count int64) string {
	runes := []rune(text)
	runeCount := int64(len(runes))
	if count >= 0 {
		if count >= runeCount {
			return text
		}
		return string(runes[runeCount-count:])
	}
	if count == -1<<63 {
		return ""
	}
	skip := -count
	if skip >= runeCount {
		return ""
	}
	return string(runes[skip:])
}

func predicateSignArgumentValueSetImplies(indexValues predicateValueSet, queryValues predicateValueSet) bool {
	argumentKey, ok := predicateUnaryFunctionArgumentExprKey(indexValues.exprKey, "sign")
	if !ok || queryValues.exprKey != argumentKey {
		return false
	}
	signValues := make(map[string]struct{}, len(queryValues.values))
	for value := range queryValues.values {
		signValue, ok := predicateSignNumericLiteralKey(value)
		if !ok {
			return false
		}
		signValues[signValue] = struct{}{}
	}
	return predicateValueSet{exprKey: indexValues.exprKey, values: signValues}.subsetOf(indexValues)
}

func predicateSignArgumentRangeImplies(indexValues predicateValueSet, queryRange numericPredicateRangeWithColumn) bool {
	argumentKey, ok := predicateUnaryFunctionArgumentExprKey(indexValues.exprKey, "sign")
	if !ok || argumentKey != "column:"+queryRange.column {
		return false
	}
	signValues, ok := predicateSignRangeLiteralKeys(queryRange.bounds)
	if !ok {
		return false
	}
	return predicateValueSet{exprKey: indexValues.exprKey, values: signValues}.subsetOf(indexValues)
}

func predicateSignNumericLiteralKey(value string) (string, bool) {
	const prefix = "n:"
	if !strings.HasPrefix(value, prefix) {
		return "", false
	}
	number, err := strconv.ParseFloat(strings.TrimPrefix(value, prefix), 64)
	if err != nil {
		return "", false
	}
	return predicateSignLiteralKey(number), true
}

func predicateSignRangeLiteralKeys(bounds numericPredicateRange) (map[string]struct{}, bool) {
	if !bounds.valid() {
		return nil, false
	}
	if bounds.hasLower && (bounds.lower > 0 || (bounds.lower == 0 && !bounds.lowerInclusive)) {
		return map[string]struct{}{predicateSignLiteralKey(1): {}}, true
	}
	if bounds.hasUpper && (bounds.upper < 0 || (bounds.upper == 0 && !bounds.upperInclusive)) {
		return map[string]struct{}{predicateSignLiteralKey(-1): {}}, true
	}
	if bounds.hasLower && bounds.hasUpper && bounds.lower == 0 && bounds.upper == 0 &&
		bounds.lowerInclusive && bounds.upperInclusive {
		return map[string]struct{}{predicateSignLiteralKey(0): {}}, true
	}
	return nil, false
}

func predicateSignLiteralKey(number float64) string {
	switch {
	case number > 0:
		return "n:1"
	case number < 0:
		return "n:-1"
	default:
		return "n:0"
	}
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
