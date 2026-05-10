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
	"unicode"

	"github.com/dolthub/go-mysql-server/sql"
	gmsexpression "github.com/dolthub/go-mysql-server/sql/expression"

	pgexpression "github.com/dolthub/doltgresql/server/expression"
	"github.com/dolthub/doltgresql/server/indexmetadata"
	"github.com/dolthub/doltgresql/server/indexpredicate"
)

type partialBtreeLookup struct {
	index     sql.Index
	predicate string
	columns   []partialBtreeLookupColumn
}

type partialBtreeLookupColumn struct {
	columnName string
	expression string
	typ        sql.Type
}

type partialPlannerHiddenIndex struct {
	sql.Index
}

func (i partialPlannerHiddenIndex) CanSupport(*sql.Context, ...sql.Range) bool {
	return false
}

func unwrapPlannerIndexLookup(lookup sql.IndexLookup) (sql.IndexLookup, bool) {
	if hidden, ok := lookup.Index.(partialPlannerHiddenIndex); ok {
		lookup.Index = hidden.Index
		return lookup, true
	}
	if ordered, ok := lookup.Index.(metadataOnlyOrderedIndex); ok {
		lookup.Index = ordered.Index
		return lookup, true
	}
	return lookup, false
}

func partialBtreeIndexLookup(ctx *sql.Context, index sql.Index, tableSchema sql.Schema) (partialBtreeLookup, bool) {
	predicate := indexmetadata.Predicate(index.Comment())
	if predicate == "" || indexmetadata.AccessMethod(index.IndexType(), index.Comment()) != indexmetadata.AccessMethodBtree {
		return partialBtreeLookup{}, false
	}
	logicalColumns := indexmetadata.LogicalColumns(index, tableSchema)
	columnTypes := index.ColumnExpressionTypes(ctx)
	indexExpressions := index.Expressions()
	if len(indexExpressions) == 0 || len(columnTypes) < len(indexExpressions) {
		return partialBtreeLookup{}, false
	}
	columns := make([]partialBtreeLookupColumn, 0, len(indexExpressions))
	for i := range indexExpressions {
		columnName := unqualifiedPlannerIndexColumn(indexExpressions[i])
		if i < len(logicalColumns) {
			logicalColumn := logicalColumns[i]
			if logicalColumn.Expression {
				return partialBtreeLookup{}, false
			}
			if logicalColumn.StorageName != "" {
				columnName = logicalColumn.StorageName
			}
		}
		if columnName == "" || tableSchema.IndexOfColName(columnName) < 0 {
			return partialBtreeLookup{}, false
		}
		columns = append(columns, partialBtreeLookupColumn{
			columnName: columnName,
			expression: columnTypes[i].Expression,
			typ:        columnTypes[i].Type,
		})
	}
	return partialBtreeLookup{
		index:     index,
		predicate: predicate,
		columns:   columns,
	}, true
}

func (t *BtreePlannerBoundaryTable) lookupForPartialIndexes(ctx *sql.Context, exprs ...sql.Expression) (sql.IndexLookup, bool, error) {
	if len(t.partialLookup) == 0 {
		return sql.IndexLookup{}, false, nil
	}
	queryPredicate, ok := plannerPredicateSQL(exprs)
	if !ok {
		return sql.IndexLookup{}, false, nil
	}
	for _, cached := range t.partialLookup {
		if !indexpredicate.Implies(cached.predicate, queryPredicate) {
			continue
		}
		lookup, ok, err := cached.lookup(ctx, exprs...)
		if err != nil || !ok {
			if err != nil {
				return sql.IndexLookup{}, false, err
			}
			continue
		}
		return lookup, true, nil
	}
	return sql.IndexLookup{}, false, nil
}

func (t *BtreePlannerBoundaryTable) lookupForPartialPatternLike(ctx *sql.Context, exprs ...sql.Expression) (sql.IndexLookup, bool, error) {
	if len(t.partialPattern) == 0 {
		return sql.IndexLookup{}, false, nil
	}
	queryPredicate, ok := plannerPredicateSQL(exprs)
	if !ok {
		return sql.IndexLookup{}, false, nil
	}
	for _, expr := range exprs {
		fieldName, prefix, upper, ok := prefixLikeLookupBounds(expr)
		if !ok {
			continue
		}
		for _, cached := range t.partialPattern {
			if !strings.EqualFold(cached.columnName, fieldName) || !indexpredicate.Implies(cached.predicate, queryPredicate) {
				continue
			}
			lookup, err := buildPatternLookup(ctx, cached.btreePatternOpLookup, prefix, upper)
			if err != nil {
				return sql.IndexLookup{}, false, err
			}
			if lookup.IsEmpty() {
				continue
			}
			return lookup, true, nil
		}
	}
	return sql.IndexLookup{}, false, nil
}

func (p partialBtreeLookup) lookup(ctx *sql.Context, exprs ...sql.Expression) (sql.IndexLookup, bool, error) {
	builder := sql.NewMySQLIndexBuilder(ctx, p.index)
	matched := false
	for i, column := range p.columns {
		columnMatched := false
		columnEquality := false
		for _, expr := range exprs {
			comparison, ok, err := partialLookupComparison(ctx, expr, column.columnName)
			if err != nil {
				return sql.IndexLookup{}, false, err
			}
			if !ok {
				continue
			}
			switch comparison.op {
			case sql.IndexScanOpEq, sql.IndexScanOpNullSafeEq:
				builder.Equals(ctx, column.expression, column.typ, comparison.value)
				columnEquality = true
			case sql.IndexScanOpGt:
				builder.GreaterThan(ctx, column.expression, column.typ, comparison.value)
			case sql.IndexScanOpGte:
				builder.GreaterOrEqual(ctx, column.expression, column.typ, comparison.value)
			case sql.IndexScanOpLt:
				builder.LessThan(ctx, column.expression, column.typ, comparison.value)
			case sql.IndexScanOpLte:
				builder.LessOrEqual(ctx, column.expression, column.typ, comparison.value)
			default:
				continue
			}
			matched = true
			columnMatched = true
		}
		if !columnMatched {
			if i == 0 {
				return sql.IndexLookup{}, false, nil
			}
			break
		}
		if !columnEquality {
			break
		}
	}
	if !matched {
		return sql.IndexLookup{}, false, nil
	}
	lookup, err := builder.Build(ctx)
	if err != nil {
		return sql.IndexLookup{}, false, err
	}
	if lookup.IsEmpty() {
		return sql.IndexLookup{}, false, nil
	}
	return lookup, true, nil
}

type partialIndexComparisonLookup struct {
	op    sql.IndexScanOp
	value any
}

func partialLookupComparison(ctx *sql.Context, expr sql.Expression, columnName string) (partialIndexComparisonLookup, bool, error) {
	indexComparison, ok := unwrapGMSCast(expr).(sql.IndexComparisonExpression)
	if !ok {
		return partialIndexComparisonLookup{}, false, nil
	}
	op, left, right, ok := indexComparison.IndexScanOperation()
	if !ok || op == sql.IndexScanOpNotEq {
		return partialIndexComparisonLookup{}, false, nil
	}

	valueExpr := right
	if !partialLookupFieldMatches(left, columnName) {
		if !partialLookupFieldMatches(right, columnName) {
			return partialIndexComparisonLookup{}, false, nil
		}
		op = invertIndexScanOp(op)
		valueExpr = left
	}
	value, ok, err := constantLookupValue(ctx, valueExpr)
	if err != nil || !ok {
		return partialIndexComparisonLookup{}, false, err
	}
	return partialIndexComparisonLookup{op: op, value: value}, true, nil
}

func partialLookupFieldMatches(expr sql.Expression, columnName string) bool {
	field, ok := unwrapGMSCast(expr).(*gmsexpression.GetField)
	return ok && strings.EqualFold(field.Name(), columnName)
}

func constantLookupValue(ctx *sql.Context, expr sql.Expression) (any, bool, error) {
	if !expr.Resolved() || plannerExpressionReferencesField(expr) {
		return nil, false, nil
	}
	value, err := expr.Eval(ctx, nil)
	if err != nil || value == nil {
		return nil, false, err
	}
	value, err = sql.UnwrapAny(ctx, value)
	if err != nil || value == nil {
		return nil, false, err
	}
	return value, true, nil
}

func plannerExpressionReferencesField(expr sql.Expression) bool {
	expr = unwrapGMSCast(expr)
	if _, ok := expr.(*gmsexpression.GetField); ok {
		return true
	}
	for _, child := range expr.Children() {
		if plannerExpressionReferencesField(child) {
			return true
		}
	}
	return false
}

func plannerPredicateSQL(exprs []sql.Expression) (string, bool) {
	parts := make([]string, 0, len(exprs))
	for _, expr := range exprs {
		part, ok := plannerPredicateExprSQL(expr)
		if ok {
			parts = append(parts, part)
		}
	}
	if len(parts) == 0 {
		return "", false
	}
	return strings.Join(parts, " AND "), true
}

func plannerPredicateExprSQL(expr sql.Expression) (string, bool) {
	expr = unwrapGMSCast(expr)
	if comparison, ok := expr.(sql.IndexComparisonExpression); ok {
		op, left, right, ok := comparison.IndexScanOperation()
		if !ok {
			return "", false
		}
		opSQL, ok := indexScanOpPredicateSQL(op)
		if !ok {
			return "", false
		}
		return binaryPredicateSQL(left, opSQL, right)
	}
	switch expr := expr.(type) {
	case gmsexpression.Tuple:
		parts := make([]string, 0, len(expr))
		for _, child := range expr {
			part, ok := plannerPredicateExprSQL(child)
			if !ok {
				return "", false
			}
			parts = append(parts, part)
		}
		return "(" + strings.Join(parts, ", ") + ")", true
	case *gmsexpression.GetField:
		return predicateIdentifier(expr.Name()), true
	case *gmsexpression.Literal:
		return expr.String(), true
	case sql.FunctionExpression:
		return plannerFunctionPredicateSQL(expr)
	case *gmsexpression.Equals:
		return binaryPredicateSQL(expr.Left(), "=", expr.Right())
	case *gmsexpression.NullSafeEquals:
		return binaryPredicateSQL(expr.Left(), "IS NOT DISTINCT FROM", expr.Right())
	case *gmsexpression.GreaterThan:
		return binaryPredicateSQL(expr.Left(), ">", expr.Right())
	case *gmsexpression.GreaterThanOrEqual:
		return binaryPredicateSQL(expr.Left(), ">=", expr.Right())
	case *gmsexpression.LessThan:
		return binaryPredicateSQL(expr.Left(), "<", expr.Right())
	case *gmsexpression.LessThanOrEqual:
		return binaryPredicateSQL(expr.Left(), "<=", expr.Right())
	case *gmsexpression.Like:
		if expr.Escape != nil {
			return "", false
		}
		return binaryPredicateSQL(expr.Left(), "LIKE", expr.Right())
	case *gmsexpression.IsNull:
		child, ok := plannerPredicateExprSQL(expr.Child)
		if !ok {
			return "", false
		}
		return child + " IS NULL", true
	case *pgexpression.IsNull:
		child, ok := plannerPredicateExprSQL(expr.Child)
		if !ok {
			return "", false
		}
		return child + " IS NULL", true
	case *pgexpression.IsNotNull:
		child, ok := plannerPredicateExprSQL(expr.Child)
		if !ok {
			return "", false
		}
		return child + " IS NOT NULL", true
	case *pgexpression.IsNotDistinctFrom:
		children := expr.Children()
		if len(children) != 2 {
			return "", false
		}
		return binaryPredicateSQL(children[0], "IS NOT DISTINCT FROM", children[1])
	case *pgexpression.IsDistinctFrom:
		children := expr.Children()
		if len(children) != 2 {
			return "", false
		}
		return binaryPredicateSQL(children[0], "IS DISTINCT FROM", children[1])
	case *gmsexpression.Not:
		child, ok := plannerPredicateExprSQL(expr.Child)
		if !ok {
			return "", false
		}
		return "NOT " + child, true
	case *gmsexpression.And:
		return binaryPredicateSQL(expr.LeftChild, "AND", expr.RightChild)
	case *gmsexpression.Or:
		return binaryPredicateSQL(expr.LeftChild, "OR", expr.RightChild)
	case *gmsexpression.Between:
		value, ok := plannerPredicateExprSQL(expr.Val)
		if !ok {
			return "", false
		}
		lower, ok := plannerPredicateExprSQL(expr.Lower)
		if !ok {
			return "", false
		}
		upper, ok := plannerPredicateExprSQL(expr.Upper)
		if !ok {
			return "", false
		}
		return "(" + value + " BETWEEN " + lower + " AND " + upper + ")", true
	default:
		return "", false
	}
}

func plannerFunctionPredicateSQL(expr sql.FunctionExpression) (string, bool) {
	name := strings.ToLower(expr.FunctionName())
	if name == "coalesce" {
		return plannerFunctionCallPredicateSQL(name, expr.Children())
	}
	if name == "strpos" {
		children := expr.Children()
		if len(children) != 2 {
			return "", false
		}
		return plannerFunctionCallPredicateSQL(name, children)
	}
	if name == "starts_with" {
		children := expr.Children()
		if len(children) != 2 {
			return "", false
		}
		return plannerFunctionCallPredicateSQL(name, children)
	}
	if name == "left" || name == "right" {
		children := expr.Children()
		if len(children) != 2 {
			return "", false
		}
		return plannerFunctionCallPredicateSQL(name, children)
	}
	if name == "replace" {
		children := expr.Children()
		if len(children) != 3 {
			return "", false
		}
		return plannerFunctionCallPredicateSQL(name, children)
	}
	if name == "translate" {
		children := expr.Children()
		if len(children) != 3 {
			return "", false
		}
		return plannerFunctionCallPredicateSQL(name, children)
	}
	if name == "split_part" {
		children := expr.Children()
		if len(children) != 3 {
			return "", false
		}
		return plannerFunctionCallPredicateSQL(name, children)
	}
	if name == "substr" || name == "substring" {
		children := expr.Children()
		if len(children) != 2 && len(children) != 3 {
			return "", false
		}
		return plannerFunctionCallPredicateSQL("substring", children)
	}
	name, ok := plannerCanonicalFunctionPredicateName(name)
	if !ok {
		return "", false
	}
	children := expr.Children()
	if len(children) != 1 {
		return "", false
	}
	childSQL, ok := plannerPredicateExprSQL(children[0])
	if !ok {
		return "", false
	}
	return name + "(" + childSQL + ")", true
}

func plannerFunctionCallPredicateSQL(name string, children []sql.Expression) (string, bool) {
	if len(children) == 0 {
		return "", false
	}
	parts := make([]string, 0, len(children))
	for _, child := range children {
		childSQL, ok := plannerPredicateExprSQL(child)
		if !ok {
			return "", false
		}
		parts = append(parts, childSQL)
	}
	return name + "(" + strings.Join(parts, ", ") + ")", true
}

func plannerCanonicalFunctionPredicateName(name string) (string, bool) {
	switch name {
	case "abs":
		return name, true
	case "bit_length":
		return name, true
	case "octet_length":
		return name, true
	case "ascii", "lower", "upper", "btrim", "ltrim", "rtrim", "md5", "reverse", "to_hex", "initcap":
		return name, true
	case "char_length", "character_length", "length":
		return "length", true
	default:
		return "", false
	}
}

func indexScanOpPredicateSQL(op sql.IndexScanOp) (string, bool) {
	switch op {
	case sql.IndexScanOpEq:
		return "=", true
	case sql.IndexScanOpNullSafeEq:
		return "IS NOT DISTINCT FROM", true
	case sql.IndexScanOpInSet:
		return "IN", true
	case sql.IndexScanOpNotInSet:
		return "NOT IN", true
	case sql.IndexScanOpGt:
		return ">", true
	case sql.IndexScanOpGte:
		return ">=", true
	case sql.IndexScanOpLt:
		return "<", true
	case sql.IndexScanOpLte:
		return "<=", true
	default:
		return "", false
	}
}

func binaryPredicateSQL(left sql.Expression, op string, right sql.Expression) (string, bool) {
	leftSQL, ok := plannerPredicateExprSQL(left)
	if !ok {
		return "", false
	}
	rightSQL, ok := plannerPredicateExprSQL(right)
	if !ok {
		return "", false
	}
	return "(" + leftSQL + " " + op + " " + rightSQL + ")", true
}

func predicateIdentifier(name string) string {
	if name == "" {
		return `""`
	}
	for i, r := range name {
		if i == 0 {
			if r != '_' && !unicode.IsLetter(r) {
				return quotePredicateIdentifier(name)
			}
			continue
		}
		if r != '_' && !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			return quotePredicateIdentifier(name)
		}
	}
	return name
}

func quotePredicateIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func unqualifiedPlannerIndexColumn(expr string) string {
	expr = strings.Trim(strings.TrimSpace(expr), "`\"")
	if dot := strings.LastIndex(expr, "."); dot >= 0 {
		expr = expr[dot+1:]
	}
	return strings.Trim(expr, "`\"")
}
