// Copyright 2023 Dolthub, Inc.
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

package ast

import (
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql/expression"

	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/postgres/parser/sem/tree"
	pgexprs "github.com/dolthub/doltgresql/server/expression"
)

// nodeOrderBy handles *tree.OrderBy nodes.
func nodeOrderBy(ctx *Context, node tree.OrderBy, selectStmt vitess.SelectStatement) (vitess.OrderBy, error) {
	if len(node) == 0 {
		return nil, nil
	}
	orderBys := make([]*vitess.Order, 0, len(node)*2)
	for i := range node {
		if node[i].OrderType != tree.OrderByColumn && node[i].OrderType != tree.OrderByUsing {
			return nil, errors.Errorf("ORDER BY type is not yet supported")
		}
		direction, err := orderByDirection(node[i])
		if err != nil {
			return nil, err
		}

		desiredNullsLast := direction == vitess.AscScr
		switch node[i].NullsOrder {
		case tree.DefaultNullsOrder:
			// PostgreSQL defaults to ASC NULLS LAST and DESC NULLS FIRST.
			desiredNullsLast = direction == vitess.AscScr
		case tree.NullsFirst:
			desiredNullsLast = false
		case tree.NullsLast:
			desiredNullsLast = true
		default:
			return nil, errors.Errorf("unknown NULL ordering in ORDER BY")
		}
		var expr vitess.Expr
		if ordinal, ok := orderByOutputOrdinal(selectStmt, node[i].Expr); ok && !selectStatementIsSetOp(selectStmt) {
			expr = vitess.NewIntVal([]byte(strconv.Itoa(ordinal)))
		} else {
			var err error
			expr, err = nodeExpr(ctx, node[i].Expr)
			if err != nil {
				return nil, err
			}
		}

		if needsExplicitNullSort(direction, desiredNullsLast) && !selectStatementIsSetOp(selectStmt) {
			nullProbeExpr := expr
			if outputExpr, ok := orderByOutputExpr(selectStmt, node[i].Expr); ok {
				nullProbeExpr = outputExpr
			}
			nullProbeDirection := vitess.AscScr
			if !desiredNullsLast {
				nullProbeDirection = vitess.DescScr
			}
			orderBys = append(orderBys, &vitess.Order{
				Expr: &vitess.IsExpr{
					Expr:     nullProbeExpr,
					Operator: vitess.IsNullStr,
				},
				Direction: nullProbeDirection,
			})
		}

		orderBys = append(orderBys, &vitess.Order{
			Expr:      normalizeOrderByLiteralExpr(expr),
			Direction: direction,
		})
	}
	return orderBys, nil
}

func selectStatementIsSetOp(selectStmt vitess.SelectStatement) bool {
	switch stmt := selectStmt.(type) {
	case *vitess.SetOp:
		return true
	case *vitess.ParenSelect:
		return selectStatementIsSetOp(stmt.Select)
	default:
		return false
	}
}

func orderByDirection(order *tree.Order) (string, error) {
	if order.OrderType == tree.OrderByUsing {
		switch order.Operator {
		case tree.LT:
			return vitess.AscScr, nil
		case tree.GT:
			return vitess.DescScr, nil
		default:
			return "", errors.Errorf("ORDER BY USING operator %s is not yet supported", order.Operator)
		}
	}
	switch order.Direction {
	case tree.DefaultDirection:
		return vitess.AscScr, nil
	case tree.Ascending:
		return vitess.AscScr, nil
	case tree.Descending:
		return vitess.DescScr, nil
	default:
		return "", errors.Errorf("unknown ORDER BY sorting direction")
	}
}

func needsExplicitNullSort(direction string, desiredNullsLast bool) bool {
	naturalNullsLast := direction == vitess.DescScr
	return desiredNullsLast != naturalNullsLast
}

func normalizeOrderByLiteralExpr(expr vitess.Expr) vitess.Expr {
	// GMS order by is hardcoded to expect vitess.SQLVal for expressions such as `ORDER BY 1`.
	// In addition, there is the requirement that columns in the order by also need to be referenced somewhere in
	// the query, which is not a requirement for Postgres. Whenever we add that functionality, we also need to
	// remove the dependency on vitess.SQLVal. For now, we'll just convert our literals to a vitess.SQLVal.
	if injectedExpr, ok := expr.(vitess.InjectedExpr); ok {
		if literal, ok := injectedExpr.Expression.(*expression.Literal); ok {
			return pgexprs.ToVitessLiteral(literal)
		}
	}
	return expr
}

func orderByOutputOrdinal(selectStmt vitess.SelectStatement, orderExpr tree.Expr) (int, bool) {
	name, ok := orderExpr.(*tree.UnresolvedName)
	if !ok || name.NumParts != 1 || name.Star {
		return 0, false
	}
	orderName := name.Parts[0]
	selectExprs := outputSelectExprs(selectStmt)
	var ordinal int
	for i, selectExpr := range selectExprs {
		outputName, ok := selectExprOutputName(selectExpr)
		if !ok || !strings.EqualFold(outputName, orderName) {
			continue
		}
		if ordinal != 0 {
			return 0, false
		}
		ordinal = i + 1
	}
	return ordinal, ordinal != 0
}

func orderByOutputExpr(selectStmt vitess.SelectStatement, orderExpr tree.Expr) (vitess.Expr, bool) {
	name, ok := orderExpr.(*tree.UnresolvedName)
	if !ok || name.NumParts != 1 || name.Star {
		return nil, false
	}
	orderName := name.Parts[0]
	selectExprs := outputSelectExprs(selectStmt)
	var ret vitess.Expr
	for _, selectExpr := range selectExprs {
		outputName, ok := selectExprOutputName(selectExpr)
		if !ok || !strings.EqualFold(outputName, orderName) {
			continue
		}
		if ret != nil {
			return nil, false
		}
		aliasedExpr, ok := selectExpr.(*vitess.AliasedExpr)
		if !ok {
			return nil, false
		}
		ret = aliasedExpr.Expr
	}
	return ret, ret != nil
}

func expandDistinctOnForNullOrdering(selectStmt *vitess.Select, orderBy vitess.OrderBy) {
	if len(selectStmt.QueryOpts.DistinctOn) == 0 || len(orderBy) == 0 {
		return
	}
	expanded := make(vitess.Exprs, 0, len(selectStmt.QueryOpts.DistinctOn)*2)
	orderIdx := 0
	for _, distinctExpr := range selectStmt.QueryOpts.DistinctOn {
		hadNullProbe := false
		if orderIdx < len(orderBy) && isNullProbeForExpr(orderBy[orderIdx].Expr, distinctExpr) {
			expanded = append(expanded, orderBy[orderIdx].Expr)
			orderIdx++
			hadNullProbe = true
		}
		expanded = append(expanded, distinctExpr)
		if orderIdx < len(orderBy) && (hadNullProbe || sameOrderByExpr(orderBy[orderIdx].Expr, distinctExpr)) {
			orderIdx++
		}
	}
	selectStmt.QueryOpts.DistinctOn = expanded
}

func isNullProbeForExpr(candidate vitess.Expr, expr vitess.Expr) bool {
	isExpr, ok := candidate.(*vitess.IsExpr)
	return ok && isExpr.Operator == vitess.IsNullStr && sameOrderByExpr(isExpr.Expr, expr)
}

func sameOrderByExpr(left vitess.Expr, right vitess.Expr) bool {
	return strings.EqualFold(vitess.String(left), vitess.String(right))
}

func outputSelectExprs(selectStmt vitess.SelectStatement) vitess.SelectExprs {
	switch stmt := selectStmt.(type) {
	case *vitess.Select:
		return stmt.SelectExprs
	case *vitess.SetOp:
		return outputSelectExprs(stmt.Left)
	case *vitess.ParenSelect:
		return outputSelectExprs(stmt.Select)
	default:
		return nil
	}
}

func selectExprOutputName(selectExpr vitess.SelectExpr) (string, bool) {
	aliasedExpr, ok := selectExpr.(*vitess.AliasedExpr)
	if !ok {
		return "", false
	}
	if !aliasedExpr.As.IsEmpty() {
		return aliasedExpr.As.String(), true
	}
	if colName, ok := aliasedExpr.Expr.(*vitess.ColName); ok {
		return colName.Name.String(), true
	}
	return "", false
}
