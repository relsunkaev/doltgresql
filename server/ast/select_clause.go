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
	"strings"

	"github.com/dolthub/go-mysql-server/sql/expression"

	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/postgres/parser/sem/tree"
	"github.com/dolthub/doltgresql/server/auth"
	pgexprs "github.com/dolthub/doltgresql/server/expression"
)

// nodeSelectClause handles tree.SelectClause nodes.
func nodeSelectClause(ctx *Context, node *tree.SelectClause) (*vitess.Select, error) {
	if node == nil {
		return nil, nil
	}
	selectExprs, err := nodeSelectExprs(ctx, node.Exprs)
	if err != nil {
		return nil, err
	}
	// Multiple tables in the FROM column with an "equals" filter for some columns within each table should be treated
	// as a join. The analyzer should catch this, however GMS processes this form of a join differently than a standard
	// join, which is currently incompatible with Doltgres expressions. As a workaround, we rewrite the tree so that we
	// pass along a join node.
	// TODO: handle more than two tables, also make this more robust with handling more node types
	if len(node.From.Tables) == 2 && node.Where != nil {
		tableNames := make(map[tree.TableName]int)
		tableAliases := make(map[tree.TableName]int)
		// First we need to get the table names and aliases, since they'll be referenced by the filters
		for i := range node.From.Tables {
			switch table := node.From.Tables[i].(type) {
			case *tree.AliasedTableExpr:
				if tableName, ok := table.Expr.(*tree.TableName); ok {
					tableNames[*tableName] = i
				} else {
					goto PostJoinRewrite
				}
				tableAliases[tree.MakeUnqualifiedTableName(table.As.Alias)] = i
			case *tree.TableName:
				tableNames[*table] = i
			case *tree.UnresolvedObjectName:
				tableNames[table.ToTableName()] = i
			default:
				goto PostJoinRewrite
			}
		}
		// For now, we'll check if the entire filter should be moved into the join condition. Eventually, this should
		// move only the needed expressions into the join condition.
		var delveExprs func(expr tree.Expr) bool
		delveExprs = func(expr tree.Expr) bool {
			switch expr := expr.(type) {
			case *tree.AndExpr:
				return delveExprs(expr.Left) && delveExprs(expr.Right)
			case *tree.OrExpr:
				return delveExprs(expr.Left) && delveExprs(expr.Right)
			case *tree.ComparisonExpr:
				if expr.Operator != tree.EQ {
					return false
				}
				var refTables [2]int
				for argIndex, arg := range []tree.Expr{expr.Left, expr.Right} {
					switch arg := arg.(type) {
					case *tree.UnresolvedName:
						refTable := arg.GetUnresolvedObjectName().ToTableName()
						if aliasIndex, ok := tableAliases[refTable]; ok {
							refTables[argIndex] = aliasIndex
						} else if tableIndex, ok := tableNames[refTable]; ok {
							refTables[argIndex] = tableIndex
						} else {
							return false
						}
					default:
						return false
					}
				}
				// In this case, the expression does not reference multiple tables, so it's not a join condition
				if refTables[0] == refTables[1] {
					return false
				}
				return true
			default:
				return false
			}
		}
		if !delveExprs(node.Where.Expr) {
			goto PostJoinRewrite
		}
		// The filter condition represents a join, so we need to rewrite our FROM node to be a join node
		node.From.Tables = tree.TableExprs{&tree.JoinTableExpr{
			JoinType: "",
			Left:     node.From.Tables[0],
			Right:    node.From.Tables[1],
			Cond:     &tree.OnJoinCond{Expr: node.Where.Expr},
		}}
		node.Where = nil
	}
PostJoinRewrite:
	from, err := nodeFrom(ctx, node.From)
	if err != nil {
		return nil, err
	}
	// We use TableFuncExprs to represent queries on functions that behave as though they were tables. This is something
	// that we have to situationally support, as inner nodes do not have the proper context to output a TableFuncExpr,
	// since TableFuncExprs pertain only to SELECT statements.
	for i, fromExpr := range from {
		from[i] = rewriteTableFunctionExpr(fromExpr)
	}
	applySelectColumnAuth(node, from)
	distinct := node.Distinct
	var distinctOn vitess.Exprs
	if len(node.DistinctOn) > 0 {
		distinct = true
		distinctOn = make(vitess.Exprs, len(node.DistinctOn))
		for i, expr := range node.DistinctOn {
			distinctOn[i], err = nodeExpr(ctx, expr)
			if err != nil {
				return nil, err
			}
		}
	}
	where, err := nodeWhere(ctx, node.Where)
	if err != nil {
		return nil, err
	}
	having, err := nodeWhere(ctx, node.Having)
	if err != nil {
		return nil, err
	}
	groupBy, err := nodeGroupBy(ctx, node.GroupBy)
	if err != nil {
		return nil, err
	}
	window, err := nodeWindow(ctx, node.Window)
	if err != nil {
		return nil, err
	}
	return &vitess.Select{
		QueryOpts: vitess.QueryOpts{
			Distinct:   distinct,
			DistinctOn: distinctOn,
		},
		SelectExprs: selectExprs,
		From:        from,
		Where:       where,
		GroupBy:     groupBy,
		Having:      having,
		Window:      window,
		Comments:    vitess.Comments{[]byte(node.BlockComment)},
	}, nil
}

func applySelectColumnAuth(node *tree.SelectClause, from vitess.TableExprs) {
	if len(from) != 1 {
		return
	}
	columns, ok := selectColumnAuthColumns(node)
	if !ok || len(columns) == 0 {
		return
	}
	tableExpr, ok := from[0].(*vitess.AliasedTableExpr)
	if !ok || tableExpr.Auth.AuthType != auth.AuthType_SELECT || tableExpr.Auth.TargetType != auth.AuthTargetType_TableIdentifiers || len(tableExpr.Auth.TargetNames) != 3 {
		return
	}
	tableExpr.Auth.TargetType = auth.AuthTargetType_TableColumnIdents
	tableExpr.Auth.TargetNames = tableColumnAuthTargets(tableExpr.Auth.TargetNames, columns)
}

func selectColumnAuthColumns(node *tree.SelectClause) ([]string, bool) {
	collector := &selectColumnAuthCollector{
		columns: make(map[string]string),
	}
	for _, expr := range node.Exprs {
		if !collector.walk(expr.Expr) {
			return nil, false
		}
	}
	for _, expr := range node.DistinctOn {
		if !collector.walk(expr) {
			return nil, false
		}
	}
	if node.Where != nil && !collector.walk(node.Where.Expr) {
		return nil, false
	}
	if node.Having != nil && !collector.walk(node.Having.Expr) {
		return nil, false
	}
	for _, expr := range node.GroupBy {
		if !collector.walk(expr) {
			return nil, false
		}
	}
	columns := make([]string, 0, len(collector.columns))
	for _, column := range collector.columns {
		columns = append(columns, column)
	}
	return columns, true
}

type selectColumnAuthCollector struct {
	columns                map[string]string
	ignoredTableQualifiers map[string]struct{}
	valid                  bool
}

func (c *selectColumnAuthCollector) walk(expr tree.Expr) bool {
	c.valid = true
	tree.WalkExprConst(c, expr)
	return c.valid
}

func (c *selectColumnAuthCollector) VisitPre(expr tree.Expr) (bool, tree.Expr) {
	switch expr := expr.(type) {
	case tree.UnqualifiedStar, *tree.AllColumnsSelector:
		c.valid = false
		return false, expr
	case *tree.UnresolvedName:
		if expr.Star || expr.NumParts == 0 || expr.NumParts > 3 {
			c.valid = false
			return false, expr
		}
		if expr.NumParts >= 2 {
			if _, ok := c.ignoredTableQualifiers[strings.ToLower(expr.Parts[1])]; ok {
				return false, expr
			}
		}
		column := expr.Parts[0]
		if column == "" {
			c.valid = false
			return false, expr
		}
		key := strings.ToLower(column)
		if _, ok := c.columns[key]; !ok {
			c.columns[key] = column
		}
		return false, expr
	default:
		return true, expr
	}
}

func (c *selectColumnAuthCollector) VisitPost(expr tree.Expr) tree.Expr {
	return expr
}

func rewriteTableFunctionExpr(fromExpr vitess.TableExpr) vitess.TableExpr {
	switch expr := fromExpr.(type) {
	case *vitess.AliasedTableExpr:
		tableFunc, subquery, ok := tableFunctionFromAliasedValuesWrapper(expr)
		if !ok {
			return expr
		}
		if subquery.SelectExprs == nil {
			subquery.SelectExprs = vitess.SelectExprs{&vitess.StarExpr{}}
		}
		subquery.From = vitess.TableExprs{tableFunc}
		if expr.As.IsEmpty() {
			expr.As = vitess.NewTableIdent(tableFunc.Name)
		}
		expr.Lateral = true
		return expr
	case *vitess.JoinTableExpr:
		expr.LeftExpr = rewriteTableFunctionExpr(expr.LeftExpr)
		expr.RightExpr = rewriteTableFunctionExpr(expr.RightExpr)
		return expr
	case *vitess.ParenTableExpr:
		for i, child := range expr.Exprs {
			expr.Exprs[i] = rewriteTableFunctionExpr(child)
		}
		return expr
	default:
		return fromExpr
	}
}

func tableFunctionFromAliasedValuesWrapper(aliasedTableExpr *vitess.AliasedTableExpr) (*vitess.TableFuncExpr, *vitess.Select, bool) {
	if aliasedTableExpr.Hints != nil || len(aliasedTableExpr.Partitions) != 0 {
		return nil, nil, false
	}
	subquery, ok := aliasedTableExpr.Expr.(*vitess.Subquery)
	if !ok || len(subquery.Columns) != 0 {
		return nil, nil, false
	}
	subquerySelect, ok := subquery.Select.(*vitess.Select)
	if !ok || len(subquerySelect.From) != 1 {
		return nil, nil, false
	}
	valuesStatement, ok := subquerySelect.From[0].(*vitess.ValuesStatement)
	if !ok || len(valuesStatement.Columns) != 0 || len(valuesStatement.Rows) != 1 || len(valuesStatement.Rows[0]) != 1 {
		return nil, nil, false
	}
	funcExpr, ok := valuesStatement.Rows[0][0].(*vitess.FuncExpr)
	if !ok {
		return nil, nil, false
	}
	// It appears that GMS hardcodes the expectation of vitess literals here, so we have to
	// convert from Doltgres literals to GMS literals. Eventually we need to remove this
	// hardcoded behavior.
	for _, fExpr := range funcExpr.Exprs {
		if aliasedExpr, ok := fExpr.(*vitess.AliasedExpr); ok {
			if injectedExpr, ok := aliasedExpr.Expr.(vitess.InjectedExpr); ok {
				if literal, ok := injectedExpr.Expression.(*expression.Literal); ok {
					aliasedExpr.Expr = pgexprs.ToVitessLiteral(literal)
				}
			}
		}
	}
	return &vitess.TableFuncExpr{
		Name:  funcExpr.Name.String(),
		Exprs: funcExpr.Exprs,
		Alias: aliasedTableExpr.As,
	}, subquerySelect, true
}
