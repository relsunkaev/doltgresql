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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	gmsanalyzer "github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/analyzer/analyzererrors"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"

	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
)

// ValidateGroupBy is the Postgres-flavored replacement for the GMS
// ONLY_FULL_GROUP_BY validation rule. It keeps the same functional-dependency
// checks for regular expressions, but also handles Postgres aggregate
// subqueries whose aggregate arguments refer to the outer grouped query.
func ValidateGroupBy(ctx *sql.Context, a *gmsanalyzer.Analyzer, n sql.Node, scope *plan.Scope, sel gmsanalyzer.RuleSelector, qFlags *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	if !gmsanalyzer.FlagIsSet(qFlags, sql.QFlagAggregation) {
		return n, transform.SameTree, nil
	}

	span, ctx := ctx.Span("validate_group_by")
	defer span.End()

	if !sql.LoadSqlMode(ctx).OnlyFullGroupBy() {
		return n, transform.SameTree, nil
	}

	var err error
	var parent sql.Node
	var project *plan.Project
	var orderBy *plan.Sort
	transform.InspectWithOpaque(ctx, n, func(ctx *sql.Context, n sql.Node) bool {
		defer func() {
			parent = n
		}()
		switch n := n.(type) {
		case *plan.GroupBy:
			if aliasName, ok := invalidCorrelatedGroupByAlias(ctx, scope, n.GroupByExprs); ok {
				err = pgerror.Newf(pgcode.UndefinedColumn, `column "%s" does not exist`, aliasName)
				return false
			}

			var noGroupBy bool
			if len(n.GroupByExprs) == 0 {
				noGroupBy = true
			}

			primaryKeys := make(map[string]bool)
			for _, col := range n.Child.Schema(ctx) {
				if col.PrimaryKey {
					primaryKeys[strings.ToLower(col.String())] = true
				}
			}

			groupBys := make(map[string]bool)
			groupByCols := sql.NewColSet()
			groupByPrimaryKeys := 0
			isJoin := false
			exprs := make([]sql.Expression, 0)
			exprs = append(exprs, n.GroupByExprs...)
			possibleJoin := n.Child
			if filter, ok := n.Child.(*plan.Filter); ok {
				possibleJoin = filter.Child
				exprs = append(exprs, getGroupByEqualsDependencies(ctx, filter.Expression)...)
			}
			if join, ok := possibleJoin.(*plan.JoinNode); ok {
				isJoin = true
				exprs = append(exprs, getGroupByEqualsDependencies(ctx, join.Filter)...)
			}
			for _, expr := range exprs {
				sql.Inspect(ctx, expr, func(ctx *sql.Context, expr sql.Expression) bool {
					exprStr := strings.ToLower(expr.String())
					if primaryKeys[exprStr] && !groupBys[exprStr] {
						groupByPrimaryKeys++
					}
					groupBys[exprStr] = true

					if getField, ok := expr.(*expression.GetField); ok {
						groupByCols.Add(getField.Id())
					}

					if nameable, ok := expr.(sql.Nameable); ok {
						groupBys[strings.ToLower(nameable.Name())] = true
					}
					_, isAlias := expr.(*expression.Alias)
					return isAlias
				})
			}

			if len(primaryKeys) != 0 && (groupByPrimaryKeys == len(primaryKeys) || (isJoin && groupByPrimaryKeys > 0)) {
				return true
			}

			selectExprs, orderByExprs := getGroupBySelectAndOrderByExprs(ctx, project, orderBy, n.SelectDeps, groupBys)

			for i, expr := range selectExprs {
				if valid, col := expressionReferencesOnlyPostgresGroupBys(ctx, groupBys, groupByCols, expr, noGroupBy); !valid {
					if noGroupBy {
						err = groupByValidationError(sql.ErrNonAggregatedColumnWithoutGroupBy.New(i+1, col))
					} else {
						err = groupByValidationError(analyzererrors.ErrValidationGroupBy.New(i+1, col))
					}
					return false
				}
			}
			if !noGroupBy {
				for i, expr := range orderByExprs {
					if valid, col := expressionReferencesOnlyPostgresGroupBys(ctx, groupBys, groupByCols, expr, noGroupBy); !valid {
						err = groupByValidationError(analyzererrors.ErrValidationGroupByOrderBy.New(i+1, col))
						return false
					}
				}
			}
		case *plan.Project:
			if _, isHaving := parent.(*plan.Having); !isHaving {
				project = n
				orderBy = nil
			}
		case *plan.Sort:
			orderBy = n
		}
		return true
	})

	return n, transform.SameTree, err
}

func invalidCorrelatedGroupByAlias(ctx *sql.Context, scope *plan.Scope, exprs []sql.Expression) (string, bool) {
	correlated := scope.Correlated()
	if correlated.Empty() {
		return "", false
	}

	// GMS may resolve an outer select-list alias inside a scalar subquery's
	// GROUP BY. PostgreSQL exposes the underlying outer column names, but not
	// renamed output aliases, to that subquery scope.
	for _, expr := range exprs {
		var aliasName string
		sql.Inspect(ctx, expr, func(ctx *sql.Context, expr sql.Expression) bool {
			if aliasName != "" {
				return false
			}

			alias, ok := expr.(*expression.Alias)
			if !ok {
				return true
			}
			if !correlated.Contains(alias.Id()) || aliasReferencesSameNamedColumn(alias) || aliasReferencesNoColumns(ctx, alias) {
				return true
			}

			aliasName = alias.Name()
			return false
		})
		if aliasName != "" {
			return aliasName, true
		}
	}

	return "", false
}

func aliasReferencesSameNamedColumn(alias *expression.Alias) bool {
	child := alias.Child
	for {
		nestedAlias, ok := child.(*expression.Alias)
		if !ok {
			break
		}
		child = nestedAlias.Child
	}

	getField, ok := child.(*expression.GetField)
	return ok && strings.EqualFold(alias.Name(), getField.Name())
}

func aliasReferencesNoColumns(ctx *sql.Context, alias *expression.Alias) bool {
	referencesColumn := false
	sql.Inspect(ctx, alias.Child, func(ctx *sql.Context, expr sql.Expression) bool {
		if referencesColumn {
			return false
		}
		_, referencesColumn = expr.(*expression.GetField)
		return !referencesColumn
	})
	return !referencesColumn
}

func groupByValidationError(err error) error {
	return pgerror.WithCandidateCode(err, pgcode.Grouping)
}

func getGroupByEqualsDependencies(ctx *sql.Context, expr sql.Expression) []sql.Expression {
	exprs := make([]sql.Expression, 0)
	sql.Inspect(ctx, expr, func(ctx *sql.Context, expr sql.Expression) bool {
		switch expr := expr.(type) {
		case *expression.And:
			return true
		case *expression.Equals:
			for _, e := range expr.Children() {
				if and, ok := e.(*expression.And); ok {
					exprs = append(exprs, getGroupByEqualsDependencies(ctx, and)...)
				} else if _, ok := e.(*expression.Literal); !ok {
					exprs = append(exprs, e)
				}
			}
		}
		return false
	})
	return exprs
}

func getGroupBySelectAndOrderByExprs(ctx *sql.Context, project *plan.Project, orderBy *plan.Sort, selectDeps []sql.Expression, groupBys map[string]bool) ([]sql.Expression, []sql.Expression) {
	if project == nil && orderBy == nil {
		return selectDeps, nil
	}

	sd := make(map[string]sql.Expression, len(selectDeps))
	for _, dep := range selectDeps {
		sd[strings.ToLower(dep.String())] = dep
	}

	selectExprs := make([]sql.Expression, 0)
	orderByExprs := make([]sql.Expression, 0)

	for _, expr := range project.Projections {
		if !project.AliasDeps[strings.ToLower(expr.String())] {
			resolvedExpr := resolveGroupByExpr(ctx, expr, sd, groupBys)
			selectExprs = append(selectExprs, resolvedExpr)
		}
	}

	if orderBy != nil {
		for _, expr := range orderBy.Expressions() {
			resolvedExpr := resolveGroupByExpr(ctx, expr, sd, groupBys)
			orderByExprs = append(orderByExprs, resolvedExpr)
		}
	}

	return selectExprs, orderByExprs
}

func resolveGroupByExpr(ctx *sql.Context, expr sql.Expression, selectDeps map[string]sql.Expression, groupBys map[string]bool) sql.Expression {
	resolvedExpr, _, _ := transform.Expr(ctx, expr, func(ctx *sql.Context, expr sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		if groupBys[strings.ToLower(expr.String())] {
			return expr, transform.SameTree, nil
		}
		switch expr := expr.(type) {
		case *expression.Alias:
			if dep, ok := selectDeps[strings.ToLower(expr.Child.String())]; ok {
				selectDeps[strings.ToLower(expr.Name())] = dep
				return dep, transform.NewTree, nil
			}
		case *expression.GetField:
			if dep, ok := selectDeps[strings.ToLower(expr.String())]; ok {
				return dep, transform.NewTree, nil
			}
		}
		return expr, transform.SameTree, nil
	})
	return resolvedExpr
}

func expressionReferencesOnlyPostgresGroupBys(ctx *sql.Context, groupBys map[string]bool, groupByCols sql.ColSet, expr sql.Expression, noGroupBy bool) (bool, string) {
	var col string
	valid := true
	sql.Inspect(ctx, expr, func(ctx *sql.Context, expr sql.Expression) bool {
		switch expr := expr.(type) {
		case nil, sql.Aggregation, *expression.Literal:
			return false
		case *plan.Subquery:
			valid, col = subqueryReferencesOnlyGroupedOrAggregatedOuterFields(ctx, groupBys, groupByCols, expr)
			return false
		default:
			if groupBys[strings.ToLower(expr.String())] {
				return false
			}

			if nameable, ok := expr.(sql.Nameable); ok {
				if groupBys[strings.ToLower(nameable.Name())] {
					return false
				}
			}

			if len(expr.Children()) == 0 {
				valid = false
				col = expr.String()
				return false
			}

			return true
		}
	})

	return valid, col
}

func subqueryReferencesOnlyGroupedOrAggregatedOuterFields(ctx *sql.Context, groupBys map[string]bool, groupByCols sql.ColSet, subquery *plan.Subquery) (bool, string) {
	correlated := subquery.Correlated()
	if correlated.Empty() {
		return true, ""
	}

	var col string
	valid := true
	transform.InspectExpressionsWithNode(ctx, subquery.Query, func(ctx *sql.Context, node sql.Node, expr sql.Expression) bool {
		if !valid {
			return false
		}
		if _, ok := node.(*plan.GroupBy); ok {
			return false
		}

		switch expr := expr.(type) {
		case nil, sql.Aggregation, *expression.Literal:
			return false
		case *plan.Subquery:
			valid, col = subqueryReferencesOnlyGroupedOrAggregatedOuterFields(ctx, groupBys, groupByCols, expr)
			return false
		case *expression.GetField:
			if correlated.Contains(expr.Id()) &&
				!groupByCols.Contains(expr.Id()) &&
				!groupBys[strings.ToLower(expr.String())] &&
				!groupBys[strings.ToLower(expr.Name())] {
				valid = false
				col = expr.String()
			}
			return false
		default:
			return true
		}
	})

	return valid, col
}
