// Copyright 2024 Dolthub, Inc.
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

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/expression"
	gmsaggregation "github.com/dolthub/go-mysql-server/sql/expression/function/aggregation"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/planbuilder"
	"github.com/dolthub/go-mysql-server/sql/transform"

	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
	pgexprs "github.com/dolthub/doltgresql/server/expression"
	"github.com/dolthub/doltgresql/server/functions/framework"
	pgnodes "github.com/dolthub/doltgresql/server/node"
	pgtransform "github.com/dolthub/doltgresql/server/transform"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// OptimizeFunctions replaces all functions that fit specific criteria with their optimized variants. Also handles
// SRFs (set-returning functions) by setting the `IncludesNestedIters` flag on the Project node if any SRF is found
// inside projection expressions.
func OptimizeFunctions(ctx *sql.Context, a *analyzer.Analyzer, node sql.Node, scope *plan.Scope, selector analyzer.RuleSelector, qFlags *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	// This is supposed to be one of the last rules to run. Subqueries break that assumption, so we skip this rule in such cases.
	if scope != nil && scope.CurrentNodeIsFromSubqueryExpression {
		return node, transform.SameTree, nil
	}

	_, isInsertNode := node.(*plan.InsertInto)
	return pgtransform.NodeWithOpaque(ctx, node, func(ctx *sql.Context, n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		if windowNode, ok := n.(*plan.Window); ok {
			newNode, sameWindow, err := rewriteWindowSelectExprCasts(ctx, windowNode)
			if err != nil {
				return nil, transform.SameTree, err
			}
			newNode, sameFunctions, err := optimizeNodeCompiledFunctions(ctx, a, newNode)
			return newNode, sameWindow && sameFunctions, err
		}
		if groupByNode, ok := n.(*plan.GroupBy); ok {
			groupByNode, sameVectorAggregates, err := rewriteGroupByVectorAggregates(ctx, groupByNode)
			if err != nil {
				return nil, transform.SameTree, err
			}
			newNode, sameAggregateCasts, err := rewriteGroupByAggregateCasts(ctx, groupByNode)
			if err != nil {
				return nil, transform.SameTree, err
			}
			newNode, sameFunctions, err := optimizeNodeCompiledFunctions(ctx, a, newNode)
			return newNode, sameVectorAggregates && sameAggregateCasts && sameFunctions, err
		}
		if sortNode, ok := n.(*plan.Sort); ok {
			sortNode, sameSort := rewriteSortFieldsWithProjectedSRFs(ctx, sortNode)
			newNode, sameFunctions, err := optimizeNodeCompiledFunctions(ctx, a, sortNode)
			return newNode, sameSort && sameFunctions, err
		}
		if topNNode, ok := n.(*plan.TopN); ok {
			topNNode, sameTopN := rewriteTopNFieldsWithProjectedSRFs(ctx, topNNode)
			newNode, sameFunctions, err := optimizeNodeCompiledFunctions(ctx, a, topNNode)
			return newNode, sameTopN && sameFunctions, err
		}
		if filterNode, ok := n.(*plan.Filter); ok {
			filterNode, sameGetFields := rewriteFilterGetFieldsFromChildSchema(ctx, filterNode)
			newNode, sameFunctions, err := optimizeNodeCompiledFunctions(ctx, a, filterNode)
			return newNode, sameGetFields && sameFunctions, err
		}
		if havingNode, ok := n.(*plan.Having); ok {
			havingNode, sameGetFields := rewriteHavingGetFieldsFromChildSchema(ctx, havingNode)
			newNode, sameFunctions, err := optimizeNodeCompiledFunctions(ctx, a, havingNode)
			return newNode, sameGetFields && sameFunctions, err
		}
		if createView, ok := n.(*plan.CreateView); ok {
			return preserveCreateViewFunctionBindings(ctx, createView)
		}

		projectNode, ok := n.(*plan.Project)
		if !ok {
			return optimizeNodeCompiledFunctions(ctx, a, n)
		}
		projectNode, sameProjection := rewriteProjectionsWithProjectedSRFs(ctx, projectNode)
		projectNode, sameGetFields := rewriteProjectionGetFieldsFromChildSchema(ctx, projectNode)

		hasMultipleExpressionTuples := false
		hasSRF := false
		// Check if there is set returning function in the source node (e.g. SELECT * FROM unnest())
		n, sameNode, err := transform.NodeExprsWithNode(ctx, projectNode.Child, func(ctx *sql.Context, in sql.Node, expr sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
			expr, sameFunction, err := optimizeCompiledFunction(ctx, a, in, expr)
			if err != nil {
				return nil, transform.SameTree, err
			}
			if rowIterExpr, ok := expr.(sql.RowIterExpression); ok {
				hasSRF = hasSRF || rowIterExpr.ReturnsRowIter()
			}
			if v, ok := in.(*plan.Values); ok {
				hasMultipleExpressionTuples = len(v.ExpressionTuples) > 1
			}
			return expr, sameFunction, nil
		})
		if err != nil {
			return nil, transform.SameTree, err
		}
		if !sameNode {
			projectNode.Child = n
		}
		projectNode, sameGetFieldsAfterChildRewrite := rewriteProjectionGetFieldsFromChildSchema(ctx, projectNode)

		// insert node cannot have more than 1 row value if it has set returning function
		if isInsertNode && hasMultipleExpressionTuples && hasSRF {
			return nil, false, pgerror.New(pgcode.FeatureNotSupported, "set-returning functions are not allowed in VALUES")
		}

		// Check if there is set returning function in the projection expressions (e.g. SELECT unnest() [FROM table/srf])
		exprs, sameExprs, err := transform.Exprs(ctx, projectNode.Projections, func(ctx *sql.Context, expr sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
			return optimizeCompiledFunction(ctx, a, projectNode, expr)
		})
		if err != nil {
			return nil, transform.SameTree, err
		}
		if !sameExprs {
			projectNode.Projections = exprs
		}
		hasSRFInProjection := false
		for _, expr := range projectNode.Projections {
			if expressionReturnsRowIter(ctx, expr) {
				hasSRFInProjection = true
				break
			}
		}

		// nested iter is used for set returning functions in the projections only
		if hasSRFInProjection {
			// Under some conditions, there will be no quick-function replacement, but changing the Project node to include
			// nested iterators is still a change we need to tell the transform functions about.
			sameExprs = transform.NewTree
			projectNode = projectNode.WithIncludesNestedIters(true)
		}

		return projectNode, sameNode && sameExprs && sameProjection && sameGetFields && sameGetFieldsAfterChildRewrite, err
	})
}

func optimizeNodeCompiledFunctions(ctx *sql.Context, a *analyzer.Analyzer, node sql.Node) (sql.Node, transform.TreeIdentity, error) {
	return transform.OneNodeExprsWithNode(ctx, node, func(ctx *sql.Context, in sql.Node, expr sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		return optimizeCompiledFunction(ctx, a, in, expr)
	})
}

func optimizeCompiledFunction(ctx *sql.Context, a *analyzer.Analyzer, in sql.Node, expr sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
	compiledFunction, ok := expr.(*framework.CompiledFunction)
	if !ok {
		return expr, transform.SameTree, nil
	}

	compiledFunction = compiledFunction.SetStatementRunner(ctx, a.Runner).(*framework.CompiledFunction)
	if err := checkResolvedRoutineExecutePrivilege(ctx, compiledFunction); err != nil {
		return nil, transform.SameTree, err
	}
	// TODO: need better way to detect sequence usage
	switch compiledFunction.FunctionName() {
	case "nextval", "setval", "currval":
		if !skipStoredDefaultSequenceAuth(in) {
			err := authCheckSequenceFromExpr(ctx, a.Catalog.AuthHandler, compiledFunction.Arguments[0])
			if err != nil {
				return nil, transform.SameTree, err
			}
		}
	}
	if quickFunction := compiledFunction.GetQuickFunction(); quickFunction != nil {
		return quickFunction, transform.NewTree, nil
	}

	// Fill in default exprs if applicable.
	if err := compiledFunction.ResolveDefaultValues(ctx, func(defExpr string) (sql.Expression, error) {
		return getDefaultExpr(ctx, a.Catalog, defExpr)
	}); err != nil {
		return nil, transform.SameTree, err
	}
	return compiledFunction, transform.NewTree, nil
}

func skipStoredDefaultSequenceAuth(node sql.Node) bool {
	switch node.(type) {
	case *plan.AlterDefaultSet, *plan.AddColumn, *plan.ModifyColumn, *plan.CreateTable, *pgnodes.CreateTable:
		return true
	default:
		return false
	}
}

func rewriteGroupByVectorAggregates(ctx *sql.Context, groupByNode *plan.GroupBy) (*plan.GroupBy, transform.TreeIdentity, error) {
	selectDeps, sameExprs, err := transform.Exprs(ctx, groupByNode.SelectDeps, func(ctx *sql.Context, expr sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		switch agg := expr.(type) {
		case *gmsaggregation.Sum:
			child := agg.Children()[0]
			if !expressionTypeIsVector(ctx, child) {
				return expr, transform.SameTree, nil
			}
			return vectorAggregateWithID(pgexprs.NewVectorSum(child), agg), transform.NewTree, nil
		case *gmsaggregation.Avg:
			child := agg.Children()[0]
			if !expressionTypeIsVector(ctx, child) {
				return expr, transform.SameTree, nil
			}
			return vectorAggregateWithID(pgexprs.NewVectorAvg(child), agg), transform.NewTree, nil
		default:
			return expr, transform.SameTree, nil
		}
	})
	if err != nil {
		return nil, transform.SameTree, err
	}
	if sameExprs {
		return groupByNode, transform.SameTree, nil
	}
	exprs := make([]sql.Expression, 0, len(selectDeps)+len(groupByNode.GroupByExprs))
	exprs = append(exprs, selectDeps...)
	exprs = append(exprs, groupByNode.GroupByExprs...)
	newNode, err := groupByNode.WithExpressions(ctx, exprs...)
	if err != nil {
		return nil, transform.SameTree, err
	}
	newGroupByNode, ok := newNode.(*plan.GroupBy)
	if !ok {
		return nil, transform.SameTree, errors.Errorf("expected GroupBy, got %T", newNode)
	}
	return newGroupByNode, transform.NewTree, nil
}

func expressionTypeIsVector(ctx *sql.Context, expr sql.Expression) bool {
	dt, ok := expr.Type(ctx).(*pgtypes.DoltgresType)
	if !ok {
		return false
	}
	typeName, ok := pgtypes.PgvectorBaseTypeName(dt)
	return ok && typeName == "vector"
}

func vectorAggregateWithID(newAgg sql.IdExpression, oldAgg sql.IdExpression) sql.Expression {
	return newAgg.WithId(oldAgg.Id())
}

func rewriteGroupByAggregateCasts(ctx *sql.Context, groupByNode *plan.GroupBy) (sql.Node, transform.TreeIdentity, error) {
	selectDeps := make([]sql.Expression, len(groupByNode.SelectDeps))
	copy(selectDeps, groupByNode.SelectDeps)

	var changed bool
	for i, expr := range selectDeps {
		if _, ok := expr.(*pgexprs.AggregationGMSCast); ok {
			continue
		}
		aggregation, ok := expr.(sql.Aggregation)
		if !ok {
			continue
		}
		if _, ok := expr.(framework.Function); ok {
			continue
		}
		if _, ok := pgexprs.FunctionDoltgresType(ctx, expr); ok {
			selectDeps[i] = pgexprs.NewAggregationGMSCast(aggregation)
			changed = true
			continue
		}
		if _, ok := expr.Type(ctx).(*pgtypes.DoltgresType); !ok {
			selectDeps[i] = pgexprs.NewAggregationGMSCast(aggregation)
			changed = true
		}
	}
	if !changed {
		return groupByNode, transform.SameTree, nil
	}
	exprs := make([]sql.Expression, 0, len(selectDeps)+len(groupByNode.GroupByExprs))
	exprs = append(exprs, selectDeps...)
	exprs = append(exprs, groupByNode.GroupByExprs...)
	newNode, err := groupByNode.WithExpressions(ctx, exprs...)
	if err != nil {
		return nil, transform.SameTree, err
	}
	return newNode, transform.NewTree, nil
}

func rewriteWindowSelectExprCasts(ctx *sql.Context, windowNode *plan.Window) (sql.Node, transform.TreeIdentity, error) {
	selectExprs := make([]sql.Expression, len(windowNode.SelectExprs))
	copy(selectExprs, windowNode.SelectExprs)

	var changed bool
	for i, expr := range selectExprs {
		if _, ok := expr.(*pgexprs.WindowGMSCast); ok {
			continue
		}
		windowExpr, ok := expr.(sql.WindowAdaptableExpression)
		if !ok || windowExpr.Window() == nil {
			continue
		}
		if _, ok := pgexprs.WindowFunctionDoltgresType(ctx, expr); ok {
			selectExprs[i] = pgexprs.NewWindowGMSCast(windowExpr)
			changed = true
			continue
		}
		if _, ok := expr.Type(ctx).(*pgtypes.DoltgresType); !ok {
			selectExprs[i] = pgexprs.NewWindowGMSCast(windowExpr)
			changed = true
		}
	}
	if !changed {
		return windowNode, transform.SameTree, nil
	}
	newNode, err := windowNode.WithExpressions(ctx, selectExprs...)
	if err != nil {
		return nil, transform.SameTree, err
	}
	return newNode, transform.NewTree, nil
}

func rewriteProjectionGetFieldsFromChildSchema(ctx *sql.Context, projectNode *plan.Project) (*plan.Project, transform.TreeIdentity) {
	childSchema := projectNode.Child.Schema(ctx)
	projections := make([]sql.Expression, len(projectNode.Projections))
	copy(projections, projectNode.Projections)

	var changed bool
	for i, projection := range projections {
		rewritten, ok := rewriteGetFieldsFromSchema(ctx, childSchema, projection)
		if !ok {
			continue
		}
		projections[i] = rewritten
		changed = true
	}
	if !changed {
		return projectNode, transform.SameTree
	}
	return copyProjectWithProjections(projectNode, projections), transform.NewTree
}

func rewriteFilterGetFieldsFromChildSchema(ctx *sql.Context, filterNode *plan.Filter) (*plan.Filter, transform.TreeIdentity) {
	rewritten, ok := rewriteGetFieldsFromSchema(ctx, filterNode.Child.Schema(ctx), filterNode.Expression)
	if !ok {
		return filterNode, transform.SameTree
	}
	return plan.NewFilter(rewritten, filterNode.Child), transform.NewTree
}

func rewriteHavingGetFieldsFromChildSchema(ctx *sql.Context, havingNode *plan.Having) (*plan.Having, transform.TreeIdentity) {
	rewritten, ok := rewriteGetFieldsFromSchema(ctx, havingNode.Child.Schema(ctx), havingNode.Cond)
	if !ok {
		return havingNode, transform.SameTree
	}
	return plan.NewHaving(rewritten, havingNode.Child), transform.NewTree
}

func rewriteGetFieldsFromSchema(ctx *sql.Context, schema sql.Schema, expr sql.Expression) (sql.Expression, bool) {
	if alias, ok := expr.(*expression.Alias); ok {
		rewrittenChild, changed := rewriteGetFieldsFromSchema(ctx, schema, alias.Child)
		if !changed {
			return expr, false
		}
		newAlias := expression.NewAlias(alias.Name(), rewrittenChild)
		if alias.Unreferencable() {
			newAlias = newAlias.AsUnreferencable()
		}
		return newAlias.WithId(alias.Id()).(sql.Expression), true
	}

	rewritten, same, _ := transform.Expr(ctx, expr, func(ctx *sql.Context, in sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		getField, ok := in.(*expression.GetField)
		if !ok {
			return in, transform.SameTree, nil
		}
		idx := getField.Index()
		if idx < 0 || idx >= len(schema) {
			return in, transform.SameTree, nil
		}
		col := schema[idx]
		getFieldType := getField.Type(ctx)
		if getFieldType == nil || col == nil || col.Type == nil {
			return in, transform.SameTree, nil
		}
		if getFieldType.String() == col.Type.String() && getField.IsNullable(ctx) == col.Nullable {
			return in, transform.SameTree, nil
		}
		rewritten := expression.NewGetFieldWithTable(
			idx,
			int(getField.TableID()),
			col.Type,
			getField.Database(),
			getField.Table(),
			getField.Name(),
			col.Nullable,
		)
		return rewritten.WithId(getField.Id()).(sql.Expression), transform.NewTree, nil
	})
	return rewritten, same == transform.NewTree
}

func copyProjectWithProjections(projectNode *plan.Project, projections []sql.Expression) *plan.Project {
	newProject := plan.NewProject(projections, projectNode.Child)
	if projectNode.IncludesNestedIters {
		newProject = newProject.WithIncludesNestedIters(true)
	}
	if projectNode.CanDefer {
		newProject = newProject.WithCanDefer(true)
	}
	if projectNode.AliasDeps != nil {
		newProject = newProject.WithAliasDeps(projectNode.AliasDeps)
	}
	return newProject
}

func rewriteProjectionsWithProjectedSRFs(ctx *sql.Context, projectNode *plan.Project) (*plan.Project, transform.TreeIdentity) {
	projections := make([]sql.Expression, len(projectNode.Projections))
	copy(projections, projectNode.Projections)

	var changed bool
	for i, projection := range projections {
		if !expressionReturnsRowIter(ctx, projection) {
			continue
		}
		nameable, ok := projection.(sql.Nameable)
		if !ok {
			continue
		}
		getField, ok := projectedSRFGetField(ctx, projectNode.Child, nameable.Name())
		if !ok {
			continue
		}
		projections[i] = getField
		changed = true
	}
	if !changed {
		return projectNode, transform.SameTree
	}
	return copyProjectWithProjections(projectNode, projections), transform.NewTree
}

func rewriteSortFieldsWithProjectedSRFs(ctx *sql.Context, sortNode *plan.Sort) (*plan.Sort, transform.TreeIdentity) {
	fields := make(sql.SortFields, len(sortNode.SortFields))
	copy(fields, sortNode.SortFields)

	var changed bool
	for i, field := range fields {
		if !expressionReturnsRowIter(ctx, field.Column) {
			continue
		}
		nameable, ok := field.Column.(sql.Nameable)
		if !ok {
			continue
		}
		getField, ok := projectedSRFGetField(ctx, sortNode.Child, nameable.Name())
		if !ok {
			continue
		}
		fields[i].Column = getField
		changed = true
	}
	if !changed {
		return sortNode, transform.SameTree
	}
	return plan.NewSort(fields, sortNode.Child), transform.NewTree
}

func rewriteTopNFieldsWithProjectedSRFs(ctx *sql.Context, topNNode *plan.TopN) (*plan.TopN, transform.TreeIdentity) {
	fields := make(sql.SortFields, len(topNNode.Fields))
	copy(fields, topNNode.Fields)

	var changed bool
	for i, field := range fields {
		if !expressionReturnsRowIter(ctx, field.Column) {
			continue
		}
		nameable, ok := field.Column.(sql.Nameable)
		if !ok {
			continue
		}
		getField, ok := projectedSRFGetField(ctx, topNNode.Child, nameable.Name())
		if !ok {
			continue
		}
		fields[i].Column = getField
		changed = true
	}
	if !changed {
		return topNNode, transform.SameTree
	}
	newTopN := plan.NewTopN(fields, topNNode.Limit, topNNode.Child)
	newTopN.CalcFoundRows = topNNode.CalcFoundRows
	return newTopN, transform.NewTree
}

func projectedSRFGetField(ctx *sql.Context, child sql.Node, name string) (sql.Expression, bool) {
	// ORDER BY aliases are materialized in an inner Project. Reuse that projected
	// SRF column instead of re-evaluating the SRF in Sort/TopN/final Project.
	projectNode, ok := child.(*plan.Project)
	if !ok {
		switch node := child.(type) {
		case *plan.Sort:
			projectNode, ok = node.Child.(*plan.Project)
		case *plan.TopN:
			projectNode, ok = node.Child.(*plan.Project)
		default:
			return nil, false
		}
		if !ok {
			return nil, false
		}
	}
	childSchema := child.Schema(ctx)
	if len(projectNode.Projections) > len(childSchema) {
		return nil, false
	}
	for colIdx, projection := range projectNode.Projections {
		if !expressionReturnsRowIter(ctx, projection) {
			continue
		}
		nameable, ok := projection.(sql.Nameable)
		if !ok || !strings.EqualFold(nameable.Name(), name) {
			continue
		}
		col := childSchema[colIdx]
		return expression.NewGetFieldWithTable(
			colIdx,
			0,
			col.Type,
			col.DatabaseSource,
			col.Source,
			col.Name,
			col.Nullable,
		), true
	}
	return nil, false
}

func expressionReturnsRowIter(ctx *sql.Context, expr sql.Expression) bool {
	var found bool
	sql.Inspect(ctx, expr, func(ctx *sql.Context, expr sql.Expression) bool {
		if _, ok := expr.(pgexprs.ArrayFromRowIter); ok {
			return false
		}
		rowIterExpr, ok := expr.(sql.RowIterExpression)
		if ok && rowIterExpr.ReturnsRowIter() {
			found = true
			return false
		}
		return true
	})
	return found
}

// getDefaultExpr takes the default value definition, parses, builds and returns sql.ColumnDefaultValue.
func getDefaultExpr(ctx *sql.Context, c sql.Catalog, defExpr string) (sql.Expression, error) {
	builder := planbuilder.New(ctx, c, nil)
	proj, _, _, _, err := builder.Parse(fmt.Sprintf("select %s", defExpr), nil, false)
	if err != nil {
		return nil, err
	}
	parsedExpr := proj.(*plan.Project).Projections[0]
	if a, ok := parsedExpr.(*expression.Alias); ok {
		parsedExpr = a.Child
	}
	return parsedExpr, nil
}
