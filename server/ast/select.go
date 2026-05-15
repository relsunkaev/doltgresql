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
	"sync/atomic"

	"github.com/cockroachdb/errors"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
	"github.com/dolthub/doltgresql/postgres/parser/sem/tree"
	pgexprs "github.com/dolthub/doltgresql/server/expression"
)

// AnonColumnAliasPrefix tags aliases minted for unaliased PostgreSQL
// expressions (`?column?`, `case`). The protocol layer recognises this
// prefix and rewrites these aliases back to the user-visible name on
// the wire so clients still observe Postgres-style result column names
// while the analyzer internally keeps each projection slot uniquely
// identified.
const AnonColumnAliasPrefix = "__doltgres_anon__"

var anonColumnAliasCounter uint64

// anonColumnAlias mints a unique sentinel alias whose suffix encodes
// the user-visible column name (typically `?column?` or `case`).
func anonColumnAlias(displayName string) string {
	n := atomic.AddUint64(&anonColumnAliasCounter, 1)
	return AnonColumnAliasPrefix + displayName + "__" + uint64ToBase36(n)
}

// AnonColumnAliasDisplayName returns the user-visible column name
// embedded in an alias produced by anonColumnAlias, plus a flag
// indicating whether the input matched the sentinel pattern.
func AnonColumnAliasDisplayName(alias string) (string, bool) {
	if !strings.HasPrefix(alias, AnonColumnAliasPrefix) {
		return alias, false
	}
	rest := alias[len(AnonColumnAliasPrefix):]
	idx := strings.LastIndex(rest, "__")
	if idx < 0 {
		return alias, false
	}
	return rest[:idx], true
}

func uint64ToBase36(n uint64) string {
	if n == 0 {
		return "0"
	}
	const digits = "0123456789abcdefghijklmnopqrstuvwxyz"
	var buf [16]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = digits[n%36]
		n /= 36
	}
	return string(buf[i:])
}

func outputColumnIdent(name string) vitess.ColIdent {
	return vitess.NewColIdent(core.EncodePhysicalColumnName(name))
}

// nodeSelect handles *tree.Select nodes.
func nodeSelect(ctx *Context, node *tree.Select) (vitess.SelectStatement, error) {
	if node == nil {
		return nil, nil
	}
	if selectInto := selectIntoClause(node); selectInto != nil {
		return nil, errors.Errorf("SELECT INTO is only supported as a top-level statement")
	}
	if len(node.Locking) > 0 {
		if err := validateLockingClauseTarget(node.Select, node.Locking); err != nil {
			return nil, err
		}
	}
	if node.Select == nil {
		node.Select = &tree.ValuesClause{
			Rows: []tree.Exprs{},
		}
	}
	prevJsonArrayElementAliases := ctx.jsonArrayElementAliases
	if selectClause, ok := node.Select.(*tree.SelectClause); ok {
		ctx.jsonArrayElementAliases = jsonArrayElementAliasesFromFrom(selectClause.From)
	}
	defer func() { ctx.jsonArrayElementAliases = prevJsonArrayElementAliases }()
	selectStmt, err := nodeSelectStatement(ctx, node.Select)
	if err != nil {
		return nil, err
	}
	orderBy, err := nodeOrderBy(ctx, node.OrderBy, selectStmt)
	if err != nil {
		return nil, err
	}
	withTies := node.Limit != nil && node.Limit.WithTies
	if withTies && len(orderBy) == 0 {
		return nil, errors.Errorf("WITH TIES cannot be specified without ORDER BY")
	}
	with, err := nodeWith(ctx, node.With)
	if err != nil {
		return nil, err
	}
	limit, err := nodeLimit(ctx, node.Limit)
	if err != nil {
		return nil, err
	}
	lock, err := nodeLockingClause(ctx, node.Locking)
	if err != nil {
		return nil, err
	}

	switch selectStmt := selectStmt.(type) {
	case *vitess.ParenSelect:
		return selectStmt, nil
	case *vitess.Select:
		expandDistinctOnForNullOrdering(selectStmt, orderBy)
		if withTies {
			selectStmt.QueryOpts.SQLCalcFoundRows = true
		}
		selectStmt.OrderBy = orderBy
		selectStmt.With = with
		selectStmt.Limit = limit
		selectStmt.Lock = lock
		return selectStmt, nil
	case *vitess.SetOp:
		if withTies {
			return nil, errors.Errorf("WITH TIES is not yet supported for set operations")
		}
		selectStmt.OrderBy = orderBy
		selectStmt.With = with
		selectStmt.Limit = limit
		selectStmt.Lock = lock
		return selectStmt, nil
	default:
		return nil, errors.Errorf("SELECT has encountered an unknown clause: `%T`", selectStmt)
	}
}

func nodeSelectInto(ctx *Context, node *tree.Select) (vitess.Statement, bool, error) {
	selectInto := selectIntoClause(node)
	if selectInto == nil {
		return nil, false, nil
	}
	selectCopy := *node
	clauseCopy := *selectCopy.Select.(*tree.SelectClause)
	clauseCopy.Into = nil
	selectCopy.Select = &clauseCopy
	stmt, err := nodeCreateTable(ctx, &tree.CreateTable{
		Table:       selectInto.Table,
		Persistence: selectInto.Persistence,
		AsSource:    &selectCopy,
	})
	return stmt, true, err
}

func nodeDataModifyingCTESelect(ctx *Context, node *tree.Select) (vitess.Statement, bool, error) {
	if node == nil || node.With == nil || node.With.Recursive || len(node.With.CTEList) != 1 {
		return nil, false, nil
	}
	selectClause, ok := node.Select.(*tree.SelectClause)
	if !ok || !simpleSelectFromSingleCTE(selectClause, string(node.With.CTEList[0].Name.Alias)) {
		return nil, false, nil
	}
	if len(node.Locking) > 0 || node.Limit != nil {
		return nil, false, nil
	}

	cte := node.With.CTEList[0]
	if !outerSelectMatchesReturning(selectClause.Exprs, returningExprsForStatement(cte.Stmt)) {
		return nil, false, nil
	}
	switch stmt := cte.Stmt.(type) {
	case *tree.Insert:
		converted, err := nodeInsert(ctx, stmt)
		return converted, true, err
	case *tree.Update:
		converted, err := nodeUpdate(ctx, stmt)
		return converted, true, err
	case *tree.Delete:
		converted, err := nodeDelete(ctx, stmt)
		return converted, true, err
	default:
		return nil, false, nil
	}
}

func simpleSelectFromSingleCTE(selectClause *tree.SelectClause, cteName string) bool {
	if selectClause == nil || cteName == "" {
		return false
	}
	if selectClause.Distinct || len(selectClause.DistinctOn) > 0 || selectClause.Into != nil ||
		selectClause.Where != nil || len(selectClause.GroupBy) > 0 || selectClause.Having != nil ||
		len(selectClause.Window) > 0 || selectClause.TableSelect || len(selectClause.From.Tables) != 1 {
		return false
	}
	aliased, ok := selectClause.From.Tables[0].(*tree.AliasedTableExpr)
	if !ok || aliased.IndexFlags != nil || aliased.Ordinality || aliased.Lateral || aliased.AsOf != nil {
		return false
	}
	if aliased.As.Alias != "" || len(aliased.As.Cols) > 0 || len(aliased.As.ColDefs) > 0 {
		return false
	}
	tableName, ok := aliased.Expr.(*tree.TableName)
	return ok && tableName.Table() == cteName
}

func returningExprsForStatement(stmt tree.Statement) tree.SelectExprs {
	switch stmt := stmt.(type) {
	case *tree.Insert:
		return returningExprs(stmt.Returning)
	case *tree.Update:
		return returningExprs(stmt.Returning)
	case *tree.Delete:
		return returningExprs(stmt.Returning)
	default:
		return nil
	}
}

func returningExprs(returning tree.ReturningClause) tree.SelectExprs {
	if exprs, ok := returning.(*tree.ReturningExprs); ok {
		return tree.SelectExprs(*exprs)
	}
	return nil
}

func outerSelectMatchesReturning(outer tree.SelectExprs, returning tree.SelectExprs) bool {
	if len(outer) == 1 {
		if _, ok := outer[0].Expr.(tree.UnqualifiedStar); ok {
			return len(returning) > 0
		}
	}
	if len(outer) == 0 || len(outer) != len(returning) {
		return false
	}
	for i := range outer {
		if outer[i].As != "" || returning[i].As != "" {
			return false
		}
		if !sameSimpleSelectExpr(outer[i].Expr, returning[i].Expr) {
			return false
		}
	}
	return true
}

func sameSimpleSelectExpr(left tree.Expr, right tree.Expr) bool {
	leftName, ok := simpleSelectExprName(left)
	if !ok {
		return false
	}
	rightName, ok := simpleSelectExprName(right)
	return ok && leftName == rightName
}

func simpleSelectExprName(expr tree.Expr) (string, bool) {
	switch expr := expr.(type) {
	case *tree.UnresolvedName:
		if expr.Star || expr.NumParts != 1 {
			return "", false
		}
		return expr.Parts[0], true
	case *tree.ColumnItem:
		if expr.TableName != nil {
			return "", false
		}
		return expr.Column(), true
	default:
		return "", false
	}
}

func selectIntoClause(node *tree.Select) *tree.SelectInto {
	if node == nil {
		return nil
	}
	clause, ok := node.Select.(*tree.SelectClause)
	if !ok {
		return nil
	}
	return clause.Into
}

func validateLockingClauseTarget(node tree.SelectStatement, locking tree.LockingClause) error {
	switch node := node.(type) {
	case *tree.ParenSelect:
		if node.Select == nil {
			return nil
		}
		return validateLockingClauseTarget(node.Select.Select, locking)
	case *tree.SelectClause:
		if node.Distinct || len(node.GroupBy) > 0 || node.Having != nil || selectExprsContainAggregate(node.Exprs) {
			return errors.Errorf("FOR UPDATE is not allowed with DISTINCT, GROUP BY, aggregate, or HAVING query results")
		}
		if err := validateLockingClauseOfTargets(node.From, locking); err != nil {
			return err
		}
	case *tree.UnionClause:
		return errors.Errorf("FOR UPDATE is not allowed with set operation query results")
	case *tree.ValuesClause:
		return errors.Errorf("FOR UPDATE is not allowed with VALUES query results")
	}
	return nil
}

func validateLockingClauseOfTargets(from tree.From, locking tree.LockingClause) error {
	sourceNames := map[string]struct{}{}
	for _, table := range from.Tables {
		collectLockingClauseSourceNames(table, sourceNames)
	}

	seenTargets := map[string]struct{}{}
	for _, item := range locking {
		if item == nil {
			continue
		}
		for _, target := range item.Targets {
			targetName := strings.ToLower(target.Table())
			if targetName == "" {
				continue
			}
			if _, ok := seenTargets[targetName]; ok {
				continue
			}
			seenTargets[targetName] = struct{}{}
			if _, ok := sourceNames[targetName]; !ok {
				return pgerror.Newf(pgcode.UndefinedTable, "unresolved table name `%s` in locking clause.", targetName)
			}
		}
	}
	return nil
}

func collectLockingClauseSourceNames(table tree.TableExpr, sourceNames map[string]struct{}) {
	switch table := table.(type) {
	case *tree.AliasedTableExpr:
		if table.As.Alias != "" {
			sourceNames[strings.ToLower(string(table.As.Alias))] = struct{}{}
			return
		}
		collectLockingClauseSourceNames(table.Expr, sourceNames)
	case *tree.TableName:
		if tableName := table.Table(); tableName != "" {
			sourceNames[strings.ToLower(tableName)] = struct{}{}
		}
	case *tree.JoinTableExpr:
		collectLockingClauseSourceNames(table.Left, sourceNames)
		collectLockingClauseSourceNames(table.Right, sourceNames)
	case *tree.ParenTableExpr:
		collectLockingClauseSourceNames(table.Expr, sourceNames)
	}
}

func selectExprsContainAggregate(exprs tree.SelectExprs) bool {
	visitor := aggregateFunctionVisitor{}
	for _, expr := range exprs {
		tree.WalkExprConst(&visitor, expr.Expr)
		if visitor.found {
			return true
		}
	}
	return false
}

type aggregateFunctionVisitor struct {
	found bool
}

func (v *aggregateFunctionVisitor) VisitPre(expr tree.Expr) (bool, tree.Expr) {
	if v.found {
		return false, expr
	}
	fn, ok := expr.(*tree.FuncExpr)
	if !ok {
		return true, expr
	}
	if fn.AggType != 0 || isAggregateFunctionName(fn.Func) {
		v.found = true
		return false, expr
	}
	return true, expr
}

func (v *aggregateFunctionVisitor) VisitPost(expr tree.Expr) tree.Expr {
	return expr
}

func isAggregateFunctionName(ref tree.ResolvableFunctionReference) bool {
	var name string
	switch fn := ref.FunctionReference.(type) {
	case *tree.FunctionDefinition:
		name = fn.Name
	case *tree.UnresolvedName:
		if fn.NumParts > 0 {
			name = fn.Parts[0]
		}
	}
	switch strings.ToLower(name) {
	case "array_agg", "avg", "bit_and", "bit_or", "bit_xor", "bool_and", "bool_or",
		"count", "every", "json_agg", "json_object_agg", "jsonb_agg", "jsonb_object_agg",
		"max", "min", "stddev", "stddev_pop", "stddev_samp", "string_agg", "sum",
		"var_pop", "var_samp", "variance":
		return true
	default:
		return false
	}
}

// nodeSelectStatement handles tree.SelectStatement nodes.
func nodeSelectStatement(ctx *Context, node tree.SelectStatement) (vitess.SelectStatement, error) {
	if node == nil {
		return nil, nil
	}
	ctx.Auth().PushAuthType(ctx.SelectAuthType())
	defer ctx.Auth().PopAuthType()

	switch node := node.(type) {
	case *tree.ParenSelect:
		return nodeParenSelect(ctx, node)
	case *tree.SelectClause:
		return nodeSelectClause(ctx, node)
	case *tree.UnionClause:
		return nodeUnionClause(ctx, node)
	case *tree.ValuesClause:
		return nodeValuesClause(ctx, node)
	default:
		return nil, errors.Errorf("unknown type of SELECT statement: `%T`", node)
	}
}

// nodeSelectExpr handles tree.SelectExpr nodes.
func nodeSelectExpr(ctx *Context, node tree.SelectExpr) (vitess.SelectExpr, error) {
	switch expr := node.Expr.(type) {
	case *tree.AllColumnsSelector:
		if expr.TableName.NumParts > 1 {
			return nil, errors.Errorf("referencing items outside the schema or database is not yet supported")
		}
		return &vitess.StarExpr{
			TableName: vitess.TableName{
				Name: vitess.NewTableIdent(expr.TableName.Parts[0]),
			},
		}, nil
	case tree.UnqualifiedStar:
		return &vitess.StarExpr{}, nil
	case *tree.UnresolvedName:
		if ctx.ResolveExcludedRefs() && isExcludedRef(expr) {
			if expr.Star {
				return nil, errors.Errorf("* syntax is not yet supported in this context")
			}
			vitessExpr, err := nodeExpr(ctx, expr)
			if err != nil {
				return nil, err
			}
			return &vitess.AliasedExpr{
				Expr: vitessExpr,
				As:   outputColumnIdent(string(node.As)),
			}, nil
		}
		if vitessExpr, ok := jsonArrayElementAliasExpr(ctx, expr); ok {
			return &vitess.AliasedExpr{
				Expr: vitessExpr,
				As:   outputColumnIdent(string(node.As)),
			}, nil
		}

		colName, err := unresolvedNameToColName(expr)
		if err != nil {
			return nil, err
		}

		if expr.Star {
			return &vitess.StarExpr{
				TableName: colName.Qualifier,
			}, nil
		}
		if wholeRowExpr, ok := wholeRowDuplicateAliasExpr(ctx, expr); ok {
			if node.As == "" {
				node.As = tree.UnrestrictedName(expr.Parts[0])
			}
			return &vitess.AliasedExpr{
				Expr:            wholeRowExpr,
				As:              outputColumnIdent(string(node.As)),
				InputExpression: inputExpressionForSelectExpr(node),
			}, nil
		}

		if ctx.InSetOpOperand() {
			as := vitess.ColIdent{}
			sourceName := ""
			if node.As != "" {
				as = outputColumnIdent(string(node.As))
			} else if expr.NumParts == 1 {
				as = outputColumnIdent(expr.Parts[0])
			} else if colName.Qualifier.Name.String() != "" && expr.NumParts > 0 {
				as = outputColumnIdent(expr.Parts[0])
				sourceName = tree.AsString(expr)
			}
			return &vitess.AliasedExpr{
				Expr: vitess.InjectedExpr{
					Expression: pgexprs.NewSetOpProjection(sourceName),
					Children:   vitess.Exprs{colName},
				},
				As: as,
			}, nil
		}
		// We don't set the InputExpression for ColName expressions. This matches the behavior in vitess's
		// post-processing found in ast.go. Input expressions are load bearing for some parts of plan building
		// so we need to match the behavior exactly.
		return &vitess.AliasedExpr{
			Expr: colName,
			As:   outputColumnIdent(string(node.As)),
		}, nil
	default:
		vitessExpr, err := nodeExpr(ctx, expr)
		if err != nil {
			return nil, err
		}

		if ce, ok := expr.(*tree.CastExpr); ok && node.As == "" {
			hasConst := false
			_, _ = tree.SimpleVisit(expr, func(visitingExpr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
				switch visitingExpr.(type) {
				case tree.Constant:
					hasConst = true
					return false, visitingExpr, nil
				}
				return true, visitingExpr, nil
			})
			if hasConst {
				_, dt, err := nodeResolvableTypeReference(ctx, ce.Type, false)
				if err != nil {
					return nil, err
				}
				// constant value is not part of column name
				// e.g. `1::INT2` should create column name as `int2`.
				node.As = tree.UnrestrictedName(dt.Name())
			} else {
				// cast type is not part of column name
				// e.g. `id::INT2` should create column name as `id`.
				node.As = defaultCastColumnName(ce.Expr)
			}
		}

		// PostgreSQL has its own conventions for the auto-generated
		// column name when the user does not supply AS. Match the
		// most common cases here so the result-row description sent
		// back to clients is what migration tools and ORMs expect.
		// The general rule is: bare literal expressions and operator
		// expressions without a natural name show up as `?column?`;
		// `CASE` shows up as `case`. Function calls are already
		// handled by the engine via the function name.
		//
		// We must keep these aliases unique — otherwise GMS's analyzer
		// assigns the same column id to every `?column?` projection,
		// and INSERT...SELECT with multiple anonymous expressions in a
		// permuted column list collapses both projection slots to the
		// same value. We mint a unique sentinel here and remap it back
		// to the user-visible `?column?` (or `case`) name in the
		// protocol response (see protocolDisplayName in
		// server/doltgres_handler.go).
		if node.As == "" {
			if functionDisplayName, ok := defaultFunctionColumnName(expr); ok {
				node.As = tree.UnrestrictedName(functionDisplayName)
			}
		}

		if node.As == "" {
			switch expr.(type) {
			case *tree.CaseExpr:
				node.As = tree.UnrestrictedName(anonColumnAlias("case"))
			case tree.Constant, *tree.BinaryExpr, *tree.ComparisonExpr,
				*tree.UnaryExpr, *tree.NotExpr, *tree.AndExpr, *tree.OrExpr,
				*tree.IsNullExpr, *tree.IsNotNullExpr, *tree.IsOfTypeExpr,
				*tree.ParenExpr:
				node.As = tree.UnrestrictedName(anonColumnAlias("?column?"))
			}
		}

		return &vitess.AliasedExpr{
			Expr:            vitessExpr,
			As:              outputColumnIdent(string(node.As)),
			InputExpression: inputExpressionForSelectExpr(node),
		}, nil
	}
}

func defaultFunctionColumnName(expr tree.Expr) (string, bool) {
	funcExpr, ok := expr.(*tree.FuncExpr)
	if !ok {
		return "", false
	}
	unresolved, ok := funcExpr.Func.FunctionReference.(*tree.UnresolvedName)
	if !ok || unresolved.NumParts != 1 {
		return "", false
	}
	switch strings.ToLower(unresolved.Parts[0]) {
	case "current_catalog", "current_schema":
		return strings.ToLower(unresolved.Parts[0]), true
	default:
		return "", false
	}
}

func defaultCastColumnName(expr tree.Expr) tree.UnrestrictedName {
	if name, ok := expr.(*tree.UnresolvedName); ok && !name.Star && name.NumParts > 0 {
		return tree.UnrestrictedName(name.Parts[0])
	}
	return tree.UnrestrictedName(tree.AsString(expr))
}

// inputExpressionForSelectExpr returns the input expression for a tree.SelectExpr.
// Postgres has specific handling for function calls that differs from the default printing behavior.
func inputExpressionForSelectExpr(node tree.SelectExpr) string {
	inputExpression := tree.AsStringWithFlags(&node, tree.FmtOmitFunctionArgs)
	// To be consistent with vitess handling, InputExpression always gets its outer quotes trimmed
	if strings.HasPrefix(inputExpression, "'") && strings.HasSuffix(inputExpression, "'") {
		inputExpression = inputExpression[1 : len(inputExpression)-1]
	}
	return inputExpression
}

// nodeSelectExprs handles tree.SelectExprs nodes.
func nodeSelectExprs(ctx *Context, node tree.SelectExprs) (vitess.SelectExprs, error) {
	if len(node) == 0 {
		return nil, nil
	}
	selectExprs := make(vitess.SelectExprs, len(node))
	for i := range node {
		var err error
		selectExprs[i], err = nodeSelectExpr(ctx, node[i])
		if err != nil {
			return nil, err
		}
	}
	return selectExprs, nil
}

// nodeExprToSelectExpr handles tree.Expr nodes and returns the result as a vitess.SelectExpr.
func nodeExprToSelectExpr(ctx *Context, node tree.Expr) (vitess.SelectExpr, error) {
	if node == nil {
		return nil, nil
	}
	return nodeSelectExpr(ctx, tree.SelectExpr{
		Expr: node,
	})
}

// nodeExprsToSelectExprs handles tree.Exprs nodes and returns the results as vitess.SelectExprs.
func nodeExprsToSelectExprs(ctx *Context, node tree.Exprs) (vitess.SelectExprs, error) {
	if len(node) == 0 {
		return nil, nil
	}
	selectExprs := make(vitess.SelectExprs, len(node))
	for i := range node {
		var err error
		selectExprs[i], err = nodeSelectExpr(ctx, tree.SelectExpr{
			Expr: node[i],
		})
		if err != nil {
			return nil, err
		}
	}
	return selectExprs, nil
}
