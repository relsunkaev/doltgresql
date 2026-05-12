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

	"github.com/dolthub/doltgresql/postgres/parser/sem/tree"
	"github.com/dolthub/doltgresql/server/auth"
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

// nodeSelect handles *tree.Select nodes.
func nodeSelect(ctx *Context, node *tree.Select) (vitess.SelectStatement, error) {
	if node == nil {
		return nil, nil
	}
	if node.Select == nil {
		node.Select = &tree.ValuesClause{
			Rows: []tree.Exprs{},
		}
	}
	selectStmt, err := nodeSelectStatement(ctx, node.Select)
	if err != nil {
		return nil, err
	}
	orderBy, err := nodeOrderBy(ctx, node.OrderBy, selectStmt)
	if err != nil {
		return nil, err
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
		selectStmt.OrderBy = orderBy
		selectStmt.With = with
		selectStmt.Limit = limit
		selectStmt.Lock = lock
		return selectStmt, nil
	case *vitess.SetOp:
		selectStmt.OrderBy = orderBy
		selectStmt.With = with
		selectStmt.Limit = limit
		selectStmt.Lock = lock
		return selectStmt, nil
	default:
		return nil, errors.Errorf("SELECT has encountered an unknown clause: `%T`", selectStmt)
	}
}

// nodeSelectStatement handles tree.SelectStatement nodes.
func nodeSelectStatement(ctx *Context, node tree.SelectStatement) (vitess.SelectStatement, error) {
	if node == nil {
		return nil, nil
	}
	ctx.Auth().PushAuthType(auth.AuthType_SELECT)
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
		colName, err := unresolvedNameToColName(expr)
		if err != nil {
			return nil, err
		}

		if expr.Star {
			return &vitess.StarExpr{
				TableName: colName.Qualifier,
			}, nil
		}

		if ctx.InSetOpOperand() {
			if node.As == "" {
				node.As = tree.UnrestrictedName(expr.Parts[0])
			}
			return &vitess.AliasedExpr{
				Expr: vitess.InjectedExpr{
					Expression: pgexprs.NewSetOpProjection(),
					Children:   vitess.Exprs{colName},
				},
				As: vitess.NewColIdent(string(node.As)),
			}, nil
		}
		// We don't set the InputExpression for ColName expressions. This matches the behavior in vitess's
		// post-processing found in ast.go. Input expressions are load bearing for some parts of plan building
		// so we need to match the behavior exactly.
		return &vitess.AliasedExpr{
			Expr: colName,
			As:   vitess.NewColIdent(string(node.As)),
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
			As:              vitess.NewColIdent(string(node.As)),
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
