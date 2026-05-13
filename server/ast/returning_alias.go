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

package ast

import (
	"strings"

	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	pgexprs "github.com/dolthub/doltgresql/server/expression"
)

type returningAliasMode int

const (
	returningAliasInsert returningAliasMode = iota
	returningAliasDelete
)

func rewriteInsertDeleteReturningAliases(exprs vitess.SelectExprs, mode returningAliasMode) vitess.SelectExprs {
	if len(exprs) == 0 {
		return exprs
	}
	rewritten := make(vitess.SelectExprs, len(exprs))
	for i, selectExpr := range exprs {
		aliased, ok := selectExpr.(*vitess.AliasedExpr)
		if !ok {
			rewritten[i] = selectExpr
			continue
		}
		copy := *aliased
		copy.Expr = rewriteInsertDeleteReturningAliasExpr(copy.Expr, mode)
		rewritten[i] = &copy
	}
	return rewritten
}

func rewriteUpdateReturningAliases(exprs vitess.SelectExprs) vitess.SelectExprs {
	if len(exprs) == 0 {
		return exprs
	}
	rewritten := make(vitess.SelectExprs, len(exprs))
	for i, selectExpr := range exprs {
		aliased, ok := selectExpr.(*vitess.AliasedExpr)
		if !ok {
			rewritten[i] = selectExpr
			continue
		}
		copy := *aliased
		copy.Expr = rewriteUpdateReturningAliasExpr(copy.Expr)
		rewritten[i] = &copy
	}
	return rewritten
}

func rewriteUpdateReturningAliasExpr(expr vitess.Expr) vitess.Expr {
	switch expr := expr.(type) {
	case *vitess.ColName:
		return rewriteUpdateReturningAliasColumn(expr)
	case *vitess.IsExpr:
		copy := *expr
		copy.Expr = rewriteUpdateReturningAliasExpr(copy.Expr)
		return &copy
	case *vitess.AndExpr:
		copy := *expr
		copy.Left = rewriteUpdateReturningAliasExpr(copy.Left)
		copy.Right = rewriteUpdateReturningAliasExpr(copy.Right)
		return &copy
	case *vitess.OrExpr:
		copy := *expr
		copy.Left = rewriteUpdateReturningAliasExpr(copy.Left)
		copy.Right = rewriteUpdateReturningAliasExpr(copy.Right)
		return &copy
	case *vitess.NotExpr:
		copy := *expr
		copy.Expr = rewriteUpdateReturningAliasExpr(copy.Expr)
		return &copy
	case *vitess.ComparisonExpr:
		copy := *expr
		copy.Left = rewriteUpdateReturningAliasExpr(copy.Left)
		copy.Right = rewriteUpdateReturningAliasExpr(copy.Right)
		return &copy
	case *vitess.BinaryExpr:
		copy := *expr
		copy.Left = rewriteUpdateReturningAliasExpr(copy.Left)
		copy.Right = rewriteUpdateReturningAliasExpr(copy.Right)
		return &copy
	case *vitess.UnaryExpr:
		copy := *expr
		copy.Expr = rewriteUpdateReturningAliasExpr(copy.Expr)
		return &copy
	case *vitess.ParenExpr:
		copy := *expr
		copy.Expr = rewriteUpdateReturningAliasExpr(copy.Expr)
		return &copy
	default:
		return expr
	}
}

func rewriteUpdateReturningAliasColumn(col *vitess.ColName) vitess.Expr {
	qualifier := col.Qualifier
	if qualifier.SchemaQualifier.String() != "" || qualifier.DbQualifier.String() != "" {
		return col
	}
	var kind pgexprs.UpdateReturningAliasKind
	switch {
	case strings.EqualFold(qualifier.Name.String(), "old"):
		kind = pgexprs.UpdateReturningAliasOld
	case strings.EqualFold(qualifier.Name.String(), "new"):
		kind = pgexprs.UpdateReturningAliasNew
	default:
		return col
	}
	copy := *col
	copy.Qualifier = vitess.TableName{}
	return vitess.InjectedExpr{
		Expression: pgexprs.NewUpdateReturningAlias(kind),
		Children:   vitess.Exprs{&copy},
	}
}

func rewriteInsertDeleteReturningAliasExpr(expr vitess.Expr, mode returningAliasMode) vitess.Expr {
	switch expr := expr.(type) {
	case *vitess.ColName:
		return rewriteReturningAliasColumn(expr, mode)
	case *vitess.IsExpr:
		copy := *expr
		copy.Expr = rewriteInsertDeleteReturningAliasExpr(copy.Expr, mode)
		return &copy
	case *vitess.AndExpr:
		copy := *expr
		copy.Left = rewriteInsertDeleteReturningAliasExpr(copy.Left, mode)
		copy.Right = rewriteInsertDeleteReturningAliasExpr(copy.Right, mode)
		return &copy
	case *vitess.OrExpr:
		copy := *expr
		copy.Left = rewriteInsertDeleteReturningAliasExpr(copy.Left, mode)
		copy.Right = rewriteInsertDeleteReturningAliasExpr(copy.Right, mode)
		return &copy
	case *vitess.NotExpr:
		copy := *expr
		copy.Expr = rewriteInsertDeleteReturningAliasExpr(copy.Expr, mode)
		return &copy
	case *vitess.ComparisonExpr:
		copy := *expr
		copy.Left = rewriteInsertDeleteReturningAliasExpr(copy.Left, mode)
		copy.Right = rewriteInsertDeleteReturningAliasExpr(copy.Right, mode)
		return &copy
	case *vitess.BinaryExpr:
		copy := *expr
		copy.Left = rewriteInsertDeleteReturningAliasExpr(copy.Left, mode)
		copy.Right = rewriteInsertDeleteReturningAliasExpr(copy.Right, mode)
		return &copy
	case *vitess.UnaryExpr:
		copy := *expr
		copy.Expr = rewriteInsertDeleteReturningAliasExpr(copy.Expr, mode)
		return &copy
	case *vitess.ParenExpr:
		copy := *expr
		copy.Expr = rewriteInsertDeleteReturningAliasExpr(copy.Expr, mode)
		return &copy
	default:
		return expr
	}
}

func rewriteReturningAliasColumn(col *vitess.ColName, mode returningAliasMode) vitess.Expr {
	qualifier := col.Qualifier
	if qualifier.SchemaQualifier.String() != "" || qualifier.DbQualifier.String() != "" {
		return col
	}
	switch {
	case strings.EqualFold(qualifier.Name.String(), "old"):
		if mode == returningAliasInsert {
			return &vitess.NullVal{}
		}
		copy := *col
		copy.Qualifier = vitess.TableName{}
		return &copy
	case strings.EqualFold(qualifier.Name.String(), "new"):
		if mode == returningAliasDelete {
			return &vitess.NullVal{}
		}
		copy := *col
		copy.Qualifier = vitess.TableName{}
		return &copy
	default:
		return col
	}
}
