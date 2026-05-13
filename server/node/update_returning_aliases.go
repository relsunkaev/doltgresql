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
	"github.com/dolthub/go-mysql-server/sql"
	gmsexpr "github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"

	pgexprs "github.com/dolthub/doltgresql/server/expression"
)

// UpdateReturningAliases evaluates UPDATE RETURNING old/new aliases against
// the old+new row pair produced by GMS update execution.
type UpdateReturningAliases struct {
	update *plan.Update
}

var _ sql.DebugStringer = (*UpdateReturningAliases)(nil)
var _ sql.ExecBuilderNode = (*UpdateReturningAliases)(nil)

func NewUpdateReturningAliases(update *plan.Update) *UpdateReturningAliases {
	return &UpdateReturningAliases{update: update}
}

func HasUpdateReturningAlias(ctx *sql.Context, exprs []sql.Expression) bool {
	for _, expr := range exprs {
		if transform.InspectExpr(ctx, expr, func(ctx *sql.Context, expr sql.Expression) bool {
			_, ok := expr.(*pgexprs.UpdateReturningAlias)
			return ok
		}) {
			return true
		}
	}
	return false
}

func (u *UpdateReturningAliases) Children() []sql.Node {
	return u.update.Children()
}

func (u *UpdateReturningAliases) DebugString(ctx *sql.Context) string {
	return sql.DebugString(ctx, u.update)
}

func (u *UpdateReturningAliases) IsReadOnly() bool {
	return false
}

func (u *UpdateReturningAliases) Resolved() bool {
	return u.update != nil && u.update.Resolved()
}

func (u *UpdateReturningAliases) BuildRowIter(ctx *sql.Context, b sql.NodeExecBuilder, row sql.Row) (sql.RowIter, error) {
	updatable, err := plan.GetUpdatable(u.update.Child)
	if err != nil {
		return nil, err
	}
	rowLen := len(updatable.Schema(ctx))
	returning, err := updateReturningExprsForRowPair(ctx, u.update.Returning, rowLen)
	if err != nil {
		return nil, err
	}
	inner := *u.update
	inner.Returning = nil
	child, err := b.Build(ctx, &inner, row)
	if err != nil {
		return nil, err
	}
	return &updateReturningAliasesIter{
		child:     child,
		returning: returning,
	}, nil
}

func (u *UpdateReturningAliases) Schema(ctx *sql.Context) sql.Schema {
	return u.update.Schema(ctx)
}

func (u *UpdateReturningAliases) String() string {
	return u.update.String()
}

func (u *UpdateReturningAliases) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	update, err := u.update.WithChildren(ctx, children...)
	if err != nil {
		return nil, err
	}
	return NewUpdateReturningAliases(update.(*plan.Update)), nil
}

type updateReturningAliasesIter struct {
	child     sql.RowIter
	returning []sql.Expression
}

func (i *updateReturningAliasesIter) Next(ctx *sql.Context) (sql.Row, error) {
	row, err := i.child.Next(ctx)
	if err != nil {
		return nil, err
	}
	retRow := make(sql.Row, 0, len(i.returning))
	for _, expr := range i.returning {
		val, err := expr.Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		retRow = append(retRow, val)
	}
	return retRow, nil
}

func (i *updateReturningAliasesIter) Close(ctx *sql.Context) error {
	return i.child.Close(ctx)
}

func updateReturningExprsForRowPair(ctx *sql.Context, exprs []sql.Expression, rowLen int) ([]sql.Expression, error) {
	rewritten := make([]sql.Expression, len(exprs))
	for i, expr := range exprs {
		rewrittenExpr, err := updateReturningExprForRowPair(ctx, expr, rowLen, pgexprs.UpdateReturningAliasNew)
		if err != nil {
			return nil, err
		}
		rewritten[i] = rewrittenExpr
	}
	return rewritten, nil
}

func updateReturningExprForRowPair(ctx *sql.Context, expr sql.Expression, rowLen int, mode pgexprs.UpdateReturningAliasKind) (sql.Expression, error) {
	if alias, ok := expr.(*pgexprs.UpdateReturningAlias); ok {
		return updateReturningExprForRowPair(ctx, alias.Child(), rowLen, alias.Kind())
	}
	if field, ok := expr.(*gmsexpr.GetField); ok {
		if mode == pgexprs.UpdateReturningAliasNew {
			return field.WithIndex(field.Index() + rowLen), nil
		}
		return field, nil
	}
	children := expr.Children()
	if len(children) == 0 {
		return expr, nil
	}
	rewrittenChildren := make([]sql.Expression, len(children))
	changed := false
	for i, child := range children {
		rewrittenChild, err := updateReturningExprForRowPair(ctx, child, rowLen, mode)
		if err != nil {
			return nil, err
		}
		rewrittenChildren[i] = rewrittenChild
		changed = changed || rewrittenChild != child
	}
	if !changed {
		return expr, nil
	}
	return expr.WithChildren(ctx, rewrittenChildren...)
}
