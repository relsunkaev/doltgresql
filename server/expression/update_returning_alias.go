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

package expression

import (
	"context"
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"
)

type UpdateReturningAliasKind uint8

const (
	UpdateReturningAliasOld UpdateReturningAliasKind = iota
	UpdateReturningAliasNew
)

// UpdateReturningAlias marks an UPDATE RETURNING old.col or new.col reference
// until the UPDATE node is wrapped to evaluate against the old+new row pair.
type UpdateReturningAlias struct {
	kind  UpdateReturningAliasKind
	child sql.Expression
}

var _ sql.Expression = (*UpdateReturningAlias)(nil)
var _ vitess.Injectable = (*UpdateReturningAlias)(nil)

func NewUpdateReturningAlias(kind UpdateReturningAliasKind) *UpdateReturningAlias {
	return &UpdateReturningAlias{kind: kind}
}

func (a *UpdateReturningAlias) Kind() UpdateReturningAliasKind {
	return a.kind
}

func (a *UpdateReturningAlias) Child() sql.Expression {
	return a.child
}

func (a *UpdateReturningAlias) Children() []sql.Expression {
	if a.child == nil {
		return nil
	}
	return []sql.Expression{a.child}
}

func (a *UpdateReturningAlias) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	if a.child == nil {
		return nil, fmt.Errorf("unresolved update returning alias")
	}
	return a.child.Eval(ctx, row)
}

func (a *UpdateReturningAlias) IsNullable(ctx *sql.Context) bool {
	return a.child == nil || a.child.IsNullable(ctx)
}

func (a *UpdateReturningAlias) Resolved() bool {
	return a.child != nil && a.child.Resolved()
}

func (a *UpdateReturningAlias) String() string {
	switch a.kind {
	case UpdateReturningAliasOld:
		return "OLD " + a.child.String()
	case UpdateReturningAliasNew:
		return "NEW " + a.child.String()
	default:
		return a.child.String()
	}
}

func (a *UpdateReturningAlias) Type(ctx *sql.Context) sql.Type {
	if a.child == nil {
		return nil
	}
	return a.child.Type(ctx)
}

func (a *UpdateReturningAlias) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(a, len(children), 1)
	}
	ret := *a
	ret.child = children[0]
	return &ret, nil
}

func (a *UpdateReturningAlias) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(a, len(children), 1)
	}
	child, ok := children[0].(sql.Expression)
	if !ok {
		return nil, fmt.Errorf("expected sql.Expression child but found %T", children[0])
	}
	return a.WithChildren(ctx.(*sql.Context), child)
}
