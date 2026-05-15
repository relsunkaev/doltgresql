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

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"
	gmsexpression "github.com/dolthub/go-mysql-server/sql/expression"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// SetOpProjection is a no-op wrapper for bare column projections inside
// UNION / INTERSECT / EXCEPT operands. It preserves the child value and type
// without implementing sql.IdExpression, which prevents the GMS set-op planner
// from reusing sparse source-table indexes as output projection indexes.
type SetOpProjection struct {
	child      sql.Expression
	sourceName string
	typ        sql.Type
}

var _ sql.Expression = (*SetOpProjection)(nil)
var _ vitess.Injectable = (*SetOpProjection)(nil)

func NewSetOpProjection(sourceName string) *SetOpProjection {
	return &SetOpProjection{sourceName: sourceName}
}

func (p *SetOpProjection) Children() []sql.Expression {
	if p.child == nil {
		return nil
	}
	return []sql.Expression{p.child}
}

func (p *SetOpProjection) Eval(ctx *sql.Context, row sql.Row) (any, error) {
	return p.child.Eval(ctx, row)
}

func (p *SetOpProjection) IsNullable(ctx *sql.Context) bool {
	return p.child.IsNullable(ctx)
}

func (p *SetOpProjection) Resolved() bool {
	return p.child != nil && p.child.Resolved()
}

func (p *SetOpProjection) String() string {
	if p.child == nil {
		return "unresolved"
	}
	if p.sourceName != "" {
		return p.sourceName
	}
	return p.child.String()
}

func (p *SetOpProjection) Type(ctx *sql.Context) sql.Type {
	if p.typ != nil {
		return p.typ
	}
	return setOpProjectionType(ctx, p.child)
}

func setOpProjectionType(ctx *sql.Context, child sql.Expression) sql.Type {
	if field, ok := child.(*gmsexpression.GetField); ok {
		switch field.Name() {
		case "objid", "refobjid":
			if field.Type(ctx).Equals(pgtypes.Int32) {
				return pgtypes.Oid
			}
		}
	}
	return child.Type(ctx)
}

func (p *SetOpProjection) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 1)
	}
	typ := p.typ
	if typ == nil {
		typ = setOpProjectionType(ctx, p.child)
	}
	if !setOpProjectionType(ctx, children[0]).Equals(typ) {
		return p, nil
	}
	return &SetOpProjection{child: children[0], sourceName: p.sourceName, typ: typ}, nil
}

func (p *SetOpProjection) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 1 {
		return nil, errors.Errorf("invalid vitess child count, expected 1 but got %d", len(children))
	}
	child, ok := children[0].(sql.Expression)
	if !ok {
		return nil, errors.Errorf("expected vitess child to be an expression but has type %T", children[0])
	}
	return &SetOpProjection{child: child, sourceName: p.sourceName, typ: setOpProjectionType(nil, child)}, nil
}
