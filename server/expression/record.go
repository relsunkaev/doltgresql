// Copyright 2025 Dolthub, Inc.
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

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// NewRecordExpr creates a new record expression.
func NewRecordExpr() *RecordExpr {
	return &RecordExpr{}
}

// NewRecordExpansion marks a composite child that should be flattened into a
// surrounding row constructor.
func NewRecordExpansion() *RecordExpansion {
	return &RecordExpansion{}
}

// RecordExpr is a set of sql.Expressions wrapped together in a single value.
type RecordExpr struct {
	exprs []sql.Expression
}

var _ sql.Expression = (*RecordExpr)(nil)
var _ vitess.Injectable = (*RecordExpr)(nil)

// Resolved implements the sql.Expression interface.
func (t *RecordExpr) Resolved() bool {
	for _, expr := range t.exprs {
		if !expr.Resolved() {
			return false
		}
	}
	return true
}

// String implements the sql.Expression interface.
func (t *RecordExpr) String() string {
	return "RECORD EXPR"
}

// Type implements the sql.Expression interface.
func (t *RecordExpr) Type(ctx *sql.Context) sql.Type {
	return pgtypes.Record
}

// IsNullable implements the sql.Expression interface.
func (t *RecordExpr) IsNullable(ctx *sql.Context) bool {
	return false
}

// Eval implements the sql.Expression interface.
func (t *RecordExpr) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	vals := make([]pgtypes.RecordValue, 0, len(t.exprs))
	for _, expr := range t.exprs {
		val, err := expr.Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		if _, ok := expr.(*RecordExpansion); ok {
			recordValues, ok := val.([]pgtypes.RecordValue)
			if !ok {
				return nil, fmt.Errorf("expected a record expansion, but got %T", val)
			}
			vals = append(vals, recordValues...)
			continue
		}

		typ, ok := expr.Type(ctx).(*pgtypes.DoltgresType)
		if !ok {
			return nil, fmt.Errorf("expected a DoltgresType, but got %T", expr.Type(ctx))
		}
		vals = append(vals, pgtypes.RecordValue{
			Value: val,
			Type:  typ,
		})
	}

	return vals, nil
}

// Children implements the sql.Expression interface.
func (t *RecordExpr) Children() []sql.Expression {
	return t.exprs
}

// WithChildren implements the sql.Expression interface.
func (t *RecordExpr) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	tCopy := *t
	tCopy.exprs = children
	return &tCopy, nil
}

// WithResolvedChildren implements the vitess.Injectable interface
func (t *RecordExpr) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	newExpressions := make([]sql.Expression, len(children))
	for i, resolvedChild := range children {
		resolvedExpression, ok := resolvedChild.(sql.Expression)
		if !ok {
			return nil, errors.Errorf("expected vitess child to be an expression but has type `%T`", resolvedChild)
		}
		newExpressions[i] = resolvedExpression
	}
	return t.WithChildren(ctx.(*sql.Context), newExpressions...)
}

// RecordExpansion is a marker expression used inside row constructors for
// qualified table-star arguments, for example ROW(t.*, 1).
type RecordExpansion struct {
	child sql.Expression
}

var _ sql.Expression = (*RecordExpansion)(nil)
var _ vitess.Injectable = (*RecordExpansion)(nil)

// Resolved implements the sql.Expression interface.
func (r *RecordExpansion) Resolved() bool {
	return r.child != nil && r.child.Resolved()
}

// String implements the sql.Expression interface.
func (r *RecordExpansion) String() string {
	return "RECORD EXPANSION"
}

// Type implements the sql.Expression interface.
func (r *RecordExpansion) Type(ctx *sql.Context) sql.Type {
	if r.child == nil {
		return pgtypes.Record
	}
	return r.child.Type(ctx)
}

// IsNullable implements the sql.Expression interface.
func (r *RecordExpansion) IsNullable(ctx *sql.Context) bool {
	return r.child == nil || r.child.IsNullable(ctx)
}

// Eval implements the sql.Expression interface.
func (r *RecordExpansion) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	if r.child == nil {
		return nil, errors.Errorf("record expansion child is unresolved")
	}
	return r.child.Eval(ctx, row)
}

// Children implements the sql.Expression interface.
func (r *RecordExpansion) Children() []sql.Expression {
	if r.child == nil {
		return nil
	}
	return []sql.Expression{r.child}
}

// WithChildren implements the sql.Expression interface.
func (r *RecordExpansion) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, errors.Errorf("record expansion expects 1 child, found %d", len(children))
	}
	rCopy := *r
	rCopy.child = children[0]
	return &rCopy, nil
}

// WithResolvedChildren implements the vitess.Injectable interface
func (r *RecordExpansion) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 1 {
		return nil, errors.Errorf("record expansion expects 1 child, found %d", len(children))
	}
	resolvedExpression, ok := children[0].(sql.Expression)
	if !ok {
		return nil, errors.Errorf("expected vitess child to be an expression but has type `%T`", children[0])
	}
	return r.WithChildren(ctx.(*sql.Context), resolvedExpression)
}
