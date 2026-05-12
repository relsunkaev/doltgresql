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

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// RowValueField extracts a single field from a row-valued expression.
type RowValueField struct {
	index int
	child sql.Expression
}

var _ sql.Expression = (*RowValueField)(nil)
var _ vitess.Injectable = (*RowValueField)(nil)

// NewRowValueField returns a new RowValueField expression.
func NewRowValueField(index int) *RowValueField {
	return &RowValueField{index: index}
}

// Children implements the sql.Expression interface.
func (r *RowValueField) Children() []sql.Expression {
	return []sql.Expression{r.child}
}

// Eval implements the sql.Expression interface.
func (r *RowValueField) Eval(ctx *sql.Context, row sql.Row) (any, error) {
	evalRow := row
	if subquery, ok := r.child.(*plan.Subquery); ok && subquery.Correlated().Empty() {
		evalRow = nil
	}
	val, err := r.child.Eval(ctx, evalRow)
	if err != nil || val == nil {
		return nil, err
	}
	switch val := val.(type) {
	case []pgtypes.RecordValue:
		if r.index < 0 || r.index >= len(val) {
			return nil, sql.ErrInvalidColumnNumber.New(r.index+1, len(val))
		}
		return val[r.index].Value, nil
	case []any:
		if r.index < 0 || r.index >= len(val) {
			return nil, sql.ErrInvalidColumnNumber.New(r.index+1, len(val))
		}
		return val[r.index], nil
	case sql.Row:
		if r.index < 0 || r.index >= len(val) {
			return nil, sql.ErrInvalidColumnNumber.New(r.index+1, len(val))
		}
		return val[r.index], nil
	default:
		return nil, errors.Errorf("expected row-valued expression, got %T", val)
	}
}

// IsNullable implements the sql.Expression interface.
func (r *RowValueField) IsNullable(ctx *sql.Context) bool {
	return true
}

// Resolved implements the sql.Expression interface.
func (r *RowValueField) Resolved() bool {
	return r.child != nil && r.child.Resolved()
}

// String implements the sql.Expression interface.
func (r *RowValueField) String() string {
	if r.child == nil {
		return fmt.Sprintf("ROW_VALUE_FIELD(%d)", r.index+1)
	}
	return fmt.Sprintf("(%s).@%d", r.child.String(), r.index+1)
}

// Type implements the sql.Expression interface.
func (r *RowValueField) Type(ctx *sql.Context) sql.Type {
	if r.child == nil {
		return pgtypes.Unknown
	}
	switch typ := r.child.Type(ctx).(type) {
	case gmstypes.TupleType:
		if r.index >= 0 && r.index < len(typ) {
			return typ[r.index]
		}
	case *pgtypes.DoltgresType:
		if typ == pgtypes.Record {
			if children := r.child.Children(); r.index >= 0 && r.index < len(children) {
				return children[r.index].Type(ctx)
			}
		}
	}
	return pgtypes.Unknown
}

// WithChildren implements the sql.Expression interface.
func (r *RowValueField) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(r, len(children), 1)
	}
	return &RowValueField{index: r.index, child: children[0]}, nil
}

// WithResolvedChildren implements the vitess.Injectable interface.
func (r *RowValueField) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(r, len(children), 1)
	}
	child, ok := children[0].(sql.Expression)
	if !ok {
		return nil, errors.Errorf("expected vitess child to be an expression but has type `%T`", children[0])
	}
	return r.WithChildren(ctx.(*sql.Context), child)
}
