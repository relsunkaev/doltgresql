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
	"io"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// ArrayFromRowIter evaluates a scalar or set-returning child expression as the
// element source for ARRAY(SELECT expr) forms that have no FROM clause.
type ArrayFromRowIter struct {
	Child sql.Expression
}

var _ vitess.Injectable = (*ArrayFromRowIter)(nil)
var _ sql.Expression = (*ArrayFromRowIter)(nil)

func NewArrayFromRowIter() ArrayFromRowIter {
	return ArrayFromRowIter{}
}

// Resolved implements sql.Expression.
func (a ArrayFromRowIter) Resolved() bool {
	return a.Child != nil && a.Child.Resolved()
}

// String implements sql.Expression.
func (a ArrayFromRowIter) String() string {
	if a.Child == nil {
		return "ARRAY(unresolved)"
	}
	return "ARRAY(SELECT " + a.Child.String() + ")"
}

// Type implements sql.Expression.
func (a ArrayFromRowIter) Type(ctx *sql.Context) sql.Type {
	childType := a.Child.Type(ctx)
	dt, ok := childType.(*pgtypes.DoltgresType)
	if !ok {
		return childType
	}
	if dt.ID == pgtypes.Row.ID {
		return dt.BaseType().ToArrayType()
	}
	return dt.ToArrayType()
}

// IsNullable implements sql.Expression.
func (a ArrayFromRowIter) IsNullable(ctx *sql.Context) bool {
	return false
}

// Eval implements sql.Expression.
func (a ArrayFromRowIter) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	value, err := a.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	iter, ok := value.(sql.RowIter)
	if !ok {
		return []any{value}, nil
	}

	values := make([]any, 0)
	for {
		iterRow, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			_ = iter.Close(ctx)
			return nil, err
		}
		if len(iterRow) == 1 {
			values = append(values, iterRow[0])
		} else {
			values = append(values, append(sql.Row{}, iterRow...))
		}
	}
	if err := iter.Close(ctx); err != nil {
		return nil, err
	}
	return values, nil
}

// Children implements sql.Expression.
func (a ArrayFromRowIter) Children() []sql.Expression {
	return []sql.Expression{a.Child}
}

// WithChildren implements sql.Expression.
func (a ArrayFromRowIter) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(a, len(children), 1)
	}
	return ArrayFromRowIter{Child: children[0]}, nil
}

// WithResolvedChildren implements vitess.Injectable.
func (a ArrayFromRowIter) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(a, len(children), 1)
	}
	child, ok := children[0].(sql.Expression)
	if !ok {
		return nil, errors.Errorf("expected sql.Expression child, got %T", children[0])
	}
	return ArrayFromRowIter{Child: child}, nil
}
