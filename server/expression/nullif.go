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
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/server/functions/framework"
)

// NullIf implements PostgreSQL NULLIF using the normal binary equality
// operator, so comparison coercion and errors match other PostgreSQL operators.
type NullIf struct {
	left  sql.Expression
	right sql.Expression
	equal sql.Expression
}

var _ vitess.Injectable = (*NullIf)(nil)
var _ sql.Expression = (*NullIf)(nil)

// NewNullIf returns a new *NullIf.
func NewNullIf() *NullIf {
	return &NullIf{}
}

// Children implements the sql.Expression interface.
func (n *NullIf) Children() []sql.Expression {
	return []sql.Expression{n.left, n.right}
}

// Eval implements the sql.Expression interface.
func (n *NullIf) Eval(ctx *sql.Context, row sql.Row) (any, error) {
	equal, err := n.equal.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if equal == true {
		return nil, nil
	}
	return n.left.Eval(ctx, row)
}

// IsNullable implements the sql.Expression interface.
func (n *NullIf) IsNullable(*sql.Context) bool {
	return true
}

// Resolved implements the sql.Expression interface.
func (n *NullIf) Resolved() bool {
	return n.left != nil && n.right != nil && n.left.Resolved() && n.right.Resolved()
}

// String implements the sql.Expression interface.
func (n *NullIf) String() string {
	if n.left == nil || n.right == nil {
		return "NULLIF(?, ?)"
	}
	return fmt.Sprintf("NULLIF(%s, %s)", n.left.String(), n.right.String())
}

// Type implements the sql.Expression interface.
func (n *NullIf) Type(ctx *sql.Context) sql.Type {
	return n.left.Type(ctx)
}

// WithChildren implements the sql.Expression interface.
func (n *NullIf) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(n, len(children), 2)
	}
	return n.withChildren(ctx, children[0], children[1])
}

// WithResolvedChildren implements the vitess.Injectable interface.
func (n *NullIf) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 2 {
		return nil, errors.Errorf("invalid vitess child count, expected `2` but got `%d`", len(children))
	}
	left, ok := children[0].(sql.Expression)
	if !ok {
		return nil, errors.Errorf("expected vitess child to be an expression but has type `%T`", children[0])
	}
	right, ok := children[1].(sql.Expression)
	if !ok {
		return nil, errors.Errorf("expected vitess child to be an expression but has type `%T`", children[1])
	}
	return n.withChildren(ctx.(*sql.Context), left, right)
}

func (n *NullIf) withChildren(ctx *sql.Context, left sql.Expression, right sql.Expression) (*NullIf, error) {
	equal, err := NewBinaryOperator(framework.Operator_BinaryEqual).WithChildren(ctx, left, right)
	if err != nil {
		return nil, err
	}
	binaryEqual := equal.(*BinaryOperator)
	return &NullIf{
		left:  binaryEqual.Left(),
		right: binaryEqual.Right(),
		equal: binaryEqual,
	}, nil
}
