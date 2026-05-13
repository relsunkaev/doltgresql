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

package framework

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"
)

// NamedArgument preserves PostgreSQL's name => value function-call notation
// until the function framework can reorder arguments by parameter metadata.
type NamedArgument struct {
	name  string
	child sql.Expression
}

var _ vitess.Injectable = (*NamedArgument)(nil)
var _ sql.Expression = (*NamedArgument)(nil)

// NewNamedArgument returns a placeholder named argument for parser injection.
func NewNamedArgument(name string) *NamedArgument {
	return &NamedArgument{name: name}
}

// ArgumentName returns the explicit argument name.
func (n *NamedArgument) ArgumentName() string {
	return n.name
}

// Argument returns the wrapped argument value expression.
func (n *NamedArgument) Argument() sql.Expression {
	return n.child
}

// WithResolvedChildren implements the vitess.Injectable interface.
func (n *NamedArgument) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(n, len(children), 1)
	}
	child, ok := children[0].(sql.Expression)
	if !ok {
		return nil, errors.Errorf("expected vitess child to be an expression but has type `%T`", children[0])
	}
	return &NamedArgument{name: n.name, child: child}, nil
}

// Resolved implements the sql.Expression interface.
func (n *NamedArgument) Resolved() bool {
	return n.child != nil && n.child.Resolved()
}

// String implements the sql.Expression interface.
func (n *NamedArgument) String() string {
	if n.child == nil {
		return fmt.Sprintf("%s => <unresolved>", n.name)
	}
	return fmt.Sprintf("%s => %s", n.name, n.child.String())
}

// Type implements the sql.Expression interface.
func (n *NamedArgument) Type(ctx *sql.Context) sql.Type {
	return n.child.Type(ctx)
}

// IsNullable implements the sql.Expression interface.
func (n *NamedArgument) IsNullable(ctx *sql.Context) bool {
	return n.child.IsNullable(ctx)
}

// Eval implements the sql.Expression interface.
func (n *NamedArgument) Eval(ctx *sql.Context, row sql.Row) (any, error) {
	return n.child.Eval(ctx, row)
}

// Children implements the sql.Expression interface.
func (n *NamedArgument) Children() []sql.Expression {
	if n.child == nil {
		return nil
	}
	return []sql.Expression{n.child}
}

// WithChildren implements the sql.Expression interface.
func (n *NamedArgument) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(n, len(children), 1)
	}
	return &NamedArgument{name: n.name, child: children[0]}, nil
}
