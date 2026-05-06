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
	"strconv"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"
)

// Collate preserves an explicit PostgreSQL COLLATE clause as a planner-visible
// expression boundary. Evaluation currently delegates to the child expression;
// keeping the wrapper prevents ordinary btree indexes with different collation
// semantics from being planned as if COLLATE had not been specified.
type Collate struct {
	child  sql.Expression
	locale string
}

var _ vitess.Injectable = (*Collate)(nil)
var _ sql.Expression = (*Collate)(nil)
var _ sql.CollationCoercible = (*Collate)(nil)

func NewCollate(locale string) *Collate {
	return &Collate{
		locale: locale,
	}
}

func (c *Collate) Children() []sql.Expression {
	return []sql.Expression{c.child}
}

func (c *Collate) Child() sql.Expression {
	return c.child
}

func (c *Collate) Locale() string {
	return c.locale
}

func (c *Collate) Eval(ctx *sql.Context, row sql.Row) (any, error) {
	return c.child.Eval(ctx, row)
}

func (c *Collate) IsNullable(ctx *sql.Context) bool {
	return c.child == nil || c.child.IsNullable(ctx)
}

func (c *Collate) Resolved() bool {
	return c.child != nil && c.child.Resolved()
}

func (c *Collate) String() string {
	if c.child == nil {
		return "? COLLATE " + strconv.Quote(c.locale)
	}
	return c.child.String() + " COLLATE " + strconv.Quote(c.locale)
}

func (c *Collate) Type(ctx *sql.Context) sql.Type {
	return c.child.Type(ctx)
}

func (c *Collate) CollationCoercibility(ctx *sql.Context) (sql.CollationID, byte) {
	if coercible, ok := c.child.(sql.CollationCoercible); ok {
		return coercible.CollationCoercibility(ctx)
	}
	return sql.Collation_binary, 5
}

func (c *Collate) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 1)
	}
	return &Collate{
		child:  children[0],
		locale: c.locale,
	}, nil
}

func (c *Collate) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 1 {
		return nil, errors.Errorf("invalid vitess child count, expected `1` but got `%d`", len(children))
	}
	child, ok := children[0].(sql.Expression)
	if !ok {
		return nil, errors.Errorf("expected vitess child to be an expression but has type `%T`", children[0])
	}
	return &Collate{
		child:  child,
		locale: c.locale,
	}, nil
}
