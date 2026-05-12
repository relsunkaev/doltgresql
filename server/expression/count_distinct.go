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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	gmsexpression "github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/transform"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	pgtypes "github.com/dolthub/doltgresql/server/types"
)

type CountDistinct struct {
	selectExprs []sql.Expression
	id          sql.ColumnId
}

var _ sql.Aggregation = (*CountDistinct)(nil)
var _ vitess.Injectable = (*CountDistinct)(nil)

func NewCountDistinct() *CountDistinct {
	return &CountDistinct{}
}

func (c *CountDistinct) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) > 0 {
		if _, ok := children[len(children)-1].(sql.SortFields); ok {
			children = children[:len(children)-1]
		}
	}
	c.selectExprs = make([]sql.Expression, len(children))
	for i, child := range children {
		expr, ok := child.(sql.Expression)
		if !ok {
			return nil, fmt.Errorf("count distinct child %d was %T, expected sql.Expression", i, child)
		}
		c.selectExprs[i] = expr
	}
	return c, nil
}

func (c *CountDistinct) Resolved() bool {
	return gmsexpression.ExpressionsResolved(c.selectExprs...)
}

func (c *CountDistinct) String() string {
	exprs := make([]string, len(c.selectExprs))
	for i, expr := range c.selectExprs {
		exprs[i] = expr.String()
	}
	return fmt.Sprintf("count(DISTINCT %s)", strings.Join(exprs, ", "))
}

func (c *CountDistinct) Type(ctx *sql.Context) sql.Type {
	return pgtypes.Int64
}

func (c *CountDistinct) IsNullable(ctx *sql.Context) bool {
	return false
}

func (c *CountDistinct) Eval(ctx *sql.Context, row sql.Row) (any, error) {
	panic("eval should never be called on an aggregation function")
}

func (c *CountDistinct) Children() []sql.Expression {
	return c.selectExprs
}

func (c *CountDistinct) OutputExpressions() []sql.Expression {
	return c.selectExprs
}

func (c CountDistinct) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != len(c.selectExprs) {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), len(c.selectExprs))
	}
	c.selectExprs = children
	return &c, nil
}

func (c *CountDistinct) Id() sql.ColumnId {
	return c.id
}

func (c CountDistinct) WithId(id sql.ColumnId) sql.IdExpression {
	c.id = id
	return &c
}

func (c *CountDistinct) NewWindowFunction(ctx *sql.Context) (sql.WindowFunction, error) {
	panic("window functions not yet supported for count(distinct)")
}

func (c *CountDistinct) WithWindow(ctx *sql.Context, window *sql.WindowDefinition) sql.WindowAdaptableExpression {
	panic("window functions not yet supported for count(distinct)")
}

func (c *CountDistinct) Window() *sql.WindowDefinition {
	return nil
}

func (c *CountDistinct) NewBuffer(ctx *sql.Context) (sql.AggregationBuffer, error) {
	exprs := make([]sql.Expression, len(c.selectExprs))
	for i, expr := range c.selectExprs {
		child, err := transform.Clone(ctx, expr)
		if err != nil {
			return nil, err
		}
		exprs[i] = child
	}
	return &countDistinctBuffer{
		seen:  make(map[string]struct{}),
		exprs: exprs,
	}, nil
}

type countDistinctBuffer struct {
	seen  map[string]struct{}
	exprs []sql.Expression
}

func (c *countDistinctBuffer) Update(ctx *sql.Context, row sql.Row) error {
	if len(c.exprs) == 0 {
		return fmt.Errorf("count distinct requires at least one expression")
	}
	var evalRow sql.Row
	if _, ok := c.exprs[0].(*gmsexpression.Star); ok {
		evalRow = row
	} else {
		var err error
		evalRow, err = evalExprs(ctx, c.exprs, row)
		if err != nil {
			return err
		}
	}
	for _, val := range evalRow {
		if val == nil {
			return nil
		}
	}
	key, err := countDistinctKey(ctx, c.exprs, evalRow)
	if err != nil {
		return err
	}
	c.seen[key] = struct{}{}
	return nil
}

func (c *countDistinctBuffer) Eval(ctx *sql.Context) (any, error) {
	return int64(len(c.seen)), nil
}

func (c *countDistinctBuffer) Dispose(ctx *sql.Context) {
	for _, expr := range c.exprs {
		gmsexpression.Dispose(ctx, expr)
	}
}

func countDistinctKey(ctx *sql.Context, exprs []sql.Expression, row sql.Row) (string, error) {
	var sb strings.Builder
	for i, val := range row {
		if i > 0 {
			sb.WriteRune(0)
		}
		if i < len(exprs) {
			typ := exprs[i].Type(ctx)
			sb.WriteString(typ.String())
			sb.WriteRune(':')
			if extTyp, ok := typ.(sql.ExtendedType); ok {
				serialized, err := extTyp.SerializeValue(ctx, val)
				if err != nil {
					return "", err
				}
				_, _ = sb.Write(serialized)
				continue
			}
		}
		res, err := sql.UnwrapAny(ctx, val)
		if err != nil {
			return "", err
		}
		sb.WriteString(fmt.Sprintf("%T:%#v", res, res))
	}
	return sb.String(), nil
}
