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
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"

	pgexprs "github.com/dolthub/doltgresql/server/expression"
)

// OnConflictReturningInsert fixes PostgreSQL RETURNING semantics for INSERT
// ... ON CONFLICT paths where the GMS executor otherwise treats the statement
// like MySQL INSERT IGNORE / ON DUPLICATE KEY UPDATE.
type OnConflictReturningInsert struct {
	insert   *plan.InsertInto
	observer OnConflictReturningObserver
}

var _ sql.DebugStringer = (*OnConflictReturningInsert)(nil)
var _ sql.ExecBuilderNode = (*OnConflictReturningInsert)(nil)
var _ plan.DisjointedChildrenNode = (*OnConflictReturningInsert)(nil)

// OnConflictReturningObserver observes the executor's pre-RETURNING row shape.
type OnConflictReturningObserver interface {
	ObserveOnConflictReturningRow(ctx *sql.Context, oldRow sql.Row, newRow sql.Row) error
}

// NewOnConflictReturningInsert returns a new *OnConflictReturningInsert.
func NewOnConflictReturningInsert(insert *plan.InsertInto) *OnConflictReturningInsert {
	return &OnConflictReturningInsert{insert: insert}
}

// WithObserver returns a new *OnConflictReturningInsert with the given observer.
func (i *OnConflictReturningInsert) WithObserver(observer OnConflictReturningObserver) *OnConflictReturningInsert {
	ret := *i
	ret.observer = observer
	return &ret
}

// Children implements the sql.Node interface.
func (i *OnConflictReturningInsert) Children() []sql.Node {
	return i.insert.Children()
}

// DebugString implements the sql.DebugStringer interface.
func (i *OnConflictReturningInsert) DebugString(ctx *sql.Context) string {
	return sql.DebugString(ctx, i.insert)
}

// DisjointedChildren implements the plan.DisjointedChildrenNode interface.
func (i *OnConflictReturningInsert) DisjointedChildren() [][]sql.Node {
	return i.insert.DisjointedChildren()
}

// IsReadOnly implements the sql.Node interface.
func (i *OnConflictReturningInsert) IsReadOnly() bool {
	return false
}

// Resolved implements the sql.Node interface.
func (i *OnConflictReturningInsert) Resolved() bool {
	return i.insert != nil && i.insert.Resolved()
}

// BuildRowIter implements the sql.ExecBuilderNode interface.
func (i *OnConflictReturningInsert) BuildRowIter(ctx *sql.Context, b sql.NodeExecBuilder, row sql.Row) (sql.RowIter, error) {
	inner := *i.insert
	inner.Returning = nil
	execCtx := ctx
	var updateWhereState *pgexprs.OnConflictUpdateWhereState
	if hasOnConflictUpdateWhereMarker(ctx, i.insert.OnDupExprs) {
		updateWhereState = pgexprs.NewOnConflictUpdateWhereState()
		execCtx = pgexprs.ContextWithOnConflictUpdateWhereState(ctx, updateWhereState)
	}
	child, err := b.Build(execCtx, &inner, row)
	if err != nil {
		return nil, err
	}
	return &onConflictReturningInsertIter{
		child:          child,
		ctx:            execCtx,
		returning:      i.insert.Returning,
		destinationLen: len(i.insert.Destination.Schema(ctx)),
		onDupUpdates:   i.insert.OnDupExprs != nil && i.insert.OnDupExprs.HasUpdates(),
		updateWhere:    updateWhereState,
		observer:       i.observer,
	}, nil
}

// Schema implements the sql.Node interface.
func (i *OnConflictReturningInsert) Schema(ctx *sql.Context) sql.Schema {
	return i.insert.Schema(ctx)
}

// String implements the fmt.Stringer interface.
func (i *OnConflictReturningInsert) String() string {
	return i.insert.String()
}

// WithChildren implements the sql.Node interface.
func (i *OnConflictReturningInsert) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	insert, err := i.insert.WithChildren(ctx, children...)
	if err != nil {
		return nil, err
	}
	ret := *i
	ret.insert = insert.(*plan.InsertInto)
	return &ret, nil
}

// WithDisjointedChildren implements the plan.DisjointedChildrenNode interface.
func (i *OnConflictReturningInsert) WithDisjointedChildren(children [][]sql.Node) (sql.Node, error) {
	insert, err := i.insert.WithDisjointedChildren(children)
	if err != nil {
		return nil, err
	}
	ret := *i
	ret.insert = insert.(*plan.InsertInto)
	return &ret, nil
}

type onConflictReturningInsertIter struct {
	child          sql.RowIter
	ctx            *sql.Context
	returning      []sql.Expression
	destinationLen int
	onDupUpdates   bool
	updateWhere    *pgexprs.OnConflictUpdateWhereState
	observer       OnConflictReturningObserver
}

var _ sql.RowIter = (*onConflictReturningInsertIter)(nil)

// Next implements the sql.RowIter interface.
func (i *onConflictReturningInsertIter) Next(ctx *sql.Context) (sql.Row, error) {
	for {
		execCtx := i.ctx
		if execCtx == nil {
			execCtx = ctx
		}
		row, err := i.child.Next(execCtx)
		if _, ok := err.(sql.IgnorableError); ok {
			continue
		}
		if err != nil {
			return row, err
		}
		if i.updateWhere.ConsumeSkipped() {
			continue
		}
		if i.onDupUpdates && len(row) == i.destinationLen*2 {
			oldRow := row[:i.destinationLen]
			newRow := row[i.destinationLen:]
			if i.observer != nil {
				if err = i.observer.ObserveOnConflictReturningRow(execCtx, oldRow, newRow); err != nil {
					return nil, err
				}
			}
			row = newRow
		} else if i.observer != nil {
			if err = i.observer.ObserveOnConflictReturningRow(execCtx, nil, row); err != nil {
				return nil, err
			}
		}
		return evalReturning(execCtx, row, i.returning)
	}
}

// Close implements the sql.RowIter interface.
func (i *onConflictReturningInsertIter) Close(ctx *sql.Context) error {
	return i.child.Close(ctx)
}

func evalReturning(ctx *sql.Context, row sql.Row, returning []sql.Expression) (sql.Row, error) {
	retRow := make(sql.Row, 0, len(returning))
	for _, returnExpr := range returning {
		result, err := returnExpr.Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		retRow = append(retRow, result)
	}
	return retRow, nil
}

func hasOnConflictUpdateWhereMarker(ctx *sql.Context, exprs *plan.UpdateExprs) bool {
	if exprs == nil {
		return false
	}
	for _, expr := range exprs.AllExpressions() {
		if transform.InspectExpr(ctx, expr, func(ctx *sql.Context, expr sql.Expression) bool {
			_, ok := expr.(*pgexprs.OnConflictUpdateWhere)
			return ok
		}) {
			return true
		}
	}
	return false
}
