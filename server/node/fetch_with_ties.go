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
	"fmt"
	"io"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/iters"
)

var _ sql.ExecBuilderNode = (*FetchWithTies)(nil)
var _ sql.Expressioner = (*FetchWithTies)(nil)
var _ sql.CollationCoercible = (*FetchWithTies)(nil)

// FetchWithTies returns the requested number of sorted rows plus any additional
// rows tied with the final returned row according to the ORDER BY expressions.
type FetchWithTies struct {
	Child      sql.Node
	Limit      sql.Expression
	Offset     sql.Expression
	SortFields sql.SortFields
}

// NewFetchWithTies creates a FetchWithTies node.
func NewFetchWithTies(limit, offset sql.Expression, sortFields sql.SortFields, child sql.Node) *FetchWithTies {
	return &FetchWithTies{
		Child:      child,
		Limit:      limit,
		Offset:     offset,
		SortFields: sortFields,
	}
}

// Children implements sql.Node.
func (f *FetchWithTies) Children() []sql.Node {
	return []sql.Node{f.Child}
}

// WithChildren implements sql.Node.
func (f *FetchWithTies) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(f, len(children), 1)
	}
	nf := *f
	nf.Child = children[0]
	return &nf, nil
}

// Expressions implements sql.Expressioner.
func (f *FetchWithTies) Expressions() []sql.Expression {
	exprs := []sql.Expression{f.Limit}
	if f.Offset != nil {
		exprs = append(exprs, f.Offset)
	}
	exprs = append(exprs, f.SortFields.ToExpressions()...)
	return exprs
}

// WithExpressions implements sql.Expressioner.
func (f *FetchWithTies) WithExpressions(ctx *sql.Context, exprs ...sql.Expression) (sql.Node, error) {
	expected := len(f.SortFields) + 1
	if f.Offset != nil {
		expected++
	}
	if len(exprs) != expected {
		return nil, sql.ErrInvalidChildrenNumber.New(f, len(exprs), expected)
	}

	nf := *f
	nf.Limit = exprs[0]
	offset := 1
	if f.Offset != nil {
		nf.Offset = exprs[1]
		offset = 2
	}
	nf.SortFields = f.SortFields.FromExpressions(ctx, exprs[offset:]...)
	return &nf, nil
}

// Resolved implements sql.Node.
func (f *FetchWithTies) Resolved() bool {
	if f == nil || f.Child == nil || f.Limit == nil || !f.Child.Resolved() || !f.Limit.Resolved() {
		return false
	}
	if f.Offset != nil && !f.Offset.Resolved() {
		return false
	}
	for _, field := range f.SortFields {
		if !field.Column.Resolved() {
			return false
		}
	}
	return true
}

// IsReadOnly implements sql.Node.
func (f *FetchWithTies) IsReadOnly() bool {
	return f.Child.IsReadOnly()
}

// Schema implements sql.Node.
func (f *FetchWithTies) Schema(ctx *sql.Context) sql.Schema {
	return f.Child.Schema(ctx)
}

// CollationCoercibility implements sql.CollationCoercible.
func (f *FetchWithTies) CollationCoercibility(ctx *sql.Context) (sql.CollationID, byte) {
	return sql.GetCoercibility(ctx, f.Child)
}

// String implements sql.Node.
func (f *FetchWithTies) String() string {
	fields := make([]string, len(f.SortFields))
	for i, field := range f.SortFields {
		fields[i] = fmt.Sprintf("%s %s", field.Column, field.Order)
	}
	if f.Offset != nil {
		return fmt.Sprintf("FetchWithTies(Limit: [%s], Offset: [%s]; %s)", f.Limit, f.Offset, strings.Join(fields, ", "))
	}
	return fmt.Sprintf("FetchWithTies(Limit: [%s]; %s)", f.Limit, strings.Join(fields, ", "))
}

// DebugString implements sql.DebugStringer.
func (f *FetchWithTies) DebugString(ctx *sql.Context) string {
	fields := make([]string, len(f.SortFields))
	for i, field := range f.SortFields {
		fields[i] = sql.DebugString(ctx, field)
	}
	if f.Offset != nil {
		return fmt.Sprintf("FetchWithTies(Limit: [%s], Offset: [%s]; %s)\n  child: %s", sql.DebugString(ctx, f.Limit), sql.DebugString(ctx, f.Offset), strings.Join(fields, ", "), sql.DebugString(ctx, f.Child))
	}
	return fmt.Sprintf("FetchWithTies(Limit: [%s]; %s)\n  child: %s", sql.DebugString(ctx, f.Limit), strings.Join(fields, ", "), sql.DebugString(ctx, f.Child))
}

// BuildRowIter implements sql.ExecBuilderNode.
func (f *FetchWithTies) BuildRowIter(ctx *sql.Context, builder sql.NodeExecBuilder, row sql.Row) (sql.RowIter, error) {
	limit, err := iters.GetInt64Value(ctx, f.Limit)
	if err != nil {
		return nil, err
	}
	var offset int64
	if f.Offset != nil {
		offset, err = iters.GetInt64Value(ctx, f.Offset)
		if err != nil {
			return nil, err
		}
	}
	childIter, err := builder.Build(ctx, f.Child, row)
	if err != nil {
		return nil, err
	}
	return &fetchWithTiesIter{
		childIter:  childIter,
		limit:      limit,
		offset:     offset,
		sortFields: f.SortFields,
	}, nil
}

type fetchWithTiesIter struct {
	childIter  sql.RowIter
	limit      int64
	offset     int64
	sortFields sql.SortFields
	skipped    bool
	returned   int64
	lastRow    sql.Row
	done       bool
}

func (i *fetchWithTiesIter) Next(ctx *sql.Context) (sql.Row, error) {
	if i.done || i.limit <= 0 {
		i.done = true
		return nil, io.EOF
	}
	if !i.skipped {
		for skipped := int64(0); skipped < i.offset; skipped++ {
			_, err := i.childIter.Next(ctx)
			if err != nil {
				i.done = true
				return nil, err
			}
		}
		i.skipped = true
	}

	row, err := i.childIter.Next(ctx)
	if err != nil {
		i.done = true
		return nil, err
	}
	if i.returned < i.limit {
		i.returned++
		if i.returned == i.limit {
			i.lastRow = append(sql.Row(nil), row...)
		}
		return row, nil
	}

	tied, err := sortRowsTie(ctx, i.sortFields, i.lastRow, row)
	if err != nil {
		i.done = true
		return nil, err
	}
	if tied {
		i.returned++
		return row, nil
	}
	i.done = true
	return nil, io.EOF
}

func (i *fetchWithTiesIter) Close(ctx *sql.Context) error {
	return i.childIter.Close(ctx)
}

func sortRowsTie(ctx *sql.Context, sortFields sql.SortFields, left, right sql.Row) (bool, error) {
	sorter := expression.Sorter{Ctx: ctx, SortFields: sortFields}
	leftLess := sorter.IsLesserRow(left, right)
	if sorter.LastError != nil {
		return false, sorter.LastError
	}
	rightLess := sorter.IsLesserRow(right, left)
	if sorter.LastError != nil {
		return false, sorter.LastError
	}
	return !leftLess && !rightLess, nil
}
