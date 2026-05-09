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
	"io"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// TableMetadataApplier runs a child DDL node and then reapplies Doltgres table
// metadata that lower-level table-copy optimizations may overwrite.
type TableMetadataApplier struct {
	child     sql.Node
	db        sql.Database
	tableName string
	comment   string
}

var _ sql.DebugStringer = (*TableMetadataApplier)(nil)
var _ sql.ExecBuilderNode = (*TableMetadataApplier)(nil)

// NewTableMetadataApplier returns a new *TableMetadataApplier.
func NewTableMetadataApplier(child sql.Node, db sql.Database, tableName string, comment string) *TableMetadataApplier {
	return &TableMetadataApplier{
		child:     child,
		db:        db,
		tableName: tableName,
		comment:   comment,
	}
}

// Children implements the interface sql.ExecBuilderNode.
func (m *TableMetadataApplier) Children() []sql.Node {
	return []sql.Node{m.child}
}

// DebugString implements the sql.DebugStringer interface.
func (m *TableMetadataApplier) DebugString(ctx *sql.Context) string {
	return sql.DebugString(ctx, m.child)
}

// IsReadOnly implements the interface sql.ExecBuilderNode.
func (m *TableMetadataApplier) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecBuilderNode.
func (m *TableMetadataApplier) Resolved() bool {
	return m.child.Resolved()
}

// BuildRowIter implements the interface sql.ExecBuilderNode.
func (m *TableMetadataApplier) BuildRowIter(ctx *sql.Context, b sql.NodeExecBuilder, r sql.Row) (sql.RowIter, error) {
	childIter, err := b.Build(ctx, m.child, r)
	if err != nil {
		return nil, err
	}
	if childIter == nil {
		childIter = sql.RowsToRowIter()
	}
	return &tableMetadataApplierIter{
		childIter: childIter,
		db:        m.db,
		tableName: m.tableName,
		comment:   m.comment,
	}, nil
}

// Schema implements the interface sql.ExecBuilderNode.
func (m *TableMetadataApplier) Schema(ctx *sql.Context) sql.Schema {
	return types.OkResultSchema
}

// String implements the interface sql.ExecBuilderNode.
func (m *TableMetadataApplier) String() string {
	return m.child.String()
}

// WithChildren implements the interface sql.ExecBuilderNode.
func (m *TableMetadataApplier) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(m, len(children), 1)
	}
	return NewTableMetadataApplier(children[0], m.db, m.tableName, m.comment), nil
}

type tableMetadataApplierIter struct {
	childIter sql.RowIter
	db        sql.Database
	tableName string
	comment   string
	done      bool
	closed    bool
}

var _ sql.RowIter = (*tableMetadataApplierIter)(nil)

// Next implements the interface sql.RowIter.
func (m *tableMetadataApplierIter) Next(ctx *sql.Context) (sql.Row, error) {
	if m.done {
		return nil, io.EOF
	}
	m.done = true

	rowsAffected := 0
	for {
		row, err := m.childIter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			_ = m.closeChild(ctx)
			return nil, err
		}
		if types.IsOkResult(row) {
			rowsAffected += int(types.GetOkResult(row).RowsAffected)
		} else {
			rowsAffected++
		}
	}
	if err := m.closeChildAndApply(ctx); err != nil {
		return nil, err
	}
	return sql.NewRow(types.NewOkResult(rowsAffected)), nil
}

// Close implements the interface sql.RowIter.
func (m *tableMetadataApplierIter) Close(ctx *sql.Context) error {
	return m.closeChildAndApply(ctx)
}

func (m *tableMetadataApplierIter) closeChildAndApply(ctx *sql.Context) error {
	if err := m.closeChild(ctx); err != nil {
		return err
	}
	return modifyTableComment(ctx, m.db, m.tableName, m.comment)
}

func (m *tableMetadataApplierIter) closeChild(ctx *sql.Context) error {
	if m.closed {
		return nil
	}
	m.closed = true
	return m.childIter.Close(ctx)
}
