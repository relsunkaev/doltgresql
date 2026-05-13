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
	"github.com/dolthub/go-mysql-server/sql/types"
)

// InheritedAlterTable runs ALTER TABLE operations for a parent table and each
// inherited child table using the normal ALTER executors.
type InheritedAlterTable struct {
	nodes []sql.Node
}

var _ sql.ExecBuilderNode = (*InheritedAlterTable)(nil)

// NewInheritedAlterTable returns a new *InheritedAlterTable.
func NewInheritedAlterTable(nodes []sql.Node) *InheritedAlterTable {
	return &InheritedAlterTable{nodes: append([]sql.Node(nil), nodes...)}
}

// Children implements the interface sql.ExecBuilderNode.
func (i *InheritedAlterTable) Children() []sql.Node {
	return append([]sql.Node(nil), i.nodes...)
}

// IsReadOnly implements the interface sql.ExecBuilderNode.
func (i *InheritedAlterTable) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecBuilderNode.
func (i *InheritedAlterTable) Resolved() bool {
	for _, node := range i.nodes {
		if !node.Resolved() {
			return false
		}
	}
	return true
}

// BuildRowIter implements the interface sql.ExecBuilderNode.
func (i *InheritedAlterTable) BuildRowIter(ctx *sql.Context, b sql.NodeExecBuilder, r sql.Row) (sql.RowIter, error) {
	rowsAffected := 0
	for _, node := range i.nodes {
		iter, err := b.Build(ctx, node, r)
		if err != nil {
			return nil, err
		}
		affected, err := drainInheritedAlterDefaultIter(ctx, iter)
		if err != nil {
			return nil, err
		}
		rowsAffected += affected
	}
	return sql.RowsToRowIter(sql.NewRow(types.NewOkResult(rowsAffected))), nil
}

// Schema implements the interface sql.ExecBuilderNode.
func (i *InheritedAlterTable) Schema(ctx *sql.Context) sql.Schema {
	return types.OkResultSchema
}

// String implements the interface sql.ExecBuilderNode.
func (i *InheritedAlterTable) String() string {
	if len(i.nodes) == 0 {
		return "ALTER TABLE"
	}
	return i.nodes[0].String()
}

// WithChildren implements the interface sql.ExecBuilderNode.
func (i *InheritedAlterTable) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	if len(children) != len(i.nodes) {
		return nil, sql.ErrInvalidChildrenNumber.New(i, len(children), len(i.nodes))
	}
	return NewInheritedAlterTable(children), nil
}
