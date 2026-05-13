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
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// InheritedAlterNotNull runs ALTER COLUMN SET/DROP NOT NULL for a parent table
// and inherited child tables using the normal ModifyColumn executor.
type InheritedAlterNotNull struct {
	nodes       []sql.Node
	columnName  string
	validateSet bool
}

var _ sql.ExecBuilderNode = (*InheritedAlterNotNull)(nil)

// NewInheritedAlterNotNull returns a new *InheritedAlterNotNull.
func NewInheritedAlterNotNull(nodes []sql.Node, columnName string, validateSet bool) *InheritedAlterNotNull {
	return &InheritedAlterNotNull{
		nodes:       append([]sql.Node(nil), nodes...),
		columnName:  columnName,
		validateSet: validateSet,
	}
}

// Children implements the interface sql.ExecBuilderNode.
func (i *InheritedAlterNotNull) Children() []sql.Node {
	return append([]sql.Node(nil), i.nodes...)
}

// IsReadOnly implements the interface sql.ExecBuilderNode.
func (i *InheritedAlterNotNull) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecBuilderNode.
func (i *InheritedAlterNotNull) Resolved() bool {
	for _, node := range i.nodes {
		if !node.Resolved() {
			return false
		}
	}
	return true
}

// BuildRowIter implements the interface sql.ExecBuilderNode.
func (i *InheritedAlterNotNull) BuildRowIter(ctx *sql.Context, b sql.NodeExecBuilder, r sql.Row) (sql.RowIter, error) {
	if i.validateSet {
		if err := i.validateNoNullRows(ctx); err != nil {
			return nil, err
		}
	}
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

func (i *InheritedAlterNotNull) validateNoNullRows(ctx *sql.Context) error {
	for _, node := range i.nodes {
		modify, ok := node.(*plan.ModifyColumn)
		if !ok {
			continue
		}
		table, ok := modify.Table.(*plan.ResolvedTable)
		if !ok {
			continue
		}
		schema := table.Schema(ctx)
		columnIndex := -1
		columnName := i.columnName
		for idx, column := range schema {
			if strings.EqualFold(column.Name, i.columnName) {
				columnIndex = idx
				columnName = column.Name
				break
			}
		}
		if columnIndex < 0 {
			return sql.ErrTableColumnNotFound.New(table.Name(), i.columnName)
		}
		partitions, err := table.Partitions(ctx)
		if err != nil {
			return err
		}
		iter := sql.NewTableRowIter(ctx, table, partitions)
		for {
			row, err := iter.Next(ctx)
			if err == io.EOF {
				break
			}
			if err != nil {
				_ = iter.Close(ctx)
				return err
			}
			if row[columnIndex] == nil {
				_ = iter.Close(ctx)
				return errors.Errorf(`column "%s" of relation "%s" contains null values`, columnName, table.Name())
			}
		}
		if err := iter.Close(ctx); err != nil {
			return err
		}
	}
	return nil
}

// Schema implements the interface sql.ExecBuilderNode.
func (i *InheritedAlterNotNull) Schema(ctx *sql.Context) sql.Schema {
	return types.OkResultSchema
}

// String implements the interface sql.ExecBuilderNode.
func (i *InheritedAlterNotNull) String() string {
	if len(i.nodes) == 0 {
		return "ALTER NOT NULL"
	}
	return i.nodes[0].String()
}

// WithChildren implements the interface sql.ExecBuilderNode.
func (i *InheritedAlterNotNull) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	if len(children) != len(i.nodes) {
		return nil, sql.ErrInvalidChildrenNumber.New(i, len(children), len(i.nodes))
	}
	return NewInheritedAlterNotNull(children, i.columnName, i.validateSet), nil
}
