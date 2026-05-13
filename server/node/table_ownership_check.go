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
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/go-mysql-server/sql"
)

// TableOwnershipCheck runs a child DDL node only after the current role is
// allowed to operate as the table owner.
type TableOwnershipCheck struct {
	child     sql.Node
	tableName doltdb.TableName
}

var _ sql.ExecBuilderNode = (*TableOwnershipCheck)(nil)

// NewTableOwnershipCheck returns a new *TableOwnershipCheck.
func NewTableOwnershipCheck(child sql.Node, tableName doltdb.TableName) *TableOwnershipCheck {
	return &TableOwnershipCheck{
		child:     child,
		tableName: tableName,
	}
}

// Children implements sql.ExecBuilderNode.
func (c *TableOwnershipCheck) Children() []sql.Node {
	return []sql.Node{c.child}
}

// IsReadOnly implements sql.ExecBuilderNode.
func (c *TableOwnershipCheck) IsReadOnly() bool {
	return c.child.IsReadOnly()
}

// Resolved implements sql.ExecBuilderNode.
func (c *TableOwnershipCheck) Resolved() bool {
	return c.child.Resolved()
}

// BuildRowIter implements sql.ExecBuilderNode.
func (c *TableOwnershipCheck) BuildRowIter(ctx *sql.Context, b sql.NodeExecBuilder, r sql.Row) (sql.RowIter, error) {
	if err := checkIndexTableOwnership(ctx, c.tableName); err != nil {
		return nil, err
	}
	return b.Build(ctx, c.child, r)
}

// Schema implements sql.ExecBuilderNode.
func (c *TableOwnershipCheck) Schema(ctx *sql.Context) sql.Schema {
	return c.child.Schema(ctx)
}

// String implements sql.ExecBuilderNode.
func (c *TableOwnershipCheck) String() string {
	return c.child.String()
}

// WithChildren implements sql.ExecBuilderNode.
func (c *TableOwnershipCheck) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 1)
	}
	return NewTableOwnershipCheck(children[0], c.tableName), nil
}
