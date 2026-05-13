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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// CreateTableAs wraps go-mysql-server's TableCopier so PostgreSQL CTAS
// IF NOT EXISTS semantics can skip the source query once the target exists.
type CreateTableAs struct {
	tableCopier *plan.TableCopier
	createTable *plan.CreateTable
	createNode  sql.Node
}

var _ sql.ExecBuilderNode = (*CreateTableAs)(nil)

func NewCreateTableAs(tableCopier *plan.TableCopier) *CreateTableAs {
	var createTable *plan.CreateTable
	var createNode sql.Node
	switch destination := tableCopier.Destination.(type) {
	case *plan.CreateTable:
		createTable = destination
		createNode = destination
	case *CreateTable:
		createTable = destination.GMSCreateTable()
		createNode = destination
	}
	return &CreateTableAs{
		tableCopier: tableCopier,
		createTable: createTable,
		createNode:  createNode,
	}
}

func (c *CreateTableAs) Children() []sql.Node {
	return []sql.Node{c.tableCopier}
}

func (c *CreateTableAs) IsReadOnly() bool {
	return false
}

func (c *CreateTableAs) Resolved() bool {
	return c.tableCopier != nil && c.tableCopier.Resolved()
}

func (c *CreateTableAs) BuildRowIter(ctx *sql.Context, b sql.NodeExecBuilder, r sql.Row) (sql.RowIter, error) {
	if c.createTable == nil || c.createNode == nil {
		return b.Build(ctx, c.tableCopier, r)
	}
	db := c.createTable.Database()
	if c.createTable.IfNotExists() {
		_, ok, err := db.GetTableInsensitive(ctx, c.createTable.Name())
		if err != nil {
			return nil, err
		}
		if ok {
			return sql.RowsToRowIter(sql.NewRow(types.NewOkResult(0))), nil
		}
	}
	createIter, err := b.Build(ctx, c.createNode, r)
	if createIter != nil {
		_ = createIter.Close(ctx)
	}
	if err != nil {
		return nil, err
	}
	table, ok, err := db.GetTableInsensitive(ctx, c.createTable.Name())
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, sql.ErrTableCreatedNotFound.New(c.createTable.Name())
	}
	insert := plan.NewInsertInto(db, plan.NewResolvedTable(table, db, nil), c.tableCopier.Source, false, nil, nil, false)
	return b.Build(ctx, insert, r)
}

func (c *CreateTableAs) Schema(ctx *sql.Context) sql.Schema {
	return c.tableCopier.Schema(ctx)
}

func (c *CreateTableAs) String() string {
	return fmt.Sprintf("CREATE TABLE AS: %s", c.tableCopier)
}

func (c *CreateTableAs) WithChildren(_ *sql.Context, children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 1)
	}
	tableCopier, ok := children[0].(*plan.TableCopier)
	if !ok {
		return nil, fmt.Errorf("expected *plan.TableCopier child, found %T", children[0])
	}
	return NewCreateTableAs(tableCopier), nil
}
