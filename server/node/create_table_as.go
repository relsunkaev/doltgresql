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
	"sync/atomic"

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

var createTableAsSavepointCounter uint64

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
	savepoint, hasSavepoint, err := createCreateTableAsSavepoint(ctx)
	if err != nil {
		return nil, err
	}
	abort := func(cause error) (sql.RowIter, error) {
		if hasSavepoint {
			return nil, rollbackCreateTableAsSavepoint(ctx, savepoint, cause)
		}
		return nil, cause
	}
	createIter, err := b.Build(ctx, c.createNode, r)
	if createIter != nil {
		if closeErr := createIter.Close(ctx); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	if err != nil {
		return abort(err)
	}
	table, ok, err := db.GetTableInsensitive(ctx, c.createTable.Name())
	if err != nil {
		return abort(err)
	}
	if !ok {
		return abort(sql.ErrTableCreatedNotFound.New(c.createTable.Name()))
	}
	insert := plan.NewInsertInto(db, plan.NewResolvedTable(table, db, nil), c.tableCopier.Source, false, nil, nil, false)
	insertIter, err := b.Build(ctx, insert, r)
	if err != nil {
		return abort(err)
	}
	if !hasSavepoint {
		return insertIter, nil
	}
	if insertIter == nil {
		insertIter = sql.RowsToRowIter()
	}
	return &createTableAsSavepointIter{
		childIter: insertIter,
		savepoint: savepoint,
	}, nil
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

func createCreateTableAsSavepoint(ctx *sql.Context) (string, bool, error) {
	tx := ctx.GetTransaction()
	if tx == nil {
		return "", false, nil
	}
	txSession, ok := ctx.Session.(sql.TransactionSession)
	if !ok {
		return "", false, nil
	}
	name := fmt.Sprintf("__doltgresql_ctas_%d", atomic.AddUint64(&createTableAsSavepointCounter, 1))
	if err := txSession.CreateSavepoint(ctx, tx, name); err != nil {
		return "", false, err
	}
	return name, true, nil
}

func rollbackCreateTableAsSavepoint(ctx *sql.Context, name string, cause error) error {
	tx := ctx.GetTransaction()
	if tx == nil {
		return cause
	}
	txSession, ok := ctx.Session.(sql.TransactionSession)
	if !ok {
		return fmt.Errorf("%w; CREATE TABLE AS rollback failed: session does not implement sql.TransactionSession", cause)
	}
	if err := txSession.RollbackToSavepoint(ctx, tx, name); err != nil {
		return fmt.Errorf("%w; CREATE TABLE AS rollback failed: %v", cause, err)
	}
	if err := txSession.ReleaseSavepoint(ctx, ctx.GetTransaction(), name); err != nil {
		return fmt.Errorf("%w; CREATE TABLE AS savepoint release failed: %v", cause, err)
	}
	return cause
}

func releaseCreateTableAsSavepoint(ctx *sql.Context, name string) error {
	tx := ctx.GetTransaction()
	if tx == nil {
		return nil
	}
	txSession, ok := ctx.Session.(sql.TransactionSession)
	if !ok {
		return nil
	}
	return txSession.ReleaseSavepoint(ctx, tx, name)
}

type createTableAsSavepointIter struct {
	childIter sql.RowIter
	savepoint string
	closed    bool
	done      bool
	finalized bool
}

var _ sql.RowIter = (*createTableAsSavepointIter)(nil)

func (i *createTableAsSavepointIter) Next(ctx *sql.Context) (sql.Row, error) {
	if i.done {
		return nil, io.EOF
	}
	i.done = true

	rowsAffected := 0
	for {
		row, err := i.childIter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, i.abort(ctx, err)
		}
		if types.IsOkResult(row) {
			rowsAffected += int(types.GetOkResult(row).RowsAffected)
		} else {
			rowsAffected++
		}
	}
	if err := i.complete(ctx); err != nil {
		return nil, err
	}
	return sql.NewRow(types.NewOkResult(rowsAffected)), nil
}

func (i *createTableAsSavepointIter) Close(ctx *sql.Context) error {
	if i.finalized {
		return nil
	}
	return i.complete(ctx)
}

func (i *createTableAsSavepointIter) complete(ctx *sql.Context) error {
	if i.finalized {
		return nil
	}
	if err := i.closeChild(ctx); err != nil {
		return i.abort(ctx, err)
	}
	i.finalized = true
	return releaseCreateTableAsSavepoint(ctx, i.savepoint)
}

func (i *createTableAsSavepointIter) abort(ctx *sql.Context, cause error) error {
	if i.finalized {
		return cause
	}
	_ = i.closeChild(ctx)
	i.finalized = true
	return rollbackCreateTableAsSavepoint(ctx, i.savepoint, cause)
}

func (i *createTableAsSavepointIter) closeChild(ctx *sql.Context) error {
	if i.closed {
		return nil
	}
	i.closed = true
	return i.childIter.Close(ctx)
}
