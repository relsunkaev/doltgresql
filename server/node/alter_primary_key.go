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
)

// AlterPrimaryKey wraps GMS primary-key alteration with Doltgres metadata
// maintenance for PostgreSQL-facing primary-key constraint names.
type AlterPrimaryKey struct {
	alterPk *plan.AlterPK
	comment string
}

var _ sql.ExecBuilderNode = (*AlterPrimaryKey)(nil)
var _ sql.SchemaTarget = (*AlterPrimaryKey)(nil)
var _ sql.Expressioner = (*AlterPrimaryKey)(nil)

// NewAlterPrimaryKey returns a new *AlterPrimaryKey.
func NewAlterPrimaryKey(alterPk *plan.AlterPK, comment string) *AlterPrimaryKey {
	return &AlterPrimaryKey{
		alterPk: alterPk,
		comment: comment,
	}
}

// Children implements the interface sql.ExecBuilderNode.
func (a *AlterPrimaryKey) Children() []sql.Node {
	return a.alterPk.Children()
}

// Expressions implements the interface sql.Expressioner.
func (a *AlterPrimaryKey) Expressions() []sql.Expression {
	return a.alterPk.Expressions()
}

// IsReadOnly implements the interface sql.ExecBuilderNode.
func (a *AlterPrimaryKey) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecBuilderNode.
func (a *AlterPrimaryKey) Resolved() bool {
	return a.alterPk.Resolved()
}

// BuildRowIter implements the interface sql.ExecBuilderNode.
func (a *AlterPrimaryKey) BuildRowIter(ctx *sql.Context, b sql.NodeExecBuilder, r sql.Row) (sql.RowIter, error) {
	childIter, err := b.Build(ctx, a.alterPk, r)
	if err != nil {
		return nil, err
	}
	return &alterPrimaryKeyMetadataIter{
		childIter: childIter,
		db:        a.alterPk.Database(),
		tableName: nodeTableName(a.alterPk.Table),
		comment:   a.comment,
	}, nil
}

// Schema implements the interface sql.ExecBuilderNode.
func (a *AlterPrimaryKey) Schema(ctx *sql.Context) sql.Schema {
	return a.alterPk.Schema(ctx)
}

// String implements the interface sql.ExecBuilderNode.
func (a *AlterPrimaryKey) String() string {
	return a.alterPk.String()
}

// TargetSchema implements the interface sql.SchemaTarget.
func (a *AlterPrimaryKey) TargetSchema() sql.Schema {
	return a.alterPk.TargetSchema()
}

// WithChildren implements the interface sql.ExecBuilderNode.
func (a *AlterPrimaryKey) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	alterPk, err := a.alterPk.WithChildren(ctx, children...)
	if err != nil {
		return nil, err
	}
	return &AlterPrimaryKey{
		alterPk: alterPk.(*plan.AlterPK),
		comment: a.comment,
	}, nil
}

// WithExpressions implements the interface sql.Expressioner.
func (a *AlterPrimaryKey) WithExpressions(ctx *sql.Context, expressions ...sql.Expression) (sql.Node, error) {
	alterPk, err := a.alterPk.WithExpressions(ctx, expressions...)
	if err != nil {
		return nil, err
	}
	return &AlterPrimaryKey{
		alterPk: alterPk.(*plan.AlterPK),
		comment: a.comment,
	}, nil
}

// WithTargetSchema implements the interface sql.SchemaTarget.
func (a AlterPrimaryKey) WithTargetSchema(schema sql.Schema) (sql.Node, error) {
	alterPk, err := a.alterPk.WithTargetSchema(schema)
	if err != nil {
		return nil, err
	}
	a.alterPk = alterPk.(*plan.AlterPK)
	return &a, nil
}

type alterPrimaryKeyMetadataIter struct {
	childIter sql.RowIter
	db        sql.Database
	tableName string
	comment   string
	applied   bool
}

func (a *alterPrimaryKeyMetadataIter) Next(ctx *sql.Context) (sql.Row, error) {
	row, err := a.childIter.Next(ctx)
	if err != nil {
		return row, err
	}
	if !a.applied {
		a.applied = true
		if err = modifyTableComment(ctx, a.db, a.tableName, a.comment); err != nil {
			return nil, err
		}
	}
	return row, nil
}

func (a *alterPrimaryKeyMetadataIter) Close(ctx *sql.Context) error {
	return a.childIter.Close(ctx)
}

func nodeTableName(node sql.Node) string {
	if named, ok := node.(interface{ Name() string }); ok {
		return named.Name()
	}
	return fmt.Sprint(node)
}
