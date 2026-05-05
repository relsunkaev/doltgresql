// Copyright 2024 Dolthub, Inc.
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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/server/tablemetadata"
)

// CreateTable is a node that implements functionality specifically relevant to Doltgres' table creation needs.
type CreateTable struct {
	gmsCreateTable *plan.CreateTable
	sequences      []*CreateSequence
}

var _ sql.ExecBuilderNode = (*CreateTable)(nil)
var _ sql.SchemaTarget = (*CreateTable)(nil)
var _ sql.Expressioner = (*CreateTable)(nil)

// NewCreateTable returns a new *CreateTable.
func NewCreateTable(createTable *plan.CreateTable, sequences []*CreateSequence) *CreateTable {
	return &CreateTable{
		gmsCreateTable: createTable,
		sequences:      sequences,
	}
}

// Children implements the interface sql.ExecBuilderNode.
func (c *CreateTable) Children() []sql.Node {
	return c.gmsCreateTable.Children()
}

// DebugString implements the sql.DebugStringer interface
func (c *CreateTable) DebugString(ctx *sql.Context) string {
	return sql.DebugString(ctx, c.gmsCreateTable)
}

// Expressions implements the sql.Expressioner interface.
func (c *CreateTable) Expressions() []sql.Expression {
	return c.gmsCreateTable.Expressions()
}

// IsReadOnly implements the interface sql.ExecBuilderNode.
func (c *CreateTable) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecBuilderNode.
func (c *CreateTable) Resolved() bool {
	return c.gmsCreateTable != nil && c.gmsCreateTable.Resolved()
}

// BuildRowIter implements the interface sql.ExecBuilderNode.
func (c *CreateTable) BuildRowIter(ctx *sql.Context, b sql.NodeExecBuilder, r sql.Row) (sql.RowIter, error) {
	// Prevent tables from having names like `guid()`, which resembles a function
	leftParen := strings.IndexByte(c.gmsCreateTable.Name(), '(')
	rightParen := strings.IndexByte(c.gmsCreateTable.Name(), ')')
	if leftParen != -1 && rightParen != -1 && rightParen > leftParen {
		return nil, fmt.Errorf("table name `%s` cannot contain a parenthesized portion", c.gmsCreateTable.Name())
	}

	tableAlreadyExisted := false
	if c.gmsCreateTable.IfNotExists() {
		_, alreadyExisted, err := c.gmsCreateTable.Db.GetTableInsensitive(ctx, c.gmsCreateTable.Name())
		if err != nil {
			return nil, err
		}
		tableAlreadyExisted = alreadyExisted
	}

	createTableIter, err := b.Build(ctx, c.gmsCreateTable, r)
	if err != nil {
		return nil, err
	}

	if !tableAlreadyExisted {
		if comment, ok := doltgresTableMetadataComment(c.gmsCreateTable.TableOpts); ok {
			if err = modifyTableComment(ctx, c.gmsCreateTable.Db, c.gmsCreateTable.Name(), comment); err != nil {
				_ = createTableIter.Close(ctx)
				return nil, err
			}
		}
	}

	schemaName, err := core.GetSchemaName(ctx, c.gmsCreateTable.Db, "")
	if err != nil {
		return nil, err
	}
	for _, sequence := range c.sequences {
		sequence.schema = schemaName
		_, err = sequence.RowIter(ctx, r)
		if err != nil {
			_ = createTableIter.Close(ctx)
			return nil, err
		}
	}
	return createTableIter, err
}

// Schema implements the interface sql.ExecBuilderNode.
func (c *CreateTable) Schema(ctx *sql.Context) sql.Schema {
	return c.gmsCreateTable.Schema(ctx)
}

// String implements the interface sql.ExecBuilderNode.
func (c *CreateTable) String() string {
	return c.gmsCreateTable.String()
}

// TargetSchema implements the interface sql.SchemaTarget.
func (c *CreateTable) TargetSchema() sql.Schema {
	return c.gmsCreateTable.TargetSchema()
}

// WithChildren implements the interface sql.ExecBuilderNode.
func (c *CreateTable) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	gmsCreateTable, err := c.gmsCreateTable.WithChildren(ctx, children...)
	if err != nil {
		return nil, err
	}
	return &CreateTable{
		gmsCreateTable: gmsCreateTable.(*plan.CreateTable),
		sequences:      c.sequences,
	}, nil
}

// WithExpressions implements the interface sql.Expressioner.
func (c *CreateTable) WithExpressions(ctx *sql.Context, expression ...sql.Expression) (sql.Node, error) {
	nc := *c
	n, err := nc.gmsCreateTable.WithExpressions(ctx, expression...)
	if err != nil {
		return nil, err
	}

	nc.gmsCreateTable = n.(*plan.CreateTable)
	return &nc, nil
}

// WithTargetSchema implements the interface sql.SchemaTarget.
func (c CreateTable) WithTargetSchema(schema sql.Schema) (sql.Node, error) {
	n, err := c.gmsCreateTable.WithTargetSchema(schema)
	if err != nil {
		return nil, err
	}

	c.gmsCreateTable = n.(*plan.CreateTable)

	return &c, nil
}

func doltgresTableMetadataComment(tableOpts map[string]any) (string, bool) {
	if tableOpts == nil {
		return "", false
	}
	comment, ok := tableOpts["comment"].(string)
	if !ok {
		return "", false
	}
	if _, ok = tablemetadata.DecodeComment(comment); !ok {
		return "", false
	}
	return comment, true
}

func modifyTableComment(ctx *sql.Context, db sql.Database, tableName string, comment string) error {
	db, err := freshDatabase(ctx, db)
	if err != nil {
		return err
	}
	table, ok, err := db.GetTableInsensitive(ctx, tableName)
	if err != nil {
		return err
	}
	if !ok {
		return sql.ErrTableNotFound.New(tableName)
	}
	alterable, ok := table.(sql.CommentAlterableTable)
	if !ok {
		return sql.ErrAlterTableCommentNotSupported.New(table.Name())
	}
	return alterable.ModifyComment(ctx, comment)
}

func freshDatabase(ctx *sql.Context, db sql.Database) (sql.Database, error) {
	currentDb, err := core.GetSqlDatabaseFromContext(ctx, "")
	if err != nil || currentDb == nil {
		return db, err
	}
	databaseSchema, ok := db.(sql.DatabaseSchema)
	if !ok {
		return currentDb, nil
	}
	schemaDb, ok := currentDb.(sql.SchemaDatabase)
	if !ok {
		return db, nil
	}
	freshSchema, ok, err := schemaDb.GetSchema(ctx, databaseSchema.SchemaName())
	if err != nil {
		return nil, err
	}
	if !ok {
		return db, nil
	}
	return freshSchema, nil
}
