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
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/server/auth"
	"github.com/dolthub/doltgresql/server/indexmetadata"
	"github.com/dolthub/doltgresql/server/sessionstate"
	"github.com/dolthub/doltgresql/server/tablemetadata"
)

// TableOptionTemporaryOnCommit carries PostgreSQL's ON COMMIT setting through
// the go-mysql-server CREATE TABLE plan for Doltgres execution.
const TableOptionTemporaryOnCommit = "doltgres_on_commit"

// TemporaryTableOnCommit is the PostgreSQL ON COMMIT action for a temporary table.
type TemporaryTableOnCommit string

const (
	TemporaryTableOnCommitUnset        TemporaryTableOnCommit = ""
	TemporaryTableOnCommitPreserveRows TemporaryTableOnCommit = "preserve_rows"
	TemporaryTableOnCommitDeleteRows   TemporaryTableOnCommit = "delete_rows"
	TemporaryTableOnCommitDrop         TemporaryTableOnCommit = "drop"
)

// CreateTable is a node that implements functionality specifically relevant to Doltgres' table creation needs.
type CreateTable struct {
	gmsCreateTable     *plan.CreateTable
	sequences          []*CreateSequence
	inheritanceParents []tablemetadata.InheritedTable
	temporaryOnCommit  TemporaryTableOnCommit
}

var _ sql.ExecBuilderNode = (*CreateTable)(nil)
var _ sql.SchemaTarget = (*CreateTable)(nil)
var _ sql.Expressioner = (*CreateTable)(nil)

// NewCreateTable returns a new *CreateTable.
func NewCreateTable(createTable *plan.CreateTable, sequences []*CreateSequence, inheritanceParents ...tablemetadata.InheritedTable) *CreateTable {
	return &CreateTable{
		gmsCreateTable:     createTable,
		sequences:          sequences,
		inheritanceParents: append([]tablemetadata.InheritedTable(nil), inheritanceParents...),
		temporaryOnCommit:  temporaryOnCommitFromTableOpts(createTable.TableOpts),
	}
}

// GMSCreateTable returns the wrapped go-mysql-server CREATE TABLE node.
func (c *CreateTable) GMSCreateTable() *plan.CreateTable {
	return c.gmsCreateTable
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
	for _, check := range c.gmsCreateTable.Checks() {
		if err := validateCheckConstraintExpression(ctx, check); err != nil {
			return nil, err
		}
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

	if !tableAlreadyExisted && !c.gmsCreateTable.Temporary() {
		comment := ""
		if existingComment, ok := doltgresTableMetadataComment(c.gmsCreateTable.TableOpts); ok {
			comment = existingComment
		}
		if len(c.inheritanceParents) > 0 {
			comment = tablemetadata.SetInherits(comment, c.inheritanceParents)
		}
		if user := ctx.Client().User; user != "" {
			comment = tablemetadata.SetOwner(comment, user)
		}
		if comment != "" {
			if err = modifyTableComment(ctx, c.gmsCreateTable.Db, c.gmsCreateTable.Name(), comment); err != nil {
				_ = createTableIter.Close(ctx)
				return nil, err
			}
		}
	}
	if !tableAlreadyExisted {
		if err = c.registerTemporaryOnCommit(ctx); err != nil {
			_ = createTableIter.Close(ctx)
			return nil, err
		}
	}

	schemaName, err := core.GetSchemaName(ctx, c.gmsCreateTable.Db, "")
	if err != nil {
		return nil, err
	}
	if !tableAlreadyExisted && !c.gmsCreateTable.Temporary() {
		if err = c.applyPrimaryKeyIndexMetadata(ctx); err != nil {
			_ = createTableIter.Close(ctx)
			return nil, err
		}
	}
	databaseName := databaseNameForSQLDatabase(c.gmsCreateTable.Db)
	for _, sequence := range c.sequences {
		sequence.database = databaseName
		sequence.schema = schemaName
		_, err = sequence.RowIter(ctx, r)
		if err != nil {
			_ = createTableIter.Close(ctx)
			return nil, err
		}
	}
	if !tableAlreadyExisted {
		if err = auth.ApplyDefaultPrivilegesToTable(ctx.Client().User, schemaName, c.gmsCreateTable.Name()); err != nil {
			_ = createTableIter.Close(ctx)
			return nil, err
		}
	}
	return createTableIter, err
}

func (c *CreateTable) applyPrimaryKeyIndexMetadata(ctx *sql.Context) error {
	for _, indexDef := range c.gmsCreateTable.Indexes() {
		if indexDef == nil || indexDef.Constraint != sql.IndexConstraint_Primary || indexDef.Comment == "" {
			continue
		}
		if _, ok := indexmetadata.DecodeComment(indexDef.Comment); !ok {
			continue
		}
		return modifyPrimaryKeyIndexComment(ctx, c.gmsCreateTable.Db, c.gmsCreateTable.Name(), indexDef.Comment)
	}
	return nil
}

type revisionQualifiedDatabase interface {
	RevisionQualifiedName() string
}

func databaseNameForSQLDatabase(db sql.Database) string {
	if db == nil {
		return ""
	}
	if revisionDb, ok := db.(revisionQualifiedDatabase); ok {
		return revisionDb.RevisionQualifiedName()
	}
	return db.Name()
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
		gmsCreateTable:     gmsCreateTable.(*plan.CreateTable),
		sequences:          c.sequences,
		inheritanceParents: append([]tablemetadata.InheritedTable(nil), c.inheritanceParents...),
		temporaryOnCommit:  c.temporaryOnCommit,
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

func temporaryOnCommitFromTableOpts(tableOpts map[string]any) TemporaryTableOnCommit {
	if tableOpts == nil {
		return TemporaryTableOnCommitUnset
	}
	value, _ := tableOpts[TableOptionTemporaryOnCommit].(string)
	switch TemporaryTableOnCommit(value) {
	case TemporaryTableOnCommitPreserveRows, TemporaryTableOnCommitDeleteRows, TemporaryTableOnCommitDrop:
		return TemporaryTableOnCommit(value)
	default:
		return TemporaryTableOnCommitUnset
	}
}

func (c *CreateTable) registerTemporaryOnCommit(ctx *sql.Context) error {
	if !c.gmsCreateTable.Temporary() {
		return nil
	}
	switch c.temporaryOnCommit {
	case TemporaryTableOnCommitUnset, TemporaryTableOnCommitPreserveRows:
		return nil
	case TemporaryTableOnCommitDeleteRows, TemporaryTableOnCommitDrop:
	default:
		return nil
	}

	dbName := c.gmsCreateTable.Db.Name()
	tableName := c.gmsCreateTable.Name()
	action := c.temporaryOnCommit
	connectionID := ctx.Session.ID()
	callbackCtx := ctx.WithContext(context.Background())
	key := strings.ToLower(dbName) + "." + strings.ToLower(tableName)
	sessionstate.RegisterCommitAction(connectionID, key, func() (bool, error) {
		session := dsess.DSessFromSess(callbackCtx.Session)
		table, ok := session.GetTemporaryTable(callbackCtx, dbName, tableName)
		if !ok {
			return false, nil
		}
		switch action {
		case TemporaryTableOnCommitDeleteRows:
			if err := deleteAllTemporaryRows(callbackCtx, table); err != nil {
				return true, err
			}
			return true, nil
		case TemporaryTableOnCommitDrop:
			session.DropTemporaryTable(callbackCtx, dbName, tableName)
			return false, nil
		default:
			return false, nil
		}
	})
	return nil
}

func deleteAllTemporaryRows(ctx *sql.Context, table sql.Table) error {
	if truncateable, ok := table.(sql.TruncateableTable); ok {
		_, err := truncateable.Truncate(ctx)
		return err
	}
	deletable, ok := table.(sql.DeletableTable)
	if !ok {
		return fmt.Errorf("temporary table %s does not support row deletion", table.Name())
	}
	rows, err := collectTableRows(ctx, table)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}
	deleter := deletable.Deleter(ctx)
	deleter.StatementBegin(ctx)
	for _, row := range rows {
		if err = deleter.Delete(ctx, row); err != nil {
			_ = deleter.DiscardChanges(ctx, err)
			_ = deleter.Close(ctx)
			return err
		}
	}
	if err = deleter.StatementComplete(ctx); err != nil {
		_ = deleter.Close(ctx)
		return err
	}
	return deleter.Close(ctx)
}

func collectTableRows(ctx *sql.Context, table sql.Table) ([]sql.Row, error) {
	partitionIter, err := table.Partitions(ctx)
	if err != nil {
		return nil, err
	}
	defer partitionIter.Close(ctx)

	var rows []sql.Row
	for {
		partition, err := partitionIter.Next(ctx)
		if err == io.EOF {
			return rows, nil
		}
		if err != nil {
			return nil, err
		}
		rowIter, err := table.PartitionRows(ctx, partition)
		if err != nil {
			return nil, err
		}
		for {
			row, err := rowIter.Next(ctx)
			if err == io.EOF {
				break
			}
			if err != nil {
				_ = rowIter.Close(ctx)
				return nil, err
			}
			rows = append(rows, append(sql.Row(nil), row...))
		}
		if err = rowIter.Close(ctx); err != nil {
			return nil, err
		}
	}
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

func modifyPrimaryKeyIndexComment(ctx *sql.Context, db sql.Database, tableName string, indexComment string) error {
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
	comment := ""
	if commented, ok := table.(sql.CommentedTable); ok {
		comment = commented.Comment()
	}
	return alterable.ModifyComment(ctx, tablemetadata.SetPrimaryKeyIndexComment(comment, indexComment))
}

func freshDatabase(ctx *sql.Context, db sql.Database) (sql.Database, error) {
	currentDb, err := core.GetSqlDatabaseFromContext(ctx, db.Name())
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
