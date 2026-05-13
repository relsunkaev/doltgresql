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
	"context"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
	"github.com/dolthub/go-mysql-server/sql/types"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/core"
	pgexprs "github.com/dolthub/doltgresql/server/expression"
)

// TruncateTables executes a PostgreSQL TRUNCATE relation list by delegating each
// relation to the existing single-table TRUNCATE implementation.
type TruncateTables struct {
	Statements []TruncateTableStatement
	Cascade    bool
	Runner     pgexprs.StatementRunner
}

// TruncateTableStatement is a single-table TRUNCATE statement delegated by
// TruncateTables.
type TruncateTableStatement struct {
	Query      string
	Database   string
	Schema     string
	Table      string
	TempShadow *TruncateTempShadow
}

// TruncateTempShadow describes a same-name temporary table to hide while an
// explicit non-temp schema-qualified TRUNCATE resolves its persistent target.
type TruncateTempShadow struct {
	Database string
	Table    string
}

var _ sql.ExecSourceRel = (*TruncateTables)(nil)
var _ sql.Expressioner = (*TruncateTables)(nil)
var _ vitess.Injectable = (*TruncateTables)(nil)

// NewTruncateTables returns a new *TruncateTables.
func NewTruncateTables(statements []TruncateTableStatement, cascade bool) *TruncateTables {
	return &TruncateTables{Statements: statements, Cascade: cascade}
}

// Children implements the interface sql.ExecSourceRel.
func (t *TruncateTables) Children() []sql.Node {
	return nil
}

// Expressions implements the interface sql.Expressioner.
func (t *TruncateTables) Expressions() []sql.Expression {
	return []sql.Expression{t.Runner}
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (t *TruncateTables) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (t *TruncateTables) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (t *TruncateTables) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	if t.Runner.Runner == nil {
		return nil, errors.New("statement runner is not available for TRUNCATE")
	}
	statements := t.Statements
	if t.Cascade {
		var err error
		statements, err = t.cascadeStatements(ctx, statements)
		if err != nil {
			return nil, err
		}
		restore, err := disableForeignKeyChecks(ctx)
		if err != nil {
			return nil, err
		}
		defer restore()
	}
	rowsAffected := 0
	for _, statement := range statements {
		rows, err := t.runStatement(ctx, statement)
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			if types.IsOkResult(row) {
				rowsAffected += int(types.GetOkResult(row).RowsAffected)
			}
		}
	}
	return sql.RowsToRowIter(sql.NewRow(types.NewOkResult(rowsAffected))), nil
}

func disableForeignKeyChecks(ctx *sql.Context) (func(), error) {
	previous, err := ctx.GetSessionVariable(ctx, "foreign_key_checks")
	if err != nil {
		return nil, err
	}
	if err = ctx.Session.SetSessionVariable(ctx, "foreign_key_checks", 0); err != nil {
		return nil, err
	}
	return func() {
		_ = ctx.Session.SetSessionVariable(ctx, "foreign_key_checks", previous)
	}, nil
}

func (t *TruncateTables) cascadeStatements(ctx *sql.Context, statements []TruncateTableStatement) ([]TruncateTableStatement, error) {
	seen := make(map[string]struct{}, len(statements))
	expanded := make([]TruncateTableStatement, 0, len(statements))
	var appendWithChildren func(TruncateTableStatement) error
	appendWithChildren = func(statement TruncateTableStatement) error {
		key := statementKey(ctx, statement)
		if _, ok := seen[key]; ok {
			return nil
		}
		seen[key] = struct{}{}
		children, err := t.referencingTables(ctx, statement)
		if err != nil {
			return err
		}
		for _, child := range children {
			if err = appendWithChildren(child); err != nil {
				return err
			}
		}
		expanded = append(expanded, statement)
		return nil
	}
	for _, statement := range statements {
		if err := appendWithChildren(statement); err != nil {
			return nil, err
		}
	}
	return expanded, nil
}

func (t *TruncateTables) referencingTables(ctx *sql.Context, target TruncateTableStatement) ([]TruncateTableStatement, error) {
	dbName := target.Database
	if dbName == "" {
		dbName = ctx.GetCurrentDatabase()
	}
	db, err := core.GetSqlDatabaseFromContext(ctx, dbName)
	if err != nil {
		return nil, err
	}
	if privileged, ok := db.(mysql_db.PrivilegedDatabase); ok {
		db = privileged.Unwrap()
	}
	if schemaDB, ok := db.(sql.SchemaDatabase); ok {
		schemas, err := schemaDB.AllSchemas(ctx)
		if err != nil {
			return nil, err
		}
		var children []TruncateTableStatement
		for _, schema := range schemas {
			schemaChildren, err := t.referencingTablesInDatabase(ctx, target, schema)
			if err != nil {
				return nil, err
			}
			children = append(children, schemaChildren...)
		}
		return children, nil
	}
	return t.referencingTablesInDatabase(ctx, target, db)
}

func (t *TruncateTables) referencingTablesInDatabase(ctx *sql.Context, target TruncateTableStatement, db sql.Database) ([]TruncateTableStatement, error) {
	tableNames, err := db.GetTableNames(ctx)
	if err != nil {
		return nil, err
	}
	children := make([]TruncateTableStatement, 0)
	for _, tableName := range tableNames {
		table, ok, err := db.GetTableInsensitive(ctx, tableName)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		fkTable, ok := table.(sql.ForeignKeyTable)
		if !ok {
			continue
		}
		fks, err := fkTable.GetDeclaredForeignKeys(ctx)
		if err != nil {
			return nil, err
		}
		for _, fk := range fks {
			if foreignKeyReferencesStatement(ctx, fk, target) {
				children = append(children, newTruncateTableStatement(fk.Database, fk.SchemaName, fk.Table))
			}
		}
	}
	return children, nil
}

func foreignKeyReferencesStatement(ctx *sql.Context, fk sql.ForeignKeyConstraint, target TruncateTableStatement) bool {
	dbName := target.Database
	if dbName == "" {
		dbName = ctx.GetCurrentDatabase()
	}
	if fk.ParentDatabase != "" && dbName != "" && !strings.EqualFold(fk.ParentDatabase, dbName) {
		return false
	}
	if target.Schema != "" && fk.ParentSchema != "" && !strings.EqualFold(fk.ParentSchema, target.Schema) {
		return false
	}
	return strings.EqualFold(fk.ParentTable, target.Table)
}

func newTruncateTableStatement(database, schema, table string) TruncateTableStatement {
	tableName := vitess.TableName{
		Name:            vitess.NewTableIdent(table),
		DbQualifier:     vitess.NewTableIdent(database),
		SchemaQualifier: vitess.NewTableIdent(schema),
	}
	statement := TruncateTableStatement{
		Query:    "TRUNCATE TABLE " + vitess.String(tableName),
		Database: database,
		Schema:   schema,
		Table:    table,
	}
	if schema != "" {
		statement.TempShadow = &TruncateTempShadow{
			Database: database,
			Table:    table,
		}
	}
	return statement
}

func statementKey(ctx *sql.Context, statement TruncateTableStatement) string {
	dbName := statement.Database
	if dbName == "" {
		dbName = ctx.GetCurrentDatabase()
	}
	return strings.ToLower(dbName + "." + statement.Schema + "." + statement.Table)
}

func (t *TruncateTables) runStatement(ctx *sql.Context, statement TruncateTableStatement) ([]sql.Row, error) {
	if statement.TempShadow != nil {
		session := dsess.DSessFromSess(ctx.Session)
		database := statement.TempShadow.Database
		if database == "" {
			database = ctx.GetCurrentDatabase()
		}
		if table, ok := session.GetTemporaryTable(ctx, database, statement.TempShadow.Table); ok {
			session.DropTemporaryTable(ctx, database, statement.TempShadow.Table)
			defer session.AddTemporaryTable(ctx, database, table)
		}
	}
	return sql.RunInterpreted(ctx, func(subCtx *sql.Context) ([]sql.Row, error) {
		_, rowIter, _, err := t.Runner.Runner.QueryWithBindings(subCtx, statement.Query, nil, nil, nil)
		if err != nil {
			return nil, err
		}
		return sql.RowIterToRows(subCtx, rowIter)
	})
}

// Schema implements the interface sql.ExecSourceRel.
func (t *TruncateTables) Schema(ctx *sql.Context) sql.Schema {
	return types.OkResultSchema
}

// String implements the interface sql.ExecSourceRel.
func (t *TruncateTables) String() string {
	return "TRUNCATE TABLE"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (t *TruncateTables) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return t, nil
}

// WithExpressions implements the interface sql.Expressioner.
func (t *TruncateTables) WithExpressions(ctx *sql.Context, expressions ...sql.Expression) (sql.Node, error) {
	if len(expressions) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(t, len(expressions), 1)
	}
	newT := *t
	newT.Runner = expressions[0].(pgexprs.StatementRunner)
	return &newT, nil
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (t *TruncateTables) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return t, nil
}
