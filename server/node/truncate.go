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

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	pgexprs "github.com/dolthub/doltgresql/server/expression"
)

// TruncateTables executes a PostgreSQL TRUNCATE relation list by delegating each
// relation to the existing single-table TRUNCATE implementation.
type TruncateTables struct {
	Statements []TruncateTableStatement
	Runner     pgexprs.StatementRunner
}

// TruncateTableStatement is a single-table TRUNCATE statement delegated by
// TruncateTables.
type TruncateTableStatement struct {
	Query      string
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
func NewTruncateTables(statements []TruncateTableStatement) *TruncateTables {
	return &TruncateTables{Statements: statements}
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
	rowsAffected := 0
	for _, statement := range t.Statements {
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
