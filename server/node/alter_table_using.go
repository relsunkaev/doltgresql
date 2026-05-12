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
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	pgexprs "github.com/dolthub/doltgresql/server/expression"
)

// AlterTableAlterColumnTypeUsing applies a PostgreSQL USING expression before
// delegating the type rewrite to the existing ALTER COLUMN TYPE path.
type AlterTableAlterColumnTypeUsing struct {
	table      string
	column     string
	targetType string
	usingExpr  string
	Runner     pgexprs.StatementRunner
}

var _ sql.ExecSourceRel = (*AlterTableAlterColumnTypeUsing)(nil)
var _ sql.Expressioner = (*AlterTableAlterColumnTypeUsing)(nil)
var _ vitess.Injectable = (*AlterTableAlterColumnTypeUsing)(nil)

// NewAlterTableAlterColumnTypeUsing returns a new *AlterTableAlterColumnTypeUsing.
func NewAlterTableAlterColumnTypeUsing(table string, column string, targetType string, usingExpr string) *AlterTableAlterColumnTypeUsing {
	return &AlterTableAlterColumnTypeUsing{
		table:      table,
		column:     column,
		targetType: targetType,
		usingExpr:  usingExpr,
	}
}

// Children implements sql.ExecSourceRel.
func (a *AlterTableAlterColumnTypeUsing) Children() []sql.Node { return nil }

// Expressions implements sql.Expressioner.
func (a *AlterTableAlterColumnTypeUsing) Expressions() []sql.Expression {
	return []sql.Expression{a.Runner}
}

// IsReadOnly implements sql.ExecSourceRel.
func (a *AlterTableAlterColumnTypeUsing) IsReadOnly() bool { return false }

// Resolved implements sql.ExecSourceRel.
func (a *AlterTableAlterColumnTypeUsing) Resolved() bool { return true }

// RowIter implements sql.ExecSourceRel.
func (a *AlterTableAlterColumnTypeUsing) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	if a.Runner.Runner == nil {
		return nil, errors.New("statement runner is not available for ALTER COLUMN TYPE USING")
	}
	tempColumn := quoteAlterUsingIdent("__doltgres_alter_using_" + strings.Trim(a.column, `"`))
	if err := a.run(ctx, fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", a.table, tempColumn, a.targetType)); err != nil {
		return nil, err
	}
	if err := a.run(ctx, fmt.Sprintf("UPDATE %s SET %s = %s", a.table, tempColumn, a.usingExpr)); err != nil {
		_ = a.run(ctx, fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", a.table, tempColumn))
		return nil, err
	}
	if err := a.run(ctx, fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", a.table, a.column)); err != nil {
		return nil, err
	}
	if err := a.run(ctx, fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s", a.table, tempColumn, a.column)); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

// Schema implements sql.ExecSourceRel.
func (a *AlterTableAlterColumnTypeUsing) Schema(ctx *sql.Context) sql.Schema { return nil }

// String implements sql.ExecSourceRel.
func (a *AlterTableAlterColumnTypeUsing) String() string {
	return "ALTER TABLE ALTER COLUMN TYPE USING"
}

// WithChildren implements sql.ExecSourceRel.
func (a *AlterTableAlterColumnTypeUsing) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(a, children...)
}

// WithResolvedChildren implements vitess.Injectable.
func (a *AlterTableAlterColumnTypeUsing) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return a, nil
}

// WithExpressions implements sql.Expressioner.
func (a *AlterTableAlterColumnTypeUsing) WithExpressions(ctx *sql.Context, expressions ...sql.Expression) (sql.Node, error) {
	if len(expressions) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(a, len(expressions), 1)
	}
	newA := *a
	newA.Runner = expressions[0].(pgexprs.StatementRunner)
	return &newA, nil
}

func (a *AlterTableAlterColumnTypeUsing) run(ctx *sql.Context, query string) error {
	_, err := sql.RunInterpreted(ctx, func(subCtx *sql.Context) ([]sql.Row, error) {
		_, rowIter, _, err := a.Runner.Runner.QueryWithBindings(subCtx, query, nil, nil, nil)
		if err != nil {
			return nil, err
		}
		return sql.RowIterToRows(subCtx, rowIter)
	})
	return err
}

func quoteAlterUsingIdent(ident string) string {
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
}
