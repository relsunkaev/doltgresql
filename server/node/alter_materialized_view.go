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

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	pgexprs "github.com/dolthub/doltgresql/server/expression"
	"github.com/dolthub/doltgresql/server/tablemetadata"
)

// AlterMaterializedViewRenameColumn implements the supported
// ALTER MATERIALIZED VIEW ... RENAME COLUMN command for table-backed
// materialized views.
type AlterMaterializedViewRenameColumn struct {
	name      string
	schema    string
	oldColumn string
	newColumn string
	ifExists  bool
	Runner    pgexprs.StatementRunner
}

var _ sql.ExecSourceRel = (*AlterMaterializedViewRenameColumn)(nil)
var _ sql.Expressioner = (*AlterMaterializedViewRenameColumn)(nil)
var _ vitess.Injectable = (*AlterMaterializedViewRenameColumn)(nil)

// NewAlterMaterializedViewRenameColumn returns a new
// *AlterMaterializedViewRenameColumn node.
func NewAlterMaterializedViewRenameColumn(name string, schema string, oldColumn string, newColumn string, ifExists bool) *AlterMaterializedViewRenameColumn {
	return &AlterMaterializedViewRenameColumn{
		name:      name,
		schema:    schema,
		oldColumn: oldColumn,
		newColumn: newColumn,
		ifExists:  ifExists,
	}
}

// Children implements sql.ExecSourceRel.
func (a *AlterMaterializedViewRenameColumn) Children() []sql.Node {
	return nil
}

// Expressions implements sql.Expressioner.
func (a *AlterMaterializedViewRenameColumn) Expressions() []sql.Expression {
	return []sql.Expression{a.Runner}
}

// IsReadOnly implements sql.ExecSourceRel.
func (a *AlterMaterializedViewRenameColumn) IsReadOnly() bool {
	return false
}

// Resolved implements sql.ExecSourceRel.
func (a *AlterMaterializedViewRenameColumn) Resolved() bool {
	return true
}

// RowIter implements sql.ExecSourceRel.
func (a *AlterMaterializedViewRenameColumn) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	target, ok, err := findMaterializedViewRelation(ctx, a.name, a.schema)
	if err != nil {
		return nil, err
	}
	if !ok {
		if a.ifExists {
			return sql.RowsToRowIter(), nil
		}
		return nil, errors.Errorf(`relation "%s" does not exist`, a.name)
	}
	if !tablemetadata.IsMaterializedView(tableComment(target.table)) {
		return nil, errors.Errorf(`relation "%s" is not a materialized view`, a.name)
	}
	comment := tableComment(target.table)

	query := fmt.Sprintf(
		"ALTER TABLE %s RENAME COLUMN %s TO %s",
		quoteQualifiedIdentifier(target.schema, target.table.Name()),
		quoteIdentifier(a.oldColumn),
		quoteIdentifier(a.newColumn),
	)
	if err = a.runStatement(ctx, query); err != nil {
		return nil, err
	}
	if err = modifyTableComment(ctx, target.db, target.table.Name(), comment); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

// Schema implements sql.ExecSourceRel.
func (a *AlterMaterializedViewRenameColumn) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements sql.ExecSourceRel.
func (a *AlterMaterializedViewRenameColumn) String() string {
	return "ALTER MATERIALIZED VIEW RENAME COLUMN"
}

// WithChildren implements sql.ExecSourceRel.
func (a *AlterMaterializedViewRenameColumn) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(a, children...)
}

// WithExpressions implements sql.Expressioner.
func (a *AlterMaterializedViewRenameColumn) WithExpressions(ctx *sql.Context, expressions ...sql.Expression) (sql.Node, error) {
	if len(expressions) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(a, len(expressions), 1)
	}
	newA := *a
	newA.Runner = expressions[0].(pgexprs.StatementRunner)
	return &newA, nil
}

// WithResolvedChildren implements vitess.Injectable.
func (a *AlterMaterializedViewRenameColumn) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return a, nil
}

func (a *AlterMaterializedViewRenameColumn) runStatement(ctx *sql.Context, query string) error {
	if a.Runner.Runner == nil {
		return errors.Errorf("statement runner is not available")
	}
	_, err := sql.RunInterpreted(ctx, func(subCtx *sql.Context) ([]sql.Row, error) {
		_, rowIter, _, err := a.Runner.Runner.QueryWithBindings(subCtx, query, nil, nil, nil)
		if err != nil {
			return nil, err
		}
		return sql.RowIterToRows(subCtx, rowIter)
	})
	return err
}
