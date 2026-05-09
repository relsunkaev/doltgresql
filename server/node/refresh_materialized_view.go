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
	"github.com/dolthub/doltgresql/server/functions"
	"github.com/dolthub/doltgresql/server/settings"
	"github.com/dolthub/doltgresql/server/tablemetadata"
)

// RefreshMaterializedView implements PostgreSQL's non-concurrent
// REFRESH MATERIALIZED VIEW ... WITH DATA path for table-backed snapshots.
type RefreshMaterializedView struct {
	name         string
	schema       string
	concurrently bool
	withNoData   bool
	Runner       pgexprs.StatementRunner
}

var _ sql.ExecSourceRel = (*RefreshMaterializedView)(nil)
var _ sql.Expressioner = (*RefreshMaterializedView)(nil)
var _ vitess.Injectable = (*RefreshMaterializedView)(nil)

// NewRefreshMaterializedView returns a new *RefreshMaterializedView.
func NewRefreshMaterializedView(name string, schema string, concurrently bool, withNoData bool) *RefreshMaterializedView {
	return &RefreshMaterializedView{
		name:         name,
		schema:       schema,
		concurrently: concurrently,
		withNoData:   withNoData,
	}
}

// Children implements the interface sql.ExecSourceRel.
func (r *RefreshMaterializedView) Children() []sql.Node {
	return nil
}

// Expressions implements the interface sql.Expressioner.
func (r *RefreshMaterializedView) Expressions() []sql.Expression {
	return []sql.Expression{r.Runner}
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (r *RefreshMaterializedView) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (r *RefreshMaterializedView) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (r *RefreshMaterializedView) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	if r.concurrently {
		return nil, errors.Errorf("REFRESH MATERIALIZED VIEW CONCURRENTLY is not yet supported")
	}
	if r.withNoData {
		return nil, errors.Errorf("REFRESH MATERIALIZED VIEW WITH NO DATA is not yet supported")
	}
	target, err := r.resolveTarget(ctx)
	if err != nil {
		return nil, err
	}
	definition := tablemetadata.MaterializedViewDefinition(tableComment(target.table))
	if strings.TrimSpace(definition) == "" {
		return nil, errors.Errorf(`materialized view "%s" does not have a stored definition`, target.table.Name())
	}

	qualifiedName := quoteQualifiedIdentifier(target.schema, target.table.Name())
	columnList := quoteColumnList(target.table.Schema(ctx))
	if err = r.runRefreshStatement(ctx, "TRUNCATE TABLE "+qualifiedName); err != nil {
		return nil, err
	}
	if err = r.runRefreshStatement(ctx, fmt.Sprintf("INSERT INTO %s (%s) %s", qualifiedName, columnList, definition)); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

// Schema implements the interface sql.ExecSourceRel.
func (r *RefreshMaterializedView) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (r *RefreshMaterializedView) String() string {
	return "REFRESH MATERIALIZED VIEW"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (r *RefreshMaterializedView) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(r, children...)
}

// WithExpressions implements the interface sql.Expressioner.
func (r *RefreshMaterializedView) WithExpressions(ctx *sql.Context, expressions ...sql.Expression) (sql.Node, error) {
	if len(expressions) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(r, len(expressions), 1)
	}
	newR := *r
	newR.Runner = expressions[0].(pgexprs.StatementRunner)
	return &newR, nil
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (r *RefreshMaterializedView) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return r, nil
}

type refreshMaterializedViewTarget struct {
	schema string
	table  sql.Table
}

func (r *RefreshMaterializedView) resolveTarget(ctx *sql.Context) (refreshMaterializedViewTarget, error) {
	searchSchemas, err := r.searchSchemas(ctx)
	if err != nil {
		return refreshMaterializedViewTarget{}, err
	}
	var found refreshMaterializedViewTarget
	err = functions.IterateCurrentDatabase(ctx, functions.Callbacks{
		SearchSchemas: searchSchemas,
		Table: func(ctx *sql.Context, schema functions.ItemSchema, table functions.ItemTable) (cont bool, err error) {
			if table.Item.Name() != r.name {
				return true, nil
			}
			found = refreshMaterializedViewTarget{
				schema: schema.Item.SchemaName(),
				table:  table.Item,
			}
			return false, nil
		},
	})
	if err != nil {
		return refreshMaterializedViewTarget{}, err
	}
	if found.table == nil {
		return refreshMaterializedViewTarget{}, errors.Errorf(`relation "%s" does not exist`, r.name)
	}
	if !tablemetadata.IsMaterializedView(tableComment(found.table)) {
		return refreshMaterializedViewTarget{}, errors.Errorf(`relation "%s" is not a materialized view`, r.name)
	}
	return found, nil
}

func (r *RefreshMaterializedView) searchSchemas(ctx *sql.Context) ([]string, error) {
	if r.schema != "" {
		return []string{r.schema}, nil
	}
	return settings.GetCurrentSchemas(ctx)
}

func (r *RefreshMaterializedView) runRefreshStatement(ctx *sql.Context, query string) error {
	if r.Runner.Runner == nil {
		return errors.Errorf("statement runner is not available")
	}
	_, err := sql.RunInterpreted(ctx, func(subCtx *sql.Context) ([]sql.Row, error) {
		_, rowIter, _, err := r.Runner.Runner.QueryWithBindings(subCtx, query, nil, nil, nil)
		if err != nil {
			return nil, err
		}
		return sql.RowIterToRows(subCtx, rowIter)
	})
	return err
}

func quoteQualifiedIdentifier(schema string, name string) string {
	if schema == "" {
		return quoteIdentifier(name)
	}
	return quoteIdentifier(schema) + "." + quoteIdentifier(name)
}

func quoteColumnList(schema sql.Schema) string {
	quoted := make([]string, len(schema))
	for i, col := range schema {
		quoted[i] = quoteIdentifier(col.Name)
	}
	return strings.Join(quoted, ", ")
}

func quoteIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

func tableComment(table sql.Table) string {
	commented, ok := table.(sql.CommentedTable)
	if !ok {
		return ""
	}
	return commented.Comment()
}
