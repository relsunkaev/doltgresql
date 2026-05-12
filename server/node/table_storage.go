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
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/server/tablemetadata"
)

// AlterTableSetStorage handles ALTER TABLE ... SET/RESET storage parameters.
type AlterTableSetStorage struct {
	ifExists   bool
	schema     string
	table      string
	relOptions []string
	resetKeys  []string
}

var _ sql.ExecSourceRel = (*AlterTableSetStorage)(nil)
var _ vitess.Injectable = (*AlterTableSetStorage)(nil)

// NewAlterTableSetStorage returns a new *AlterTableSetStorage.
func NewAlterTableSetStorage(ifExists bool, schema string, table string, relOptions []string, resetKeys []string) *AlterTableSetStorage {
	return &AlterTableSetStorage{
		ifExists:   ifExists,
		schema:     schema,
		table:      table,
		relOptions: append([]string(nil), relOptions...),
		resetKeys:  append([]string(nil), resetKeys...),
	}
}

// Children implements the interface sql.ExecSourceRel.
func (a *AlterTableSetStorage) Children() []sql.Node {
	return nil
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (a *AlterTableSetStorage) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (a *AlterTableSetStorage) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (a *AlterTableSetStorage) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	table, err := a.resolveTable(ctx)
	if err != nil {
		return nil, err
	}
	if table == nil {
		return sql.RowsToRowIter(), nil
	}
	commented, ok := table.(sql.CommentedTable)
	if !ok {
		return nil, sql.ErrAlterTableCommentNotSupported.New(table.Name())
	}
	alterable, ok := table.(sql.CommentAlterableTable)
	if !ok {
		return nil, sql.ErrAlterTableCommentNotSupported.New(table.Name())
	}
	comment := commented.Comment()
	relOptions := tablemetadata.RelOptions(comment)
	if len(a.resetKeys) > 0 {
		relOptions = tablemetadata.ResetRelOptions(relOptions, a.resetKeys)
	} else {
		relOptions = tablemetadata.MergeRelOptions(relOptions, a.relOptions)
	}
	if err = alterable.ModifyComment(ctx, tablemetadata.SetRelOptions(comment, relOptions)); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

// Schema implements the interface sql.ExecSourceRel.
func (a *AlterTableSetStorage) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (a *AlterTableSetStorage) String() string {
	return "ALTER TABLE SET STORAGE"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (a *AlterTableSetStorage) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(a, children...)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (a *AlterTableSetStorage) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return a, nil
}

func (a *AlterTableSetStorage) resolveTable(ctx *sql.Context) (sql.Table, error) {
	if a.schema != "" {
		table, err := core.GetSqlTableFromContext(ctx, "", doltdb.TableName{Name: a.table, Schema: a.schema})
		if err != nil {
			return nil, err
		}
		if table == nil && !a.ifExists {
			return nil, sql.ErrTableNotFound.New(a.table)
		}
		return table, nil
	}
	searchPaths, err := core.SearchPath(ctx)
	if err != nil {
		return nil, err
	}
	for _, schema := range searchPaths {
		table, err := core.GetSqlTableFromContext(ctx, "", doltdb.TableName{Name: a.table, Schema: schema})
		if err != nil {
			return nil, err
		}
		if table != nil {
			return table, nil
		}
	}
	if a.ifExists {
		return nil, nil
	}
	return nil, errors.Errorf(`relation "%s" does not exist`, a.table)
}
