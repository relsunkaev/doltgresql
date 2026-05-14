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

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
	"github.com/dolthub/doltgresql/server/indexmetadata"
)

// ReindexIndex validates a REINDEX INDEX target. Doltgres indexes are
// maintained eagerly, so no rebuild work is needed for current index metadata.
type ReindexIndex struct {
	schema string
	table  string
	index  string
}

var _ sql.ExecSourceRel = (*ReindexIndex)(nil)
var _ vitess.Injectable = (*ReindexIndex)(nil)

// NewReindexIndex returns a new *ReindexIndex.
func NewReindexIndex(schema string, table string, index string) *ReindexIndex {
	return &ReindexIndex{
		schema: schema,
		table:  table,
		index:  index,
	}
}

// Children implements the interface sql.ExecSourceRel.
func (r *ReindexIndex) Children() []sql.Node {
	return nil
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (r *ReindexIndex) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (r *ReindexIndex) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (r *ReindexIndex) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	located, ok, err := locateIndex(ctx, r.schema, r.table, r.index, false)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, pgerror.New(pgcode.UndefinedTable, `relation "`+r.index+`" does not exist`)
	}
	if err = checkIndexTableOwnership(ctx, doltdb.TableName{Schema: located.schema, Name: located.tableName}); err != nil {
		return nil, err
	}
	if err = reindexJsonbGinIndexIfRequested(located); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

// Schema implements the interface sql.ExecSourceRel.
func (r *ReindexIndex) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (r *ReindexIndex) String() string {
	return "REINDEX INDEX"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (r *ReindexIndex) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(r, children...)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (r *ReindexIndex) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return r, nil
}

func reindexJsonbGinIndexIfRequested(located *locatedIndex) error {
	metadata, ok := indexmetadata.DecodeComment(located.index.Comment())
	if !ok || metadata.AccessMethod != indexmetadata.AccessMethodGin || metadata.Gin == nil {
		return nil
	}
	return nil
}

// ReindexTable validates a REINDEX TABLE target.
type ReindexTable struct {
	schema string
	table  string
}

var _ sql.ExecSourceRel = (*ReindexTable)(nil)
var _ vitess.Injectable = (*ReindexTable)(nil)

// NewReindexTable returns a new *ReindexTable.
func NewReindexTable(schema string, table string) *ReindexTable {
	return &ReindexTable{
		schema: schema,
		table:  table,
	}
}

// Children implements the interface sql.ExecSourceRel.
func (r *ReindexTable) Children() []sql.Node {
	return nil
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (r *ReindexTable) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (r *ReindexTable) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (r *ReindexTable) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	schemaName, err := core.GetSchemaName(ctx, nil, r.schema)
	if err != nil {
		return nil, err
	}
	db, err := indexDDLDatabase(ctx, schemaName, false)
	if err != nil {
		return nil, err
	}
	if _, ok, err := db.GetTableInsensitive(ctx, r.table); err != nil {
		return nil, err
	} else if !ok {
		return nil, sql.ErrTableNotFound.New(r.table)
	}
	if err = checkIndexTableOwnership(ctx, doltdb.TableName{Schema: schemaName, Name: r.table}); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

// Schema implements the interface sql.ExecSourceRel.
func (r *ReindexTable) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (r *ReindexTable) String() string {
	return "REINDEX TABLE"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (r *ReindexTable) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(r, children...)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (r *ReindexTable) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return r, nil
}
