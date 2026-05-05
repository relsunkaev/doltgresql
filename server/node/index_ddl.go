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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/server/indexmetadata"
)

type locatedIndex struct {
	db        sql.Database
	table     sql.Table
	alterable sql.IndexAlterableTable
	index     sql.Index
}

// DropIndexTarget identifies one index named in a PostgreSQL DROP INDEX statement.
type DropIndexTarget struct {
	Schema string
	Table  string
	Index  string
}

// DropIndex handles PostgreSQL DROP INDEX, including table-less index names.
type DropIndex struct {
	ifExists bool
	targets  []DropIndexTarget
}

var _ sql.ExecSourceRel = (*DropIndex)(nil)
var _ vitess.Injectable = (*DropIndex)(nil)

// NewDropIndex returns a new *DropIndex.
func NewDropIndex(ifExists bool, schema string, table string, index string) *DropIndex {
	return NewDropIndexes(ifExists, []DropIndexTarget{{
		Schema: schema,
		Table:  table,
		Index:  index,
	}})
}

// NewDropIndexes returns a new *DropIndex for one or more indexes.
func NewDropIndexes(ifExists bool, targets []DropIndexTarget) *DropIndex {
	targets = append([]DropIndexTarget(nil), targets...)
	return &DropIndex{
		ifExists: ifExists,
		targets:  targets,
	}
}

// Children implements the interface sql.ExecSourceRel.
func (d *DropIndex) Children() []sql.Node {
	return nil
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (d *DropIndex) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (d *DropIndex) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (d *DropIndex) RowIter(ctx *sql.Context, r sql.Row) (sql.RowIter, error) {
	type targetIndex struct {
		located     *locatedIndex
		metadata    indexmetadata.Metadata
		hasMetadata bool
	}
	locatedIndexes := make([]targetIndex, 0, len(d.targets))
	for _, target := range d.targets {
		located, ok, err := locateIndex(ctx, target.Schema, target.Table, target.Index, d.ifExists)
		if err != nil {
			return nil, err
		}
		if !ok {
			if d.ifExists {
				continue
			}
			return nil, sql.ErrIndexNotFound.New(target.Index)
		}
		if isConstraintBackedIndex(located.index) {
			indexName := indexmetadata.DisplayNameForTable(located.index, located.table)
			return nil, errors.Errorf(`cannot drop index "%s" because constraint "%s" on table "%s" requires it`,
				indexName, indexName, located.index.Table())
		}

		metadata, hasMetadata := indexmetadata.DecodeComment(located.index.Comment())
		locatedIndexes = append(locatedIndexes, targetIndex{
			located:     located,
			metadata:    metadata,
			hasMetadata: hasMetadata,
		})
	}

	for _, target := range locatedIndexes {
		if err := target.located.alterable.DropIndex(ctx, target.located.index.ID()); err != nil {
			if sql.ErrIndexNotFound.Is(err) && d.ifExists {
				continue
			}
			return nil, err
		}
		if target.hasMetadata && target.metadata.Gin != nil && target.metadata.Gin.PostingTable != "" {
			if err := dropTable(ctx, target.located.db, target.metadata.Gin.PostingTable); err != nil && !sql.ErrTableNotFound.Is(err) {
				return nil, err
			}
		}
	}
	return sql.RowsToRowIter(), nil
}

// Schema implements the interface sql.ExecSourceRel.
func (d *DropIndex) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (d *DropIndex) String() string {
	return "DROP INDEX"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (d *DropIndex) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(d, children...)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (d *DropIndex) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return d, nil
}

// RenameIndex handles PostgreSQL ALTER INDEX RENAME, including table-less index names.
type RenameIndex struct {
	ifExists bool
	schema   string
	table    string
	from     string
	to       string
}

var _ sql.ExecSourceRel = (*RenameIndex)(nil)
var _ vitess.Injectable = (*RenameIndex)(nil)

// NewRenameIndex returns a new *RenameIndex.
func NewRenameIndex(ifExists bool, schema string, table string, from string, to string) *RenameIndex {
	return &RenameIndex{
		ifExists: ifExists,
		schema:   schema,
		table:    table,
		from:     from,
		to:       to,
	}
}

// Children implements the interface sql.ExecSourceRel.
func (r *RenameIndex) Children() []sql.Node {
	return nil
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (r *RenameIndex) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (r *RenameIndex) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (r *RenameIndex) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	located, ok, err := locateIndex(ctx, r.schema, r.table, r.from, r.ifExists)
	if err != nil {
		return nil, err
	}
	if !ok {
		if r.ifExists {
			return sql.RowsToRowIter(), nil
		}
		return nil, sql.ErrIndexNotFound.New(r.from)
	}
	if isPrimaryKeyIndex(located.index) {
		return nil, errors.Errorf("renaming primary key indexes is not yet supported")
	}
	if err = located.alterable.RenameIndex(ctx, located.index.ID(), r.to); err != nil {
		if sql.ErrIndexNotFound.Is(err) && r.ifExists {
			return sql.RowsToRowIter(), nil
		}
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

// Schema implements the interface sql.ExecSourceRel.
func (r *RenameIndex) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (r *RenameIndex) String() string {
	return "ALTER INDEX RENAME"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (r *RenameIndex) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(r, children...)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (r *RenameIndex) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return r, nil
}

func locateIndex(ctx *sql.Context, schemaName string, tableName string, indexName string, missingOK bool) (*locatedIndex, bool, error) {
	schemaName, err := core.GetSchemaName(ctx, nil, schemaName)
	if err != nil {
		return nil, false, err
	}
	db, err := indexDDLDatabase(ctx, schemaName, missingOK)
	if err != nil {
		return nil, false, err
	}
	if db == nil {
		return nil, false, nil
	}
	if tableName != "" {
		table, ok, err := db.GetTableInsensitive(ctx, tableName)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			if missingOK {
				return nil, false, nil
			}
			return nil, false, sql.ErrTableNotFound.New(tableName)
		}
		return locateIndexOnTable(ctx, db, tableName, table, indexName)
	}

	tableNames, err := db.GetTableNames(ctx)
	if err != nil {
		return nil, false, err
	}
	var match *locatedIndex
	for _, nextTableName := range tableNames {
		table, ok, err := db.GetTableInsensitive(ctx, nextTableName)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			continue
		}
		located, ok, err := locateIndexOnTable(ctx, db, nextTableName, table, indexName)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			continue
		}
		if match != nil {
			return nil, false, errors.Errorf(`index "%s" is ambiguous`, indexName)
		}
		match = located
	}
	if match == nil {
		return nil, false, nil
	}
	return match, true, nil
}

func indexDDLDatabase(ctx *sql.Context, schemaName string, missingOK bool) (sql.Database, error) {
	db, err := core.GetSqlDatabaseFromContext(ctx, "")
	if err != nil {
		return nil, err
	}
	if db == nil {
		if missingOK {
			return nil, nil
		}
		return nil, errors.Errorf("database not found")
	}
	schemaDb, ok := db.(sql.SchemaDatabase)
	if !ok {
		return db, nil
	}
	db, ok, err = schemaDb.GetSchema(ctx, schemaName)
	if err != nil {
		return nil, err
	}
	if !ok {
		if missingOK {
			return nil, nil
		}
		return nil, errors.Errorf(`schema "%s" does not exist`, schemaName)
	}
	return db, nil
}

func locateIndexOnTable(ctx *sql.Context, db sql.Database, tableName string, table sql.Table, indexName string) (*locatedIndex, bool, error) {
	indexAddressable, ok := table.(sql.IndexAddressable)
	if !ok {
		return nil, false, nil
	}
	indexes, err := indexAddressable.GetIndexes(ctx)
	if err != nil {
		return nil, false, err
	}
	for _, index := range indexes {
		if !indexNameMatches(index, table, indexName) {
			continue
		}
		alterable, ok := table.(sql.IndexAlterableTable)
		if !ok {
			return nil, false, errors.Errorf(`relation "%s" does not support index alteration`, tableName)
		}
		return &locatedIndex{
			db:        db,
			table:     table,
			alterable: alterable,
			index:     index,
		}, true, nil
	}
	return nil, false, nil
}

func indexNameMatches(index sql.Index, table sql.Table, name string) bool {
	return strings.EqualFold(index.ID(), name) || strings.EqualFold(indexmetadata.DisplayNameForTable(index, table), name)
}

func isPrimaryKeyIndex(index sql.Index) bool {
	if strings.EqualFold(index.ID(), "PRIMARY") {
		return true
	}
	primaryKeyIndex, ok := index.(interface {
		IsPrimaryKey() bool
	})
	return ok && primaryKeyIndex.IsPrimaryKey()
}

func isConstraintBackedIndex(index sql.Index) bool {
	return isPrimaryKeyIndex(index) || (index.IsUnique() && !indexmetadata.IsStandaloneIndex(index.Comment()))
}
