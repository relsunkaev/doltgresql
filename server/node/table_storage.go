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

type alterTableStorageTarget struct {
	ifExists bool
	schema   string
	table    string
}

// AlterTableSetStorage handles ALTER TABLE ... SET/RESET storage parameters.
type AlterTableSetStorage struct {
	target     alterTableStorageTarget
	relOptions []string
	resetKeys  []string
}

var _ sql.ExecSourceRel = (*AlterTableSetStorage)(nil)
var _ vitess.Injectable = (*AlterTableSetStorage)(nil)

// NewAlterTableSetStorage returns a new *AlterTableSetStorage.
func NewAlterTableSetStorage(ifExists bool, schema string, table string, relOptions []string, resetKeys []string) *AlterTableSetStorage {
	return &AlterTableSetStorage{
		target: alterTableStorageTarget{
			ifExists: ifExists,
			schema:   schema,
			table:    table,
		},
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
	table, err := a.target.resolveTable(ctx)
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

func (a alterTableStorageTarget) resolveTable(ctx *sql.Context) (sql.Table, error) {
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

// AlterTableSetColumnOptions handles ALTER TABLE ... ALTER COLUMN ... SET/RESET
// column storage parameters exposed through pg_attribute.attoptions.
type AlterTableSetColumnOptions struct {
	target    alterTableStorageTarget
	column    string
	options   []string
	resetKeys []string
}

var _ sql.ExecSourceRel = (*AlterTableSetColumnOptions)(nil)
var _ vitess.Injectable = (*AlterTableSetColumnOptions)(nil)

// NewAlterTableSetColumnOptions returns a new *AlterTableSetColumnOptions.
func NewAlterTableSetColumnOptions(ifExists bool, schema string, table string, column string, options []string, resetKeys []string) *AlterTableSetColumnOptions {
	return &AlterTableSetColumnOptions{
		target: alterTableStorageTarget{
			ifExists: ifExists,
			schema:   schema,
			table:    table,
		},
		column:    column,
		options:   append([]string(nil), options...),
		resetKeys: append([]string(nil), resetKeys...),
	}
}

// Children implements the interface sql.ExecSourceRel.
func (a *AlterTableSetColumnOptions) Children() []sql.Node {
	return nil
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (a *AlterTableSetColumnOptions) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (a *AlterTableSetColumnOptions) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (a *AlterTableSetColumnOptions) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	table, err := a.target.resolveTable(ctx)
	if err != nil {
		return nil, err
	}
	if table == nil {
		return sql.RowsToRowIter(), nil
	}
	if _, ok := columnByName(table.Schema(ctx), a.column); !ok {
		return nil, errors.Errorf(`column "%s" of relation "%s" does not exist`, a.column, a.target.table)
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
	options := tablemetadata.ColumnOptions(comment, a.column)
	if len(a.resetKeys) > 0 {
		options = tablemetadata.ResetRelOptions(options, a.resetKeys)
	} else {
		options = tablemetadata.MergeRelOptions(options, a.options)
	}
	if err = alterable.ModifyComment(ctx, tablemetadata.SetColumnOptions(comment, a.column, options)); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

// Schema implements the interface sql.ExecSourceRel.
func (a *AlterTableSetColumnOptions) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (a *AlterTableSetColumnOptions) String() string {
	return "ALTER TABLE SET COLUMN OPTIONS"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (a *AlterTableSetColumnOptions) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(a, children...)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (a *AlterTableSetColumnOptions) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return a, nil
}

// AlterTableSetColumnStorage handles ALTER TABLE ... ALTER COLUMN ... SET
// STORAGE.
type AlterTableSetColumnStorage struct {
	target  alterTableStorageTarget
	column  string
	storage string
}

var _ sql.ExecSourceRel = (*AlterTableSetColumnStorage)(nil)
var _ vitess.Injectable = (*AlterTableSetColumnStorage)(nil)

// NewAlterTableSetColumnStorage returns a new *AlterTableSetColumnStorage.
func NewAlterTableSetColumnStorage(ifExists bool, schema string, table string, column string, storage string) *AlterTableSetColumnStorage {
	return &AlterTableSetColumnStorage{
		target: alterTableStorageTarget{
			ifExists: ifExists,
			schema:   schema,
			table:    table,
		},
		column:  column,
		storage: storage,
	}
}

// Children implements the interface sql.ExecSourceRel.
func (a *AlterTableSetColumnStorage) Children() []sql.Node {
	return nil
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (a *AlterTableSetColumnStorage) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (a *AlterTableSetColumnStorage) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (a *AlterTableSetColumnStorage) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return alterColumnMetadata(ctx, a.target, a.column, func(comment string) string {
		return tablemetadata.SetColumnStorage(comment, a.column, a.storage)
	})
}

// Schema implements the interface sql.ExecSourceRel.
func (a *AlterTableSetColumnStorage) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (a *AlterTableSetColumnStorage) String() string {
	return "ALTER TABLE SET COLUMN STORAGE"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (a *AlterTableSetColumnStorage) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(a, children...)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (a *AlterTableSetColumnStorage) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return a, nil
}

// AlterTableSetColumnCompression handles ALTER TABLE ... ALTER COLUMN ... SET
// COMPRESSION.
type AlterTableSetColumnCompression struct {
	target      alterTableStorageTarget
	column      string
	compression string
}

var _ sql.ExecSourceRel = (*AlterTableSetColumnCompression)(nil)
var _ vitess.Injectable = (*AlterTableSetColumnCompression)(nil)

// NewAlterTableSetColumnCompression returns a new *AlterTableSetColumnCompression.
func NewAlterTableSetColumnCompression(ifExists bool, schema string, table string, column string, compression string) *AlterTableSetColumnCompression {
	return &AlterTableSetColumnCompression{
		target: alterTableStorageTarget{
			ifExists: ifExists,
			schema:   schema,
			table:    table,
		},
		column:      column,
		compression: compression,
	}
}

// Children implements the interface sql.ExecSourceRel.
func (a *AlterTableSetColumnCompression) Children() []sql.Node {
	return nil
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (a *AlterTableSetColumnCompression) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (a *AlterTableSetColumnCompression) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (a *AlterTableSetColumnCompression) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return alterColumnMetadata(ctx, a.target, a.column, func(comment string) string {
		return tablemetadata.SetColumnCompression(comment, a.column, a.compression)
	})
}

// Schema implements the interface sql.ExecSourceRel.
func (a *AlterTableSetColumnCompression) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (a *AlterTableSetColumnCompression) String() string {
	return "ALTER TABLE SET COLUMN COMPRESSION"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (a *AlterTableSetColumnCompression) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(a, children...)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (a *AlterTableSetColumnCompression) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return a, nil
}

// AlterTableSetColumnStatistics handles ALTER TABLE ... ALTER COLUMN ... SET
// STATISTICS.
type AlterTableSetColumnStatistics struct {
	target      alterTableStorageTarget
	column      string
	targetValue int16
}

var _ sql.ExecSourceRel = (*AlterTableSetColumnStatistics)(nil)
var _ vitess.Injectable = (*AlterTableSetColumnStatistics)(nil)

// NewAlterTableSetColumnStatistics returns a new *AlterTableSetColumnStatistics.
func NewAlterTableSetColumnStatistics(ifExists bool, schema string, table string, column string, targetValue int16) *AlterTableSetColumnStatistics {
	return &AlterTableSetColumnStatistics{
		target: alterTableStorageTarget{
			ifExists: ifExists,
			schema:   schema,
			table:    table,
		},
		column:      column,
		targetValue: targetValue,
	}
}

// Children implements the interface sql.ExecSourceRel.
func (a *AlterTableSetColumnStatistics) Children() []sql.Node {
	return nil
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (a *AlterTableSetColumnStatistics) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (a *AlterTableSetColumnStatistics) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (a *AlterTableSetColumnStatistics) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return alterColumnMetadata(ctx, a.target, a.column, func(comment string) string {
		return tablemetadata.SetColumnStatisticsTarget(comment, a.column, a.targetValue)
	})
}

// Schema implements the interface sql.ExecSourceRel.
func (a *AlterTableSetColumnStatistics) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (a *AlterTableSetColumnStatistics) String() string {
	return "ALTER TABLE SET COLUMN STATISTICS"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (a *AlterTableSetColumnStatistics) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(a, children...)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (a *AlterTableSetColumnStatistics) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return a, nil
}

func alterColumnMetadata(ctx *sql.Context, target alterTableStorageTarget, column string, updateComment func(string) string) (sql.RowIter, error) {
	table, err := target.resolveTable(ctx)
	if err != nil {
		return nil, err
	}
	if table == nil {
		return sql.RowsToRowIter(), nil
	}
	if _, ok := columnByName(table.Schema(ctx), column); !ok {
		return nil, errors.Errorf(`column "%s" of relation "%s" does not exist`, column, target.table)
	}
	commented, ok := table.(sql.CommentedTable)
	if !ok {
		return nil, sql.ErrAlterTableCommentNotSupported.New(table.Name())
	}
	alterable, ok := table.(sql.CommentAlterableTable)
	if !ok {
		return nil, sql.ErrAlterTableCommentNotSupported.New(table.Name())
	}
	if err = alterable.ModifyComment(ctx, updateComment(commented.Comment())); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

func columnByName(schema sql.Schema, name string) (*sql.Column, bool) {
	for _, column := range schema {
		if column.Name == name {
			return column, true
		}
	}
	return nil, false
}
