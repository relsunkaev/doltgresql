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
	"fmt"
	"io"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
)

// TableMetadataApplier runs a child DDL node and then applies Doltgres table
// metadata and output-column aliases that lower-level table-copy optimizations
// may overwrite or drop.
type TableMetadataApplier struct {
	child         sql.Node
	db            sql.Database
	tableName     string
	comment       string
	columnAliases []string
}

var _ sql.DebugStringer = (*TableMetadataApplier)(nil)
var _ sql.ExecBuilderNode = (*TableMetadataApplier)(nil)

// NewTableMetadataApplier returns a new *TableMetadataApplier.
func NewTableMetadataApplier(child sql.Node, db sql.Database, tableName string, comment string) *TableMetadataApplier {
	return &TableMetadataApplier{
		child:     child,
		db:        db,
		tableName: tableName,
		comment:   comment,
	}
}

// NewTableMetadataApplierWithColumnAliases returns a new
// *TableMetadataApplier that also renames the first N output columns after a
// successful CREATE TABLE AS copy.
func NewTableMetadataApplierWithColumnAliases(child sql.Node, db sql.Database, tableName string, comment string, columnAliases []string) *TableMetadataApplier {
	return &TableMetadataApplier{
		child:         child,
		db:            db,
		tableName:     tableName,
		comment:       comment,
		columnAliases: columnAliases,
	}
}

// Children implements the interface sql.ExecBuilderNode.
func (m *TableMetadataApplier) Children() []sql.Node {
	return []sql.Node{m.child}
}

// DebugString implements the sql.DebugStringer interface.
func (m *TableMetadataApplier) DebugString(ctx *sql.Context) string {
	return sql.DebugString(ctx, m.child)
}

// IsReadOnly implements the interface sql.ExecBuilderNode.
func (m *TableMetadataApplier) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecBuilderNode.
func (m *TableMetadataApplier) Resolved() bool {
	return m.child.Resolved()
}

// BuildRowIter implements the interface sql.ExecBuilderNode.
func (m *TableMetadataApplier) BuildRowIter(ctx *sql.Context, b sql.NodeExecBuilder, r sql.Row) (sql.RowIter, error) {
	childIter, err := b.Build(ctx, m.child, r)
	if err != nil {
		return nil, err
	}
	if childIter == nil {
		childIter = sql.RowsToRowIter()
	}
	return &tableMetadataApplierIter{
		childIter:     childIter,
		db:            m.db,
		tableName:     m.tableName,
		comment:       m.comment,
		columnAliases: m.columnAliases,
	}, nil
}

// Schema implements the interface sql.ExecBuilderNode.
func (m *TableMetadataApplier) Schema(ctx *sql.Context) sql.Schema {
	return types.OkResultSchema
}

// String implements the interface sql.ExecBuilderNode.
func (m *TableMetadataApplier) String() string {
	return m.child.String()
}

// WithChildren implements the interface sql.ExecBuilderNode.
func (m *TableMetadataApplier) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(m, len(children), 1)
	}
	return NewTableMetadataApplierWithColumnAliases(children[0], m.db, m.tableName, m.comment, m.columnAliases), nil
}

type tableMetadataApplierIter struct {
	childIter     sql.RowIter
	db            sql.Database
	tableName     string
	comment       string
	columnAliases []string
	done          bool
	closed        bool
	applied       bool
	childFailed   bool
}

var _ sql.RowIter = (*tableMetadataApplierIter)(nil)

// Next implements the interface sql.RowIter.
func (m *tableMetadataApplierIter) Next(ctx *sql.Context) (sql.Row, error) {
	if m.done {
		return nil, io.EOF
	}
	m.done = true

	rowsAffected := 0
	for {
		row, err := m.childIter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			m.childFailed = true
			_ = m.closeChild(ctx)
			return nil, err
		}
		if types.IsOkResult(row) {
			rowsAffected += int(types.GetOkResult(row).RowsAffected)
		} else {
			rowsAffected++
		}
	}
	if err := m.closeChildAndApply(ctx); err != nil {
		return nil, err
	}
	return sql.NewRow(types.NewOkResult(rowsAffected)), nil
}

// Close implements the interface sql.RowIter.
func (m *tableMetadataApplierIter) Close(ctx *sql.Context) error {
	return m.closeChildAndApply(ctx)
}

func (m *tableMetadataApplierIter) closeChildAndApply(ctx *sql.Context) error {
	if err := m.closeChild(ctx); err != nil {
		m.childFailed = true
		return err
	}
	if m.applied || m.childFailed {
		return nil
	}
	m.applied = true
	if err := applyTableColumnAliases(ctx, m.db, m.tableName, m.columnAliases); err != nil {
		return err
	}
	if m.comment == "" {
		return nil
	}
	return modifyTableComment(ctx, m.db, m.tableName, m.comment)
}

func (m *tableMetadataApplierIter) closeChild(ctx *sql.Context) error {
	if m.closed {
		return nil
	}
	m.closed = true
	return m.childIter.Close(ctx)
}

func applyTableColumnAliases(ctx *sql.Context, db sql.Database, tableName string, aliases []string) error {
	if len(aliases) == 0 {
		return nil
	}
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
	schema := table.Schema(ctx)
	if err := ValidateColumnAliases(schema, aliases); err != nil {
		return err
	}
	alterable, ok := table.(sql.AlterableTable)
	if !ok {
		return sql.ErrAlterTableNotSupported.New(table.Name())
	}

	type renameStep struct {
		index int
		from  string
		to    string
		temp  string
	}
	steps := make([]renameStep, 0, len(aliases))
	reservedNames := make(map[string]struct{}, len(schema)+len(aliases))
	for i, col := range schema {
		reservedNames[col.Name] = struct{}{}
		if i < len(aliases) {
			reservedNames[aliases[i]] = struct{}{}
		}
	}
	for i, alias := range aliases {
		from := schema[i].Name
		if from == alias {
			continue
		}
		temp := uniqueMaterializedViewAliasTempName(reservedNames, i)
		reservedNames[temp] = struct{}{}
		steps = append(steps, renameStep{index: i, from: from, to: alias, temp: temp})
	}

	for _, step := range steps {
		if err := modifyColumnName(ctx, alterable, schema[step.index], step.from, step.temp); err != nil {
			return err
		}
	}
	for _, step := range steps {
		if err := modifyColumnName(ctx, alterable, schema[step.index], step.temp, step.to); err != nil {
			return err
		}
	}
	return nil
}

// ValidateColumnAliases checks PostgreSQL CREATE MATERIALIZED VIEW column-list
// semantics: too many aliases are rejected, shorter lists rename the leading
// columns, and the resulting column set must not contain duplicates.
func ValidateColumnAliases(schema sql.Schema, aliases []string) error {
	if len(aliases) == 0 {
		return nil
	}
	if len(aliases) > len(schema) {
		return pgerror.New(pgcode.Syntax, "too many column names were specified")
	}
	seen := make(map[string]struct{}, len(schema))
	for i, col := range schema {
		name := col.Name
		if i < len(aliases) {
			name = aliases[i]
		}
		if _, ok := seen[name]; ok {
			return pgerror.Newf(pgcode.DuplicateColumn, `column "%s" specified more than once`, name)
		}
		seen[name] = struct{}{}
	}
	return nil
}

func uniqueMaterializedViewAliasTempName(existing map[string]struct{}, index int) string {
	for suffix := 0; ; suffix++ {
		name := fmt.Sprintf("__doltgres_mv_alias_%d_%d", index, suffix)
		if _, ok := existing[name]; !ok {
			return name
		}
	}
}

func modifyColumnName(ctx *sql.Context, alterable sql.AlterableTable, original *sql.Column, from string, to string) error {
	renamed := *original
	renamed.Name = to
	return alterable.ModifyColumn(ctx, from, &renamed, nil)
}
