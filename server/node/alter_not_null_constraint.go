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

	"github.com/dolthub/doltgresql/server/tablemetadata"
)

// AlterNotNullConstraintInheritance handles ALTER TABLE ... ALTER CONSTRAINT
// for PostgreSQL 18 NOT NULL constraints.
type AlterNotNullConstraintInheritance struct {
	target     alterTableStorageTarget
	constraint string
	inherit    bool
}

var _ sql.ExecSourceRel = (*AlterNotNullConstraintInheritance)(nil)
var _ vitess.Injectable = (*AlterNotNullConstraintInheritance)(nil)

// NewAlterNotNullConstraintInheritance returns a new
// *AlterNotNullConstraintInheritance.
func NewAlterNotNullConstraintInheritance(ifExists bool, schema string, table string, constraint string, inherit bool) *AlterNotNullConstraintInheritance {
	return &AlterNotNullConstraintInheritance{
		target: alterTableStorageTarget{
			ifExists: ifExists,
			schema:   schema,
			table:    table,
		},
		constraint: strings.TrimSpace(constraint),
		inherit:    inherit,
	}
}

func (a *AlterNotNullConstraintInheritance) Children() []sql.Node {
	return nil
}

func (a *AlterNotNullConstraintInheritance) IsReadOnly() bool {
	return false
}

func (a *AlterNotNullConstraintInheritance) Resolved() bool {
	return true
}

func (a *AlterNotNullConstraintInheritance) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
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
	for _, column := range table.Schema(ctx) {
		if column.Nullable || column.PrimaryKey || column.HiddenSystem {
			continue
		}
		metadata, ok := tablemetadata.NotNullConstraintMetadata(commented.Comment(), column.Name)
		name := notNullConstraintName(table.Name(), column.Name)
		if ok && metadata.Name != "" {
			name = metadata.Name
		}
		if !strings.EqualFold(name, a.constraint) {
			continue
		}
		metadata.Name = name
		metadata.NoInherit = !a.inherit
		return sql.RowsToRowIter(), alterable.ModifyComment(ctx, tablemetadata.SetNotNullConstraint(commented.Comment(), column.Name, metadata))
	}
	return nil, errors.Errorf(`constraint "%s" of relation "%s" is not a not-null constraint`, a.constraint, a.target.table)
}

func (a *AlterNotNullConstraintInheritance) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

func (a *AlterNotNullConstraintInheritance) String() string {
	return "ALTER TABLE ALTER CONSTRAINT"
}

func (a *AlterNotNullConstraintInheritance) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(a, children...)
}

func (a *AlterNotNullConstraintInheritance) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return a, nil
}

func notNullConstraintName(tableName string, columnName string) string {
	return tableName + "_" + columnName + "_not_null"
}
