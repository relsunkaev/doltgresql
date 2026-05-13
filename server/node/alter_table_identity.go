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
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/sequences"
	"github.com/dolthub/doltgresql/server/tablemetadata"
)

type AlterTableIdentityGeneration uint8

const (
	AlterTableIdentityGenerationUnchanged AlterTableIdentityGeneration = iota
	AlterTableIdentityGenerationAlways
	AlterTableIdentityGenerationByDefault
)

// AlterTableIdentity handles ALTER TABLE ... ALTER COLUMN identity operations.
type AlterTableIdentity struct {
	schema     string
	table      string
	column     string
	ifExists   bool
	drop       bool
	generation AlterTableIdentityGeneration
	options    []AlterSequenceOption
}

var _ sql.ExecSourceRel = (*AlterTableIdentity)(nil)
var _ vitess.Injectable = (*AlterTableIdentity)(nil)

func NewAlterTableIdentity(
	schema string,
	table string,
	column string,
	ifExists bool,
	drop bool,
	generation AlterTableIdentityGeneration,
	options []AlterSequenceOption,
) *AlterTableIdentity {
	return &AlterTableIdentity{
		schema:     schema,
		table:      table,
		column:     column,
		ifExists:   ifExists,
		drop:       drop,
		generation: generation,
		options:    options,
	}
}

func (a *AlterTableIdentity) Children() []sql.Node {
	return nil
}

func (a *AlterTableIdentity) IsReadOnly() bool {
	return false
}

func (a *AlterTableIdentity) Resolved() bool {
	return true
}

func (a *AlterTableIdentity) RowIter(ctx *sql.Context, r sql.Row) (sql.RowIter, error) {
	schemaName, err := core.GetSchemaName(ctx, nil, a.schema)
	if err != nil {
		return nil, err
	}
	tableName := doltdb.TableName{Name: a.table, Schema: schemaName}
	if err = checkTableOwnership(ctx, tableName); err != nil {
		return nil, errors.Wrap(err, "permission denied")
	}
	table, err := core.GetSqlTableFromContext(ctx, ctx.GetCurrentDatabase(), tableName)
	if err != nil {
		return nil, err
	}
	if table == nil {
		return nil, errors.Errorf(`relation "%s" does not exist`, a.table)
	}
	alterable, ok := table.(*sqle.AlterableDoltTable)
	if !ok {
		return nil, errors.Errorf(`expected a Dolt table but received "%T"`, table)
	}
	column := findIdentityColumn(ctx, table, a.column)
	if column == nil {
		return nil, errors.Errorf(`column "%s" of relation "%s" does not exist`, a.column, a.table)
	}

	seq, err := a.identitySequence(ctx, tableName)
	if err != nil {
		return nil, err
	}
	if seq == nil {
		if a.drop && a.ifExists {
			return sql.RowsToRowIter(), nil
		}
		return nil, errors.Errorf(`column "%s" of relation "%s" is not an identity column`, a.column, a.table)
	}

	if len(a.options) > 0 {
		if err = applyAlterSequenceOptions(seq, a.options); err != nil {
			return nil, err
		}
	}
	if a.drop || a.generation != AlterTableIdentityGenerationUnchanged {
		updated := column.Copy()
		identity := ""
		switch {
		case a.drop:
			updated.Default = nil
			updated.Generated = nil
		case a.generation == AlterTableIdentityGenerationAlways:
			identity = "a"
			expr := updated.Generated
			if expr == nil {
				expr = updated.Default
			}
			if expr == nil {
				return nil, errors.Errorf(`column "%s" of relation "%s" is not an identity column`, a.column, a.table)
			}
			updated.Generated = expr
			updated.Default = nil
		case a.generation == AlterTableIdentityGenerationByDefault:
			identity = "d"
			expr := updated.Default
			if expr == nil {
				expr = updated.Generated
			}
			if expr == nil {
				return nil, errors.Errorf(`column "%s" of relation "%s" is not an identity column`, a.column, a.table)
			}
			updated.Default = expr
			updated.Generated = nil
		}
		if err = alterable.ModifyColumn(ctx, column.Name, updated, nil); err != nil {
			return nil, err
		}
		if err = updateIdentityColumnMetadata(ctx, table, updated.Name, identity); err != nil {
			return nil, err
		}
	}
	return sql.RowsToRowIter(), nil
}

func updateIdentityColumnMetadata(ctx *sql.Context, table sql.Table, columnName string, identity string) error {
	commented, ok := table.(sql.CommentedTable)
	if !ok {
		return sql.ErrAlterTableCommentNotSupported.New(table.Name())
	}
	alterable, ok := table.(sql.CommentAlterableTable)
	if !ok {
		return sql.ErrAlterTableCommentNotSupported.New(table.Name())
	}
	return alterable.ModifyComment(ctx, tablemetadata.SetColumnIdentity(commented.Comment(), columnName, identity))
}

func findIdentityColumn(ctx *sql.Context, table sql.Table, columnName string) *sql.Column {
	for _, column := range table.Schema(ctx) {
		if strings.EqualFold(column.Name, columnName) {
			return column.Copy()
		}
	}
	return nil
}

func (a *AlterTableIdentity) identitySequence(ctx *sql.Context, tableName doltdb.TableName) (*sequences.Sequence, error) {
	collection, err := core.GetSequencesCollectionFromContext(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return nil, err
	}
	seqs, err := collection.GetSequencesWithTable(ctx, tableName)
	if err != nil {
		return nil, err
	}
	for _, seq := range seqs {
		if strings.EqualFold(seq.OwnerColumn, a.column) {
			return seq, nil
		}
	}
	return nil, nil
}

func (a *AlterTableIdentity) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

func (a *AlterTableIdentity) String() string {
	return "ALTER TABLE ALTER COLUMN IDENTITY"
}

func (a *AlterTableIdentity) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(a, children...)
}

func (a *AlterTableIdentity) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return a, nil
}
