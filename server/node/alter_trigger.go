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
	"github.com/dolthub/doltgresql/core/id"
)

// AlterTriggerRename handles ALTER TRIGGER ... RENAME TO.
type AlterTriggerRename struct {
	schema  string
	table   string
	trigger string
	newName string
}

var _ sql.ExecSourceRel = (*AlterTriggerRename)(nil)
var _ vitess.Injectable = (*AlterTriggerRename)(nil)

// NewAlterTriggerRename returns a new *AlterTriggerRename.
func NewAlterTriggerRename(schema string, table string, trigger string, newName string) *AlterTriggerRename {
	return &AlterTriggerRename{
		schema:  schema,
		table:   table,
		trigger: trigger,
		newName: newName,
	}
}

// Children implements sql.ExecSourceRel.
func (a *AlterTriggerRename) Children() []sql.Node {
	return nil
}

// IsReadOnly implements sql.ExecSourceRel.
func (a *AlterTriggerRename) IsReadOnly() bool {
	return false
}

// Resolved implements sql.ExecSourceRel.
func (a *AlterTriggerRename) Resolved() bool {
	return true
}

// RowIter implements sql.ExecSourceRel.
func (a *AlterTriggerRename) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	schema, err := core.GetSchemaName(ctx, nil, a.schema)
	if err != nil {
		return nil, err
	}
	relationType, err := core.GetRelationType(ctx, schema, a.table)
	if err != nil {
		return nil, err
	}
	if relationType == core.RelationType_DoesNotExist {
		return nil, errors.Errorf(`relation "%s" does not exist`, a.table)
	}
	if err = checkTableOwnership(ctx, doltdb.TableName{Name: a.table, Schema: schema}); err != nil {
		return nil, errors.Wrap(err, "permission denied")
	}

	collection, err := core.GetTriggersCollectionFromContext(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return nil, err
	}
	oldID := id.NewTrigger(schema, a.table, a.trigger)
	newID := id.NewTrigger(schema, a.table, a.newName)
	if err = collection.RenameRootObject(ctx, oldID.AsId(), newID.AsId()); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

// Schema implements sql.ExecSourceRel.
func (a *AlterTriggerRename) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements sql.ExecSourceRel.
func (a *AlterTriggerRename) String() string {
	return "ALTER TRIGGER RENAME"
}

// WithChildren implements sql.ExecSourceRel.
func (a *AlterTriggerRename) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(a, children...)
}

// WithResolvedChildren implements vitess.Injectable.
func (a *AlterTriggerRename) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return a, nil
}
