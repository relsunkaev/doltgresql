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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/core/triggers"
)

// AlterTableTrigger handles ALTER TABLE ... ENABLE/DISABLE TRIGGER.
type AlterTableTrigger struct {
	ifExists bool
	schema   string
	table    string
	trigger  string
	enabled  string
}

var _ sql.ExecSourceRel = (*AlterTableTrigger)(nil)
var _ vitess.Injectable = (*AlterTableTrigger)(nil)

// NewAlterTableTrigger returns a new *AlterTableTrigger.
func NewAlterTableTrigger(ifExists bool, schema string, table string, trigger string, enabled string) *AlterTableTrigger {
	return &AlterTableTrigger{
		ifExists: ifExists,
		schema:   schema,
		table:    table,
		trigger:  trigger,
		enabled:  enabled,
	}
}

func (a *AlterTableTrigger) Children() []sql.Node { return nil }

func (a *AlterTableTrigger) IsReadOnly() bool { return false }

func (a *AlterTableTrigger) Resolved() bool { return true }

func (a *AlterTableTrigger) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	schema, err := core.GetSchemaName(ctx, nil, a.schema)
	if err != nil {
		return nil, err
	}
	relationType, err := core.GetRelationType(ctx, schema, a.table)
	if err != nil {
		return nil, err
	}
	if relationType == core.RelationType_DoesNotExist {
		if a.ifExists {
			return sql.RowsToRowIter(), nil
		}
		return nil, errors.Errorf(`relation "%s" does not exist`, a.table)
	}
	if err = checkTableOwnership(ctx, doltdb.TableName{Name: a.table, Schema: schema}); err != nil {
		return nil, errors.Wrap(err, "permission denied")
	}
	collection, err := core.GetTriggersCollectionFromContext(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return nil, err
	}
	triggerIDs, err := a.triggerIDs(ctx, collection, schema)
	if err != nil {
		return nil, err
	}
	for _, triggerID := range triggerIDs {
		trigger, err := collection.GetTrigger(ctx, triggerID)
		if err != nil {
			return nil, err
		}
		trigger.Enabled = a.enabled
		if err = collection.UpdateTrigger(ctx, trigger); err != nil {
			return nil, err
		}
	}
	return sql.RowsToRowIter(), nil
}

func (a *AlterTableTrigger) triggerIDs(ctx *sql.Context, collection *triggers.Collection, schema string) ([]id.Trigger, error) {
	tableID := id.NewTable(schema, a.table)
	switch strings.ToLower(a.trigger) {
	case "all", "user":
		triggerIDs := collection.GetTriggerIDsForTable(ctx, tableID)
		return append([]id.Trigger(nil), triggerIDs...), nil
	default:
		triggerID := id.NewTrigger(schema, a.table, a.trigger)
		if !collection.HasTrigger(ctx, triggerID) {
			return nil, errors.Errorf(`trigger "%s" for table "%s" does not exist`, a.trigger, a.table)
		}
		return []id.Trigger{triggerID}, nil
	}
}

func (a *AlterTableTrigger) Schema(ctx *sql.Context) sql.Schema { return nil }

func (a *AlterTableTrigger) String() string { return "ALTER TABLE TRIGGER" }

func (a *AlterTableTrigger) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(a, children...)
}

func (a *AlterTableTrigger) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return a, nil
}
