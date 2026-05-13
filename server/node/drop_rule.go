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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/auth"
)

// DropRule implements DROP RULE for rules created through the trigger rewrite.
type DropRule struct {
	TableSchema         string
	TableName           string
	Name                string
	BackingFunctionName string
	IfExists            bool
}

var _ sql.ExecSourceRel = (*DropRule)(nil)
var _ vitess.Injectable = (*DropRule)(nil)

// NewDropRule returns a new *DropRule.
func NewDropRule(tableSchema string, tableName string, name string, backingFunctionName string, ifExists bool) *DropRule {
	return &DropRule{
		TableSchema:         tableSchema,
		TableName:           tableName,
		Name:                name,
		BackingFunctionName: backingFunctionName,
		IfExists:            ifExists,
	}
}

// Children implements the interface sql.ExecSourceRel.
func (d *DropRule) Children() []sql.Node { return nil }

// IsReadOnly implements the interface sql.ExecSourceRel.
func (d *DropRule) IsReadOnly() bool { return false }

// Resolved implements the interface sql.ExecSourceRel.
func (d *DropRule) Resolved() bool { return true }

// RowIter implements the interface sql.ExecSourceRel.
func (d *DropRule) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	schema, err := core.GetSchemaName(ctx, nil, d.TableSchema)
	if err != nil {
		return nil, err
	}
	triggerID := id.NewTrigger(schema, d.TableName, d.Name)
	triggerCollection, err := core.GetTriggersCollectionFromContext(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return nil, err
	}
	trigger, err := triggerCollection.GetTrigger(ctx, triggerID)
	if err != nil {
		return nil, err
	}
	if !trigger.ID.IsValid() || !isRuleTriggerFunction(trigger.Function, d.BackingFunctionName) {
		if d.IfExists {
			return sql.RowsToRowIter(), nil
		}
		return nil, errors.Errorf(`rule "%s" for relation "%s" does not exist`, d.Name, d.TableName)
	}
	if err = triggerCollection.DropTrigger(ctx, triggerID); err != nil {
		return nil, err
	}
	if err = dropRuleBackingFunction(ctx, trigger.Function); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

// Schema implements the interface sql.ExecSourceRel.
func (d *DropRule) Schema(ctx *sql.Context) sql.Schema { return nil }

// String implements the interface sql.ExecSourceRel.
func (d *DropRule) String() string { return "DROP RULE" }

// WithChildren implements the interface sql.ExecSourceRel.
func (d *DropRule) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(d, children...)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (d *DropRule) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return d, nil
}

func isRuleTriggerFunction(functionID id.Function, backingFunctionName string) bool {
	return functionID.IsValid() && functionID.FunctionName() == backingFunctionName
}

func dropRuleBackingFunction(ctx *sql.Context, functionID id.Function) error {
	funcCollection, err := core.GetFunctionsCollectionFromContext(ctx)
	if err != nil {
		return err
	}
	if !funcCollection.HasFunction(ctx, functionID) {
		return nil
	}
	if err = funcCollection.DropFunction(ctx, functionID); err != nil {
		return err
	}
	clearFunctionComment(functionID)
	var persistErr error
	auth.LockWrite(func() {
		auth.RemoveAllRoutinePrivileges(functionID.SchemaName(), functionID.FunctionName())
		persistErr = auth.PersistChanges()
	})
	return persistErr
}
