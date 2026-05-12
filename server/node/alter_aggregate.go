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
	corefunctions "github.com/dolthub/doltgresql/core/functions"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/auth"
)

// AlterAggregate implements ALTER AGGREGATE.
type AlterAggregate struct {
	Routine      *RoutineWithParams
	Rename       string
	TargetSchema string
	Owner        string
}

var _ sql.ExecSourceRel = (*AlterAggregate)(nil)
var _ vitess.Injectable = (*AlterAggregate)(nil)

// NewAlterAggregateRename returns a new *AlterAggregate for ALTER AGGREGATE ... RENAME TO.
func NewAlterAggregateRename(routine *RoutineWithParams, newName string) *AlterAggregate {
	return &AlterAggregate{
		Routine: routine,
		Rename:  newName,
	}
}

// NewAlterAggregateSetSchema returns a new *AlterAggregate for ALTER AGGREGATE ... SET SCHEMA.
func NewAlterAggregateSetSchema(routine *RoutineWithParams, schema string) *AlterAggregate {
	return &AlterAggregate{
		Routine:      routine,
		TargetSchema: schema,
	}
}

// NewAlterAggregateOwner returns a new *AlterAggregate for ALTER AGGREGATE ... OWNER TO.
func NewAlterAggregateOwner(routine *RoutineWithParams, owner string) *AlterAggregate {
	return &AlterAggregate{
		Routine: routine,
		Owner:   owner,
	}
}

// Children implements the interface sql.ExecSourceRel.
func (a *AlterAggregate) Children() []sql.Node {
	return nil
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (a *AlterAggregate) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (a *AlterAggregate) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (a *AlterAggregate) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	funcColl, err := core.GetFunctionsCollectionFromContext(ctx)
	if err != nil {
		return nil, err
	}
	aggregateID, err := resolveAggregateID(ctx, funcColl, a.Routine)
	if err != nil {
		return nil, err
	}
	aggregate, err := funcColl.GetFunction(ctx, aggregateID)
	if err != nil {
		return nil, err
	}
	if err = checkAggregateOwnership(ctx, aggregate); err != nil {
		return nil, err
	}
	newAggregateID := aggregateID
	if a.Rename != "" {
		newAggregateID = id.NewFunction(newAggregateID.SchemaName(), a.Rename, newAggregateID.Parameters()...)
	}
	if a.TargetSchema != "" {
		schema, err := core.GetSchemaName(ctx, nil, a.TargetSchema)
		if err != nil {
			return nil, err
		}
		newAggregateID = id.NewFunction(schema, newAggregateID.FunctionName(), newAggregateID.Parameters()...)
	}
	if a.Owner != "" {
		var exists bool
		auth.LockRead(func() {
			exists = auth.RoleExists(a.Owner)
		})
		if !exists {
			return nil, errors.Errorf(`role "%s" does not exist`, a.Owner)
		}
		aggregate.Owner = a.Owner
	}
	if newAggregateID != aggregateID {
		if funcColl.HasFunction(ctx, newAggregateID) {
			return nil, errors.Errorf(`aggregate "%s" already exists with same argument types`, newAggregateID.FunctionName())
		}
		aggregate.ID = newAggregateID
	}
	if err = funcColl.DropFunction(ctx, aggregateID); err != nil {
		return nil, err
	}
	if err = funcColl.AddFunction(ctx, aggregate); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

func checkAggregateOwnership(ctx *sql.Context, aggregate corefunctions.Function) error {
	if len(aggregate.Owner) == 0 || aggregate.Owner == ctx.Client().User {
		return nil
	}
	var userRole auth.Role
	auth.LockRead(func() {
		userRole = auth.GetRole(ctx.Client().User)
	})
	if userRole.IsValid() && userRole.IsSuperUser {
		return nil
	}
	return errors.Errorf("must be owner of aggregate %s", aggregate.ID.FunctionName())
}

// Schema implements the interface sql.ExecSourceRel.
func (a *AlterAggregate) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (a *AlterAggregate) String() string {
	return "ALTER AGGREGATE"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (a *AlterAggregate) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(a, children...)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (a *AlterAggregate) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return a, nil
}
