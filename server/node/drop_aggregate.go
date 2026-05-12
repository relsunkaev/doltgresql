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
	"github.com/dolthub/doltgresql/core/functions"
	"github.com/dolthub/doltgresql/core/id"
)

// DropAggregate implements DROP AGGREGATE.
type DropAggregate struct {
	RoutinesWithArgs []*RoutineWithParams
	IfExists         bool
	Cascade          bool
}

var _ sql.ExecSourceRel = (*DropAggregate)(nil)
var _ vitess.Injectable = (*DropAggregate)(nil)

// NewDropAggregate returns a new *DropAggregate.
func NewDropAggregate(ifExists bool, routinesWithArgs []*RoutineWithParams, cascade bool) *DropAggregate {
	return &DropAggregate{
		IfExists:         ifExists,
		RoutinesWithArgs: routinesWithArgs,
		Cascade:          cascade,
	}
}

// Children implements the interface sql.ExecSourceRel.
func (d *DropAggregate) Children() []sql.Node {
	return nil
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (d *DropAggregate) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (d *DropAggregate) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (d *DropAggregate) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	funcColl, err := core.GetFunctionsCollectionFromContext(ctx)
	if err != nil {
		return nil, err
	}
	for _, routine := range d.RoutinesWithArgs {
		aggregateID, err := resolveAggregateID(ctx, funcColl, routine)
		if err != nil {
			if d.IfExists && errors.Is(err, errAggregateDoesNotExist) {
				continue
			}
			return nil, err
		}
		if err = funcColl.DropFunction(ctx, aggregateID); err != nil {
			return nil, err
		}
	}
	return sql.RowsToRowIter(), nil
}

// Schema implements the interface sql.ExecSourceRel.
func (d *DropAggregate) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (d *DropAggregate) String() string {
	return "DROP AGGREGATE"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (d *DropAggregate) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(d, children...)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (d *DropAggregate) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return d, nil
}

var errAggregateDoesNotExist = errors.New("aggregate does not exist")

func resolveAggregateID(ctx *sql.Context, funcColl *functions.Collection, routine *RoutineWithParams) (id.Function, error) {
	funcID, err := resolveFunctionID(ctx, funcColl, routine)
	if err != nil {
		return id.NullFunction, err
	}
	function, err := funcColl.GetFunction(ctx, funcID)
	if err != nil {
		return id.NullFunction, err
	}
	if !function.ID.IsValid() || !function.Aggregate {
		return id.NullFunction, errors.Wrapf(errAggregateDoesNotExist, `aggregate "%s" does not exist`, routine.RoutineName)
	}
	return funcID, nil
}
