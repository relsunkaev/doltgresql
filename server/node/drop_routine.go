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
)

// DropRoutine implements DROP ROUTINE.
type DropRoutine struct {
	RoutinesWithArgs []*RoutineWithParams
	IfExists         bool
	Cascade          bool
}

var _ sql.ExecSourceRel = (*DropRoutine)(nil)
var _ vitess.Injectable = (*DropRoutine)(nil)

// NewDropRoutine returns a new *DropRoutine.
func NewDropRoutine(ifExists bool, routinesWithArgs []*RoutineWithParams, cascade bool) *DropRoutine {
	return &DropRoutine{
		IfExists:         ifExists,
		RoutinesWithArgs: routinesWithArgs,
		Cascade:          cascade,
	}
}

// Resolved implements the interface sql.ExecSourceRel.
func (d *DropRoutine) Resolved() bool {
	return true
}

// String implements the interface sql.ExecSourceRel.
func (d *DropRoutine) String() string {
	return "DROP ROUTINE"
}

// Schema implements the interface sql.ExecSourceRel.
func (d *DropRoutine) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// Children implements the interface sql.ExecSourceRel.
func (d *DropRoutine) Children() []sql.Node {
	return nil
}

// WithChildren implements the interface sql.ExecSourceRel.
func (d *DropRoutine) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(d, children...)
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (d *DropRoutine) IsReadOnly() bool {
	return false
}

// RowIter implements the interface sql.ExecSourceRel.
func (d *DropRoutine) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	funcColl, err := core.GetFunctionsCollectionFromContext(ctx)
	if err != nil {
		return nil, err
	}
	procColl, err := core.GetProceduresCollectionFromContext(ctx)
	if err != nil {
		return nil, err
	}

	for _, routineWithArgs := range d.RoutinesWithArgs {
		funcID, funcErr := resolveFunctionID(ctx, funcColl, routineWithArgs)
		procID, procErr := resolveProcedureID(ctx, procColl, routineWithArgs)
		if funcErr != nil && procErr != nil {
			return nil, funcErr
		}

		funcExists := funcErr == nil && funcColl.HasFunction(ctx, funcID)
		procExists := procErr == nil && procColl.HasProcedure(ctx, procID)
		if funcErr != nil && !procExists {
			return nil, funcErr
		}
		if procErr != nil && !funcExists {
			return nil, procErr
		}
		if funcExists && procExists {
			return nil, errors.Errorf(`routine name "%s" is not unique`, routineWithArgs.RoutineName)
		}
		if funcExists {
			if err = dropFunction(ctx, funcColl, routineWithArgs, d.IfExists); err != nil {
				return nil, err
			}
			continue
		}
		if procExists {
			if err = dropProcedure(ctx, procColl, routineWithArgs, d.IfExists); err != nil {
				return nil, err
			}
			continue
		}
		if !d.IfExists {
			return nil, errors.Errorf(`routine "%s" does not exist`, routineWithArgs.RoutineName)
		}
	}

	return sql.RowsToRowIter(), nil
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (d *DropRoutine) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return d, nil
}
