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
	"github.com/dolthub/doltgresql/core/procedures"
	"github.com/dolthub/doltgresql/server/auth"
)

// AlterProcedure implements ALTER PROCEDURE metadata changes.
type AlterProcedure struct {
	Routine      *RoutineWithParams
	Metadata     AlterProcedureOptionMetadata
	Rename       string
	TargetSchema string
	Owner        string
}

var _ sql.ExecSourceRel = (*AlterProcedure)(nil)
var _ vitess.Injectable = (*AlterProcedure)(nil)

// AlterProcedureOptionMetadata represents optional metadata updates from ALTER PROCEDURE options.
type AlterProcedureOptionMetadata struct {
	SetConfig      map[string]string
	ResetConfig    []string
	ResetAllConfig bool
}

// NewAlterProcedureOptions returns a new *AlterProcedure.
func NewAlterProcedureOptions(routine *RoutineWithParams, metadata AlterProcedureOptionMetadata) *AlterProcedure {
	return &AlterProcedure{
		Routine:  routine,
		Metadata: metadata,
	}
}

// NewAlterProcedureRename returns a new *AlterProcedure for ALTER PROCEDURE ... RENAME TO.
func NewAlterProcedureRename(routine *RoutineWithParams, newName string) *AlterProcedure {
	return &AlterProcedure{
		Routine: routine,
		Rename:  newName,
	}
}

// NewAlterProcedureSetSchema returns a new *AlterProcedure for ALTER PROCEDURE ... SET SCHEMA.
func NewAlterProcedureSetSchema(routine *RoutineWithParams, schema string) *AlterProcedure {
	return &AlterProcedure{
		Routine:      routine,
		TargetSchema: schema,
	}
}

// NewAlterProcedureOwner returns a new *AlterProcedure for ALTER PROCEDURE ... OWNER TO.
func NewAlterProcedureOwner(routine *RoutineWithParams, owner string) *AlterProcedure {
	return &AlterProcedure{
		Routine: routine,
		Owner:   owner,
	}
}

// Children implements the interface sql.ExecSourceRel.
func (a *AlterProcedure) Children() []sql.Node {
	return nil
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (a *AlterProcedure) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (a *AlterProcedure) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (a *AlterProcedure) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	procColl, err := core.GetProceduresCollectionFromContext(ctx)
	if err != nil {
		return nil, err
	}
	procID, err := resolveProcedureID(ctx, procColl, a.Routine)
	if err != nil {
		return nil, err
	}
	procedure, err := procColl.GetProcedure(ctx, procID)
	if err != nil {
		return nil, err
	}
	if !procedure.ID.IsValid() {
		return nil, errors.Errorf(`procedure %s does not exist`, a.Routine.RoutineName)
	}
	if err = checkAlterProcedureOwnership(ctx, procedure); err != nil {
		return nil, err
	}

	procedure.SetConfig = applyRoutineConfigOptions(procedure.SetConfig, a.Metadata.SetConfig, a.Metadata.ResetConfig, a.Metadata.ResetAllConfig)

	newProcID := procID
	if a.Rename != "" {
		newProcID = id.NewProcedure(newProcID.SchemaName(), a.Rename, newProcID.Parameters()...)
	}
	if a.TargetSchema != "" {
		schema, err := core.GetSchemaName(ctx, nil, a.TargetSchema)
		if err != nil {
			return nil, err
		}
		newProcID = id.NewProcedure(schema, newProcID.ProcedureName(), newProcID.Parameters()...)
	}
	if a.Owner != "" {
		var exists bool
		auth.LockRead(func() {
			exists = auth.RoleExists(a.Owner)
		})
		if !exists {
			return nil, errors.Errorf(`role "%s" does not exist`, a.Owner)
		}
		procedure.Owner = a.Owner
	}
	if newProcID != procID {
		if procColl.HasProcedure(ctx, newProcID) {
			return nil, errors.Errorf(`procedure "%s" already exists with same argument types`, newProcID.ProcedureName())
		}
		procedure.ID = newProcID
	}
	if err = procColl.DropProcedure(ctx, procID); err != nil {
		return nil, err
	}
	if err = procColl.AddProcedure(ctx, procedure); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

func checkAlterProcedureOwnership(ctx *sql.Context, proc procedures.Procedure) error {
	if len(proc.Owner) == 0 || proc.Owner == ctx.Client().User {
		return nil
	}
	var userRole auth.Role
	auth.LockRead(func() {
		userRole = auth.GetRole(ctx.Client().User)
	})
	if userRole.IsValid() && userRole.IsSuperUser {
		return nil
	}
	return errors.Errorf("must be owner of procedure %s", proc.ID.ProcedureName())
}

func resolveProcedureID(ctx *sql.Context, procColl *procedures.Collection, routine *RoutineWithParams) (id.Procedure, error) {
	schema, err := core.GetSchemaName(ctx, nil, routine.SchemaName)
	if err != nil {
		return id.NullProcedure, err
	}
	procID := id.NewProcedure(schema, routine.RoutineName)
	if len(routine.Args) == 0 {
		procs, err := procColl.GetProcedureOverloads(ctx, procID)
		if err != nil {
			return id.NullProcedure, err
		}
		if len(procs) == 1 {
			procID = procs[0].ID
		} else if len(procs) > 1 && !procColl.HasProcedure(ctx, procID) {
			return id.NullProcedure, errors.Errorf(`procedure name "%s" is not unique`, routine.RoutineName)
		}
	} else {
		argTypes := make([]id.Type, len(routine.Args))
		for i, arg := range routine.Args {
			argTypes[i] = arg.Type.ID
		}
		procID = id.NewProcedure(schema, routine.RoutineName, argTypes...)
	}
	return procID, nil
}

// Schema implements the interface sql.ExecSourceRel.
func (a *AlterProcedure) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (a *AlterProcedure) String() string {
	return "ALTER PROCEDURE"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (a *AlterProcedure) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(a, children...)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (a *AlterProcedure) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return a, nil
}
