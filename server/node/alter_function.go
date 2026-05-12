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
	"slices"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/auth"
)

// AlterFunctionOptions implements ALTER FUNCTION option changes.
type AlterFunctionOptions struct {
	Routine      *RoutineWithParams
	Strict       *bool
	Metadata     AlterFunctionOptionMetadata
	Rename       string
	TargetSchema string
	Owner        string
	Extension    string
	RemoveDep    bool
}

var _ sql.ExecSourceRel = (*AlterFunctionOptions)(nil)
var _ vitess.Injectable = (*AlterFunctionOptions)(nil)

// AlterFunctionOptionMetadata represents optional metadata updates from ALTER FUNCTION options.
type AlterFunctionOptionMetadata struct {
	SecurityDefiner *bool
	LeakProof       *bool
	Volatility      *string
	Parallel        *string
	Cost            *float32
	Rows            *float32
}

// NewAlterFunctionOptions returns a new *AlterFunctionOptions.
func NewAlterFunctionOptions(routine *RoutineWithParams, strict *bool, metadata AlterFunctionOptionMetadata) *AlterFunctionOptions {
	return &AlterFunctionOptions{
		Routine:  routine,
		Strict:   strict,
		Metadata: metadata,
	}
}

// NewAlterFunctionRename returns a new *AlterFunctionOptions for ALTER FUNCTION ... RENAME TO.
func NewAlterFunctionRename(routine *RoutineWithParams, newName string) *AlterFunctionOptions {
	return &AlterFunctionOptions{
		Routine: routine,
		Rename:  newName,
	}
}

// NewAlterFunctionSetSchema returns a new *AlterFunctionOptions for ALTER FUNCTION ... SET SCHEMA.
func NewAlterFunctionSetSchema(routine *RoutineWithParams, schema string) *AlterFunctionOptions {
	return &AlterFunctionOptions{
		Routine:      routine,
		TargetSchema: schema,
	}
}

// NewAlterFunctionOwner returns a new *AlterFunctionOptions for ALTER FUNCTION ... OWNER TO.
func NewAlterFunctionOwner(routine *RoutineWithParams, owner string) *AlterFunctionOptions {
	return &AlterFunctionOptions{
		Routine: routine,
		Owner:   owner,
	}
}

// NewAlterFunctionDependsOnExtension returns a new *AlterFunctionOptions for ALTER FUNCTION ... DEPENDS ON EXTENSION.
func NewAlterFunctionDependsOnExtension(routine *RoutineWithParams, extension string, remove bool) *AlterFunctionOptions {
	return &AlterFunctionOptions{
		Routine:   routine,
		Extension: extension,
		RemoveDep: remove,
	}
}

// Children implements the interface sql.ExecSourceRel.
func (a *AlterFunctionOptions) Children() []sql.Node {
	return nil
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (a *AlterFunctionOptions) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (a *AlterFunctionOptions) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (a *AlterFunctionOptions) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	funcColl, err := core.GetFunctionsCollectionFromContext(ctx)
	if err != nil {
		return nil, err
	}
	funcID, err := resolveFunctionID(ctx, funcColl, a.Routine)
	if err != nil {
		return nil, err
	}
	function, err := funcColl.GetFunction(ctx, funcID)
	if err != nil {
		return nil, err
	}
	if !function.ID.IsValid() {
		return nil, errors.Errorf(`function %s does not exist`, a.Routine.RoutineName)
	}
	if a.Strict != nil {
		function.Strict = *a.Strict
	}
	if a.Metadata.SecurityDefiner != nil {
		function.SecurityDefiner = *a.Metadata.SecurityDefiner
	}
	if a.Metadata.LeakProof != nil {
		if *a.Metadata.LeakProof {
			var userRole auth.Role
			auth.LockRead(func() {
				userRole = auth.GetRole(ctx.Client().User)
			})
			if !userRole.IsValid() || !userRole.IsSuperUser {
				return nil, errors.Errorf("permission denied")
			}
		}
		function.LeakProof = *a.Metadata.LeakProof
	}
	if a.Metadata.Volatility != nil {
		function.Volatility = *a.Metadata.Volatility
	}
	if a.Metadata.Parallel != nil {
		function.Parallel = *a.Metadata.Parallel
	}
	if a.Metadata.Cost != nil {
		function.Cost = *a.Metadata.Cost
	}
	if a.Metadata.Rows != nil {
		if !function.SetOf {
			return nil, errors.Errorf("ROWS is not applicable when function does not return a set")
		}
		function.Rows = *a.Metadata.Rows
	}
	newFuncID := funcID
	if a.Rename != "" {
		newFuncID = id.NewFunction(newFuncID.SchemaName(), a.Rename, newFuncID.Parameters()...)
	}
	if a.TargetSchema != "" {
		schema, err := core.GetSchemaName(ctx, nil, a.TargetSchema)
		if err != nil {
			return nil, err
		}
		newFuncID = id.NewFunction(schema, newFuncID.FunctionName(), newFuncID.Parameters()...)
	}
	if a.Owner != "" {
		var exists bool
		auth.LockRead(func() {
			exists = auth.RoleExists(a.Owner)
		})
		if !exists {
			return nil, errors.Errorf(`role "%s" does not exist`, a.Owner)
		}
		function.Owner = a.Owner
	}
	if a.Extension != "" {
		extCollection, err := core.GetExtensionsCollectionFromContext(ctx, "")
		if err != nil {
			return nil, err
		}
		if !extCollection.HasLoadedExtension(ctx, id.NewExtension(a.Extension)) {
			return nil, errors.Errorf(`extension "%s" does not exist`, a.Extension)
		}
		if a.RemoveDep {
			function.ExtensionDeps = slices.DeleteFunc(function.ExtensionDeps, func(dep string) bool {
				return dep == a.Extension
			})
		} else if !slices.Contains(function.ExtensionDeps, a.Extension) {
			function.ExtensionDeps = append(function.ExtensionDeps, a.Extension)
			slices.Sort(function.ExtensionDeps)
		}
	}
	if newFuncID != funcID {
		if funcColl.HasFunction(ctx, newFuncID) {
			return nil, errors.Errorf(`function "%s" already exists with same argument types`, newFuncID.FunctionName())
		}
		function.ID = newFuncID
	}
	if err = funcColl.DropFunction(ctx, funcID); err != nil {
		return nil, err
	}
	if err = funcColl.AddFunction(ctx, function); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

// Schema implements the interface sql.ExecSourceRel.
func (a *AlterFunctionOptions) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (a *AlterFunctionOptions) String() string {
	return "ALTER FUNCTION"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (a *AlterFunctionOptions) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(a, children...)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (a *AlterFunctionOptions) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return a, nil
}
