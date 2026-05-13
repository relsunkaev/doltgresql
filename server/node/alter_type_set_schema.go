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
	"github.com/dolthub/doltgresql/server/types"
)

// AlterTypeSetSchema handles ALTER TYPE/DOMAIN ... SET SCHEMA.
type AlterTypeSetSchema struct {
	DatabaseName string
	SchemaName   string
	TypeName     string
	TargetSchema string
	DomainOnly   bool
}

var _ sql.ExecSourceRel = (*AlterTypeSetSchema)(nil)
var _ vitess.Injectable = (*AlterTypeSetSchema)(nil)

// NewAlterTypeSetSchema returns a new *AlterTypeSetSchema.
func NewAlterTypeSetSchema(databaseName, schemaName, typeName, targetSchema string, domainOnly bool) *AlterTypeSetSchema {
	return &AlterTypeSetSchema{
		DatabaseName: databaseName,
		SchemaName:   schemaName,
		TypeName:     typeName,
		TargetSchema: targetSchema,
		DomainOnly:   domainOnly,
	}
}

func (a *AlterTypeSetSchema) Children() []sql.Node { return nil }

func (a *AlterTypeSetSchema) IsReadOnly() bool { return false }

func (a *AlterTypeSetSchema) Resolved() bool { return true }

func (a *AlterTypeSetSchema) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	schema, err := core.GetSchemaName(ctx, nil, a.SchemaName)
	if err != nil {
		return nil, err
	}
	targetSchema, err := core.GetSchemaName(ctx, nil, a.TargetSchema)
	if err != nil {
		return nil, err
	}
	collection, err := core.GetTypesCollectionFromContextForDatabase(ctx, a.DatabaseName)
	if err != nil {
		return nil, err
	}

	oldTypeID := id.NewType(schema, a.TypeName)
	newTypeID := id.NewType(targetSchema, a.TypeName)
	typ, err := collection.GetType(ctx, oldTypeID)
	if err != nil {
		return nil, err
	}
	if typ == nil {
		return nil, types.ErrTypeDoesNotExist.New(a.TypeName)
	}
	if a.DomainOnly && typ.TypType != types.TypeType_Domain {
		return nil, errors.Errorf(`type "%s" is not a domain`, a.TypeName)
	}
	if _, ok := types.IDToBuiltInDoltgresType[typ.ID]; ok {
		return nil, errors.Errorf(`cannot alter type "%s" because it is a system type`, a.TypeName)
	}
	if err = checkTypeOwnership(ctx, typ); err != nil {
		return nil, err
	}
	if collection.HasType(ctx, newTypeID) {
		return nil, types.ErrTypeAlreadyExists.New(a.TypeName)
	}

	updatedType := *typ
	updatedType.ID = newTypeID
	if typ.Array.IsValid() {
		updatedType.Array = id.NewType(targetSchema, "_"+a.TypeName)
		if collection.HasType(ctx, updatedType.Array) {
			return nil, types.ErrTypeAlreadyExists.New(updatedType.Array.TypeName())
		}
	}
	updatedType.EnumLabels = renameEnumLabelParents(typ.EnumLabels, newTypeID)

	dropIDs := []id.Type{oldTypeID}
	if typ.Array.IsValid() {
		dropIDs = append(dropIDs, typ.Array)
	}
	if err = collection.DropType(ctx, dropIDs...); err != nil {
		return nil, err
	}
	if err = collection.CreateType(ctx, &updatedType); err != nil {
		return nil, err
	}
	if updatedType.Array.IsValid() {
		if err = collection.CreateType(ctx, types.CreateArrayTypeFromBaseType(&updatedType)); err != nil {
			return nil, err
		}
	}
	if err = core.MarkTypesCollectionDirty(ctx, a.DatabaseName); err != nil {
		return nil, err
	}

	rename := &AlterTypeRename{DatabaseName: a.DatabaseName}
	if err = rename.renameDependentTableColumns(ctx, oldTypeID, typ.Array, &updatedType); err != nil {
		return nil, err
	}
	if err = rename.renameDependentFunctions(ctx, oldTypeID, typ.Array, updatedType.ID, updatedType.Array); err != nil {
		return nil, err
	}
	if err = rename.renameDependentViews(ctx, oldTypeID, typ.Array, updatedType.ID, updatedType.Array); err != nil {
		return nil, err
	}
	auth.LockWrite(func() {
		auth.RenameTypePrivileges(schema, a.TypeName, targetSchema, a.TypeName)
		if typ.Array.IsValid() {
			auth.RenameTypePrivileges(schema, typ.Array.TypeName(), targetSchema, updatedType.Array.TypeName())
		}
		err = auth.PersistChanges()
	})
	if err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

func (a *AlterTypeSetSchema) Schema(ctx *sql.Context) sql.Schema { return nil }

func (a *AlterTypeSetSchema) String() string { return "ALTER TYPE SET SCHEMA" }

func (a *AlterTypeSetSchema) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(a, children...)
}

func (a *AlterTypeSetSchema) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return a, nil
}
