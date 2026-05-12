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
	"github.com/dolthub/doltgresql/server/types"
)

// AlterTypeRename handles ALTER TYPE/DOMAIN ... RENAME TO.
type AlterTypeRename struct {
	DatabaseName string
	SchemaName   string
	TypeName     string
	NewName      string
	DomainOnly   bool
}

var _ sql.ExecSourceRel = (*AlterTypeRename)(nil)
var _ vitess.Injectable = (*AlterTypeRename)(nil)

// NewAlterTypeRename returns a new *AlterTypeRename.
func NewAlterTypeRename(databaseName, schemaName, typeName, newName string, domainOnly bool) *AlterTypeRename {
	return &AlterTypeRename{
		DatabaseName: databaseName,
		SchemaName:   schemaName,
		TypeName:     typeName,
		NewName:      newName,
		DomainOnly:   domainOnly,
	}
}

// Children implements the interface sql.ExecSourceRel.
func (a *AlterTypeRename) Children() []sql.Node { return nil }

// IsReadOnly implements the interface sql.ExecSourceRel.
func (a *AlterTypeRename) IsReadOnly() bool { return false }

// Resolved implements the interface sql.ExecSourceRel.
func (a *AlterTypeRename) Resolved() bool { return true }

// RowIter implements the interface sql.ExecSourceRel.
func (a *AlterTypeRename) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	schema, err := core.GetSchemaName(ctx, nil, a.SchemaName)
	if err != nil {
		return nil, err
	}
	collection, err := core.GetTypesCollectionFromContextForDatabase(ctx, a.DatabaseName)
	if err != nil {
		return nil, err
	}
	oldTypeID := id.NewType(schema, a.TypeName)
	newTypeID := id.NewType(schema, a.NewName)
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
	if collection.HasType(ctx, newTypeID) {
		return nil, types.ErrTypeAlreadyExists.New(a.NewName)
	}
	if err = checkTypeOwnership(ctx, typ); err != nil {
		return nil, err
	}

	updatedType := *typ
	updatedType.ID = newTypeID
	if typ.Array.IsValid() {
		updatedType.Array = id.NewType(schema, "_"+a.NewName)
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
	return sql.RowsToRowIter(), nil
}

func renameEnumLabelParents(labels map[string]types.EnumLabel, parent id.Type) map[string]types.EnumLabel {
	if len(labels) == 0 {
		return labels
	}
	renamed := make(map[string]types.EnumLabel, len(labels))
	for label, enumLabel := range labels {
		enumLabel.ID = id.NewEnumLabel(parent, label)
		renamed[label] = enumLabel
	}
	return renamed
}

// Schema implements the interface sql.ExecSourceRel.
func (a *AlterTypeRename) Schema(ctx *sql.Context) sql.Schema { return nil }

// String implements the interface sql.ExecSourceRel.
func (a *AlterTypeRename) String() string { return "ALTER TYPE RENAME" }

// WithChildren implements the interface sql.ExecSourceRel.
func (a *AlterTypeRename) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(a, children...)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (a *AlterTypeRename) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return a, nil
}
