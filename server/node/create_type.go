// Copyright 2024 Dolthub, Inc.
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

// CreateType handles the CREATE TYPE statement.
type CreateType struct {
	DatabaseName string
	SchemaName   string
	Name         string

	// composite type
	AsTypes []CompositeAsType

	// enum type
	Labels []string

	typType types.TypeType
}

// CompositeAsType represents an attribute name
// and data type for a composite type.
type CompositeAsType struct {
	AttrName  string
	Typ       *types.DoltgresType
	Collation string
}

var _ sql.ExecSourceRel = (*CreateType)(nil)
var _ vitess.Injectable = (*CreateType)(nil)

// NewCreateCompositeType creates CreateType node for creating COMPOSITE type.
func NewCreateCompositeType(database, schema, name string, typs []CompositeAsType) *CreateType {
	return &CreateType{DatabaseName: database, SchemaName: schema, Name: name, AsTypes: typs, typType: types.TypeType_Composite}
}

// NewCreateEnumType creates CreateType node for creating ENUM type.
func NewCreateEnumType(database, schema, name string, labels []string) *CreateType {
	return &CreateType{DatabaseName: database, SchemaName: schema, Name: name, Labels: labels, typType: types.TypeType_Enum}
}

// NewCreateShellType creates CreateType node for creating
// a placeholder for a type to be defined later.
func NewCreateShellType(database, schema, name string) *CreateType {
	return &CreateType{DatabaseName: database, SchemaName: schema, Name: name, typType: types.TypeType_Pseudo}
}

// Children implements the interface sql.ExecSourceRel.
func (c *CreateType) Children() []sql.Node {
	return nil
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (c *CreateType) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (c *CreateType) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (c *CreateType) RowIter(ctx *sql.Context, r sql.Row) (sql.RowIter, error) {
	schema, err := core.GetSchemaName(ctx, nil, c.SchemaName)
	if err != nil {
		return nil, err
	}
	if err = checkSchemaCreatePrivilege(ctx, schema); err != nil {
		return nil, err
	}
	collection, err := core.GetTypesCollectionFromContextForDatabase(ctx, c.DatabaseName)
	if err != nil {
		return nil, err
	}

	typeID := id.NewType(schema, c.Name)
	arrayID := id.NewType(schema, "_"+c.Name)

	if collection.HasType(ctx, typeID) {
		// TODO: if the existing type is array type, it updates the array type name and creates the new type.
		return nil, types.ErrTypeAlreadyExists.New(c.Name)
	}

	var newType *types.DoltgresType
	switch c.typType {
	case types.TypeType_Pseudo:
		newType = types.NewShellType(ctx, typeID)
	case types.TypeType_Enum:
		enumLabelMap := make(map[string]types.EnumLabel)
		for i, l := range c.Labels {
			if _, ok := enumLabelMap[l]; ok {
				// DETAIL:  Key (enumtypid, enumlabel)=(16702, ok) already exists.
				return nil, errors.Errorf(`duplicate key value violates unique constraint "pg_enum_typid_label_index"`)
			}
			labelID := id.NewEnumLabel(typeID, l)
			el := types.NewEnumLabel(ctx, labelID, float32(i+1))
			enumLabelMap[l] = el
		}
		newType = types.NewEnumType(ctx, arrayID, typeID, enumLabelMap)
		// TODO: store labels somewhere
	case types.TypeType_Composite:
		// TODO: non-composite types have a zero oid for their relID, which for us would be a null ID.
		//  We need to find a way to distinguish a null ID from a composite type that does not reference a table
		//  (which is what relID points to if it represents a table row's composite type)
		relID := id.Null
		attrs := make([]types.CompositeAttribute, len(c.AsTypes))
		for i, a := range c.AsTypes {
			attrs[i] = types.NewCompositeAttribute(ctx, relID, a.AttrName, a.Typ.ID, a.Typ.GetAttTypMod(), int16(i+1), a.Collation)
		}
		newType = types.NewCompositeType(ctx, relID, arrayID, typeID, attrs)
	default:
		return nil, errors.Errorf("create type as %s is not supported", c.typType)
	}
	newType.Owner = ctx.Client().User

	err = collection.CreateType(ctx, newType)
	if err != nil {
		return nil, err
	}

	// create array type for defined types
	if newType.IsDefined {
		arrayType := types.CreateArrayTypeFromBaseType(newType)
		err = collection.CreateType(ctx, arrayType)
		if err != nil {
			return nil, err
		}
	}
	if err = core.MarkTypesCollectionDirty(ctx, c.DatabaseName); err != nil {
		return nil, err
	}

	return sql.RowsToRowIter(), nil
}

// Schema implements the interface sql.ExecSourceRel.
func (c *CreateType) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (c *CreateType) String() string {
	return "CREATE TYPE"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (c *CreateType) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(c, children...)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (c *CreateType) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return c, nil
}
