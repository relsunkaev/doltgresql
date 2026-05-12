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

// AlterTypeOwner handles ALTER TYPE/DOMAIN ... OWNER TO.
type AlterTypeOwner struct {
	DatabaseName string
	SchemaName   string
	TypeName     string
	Owner        string
	DomainOnly   bool
}

var _ sql.ExecSourceRel = (*AlterTypeOwner)(nil)
var _ vitess.Injectable = (*AlterTypeOwner)(nil)

// NewAlterTypeOwner returns a new *AlterTypeOwner.
func NewAlterTypeOwner(databaseName, schemaName, typeName, owner string, domainOnly bool) *AlterTypeOwner {
	return &AlterTypeOwner{
		DatabaseName: databaseName,
		SchemaName:   schemaName,
		TypeName:     typeName,
		Owner:        owner,
		DomainOnly:   domainOnly,
	}
}

// Children implements the interface sql.ExecSourceRel.
func (a *AlterTypeOwner) Children() []sql.Node { return nil }

// IsReadOnly implements the interface sql.ExecSourceRel.
func (a *AlterTypeOwner) IsReadOnly() bool { return false }

// Resolved implements the interface sql.ExecSourceRel.
func (a *AlterTypeOwner) Resolved() bool { return true }

// RowIter implements the interface sql.ExecSourceRel.
func (a *AlterTypeOwner) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	if !auth.RoleExists(a.Owner) {
		return nil, errors.Errorf(`role "%s" does not exist`, a.Owner)
	}
	schema, err := core.GetSchemaName(ctx, nil, a.SchemaName)
	if err != nil {
		return nil, err
	}
	collection, err := core.GetTypesCollectionFromContextForDatabase(ctx, a.DatabaseName)
	if err != nil {
		return nil, err
	}
	typeID := id.NewType(schema, a.TypeName)
	typ, err := collection.GetType(ctx, typeID)
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

	updatedType := *typ
	updatedType.Owner = a.Owner
	var updatedArray *types.DoltgresType
	if typ.Array.IsValid() {
		arrayType, err := collection.GetType(ctx, typ.Array)
		if err != nil {
			return nil, err
		}
		if arrayType != nil {
			arrayCopy := *arrayType
			arrayCopy.Owner = a.Owner
			updatedArray = &arrayCopy
		}
	}
	if updatedArray != nil {
		if err = collection.DropType(ctx, typ.ID, updatedArray.ID); err != nil {
			return nil, err
		}
	} else if err = collection.DropType(ctx, typ.ID); err != nil {
		return nil, err
	}
	if err = collection.CreateType(ctx, &updatedType); err != nil {
		return nil, err
	}
	if updatedArray != nil {
		if err = collection.CreateType(ctx, updatedArray); err != nil {
			return nil, err
		}
	}
	if err = core.MarkTypesCollectionDirty(ctx, a.DatabaseName); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

// Schema implements the interface sql.ExecSourceRel.
func (a *AlterTypeOwner) Schema(ctx *sql.Context) sql.Schema { return nil }

// String implements the interface sql.ExecSourceRel.
func (a *AlterTypeOwner) String() string { return "ALTER TYPE OWNER" }

// WithChildren implements the interface sql.ExecSourceRel.
func (a *AlterTypeOwner) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(a, children...)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (a *AlterTypeOwner) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return a, nil
}
