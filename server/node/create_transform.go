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
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/auth"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// CreateTransform implements CREATE TRANSFORM.
type CreateTransform struct {
	TypeName string
	Language string
	FromSQL  string
	ToSQL    string
}

var _ sql.ExecSourceRel = (*CreateTransform)(nil)
var _ vitess.Injectable = (*CreateTransform)(nil)

// NewCreateTransform returns a new *CreateTransform.
func NewCreateTransform(typeName string, language string, fromSQL string, toSQL string) *CreateTransform {
	return &CreateTransform{
		TypeName: typeName,
		Language: language,
		FromSQL:  fromSQL,
		ToSQL:    toSQL,
	}
}

// Children implements the interface sql.ExecSourceRel.
func (c *CreateTransform) Children() []sql.Node {
	return nil
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (c *CreateTransform) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (c *CreateTransform) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (c *CreateTransform) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	typeID, err := transformTypeID(ctx, c.TypeName)
	if err != nil {
		return nil, err
	}
	if err = checkTransformTypeOwnership(ctx, typeID); err != nil {
		return nil, err
	}
	auth.LockWrite(func() {
		err = auth.CreateTransform(auth.Transform{
			TypeID:  typeID,
			Lang:    c.Language,
			FromSQL: c.FromSQL,
			ToSQL:   c.ToSQL,
		})
		if err == nil {
			err = auth.PersistChanges()
		}
	})
	if err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

// Schema implements the interface sql.ExecSourceRel.
func (c *CreateTransform) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (c *CreateTransform) String() string {
	return "CREATE TRANSFORM"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (c *CreateTransform) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(c, children...)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (c *CreateTransform) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return c, nil
}

func transformTypeID(ctx *sql.Context, rawTypeName string) (id.Id, error) {
	typeName := strings.TrimSpace(rawTypeName)
	typeName = strings.TrimSuffix(strings.Split(typeName, "(")[0], ";")
	parts := strings.Split(typeName, ".")
	var schemas []string
	if len(parts) == 2 {
		schemas = []string{normalizeTransformIdentifier(parts[0])}
		typeName = normalizeTransformIdentifier(parts[1])
	} else {
		var err error
		schemas, err = core.SearchPath(ctx)
		if err != nil {
			return id.Null, err
		}
		typeName = normalizeTransformIdentifier(typeName)
	}
	if typeName == "int" {
		typeName = "int4"
	}
	typeCollection, err := core.GetTypesCollectionFromContext(ctx)
	if err != nil {
		return id.Null, err
	}
	for _, schema := range schemas {
		if internalID, ok := pgtypes.NameToInternalID[typeName]; ok && internalID.SchemaName() == schema {
			return internalID.AsId(), nil
		}
		typ, err := typeCollection.GetType(ctx, id.NewType(schema, typeName))
		if err != nil {
			return id.Null, err
		}
		if typ != nil {
			return typ.ID.AsId(), nil
		}
	}
	return id.Null, errors.Errorf(`type "%s" does not exist`, rawTypeName)
}

func checkTransformTypeOwnership(ctx *sql.Context, typeID id.Id) error {
	var userRole auth.Role
	auth.LockRead(func() {
		userRole = auth.GetRole(ctx.Client().User)
	})
	if userRole.IsValid() && userRole.IsSuperUser {
		return nil
	}
	if _, ok := pgtypes.IDToBuiltInDoltgresType[id.Type(typeID)]; ok {
		return errors.Errorf("must be owner of type %s", id.Type(typeID).TypeName())
	}
	return nil
}

func normalizeTransformIdentifier(identifier string) string {
	identifier = strings.TrimSpace(identifier)
	if len(identifier) >= 2 && identifier[0] == '"' && identifier[len(identifier)-1] == '"' {
		return strings.ReplaceAll(identifier[1:len(identifier)-1], `""`, `"`)
	}
	return strings.ToLower(identifier)
}
