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
	"github.com/dolthub/doltgresql/server/types"
)

// AlterTypeAttributeAction describes one ALTER TYPE ADD/DROP/ALTER ATTRIBUTE action.
type AlterTypeAttributeAction struct {
	Action    string
	AttrName  string
	IfExists  bool
	TypeName  string
	Typ       *types.DoltgresType
	Collation string
	Cascade   bool
}

// AlterTypeAlterAttribute executes ALTER TYPE ... ADD/DROP ATTRIBUTE actions.
type AlterTypeAlterAttribute struct {
	database string
	schName  string
	typName  string
	actions  []AlterTypeAttributeAction
}

var _ sql.ExecSourceRel = (*AlterTypeAlterAttribute)(nil)
var _ vitess.Injectable = (*AlterTypeAlterAttribute)(nil)

// NewAlterTypeAlterAttribute returns a new *AlterTypeAlterAttribute.
func NewAlterTypeAlterAttribute(db, sch, typ string, actions []AlterTypeAttributeAction) *AlterTypeAlterAttribute {
	return &AlterTypeAlterAttribute{
		database: db,
		schName:  sch,
		typName:  typ,
		actions:  actions,
	}
}

// Children implements the interface sql.ExecSourceRel.
func (a *AlterTypeAlterAttribute) Children() []sql.Node {
	return nil
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (a *AlterTypeAlterAttribute) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (a *AlterTypeAlterAttribute) Resolved() bool {
	return true
}

// Schema implements the interface sql.ExecSourceRel.
func (a *AlterTypeAlterAttribute) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (a *AlterTypeAlterAttribute) String() string {
	return "ALTER TYPE ATTRIBUTE"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (a *AlterTypeAlterAttribute) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(a, children...)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (a *AlterTypeAlterAttribute) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return a, nil
}

// RowIter implements the interface sql.ExecSourceRel.
func (a *AlterTypeAlterAttribute) RowIter(ctx *sql.Context, r sql.Row) (sql.RowIter, error) {
	currentDb := ctx.GetCurrentDatabase()
	if len(a.database) > 0 && a.database != currentDb {
		return nil, errors.Errorf("ALTER TYPE is currently only supported for the current database")
	}
	schema, err := core.GetSchemaName(ctx, nil, a.schName)
	if err != nil {
		return nil, err
	}
	collection, err := core.GetTypesCollectionFromContext(ctx)
	if err != nil {
		return nil, err
	}
	typeID := id.NewType(schema, a.typName)
	typ, err := collection.GetType(ctx, typeID)
	if err != nil {
		return nil, err
	}
	if typ == nil {
		return nil, errors.Errorf(`type "%s" does not exist`, a.typName)
	}
	if typ.TypType != types.TypeType_Composite {
		return nil, errors.Errorf(`"%s" is not a composite type`, a.typName)
	}
	if _, isBuiltIn := types.IDToBuiltInDoltgresType[typ.ID]; isBuiltIn {
		return nil, errors.Errorf(`cannot alter type "%s" because it is a built-in type`, a.typName)
	}
	if err = checkTypeOwnership(ctx, typ); err != nil {
		return nil, err
	}

	for _, action := range a.actions {
		switch action.Action {
		case "add":
			if err = a.addAttribute(ctx, typ, action); err != nil {
				return nil, err
			}
		case "drop":
			if err = a.dropAttribute(typ, action); err != nil {
				return nil, err
			}
		default:
			return nil, errors.Errorf("ALTER TYPE ALTER ATTRIBUTE is not yet supported")
		}
	}

	if err = core.MarkTypesCollectionDirty(ctx, ""); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

func (a *AlterTypeAlterAttribute) addAttribute(ctx *sql.Context, typ *types.DoltgresType, action AlterTypeAttributeAction) error {
	attrType := action.Typ
	if attrType == nil && action.TypeName != "" {
		if internalID, ok := types.NameToInternalID[strings.ToLower(action.TypeName)]; ok {
			attrType = types.IDToBuiltInDoltgresType[internalID]
		}
	}
	if attrType == nil {
		return errors.Errorf(`type for column "%s" does not exist`, action.AttrName)
	}
	for _, attr := range typ.CompositeAttrs {
		if attr.Name == action.AttrName {
			return errors.Errorf(`column "%s" of relation "%s" already exists`, action.AttrName, a.typName)
		}
	}
	typ.CompositeAttrs = append(typ.CompositeAttrs, types.NewCompositeAttribute(
		ctx,
		typ.RelID,
		action.AttrName,
		attrType.ID,
		attrType.GetAttTypMod(),
		int16(len(typ.CompositeAttrs)+1),
		action.Collation,
	))
	return nil
}

func (a *AlterTypeAlterAttribute) dropAttribute(typ *types.DoltgresType, action AlterTypeAttributeAction) error {
	attrIdx := -1
	for i, attr := range typ.CompositeAttrs {
		if attr.Name == action.AttrName {
			attrIdx = i
			break
		}
	}
	if attrIdx < 0 {
		if action.IfExists {
			return nil
		}
		return errors.Errorf(`column "%s" of relation "%s" does not exist`, action.AttrName, a.typName)
	}
	typ.CompositeAttrs = append(typ.CompositeAttrs[:attrIdx], typ.CompositeAttrs[attrIdx+1:]...)
	for i := range typ.CompositeAttrs {
		typ.CompositeAttrs[i].Num = int16(i + 1)
	}
	return nil
}
