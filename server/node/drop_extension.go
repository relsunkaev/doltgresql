// Copyright 2025 Dolthub, Inc.
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
	"fmt"
	"slices"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/core"
	coreextensions "github.com/dolthub/doltgresql/core/extensions"
	corefunctions "github.com/dolthub/doltgresql/core/functions"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/auth"
	"github.com/dolthub/doltgresql/server/comments"
	pgexprs "github.com/dolthub/doltgresql/server/expression"
	serverfunctions "github.com/dolthub/doltgresql/server/functions"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// DropExtension implements DROP EXTENSION.
type DropExtension struct {
	Names    []string
	IfExists bool
	Cascade  bool
	Runner   pgexprs.StatementRunner
}

var _ sql.ExecSourceRel = (*DropExtension)(nil)
var _ sql.Expressioner = (*DropExtension)(nil)
var _ vitess.Injectable = (*DropExtension)(nil)

// NewDropExtension returns a new *DropExtension.
func NewDropExtension(names []string, ifExists bool, cascade bool) *DropExtension {
	return &DropExtension{
		Names:    names,
		IfExists: ifExists,
		Cascade:  cascade,
	}
}

// Children implements the interface sql.ExecSourceRel.
func (c *DropExtension) Children() []sql.Node {
	return nil
}

// Expressions implements the interface sql.Expressioner.
func (c *DropExtension) Expressions() []sql.Expression {
	return []sql.Expression{c.Runner}
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (c *DropExtension) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (c *DropExtension) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (c *DropExtension) RowIter(ctx *sql.Context, r sql.Row) (sql.RowIter, error) {
	extCollection, err := core.GetExtensionsCollectionFromContext(ctx, "")
	if err != nil {
		return nil, err
	}
	funcCollection, err := core.GetFunctionsCollectionFromContext(ctx)
	if err != nil {
		return nil, err
	}
	extensionsToDrop := make([]id.Extension, 0, len(c.Names))
	loadedExtensionsToDrop := make([]coreextensions.Extension, 0, len(c.Names))
	for _, name := range c.Names {
		extID := id.NewExtension(name)
		if !extCollection.HasLoadedExtension(ctx, extID) {
			if c.IfExists {
				continue
			}
			return nil, errors.Errorf(`extension "%s" does not exist`, name)
		}
		ext, err := extCollection.GetLoadedExtension(ctx, extID)
		if err != nil {
			return nil, err
		}
		if err = checkExtensionOwnership(ctx, ext); err != nil {
			return nil, errors.Wrap(err, "permission denied")
		}
		extensionsToDrop = append(extensionsToDrop, extID)
		loadedExtensionsToDrop = append(loadedExtensionsToDrop, ext)
	}
	functionsToDrop := make([]id.Function, 0)
	err = funcCollection.IterateFunctions(ctx, func(f corefunctions.Function) (stop bool, err error) {
		for _, extID := range extensionsToDrop {
			if slices.Contains(f.ExtensionDeps, extID.Name()) {
				if !c.Cascade {
					return true, errors.Errorf(`cannot drop extension "%s" because other objects depend on it`, extID.Name())
				}
				functionsToDrop = append(functionsToDrop, f.ID)
				break
			}
		}
		return false, nil
	})
	if err != nil {
		return nil, err
	}
	memberTypes := extensionMemberTypesByID(loadedExtensionsToDrop)
	dependentColumns, err := extensionDependentColumns(ctx, memberTypes)
	if err != nil {
		return nil, err
	}
	if len(dependentColumns) > 0 && !c.Cascade {
		return nil, errors.Errorf(`cannot drop extension "%s" because other objects depend on it`, dependentColumns[0].extension)
	}
	if c.Cascade {
		for _, dep := range dependentColumns {
			if err = c.dropDependentColumn(ctx, dep); err != nil {
				return nil, err
			}
		}
	}
	if err = funcCollection.DropFunction(ctx, functionsToDrop...); err != nil {
		return nil, err
	}
	if err = dropExtensionMemberTypes(ctx, memberTypes); err != nil {
		return nil, err
	}
	if err = extCollection.DropLoadedExtension(ctx, extensionsToDrop...); err != nil {
		return nil, err
	}
	for _, extID := range extensionsToDrop {
		clearExtensionComment(extID)
	}
	return sql.RowsToRowIter(), nil
}

type extensionDependentColumn struct {
	schema    string
	table     string
	column    string
	extension string
}

func extensionDependentColumns(ctx *sql.Context, memberTypes map[id.Type]string) ([]extensionDependentColumn, error) {
	if len(memberTypes) == 0 {
		return nil, nil
	}
	dependentColumns := make([]extensionDependentColumn, 0)
	err := serverfunctions.IterateCurrentDatabase(ctx, serverfunctions.Callbacks{
		Table: func(ctx *sql.Context, schema serverfunctions.ItemSchema, table serverfunctions.ItemTable) (cont bool, err error) {
			if schema.IsSystemSchema() {
				return true, nil
			}
			for _, col := range table.Item.Schema(ctx) {
				typ, ok := col.Type.(*pgtypes.DoltgresType)
				if !ok {
					continue
				}
				extension, ok := memberTypes[typ.ID]
				if !ok {
					continue
				}
				dependentColumns = append(dependentColumns, extensionDependentColumn{
					schema:    schema.Item.SchemaName(),
					table:     table.Item.Name(),
					column:    col.Name,
					extension: extension,
				})
			}
			return true, nil
		},
	})
	return dependentColumns, err
}

func (c *DropExtension) dropDependentColumn(ctx *sql.Context, dep extensionDependentColumn) error {
	if c.Runner.Runner == nil {
		return errors.New("statement runner is not available for DROP EXTENSION CASCADE")
	}
	query := fmt.Sprintf(`ALTER TABLE %s.%s DROP COLUMN %s`,
		quoteDropExtensionIdent(dep.schema),
		quoteDropExtensionIdent(dep.table),
		quoteDropExtensionIdent(dep.column),
	)
	_, err := sql.RunInterpreted(ctx, func(subCtx *sql.Context) ([]sql.Row, error) {
		_, rowIter, _, err := c.Runner.Runner.QueryWithBindings(subCtx, query, nil, nil, nil)
		if err != nil {
			return nil, err
		}
		return sql.RowIterToRows(subCtx, rowIter)
	})
	return err
}

func dropExtensionMemberTypes(ctx *sql.Context, memberTypes map[id.Type]string) error {
	if len(memberTypes) == 0 {
		return nil
	}
	typesCollection, err := core.GetTypesCollectionFromContext(ctx)
	if err != nil {
		return err
	}
	droppedTypes := make([]id.Type, 0, len(memberTypes))
	for typeID := range memberTypes {
		typ, err := typesCollection.GetType(ctx, typeID)
		if err != nil {
			return err
		}
		if typ == nil {
			continue
		}
		if err = typesCollection.DropType(ctx, typeID); err != nil {
			return err
		}
		clearTypeComment(typeID)
		droppedTypes = append(droppedTypes, typeID)
	}
	if len(droppedTypes) == 0 {
		return nil
	}
	if err = core.MarkTypesCollectionDirty(ctx, ""); err != nil {
		return err
	}
	auth.LockWrite(func() {
		for _, typeID := range droppedTypes {
			auth.RemoveAllTypePrivileges(typeID.SchemaName(), typeID.TypeName())
		}
		err = auth.PersistChanges()
	})
	return err
}

func extensionOwningType(ctx *sql.Context, typeID id.Type) (string, bool, error) {
	extCollection, err := core.GetExtensionsCollectionFromContext(ctx, "")
	if err != nil {
		return "", false, err
	}
	memberTypes := extensionMemberTypesByID(extCollection.GetLoadedExtensions(ctx))
	extension, ok := memberTypes[typeID]
	return extension, ok, nil
}

func extensionMemberTypesByID(extensions []coreextensions.Extension) map[id.Type]string {
	memberTypes := make(map[id.Type]string)
	for _, ext := range extensions {
		extName := strings.ToLower(ext.ExtName.Name())
		switch extName {
		case "citext", "hstore", "vector":
		default:
			continue
		}
		schemaName := ext.Namespace.SchemaName()
		if schemaName == "" {
			schemaName = "public"
		}
		memberTypes[id.NewType(schemaName, extName)] = extName
		memberTypes[id.NewType(schemaName, "_"+extName)] = extName
	}
	return memberTypes
}

func quoteDropExtensionIdent(ident string) string {
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
}

func clearExtensionComment(extID id.Extension) {
	comments.Set(comments.Key{
		ObjOID:   id.Cache().ToOID(extID.AsId()),
		ClassOID: comments.ClassOID("pg_extension"),
		ObjSubID: 0,
	}, nil)
}

func checkExtensionOwnership(ctx *sql.Context, ext coreextensions.Extension) error {
	owner := ext.Owner
	if owner == "" {
		owner = "postgres"
	}
	userRole := auth.GetRole(ctx.Client().User)
	if userRole.IsValid() && roleCanOperateAsOwner(userRole, owner) {
		return nil
	}
	return errors.Errorf("must be owner of extension %s", ext.ExtName.Name())
}

// Schema implements the interface sql.ExecSourceRel.
func (c *DropExtension) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (c *DropExtension) String() string {
	return "DROP EXTENSION"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (c *DropExtension) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(c, children...)
}

// WithExpressions implements the interface sql.Expressioner.
func (c *DropExtension) WithExpressions(ctx *sql.Context, expressions ...sql.Expression) (sql.Node, error) {
	if len(expressions) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(expressions), 1)
	}
	newC := *c
	newC.Runner = expressions[0].(pgexprs.StatementRunner)
	return &newC, nil
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (c *DropExtension) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return c, nil
}
