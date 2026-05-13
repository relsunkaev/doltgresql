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
	coreextensions "github.com/dolthub/doltgresql/core/extensions"
	"github.com/dolthub/doltgresql/core/id"
)

// AlterExtensionSetSchema implements ALTER EXTENSION ... SET SCHEMA.
type AlterExtensionSetSchema struct {
	Name         string
	TargetSchema string
}

var _ sql.ExecSourceRel = (*AlterExtensionSetSchema)(nil)
var _ vitess.Injectable = (*AlterExtensionSetSchema)(nil)

// NewAlterExtensionSetSchema returns a new *AlterExtensionSetSchema.
func NewAlterExtensionSetSchema(name string, targetSchema string) *AlterExtensionSetSchema {
	return &AlterExtensionSetSchema{
		Name:         name,
		TargetSchema: targetSchema,
	}
}

func (a *AlterExtensionSetSchema) Children() []sql.Node { return nil }

func (a *AlterExtensionSetSchema) IsReadOnly() bool { return false }

func (a *AlterExtensionSetSchema) Resolved() bool { return true }

func (a *AlterExtensionSetSchema) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	targetSchema, err := core.GetSchemaName(ctx, nil, a.TargetSchema)
	if err != nil {
		return nil, err
	}
	if exists, err := schemaExists(ctx, targetSchema); err != nil {
		return nil, err
	} else if !exists {
		return nil, errors.Errorf(`schema "%s" does not exist`, targetSchema)
	}

	extCollection, err := core.GetExtensionsCollectionFromContext(ctx, "")
	if err != nil {
		return nil, err
	}
	extID := id.NewExtension(a.Name)
	ext, err := extCollection.GetLoadedExtension(ctx, extID)
	if err != nil {
		return nil, err
	}
	if !ext.ExtName.IsValid() {
		return nil, errors.Errorf(`extension "%s" does not exist`, a.Name)
	}
	if !ext.Relocatable {
		return nil, errors.Errorf(`extension "%s" does not support SET SCHEMA`, a.Name)
	}
	if err = checkExtensionOwnership(ctx, ext); err != nil {
		return nil, errors.Wrap(err, "permission denied")
	}
	if strings.EqualFold(ext.Namespace.SchemaName(), targetSchema) {
		return sql.RowsToRowIter(), nil
	}

	if err = moveExtensionMemberTypes(ctx, ext, targetSchema); err != nil {
		return nil, err
	}
	updated := ext
	updated.Namespace = id.NewNamespace(targetSchema)
	if err = extCollection.UpdateLoadedExtension(ctx, updated); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

func moveExtensionMemberTypes(ctx *sql.Context, ext coreextensions.Extension, targetSchema string) error {
	sourceSchema := ext.Namespace.SchemaName()
	if sourceSchema == "" {
		sourceSchema = "public"
	}
	typesCollection, err := core.GetTypesCollectionFromContext(ctx)
	if err != nil {
		return err
	}
	for _, typeName := range extensionMemberBaseTypes(ext) {
		typeID := id.NewType(sourceSchema, typeName)
		typ, err := typesCollection.GetType(ctx, typeID)
		if err != nil {
			return err
		}
		if typ == nil {
			continue
		}
		_, err = NewAlterTypeSetSchema("", sourceSchema, typeName, targetSchema, false).RowIter(ctx, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func extensionMemberBaseTypes(ext coreextensions.Extension) []string {
	switch strings.ToLower(ext.ExtName.Name()) {
	case "citext", "hstore", "vector":
		return []string{strings.ToLower(ext.ExtName.Name())}
	default:
		return nil
	}
}

func (a *AlterExtensionSetSchema) Schema(ctx *sql.Context) sql.Schema { return nil }

func (a *AlterExtensionSetSchema) String() string { return "ALTER EXTENSION SET SCHEMA" }

func (a *AlterExtensionSetSchema) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(a, children...)
}

func (a *AlterExtensionSetSchema) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return a, nil
}
