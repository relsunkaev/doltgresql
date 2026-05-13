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
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/core"
	pgfunctions "github.com/dolthub/doltgresql/server/functions"
)

// AlterViewRename handles ALTER VIEW ... RENAME TO.
type AlterViewRename struct {
	ifExists  bool
	schema    string
	view      string
	newSchema string
	newView   string
}

var _ sql.ExecSourceRel = (*AlterViewRename)(nil)
var _ vitess.Injectable = (*AlterViewRename)(nil)

// NewAlterViewRename returns a new *AlterViewRename.
func NewAlterViewRename(ifExists bool, schema string, view string, newSchema string, newView string) *AlterViewRename {
	return &AlterViewRename{
		ifExists:  ifExists,
		schema:    schema,
		view:      view,
		newSchema: newSchema,
		newView:   newView,
	}
}

// Children implements sql.ExecSourceRel.
func (a *AlterViewRename) Children() []sql.Node {
	return nil
}

// IsReadOnly implements sql.ExecSourceRel.
func (a *AlterViewRename) IsReadOnly() bool {
	return false
}

// Resolved implements sql.ExecSourceRel.
func (a *AlterViewRename) Resolved() bool {
	return true
}

// RowIter implements sql.ExecSourceRel.
func (a *AlterViewRename) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	resolver := AlterRelationOwner{
		kind:     relationOwnerKindView,
		ifExists: a.ifExists,
		schema:   a.schema,
		name:     a.view,
	}
	oldName, err := resolver.resolveView(ctx)
	if err != nil {
		return nil, err
	}
	if oldName.Name == "" {
		return sql.RowsToRowIter(), nil
	}
	if err = resolver.checkOwnership(ctx, oldName); err != nil {
		return nil, errors.Wrap(err, "permission denied")
	}

	newSchema := oldName.Schema
	if a.newSchema != "" {
		newSchema, err = core.GetSchemaName(ctx, nil, a.newSchema)
		if err != nil {
			return nil, err
		}
		if newSchema != oldName.Schema {
			return nil, errors.New("cannot change view schema with RENAME")
		}
	}
	newName := doltdb.TableName{Name: a.newView, Schema: newSchema}
	if oldName == newName {
		return sql.RowsToRowIter(), nil
	}

	schemaDatabase, err := currentSchemaDatabase(ctx)
	if err != nil {
		return nil, err
	}
	schema, ok, err := schemaDatabase.GetSchema(ctx, oldName.Schema)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, sql.ErrDatabaseSchemaNotFound.New(oldName.Schema)
	}
	viewDatabase, ok := schema.(sql.ViewDatabase)
	if !ok {
		return nil, errors.Errorf("schema %s does not support views", oldName.Schema)
	}
	view, exists, err := viewDatabase.GetViewDefinition(ctx, oldName.Name)
	if err != nil {
		return nil, err
	}
	if !exists {
		if a.ifExists {
			return sql.RowsToRowIter(), nil
		}
		return nil, errors.Errorf(`relation "%s" does not exist`, a.view)
	}
	if err = relationNameAvailable(ctx, schema, viewDatabase, newName.Name); err != nil {
		return nil, err
	}
	textDefinition, createViewStatement, err := renamedViewDefinitions(view, oldName.Schema, newName.Name)
	if err != nil {
		return nil, err
	}
	if err = viewDatabase.DropView(ctx, oldName.Name); err != nil {
		return nil, err
	}
	if err = viewDatabase.CreateView(ctx, newName.Name, textDefinition, createViewStatement); err != nil {
		return nil, err
	}
	if err = moveExplicitRelationOwner(oldName, newName); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

// Schema implements sql.ExecSourceRel.
func (a *AlterViewRename) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements sql.ExecSourceRel.
func (a *AlterViewRename) String() string {
	return "ALTER VIEW RENAME"
}

// WithChildren implements sql.ExecSourceRel.
func (a *AlterViewRename) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(a, children...)
}

// WithResolvedChildren implements vitess.Injectable.
func (a *AlterViewRename) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return a, nil
}

func renamedViewDefinitions(view sql.ViewDefinition, sourceSchema string, newName string) (string, string, error) {
	textDefinition, err := pgfunctions.SchemaQualifiedViewDefinition(view.CreateViewStatement, sourceSchema)
	if err != nil {
		return "", "", err
	}
	if textDefinition == "" {
		textDefinition = view.TextDefinition
	}
	createViewStatement := "CREATE VIEW " + quoteSQLIdentifier(newName) + " AS " + textDefinition
	return textDefinition, createViewStatement, nil
}
