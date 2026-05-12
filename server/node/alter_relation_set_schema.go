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
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/core/sequences"
	"github.com/dolthub/doltgresql/server/auth"
	pgfunctions "github.com/dolthub/doltgresql/server/functions"
	"github.com/dolthub/doltgresql/server/tablemetadata"
)

// AlterRelationSetSchema handles ALTER TABLE/VIEW/MATERIALIZED VIEW/SEQUENCE
// ... SET SCHEMA.
type AlterRelationSetSchema struct {
	kind         relationOwnerKind
	ifExists     bool
	schema       string
	name         string
	targetSchema string
}

var _ sql.ExecSourceRel = (*AlterRelationSetSchema)(nil)
var _ vitess.Injectable = (*AlterRelationSetSchema)(nil)

// NewAlterTableSetSchema returns a new *AlterRelationSetSchema for ALTER TABLE ... SET SCHEMA.
func NewAlterTableSetSchema(ifExists bool, schema string, table string, targetSchema string) *AlterRelationSetSchema {
	return &AlterRelationSetSchema{
		kind:         relationOwnerKindTable,
		ifExists:     ifExists,
		schema:       schema,
		name:         table,
		targetSchema: targetSchema,
	}
}

// NewAlterViewSetSchema returns a new *AlterRelationSetSchema for ALTER VIEW ... SET SCHEMA.
func NewAlterViewSetSchema(ifExists bool, schema string, view string, targetSchema string) *AlterRelationSetSchema {
	return &AlterRelationSetSchema{
		kind:         relationOwnerKindView,
		ifExists:     ifExists,
		schema:       schema,
		name:         view,
		targetSchema: targetSchema,
	}
}

// NewAlterMaterializedViewSetSchema returns a new *AlterRelationSetSchema for ALTER MATERIALIZED VIEW ... SET SCHEMA.
func NewAlterMaterializedViewSetSchema(ifExists bool, schema string, view string, targetSchema string) *AlterRelationSetSchema {
	return &AlterRelationSetSchema{
		kind:         relationOwnerKindMaterializedView,
		ifExists:     ifExists,
		schema:       schema,
		name:         view,
		targetSchema: targetSchema,
	}
}

// NewAlterSequenceSetSchema returns a new *AlterRelationSetSchema for ALTER SEQUENCE ... SET SCHEMA.
func NewAlterSequenceSetSchema(ifExists bool, schema string, sequence string, targetSchema string) *AlterRelationSetSchema {
	return &AlterRelationSetSchema{
		kind:         relationOwnerKindSequence,
		ifExists:     ifExists,
		schema:       schema,
		name:         sequence,
		targetSchema: targetSchema,
	}
}

// Children implements sql.ExecSourceRel.
func (a *AlterRelationSetSchema) Children() []sql.Node {
	return nil
}

// IsReadOnly implements sql.ExecSourceRel.
func (a *AlterRelationSetSchema) IsReadOnly() bool {
	return false
}

// Resolved implements sql.ExecSourceRel.
func (a *AlterRelationSetSchema) Resolved() bool {
	return true
}

// RowIter implements sql.ExecSourceRel.
func (a *AlterRelationSetSchema) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	targetSchema, err := a.resolveTargetSchema(ctx)
	if err != nil {
		return nil, err
	}
	switch a.kind {
	case relationOwnerKindTable, relationOwnerKindMaterializedView:
		err = a.moveTableBackedRelation(ctx, targetSchema)
	case relationOwnerKindView:
		err = a.moveView(ctx, targetSchema)
	case relationOwnerKindSequence:
		err = a.moveSequence(ctx, targetSchema)
	default:
		err = errors.Errorf("unknown relation set schema kind %s", a.kind)
	}
	if err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(sql.Row{types.OkResult{RowsAffected: 1}}), nil
}

// Schema implements sql.ExecSourceRel.
func (a *AlterRelationSetSchema) Schema(ctx *sql.Context) sql.Schema {
	return types.OkResultSchema
}

// String implements sql.ExecSourceRel.
func (a *AlterRelationSetSchema) String() string {
	return "ALTER RELATION SET SCHEMA"
}

// WithChildren implements sql.ExecSourceRel.
func (a *AlterRelationSetSchema) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(a, children...)
}

// WithResolvedChildren implements vitess.Injectable.
func (a *AlterRelationSetSchema) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return a, nil
}

func (a *AlterRelationSetSchema) resolveTargetSchema(ctx *sql.Context) (string, error) {
	sdb, err := currentSchemaDDLDatabase(ctx)
	if err != nil {
		return "", err
	}
	_, exists, err := sdb.GetSchema(ctx, a.targetSchema)
	if err != nil {
		return "", err
	}
	if !exists {
		return "", sql.ErrDatabaseSchemaNotFound.New(a.targetSchema)
	}
	return a.targetSchema, nil
}

func (a *AlterRelationSetSchema) moveTableBackedRelation(ctx *sql.Context, targetSchema string) error {
	resolver := AlterRelationOwner{
		kind:     a.kind,
		ifExists: a.ifExists,
		schema:   a.schema,
		name:     a.name,
	}
	var oldName doltdb.TableName
	var err error
	if a.kind == relationOwnerKindMaterializedView {
		oldName, err = resolver.resolveMaterializedView(ctx)
	} else {
		oldName, err = resolver.resolveTable(ctx)
	}
	if err != nil {
		return err
	}
	if oldName.Name == "" {
		return nil
	}
	if oldName.Schema == targetSchema {
		return nil
	}
	if err = resolver.checkOwnership(ctx, oldName); err != nil {
		return errors.Wrap(err, "permission denied")
	}
	newName := doltdb.TableName{Name: oldName.Name, Schema: targetSchema}
	return a.renameRootRelation(ctx, oldName, newName)
}

func (a *AlterRelationSetSchema) moveSequence(ctx *sql.Context, targetSchema string) error {
	sourceSchema, err := core.GetSchemaName(ctx, nil, a.schema)
	if err != nil {
		return err
	}
	if sourceSchema == targetSchema {
		return nil
	}
	_, root, err := core.GetRootFromContext(ctx)
	if err != nil {
		return err
	}
	collection, err := sequences.LoadSequences(ctx, root)
	if err != nil {
		return err
	}
	sequenceID := id.NewSequence(sourceSchema, a.name)
	seq, err := collection.GetSequence(ctx, sequenceID)
	if err != nil {
		return err
	}
	if seq == nil {
		if a.ifExists {
			return nil
		}
		return errors.Errorf(`relation "%s" does not exist`, a.name)
	}
	if err = checkSequenceOwnership(ctx, seq); err != nil {
		return errors.Wrap(err, "permission denied")
	}
	oldName := doltdb.TableName{Name: a.name, Schema: sourceSchema}
	newName := doltdb.TableName{Name: a.name, Schema: targetSchema}
	return a.renameRootRelation(ctx, oldName, newName)
}

func (a *AlterRelationSetSchema) moveView(ctx *sql.Context, targetSchema string) error {
	resolver := AlterRelationOwner{
		kind:     relationOwnerKindView,
		ifExists: a.ifExists,
		schema:   a.schema,
		name:     a.name,
	}
	oldName, err := resolver.resolveView(ctx)
	if err != nil {
		return err
	}
	if oldName.Name == "" {
		return nil
	}
	if oldName.Schema == targetSchema {
		return nil
	}
	if err = resolver.checkOwnership(ctx, oldName); err != nil {
		return errors.Wrap(err, "permission denied")
	}

	schemaDatabase, err := currentSchemaDatabase(ctx)
	if err != nil {
		return err
	}
	sourceSchema, ok, err := schemaDatabase.GetSchema(ctx, oldName.Schema)
	if err != nil {
		return err
	}
	if !ok {
		return sql.ErrDatabaseSchemaNotFound.New(oldName.Schema)
	}
	targetSchemaDB, ok, err := schemaDatabase.GetSchema(ctx, targetSchema)
	if err != nil {
		return err
	}
	if !ok {
		return sql.ErrDatabaseSchemaNotFound.New(targetSchema)
	}
	sourceViewDB, ok := sourceSchema.(sql.ViewDatabase)
	if !ok {
		return errors.Errorf("schema %s does not support views", oldName.Schema)
	}
	targetViewDB, ok := targetSchemaDB.(sql.ViewDatabase)
	if !ok {
		return errors.Errorf("schema %s does not support views", targetSchema)
	}
	view, exists, err := sourceViewDB.GetViewDefinition(ctx, oldName.Name)
	if err != nil {
		return err
	}
	if !exists {
		if a.ifExists {
			return nil
		}
		return errors.Errorf(`relation "%s" does not exist`, a.name)
	}
	if err = relationNameAvailable(ctx, targetSchemaDB, targetViewDB, oldName.Name); err != nil {
		return err
	}
	if err = sourceViewDB.DropView(ctx, oldName.Name); err != nil {
		return err
	}
	textDefinition, createViewStatement, err := movedViewDefinitions(view, oldName.Schema, oldName.Name)
	if err != nil {
		return err
	}
	if err = targetViewDB.CreateView(ctx, oldName.Name, textDefinition, createViewStatement); err != nil {
		return err
	}
	return moveExplicitRelationOwner(oldName, doltdb.TableName{Name: oldName.Name, Schema: targetSchema})
}

func (a *AlterRelationSetSchema) renameRootRelation(ctx *sql.Context, oldName doltdb.TableName, newName doltdb.TableName) error {
	_, root, err := core.GetRootFromContext(ctx)
	if err != nil {
		return err
	}
	if _, exists, err := root.GetTableHash(ctx, newName); err != nil {
		return err
	} else if exists {
		return sql.ErrTableAlreadyExists.New(newName.Name)
	}
	newRoot, err := root.RenameTable(ctx, oldName, newName)
	if err != nil {
		return err
	}
	session, _, err := core.GetRootFromContext(ctx)
	if err != nil {
		return err
	}
	if err = session.SetWorkingRoot(ctx, ctx.GetCurrentDatabase(), newRoot); err != nil {
		return err
	}
	return moveExplicitRelationOwner(oldName, newName)
}

func relationNameAvailable(ctx *sql.Context, schema sql.DatabaseSchema, viewDB sql.ViewDatabase, name string) error {
	if table, found, err := schema.GetTableInsensitive(ctx, name); err != nil {
		return err
	} else if found {
		if tablemetadata.IsMaterializedView(tableComment(table)) {
			return errors.Errorf(`relation "%s" already exists`, name)
		}
		return sql.ErrTableAlreadyExists.New(name)
	}
	if _, found, err := viewDB.GetViewDefinition(ctx, name); err != nil {
		return err
	} else if found {
		return sql.ErrExistingView.New(schema.Name(), name)
	}
	return nil
}

func moveExplicitRelationOwner(oldName doltdb.TableName, newName doltdb.TableName) error {
	var err error
	auth.LockWrite(func() {
		owner := auth.GetRelationOwner(oldName)
		if owner == "" {
			return
		}
		auth.RemoveRelationOwner(oldName)
		auth.SetRelationOwner(newName, owner)
		err = auth.PersistChanges()
	})
	return err
}

func movedViewDefinitions(view sql.ViewDefinition, sourceSchema string, viewName string) (string, string, error) {
	textDefinition, err := pgfunctions.SchemaQualifiedViewDefinition(view.CreateViewStatement, sourceSchema)
	if err != nil {
		return "", "", err
	}
	if textDefinition == "" {
		return view.TextDefinition, view.CreateViewStatement, nil
	}
	createViewStatement := "CREATE VIEW " + quoteSQLIdentifier(viewName) + " AS " + textDefinition
	return textDefinition, createViewStatement, nil
}

func quoteSQLIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}
