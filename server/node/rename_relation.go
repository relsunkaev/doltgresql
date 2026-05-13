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
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/auth"
	pgexprs "github.com/dolthub/doltgresql/server/expression"
	"github.com/dolthub/doltgresql/server/rowsecurity"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// RenameRelation handles schema-qualified ALTER TABLE/MATERIALIZED VIEW ... RENAME TO.
type RenameRelation struct {
	kind      relationOwnerKind
	ifExists  bool
	schema    string
	name      string
	newSchema string
	newName   string
	Runner    pgexprs.StatementRunner
}

var _ sql.ExecSourceRel = (*RenameRelation)(nil)
var _ sql.Expressioner = (*RenameRelation)(nil)
var _ vitess.Injectable = (*RenameRelation)(nil)

// NewRenameTable returns a new *RenameRelation for ALTER TABLE ... RENAME TO.
func NewRenameTable(ifExists bool, schema string, table string, newSchema string, newTable string) *RenameRelation {
	return &RenameRelation{
		kind:      relationOwnerKindTable,
		ifExists:  ifExists,
		schema:    schema,
		name:      table,
		newSchema: newSchema,
		newName:   newTable,
	}
}

// NewRenameMaterializedView returns a new *RenameRelation for ALTER MATERIALIZED VIEW ... RENAME TO.
func NewRenameMaterializedView(ifExists bool, schema string, view string, newSchema string, newView string) *RenameRelation {
	return &RenameRelation{
		kind:      relationOwnerKindMaterializedView,
		ifExists:  ifExists,
		schema:    schema,
		name:      view,
		newSchema: newSchema,
		newName:   newView,
	}
}

// Children implements sql.ExecSourceRel.
func (r *RenameRelation) Children() []sql.Node {
	return nil
}

// Expressions implements sql.Expressioner.
func (r *RenameRelation) Expressions() []sql.Expression {
	return []sql.Expression{r.Runner}
}

// IsReadOnly implements sql.ExecSourceRel.
func (r *RenameRelation) IsReadOnly() bool {
	return false
}

// Resolved implements sql.ExecSourceRel.
func (r *RenameRelation) Resolved() bool {
	return true
}

// RowIter implements sql.ExecSourceRel.
func (r *RenameRelation) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	resolver := AlterRelationOwner{
		kind:     r.kind,
		ifExists: r.ifExists,
		schema:   r.schema,
		name:     r.name,
	}
	var oldName doltdb.TableName
	var err error
	if r.kind == relationOwnerKindMaterializedView {
		oldName, err = resolver.resolveMaterializedView(ctx)
	} else {
		oldName, err = resolver.resolveTable(ctx)
	}
	if err != nil {
		return nil, err
	}
	if oldName.Name == "" {
		return sql.RowsToRowIter(sql.Row{types.OkResult{RowsAffected: 0}}), nil
	}
	if err = resolver.checkOwnership(ctx, oldName); err != nil {
		return nil, errors.Wrap(err, "permission denied")
	}

	newSchema := oldName.Schema
	if r.newSchema != "" {
		newSchema, err = core.GetSchemaName(ctx, nil, r.newSchema)
		if err != nil {
			return nil, err
		}
		if newSchema != oldName.Schema {
			return nil, errors.Errorf("cannot change %s schema with RENAME", r.kind)
		}
	}
	newName := doltdb.TableName{Name: r.newName, Schema: newSchema}
	if oldName == newName {
		return sql.RowsToRowIter(sql.Row{types.OkResult{RowsAffected: 0}}), nil
	}

	schema, err := renameRelationSchema(ctx, oldName.Schema)
	if err != nil {
		return nil, err
	}
	viewDB, _ := schema.(sql.ViewDatabase)
	if err = relationNameAvailable(ctx, schema, viewDB, newName.Name); err != nil {
		return nil, err
	}
	table, ok, err := schema.GetTableInsensitive(ctx, oldName.Name)
	if err != nil {
		return nil, err
	}
	if !ok {
		if r.ifExists {
			return sql.RowsToRowIter(sql.Row{types.OkResult{RowsAffected: 0}}), nil
		}
		return nil, errors.Errorf(`relation "%s" does not exist`, r.name)
	}
	renamePlan := plan.NewRenameTable(schema, []string{oldName.Name}, []string{newName.Name}, true)
	renamer := schemaRootRenamer{
		Database: schema,
		oldName:  oldName,
		newName:  newName,
	}
	if err = renamePlan.RenameTable(ctx, renamer, table, oldName.Name, newName.Name); err != nil {
		return nil, err
	}
	if err = renameTableMetadata(ctx, oldName, newName); err != nil {
		return nil, err
	}
	if err = r.renameTableTypeReferences(ctx, oldName, newName); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(sql.Row{types.OkResult{RowsAffected: 1}}), nil
}

// Schema implements sql.ExecSourceRel.
func (r *RenameRelation) Schema(ctx *sql.Context) sql.Schema {
	return types.OkResultSchema
}

// String implements sql.ExecSourceRel.
func (r *RenameRelation) String() string {
	return "RENAME RELATION"
}

// WithChildren implements sql.ExecSourceRel.
func (r *RenameRelation) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(r, children...)
}

// WithExpressions implements sql.Expressioner.
func (r *RenameRelation) WithExpressions(ctx *sql.Context, expressions ...sql.Expression) (sql.Node, error) {
	if len(expressions) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(r, len(expressions), 1)
	}
	newR := *r
	newR.Runner = expressions[0].(pgexprs.StatementRunner)
	return &newR, nil
}

// WithResolvedChildren implements vitess.Injectable.
func (r *RenameRelation) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return r, nil
}

func renameRelationSchema(ctx *sql.Context, schemaName string) (sql.DatabaseSchema, error) {
	schemaDatabase, err := currentSchemaDatabase(ctx)
	if err != nil {
		return nil, err
	}
	schema, ok, err := schemaDatabase.GetSchema(ctx, schemaName)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, sql.ErrDatabaseSchemaNotFound.New(schemaName)
	}
	return schema, nil
}

type schemaRootRenamer struct {
	sql.Database
	oldName doltdb.TableName
	newName doltdb.TableName
}

func (r schemaRootRenamer) RenameTable(ctx *sql.Context, _, _ string) error {
	return renameRootTable(ctx, r.oldName, r.newName)
}

func renameRootTable(ctx *sql.Context, oldName doltdb.TableName, newName doltdb.TableName) error {
	session, root, err := core.GetRootFromContext(ctx)
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
	return session.SetWorkingRoot(ctx, ctx.GetCurrentDatabase(), newRoot)
}

func renameTableMetadata(ctx *sql.Context, oldName doltdb.TableName, newName doltdb.TableName) error {
	var persistErr error
	auth.LockWrite(func() {
		auth.RenameRelationOwner(oldName, newName)
		auth.RenameTablePrivileges(oldName, newName)
		persistErr = auth.PersistChanges()
	})
	if persistErr != nil {
		return persistErr
	}
	rowsecurity.RenameTable(ctx.GetCurrentDatabase(), oldName.Schema, oldName.Name, newName.Schema, newName.Name)
	return nil
}

func (r *RenameRelation) renameTableTypeReferences(ctx *sql.Context, oldName doltdb.TableName, newName doltdb.TableName) error {
	if r.Runner.Runner == nil {
		return nil
	}
	tableAsType := id.NewType(oldName.Schema, oldName.Name)
	_, root, err := core.GetRootFromContext(ctx)
	if err != nil {
		return err
	}
	allTableNames, err := root.GetAllTableNames(ctx, false)
	if err != nil {
		return err
	}
	for _, otherTableName := range allTableNames {
		if doltdb.IsSystemTable(otherTableName) {
			continue
		}
		otherTable, ok, err := root.GetTable(ctx, otherTableName)
		if err != nil {
			return err
		}
		if !ok {
			return errors.Errorf("root returned table name `%s` but it could not be found?", otherTableName.String())
		}
		otherTableSch, err := otherTable.GetSchema(ctx)
		if err != nil {
			return err
		}
		for _, otherCol := range otherTableSch.GetAllCols().GetColumns() {
			dgtype, ok := otherCol.TypeInfo.ToSqlType().(*pgtypes.DoltgresType)
			if !ok || dgtype.ID != tableAsType {
				continue
			}
			query := fmt.Sprintf(`ALTER TABLE "%s"."%s" ALTER COLUMN "%s" TYPE "%s"."%s";`,
				otherTableName.Schema, otherTableName.Name, otherCol.Name, oldName.Schema, newName.Name)
			_, err = sql.RunInterpreted(ctx, func(subCtx *sql.Context) ([]sql.Row, error) {
				_, rowIter, _, err := r.Runner.Runner.QueryWithBindings(subCtx, query, nil, nil, nil)
				if err != nil {
					return nil, err
				}
				return sql.RowIterToRows(subCtx, rowIter)
			})
			if err != nil {
				return err
			}
		}
	}
	return nil
}
