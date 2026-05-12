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
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/auth"
)

type relationOwnerKind string

const (
	relationOwnerKindTable relationOwnerKind = "table"
	relationOwnerKindView  relationOwnerKind = "view"
)

// AlterRelationOwner handles ALTER TABLE/VIEW ... OWNER TO.
type AlterRelationOwner struct {
	kind     relationOwnerKind
	ifExists bool
	schema   string
	name     string
	owner    string
}

var _ sql.ExecSourceRel = (*AlterRelationOwner)(nil)
var _ vitess.Injectable = (*AlterRelationOwner)(nil)

// NewAlterTableOwner returns a new *AlterRelationOwner for ALTER TABLE ... OWNER TO.
func NewAlterTableOwner(ifExists bool, schema string, table string, owner string) *AlterRelationOwner {
	return &AlterRelationOwner{
		kind:     relationOwnerKindTable,
		ifExists: ifExists,
		schema:   schema,
		name:     table,
		owner:    owner,
	}
}

// NewAlterViewOwner returns a new *AlterRelationOwner for ALTER VIEW ... OWNER TO.
func NewAlterViewOwner(ifExists bool, schema string, view string, owner string) *AlterRelationOwner {
	return &AlterRelationOwner{
		kind:     relationOwnerKindView,
		ifExists: ifExists,
		schema:   schema,
		name:     view,
		owner:    owner,
	}
}

// Children implements sql.ExecSourceRel.
func (a *AlterRelationOwner) Children() []sql.Node {
	return nil
}

// IsReadOnly implements sql.ExecSourceRel.
func (a *AlterRelationOwner) IsReadOnly() bool {
	return false
}

// Resolved implements sql.ExecSourceRel.
func (a *AlterRelationOwner) Resolved() bool {
	return true
}

// RowIter implements sql.ExecSourceRel.
func (a *AlterRelationOwner) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	if !auth.RoleExists(a.owner) {
		return nil, errors.Errorf(`role "%s" does not exist`, a.owner)
	}

	var relation doltdb.TableName
	var err error
	switch a.kind {
	case relationOwnerKindTable:
		relation, err = a.resolveTable(ctx)
	case relationOwnerKindView:
		relation, err = a.resolveView(ctx)
	default:
		err = errors.Errorf("unknown relation owner kind %s", a.kind)
	}
	if err != nil {
		return nil, err
	}
	if relation.Name == "" {
		return sql.RowsToRowIter(), nil
	}
	if err = a.checkOwnership(ctx, relation); err != nil {
		return nil, errors.Wrap(err, "permission denied")
	}
	auth.LockWrite(func() {
		auth.SetRelationOwner(relation, a.owner)
	})
	return sql.RowsToRowIter(), auth.PersistChanges()
}

// Schema implements sql.ExecSourceRel.
func (a *AlterRelationOwner) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements sql.ExecSourceRel.
func (a *AlterRelationOwner) String() string {
	return "ALTER RELATION OWNER"
}

// WithChildren implements sql.ExecSourceRel.
func (a *AlterRelationOwner) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(a, children...)
}

// WithResolvedChildren implements vitess.Injectable.
func (a *AlterRelationOwner) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return a, nil
}

func (a *AlterRelationOwner) resolveTable(ctx *sql.Context) (doltdb.TableName, error) {
	table, err := core.GetSqlTableFromContext(ctx, "", doltdb.TableName{Name: a.name, Schema: a.schema})
	if err != nil {
		return doltdb.TableName{}, err
	}
	if table == nil {
		if a.ifExists {
			return doltdb.TableName{}, nil
		}
		return doltdb.TableName{}, errors.Errorf(`relation "%s" does not exist`, a.name)
	}
	tableID, ok, err := id.GetFromTable(ctx, table)
	if err != nil {
		return doltdb.TableName{}, err
	}
	if !ok {
		schemaName, err := core.GetSchemaName(ctx, nil, a.schema)
		if err != nil {
			return doltdb.TableName{}, err
		}
		return doltdb.TableName{Name: table.Name(), Schema: schemaName}, nil
	}
	return doltdb.TableName{Name: tableID.TableName(), Schema: tableID.SchemaName()}, nil
}

func (a *AlterRelationOwner) resolveView(ctx *sql.Context) (doltdb.TableName, error) {
	relation := vitess.TableName{
		Name:            vitess.NewTableIdent(a.name),
		SchemaQualifier: vitess.NewTableIdent(a.schema),
	}
	searchSchemas, err := commentSearchSchemas(ctx, relation)
	if err != nil {
		return doltdb.TableName{}, err
	}
	schemaDatabase, err := currentSchemaDatabase(ctx)
	if err != nil {
		return doltdb.TableName{}, err
	}
	for _, schemaName := range searchSchemas {
		schema, ok, err := schemaDatabase.GetSchema(ctx, schemaName)
		if err != nil {
			return doltdb.TableName{}, err
		}
		if !ok {
			continue
		}
		viewDatabase, ok := schema.(sql.ViewDatabase)
		if !ok {
			continue
		}
		views, err := viewDatabase.AllViews(ctx)
		if err != nil {
			return doltdb.TableName{}, err
		}
		for _, view := range views {
			if view.Name == a.name {
				viewID := id.NewView(schema.SchemaName(), view.Name)
				return doltdb.TableName{Name: viewID.ViewName(), Schema: viewID.SchemaName()}, nil
			}
		}
	}
	if a.ifExists {
		return doltdb.TableName{}, nil
	}
	return doltdb.TableName{}, errors.Errorf(`relation "%s" does not exist`, a.name)
}

func (a *AlterRelationOwner) checkOwnership(ctx *sql.Context, relation doltdb.TableName) error {
	owner := auth.GetRelationOwner(relation)
	if owner == "" {
		if a.kind == relationOwnerKindTable {
			var err error
			owner, err = tableOwner(ctx, relation)
			if err != nil {
				return err
			}
		} else {
			owner = "postgres"
		}
	}
	if owner == "" || owner == ctx.Client().User {
		return nil
	}
	var userRole auth.Role
	auth.LockRead(func() {
		userRole = auth.GetRole(ctx.Client().User)
	})
	if userRole.IsValid() && userRole.IsSuperUser {
		return nil
	}
	return errors.Errorf("must be owner of %s %s", a.kind, relation.Name)
}
