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
	"github.com/dolthub/go-mysql-server/sql/types"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/server/auth"
)

// AlterSchema implements ALTER SCHEMA.
type AlterSchema struct {
	SchemaName string
	Owner      string
}

var _ sql.ExecSourceRel = (*AlterSchema)(nil)
var _ vitess.Injectable = (*AlterSchema)(nil)

// NewAlterSchemaOwner returns a new *AlterSchema for ALTER SCHEMA ... OWNER TO.
func NewAlterSchemaOwner(schema string, owner string) *AlterSchema {
	return &AlterSchema{SchemaName: schema, Owner: owner}
}

// Children implements the interface sql.ExecSourceRel.
func (a *AlterSchema) Children() []sql.Node {
	return nil
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (a *AlterSchema) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (a *AlterSchema) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (a *AlterSchema) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	sdb, err := currentSchemaDDLDatabase(ctx)
	if err != nil {
		return nil, err
	}
	_, exists, err := sdb.GetSchema(ctx, a.SchemaName)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, sql.ErrDatabaseSchemaNotFound.New(a.SchemaName)
	}

	auth.LockWrite(func() {
		err = a.checkOwnership(ctx)
		if err != nil {
			return
		}
		auth.SetSchemaOwner(a.SchemaName, a.Owner)
		err = auth.PersistChanges()
	})
	if err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(sql.Row{types.OkResult{RowsAffected: 1}}), nil
}

func (a *AlterSchema) checkOwnership(ctx *sql.Context) error {
	if !auth.RoleExists(a.Owner) {
		return errors.Errorf(`role "%s" does not exist`, a.Owner)
	}
	role := auth.GetRole(ctx.Client().User)
	if role.IsValid() && role.IsSuperUser {
		return nil
	}
	if auth.SchemaOwnedByRole(a.SchemaName, ctx.Client().User) {
		return nil
	}
	return errors.Errorf("must be owner of schema %s", a.SchemaName)
}

// Schema implements the interface sql.ExecSourceRel.
func (a *AlterSchema) Schema(ctx *sql.Context) sql.Schema {
	return types.OkResultSchema
}

// String implements the interface sql.ExecSourceRel.
func (a *AlterSchema) String() string {
	return "ALTER SCHEMA"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (a *AlterSchema) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return a, nil
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (a *AlterSchema) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return a, nil
}
