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
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/mysql"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/server/auth"
	pgexprs "github.com/dolthub/doltgresql/server/expression"
)

// CreateSchema implements PostgreSQL CREATE SCHEMA.
type CreateSchema struct {
	Name           string
	Owner          string
	IfNotExists    bool
	SchemaElements []string
	Runner         pgexprs.StatementRunner
}

var _ sql.ExecSourceRel = (*CreateSchema)(nil)
var _ sql.Expressioner = (*CreateSchema)(nil)
var _ vitess.Injectable = (*CreateSchema)(nil)

// NewCreateSchema returns a new *CreateSchema.
func NewCreateSchema(name string, owner string, ifNotExists bool, schemaElements []string) *CreateSchema {
	return &CreateSchema{Name: name, Owner: owner, IfNotExists: ifNotExists, SchemaElements: schemaElements}
}

// Children implements the interface sql.ExecSourceRel.
func (c *CreateSchema) Children() []sql.Node {
	return nil
}

// Expressions implements the interface sql.Expressioner.
func (c *CreateSchema) Expressions() []sql.Expression {
	return []sql.Expression{c.Runner}
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (c *CreateSchema) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (c *CreateSchema) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (c *CreateSchema) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	if strings.HasPrefix(strings.ToLower(c.Name), "dolt_") {
		return nil, errors.Errorf("invalid schema name %s", c.Name)
	}

	owner := c.Owner
	if owner == "" {
		owner = ctx.Client().User
	}
	if err := c.checkCreatePrivileges(ctx, owner); err != nil {
		return nil, err
	}

	sdb, err := currentSchemaDDLDatabase(ctx)
	if err != nil {
		return nil, err
	}
	_, exists, err := sdb.GetSchema(ctx, c.Name)
	if err != nil {
		return nil, err
	}

	rows := []sql.Row{{types.OkResult{RowsAffected: 1}}}
	if exists {
		if c.IfNotExists && ctx != nil && ctx.Session != nil {
			ctx.Session.Warn(&sql.Warning{
				Level:   "Note",
				Code:    mysql.ERDbCreateExists,
				Message: fmt.Sprintf("Can't create schema %s; schema exists ", c.Name),
			})
			return sql.RowsToRowIter(rows...), nil
		}
		return nil, sql.ErrDatabaseSchemaExists.New(c.Name)
	}

	if err = sdb.CreateSchema(ctx, c.Name); err != nil {
		return nil, err
	}
	auth.LockWrite(func() {
		auth.SetSchemaOwner(c.Name, owner)
		err = auth.PersistChanges()
	})
	if err != nil {
		return nil, err
	}
	if err = c.runSchemaElements(ctx); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(rows...), nil
}

func (c *CreateSchema) runSchemaElements(ctx *sql.Context) error {
	if len(c.SchemaElements) == 0 {
		return nil
	}
	if c.Runner.Runner == nil {
		return errors.New("statement runner is not available for CREATE SCHEMA schema elements")
	}
	for _, query := range c.SchemaElements {
		_, err := sql.RunInterpreted(ctx, func(subCtx *sql.Context) ([]sql.Row, error) {
			_, rowIter, _, err := c.Runner.Runner.QueryWithBindings(subCtx, query, nil, nil, nil)
			if err != nil {
				return nil, err
			}
			return sql.RowIterToRows(subCtx, rowIter)
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *CreateSchema) checkCreatePrivileges(ctx *sql.Context, owner string) error {
	var err error
	auth.LockRead(func() {
		currentRole := auth.GetRole(ctx.Client().User)
		ownerRole := auth.GetRole(owner)
		if !ownerRole.IsValid() {
			err = errors.Errorf(`role "%s" does not exist`, owner)
			return
		}
		memberID, _, _ := auth.IsRoleAMember(currentRole.ID(), ownerRole.ID())
		if currentRole.ID() != ownerRole.ID() && !currentRole.IsSuperUser && !memberID.IsValid() {
			err = errors.Errorf("permission denied to create schema for role %s", owner)
			return
		}
		key := auth.DatabasePrivilegeKey{Role: currentRole.ID(), Name: ctx.GetCurrentDatabase()}
		if !auth.HasDatabasePrivilege(key, auth.Privilege_CREATE) {
			err = errors.Errorf("permission denied for database %s", ctx.GetCurrentDatabase())
		}
	})
	return err
}

// Schema implements the interface sql.ExecSourceRel.
func (c *CreateSchema) Schema(ctx *sql.Context) sql.Schema {
	return types.OkResultSchema
}

// String implements the interface sql.ExecSourceRel.
func (c *CreateSchema) String() string {
	return "CREATE SCHEMA"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (c *CreateSchema) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return c, nil
}

// WithExpressions implements the interface sql.Expressioner.
func (c *CreateSchema) WithExpressions(ctx *sql.Context, expressions ...sql.Expression) (sql.Node, error) {
	if len(expressions) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(expressions), 1)
	}
	newC := *c
	newC.Runner = expressions[0].(pgexprs.StatementRunner)
	return &newC, nil
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (c *CreateSchema) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return c, nil
}

func currentSchemaDDLDatabase(ctx *sql.Context) (sql.SchemaDatabase, error) {
	if ctx.GetCurrentDatabase() == "" {
		return nil, sql.ErrNoDatabaseSelected.New()
	}
	db, err := core.GetSqlDatabaseFromContext(ctx, "")
	if err != nil {
		return nil, err
	}
	if db == nil {
		return nil, sql.ErrDatabaseNotFound.New(ctx.GetCurrentDatabase())
	}
	if pdb, ok := db.(mysql_db.PrivilegedDatabase); ok {
		db = pdb.Unwrap()
	}
	sdb, ok := db.(sql.SchemaDatabase)
	if !ok || !sdb.SupportsDatabaseSchemas() {
		return nil, errors.Errorf("database %s does not support schemas", ctx.GetCurrentDatabase())
	}
	return sdb, nil
}
