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
	"github.com/dolthub/go-mysql-server/sql/plan"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
	"github.com/dolthub/doltgresql/server/auth"
)

// ForeignTableName identifies a foreign table.
type ForeignTableName struct {
	Schema string
	Name   string
}

// CreateForeignDataWrapper implements CREATE FOREIGN DATA WRAPPER.
type CreateForeignDataWrapper struct {
	Name    string
	Options []string
}

// AlterForeignDataWrapper implements ALTER FOREIGN DATA WRAPPER.
type AlterForeignDataWrapper struct {
	Name string
}

// AlterForeignTableOptions implements ALTER FOREIGN TABLE ... OPTIONS.
type AlterForeignTableOptions struct {
	SchemaName string
	Name       string
	Options    []string
}

// DropForeignTable implements DROP FOREIGN TABLE.
type DropForeignTable struct {
	Tables   []ForeignTableName
	IfExists bool
	Cascade  bool
}

// DropForeignDataWrapper implements DROP FOREIGN DATA WRAPPER.
type DropForeignDataWrapper struct {
	Names    []string
	IfExists bool
	Cascade  bool
}

// CreateForeignServer implements CREATE SERVER.
type CreateForeignServer struct {
	Name    string
	Wrapper string
	Type    string
	Version string
	Options []string
}

// AlterForeignServer implements ALTER SERVER.
type AlterForeignServer struct {
	Name    string
	Version string
}

// DropForeignServer implements DROP SERVER.
type DropForeignServer struct {
	Names    []string
	IfExists bool
	Cascade  bool
}

// CreateUserMapping implements CREATE USER MAPPING.
type CreateUserMapping struct {
	User    string
	Server  string
	Options []string
}

// AlterUserMapping implements ALTER USER MAPPING.
type AlterUserMapping struct {
	User   string
	Server string
}

// DropUserMapping implements DROP USER MAPPING.
type DropUserMapping struct {
	User     string
	Server   string
	IfExists bool
}

// ImportForeignSchema implements IMPORT FOREIGN SCHEMA.
type ImportForeignSchema struct {
	RemoteSchema string
	Server       string
	Into         string
}

var _ sql.ExecSourceRel = (*CreateForeignDataWrapper)(nil)
var _ sql.ExecSourceRel = (*AlterForeignDataWrapper)(nil)
var _ sql.ExecSourceRel = (*AlterForeignTableOptions)(nil)
var _ sql.ExecSourceRel = (*DropForeignTable)(nil)
var _ sql.ExecSourceRel = (*DropForeignDataWrapper)(nil)
var _ sql.ExecSourceRel = (*CreateForeignServer)(nil)
var _ sql.ExecSourceRel = (*AlterForeignServer)(nil)
var _ sql.ExecSourceRel = (*DropForeignServer)(nil)
var _ sql.ExecSourceRel = (*CreateUserMapping)(nil)
var _ sql.ExecSourceRel = (*AlterUserMapping)(nil)
var _ sql.ExecSourceRel = (*DropUserMapping)(nil)
var _ sql.ExecSourceRel = (*ImportForeignSchema)(nil)
var _ vitess.Injectable = (*CreateForeignDataWrapper)(nil)
var _ vitess.Injectable = (*AlterForeignDataWrapper)(nil)
var _ vitess.Injectable = (*AlterForeignTableOptions)(nil)
var _ vitess.Injectable = (*DropForeignTable)(nil)
var _ vitess.Injectable = (*DropForeignDataWrapper)(nil)
var _ vitess.Injectable = (*CreateForeignServer)(nil)
var _ vitess.Injectable = (*AlterForeignServer)(nil)
var _ vitess.Injectable = (*DropForeignServer)(nil)
var _ vitess.Injectable = (*CreateUserMapping)(nil)
var _ vitess.Injectable = (*AlterUserMapping)(nil)
var _ vitess.Injectable = (*DropUserMapping)(nil)
var _ vitess.Injectable = (*ImportForeignSchema)(nil)

// NewCreateForeignDataWrapper returns a new *CreateForeignDataWrapper.
func NewCreateForeignDataWrapper(name string, options []string) *CreateForeignDataWrapper {
	return &CreateForeignDataWrapper{Name: name, Options: options}
}

// NewAlterForeignDataWrapper returns a new *AlterForeignDataWrapper.
func NewAlterForeignDataWrapper(name string) *AlterForeignDataWrapper {
	return &AlterForeignDataWrapper{Name: name}
}

// NewAlterForeignTableOptions returns a new *AlterForeignTableOptions.
func NewAlterForeignTableOptions(schema string, name string, options []string) *AlterForeignTableOptions {
	return &AlterForeignTableOptions{SchemaName: schema, Name: name, Options: options}
}

// NewDropForeignTable returns a new *DropForeignTable.
func NewDropForeignTable(tables []ForeignTableName, ifExists bool, cascade bool) *DropForeignTable {
	return &DropForeignTable{Tables: tables, IfExists: ifExists, Cascade: cascade}
}

// NewDropForeignDataWrapper returns a new *DropForeignDataWrapper.
func NewDropForeignDataWrapper(names []string, ifExists bool, cascade bool) *DropForeignDataWrapper {
	return &DropForeignDataWrapper{Names: names, IfExists: ifExists, Cascade: cascade}
}

// NewCreateForeignServer returns a new *CreateForeignServer.
func NewCreateForeignServer(name string, wrapper string, typ string, version string, options []string) *CreateForeignServer {
	return &CreateForeignServer{Name: name, Wrapper: wrapper, Type: typ, Version: version, Options: options}
}

// NewAlterForeignServer returns a new *AlterForeignServer.
func NewAlterForeignServer(name string, version string) *AlterForeignServer {
	return &AlterForeignServer{Name: name, Version: version}
}

// NewDropForeignServer returns a new *DropForeignServer.
func NewDropForeignServer(names []string, ifExists bool, cascade bool) *DropForeignServer {
	return &DropForeignServer{Names: names, IfExists: ifExists, Cascade: cascade}
}

// NewCreateUserMapping returns a new *CreateUserMapping.
func NewCreateUserMapping(user string, server string, options []string) *CreateUserMapping {
	return &CreateUserMapping{User: user, Server: server, Options: options}
}

// NewAlterUserMapping returns a new *AlterUserMapping.
func NewAlterUserMapping(user string, server string) *AlterUserMapping {
	return &AlterUserMapping{User: user, Server: server}
}

// NewDropUserMapping returns a new *DropUserMapping.
func NewDropUserMapping(user string, server string, ifExists bool) *DropUserMapping {
	return &DropUserMapping{User: user, Server: server, IfExists: ifExists}
}

// NewImportForeignSchema returns a new *ImportForeignSchema.
func NewImportForeignSchema(schema string, server string, into string) *ImportForeignSchema {
	return &ImportForeignSchema{RemoteSchema: schema, Server: server, Into: into}
}

func (c *CreateForeignDataWrapper) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	var err error
	auth.LockWrite(func() {
		err = auth.CreateForeignDataWrapper(auth.ForeignDataWrapper{
			Name:    c.Name,
			Owner:   ctx.Client().User,
			Options: append([]string(nil), c.Options...),
		})
		if err == nil {
			err = auth.PersistChanges()
		}
	})
	return rowIterOrError(err)
}

func (a *AlterForeignDataWrapper) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	var err error
	auth.LockRead(func() {
		if _, ok := auth.GetForeignDataWrapper(a.Name); !ok {
			err = pgerror.Newf(pgcode.UndefinedObject, `foreign-data wrapper "%s" does not exist`, a.Name)
		}
	})
	return rowIterOrError(err)
}

func (a *AlterForeignTableOptions) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	if err := requireForeignTableExists(ctx, a.SchemaName, a.Name, false, foreignTableUndefinedTableError); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

func (d *DropForeignTable) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	for _, table := range d.Tables {
		if err := requireForeignTableExists(ctx, table.Schema, table.Name, d.IfExists, foreignTableUndefinedObjectError); err != nil {
			return nil, err
		}
		if d.IfExists {
			relationType, err := foreignTableRelationType(ctx, table.Schema, table.Name)
			if err != nil {
				return nil, err
			}
			if relationType == core.RelationType_DoesNotExist {
				continue
			}
		}
		schema, err := core.GetSchemaName(ctx, nil, table.Schema)
		if err != nil {
			return nil, err
		}
		db, err := schemaDatabase(ctx, schema)
		if err != nil {
			return nil, err
		}
		dropper, ok := unwrapPrivilegedDatabase(db).(sql.TableDropper)
		if !ok {
			return nil, sql.ErrDropTableNotSupported.New(db.Name())
		}
		if err = dropper.DropTable(ctx, table.Name); err != nil {
			return nil, err
		}
	}
	return sql.RowsToRowIter(), nil
}

func (d *DropForeignDataWrapper) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	var err error
	auth.LockWrite(func() {
		for _, name := range d.Names {
			if _, ok := auth.GetForeignDataWrapper(name); !ok {
				if !d.IfExists {
					err = pgerror.Newf(pgcode.UndefinedObject, `foreign-data wrapper "%s" does not exist`, name)
					return
				}
				continue
			}
			if !auth.DropForeignDataWrapper(name) && !d.IfExists {
				err = pgerror.Newf(pgcode.UndefinedObject, `foreign-data wrapper "%s" does not exist`, name)
				return
			}
		}
		if err == nil {
			err = auth.PersistChanges()
		}
	})
	return rowIterOrError(err)
}

func (c *CreateForeignServer) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	var err error
	auth.LockWrite(func() {
		err = auth.CreateForeignServer(auth.ForeignServer{
			Name:    c.Name,
			Owner:   ctx.Client().User,
			Wrapper: c.Wrapper,
			Type:    c.Type,
			Version: c.Version,
			Options: append([]string(nil), c.Options...),
		})
		if err == nil {
			err = auth.PersistChanges()
		}
	})
	return rowIterOrError(err)
}

func (a *AlterForeignServer) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	var err error
	auth.LockWrite(func() {
		err = auth.AlterForeignServerVersion(a.Name, a.Version)
		if err == nil {
			err = auth.PersistChanges()
		}
	})
	return rowIterOrError(err)
}

func (d *DropForeignServer) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	var err error
	auth.LockWrite(func() {
		for _, name := range d.Names {
			if _, ok := auth.GetForeignServer(name); !ok {
				if !d.IfExists {
					err = pgerror.Newf(pgcode.UndefinedObject, `server "%s" does not exist`, name)
					return
				}
				continue
			}
			if !auth.DropForeignServer(name) && !d.IfExists {
				err = pgerror.Newf(pgcode.UndefinedObject, `server "%s" does not exist`, name)
				return
			}
		}
		if err == nil {
			err = auth.PersistChanges()
		}
	})
	return rowIterOrError(err)
}

func (c *CreateUserMapping) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	var err error
	auth.LockWrite(func() {
		err = auth.CreateUserMapping(auth.UserMapping{
			User:    resolveUserMappingUser(ctx, c.User),
			Server:  c.Server,
			Options: append([]string(nil), c.Options...),
		})
		if err == nil {
			err = auth.PersistChanges()
		}
	})
	return rowIterOrError(err)
}

func (a *AlterUserMapping) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	var err error
	auth.LockWrite(func() {
		err = auth.AlterUserMapping(resolveUserMappingUser(ctx, a.User), a.Server)
	})
	return rowIterOrError(err)
}

func (d *DropUserMapping) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	var err error
	auth.LockWrite(func() {
		err = auth.DropUserMapping(resolveUserMappingUser(ctx, d.User), d.Server)
		if err != nil && d.IfExists {
			err = nil
		}
		if err == nil {
			err = auth.PersistChanges()
		}
	})
	return rowIterOrError(err)
}

func (i *ImportForeignSchema) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	var exists bool
	auth.LockRead(func() {
		_, exists = auth.GetForeignServer(i.Server)
	})
	if !exists {
		return nil, pgerror.Newf(pgcode.UndefinedObject, `server "%s" does not exist`, i.Server)
	}
	return nil, errors.New("IMPORT FOREIGN SCHEMA is not yet supported")
}

func rowIterOrError(err error) (sql.RowIter, error) {
	if err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

func resolveUserMappingUser(ctx *sql.Context, user string) string {
	switch user {
	case "", "current_user", "CURRENT_USER", "user", "USER":
		return ctx.Client().User
	default:
		return user
	}
}

func requireForeignTableExists(ctx *sql.Context, schema string, table string, ifExists bool, missingErr func(string) error) error {
	relationType, err := foreignTableRelationType(ctx, schema, table)
	if err != nil {
		return err
	}
	if relationType == core.RelationType_DoesNotExist {
		if ifExists {
			return nil
		}
		return missingErr(table)
	}
	return nil
}

func foreignTableUndefinedObjectError(table string) error {
	return pgerror.Newf(pgcode.UndefinedObject, `foreign table "%s" does not exist`, table)
}

func foreignTableUndefinedTableError(table string) error {
	return sql.ErrTableNotFound.New(table)
}

func foreignTableRelationType(ctx *sql.Context, schema string, table string) (core.RelationType, error) {
	resolvedSchema, err := core.GetSchemaName(ctx, nil, schema)
	if err != nil {
		return core.RelationType_DoesNotExist, err
	}
	return core.GetRelationType(ctx, resolvedSchema, table)
}

func (c *CreateForeignDataWrapper) Children() []sql.Node { return nil }
func (a *AlterForeignDataWrapper) Children() []sql.Node  { return nil }
func (a *AlterForeignTableOptions) Children() []sql.Node { return nil }
func (d *DropForeignTable) Children() []sql.Node         { return nil }
func (d *DropForeignDataWrapper) Children() []sql.Node   { return nil }
func (c *CreateForeignServer) Children() []sql.Node      { return nil }
func (a *AlterForeignServer) Children() []sql.Node       { return nil }
func (d *DropForeignServer) Children() []sql.Node        { return nil }
func (c *CreateUserMapping) Children() []sql.Node        { return nil }
func (a *AlterUserMapping) Children() []sql.Node         { return nil }
func (d *DropUserMapping) Children() []sql.Node          { return nil }
func (i *ImportForeignSchema) Children() []sql.Node      { return nil }

func (c *CreateForeignDataWrapper) IsReadOnly() bool { return false }
func (a *AlterForeignDataWrapper) IsReadOnly() bool  { return false }
func (a *AlterForeignTableOptions) IsReadOnly() bool { return false }
func (d *DropForeignTable) IsReadOnly() bool         { return false }
func (d *DropForeignDataWrapper) IsReadOnly() bool   { return false }
func (c *CreateForeignServer) IsReadOnly() bool      { return false }
func (a *AlterForeignServer) IsReadOnly() bool       { return false }
func (d *DropForeignServer) IsReadOnly() bool        { return false }
func (c *CreateUserMapping) IsReadOnly() bool        { return false }
func (a *AlterUserMapping) IsReadOnly() bool         { return false }
func (d *DropUserMapping) IsReadOnly() bool          { return false }
func (i *ImportForeignSchema) IsReadOnly() bool      { return false }

func (c *CreateForeignDataWrapper) Resolved() bool { return true }
func (a *AlterForeignDataWrapper) Resolved() bool  { return true }
func (a *AlterForeignTableOptions) Resolved() bool { return true }
func (d *DropForeignTable) Resolved() bool         { return true }
func (d *DropForeignDataWrapper) Resolved() bool   { return true }
func (c *CreateForeignServer) Resolved() bool      { return true }
func (a *AlterForeignServer) Resolved() bool       { return true }
func (d *DropForeignServer) Resolved() bool        { return true }
func (c *CreateUserMapping) Resolved() bool        { return true }
func (a *AlterUserMapping) Resolved() bool         { return true }
func (d *DropUserMapping) Resolved() bool          { return true }
func (i *ImportForeignSchema) Resolved() bool      { return true }

func (c *CreateForeignDataWrapper) Schema(ctx *sql.Context) sql.Schema { return nil }
func (a *AlterForeignDataWrapper) Schema(ctx *sql.Context) sql.Schema  { return nil }
func (a *AlterForeignTableOptions) Schema(ctx *sql.Context) sql.Schema { return nil }
func (d *DropForeignTable) Schema(ctx *sql.Context) sql.Schema         { return nil }
func (d *DropForeignDataWrapper) Schema(ctx *sql.Context) sql.Schema   { return nil }
func (c *CreateForeignServer) Schema(ctx *sql.Context) sql.Schema      { return nil }
func (a *AlterForeignServer) Schema(ctx *sql.Context) sql.Schema       { return nil }
func (d *DropForeignServer) Schema(ctx *sql.Context) sql.Schema        { return nil }
func (c *CreateUserMapping) Schema(ctx *sql.Context) sql.Schema        { return nil }
func (a *AlterUserMapping) Schema(ctx *sql.Context) sql.Schema         { return nil }
func (d *DropUserMapping) Schema(ctx *sql.Context) sql.Schema          { return nil }
func (i *ImportForeignSchema) Schema(ctx *sql.Context) sql.Schema      { return nil }

func (c *CreateForeignDataWrapper) String() string { return "CREATE FOREIGN DATA WRAPPER" }
func (a *AlterForeignDataWrapper) String() string  { return "ALTER FOREIGN DATA WRAPPER" }
func (a *AlterForeignTableOptions) String() string { return "ALTER FOREIGN TABLE" }
func (d *DropForeignTable) String() string         { return "DROP FOREIGN TABLE" }
func (d *DropForeignDataWrapper) String() string   { return "DROP FOREIGN DATA WRAPPER" }
func (c *CreateForeignServer) String() string      { return "CREATE SERVER" }
func (a *AlterForeignServer) String() string       { return "ALTER SERVER" }
func (d *DropForeignServer) String() string        { return "DROP SERVER" }
func (c *CreateUserMapping) String() string        { return "CREATE USER MAPPING" }
func (a *AlterUserMapping) String() string         { return "ALTER USER MAPPING" }
func (d *DropUserMapping) String() string          { return "DROP USER MAPPING" }
func (i *ImportForeignSchema) String() string      { return "IMPORT FOREIGN SCHEMA" }

func (c *CreateForeignDataWrapper) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(c, children...)
}

func (a *AlterForeignDataWrapper) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(a, children...)
}

func (a *AlterForeignTableOptions) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(a, children...)
}

func (d *DropForeignTable) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(d, children...)
}

func (d *DropForeignDataWrapper) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(d, children...)
}

func (c *CreateForeignServer) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(c, children...)
}

func (a *AlterForeignServer) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(a, children...)
}

func (d *DropForeignServer) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(d, children...)
}

func (c *CreateUserMapping) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(c, children...)
}

func (a *AlterUserMapping) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(a, children...)
}

func (d *DropUserMapping) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(d, children...)
}

func (i *ImportForeignSchema) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(i, children...)
}

func withNoResolvedChildren[T any](node T, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return node, nil
}

func (c *CreateForeignDataWrapper) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	return withNoResolvedChildren(c, children)
}

func (a *AlterForeignDataWrapper) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	return withNoResolvedChildren(a, children)
}

func (a *AlterForeignTableOptions) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	return withNoResolvedChildren(a, children)
}

func (d *DropForeignTable) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	return withNoResolvedChildren(d, children)
}

func (d *DropForeignDataWrapper) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	return withNoResolvedChildren(d, children)
}

func (c *CreateForeignServer) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	return withNoResolvedChildren(c, children)
}

func (a *AlterForeignServer) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	return withNoResolvedChildren(a, children)
}

func (d *DropForeignServer) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	return withNoResolvedChildren(d, children)
}

func (c *CreateUserMapping) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	return withNoResolvedChildren(c, children)
}

func (a *AlterUserMapping) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	return withNoResolvedChildren(a, children)
}

func (d *DropUserMapping) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	return withNoResolvedChildren(d, children)
}

func (i *ImportForeignSchema) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	return withNoResolvedChildren(i, children)
}
