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

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
	"github.com/dolthub/doltgresql/server/rowsecurity"
)

// AlterTableRowSecurity implements ALTER TABLE ... ROW LEVEL SECURITY.
type AlterTableRowSecurity struct {
	TableName doltdb.TableName
	Enabled   *bool
	Forced    *bool
}

var _ sql.ExecSourceRel = (*AlterTableRowSecurity)(nil)
var _ vitess.Injectable = (*AlterTableRowSecurity)(nil)

// NewAlterTableRowSecurity returns a new row-security mode node.
func NewAlterTableRowSecurity(tableName doltdb.TableName, enabled *bool, forced *bool) *AlterTableRowSecurity {
	return &AlterTableRowSecurity{TableName: tableName, Enabled: enabled, Forced: forced}
}

func (a *AlterTableRowSecurity) Children() []sql.Node { return nil }
func (a *AlterTableRowSecurity) IsReadOnly() bool     { return false }
func (a *AlterTableRowSecurity) Resolved() bool       { return true }
func (a *AlterTableRowSecurity) Schema(ctx *sql.Context) sql.Schema {
	return nil
}
func (a *AlterTableRowSecurity) String() string { return "ALTER TABLE ROW LEVEL SECURITY" }

func (a *AlterTableRowSecurity) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	if err := checkTableOwnership(ctx, a.TableName); err != nil {
		return nil, err
	}
	rowsecurity.SetTableMode(ctx.GetCurrentDatabase(), a.TableName.Schema, a.TableName.Name, a.Enabled, a.Forced)
	return sql.RowsToRowIter(), nil
}

func (a *AlterTableRowSecurity) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(a, children...)
}

func (a *AlterTableRowSecurity) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return a, nil
}

// CreatePolicy implements the supported subset of CREATE POLICY.
type CreatePolicy struct {
	TableName doltdb.TableName
	Policy    rowsecurity.Policy
}

var _ sql.ExecSourceRel = (*CreatePolicy)(nil)
var _ vitess.Injectable = (*CreatePolicy)(nil)

// NewCreatePolicy returns a new CREATE POLICY node.
func NewCreatePolicy(tableName doltdb.TableName, policy rowsecurity.Policy) *CreatePolicy {
	return &CreatePolicy{TableName: tableName, Policy: policy}
}

func (c *CreatePolicy) Children() []sql.Node { return nil }
func (c *CreatePolicy) IsReadOnly() bool     { return false }
func (c *CreatePolicy) Resolved() bool       { return true }
func (c *CreatePolicy) Schema(ctx *sql.Context) sql.Schema {
	return nil
}
func (c *CreatePolicy) String() string { return "CREATE POLICY" }

func (c *CreatePolicy) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	if err := checkTableOwnership(ctx, c.TableName); err != nil {
		return nil, err
	}
	if !rowsecurity.AddPolicy(ctx.GetCurrentDatabase(), c.TableName.Schema, c.TableName.Name, c.Policy) {
		return nil, pgerror.Newf(pgcode.DuplicateObject,
			`policy "%s" for table "%s" already exists`,
			c.Policy.Name,
			c.TableName.Name,
		)
	}
	return sql.RowsToRowIter(), nil
}

func (c *CreatePolicy) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(c, children...)
}

func (c *CreatePolicy) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return c, nil
}
