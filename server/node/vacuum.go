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
	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
	"github.com/dolthub/doltgresql/server/auth"
)

// Vacuum implements VACUUM as a PostgreSQL utility statement.
type Vacuum struct {
	Tables []doltdb.TableName
}

var _ sql.ExecSourceRel = (*Vacuum)(nil)
var _ vitess.Injectable = (*Vacuum)(nil)

// NewVacuum returns a new *Vacuum.
func NewVacuum(tables []doltdb.TableName) *Vacuum {
	return &Vacuum{Tables: tables}
}

// Children implements the interface sql.ExecSourceRel.
func (v *Vacuum) Children() []sql.Node {
	return nil
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (v *Vacuum) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (v *Vacuum) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (v *Vacuum) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	if ctx.GetIgnoreAutoCommit() {
		return nil, pgerror.New(pgcode.ActiveSQLTransaction, "VACUUM cannot run inside a transaction block")
	}
	for _, table := range v.Tables {
		schemaName, err := core.GetSchemaName(ctx, nil, table.Schema)
		if err != nil {
			return nil, err
		}
		relationType, err := core.GetRelationType(ctx, schemaName, table.Name)
		if err != nil {
			return nil, err
		}
		if relationType == core.RelationType_DoesNotExist {
			return nil, sql.ErrTableNotFound.New(table.Name)
		}
		if err = checkVacuumTablePrivilege(ctx, doltdb.TableName{Schema: schemaName, Name: table.Name}); err != nil {
			return nil, err
		}
	}
	return sql.RowsToRowIter(), nil
}

// Schema implements the interface sql.ExecSourceRel.
func (v *Vacuum) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (v *Vacuum) String() string {
	return "VACUUM"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (v *Vacuum) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(v, children...)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (v *Vacuum) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return v, nil
}

func checkVacuumTablePrivilege(ctx *sql.Context, table doltdb.TableName) error {
	var allowed bool
	auth.LockRead(func() {
		role := auth.GetRole(ctx.Client().User)
		if !role.IsValid() {
			return
		}
		allowed = role.IsSuperUser || auth.HasTablePrivilege(auth.TablePrivilegeKey{
			Role:  role.ID(),
			Table: table,
		}, auth.Privilege_DROP) || auth.HasTablePrivilege(auth.TablePrivilegeKey{
			Role:  role.ID(),
			Table: table,
		}, auth.Privilege_MAINTAIN) || auth.HasInheritedRole(role.ID(), "pg_maintain")
	})
	if !allowed {
		return errors.Errorf("permission denied for table %s", table.Name)
	}
	return nil
}
