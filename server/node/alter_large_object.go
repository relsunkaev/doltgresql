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

	"github.com/dolthub/doltgresql/server/auth"
	"github.com/dolthub/doltgresql/server/largeobject"
)

// AlterLargeObject implements ALTER LARGE OBJECT.
type AlterLargeObject struct {
	OID   uint32
	Owner string
}

var _ sql.ExecSourceRel = (*AlterLargeObject)(nil)
var _ vitess.Injectable = (*AlterLargeObject)(nil)

// NewAlterLargeObjectOwner returns a new *AlterLargeObject for ALTER LARGE OBJECT ... OWNER TO.
func NewAlterLargeObjectOwner(oid uint32, owner string) *AlterLargeObject {
	return &AlterLargeObject{OID: oid, Owner: owner}
}

// Children implements the interface sql.ExecSourceRel.
func (a *AlterLargeObject) Children() []sql.Node {
	return nil
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (a *AlterLargeObject) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (a *AlterLargeObject) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (a *AlterLargeObject) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	var roleExists bool
	auth.LockRead(func() {
		roleExists = auth.RoleExists(a.Owner)
	})
	if !roleExists {
		return nil, errors.Errorf(`role "%s" does not exist`, a.Owner)
	}
	if err := largeobject.AlterOwner(a.OID, a.Owner); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

// Schema implements the interface sql.ExecSourceRel.
func (a *AlterLargeObject) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (a *AlterLargeObject) String() string {
	return "ALTER LARGE OBJECT"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (a *AlterLargeObject) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(a, children...)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (a *AlterLargeObject) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return a, nil
}
