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

	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/auth"
)

// CreateCast implements CREATE CAST.
type CreateCast struct {
	SourceType string
	TargetType string
	Function   string
}

var _ sql.ExecSourceRel = (*CreateCast)(nil)
var _ vitess.Injectable = (*CreateCast)(nil)

// NewCreateCast returns a new *CreateCast.
func NewCreateCast(sourceType string, targetType string, function string) *CreateCast {
	return &CreateCast{SourceType: sourceType, TargetType: targetType, Function: function}
}

// Children implements the interface sql.ExecSourceRel.
func (c *CreateCast) Children() []sql.Node { return nil }

// IsReadOnly implements the interface sql.ExecSourceRel.
func (c *CreateCast) IsReadOnly() bool { return false }

// Resolved implements the interface sql.ExecSourceRel.
func (c *CreateCast) Resolved() bool { return true }

// RowIter implements the interface sql.ExecSourceRel.
func (c *CreateCast) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	sourceID, err := transformTypeID(ctx, c.SourceType)
	if err != nil {
		return nil, err
	}
	targetID, err := transformTypeID(ctx, c.TargetType)
	if err != nil {
		return nil, err
	}
	auth.LockWrite(func() {
		err = auth.CreateCast(auth.Cast{
			SourceType: id.Type(sourceID),
			TargetType: id.Type(targetID),
			Function:   c.Function,
		})
		if err == nil {
			err = auth.PersistChanges()
		}
	})
	if err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

// Schema implements the interface sql.ExecSourceRel.
func (c *CreateCast) Schema(ctx *sql.Context) sql.Schema { return nil }

// String implements the interface sql.ExecSourceRel.
func (c *CreateCast) String() string { return "CREATE CAST" }

// WithChildren implements the interface sql.ExecSourceRel.
func (c *CreateCast) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(c, children...)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (c *CreateCast) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return c, nil
}

// DropCast implements DROP CAST.
type DropCast struct {
	SourceType string
	TargetType string
	IfExists   bool
}

var _ sql.ExecSourceRel = (*DropCast)(nil)
var _ vitess.Injectable = (*DropCast)(nil)

// NewDropCast returns a new *DropCast.
func NewDropCast(sourceType string, targetType string, ifExists bool) *DropCast {
	return &DropCast{SourceType: sourceType, TargetType: targetType, IfExists: ifExists}
}

// Children implements the interface sql.ExecSourceRel.
func (d *DropCast) Children() []sql.Node { return nil }

// IsReadOnly implements the interface sql.ExecSourceRel.
func (d *DropCast) IsReadOnly() bool { return false }

// Resolved implements the interface sql.ExecSourceRel.
func (d *DropCast) Resolved() bool { return true }

// RowIter implements the interface sql.ExecSourceRel.
func (d *DropCast) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	sourceID, err := transformTypeID(ctx, d.SourceType)
	if err != nil {
		if d.IfExists {
			return sql.RowsToRowIter(), nil
		}
		return nil, err
	}
	targetID, err := transformTypeID(ctx, d.TargetType)
	if err != nil {
		if d.IfExists {
			return sql.RowsToRowIter(), nil
		}
		return nil, err
	}
	auth.LockWrite(func() {
		if ok := auth.DropCast(id.Type(sourceID), id.Type(targetID)); !ok && !d.IfExists {
			err = errors.New("cast does not exist")
			return
		}
		err = auth.PersistChanges()
	})
	if err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

// Schema implements the interface sql.ExecSourceRel.
func (d *DropCast) Schema(ctx *sql.Context) sql.Schema { return nil }

// String implements the interface sql.ExecSourceRel.
func (d *DropCast) String() string { return "DROP CAST" }

// WithChildren implements the interface sql.ExecSourceRel.
func (d *DropCast) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(d, children...)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (d *DropCast) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return d, nil
}
