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

	"github.com/dolthub/go-mysql-server/sql"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"
)

// PrepareTransaction is handled by the connection handler, which owns transaction lifecycle.
type PrepareTransaction struct {
	GID string
}

var _ vitess.Injectable = PrepareTransaction{}
var _ sql.ExecSourceRel = PrepareTransaction{}

func (p PrepareTransaction) Children() []sql.Node               { return nil }
func (p PrepareTransaction) IsReadOnly() bool                   { return false }
func (p PrepareTransaction) Resolved() bool                     { return true }
func (p PrepareTransaction) Schema(ctx *sql.Context) sql.Schema { return nil }
func (p PrepareTransaction) String() string                     { return fmt.Sprintf("PREPARE TRANSACTION %q", p.GID) }

func (p PrepareTransaction) RowIter(ctx *sql.Context, r sql.Row) (sql.RowIter, error) {
	panic("PREPARE TRANSACTION should be handled by the connection handler")
}

func (p PrepareTransaction) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 0)
	}
	return p, nil
}

func (p PrepareTransaction) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return p, nil
}

// CommitPrepared is handled by the connection handler, which owns transaction lifecycle.
type CommitPrepared struct {
	GID string
}

var _ vitess.Injectable = CommitPrepared{}
var _ sql.ExecSourceRel = CommitPrepared{}

func (c CommitPrepared) Children() []sql.Node               { return nil }
func (c CommitPrepared) IsReadOnly() bool                   { return false }
func (c CommitPrepared) Resolved() bool                     { return true }
func (c CommitPrepared) Schema(ctx *sql.Context) sql.Schema { return nil }
func (c CommitPrepared) String() string                     { return fmt.Sprintf("COMMIT PREPARED %q", c.GID) }

func (c CommitPrepared) RowIter(ctx *sql.Context, r sql.Row) (sql.RowIter, error) {
	panic("COMMIT PREPARED should be handled by the connection handler")
}

func (c CommitPrepared) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 0)
	}
	return c, nil
}

func (c CommitPrepared) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return c, nil
}

// RollbackPrepared is handled by the connection handler, which owns transaction lifecycle.
type RollbackPrepared struct {
	GID string
}

var _ vitess.Injectable = RollbackPrepared{}
var _ sql.ExecSourceRel = RollbackPrepared{}

func (r RollbackPrepared) Children() []sql.Node               { return nil }
func (r RollbackPrepared) IsReadOnly() bool                   { return false }
func (r RollbackPrepared) Resolved() bool                     { return true }
func (r RollbackPrepared) Schema(ctx *sql.Context) sql.Schema { return nil }
func (r RollbackPrepared) String() string                     { return fmt.Sprintf("ROLLBACK PREPARED %q", r.GID) }

func (r RollbackPrepared) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	panic("ROLLBACK PREPARED should be handled by the connection handler")
}

func (r RollbackPrepared) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(r, len(children), 0)
	}
	return r, nil
}

func (r RollbackPrepared) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return r, nil
}
