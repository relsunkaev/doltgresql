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

	"github.com/dolthub/go-mysql-server/sql"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"
)

type ListenStatement struct {
	Channel string
}

type UnlistenStatement struct {
	Channel string
	All     bool
}

type NotifyStatement struct {
	Channel string
	Payload string
}

var _ vitess.Injectable = ListenStatement{}
var _ vitess.Injectable = UnlistenStatement{}
var _ vitess.Injectable = NotifyStatement{}
var _ sql.ExecSourceRel = ListenStatement{}
var _ sql.ExecSourceRel = UnlistenStatement{}
var _ sql.ExecSourceRel = NotifyStatement{}

func (n ListenStatement) Children() []sql.Node { return nil }

func (n ListenStatement) IsReadOnly() bool { return true }

func (n ListenStatement) Resolved() bool { return true }

func (n ListenStatement) RowIter(ctx *sql.Context, r sql.Row) (sql.RowIter, error) {
	panic("LISTEN should be handled by the connection handler")
}

func (n ListenStatement) Schema(ctx *sql.Context) sql.Schema { return nil }

func (n ListenStatement) String() string { return "LISTEN " + n.Channel }

func (n ListenStatement) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return n, nil
}

func (n ListenStatement) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return n, nil
}

func (n UnlistenStatement) Children() []sql.Node { return nil }

func (n UnlistenStatement) IsReadOnly() bool { return true }

func (n UnlistenStatement) Resolved() bool { return true }

func (n UnlistenStatement) RowIter(ctx *sql.Context, r sql.Row) (sql.RowIter, error) {
	panic("UNLISTEN should be handled by the connection handler")
}

func (n UnlistenStatement) Schema(ctx *sql.Context) sql.Schema { return nil }

func (n UnlistenStatement) String() string {
	if n.All {
		return "UNLISTEN *"
	}
	return "UNLISTEN " + n.Channel
}

func (n UnlistenStatement) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return n, nil
}

func (n UnlistenStatement) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return n, nil
}

func (n NotifyStatement) Children() []sql.Node { return nil }

func (n NotifyStatement) IsReadOnly() bool { return true }

func (n NotifyStatement) Resolved() bool { return true }

func (n NotifyStatement) RowIter(ctx *sql.Context, r sql.Row) (sql.RowIter, error) {
	panic("NOTIFY should be handled by the connection handler")
}

func (n NotifyStatement) Schema(ctx *sql.Context) sql.Schema { return nil }

func (n NotifyStatement) String() string { return "NOTIFY " + n.Channel }

func (n NotifyStatement) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return n, nil
}

func (n NotifyStatement) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return n, nil
}
