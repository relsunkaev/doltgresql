// Copyright 2025 Dolthub, Inc.
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
	"io"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/go-mysql-server/sql"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/jackc/pgx/v5/pgproto3"
)

var _ vitess.Injectable = (*NoOp)(nil)
var _ sql.ExecSourceRel = (*NoOp)(nil)

// NoOp is a node that does nothing and issues zero or more messages when run.
// Used when a statement should parse but isn't expected to do anything, for compatibility with Postgres dumps / tools.
type NoOp struct {
	Warnings []string
	// Severity is the PostgreSQL severity used when sending each message to the
	// client. An empty value falls back to "WARNING" so existing call sites that
	// don't care about severity keep their historical behaviour.
	Severity string
}

func (n NoOp) Resolved() bool {
	return true
}

func (n NoOp) String() string {
	return fmt.Sprintf("%v", n.Warnings)
}

func (n NoOp) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

func (n NoOp) Children() []sql.Node {
	return nil
}

func (n NoOp) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return n, nil
}

func (n NoOp) IsReadOnly() bool {
	return true
}

func (n NoOp) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	return n, nil
}

type noOpRowIter struct {
	warnings []string
	severity string
}

func (n noOpRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	return nil, io.EOF
}

func (n noOpRowIter) Close(ctx *sql.Context) error {
	severity := n.severity
	if severity == "" {
		severity = "WARNING"
	}
	for _, warning := range n.warnings {
		noticeResponse := &pgproto3.NoticeResponse{
			Severity: severity,
			Message:  warning,
		}
		sess := dsess.DSessFromSess(ctx.Session)
		sess.Notice(noticeResponse)
	}
	return nil
}

func (n NoOp) RowIter(ctx *sql.Context, r sql.Row) (sql.RowIter, error) {
	return noOpRowIter{warnings: n.Warnings, severity: n.Severity}, nil
}
