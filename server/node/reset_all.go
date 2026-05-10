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
	"io"

	"github.com/dolthub/go-mysql-server/sql"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/server/config"
)

type ResetAllStatement struct{}

var _ vitess.Injectable = ResetAllStatement{}
var _ sql.ExecSourceRel = ResetAllStatement{}

func (r ResetAllStatement) Children() []sql.Node {
	return nil
}

func (r ResetAllStatement) IsReadOnly() bool {
	return false
}

func (r ResetAllStatement) Resolved() bool {
	return true
}

func (r ResetAllStatement) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	if err := config.ResetAllSessionVariables(ctx); err != nil {
		return nil, err
	}
	return resetAllRowIter{}, nil
}

func (r ResetAllStatement) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

func (r ResetAllStatement) String() string {
	return "RESET ALL"
}

func (r ResetAllStatement) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return r, nil
}

func (r ResetAllStatement) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return r, nil
}

type resetAllRowIter struct{}

func (r resetAllRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	return nil, io.EOF
}

func (r resetAllRowIter) Close(ctx *sql.Context) error {
	return nil
}
