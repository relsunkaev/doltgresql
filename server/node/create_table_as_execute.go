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

// CreateTableAsExecuteStatement is handled by the connection handler, which
// owns SQL-level prepared statement lifecycle.
type CreateTableAsExecuteStatement struct {
	CreatePrefix string
	Execute      ExecuteStatement
	WithNoData   bool
}

var _ vitess.Injectable = CreateTableAsExecuteStatement{}
var _ sql.ExecSourceRel = CreateTableAsExecuteStatement{}

func (c CreateTableAsExecuteStatement) Children() []sql.Node {
	return nil
}

func (c CreateTableAsExecuteStatement) IsReadOnly() bool {
	return false
}

func (c CreateTableAsExecuteStatement) Resolved() bool {
	return true
}

func (c CreateTableAsExecuteStatement) RowIter(ctx *sql.Context, r sql.Row) (sql.RowIter, error) {
	panic("CREATE TABLE AS EXECUTE should be handled by the connection handler")
}

func (c CreateTableAsExecuteStatement) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

func (c CreateTableAsExecuteStatement) String() string {
	suffix := ""
	if c.WithNoData {
		suffix = " WITH NO DATA"
	}
	return fmt.Sprintf("%s%s%s", c.CreatePrefix, c.Execute.String(), suffix)
}

func (c CreateTableAsExecuteStatement) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 0)
	}
	return c, nil
}

func (c CreateTableAsExecuteStatement) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return c, nil
}
