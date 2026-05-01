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
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"
)

// ExecuteStatement is handled by the connection handler, which owns prepared statement lifecycle.
type ExecuteStatement struct {
	Name        string
	Params      []string
	DiscardRows bool
}

var _ vitess.Injectable = ExecuteStatement{}
var _ sql.ExecSourceRel = ExecuteStatement{}

// Children implements the interface sql.ExecSourceRel.
func (e ExecuteStatement) Children() []sql.Node {
	return nil
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (e ExecuteStatement) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (e ExecuteStatement) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (e ExecuteStatement) RowIter(ctx *sql.Context, r sql.Row) (sql.RowIter, error) {
	panic("EXECUTE should be handled by the connection handler")
}

// Schema implements the interface sql.ExecSourceRel.
func (e ExecuteStatement) Schema() sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (e ExecuteStatement) String() string {
	if len(e.Params) == 0 {
		return fmt.Sprintf("EXECUTE %s", e.Name)
	}
	return fmt.Sprintf("EXECUTE %s(%s)", e.Name, strings.Join(e.Params, ", "))
}

// WithChildren implements the interface sql.ExecSourceRel.
func (e ExecuteStatement) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(e, len(children), 0)
	}
	return e, nil
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (e ExecuteStatement) WithResolvedChildren(children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return e, nil
}
