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

// PrepareStatement is handled by the connection handler, which owns prepared statement lifecycle.
type PrepareStatement struct {
	Name           string
	Statement      string
	ParameterTypes []string
}

var _ vitess.Injectable = PrepareStatement{}
var _ sql.ExecSourceRel = PrepareStatement{}

// Children implements the interface sql.ExecSourceRel.
func (p PrepareStatement) Children() []sql.Node {
	return nil
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (p PrepareStatement) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (p PrepareStatement) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (p PrepareStatement) RowIter(ctx *sql.Context, r sql.Row) (sql.RowIter, error) {
	panic("PREPARE should be handled by the connection handler")
}

// Schema implements the interface sql.ExecSourceRel.
func (p PrepareStatement) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (p PrepareStatement) String() string {
	return fmt.Sprintf("PREPARE %s AS %s", p.Name, p.Statement)
}

// WithChildren implements the interface sql.ExecSourceRel.
func (p PrepareStatement) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 0)
	}
	return p, nil
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (p PrepareStatement) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return p, nil
}
