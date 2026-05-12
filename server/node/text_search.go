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
	"github.com/dolthub/go-mysql-server/sql/plan"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/auth"
)

// CreateTextSearchConfiguration implements CREATE TEXT SEARCH CONFIGURATION ... (COPY = ...).
type CreateTextSearchConfiguration struct {
	Name string
}

var _ sql.ExecSourceRel = (*CreateTextSearchConfiguration)(nil)
var _ vitess.Injectable = (*CreateTextSearchConfiguration)(nil)

// NewCreateTextSearchConfiguration returns a new *CreateTextSearchConfiguration.
func NewCreateTextSearchConfiguration(name string) *CreateTextSearchConfiguration {
	return &CreateTextSearchConfiguration{Name: name}
}

// Children implements the interface sql.ExecSourceRel.
func (c *CreateTextSearchConfiguration) Children() []sql.Node { return nil }

// IsReadOnly implements the interface sql.ExecSourceRel.
func (c *CreateTextSearchConfiguration) IsReadOnly() bool { return false }

// Resolved implements the interface sql.ExecSourceRel.
func (c *CreateTextSearchConfiguration) Resolved() bool { return true }

// RowIter implements the interface sql.ExecSourceRel.
func (c *CreateTextSearchConfiguration) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	schemaName, err := core.GetCurrentSchema(ctx)
	if err != nil {
		return nil, err
	}
	if err = checkSchemaCreatePrivilege(ctx, schemaName); err != nil {
		return nil, err
	}
	auth.LockWrite(func() {
		auth.CreateTextSearchConfig(auth.TextSearchConfig{
			Name:      c.Name,
			Namespace: id.NewNamespace(schemaName),
		})
		err = auth.PersistChanges()
	})
	if err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

// Schema implements the interface sql.ExecSourceRel.
func (c *CreateTextSearchConfiguration) Schema(ctx *sql.Context) sql.Schema { return nil }

// String implements the interface sql.ExecSourceRel.
func (c *CreateTextSearchConfiguration) String() string { return "CREATE TEXT SEARCH CONFIGURATION" }

// WithChildren implements the interface sql.ExecSourceRel.
func (c *CreateTextSearchConfiguration) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(c, children...)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (c *CreateTextSearchConfiguration) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return c, nil
}
