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

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/auth"
	"github.com/dolthub/doltgresql/server/comments"
)

// CreateTextSearchConfiguration implements CREATE TEXT SEARCH CONFIGURATION ... (COPY = ...).
type CreateTextSearchConfiguration struct {
	Namespace string
	Name      string
}

var _ sql.ExecSourceRel = (*CreateTextSearchConfiguration)(nil)
var _ vitess.Injectable = (*CreateTextSearchConfiguration)(nil)

// NewCreateTextSearchConfiguration returns a new *CreateTextSearchConfiguration.
func NewCreateTextSearchConfiguration(namespace string, name string) *CreateTextSearchConfiguration {
	return &CreateTextSearchConfiguration{Namespace: namespace, Name: name}
}

// Children implements the interface sql.ExecSourceRel.
func (c *CreateTextSearchConfiguration) Children() []sql.Node { return nil }

// IsReadOnly implements the interface sql.ExecSourceRel.
func (c *CreateTextSearchConfiguration) IsReadOnly() bool { return false }

// Resolved implements the interface sql.ExecSourceRel.
func (c *CreateTextSearchConfiguration) Resolved() bool { return true }

// RowIter implements the interface sql.ExecSourceRel.
func (c *CreateTextSearchConfiguration) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	schemaName := c.Namespace
	var err error
	if schemaName == "" {
		schemaName, err = core.GetCurrentSchema(ctx)
		if err != nil {
			return nil, err
		}
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

// DropTextSearchConfiguration implements DROP TEXT SEARCH CONFIGURATION.
type DropTextSearchConfiguration struct {
	Namespace string
	Name      string
	IfExists  bool
}

var _ sql.ExecSourceRel = (*DropTextSearchConfiguration)(nil)
var _ vitess.Injectable = (*DropTextSearchConfiguration)(nil)

// NewDropTextSearchConfiguration returns a new *DropTextSearchConfiguration.
func NewDropTextSearchConfiguration(namespace string, name string, ifExists bool) *DropTextSearchConfiguration {
	return &DropTextSearchConfiguration{Namespace: namespace, Name: name, IfExists: ifExists}
}

// Children implements the interface sql.ExecSourceRel.
func (d *DropTextSearchConfiguration) Children() []sql.Node { return nil }

// IsReadOnly implements the interface sql.ExecSourceRel.
func (d *DropTextSearchConfiguration) IsReadOnly() bool { return false }

// Resolved implements the interface sql.ExecSourceRel.
func (d *DropTextSearchConfiguration) Resolved() bool { return true }

// RowIter implements the interface sql.ExecSourceRel.
func (d *DropTextSearchConfiguration) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	schemaName := d.Namespace
	var err error
	if schemaName == "" {
		schemaName, err = core.GetCurrentSchema(ctx)
		if err != nil {
			return nil, err
		}
	}
	namespace := id.NewNamespace(schemaName)
	auth.LockWrite(func() {
		if ok := auth.DropTextSearchConfig(namespace, d.Name); !ok {
			if !d.IfExists {
				err = errors.Errorf(`text search configuration "%s" does not exist`, d.Name)
			}
			return
		}
		comments.RemoveObject(id.NewId(id.Section_TextSearchConfig, schemaName, d.Name), "pg_ts_config")
		err = auth.PersistChanges()
	})
	if err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

// Schema implements the interface sql.ExecSourceRel.
func (d *DropTextSearchConfiguration) Schema(ctx *sql.Context) sql.Schema { return nil }

// String implements the interface sql.ExecSourceRel.
func (d *DropTextSearchConfiguration) String() string { return "DROP TEXT SEARCH CONFIGURATION" }

// WithChildren implements the interface sql.ExecSourceRel.
func (d *DropTextSearchConfiguration) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(d, children...)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (d *DropTextSearchConfiguration) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return d, nil
}
