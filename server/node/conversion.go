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
	"github.com/dolthub/doltgresql/server/auth"
)

// CreateConversion implements CREATE CONVERSION.
type CreateConversion struct {
	Name        string
	Namespace   string
	ForEncoding int32
	ToEncoding  int32
	Proc        string
	Default     bool
}

var _ sql.ExecSourceRel = (*CreateConversion)(nil)
var _ vitess.Injectable = (*CreateConversion)(nil)

// NewCreateConversion returns a new *CreateConversion.
func NewCreateConversion(name string, namespace string, forEncoding int32, toEncoding int32, proc string, isDefault bool) *CreateConversion {
	return &CreateConversion{
		Name:        name,
		Namespace:   namespace,
		ForEncoding: forEncoding,
		ToEncoding:  toEncoding,
		Proc:        proc,
		Default:     isDefault,
	}
}

// Children implements the interface sql.ExecSourceRel.
func (c *CreateConversion) Children() []sql.Node {
	return nil
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (c *CreateConversion) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (c *CreateConversion) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (c *CreateConversion) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	namespace := c.Namespace
	if namespace == "" {
		searchPath, err := core.SearchPath(ctx)
		if err != nil {
			return nil, err
		}
		if len(searchPath) > 0 {
			namespace = searchPath[0]
		}
	}
	var err error
	auth.LockWrite(func() {
		err = auth.CreateConversion(auth.Conversion{
			Name:        c.Name,
			Namespace:   namespace,
			Owner:       ctx.Client().User,
			ForEncoding: c.ForEncoding,
			ToEncoding:  c.ToEncoding,
			Proc:        c.Proc,
			Default:     c.Default,
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
func (c *CreateConversion) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (c *CreateConversion) String() string {
	return "CREATE CONVERSION"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (c *CreateConversion) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(c, children...)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (c *CreateConversion) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return c, nil
}

// DropConversion implements DROP CONVERSION.
type DropConversion struct {
	Name      string
	Namespace string
	IfExists  bool
}

var _ sql.ExecSourceRel = (*DropConversion)(nil)
var _ vitess.Injectable = (*DropConversion)(nil)

// NewDropConversion returns a new *DropConversion.
func NewDropConversion(name string, namespace string, ifExists bool) *DropConversion {
	return &DropConversion{Name: name, Namespace: namespace, IfExists: ifExists}
}

// Children implements the interface sql.ExecSourceRel.
func (d *DropConversion) Children() []sql.Node {
	return nil
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (d *DropConversion) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (d *DropConversion) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (d *DropConversion) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	namespace := d.Namespace
	if namespace == "" {
		searchPath, err := core.SearchPath(ctx)
		if err != nil {
			return nil, err
		}
		if len(searchPath) > 0 {
			namespace = searchPath[0]
		}
	}
	var err error
	auth.LockWrite(func() {
		if ok := auth.DropConversion(namespace, d.Name); !ok && !d.IfExists {
			err = errors.Errorf(`conversion "%s" does not exist`, d.Name)
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
func (d *DropConversion) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (d *DropConversion) String() string {
	return "DROP CONVERSION"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (d *DropConversion) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(d, children...)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (d *DropConversion) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return d, nil
}
