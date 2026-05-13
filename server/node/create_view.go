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
	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/server/auth"
)

// CreateView wraps go-mysql-server's CREATE VIEW node so Doltgres can record
// PostgreSQL relation ownership metadata after the view is created.
type CreateView struct {
	gmsCreateView *plan.CreateView
}

var _ sql.ExecBuilderNode = (*CreateView)(nil)

// NewCreateView returns a new *CreateView.
func NewCreateView(createView *plan.CreateView) *CreateView {
	return &CreateView{gmsCreateView: createView}
}

// Children implements the interface sql.ExecBuilderNode.
func (c *CreateView) Children() []sql.Node {
	return []sql.Node{c.gmsCreateView}
}

// IsReadOnly implements the interface sql.ExecBuilderNode.
func (c *CreateView) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecBuilderNode.
func (c *CreateView) Resolved() bool {
	return c.gmsCreateView != nil && c.gmsCreateView.Resolved()
}

// BuildRowIter implements the interface sql.ExecBuilderNode.
func (c *CreateView) BuildRowIter(ctx *sql.Context, b sql.NodeExecBuilder, r sql.Row) (sql.RowIter, error) {
	existed, err := viewExists(ctx, c.gmsCreateView)
	if err != nil {
		return nil, err
	}
	if !existed {
		schemaName, err := core.GetSchemaName(ctx, c.gmsCreateView.Database(), "")
		if err != nil {
			return nil, err
		}
		if err = checkSchemaCreatePrivilege(ctx, schemaName); err != nil {
			return nil, err
		}
	}
	iter, err := b.Build(ctx, c.gmsCreateView, r)
	if err != nil {
		return nil, err
	}
	if !existed {
		if err = recordCreatedViewOwner(ctx, c.gmsCreateView); err != nil {
			if iter != nil {
				_ = iter.Close(ctx)
			}
			return nil, err
		}
	}
	return iter, nil
}

// Schema implements the interface sql.ExecBuilderNode.
func (c *CreateView) Schema(ctx *sql.Context) sql.Schema {
	return c.gmsCreateView.Schema(ctx)
}

// String implements the interface sql.ExecBuilderNode.
func (c *CreateView) String() string {
	return c.gmsCreateView.String()
}

// WithChildren implements the interface sql.ExecBuilderNode.
func (c *CreateView) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 1)
	}
	createView, ok := children[0].(*plan.CreateView)
	if !ok {
		return nil, errors.Errorf("expected child to be *plan.CreateView but found %T", children[0])
	}
	return NewCreateView(createView), nil
}

func viewExists(ctx *sql.Context, createView *plan.CreateView) (bool, error) {
	if viewDB, ok := createView.Database().(sql.ViewDatabase); ok {
		_, exists, err := viewDB.GetViewDefinition(ctx, createView.Name)
		return exists, err
	}
	_, exists := ctx.GetViewRegistry().View(createView.Database().Name(), createView.Name)
	return exists, nil
}

func recordCreatedViewOwner(ctx *sql.Context, createView *plan.CreateView) error {
	user := ctx.Client().User
	if user == "" {
		return nil
	}
	schemaName, err := core.GetSchemaName(ctx, createView.Database(), "")
	if err != nil {
		return err
	}
	auth.SetRelationOwner(doltdb.TableName{Name: createView.Name, Schema: schemaName}, user)
	return nil
}
