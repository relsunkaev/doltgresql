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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/auth"
)

// DropView is a node that implements functionality specifically relevant to Doltgres' view dropping needs.
type DropView struct {
	gmsDropView *plan.DropView
}

var _ sql.ExecBuilderNode = (*DropView)(nil)

// NewDropView returns a new *DropView.
func NewDropView(dropView *plan.DropView) *DropView {
	return &DropView{gmsDropView: dropView}
}

// Children implements the interface sql.ExecBuilderNode.
func (d *DropView) Children() []sql.Node {
	return d.gmsDropView.Children()
}

// IsReadOnly implements the interface sql.ExecBuilderNode.
func (d *DropView) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecBuilderNode.
func (d *DropView) Resolved() bool {
	return d.gmsDropView != nil && d.gmsDropView.Resolved()
}

// BuildRowIter implements the interface sql.ExecBuilderNode.
func (d *DropView) BuildRowIter(ctx *sql.Context, b sql.NodeExecBuilder, r sql.Row) (sql.RowIter, error) {
	targets := make([]dropViewTarget, 0, len(d.gmsDropView.Children()))
	for _, child := range d.gmsDropView.Children() {
		drop, ok := child.(*plan.SingleDropView)
		if !ok {
			return nil, plan.ErrDropViewChild.New()
		}
		exists, err := dropViewExists(ctx, drop)
		if err != nil {
			return nil, err
		}
		if !exists {
			if d.gmsDropView.IfExists {
				continue
			}
			return nil, sql.ErrViewDoesNotExist.New(drop.Database().Name(), drop.ViewName)
		}

		schemaName, err := core.GetSchemaName(ctx, drop.Database(), "")
		if err != nil {
			return nil, err
		}
		if err = checkViewOwnership(ctx, drop.ViewName); err != nil {
			return nil, errors.Wrap(err, "permission denied")
		}

		viewID := id.NewView(schemaName, drop.ViewName).AsId()
		if err = id.ValidateOperation(ctx, id.Section_View, id.Operation_Delete, drop.Database().Name(), viewID, id.Null); err != nil {
			return nil, err
		}
		targets = append(targets, dropViewTarget{dbName: drop.Database().Name(), viewID: viewID})
	}

	dropViewIter, err := b.Build(ctx, d.gmsDropView, r)
	if err != nil {
		return nil, err
	}

	for _, target := range targets {
		if err = id.PerformOperation(ctx, id.Section_View, id.Operation_Delete, target.dbName, target.viewID, id.Null); err != nil {
			return nil, err
		}
	}
	return dropViewIter, nil
}

type dropViewTarget struct {
	dbName string
	viewID id.Id
}

func dropViewExists(ctx *sql.Context, drop *plan.SingleDropView) (bool, error) {
	if viewDatabase, ok := drop.Database().(sql.ViewDatabase); ok {
		_, exists, err := viewDatabase.GetViewDefinition(ctx, drop.ViewName)
		return exists, err
	}
	return ctx.GetViewRegistry().Exists(drop.Database().Name(), drop.ViewName), nil
}

func checkViewOwnership(ctx *sql.Context, viewName string) error {
	owner, _ := auth.GetSuperUserAndPassword()
	if owner == "" {
		owner = "postgres"
	}
	if owner == "" || owner == ctx.Client().User {
		return nil
	}
	var userRole auth.Role
	auth.LockRead(func() {
		userRole = auth.GetRole(ctx.Client().User)
	})
	if userRole.IsValid() && userRole.IsSuperUser {
		return nil
	}
	return errors.Errorf("must be owner of view %s", viewName)
}

// Schema implements the interface sql.ExecBuilderNode.
func (d *DropView) Schema(ctx *sql.Context) sql.Schema {
	return d.gmsDropView.Schema(ctx)
}

// String implements the interface sql.ExecBuilderNode.
func (d *DropView) String() string {
	return d.gmsDropView.String()
}

// WithChildren implements the interface sql.ExecBuilderNode.
func (d *DropView) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	gmsDropView, err := d.gmsDropView.WithChildren(ctx, children...)
	if err != nil {
		return nil, err
	}
	return &DropView{gmsDropView: gmsDropView.(*plan.DropView)}, nil
}
