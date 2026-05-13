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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/comments"
)

// DropCheck is a node that implements functionality specifically relevant to Doltgres' check constraint drops.
type DropCheck struct {
	gmsDropCheck *plan.DropCheck
}

var _ sql.ExecBuilderNode = (*DropCheck)(nil)

// NewDropCheck returns a new *DropCheck.
func NewDropCheck(dropCheck *plan.DropCheck) *DropCheck {
	return &DropCheck{gmsDropCheck: dropCheck}
}

// Children implements the interface sql.ExecBuilderNode.
func (d *DropCheck) Children() []sql.Node {
	return d.gmsDropCheck.Children()
}

// IsReadOnly implements the interface sql.ExecBuilderNode.
func (d *DropCheck) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecBuilderNode.
func (d *DropCheck) Resolved() bool {
	return d.gmsDropCheck != nil && d.gmsDropCheck.Resolved()
}

// BuildRowIter implements the interface sql.ExecBuilderNode.
func (d *DropCheck) BuildRowIter(ctx *sql.Context, b sql.NodeExecBuilder, r sql.Row) (sql.RowIter, error) {
	dropCheckIter, err := b.Build(ctx, d.gmsDropCheck, r)
	if err != nil {
		return nil, err
	}

	schemaName, err := core.GetSchemaName(ctx, d.gmsDropCheck.Table.Database(), "")
	if err != nil {
		return nil, err
	}
	checkID := id.NewCheck(schemaName, d.gmsDropCheck.Table.Name(), d.gmsDropCheck.Name).AsId()
	comments.RemoveObject(checkID, "pg_constraint")
	return dropCheckIter, nil
}

// Schema implements the interface sql.ExecBuilderNode.
func (d *DropCheck) Schema(ctx *sql.Context) sql.Schema {
	return d.gmsDropCheck.Schema(ctx)
}

// String implements the interface sql.ExecBuilderNode.
func (d *DropCheck) String() string {
	return d.gmsDropCheck.String()
}

// WithChildren implements the interface sql.ExecBuilderNode.
func (d *DropCheck) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	gmsDropCheck, err := d.gmsDropCheck.WithChildren(ctx, children...)
	if err != nil {
		return nil, err
	}
	return &DropCheck{gmsDropCheck: gmsDropCheck.(*plan.DropCheck)}, nil
}
