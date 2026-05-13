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
	"github.com/dolthub/go-mysql-server/sql/types"
)

// CreateCheck wraps GMS check creation so non-enforced PostgreSQL checks are
// stored as metadata without validating existing table rows.
type CreateCheck struct {
	gmsCreateCheck         *plan.CreateCheck
	skipExistingValidation bool
	overrides              sql.EngineOverrides
}

var _ sql.ExecBuilderNode = (*CreateCheck)(nil)

// NewCreateCheck returns a new *CreateCheck.
func NewCreateCheck(createCheck *plan.CreateCheck, overrides sql.EngineOverrides, skipExistingValidation bool) *CreateCheck {
	return &CreateCheck{
		gmsCreateCheck:         createCheck,
		skipExistingValidation: skipExistingValidation,
		overrides:              overrides,
	}
}

// Children implements sql.ExecBuilderNode.
func (c *CreateCheck) Children() []sql.Node {
	return c.gmsCreateCheck.Children()
}

// IsReadOnly implements sql.ExecBuilderNode.
func (c *CreateCheck) IsReadOnly() bool {
	return false
}

// Resolved implements sql.ExecBuilderNode.
func (c *CreateCheck) Resolved() bool {
	return c.gmsCreateCheck != nil && c.gmsCreateCheck.Resolved()
}

// BuildRowIter implements sql.ExecBuilderNode.
func (c *CreateCheck) BuildRowIter(ctx *sql.Context, b sql.NodeExecBuilder, r sql.Row) (sql.RowIter, error) {
	if err := validateCheckConstraintExpression(ctx, c.gmsCreateCheck.Check); err != nil {
		return nil, err
	}
	if c.gmsCreateCheck.Check.Enforced && !c.skipExistingValidation {
		return b.Build(ctx, c.gmsCreateCheck, r)
	}
	checkAlterable, ok := typedTableCheckAlterable(c.gmsCreateCheck.Table.UnderlyingTable())
	if !ok {
		return nil, plan.ErrNoCheckConstraintSupport.New(c.gmsCreateCheck.Table.Name())
	}
	check, err := plan.NewCheckDefinition(ctx, c.gmsCreateCheck.Check, sql.GetSchemaFormatter(c.overrides))
	if err != nil {
		return nil, err
	}
	if err = checkAlterable.CreateCheck(ctx, check); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(sql.NewRow(types.NewOkResult(0))), nil
}

// Schema implements sql.ExecBuilderNode.
func (c *CreateCheck) Schema(ctx *sql.Context) sql.Schema {
	return c.gmsCreateCheck.Schema(ctx)
}

// String implements sql.ExecBuilderNode.
func (c *CreateCheck) String() string {
	return c.gmsCreateCheck.String()
}

// WithChildren implements sql.ExecBuilderNode.
func (c *CreateCheck) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	gmsCreateCheck, err := c.gmsCreateCheck.WithChildren(ctx, children...)
	if err != nil {
		return nil, err
	}
	return &CreateCheck{
		gmsCreateCheck:         gmsCreateCheck.(*plan.CreateCheck),
		skipExistingValidation: c.skipExistingValidation,
		overrides:              c.overrides,
	}, nil
}

// WithOverrides implements sql.NodeOverriding.
func (c *CreateCheck) WithOverrides(overrides sql.EngineOverrides) sql.Node {
	ret := *c
	ret.overrides = overrides
	return &ret
}
