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
	"io"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/doltgresql/core"
)

// InheritedCreateCheck creates CHECK constraints for a parent table and each
// inherited child table after validating all affected rows.
type InheritedCreateCheck struct {
	nodes                  []*plan.CreateCheck
	overrides              sql.EngineOverrides
	skipExistingValidation []bool
}

var _ sql.ExecBuilderNode = (*InheritedCreateCheck)(nil)

// NewInheritedCreateCheck returns a new *InheritedCreateCheck.
func NewInheritedCreateCheck(nodes []*plan.CreateCheck, overrides sql.EngineOverrides, skipExistingValidation []bool) *InheritedCreateCheck {
	return &InheritedCreateCheck{
		nodes:                  append([]*plan.CreateCheck(nil), nodes...),
		overrides:              overrides,
		skipExistingValidation: append([]bool(nil), skipExistingValidation...),
	}
}

// Children implements the interface sql.ExecBuilderNode.
func (i *InheritedCreateCheck) Children() []sql.Node {
	children := make([]sql.Node, len(i.nodes))
	for idx, node := range i.nodes {
		children[idx] = node
	}
	return children
}

// IsReadOnly implements the interface sql.ExecBuilderNode.
func (i *InheritedCreateCheck) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecBuilderNode.
func (i *InheritedCreateCheck) Resolved() bool {
	for _, node := range i.nodes {
		if !node.Resolved() {
			return false
		}
	}
	return true
}

// BuildRowIter implements the interface sql.ExecBuilderNode.
func (i *InheritedCreateCheck) BuildRowIter(ctx *sql.Context, b sql.NodeExecBuilder, r sql.Row) (sql.RowIter, error) {
	nodes := make([]*plan.CreateCheck, len(i.nodes))
	for idx, node := range i.nodes {
		nodes[idx] = node
		if node.Check.Enforced && !i.skipExistingValidation[idx] {
			if err := validateInheritedCreateCheckRows(ctx, b, r, node); err != nil {
				return nil, err
			}
		}
	}
	rowsAffected := 0
	for _, node := range nodes {
		iter, err := b.Build(ctx, NewCreateCheck(node, i.overrides, true), r)
		if err != nil {
			return nil, err
		}
		affected, err := drainInheritedAlterDefaultIter(ctx, iter)
		if err != nil {
			return nil, err
		}
		rowsAffected += affected
	}
	return sql.RowsToRowIter(sql.NewRow(types.NewOkResult(rowsAffected))), nil
}

// Schema implements the interface sql.ExecBuilderNode.
func (i *InheritedCreateCheck) Schema(ctx *sql.Context) sql.Schema {
	return types.OkResultSchema
}

// String implements the interface sql.ExecBuilderNode.
func (i *InheritedCreateCheck) String() string {
	if len(i.nodes) == 0 {
		return "ALTER CHECK"
	}
	return i.nodes[0].String()
}

// WithChildren implements the interface sql.ExecBuilderNode.
func (i *InheritedCreateCheck) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	if len(children) != len(i.nodes) {
		return nil, sql.ErrInvalidChildrenNumber.New(i, len(children), len(i.nodes))
	}
	nodes := make([]*plan.CreateCheck, len(children))
	skipExistingValidation := make([]bool, len(children))
	for idx, child := range children {
		switch createCheck := child.(type) {
		case *plan.CreateCheck:
			nodes[idx] = createCheck
			skipExistingValidation[idx] = i.skipExistingValidation[idx]
		case *CreateCheck:
			nodes[idx] = createCheck.gmsCreateCheck
			skipExistingValidation[idx] = i.skipExistingValidation[idx] || createCheck.skipExistingValidation
		default:
			return nil, errors.Errorf("expected *plan.CreateCheck child, got %T", child)
		}
	}
	return NewInheritedCreateCheck(nodes, i.overrides, skipExistingValidation), nil
}

func validateInheritedCreateCheckRows(ctx *sql.Context, b sql.NodeExecBuilder, r sql.Row, node *plan.CreateCheck) error {
	if err := validateCheckConstraintExpression(ctx, node.Check); err != nil {
		return err
	}
	rowIter, err := b.Build(ctx, node.Table, r)
	if err != nil {
		return err
	}
	defer rowIter.Close(ctx)
	for {
		row, err := rowIter.Next(ctx)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		res, err := sql.EvaluateCondition(ctx, node.Check.Expr, row)
		if err != nil {
			return err
		}
		if sql.IsFalse(res) {
			return errors.Errorf("check constraint %q of relation %q is violated by some row", core.DecodePhysicalConstraintName(node.Check.Name), node.Table.Name())
		}
	}
}
