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

	"github.com/dolthub/doltgresql/core/id"
	pgexprs "github.com/dolthub/doltgresql/server/expression"
	"github.com/dolthub/doltgresql/server/functions/framework"
	"github.com/dolthub/doltgresql/server/plpgsql"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// DoBlock implements PostgreSQL's anonymous DO statement.
type DoBlock struct {
	Definition string
	Statements []plpgsql.InterpreterOperation
	Runner     pgexprs.StatementRunner
}

var _ sql.ExecSourceRel = (*DoBlock)(nil)
var _ sql.Expressioner = (*DoBlock)(nil)
var _ vitess.Injectable = (*DoBlock)(nil)

// NewDoBlock returns a new *DoBlock.
func NewDoBlock(definition string, statements []plpgsql.InterpreterOperation) *DoBlock {
	return &DoBlock{
		Definition: definition,
		Statements: statements,
	}
}

// Children implements the interface sql.ExecSourceRel.
func (d *DoBlock) Children() []sql.Node {
	return nil
}

// Expressions implements the interface sql.Expressioner.
func (d *DoBlock) Expressions() []sql.Expression {
	return []sql.Expression{d.Runner}
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (d *DoBlock) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (d *DoBlock) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (d *DoBlock) RowIter(ctx *sql.Context, r sql.Row) (sql.RowIter, error) {
	iFunc := framework.InterpretedFunction{
		ID:                 id.NewFunction("pg_catalog", "__doltgres_do_block"),
		ReturnType:         pgtypes.Void,
		IsNonDeterministic: true,
		Statements:         d.Statements,
	}
	if _, err := plpgsql.Call(ctx, iFunc, d.Runner.Runner, nil, nil); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

// Schema implements the interface sql.ExecSourceRel.
func (d *DoBlock) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (d *DoBlock) String() string {
	return "DO"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (d *DoBlock) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(d, children...)
}

// WithExpressions implements the interface sql.Expressioner.
func (d *DoBlock) WithExpressions(ctx *sql.Context, expressions ...sql.Expression) (sql.Node, error) {
	if len(expressions) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(d, len(expressions), 1)
	}
	newD := *d
	newD.Runner = expressions[0].(pgexprs.StatementRunner)
	return &newD, nil
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (d *DoBlock) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return d, nil
}
