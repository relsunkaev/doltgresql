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

	"github.com/dolthub/doltgresql/server/deferrable"
	pgexprs "github.com/dolthub/doltgresql/server/expression"
)

// SetConstraints implements PostgreSQL's transaction-local SET CONSTRAINTS
// mode switch for currently supported deferrable foreign keys.
type SetConstraints struct {
	names    []string
	all      bool
	deferred bool
	Runner   pgexprs.StatementRunner
}

var _ sql.ExecSourceRel = (*SetConstraints)(nil)
var _ sql.Expressioner = (*SetConstraints)(nil)
var _ vitess.Injectable = (*SetConstraints)(nil)

// NewSetConstraints returns a new *SetConstraints.
func NewSetConstraints(names []string, all bool, deferred bool) *SetConstraints {
	return &SetConstraints{
		names:    names,
		all:      all,
		deferred: deferred,
	}
}

// Children implements the interface sql.ExecSourceRel.
func (s *SetConstraints) Children() []sql.Node {
	return nil
}

// Expressions implements the interface sql.Expressioner.
func (s *SetConstraints) Expressions() []sql.Expression {
	return []sql.Expression{s.Runner}
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (s *SetConstraints) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (s *SetConstraints) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (s *SetConstraints) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	connectionID := ctx.Session.ID()
	if !deferrable.Active(connectionID) {
		return sql.RowsToRowIter(), nil
	}
	if !s.deferred {
		checks := deferrable.PendingChecksForConstraints(connectionID, s.names, s.all)
		for _, check := range checks {
			hasViolation, err := s.hasViolation(ctx, check.Query)
			if err != nil {
				return nil, err
			}
			if hasViolation {
				fk := check.ForeignKey
				return nil, sql.ErrForeignKeyChildViolation.New(fk.Name, fk.Table, fk.ParentTable, "deferred")
			}
		}
		deferrable.ClearPendingChecksForConstraints(connectionID, s.names, s.all)
	}
	deferrable.SetConstraints(connectionID, s.names, s.all, s.deferred)
	return sql.RowsToRowIter(), nil
}

// Schema implements the interface sql.ExecSourceRel.
func (s *SetConstraints) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (s *SetConstraints) String() string {
	return "SET CONSTRAINTS"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (s *SetConstraints) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(s, children...)
}

// WithExpressions implements the interface sql.Expressioner.
func (s *SetConstraints) WithExpressions(ctx *sql.Context, expressions ...sql.Expression) (sql.Node, error) {
	if len(expressions) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(s, len(expressions), 1)
	}
	newS := *s
	newS.Runner = expressions[0].(pgexprs.StatementRunner)
	return &newS, nil
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (s *SetConstraints) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return s, nil
}

func (s *SetConstraints) hasViolation(ctx *sql.Context, query string) (bool, error) {
	if s.Runner.Runner == nil {
		return false, errors.Errorf("statement runner is not available")
	}
	rows, err := sql.RunInterpreted(ctx, func(subCtx *sql.Context) ([]sql.Row, error) {
		_, rowIter, _, err := s.Runner.Runner.QueryWithBindings(subCtx, query, nil, nil, nil)
		if err != nil {
			return nil, err
		}
		return sql.RowIterToRows(subCtx, rowIter)
	})
	if err != nil {
		return false, err
	}
	return len(rows) > 0, nil
}
