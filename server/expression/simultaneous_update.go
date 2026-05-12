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

package expression

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"
	gmsexpression "github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// SimultaneousUpdate evaluates all explicit UPDATE assignments against the
// original input row before applying any target-column changes.
type SimultaneousUpdate struct {
	exprs []sql.Expression
}

var _ sql.Expression = (*SimultaneousUpdate)(nil)
var _ sql.CollationCoercible = (*SimultaneousUpdate)(nil)

// NewSimultaneousUpdate returns a new SimultaneousUpdate expression.
func NewSimultaneousUpdate(exprs []sql.Expression) *SimultaneousUpdate {
	return &SimultaneousUpdate{exprs: exprs}
}

// Children implements the sql.Expression interface.
func (s *SimultaneousUpdate) Children() []sql.Expression {
	return s.exprs
}

// Eval implements the sql.Expression interface.
func (s *SimultaneousUpdate) Eval(ctx *sql.Context, row sql.Row) (any, error) {
	updatedRow := row.Copy()
	for _, expr := range s.exprs {
		setField, ok := expr.(*gmsexpression.SetField)
		if !ok {
			return nil, errors.Errorf("SIMULTANEOUS_UPDATE: expected SetField expression but found %T", expr)
		}
		getField, ok := setField.LeftChild.(*gmsexpression.GetField)
		if !ok {
			return nil, errors.Errorf("SIMULTANEOUS_UPDATE: expected GetField target but found %T", setField.LeftChild)
		}
		val, err := setField.Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		rowVal, ok := val.(sql.Row)
		if !ok {
			return nil, plan.ErrUpdateUnexpectedSetResult.New(val)
		}
		updatedRow[getField.Index()] = rowVal[getField.Index()]
	}
	return updatedRow, nil
}

// IsNullable implements the sql.Expression interface.
func (s *SimultaneousUpdate) IsNullable(ctx *sql.Context) bool {
	return false
}

// Resolved implements the sql.Expression interface.
func (s *SimultaneousUpdate) Resolved() bool {
	for _, expr := range s.exprs {
		if !expr.Resolved() {
			return false
		}
	}
	return true
}

// String implements the sql.Expression interface.
func (s *SimultaneousUpdate) String() string {
	parts := make([]string, len(s.exprs))
	for i, expr := range s.exprs {
		parts[i] = expr.String()
	}
	return fmt.Sprintf("SIMULTANEOUS_UPDATE(%s)", strings.Join(parts, ", "))
}

// Type implements the sql.Expression interface.
func (s *SimultaneousUpdate) Type(ctx *sql.Context) sql.Type {
	if len(s.exprs) == 0 {
		return nil
	}
	return s.exprs[len(s.exprs)-1].Type(ctx)
}

// WithChildren implements the sql.Expression interface.
func (s *SimultaneousUpdate) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	return NewSimultaneousUpdate(children), nil
}

// CollationCoercibility implements the sql.CollationCoercible interface.
func (s *SimultaneousUpdate) CollationCoercibility(ctx *sql.Context) (sql.CollationID, byte) {
	if len(s.exprs) == 0 {
		return sql.Collation_binary, 7
	}
	return sql.GetCoercibility(ctx, s.exprs[len(s.exprs)-1])
}
