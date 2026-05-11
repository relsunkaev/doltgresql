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
	"bytes"
	"context"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

type onConflictUpdateWhereStateKey struct{}

// OnConflictUpdateWhereState tracks whether the current ON CONFLICT row was
// skipped by the DO UPDATE WHERE predicate.
type OnConflictUpdateWhereState struct {
	skipped bool
}

// NewOnConflictUpdateWhereState returns a fresh ON CONFLICT WHERE state.
func NewOnConflictUpdateWhereState() *OnConflictUpdateWhereState {
	return &OnConflictUpdateWhereState{}
}

// ContextWithOnConflictUpdateWhereState returns a query context carrying state
// for ON CONFLICT DO UPDATE WHERE evaluation.
func ContextWithOnConflictUpdateWhereState(ctx *sql.Context, state *OnConflictUpdateWhereState) *sql.Context {
	if ctx == nil || state == nil {
		return ctx
	}
	return ctx.WithContext(context.WithValue(ctx.Context, onConflictUpdateWhereStateKey{}, state))
}

func onConflictUpdateWhereStateFromContext(ctx *sql.Context) *OnConflictUpdateWhereState {
	if ctx == nil {
		return nil
	}
	state, _ := ctx.Value(onConflictUpdateWhereStateKey{}).(*OnConflictUpdateWhereState)
	return state
}

// ConsumeSkipped returns whether the current row was skipped and resets the
// state for the next row.
func (s *OnConflictUpdateWhereState) ConsumeSkipped() bool {
	if s == nil {
		return false
	}
	skipped := s.skipped
	s.skipped = false
	return skipped
}

// OnConflictUpdateWhere preserves the existing CASE-based lowering of
// ON CONFLICT DO UPDATE WHERE while recording when the predicate rejects a
// conflicting row, which PostgreSQL omits from RETURNING.
type OnConflictUpdateWhere struct {
	condition sql.Expression
	value     sql.Expression
	elseExpr  sql.Expression
}

var _ sql.Expression = (*OnConflictUpdateWhere)(nil)
var _ sql.CollationCoercible = (*OnConflictUpdateWhere)(nil)

// NewOnConflictUpdateWhere returns a new ON CONFLICT WHERE expression.
func NewOnConflictUpdateWhere(condition, value, elseExpr sql.Expression) *OnConflictUpdateWhere {
	return &OnConflictUpdateWhere{
		condition: condition,
		value:     value,
		elseExpr:  elseExpr,
	}
}

// Children implements sql.Expression.
func (e *OnConflictUpdateWhere) Children() []sql.Expression {
	return []sql.Expression{e.condition, e.value, e.elseExpr}
}

// CollationCoercibility implements sql.CollationCoercible.
func (e *OnConflictUpdateWhere) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return e.Type(ctx).CollationCoercibility(ctx)
}

// Eval implements sql.Expression.
func (e *OnConflictUpdateWhere) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	matches, err := sql.EvaluateCondition(ctx, e.condition, row)
	if err != nil {
		return nil, err
	}
	if sql.IsTrue(matches) {
		return e.value.Eval(ctx, row)
	}
	if state := onConflictUpdateWhereStateFromContext(ctx); state != nil {
		state.skipped = true
	}
	return e.elseExpr.Eval(ctx, row)
}

// IsNullable implements sql.Expression.
func (e *OnConflictUpdateWhere) IsNullable(ctx *sql.Context) bool {
	return e.value.IsNullable(ctx) || e.elseExpr.IsNullable(ctx)
}

// Resolved implements sql.Expression.
func (e *OnConflictUpdateWhere) Resolved() bool {
	return e.condition.Resolved() && e.value.Resolved() && e.elseExpr.Resolved()
}

// String implements fmt.Stringer.
func (e *OnConflictUpdateWhere) String() string {
	var buf bytes.Buffer
	buf.WriteString("ON_CONFLICT_UPDATE_WHERE(")
	buf.WriteString(e.condition.String())
	buf.WriteString(", ")
	buf.WriteString(e.value.String())
	buf.WriteString(", ")
	buf.WriteString(e.elseExpr.String())
	buf.WriteString(")")
	return buf.String()
}

// Type implements sql.Expression.
func (e *OnConflictUpdateWhere) Type(ctx *sql.Context) sql.Type {
	return types.GeneralizeTypes(e.value.Type(ctx), e.elseExpr.Type(ctx))
}

// WithChildren implements sql.Expression.
func (e *OnConflictUpdateWhere) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 3 {
		return nil, sql.ErrInvalidChildrenNumber.New(e, len(children), 3)
	}
	return NewOnConflictUpdateWhere(children[0], children[1], children[2]), nil
}
