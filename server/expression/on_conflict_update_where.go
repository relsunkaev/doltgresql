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
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
)

type onConflictUpdateWhereStateKey struct{}

// OnConflictUpdateWhereState tracks whether the current ON CONFLICT row was
// skipped by the DO UPDATE WHERE predicate.
type OnConflictUpdateWhereState struct {
	skipped        bool
	originalRow    sql.Row
	whereEvaluated bool
	whereMatches   bool
	seenTargetRows map[string]struct{}
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
	s.clearOriginalRow()
	return skipped
}

func (s *OnConflictUpdateWhereState) evalOriginalRow(ctx *sql.Context, row sql.Row, destinationLen int, targetIndexes []int, child sql.Expression) (interface{}, error) {
	if s == nil {
		return child.Eval(ctx, row)
	}
	if s.originalRow == nil {
		s.originalRow = row.Copy()
		s.whereEvaluated = false
		s.whereMatches = false
		if err := s.markTargetRow(row, destinationLen, targetIndexes); err != nil {
			s.clearOriginalRow()
			return nil, err
		}
	}
	return child.Eval(ctx, s.originalRow)
}

func (s *OnConflictUpdateWhereState) clearOriginalRow() {
	s.originalRow = nil
	s.whereEvaluated = false
	s.whereMatches = false
}

func (s *OnConflictUpdateWhereState) markTargetRow(row sql.Row, destinationLen int, targetIndexes []int) error {
	if len(targetIndexes) == 0 || destinationLen <= 0 {
		return nil
	}
	key := conflictTargetKey(row, destinationLen, targetIndexes)
	if key == "" {
		return nil
	}
	if s.seenTargetRows == nil {
		s.seenTargetRows = make(map[string]struct{})
	}
	if _, ok := s.seenTargetRows[key]; ok {
		return pgerror.New(pgcode.CardinalityViolation, "ON CONFLICT DO UPDATE command cannot affect row a second time")
	}
	s.seenTargetRows[key] = struct{}{}
	return nil
}

func conflictTargetKey(row sql.Row, destinationLen int, targetIndexes []int) string {
	var buf bytes.Buffer
	for _, idx := range targetIndexes {
		if idx < 0 || idx >= destinationLen || idx >= len(row) {
			continue
		}
		fmt.Fprintf(&buf, "%T:%v\x00", row[idx], row[idx])
	}
	return buf.String()
}

func (s *OnConflictUpdateWhereState) evaluateWhere(ctx *sql.Context, row sql.Row, condition sql.Expression) (bool, error) {
	if s == nil {
		matches, err := sql.EvaluateCondition(ctx, condition, row)
		return sql.IsTrue(matches), err
	}
	if !s.whereEvaluated {
		matches, err := sql.EvaluateCondition(ctx, condition, row)
		if err != nil {
			return false, err
		}
		s.whereMatches = sql.IsTrue(matches)
		s.whereEvaluated = true
	}
	return s.whereMatches, nil
}

// OnConflictUpdateSource evaluates the wrapped explicit SET expression against
// the original ON CONFLICT old+excluded row while applying the result to GMS'
// mutable update accumulator.
type OnConflictUpdateSource struct {
	child          sql.Expression
	destinationLen int
	targetIndexes  []int
}

var _ sql.Expression = (*OnConflictUpdateSource)(nil)
var _ sql.CollationCoercible = (*OnConflictUpdateSource)(nil)

// NewOnConflictUpdateSource returns a new ON CONFLICT source-row wrapper.
func NewOnConflictUpdateSource(child sql.Expression, destinationLen int, targetIndexes []int) *OnConflictUpdateSource {
	indexes := append([]int(nil), targetIndexes...)
	return &OnConflictUpdateSource{
		child:          child,
		destinationLen: destinationLen,
		targetIndexes:  indexes,
	}
}

// Children implements sql.Expression.
func (e *OnConflictUpdateSource) Children() []sql.Expression {
	return []sql.Expression{e.child}
}

// CollationCoercibility implements sql.CollationCoercible.
func (e *OnConflictUpdateSource) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.GetCoercibility(ctx, e.child)
}

// Eval implements sql.Expression.
func (e *OnConflictUpdateSource) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	state := onConflictUpdateWhereStateFromContext(ctx)
	return state.evalOriginalRow(ctx, row, e.destinationLen, e.targetIndexes, e.child)
}

// IsNullable implements sql.Expression.
func (e *OnConflictUpdateSource) IsNullable(ctx *sql.Context) bool {
	return e.child.IsNullable(ctx)
}

// Resolved implements sql.Expression.
func (e *OnConflictUpdateSource) Resolved() bool {
	return e.child.Resolved()
}

// String implements fmt.Stringer.
func (e *OnConflictUpdateSource) String() string {
	return "ON_CONFLICT_UPDATE_SOURCE(" + e.child.String() + ")"
}

// Type implements sql.Expression.
func (e *OnConflictUpdateSource) Type(ctx *sql.Context) sql.Type {
	return e.child.Type(ctx)
}

// WithChildren implements sql.Expression.
func (e *OnConflictUpdateSource) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(e, len(children), 1)
	}
	return NewOnConflictUpdateSource(children[0], e.destinationLen, e.targetIndexes), nil
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
	state := onConflictUpdateWhereStateFromContext(ctx)
	matches, err := state.evaluateWhere(ctx, row, e.condition)
	if err != nil {
		return nil, err
	}
	if matches {
		return e.value.Eval(ctx, row)
	}
	if state != nil {
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
