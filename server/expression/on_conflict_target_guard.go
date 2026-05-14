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
	"reflect"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"

	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
)

// OnConflictTargetGuard wraps an ON DUPLICATE KEY UPDATE assignment
// expression so that it raises when the conflict that triggered the
// update came from a unique index OTHER than the one PostgreSQL's
// ON CONFLICT clause targeted.
//
// GMS evaluates ON DUP expressions against the concatenated row
// (oldRow ++ newRow) — row[i] is the existing value for column i,
// row[i + schemaLen] is the proposed new value. PostgreSQL's
// ON CONFLICT (target_cols) DO ... fires only for conflicts on
// target_cols; if the targeted-index columns of oldRow and newRow
// match, the conflict was on the target. If they differ, the
// conflict must have come from a different unique index — in PG
// that raises "duplicate key value violates unique constraint X".
type OnConflictTargetGuard struct {
	expression.UnaryExpressionStub
	targetIndexes    []int
	schemaLen        int
	constraintName   string
	nullsNotDistinct bool
}

var _ sql.Expression = (*OnConflictTargetGuard)(nil)

// NewOnConflictTargetGuard constructs a guard wrapping inner. The
// target indexes are positions in the destination schema; schemaLen
// is the destination schema length so the guard can find the new-row
// half of the concatenated row passed to ON DUP eval.
func NewOnConflictTargetGuard(inner sql.Expression, targetIndexes []int, schemaLen int, constraintName string, nullsNotDistinct bool) *OnConflictTargetGuard {
	indexes := make([]int, len(targetIndexes))
	copy(indexes, targetIndexes)
	return &OnConflictTargetGuard{
		UnaryExpressionStub: expression.UnaryExpressionStub{Child: inner},
		targetIndexes:       indexes,
		schemaLen:           schemaLen,
		constraintName:      constraintName,
		nullsNotDistinct:    nullsNotDistinct,
	}
}

// Eval implements sql.Expression.
func (g *OnConflictTargetGuard) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	if g.schemaLen > 0 && len(row) >= 2*g.schemaLen {
		for _, idx := range g.targetIndexes {
			oldVal := row[idx]
			newVal := row[idx+g.schemaLen]
			if !valuesEqual(oldVal, newVal, g.nullsNotDistinct) {
				return nil, pgerror.Newf(pgcode.UniqueViolation,
					"duplicate key value violates unique constraint %q",
					g.constraintName)
			}
		}
	}
	return g.Child.Eval(ctx, row)
}

// IsNullable implements sql.Expression.
func (g *OnConflictTargetGuard) IsNullable(ctx *sql.Context) bool {
	return g.Child.IsNullable(ctx)
}

// String implements sql.Expression.
func (g *OnConflictTargetGuard) String() string {
	return "ON_CONFLICT_TARGET_GUARD(" + g.Child.String() + ")"
}

// Type implements sql.Expression.
func (g *OnConflictTargetGuard) Type(ctx *sql.Context) sql.Type {
	return g.Child.Type(ctx)
}

// WithChildren implements sql.Expression.
func (g *OnConflictTargetGuard) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(g, len(children), 1)
	}
	return &OnConflictTargetGuard{
		UnaryExpressionStub: expression.UnaryExpressionStub{Child: children[0]},
		targetIndexes:       g.targetIndexes,
		schemaLen:           g.schemaLen,
		constraintName:      g.constraintName,
		nullsNotDistinct:    g.nullsNotDistinct,
	}, nil
}

// valuesEqual returns true when the two values are semantically
// equal for the targeted conflict arbiter.
func valuesEqual(a, b interface{}, nullsNotDistinct bool) bool {
	if a == nil || b == nil {
		return nullsNotDistinct && a == nil && b == nil
	}
	return reflect.DeepEqual(a, b)
}
