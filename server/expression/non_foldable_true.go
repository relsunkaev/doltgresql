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
	"github.com/dolthub/go-mysql-server/sql"

	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// NonFoldableTrue is a boolean expression that evaluates to true while
// intentionally not presenting as a literal to GMS simplification rules.
type NonFoldableTrue struct{}

var _ sql.Expression = NonFoldableTrue{}
var _ sql.CollationCoercible = NonFoldableTrue{}

// NewNonFoldableTrue returns a new NonFoldableTrue expression.
func NewNonFoldableTrue() NonFoldableTrue {
	return NonFoldableTrue{}
}

// Children implements sql.Expression.
func (NonFoldableTrue) Children() []sql.Expression {
	return nil
}

// CollationCoercibility implements sql.CollationCoercible.
func (NonFoldableTrue) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

// Eval implements sql.Expression.
func (NonFoldableTrue) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return true, nil
}

// IsNullable implements sql.Expression.
func (NonFoldableTrue) IsNullable(ctx *sql.Context) bool {
	return false
}

// Resolved implements sql.Expression.
func (NonFoldableTrue) Resolved() bool {
	return true
}

// String implements fmt.Stringer.
func (NonFoldableTrue) String() string {
	return "TRUE"
}

// Type implements sql.Expression.
func (NonFoldableTrue) Type(ctx *sql.Context) sql.Type {
	return pgtypes.Bool
}

// WithChildren implements sql.Expression.
func (NonFoldableTrue) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(NonFoldableTrue{}, len(children), 0)
	}
	return NonFoldableTrue{}, nil
}
