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
	"context"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"
)

// IdentityOverrideValue wraps an explicit INSERT value so GMS accepts it in the
// generated-column validation path. PostgreSQL identity override semantics are
// applied by the analyzer after the plan builder has finished validation.
type IdentityOverrideValue struct{}

var _ vitess.Injectable = IdentityOverrideValue{}

// NewIdentityOverrideValue returns a new IdentityOverrideValue.
func NewIdentityOverrideValue() IdentityOverrideValue {
	return IdentityOverrideValue{}
}

// WithResolvedChildren implements the vitess.Injectable interface.
func (i IdentityOverrideValue) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(i, len(children), 1)
	}
	child, ok := children[0].(sql.Expression)
	if !ok {
		return nil, errors.Errorf("expected vitess child to be an expression but has type `%T`", children[0])
	}
	sqlCtx, ok := ctx.(*sql.Context)
	if !ok {
		return nil, errors.Errorf("expected SQL context but has type `%T`", ctx)
	}
	return sql.NewColumnDefaultValue(child, child.Type(sqlCtx), false, true, true)
}
