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
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/functions"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/auth"
)

// CreateOperator implements CREATE OPERATOR for function-backed binary operators.
type CreateOperator struct {
	Name      string
	LeftType  string
	RightType string
	Function  string
}

var _ sql.ExecSourceRel = (*CreateOperator)(nil)
var _ vitess.Injectable = (*CreateOperator)(nil)

// NewCreateOperator returns a new *CreateOperator.
func NewCreateOperator(name string, leftType string, rightType string, function string) *CreateOperator {
	return &CreateOperator{Name: name, LeftType: leftType, RightType: rightType, Function: function}
}

// Children implements the interface sql.ExecSourceRel.
func (c *CreateOperator) Children() []sql.Node { return nil }

// IsReadOnly implements the interface sql.ExecSourceRel.
func (c *CreateOperator) IsReadOnly() bool { return false }

// Resolved implements the interface sql.ExecSourceRel.
func (c *CreateOperator) Resolved() bool { return true }

// RowIter implements the interface sql.ExecSourceRel.
func (c *CreateOperator) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	leftID, err := transformTypeID(ctx, c.LeftType)
	if err != nil {
		return nil, err
	}
	rightID, err := transformTypeID(ctx, c.RightType)
	if err != nil {
		return nil, err
	}
	schemaName, err := core.GetCurrentSchema(ctx)
	if err != nil {
		return nil, err
	}
	functionSchema, functionName, resultType, err := c.resolveFunction(ctx, id.Type(leftID), id.Type(rightID))
	if err != nil {
		return nil, err
	}
	auth.LockWrite(func() {
		err = auth.CreateOperator(auth.Operator{
			Name:           c.Name,
			Namespace:      id.NewNamespace(schemaName),
			LeftType:       id.Type(leftID),
			RightType:      id.Type(rightID),
			ResultType:     resultType,
			Function:       functionName,
			FunctionSchema: functionSchema,
		})
		if err == nil {
			err = auth.PersistChanges()
		}
	})
	if err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

func (c *CreateOperator) resolveFunction(ctx *sql.Context, leftType id.Type, rightType id.Type) (string, string, id.Type, error) {
	funcCollection, err := core.GetFunctionsCollectionFromContext(ctx)
	if err != nil {
		return "", "", id.NullType, err
	}
	functionSchema, functionName := splitOperatorFunctionName(c.Function)
	if functionSchema == "" {
		functionSchema, err = core.GetCurrentSchema(ctx)
		if err != nil {
			return "", "", id.NullType, err
		}
	}
	paramTypes := []id.Type{leftType, rightType}
	fn, err := funcCollection.GetFunction(ctx, id.NewFunction(functionSchema, functionName, paramTypes...))
	if err != nil {
		return "", "", id.NullType, err
	}
	if !fn.ID.IsValid() {
		overloads, err := funcCollection.GetFunctionOverloads(ctx, id.NewFunction(functionSchema, functionName))
		if err != nil {
			return "", "", id.NullType, err
		}
		for _, overload := range overloads {
			if functionParameterTypesMatch(overload.ParameterTypes, paramTypes) {
				fn = overload
				break
			}
		}
	}
	if !fn.ID.IsValid() && !strings.Contains(c.Function, ".") {
		err = funcCollection.IterateFunctions(ctx, func(f functions.Function) (stop bool, err error) {
			if !strings.EqualFold(f.ID.FunctionName(), functionName) || !functionParameterTypesMatch(f.ParameterTypes, paramTypes) {
				return false, nil
			}
			fn = f
			functionSchema = f.ID.SchemaName()
			return true, nil
		})
		if err != nil {
			return "", "", id.NullType, err
		}
	}
	if !fn.ID.IsValid() {
		return "", "", id.NullType, errors.Errorf(`function "%s" does not exist`, c.Function)
	}
	return functionSchema, functionName, fn.ReturnType, nil
}

func splitOperatorFunctionName(raw string) (schema string, name string) {
	parts := strings.Split(strings.TrimSpace(raw), ".")
	if len(parts) == 2 {
		return normalizeOperatorIdentifier(parts[0]), normalizeOperatorIdentifier(parts[1])
	}
	return "", normalizeOperatorIdentifier(raw)
}

func normalizeOperatorIdentifier(raw string) string {
	raw = strings.TrimSpace(raw)
	if len(raw) >= 2 && raw[0] == '"' && raw[len(raw)-1] == '"' {
		return strings.ReplaceAll(raw[1:len(raw)-1], `""`, `"`)
	}
	return strings.ToLower(raw)
}

// Schema implements the interface sql.ExecSourceRel.
func (c *CreateOperator) Schema(ctx *sql.Context) sql.Schema { return nil }

// String implements the interface sql.ExecSourceRel.
func (c *CreateOperator) String() string { return "CREATE OPERATOR" }

// WithChildren implements the interface sql.ExecSourceRel.
func (c *CreateOperator) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(c, children...)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (c *CreateOperator) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return c, nil
}
