// Copyright 2024 Dolthub, Inc.
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
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/procedures"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/auth"
	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// ExplicitCast represents a VALUE::TYPE expression.
type ExplicitCast struct {
	sqlChild       sql.Expression
	castToType     *pgtypes.DoltgresType
	domainNullable bool
	domainChecks   sql.CheckConstraints
	runner         sql.StatementRunner
}

var _ vitess.Injectable = (*ExplicitCast)(nil)
var _ sql.Expression = (*ExplicitCast)(nil)
var _ procedures.InterpreterExpr = (*ExplicitCast)(nil)

// NewExplicitCastInjectable returns an incomplete *ExplicitCast that must be resolved through the vitess.Injectable interface.
func NewExplicitCastInjectable(castToType sql.Type) (*ExplicitCast, error) {
	pgtype, ok := castToType.(*pgtypes.DoltgresType)
	if !ok {
		return nil, errors.Errorf("cast expects a Doltgres type as the target type")
	}
	return &ExplicitCast{
		sqlChild:   nil,
		castToType: pgtype,
	}, nil
}

// NewExplicitCast returns a new *ExplicitCast expression.
func NewExplicitCast(expr sql.Expression, toType *pgtypes.DoltgresType) *ExplicitCast {
	toType = checkForDomainType(toType)
	return &ExplicitCast{
		sqlChild:   expr,
		castToType: toType,
	}
}

// Children implements the sql.Expression interface.
func (c *ExplicitCast) Children() []sql.Expression {
	return []sql.Expression{c.sqlChild}
}

// Child returns the child that is being cast.
func (c *ExplicitCast) Child() sql.Expression {
	return c.sqlChild
}

// Eval implements the sql.Expression interface.
func (c *ExplicitCast) Eval(ctx *sql.Context, row sql.Row) (any, error) {
	if !c.castToType.IsResolvedType() {
		return nil, errors.Errorf("cannot call ExplicitCast.Eval with unresolved cast to type: %s", c.castToType.String())
	}

	val, err := c.sqlChild.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	var fromType *pgtypes.DoltgresType
	if newVal, ok, err := castFunctionValue(ctx, val, c.sqlChild); ok {
		if err != nil {
			return nil, err
		}
		val = newVal
		fromType, _ = FunctionDoltgresType(ctx, c.sqlChild)
	} else if exprType, ok := c.sqlChild.Type(ctx).(*pgtypes.DoltgresType); ok {
		fromType = exprType
	} else {
		// We'll leverage GMSCast to handle the conversion from a GMS type to a Doltgres type.
		// Rather than re-evaluating the expression, we put the result in a literal.
		gmsCast := NewGMSCast(expression.NewLiteral(val, c.sqlChild.Type(ctx)))
		val, err = gmsCast.Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		fromType = gmsCast.DoltgresType(ctx)
	}
	if val == nil {
		if c.castToType.TypType == pgtypes.TypeType_Domain && !c.domainNullable {
			return nil, pgtypes.ErrDomainDoesNotAllowNullValues.New(c.castToType.Name())
		}
		return nil, nil
	}
	fromType = runtimeFromTypeForExplicitCast(fromType, val)

	baseCastToType := checkForDomainType(c.castToType)
	if userCast, ok := auth.GetCast(fromType.ID, baseCastToType.ID); ok {
		castResult, err := c.evalUserDefinedCast(ctx, val, fromType, userCast)
		if err != nil {
			return nil, err
		}
		return castResult, nil
	}
	castFunction := framework.GetExplicitCast(fromType, baseCastToType)
	if castFunction == nil {
		return nil, errors.Errorf(
			"EXPLICIT CAST: cast from `%s` to `%s` does not exist: %s",
			fromType.String(), c.castToType.String(), c.sqlChild.String(),
		)
	}
	castResult, err := castFunction(ctx, val, c.castToType)
	if err != nil {
		// For string types and string array types, we intentionally ignore the error as using a length-restricted cast
		// is a way to intentionally truncate the data. All string types will always return the truncated result, even
		// during an error, so it's safe to use.
		castToType := c.castToType
		if c.castToType.IsArrayType() {
			castToType = c.castToType.ArrayBaseType()
		}
		// A nil result will be returned if there's a critical error, which we should never ignore.
		if castToType.TypCategory != pgtypes.TypeCategory_StringTypes || castResult == nil {
			return nil, err
		}
	}

	if c.castToType.TypType == pgtypes.TypeType_Domain {
		for _, check := range c.domainChecks {
			res, err := sql.EvaluateCondition(ctx, check.Expr, sql.Row{castResult})
			if err != nil {
				return nil, err
			}
			if sql.IsFalse(res) {
				return nil, pgtypes.ErrDomainValueViolatesCheckConstraint.New(c.castToType.Name(), check.Name)
			}
		}
	}

	return castResult, nil
}

func (c *ExplicitCast) evalUserDefinedCast(ctx *sql.Context, val any, fromType *pgtypes.DoltgresType, cast auth.Cast) (any, error) {
	if c.runner == nil {
		return nil, errors.Errorf("statement runner is not available for cast %s", cast.Function)
	}
	fn, ok := (&framework.FunctionProvider{}).Function(ctx, cast.Function)
	if !ok {
		return nil, errors.Errorf("function %s does not exist", cast.Function)
	}
	fnExpr, err := fn.NewInstance(ctx, []sql.Expression{expression.NewLiteral(val, fromType)})
	if err != nil {
		return nil, err
	}
	if interp, ok := fnExpr.(procedures.InterpreterExpr); ok {
		fnExpr = interp.SetStatementRunner(ctx, c.runner)
	}
	return fnExpr.Eval(ctx, nil)
}

func runtimeFromTypeForExplicitCast(fromType *pgtypes.DoltgresType, val any) *pgtypes.DoltgresType {
	if _, ok := val.(id.Id); !ok || isIdBackedType(fromType) {
		return fromType
	}
	return pgtypes.Oid
}

func isIdBackedType(fromType *pgtypes.DoltgresType) bool {
	switch fromType.ID {
	case pgtypes.Oid.ID, pgtypes.Regclass.ID, pgtypes.Regconfig.ID, pgtypes.Regdatabase.ID, pgtypes.Regdictionary.ID,
		pgtypes.Regnamespace.ID, pgtypes.Regoperator.ID, pgtypes.Regproc.ID, pgtypes.Regprocedure.ID,
		pgtypes.Regrole.ID, pgtypes.Regtype.ID:
		return true
	default:
		return false
	}
}

// IsNullable implements the sql.Expression interface.
func (c *ExplicitCast) IsNullable(ctx *sql.Context) bool {
	// TODO: verify if this is actually nullable
	return true
}

// Resolved implements the sql.Expression interface.
func (c *ExplicitCast) Resolved() bool {
	if c.sqlChild != nil && c.sqlChild.Resolved() {
		return true
	}
	return false
}

// String implements the sql.Expression interface.
func (c *ExplicitCast) String() string {
	var sqlChild string
	if c.sqlChild == nil {
		sqlChild = "unresolved"
	} else {
		sqlChild = c.sqlChild.String()
		switch c.sqlChild.(type) {
		case *BinaryOperator:
			sqlChild = fmt.Sprintf("(%s)", sqlChild)
		}
	}
	// type needs to be upper-case to match InputExpression in AliasExpr
	return fmt.Sprintf("%s::%s", sqlChild, strings.ToUpper(c.castToType.String()))
}

// Type implements the sql.Expression interface.
func (c *ExplicitCast) Type(ctx *sql.Context) sql.Type {
	return c.castToType
}

// WithChildren implements the sql.Expression interface.
func (c *ExplicitCast) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 1)
	}
	return &ExplicitCast{
		sqlChild:       children[0],
		castToType:     c.castToType,
		domainNullable: c.domainNullable,
		domainChecks:   c.domainChecks,
		runner:         c.runner,
	}, nil
}

// WithResolvedChildren implements the vitess.InjectableExpression interface.
func (c *ExplicitCast) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 1 {
		return nil, errors.Errorf("invalid vitess child count, expected `1` but got `%d`", len(children))
	}
	resolvedExpression, ok := children[0].(sql.Expression)
	if !ok {
		return nil, errors.Errorf("expected vitess child to be an expression but has type `%T`", children[0])
	}
	return &ExplicitCast{
		sqlChild:       resolvedExpression,
		castToType:     c.castToType,
		domainNullable: c.domainNullable,
		domainChecks:   c.domainChecks,
		runner:         c.runner,
	}, nil
}

// WithCastToType returns a copy of the expression with castToType replaced.
func (c *ExplicitCast) WithCastToType(t *pgtypes.DoltgresType) sql.Expression {
	ec := *c
	ec.castToType = t
	return &ec
}

// WithDomainConstraints returns a copy of the expression with domain constraints defined.
func (c *ExplicitCast) WithDomainConstraints(nullable bool, checks sql.CheckConstraints) sql.Expression {
	ec := *c
	ec.domainNullable = nullable
	ec.domainChecks = checks
	return &ec
}

// SetStatementRunner implements procedures.InterpreterExpr.
func (c *ExplicitCast) SetStatementRunner(ctx *sql.Context, runner sql.StatementRunner) sql.Expression {
	ec := *c
	ec.runner = runner
	return &ec
}
