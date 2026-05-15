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
	"github.com/cockroachdb/errors"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// AssignmentCast handles assignment casts.
type AssignmentCast struct {
	expr           sql.Expression
	fromType       *pgtypes.DoltgresType
	toType         *pgtypes.DoltgresType
	domainNullable bool
	domainChecks   sql.CheckConstraints
	domainElemType *pgtypes.DoltgresType
	domainElemNull bool
	domainElemChk  sql.CheckConstraints
	compositeAttrs []CompositeDomainAttributeConstraint
}

var _ sql.Expression = (*AssignmentCast)(nil)

// NewAssignmentCast returns a new *AssignmentCast expression.
func NewAssignmentCast(expr sql.Expression, fromType *pgtypes.DoltgresType, toType *pgtypes.DoltgresType) *AssignmentCast {
	fromType = checkForDomainType(fromType)
	return &AssignmentCast{
		expr:           expr,
		fromType:       fromType,
		toType:         toType,
		domainNullable: true,
	}
}

// Children implements the sql.Expression interface.
func (ac *AssignmentCast) Children() []sql.Expression {
	return []sql.Expression{ac.expr}
}

// Eval implements the sql.Expression interface.
func (ac *AssignmentCast) Eval(ctx *sql.Context, row sql.Row) (any, error) {
	val, err := ac.expr.Eval(ctx, row)
	if err != nil {
		return val, err
	}
	if val == nil {
		if ac.toType.TypType == pgtypes.TypeType_Domain && !ac.domainNullable {
			return nil, pgtypes.ErrDomainDoesNotAllowNullValues.New(ac.toType.Name())
		}
		return nil, nil
	}
	fromType, err := checkForDomainTypeWithContext(ctx, ac.fromType)
	if err != nil {
		return nil, err
	}
	toType, err := checkForDomainTypeWithContext(ctx, ac.toType)
	if err != nil {
		return nil, err
	}
	castFunc := framework.GetAssignmentCast(fromType, toType)
	if castFunc == nil {
		if toType.ID == pgtypes.Name.ID {
			output, err := fromType.IoOutput(ctx, val)
			if err != nil {
				return nil, err
			}
			return toType.IoInput(ctx, output)
		}
		return nil, errors.Errorf("ASSIGNMENT_CAST: target is of type %s but expression is of type %s: %s",
			toType.String(), fromType.String(), ac.expr.String())
	}
	castResult, err := castFunc(ctx, val, toType)
	if err != nil {
		return nil, err
	}
	if ac.toType.TypType == pgtypes.TypeType_Domain {
		if err = validateDomainValue(ctx, ac.toType, ac.domainNullable, ac.domainChecks, castResult); err != nil {
			return nil, err
		}
	}
	if ac.domainElemType != nil {
		if err = validateDomainArrayElements(ctx, ac.domainElemType, ac.domainElemNull, ac.domainElemChk, castResult); err != nil {
			return nil, err
		}
	}
	if err = validateCompositeDomainAttributes(ctx, ac.compositeAttrs, castResult); err != nil {
		return nil, err
	}
	return castResult, nil
}

// IsNullable implements the sql.Expression interface.
func (ac *AssignmentCast) IsNullable(ctx *sql.Context) bool {
	return true
}

// Resolved implements the sql.Expression interface.
func (ac *AssignmentCast) Resolved() bool {
	return ac.expr.Resolved()
}

// String implements the sql.Expression interface.
func (ac *AssignmentCast) String() string {
	return ac.expr.String()
}

// Type implements the sql.Expression interface.
func (ac *AssignmentCast) Type(ctx *sql.Context) sql.Type {
	return ac.toType
}

// WithChildren implements the sql.Expression interface.
func (ac *AssignmentCast) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(ac, len(children), 1)
	}
	newCast := *ac
	newCast.expr = children[0]
	return &newCast, nil
}

// WithDomainConstraints returns a copy of the expression with domain constraints defined.
func (ac *AssignmentCast) WithDomainConstraints(nullable bool, checks sql.CheckConstraints) sql.Expression {
	newCast := *ac
	newCast.domainNullable = nullable
	newCast.domainChecks = checks
	return &newCast
}

// WithDomainElementConstraints returns a copy of the expression with array element domain constraints defined.
func (ac *AssignmentCast) WithDomainElementConstraints(domainType *pgtypes.DoltgresType, nullable bool, checks sql.CheckConstraints) sql.Expression {
	newCast := *ac
	newCast.domainElemType = domainType
	newCast.domainElemNull = nullable
	newCast.domainElemChk = checks
	return &newCast
}

// WithCompositeDomainAttributeConstraints returns a copy of the expression with composite attribute domain constraints defined.
func (ac *AssignmentCast) WithCompositeDomainAttributeConstraints(constraints []CompositeDomainAttributeConstraint) sql.Expression {
	newCast := *ac
	newCast.compositeAttrs = constraints
	return &newCast
}

func checkForDomainType(t *pgtypes.DoltgresType) *pgtypes.DoltgresType {
	if t.TypType == pgtypes.TypeType_Domain {
		t = t.DomainUnderlyingBaseType()
	}
	return t
}

func checkForDomainTypeWithContext(ctx *sql.Context, t *pgtypes.DoltgresType) (*pgtypes.DoltgresType, error) {
	if t.TypType != pgtypes.TypeType_Domain {
		return t, nil
	}
	if ctx == nil {
		return t.DomainUnderlyingBaseType(), nil
	}
	return domainUnderlyingBaseTypeWithContext(ctx, t)
}

func domainUnderlyingBaseTypeWithContext(ctx *sql.Context, t *pgtypes.DoltgresType) (*pgtypes.DoltgresType, error) {
	bt, ok := pgtypes.IDToBuiltInDoltgresType[t.BaseTypeID]
	if !ok {
		typeColl, err := pgtypes.GetTypesCollectionFromContext(ctx)
		if err != nil {
			return nil, err
		}
		schema, err := core.GetSchemaName(ctx, nil, t.BaseTypeID.SchemaName())
		if err != nil {
			return nil, err
		}
		bt, err = typeColl.GetType(ctx, id.NewType(schema, t.BaseTypeID.TypeName()))
		if err != nil {
			return nil, err
		}
		if bt == nil {
			return nil, pgtypes.ErrTypeDoesNotExist.New(t.BaseTypeID.TypeName())
		}
	}
	if bt.TypType == pgtypes.TypeType_Domain {
		var err error
		bt, err = domainUnderlyingBaseTypeWithContext(ctx, bt)
		if err != nil {
			return nil, err
		}
	}
	if typmod := t.GetAttTypMod(); typmod != -1 {
		return bt.WithAttTypMod(typmod), nil
	}
	return bt, nil
}
