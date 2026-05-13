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

type CompositeDomainAttributeConstraint struct {
	Index      int
	DomainType *pgtypes.DoltgresType
	Nullable   bool
	Checks     sql.CheckConstraints
}

func validateDomainValue(ctx *sql.Context, domainType *pgtypes.DoltgresType, nullable bool, checks sql.CheckConstraints, val any) error {
	if val == nil {
		if !nullable {
			return pgtypes.ErrDomainDoesNotAllowNullValues.New(domainType.Name())
		}
		return nil
	}
	for _, check := range checks {
		res, err := sql.EvaluateCondition(ctx, check.Expr, sql.Row{val})
		if err != nil {
			return err
		}
		if sql.IsFalse(res) {
			return pgtypes.ErrDomainValueViolatesCheckConstraint.New(domainType.Name(), check.Name)
		}
	}
	return nil
}

func validateCompositeDomainAttributes(ctx *sql.Context, constraints []CompositeDomainAttributeConstraint, val any) error {
	if len(constraints) == 0 || val == nil {
		return nil
	}
	recordVals, ok := val.([]pgtypes.RecordValue)
	if !ok {
		return nil
	}
	for _, constraint := range constraints {
		if constraint.Index < 0 || constraint.Index >= len(recordVals) {
			continue
		}
		if err := validateDomainValue(ctx, constraint.DomainType, constraint.Nullable, constraint.Checks, recordVals[constraint.Index].Value); err != nil {
			return err
		}
	}
	return nil
}

func validateDomainArrayElements(ctx *sql.Context, domainType *pgtypes.DoltgresType, nullable bool, checks sql.CheckConstraints, val any) error {
	vals, ok := val.([]any)
	if !ok {
		return validateDomainValue(ctx, domainType, nullable, checks, val)
	}
	for _, elem := range vals {
		if err := validateDomainArrayElements(ctx, domainType, nullable, checks, elem); err != nil {
			return err
		}
	}
	return nil
}
