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

package framework

import (
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	sqlexpression "github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/core/procedures"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

func TestCompiledFunctionStringSchemaQualifiesResolvedUserFunction(t *testing.T) {
	fn := SQLFunction{
		ID:           id.NewFunction(`mixed"schema`, "lookup_default"),
		ReturnType:   pgtypes.Int32,
		SqlStatement: "SELECT 1",
	}
	compiled := &CompiledFunction{
		Name: "lookup_default",
		overload: overloadMatch{
			params: Overload{function: fn},
		},
	}

	require.Equal(t, `"mixed""schema"."lookup_default"()`, compiled.String())
}

func TestCompiledFunctionUserDefinedFunctionsRequireStatementRunner(t *testing.T) {
	ctx := sql.NewEmptyContext()

	for _, fn := range []FunctionInterface{
		InterpretedFunction{ID: id.NewFunction("public", "volatile_plpgsql"), ReturnType: pgtypes.Int32},
		SQLFunction{ID: id.NewFunction("public", "volatile_sql"), ReturnType: pgtypes.Int32},
	} {
		compiled := &CompiledFunction{Name: fn.GetName()}
		_, err := compiled.callResolvedFunction(ctx, fn, nil)
		require.ErrorContains(t, err, "statement runner is not available")
	}
}

func TestOverloadsIgnorePureOutParametersForCallableSignature(t *testing.T) {
	overloads := NewOverloads()
	err := overloads.Add(InterpretedFunction{
		ID:             id.NewFunction("public", "out_value", pgtypes.Int32.ID),
		ReturnType:     pgtypes.Int32,
		ParameterTypes: []*pgtypes.DoltgresType{pgtypes.Int32, pgtypes.Int32, pgtypes.Int32},
		ParameterModes: []uint8{
			uint8(procedures.ParameterMode_IN),
			uint8(procedures.ParameterMode_OUT),
			uint8(procedures.ParameterMode_OUT),
		},
	})

	require.NoError(t, err)
	_, ok := overloads.ExactMatchForTypes(pgtypes.Int32)
	require.True(t, ok)
	require.Len(t, overloads.overloadsForParams(1), 1)
	require.Empty(t, overloads.overloadsForParams(3))
}

func TestCatalogInternalCharTypeForUnknownOperatorLiteral(t *testing.T) {
	compiled := &CompiledFunction{Arguments: []sql.Expression{
		sqlexpression.NewGetFieldWithTable(0, 0, pgtypes.Oid, "", "pg_attribute", "attgenerated", false),
		sqlexpression.NewLiteral("", pgtypes.Unknown),
	}}

	require.Same(t, pgtypes.InternalChar, compiled.catalogInternalCharTypeForUnknownOperatorLiteral(pgtypes.Oid, 1, 0))

	compiled.Arguments[0] = sqlexpression.NewGetFieldWithTable(0, 0, pgtypes.Oid, "", "pg_attribute", "attrelid", false)
	require.Same(t, pgtypes.Oid, compiled.catalogInternalCharTypeForUnknownOperatorLiteral(pgtypes.Oid, 1, 0))
}
