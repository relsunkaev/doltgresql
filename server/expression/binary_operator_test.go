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

package expression_test

import (
	"os"
	"reflect"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	gmsexpression "github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/doltgresql/core/id"
	pgexpression "github.com/dolthub/doltgresql/server/expression"
	"github.com/dolthub/doltgresql/server/functions"
	"github.com/dolthub/doltgresql/server/functions/binary"
	"github.com/dolthub/doltgresql/server/functions/framework"
	"github.com/dolthub/doltgresql/server/functions/unary"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

var binaryOperatorBenchSink any
var unaryOperatorBenchSink any

func TestMain(m *testing.M) {
	functions.Init()
	binary.Init()
	unary.Init()
	framework.Initialize()
	os.Exit(m.Run())
}

func TestBinaryOperatorUsesQuickFunctionForExactOverload(t *testing.T) {
	ctx := sql.NewEmptyContext()
	left := gmsexpression.NewGetField(0, pgtypes.Int32, "i", true)
	right := gmsexpression.NewLiteral(int32(42), pgtypes.Int32)

	expr, err := pgexpression.NewBinaryOperator(framework.Operator_BinaryEqual).WithChildren(ctx, left, right)
	require.NoError(t, err)
	requireOperatorFunctionType(t, expr, "*framework.QuickFunction2")

	matches, err := expr.Eval(ctx, sql.NewRow(int32(42)))
	require.NoError(t, err)
	require.Equal(t, true, matches)

	matches, err = expr.Eval(ctx, sql.NewRow(int32(7)))
	require.NoError(t, err)
	require.Equal(t, false, matches)

	matches, err = expr.Eval(ctx, sql.NewRow(nil))
	require.NoError(t, err)
	require.Nil(t, matches)
}

func TestBinaryOperatorKeepsCompiledFunctionWhenCastsAreNeeded(t *testing.T) {
	ctx := sql.NewEmptyContext()
	left := gmsexpression.NewGetField(0, pgtypes.Int32, "i", true)
	right := gmsexpression.NewLiteral("42", pgtypes.Unknown)

	expr, err := pgexpression.NewBinaryOperator(framework.Operator_BinaryEqual).WithChildren(ctx, left, right)
	require.NoError(t, err)
	requireOperatorFunctionType(t, expr, "*framework.CompiledFunction")
}

func TestBinaryOperatorDomainBpcharEqualityReturnsBool(t *testing.T) {
	ctx := sql.NewEmptyContext()
	baseType, err := pgtypes.NewCharType(3)
	require.NoError(t, err)
	domainType := pgtypes.NewDomainType(ctx, baseType, "", false, nil, id.NewType("public", "_char3_domain"), id.NewType("public", "char3_domain"))
	left := gmsexpression.NewGetField(0, domainType, "c", true)
	right := gmsexpression.NewLiteral("ab ", baseType)

	expr, err := pgexpression.NewBinaryOperator(framework.Operator_BinaryEqual).WithChildren(ctx, left, right)
	require.NoError(t, err)
	require.Equal(t, pgtypes.Bool, expr.Type(ctx))

	result, err := expr.Eval(ctx, sql.NewRow("ab "))
	require.NoError(t, err)
	require.Equal(t, true, result)
}

func TestUnaryOperatorUsesQuickFunctionForExactOverload(t *testing.T) {
	ctx := sql.NewEmptyContext()
	child := gmsexpression.NewGetField(0, pgtypes.Int32, "i", true)

	exprAny, err := pgexpression.NewUnaryOperator(framework.Operator_UnaryMinus).WithResolvedChildren(ctx, []any{child})
	require.NoError(t, err)
	expr := exprAny.(sql.Expression)
	requireOperatorFunctionType(t, expr, "*framework.QuickFunction1")

	result, err := expr.Eval(ctx, sql.NewRow(int32(42)))
	require.NoError(t, err)
	require.Equal(t, int32(-42), result)

	result, err = expr.Eval(ctx, sql.NewRow(nil))
	require.NoError(t, err)
	require.Nil(t, result)
}

func requireOperatorFunctionType(t *testing.T, expr sql.Expression, expected string) {
	t.Helper()
	compiledFunc := reflect.ValueOf(expr).Elem().FieldByName("compiledFunc")
	require.False(t, compiledFunc.IsNil())
	require.Equal(t, expected, compiledFunc.Elem().Type().String())
}

func BenchmarkBinaryOperatorInt32Equality(b *testing.B) {
	ctx := sql.NewEmptyContext()
	left := gmsexpression.NewGetField(0, pgtypes.Int32, "i", false)
	right := gmsexpression.NewLiteral(int32(42), pgtypes.Int32)
	expr, err := pgexpression.NewBinaryOperator(framework.Operator_BinaryEqual).WithChildren(ctx, left, right)
	require.NoError(b, err)
	row := sql.NewRow(int32(42))

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		binaryOperatorBenchSink, err = expr.Eval(ctx, row)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUnaryOperatorInt32Minus(b *testing.B) {
	ctx := sql.NewEmptyContext()
	child := gmsexpression.NewGetField(0, pgtypes.Int32, "i", false)
	exprAny, err := pgexpression.NewUnaryOperator(framework.Operator_UnaryMinus).WithResolvedChildren(ctx, []any{child})
	require.NoError(b, err)
	expr := exprAny.(sql.Expression)
	row := sql.NewRow(int32(42))

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		unaryOperatorBenchSink, err = expr.Eval(ctx, row)
		if err != nil {
			b.Fatal(err)
		}
	}
}
