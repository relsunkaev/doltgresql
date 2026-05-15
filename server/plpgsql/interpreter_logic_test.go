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

package plpgsql

import (
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

type testInterpretedFunction struct {
	returnType     *pgtypes.DoltgresType
	parameterNames []string
	parameterTypes []*pgtypes.DoltgresType
	parameterModes []uint8
	statements     []InterpreterOperation
}

var _ InterpretedFunction = testInterpretedFunction{}

func (f testInterpretedFunction) ApplyBindings(ctx *sql.Context, stack InterpreterStack, stmt string, bindings []string, enforceType bool) (string, bool, error) {
	return stmt, false, nil
}

func (f testInterpretedFunction) GetName() string {
	return "test_func"
}

func (f testInterpretedFunction) GetParameters() []*pgtypes.DoltgresType {
	return f.parameterTypes
}

func (f testInterpretedFunction) GetParameterNames() []string {
	return f.parameterNames
}

func (f testInterpretedFunction) GetParameterModes() []uint8 {
	return f.parameterModes
}

func (f testInterpretedFunction) GetReturn() *pgtypes.DoltgresType {
	if f.returnType == nil {
		return pgtypes.Void
	}
	return f.returnType
}

func (f testInterpretedFunction) GetSetConfig() map[string]string {
	return nil
}

func (f testInterpretedFunction) GetStatements() []InterpreterOperation {
	return f.statements
}

func (f testInterpretedFunction) InternalID() id.Id {
	return id.Null
}

func (f testInterpretedFunction) QueryMultiReturn(ctx *sql.Context, stack InterpreterStack, stmt string, bindings []string) (sql.Schema, []sql.Row, error) {
	return nil, nil, nil
}

func (f testInterpretedFunction) QuerySingleReturn(ctx *sql.Context, stack InterpreterStack, stmt string, targetType *pgtypes.DoltgresType, bindings []string) (any, error) {
	return nil, nil
}

func (f testInterpretedFunction) StoreCursor(ctx *sql.Context, name string, statement string, schema sql.Schema, rows []sql.Row) error {
	return nil
}

func (f testInterpretedFunction) IsSRF() bool {
	return false
}

func TestNewVariableInitializesToSQLNull(t *testing.T) {
	stack := NewInterpreterStack(nil)
	stack.NewVariable("counter", pgtypes.Int32)

	variable := stack.GetVariable("counter")
	require.NotNil(t, variable.Value)
	require.Nil(t, *variable.Value)
}

func TestParseColumnPercentType(t *testing.T) {
	tableParts, columnName, ok := parseColumnPercentType("public.items.label%TYPE")
	require.True(t, ok)
	require.Equal(t, []string{"public", "items"}, tableParts)
	require.Equal(t, "label", columnName)

	tableParts, columnName, ok = parseColumnPercentType("items.label%type")
	require.True(t, ok)
	require.Equal(t, []string{"items"}, tableParts)
	require.Equal(t, "label", columnName)

	_, _, ok = parseColumnPercentType("label%type")
	require.False(t, ok)
}

func TestParseTablePercentRowType(t *testing.T) {
	tableParts, ok := parseTablePercentRowType("public.items%ROWTYPE")
	require.True(t, ok)
	require.Equal(t, []string{"public", "items"}, tableParts)

	tableParts, ok = parseTablePercentRowType("items%rowtype")
	require.True(t, ok)
	require.Equal(t, []string{"items"}, tableParts)

	_, ok = parseTablePercentRowType("items%type")
	require.False(t, ok)
}

func TestCallVoidFunctionWithoutReturnReturnsVoidValue(t *testing.T) {
	result, err := Call(sql.NewEmptyContext(), testInterpretedFunction{returnType: pgtypes.Void}, nil, nil, nil)
	require.NoError(t, err)
	require.Equal(t, "", result)
}

func TestCallNonVoidFunctionWithoutReturnRequiresReturnValue(t *testing.T) {
	_, err := Call(sql.NewEmptyContext(), testInterpretedFunction{returnType: pgtypes.Int32}, nil, nil, nil)
	require.Error(t, err)
	require.Equal(t, pgcode.RoutineExceptionFunctionExecutedNoReturnStatement, pgerror.GetPGCode(err))
}

func TestCallInitializesOutParametersWithoutConsumingInputs(t *testing.T) {
	result, err := Call(sql.NewEmptyContext(), testInterpretedFunction{
		returnType:     pgtypes.Int32,
		parameterNames: []string{"input_value", "doubled"},
		parameterTypes: []*pgtypes.DoltgresType{pgtypes.Int32, pgtypes.Int32},
		parameterModes: []uint8{0, 1},
	}, nil, nil, []any{int32(4)})

	require.NoError(t, err)
	require.Equal(t, sql.Row{nil}, result)
}

func TestCallReturnsInoutParametersFromInputValues(t *testing.T) {
	result, err := Call(sql.NewEmptyContext(), testInterpretedFunction{
		returnType:     pgtypes.Int32,
		parameterNames: []string{"value"},
		parameterTypes: []*pgtypes.DoltgresType{pgtypes.Int32},
		parameterModes: []uint8{2},
	}, nil, nil, []any{int32(7)})

	require.NoError(t, err)
	require.Equal(t, sql.Row{int32(7)}, result)
}

func TestImplicitBareReturnInNonVoidFunctionRequiresReturnValue(t *testing.T) {
	_, err := Call(sql.NewEmptyContext(), testInterpretedFunction{
		returnType: pgtypes.Int32,
		statements: []InterpreterOperation{{
			OpCode: OpCode_Return,
		}},
	}, nil, nil, nil)

	require.Error(t, err)
	require.Equal(t, pgcode.RoutineExceptionFunctionExecutedNoReturnStatement, pgerror.GetPGCode(err))
}

func TestRaiseExceptionCarriesSQLState(t *testing.T) {
	_, err := Call(sql.NewEmptyContext(), testInterpretedFunction{
		returnType: pgtypes.Void,
		statements: []InterpreterOperation{{
			OpCode:        OpCode_Raise,
			PrimaryData:   "EXCEPTION",
			SecondaryData: []string{"boom"},
		}},
	}, nil, nil, nil)

	require.Error(t, err)
	require.Equal(t, pgcode.RaiseException, pgerror.GetPGCode(err))
}

func TestRaiseValidationErrorCarriesSyntaxSQLState(t *testing.T) {
	_, err := Call(sql.NewEmptyContext(), testInterpretedFunction{
		returnType: pgtypes.Void,
		statements: []InterpreterOperation{{
			OpCode:  OpCode_Raise,
			Options: map[string]string{raiseValidationErrorOption: "RAISE option already specified: MESSAGE"},
		}},
	}, nil, nil, nil)

	require.Error(t, err)
	require.Equal(t, pgcode.Syntax, pgerror.GetPGCode(err))
}
