// Copyright 2025 Dolthub, Inc.
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
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/core/typecollection"
	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
	"github.com/dolthub/doltgresql/postgres/parser/types"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// InterpretedFunction is an interface that essentially mirrors the implementation of InterpretedFunction in the
// framework package.
type InterpretedFunction interface {
	ApplyBindings(ctx *sql.Context, stack InterpreterStack, stmt string, bindings []string, enforceType bool) (newStmt string, varFound bool, err error)
	GetName() string
	GetParameters() []*pgtypes.DoltgresType
	GetParameterNames() []string
	GetParameterModes() []uint8
	GetReturn() *pgtypes.DoltgresType
	GetSetConfig() map[string]string
	GetStatements() []InterpreterOperation
	InternalID() id.Id
	QueryMultiReturn(ctx *sql.Context, stack InterpreterStack, stmt string, bindings []string) (schema sql.Schema, rows []sql.Row, err error)
	QuerySingleReturn(ctx *sql.Context, stack InterpreterStack, stmt string, targetType *pgtypes.DoltgresType, bindings []string) (val any, err error)
	// IsSRF returns whether the function is a set returning function, meaning whether the
	// function returns one or more rows as a result.
	IsSRF() bool
}

// GetTypesCollectionFromContext is declared within the core package, but is assigned to this variable to work around
// import cycles.
var GetTypesCollectionFromContext func(ctx *sql.Context) (*typecollection.TypeCollection, error)

const (
	diagnosticOptionAction            = "pgContextAction"
	diagnosticOptionLineNumber        = "lineNumber"
	diagnosticOptionStatement         = "pgContextStatement"
	dmlReturningIntoOption            = "dmlReturningInto"
	integerForLoopFoundOption         = "integerForLoopFound"
	raiseValidationErrorOption        = "raiseValidationError"
	notNullVariableOption             = "notNullVariable"
	transactionControlNoop            = "transactionControlNoop"
	assertMessageQueryOption          = "assertMessageQuery"
	assertConditionBindingCountOption = "assertConditionBindingCount"
)

// The interpreter has no async statement timeout hook, so bound runaway loops
// at operation boundaries to match PostgreSQL's query-canceled behavior.
const maxInterpretedFunctionOperations = 10000

var plpgsqlExceptionSavepointCounter uint64

type interpreterExecutionState struct {
	statements           []InterpreterOperation
	operationCount       int
	lastRowCount         int64
	lastExceptionContext string
	integerForLoops      map[int]bool
	stackedDiagnostics   *plpgsqlExceptionDiagnostics
}

type plpgsqlExceptionDiagnostics struct {
	MessageText      string
	ReturnedSQLState string
	ColumnName       string
	ConstraintName   string
	DataTypeName     string
	TableName        string
	SchemaName       string
	Detail           string
	Hint             string
	Context          string
}

type plpgsqlExceptionError struct {
	diagnostics plpgsqlExceptionDiagnostics
}

func (e plpgsqlExceptionError) Error() string {
	return e.diagnostics.MessageText
}

func createExceptionBlockSavepoint(ctx *sql.Context) (string, bool, error) {
	tx := ctx.GetTransaction()
	if tx == nil {
		return "", false, nil
	}
	txSession, ok := ctx.Session.(sql.TransactionSession)
	if !ok {
		return "", false, nil
	}
	name := fmt.Sprintf("__doltgresql_plpgsql_exception_%d", atomic.AddUint64(&plpgsqlExceptionSavepointCounter, 1))
	if err := txSession.CreateSavepoint(ctx, tx, name); err != nil {
		return "", false, err
	}
	return name, true, nil
}

func rollbackExceptionBlockSavepoint(ctx *sql.Context, name string) error {
	tx := ctx.GetTransaction()
	if tx == nil {
		return nil
	}
	txSession, ok := ctx.Session.(sql.TransactionSession)
	if !ok {
		return nil
	}
	if err := txSession.RollbackToSavepoint(ctx, tx, name); err != nil {
		return err
	}
	return txSession.ReleaseSavepoint(ctx, ctx.GetTransaction(), name)
}

func releaseExceptionBlockSavepoint(ctx *sql.Context, name string) error {
	tx := ctx.GetTransaction()
	if tx == nil {
		return nil
	}
	txSession, ok := ctx.Session.(sql.TransactionSession)
	if !ok {
		return nil
	}
	return txSession.ReleaseSavepoint(ctx, tx, name)
}

// Call runs the contained operations on the given runner.
func Call(ctx *sql.Context, iFunc InterpretedFunction, runner sql.StatementRunner, paramsAndReturn []*pgtypes.DoltgresType, vals []any) (any, error) {
	// Set up the initial state of the function
	stack := NewInterpreterStack(runner)
	// Add the parameters
	parameterTypes := iFunc.GetParameters()
	parameterNames := iFunc.GetParameterNames()
	if len(vals) != len(parameterTypes) {
		return nil, fmt.Errorf("parameter count mismatch: expected %d got %d", len(parameterTypes), len(vals))
	}
	for i := range vals {
		stack.NewVariableWithValue(parameterNames[i], parameterTypes[i], vals[i])
	}
	if statementsUseFoundVariable(iFunc.GetStatements()) {
		initFoundVariable(stack)
	}
	restoreSetConfig, err := applyRoutineSetConfig(ctx, iFunc.GetSetConfig())
	if err != nil {
		return nil, err
	}
	defer restoreSetConfig()
	restoreDiagnosticContext := pushDiagnosticCallFrame(ctx, iFunc)
	defer restoreDiagnosticContext()
	result, returned, err := call(ctx, iFunc, stack)
	if err != nil || returned {
		return result, err
	}
	if iFunc.IsSRF() {
		return sql.RowsToRowIter(), nil
	}
	if outputRow := procedureOutputRow(iFunc, stack); outputRow != nil {
		return outputRow, nil
	}
	if iFunc.GetReturn().ID == pgtypes.Void.ID {
		return "", nil
	}
	return nil, functionExecutedNoReturnStatementError()
}

func procedureOutputRow(iFunc InterpretedFunction, stack InterpreterStack) sql.Row {
	modes := iFunc.GetParameterModes()
	if len(modes) == 0 {
		return nil
	}
	names := iFunc.GetParameterNames()
	outputRow := make(sql.Row, 0)
	for i, mode := range modes {
		if mode != 1 && mode != 2 {
			continue
		}
		if i >= len(names) {
			continue
		}
		variable := stack.GetVariable(names[i])
		if variable.Type == nil || variable.Value == nil {
			outputRow = append(outputRow, nil)
		} else {
			outputRow = append(outputRow, *variable.Value)
		}
	}
	if len(outputRow) == 0 {
		return nil
	}
	return outputRow
}

// TriggerCall runs the contained trigger operations on the given runner.
func TriggerCall(ctx *sql.Context, iFunc InterpretedFunction, runner sql.StatementRunner, sch sql.Schema, oldRow sql.Row, newRow sql.Row, trigVars map[string]any) (any, error) {
	// Set up the initial state of the function
	stack := NewInterpreterStack(runner)
	// Add the special variables
	stack.NewRecord("OLD", sch, oldRow)
	stack.NewRecord("NEW", sch, newRow)
	for varName, val := range trigVars {
		varType, ok := triggerSpecialVariables[varName]
		if !ok {
			return nil, fmt.Errorf("unknown variable %s for trigger", varName)
		}
		stack.NewVariableWithValue(varName, varType, val)
	}
	if statementsUseFoundVariable(iFunc.GetStatements()) {
		initFoundVariable(stack)
	}
	restoreDiagnosticContext := pushDiagnosticCallFrame(ctx, iFunc)
	defer restoreDiagnosticContext()
	result, _, err := call(ctx, iFunc, stack)
	return result, err
}

// call runs the contained operations on the given runner.
func call(ctx *sql.Context, iFunc InterpretedFunction, stack InterpreterStack) (any, bool, error) {
	state := &interpreterExecutionState{
		statements: iFunc.GetStatements(),
	}
	ret, returned, err := runOperations(ctx, iFunc, stack, state, 0, len(state.statements))
	if err != nil {
		return nil, false, err
	}
	if returned {
		return ret, true, nil
	}
	return nil, false, nil
}

func runExceptionHandlerOperations(
	ctx *sql.Context,
	iFunc InterpretedFunction,
	stack InterpreterStack,
	state *interpreterExecutionState,
	diagnostics plpgsqlExceptionDiagnostics,
	handler exceptionHandlerOperation,
) (any, bool, error) {
	stack.PushScope()
	stack.NewVariableWithValue("SQLSTATE", pgtypes.Text, diagnostics.ReturnedSQLState)
	stack.NewVariableWithValue("SQLERRM", pgtypes.Text, diagnostics.MessageText)
	defer stack.PopScope()
	return runOperations(ctx, iFunc, stack, state, handler.start, handler.end)
}

func runOperations(ctx *sql.Context, iFunc InterpretedFunction, stack InterpreterStack, state *interpreterExecutionState, start, end int) (any, bool, error) {
	// We increment before accessing, so start at -1
	counter := start - 1
	// Run the statements
	statements := state.statements
	for {
		if err := checkInterpreterExecutionBudget(ctx, state); err != nil {
			return nil, false, err
		}
		counter++
		if counter >= end {
			break
		} else if counter < 0 {
			panic("negative function counter")
		}

		operation := statements[counter]
		switch operation.OpCode {
		case OpCode_Alias:
			iv := stack.GetVariable(operation.PrimaryData)
			if iv.Type == nil {
				return nil, false, fmt.Errorf("variable `%s` could not be found", operation.PrimaryData)
			}
			stack.NewVariableAlias(operation.Target, operation.PrimaryData)
		case OpCode_Assign:
			iv := stack.GetVariable(operation.Target)
			if iv.Type == nil {
				return nil, false, fmt.Errorf("variable `%s` could not be found", operation.Target)
			}
			restoreCallSite := pushDiagnosticCallSite(ctx, operation)
			retVal, err := iFunc.QuerySingleReturn(ctx, stack, operation.PrimaryData, iv.Type, operation.SecondaryData)
			restoreCallSite()
			if err != nil {
				state.lastExceptionContext = diagnosticPGExceptionContext(ctx, iFunc, operation)
				return nil, false, err
			}
			err = stack.SetVariable(ctx, operation.Target, retVal)
			if err != nil {
				return nil, false, err
			}
		case OpCode_Assert:
			conditionBindingCount := len(operation.SecondaryData)
			if countText := operation.Options[assertConditionBindingCountOption]; countText != "" {
				count, err := strconv.Atoi(countText)
				if err != nil {
					return nil, false, err
				}
				conditionBindingCount = count
			}
			if conditionBindingCount < 0 || conditionBindingCount > len(operation.SecondaryData) {
				return nil, false, fmt.Errorf("invalid ASSERT binding count %d", conditionBindingCount)
			}
			conditionRefs := operation.SecondaryData[:conditionBindingCount]
			messageRefs := operation.SecondaryData[conditionBindingCount:]
			restoreCallSite := pushDiagnosticCallSite(ctx, operation)
			retVal, err := iFunc.QuerySingleReturn(ctx, stack, operation.PrimaryData, pgtypes.Bool, conditionRefs)
			restoreCallSite()
			if err != nil {
				state.lastExceptionContext = diagnosticPGExceptionContext(ctx, iFunc, operation)
				return nil, false, err
			}
			condition, _ := retVal.(bool)
			if condition {
				continue
			}
			message := "assertion failed"
			if messageQuery := operation.Options[assertMessageQueryOption]; messageQuery != "" {
				restoreCallSite = pushDiagnosticCallSite(ctx, operation)
				retVal, err = iFunc.QuerySingleReturn(ctx, stack, messageQuery, pgtypes.Text, messageRefs)
				restoreCallSite()
				if err != nil {
					state.lastExceptionContext = diagnosticPGExceptionContext(ctx, iFunc, operation)
					return nil, false, err
				}
				if retVal != nil {
					message = fmt.Sprintf("%v", retVal)
				}
			}
			return nil, false, plpgsqlExceptionErrorFromDiagnostics(plpgsqlExceptionDiagnostics{
				MessageText:      message,
				ReturnedSQLState: pgcode.AssertFailure.String(),
				Context:          diagnosticPGExceptionContext(ctx, iFunc, operation),
			})
		case OpCode_Declare:
			typeCollection, err := GetTypesCollectionFromContext(ctx)
			if err != nil {
				return nil, false, err
			}

			recordSchema, resolvedRowType, err := resolveTablePercentRowType(ctx, iFunc, stack, operation.PrimaryData)
			if err != nil {
				return nil, false, err
			}
			if resolvedRowType {
				stack.NewRecord(operation.Target, recordSchema, nil)
				continue
			}

			resolvedType, resolvedColumnType, err := resolveColumnPercentType(ctx, iFunc, stack, operation.PrimaryData)
			if err != nil {
				return nil, false, err
			}
			if !resolvedColumnType {
				schemaName, typeName := normalizeDeclareTypeName(operation.PrimaryData)
				if (schemaName == "" || schemaName == "pg_catalog") && strings.EqualFold(typeName, "record") {
					stack.NewRecord(operation.Target, nil, nil)
					continue
				}
				resolvedType, err = resolveDeclareType(ctx, typeCollection, operation.PrimaryData)
				if err != nil {
					return nil, false, err
				}
			}
			if resolvedType == nil {
				return nil, false, pgtypes.ErrTypeDoesNotExist.New(operation.PrimaryData)
			}
			if len(operation.SecondaryData) != 0 {
				defVal := operation.SecondaryData[0]
				notNull := operation.Options[notNullVariableOption] == "true"
				// Default value can be a literal value or a reference to parameter
				isParam := false
				for _, param := range iFunc.GetParameterNames() {
					if param == defVal {
						isParam = true
						break
					}
				}
				if isParam {
					ivr := stack.GetVariable(defVal)
					if ivr.Value != nil {
						stack.NewVariableWithValueAndNotNull(operation.Target, resolvedType, *ivr.Value, notNull)
					} else {
						stack.NewVariableWithValueAndNotNull(operation.Target, resolvedType, nil, notNull)
					}
				} else {
					val, err := resolvedType.IoInput(ctx, strings.Trim(operation.SecondaryData[0], "'"))
					if err != nil {
						return nil, false, err
					}
					stack.NewVariableWithValueAndNotNull(operation.Target, resolvedType, val, notNull)
				}
			} else {
				stack.NewVariableWithValueAndNotNull(operation.Target, resolvedType, nil, operation.Options[notNullVariableOption] == "true")
			}
		case OpCode_DeleteInto:
			// TODO: implement
		case OpCode_Exception:
			handlers, handlerEnd, err := exceptionHandlersFromOperation(operation)
			if err != nil {
				return nil, false, err
			}
			savepointName, hasSavepoint, err := createExceptionBlockSavepoint(ctx)
			if err != nil {
				return nil, false, err
			}
			ret, returned, err := runOperations(ctx, iFunc, stack, state, counter+1, operation.Index)
			if err != nil {
				if hasSavepoint {
					if rollbackErr := rollbackExceptionBlockSavepoint(ctx, savepointName); rollbackErr != nil {
						return nil, false, fmt.Errorf("%w; exception block rollback failed: %v", err, rollbackErr)
					}
				}
				diagnostics := plpgsqlExceptionDiagnosticsFromError(err)
				if diagnostics.Context == "" {
					diagnostics.Context = state.lastExceptionContext
				}
				handler, ok := matchingExceptionHandler(handlers, diagnostics)
				if !ok {
					return nil, false, err
				}
				state.lastExceptionContext = ""
				priorDiagnostics := state.stackedDiagnostics
				state.stackedDiagnostics = &diagnostics
				ret, returned, err = runExceptionHandlerOperations(ctx, iFunc, stack, state, diagnostics, handler)
				state.stackedDiagnostics = priorDiagnostics
				if err != nil {
					return nil, false, err
				}
				if returned {
					return ret, true, nil
				}
			} else {
				if hasSavepoint {
					if err = releaseExceptionBlockSavepoint(ctx, savepointName); err != nil {
						return nil, false, err
					}
				}
				if returned {
					return ret, true, nil
				}
			}
			counter = handlerEnd - 1
		case OpCode_Execute:
			if operation.Options[transactionControlNoop] == "true" {
				continue
			}
			statement := operation.PrimaryData
			bindings := operation.SecondaryData
			isDynamicExecute := operation.Options["dynamic"] == "true"
			dynamicUsingScope := false
			if isDynamicExecute {
				queryBindingCount, err := strconv.Atoi(operation.Options["queryBindingCount"])
				if err != nil {
					return nil, false, err
				}
				if queryBindingCount > len(bindings) {
					return nil, false, errors.New("dynamic execute query binding count exceeds available bindings")
				}
				queryBindings := bindings[:queryBindingCount]
				bindings = bindings[queryBindingCount:]
				queryVal, err := iFunc.QuerySingleReturn(ctx, stack, "SELECT "+statement, pgtypes.Text, queryBindings)
				if err != nil {
					state.lastExceptionContext = diagnosticPGExceptionContext(ctx, iFunc, operation)
					return nil, false, err
				}
				if queryVal == nil {
					return nil, false, errors.New("query string argument of EXECUTE is null")
				}
				statement = queryVal.(string)
				if len(bindings) > 0 {
					stack.PushScope()
					dynamicUsingScope = true
					bindings, err = evaluateDynamicExecuteUsingParams(ctx, iFunc, stack, bindings)
					if err != nil {
						stack.PopScope()
						return nil, false, err
					}
				}
			}
			queryMultiReturn := func() (sql.Schema, []sql.Row, error) {
				if dynamicUsingScope {
					defer stack.PopScope()
				}
				restoreCallSite := pushDiagnosticCallSite(ctx, operation)
				defer restoreCallSite()
				sch, rows, err := iFunc.QueryMultiReturn(ctx, stack, statement, bindings)
				if err != nil {
					state.lastExceptionContext = diagnosticPGExceptionContext(ctx, iFunc, operation)
				}
				return sch, rows, err
			}
			if len(operation.Target) > 0 {
				sch, rows, err := queryMultiReturn()
				if err != nil {
					return nil, false, err
				}
				state.lastRowCount = rowCountFromResultRows(rows)
				found := state.lastRowCount > 0
				if !isDynamicExecute {
					if err = setFoundVariable(ctx, stack, found); err != nil {
						return nil, false, err
					}
				}
				strict := operation.Options["strict"] == "true"
				if strict && !found {
					return nil, false, errors.New("query returned no rows")
				}
				if strict && len(rows) > 1 {
					return nil, false, errors.New("query returned more than one row")
				}
				if operation.Options[dmlReturningIntoOption] == "true" && len(rows) > 1 {
					return nil, false, errors.New("query returned more than one row")
				}
				if vars := strings.Split(operation.Target, ","); len(vars) > 1 {
					// multiple column row result
					if !found {
						for _, variableName := range vars {
							if err = stack.SetVariable(ctx, variableName, nil); err != nil {
								return nil, false, err
							}
						}
					} else {
						row := rows[0]
						if len(row) != len(vars) || len(sch) != len(vars) {
							return nil, false, errors.New("number of row values does not match number of schema columns")
						}
						for i, variableName := range vars {
							if err = assignSQLRowValue(ctx, stack, variableName, sch[i].Type, row[i]); err != nil {
								return nil, false, err
							}
						}
					}
				} else {
					// single column
					if !found {
						if stack.IsRecordVariable(operation.Target) {
							if err = stack.UpdateRecord(operation.Target, nil, nil); err != nil {
								return nil, false, err
							}
							continue
						}
						if err = stack.SetVariable(ctx, operation.Target, nil); err != nil {
							return nil, false, err
						}
					} else {
						if stack.IsRecordVariable(operation.Target) {
							if err = stack.UpdateRecord(operation.Target, sch, rows[0]); err != nil {
								return nil, false, err
							}
							continue
						}
						target := stack.GetVariable(operation.Target)
						if target.Type != nil && target.Type.IsCompositeType() && !target.Type.IsRecordType() &&
							len(rows[0]) == len(target.Type.CompositeAttrs) && len(sch) == len(target.Type.CompositeAttrs) {
							if err = assignSQLCompositeRowValue(ctx, stack, operation.Target, sch, rows[0]); err != nil {
								return nil, false, err
							}
							continue
						}
						if len(rows[0]) != 1 || len(sch) != 1 {
							return nil, false, errors.New("expression returned multiple results")
						}
						if err = assignSQLRowValue(ctx, stack, operation.Target, sch[0].Type, rows[0][0]); err != nil {
							return nil, false, err
						}
					}
				}
			} else {
				_, rows, err := queryMultiReturn()
				if err != nil {
					return nil, false, err
				}
				state.lastRowCount = rowCountFromResultRows(rows)
				if !isDynamicExecute {
					if err = setFoundVariable(ctx, stack, state.lastRowCount > 0); err != nil {
						return nil, false, err
					}
				}
			}
		case OpCode_Get:
			if operation.Options["stacked"] == "true" {
				if state.stackedDiagnostics == nil {
					return nil, false, errors.New("GET STACKED DIAGNOSTICS cannot be used outside an exception handler")
				}
				value, ok := stackedDiagnosticValue(*state.stackedDiagnostics, operation.PrimaryData)
				if !ok {
					return nil, false, fmt.Errorf("GET STACKED DIAGNOSTICS item %s is not supported", operation.PrimaryData)
				}
				if err := assignSQLRowValue(ctx, stack, operation.Target, pgtypes.Text, value); err != nil {
					return nil, false, err
				}
			} else {
				switch operation.PrimaryData {
				case "ROW_COUNT":
					if err := assignSQLRowValue(ctx, stack, operation.Target, pgtypes.Int64, state.lastRowCount); err != nil {
						return nil, false, err
					}
				case "PG_CONTEXT":
					if err := assignSQLRowValue(ctx, stack, operation.Target, pgtypes.Text, diagnosticPGContext(ctx, iFunc, operation)); err != nil {
						return nil, false, err
					}
				case "PG_ROUTINE_OID":
					if err := assignSQLRowValue(ctx, stack, operation.Target, pgtypes.Oid, diagnosticPGRoutineOID(iFunc)); err != nil {
						return nil, false, err
					}
				default:
					return nil, false, fmt.Errorf("GET DIAGNOSTICS item %s is not supported", operation.PrimaryData)
				}
			}
		case OpCode_Goto:
			// We must compare to the index - 1, so that the increment hits our target
			if counter <= operation.Index {
				for ; counter < operation.Index-1; counter++ {
					switch statements[counter].OpCode {
					case OpCode_ScopeBegin:
						stack.PushScope()
					case OpCode_ScopeEnd:
						stack.PopScope()
					}
				}
			} else {
				for ; counter > operation.Index-1; counter-- {
					switch statements[counter].OpCode {
					case OpCode_ScopeBegin:
						stack.PopScope()
					case OpCode_ScopeEnd:
						stack.PushScope()
					}
				}
			}
		case OpCode_If:
			retVal, err := iFunc.QuerySingleReturn(ctx, stack, operation.PrimaryData, pgtypes.Bool, operation.SecondaryData)
			if err != nil {
				state.lastExceptionContext = diagnosticPGExceptionContext(ctx, iFunc, operation)
				return nil, false, err
			}
			condition, _ := retVal.(bool)
			if operation.Options[integerForLoopFoundOption] == "true" {
				if condition {
					if state.integerForLoops == nil {
						state.integerForLoops = make(map[int]bool)
					}
					state.integerForLoops[counter] = true
				} else {
					if err := setFoundVariable(ctx, stack, state.integerForLoops[counter]); err != nil {
						return nil, false, err
					}
					delete(state.integerForLoops, counter)
				}
			}
			if condition {
				// We're never changing the scope, so we can just assign it directly.
				// Also, we must assign to index-1, so that the increment hits our target.
				counter = operation.Index - 1
			}
		case OpCode_InsertInto:
			// TODO: implement
		case OpCode_Perform:
			restoreCallSite := pushDiagnosticCallSite(ctx, operation)
			_, rows, err := iFunc.QueryMultiReturn(ctx, stack, operation.PrimaryData, operation.SecondaryData)
			restoreCallSite()
			if err != nil {
				state.lastExceptionContext = diagnosticPGExceptionContext(ctx, iFunc, operation)
				return nil, false, err
			}
			state.lastRowCount = rowCountFromResultRows(rows)
			if err = setFoundVariable(ctx, stack, state.lastRowCount > 0); err != nil {
				return nil, false, err
			}
		case OpCode_Raise:
			// TODO: Use the client_min_messages config param to determine which
			//       notice levels to send to the client.
			// https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-CLIENT-MIN-MESSAGES

			if validationErr := operation.Options[raiseValidationErrorOption]; validationErr != "" {
				return nil, false, pgerror.New(pgcode.Syntax, validationErr)
			}
			if isBareRaiseOperation(operation) {
				if state.stackedDiagnostics == nil {
					return nil, false, pgerror.New(pgcode.StackedDiagnosticsAccessedWithoutActiveHandler, "RAISE without parameters cannot be used outside an exception handler")
				}
				return nil, false, plpgsqlExceptionErrorFromDiagnostics(*state.stackedDiagnostics)
			}

			message, err := evaluteNoticeMessage(ctx, iFunc, operation, stack)
			if err != nil {
				return nil, false, err
			}

			if operation.PrimaryData == "EXCEPTION" {
				// TODO: Notices at the EXCEPTION level should also abort the current tx.
				diagnostics := plpgsqlExceptionDiagnosticsFromRaise(ctx, iFunc, operation, message)
				return nil, false, plpgsqlExceptionErrorFromDiagnostics(diagnostics)
			} else {
				noticeResponse := &pgproto3.NoticeResponse{
					Severity: operation.PrimaryData,
					Message:  message,
				}
				if err = applyNoticeOptions(ctx, noticeResponse, operation.Options); err != nil {
					return nil, false, err
				}
				sess := dsess.DSessFromSess(ctx.Session)
				sess.Notice(noticeResponse)
			}
		case OpCode_Return:
			// If RETURN QUERY results are being buffered, return those
			if len(stack.ReturnQueryResults()) > 0 {
				records := stack.ReturnQueryResults()

				rows := make([]sql.Row, len(records))
				for i, record := range records {
					rows[i] = returnRecordToRow(iFunc.GetReturn(), record)
				}

				return sql.RowsToRowIter(rows...), true, nil
			}

			if len(operation.PrimaryData) == 0 {
				if iFunc.IsSRF() {
					return sql.RowsToRowIter(), true, nil
				}
				if outputRow := procedureOutputRow(iFunc, stack); outputRow != nil {
					return outputRow, true, nil
				}
				if iFunc.GetReturn().ID == pgtypes.Void.ID {
					return "", true, nil
				}
				if isImplicitBareReturn(operation) {
					return nil, false, functionExecutedNoReturnStatementError()
				}
				return nil, true, nil
			}

			// TODO: handle record types properly, we'll special case triggers for now
			if iFunc.GetReturn().ID == pgtypes.Trigger.ID && len(operation.SecondaryData) == 1 {
				normalized := strings.ReplaceAll(strings.ToLower(operation.PrimaryData), " ", "")
				if normalized == "select$1;" {
					if strings.EqualFold(operation.SecondaryData[0], "new") {
						return *stack.GetVariable("NEW").Value, true, nil
					} else if strings.EqualFold(operation.SecondaryData[0], "old") {
						return *stack.GetVariable("OLD").Value, true, nil
					}
				}
			}
			if len(operation.SecondaryData) == 1 {
				normalized := strings.ReplaceAll(strings.ToLower(operation.PrimaryData), " ", "")
				if normalized == "select$1;" {
					retVariable := stack.GetVariable(operation.SecondaryData[0])
					if retVariable.Type != nil && retVariable.Type.ID == iFunc.GetReturn().ID {
						retVal := *retVariable.Value
						if iFunc.IsSRF() {
							return sql.RowsToRowIter(sql.Row{retVal}), true, nil
						}
						return retVal, true, nil
					}
				}
			}
			val, err := iFunc.QuerySingleReturn(ctx, stack, operation.PrimaryData, iFunc.GetReturn(), operation.SecondaryData)

			// If this is a set returning function, then we need to return a RowIter and wrap
			// the composite value in a sql.Row.
			if iFunc.IsSRF() {
				return sql.RowsToRowIter(sql.Row{val}), true, nil
			}
			if err != nil {
				state.lastExceptionContext = diagnosticPGExceptionContext(ctx, iFunc, operation)
				return nil, false, err
			}
			return val, true, nil

		case OpCode_ForQueryInit:
			statement := operation.PrimaryData
			bindings := operation.SecondaryData
			var cleanupDynamicForQuery func()
			if operation.Options["dynamic"] == "true" {
				queryBindingCount, err := strconv.Atoi(operation.Options["queryBindingCount"])
				if err != nil {
					return nil, false, err
				}
				statement, bindings, cleanupDynamicForQuery, err = prepareDynamicStatement(ctx, iFunc, stack, statement, bindings, queryBindingCount)
				if err != nil {
					return nil, false, err
				}
			}
			schema, rows, err := iFunc.QueryMultiReturn(ctx, stack, statement, bindings)
			if cleanupDynamicForQuery != nil {
				cleanupDynamicForQuery()
			}
			if err != nil {
				state.lastExceptionContext = diagnosticPGExceptionContext(ctx, iFunc, operation)
				return nil, false, err
			}
			state.lastRowCount = rowCountFromResultRows(rows)
			stack.InitCursor(operation.Target, schema, rows)
		case OpCode_ForEachInit:
			targetName := operation.Options["target"]
			target := stack.GetVariable(targetName)
			if target.Type == nil {
				return nil, false, fmt.Errorf("variable `%s` could not be found", targetName)
			}
			slice, err := strconv.Atoi(operation.Options["slice"])
			if err != nil {
				return nil, false, err
			}
			restoreCallSite := pushDiagnosticCallSite(ctx, operation)
			schema, rows, err := iFunc.QueryMultiReturn(ctx, stack, "SELECT "+operation.PrimaryData+";", operation.SecondaryData)
			restoreCallSite()
			if err != nil {
				state.lastExceptionContext = diagnosticPGExceptionContext(ctx, iFunc, operation)
				return nil, false, err
			}
			if len(schema) != 1 || len(rows) != 1 || len(rows[0]) != 1 {
				return nil, false, errors.New("FOREACH expression must return a single array value")
			}
			values, err := foreachArrayIterationValues(rows[0][0], int32(slice))
			if err != nil {
				return nil, false, err
			}
			cursorRows := make([]sql.Row, len(values))
			for i, value := range values {
				cursorRows[i] = sql.Row{value}
			}
			state.lastRowCount = int64(len(cursorRows))
			stack.InitCursor(operation.Target, sql.Schema{{Name: targetName, Type: target.Type}}, cursorRows)
		case OpCode_ForQueryNext:
			schema, row, ok := stack.AdvanceCursor(operation.PrimaryData)
			if !ok {
				if err := setFoundVariable(ctx, stack, stack.CursorAdvanced(operation.PrimaryData)); err != nil {
					return nil, false, err
				}
				stack.CloseCursor(operation.PrimaryData)
				// Jump forward past the loop body and back-goto, same mechanism as OpCode_If.
				counter = operation.Index - 1
			} else {
				if err := assignForQueryRow(ctx, stack, operation.Target, schema, row); err != nil {
					return nil, false, err
				}
			}
		case OpCode_CursorFetch:
			schema, row, ok := stack.AdvanceCursor(operation.PrimaryData)
			if !ok {
				if err := setFoundVariable(ctx, stack, false); err != nil {
					return nil, false, err
				}
				state.lastRowCount = 0
				continue
			}
			if err := assignForQueryRow(ctx, stack, operation.Target, schema, row); err != nil {
				return nil, false, err
			}
			if err := setFoundVariable(ctx, stack, true); err != nil {
				return nil, false, err
			}
			state.lastRowCount = 1
		case OpCode_CursorClose:
			stack.CloseCursor(operation.PrimaryData)
		case OpCode_ReturnQuery:
			statement := operation.PrimaryData
			bindings := operation.SecondaryData
			var cleanupDynamicReturnQuery func()
			if operation.Options["dynamic"] == "true" {
				queryBindingCount, err := strconv.Atoi(operation.Options["queryBindingCount"])
				if err != nil {
					return nil, false, err
				}
				statement, bindings, cleanupDynamicReturnQuery, err = prepareDynamicStatement(ctx, iFunc, stack, statement, bindings, queryBindingCount)
				if err != nil {
					return nil, false, err
				}
			}
			schema, rows, err := iFunc.QueryMultiReturn(ctx, stack, statement, bindings)
			if cleanupDynamicReturnQuery != nil {
				cleanupDynamicReturnQuery()
			}
			if err != nil {
				state.lastExceptionContext = diagnosticPGExceptionContext(ctx, iFunc, operation)
				return nil, false, err
			}
			state.lastRowCount = rowCountFromResultRows(rows)
			records, err := returnQueryRecords(ctx, iFunc.GetReturn(), schema, rows)
			if err != nil {
				return nil, false, err
			}
			stack.BufferReturnQueryResults(records)

		case OpCode_ReturnNext:
			records, err := returnNextRecords(ctx, iFunc, stack, operation)
			if err != nil {
				state.lastExceptionContext = diagnosticPGExceptionContext(ctx, iFunc, operation)
				return nil, false, err
			}
			stack.BufferReturnQueryResults(records)
			state.lastRowCount = int64(len(records))
			if err = setFoundVariable(ctx, stack, len(records) > 0); err != nil {
				return nil, false, err
			}

		case OpCode_ScopeBegin:
			stack.PushScope()
		case OpCode_ScopeEnd:
			stack.PopScope()
		case OpCode_SelectInto:
			// TODO: implement
		case OpCode_UpdateInto:
			// TODO: implement
		default:
			panic("unimplemented opcode")
		}
	}
	return nil, false, nil
}

func checkInterpreterExecutionBudget(ctx *sql.Context, state *interpreterExecutionState) error {
	select {
	case <-ctx.Done():
		return pgerror.New(pgcode.QueryCanceled, "canceling statement due to user request")
	default:
	}
	state.operationCount++
	if state.operationCount > maxInterpretedFunctionOperations {
		return pgerror.New(pgcode.QueryCanceled, "canceling statement due to statement timeout")
	}
	return nil
}

func rowCountFromResultRows(rows []sql.Row) int64 {
	if len(rows) == 1 && gmstypes.IsOkResult(rows[0]) {
		return int64(gmstypes.GetOkResult(rows[0]).RowsAffected)
	}
	return int64(len(rows))
}

func foreachArrayIterationValues(value any, slice int32) ([]any, error) {
	if slice < 0 {
		return nil, errors.New("FOREACH SLICE must be non-negative")
	}
	arr, ok := pgtypes.ArrayElements(value)
	if !ok {
		return nil, errors.Errorf("FOREACH expression must yield an array, got %T", value)
	}
	if len(arr) == 0 {
		return nil, nil
	}
	if slice == 0 {
		return flattenForeachArrayValues(arr), nil
	}
	depth := foreachArrayDepth(arr)
	if int(slice) > depth {
		return nil, errors.Errorf("FOREACH SLICE %d is out of bounds for array with %d dimensions", slice, depth)
	}
	return collectForeachArraySlices(arr, depth, int(slice)), nil
}

func flattenForeachArrayValues(value any) []any {
	arr, ok := pgtypes.ArrayElements(value)
	if !ok {
		return []any{value}
	}
	values := make([]any, 0)
	for _, item := range arr {
		values = append(values, flattenForeachArrayValues(item)...)
	}
	return values
}

func foreachArrayDepth(arr []any) int {
	for _, item := range arr {
		nested, ok := pgtypes.ArrayElements(item)
		if ok {
			return 1 + foreachArrayDepth(nested)
		}
	}
	return 1
}

func collectForeachArraySlices(value any, depth int, slice int) []any {
	if depth <= slice {
		return []any{value}
	}
	arr, ok := pgtypes.ArrayElements(value)
	if !ok {
		return []any{value}
	}
	values := make([]any, 0)
	for _, item := range arr {
		values = append(values, collectForeachArraySlices(item, depth-1, slice)...)
	}
	return values
}

func returnRecordToRow(returnType *pgtypes.DoltgresType, record []pgtypes.RecordValue) sql.Row {
	if returnType.TypCategory == pgtypes.TypeCategory_CompositeTypes {
		return sql.Row{record}
	}
	if len(record) == 1 {
		return sql.Row{record[0].Value}
	}
	row := make(sql.Row, len(record))
	for i, field := range record {
		row[i] = field.Value
	}
	return row
}

func returnNextRecords(ctx *sql.Context, iFunc InterpretedFunction, stack InterpreterStack, operation InterpreterOperation) ([][]pgtypes.RecordValue, error) {
	if len(operation.PrimaryData) == 0 {
		return nil, errors.New("RETURN NEXT requires a value")
	}
	if len(operation.SecondaryData) == 1 {
		normalized := strings.ReplaceAll(strings.ToLower(operation.PrimaryData), " ", "")
		if normalized == "select$1;" {
			varName := operation.SecondaryData[0]
			if schema, row, ok := stack.GetRecord(varName); ok {
				return convertRowsToRecords(schema, []sql.Row{row})
			}
			retVariable := stack.GetVariable(varName)
			if retVariable.Type != nil {
				var retVal any
				if retVariable.Value != nil {
					retVal = *retVariable.Value
				}
				return [][]pgtypes.RecordValue{{
					{
						Value: retVal,
						Type:  retVariable.Type,
					},
				}}, nil
			}
		}
	}
	val, err := iFunc.QuerySingleReturn(ctx, stack, operation.PrimaryData, iFunc.GetReturn(), operation.SecondaryData)
	if err != nil {
		return nil, err
	}
	if record, ok := val.([]pgtypes.RecordValue); ok {
		return [][]pgtypes.RecordValue{record}, nil
	}
	return [][]pgtypes.RecordValue{{
		{
			Value: val,
			Type:  iFunc.GetReturn(),
		},
	}}, nil
}

func isBareRaiseOperation(operation InterpreterOperation) bool {
	if operation.PrimaryData != "EXCEPTION" || len(operation.SecondaryData) != 1 || operation.SecondaryData[0] != "" {
		return false
	}
	for key := range operation.Options {
		if _, err := strconv.Atoi(key); err == nil {
			return false
		}
	}
	return true
}

func functionExecutedNoReturnStatementError() error {
	return pgerror.New(pgcode.RoutineExceptionFunctionExecutedNoReturnStatement, "control reached end of function without RETURN")
}

func plpgsqlExceptionErrorFromDiagnostics(diagnostics plpgsqlExceptionDiagnostics) error {
	err := plpgsqlExceptionError{diagnostics: diagnostics}
	return pgerror.WithCandidateCode(err, pgcode.MakeCode(diagnostics.ReturnedSQLState))
}

func plpgsqlExceptionDiagnosticsFromRaise(ctx *sql.Context, iFunc InterpretedFunction, operation InterpreterOperation, message string) plpgsqlExceptionDiagnostics {
	diagnostics := plpgsqlExceptionDiagnostics{
		MessageText:      message,
		ReturnedSQLState: "P0001",
		Context:          diagnosticPGContext(ctx, iFunc, operation),
	}
	for key, value := range operation.Options {
		i, err := strconv.Atoi(key)
		if err != nil {
			continue
		}
		value = plpgsqlRaiseOptionText(value)
		switch NoticeOptionType(i) {
		case NoticeOptionTypeErrCode:
			diagnostics.ReturnedSQLState = plpgsqlNormalizeConditionSQLState(value)
		case NoticeOptionTypeMessage:
			diagnostics.MessageText = value
		case NoticeOptionTypeDetail:
			diagnostics.Detail = value
		case NoticeOptionTypeHint:
			diagnostics.Hint = value
		case NoticeOptionTypeColumn:
			diagnostics.ColumnName = value
		case NoticeOptionTypeConstraint:
			diagnostics.ConstraintName = value
		case NoticeOptionTypeDataType:
			diagnostics.DataTypeName = value
		case NoticeOptionTypeTable:
			diagnostics.TableName = value
		case NoticeOptionTypeSchema:
			diagnostics.SchemaName = value
		}
	}
	return diagnostics
}

func plpgsqlExceptionDiagnosticsFromError(err error) plpgsqlExceptionDiagnostics {
	var exceptionErr plpgsqlExceptionError
	if errors.As(err, &exceptionErr) {
		return exceptionErr.diagnostics
	}
	return plpgsqlExceptionDiagnostics{
		MessageText:      err.Error(),
		ReturnedSQLState: plpgsqlExceptionSQLStateFromError(err),
	}
}

func plpgsqlExceptionSQLStateFromError(err error) string {
	if code := pgerror.GetPGCode(err); code != pgcode.Uncategorized {
		return code.String()
	}
	switch {
	case sql.ErrPrimaryKeyViolation.Is(err),
		sql.ErrUniqueKeyViolation.Is(err):
		return pgcode.UniqueViolation.String()
	case sql.ErrForeignKeyChildViolation.Is(err),
		sql.ErrForeignKeyParentViolation.Is(err):
		return pgcode.ForeignKeyViolation.String()
	case sql.ErrInsertIntoNonNullableProvidedNull.Is(err),
		sql.ErrInsertIntoNonNullableDefaultNullColumn.Is(err):
		return pgcode.NotNullViolation.String()
	case sql.ErrCheckConstraintViolated.Is(err):
		return pgcode.CheckViolation.String()
	case sql.ErrTableNotFound.Is(err):
		return pgcode.UndefinedTable.String()
	case sql.ErrColumnNotFound.Is(err):
		return pgcode.UndefinedColumn.String()
	case sql.ErrInvalidValue.Is(err):
		return pgcode.InvalidTextRepresentation.String()
	case sql.ErrLockDeadlock.Is(err):
		return pgcode.SerializationFailure.String()
	}
	var mysqlErr *mysql.SQLError
	if errors.As(err, &mysqlErr) {
		if code, ok := plpgsqlMysqlErrnoSQLState(mysqlErr.Number()); ok {
			return code
		}
		if code, ok := plpgsqlErrorMessageSQLState(mysqlErr.Message); ok {
			return code
		}
	}
	if code, ok := plpgsqlErrorMessageSQLState(err.Error()); ok {
		return code
	}
	return "XX000"
}

func plpgsqlErrorMessageSQLState(msg string) (string, bool) {
	switch {
	case strings.HasPrefix(msg, "Check constraint "):
		return pgcode.CheckViolation.String(), true
	case strings.HasPrefix(msg, "column ") && strings.Contains(msg, "could not be found"):
		return pgcode.UndefinedColumn.String(), true
	case strings.HasPrefix(msg, "duplicate key value violates unique constraint"):
		return pgcode.UniqueViolation.String(), true
	case strings.Contains(msg, "Unique Key Constraint Violation"):
		return pgcode.UniqueViolation.String(), true
	case strings.HasPrefix(msg, "duplicate primary key given"),
		strings.HasPrefix(msg, "duplicate unique key given"):
		return pgcode.UniqueViolation.String(), true
	case strings.HasPrefix(msg, "date field value out of range"),
		strings.HasPrefix(msg, "time field value out of range"),
		strings.HasPrefix(msg, "date/time field value out of range"),
		strings.HasPrefix(msg, "timestamp out of range"):
		return pgcode.DatetimeFieldOverflow.String(), true
	case strings.HasPrefix(msg, "invalid input syntax for type "):
		return pgcode.InvalidTextRepresentation.String(), true
	}
	return "", false
}

func plpgsqlMysqlErrnoSQLState(errno int) (string, bool) {
	switch errno {
	case mysql.ERDupEntry:
		return pgcode.UniqueViolation.String(), true
	case mysql.ErNoReferencedRow2, mysql.ERNoReferencedRow:
		return pgcode.ForeignKeyViolation.String(), true
	case mysql.ERRowIsReferenced2, mysql.ERRowIsReferenced:
		return pgcode.ForeignKeyViolation.String(), true
	case mysql.ERBadNullError:
		return pgcode.NotNullViolation.String(), true
	case mysql.ERNoSuchTable:
		return pgcode.UndefinedTable.String(), true
	case mysql.ERBadFieldError:
		return pgcode.UndefinedColumn.String(), true
	case mysql.ERLockDeadlock:
		return pgcode.SerializationFailure.String(), true
	}
	return "", false
}

func plpgsqlRaiseOptionText(value string) string {
	value = strings.TrimSpace(value)
	if len(value) >= 2 && value[0] == '\'' && value[len(value)-1] == '\'' {
		value = value[1 : len(value)-1]
		value = strings.ReplaceAll(value, "''", "'")
	}
	return value
}

func stackedDiagnosticValue(diagnostics plpgsqlExceptionDiagnostics, kind string) (string, bool) {
	switch kind {
	case "MESSAGE_TEXT":
		return diagnostics.MessageText, true
	case "RETURNED_SQLSTATE":
		return diagnostics.ReturnedSQLState, true
	case "COLUMN_NAME":
		return diagnostics.ColumnName, true
	case "CONSTRAINT_NAME":
		return diagnostics.ConstraintName, true
	case "PG_DATATYPE_NAME":
		return diagnostics.DataTypeName, true
	case "TABLE_NAME":
		return diagnostics.TableName, true
	case "SCHEMA_NAME":
		return diagnostics.SchemaName, true
	case "PG_EXCEPTION_DETAIL":
		return diagnostics.Detail, true
	case "PG_EXCEPTION_HINT":
		return diagnostics.Hint, true
	case "PG_EXCEPTION_CONTEXT":
		return diagnostics.Context, true
	default:
		return "", false
	}
}

type exceptionHandlerOperation struct {
	conditions string
	start      int
	end        int
}

func exceptionHandlersFromOperation(operation InterpreterOperation) ([]exceptionHandlerOperation, int, error) {
	handlerCount := 1
	if countText := operation.Options["handlerCount"]; countText != "" {
		count, err := strconv.Atoi(countText)
		if err != nil {
			return nil, 0, err
		}
		handlerCount = count
	}
	handlers := make([]exceptionHandlerOperation, 0, handlerCount)
	handlerEnd := 0
	for i := 0; i < handlerCount; i++ {
		conditionsKey := "handlerConditions"
		startKey := "handlerStart"
		endKey := "handlerEnd"
		if operation.Options["handlerCount"] != "" {
			conditionsKey = fmt.Sprintf("handlerConditions.%d", i)
			startKey = fmt.Sprintf("handlerStart.%d", i)
			endKey = fmt.Sprintf("handlerEnd.%d", i)
		}
		handlerStart, err := strconv.Atoi(operation.Options[startKey])
		if err != nil {
			return nil, 0, err
		}
		currentHandlerEnd, err := strconv.Atoi(operation.Options[endKey])
		if err != nil {
			return nil, 0, err
		}
		handlers = append(handlers, exceptionHandlerOperation{
			conditions: operation.Options[conditionsKey],
			start:      handlerStart,
			end:        currentHandlerEnd,
		})
		handlerEnd = currentHandlerEnd
	}
	return handlers, handlerEnd, nil
}

func matchingExceptionHandler(handlers []exceptionHandlerOperation, diagnostics plpgsqlExceptionDiagnostics) (exceptionHandlerOperation, bool) {
	for _, handler := range handlers {
		if exceptionHandlerMatches(handler.conditions, diagnostics) {
			return handler, true
		}
	}
	return exceptionHandlerOperation{}, false
}

func exceptionHandlerMatches(conditions string, diagnostics plpgsqlExceptionDiagnostics) bool {
	for _, condition := range strings.Split(conditions, ",") {
		condition = strings.ToLower(strings.TrimSpace(condition))
		if condition == "" {
			continue
		}
		if condition == "others" {
			return true
		}
		if plpgsqlConditionMatchesSQLState(condition, diagnostics.ReturnedSQLState) {
			return true
		}
	}
	return false
}

func plpgsqlConditionMatchesSQLState(condition string, sqlState string) bool {
	if plpgsqlNormalizeConditionSQLState(condition) == sqlState {
		return true
	}
	if prefix, ok := plpgsqlConditionClassPrefixes[plpgsqlNormalizeConditionName(condition)]; ok {
		return strings.HasPrefix(sqlState, prefix)
	}
	return false
}

func plpgsqlNormalizeConditionName(condition string) string {
	condition = strings.TrimSpace(condition)
	if len(condition) >= len("sqlstate ") && strings.EqualFold(condition[:len("sqlstate ")], "sqlstate ") {
		condition = strings.TrimSpace(condition[len("sqlstate "):])
	}
	return strings.ToLower(plpgsqlRaiseOptionText(condition))
}

func plpgsqlNormalizeConditionSQLState(condition string) string {
	condition = strings.TrimSpace(condition)
	if len(condition) >= len("sqlstate ") && strings.EqualFold(condition[:len("sqlstate ")], "sqlstate ") {
		condition = strings.TrimSpace(condition[len("sqlstate "):])
	}
	condition = plpgsqlRaiseOptionText(condition)
	conditionName := strings.ToLower(condition)
	if sqlState, ok := plpgsqlConditionNameSQLStates[conditionName]; ok {
		return sqlState
	}
	if len(condition) == 5 {
		return strings.ToUpper(condition)
	}
	return condition
}

var plpgsqlConditionNameSQLStates = map[string]string{
	"case_not_found":              "20000",
	"check_violation":             "23514",
	"division_by_zero":            "22012",
	"exclusion_violation":         "23P01",
	"foreign_key_violation":       "23503",
	"invalid_text_representation": "22P02",
	"no_data_found":               "P0002",
	"not_null_violation":          "23502",
	"raise_exception":             "P0001",
	"too_many_rows":               "P0003",
	"unique_violation":            "23505",
}

var plpgsqlConditionClassPrefixes = map[string]string{
	"successful_completion":                       "00",
	"warning":                                     "01",
	"no_data":                                     "02",
	"sql_statement_not_yet_complete":              "03",
	"connection_exception":                        "08",
	"triggered_action_exception":                  "09",
	"feature_not_supported":                       "0A",
	"invalid_transaction_initiation":              "0B",
	"locator_exception":                           "0F",
	"invalid_grantor":                             "0L",
	"invalid_role_specification":                  "0P",
	"diagnostics_exception":                       "0Z",
	"case_not_found":                              "20",
	"cardinality_violation":                       "21",
	"data_exception":                              "22",
	"integrity_constraint_violation":              "23",
	"invalid_cursor_state":                        "24",
	"invalid_transaction_state":                   "25",
	"invalid_sql_statement_name":                  "26",
	"triggered_data_change_violation":             "27",
	"invalid_authorization_specification":         "28",
	"dependent_privilege_descriptors_still_exist": "2B",
	"invalid_transaction_termination":             "2D",
	"sql_routine_exception":                       "2F",
	"invalid_cursor_name":                         "34",
	"external_routine_exception":                  "38",
	"external_routine_invocation_exception":       "39",
	"savepoint_exception":                         "3B",
	"invalid_catalog_name":                        "3D",
	"invalid_schema_name":                         "3F",
	"transaction_rollback":                        "40",
	"syntax_error_or_access_rule_violation":       "42",
	"with_check_option_violation":                 "44",
	"insufficient_resources":                      "53",
	"program_limit_exceeded":                      "54",
	"object_not_in_prerequisite_state":            "55",
	"operator_intervention":                       "57",
	"system_error":                                "58",
	"config_file_error":                           "F0",
	"foreign_data_wrapper_error":                  "HV",
	"fdw_error":                                   "HV",
	"plpgsql_error":                               "P0",
	"internal_error":                              "XX",
}

// convertRowsToRecords iterates overs |rows| and converts each field in each row
// into a RecordValue. |schema| is specified for type information.
func convertRowsToRecords(schema sql.Schema, rows []sql.Row) ([][]pgtypes.RecordValue, error) {
	records := make([][]pgtypes.RecordValue, 0, len(rows))
	for _, row := range rows {
		record := make([]pgtypes.RecordValue, len(row))
		for i, field := range row {
			t := schema[i].Type
			doltgresType, ok := t.(*pgtypes.DoltgresType)
			if !ok {
				// non-Doltgres types are still used in analysis, but we only support disk serialization
				// for Doltgres types, so we must convert the GMS type to the nearest Doltgres type here.
				// TODO: this conversion isn't fully accurate. expression.GMSCast has additional logic in
				//       its Eval() method to handle types more exactly and also handles converting the
				//       value to ensure it is well formed for the returned DoltgresType. We can't
				//       currently use GMSCast directly here though, because of a dependency cycle, so
				//       that conversion logic needs to be extracted into a package both places can import.
				var err error
				doltgresType, err = pgtypes.FromGmsTypeToDoltgresType(t)
				if err != nil {
					return nil, err
				}
			}

			record[i] = pgtypes.RecordValue{
				Value: field,
				Type:  doltgresType,
			}
		}
		records = append(records, record)
	}

	return records, nil
}

func returnQueryRecords(ctx *sql.Context, returnType *pgtypes.DoltgresType, schema sql.Schema, rows []sql.Row) ([][]pgtypes.RecordValue, error) {
	if returnType == nil || !returnType.IsCompositeType() || returnType.IsRecordType() || len(returnType.CompositeAttrs) == 0 {
		return convertRowsToRecords(schema, rows)
	}
	if len(schema) != len(returnType.CompositeAttrs) {
		return nil, pgerror.New(pgcode.DatatypeMismatch, "structure of query does not match function result type")
	}
	typeCollection, err := GetTypesCollectionFromContext(ctx)
	if err != nil {
		return nil, err
	}
	targetTypes := make([]*pgtypes.DoltgresType, len(returnType.CompositeAttrs))
	for i, attr := range returnType.CompositeAttrs {
		targetTypes[i], err = resolveReturnQueryAttributeType(ctx, typeCollection, attr)
		if err != nil {
			return nil, err
		}
		if targetTypes[i] == nil {
			return nil, pgtypes.ErrTypeDoesNotExist.New(attr.TypeID.TypeName())
		}
		fromType, err := doltgresTypeFromSQLType(schema[i].Type)
		if err != nil {
			return nil, err
		}
		if fromType.ID != pgtypes.Unknown.ID && fromType.ID != targetTypes[i].ID {
			return nil, pgerror.New(pgcode.DatatypeMismatch, "structure of query does not match function result type")
		}
	}

	records := make([][]pgtypes.RecordValue, 0, len(rows))
	for _, row := range rows {
		if len(row) != len(targetTypes) {
			return nil, pgerror.New(pgcode.DatatypeMismatch, "structure of query does not match function result type")
		}
		record := make([]pgtypes.RecordValue, len(row))
		for i, field := range row {
			record[i] = pgtypes.RecordValue{
				Value: field,
				Type:  targetTypes[i],
			}
		}
		records = append(records, record)
	}
	return records, nil
}

func resolveReturnQueryAttributeType(ctx *sql.Context, typeCollection *typecollection.TypeCollection, attr pgtypes.CompositeAttribute) (*pgtypes.DoltgresType, error) {
	attrType, err := attr.ResolveType(ctx, typeCollection)
	if err != nil || attrType != nil {
		return attrType, err
	}
	if attr.TypeID.SchemaName() == "" {
		attrType, err = typeCollection.GetType(ctx, id.NewType("pg_catalog", attr.TypeID.TypeName()))
		if err != nil || attrType == nil {
			return attrType, err
		}
		return attr.ApplyTypMod(attrType), nil
	}
	return nil, nil
}

type diagnosticCallFrame struct {
	functionName string
	callSite     diagnosticCallSite
}

type diagnosticCallSite struct {
	lineNumber string
	action     string
	statement  string
}

type diagnosticCallStack struct {
	frames []diagnosticCallFrame
}

type diagnosticCallFrameKey struct{}

func pushDiagnosticCallFrame(ctx *sql.Context, iFunc InterpretedFunction) func() {
	if ctx == nil || ctx.Context == nil {
		return func() {}
	}
	if stack := diagnosticCallStackFromContext(ctx); stack != nil {
		previousLength := len(stack.frames)
		stack.frames = append(stack.frames, diagnosticCallFrame{functionName: diagnosticFunctionName(iFunc)})
		return func() {
			stack.frames = stack.frames[:previousLength]
		}
	}

	previousContext := ctx.Context
	ctx.Context = context.WithValue(ctx.Context, diagnosticCallFrameKey{}, &diagnosticCallStack{
		frames: []diagnosticCallFrame{{functionName: diagnosticFunctionName(iFunc)}},
	})
	return func() {
		ctx.Context = previousContext
	}
}

func pushDiagnosticCallSite(ctx *sql.Context, operation InterpreterOperation) func() {
	stack := diagnosticCallStackFromContext(ctx)
	if stack == nil || len(stack.frames) == 0 {
		return func() {}
	}
	idx := len(stack.frames) - 1
	previousCallSite := stack.frames[idx].callSite
	stack.frames[idx].callSite = diagnosticCallSite{
		lineNumber: diagnosticOperationLineNumber(operation),
		action:     diagnosticOperationAction(operation),
		statement:  diagnosticOperationStatement(operation),
	}
	return func() {
		stack.frames[idx].callSite = previousCallSite
	}
}

func diagnosticCallFrames(ctx *sql.Context) []diagnosticCallFrame {
	stack := diagnosticCallStackFromContext(ctx)
	if stack == nil {
		return nil
	}
	return stack.frames
}

func diagnosticCallStackFromContext(ctx *sql.Context) *diagnosticCallStack {
	if ctx == nil || ctx.Context == nil {
		return nil
	}
	stack, ok := ctx.Context.Value(diagnosticCallFrameKey{}).(*diagnosticCallStack)
	if !ok {
		return nil
	}
	return stack
}

func diagnosticPGContext(ctx *sql.Context, iFunc InterpretedFunction, operation InterpreterOperation) string {
	lineNumber := diagnosticOperationLineNumber(operation)
	frames := diagnosticCallFrames(ctx)
	if len(frames) == 0 {
		return fmt.Sprintf("PL/pgSQL function %s line %s at GET DIAGNOSTICS", diagnosticFunctionName(iFunc), lineNumber)
	}
	lines := make([]string, 0, len(frames))
	for i := len(frames) - 1; i >= 0; i-- {
		if i == len(frames)-1 {
			lines = append(lines, fmt.Sprintf("PL/pgSQL function %s line %s at GET DIAGNOSTICS", frames[i].functionName, lineNumber))
		} else {
			lines = append(lines, diagnosticCallerContextLines(frames[i])...)
		}
	}
	return strings.Join(lines, "\n")
}

func diagnosticPGExceptionContext(ctx *sql.Context, iFunc InterpretedFunction, operation InterpreterOperation) string {
	currentFrame := diagnosticCallFrame{
		functionName: diagnosticFunctionName(iFunc),
		callSite: diagnosticCallSite{
			lineNumber: diagnosticOperationLineNumber(operation),
			action:     diagnosticOperationAction(operation),
			statement:  diagnosticOperationStatement(operation),
		},
	}
	lines := diagnosticCallerContextLines(currentFrame)
	frames := diagnosticCallFrames(ctx)
	for i := len(frames) - 2; i >= 0; i-- {
		lines = append(lines, diagnosticCallerContextLines(frames[i])...)
	}
	return strings.Join(lines, "\n")
}

func diagnosticCallerContextLines(frame diagnosticCallFrame) []string {
	callSite := frame.callSite
	lineNumber := strings.TrimSpace(callSite.lineNumber)
	if lineNumber == "" {
		lineNumber = "0"
	}
	action := strings.TrimSpace(callSite.action)
	if action == "" {
		action = "SQL statement"
	}
	lines := make([]string, 0, 2)
	if statement := strings.TrimSpace(callSite.statement); statement != "" && action == "SQL statement" {
		lines = append(lines, fmt.Sprintf("SQL statement %q", statement))
	}
	lines = append(lines, fmt.Sprintf("PL/pgSQL function %s line %s at %s", frame.functionName, lineNumber, action))
	return lines
}

func diagnosticFunctionName(iFunc InterpretedFunction) string {
	functionName := iFunc.GetName()
	if functionName == "__doltgres_do_block" {
		functionName = "inline_code_block"
	} else if functionName == "" {
		functionName = "unknown"
	} else {
		functionName += "()"
	}
	return functionName
}

func diagnosticOperationLineNumber(operation InterpreterOperation) string {
	lineNumber := strings.TrimSpace(operation.Options[diagnosticOptionLineNumber])
	if lineNumber == "" {
		lineNumber = "0"
	}
	return lineNumber
}

func diagnosticOperationAction(operation InterpreterOperation) string {
	action := strings.TrimSpace(operation.Options[diagnosticOptionAction])
	if action == "" {
		action = "SQL statement"
	}
	return action
}

func diagnosticOperationStatement(operation InterpreterOperation) string {
	return strings.TrimSpace(operation.Options[diagnosticOptionStatement])
}

func diagnosticPGRoutineOID(iFunc InterpretedFunction) id.Id {
	if iFunc.GetName() == "__doltgres_do_block" {
		return id.NewOID(0).AsId()
	}
	return iFunc.InternalID()
}

func prepareDynamicStatement(
	ctx *sql.Context,
	iFunc InterpretedFunction,
	stack InterpreterStack,
	statement string,
	bindings []string,
	queryBindingCount int,
) (string, []string, func(), error) {
	if queryBindingCount > len(bindings) {
		return "", nil, nil, errors.New("dynamic execute query binding count exceeds available bindings")
	}
	queryBindings := bindings[:queryBindingCount]
	bindings = bindings[queryBindingCount:]
	queryVal, err := iFunc.QuerySingleReturn(ctx, stack, "SELECT "+statement, pgtypes.Text, queryBindings)
	if err != nil {
		return "", nil, nil, err
	}
	if queryVal == nil {
		return "", nil, nil, errors.New("query string argument of EXECUTE is null")
	}
	statement = queryVal.(string)
	if len(bindings) == 0 {
		return statement, bindings, nil, nil
	}
	stack.PushScope()
	cleanup := func() {
		stack.PopScope()
	}
	bindings, err = evaluateDynamicExecuteUsingParams(ctx, iFunc, stack, bindings)
	if err != nil {
		cleanup()
		return "", nil, nil, err
	}
	return statement, bindings, cleanup, nil
}

func evaluateDynamicExecuteUsingParams(ctx *sql.Context, iFunc InterpretedFunction, stack InterpreterStack, params []string) ([]string, error) {
	bindings := make([]string, len(params))
	for i, param := range params {
		expression, referencedVariables, err := substituteVariableReferences(param, &stack)
		if err != nil {
			return nil, err
		}
		schema, rows, err := iFunc.QueryMultiReturn(ctx, stack, "SELECT "+expression, referencedVariables)
		if err != nil {
			return nil, err
		}
		if len(schema) != 1 {
			return nil, errors.New("USING expression does not result in a single value")
		}
		if len(rows) != 1 {
			return nil, errors.New("USING expression returned multiple result sets")
		}
		if len(rows[0]) != 1 {
			return nil, errors.New("USING expression returned multiple results")
		}
		expressionType, err := doltgresTypeFromSQLType(schema[0].Type)
		if err != nil {
			return nil, err
		}
		bindingName := fmt.Sprintf("\tdynamic_execute_param_%d", i+1)
		stack.NewVariableWithValue(bindingName, expressionType, rows[0][0])
		bindings[i] = bindingName
	}
	return bindings, nil
}

// applyNoticeOptions adds the specified |options| to the |noticeResponse|.
func applyNoticeOptions(ctx *sql.Context, noticeResponse *pgproto3.NoticeResponse, options map[string]string) error {
	for key, value := range options {
		i, err := strconv.Atoi(key)
		if err != nil {
			continue
		}

		switch NoticeOptionType(i) {
		case NoticeOptionTypeErrCode:
			noticeResponse.Code = value
		case NoticeOptionTypeMessage:
			noticeResponse.Message = value
		case NoticeOptionTypeDetail:
			noticeResponse.Detail = value
		case NoticeOptionTypeHint:
			noticeResponse.Hint = value
		case NoticeOptionTypeColumn:
			noticeResponse.ColumnName = value
		case NoticeOptionTypeConstraint:
			noticeResponse.ConstraintName = value
		case NoticeOptionTypeDataType:
			noticeResponse.DataTypeName = value
		case NoticeOptionTypeTable:
			noticeResponse.TableName = value
		case NoticeOptionTypeSchema:
			noticeResponse.SchemaName = value
		default:
			ctx.GetLogger().Warnf("unhandled notice option type: %s", key)
		}
	}
	return nil
}

// evaluteNoticeMessage evaluates the message for a RAISE NOTICE statement, including
// evaluating any specified parameters and plugging them into the message in place of
// the % placeholders.
func evaluteNoticeMessage(ctx *sql.Context, iFunc InterpretedFunction,
	operation InterpreterOperation, stack InterpreterStack) (string, error) {
	message := operation.SecondaryData[0]
	if len(operation.SecondaryData) > 1 {
		params := operation.SecondaryData[1:]
		currentParamIdx := 0

		parts := strings.Split(message, "%%")
		for i, part := range parts {
			for strings.Contains(part, "%") {
				if currentParamIdx >= len(params) {
					return "", errors.New("too few parameters specified for RAISE")
				}
				currentParam := params[currentParamIdx]
				currentParamIdx += 1
				formattedVar, varFound, err := iFunc.ApplyBindings(ctx, stack, "$1", []string{currentParam}, false)
				if varFound {
					if err != nil {
						return "", err
					}
					part = strings.Replace(part, "%", formattedVar, 1)
				} else {
					retVal, err := iFunc.QuerySingleReturn(ctx, stack, fmt.Sprintf("SELECT (%s)::text", currentParam), nil, nil)
					if err != nil {
						return "", err
					}
					stringVal := fmt.Sprintf("%v", retVal) // We should always return a string, but this is just a safety net
					part = strings.Replace(part, "%", stringVal, 1)
				}
			}
			parts[i] = part
		}
		if currentParamIdx < len(params) {
			return "", errors.New("too many parameters specified for RAISE")
		}
		message = strings.Join(parts, "%")
	}
	return message, nil
}

func initFoundVariable(stack InterpreterStack) {
	stack.NewVariableWithValue("FOUND", pgtypes.Bool, false)
	stack.NewVariableAlias("found", "FOUND")
}

func statementsUseFoundVariable(statements []InterpreterOperation) bool {
	for _, operation := range statements {
		if strings.EqualFold(operation.Target, "FOUND") {
			return true
		}
		for _, referencedVariable := range operation.SecondaryData {
			if strings.EqualFold(referencedVariable, "FOUND") {
				return true
			}
		}
	}
	return false
}

func setFoundVariable(ctx *sql.Context, stack InterpreterStack, found bool) error {
	if stack.GetVariable("FOUND").Type == nil {
		return nil
	}
	return stack.SetVariable(ctx, "FOUND", found)
}

func assignSQLRowValue(ctx *sql.Context, stack InterpreterStack, variableName string, fromSqlType sql.Type, value any) error {
	target := stack.GetVariable(variableName)
	if target.Type == nil {
		return fmt.Errorf("variable `%s` could not be found", variableName)
	}
	if value == nil {
		return stack.SetVariable(ctx, variableName, nil)
	}

	fromDoltgresType, err := doltgresTypeFromSQLType(fromSqlType)
	if err != nil {
		return err
	}
	if fromDoltgresType.ID == target.Type.ID {
		return stack.SetVariable(ctx, variableName, value)
	}
	str, err := fromDoltgresType.IoOutput(ctx, value)
	if err != nil {
		return err
	}
	castValue, err := target.Type.IoInput(ctx, str)
	if err != nil {
		return err
	}
	return stack.SetVariable(ctx, variableName, castValue)
}

func assignForQueryRow(ctx *sql.Context, stack InterpreterStack, variableName string, sch sql.Schema, row sql.Row) error {
	if stack.IsRecordVariable(variableName) {
		return stack.UpdateRecord(variableName, sch, row)
	}
	target := stack.GetVariable(variableName)
	if target.Type == nil {
		return fmt.Errorf("variable `%s` could not be found", variableName)
	}
	if target.Type.IsCompositeType() && !target.Type.IsRecordType() {
		return assignSQLCompositeRowValue(ctx, stack, variableName, sch, row)
	}
	if len(row) != 1 || len(sch) != 1 {
		return errors.New("loop variable of loop over rows must be a record variable or list of scalar variables")
	}
	return assignSQLRowValue(ctx, stack, variableName, sch[0].Type, row[0])
}

func assignSQLCompositeRowValue(ctx *sql.Context, stack InterpreterStack, variableName string, sch sql.Schema, row sql.Row) error {
	target := stack.GetVariable(variableName)
	if target.Type == nil {
		return fmt.Errorf("variable `%s` could not be found", variableName)
	}
	if !target.Type.IsCompositeType() || target.Type.IsRecordType() {
		return errors.New("target is not a named composite type")
	}
	if len(row) != len(target.Type.CompositeAttrs) || len(sch) != len(target.Type.CompositeAttrs) {
		return errors.New("number of row values does not match number of composite attributes")
	}
	typeCollection, err := GetTypesCollectionFromContext(ctx)
	if err != nil {
		return err
	}
	values := make([]pgtypes.RecordValue, len(target.Type.CompositeAttrs))
	for i, attr := range target.Type.CompositeAttrs {
		targetType, err := attr.ResolveType(ctx, typeCollection)
		if err != nil {
			return err
		}
		if targetType == nil {
			return pgtypes.ErrTypeDoesNotExist.New(attr.TypeID.TypeName())
		}
		value := row[i]
		if value != nil {
			fromType, err := doltgresTypeFromSQLType(sch[i].Type)
			if err != nil {
				return err
			}
			if fromType.ID != targetType.ID {
				str, err := fromType.IoOutput(ctx, value)
				if err != nil {
					return err
				}
				value, err = targetType.IoInput(ctx, str)
				if err != nil {
					return err
				}
			}
		}
		values[i] = pgtypes.RecordValue{
			Value: value,
			Type:  targetType,
		}
	}
	return stack.SetVariable(ctx, variableName, values)
}

func doltgresTypeFromSQLType(sqlType sql.Type) (*pgtypes.DoltgresType, error) {
	if doltgresType, ok := sqlType.(*pgtypes.DoltgresType); ok {
		return doltgresType, nil
	}
	return pgtypes.FromGmsTypeToDoltgresType(sqlType)
}

func resolveColumnPercentType(ctx *sql.Context, iFunc InterpretedFunction, stack InterpreterStack, rawTypeName string) (*pgtypes.DoltgresType, bool, error) {
	tableParts, columnName, ok := parseColumnPercentType(rawTypeName)
	if !ok {
		return nil, false, nil
	}
	query := fmt.Sprintf("SELECT %s FROM %s LIMIT 0", quoteSQLIdentifier(columnName), formatSQLQualifiedIdentifier(tableParts))
	schema, _, err := iFunc.QueryMultiReturn(ctx, stack, query, nil)
	if err != nil {
		return nil, true, err
	}
	if len(schema) != 1 {
		return nil, true, errors.Errorf("%%TYPE reference %s must resolve to exactly one column", rawTypeName)
	}
	resolvedType, err := doltgresTypeFromSQLType(schema[0].Type)
	return resolvedType, true, err
}

func parseColumnPercentType(rawTypeName string) (tableParts []string, columnName string, ok bool) {
	typeName := strings.TrimSpace(strings.ReplaceAll(rawTypeName, `"`, ""))
	if !strings.HasSuffix(strings.ToLower(typeName), "%type") {
		return nil, "", false
	}
	reference := strings.TrimSpace(typeName[:len(typeName)-len("%type")])
	parts := strings.Split(reference, ".")
	if len(parts) < 2 {
		return nil, "", false
	}
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
		if parts[i] == "" {
			return nil, "", false
		}
	}
	return parts[:len(parts)-1], parts[len(parts)-1], true
}

func resolveTablePercentRowType(ctx *sql.Context, iFunc InterpretedFunction, stack InterpreterStack, rawTypeName string) (sql.Schema, bool, error) {
	tableParts, ok := parseTablePercentRowType(rawTypeName)
	if !ok {
		return nil, false, nil
	}
	query := fmt.Sprintf("SELECT * FROM %s LIMIT 0", formatSQLQualifiedIdentifier(tableParts))
	schema, _, err := iFunc.QueryMultiReturn(ctx, stack, query, nil)
	if err != nil {
		return nil, true, err
	}
	return schema, true, nil
}

func parseTablePercentRowType(rawTypeName string) (tableParts []string, ok bool) {
	typeName := strings.TrimSpace(strings.ReplaceAll(rawTypeName, `"`, ""))
	if !strings.HasSuffix(strings.ToLower(typeName), "%rowtype") {
		return nil, false
	}
	reference := strings.TrimSpace(typeName[:len(typeName)-len("%rowtype")])
	parts := strings.Split(reference, ".")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
		if parts[i] == "" {
			return nil, false
		}
	}
	return parts, len(parts) > 0
}

func formatSQLQualifiedIdentifier(parts []string) string {
	quoted := make([]string, len(parts))
	for i, part := range parts {
		quoted[i] = quoteSQLIdentifier(part)
	}
	return strings.Join(quoted, ".")
}

func quoteSQLIdentifier(ident string) string {
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
}

func resolveDeclareType(ctx *sql.Context, typeCollection *typecollection.TypeCollection, rawTypeName string) (*pgtypes.DoltgresType, error) {
	schemaName, typeName := normalizeDeclareTypeName(rawTypeName)
	resolvedType, err := typeCollection.GetType(ctx, id.NewType(schemaName, typeName))
	if err != nil || resolvedType != nil {
		return resolvedType, err
	}
	if schemaName != "pg_catalog" || strings.Contains(strings.TrimSpace(rawTypeName), ".") {
		return nil, nil
	}
	currentSchema, err := typecollection.GetSchemaName(ctx, nil, "")
	if err != nil {
		return nil, err
	}
	if currentSchema != "" && currentSchema != schemaName {
		resolvedType, err = typeCollection.GetType(ctx, id.NewType(currentSchema, typeName))
		if err != nil || resolvedType != nil {
			return resolvedType, err
		}
	}
	return typeCollection.GetType(ctx, id.NewType("", typeName))
}

// normalizeDeclareTypeName maps pg_query_go's PL/pgSQL declaration type names
// to the Doltgres internal pg_catalog lookup keys.
func normalizeDeclareTypeName(rawTypeName string) (schemaName string, typeName string) {
	typeName = strings.TrimSpace(strings.ReplaceAll(rawTypeName, `"`, ""))
	schemaName = "pg_catalog"
	if strings.Contains(typeName, ".") {
		parts := strings.SplitN(typeName, ".", 2)
		schemaName = strings.TrimSpace(parts[0])
		typeName = strings.TrimSpace(parts[1])
	}
	if schemaName == "" {
		schemaName = "pg_catalog"
	}

	isArray := strings.HasSuffix(typeName, "[]")
	if isArray {
		typeName = strings.TrimSpace(strings.TrimSuffix(typeName, "[]"))
	}

	if schemaName == "pg_catalog" && !strings.HasPrefix(typeName, "_") {
		if alias, ok := plpgsqlDeclareTypeAliases[strings.ToLower(typeName)]; ok {
			typeName = alias
		} else if typ, ok, _ := types.TypeForNonKeywordTypeName(typeName); ok && typ != nil {
			typeName = typ.Name()
		}
	}

	if isArray && !strings.HasPrefix(typeName, "_") {
		typeName = "_" + typeName
	}
	return schemaName, typeName
}

var plpgsqlDeclareTypeAliases = map[string]string{
	"bigint":            "int8",
	"boolean":           "bool",
	"character":         "char",
	"character varying": "varchar",
	"double precision":  "float8",
	"int":               "int4",
	"record":            "record",
	"smallint":          "int2",
}

// triggerSpecialVariables are the list of special variables for triggers.
// https://www.postgresql.org/docs/15/plpgsql-trigger.html
// TODO: NEW and OLD variables are handled separately using `InterpreterStack.NewRecord` function.
var triggerSpecialVariables = map[string]*pgtypes.DoltgresType{
	//"NEW":
	//"OLD":
	"TG_NAME":         pgtypes.Name,
	"TG_WHEN":         pgtypes.Text,
	"TG_LEVEL":        pgtypes.Text,
	"TG_OP":           pgtypes.Text,
	"TG_EVENT":        pgtypes.Text,
	"TG_TAG":          pgtypes.Text,
	"TG_RELID":        pgtypes.Oid,
	"TG_RELNAME":      pgtypes.Name,
	"TG_TABLE_NAME":   pgtypes.Name,
	"TG_TABLE_SCHEMA": pgtypes.Name,
	"TG_NARGS":        pgtypes.Int32,
	"TG_ARGV[]":       pgtypes.TextArray,
}
