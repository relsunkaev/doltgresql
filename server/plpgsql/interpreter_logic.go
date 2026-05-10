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
	GetReturn() *pgtypes.DoltgresType
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
	diagnosticOptionAction     = "pgContextAction"
	diagnosticOptionLineNumber = "lineNumber"
	diagnosticOptionStatement  = "pgContextStatement"
)

type interpreterExecutionState struct {
	statements           []InterpreterOperation
	lastRowCount         int64
	lastExceptionContext string
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
	restoreDiagnosticContext := pushDiagnosticCallFrame(ctx, iFunc)
	defer restoreDiagnosticContext()
	return call(ctx, iFunc, stack)
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
	return call(ctx, iFunc, stack)
}

// call runs the contained operations on the given runner.
func call(ctx *sql.Context, iFunc InterpretedFunction, stack InterpreterStack) (any, error) {
	state := &interpreterExecutionState{
		statements: iFunc.GetStatements(),
	}
	ret, returned, err := runOperations(ctx, iFunc, stack, state, 0, len(state.statements))
	if err != nil {
		return nil, err
	}
	if returned {
		return ret, nil
	}
	return nil, nil
}

func runOperations(ctx *sql.Context, iFunc InterpretedFunction, stack InterpreterStack, state *interpreterExecutionState, start, end int) (any, bool, error) {
	// We increment before accessing, so start at -1
	counter := start - 1
	// Run the statements
	statements := state.statements
	for {
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
		case OpCode_Declare:
			typeCollection, err := GetTypesCollectionFromContext(ctx)
			if err != nil {
				return nil, false, err
			}

			schemaName, typeName := normalizeDeclareTypeName(operation.PrimaryData)
			if (schemaName == "" || schemaName == "pg_catalog") && strings.EqualFold(typeName, "record") {
				stack.NewRecord(operation.Target, nil, nil)
				continue
			}
			resolvedType, err := typeCollection.GetType(ctx, id.NewType(schemaName, typeName))
			if err != nil {
				return nil, false, err
			}
			if resolvedType == nil {
				return nil, false, pgtypes.ErrTypeDoesNotExist.New(operation.PrimaryData)
			}
			if len(operation.SecondaryData) != 0 {
				defVal := operation.SecondaryData[0]
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
						stack.NewVariableWithValue(operation.Target, resolvedType, *ivr.Value)
					} else {
						stack.NewVariable(operation.Target, resolvedType)
					}
				} else {
					val, err := resolvedType.IoInput(ctx, strings.Trim(operation.SecondaryData[0], "'"))
					if err != nil {
						return nil, false, err
					}
					stack.NewVariableWithValue(operation.Target, resolvedType, val)
				}
			} else {
				stack.NewVariable(operation.Target, resolvedType)
			}
		case OpCode_DeleteInto:
			// TODO: implement
		case OpCode_Exception:
			handlers, handlerEnd, err := exceptionHandlersFromOperation(operation)
			if err != nil {
				return nil, false, err
			}
			ret, returned, err := runOperations(ctx, iFunc, stack, state, counter+1, operation.Index)
			if err != nil {
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
				ret, returned, err = runOperations(ctx, iFunc, stack, state, handler.start, handler.end)
				state.stackedDiagnostics = priorDiagnostics
				if err != nil {
					return nil, false, err
				}
				if returned {
					return ret, true, nil
				}
			} else if returned {
				return ret, true, nil
			}
			counter = handlerEnd - 1
		case OpCode_Execute:
			statement := operation.PrimaryData
			bindings := operation.SecondaryData
			dynamicUsingScope := false
			if operation.Options["dynamic"] == "true" {
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
				if err = setFoundVariable(ctx, stack, found); err != nil {
					return nil, false, err
				}
				strict := operation.Options["strict"] == "true"
				if strict && !found {
					return nil, false, errors.New("query returned no rows")
				}
				if strict && len(rows) > 1 {
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
						if err = stack.SetVariable(ctx, operation.Target, nil); err != nil {
							return nil, false, err
						}
					} else {
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
				if err = setFoundVariable(ctx, stack, state.lastRowCount > 0); err != nil {
					return nil, false, err
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
			if retVal.(bool) {
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

			message, err := evaluteNoticeMessage(ctx, iFunc, operation, stack)
			if err != nil {
				return nil, false, err
			}

			if operation.PrimaryData == "EXCEPTION" {
				// TODO: Notices at the EXCEPTION level should also abort the current tx.
				return nil, false, plpgsqlExceptionError{
					diagnostics: plpgsqlExceptionDiagnosticsFromRaise(ctx, iFunc, operation, message),
				}
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
					rows[i] = sql.Row{record}
				}

				return sql.RowsToRowIter(rows...), true, nil
			}

			if len(operation.PrimaryData) == 0 {
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
			schema, rows, err := iFunc.QueryMultiReturn(ctx, stack, operation.PrimaryData, operation.SecondaryData)
			if err != nil {
				state.lastExceptionContext = diagnosticPGExceptionContext(ctx, iFunc, operation)
				return nil, false, err
			}
			state.lastRowCount = rowCountFromResultRows(rows)
			stack.InitCursor(operation.Target, schema, rows)
		case OpCode_ForQueryNext:
			schema, row, ok := stack.AdvanceCursor(operation.PrimaryData)
			if !ok {
				stack.CloseCursor(operation.PrimaryData)
				// Jump forward past the loop body and back-goto, same mechanism as OpCode_If.
				counter = operation.Index - 1
			} else {
				if err := stack.UpdateRecord(operation.Target, schema, row); err != nil {
					return nil, false, err
				}
			}
		case OpCode_ReturnQuery:
			schema, rows, err := iFunc.QueryMultiReturn(ctx, stack, operation.PrimaryData, operation.SecondaryData)
			if err != nil {
				state.lastExceptionContext = diagnosticPGExceptionContext(ctx, iFunc, operation)
				return nil, false, err
			}
			state.lastRowCount = rowCountFromResultRows(rows)
			records, err := convertRowsToRecords(schema, rows)
			if err != nil {
				return nil, false, err
			}
			stack.BufferReturnQueryResults(records)

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

func rowCountFromResultRows(rows []sql.Row) int64 {
	if len(rows) == 1 && gmstypes.IsOkResult(rows[0]) {
		return int64(gmstypes.GetOkResult(rows[0]).RowsAffected)
	}
	return int64(len(rows))
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
		if plpgsqlNormalizeConditionSQLState(condition) == diagnostics.ReturnedSQLState {
			return true
		}
	}
	return false
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

func doltgresTypeFromSQLType(sqlType sql.Type) (*pgtypes.DoltgresType, error) {
	if doltgresType, ok := sqlType.(*pgtypes.DoltgresType); ok {
		return doltgresType, nil
	}
	return pgtypes.FromGmsTypeToDoltgresType(sqlType)
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
	"TG_RELID":        pgtypes.Oid,
	"TG_RELNAME":      pgtypes.Name,
	"TG_TABLE_NAME":   pgtypes.Name,
	"TG_TABLE_SCHEMA": pgtypes.Name,
	"TG_NARGS":        pgtypes.Int32,
	"TG_ARGV[]":       pgtypes.TextArray,
}
