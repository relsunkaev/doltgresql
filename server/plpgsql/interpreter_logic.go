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
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/core/typecollection"
	"github.com/dolthub/doltgresql/postgres/parser/types"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// InterpretedFunction is an interface that essentially mirrors the implementation of InterpretedFunction in the
// framework package.
type InterpretedFunction interface {
	ApplyBindings(ctx *sql.Context, stack InterpreterStack, stmt string, bindings []string, enforceType bool) (newStmt string, varFound bool, err error)
	GetParameters() []*pgtypes.DoltgresType
	GetParameterNames() []string
	GetReturn() *pgtypes.DoltgresType
	GetStatements() []InterpreterOperation
	QueryMultiReturn(ctx *sql.Context, stack InterpreterStack, stmt string, bindings []string) (schema sql.Schema, rows []sql.Row, err error)
	QuerySingleReturn(ctx *sql.Context, stack InterpreterStack, stmt string, targetType *pgtypes.DoltgresType, bindings []string) (val any, err error)
	// IsSRF returns whether the function is a set returning function, meaning whether the
	// function returns one or more rows as a result.
	IsSRF() bool
}

// GetTypesCollectionFromContext is declared within the core package, but is assigned to this variable to work around
// import cycles.
var GetTypesCollectionFromContext func(ctx *sql.Context) (*typecollection.TypeCollection, error)

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
	return call(ctx, iFunc, stack)
}

// call runs the contained operations on the given runner.
func call(ctx *sql.Context, iFunc InterpretedFunction, stack InterpreterStack) (any, error) {
	// We increment before accessing, so start at -1
	counter := -1
	// Run the statements
	statements := iFunc.GetStatements()
	for {
		counter++
		if counter >= len(statements) {
			break
		} else if counter < 0 {
			panic("negative function counter")
		}

		operation := statements[counter]
		switch operation.OpCode {
		case OpCode_Alias:
			iv := stack.GetVariable(operation.PrimaryData)
			if iv.Type == nil {
				return nil, fmt.Errorf("variable `%s` could not be found", operation.PrimaryData)
			}
			stack.NewVariableAlias(operation.Target, operation.PrimaryData)
		case OpCode_Assign:
			iv := stack.GetVariable(operation.Target)
			if iv.Type == nil {
				return nil, fmt.Errorf("variable `%s` could not be found", operation.Target)
			}
			retVal, err := iFunc.QuerySingleReturn(ctx, stack, operation.PrimaryData, iv.Type, operation.SecondaryData)
			if err != nil {
				return nil, err
			}
			err = stack.SetVariable(ctx, operation.Target, retVal)
			if err != nil {
				return nil, err
			}
		case OpCode_Declare:
			typeCollection, err := GetTypesCollectionFromContext(ctx)
			if err != nil {
				return nil, err
			}

			schemaName, typeName := normalizeDeclareTypeName(operation.PrimaryData)
			if (schemaName == "" || schemaName == "pg_catalog") && strings.EqualFold(typeName, "record") {
				stack.NewRecord(operation.Target, nil, nil)
				continue
			}
			resolvedType, err := typeCollection.GetType(ctx, id.NewType(schemaName, typeName))
			if err != nil {
				return nil, err
			}
			if resolvedType == nil {
				return nil, pgtypes.ErrTypeDoesNotExist.New(operation.PrimaryData)
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
						return nil, err
					}
					stack.NewVariableWithValue(operation.Target, resolvedType, val)
				}
			} else {
				stack.NewVariable(operation.Target, resolvedType)
			}
		case OpCode_DeleteInto:
			// TODO: implement
		case OpCode_Exception:
			// TODO: implement
		case OpCode_Execute:
			statement := operation.PrimaryData
			bindings := operation.SecondaryData
			dynamicUsingScope := false
			if operation.Options["dynamic"] == "true" {
				queryBindingCount, err := strconv.Atoi(operation.Options["queryBindingCount"])
				if err != nil {
					return nil, err
				}
				if queryBindingCount > len(bindings) {
					return nil, errors.New("dynamic execute query binding count exceeds available bindings")
				}
				queryBindings := bindings[:queryBindingCount]
				bindings = bindings[queryBindingCount:]
				queryVal, err := iFunc.QuerySingleReturn(ctx, stack, "SELECT "+statement, pgtypes.Text, queryBindings)
				if err != nil {
					return nil, err
				}
				if queryVal == nil {
					return nil, errors.New("query string argument of EXECUTE is null")
				}
				statement = queryVal.(string)
				if len(bindings) > 0 {
					stack.PushScope()
					dynamicUsingScope = true
					bindings, err = evaluateDynamicExecuteUsingParams(ctx, iFunc, stack, bindings)
					if err != nil {
						stack.PopScope()
						return nil, err
					}
				}
			}
			queryMultiReturn := func() (sql.Schema, []sql.Row, error) {
				if dynamicUsingScope {
					defer stack.PopScope()
				}
				return iFunc.QueryMultiReturn(ctx, stack, statement, bindings)
			}
			if len(operation.Target) > 0 {
				sch, rows, err := queryMultiReturn()
				if err != nil {
					return nil, err
				}
				found := len(rows) > 0
				if err = setFoundVariable(ctx, stack, found); err != nil {
					return nil, err
				}
				strict := operation.Options["strict"] == "true"
				if strict && !found {
					return nil, errors.New("query returned no rows")
				}
				if strict && len(rows) > 1 {
					return nil, errors.New("query returned more than one row")
				}
				if vars := strings.Split(operation.Target, ","); len(vars) > 1 {
					// multiple column row result
					if !found {
						for _, variableName := range vars {
							if err = stack.SetVariable(ctx, variableName, nil); err != nil {
								return nil, err
							}
						}
					} else {
						row := rows[0]
						if len(row) != len(vars) || len(sch) != len(vars) {
							return nil, errors.New("number of row values does not match number of schema columns")
						}
						for i, variableName := range vars {
							if err = assignSQLRowValue(ctx, stack, variableName, sch[i].Type, row[i]); err != nil {
								return nil, err
							}
						}
					}
				} else {
					// single column
					if !found {
						if err = stack.SetVariable(ctx, operation.Target, nil); err != nil {
							return nil, err
						}
					} else {
						if len(rows[0]) != 1 || len(sch) != 1 {
							return nil, errors.New("expression returned multiple results")
						}
						if err = assignSQLRowValue(ctx, stack, operation.Target, sch[0].Type, rows[0][0]); err != nil {
							return nil, err
						}
					}
				}
			} else {
				_, rows, err := queryMultiReturn()
				if err != nil {
					return nil, err
				}
				if err = setFoundVariable(ctx, stack, len(rows) > 0); err != nil {
					return nil, err
				}
			}
		case OpCode_Get:
			// TODO: implement
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
				return nil, err
			}
			if retVal.(bool) {
				// We're never changing the scope, so we can just assign it directly.
				// Also, we must assign to index-1, so that the increment hits our target.
				counter = operation.Index - 1
			}
		case OpCode_InsertInto:
			// TODO: implement
		case OpCode_Perform:
			_, rows, err := iFunc.QueryMultiReturn(ctx, stack, operation.PrimaryData, operation.SecondaryData)
			if err != nil {
				return nil, err
			}
			if err = setFoundVariable(ctx, stack, len(rows) > 0); err != nil {
				return nil, err
			}
		case OpCode_Raise:
			// TODO: Use the client_min_messages config param to determine which
			//       notice levels to send to the client.
			// https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-CLIENT-MIN-MESSAGES

			message, err := evaluteNoticeMessage(ctx, iFunc, operation, stack)
			if err != nil {
				return nil, err
			}

			if operation.PrimaryData == "EXCEPTION" {
				// TODO: Notices at the EXCEPTION level should also abort the current tx.
				return nil, errors.New(message)
			} else {
				noticeResponse := &pgproto3.NoticeResponse{
					Severity: operation.PrimaryData,
					Message:  message,
				}
				if err = applyNoticeOptions(ctx, noticeResponse, operation.Options); err != nil {
					return nil, err
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

				return sql.RowsToRowIter(rows...), nil
			}

			if len(operation.PrimaryData) == 0 {
				return nil, nil
			}

			// TODO: handle record types properly, we'll special case triggers for now
			if iFunc.GetReturn().ID == pgtypes.Trigger.ID && len(operation.SecondaryData) == 1 {
				normalized := strings.ReplaceAll(strings.ToLower(operation.PrimaryData), " ", "")
				if normalized == "select$1;" {
					if strings.EqualFold(operation.SecondaryData[0], "new") {
						return *stack.GetVariable("NEW").Value, nil
					} else if strings.EqualFold(operation.SecondaryData[0], "old") {
						return *stack.GetVariable("OLD").Value, nil
					}
				}
			}
			val, err := iFunc.QuerySingleReturn(ctx, stack, operation.PrimaryData, iFunc.GetReturn(), operation.SecondaryData)

			// If this is a set returning function, then we need to return a RowIter and wrap
			// the composite value in a sql.Row.
			if iFunc.IsSRF() {
				return sql.RowsToRowIter(sql.Row{val}), nil
			}
			return val, err

		case OpCode_ForQueryInit:
			schema, rows, err := iFunc.QueryMultiReturn(ctx, stack, operation.PrimaryData, operation.SecondaryData)
			if err != nil {
				return nil, err
			}
			stack.InitCursor(operation.Target, schema, rows)
		case OpCode_ForQueryNext:
			schema, row, ok := stack.AdvanceCursor(operation.PrimaryData)
			if !ok {
				stack.CloseCursor(operation.PrimaryData)
				// Jump forward past the loop body and back-goto, same mechanism as OpCode_If.
				counter = operation.Index - 1
			} else {
				if err := stack.UpdateRecord(operation.Target, schema, row); err != nil {
					return nil, err
				}
			}
		case OpCode_ReturnQuery:
			schema, rows, err := iFunc.QueryMultiReturn(ctx, stack, operation.PrimaryData, operation.SecondaryData)
			if err != nil {
				return nil, err
			}
			records, err := convertRowsToRecords(schema, rows)
			if err != nil {
				return nil, err
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
	return nil, nil
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
			return err
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
