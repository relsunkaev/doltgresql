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

package framework

import (
	"encoding/hex"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/extensions"
	corefunctions "github.com/dolthub/doltgresql/core/functions"
	"github.com/dolthub/doltgresql/core/id"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// FunctionProvider is the special sql.FunctionProvider for Doltgres that allows us to handle functions that
// are created by users.
type FunctionProvider struct{}

const qualifiedFunctionNamePrefix = "__doltgres_qualified_function__"
const qualifiedFunctionNameSeparator = "\x1f"

var _ sql.FunctionProvider = (*FunctionProvider)(nil)

// Function implements the interface sql.FunctionProvider.
func (fp *FunctionProvider) Function(ctx *sql.Context, name string) (sql.Function, bool) {
	// TODO: this should be configurable from within Dolt, rather than set on an external variable
	if !core.IsContextValid(ctx) {
		return nil, false
	}
	databaseName, schemaName, functionName, qualified := parseQualifiedFunctionName(name)
	funcCollection, err := core.GetFunctionsCollectionFromContextForDatabase(ctx, databaseName)
	if err != nil {
		return nil, false
	}
	typesCollection, err := core.GetTypesCollectionFromContextForDatabase(ctx, databaseName)
	if err != nil {
		return nil, false
	}
	var funcName id.Function
	var overloads []corefunctions.Function
	if qualified {
		funcName = id.NewFunction(schemaName, functionName)
		overloads, err = funcCollection.GetFunctionOverloads(ctx, funcName)
		if err != nil || len(overloads) == 0 {
			if fn, ok := compiledPgCatalogFunction(schemaName, functionName); ok {
				return fn, true
			}
			return nil, false
		}
	} else {
		// TODO: this should search all schemas in the search path, but the search path doesn't handle pg_catalog yet
		funcName = id.NewFunction("pg_catalog", functionName)
		overloads, err = funcCollection.GetFunctionOverloads(ctx, funcName)
		if err != nil {
			return nil, false
		}
		if len(overloads) == 0 {
			currentSchema, err := core.GetCurrentSchema(ctx)
			if err != nil {
				return nil, false
			}
			funcName = id.NewFunction(currentSchema, functionName)
			overloads, err = funcCollection.GetFunctionOverloads(ctx, funcName)
			if err != nil {
				return nil, false
			}
			if len(overloads) == 0 {
				return nil, false
			}
		}
	}

	overloadTree := NewOverloads()
	var aggregateNewBuffer NewBufferFn
	var hasAggregate bool
	for _, overload := range overloads {
		returnType, err := typesCollection.GetType(ctx, overload.ReturnType)
		if err != nil || returnType == nil {
			return nil, false
		}

		paramTypes := make([]*pgtypes.DoltgresType, len(overload.ParameterTypes))
		for i, paramType := range overload.ParameterTypes {
			paramTypes[i], err = typesCollection.GetType(ctx, paramType)
			if err != nil || paramTypes[i] == nil {
				return nil, false
			}
		}
		if overload.Aggregate {
			hasAggregate = true
			stateType, err := typesCollection.GetType(ctx, overload.AggregateStateType)
			if err != nil || stateType == nil {
				return nil, false
			}
			transition, err := funcCollection.GetFunction(ctx, overload.AggregateSFunc)
			if err != nil || !transition.ID.IsValid() || len(transition.SQLDefinition) == 0 {
				return nil, false
			}
			transitionReturnType, err := typesCollection.GetType(ctx, transition.ReturnType)
			if err != nil || transitionReturnType == nil {
				return nil, false
			}
			transitionParamTypes := make([]*pgtypes.DoltgresType, len(transition.ParameterTypes))
			for i, paramType := range transition.ParameterTypes {
				transitionParamTypes[i], err = typesCollection.GetType(ctx, paramType)
				if err != nil || transitionParamTypes[i] == nil {
					return nil, false
				}
			}
			aggregateFunction := SQLAggregateFunction{
				ID:             overload.ID,
				ReturnType:     returnType,
				ParameterTypes: paramTypes,
				StateType:      stateType,
				TransitionFunction: SQLFunction{
					ID:                 transition.ID,
					ReturnType:         transitionReturnType,
					ParameterNames:     transition.ParameterNames,
					ParameterTypes:     transitionParamTypes,
					ParameterDefaults:  transition.ParameterDefaults,
					Variadic:           transition.Variadic,
					IsNonDeterministic: transition.IsNonDeterministic,
					Strict:             transition.Strict,
					SqlStatement:       transition.SQLDefinition,
					SetOf:              transition.SetOf,
					SetConfig:          transition.SetConfig,
					Owner:              transition.Owner,
					SecurityDefiner:    transition.SecurityDefiner,
				},
				InitCond:           overload.AggregateInitCond,
				IsNonDeterministic: overload.IsNonDeterministic,
			}
			aggregateNewBuffer = aggregateFunction.NewBuffer
			if err = overloadTree.Add(aggregateFunction); err != nil {
				return nil, false
			}
		} else if len(overload.ExtensionName) > 0 {
			if err = overloadTree.Add(CFunction{
				ID:                 overload.ID,
				ReturnType:         returnType,
				ParameterTypes:     paramTypes,
				Variadic:           overload.Variadic,
				IsNonDeterministic: overload.IsNonDeterministic,
				Strict:             overload.Strict,
				ExtensionName:      extensions.LibraryIdentifier(overload.ExtensionName),
				ExtensionSymbol:    overload.ExtensionSymbol,
				Owner:              overload.Owner,
				SecurityDefiner:    overload.SecurityDefiner,
			}); err != nil {
				return nil, false
			}
		} else if len(overload.SQLDefinition) > 0 {
			if err = overloadTree.Add(SQLFunction{
				ID:                 overload.ID,
				ReturnType:         returnType,
				ParameterNames:     overload.ParameterNames,
				ParameterTypes:     paramTypes,
				ParameterDefaults:  overload.ParameterDefaults,
				Variadic:           overload.Variadic,
				IsNonDeterministic: overload.IsNonDeterministic,
				Strict:             overload.Strict,
				SqlStatement:       overload.SQLDefinition,
				SetOf:              overload.SetOf,
				SetConfig:          overload.SetConfig,
				Owner:              overload.Owner,
				SecurityDefiner:    overload.SecurityDefiner,
			}); err != nil {
				return nil, false
			}
		} else {
			if err = overloadTree.Add(InterpretedFunction{
				ID:                 overload.ID,
				ReturnType:         returnType,
				ParameterNames:     overload.ParameterNames,
				ParameterTypes:     paramTypes,
				Variadic:           overload.Variadic,
				IsNonDeterministic: overload.IsNonDeterministic,
				Strict:             overload.Strict,
				Statements:         overload.Operations,
				SetConfig:          overload.SetConfig,
				Owner:              overload.Owner,
				SecurityDefiner:    overload.SecurityDefiner,
			}); err != nil {
				return nil, false
			}
		}
	}
	if hasAggregate {
		return sql.FunctionN{
			Name: functionName,
			Fn: func(ctx *sql.Context, params ...sql.Expression) (sql.Expression, error) {
				return NewCompiledAggregateFunction(ctx, functionName, params, overloadTree, aggregateNewBuffer), nil
			},
		}, true
	}
	return sql.FunctionN{
		Name: functionName,
		Fn: func(ctx *sql.Context, params ...sql.Expression) (sql.Expression, error) {
			return NewCompiledFunction(ctx, functionName, params, overloadTree, false), nil
		},
	}, true
}

func compiledPgCatalogFunction(schemaName string, functionName string) (sql.Function, bool) {
	if !strings.EqualFold(schemaName, "pg_catalog") {
		return nil, false
	}
	createFunc, ok := compiledCatalog[strings.ToLower(functionName)]
	if !ok {
		return nil, false
	}
	return sql.FunctionN{
		Name: functionName,
		Fn:   createFunc,
	}, true
}

func parseQualifiedFunctionName(name string) (databaseName string, schemaName string, functionName string, qualified bool) {
	if !strings.HasPrefix(name, qualifiedFunctionNamePrefix) {
		return "", "", name, false
	}
	parts := strings.Split(strings.TrimPrefix(name, qualifiedFunctionNamePrefix), qualifiedFunctionNameSeparator)
	if len(parts) != 3 {
		return "", "", name, false
	}
	databaseBytes, err := hex.DecodeString(parts[0])
	if err != nil {
		return "", "", name, false
	}
	schemaBytes, err := hex.DecodeString(parts[1])
	if err != nil {
		return "", "", name, false
	}
	functionBytes, err := hex.DecodeString(parts[2])
	if err != nil {
		return "", "", name, false
	}
	return string(databaseBytes), string(schemaBytes), string(functionBytes), true
}
