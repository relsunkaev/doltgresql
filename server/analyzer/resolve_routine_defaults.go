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

package analyzer

import (
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/extensions"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/core/procedures"
	"github.com/dolthub/doltgresql/server/functions"
	"github.com/dolthub/doltgresql/server/functions/framework"
	pgnodes "github.com/dolthub/doltgresql/server/node"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// ResolveProcedureDefaults resolves default expressions of routines that are in string format by parsing it into sql.Expression.
// This function retrieves the procedure overloads and sets CompiledFunction in the Call node.
func ResolveProcedureDefaults(ctx *sql.Context, a *analyzer.Analyzer, node sql.Node, scope *plan.Scope, selector analyzer.RuleSelector, qFlags *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	switch n := node.(type) {
	case *pgnodes.Call:
		procCollection, err := core.GetProceduresCollectionFromContextForDatabase(ctx, n.DatabaseName)
		if err != nil {
			return nil, transform.SameTree, err
		}
		typesCollection, err := core.GetTypesCollectionFromContextForDatabase(ctx, n.DatabaseName)
		if err != nil {
			return nil, transform.SameTree, err
		}
		schemaName, err := core.GetSchemaName(ctx, nil, n.SchemaName)
		if err != nil {
			return nil, transform.SameTree, err
		}
		procName := id.NewProcedure(schemaName, n.ProcedureName)
		overloads, err := procCollection.GetProcedureOverloads(ctx, procName)
		if err != nil {
			return nil, transform.SameTree, err
		}
		if len(overloads) == 0 {
			if strings.HasPrefix(n.ProcedureName, "dolt_") {
				return nil, transform.SameTree, functions.ErrDoltProcedureSelectOnly
			}
			return nil, transform.SameTree, sql.ErrStoredProcedureDoesNotExist.New(n.ProcedureName)
		}

		same := transform.SameTree
		if hasNamedProcedureArgs(n.ArgNames) {
			orderedExprs, err := orderProcedureCallArgs(n.ArgNames, n.Exprs, overloads)
			if err != nil {
				return nil, transform.SameTree, err
			}
			n.Exprs = orderedExprs
			same = transform.NewTree
		}
		overloadTree := framework.NewOverloads()
		outputSchema := sql.Schema(nil)
		for _, overload := range overloads {
			paramTypes := make([]*pgtypes.DoltgresType, len(overload.ParameterTypes))
			for i, paramType := range overload.ParameterTypes {
				paramTypes[i], err = typesCollection.GetType(ctx, paramType)
				if err != nil || paramTypes[i] == nil {
					return nil, transform.SameTree, err
				}
			}
			// TODO: we should probably have procedure equivalents instead of converting these to functions
			//  probably fine for now since we don't implement/support the differing functionality between the two just yet
			returnType := procedureReturnType(overload.ParameterModes, paramTypes)
			if len(overload.ExtensionName) > 0 {
				if err = overloadTree.Add(framework.CFunction{
					ID:                 id.Function(overload.ID),
					ReturnType:         returnType,
					ParameterTypes:     paramTypes,
					Variadic:           false,
					IsNonDeterministic: true,
					Strict:             false,
					ExtensionName:      extensions.LibraryIdentifier(overload.ExtensionName),
					ExtensionSymbol:    overload.ExtensionSymbol,
					Owner:              overload.Owner,
					SecurityDefiner:    overload.SecurityDefiner,
				}); err != nil {
					return nil, transform.SameTree, err
				}
			} else if len(overload.SQLDefinition) > 0 {
				if err = overloadTree.Add(framework.SQLFunction{
					ID:                 id.Function(overload.ID),
					ReturnType:         returnType,
					ParameterNames:     overload.ParameterNames,
					ParameterTypes:     paramTypes,
					ParameterDefaults:  overload.ParameterDefaults,
					Variadic:           false,
					IsNonDeterministic: true,
					Strict:             false,
					SqlStatement:       overload.SQLDefinition,
					SetOf:              false,
					SetConfig:          overload.SetConfig,
					Owner:              overload.Owner,
					SecurityDefiner:    overload.SecurityDefiner,
				}); err != nil {
					return nil, transform.SameTree, err
				}
			} else {
				if err = overloadTree.Add(framework.InterpretedFunction{
					ID:                 id.Function(overload.ID),
					ReturnType:         returnType,
					ParameterNames:     overload.ParameterNames,
					ParameterTypes:     paramTypes,
					ParameterModes:     procedureParameterModes(overload.ParameterModes),
					Variadic:           false,
					SetOf:              false,
					IsNonDeterministic: true,
					Strict:             false,
					Statements:         overload.Operations,
					SetConfig:          overload.SetConfig,
					Owner:              overload.Owner,
					SecurityDefiner:    overload.SecurityDefiner,
				}); err != nil {
					return nil, transform.SameTree, err
				}
			}
			if outputSchema == nil {
				outputSchema = procedureOutputSchema(overload.ParameterNames, overload.ParameterModes, paramTypes)
			}
		}
		compiledFunction := framework.NewCompiledFunction(ctx, n.ProcedureName, n.Exprs, overloadTree, false)
		// fill in default exprs if applicable
		if err := compiledFunction.ResolveDefaultValues(ctx, func(defExpr string) (sql.Expression, error) {
			return getDefaultExpr(ctx, a.Catalog, defExpr)
		}); err != nil {
			return nil, transform.SameTree, err
		}
		if err := checkResolvedRoutineExecutePrivilege(ctx, compiledFunction); err != nil {
			return nil, transform.SameTree, err
		}
		n.SetResolvedProcedure(compiledFunction, outputSchema)
		return node, same, nil
	default:
		return node, transform.SameTree, nil
	}
}

func hasNamedProcedureArgs(argNames []string) bool {
	for _, argName := range argNames {
		if argName != "" {
			return true
		}
	}
	return false
}

func orderProcedureCallArgs(argNames []string, exprs []sql.Expression, overloads []procedures.Procedure) ([]sql.Expression, error) {
	for _, overload := range overloads {
		orderedExprs, ok, err := orderProcedureCallArgsForOverload(argNames, exprs, overload.ParameterNames, len(overload.ParameterTypes))
		if err != nil {
			return nil, err
		}
		if ok {
			return orderedExprs, nil
		}
	}
	return nil, fmt.Errorf("could not match named arguments to procedure parameters")
}

func orderProcedureCallArgsForOverload(argNames []string, exprs []sql.Expression, paramNames []string, paramCount int) ([]sql.Expression, bool, error) {
	if len(argNames) != len(exprs) {
		return nil, false, fmt.Errorf("procedure argument metadata length mismatch")
	}
	if len(exprs) > paramCount {
		return nil, false, nil
	}
	orderedExprs := make([]sql.Expression, paramCount)
	filled := make([]bool, paramCount)
	nextPositional := 0
	seenNamed := false
	maxFilled := -1
	for i, expr := range exprs {
		argName := argNames[i]
		if argName == "" {
			if seenNamed {
				return nil, false, fmt.Errorf("positional argument cannot follow named argument")
			}
			for nextPositional < paramCount && filled[nextPositional] {
				nextPositional++
			}
			if nextPositional >= paramCount {
				return nil, false, nil
			}
			orderedExprs[nextPositional] = expr
			filled[nextPositional] = true
			if nextPositional > maxFilled {
				maxFilled = nextPositional
			}
			nextPositional++
			continue
		}
		seenNamed = true
		paramIndex := -1
		for j, paramName := range paramNames {
			if strings.EqualFold(paramName, argName) {
				paramIndex = j
				break
			}
		}
		if paramIndex < 0 || paramIndex >= paramCount {
			return nil, false, nil
		}
		if filled[paramIndex] {
			return nil, false, fmt.Errorf("duplicate procedure argument name %q", argName)
		}
		orderedExprs[paramIndex] = expr
		filled[paramIndex] = true
		if paramIndex > maxFilled {
			maxFilled = paramIndex
		}
	}
	for i := 0; i <= maxFilled; i++ {
		if !filled[i] {
			return nil, false, nil
		}
	}
	return orderedExprs[:maxFilled+1], true, nil
}

func procedureParameterModes(modes []procedures.ParameterMode) []uint8 {
	if len(modes) == 0 {
		return nil
	}
	ret := make([]uint8, len(modes))
	for i, mode := range modes {
		ret[i] = uint8(mode)
	}
	return ret
}

func procedureOutputSchema(names []string, modes []procedures.ParameterMode, types []*pgtypes.DoltgresType) sql.Schema {
	var schema sql.Schema
	for i, mode := range modes {
		if mode != procedures.ParameterMode_OUT && mode != procedures.ParameterMode_INOUT {
			continue
		}
		if i >= len(types) {
			continue
		}
		name := ""
		if i < len(names) {
			name = names[i]
		}
		if len(name) == 0 {
			name = fmt.Sprintf("column%d", len(schema)+1)
		}
		schema = append(schema, &sql.Column{
			Name:     name,
			Type:     types[i],
			Nullable: true,
		})
	}
	return schema
}

func procedureReturnType(modes []procedures.ParameterMode, types []*pgtypes.DoltgresType) *pgtypes.DoltgresType {
	for i, mode := range modes {
		if mode != procedures.ParameterMode_OUT && mode != procedures.ParameterMode_INOUT {
			continue
		}
		if i < len(types) {
			return types[i]
		}
	}
	return pgtypes.Void
}
