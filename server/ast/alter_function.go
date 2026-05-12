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

package ast

import (
	"github.com/cockroachdb/errors"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/postgres/parser/sem/tree"
	pgnodes "github.com/dolthub/doltgresql/server/node"
)

// nodeAlterFunction handles *tree.AlterFunction nodes.
func nodeAlterFunction(ctx *Context, node *tree.AlterFunction) (vitess.Statement, error) {
	options, err := validateRoutineOptions(ctx, node.Options)
	if err != nil {
		return nil, err
	}

	routine, err := routineWithParams(ctx, node.Name, node.Args)
	if err != nil {
		return nil, err
	}
	strict, metadata, hasOptions, err := alterFunctionOptions(options)
	if err != nil {
		return nil, err
	}
	if hasOptions {
		return vitess.InjectedStatement{
			Statement: pgnodes.NewAlterFunctionOptions(routine, strict, metadata),
			Children:  nil,
		}, nil
	}
	if node.Rename != nil {
		newName := node.Rename.ToTableName()
		return vitess.InjectedStatement{
			Statement: pgnodes.NewAlterFunctionRename(routine, newName.Object()),
			Children:  nil,
		}, nil
	}
	if node.Schema != "" {
		return vitess.InjectedStatement{
			Statement: pgnodes.NewAlterFunctionSetSchema(routine, node.Schema),
			Children:  nil,
		}, nil
	}
	if node.Owner != "" {
		return vitess.InjectedStatement{
			Statement: pgnodes.NewAlterFunctionOwner(routine, node.Owner),
			Children:  nil,
		}, nil
	}
	if node.Extension != "" {
		return vitess.InjectedStatement{
			Statement: pgnodes.NewAlterFunctionDependsOnExtension(routine, node.Extension, node.No),
			Children:  nil,
		}, nil
	}

	return NotYetSupportedError("ALTER FUNCTION statement is not yet supported")
}

func alterFunctionOptions(options map[tree.FunctionOption]tree.RoutineOption) (*bool, pgnodes.AlterFunctionOptionMetadata, bool, error) {
	var strict *bool
	var metadata pgnodes.AlterFunctionOptionMetadata
	hasOptions := false
	if nullInputOption, ok := options[tree.OptionNullInput]; ok {
		value := nullInputOption.NullInput == tree.ReturnsNullOnNullInput || nullInputOption.NullInput == tree.StrictNullInput
		strict = &value
		hasOptions = true
	}
	if security, ok := options[tree.OptionSecurity]; ok {
		value := security.Definer
		metadata.SecurityDefiner = &value
		hasOptions = true
	}
	if leakproof, ok := options[tree.OptionLeakProof]; ok {
		value := leakproof.IsLeakProof
		metadata.LeakProof = &value
		hasOptions = true
	}
	if volatility, ok := options[tree.OptionVolatility]; ok {
		value := volatilityChar(volatility.Volatility)
		metadata.Volatility = &value
		hasOptions = true
	}
	if parallel, ok := options[tree.OptionParallel]; ok {
		value := parallelChar(parallel.Parallel)
		metadata.Parallel = &value
		hasOptions = true
	}
	if cost, ok := options[tree.OptionCost]; ok {
		value, ok := routineOptionNumber(cost.Cost)
		if !ok || value <= 0 {
			return nil, metadata, false, errors.Errorf("COST must be positive")
		}
		metadata.Cost = &value
		hasOptions = true
	}
	if rows, ok := options[tree.OptionRows]; ok {
		value, ok := routineOptionNumber(rows.Rows)
		if !ok || value <= 0 {
			return nil, metadata, false, errors.Errorf("ROWS must be positive")
		}
		metadata.Rows = &value
		hasOptions = true
	}
	if _, ok := options[tree.OptionSet]; ok {
		setConfig, err := routineSetOptions(options)
		if err != nil {
			return nil, metadata, false, err
		}
		metadata.SetConfig = setConfig
		hasOptions = true
	}
	if reset, ok := options[tree.OptionReset]; ok {
		metadata.ResetAllConfig = reset.ResetAll
		if !reset.ResetAll {
			metadata.ResetConfig = []string{reset.ResetParam}
		}
		hasOptions = true
	}
	return strict, metadata, hasOptions, nil
}

func routineWithParams(ctx *Context, name *tree.UnresolvedObjectName, args tree.RoutineArgs) (*pgnodes.RoutineWithParams, error) {
	routineArgs := make([]pgnodes.RoutineParam, 0, len(args))
	for _, arg := range args {
		if arg.Mode == tree.RoutineArgModeOut {
			continue
		}
		_, dt, err := nodeResolvableTypeReference(ctx, arg.Type, false)
		if err != nil {
			return nil, err
		}
		routineArgs = append(routineArgs, pgnodes.RoutineParam{
			Name: arg.Name.String(),
			Type: dt,
		})
	}
	objName := name.ToTableName()
	return &pgnodes.RoutineWithParams{
		Args:        routineArgs,
		SchemaName:  objName.Schema(),
		RoutineName: objName.Object(),
	}, nil
}
