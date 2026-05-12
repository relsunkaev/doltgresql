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

	// We intentionally don't support OWNER TO since we don't support owning objects
	routine, err := routineWithParams(ctx, node.Name, node.Args)
	if err != nil {
		return nil, err
	}
	if nullInputOption, ok := options[tree.OptionNullInput]; ok {
		strict := nullInputOption.NullInput == tree.ReturnsNullOnNullInput || nullInputOption.NullInput == tree.StrictNullInput
		return vitess.InjectedStatement{
			Statement: pgnodes.NewAlterFunctionOptions(routine, &strict),
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

	return NotYetSupportedError("ALTER FUNCTION statement is not yet supported")
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
