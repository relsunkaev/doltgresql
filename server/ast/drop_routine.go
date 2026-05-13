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

package ast

import (
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/postgres/parser/sem/tree"
	pgnodes "github.com/dolthub/doltgresql/server/node"
)

// nodeDropRoutine handles *tree.DropRoutine nodes.
func nodeDropRoutine(ctx *Context, node *tree.DropRoutine) (vitess.Statement, error) {
	if node == nil {
		return nil, nil
	}

	routines, err := dropRoutineArgs(ctx, node.Routines)
	if err != nil {
		return nil, err
	}

	return vitess.InjectedStatement{
		Statement: pgnodes.NewDropRoutine(
			node.IfExists,
			routines,
			node.DropBehavior == tree.DropCascade),
		Children: nil,
	}, nil
}

func dropRoutineArgs(ctx *Context, routines []tree.RoutineWithArgs) ([]*pgnodes.RoutineWithParams, error) {
	routinesWithArgs := make([]*pgnodes.RoutineWithParams, len(routines))
	for i, routine := range routines {
		var args []pgnodes.RoutineParam
		for _, a := range routine.Args {
			if a.Mode != tree.RoutineArgModeOut {
				_, dt, err := nodeResolvableTypeReference(ctx, a.Type, false)
				if err != nil {
					return nil, err
				}
				args = append(args, pgnodes.RoutineParam{
					Name: a.Name.String(),
					Type: dt,
				})
			}
		}
		objName := routine.Name.ToTableName()
		routinesWithArgs[i] = &pgnodes.RoutineWithParams{
			Args:        args,
			SchemaName:  objName.Schema(),
			RoutineName: objName.Object(),
		}
	}
	return routinesWithArgs, nil
}
