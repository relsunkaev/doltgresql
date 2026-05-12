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

// nodeDropAggregate handles *tree.DropAggregate nodes.
func nodeDropAggregate(ctx *Context, node *tree.DropAggregate) (vitess.Statement, error) {
	if node == nil {
		return nil, nil
	}

	if !ignoreUnsupportedStatements {
		for _, agg := range node.Aggregates {
			if err := validateAggArgMode(ctx, agg.AggSig.Args, agg.AggSig.OrderByArgs); err != nil {
				return nil, err
			}
		}
	}

	routines := make([]*pgnodes.RoutineWithParams, len(node.Aggregates))
	for i, agg := range node.Aggregates {
		if len(agg.AggSig.OrderByArgs) > 0 || agg.AggSig.All {
			return NotYetSupportedError("DROP AGGREGATE ordered-set signature is not yet supported")
		}
		routine, err := routineWithParams(ctx, agg.Name, agg.AggSig.Args)
		if err != nil {
			return nil, err
		}
		routines[i] = routine
	}
	return vitess.InjectedStatement{
		Statement: pgnodes.NewDropAggregate(
			node.IfExists,
			routines,
			node.DropBehavior == tree.DropCascade,
		),
		Children: nil,
	}, nil
}
