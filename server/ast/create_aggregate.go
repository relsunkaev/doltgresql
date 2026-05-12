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
	"github.com/dolthub/doltgresql/server/auth"
	pgnodes "github.com/dolthub/doltgresql/server/node"
)

// nodeCreateAggregate handles *tree.CreateAggregate nodes.
func nodeCreateAggregate(ctx *Context, node *tree.CreateAggregate) (vitess.Statement, error) {
	if node == nil {
		return nil, nil
	}

	if !ignoreUnsupportedStatements {
		if err := validateAggArgMode(ctx, node.Args, node.OrderByArgs); err != nil {
			return nil, err
		}
	}

	if len(node.OrderByArgs) > 0 {
		return NotYetSupportedError("CREATE AGGREGATE with ORDER BY arguments is not yet supported")
	}
	if len(node.AggOptions) > 0 {
		for _, option := range node.AggOptions {
			switch option.Option {
			case tree.AggOptTypeInitCond:
			default:
				return NotYetSupportedError("CREATE AGGREGATE option is not yet supported")
			}
		}
	}

	tableName := node.Name.ToTableName()
	var err error
	var params []pgnodes.RoutineParam
	if len(node.Args) > 0 {
		params = make([]pgnodes.RoutineParam, len(node.Args))
		for i, arg := range node.Args {
			params[i].Name = arg.Name.String()
			_, params[i].Type, err = nodeResolvableTypeReference(ctx, arg.Type, false)
			if err != nil {
				return nil, err
			}
		}
	} else if node.BaseType != nil {
		_, baseType, err := nodeResolvableTypeReference(ctx, node.BaseType, false)
		if err != nil {
			return nil, err
		}
		params = []pgnodes.RoutineParam{{Type: baseType}}
	} else {
		params = nil
	}
	_, stateType, err := nodeResolvableTypeReference(ctx, node.SType, false)
	if err != nil {
		return nil, err
	}
	sFuncName := node.SFunc.ToTableName()

	var initCond string
	for _, option := range node.AggOptions {
		if option.Option != tree.AggOptTypeInitCond {
			continue
		}
		switch val := option.CondVal.(type) {
		case *tree.StrVal:
			initCond = val.RawString()
		case *tree.DString:
			initCond = string(*val)
		default:
			return nil, errors.Errorf("INITCOND must be a string literal")
		}
	}

	return vitess.InjectedStatement{
		Statement: pgnodes.NewCreateAggregate(
			tableName.Table(),
			tableName.Schema(),
			node.Replace,
			params,
			stateType,
			sFuncName.Table(),
			sFuncName.Schema(),
			initCond,
			ctx.originalQuery,
		),
		Auth: vitess.AuthInformation{
			AuthType:    auth.AuthType_CREATE,
			TargetType:  auth.AuthTargetType_SchemaIdentifiers,
			TargetNames: []string{tableName.Catalog(), tableName.Schema()},
		},
	}, nil
}

// validateAggArgMode checks routine arguments for `OUT` and `INOUT` modes,
// which cannot be used for AGGREGATE arguments.
func validateAggArgMode(ctx *Context, args, orderByArgs tree.RoutineArgs) error {
	for _, sig := range args {
		if sig.Mode == tree.RoutineArgModeOut || sig.Mode == tree.RoutineArgModeInout {
			return errors.Errorf("aggregate functions do not support OUT or INOUT arguments")
		}
	}
	for _, sig := range orderByArgs {
		if sig.Mode == tree.RoutineArgModeOut || sig.Mode == tree.RoutineArgModeInout {
			return errors.Errorf("aggregate functions do not support OUT or INOUT arguments")
		}
	}
	return nil
}
