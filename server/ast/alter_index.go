// Copyright 2023 Dolthub, Inc.
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

// nodeAlterIndex handles *tree.AlterIndex nodes.
func nodeAlterIndex(ctx *Context, node *tree.AlterIndex) (vitess.Statement, error) {
	if node == nil {
		return nil, nil
	}

	switch cmd := node.Cmd.(type) {
	case *tree.AlterIndexSetTablespace:
		if !isDefaultIndexTablespaceName(cmd.Tablespace) {
			return nil, errors.Errorf("TABLESPACE is not yet supported for indexes")
		}
		schemaName, tableName, indexName, err := tableIndexNameParts(ctx, &node.Index)
		if err != nil {
			return nil, err
		}
		return vitess.InjectedStatement{
			Statement: pgnodes.NewAlterIndexSetDefaultTablespace(node.IfExists, schemaName, tableName, indexName),
		}, nil
	case *tree.AlterIndexSetStorage:
		var relOptions []string
		var resetKeys []string
		var err error
		if cmd.IsReset {
			resetKeys = nodeIndexRelOptionResetKeys(cmd.Params)
		} else {
			relOptions, err = nodeIndexRelOptions(cmd.Params)
			if err != nil {
				return nil, err
			}
		}
		schemaName, tableName, indexName, err := tableIndexNameParts(ctx, &node.Index)
		if err != nil {
			return nil, err
		}
		return vitess.InjectedStatement{
			Statement: pgnodes.NewAlterIndexSetStorage(node.IfExists, schemaName, tableName, indexName, relOptions, resetKeys),
		}, nil
	case *tree.AlterIndexSetStatistics:
		columnNumber, err := nodeIndexStorageParamInt(cmd.ColumnIdx)
		if err != nil {
			return nil, errors.Errorf("column number must be an integer")
		}
		if columnNumber < 1 || columnNumber > 32767 {
			return nil, errors.Errorf("column number must be in range from 1 to 32767")
		}
		statsTarget, err := nodeIndexStorageParamInt(cmd.Stats)
		if err != nil {
			return nil, errors.Errorf("statistics target must be an integer")
		}
		if statsTarget < -1 {
			return nil, errors.Errorf("statistics target %d is too low", statsTarget)
		}
		if statsTarget > 10000 {
			statsTarget = 10000
		}
		schemaName, tableName, indexName, err := tableIndexNameParts(ctx, &node.Index)
		if err != nil {
			return nil, err
		}
		return vitess.InjectedStatement{
			Statement: pgnodes.NewAlterIndexSetStatistics(node.IfExists, schemaName, tableName, indexName, int16(columnNumber), int16(statsTarget)),
		}, nil
	default:
		return nil, errors.Errorf("ALTER INDEX is not yet supported")
	}
}

func nodeIndexRelOptionResetKeys(params tree.StorageParams) []string {
	resetKeys := make([]string, len(params))
	for i, param := range params {
		resetKeys[i] = string(param.Key)
	}
	return resetKeys
}
