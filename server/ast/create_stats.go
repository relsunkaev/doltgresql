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
	"strings"

	"github.com/cockroachdb/errors"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/postgres/parser/sem/tree"
	pgnodes "github.com/dolthub/doltgresql/server/node"
)

// nodeCreateStats handles *tree.CreateStats nodes.
func nodeCreateStats(ctx *Context, node *tree.CreateStats) (vitess.Statement, error) {
	if node == nil {
		return nil, nil
	}

	tableExpr, ok := node.Table.(*tree.UnresolvedObjectName)
	if !ok {
		return nil, errors.Errorf("unsupported table type in CREATE STATISTICS: %T", node.Table)
	}
	tableName, err := nodeUnresolvedObjectName(ctx, tableExpr)
	if err != nil {
		return nil, err
	}
	kinds, err := createStatisticKinds(node.Kinds)
	if err != nil {
		return nil, err
	}
	columns := make([]string, 0, len(node.ColumnNames))
	for _, column := range node.ColumnNames {
		columns = append(columns, string(column))
	}
	return vitess.InjectedStatement{Statement: pgnodes.NewCreateStatistics(
		string(node.Name),
		tableName.SchemaQualifier.String(),
		tableName.Name.String(),
		columns,
		kinds,
	)}, nil
}

func createStatisticKinds(kinds tree.NameList) ([]string, error) {
	if len(kinds) == 0 {
		return []string{"d", "f", "m"}, nil
	}
	ret := make([]string, 0, len(kinds))
	for _, kind := range kinds {
		switch strings.ToLower(string(kind)) {
		case "ndistinct":
			ret = append(ret, "d")
		case "dependencies":
			ret = append(ret, "f")
		case "mcv":
			ret = append(ret, "m")
		default:
			return nil, errors.Errorf("unrecognized statistics kind: %s", kind)
		}
	}
	return ret, nil
}
