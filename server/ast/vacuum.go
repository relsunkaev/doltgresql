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

package ast

import (
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/doltgresql/postgres/parser/sem/tree"
	pgnodes "github.com/dolthub/doltgresql/server/node"
)

// nodeVacuum handles *tree.Vacuum nodes.
func nodeVacuum(_ *Context, node *tree.Vacuum) (vitess.Statement, error) {
	if node == nil {
		return nil, nil
	}
	tables := make([]doltdb.TableName, 0, len(node.TablesAndCols))
	for _, tableAndCols := range node.TablesAndCols {
		tableName := tableAndCols.Name.ToTableName()
		tables = append(tables, doltdb.TableName{
			Name:   tableName.Table(),
			Schema: tableName.Schema(),
		})
	}
	return vitess.InjectedStatement{
		Statement: pgnodes.NewVacuum(tables),
		Children:  nil,
	}, nil
}
