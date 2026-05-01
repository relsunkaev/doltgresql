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
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/postgres/parser/sem/tree"
	pgnodes "github.com/dolthub/doltgresql/server/node"
)

// nodeExecute handles *tree.Execute nodes.
func nodeExecute(ctx *Context, node *tree.Execute) (vitess.Statement, error) {
	if node == nil {
		return nil, nil
	}

	params := make([]string, len(node.Params))
	for i, param := range node.Params {
		params[i] = tree.AsString(param)
	}

	return vitess.InjectedStatement{
		Statement: pgnodes.ExecuteStatement{
			Name:        string(node.Name),
			Params:      params,
			DiscardRows: node.DiscardRows,
		},
	}, nil
}
