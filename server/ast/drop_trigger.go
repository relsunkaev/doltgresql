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

// nodeDropTrigger handles *tree.DropTrigger nodes.
func nodeDropTrigger(ctx *Context, node *tree.DropTrigger) (vitess.Statement, error) {
	switch node.DropBehavior {
	case tree.DropDefault, tree.DropRestrict:
		// RESTRICT matches the default behavior PostgreSQL uses when no
		// keyword is given; accept it for migration tools that spell it out.
	case tree.DropCascade:
		// Triggers are not currently referenced by any other catalog object
		// in doltgres, so there is nothing to cascade — accept the keyword.
	}
	return vitess.InjectedStatement{
		Statement: pgnodes.NewDropTrigger(
			node.IfExists,
			node.Name.String(),
			node.OnTable.Schema(),
			node.OnTable.Table(),
			node.DropBehavior == tree.DropCascade),
		Children: nil,
	}, nil
}
