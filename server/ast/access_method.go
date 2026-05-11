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

// nodeCreateAccessMethod handles *tree.CreateAccessMethod nodes.
func nodeCreateAccessMethod(ctx *Context, node *tree.CreateAccessMethod) (vitess.Statement, error) {
	return vitess.InjectedStatement{
		Statement: pgnodes.NewCreateAccessMethod(
			string(node.Name),
			node.Type,
			string(node.Handler),
		),
		Children: nil,
	}, nil
}

// nodeDropAccessMethod handles *tree.DropAccessMethod nodes.
func nodeDropAccessMethod(ctx *Context, node *tree.DropAccessMethod) (vitess.Statement, error) {
	return vitess.InjectedStatement{
		Statement: pgnodes.NewDropAccessMethod(
			node.Names.ToStrings(),
			node.IfExists,
			node.DropBehavior == tree.DropCascade,
		),
		Children: nil,
	}, nil
}
