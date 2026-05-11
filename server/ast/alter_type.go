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

// nodeAlterType handles *tree.AlterType nodes.
func nodeAlterType(ctx *Context, node *tree.AlterType) (vitess.Statement, error) {
	if node == nil {
		return nil, nil
	}

	switch cmd := node.Cmd.(type) {
	case *tree.AlterTypeOwner:
		// We intentionally don't support OWNER TO since we don't support owning objects
		return NewNoOp("OWNER TO is unsupported and ignored"), nil
	case *tree.AlterTypeRenameAttribute:
		tn := node.Type.ToTableName()
		return vitess.InjectedStatement{
			Statement: pgnodes.NewAlterTypeRenameAttribute(
				tn.Catalog(),
				tn.Schema(),
				tn.Object(),
				string(cmd.ColName),
				string(cmd.NewColName),
				cmd.DropBehavior == tree.DropCascade,
			),
		}, nil
	}

	return NotYetSupportedError("ALTER TYPE is not yet supported")
}
