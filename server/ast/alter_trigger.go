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
	"github.com/dolthub/doltgresql/server/auth"
	pgnodes "github.com/dolthub/doltgresql/server/node"
)

// nodeAlterTrigger handles *tree.AlterTrigger nodes.
func nodeAlterTrigger(ctx *Context, node *tree.AlterTrigger) (vitess.Statement, error) {
	if node == nil {
		return nil, nil
	}
	if node.NewName == "" {
		return NotYetSupportedError("ALTER TRIGGER DEPENDS ON EXTENSION is not yet supported")
	}
	return vitess.InjectedStatement{
		Statement: pgnodes.NewAlterTriggerRename(
			node.OnTable.Schema(),
			node.OnTable.Table(),
			node.Name.String(),
			node.NewName.String(),
		),
		Auth: vitess.AuthInformation{
			AuthType:   auth.AuthType_IGNORE,
			TargetType: auth.AuthTargetType_Ignore,
		},
	}, nil
}
