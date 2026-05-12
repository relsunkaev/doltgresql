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

// nodeCreateLanguage handles *tree.CreateLanguage nodes.
func nodeCreateLanguage(ctx *Context, node *tree.CreateLanguage) (vitess.Statement, error) {
	if node == nil {
		return nil, nil
	}
	var handler, inline, validator string
	if node.Handler != nil {
		if node.Handler.Handler != nil {
			handler = node.Handler.Handler.String()
		}
		if node.Handler.Inline != nil {
			inline = node.Handler.Inline.String()
		}
		if node.Handler.Validator != nil {
			validator = node.Handler.Validator.String()
		}
	}
	return vitess.InjectedStatement{
		Statement: pgnodes.NewCreateLanguage(
			string(node.Name),
			node.Replace,
			node.Trusted,
			true,
			handler,
			inline,
			validator,
		),
		Children: nil,
	}, nil
}
