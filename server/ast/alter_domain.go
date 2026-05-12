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

// nodeAlterDomain handles ALTER DOMAIN nodes.
func nodeAlterDomain(ctx *Context, stmt *tree.AlterDomain) (vitess.Statement, error) {
	if stmt == nil {
		return nil, nil
	}

	if owner, ok := stmt.Cmd.(*tree.AlterDomainOwner); ok {
		domainName := stmt.Name.ToTableName()
		return vitess.InjectedStatement{
			Statement: pgnodes.NewAlterTypeOwner(
				domainName.Catalog(),
				domainName.Schema(),
				domainName.Object(),
				owner.Owner,
				true,
			),
		}, nil
	}

	return NotYetSupportedError("ALTER DOMAIN is not yet supported")
}
