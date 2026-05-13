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
	"github.com/dolthub/doltgresql/server/auth"
	pgnodes "github.com/dolthub/doltgresql/server/node"
)

// nodeRenameTable handles *tree.RenameTable nodes.
func nodeRenameTable(ctx *Context, node *tree.RenameTable) (vitess.Statement, error) {
	if node == nil {
		return nil, nil
	}
	fromName, err := nodeUnresolvedObjectName(ctx, node.Name)
	if err != nil {
		return nil, err
	}
	toName, err := nodeUnresolvedObjectName(ctx, node.NewName)
	if err != nil {
		return nil, err
	}
	if node.IsSequence {
		return vitess.InjectedStatement{
			Statement: pgnodes.NewRenameSequence(
				fromName.SchemaQualifier.String(),
				fromName.Name.String(),
				toName.SchemaQualifier.String(),
				toName.Name.String(),
				node.IfExists,
			),
			Auth: vitess.AuthInformation{
				AuthType:   auth.AuthType_IGNORE,
				TargetType: auth.AuthTargetType_Ignore,
			},
		}, nil
	}
	if fromName.SchemaQualifier.String() != "" || toName.SchemaQualifier.String() != "" || node.IsMaterialized {
		var statement vitess.Injectable
		if node.IsMaterialized {
			statement = pgnodes.NewRenameMaterializedView(
				node.IfExists,
				fromName.SchemaQualifier.String(),
				fromName.Name.String(),
				toName.SchemaQualifier.String(),
				toName.Name.String(),
			)
		} else {
			statement = pgnodes.NewRenameTable(
				node.IfExists,
				fromName.SchemaQualifier.String(),
				fromName.Name.String(),
				toName.SchemaQualifier.String(),
				toName.Name.String(),
			)
		}
		return vitess.InjectedStatement{
			Statement: statement,
			Auth: vitess.AuthInformation{
				AuthType:   auth.AuthType_IGNORE,
				TargetType: auth.AuthTargetType_Ignore,
			},
		}, nil
	}
	return &vitess.DDL{
		Action:     vitess.RenameStr,
		FromTables: vitess.TableNames{fromName},
		ToTables:   vitess.TableNames{toName},
		IfExists:   node.IfExists,
		Auth: vitess.AuthInformation{
			AuthType:    auth.AuthType_OWNER,
			TargetType:  auth.AuthTargetType_TableIdentifiers,
			TargetNames: []string{fromName.DbQualifier.String(), fromName.SchemaQualifier.String(), fromName.Name.String()},
		},
	}, nil
}
