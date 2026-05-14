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
)

// nodeDropView handles *tree.DropView nodes.
func nodeDropView(ctx *Context, node *tree.DropView) (*vitess.DDL, error) {
	if node == nil || len(node.Names) == 0 {
		return nil, nil
	}
	switch node.DropBehavior {
	case tree.DropDefault, tree.DropRestrict:
		// RESTRICT matches the default behavior — dependents already cause an
		// error during DROP — so the keyword is accepted without changes.
	case tree.DropCascade:
		// The executor handles dependent view removal.
	}
	tableNames := make([]vitess.TableName, len(node.Names))
	for i := range node.Names {
		var err error
		tableNames[i], err = nodeTableName(ctx, &node.Names[i])
		if err != nil {
			return nil, err
		}
	}
	if node.IsMaterialized {
		authTableNames := make([]string, 0, len(tableNames)*3)
		for _, tableName := range tableNames {
			authTableNames = append(authTableNames,
				tableName.DbQualifier.String(), tableName.SchemaQualifier.String(), tableName.Name.String())
		}
		return &vitess.DDL{
			Action:     vitess.DropStr,
			IfExists:   node.IfExists,
			FromTables: tableNames,
			Auth: vitess.AuthInformation{
				AuthType:    auth.AuthType_DROPTABLE,
				TargetType:  auth.AuthTargetType_TableIdentifiers,
				TargetNames: authTableNames,
			},
		}, nil
	}
	return &vitess.DDL{
		Action:    vitess.DropStr,
		IfExists:  node.IfExists,
		FromViews: tableNames,
	}, nil
}
