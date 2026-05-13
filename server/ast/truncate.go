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
	"strings"

	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/postgres/parser/sem/tree"
	"github.com/dolthub/doltgresql/server/auth"
	pgnodes "github.com/dolthub/doltgresql/server/node"
)

// nodeTruncate handles *tree.Truncate nodes.
func nodeTruncate(ctx *Context, node *tree.Truncate) (vitess.Statement, error) {
	if node == nil || len(node.Tables) == 0 {
		return nil, nil
	}
	switch node.DropBehavior {
	case tree.DropDefault, tree.DropRestrict:
		// RESTRICT matches PostgreSQL's default truncate-dependency policy
		// (foreign-key references already error); accept the keyword.
	case tree.DropCascade:
		// Handled by the injected truncate helper below.
	}
	cascade := node.DropBehavior == tree.DropCascade
	tableName, err := nodeTableName(ctx, &node.Tables[0])
	if err != nil {
		return nil, err
	}
	if cascade || len(node.Tables) > 1 || hasExplicitNonTempSchema(node.Tables) {
		statements := make([]pgnodes.TruncateTableStatement, 0, len(node.Tables))
		for i := range node.Tables {
			tableName, err = nodeTableName(ctx, &node.Tables[i])
			if err != nil {
				return nil, err
			}
			statements = append(statements, truncateTableStatement(tableName, &node.Tables[i]))
		}
		return vitess.InjectedStatement{
			Statement: pgnodes.NewTruncateTables(statements, cascade),
			Children:  nil,
		}, nil
	}
	return &vitess.DDL{
		Action: vitess.TruncateStr,
		Table:  tableName,
		Auth: vitess.AuthInformation{
			AuthType:    auth.AuthType_TRUNCATE,
			TargetType:  auth.AuthTargetType_TableIdentifiers,
			TargetNames: []string{tableName.DbQualifier.String(), tableName.SchemaQualifier.String(), tableName.Name.String()},
		},
	}, nil
}

func hasExplicitNonTempSchema(tableNames tree.TableNames) bool {
	for i := range tableNames {
		if tableNames[i].ExplicitSchema && !isTempSchemaName(string(tableNames[i].SchemaName)) {
			return true
		}
	}
	return false
}

func truncateTableStatement(tableName vitess.TableName, original *tree.TableName) pgnodes.TruncateTableStatement {
	statement := pgnodes.TruncateTableStatement{
		Query:    "TRUNCATE TABLE " + vitess.String(tableName),
		Database: tableName.DbQualifier.String(),
		Schema:   tableName.SchemaQualifier.String(),
		Table:    tableName.Name.String(),
	}
	if original.ExplicitSchema && !isTempSchemaName(string(original.SchemaName)) {
		statement.TempShadow = &pgnodes.TruncateTempShadow{
			Database: tableName.DbQualifier.String(),
			Table:    tableName.Name.String(),
		}
	}
	return statement
}

func isTempSchemaName(schema string) bool {
	schema = strings.ToLower(schema)
	return schema == "pg_temp" || strings.HasPrefix(schema, "pg_temp_")
}
