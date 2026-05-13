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

	"github.com/dolthub/doltgresql/server/auth"
)

func tableColumnAuthTargets(tableTarget []string, columns []string) []string {
	targetNames := make([]string, 0, len(columns)*4)
	for _, column := range columns {
		targetNames = append(targetNames, tableTarget[0], tableTarget[1], tableTarget[2], column)
	}
	return targetNames
}

func applyAliasedTableColumnAuth(table vitess.TableExpr, authType string, columns []string) {
	if len(columns) == 0 {
		return
	}
	tableExpr, ok := table.(*vitess.AliasedTableExpr)
	if !ok || tableExpr.Auth.AuthType != authType || tableExpr.Auth.TargetType != auth.AuthTargetType_TableIdentifiers || len(tableExpr.Auth.TargetNames) != 3 {
		return
	}
	tableExpr.Auth.TargetType = auth.AuthTargetType_TableColumnIdents
	tableExpr.Auth.TargetNames = tableColumnAuthTargets(tableExpr.Auth.TargetNames, columns)
}
