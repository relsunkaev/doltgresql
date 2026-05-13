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

func appendAdditionalColumnAuth(authInfo *vitess.AuthInformation, authType string, tableTarget []string, columns []string, ok bool) {
	if len(tableTarget) != 3 {
		return
	}
	additional := vitess.AuthInformation{
		AuthType:    authType,
		TargetType:  auth.AuthTargetType_TableIdentifiers,
		TargetNames: tableTarget,
	}
	if ok {
		if len(columns) == 0 {
			return
		}
		additional.TargetType = auth.AuthTargetType_TableColumnIdents
		additional.TargetNames = tableColumnAuthTargets(tableTarget, columns)
	}
	auth.AppendAdditionalAuth(authInfo, additional)
}

func aliasedTableAuthTarget(table vitess.TableExpr) ([]string, bool) {
	tableExpr, ok := table.(*vitess.AliasedTableExpr)
	if !ok || len(tableExpr.Auth.TargetNames) != 3 {
		return nil, false
	}
	return tableExpr.Auth.TargetNames, true
}

func dmlReadColumnAuthColumns(exprs ...tree.Expr) ([]string, bool) {
	collector := &selectColumnAuthCollector{
		columns: make(map[string]string),
		ignoredTableQualifiers: map[string]struct{}{
			"excluded": {},
		},
	}
	for _, expr := range exprs {
		if expr == nil {
			continue
		}
		if !collector.walk(expr) {
			return nil, false
		}
	}
	columns := make([]string, 0, len(collector.columns))
	for _, column := range collector.columns {
		columns = append(columns, column)
	}
	return columns, true
}

func returningReadExprs(returning tree.ReturningClause) []tree.Expr {
	returningExprs, ok := returning.(*tree.ReturningExprs)
	if !ok {
		return nil
	}
	exprs := make([]tree.Expr, 0, len(*returningExprs))
	for _, returningExpr := range *returningExprs {
		exprs = append(exprs, returningExpr.Expr)
	}
	return exprs
}

func updateSourceExprs(updateExprs tree.UpdateExprs) []tree.Expr {
	exprs := make([]tree.Expr, 0, len(updateExprs))
	for _, updateExpr := range updateExprs {
		exprs = append(exprs, updateExpr.Expr)
	}
	return exprs
}

func whereReadExpr(where *tree.Where) tree.Expr {
	if where == nil {
		return nil
	}
	return where.Expr
}
