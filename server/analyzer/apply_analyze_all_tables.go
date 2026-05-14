// Copyright 2025 Dolthub, Inc.
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

package analyzer

import (
	"strings"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/dolthub/doltgresql/server/auth"
	"github.com/dolthub/doltgresql/server/tablemetadata"
)

// applyTablesForAnalyzeAllTables finds plan.AnalyzeTable nodes that don't have any tables explicitly specified and fills in all
// tables for the current database. This enables the ANALYZE; statement to analyze all tables.
func applyTablesForAnalyzeAllTables(ctx *sql.Context, a *analyzer.Analyzer, node sql.Node, scope *plan.Scope, selector analyzer.RuleSelector, qFlags *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	analyzeTable, ok := node.(*plan.AnalyzeTable)
	if !ok {
		return node, transform.SameTree, nil
	}

	// If a set of tables is already populated, we don't need to do anything. We only fill in all tables when
	// the caller didn't explicitly specify any tables to be analyzed.
	if len(analyzeTable.Tables) > 0 {
		filteredTables := filterAnalyzeTablesByPrivilege(ctx, analyzeTable.Tables)
		if len(filteredTables) == len(analyzeTable.Tables) {
			return node, transform.SameTree, nil
		}
		return analyzeTable.WithTables(filteredTables), transform.NewTree, nil
	}

	db, err := a.Catalog.Database(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return node, transform.SameTree, err
	}
	tableNames, err := db.GetTableNames(ctx)
	if err != nil {
		return node, transform.SameTree, err
	}

	var tables []sql.Table
	for _, tableName := range tableNames {
		table, ok, err := db.GetTableInsensitive(ctx, tableName)
		if err != nil {
			return node, transform.SameTree, err
		} else if !ok {
			return node, transform.SameTree, sql.ErrTableNotFound.New(tableName)
		}
		tables = append(tables, table)
	}

	analyzeTable = analyzeTable.WithTables(tables)
	analyzeTable = analyzeTable.WithTables(filterAnalyzeTablesByPrivilege(ctx, analyzeTable.Tables))
	return analyzeTable, transform.NewTree, nil
}

func filterAnalyzeTablesByPrivilege(ctx *sql.Context, tables []sql.Table) []sql.Table {
	filteredTables := make([]sql.Table, 0, len(tables))
	for _, table := range tables {
		if hasAnalyzeTablePrivilege(ctx, table) {
			filteredTables = append(filteredTables, table)
			continue
		}
		sess := dsess.DSessFromSess(ctx.Session)
		sess.Notice(&pgproto3.NoticeResponse{
			Severity: "WARNING",
			Message:  "permission denied to analyze " + quoteAnalyzeTableIdentifier(table.Name()) + ", skipping it",
		})
	}
	return filteredTables
}

func hasAnalyzeTablePrivilege(ctx *sql.Context, table sql.Table) bool {
	owner := ""
	if commented, ok := table.(sql.CommentedTable); ok {
		owner = tablemetadata.Owner(commented.Comment())
	}
	if owner == "" {
		owner = "postgres"
	}
	if owner == ctx.Client().User {
		return true
	}

	tableName := doltdb.TableName{Name: table.Name(), Schema: tableSchemaName(table)}
	allowed := false
	auth.LockRead(func() {
		role := auth.GetRole(ctx.Client().User)
		publicRole := auth.GetRole("public")
		allowed = (role.IsValid() && role.IsSuperUser) ||
			roleHasAnalyzePrivilege(role.ID(), tableName) ||
			roleHasAnalyzePrivilege(publicRole.ID(), tableName) ||
			auth.HasInheritedRole(role.ID(), "pg_maintain")
	})
	return allowed
}

func roleHasAnalyzePrivilege(role auth.RoleID, tableName doltdb.TableName) bool {
	if !role.IsValid() {
		return false
	}
	return auth.HasTablePrivilege(auth.TablePrivilegeKey{
		Role:  role,
		Table: tableName,
	}, auth.Privilege_MAINTAIN)
}

func quoteAnalyzeTableIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}
