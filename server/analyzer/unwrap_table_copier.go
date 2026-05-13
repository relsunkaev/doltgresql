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

package analyzer

import (
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"

	"github.com/dolthub/doltgresql/postgres/parser/parser"
	"github.com/dolthub/doltgresql/postgres/parser/sem/tree"
	pgnode "github.com/dolthub/doltgresql/server/node"
	"github.com/dolthub/doltgresql/server/tablemetadata"
)

// UnwrapTableCopierCreateTable keeps CREATE TABLE AS SELECT destinations in
// the raw go-mysql-server shape that TableCopier's executor expects.
func UnwrapTableCopierCreateTable(ctx *sql.Context, a *analyzer.Analyzer, node sql.Node, scope *plan.Scope, selector analyzer.RuleSelector, qFlags *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	tableCopier, ok := node.(*plan.TableCopier)
	if !ok {
		return node, transform.SameTree, nil
	}
	createTable, ok := unwrapCreateTableDestination(tableCopier.Destination)
	if !ok {
		return node, transform.SameTree, nil
	}
	createTable = sanitizeCreateTableAsDestination(createTable)
	copied := *tableCopier
	copied.Destination = pgnode.NewCreateTable(createTable, nil)
	child := sql.Node(pgnode.NewCreateTableAs(&copied))
	ctasAliases, hasCtasAliases := createTableAsColumnAliases(ctx, createTable.Name())
	mvInfo, hasMvInfo := createMaterializedViewInfo(ctx, createTable.Name())
	if hasMvInfo {
		if err := pgnode.ValidateColumnAliases(createTable.PkSchema().Schema, mvInfo.columnAliases); err != nil {
			return nil, transform.SameTree, err
		}
	}
	if comment, ok := doltgresCreateTableMetadataComment(ctx, createTable.TableOpts); ok {
		var columnAliases []string
		if hasMvInfo {
			columnAliases = mvInfo.columnAliases
		} else if hasCtasAliases {
			columnAliases = ctasAliases
		}
		return pgnode.NewTableMetadataApplierWithColumnAliases(child, createTable.Database(), createTable.Name(), comment, columnAliases), transform.NewTree, nil
	}
	if hasMvInfo {
		return pgnode.NewTableMetadataApplierWithColumnAliases(child, createTable.Database(), createTable.Name(), mvInfo.comment, mvInfo.columnAliases), transform.NewTree, nil
	}
	if hasCtasAliases {
		return pgnode.NewTableMetadataApplierWithColumnAliases(child, createTable.Database(), createTable.Name(), "", ctasAliases), transform.NewTree, nil
	}
	return child, transform.NewTree, nil
}

func unwrapCreateTableDestination(node sql.Node) (*plan.CreateTable, bool) {
	switch node := node.(type) {
	case *plan.CreateTable:
		return node, true
	case *pgnode.CreateTable:
		return node.GMSCreateTable(), true
	case *pgnode.ContextRootFinalizer:
		return unwrapCreateTableDestination(node.Child())
	default:
		return nil, false
	}
}

func doltgresCreateTableMetadataComment(ctx *sql.Context, tableOpts map[string]any) (string, bool) {
	if tableOpts == nil {
		return "", false
	}
	comment, ok := tableOpts["comment"].(string)
	if !ok {
		return "", false
	}
	if _, ok = tablemetadata.DecodeComment(comment); !ok {
		return "", false
	}
	if user := ctx.Client().User; user != "" {
		comment = tablemetadata.SetOwner(comment, user)
	}
	return comment, true
}

func sanitizeCreateTableAsDestination(createTable *plan.CreateTable) *plan.CreateTable {
	sourceSchema := createTable.PkSchema().Schema
	schema := make(sql.Schema, 0, len(sourceSchema))
	for _, col := range sourceSchema {
		copied := col.Copy()
		copied.Default = nil
		copied.Generated = nil
		copied.OnUpdate = nil
		copied.PrimaryKey = false
		copied.Nullable = true
		copied.Virtual = false
		copied.AutoIncrement = false
		copied.Extra = ""
		schema = append(schema, copied)
	}
	return plan.NewCreateTable(createTable.Database(), createTable.Name(), createTable.IfNotExists(), createTable.Temporary(), &plan.TableSpec{
		Schema:    sql.NewPrimaryKeySchema(schema),
		Collation: createTable.Collation,
		TableOpts: createTable.TableOpts,
	})
}

func createTableAsColumnAliases(ctx *sql.Context, tableName string) ([]string, bool) {
	query := ctx.Query()
	if strings.TrimSpace(query) == "" {
		return nil, false
	}
	stmts, err := parser.Parse(query)
	if err != nil || len(stmts) != 1 {
		return nil, false
	}
	node, ok := stmts[0].AST.(*tree.CreateTable)
	if !ok || node.AsSource == nil || !strings.EqualFold(string(node.Table.ObjectName), tableName) {
		return nil, false
	}
	if len(node.Defs) == 0 {
		return nil, false
	}
	aliases := make([]string, 0, len(node.Defs))
	for _, def := range node.Defs {
		column, ok := def.(*tree.ColumnTableDef)
		if !ok {
			return nil, false
		}
		aliases = append(aliases, string(column.Name))
	}
	return aliases, true
}

type materializedViewInfo struct {
	comment       string
	columnAliases []string
}

func createMaterializedViewInfo(ctx *sql.Context, tableName string) (materializedViewInfo, bool) {
	query := ctx.Query()
	if strings.TrimSpace(query) == "" {
		return materializedViewInfo{}, false
	}
	stmts, err := parser.Parse(query)
	if err != nil || len(stmts) != 1 {
		return materializedViewInfo{}, false
	}
	node, ok := stmts[0].AST.(*tree.CreateMaterializedView)
	if !ok {
		return materializedViewInfo{}, false
	}
	if !strings.EqualFold(string(node.Name.ObjectName), tableName) {
		return materializedViewInfo{}, false
	}
	columnAliases := materializedViewColumnAliases(node.ColumnNames)
	definition := materializedViewDefinitionWithColumnAliases(node.AsSource.String(), columnAliases)
	comment := tablemetadata.SetMaterializedViewDefinitionWithPopulated("", definition, !node.WithNoData)
	if len(node.Params) > 0 {
		comment = tablemetadata.SetRelOptions(comment, materializedViewRelOptions(node.Params))
	}
	if user := ctx.Client().User; user != "" {
		comment = tablemetadata.SetOwner(comment, user)
	}
	return materializedViewInfo{
		comment:       comment,
		columnAliases: columnAliases,
	}, true
}

func materializedViewRelOptions(params tree.StorageParams) []string {
	relOptions := make([]string, 0, len(params))
	for _, param := range params {
		key := strings.ToLower(strings.TrimSpace(string(param.Key)))
		value := strings.Trim(tree.AsString(param.Value), "'")
		relOptions = append(relOptions, key+"="+value)
	}
	return relOptions
}

func materializedViewColumnAliases(names tree.NameList) []string {
	if len(names) == 0 {
		return nil
	}
	aliases := make([]string, len(names))
	for i, name := range names {
		aliases[i] = string(name)
	}
	return aliases
}

func materializedViewDefinitionWithColumnAliases(definition string, aliases []string) string {
	if len(aliases) == 0 {
		return definition
	}
	statements, err := parser.Parse(definition)
	if err != nil || len(statements) != 1 {
		return definition
	}
	selectStmt, ok := statements[0].AST.(*tree.Select)
	if !ok {
		return definition
	}
	selectClause, ok := selectStmt.Select.(*tree.SelectClause)
	if !ok {
		return definition
	}
	for i, alias := range aliases {
		if i >= len(selectClause.Exprs) {
			break
		}
		selectClause.Exprs[i].As = tree.UnrestrictedName(alias)
	}
	return selectStmt.String()
}
