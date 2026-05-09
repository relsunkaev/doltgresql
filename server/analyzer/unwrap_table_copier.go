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
	copied := *tableCopier
	copied.Destination = createTable
	mvInfo, hasMvInfo := createMaterializedViewInfo(ctx, createTable.Name())
	if hasMvInfo {
		if err := pgnode.ValidateColumnAliases(createTable.PkSchema().Schema, mvInfo.columnAliases); err != nil {
			return nil, transform.SameTree, err
		}
	}
	if comment, ok := doltgresCreateTableMetadataComment(createTable.TableOpts); ok {
		var columnAliases []string
		if hasMvInfo {
			columnAliases = mvInfo.columnAliases
		}
		return pgnode.NewTableMetadataApplierWithColumnAliases(&copied, createTable.Database(), createTable.Name(), comment, columnAliases), transform.NewTree, nil
	}
	if hasMvInfo {
		return pgnode.NewTableMetadataApplierWithColumnAliases(&copied, createTable.Database(), createTable.Name(), mvInfo.comment, mvInfo.columnAliases), transform.NewTree, nil
	}
	return &copied, transform.NewTree, nil
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

func doltgresCreateTableMetadataComment(tableOpts map[string]any) (string, bool) {
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
	return comment, true
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
	return materializedViewInfo{
		comment:       tablemetadata.SetMaterializedViewDefinitionWithPopulated("", node.AsSource.String(), !node.WithNoData),
		columnAliases: materializedViewColumnAliases(node.ColumnNames),
	}, true
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
