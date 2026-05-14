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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/postgres/parser/parser"
	"github.com/dolthub/doltgresql/postgres/parser/sem/tree"
	"github.com/dolthub/doltgresql/server/ast"
	pgnodes "github.com/dolthub/doltgresql/server/node"
)

// ReplaceNode is used to replace generic top-level nodes with Doltgres versions that wrap them, without performing any
// additional analysis. This is used to handle relatively straightforward tasks, like delete cascading, etc.
func ReplaceNode(ctx *sql.Context, a *analyzer.Analyzer, node sql.Node, scope *plan.Scope, selector analyzer.RuleSelector, qFlags *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	return replaceNode(ctx, a, node)
}

func replaceNode(ctx *sql.Context, a *analyzer.Analyzer, node sql.Node) (sql.Node, transform.TreeIdentity, error) {
	// TODO: need to add the majority of other DDL operations here
	switch node := node.(type) {
	case *pgnodes.RelationLockingNode:
		child, identity, err := replaceNode(ctx, a, node.Child())
		if err != nil || identity == transform.SameTree {
			return node, identity, err
		}
		rewrapped, err := node.WithChildren(ctx, child)
		if err != nil {
			return nil, transform.SameTree, err
		}
		return rewrapped, transform.NewTree, nil
	case *plan.CreateDB:
		return pgnodes.NewCreateDatabase(node), transform.NewTree, nil
	case *plan.CreateView:
		return pgnodes.NewCreateView(node), transform.NewTree, nil
	case *plan.DropSchema:
		return pgnodes.NewDropSchema(node), transform.NewTree, nil
	case *plan.DropDB:
		return pgnodes.NewDropDatabase(node), transform.NewTree, nil
	case *plan.DropTable:
		return pgnodes.NewDropTable(node, dropTableCascadeFromQuery(ctx.Query())), transform.NewTree, nil
	case *plan.DropView:
		return pgnodes.NewDropView(node), transform.NewTree, nil
	case *plan.CreateCheck:
		logicalName := core.DecodePhysicalConstraintName(node.Check.Name)
		cleanName, options := ast.DecodeCheckConstraintNameOptions(logicalName)
		if options.NotValid || options.NoInherit {
			stripped := *node
			check := *node.Check
			check.Name = core.EncodePhysicalConstraintName(cleanName)
			stripped.Check = &check
			return pgnodes.NewCreateCheck(&stripped, a.Overrides, options.NotValid), transform.NewTree, nil
		}
		return pgnodes.NewCreateCheck(node, a.Overrides, false), transform.NewTree, nil
	case *plan.CreateForeignKey:
		cleanName, notValid := ast.DecodeNotValidForeignKeyConstraintName(node.FkDef.Name)
		if notValid {
			stripped := *node
			fkDef := *node.FkDef
			fkDef.Name = cleanName
			stripped.FkDef = &fkDef
			return pgnodes.NewCreateForeignKey(&stripped, true), transform.NewTree, nil
		}
		return node, transform.SameTree, nil
	case *plan.DropCheck:
		return pgnodes.NewDropCheck(node), transform.NewTree, nil
	case *plan.InsertInto:
		if len(node.Returning) > 0 && (node.Ignore || (node.OnDupExprs != nil && node.OnDupExprs.HasUpdates())) {
			return pgnodes.NewOnConflictReturningInsert(node), transform.NewTree, nil
		}
		return node, transform.SameTree, nil
	case *plan.Update:
		if pgnodes.HasUpdateReturningAlias(ctx, node.Returning) {
			return pgnodes.NewUpdateReturningAliases(node), transform.NewTree, nil
		}
		return node, transform.SameTree, nil
	default:
		return node, transform.SameTree, nil
	}
}

func dropTableCascadeFromQuery(query string) bool {
	if query == "" {
		return false
	}
	statements, err := parser.Parse(query)
	if err != nil || len(statements) != 1 {
		return false
	}
	dropTable, ok := statements[0].AST.(*tree.DropTable)
	return ok && dropTable.DropBehavior == tree.DropCascade
}
