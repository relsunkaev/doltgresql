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
	"github.com/dolthub/go-mysql-server/sql"
	gmsanalyzer "github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"

	"github.com/dolthub/doltgresql/postgres/parser/parser"
	"github.com/dolthub/doltgresql/postgres/parser/sem/tree"
	pgnodes "github.com/dolthub/doltgresql/server/node"
)

// AssignRelationLocking adds PostgreSQL-style statement relation locks to table
// access. Explicit LOCK TABLE uses the same lock manager but holds locks until
// transaction end; ordinary reads and writes hold the compatible relation lock
// only while the statement is executing.
func AssignRelationLocking(ctx *sql.Context, _ *gmsanalyzer.Analyzer, node sql.Node, _ *plan.Scope, _ gmsanalyzer.RuleSelector, _ *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	mode, ok := statementRelationLockMode(ctx.Query())
	if !ok {
		if node.IsReadOnly() {
			mode = pgnodes.RelationLockAccessShare
		} else {
			mode = pgnodes.RelationLockRowExclusive
		}
	}
	if _, ok := node.(*pgnodes.RelationLockingNode); ok {
		return node, transform.SameTree, nil
	}
	targetsByKey := make(map[string]pgnodes.RelationLockTarget)
	transform.InspectWithOpaque(ctx, node, func(ctx *sql.Context, n sql.Node) bool {
		switch n := n.(type) {
		case *plan.ResolvedTable:
			target := relationLockTargetForTable(ctx, ctx.GetCurrentDatabase(), n.Table)
			targetsByKey[relationLockTargetKey(target)] = target
		case *plan.TableCountLookup:
			target := relationLockTargetForTable(ctx, n.Db().Name(), n.Table())
			targetsByKey[relationLockTargetKey(target)] = target
		}
		return true
	})
	if len(targetsByKey) == 0 {
		return node, transform.SameTree, nil
	}
	targets := make([]pgnodes.RelationLockTarget, 0, len(targetsByKey))
	for _, target := range targetsByKey {
		targets = append(targets, target)
	}
	return pgnodes.NewRelationLockingNode(node, targets, mode), transform.NewTree, nil
}

func statementRelationLockMode(query string) (pgnodes.RelationLockMode, bool) {
	if query == "" {
		return 0, false
	}
	stmts, err := parser.Parse(query)
	if err != nil || len(stmts) == 0 {
		return 0, false
	}
	switch stmts[0].AST.(type) {
	case *tree.Select:
		return pgnodes.RelationLockAccessShare, true
	case *tree.Insert, *tree.Update, *tree.Delete, *tree.Truncate, *tree.CopyFrom:
		return pgnodes.RelationLockRowExclusive, true
	default:
		return 0, false
	}
}

func relationLockTargetKey(target pgnodes.RelationLockTarget) string {
	return target.Database + "." + target.Schema + "." + target.Name
}

func relationLockTargetForTable(ctx *sql.Context, db string, table sql.Table) pgnodes.RelationLockTarget {
	if db == "" {
		db = ctx.GetCurrentDatabase()
	}
	target := pgnodes.RelationLockTarget{
		Database: db,
		Schema:   "public",
		Name:     table.Name(),
	}
	if schemaTable, ok := table.(sql.DatabaseSchemaTable); ok {
		if schema := schemaTable.DatabaseSchema(); schema != nil {
			target.Schema = schema.SchemaName()
		}
	}
	return target
}
