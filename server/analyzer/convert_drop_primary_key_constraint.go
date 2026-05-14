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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"

	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
	"github.com/dolthub/doltgresql/server/ast"
	"github.com/dolthub/doltgresql/server/indexmetadata"
	pgnodes "github.com/dolthub/doltgresql/server/node"
	"github.com/dolthub/doltgresql/server/tablemetadata"
)

// convertDropPrimaryKeyConstraint converts a DropConstraint node dropping a primary key constraint into
// an AlterPK node that GMS can process to remove the primary key.
func convertDropPrimaryKeyConstraint(ctx *sql.Context, _ *analyzer.Analyzer, n sql.Node, _ *plan.Scope, _ analyzer.RuleSelector, _ *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	return transform.Node(ctx, n, func(ctx *sql.Context, n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		dropConstraint, ok := n.(*plan.DropConstraint)
		if !ok {
			return n, transform.SameTree, nil
		}

		rt, ok := dropConstraint.Child.(*plan.ResolvedTable)
		if !ok {
			return nil, transform.SameTree, analyzer.ErrInAnalysis.New(
				"Expected a TableNode for ALTER TABLE DROP CONSTRAINT statement")
		}

		table := rt.Table
		dropConstraintName, cascade := ast.DecodeDropConstraintCascade(dropConstraint.Name)
		if cascade {
			dropConstraint = dropConstraintWithName(dropConstraint, dropConstraintName)
		}
		if it, ok := table.(sql.IndexAddressableTable); ok {
			indexes, err := it.GetIndexes(ctx)
			if err != nil {
				return nil, transform.SameTree, err
			}
			for _, index := range indexes {
				if index.ID() == "PRIMARY" && indexNameMatchesDropConstraint(index, rt.Table, dropConstraintName) {
					dependentForeignKeys, err := dependentForeignKeysForPrimaryKey(ctx, table)
					if err != nil {
						return nil, transform.SameTree, err
					}
					if len(dependentForeignKeys) > 0 && !cascade {
						return nil, transform.SameTree, pgerror.Newf(
							pgcode.DependentObjectsStillExist,
							"cannot drop constraint %s on table %s because other objects depend on it",
							dropConstraintName,
							table.Name(),
						)
					}
					alterDropPk := plan.NewAlterDropPk(rt.Database(), rt)
					newNode, err := alterDropPk.WithTargetSchema(rt.Schema(ctx))
					if err != nil {
						return n, transform.SameTree, err
					}
					if len(dependentForeignKeys) > 0 {
						return plan.NewBlock([]sql.Node{
							newNode,
							pgnodes.NewDropDependentForeignKeys(dependentForeignKeys),
						}), transform.NewTree, nil
					}
					return newNode, transform.NewTree, nil
				}
			}
		}

		if cascade {
			return dropConstraint, transform.NewTree, nil
		}
		return n, transform.SameTree, nil
	})
}

func wrapPrimaryKeyMetadata(ctx *sql.Context, _ *analyzer.Analyzer, n sql.Node, _ *plan.Scope, _ analyzer.RuleSelector, _ *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	return transform.Node(ctx, n, func(ctx *sql.Context, n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		if alterPk, ok := n.(*plan.AlterPK); ok && alterPk.Action == plan.PrimaryKeyAction_Drop {
			return pgnodes.NewAlterPrimaryKey(alterPk, ""), transform.NewTree, nil
		}
		if block, ok := n.(*plan.Block); ok {
			newChildren, changed := wrapCreatePrimaryKeyMetadata(block.Children())
			if changed {
				if len(newChildren) == 1 {
					return newChildren[0], transform.NewTree, nil
				}
				newBlock := plan.NewBlock(newChildren)
				newBlock.SetSchema(block.Schema(ctx))
				return newBlock, transform.NewTree, nil
			}
		}
		return n, transform.SameTree, nil
	})
}

func wrapCreatePrimaryKeyMetadata(nodes []sql.Node) ([]sql.Node, bool) {
	newNodes := make([]sql.Node, 0, len(nodes))
	changed := false
	for i := 0; i < len(nodes); i++ {
		alterPk, ok := nodes[i].(*plan.AlterPK)
		if !ok || alterPk.Action != plan.PrimaryKeyAction_Create || i+1 >= len(nodes) {
			newNodes = append(newNodes, nodes[i])
			continue
		}
		alterComment, ok := nodes[i+1].(*plan.AlterTableComment)
		if !ok {
			newNodes = append(newNodes, nodes[i])
			continue
		}
		if alterComment.Comment != "" {
			if _, ok = tablemetadata.DecodeComment(alterComment.Comment); !ok {
				newNodes = append(newNodes, nodes[i])
				continue
			}
		}
		newNodes = append(newNodes, pgnodes.NewAlterPrimaryKey(alterPk, alterComment.Comment))
		i++
		changed = true
	}
	return newNodes, changed
}

func indexNameMatchesDropConstraint(index sql.Index, table sql.Table, name string) bool {
	return strings.EqualFold(indexmetadata.DisplayNameForTable(index, table), name)
}

func dropConstraintWithName(dropConstraint *plan.DropConstraint, name string) *plan.DropConstraint {
	copied := *dropConstraint
	copied.Name = name
	return &copied
}

func dependentForeignKeysForPrimaryKey(ctx *sql.Context, table sql.Table) ([]sql.ForeignKeyConstraint, error) {
	primaryKeyColumns := primaryKeyColumnNames(ctx, table)
	if len(primaryKeyColumns) == 0 {
		return nil, nil
	}

	foreignKeyTable, ok := table.(sql.ForeignKeyTable)
	if !ok {
		return nil, nil
	}
	referencedForeignKeys, err := foreignKeyTable.GetReferencedForeignKeys(ctx)
	if err != nil {
		return nil, err
	}

	dependentForeignKeys := make([]sql.ForeignKeyConstraint, 0, len(referencedForeignKeys))
	for _, foreignKey := range referencedForeignKeys {
		if columnListsEqualFold(foreignKey.ParentColumns, primaryKeyColumns) {
			dependentForeignKeys = append(dependentForeignKeys, foreignKey)
		}
	}
	return dependentForeignKeys, nil
}

func primaryKeyColumnNames(ctx *sql.Context, table sql.Table) []string {
	schema := table.Schema(ctx)
	columnNames := make([]string, 0, len(schema))
	for _, column := range schema {
		if column.PrimaryKey {
			columnNames = append(columnNames, column.Name)
		}
	}
	return columnNames
}

func columnListsEqualFold(left []string, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if !strings.EqualFold(left[i], right[i]) {
			return false
		}
	}
	return true
}
