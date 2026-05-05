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
		if it, ok := table.(sql.IndexAddressableTable); ok {
			indexes, err := it.GetIndexes(ctx)
			if err != nil {
				return nil, transform.SameTree, err
			}
			for _, index := range indexes {
				if index.ID() == "PRIMARY" && indexNameMatchesDropConstraint(index, rt.Table, dropConstraint.Name) {
					alterDropPk := plan.NewAlterDropPk(rt.Database(), rt)
					newNode, err := alterDropPk.WithTargetSchema(rt.Schema(ctx))
					if err != nil {
						return n, transform.SameTree, err
					}
					return newNode, transform.NewTree, nil
				}
			}
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
