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
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"

	"github.com/dolthub/doltgresql/server/deferrable"
	pgnodes "github.com/dolthub/doltgresql/server/node"
	pgtransform "github.com/dolthub/doltgresql/server/transform"
)

// ApplyForeignKeyActionColumns wraps FK handlers that need PostgreSQL
// SET NULL / SET DEFAULT action column-list handling.
func ApplyForeignKeyActionColumns(ctx *sql.Context, a *analyzer.Analyzer, node sql.Node, scope *plan.Scope, selector analyzer.RuleSelector, qFlags *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	return pgtransform.NodeWithOpaque(ctx, node, func(ctx *sql.Context, node sql.Node) (sql.Node, transform.TreeIdentity, error) {
		if deleteFrom, ok := node.(*plan.DeleteFrom); ok {
			wrappedDelete, changed, err := wrapDeleteForeignKeyActionTargets(ctx, deleteFrom)
			if err != nil {
				return nil, transform.NewTree, err
			}
			if changed {
				return wrappedDelete, transform.NewTree, nil
			}
			return node, transform.SameTree, nil
		}

		fkHandler, ok := node.(*plan.ForeignKeyHandler)
		if !ok {
			return node, transform.SameTree, nil
		}
		return wrapForeignKeyActionHandler(ctx, fkHandler)
	})
}

func wrapDeleteForeignKeyActionTargets(ctx *sql.Context, deleteFrom *plan.DeleteFrom) (*plan.DeleteFrom, bool, error) {
	targets := deleteFrom.GetDeleteTargets()
	if len(targets) == 0 {
		return deleteFrom, false, nil
	}
	wrappedTargets := make([]sql.Node, len(targets))
	var changed bool
	for i, target := range targets {
		wrapped, identity, err := pgtransform.NodeWithOpaque(ctx, target, func(ctx *sql.Context, node sql.Node) (sql.Node, transform.TreeIdentity, error) {
			fkHandler, ok := node.(*plan.ForeignKeyHandler)
			if !ok {
				return node, transform.SameTree, nil
			}
			return wrapForeignKeyActionHandler(ctx, fkHandler)
		})
		if err != nil {
			return nil, false, err
		}
		if identity == transform.NewTree {
			changed = true
		}
		wrappedTargets[i] = wrapped
	}
	if !changed {
		return deleteFrom, false, nil
	}
	return deleteFrom.WithTargets(wrappedTargets), true, nil
}

func wrapForeignKeyActionHandler(ctx *sql.Context, fkHandler *plan.ForeignKeyHandler) (sql.Node, transform.TreeIdentity, error) {
	hasActionColumns, err := foreignKeyEditorHasActionColumns(ctx, fkHandler.Editor, make(map[*plan.ForeignKeyEditor]struct{}))
	if err != nil {
		return nil, transform.NewTree, err
	}
	if !hasActionColumns {
		return fkHandler, transform.SameTree, nil
	}
	return pgnodes.NewPostgresForeignKeyActionHandler(fkHandler), transform.NewTree, nil
}

func foreignKeyEditorHasActionColumns(ctx *sql.Context, editor *plan.ForeignKeyEditor, seen map[*plan.ForeignKeyEditor]struct{}) (bool, error) {
	if editor == nil {
		return false, nil
	}
	if _, ok := seen[editor]; ok {
		return false, nil
	}
	seen[editor] = struct{}{}
	for _, refAction := range editor.RefActions {
		actionColumns, err := deferrable.ForeignKeyActionColumns(ctx, refAction.ForeignKey)
		if err != nil {
			return false, err
		}
		if !actionColumns.IsEmpty() {
			return true, nil
		}
		hasChildActionColumns, err := foreignKeyEditorHasActionColumns(ctx, refAction.Editor, seen)
		if err != nil || hasChildActionColumns {
			return hasChildActionColumns, err
		}
	}
	return false, nil
}
