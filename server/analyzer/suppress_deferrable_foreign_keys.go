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
	pgtransform "github.com/dolthub/doltgresql/server/transform"
)

// SuppressDeferrableForeignKeys removes immediate FK checks for deferrable
// constraints inside explicit transactions. The connection layer validates
// the affected constraints before COMMIT.
func SuppressDeferrableForeignKeys(ctx *sql.Context, a *analyzer.Analyzer, node sql.Node, scope *plan.Scope, selector analyzer.RuleSelector, qFlags *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	connectionID := ctx.Session.ID()
	if !deferrable.Active(connectionID) {
		return node, transform.SameTree, nil
	}
	return pgtransform.NodeWithOpaque(ctx, node, func(ctx *sql.Context, node sql.Node) (sql.Node, transform.TreeIdentity, error) {
		fkHandler, ok := node.(*plan.ForeignKeyHandler)
		if !ok {
			return node, transform.SameTree, nil
		}
		editor, changed := suppressDeferrableEditor(connectionID, fkHandler.Editor)
		if !changed {
			return node, transform.SameTree, nil
		}
		if len(editor.References) == 0 && len(editor.RefActions) == 0 {
			return fkHandler.OriginalNode, transform.NewTree, nil
		}
		copied := *fkHandler
		copied.Editor = editor
		return &copied, transform.NewTree, nil
	})
}

func suppressDeferrableEditor(connectionID uint32, editor *plan.ForeignKeyEditor) (*plan.ForeignKeyEditor, bool) {
	if editor == nil {
		return editor, false
	}
	copied := *editor
	changed := false
	copied.References = make([]*plan.ForeignKeyReferenceHandler, 0, len(editor.References))
	for _, reference := range editor.References {
		if deferrable.ShouldDefer(connectionID, reference.ForeignKey) {
			deferrable.MarkDirty(connectionID, reference.ForeignKey)
			changed = true
			continue
		}
		copied.References = append(copied.References, reference)
	}
	return &copied, changed
}
