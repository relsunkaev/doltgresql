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
	"slices"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"

	"github.com/dolthub/doltgresql/server/deferrable"
	pgtransform "github.com/dolthub/doltgresql/server/transform"
)

// EnforceForeignKeyMatchFull adds PostgreSQL MATCH FULL partial-null checks to
// GMS foreign-key editors. GMS models MATCH SIMPLE, where any NULL referencing
// value skips the parent lookup; MATCH FULL must only skip when every
// referencing value is NULL.
func EnforceForeignKeyMatchFull(ctx *sql.Context, a *analyzer.Analyzer, node sql.Node, scope *plan.Scope, selector analyzer.RuleSelector, qFlags *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	return pgtransform.NodeWithOpaque(ctx, node, func(ctx *sql.Context, node sql.Node) (sql.Node, transform.TreeIdentity, error) {
		fkHandler, ok := node.(*plan.ForeignKeyHandler)
		if !ok {
			return node, transform.SameTree, nil
		}
		editor, changed, err := wrapMatchFullForeignKeyEditor(ctx, fkHandler.Editor, make(map[*plan.ForeignKeyEditor]*plan.ForeignKeyEditor))
		if err != nil {
			return nil, transform.NewTree, err
		}
		if !changed {
			return node, transform.SameTree, nil
		}
		copied := *fkHandler
		copied.Editor = editor
		return &copied, transform.NewTree, nil
	})
}

func wrapMatchFullForeignKeyEditor(ctx *sql.Context, editor *plan.ForeignKeyEditor, seen map[*plan.ForeignKeyEditor]*plan.ForeignKeyEditor) (*plan.ForeignKeyEditor, bool, error) {
	if editor == nil {
		return editor, false, nil
	}
	if wrapped, ok := seen[editor]; ok {
		return wrapped, wrapped != editor, nil
	}
	seen[editor] = editor

	var changed bool
	copied := *editor

	checks, err := matchFullChecksForReferences(ctx, editor.References)
	if err != nil {
		return nil, false, err
	}
	if len(checks) > 0 && editor.Editor != nil {
		if existing, ok := editor.Editor.(*matchFullForeignKeyEditor); ok {
			copied.Editor = &matchFullForeignKeyEditor{
				inner:  existing.inner,
				checks: checks,
			}
		} else {
			copied.Editor = &matchFullForeignKeyEditor{
				inner:  editor.Editor,
				checks: checks,
			}
		}
		changed = true
	}

	if len(editor.RefActions) > 0 {
		copied.RefActions = make([]plan.ForeignKeyRefActionData, len(editor.RefActions))
		for i, refAction := range editor.RefActions {
			copiedRefAction := refAction
			childEditor, childChanged, err := wrapMatchFullForeignKeyEditor(ctx, refAction.Editor, seen)
			if err != nil {
				return nil, false, err
			}
			if childChanged {
				copiedRefAction.Editor = childEditor
				changed = true
			}
			copied.RefActions[i] = copiedRefAction
		}
	}

	if !changed {
		return editor, false, nil
	}
	seen[editor] = &copied
	return &copied, true, nil
}

func matchFullChecksForReferences(ctx *sql.Context, references []*plan.ForeignKeyReferenceHandler) ([]matchFullReference, error) {
	var checks []matchFullReference
	for _, reference := range references {
		if reference == nil || len(reference.RowMapper.IndexPositions) < 2 {
			continue
		}
		matchFull, err := deferrable.ForeignKeyMatchFull(ctx, reference.ForeignKey)
		if err != nil {
			return nil, err
		}
		if !matchFull {
			continue
		}
		checks = append(checks, matchFullReference{
			foreignKey:      reference.ForeignKey,
			columnPositions: slices.Clone(reference.RowMapper.IndexPositions),
		})
	}
	return checks, nil
}

type matchFullReference struct {
	foreignKey      sql.ForeignKeyConstraint
	columnPositions []int
}

type matchFullForeignKeyEditor struct {
	inner  sql.ForeignKeyEditor
	checks []matchFullReference
}

var _ sql.ForeignKeyEditor = (*matchFullForeignKeyEditor)(nil)

func (m *matchFullForeignKeyEditor) Insert(ctx *sql.Context, row sql.Row) error {
	if err := m.validate(row); err != nil {
		return err
	}
	return m.inner.Insert(ctx, row)
}

func (m *matchFullForeignKeyEditor) Update(ctx *sql.Context, old sql.Row, new sql.Row) error {
	if err := m.validate(new); err != nil {
		return err
	}
	return m.inner.Update(ctx, old, new)
}

func (m *matchFullForeignKeyEditor) Delete(ctx *sql.Context, row sql.Row) error {
	return m.inner.Delete(ctx, row)
}

func (m *matchFullForeignKeyEditor) StatementBegin(ctx *sql.Context) {
	m.inner.StatementBegin(ctx)
}

func (m *matchFullForeignKeyEditor) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	return m.inner.DiscardChanges(ctx, errorEncountered)
}

func (m *matchFullForeignKeyEditor) StatementComplete(ctx *sql.Context) error {
	return m.inner.StatementComplete(ctx)
}

func (m *matchFullForeignKeyEditor) Close(ctx *sql.Context) error {
	return m.inner.Close(ctx)
}

func (m *matchFullForeignKeyEditor) IndexedAccess(ctx *sql.Context, lookup sql.IndexLookup) sql.IndexedTable {
	return m.inner.IndexedAccess(ctx, lookup)
}

func (m *matchFullForeignKeyEditor) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	return m.inner.GetIndexes(ctx)
}

func (m *matchFullForeignKeyEditor) PreciseMatch() bool {
	return m.inner.PreciseMatch()
}

func (m *matchFullForeignKeyEditor) validate(row sql.Row) error {
	for _, check := range m.checks {
		nullCount := 0
		for _, position := range check.columnPositions {
			if position >= 0 && position < len(row) && row[position] == nil {
				nullCount++
			}
		}
		if nullCount > 0 && nullCount < len(check.columnPositions) {
			fk := check.foreignKey
			return sql.ErrForeignKeyChildViolation.New(fk.Name, fk.Table, fk.ParentTable, "MATCH FULL")
		}
	}
	return nil
}
