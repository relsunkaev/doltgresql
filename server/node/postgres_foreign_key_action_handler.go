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

package node

import (
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"

	"github.com/dolthub/doltgresql/server/deferrable"
)

// PostgresForeignKeyActionHandler preserves GMS FK handling while honoring
// PostgreSQL SET NULL / SET DEFAULT action column lists for UPDATE and DELETE.
type PostgresForeignKeyActionHandler struct {
	*plan.ForeignKeyHandler
}

var _ sql.Node = (*PostgresForeignKeyActionHandler)(nil)
var _ sql.ExecBuilderNode = (*PostgresForeignKeyActionHandler)(nil)
var _ sql.Table = (*PostgresForeignKeyActionHandler)(nil)
var _ sql.UpdatableTable = (*PostgresForeignKeyActionHandler)(nil)
var _ sql.DeletableTable = (*PostgresForeignKeyActionHandler)(nil)
var _ sql.RowUpdater = (*PostgresForeignKeyActionHandler)(nil)
var _ sql.RowDeleter = (*PostgresForeignKeyActionHandler)(nil)
var _ sql.TableWrapper = (*PostgresForeignKeyActionHandler)(nil)

func NewPostgresForeignKeyActionHandler(handler *plan.ForeignKeyHandler) *PostgresForeignKeyActionHandler {
	return &PostgresForeignKeyActionHandler{ForeignKeyHandler: handler}
}

// BuildRowIter implements sql.ExecBuilderNode by delegating reads to the
// wrapped original node, matching GMS ForeignKeyHandler execution.
func (n *PostgresForeignKeyActionHandler) BuildRowIter(ctx *sql.Context, b sql.NodeExecBuilder, row sql.Row) (sql.RowIter, error) {
	return b.Build(ctx, n.OriginalNode, row)
}

// WithChildren implements sql.Node and preserves the PostgreSQL wrapper if
// later analyzer rules replace the wrapped child.
func (n *PostgresForeignKeyActionHandler) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(n, len(children), 1)
	}
	copied := *n.ForeignKeyHandler
	copied.OriginalNode = children[0]
	return &PostgresForeignKeyActionHandler{ForeignKeyHandler: &copied}, nil
}

// Updater implements sql.UpdatableTable.
func (n *PostgresForeignKeyActionHandler) Updater(*sql.Context) sql.RowUpdater {
	return n
}

// Deleter implements sql.DeletableTable.
func (n *PostgresForeignKeyActionHandler) Deleter(*sql.Context) sql.RowDeleter {
	return n
}

// Update implements sql.RowUpdater.
func (n *PostgresForeignKeyActionHandler) Update(ctx *sql.Context, old sql.Row, new sql.Row) error {
	return postgresForeignKeyUpdate(ctx, n.Editor, old, new, 1)
}

// Delete implements sql.RowDeleter.
func (n *PostgresForeignKeyActionHandler) Delete(ctx *sql.Context, row sql.Row) error {
	return postgresForeignKeyDelete(ctx, n.Editor, row, 1)
}

func postgresForeignKeyUpdate(ctx *sql.Context, fkEditor *plan.ForeignKeyEditor, old sql.Row, new sql.Row, depth int) error {
	for _, reference := range fkEditor.References {
		hasChange := false
		for _, idx := range reference.RowMapper.IndexPositions {
			cmp, err := fkEditor.Schema[idx].Type.Compare(ctx, old[idx], new[idx])
			if err != nil {
				return err
			}
			if cmp != 0 {
				hasChange = true
				break
			}
		}
		if !hasChange {
			continue
		}
		if err := reference.CheckReference(ctx, new); err != nil {
			return err
		}
	}
	for _, refActionData := range fkEditor.RefActions {
		switch refActionData.ForeignKey.OnUpdate {
		default:
			if err := fkEditor.OnUpdateRestrict(ctx, refActionData, old, new); err != nil {
				return err
			}
		case sql.ForeignKeyReferentialAction_Cascade:
		case sql.ForeignKeyReferentialAction_SetNull:
		case sql.ForeignKeyReferentialAction_SetDefault:
		}
	}
	for i, col := range fkEditor.Schema {
		if !col.Nullable && new[i] == nil {
			return errors.New(fmt.Sprintf(`null value in column "%s" violates not-null constraint`, col.Name))
		}
	}
	if err := fkEditor.Editor.Update(ctx, old, new); err != nil {
		return err
	}
	for _, refActionData := range fkEditor.RefActions {
		switch refActionData.ForeignKey.OnUpdate {
		case sql.ForeignKeyReferentialAction_Cascade:
			if err := postgresForeignKeyOnUpdateCascade(ctx, fkEditor, refActionData, old, new, depth+1); err != nil {
				return err
			}
		case sql.ForeignKeyReferentialAction_SetNull:
			if err := postgresForeignKeyOnUpdateSetNull(ctx, fkEditor, refActionData, old, new, depth+1); err != nil {
				return err
			}
		case sql.ForeignKeyReferentialAction_SetDefault:
			if err := postgresForeignKeyOnUpdateSetDefault(ctx, fkEditor, refActionData, old, new, depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}

func postgresForeignKeyDelete(ctx *sql.Context, fkEditor *plan.ForeignKeyEditor, row sql.Row, depth int) error {
	for _, refActionData := range fkEditor.RefActions {
		switch refActionData.ForeignKey.OnDelete {
		default:
			if err := fkEditor.OnDeleteRestrict(ctx, refActionData, row); err != nil {
				return err
			}
		case sql.ForeignKeyReferentialAction_Cascade:
		case sql.ForeignKeyReferentialAction_SetNull:
		case sql.ForeignKeyReferentialAction_SetDefault:
		}
	}
	if err := fkEditor.Editor.Delete(ctx, row); err != nil {
		return err
	}
	for _, refActionData := range fkEditor.RefActions {
		switch refActionData.ForeignKey.OnDelete {
		case sql.ForeignKeyReferentialAction_Cascade:
			if err := postgresForeignKeyOnDeleteCascade(ctx, refActionData, row, depth+1); err != nil {
				return err
			}
		case sql.ForeignKeyReferentialAction_SetNull:
			if err := postgresForeignKeyOnDeleteSetNull(ctx, refActionData, row, depth+1); err != nil {
				return err
			}
		case sql.ForeignKeyReferentialAction_SetDefault:
			if err := postgresForeignKeyOnDeleteSetDefault(ctx, refActionData, row, depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}

func postgresForeignKeyOnUpdateCascade(ctx *sql.Context, fkEditor *plan.ForeignKeyEditor, refActionData plan.ForeignKeyRefActionData, old sql.Row, new sql.Row, depth int) error {
	if ok, err := fkEditor.ColumnsUpdated(ctx, refActionData, old, new); err != nil {
		return err
	} else if !ok {
		return nil
	}

	rowIter, err := refActionData.RowMapper.GetIter(ctx, old, false)
	if err != nil {
		return err
	}
	defer rowIter.Close(ctx)
	var rowToUpdate sql.Row
	for rowToUpdate, err = rowIter.Next(ctx); err == nil; rowToUpdate, err = rowIter.Next(ctx) {
		if depth > 15 {
			return sql.ErrForeignKeyDepthLimit.New()
		}
		updatedRow := make(sql.Row, len(rowToUpdate))
		for i := range rowToUpdate {
			mappedVal := refActionData.ChildParentMapping[i]
			if mappedVal == -1 {
				updatedRow[i] = rowToUpdate[i]
			} else {
				updatedRow[i] = new[mappedVal]
			}
		}
		if err = postgresForeignKeyUpdate(ctx, refActionData.Editor, rowToUpdate, updatedRow, depth); err != nil {
			return err
		}
	}
	if err == io.EOF {
		return nil
	}
	return err
}

func postgresForeignKeyOnUpdateSetDefault(ctx *sql.Context, fkEditor *plan.ForeignKeyEditor, refActionData plan.ForeignKeyRefActionData, old sql.Row, new sql.Row, depth int) error {
	if ok, err := fkEditor.ColumnsUpdated(ctx, refActionData, old, new); err != nil {
		return err
	} else if !ok {
		return nil
	}
	actionColumns, err := deferrable.ForeignKeyActionColumns(ctx, refActionData.ForeignKey)
	if err != nil {
		return err
	}
	return postgresForeignKeySetDefaultRows(ctx, refActionData, actionColumns.OnUpdate, old, depth)
}

func postgresForeignKeyOnUpdateSetNull(ctx *sql.Context, fkEditor *plan.ForeignKeyEditor, refActionData plan.ForeignKeyRefActionData, old sql.Row, new sql.Row, depth int) error {
	if ok, err := fkEditor.ColumnsUpdated(ctx, refActionData, old, new); err != nil {
		return err
	} else if !ok {
		return nil
	}
	actionColumns, err := deferrable.ForeignKeyActionColumns(ctx, refActionData.ForeignKey)
	if err != nil {
		return err
	}
	return postgresForeignKeySetNullRows(ctx, refActionData, actionColumns.OnUpdate, old, depth)
}

func postgresForeignKeyOnDeleteCascade(ctx *sql.Context, refActionData plan.ForeignKeyRefActionData, row sql.Row, depth int) error {
	rowIter, err := refActionData.RowMapper.GetIter(ctx, row, false)
	if err != nil {
		return err
	}
	defer rowIter.Close(ctx)
	var rowToDelete sql.Row
	for rowToDelete, err = rowIter.Next(ctx); err == nil; rowToDelete, err = rowIter.Next(ctx) {
		if depth >= 15 {
			if refActionData.Editor.Cyclical {
				return sql.ErrForeignKeyDepthLimit.New()
			} else if depth > 15 {
				return sql.ErrForeignKeyDepthLimit.New()
			}
		}
		if err = postgresForeignKeyDelete(ctx, refActionData.Editor, rowToDelete, depth); err != nil {
			return err
		}
	}
	if err == io.EOF {
		return nil
	}
	return err
}

func postgresForeignKeyOnDeleteSetDefault(ctx *sql.Context, refActionData plan.ForeignKeyRefActionData, row sql.Row, depth int) error {
	actionColumns, err := deferrable.ForeignKeyActionColumns(ctx, refActionData.ForeignKey)
	if err != nil {
		return err
	}
	return postgresForeignKeySetDefaultRows(ctx, refActionData, actionColumns.OnDelete, row, depth)
}

func postgresForeignKeyOnDeleteSetNull(ctx *sql.Context, refActionData plan.ForeignKeyRefActionData, row sql.Row, depth int) error {
	actionColumns, err := deferrable.ForeignKeyActionColumns(ctx, refActionData.ForeignKey)
	if err != nil {
		return err
	}
	return postgresForeignKeySetNullRows(ctx, refActionData, actionColumns.OnDelete, row, depth)
}

func postgresForeignKeySetDefaultRows(ctx *sql.Context, refActionData plan.ForeignKeyRefActionData, actionColumns []string, sourceRow sql.Row, depth int) error {
	rowIter, err := refActionData.RowMapper.GetIter(ctx, sourceRow, false)
	if err != nil {
		return err
	}
	defer rowIter.Close(ctx)
	positions, err := postgresForeignKeyActionPositions(refActionData, actionColumns)
	if err != nil {
		return err
	}
	var rowToDefault sql.Row
	for rowToDefault, err = rowIter.Next(ctx); err == nil; rowToDefault, err = rowIter.Next(ctx) {
		if depth >= 15 {
			if refActionData.Editor.Cyclical {
				return sql.ErrForeignKeyDepthLimit.New()
			} else if depth > 15 {
				return sql.ErrForeignKeyDepthLimit.New()
			}
		}

		modifiedRow := make(sql.Row, len(rowToDefault))
		copy(modifiedRow, rowToDefault)
		for _, position := range positions {
			col := refActionData.Editor.Schema[position]
			if col.Default != nil {
				newVal, err := col.Default.Eval(ctx, rowToDefault)
				if err != nil {
					return err
				}
				modifiedRow[position] = newVal
			} else {
				modifiedRow[position] = nil
			}
		}
		if err = postgresForeignKeyUpdate(ctx, refActionData.Editor, rowToDefault, modifiedRow, depth); err != nil {
			return err
		}
	}
	if err == io.EOF {
		return nil
	}
	return err
}

func postgresForeignKeySetNullRows(ctx *sql.Context, refActionData plan.ForeignKeyRefActionData, actionColumns []string, sourceRow sql.Row, depth int) error {
	rowIter, err := refActionData.RowMapper.GetIter(ctx, sourceRow, false)
	if err != nil {
		return err
	}
	defer rowIter.Close(ctx)
	positions, err := postgresForeignKeyActionPositions(refActionData, actionColumns)
	if err != nil {
		return err
	}
	var rowToNull sql.Row
	for rowToNull, err = rowIter.Next(ctx); err == nil; rowToNull, err = rowIter.Next(ctx) {
		if depth >= 15 {
			if refActionData.Editor.Cyclical {
				return sql.ErrForeignKeyDepthLimit.New()
			} else if depth > 15 {
				return sql.ErrForeignKeyDepthLimit.New()
			}
		}
		nulledRow := make(sql.Row, len(rowToNull))
		copy(nulledRow, rowToNull)
		for _, position := range positions {
			nulledRow[position] = nil
		}
		if err = postgresForeignKeyUpdate(ctx, refActionData.Editor, rowToNull, nulledRow, depth); err != nil {
			return err
		}
	}
	if err == io.EOF {
		return nil
	}
	return err
}

func postgresForeignKeyActionPositions(refActionData plan.ForeignKeyRefActionData, actionColumns []string) ([]int, error) {
	if len(actionColumns) == 0 {
		positions := make([]int, 0, len(refActionData.ChildParentMapping))
		for i, parentIndex := range refActionData.ChildParentMapping {
			if parentIndex != -1 {
				positions = append(positions, i)
			}
		}
		return positions, nil
	}

	positions := make([]int, 0, len(actionColumns))
	for _, actionColumn := range actionColumns {
		position := -1
		for i, column := range refActionData.Editor.Schema {
			if i < len(refActionData.ChildParentMapping) && refActionData.ChildParentMapping[i] != -1 && strings.EqualFold(column.Name, actionColumn) {
				position = i
				break
			}
		}
		if position == -1 {
			return nil, sql.ErrKeyColumnDoesNotExist.New(actionColumn)
		}
		positions = append(positions, position)
	}
	return positions, nil
}
