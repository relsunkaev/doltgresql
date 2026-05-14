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
	gmsexpression "github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"

	"github.com/dolthub/doltgresql/server/deferrable"
)

// ForeignKeyActionValidation contains PostgreSQL-specific validation that GMS
// FK action editors do not carry through propagated child-row rewrites.
type ForeignKeyActionValidation struct {
	Checks    sql.CheckConstraints
	Reference *plan.ForeignKeyReferenceHandler
}

// ForeignKeyActionValidations is keyed by ForeignKeyActionValidationKey.
type ForeignKeyActionValidations map[string]ForeignKeyActionValidation

// ForeignKeyActionValidationKey returns a stable key for a foreign key
// constraint across analyzer and execution packages.
func ForeignKeyActionValidationKey(fk sql.ForeignKeyConstraint) string {
	parts := []string{
		fk.Database,
		fk.SchemaName,
		fk.Table,
		fk.Name,
		fk.ParentDatabase,
		fk.ParentSchema,
		fk.ParentTable,
	}
	parts = append(parts, fk.Columns...)
	parts = append(parts, fk.ParentColumns...)
	for i := range parts {
		parts[i] = strings.ToLower(parts[i])
	}
	return strings.Join(parts, "\x00")
}

// PostgresForeignKeyActionHandler preserves GMS FK handling while honoring
// PostgreSQL SET NULL / SET DEFAULT action column lists and child-row
// validations for propagated referential actions.
type PostgresForeignKeyActionHandler struct {
	*plan.ForeignKeyHandler
	validations ForeignKeyActionValidations
}

var _ sql.Node = (*PostgresForeignKeyActionHandler)(nil)
var _ sql.ExecBuilderNode = (*PostgresForeignKeyActionHandler)(nil)
var _ sql.Table = (*PostgresForeignKeyActionHandler)(nil)
var _ sql.UpdatableTable = (*PostgresForeignKeyActionHandler)(nil)
var _ sql.DeletableTable = (*PostgresForeignKeyActionHandler)(nil)
var _ sql.RowUpdater = (*PostgresForeignKeyActionHandler)(nil)
var _ sql.RowDeleter = (*PostgresForeignKeyActionHandler)(nil)
var _ sql.TableWrapper = (*PostgresForeignKeyActionHandler)(nil)

func NewPostgresForeignKeyActionHandler(handler *plan.ForeignKeyHandler, validations ForeignKeyActionValidations) *PostgresForeignKeyActionHandler {
	return &PostgresForeignKeyActionHandler{
		ForeignKeyHandler: handler,
		validations:       validations,
	}
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
	return &PostgresForeignKeyActionHandler{
		ForeignKeyHandler: &copied,
		validations:       n.validations,
	}, nil
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
	return n.postgresForeignKeyUpdate(ctx, n.Editor, old, new, 1)
}

// Delete implements sql.RowDeleter.
func (n *PostgresForeignKeyActionHandler) Delete(ctx *sql.Context, row sql.Row) error {
	return n.postgresForeignKeyDelete(ctx, n.Editor, row, 1)
}

func (n *PostgresForeignKeyActionHandler) postgresForeignKeyUpdate(ctx *sql.Context, fkEditor *plan.ForeignKeyEditor, old sql.Row, new sql.Row, depth int) error {
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
			if err := n.postgresForeignKeyOnUpdateCascade(ctx, fkEditor, refActionData, old, new, depth+1); err != nil {
				return err
			}
		case sql.ForeignKeyReferentialAction_SetNull:
			if err := n.postgresForeignKeyOnUpdateSetNull(ctx, fkEditor, refActionData, old, new, depth+1); err != nil {
				return err
			}
		case sql.ForeignKeyReferentialAction_SetDefault:
			if err := n.postgresForeignKeyOnUpdateSetDefault(ctx, fkEditor, refActionData, old, new, depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}

func (n *PostgresForeignKeyActionHandler) postgresForeignKeyDelete(ctx *sql.Context, fkEditor *plan.ForeignKeyEditor, row sql.Row, depth int) error {
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
			if err := n.postgresForeignKeyOnDeleteCascade(ctx, refActionData, row, depth+1); err != nil {
				return err
			}
		case sql.ForeignKeyReferentialAction_SetNull:
			if err := n.postgresForeignKeyOnDeleteSetNull(ctx, refActionData, row, depth+1); err != nil {
				return err
			}
		case sql.ForeignKeyReferentialAction_SetDefault:
			if err := n.postgresForeignKeyOnDeleteSetDefault(ctx, refActionData, row, depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}

func (n *PostgresForeignKeyActionHandler) postgresForeignKeyOnUpdateCascade(ctx *sql.Context, fkEditor *plan.ForeignKeyEditor, refActionData plan.ForeignKeyRefActionData, old sql.Row, new sql.Row, depth int) error {
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
		if err = postgresForeignKeyRecomputeGeneratedColumns(ctx, refActionData.Editor, updatedRow); err != nil {
			return err
		}
		if err = n.validateForeignKeyActionRow(ctx, refActionData, updatedRow, refActionData.ForeignKey.OnUpdate); err != nil {
			return err
		}
		if err = n.postgresForeignKeyUpdate(ctx, refActionData.Editor, rowToUpdate, updatedRow, depth); err != nil {
			return err
		}
	}
	if err == io.EOF {
		return nil
	}
	return err
}

func (n *PostgresForeignKeyActionHandler) postgresForeignKeyOnUpdateSetDefault(ctx *sql.Context, fkEditor *plan.ForeignKeyEditor, refActionData plan.ForeignKeyRefActionData, old sql.Row, new sql.Row, depth int) error {
	if ok, err := fkEditor.ColumnsUpdated(ctx, refActionData, old, new); err != nil {
		return err
	} else if !ok {
		return nil
	}
	actionColumns, err := deferrable.ForeignKeyActionColumns(ctx, refActionData.ForeignKey)
	if err != nil {
		return err
	}
	return n.postgresForeignKeySetDefaultRows(ctx, refActionData, actionColumns.OnUpdate, old, refActionData.ForeignKey.OnUpdate, depth)
}

func (n *PostgresForeignKeyActionHandler) postgresForeignKeyOnUpdateSetNull(ctx *sql.Context, fkEditor *plan.ForeignKeyEditor, refActionData plan.ForeignKeyRefActionData, old sql.Row, new sql.Row, depth int) error {
	if ok, err := fkEditor.ColumnsUpdated(ctx, refActionData, old, new); err != nil {
		return err
	} else if !ok {
		return nil
	}
	actionColumns, err := deferrable.ForeignKeyActionColumns(ctx, refActionData.ForeignKey)
	if err != nil {
		return err
	}
	return n.postgresForeignKeySetNullRows(ctx, refActionData, actionColumns.OnUpdate, old, refActionData.ForeignKey.OnUpdate, depth)
}

func (n *PostgresForeignKeyActionHandler) postgresForeignKeyOnDeleteCascade(ctx *sql.Context, refActionData plan.ForeignKeyRefActionData, row sql.Row, depth int) error {
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
		if err = n.postgresForeignKeyDelete(ctx, refActionData.Editor, rowToDelete, depth); err != nil {
			return err
		}
	}
	if err == io.EOF {
		return nil
	}
	return err
}

func (n *PostgresForeignKeyActionHandler) postgresForeignKeyOnDeleteSetDefault(ctx *sql.Context, refActionData plan.ForeignKeyRefActionData, row sql.Row, depth int) error {
	actionColumns, err := deferrable.ForeignKeyActionColumns(ctx, refActionData.ForeignKey)
	if err != nil {
		return err
	}
	return n.postgresForeignKeySetDefaultRows(ctx, refActionData, actionColumns.OnDelete, row, refActionData.ForeignKey.OnDelete, depth)
}

func (n *PostgresForeignKeyActionHandler) postgresForeignKeyOnDeleteSetNull(ctx *sql.Context, refActionData plan.ForeignKeyRefActionData, row sql.Row, depth int) error {
	actionColumns, err := deferrable.ForeignKeyActionColumns(ctx, refActionData.ForeignKey)
	if err != nil {
		return err
	}
	return n.postgresForeignKeySetNullRows(ctx, refActionData, actionColumns.OnDelete, row, refActionData.ForeignKey.OnDelete, depth)
}

func (n *PostgresForeignKeyActionHandler) postgresForeignKeySetDefaultRows(ctx *sql.Context, refActionData plan.ForeignKeyRefActionData, actionColumns []string, sourceRow sql.Row, action sql.ForeignKeyReferentialAction, depth int) error {
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
		if err = postgresForeignKeyRecomputeGeneratedColumns(ctx, refActionData.Editor, modifiedRow); err != nil {
			return err
		}
		if err = n.validateForeignKeyActionRow(ctx, refActionData, modifiedRow, action); err != nil {
			return err
		}
		if err = n.postgresForeignKeyUpdate(ctx, refActionData.Editor, rowToDefault, modifiedRow, depth); err != nil {
			return err
		}
	}
	if err == io.EOF {
		return nil
	}
	return err
}

func (n *PostgresForeignKeyActionHandler) postgresForeignKeySetNullRows(ctx *sql.Context, refActionData plan.ForeignKeyRefActionData, actionColumns []string, sourceRow sql.Row, action sql.ForeignKeyReferentialAction, depth int) error {
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
		if err = postgresForeignKeyRecomputeGeneratedColumns(ctx, refActionData.Editor, nulledRow); err != nil {
			return err
		}
		if err = n.validateForeignKeyActionRow(ctx, refActionData, nulledRow, action); err != nil {
			return err
		}
		if err = n.postgresForeignKeyUpdate(ctx, refActionData.Editor, rowToNull, nulledRow, depth); err != nil {
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

func (n *PostgresForeignKeyActionHandler) validateForeignKeyActionRow(ctx *sql.Context, refActionData plan.ForeignKeyRefActionData, row sql.Row, action sql.ForeignKeyReferentialAction) error {
	validation, ok := n.validations[ForeignKeyActionValidationKey(refActionData.ForeignKey)]
	if !ok {
		return nil
	}
	if action == sql.ForeignKeyReferentialAction_SetDefault && validation.Reference != nil {
		if err := validation.Reference.CheckReference(ctx, row); err != nil {
			return err
		}
	}
	for _, check := range validation.Checks {
		if check == nil || !check.Enforced {
			continue
		}
		res, err := sql.EvaluateCondition(ctx, check.Expr, row)
		if err != nil {
			return err
		}
		if sql.IsFalse(res) {
			return sql.ErrCheckConstraintViolated.New(check.Name)
		}
	}
	return nil
}

func postgresForeignKeyRecomputeGeneratedColumns(ctx *sql.Context, fkEditor *plan.ForeignKeyEditor, row sql.Row) error {
	hasGeneratedColumn := false
	for _, col := range fkEditor.Schema {
		if col.Generated != nil && !col.AutoIncrement {
			hasGeneratedColumn = true
			break
		}
	}
	if !hasGeneratedColumn {
		return nil
	}

	colNameToIdx := make(map[string]int, len(fkEditor.Schema))
	for i, col := range fkEditor.Schema {
		colNameToIdx[strings.ToLower(col.Name)] = i
		if col.Source != "" {
			colNameToIdx[fmt.Sprintf("%s.%s", strings.ToLower(col.Source), strings.ToLower(col.Name))] = i
		}
	}
	for i, col := range fkEditor.Schema {
		if col.Generated == nil || col.AutoIncrement {
			continue
		}
		generated := *col.Generated
		expr, _, err := transform.Expr(ctx, generated.Expr, func(ctx *sql.Context, expr sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
			gf, ok := expr.(*gmsexpression.GetField)
			if !ok {
				return expr, transform.SameTree, nil
			}
			key := strings.ToLower(gf.Name())
			if gf.Table() != "" {
				key = fmt.Sprintf("%s.%s", strings.ToLower(gf.Table()), key)
			}
			idx, ok := colNameToIdx[key]
			if !ok {
				return nil, transform.SameTree, fmt.Errorf("field not found: %s", gf.String())
			}
			return gf.WithIndex(idx), transform.NewTree, nil
		})
		if err != nil {
			return err
		}
		generated.Expr = expr
		val, err := generated.Eval(ctx, row)
		if err != nil {
			return err
		}
		row[i] = val
	}
	return nil
}
