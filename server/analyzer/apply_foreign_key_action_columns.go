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
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	gmsexpression "github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/planbuilder"
	"github.com/dolthub/go-mysql-server/sql/procedures"
	"github.com/dolthub/go-mysql-server/sql/transform"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/core/triggers"
	"github.com/dolthub/doltgresql/postgres/parser/parser"
	"github.com/dolthub/doltgresql/server/ast"
	"github.com/dolthub/doltgresql/server/deferrable"
	pgnodes "github.com/dolthub/doltgresql/server/node"
	pgtransform "github.com/dolthub/doltgresql/server/transform"
)

// ApplyForeignKeyActionColumns wraps FK handlers that need PostgreSQL
// referential-action behavior on propagated child-row edits.
func ApplyForeignKeyActionColumns(ctx *sql.Context, a *analyzer.Analyzer, node sql.Node, scope *plan.Scope, selector analyzer.RuleSelector, qFlags *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	return pgtransform.NodeWithOpaque(ctx, node, func(ctx *sql.Context, node sql.Node) (sql.Node, transform.TreeIdentity, error) {
		if deleteFrom, ok := node.(*plan.DeleteFrom); ok {
			wrappedDelete, changed, err := wrapDeleteForeignKeyActionTargets(ctx, a, deleteFrom)
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
		return wrapForeignKeyActionHandler(ctx, a, fkHandler)
	})
}

func wrapDeleteForeignKeyActionTargets(ctx *sql.Context, a *analyzer.Analyzer, deleteFrom *plan.DeleteFrom) (*plan.DeleteFrom, bool, error) {
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
			return wrapForeignKeyActionHandler(ctx, a, fkHandler)
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

func wrapForeignKeyActionHandler(ctx *sql.Context, a *analyzer.Analyzer, fkHandler *plan.ForeignKeyHandler) (sql.Node, transform.TreeIdentity, error) {
	validations := make(pgnodes.ForeignKeyActionValidations)
	needsPostgresActions, err := collectForeignKeyActionValidations(ctx, a, fkHandler.Editor, validations, make(map[*plan.ForeignKeyEditor]struct{}))
	if err != nil {
		return nil, transform.NewTree, err
	}
	if !needsPostgresActions {
		return fkHandler, transform.SameTree, nil
	}
	return pgnodes.NewPostgresForeignKeyActionHandler(fkHandler, validations, a.Runner), transform.NewTree, nil
}

func collectForeignKeyActionValidations(ctx *sql.Context, a *analyzer.Analyzer, editor *plan.ForeignKeyEditor, validations pgnodes.ForeignKeyActionValidations, seen map[*plan.ForeignKeyEditor]struct{}) (bool, error) {
	if editor == nil {
		return false, nil
	}
	if _, ok := seen[editor]; ok {
		return false, nil
	}
	seen[editor] = struct{}{}
	needsPostgresActions := false
	for _, refAction := range editor.RefActions {
		actionColumns, err := deferrable.ForeignKeyActionColumns(ctx, refAction.ForeignKey)
		if err != nil {
			return false, err
		}
		if !actionColumns.IsEmpty() {
			needsPostgresActions = true
		}
		firesChildRowTriggers, err := foreignKeyActionFiresChildRowTriggers(ctx, refAction.ForeignKey)
		if err != nil {
			return false, err
		}
		if firesChildRowTriggers {
			needsPostgresActions = true
		}
		if foreignKeyActionValidatesChildRow(refAction.ForeignKey) {
			needsPostgresActions = true
			validation, err := buildForeignKeyActionValidation(ctx, a, refAction.ForeignKey)
			if err != nil {
				return false, err
			}
			validations[pgnodes.ForeignKeyActionValidationKey(refAction.ForeignKey)] = validation
		}
		needsChildPostgresActions, err := collectForeignKeyActionValidations(ctx, a, refAction.Editor, validations, seen)
		if err != nil {
			return false, err
		}
		if needsChildPostgresActions {
			needsPostgresActions = true
		}
	}
	return needsPostgresActions, nil
}

func foreignKeyActionFiresChildRowTriggers(ctx *sql.Context, fk sql.ForeignKeyConstraint) (bool, error) {
	updatesChildRows := foreignKeyActionUpdatesChildRows(fk)
	deletesChildRows := foreignKeyActionDeletesChildRows(fk)
	if !updatesChildRows && !deletesChildRows {
		return false, nil
	}
	trigCollection, err := core.GetTriggersCollectionFromContext(ctx, foreignKeyActionDatabaseName(ctx, fk.Database))
	if err != nil {
		return false, err
	}
	tableID, err := foreignKeyActionChildTableID(ctx, fk)
	if err != nil {
		return false, err
	}
	allTriggers := trigCollection.GetTriggersForTable(ctx, tableID)
	for _, trigger := range allTriggers {
		if !trigger.FiresInOriginMode() || !trigger.ForEachRow {
			continue
		}
		for _, event := range trigger.Events {
			if updatesChildRows && event.Type == triggers.TriggerEventType_Update {
				return true, nil
			}
			if deletesChildRows && event.Type == triggers.TriggerEventType_Delete {
				return true, nil
			}
		}
	}
	return false, nil
}

func foreignKeyActionUpdatesChildRows(fk sql.ForeignKeyConstraint) bool {
	return foreignKeyUpdateActionUpdatesChildRows(fk.OnUpdate) || foreignKeyDeleteActionUpdatesChildRows(fk.OnDelete)
}

func foreignKeyUpdateActionUpdatesChildRows(action sql.ForeignKeyReferentialAction) bool {
	switch action {
	case sql.ForeignKeyReferentialAction_Cascade, sql.ForeignKeyReferentialAction_SetNull, sql.ForeignKeyReferentialAction_SetDefault:
		return true
	default:
		return false
	}
}

func foreignKeyDeleteActionUpdatesChildRows(action sql.ForeignKeyReferentialAction) bool {
	switch action {
	case sql.ForeignKeyReferentialAction_SetNull, sql.ForeignKeyReferentialAction_SetDefault:
		return true
	default:
		return false
	}
}

func foreignKeyActionDeletesChildRows(fk sql.ForeignKeyConstraint) bool {
	return fk.OnDelete == sql.ForeignKeyReferentialAction_Cascade
}

func foreignKeyActionDatabaseName(ctx *sql.Context, database string) string {
	if database != "" {
		return database
	}
	return ctx.GetCurrentDatabase()
}

func foreignKeyActionChildTableID(ctx *sql.Context, fk sql.ForeignKeyConstraint) (id.Table, error) {
	schemaName := fk.SchemaName
	if schemaName == "" {
		var err error
		schemaName, err = core.GetCurrentSchema(ctx)
		if err != nil {
			return id.NullTable, err
		}
	}
	return id.NewTable(schemaName, fk.Table), nil
}

func foreignKeyActionValidatesChildRow(fk sql.ForeignKeyConstraint) bool {
	return foreignKeyUpdateActionValidatesChildRow(fk.OnUpdate) || foreignKeyDeleteActionValidatesChildRow(fk.OnDelete)
}

func foreignKeyUpdateActionValidatesChildRow(action sql.ForeignKeyReferentialAction) bool {
	switch action {
	case sql.ForeignKeyReferentialAction_Cascade, sql.ForeignKeyReferentialAction_SetNull, sql.ForeignKeyReferentialAction_SetDefault:
		return true
	default:
		return false
	}
}

func foreignKeyDeleteActionValidatesChildRow(action sql.ForeignKeyReferentialAction) bool {
	switch action {
	case sql.ForeignKeyReferentialAction_SetNull, sql.ForeignKeyReferentialAction_SetDefault:
		return true
	default:
		return false
	}
}

func buildForeignKeyActionValidation(ctx *sql.Context, a *analyzer.Analyzer, fk sql.ForeignKeyConstraint) (pgnodes.ForeignKeyActionValidation, error) {
	childTable, _, err := a.Catalog.TableSchema(ctx, fk.Database, fk.SchemaName, fk.Table)
	if err != nil {
		return pgnodes.ForeignKeyActionValidation{}, err
	}
	checks, err := buildForeignKeyActionCheckConstraints(ctx, a, childTable, fk.SchemaName, fk.Table)
	if err != nil {
		return pgnodes.ForeignKeyActionValidation{}, err
	}
	validation := pgnodes.ForeignKeyActionValidation{Checks: checks}
	if fk.OnUpdate == sql.ForeignKeyReferentialAction_SetDefault || fk.OnDelete == sql.ForeignKeyReferentialAction_SetDefault {
		parentTable, _, err := a.Catalog.TableSchema(ctx, fk.ParentDatabase, fk.ParentSchema, fk.ParentTable)
		if err != nil {
			return pgnodes.ForeignKeyActionValidation{}, err
		}
		reference, err := buildForeignKeyActionReference(ctx, fk, childTable, parentTable)
		if err != nil {
			return pgnodes.ForeignKeyActionValidation{}, err
		}
		validation.Reference = reference
	}
	return validation, nil
}

func buildForeignKeyActionCheckConstraints(ctx *sql.Context, a *analyzer.Analyzer, table sql.Table, schemaName string, tableName string) (sql.CheckConstraints, error) {
	checkTable, ok := table.(sql.CheckTable)
	if !ok && table != nil {
		checkTable, ok = sql.GetUnderlyingTable(table).(sql.CheckTable)
	}
	if !ok {
		return nil, nil
	}
	checkDefinitions, err := checkTable.GetChecks(ctx)
	if err != nil {
		return nil, err
	}
	if len(checkDefinitions) == 0 {
		return nil, nil
	}
	if schemaTable, ok := table.(sql.DatabaseSchemaTable); ok {
		schemaName = schemaTable.DatabaseSchema().SchemaName()
	}
	columnIndexes := foreignKeyActionColumnIndexes(table.Schema(ctx))
	checks := make(sql.CheckConstraints, len(checkDefinitions))
	for i, checkDefinition := range checkDefinitions {
		checkExpr, err := buildForeignKeyActionCheckExpression(ctx, a, checkDefinition.CheckExpression, schemaName, tableName, columnIndexes)
		if err != nil {
			return nil, err
		}
		checks[i] = &sql.CheckConstraint{
			Name:     checkDefinition.Name,
			Expr:     checkExpr,
			Enforced: checkDefinition.Enforced,
		}
	}
	return checks, nil
}

func buildForeignKeyActionCheckExpression(ctx *sql.Context, a *analyzer.Analyzer, checkExpression string, schemaName string, tableName string, columnIndexes map[string]int) (sql.Expression, error) {
	query := fmt.Sprintf("SELECT %s FROM %s", checkExpression, foreignKeyActionTableName(schemaName, tableName))
	stmt, err := parser.ParseOne(query)
	if err != nil {
		return nil, err
	}
	parsed, err := ast.Convert(stmt)
	if err != nil {
		return nil, err
	}
	selectStmt, ok := parsed.(*vitess.Select)
	if !ok || len(selectStmt.SelectExprs) != 1 || len(selectStmt.From) != 1 {
		return nil, sql.ErrInvalidCheckConstraint.New(checkExpression)
	}
	aliasedExpr, ok := selectStmt.SelectExprs[0].(*vitess.AliasedExpr)
	if !ok {
		return nil, sql.ErrInvalidCheckConstraint.New(checkExpression)
	}
	builder := planbuilder.New(ctx, a.Catalog, nil)
	resolvedExpr := builder.BuildScalarWithTable(aliasedExpr.Expr, selectStmt.From[0])
	resolvedExpr, _, err = transform.Expr(ctx, resolvedExpr, func(ctx *sql.Context, expr sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		if interp, ok := expr.(procedures.InterpreterExpr); ok {
			return interp.SetStatementRunner(ctx, a.Runner), transform.NewTree, nil
		}
		if gf, ok := expr.(*gmsexpression.GetField); ok {
			idx, ok := columnIndexes[foreignKeyActionColumnKey(gf.Table(), gf.Name())]
			if !ok {
				idx, ok = columnIndexes[foreignKeyActionColumnKey("", gf.Name())]
			}
			if !ok {
				return nil, transform.SameTree, fmt.Errorf("field not found: %s", gf.String())
			}
			return gf.WithIndex(idx), transform.NewTree, nil
		}
		return expr, transform.SameTree, nil
	})
	if err != nil {
		return nil, err
	}
	return resolvedExpr, nil
}

func foreignKeyActionColumnIndexes(schema sql.Schema) map[string]int {
	indexes := make(map[string]int, len(schema))
	for i, col := range schema {
		indexes[foreignKeyActionColumnKey("", col.Name)] = i
		if col.Source != "" {
			indexes[foreignKeyActionColumnKey(col.Source, col.Name)] = i
		}
	}
	return indexes
}

func foreignKeyActionColumnKey(tableName string, columnName string) string {
	if tableName == "" {
		return strings.ToLower(columnName)
	}
	return strings.ToLower(tableName) + "." + strings.ToLower(columnName)
}

func buildForeignKeyActionReference(ctx *sql.Context, fk sql.ForeignKeyConstraint, childTable sql.Table, parentTable sql.Table) (*plan.ForeignKeyReferenceHandler, error) {
	childForeignKeyTable, ok := childTable.(sql.ForeignKeyTable)
	if !ok {
		return nil, sql.ErrNoForeignKeySupport.New(fk.Table)
	}
	parentForeignKeyTable, ok := parentTable.(sql.ForeignKeyTable)
	if !ok {
		return nil, sql.ErrNoForeignKeySupport.New(fk.ParentTable)
	}
	parentIndex, ok, err := plan.FindFKIndexWithPrefix(ctx, parentForeignKeyTable, fk.ParentColumns, true)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, sql.ErrForeignKeyMissingReferenceIndex.New(fk.Name, fk.ParentTable)
	}
	indexPositions, appendTypes, err := plan.FindForeignKeyColMapping(ctx, fk.Name, childForeignKeyTable, fk.Columns, fk.ParentColumns, parentIndex)
	if err != nil {
		return nil, err
	}
	typeConversions, err := plan.GetForeignKeyTypeConversions(parentForeignKeyTable.Schema(ctx), childForeignKeyTable.Schema(ctx), fk, plan.ChildToParent)
	if err != nil {
		return nil, err
	}
	var selfCols map[string]int
	if fk.IsSelfReferential() {
		selfCols = make(map[string]int)
		for i, col := range childForeignKeyTable.Schema(ctx) {
			selfCols[strings.ToLower(col.Name)] = i
		}
	}
	return &plan.ForeignKeyReferenceHandler{
		ForeignKey: fk,
		SelfCols:   selfCols,
		RowMapper: plan.ForeignKeyRowMapper{
			Index:                 parentIndex,
			Updater:               parentForeignKeyTable.GetForeignKeyEditor(ctx),
			SourceSch:             childForeignKeyTable.Schema(ctx),
			TargetTypeConversions: typeConversions,
			IndexPositions:        indexPositions,
			AppendTypes:           appendTypes,
		},
	}, nil
}

func foreignKeyActionTableName(schemaName string, tableName string) string {
	if schemaName == "" {
		return quoteForeignKeyActionIdentifier(tableName)
	}
	return quoteForeignKeyActionIdentifier(schemaName) + "." + quoteForeignKeyActionIdentifier(tableName)
}

func quoteForeignKeyActionIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}
