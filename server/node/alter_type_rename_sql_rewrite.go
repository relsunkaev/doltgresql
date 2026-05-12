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
	"strings"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/postgres/parser/parser"
	"github.com/dolthub/doltgresql/postgres/parser/sem/tree"
	"github.com/dolthub/doltgresql/server/tablemetadata"
)

func (a *AlterTypeRename) renameDependentViews(ctx *sql.Context, oldTypeID id.Type, oldArrayID id.Type, newTypeID id.Type, newArrayID id.Type) error {
	databases, err := a.schemaDatabases(ctx)
	if err != nil {
		return err
	}
	for _, database := range databases {
		viewDatabase, ok := database.(sql.ViewDatabase)
		if !ok {
			continue
		}
		views, err := viewDatabase.AllViews(ctx)
		if err != nil {
			return err
		}
		for _, view := range views {
			createViewStatement, changed, err := rewriteSQLTypeReferences(view.CreateViewStatement, oldTypeID, oldArrayID, newTypeID, newArrayID)
			if err != nil {
				return err
			}
			if !changed {
				continue
			}
			textDefinition := view.TextDefinition
			if textDefinition != "" {
				if rewrittenTextDefinition, textChanged, err := rewriteSQLTypeReferences(textDefinition, oldTypeID, oldArrayID, newTypeID, newArrayID); err != nil {
					return err
				} else if textChanged {
					textDefinition = rewrittenTextDefinition
				}
			}
			if err = viewDatabase.DropView(ctx, view.Name); err != nil {
				return err
			}
			if err = viewDatabase.CreateView(ctx, view.Name, textDefinition, createViewStatement); err != nil {
				return err
			}
		}
	}
	return nil
}

func (a *AlterTypeRename) schemaDatabases(ctx *sql.Context) ([]sql.Database, error) {
	database, err := core.GetSqlDatabaseFromContext(ctx, a.DatabaseName)
	if err != nil || database == nil {
		return nil, err
	}
	schemaDatabase, ok := database.(sql.SchemaDatabase)
	if !ok || !schemaDatabase.SupportsDatabaseSchemas() {
		return []sql.Database{database}, nil
	}
	schemas, err := schemaDatabase.AllSchemas(ctx)
	if err != nil {
		return nil, err
	}
	databases := make([]sql.Database, len(schemas))
	for i, schema := range schemas {
		databases[i] = schema
	}
	return databases, nil
}

func (a *AlterTypeRename) databaseForTableName(ctx *sql.Context, tableName doltdb.TableName) (sql.Database, error) {
	database, err := core.GetSqlDatabaseFromContext(ctx, a.DatabaseName)
	if err != nil || database == nil || tableName.Schema == "" {
		return database, err
	}
	schemaDatabase, ok := database.(sql.SchemaDatabase)
	if !ok || !schemaDatabase.SupportsDatabaseSchemas() {
		return database, nil
	}
	schema, ok, err := schemaDatabase.GetSchema(ctx, tableName.Schema)
	if err != nil || !ok {
		return database, err
	}
	return schema, nil
}

func rewriteColumnDefaultTypeReferences(defaultValue *sql.ColumnDefaultValue, oldTypeID id.Type, oldArrayID id.Type, newTypeID id.Type, newArrayID id.Type) (*sql.ColumnDefaultValue, bool, error) {
	if defaultValue == nil || defaultValue.Expr == nil {
		return defaultValue, false, nil
	}
	rewrittenExpr, changed, err := rewriteExpressionTypeReferences(defaultValue.Expr.String(), oldTypeID, oldArrayID, newTypeID, newArrayID)
	if err != nil || !changed {
		return defaultValue, changed, err
	}
	rewritten := *defaultValue
	rewritten.Expr = sql.NewUnresolvedColumnDefaultValue(rewrittenExpr).Expr
	return &rewritten, true, nil
}

func rewriteCheckConstraintTypeReferences(check sql.CheckDefinition, oldTypeID id.Type, oldArrayID id.Type, newTypeID id.Type, newArrayID id.Type) (sql.CheckDefinition, bool, error) {
	rewrittenExpr, changed, err := rewriteExpressionTypeReferences(check.CheckExpression, oldTypeID, oldArrayID, newTypeID, newArrayID)
	if err != nil || !changed {
		return check, changed, err
	}
	check.CheckExpression = rewrittenExpr
	return check, true, nil
}

func rewriteMaterializedViewCommentTypeReferences(comment string, oldTypeID id.Type, oldArrayID id.Type, newTypeID id.Type, newArrayID id.Type) (string, bool, bool, error) {
	if !tablemetadata.IsMaterializedView(comment) {
		return comment, false, false, nil
	}
	definition := tablemetadata.MaterializedViewDefinition(comment)
	rewrittenDefinition, changed, err := rewriteSQLTypeReferences(definition, oldTypeID, oldArrayID, newTypeID, newArrayID)
	if err != nil || !changed {
		return comment, true, changed, err
	}
	updatedComment := tablemetadata.SetMaterializedViewDefinitionWithPopulated(
		comment,
		rewrittenDefinition,
		tablemetadata.IsMaterializedViewPopulated(comment),
	)
	return updatedComment, true, true, nil
}

func rewriteColumnDefaultCompositeAttributeReferences(defaultValue *sql.ColumnDefaultValue, typeID id.Type, oldAttr string, newAttr string, compositeColumns map[string]struct{}) (*sql.ColumnDefaultValue, bool, error) {
	if defaultValue == nil || defaultValue.Expr == nil {
		return defaultValue, false, nil
	}
	rewrittenExpr, changed, err := rewriteExpressionCompositeAttributeReferences(defaultValue.Expr.String(), typeID, oldAttr, newAttr, compositeColumns)
	if err != nil || !changed {
		return defaultValue, changed, err
	}
	rewritten := *defaultValue
	rewritten.Expr = sql.NewUnresolvedColumnDefaultValue(rewrittenExpr).Expr
	return &rewritten, true, nil
}

func rewriteMaterializedViewCommentCompositeAttributeReferences(comment string, typeID id.Type, oldAttr string, newAttr string) (string, bool, bool, error) {
	if !tablemetadata.IsMaterializedView(comment) {
		return comment, false, false, nil
	}
	definition := tablemetadata.MaterializedViewDefinition(comment)
	rewrittenDefinition, changed, err := rewriteSQLCompositeAttributeReferences(definition, typeID, oldAttr, newAttr)
	if err != nil || !changed {
		return comment, true, changed, err
	}
	updatedComment := tablemetadata.SetMaterializedViewDefinitionWithPopulated(
		comment,
		rewrittenDefinition,
		tablemetadata.IsMaterializedViewPopulated(comment),
	)
	return updatedComment, true, true, nil
}

func rewriteExpressionTypeReferences(expr string, oldTypeID id.Type, oldArrayID id.Type, newTypeID id.Type, newArrayID id.Type) (string, bool, error) {
	if !sqlTextMayReferenceType(expr, oldTypeID, oldArrayID) {
		return expr, false, nil
	}
	statements, err := parser.Parse("SELECT " + expr)
	if err != nil || len(statements) != 1 {
		return expr, false, err
	}
	selectStmt, ok := statements[0].AST.(*tree.Select)
	if !ok {
		return expr, false, nil
	}
	changed, err := rewriteSelectTypeReferences(selectStmt, oldTypeID, oldArrayID, newTypeID, newArrayID)
	if err != nil || !changed {
		return expr, changed, err
	}
	selectClause, ok := selectStmt.Select.(*tree.SelectClause)
	if !ok || len(selectClause.Exprs) != 1 {
		return expr, false, nil
	}
	return selectClause.Exprs[0].Expr.String(), true, nil
}

func rewriteExpressionCompositeAttributeReferences(expr string, typeID id.Type, oldAttr string, newAttr string, compositeColumns map[string]struct{}) (string, bool, error) {
	if !sqlTextMayReferenceCompositeAttribute(expr, typeID, oldAttr, compositeColumns) {
		return expr, false, nil
	}
	statements, err := parser.Parse("SELECT " + expr)
	if err != nil || len(statements) != 1 {
		return expr, false, err
	}
	selectStmt, ok := statements[0].AST.(*tree.Select)
	if !ok {
		return expr, false, nil
	}
	changed, err := rewriteSelectCompositeAttributeReferences(selectStmt, typeID, oldAttr, newAttr, compositeColumns)
	if err != nil || !changed {
		return expr, changed, err
	}
	selectClause, ok := selectStmt.Select.(*tree.SelectClause)
	if !ok || len(selectClause.Exprs) != 1 {
		return expr, false, nil
	}
	return selectClause.Exprs[0].Expr.String(), true, nil
}

func rewriteSQLTypeReferences(statement string, oldTypeID id.Type, oldArrayID id.Type, newTypeID id.Type, newArrayID id.Type) (string, bool, error) {
	if strings.TrimSpace(statement) == "" {
		return statement, false, nil
	}
	if !sqlTextMayReferenceType(statement, oldTypeID, oldArrayID) {
		return statement, false, nil
	}
	statements, err := parser.Parse(statement)
	if err != nil || len(statements) != 1 {
		return statement, false, err
	}
	switch stmt := statements[0].AST.(type) {
	case *tree.CreateView:
		changed, err := rewriteSelectTypeReferences(stmt.AsSource, oldTypeID, oldArrayID, newTypeID, newArrayID)
		if err != nil || !changed {
			return statement, changed, err
		}
		return stmt.String(), true, nil
	case *tree.CreateMaterializedView:
		changed, err := rewriteSelectTypeReferences(stmt.AsSource, oldTypeID, oldArrayID, newTypeID, newArrayID)
		if err != nil || !changed {
			return statement, changed, err
		}
		return stmt.String(), true, nil
	case *tree.Select:
		changed, err := rewriteSelectTypeReferences(stmt, oldTypeID, oldArrayID, newTypeID, newArrayID)
		if err != nil || !changed {
			return statement, changed, err
		}
		return stmt.String(), true, nil
	default:
		return statement, false, nil
	}
}

func rewriteSQLCompositeAttributeReferences(statement string, typeID id.Type, oldAttr string, newAttr string) (string, bool, error) {
	if strings.TrimSpace(statement) == "" {
		return statement, false, nil
	}
	if !sqlTextMayReferenceCompositeAttribute(statement, typeID, oldAttr, nil) {
		return statement, false, nil
	}
	statements, err := parser.Parse(statement)
	if err != nil || len(statements) == 0 {
		return statement, false, err
	}
	changed := false
	rewrittenStatements := make([]string, len(statements))
	for i, statement := range statements {
		statementChanged, err := rewriteStatementCompositeAttributeReferences(statement.AST, typeID, oldAttr, newAttr, nil)
		if err != nil {
			return statement.SQL, false, err
		}
		changed = changed || statementChanged
		rewrittenStatements[i] = statement.AST.String()
	}
	if !changed {
		return statement, false, nil
	}
	return strings.Join(rewrittenStatements, ";"), true, nil
}

func sqlTextMayReferenceType(text string, typeID id.Type, arrayID id.Type) bool {
	lowerText := strings.ToLower(text)
	if strings.Contains(lowerText, strings.ToLower(typeID.TypeName())) {
		return true
	}
	return arrayID.IsValid() && strings.Contains(lowerText, strings.ToLower(arrayID.TypeName()))
}

func sqlTextMayReferenceCompositeAttribute(text string, typeID id.Type, oldAttr string, compositeColumns map[string]struct{}) bool {
	lowerText := strings.ToLower(text)
	if !strings.Contains(lowerText, strings.ToLower(oldAttr)) {
		return false
	}
	if strings.Contains(lowerText, strings.ToLower(typeID.TypeName())) {
		return true
	}
	for column := range compositeColumns {
		if strings.Contains(lowerText, strings.ToLower(column)) {
			return true
		}
	}
	return len(compositeColumns) == 0
}

func rewriteSelectTypeReferences(selectStmt *tree.Select, oldTypeID id.Type, oldArrayID id.Type, newTypeID id.Type, newArrayID id.Type) (bool, error) {
	if selectStmt == nil {
		return false, nil
	}
	changed := false
	if selectStmt.With != nil {
		for _, cte := range selectStmt.With.CTEList {
			if cteSelect, ok := cte.Stmt.(*tree.Select); ok {
				cteChanged, err := rewriteSelectTypeReferences(cteSelect, oldTypeID, oldArrayID, newTypeID, newArrayID)
				if err != nil {
					return false, err
				}
				changed = changed || cteChanged
			}
		}
	}
	selectChanged, err := rewriteSelectStatementTypeReferences(selectStmt.Select, oldTypeID, oldArrayID, newTypeID, newArrayID)
	if err != nil {
		return false, err
	}
	changed = changed || selectChanged
	for _, order := range selectStmt.OrderBy {
		expr, exprChanged, err := rewriteExprTypeReferences(order.Expr, oldTypeID, oldArrayID, newTypeID, newArrayID)
		if err != nil {
			return false, err
		}
		if exprChanged {
			order.Expr = expr
			changed = true
		}
	}
	if selectStmt.Limit != nil {
		if selectStmt.Limit.Offset != nil {
			expr, exprChanged, err := rewriteExprTypeReferences(selectStmt.Limit.Offset, oldTypeID, oldArrayID, newTypeID, newArrayID)
			if err != nil {
				return false, err
			}
			if exprChanged {
				selectStmt.Limit.Offset = expr
				changed = true
			}
		}
		if selectStmt.Limit.Count != nil {
			expr, exprChanged, err := rewriteExprTypeReferences(selectStmt.Limit.Count, oldTypeID, oldArrayID, newTypeID, newArrayID)
			if err != nil {
				return false, err
			}
			if exprChanged {
				selectStmt.Limit.Count = expr
				changed = true
			}
		}
	}
	return changed, nil
}

func rewriteStatementCompositeAttributeReferences(statement tree.Statement, typeID id.Type, oldAttr string, newAttr string, compositeColumns map[string]struct{}) (bool, error) {
	switch stmt := statement.(type) {
	case *tree.CreateView:
		return rewriteSelectCompositeAttributeReferences(stmt.AsSource, typeID, oldAttr, newAttr, compositeColumns)
	case *tree.CreateMaterializedView:
		return rewriteSelectCompositeAttributeReferences(stmt.AsSource, typeID, oldAttr, newAttr, compositeColumns)
	case *tree.Select:
		return rewriteSelectCompositeAttributeReferences(stmt, typeID, oldAttr, newAttr, compositeColumns)
	default:
		return false, nil
	}
}

func rewriteSelectCompositeAttributeReferences(selectStmt *tree.Select, typeID id.Type, oldAttr string, newAttr string, compositeColumns map[string]struct{}) (bool, error) {
	if selectStmt == nil {
		return false, nil
	}
	changed := false
	if selectStmt.With != nil {
		for _, cte := range selectStmt.With.CTEList {
			if cteSelect, ok := cte.Stmt.(*tree.Select); ok {
				cteChanged, err := rewriteSelectCompositeAttributeReferences(cteSelect, typeID, oldAttr, newAttr, compositeColumns)
				if err != nil {
					return false, err
				}
				changed = changed || cteChanged
			}
		}
	}
	selectChanged, err := rewriteSelectStatementCompositeAttributeReferences(selectStmt.Select, typeID, oldAttr, newAttr, compositeColumns)
	if err != nil {
		return false, err
	}
	changed = changed || selectChanged
	for _, order := range selectStmt.OrderBy {
		expr, exprChanged, err := rewriteExprCompositeAttributeReferences(order.Expr, typeID, oldAttr, newAttr, compositeColumns)
		if err != nil {
			return false, err
		}
		if exprChanged {
			order.Expr = expr
			changed = true
		}
	}
	if selectStmt.Limit != nil {
		if selectStmt.Limit.Offset != nil {
			expr, exprChanged, err := rewriteExprCompositeAttributeReferences(selectStmt.Limit.Offset, typeID, oldAttr, newAttr, compositeColumns)
			if err != nil {
				return false, err
			}
			if exprChanged {
				selectStmt.Limit.Offset = expr
				changed = true
			}
		}
		if selectStmt.Limit.Count != nil {
			expr, exprChanged, err := rewriteExprCompositeAttributeReferences(selectStmt.Limit.Count, typeID, oldAttr, newAttr, compositeColumns)
			if err != nil {
				return false, err
			}
			if exprChanged {
				selectStmt.Limit.Count = expr
				changed = true
			}
		}
	}
	return changed, nil
}

func rewriteSelectStatementCompositeAttributeReferences(stmt tree.SelectStatement, typeID id.Type, oldAttr string, newAttr string, compositeColumns map[string]struct{}) (bool, error) {
	changed := false
	switch stmt := stmt.(type) {
	case *tree.SelectClause:
		for i, expr := range stmt.Exprs {
			rewritten, exprChanged, err := rewriteExprCompositeAttributeReferences(expr.Expr, typeID, oldAttr, newAttr, compositeColumns)
			if err != nil {
				return false, err
			}
			if exprChanged {
				stmt.Exprs[i].Expr = rewritten
				changed = true
			}
		}
		for i, expr := range stmt.DistinctOn {
			rewritten, exprChanged, err := rewriteExprCompositeAttributeReferences(expr, typeID, oldAttr, newAttr, compositeColumns)
			if err != nil {
				return false, err
			}
			if exprChanged {
				stmt.DistinctOn[i] = rewritten
				changed = true
			}
		}
		if stmt.Where != nil {
			rewritten, exprChanged, err := rewriteExprCompositeAttributeReferences(stmt.Where.Expr, typeID, oldAttr, newAttr, compositeColumns)
			if err != nil {
				return false, err
			}
			if exprChanged {
				stmt.Where.Expr = rewritten
				changed = true
			}
		}
		for i, expr := range stmt.GroupBy {
			rewritten, exprChanged, err := rewriteExprCompositeAttributeReferences(expr, typeID, oldAttr, newAttr, compositeColumns)
			if err != nil {
				return false, err
			}
			if exprChanged {
				stmt.GroupBy[i] = rewritten
				changed = true
			}
		}
		if stmt.Having != nil {
			rewritten, exprChanged, err := rewriteExprCompositeAttributeReferences(stmt.Having.Expr, typeID, oldAttr, newAttr, compositeColumns)
			if err != nil {
				return false, err
			}
			if exprChanged {
				stmt.Having.Expr = rewritten
				changed = true
			}
		}
	case *tree.ValuesClause:
		for rowIdx := range stmt.Rows {
			for exprIdx, expr := range stmt.Rows[rowIdx] {
				rewritten, exprChanged, err := rewriteExprCompositeAttributeReferences(expr, typeID, oldAttr, newAttr, compositeColumns)
				if err != nil {
					return false, err
				}
				if exprChanged {
					stmt.Rows[rowIdx][exprIdx] = rewritten
					changed = true
				}
			}
		}
	case *tree.ParenSelect:
		return rewriteSelectCompositeAttributeReferences(stmt.Select, typeID, oldAttr, newAttr, compositeColumns)
	case *tree.UnionClause:
		leftChanged, err := rewriteSelectCompositeAttributeReferences(stmt.Left, typeID, oldAttr, newAttr, compositeColumns)
		if err != nil {
			return false, err
		}
		rightChanged, err := rewriteSelectCompositeAttributeReferences(stmt.Right, typeID, oldAttr, newAttr, compositeColumns)
		if err != nil {
			return false, err
		}
		changed = leftChanged || rightChanged
	}
	return changed, nil
}

func rewriteSelectStatementTypeReferences(stmt tree.SelectStatement, oldTypeID id.Type, oldArrayID id.Type, newTypeID id.Type, newArrayID id.Type) (bool, error) {
	changed := false
	switch stmt := stmt.(type) {
	case *tree.SelectClause:
		for i, expr := range stmt.Exprs {
			rewritten, exprChanged, err := rewriteExprTypeReferences(expr.Expr, oldTypeID, oldArrayID, newTypeID, newArrayID)
			if err != nil {
				return false, err
			}
			if exprChanged {
				stmt.Exprs[i].Expr = rewritten
				changed = true
			}
		}
		for i, expr := range stmt.DistinctOn {
			rewritten, exprChanged, err := rewriteExprTypeReferences(expr, oldTypeID, oldArrayID, newTypeID, newArrayID)
			if err != nil {
				return false, err
			}
			if exprChanged {
				stmt.DistinctOn[i] = rewritten
				changed = true
			}
		}
		if stmt.Where != nil {
			rewritten, exprChanged, err := rewriteExprTypeReferences(stmt.Where.Expr, oldTypeID, oldArrayID, newTypeID, newArrayID)
			if err != nil {
				return false, err
			}
			if exprChanged {
				stmt.Where.Expr = rewritten
				changed = true
			}
		}
		for i, expr := range stmt.GroupBy {
			rewritten, exprChanged, err := rewriteExprTypeReferences(expr, oldTypeID, oldArrayID, newTypeID, newArrayID)
			if err != nil {
				return false, err
			}
			if exprChanged {
				stmt.GroupBy[i] = rewritten
				changed = true
			}
		}
		if stmt.Having != nil {
			rewritten, exprChanged, err := rewriteExprTypeReferences(stmt.Having.Expr, oldTypeID, oldArrayID, newTypeID, newArrayID)
			if err != nil {
				return false, err
			}
			if exprChanged {
				stmt.Having.Expr = rewritten
				changed = true
			}
		}
	case *tree.ValuesClause:
		for rowIdx := range stmt.Rows {
			for exprIdx, expr := range stmt.Rows[rowIdx] {
				rewritten, exprChanged, err := rewriteExprTypeReferences(expr, oldTypeID, oldArrayID, newTypeID, newArrayID)
				if err != nil {
					return false, err
				}
				if exprChanged {
					stmt.Rows[rowIdx][exprIdx] = rewritten
					changed = true
				}
			}
		}
	case *tree.ParenSelect:
		return rewriteSelectTypeReferences(stmt.Select, oldTypeID, oldArrayID, newTypeID, newArrayID)
	case *tree.UnionClause:
		leftChanged, err := rewriteSelectTypeReferences(stmt.Left, oldTypeID, oldArrayID, newTypeID, newArrayID)
		if err != nil {
			return false, err
		}
		rightChanged, err := rewriteSelectTypeReferences(stmt.Right, oldTypeID, oldArrayID, newTypeID, newArrayID)
		if err != nil {
			return false, err
		}
		changed = leftChanged || rightChanged
	}
	return changed, nil
}

func rewriteExprTypeReferences(expr tree.Expr, oldTypeID id.Type, oldArrayID id.Type, newTypeID id.Type, newArrayID id.Type) (tree.Expr, bool, error) {
	if expr == nil {
		return nil, false, nil
	}
	changed := false
	rewritten, err := tree.SimpleVisit(expr, func(expr tree.Expr) (bool, tree.Expr, error) {
		switch typedExpr := expr.(type) {
		case *tree.CastExpr:
			rewritten, typeChanged, err := rewriteTypeReference(typedExpr.Type, oldTypeID, oldArrayID, newTypeID, newArrayID)
			if err != nil || !typeChanged {
				return true, expr, err
			}
			rewrittenCast := *typedExpr
			rewrittenCast.Type = rewritten
			changed = true
			return true, &rewrittenCast, nil
		case *tree.IsOfTypeExpr:
			rewrittenTypes := make([]tree.ResolvableTypeReference, len(typedExpr.Types))
			copy(rewrittenTypes, typedExpr.Types)
			typeListChanged := false
			for i, typ := range rewrittenTypes {
				rewritten, typeChanged, err := rewriteTypeReference(typ, oldTypeID, oldArrayID, newTypeID, newArrayID)
				if err != nil {
					return false, expr, err
				}
				if typeChanged {
					rewrittenTypes[i] = rewritten
					typeListChanged = true
				}
			}
			if !typeListChanged {
				return true, expr, nil
			}
			rewrittenIsOf := *typedExpr
			rewrittenIsOf.Types = rewrittenTypes
			changed = true
			return true, &rewrittenIsOf, nil
		default:
			return true, expr, nil
		}
	})
	return rewritten, changed, err
}

func rewriteExprCompositeAttributeReferences(expr tree.Expr, typeID id.Type, oldAttr string, newAttr string, compositeColumns map[string]struct{}) (tree.Expr, bool, error) {
	if expr == nil {
		return nil, false, nil
	}
	changed := false
	rewritten, err := tree.SimpleVisit(expr, func(expr tree.Expr) (bool, tree.Expr, error) {
		typedExpr, ok := expr.(*tree.ColumnAccessExpr)
		if !ok || typedExpr.ByIndex || !strings.EqualFold(typedExpr.ColName, oldAttr) {
			return true, expr, nil
		}
		if !exprReferencesCompositeAttributeTarget(typedExpr.Expr, typeID, compositeColumns) {
			return true, expr, nil
		}
		rewrittenAccess := *typedExpr
		rewrittenAccess.ColName = newAttr
		changed = true
		return true, &rewrittenAccess, nil
	})
	return rewritten, changed, err
}

func exprReferencesCompositeAttributeTarget(expr tree.Expr, typeID id.Type, compositeColumns map[string]struct{}) bool {
	if expr == nil {
		return false
	}
	found := false
	_, _ = tree.SimpleVisit(expr, func(expr tree.Expr) (bool, tree.Expr, error) {
		switch typedExpr := expr.(type) {
		case *tree.CastExpr:
			if typeReferenceMatches(typedExpr.Type, typeID) {
				found = true
				return false, expr, nil
			}
		case *tree.AnnotateTypeExpr:
			if typeReferenceMatches(typedExpr.Type, typeID) {
				found = true
				return false, expr, nil
			}
		case *tree.UnresolvedName:
			if unresolvedColumnIsComposite(typedExpr.Parts[0], compositeColumns) {
				found = true
				return false, expr, nil
			}
		case *tree.ColumnItem:
			if unresolvedColumnIsComposite(string(typedExpr.ColumnName), compositeColumns) {
				found = true
				return false, expr, nil
			}
		}
		return !found, expr, nil
	})
	return found
}

func unresolvedColumnIsComposite(columnName string, compositeColumns map[string]struct{}) bool {
	if columnName == "" || len(compositeColumns) == 0 {
		return false
	}
	for column := range compositeColumns {
		if strings.EqualFold(column, columnName) {
			return true
		}
	}
	return false
}

func rewriteTypeReference(ref tree.ResolvableTypeReference, oldTypeID id.Type, oldArrayID id.Type, newTypeID id.Type, newArrayID id.Type) (tree.ResolvableTypeReference, bool, error) {
	switch ref := ref.(type) {
	case *tree.UnresolvedObjectName:
		return rewriteUnresolvedTypeReference(ref, oldTypeID, oldArrayID, newTypeID, newArrayID)
	case *tree.ArrayTypeReference:
		rewritten, changed, err := rewriteTypeReference(ref.ElementType, oldTypeID, oldArrayID, newTypeID, newArrayID)
		if err != nil || !changed {
			return ref, changed, err
		}
		rewrittenArray := *ref
		rewrittenArray.ElementType = rewritten
		return &rewrittenArray, true, nil
	case *tree.TypeReferenceWithModifiers:
		rewritten, changed, err := rewriteTypeReference(ref.Type, oldTypeID, oldArrayID, newTypeID, newArrayID)
		if err != nil || !changed {
			return ref, changed, err
		}
		rewrittenWithModifiers := *ref
		rewrittenWithModifiers.Type = rewritten
		return &rewrittenWithModifiers, true, nil
	default:
		return ref, false, nil
	}
}

func rewriteUnresolvedTypeReference(ref *tree.UnresolvedObjectName, oldTypeID id.Type, oldArrayID id.Type, newTypeID id.Type, newArrayID id.Type) (tree.ResolvableTypeReference, bool, error) {
	replacement := id.NullType
	switch {
	case unresolvedTypeReferenceMatches(ref, oldTypeID):
		replacement = newTypeID
	case oldArrayID.IsValid() && unresolvedTypeReferenceMatches(ref, oldArrayID):
		replacement = newArrayID
	default:
		return ref, false, nil
	}
	parts := [3]string{replacement.TypeName(), "", ""}
	numParts := 1
	if replacement.SchemaName() != "" {
		parts[1] = replacement.SchemaName()
		numParts = 2
	}
	if ref.HasExplicitCatalog() {
		parts[2] = ref.Catalog()
		numParts = 3
	}
	rewritten, err := tree.NewUnresolvedObjectName(numParts, parts, 0)
	if err != nil {
		return nil, false, err
	}
	return rewritten, true, nil
}

func typeReferenceMatches(ref tree.ResolvableTypeReference, typeID id.Type) bool {
	switch ref := ref.(type) {
	case *tree.UnresolvedObjectName:
		return unresolvedTypeReferenceMatches(ref, typeID)
	case *tree.ArrayTypeReference:
		return typeReferenceMatches(ref.ElementType, typeID)
	case *tree.TypeReferenceWithModifiers:
		return typeReferenceMatches(ref.Type, typeID)
	default:
		return false
	}
}

func unresolvedTypeReferenceMatches(ref *tree.UnresolvedObjectName, typeID id.Type) bool {
	if !strings.EqualFold(ref.Object(), typeID.TypeName()) {
		return false
	}
	return !ref.HasExplicitSchema() || strings.EqualFold(ref.Schema(), typeID.SchemaName())
}
