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

package indexmetadata

import (
	"fmt"
	"strings"
	"unicode"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/postgres/parser/lex"
	"github.com/dolthub/doltgresql/server/tablemetadata"
)

// DisplayName returns the PostgreSQL-facing name for index.
func DisplayName(idx sql.Index) string {
	if strings.EqualFold(idx.ID(), "PRIMARY") {
		return fmt.Sprintf("%s_pkey", idx.Table())
	}

	switch idx.(type) {
	case *index.BranchNameIndex, *index.CommitIndex:
		return fmt.Sprintf("%s_%s_key", idx.Table(), idx.ID())
	}

	return core.DecodePhysicalIndexName(idx.ID())
}

// DisplayNameForTable returns the PostgreSQL-facing name for index using
// table-level Doltgres metadata when the native index cannot carry it.
func DisplayNameForTable(idx sql.Index, table sql.Table) string {
	if strings.EqualFold(idx.ID(), "PRIMARY") {
		if commentedTable, ok := table.(sql.CommentedTable); ok {
			if name := tablemetadata.PrimaryKeyConstraintName(commentedTable.Comment()); name != "" {
				return name
			}
		}
	}
	return DisplayName(idx)
}

// Definition returns a PostgreSQL CREATE INDEX definition for index.
func Definition(index sql.Index, schema string) string {
	return DefinitionForSchema(index, schema, nil)
}

// DefinitionForSchema returns a PostgreSQL CREATE INDEX definition for index,
// using tableSchema to deparse hidden functional-index columns when available.
func DefinitionForSchema(index sql.Index, schema string, tableSchema sql.Schema) string {
	return definitionForSchema(index, schema, tableSchema, DisplayName(index), index.Comment())
}

// DefinitionForTable returns a PostgreSQL CREATE INDEX definition for index,
// using table-level metadata for PostgreSQL-facing names when needed.
func DefinitionForTable(index sql.Index, schema string, table sql.Table, tableSchema sql.Schema) string {
	return definitionForSchema(index, schema, tableSchema, DisplayNameForTable(index, table), CommentForTable(index, table))
}

// CommentForTable returns PostgreSQL index metadata for index, using table
// metadata for the native primary-key index because Dolt does not store that
// index in the secondary index collection.
func CommentForTable(idx sql.Index, table sql.Table) string {
	if strings.EqualFold(idx.ID(), "PRIMARY") {
		if commentedTable, ok := table.(sql.CommentedTable); ok {
			if comment := tablemetadata.PrimaryKeyIndexComment(commentedTable.Comment()); comment != "" {
				return comment
			}
		}
	}
	return idx.Comment()
}

func definitionForSchema(index sql.Index, schema string, tableSchema sql.Schema, displayName string, comment string) string {
	unique := ""
	if IsUnique(index) {
		unique = " UNIQUE"
	}
	definition := fmt.Sprintf("CREATE%s INDEX %s ON %s.%s USING %s (%s)",
		unique,
		quoteIdentifier(displayName),
		quoteIdentifier(schema),
		quoteIdentifier(index.Table()),
		AccessMethod(index.IndexType(), comment),
		strings.Join(ColumnDefinitionsForSchema(index, tableSchema), ", "),
	)
	if includeColumns := IncludeColumns(comment); len(includeColumns) > 0 {
		definition += " INCLUDE (" + strings.Join(quoteIdentifiers(includeColumns), ", ") + ")"
	}
	if NullsNotDistinct(comment) {
		definition += " NULLS NOT DISTINCT"
	}
	if relOptions := relOptionsDefinition(comment); relOptions != "" {
		definition += " WITH (" + relOptions + ")"
	}
	if predicate := Predicate(comment); predicate != "" {
		definition += " WHERE " + predicate
	}
	return definition
}

// LogicalColumn describes the PostgreSQL-facing indexed column or expression
// alongside the physical schema column that backs it, when one exists.
type LogicalColumn struct {
	Definition  string
	StorageName string
	Expression  bool
}

// LogicalColumns returns PostgreSQL-facing indexed columns or expressions.
func LogicalColumns(index sql.Index, tableSchema sql.Schema) []LogicalColumn {
	cols := Columns(index.Comment())
	if len(cols) > 0 {
		storageColumns := StorageColumns(index.Comment())
		expressionColumns := ExpressionColumns(index.Comment())
		logical := make([]LogicalColumn, len(cols))
		for i, col := range cols {
			storageName := col
			if i < len(storageColumns) && storageColumns[i] != "" {
				storageName = storageColumns[i]
			}
			expression := false
			if i < len(expressionColumns) {
				expression = expressionColumns[i]
			}
			logical[i] = LogicalColumn{
				Definition:  col,
				StorageName: storageName,
				Expression:  expression,
			}
		}
		return logical
	}

	exprs := index.Expressions()
	logical := make([]LogicalColumn, len(exprs))
	for i, expr := range exprs {
		storageName := unqualifiedIndexExpression(expr)
		definition := core.DecodePhysicalColumnName(storageName)
		if citextColumn, ok := lowerCitextLogicalColumn(tableSchema, storageName); ok {
			logical[i] = LogicalColumn{
				Definition:  citextColumn.Name,
				StorageName: citextColumn.Name,
			}
			continue
		}
		if col, ok := schemaColumn(tableSchema, storageName); ok {
			storageName = col.Name
			definition = core.DecodePhysicalColumnName(col.Name)
			if col.HiddenSystem && col.Generated != nil && col.Generated.Expr != nil {
				if citextColumn, ok := hiddenLowerCitextLogicalColumn(tableSchema, col); ok {
					logical[i] = LogicalColumn{
						Definition:  citextColumn.Name,
						StorageName: citextColumn.Name,
					}
					continue
				}
				definition = unqualifiedIndexExpression(col.Generated.Expr.String())
				logical[i] = LogicalColumn{
					Definition:  definition,
					StorageName: storageName,
					Expression:  true,
				}
				continue
			}
		}
		logical[i] = LogicalColumn{
			Definition:  definition,
			StorageName: storageName,
			Expression:  schemaIndexOfColName(tableSchema, storageName) < 0,
		}
	}
	return logical
}

// OpClassesForSchema returns preserved opclasses, plus narrow inferred
// opclasses for physical hidden-key indexes that cannot carry index comments
// (currently CREATE TABLE citext primary keys).
func OpClassesForSchema(index sql.Index, tableSchema sql.Schema) []string {
	opClasses := OpClasses(index.Comment())
	if len(opClasses) > 0 {
		return opClasses
	}
	if AccessMethod(index.IndexType(), index.Comment()) != AccessMethodBtree || tableSchema == nil {
		return nil
	}
	exprs := index.Expressions()
	inferred := make([]string, len(exprs))
	found := false
	for i, expr := range exprs {
		storageName := unqualifiedIndexExpression(expr)
		if _, ok := lowerCitextLogicalColumn(tableSchema, storageName); ok {
			inferred[i] = OpClassCitextOps
			found = true
			continue
		}
		storageColumn, ok := schemaColumn(tableSchema, storageName)
		if !ok {
			continue
		}
		if _, ok := hiddenLowerCitextLogicalColumn(tableSchema, storageColumn); ok {
			inferred[i] = OpClassCitextOps
			found = true
		}
	}
	if !found {
		return nil
	}
	return inferred
}

// ColumnDefinitions returns PostgreSQL-facing indexed column definitions,
// including any opclass metadata preserved by Doltgres.
func ColumnDefinitions(index sql.Index) []string {
	return ColumnDefinitionsForSchema(index, nil)
}

// ColumnDefinitionsForSchema returns PostgreSQL-facing indexed column
// definitions, including any opclass metadata preserved by Doltgres.
func ColumnDefinitionsForSchema(index sql.Index, tableSchema sql.Schema) []string {
	logicalColumns := LogicalColumns(index, tableSchema)
	cols := make([]string, len(logicalColumns))
	for i, col := range logicalColumns {
		cols[i] = col.Definition
		if !col.Expression {
			cols[i] = quoteIdentifier(cols[i])
		}
	}

	collations := Collations(index.Comment())
	opClasses := OpClassesForSchema(index, tableSchema)
	sortOptions := SortOptions(index.Comment())
	for i := range cols {
		if i < len(collations) && collations[i] != "" {
			cols[i] += " " + columnCollationDefinition(collations[i])
		}
		if i < len(opClasses) && opClasses[i] != "" {
			cols[i] += " " + opClasses[i]
		}
		if i < len(sortOptions) {
			if optionDef := columnOptionDefinition(sortOptions[i]); optionDef != "" {
				cols[i] += " " + optionDef
			}
		}
	}
	return cols
}

// AttributeDefinitionsForSchema returns PostgreSQL-facing index attributes,
// including non-key INCLUDE columns after the key column definitions.
func AttributeDefinitionsForSchema(index sql.Index, tableSchema sql.Schema) []string {
	keyColumns := ColumnDefinitionsForSchema(index, tableSchema)
	includeColumns := IncludeColumns(index.Comment())
	if len(includeColumns) == 0 {
		return keyColumns
	}
	attributes := make([]string, 0, len(keyColumns)+len(includeColumns))
	attributes = append(attributes, keyColumns...)
	attributes = append(attributes, quoteIdentifiers(includeColumns)...)
	return attributes
}

func quoteIdentifiers(identifiers []string) []string {
	quoted := make([]string, len(identifiers))
	for i, identifier := range identifiers {
		quoted[i] = quoteIdentifier(identifier)
	}
	return quoted
}

func quoteIdentifier(identifier string) string {
	if canUseBareIdentifier(identifier) {
		return identifier
	}
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

func canUseBareIdentifier(identifier string) bool {
	if len(identifier) == 0 {
		return false
	}
	for i, ch := range identifier {
		if i == 0 {
			if ch != '_' && (ch < 'a' || ch > 'z') {
				return false
			}
			continue
		}
		if ch != '_' && ch != '$' && (ch < 'a' || ch > 'z') && (ch < '0' || ch > '9') {
			return false
		}
	}
	category, ok := lex.KeywordsCategories[identifier]
	return !ok || category != "R"
}

func hiddenLowerCitextLogicalColumn(tableSchema sql.Schema, column *sql.Column) (*sql.Column, bool) {
	if column == nil || !column.HiddenSystem || column.Generated == nil || column.Generated.Expr == nil {
		return nil, false
	}
	return lowerCitextLogicalColumn(tableSchema, column.Generated.Expr.String())
}

func lowerCitextLogicalColumn(tableSchema sql.Schema, expr string) (*sql.Column, bool) {
	columnName, ok := lowerFunctionColumnName(expr)
	if !ok {
		return nil, false
	}
	logicalColumn, ok := schemaColumn(tableSchema, columnName)
	if !ok {
		return nil, false
	}
	opClass, ok := DefaultBtreeOpClassForType(logicalColumn.Type)
	return logicalColumn, ok && opClass == OpClassCitextOps
}

func lowerFunctionColumnName(expr string) (string, bool) {
	expr = trimEnclosingParens(unqualifiedIndexExpression(expr))
	if len(expr) < len("lower()") || !strings.HasPrefix(strings.ToLower(expr), "lower(") || !strings.HasSuffix(expr, ")") {
		return "", false
	}
	arg := strings.TrimSpace(expr[len("lower(") : len(expr)-1])
	arg = trimEnclosingParens(arg)
	if arg == "" || strings.ContainsAny(arg, " \t\n\r,()+-*/") {
		return "", false
	}
	return arg, true
}

// ExpressionDefinitions returns the expressions stored in pg_index.indexprs.
func ExpressionDefinitions(index sql.Index, tableSchema sql.Schema) []string {
	logicalColumns := LogicalColumns(index, tableSchema)
	exprs := make([]string, 0, len(logicalColumns))
	for _, col := range logicalColumns {
		if col.Expression {
			exprs = append(exprs, col.Definition)
		}
	}
	return exprs
}

func unqualifiedIndexExpression(expr string) string {
	expr = strings.TrimSpace(expr)
	expr = strings.ReplaceAll(expr, "`", "")
	expr = strings.ReplaceAll(expr, `"`, "")

	var builder strings.Builder
	for i := 0; i < len(expr); {
		if !isIdentifierStart(rune(expr[i])) {
			builder.WriteByte(expr[i])
			i++
			continue
		}

		start := i
		i = scanIdentifier(expr, i)
		lastStart := start
		lastEnd := i
		for i < len(expr) && expr[i] == '.' {
			nextStart := i + 1
			if nextStart >= len(expr) || !isIdentifierStart(rune(expr[nextStart])) {
				break
			}
			i = scanIdentifier(expr, nextStart)
			lastStart = nextStart
			lastEnd = i
		}
		builder.WriteString(expr[lastStart:lastEnd])
	}
	return trimEnclosingParens(builder.String())
}

func schemaColumn(schema sql.Schema, name string) (*sql.Column, bool) {
	idx := schemaIndexOfColName(schema, name)
	if idx < 0 {
		return nil, false
	}
	return schema[idx], true
}

func schemaIndexOfColName(schema sql.Schema, name string) int {
	if schema == nil {
		return -1
	}
	return schema.IndexOfColName(name)
}

func scanIdentifier(expr string, start int) int {
	i := start
	for i < len(expr) && isIdentifierPart(rune(expr[i])) {
		i++
	}
	return i
}

func isIdentifierStart(ch rune) bool {
	return ch == '_' || ch == '!' || unicode.IsLetter(ch)
}

func isIdentifierPart(ch rune) bool {
	return isIdentifierStart(ch) || unicode.IsDigit(ch)
}

func trimEnclosingParens(expr string) string {
	expr = strings.TrimSpace(expr)
	if len(expr) < 2 || expr[0] != '(' || expr[len(expr)-1] != ')' {
		return expr
	}
	depth := 0
	for i, ch := range expr {
		switch ch {
		case '(':
			depth++
		case ')':
			depth--
			if depth < 0 {
				return expr
			}
			if depth == 0 && i < len(expr)-1 {
				return expr
			}
		}
	}
	if depth != 0 {
		return expr
	}
	return strings.TrimSpace(expr[1 : len(expr)-1])
}

func columnOptionDefinition(option IndexColumnOption) string {
	var parts []string
	if option.Direction == SortDirectionDesc {
		parts = append(parts, "DESC")
	}
	if option.NullsOrder == NullsOrderFirst && option.Direction != SortDirectionDesc {
		parts = append(parts, "NULLS FIRST")
	}
	if option.NullsOrder == NullsOrderLast && option.Direction == SortDirectionDesc {
		parts = append(parts, "NULLS LAST")
	}
	return strings.Join(parts, " ")
}

func relOptionsDefinition(comment string) string {
	relOptions := RelOptions(comment)
	if len(relOptions) == 0 {
		return ""
	}
	parts := make([]string, len(relOptions))
	for i, option := range relOptions {
		key, value, ok := strings.Cut(option, "=")
		if !ok {
			parts[i] = option
			continue
		}
		parts[i] = strings.TrimSpace(key) + "='" + strings.ReplaceAll(strings.TrimSpace(value), "'", "''") + "'"
	}
	return strings.Join(parts, ", ")
}

func columnCollationDefinition(collation string) string {
	collation = NormalizeCollation(collation)
	if collation == "" {
		return ""
	}
	return `COLLATE "` + strings.ReplaceAll(collation, `"`, `""`) + `"`
}
