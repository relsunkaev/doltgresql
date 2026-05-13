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
	"fmt"
	"hash/fnv"
	"regexp"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	gmsexpression "github.com/dolthub/go-mysql-server/sql/expression"
	gmsfunction "github.com/dolthub/go-mysql-server/sql/expression/function"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	pgexpression "github.com/dolthub/doltgresql/server/expression"
	"github.com/dolthub/doltgresql/server/indexmetadata"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// validateCreateTable validates that a table can be created as specified
func validateCreateTable(ctx *sql.Context, a *analyzer.Analyzer, n sql.Node, scope *plan.Scope, sel analyzer.RuleSelector, qFlags *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	ct, ok := n.(*plan.CreateTable)
	if !ok {
		return n, transform.SameTree, nil
	}

	err := validateIdentifiers(ct)
	if err != nil {
		return nil, transform.SameTree, err
	}

	sch := ct.PkSchema().Schema
	idxs := ct.Indexes()
	err = validateIndexes(ctx, sch, idxs)
	if err != nil {
		return nil, transform.SameTree, err
	}

	ct, changed, err := normalizeCitextCreateTableIndexes(ctx, ct, sch)
	if err != nil {
		return nil, transform.SameTree, err
	}
	if changed {
		return ct, transform.NewTree, nil
	}

	if err = validateCreateTableForeignKeyReferencePrivileges(ctx, a, ct); err != nil {
		return nil, transform.SameTree, err
	}

	return n, transform.SameTree, nil
}

func validateCreateTableForeignKeyReferencePrivileges(ctx *sql.Context, a *analyzer.Analyzer, ct *plan.CreateTable) error {
	for _, fkDef := range ct.ForeignKeys() {
		if fkDef.IsSelfReferential() {
			continue
		}
		parentTable, _, err := a.Catalog.TableSchema(ctx, fkDef.ParentDatabase, fkDef.ParentSchema, fkDef.ParentTable)
		if err != nil {
			return err
		}
		parentColumns, ok := canonicalForeignKeyParentColumns(ctx, parentTable, fkDef.ParentColumns)
		if !ok {
			continue
		}
		fkCopy := *fkDef
		fkCopy.ParentColumns = parentColumns
		if schemaTable, ok := parentTable.(sql.DatabaseSchemaTable); ok {
			fkCopy.ParentSchema = schemaTable.DatabaseSchema().SchemaName()
		}
		if err = validateForeignKeyReferencePrivileges(ctx, fkCopy); err != nil {
			return err
		}
	}
	return nil
}

func canonicalForeignKeyParentColumns(ctx *sql.Context, parentTable sql.Table, parentColumns []string) ([]string, bool) {
	if len(parentColumns) == 0 {
		return parentColumns, true
	}
	parentSchema := parentTable.Schema(ctx)
	columnNames := make(map[string]string, len(parentSchema))
	for _, column := range parentSchema {
		columnNames[strings.ToLower(column.Name)] = column.Name
	}
	canonicalColumns := make([]string, len(parentColumns))
	for i, parentColumn := range parentColumns {
		columnName, ok := columnNames[strings.ToLower(parentColumn)]
		if !ok {
			return nil, false
		}
		canonicalColumns[i] = columnName
	}
	return canonicalColumns, true
}

// validateIdentifiers validates the names of all schema elements for validity
// TODO: we use 64 character as the max length for an identifier, postgres uses 63
func validateIdentifiers(ct *plan.CreateTable) error {
	err := analyzer.ValidateIdentifier(ct.Name())
	if err != nil {
		return err
	}

	colNames := make(map[string]bool)
	for _, col := range ct.PkSchema().Schema {
		err = analyzer.ValidateIdentifier(col.Name)
		if err != nil {
			return err
		}
		if colNames[col.Name] {
			return sql.ErrDuplicateColumn.New(col.Name)
		}
		colNames[col.Name] = true
	}

	for _, chDef := range ct.Checks() {
		err = analyzer.ValidateIdentifier(chDef.Name)
		if err != nil {
			return err
		}
	}

	for _, idxDef := range ct.Indexes() {
		err = analyzer.ValidateIdentifier(idxDef.Name)
		if err != nil {
			return err
		}
	}

	for _, fkDef := range ct.ForeignKeys() {
		err = analyzer.ValidateIdentifier(fkDef.Name)
		if err != nil {
			return err
		}
	}

	return nil
}

// validateIndexes validates that the index definitions being created are valid
func validateIndexes(ctx *sql.Context, sch sql.Schema, idxDefs sql.IndexDefs) error {
	colMap := schToColMap(sch)
	for _, idxDef := range idxDefs {
		if err := validateIndex(ctx, colMap, idxDef); err != nil {
			return err
		}
	}

	return nil
}

// schToColMap returns a map of columns, keyed by their name, for the specified
// schema |sch|.
func schToColMap(sch sql.Schema) map[string]*sql.Column {
	colMap := make(map[string]*sql.Column, len(sch))
	for _, col := range sch {
		colMap[strings.ToLower(col.Name)] = col
	}
	return colMap
}

// validateIndex ensures that the Index Definition is valid for the table schema.
// This function will throw errors and warnings as needed.
// All columns in the index must be:
//   - in the schema
//   - not duplicated
//   - a compatible type for an index
//
// TODO: there are other constraints on indexes that we could enforce and are not yet (e.g. JSON as an index)
func validateIndex(ctx *sql.Context, colMap map[string]*sql.Column, idxDef *sql.IndexDef) error {
	if err := validateIndexExpressionClasses(ctx, idxDef); err != nil {
		return err
	}

	seenCols := make(map[string]struct{})
	for _, idxCol := range idxDef.Columns {
		if idxCol.Expression != nil {
			continue
		}

		schCol, exists := colMap[strings.ToLower(idxCol.Name)]
		if !exists {
			return sql.ErrKeyColumnDoesNotExist.New(idxCol.Name)
		}
		if _, ok := seenCols[schCol.Name]; ok {
			return sql.ErrDuplicateColumn.New(schCol.Name)
		}
		seenCols[schCol.Name] = struct{}{}
		if idxDef.IsFullText() {
			continue
		}
	}

	if idxDef.IsSpatial() {
		return errors.Errorf("spatial indexes are not supported")
	}

	if err := validateBtreeOpClassTypes(colMap, idxDef); err != nil {
		return err
	}

	for _, includeColumn := range indexmetadata.IncludeColumns(idxDef.Comment) {
		schCol, exists := colMap[strings.ToLower(includeColumn)]
		if !exists || schCol.HiddenSystem {
			return sql.ErrKeyColumnDoesNotExist.New(includeColumn)
		}
	}

	for _, predicateColumn := range indexmetadata.PredicateColumns(idxDef.Comment) {
		schCol, exists := colMap[strings.ToLower(predicateColumn)]
		if !exists || schCol.HiddenSystem {
			return sql.ErrKeyColumnDoesNotExist.New(predicateColumn)
		}
	}

	return nil
}

func validateIndexExpressionClasses(ctx *sql.Context, idxDef *sql.IndexDef) error {
	for _, idxCol := range idxDef.Columns {
		if idxCol.Expression == nil {
			continue
		}
		if err := validateIndexExpressionClass(ctx, idxCol.Expression, "index expression", "index expressions"); err != nil {
			return err
		}
	}

	metadata, ok := indexmetadata.DecodeComment(idxDef.Comment)
	if !ok || strings.TrimSpace(metadata.Predicate) == "" {
		return nil
	}
	return validateIndexPredicateText(ctx, metadata.Predicate)
}

func validateIndexExpressionClass(ctx *sql.Context, expr sql.Expression, singular string, plural string) error {
	if expr == nil {
		return nil
	}
	if err := validateIndexExpressionText(ctx, expr.String(), singular, plural); err != nil {
		return err
	}
	var err error
	sql.Inspect(ctx, expr, func(ctx *sql.Context, e sql.Expression) bool {
		switch e.(type) {
		case *plan.Subquery:
			err = errors.Errorf("cannot use subquery in %s", singular)
			return false
		case sql.Aggregation:
			err = errors.Errorf("aggregate functions are not allowed in %s", plural)
			return false
		case sql.WindowAggregation:
			err = errors.Errorf("window functions are not allowed in %s", plural)
			return false
		}
		if windowExpr, ok := e.(sql.WindowAdaptableExpression); ok && windowExpr.Window() != nil {
			err = errors.Errorf("window functions are not allowed in %s", plural)
			return false
		}
		if rowIterExpr, ok := e.(sql.RowIterExpression); ok && rowIterExpr.ReturnsRowIter() {
			err = errors.Errorf("set-returning functions are not allowed in %s", plural)
			return false
		}
		if fn, ok := e.(sql.FunctionExpression); ok {
			if functionErr := validateIndexFunctionName(ctx, fn.FunctionName(), singular, plural); functionErr != nil {
				err = functionErr
				return false
			}
		}
		if nondeterministic, ok := e.(sql.NonDeterministicExpression); ok && nondeterministic.IsNonDeterministic() {
			err = errors.Errorf("functions in %s must be marked IMMUTABLE", singular)
			return false
		}
		return true
	})
	return err
}

func validateIndexPredicateText(ctx *sql.Context, predicate string) error {
	lower := strings.ToLower(predicate)
	if strings.Contains(lower, "select ") || strings.Contains(lower, "(select") {
		return errors.New("cannot use subquery in index predicate")
	}
	return validateIndexExpressionText(ctx, predicate, "index predicate", "index predicates")
}

func validateIndexExpressionText(ctx *sql.Context, expr string, singular string, plural string) error {
	lower := strings.ToLower(expr)
	if strings.Contains(lower, " over (") {
		return errors.Errorf("window functions are not allowed in %s", plural)
	}
	for _, name := range indexAggregateFunctions {
		if containsFunctionCall(lower, name) {
			return errors.Errorf("aggregate functions are not allowed in %s", plural)
		}
	}
	for _, name := range indexWindowFunctions {
		if containsFunctionCall(lower, name) {
			return errors.Errorf("window functions are not allowed in %s", plural)
		}
	}
	for _, name := range indexSetReturningFunctions {
		if containsFunctionCall(lower, name) {
			return errors.Errorf("set-returning functions are not allowed in %s", plural)
		}
	}
	for _, name := range indexVolatileFunctions {
		if containsFunctionCall(lower, name) {
			return errors.Errorf("functions in %s must be marked IMMUTABLE", singular)
		}
	}
	for _, name := range indexFunctionCalls(lower) {
		if err := validateIndexFunctionName(ctx, name, singular, plural); err != nil {
			return err
		}
	}
	return nil
}

func validateIndexFunctionName(ctx *sql.Context, name string, singular string, plural string) error {
	lower := strings.ToLower(name)
	switch {
	case stringInList(lower, indexAggregateFunctions):
		return errors.Errorf("aggregate functions are not allowed in %s", plural)
	case stringInList(lower, indexWindowFunctions):
		return errors.Errorf("window functions are not allowed in %s", plural)
	case stringInList(lower, indexSetReturningFunctions):
		return errors.Errorf("set-returning functions are not allowed in %s", plural)
	case stringInList(lower, indexVolatileFunctions):
		return errors.Errorf("functions in %s must be marked IMMUTABLE", singular)
	}
	if indexFunctionNameIsMutable(ctx, lower) {
		return errors.Errorf("functions in %s must be marked IMMUTABLE", singular)
	}
	return nil
}

func indexFunctionNameIsMutable(ctx *sql.Context, name string) bool {
	funcCollection, err := core.GetFunctionsCollectionFromContext(ctx)
	if err != nil {
		return false
	}
	schemas := []string{"pg_catalog"}
	if currentSchema, err := core.GetCurrentSchema(ctx); err == nil && currentSchema != "" && !strings.EqualFold(currentSchema, "pg_catalog") {
		schemas = append([]string{currentSchema}, schemas...)
	}
	for _, schema := range schemas {
		overloads, err := funcCollection.GetFunctionOverloads(ctx, id.NewFunction(schema, name))
		if err != nil || len(overloads) == 0 {
			continue
		}
		for _, overload := range overloads {
			if overload.Volatility != "" {
				if overload.Volatility != "i" {
					return true
				}
				continue
			}
			if overload.IsNonDeterministic {
				return true
			}
		}
		return false
	}
	return false
}

func indexFunctionCalls(expr string) []string {
	matches := indexFunctionCallPattern.FindAllStringSubmatch(expr, -1)
	if len(matches) == 0 {
		return nil
	}
	names := make([]string, 0, len(matches))
	seen := make(map[string]struct{}, len(matches))
	for _, match := range matches {
		if len(match) < 2 {
			continue
		}
		name := strings.ToLower(match[1])
		if _, ok := indexExpressionKeywords[name]; ok {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}
	return names
}

var indexFunctionCallPattern = regexp.MustCompile(`(?i)([a-z_][a-z0-9_$]*)\s*\(`)

var indexExpressionKeywords = map[string]struct{}{
	"case":     {},
	"cast":     {},
	"coalesce": {},
	"extract":  {},
	"greatest": {},
	"least":    {},
	"nullif":   {},
}

var indexAggregateFunctions = []string{
	"avg",
	"bit_and",
	"bit_or",
	"bit_xor",
	"bool_and",
	"bool_or",
	"count",
	"every",
	"json_agg",
	"json_object_agg",
	"max",
	"min",
	"sum",
}

var indexWindowFunctions = []string{
	"cume_dist",
	"dense_rank",
	"first_value",
	"lag",
	"last_value",
	"lead",
	"ntile",
	"percent_rank",
	"rank",
	"row_number",
}

var indexSetReturningFunctions = []string{
	"generate_series",
	"json_array_elements",
	"json_array_elements_text",
	"jsonb_array_elements",
	"jsonb_array_elements_text",
	"regexp_matches",
	"regexp_split_to_table",
	"string_to_table",
}

var indexVolatileFunctions = []string{
	"clock_timestamp",
	"current_timestamp",
	"localtimestamp",
	"now",
	"random",
	"timeofday",
	"transaction_timestamp",
}

func validateBtreeOpClassTypes(colMap map[string]*sql.Column, idxDef *sql.IndexDef) error {
	metadata, ok := indexmetadata.DecodeComment(idxDef.Comment)
	if !ok || indexmetadata.NormalizeAccessMethod(metadata.AccessMethod) != indexmetadata.AccessMethodBtree {
		return nil
	}

	for i, opClass := range metadata.OpClasses {
		opClass = indexmetadata.NormalizeOpClass(opClass)
		if opClass == "" || i >= len(idxDef.Columns) || idxDef.Columns[i].Expression != nil {
			continue
		}
		schCol, exists := colMap[strings.ToLower(idxDef.Columns[i].Name)]
		if !exists {
			continue
		}
		typeName, ok := indexmetadata.BtreeOpClassAcceptsType(opClass, schCol.Type)
		if !ok {
			return errors.Errorf(`operator class "%s" does not accept data type %s`, opClass, typeName)
		}
	}
	return nil
}

// resolveAlterColumn is a validation rule that validates the schema changes in an ALTER TABLE statement and updates
// the nodes with necessary intermediate / update schema information
func resolveAlterColumn(ctx *sql.Context, a *analyzer.Analyzer, n sql.Node, scope *plan.Scope, sel analyzer.RuleSelector, qFlags *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	if !analyzer.FlagIsSet(qFlags, sql.QFlagAlterTable) {
		return n, transform.SameTree, nil
	}

	var sch sql.Schema
	var indexes []string
	var validator sql.SchemaValidator
	keyedColumns := make(map[string]bool)
	var err error
	transform.Inspect(n, func(n sql.Node) bool {
		if st, ok := n.(sql.SchemaTarget); ok {
			sch = st.TargetSchema()
		}
		switch n := n.(type) {
		case *plan.ModifyColumn:
			if rt, ok := n.Table.(*plan.ResolvedTable); ok {
				if sv, ok := rt.UnwrappedDatabase().(sql.SchemaValidator); ok {
					validator = sv
				}
			}
			keyedColumns, err = analyzer.GetTableIndexColumns(ctx, n.Table)
			return false
		case *plan.RenameColumn:
			if rt, ok := n.Table.(*plan.ResolvedTable); ok {
				if sv, ok := rt.UnwrappedDatabase().(sql.SchemaValidator); ok {
					validator = sv
				}
			}
			return false
		case *plan.AddColumn:
			if rt, ok := n.Table.(*plan.ResolvedTable); ok {
				if sv, ok := rt.UnwrappedDatabase().(sql.SchemaValidator); ok {
					validator = sv
				}
			}
			keyedColumns, err = analyzer.GetTableIndexColumns(ctx, n.Table)
			return false
		case *plan.DropColumn:
			if rt, ok := n.Table.(*plan.ResolvedTable); ok {
				if sv, ok := rt.UnwrappedDatabase().(sql.SchemaValidator); ok {
					validator = sv
				}
			}
			return false
		case *plan.AlterIndex:
			if rt, ok := n.Table.(*plan.ResolvedTable); ok {
				if sv, ok := rt.UnwrappedDatabase().(sql.SchemaValidator); ok {
					validator = sv
				}
			}
			indexes, err = analyzer.GetTableIndexNames(ctx, a, n.Table)
		default:
		}
		return true
	})

	if err != nil {
		return nil, transform.SameTree, err
	}

	// Skip this validation if we didn't find one or more of the above node types
	if len(sch) == 0 {
		return n, transform.SameTree, nil
	}

	sch = sch.Copy() // Make a copy of the original schema to deal with any references to the original table.
	initialSch := sch

	// Need a TransformUp here because multiple of these statement types can be nested under a Block node.
	// It doesn't look it, but this is actually an iterative loop over all the independent clauses in an ALTER statement
	n, same, err := transform.Node(ctx, n, func(ctx *sql.Context, n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		switch nn := n.(type) {
		case *plan.ModifyColumn:
			n, err := nn.WithTargetSchema(sch.Copy())
			if err != nil {
				return nil, transform.SameTree, err
			}

			sch, err = analyzer.ValidateModifyColumn(ctx, initialSch, sch, n.(*plan.ModifyColumn), keyedColumns)
			if err != nil {
				return nil, transform.SameTree, err
			}
			return n, transform.NewTree, nil
		case *plan.RenameColumn:
			n, err := nn.WithTargetSchema(sch.Copy())
			if err != nil {
				return nil, transform.SameTree, err
			}
			sch, err = validateRenameColumn(ctx, initialSch, sch, n.(*plan.RenameColumn))
			if err != nil {
				return nil, transform.SameTree, err
			}
			return n, transform.NewTree, nil
		case *plan.AddColumn:
			n, err := nn.WithTargetSchema(sch.Copy())
			if err != nil {
				return nil, transform.SameTree, err
			}

			sch, err = analyzer.ValidateAddColumn(ctx, sch, n.(*plan.AddColumn))
			if err != nil {
				return nil, transform.SameTree, err
			}

			return n, transform.NewTree, nil
		case *plan.DropColumn:
			n, err := nn.WithTargetSchema(sch.Copy())
			if err != nil {
				return nil, transform.SameTree, err
			}
			sch, err = analyzer.ValidateDropColumn(ctx, initialSch, sch, n.(*plan.DropColumn))
			if err != nil {
				return nil, transform.SameTree, err
			}
			delete(keyedColumns, nn.Column)

			return n, transform.NewTree, nil
		case *plan.AlterIndex:
			n, err := nn.WithTargetSchema(sch.Copy())
			if err != nil {
				return nil, transform.SameTree, err
			}
			alterIndex := n.(*plan.AlterIndex)
			indexes, err = validateAlterIndex(ctx, initialSch, sch, alterIndex, indexes)
			if err != nil {
				return nil, transform.SameTree, err
			}
			n, sch, err = normalizeCitextAlterIndex(ctx, alterIndex, sch)
			if err != nil {
				return nil, transform.SameTree, err
			}
			if normalizedAlterIndex, ok := n.(*plan.AlterIndex); ok {
				n, sch, err = normalizeNullableSortOptionAlterIndex(ctx, normalizedAlterIndex, sch)
				if err != nil {
					return nil, transform.SameTree, err
				}
			}

			keyedColumns = updateKeyedColumnsForAlterIndexNode(keyedColumns, n)
			return n, transform.NewTree, nil
		case *plan.AlterPK:
			n, err := nn.WithTargetSchema(sch.Copy())
			if err != nil {
				return nil, transform.SameTree, err
			}
			sch, err = validatePrimaryKey(ctx, initialSch, sch, n.(*plan.AlterPK))
			if err != nil {
				return nil, transform.SameTree, err
			}
			return n, transform.NewTree, nil
		case *plan.AlterDefaultSet:
			n, err := nn.WithTargetSchema(sch.Copy())
			if err != nil {
				return nil, transform.SameTree, err
			}
			sch, err = analyzer.ValidateAlterDefault(ctx, initialSch, sch, n.(*plan.AlterDefaultSet))
			if err != nil {
				return nil, transform.SameTree, err
			}
			return n, transform.NewTree, nil
		case *plan.AlterDefaultDrop:
			n, err := nn.WithTargetSchema(sch.Copy())
			if err != nil {
				return nil, transform.SameTree, err
			}
			sch, err = analyzer.ValidateDropDefault(ctx, initialSch, sch, n.(*plan.AlterDefaultDrop))
			if err != nil {
				return nil, transform.SameTree, err
			}
			return n, transform.NewTree, nil
		}
		return n, transform.SameTree, nil
	})

	if err != nil {
		return nil, transform.SameTree, err
	}

	if validator != nil {
		if err := validator.ValidateSchema(sch); err != nil {
			return nil, transform.SameTree, err
		}
	}

	return n, same, nil
}

func validateRenameColumn(_ *sql.Context, initialSch, sch sql.Schema, rc *plan.RenameColumn) (sql.Schema, error) {
	nameable := rc.Table.(sql.Nameable)

	if err := analyzer.ValidateIdentifier(rc.NewColumnName); err != nil {
		return nil, err
	}

	if sch.Contains(rc.NewColumnName, nameable.Name()) {
		return nil, sql.ErrColumnExists.New(rc.NewColumnName)
	}

	if !initialSch.Contains(rc.ColumnName, nameable.Name()) || !sch.Contains(rc.ColumnName, nameable.Name()) {
		return nil, sql.ErrTableColumnNotFound.New(nameable.Name(), rc.ColumnName)
	}

	return renameInSchema(sch, rc.ColumnName, rc.NewColumnName, nameable.Name()), nil
}

func renameInSchema(sch sql.Schema, oldColumnName, newColumnName, tableName string) sql.Schema {
	idx := sch.IndexOf(oldColumnName, tableName)
	schCopy := make(sql.Schema, len(sch))
	for i := range sch {
		columnCopy := *sch[i]
		if i == idx {
			columnCopy.Name = newColumnName
		}
		schCopy[i] = &columnCopy
	}
	return schCopy
}

// Returns the underlying table name for the node given
func getTableName(node sql.Node) string {
	var tableName string
	transform.Inspect(node, func(node sql.Node) bool {
		switch node := node.(type) {
		case *plan.TableAlias:
			tableName = node.Name()
			return false
		case *plan.ResolvedTable:
			tableName = node.Name()
			return false
		case *plan.UnresolvedTable:
			tableName = node.Name()
			return false
		case *plan.IndexedTableAccess:
			tableName = node.Name()
			return false
		}
		return true
	})

	return tableName
}

// validatePrimaryKey validates a primary key add or drop operation.
func validatePrimaryKey(ctx *sql.Context, initialSch, sch sql.Schema, ai *plan.AlterPK) (sql.Schema, error) {
	tableName := getTableName(ai.Table)
	switch ai.Action {
	case plan.PrimaryKeyAction_Create:
		if analyzer.HasPrimaryKeys(sch) {
			return nil, sql.ErrMultiplePrimaryKeysDefined.New()
		}

		colMap := schToColMap(sch)
		idxDef := &sql.IndexDef{
			Name:       "PRIMARY",
			Columns:    ai.Columns,
			Constraint: sql.IndexConstraint_Primary,
		}

		err := validateIndex(ctx, colMap, idxDef)
		if err != nil {
			return nil, err
		}

		for _, idxCol := range ai.Columns {
			schCol := colMap[strings.ToLower(idxCol.Name)]
			if schCol.Virtual {
				return nil, sql.ErrVirtualColumnPrimaryKey.New()
			}
		}

		// Set the primary keys
		for _, col := range ai.Columns {
			sch[sch.IndexOf(col.Name, tableName)].PrimaryKey = true
		}

		return sch, nil
	case plan.PrimaryKeyAction_Drop:
		if !analyzer.HasPrimaryKeys(sch) {
			return nil, sql.ErrCantDropFieldOrKey.New("PRIMARY")
		}

		for _, col := range sch {
			if col.PrimaryKey {
				col.PrimaryKey = false
			}
		}

		return sch, nil
	default:
		return sch, nil
	}
}

func normalizeCitextAlterIndex(ctx *sql.Context, ai *plan.AlterIndex, sch sql.Schema) (sql.Node, sql.Schema, error) {
	if ai.Action != plan.IndexAction_Create ||
		ai.Constraint == sql.IndexConstraint_Primary {
		return ai, sch, nil
	}

	columns, comment, changed := normalizeCitextIndexDef(ctx, ai.Columns, ai.Using, ai.Constraint, ai.Comment, sch, false)
	if !changed {
		return ai, sch, nil
	}

	normalized := *ai
	normalized.Columns = columns
	normalized.Comment = comment
	if len(columns) == 1 {
		return &normalized, sch, nil
	}

	resolvedTable, ok := ai.Table.(*plan.ResolvedTable)
	if !ok {
		return nil, sch, errors.Errorf("alter index: table is not a resolved table: %T", ai.Table)
	}

	normalizedSchema := sch.Copy()
	nodes := make([]sql.Node, 0, len(columns)+1)
	for columnIndex, column := range columns {
		if column.Expression == nil {
			continue
		}
		hiddenColumn, err := newHiddenSystemAlterIndexColumn(ctx, ai, normalizedSchema, columnIndex, column.Expression)
		if err != nil {
			return nil, sch, err
		}
		addColumn := plan.NewAddColumnResolved(resolvedTable, *hiddenColumn, nil)
		addNode, err := addColumn.WithTargetSchema(normalizedSchema.Copy())
		if err != nil {
			return nil, sch, err
		}
		nodes = append(nodes, addNode)
		normalizedSchema = append(normalizedSchema, hiddenColumn)
		columns[columnIndex].Name = hiddenColumn.Name
		columns[columnIndex].Expression = nil
	}

	normalized.Columns = columns
	alterNode, err := (&normalized).WithTargetSchema(normalizedSchema.Copy())
	if err != nil {
		return nil, sch, err
	}
	nodes = append(nodes, alterNode)
	block := plan.NewBlock(nodes)
	block.SetSchema(ai.Schema(ctx))
	return block, normalizedSchema, nil
}

func normalizeNullableSortOptionAlterIndex(ctx *sql.Context, ai *plan.AlterIndex, sch sql.Schema) (sql.Node, sql.Schema, error) {
	if ai.Action != plan.IndexAction_Create ||
		ai.Constraint == sql.IndexConstraint_Primary ||
		(ai.Using != sql.IndexUsing_Default && ai.Using != sql.IndexUsing_BTree) {
		return ai, sch, nil
	}

	metadata, ok := indexmetadata.DecodeComment(ai.Comment)
	if !ok || indexmetadata.NormalizeAccessMethod(metadata.AccessMethod) != indexmetadata.AccessMethodBtree {
		return ai, sch, nil
	}
	if len(metadata.SortOptions) == 0 || len(ai.Columns) == 0 {
		return ai, sch, nil
	}

	columns := append([]sql.IndexColumn(nil), ai.Columns...)
	metadata.Columns = ensureStringMetadataLength(metadata.Columns, len(columns))
	metadata.StorageColumns = ensureStringMetadataLength(metadata.StorageColumns, len(columns))
	metadata.ExpressionColumns = ensureBoolMetadataLength(metadata.ExpressionColumns, len(columns))
	metadata.SortOptions = ensureIndexColumnOptionMetadataLength(metadata.SortOptions, len(columns))

	probeColumnIndexes := make(map[int]int)
	for i, column := range columns {
		if column.Expression != nil || column.Name == "" {
			return ai, sch, nil
		}
		if metadata.Columns[i] == "" {
			metadata.Columns[i] = column.Name
		}
		if metadata.StorageColumns[i] == "" {
			metadata.StorageColumns[i] = column.Name
		}
		columnIndex := sch.IndexOfColName(column.Name)
		if columnIndex < 0 {
			return ai, sch, nil
		}
		if !sch[columnIndex].Nullable || !nullableSortOptionNeedsNullProbe(metadata.SortOptions[i]) {
			continue
		}
		probeColumnIndexes[i] = columnIndex
	}
	if len(probeColumnIndexes) == 0 {
		return ai, sch, nil
	}

	resolvedTable, ok := ai.Table.(*plan.ResolvedTable)
	if !ok {
		return nil, sch, errors.Errorf("alter index: table is not a resolved table: %T", ai.Table)
	}

	normalizedSchema := sch.Copy()
	normalizedColumns := make([]sql.IndexColumn, 0, len(columns)+len(probeColumnIndexes))
	nodes := make([]sql.Node, 0, len(probeColumnIndexes)+1)
	for logicalIndex, column := range columns {
		if columnIndex, ok := probeColumnIndexes[logicalIndex]; ok {
			expr := pgexpression.NewIsNull(gmsexpression.NewGetField(columnIndex, sch[columnIndex].Type, sch[columnIndex].Name, sch[columnIndex].Nullable))
			hiddenColumn, err := newHiddenSystemNullableSortIndexColumn(ctx, ai, normalizedSchema, len(normalizedColumns), expr)
			if err != nil {
				return nil, sch, err
			}
			addColumn := plan.NewAddColumnResolved(resolvedTable, *hiddenColumn, nil)
			addNode, err := addColumn.WithTargetSchema(normalizedSchema.Copy())
			if err != nil {
				return nil, sch, err
			}
			nodes = append(nodes, addNode)
			normalizedSchema = append(normalizedSchema, hiddenColumn)
			normalizedColumns = append(normalizedColumns, sql.IndexColumn{Name: hiddenColumn.Name})
		}
		normalizedColumns = append(normalizedColumns, column)
	}

	metadata.AccessMethod = indexmetadata.AccessMethodBtree
	normalized := *ai
	normalized.Columns = normalizedColumns
	normalized.Comment = indexmetadata.EncodeComment(metadata)
	alterNode, err := (&normalized).WithTargetSchema(normalizedSchema.Copy())
	if err != nil {
		return nil, sch, err
	}
	nodes = append(nodes, alterNode)
	block := plan.NewBlock(nodes)
	block.SetSchema(ai.Schema(ctx))
	return block, normalizedSchema, nil
}

func nullableSortOptionNeedsNullProbe(option indexmetadata.IndexColumnOption) bool {
	direction := strings.ToLower(strings.TrimSpace(option.Direction))
	nullsOrder := strings.ToLower(strings.TrimSpace(option.NullsOrder))
	if direction == "" && nullsOrder == "" {
		return false
	}
	return !((direction == "" && nullsOrder == indexmetadata.NullsOrderFirst) ||
		(direction == indexmetadata.SortDirectionDesc && nullsOrder == indexmetadata.NullsOrderLast))
}

func newHiddenSystemNullableSortIndexColumn(ctx *sql.Context, ai *plan.AlterIndex, sch sql.Schema, columnIndex int, expr sql.Expression) (*sql.Column, error) {
	databaseName := ""
	if ai.Db != nil {
		databaseName = ai.Db.Name()
	}
	columnDefaultValue, err := sql.NewColumnDefaultValue(expr, expr.Type(ctx), false, true, true)
	if err != nil {
		return nil, err
	}
	hiddenColumnName := hiddenSystemNullableSortIndexColumnName(sch, ai.IndexName, columnIndex)
	return &sql.Column{
		Type:           expr.Type(ctx),
		Generated:      columnDefaultValue,
		Name:           hiddenColumnName,
		Source:         ai.Table.Name(),
		DatabaseSource: databaseName,
		Nullable:       false,
		Virtual:        true,
		HiddenSystem:   true,
	}, nil
}

func hiddenSystemNullableSortIndexColumnName(sch sql.Schema, indexName string, columnIndex int) string {
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(strings.ToLower(indexName)))
	baseName := fmt.Sprintf("__doltgres_nullsort_%08x_%d", hasher.Sum32(), columnIndex)
	name := baseName
	for suffix := 1; sch.IndexOfColName(name) >= 0; suffix++ {
		name = fmt.Sprintf("%s_%d", baseName, suffix)
	}
	return name
}

func normalizeCitextCreateTableIndexes(ctx *sql.Context, ct *plan.CreateTable, sch sql.Schema) (*plan.CreateTable, bool, error) {
	if ct.Like() != nil {
		return ct, false, nil
	}
	idxDefs := ct.Indexes()
	normalizedIdxDefs := make(sql.IndexDefs, 0, len(idxDefs)+1)
	normalizedSchema := ct.PkSchema().Schema.Copy()
	changed := false
	hasPrimaryIndexDef := false
	for _, idxDef := range idxDefs {
		if idxDef == nil {
			normalizedIdxDefs = append(normalizedIdxDefs, nil)
			continue
		}
		normalized := *idxDef
		if normalized.Constraint == sql.IndexConstraint_Primary {
			hasPrimaryIndexDef = true
			if normalized.Name == "" {
				normalized.Name = defaultPrimaryKeyIndexName(ct)
			}
		}
		if normalized.Name == "" {
			normalizedIdxDefs = append(normalizedIdxDefs, &normalized)
			continue
		}
		originalColumns := append([]sql.IndexColumn(nil), normalized.Columns...)
		columns, comment, indexChanged := normalizeCitextIndexDef(ctx, normalized.Columns, normalized.Storage, normalized.Constraint, normalized.Comment, sch, true)
		if indexChanged {
			for columnIndex, column := range columns {
				if column.Expression == nil {
					continue
				}
				hiddenColumn, err := newHiddenSystemIndexColumn(ctx, ct, normalizedSchema, normalized.Name, columnIndex, column.Expression, normalized.Constraint == sql.IndexConstraint_Primary)
				if err != nil {
					return nil, false, err
				}
				if normalized.Constraint == sql.IndexConstraint_Primary {
					clearPrimaryKeyForIndexColumn(normalizedSchema, originalColumns[columnIndex])
				}
				normalizedSchema = append(normalizedSchema, hiddenColumn)
				columns[columnIndex].Name = hiddenColumn.Name
				columns[columnIndex].Expression = nil
			}
			normalized.Columns = columns
			normalized.Comment = comment
			changed = true
		}
		normalizedIdxDefs = append(normalizedIdxDefs, &normalized)
	}
	if !hasPrimaryIndexDef {
		if primaryColumn, ok := singleCitextPrimaryKeyColumn(sch); ok {
			primaryIndex := &sql.IndexDef{
				Name:       defaultPrimaryKeyIndexName(ct),
				Storage:    sql.IndexUsing_Default,
				Constraint: sql.IndexConstraint_Primary,
				Columns: []sql.IndexColumn{{
					Name: primaryColumn.Name,
				}},
			}
			originalColumns := append([]sql.IndexColumn(nil), primaryIndex.Columns...)
			columns, comment, indexChanged := normalizeCitextIndexDef(ctx, primaryIndex.Columns, primaryIndex.Storage, primaryIndex.Constraint, primaryIndex.Comment, sch, true)
			if indexChanged {
				for columnIndex, column := range columns {
					if column.Expression == nil {
						continue
					}
					hiddenColumn, err := newHiddenSystemIndexColumn(ctx, ct, normalizedSchema, primaryIndex.Name, columnIndex, column.Expression, true)
					if err != nil {
						return nil, false, err
					}
					clearPrimaryKeyForIndexColumn(normalizedSchema, originalColumns[columnIndex])
					normalizedSchema = append(normalizedSchema, hiddenColumn)
					columns[columnIndex].Name = hiddenColumn.Name
					columns[columnIndex].Expression = nil
				}
				primaryIndex.Columns = columns
				primaryIndex.Comment = comment
				normalizedIdxDefs = append(normalizedIdxDefs, primaryIndex)
				changed = true
			}
		}
	}
	if !changed {
		return ct, false, nil
	}
	tableSpec := &plan.TableSpec{
		Schema:    sql.NewPrimaryKeySchema(normalizedSchema),
		FkDefs:    ct.ForeignKeys(),
		ChDefs:    ct.Checks(),
		IdxDefs:   normalizedIdxDefs,
		Collation: ct.Collation,
		TableOpts: ct.TableOpts,
	}
	if ct.Select() != nil {
		return plan.NewCreateTableSelect(ct.Db, ct.Name(), ct.IfNotExists(), ct.Temporary(), ct.Select(), tableSpec), true, nil
	}
	return plan.NewCreateTable(ct.Db, ct.Name(), ct.IfNotExists(), ct.Temporary(), tableSpec), true, nil
}

func defaultPrimaryKeyIndexName(ct *plan.CreateTable) string {
	return ct.Name() + "_pkey"
}

func singleCitextPrimaryKeyColumn(sch sql.Schema) (*sql.Column, bool) {
	var primaryColumn *sql.Column
	for _, column := range sch {
		if !column.PrimaryKey {
			continue
		}
		if primaryColumn != nil {
			return nil, false
		}
		primaryColumn = column
	}
	if primaryColumn == nil || !isCitextSchemaColumn(primaryColumn) {
		return nil, false
	}
	return primaryColumn, true
}

func clearPrimaryKeyForIndexColumn(sch sql.Schema, column sql.IndexColumn) {
	if column.Name == "" {
		return
	}
	columnIndex := sch.IndexOfColName(column.Name)
	if columnIndex >= 0 {
		sch[columnIndex].PrimaryKey = false
	}
}

func newHiddenSystemIndexColumn(ctx *sql.Context, ct *plan.CreateTable, sch sql.Schema, indexName string, columnIndex int, expr sql.Expression, primaryKey bool) (*sql.Column, error) {
	databaseName := ""
	if ct.Db != nil {
		databaseName = ct.Db.Name()
	}
	return newHiddenSystemIndexColumnForTable(ctx, ct.Name(), databaseName, sch, indexName, columnIndex, expr, primaryKey)
}

func newHiddenSystemAlterIndexColumn(ctx *sql.Context, ai *plan.AlterIndex, sch sql.Schema, columnIndex int, expr sql.Expression) (*sql.Column, error) {
	databaseName := ""
	if ai.Db != nil {
		databaseName = ai.Db.Name()
	}
	return newHiddenSystemIndexColumnForTable(ctx, ai.Table.Name(), databaseName, sch, ai.IndexName, columnIndex, expr, false)
}

func newHiddenSystemIndexColumnForTable(ctx *sql.Context, tableName string, databaseName string, sch sql.Schema, indexName string, columnIndex int, expr sql.Expression, primaryKey bool) (*sql.Column, error) {
	columnDefaultValue, err := sql.NewColumnDefaultValue(expr, expr.Type(ctx), false, true, true)
	if err != nil {
		return nil, err
	}
	hiddenColumnName := hiddenSystemIndexColumnName(sch, indexName, columnIndex)
	return &sql.Column{
		Type:           expr.Type(ctx),
		Generated:      columnDefaultValue,
		Name:           hiddenColumnName,
		Source:         tableName,
		DatabaseSource: databaseName,
		PrimaryKey:     primaryKey,
		Nullable:       !primaryKey,
		Virtual:        !primaryKey,
		HiddenSystem:   true,
	}, nil
}

func hiddenSystemIndexColumnName(sch sql.Schema, indexName string, columnIndex int) string {
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(strings.ToLower(indexName)))
	baseName := fmt.Sprintf("__doltgres_citext_%08x_%d", hasher.Sum32(), columnIndex)
	name := baseName
	for suffix := 1; sch.IndexOfColName(name) >= 0; suffix++ {
		name = fmt.Sprintf("%s_%d", baseName, suffix)
	}
	return name
}

func normalizeCitextIndexDef(ctx *sql.Context, indexColumns []sql.IndexColumn, using sql.IndexUsing, constraint sql.IndexConstraint, comment string, sch sql.Schema, allowPrimary bool) ([]sql.IndexColumn, string, bool) {
	if len(indexColumns) == 0 ||
		(using != sql.IndexUsing_Default && using != sql.IndexUsing_BTree) ||
		(constraint == sql.IndexConstraint_Primary && !allowPrimary) ||
		constraint == sql.IndexConstraint_Fulltext ||
		constraint == sql.IndexConstraint_Spatial ||
		constraint == sql.IndexConstraint_Vector {
		return indexColumns, comment, false
	}

	metadata, ok := indexmetadata.DecodeComment(comment)
	if !ok {
		metadata = indexmetadata.Metadata{AccessMethod: indexmetadata.AccessMethodBtree}
	}
	if indexmetadata.NormalizeAccessMethod(metadata.AccessMethod) != indexmetadata.AccessMethodBtree {
		return indexColumns, comment, false
	}

	columns := append([]sql.IndexColumn(nil), indexColumns...)
	metadata.Columns = ensureStringMetadataLength(metadata.Columns, len(columns))
	metadata.OpClasses = ensureStringMetadataLength(metadata.OpClasses, len(columns))

	changed := false
	for i, column := range columns {
		if metadata.Columns[i] == "" && column.Name != "" {
			metadata.Columns[i] = column.Name
		}
		if column.Expression != nil || column.Name == "" {
			continue
		}
		columnIndex := sch.IndexOfColName(column.Name)
		if columnIndex < 0 || !isCitextSchemaColumn(sch[columnIndex]) {
			continue
		}

		// The stored expression is a normalized text key; the index metadata keeps
		// the logical citext column/opclass surface.
		field := gmsexpression.NewGetField(columnIndex, pgtypes.Text, sch[columnIndex].Name, sch[columnIndex].Nullable)
		columns[i].Name = ""
		columns[i].Expression = gmsfunction.NewLower(ctx, field)
		metadata.Columns[i] = column.Name
		if metadata.OpClasses[i] == "" {
			metadata.OpClasses[i] = indexmetadata.OpClassCitextOps
		}
		changed = true
	}
	if !changed {
		return indexColumns, comment, false
	}

	metadata.AccessMethod = indexmetadata.AccessMethodBtree
	return columns, indexmetadata.EncodeComment(metadata), true
}

func updateKeyedColumnsForAlterIndexNode(keyedColumns map[string]bool, n sql.Node) map[string]bool {
	if alterIndex, ok := n.(*plan.AlterIndex); ok {
		return analyzer.UpdateKeyedColumns(keyedColumns, alterIndex)
	}
	block, ok := n.(*plan.Block)
	if !ok {
		return keyedColumns
	}
	for _, child := range block.Children() {
		if alterIndex, ok := child.(*plan.AlterIndex); ok {
			keyedColumns = analyzer.UpdateKeyedColumns(keyedColumns, alterIndex)
		}
	}
	return keyedColumns
}

func ensureStringMetadataLength(values []string, length int) []string {
	if len(values) >= length {
		return values
	}
	next := make([]string, length)
	copy(next, values)
	return next
}

func ensureBoolMetadataLength(values []bool, length int) []bool {
	if len(values) >= length {
		return values
	}
	next := make([]bool, length)
	copy(next, values)
	return next
}

func ensureIndexColumnOptionMetadataLength(values []indexmetadata.IndexColumnOption, length int) []indexmetadata.IndexColumnOption {
	if len(values) >= length {
		return values
	}
	next := make([]indexmetadata.IndexColumnOption, length)
	copy(next, values)
	return next
}

func isCitextSchemaColumn(column *sql.Column) bool {
	doltgresType, ok := column.Type.(*pgtypes.DoltgresType)
	return ok && doltgresType.ID.TypeName() == "citext"
}

// validateAlterIndex validates the specified column can have an index added, dropped, or renamed. Returns an updated
// list of index name given the add, drop, or rename operations.
func validateAlterIndex(ctx *sql.Context, initialSch, sch sql.Schema, ai *plan.AlterIndex, indexes []string) ([]string, error) {
	switch ai.Action {
	case plan.IndexAction_Create:
		err := analyzer.ValidateIdentifier(ai.IndexName)
		if err != nil {
			return nil, err
		}
		colMap := schToColMap(sch)

		// TODO: plan.AlterIndex should just have a sql.IndexDef
		indexDef := &sql.IndexDef{
			Name:       ai.IndexName,
			Columns:    ai.Columns,
			Constraint: ai.Constraint,
			Storage:    ai.Using,
			Comment:    ai.Comment,
		}

		err = validateIndex(ctx, colMap, indexDef)
		if err != nil {
			return nil, err
		}
		return append(indexes, ai.IndexName), nil
	case plan.IndexAction_Drop:
		savedIdx := -1
		for i, idx := range indexes {
			if strings.EqualFold(idx, ai.IndexName) {
				savedIdx = i
				break
			}
		}
		if savedIdx == -1 {
			return nil, sql.ErrCantDropFieldOrKey.New(ai.IndexName)
		}
		// Remove the index from the list
		return append(indexes[:savedIdx], indexes[savedIdx+1:]...), nil
	case plan.IndexAction_Rename:
		err := analyzer.ValidateIdentifier(ai.IndexName)
		if err != nil {
			return nil, err
		}
		savedIdx := -1
		for i, idx := range indexes {
			if strings.EqualFold(idx, ai.PreviousIndexName) {
				savedIdx = i
			}
		}
		if savedIdx == -1 {
			return nil, sql.ErrCantDropFieldOrKey.New(ai.IndexName)
		}
		// Simulate the rename by deleting the old name and adding the new one.
		return append(append(indexes[:savedIdx], indexes[savedIdx+1:]...), ai.IndexName), nil
	}

	return indexes, nil
}
