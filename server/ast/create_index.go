// Copyright 2023 Dolthub, Inc.
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

package ast

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
	"github.com/dolthub/doltgresql/postgres/parser/sem/tree"
	"github.com/dolthub/doltgresql/server/indexmetadata"
	pgnodes "github.com/dolthub/doltgresql/server/node"
)

// nodeCreateIndex handles *tree.CreateIndex nodes.
func nodeCreateIndex(ctx *Context, node *tree.CreateIndex) (vitess.Statement, error) {
	if node == nil {
		return nil, nil
	}
	// CONCURRENTLY for supported btree indexes is routed through the
	// two-phase state-machine node below so external sessions can observe
	// the in-progress build via pg_index.indisready/indisvalid.
	accessMethod := indexmetadata.NormalizeAccessMethod(node.Using)
	if node.Concurrently && accessMethod == indexmetadata.AccessMethodGist {
		return nil, errors.Errorf("index method %s is not yet supported", node.Using)
	}
	if accessMethod != indexmetadata.AccessMethodBtree &&
		accessMethod != indexmetadata.AccessMethodGin &&
		accessMethod != indexmetadata.AccessMethodGist {
		return nil, errors.Errorf("index method %s is not yet supported", node.Using)
	}
	if node.Predicate != nil {
		if accessMethod != indexmetadata.AccessMethodBtree {
			return nil, errors.Errorf("partial %s indexes are not yet supported", accessMethod)
		}
	}
	tableName, err := nodeTableName(ctx, &node.Table)
	if err != nil {
		return nil, err
	}
	metadata, err := nodeIndexMetadata(node, accessMethod)
	if err != nil {
		return nil, err
	}
	indexName := string(node.Name)
	if indexName == "" {
		indexName = defaultCreateIndexName(tableName.Name.String(), node.Columns)
	}
	indexName = core.EncodePhysicalIndexName(indexName)
	indexDef, err := nodeIndexTableDefAllowingStorageParams(ctx, &tree.IndexTableDef{
		Name:        tree.Name(indexName),
		Columns:     node.Columns,
		IndexParams: node.IndexParams,
	})
	if err != nil {
		return nil, err
	}
	needsMetadataBackedBtree := requiresMetadataBackedBtreeIndex(node.Columns)
	if accessMethod == indexmetadata.AccessMethodBtree &&
		(needsMetadataBackedBtree || (!node.Unique && node.Concurrently && hasIndexExpression(node.Columns))) {
		if node.Unique {
			return nil, errors.Errorf("unique mixed expression indexes are not yet supported")
		}
		// GMS can only build a real functional index for one expression with no other columns.
		// Preserve PostgreSQL-facing shape in metadata and use ordinary columns for Dolt storage.
		if metadata == nil {
			metadata = &indexmetadata.Metadata{}
		}
		if err = applyLogicalIndexMetadata(metadata, node.Columns); err != nil {
			return nil, err
		}
		indexDef.Fields, err = metadataBackedBtreeIndexFields(node.Columns)
		if err != nil {
			return nil, err
		}
	}
	if accessMethod == indexmetadata.AccessMethodGin {
		if node.Unique {
			return nil, errors.Errorf("unique gin indexes are not yet supported")
		}
		if len(indexDef.Fields) != 1 {
			return nil, errors.Errorf("multi-column gin indexes are not yet supported")
		}
		if indexDef.Fields[0].Expression != nil || indexDef.Fields[0].Column.IsEmpty() {
			return nil, errors.Errorf("expression gin indexes are not yet supported")
		}
		return vitess.InjectedStatement{
			Statement: pgnodes.NewCreateJsonbGinIndex(
				node.IfNotExists,
				node.Concurrently,
				tableName.SchemaQualifier.String(),
				tableName.Name.String(),
				indexDef.Info.Name.String(),
				indexDef.Fields[0].Column.String(),
				metadata.OpClasses[0],
			),
		}, nil
	}
	if node.Unique && node.Predicate != nil {
		if !node.NullsDistinct {
			return nil, errors.Errorf("NULLS NOT DISTINCT partial unique indexes are not yet supported")
		}
		if hasIndexExpression(node.Columns) {
			return nil, errors.Errorf("unique partial expression indexes are not yet supported")
		}
		if metadata == nil {
			metadata = &indexmetadata.Metadata{}
		}
		metadata.Unique = true
		metadata.Constraint = indexmetadata.ConstraintNone
		columns := indexFieldsToIndexColumns(indexDef.Fields)
		return vitess.InjectedStatement{
			Statement: pgnodes.NewCreatePartialUniqueIndex(
				node.IfNotExists,
				node.Concurrently,
				tableName.SchemaQualifier.String(),
				tableName.Name.String(),
				indexDef.Info.Name.String(),
				columns,
				*metadata,
			),
		}, nil
	}
	if node.Unique {
		if !node.NullsDistinct && hasIndexExpression(node.Columns) {
			return nil, errors.Errorf("NULLS NOT DISTINCT expression indexes are not yet supported")
		}
		if metadata == nil {
			metadata = &indexmetadata.Metadata{}
		}
		metadata.Constraint = indexmetadata.ConstraintNone
	}
	if node.Unique && metadata != nil && metadata.NullsNotDistinct && !node.Concurrently {
		columns := indexFieldsToIndexColumns(indexDef.Fields)
		return vitess.InjectedStatement{
			Statement: pgnodes.NewCreateNullsNotDistinctUniqueIndex(
				node.IfNotExists,
				false,
				false,
				tableName.SchemaQualifier.String(),
				tableName.Name.String(),
				indexDef.Info.Name.String(),
				columns,
				*metadata,
			),
		}, nil
	}
	var indexType string
	if node.Unique {
		indexType = vitess.UniqueStr
	}
	if node.Concurrently && canRouteConcurrentBtree(node, metadata) {
		columns := indexFieldsToIndexColumns(indexDef.Fields)
		baseMetadata := indexmetadata.Metadata{}
		if metadata != nil {
			baseMetadata = *metadata
		}
		createStatement := ""
		if node.Unique && hasIndexExpression(node.Columns) {
			nonConcurrent := *node
			nonConcurrent.Concurrently = false
			nonConcurrent.Name = tree.Name(indexName)
			createStatement = tree.AsString(&nonConcurrent)
		}
		return vitess.InjectedStatement{
			Statement: pgnodes.NewCreateIndexConcurrently(
				node.IfNotExists,
				tableName.SchemaQualifier.String(),
				tableName.Name.String(),
				indexDef.Info.Name.String(),
				node.Unique,
				columns,
				baseMetadata,
				createStatement,
			),
		}, nil
	}
	options := indexDef.Options
	var using vitess.ColIdent
	if metadata != nil {
		options = append(options, &vitess.IndexOption{
			Name:  vitess.KeywordString(vitess.COMMENT_KEYWORD),
			Value: vitess.NewStrVal([]byte(indexmetadata.EncodeComment(*metadata))),
		})
		using = vitess.NewColIdent(indexmetadata.AccessMethodBtree)
	}
	return &vitess.AlterTable{
		Table: tableName,
		Statements: []*vitess.DDL{
			{
				Action:      vitess.AlterStr,
				Table:       tableName,
				IfNotExists: node.IfNotExists,
				IndexSpec: &vitess.IndexSpec{
					Action:   vitess.CreateStr,
					FromName: indexDef.Info.Name,
					ToName:   indexDef.Info.Name,
					Type:     indexType,
					Using:    using,
					Fields:   indexDef.Fields,
					Options:  options,
				},
			},
		},
	}, nil
}

// canRouteConcurrentBtree reports whether a CREATE INDEX CONCURRENTLY
// statement should be handled by the two-phase state-machine node. Earlier
// validation rejects unsupported expression combinations before this point.
func canRouteConcurrentBtree(node *tree.CreateIndex, metadata *indexmetadata.Metadata) bool {
	return true
}

// indexFieldsToIndexColumns converts the vitess.IndexField slice the AST
// builder already produced into the sql.IndexColumn shape Dolt's
// IndexAlterableTable.CreateIndex consumes.
func indexFieldsToIndexColumns(fields []*vitess.IndexField) []sql.IndexColumn {
	columns := make([]sql.IndexColumn, 0, len(fields))
	for _, field := range fields {
		if field == nil {
			continue
		}
		columns = append(columns, sql.IndexColumn{
			Name: field.Column.String(),
		})
	}
	return columns
}

func defaultCreateIndexName(tableName string, columns tree.IndexElemList) string {
	parts := make([]string, 0, len(columns)+2)
	parts = append(parts, sanitizeIndexNamePart(tableName, "index"))
	for _, column := range columns {
		part := ""
		if column.Column != "" {
			part = string(column.Column)
		} else if column.Expr != nil {
			part = tree.AsString(column.Expr)
		}
		parts = append(parts, sanitizeIndexNamePart(part, "expr"))
	}
	parts = append(parts, "idx")
	return strings.Join(parts, "_")
}

func sanitizeIndexNamePart(part string, fallback string) string {
	part = strings.TrimSpace(part)
	var builder strings.Builder
	lastWasUnderscore := false
	for _, r := range part {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			builder.WriteRune(unicode.ToLower(r))
			lastWasUnderscore = false
			continue
		}
		if builder.Len() > 0 && !lastWasUnderscore {
			builder.WriteByte('_')
			lastWasUnderscore = true
		}
	}
	sanitized := strings.Trim(builder.String(), "_")
	if sanitized == "" {
		return fallback
	}
	return sanitized
}

func hasIndexExpression(columns tree.IndexElemList) bool {
	for _, column := range columns {
		if column.Expr != nil {
			return true
		}
	}
	return false
}

func nodeIndexMetadata(node *tree.CreateIndex, accessMethod string) (*indexmetadata.Metadata, error) {
	switch accessMethod {
	case indexmetadata.AccessMethodBtree:
		return nodeBtreeIndexMetadata(node)
	case indexmetadata.AccessMethodGist:
		return nodeGistIndexMetadata(node)
	case indexmetadata.AccessMethodGin:
		if len(node.IndexParams.IncludeColumns) > 0 {
			return nil, errors.Errorf("INCLUDE is not yet supported for gin indexes")
		}
		if len(node.IndexParams.StorageParams) > 0 {
			return nil, errors.Errorf("storage parameters are not yet supported for gin indexes")
		}
		opClasses := make([]string, len(node.Columns))
		for i, column := range node.Columns {
			if column.Collation != "" {
				return nil, errors.Errorf("index collation %s is not yet supported for gin indexes", column.Collation)
			}
			opClass := indexmetadata.OpClassJsonbOps
			if column.OpClass != nil {
				if len(column.OpClass.Options) > 0 {
					return nil, errors.Errorf("index operator class options are not yet supported")
				}
				opClass = indexmetadata.NormalizeOpClass(column.OpClass.Name)
			}
			if !indexmetadata.IsSupportedGinJsonbOpClass(opClass) {
				return nil, errors.Errorf("operator class %s is not yet supported for gin indexes", opClass)
			}
			opClasses[i] = opClass
		}
		return &indexmetadata.Metadata{
			AccessMethod: accessMethod,
			OpClasses:    opClasses,
		}, nil
	default:
		return nil, fmt.Errorf("unknown index access method %s", accessMethod)
	}
}

func nodeGistIndexMetadata(node *tree.CreateIndex) (*indexmetadata.Metadata, error) {
	if len(node.IndexParams.IncludeColumns) > 0 {
		return nil, errors.Errorf("INCLUDE is not yet supported for gist indexes")
	}
	if len(node.IndexParams.StorageParams) > 0 {
		return nil, errors.Errorf("storage parameters are not yet supported for gist indexes")
	}
	opClasses := make([]string, len(node.Columns))
	for i, column := range node.Columns {
		if column.Collation != "" {
			return nil, errors.Errorf("index collation %s is not yet supported for gist indexes", column.Collation)
		}
		if column.OpClass != nil {
			if len(column.OpClass.Options) > 0 {
				return nil, errors.Errorf("index operator class options are not yet supported")
			}
			opClasses[i] = indexmetadata.NormalizeOpClass(column.OpClass.Name)
		}
		switch column.Direction {
		case tree.DefaultDirection, tree.Ascending:
		default:
			return nil, errors.Errorf("index sorting direction is not supported for gist indexes")
		}
		switch column.NullsOrder {
		case tree.DefaultNullsOrder:
		default:
			return nil, errors.Errorf("NULL ordering is not supported for gist indexes")
		}
	}
	return &indexmetadata.Metadata{
		AccessMethod: indexmetadata.AccessMethodGist,
		OpClasses:    opClasses,
	}, nil
}

func nodeBtreeIndexMetadata(node *tree.CreateIndex) (*indexmetadata.Metadata, error) {
	collations := make([]string, len(node.Columns))
	opClasses := make([]string, len(node.Columns))
	sortOptions := make([]indexmetadata.IndexColumnOption, len(node.Columns))
	hasMetadata := false
	for i, column := range node.Columns {
		if column.Collation != "" {
			collation := indexmetadata.NormalizeCollation(column.Collation)
			if !indexmetadata.IsSupportedCollation(collation) {
				return nil, errors.Errorf("index collation %s is not yet supported", column.Collation)
			}
			collations[i] = collation
			hasMetadata = true
		}

		if column.OpClass != nil {
			if len(column.OpClass.Options) > 0 {
				return nil, errors.Errorf("index operator class options are not yet supported")
			}
			opClass := indexmetadata.NormalizeOpClass(column.OpClass.Name)
			if !indexmetadata.IsSupportedBtreeOpClass(opClass) {
				return nil, errors.Errorf("operator class %s is not yet supported for btree indexes", opClass)
			}
			opClasses[i] = opClass
			hasMetadata = true
		}

		switch column.Direction {
		case tree.DefaultDirection, tree.Ascending:
		case tree.Descending:
			sortOptions[i].Direction = indexmetadata.SortDirectionDesc
			hasMetadata = true
		default:
			return nil, errors.Errorf("unknown index sorting direction encountered")
		}

		switch column.NullsOrder {
		case tree.DefaultNullsOrder:
		case tree.NullsFirst:
			sortOptions[i].NullsOrder = indexmetadata.NullsOrderFirst
			hasMetadata = true
		case tree.NullsLast:
			sortOptions[i].NullsOrder = indexmetadata.NullsOrderLast
			hasMetadata = true
		default:
			return nil, errors.Errorf("unknown NULL ordering for index")
		}
	}
	relOptions, err := nodeIndexRelOptions(node.IndexParams.StorageParams)
	if err != nil {
		return nil, err
	}
	if len(relOptions) > 0 {
		hasMetadata = true
	}
	includeColumns, err := nodeIndexIncludeColumns(node.IndexParams.IncludeColumns)
	if err != nil {
		return nil, err
	}
	if len(includeColumns) > 0 {
		hasMetadata = true
	}
	predicate, predicateColumns, err := nodeIndexPredicate(node.Predicate)
	if err != nil {
		return nil, err
	}
	if predicate != "" {
		hasMetadata = true
	}
	if node.Unique && !node.NullsDistinct {
		hasMetadata = true
	}
	if !hasMetadata {
		return nil, nil
	}
	return &indexmetadata.Metadata{
		AccessMethod:     indexmetadata.AccessMethodBtree,
		IncludeColumns:   includeColumns,
		Predicate:        predicate,
		PredicateColumns: predicateColumns,
		Collations:       collations,
		OpClasses:        opClasses,
		RelOptions:       relOptions,
		SortOptions:      sortOptions,
		NullsNotDistinct: node.Unique && !node.NullsDistinct,
	}, nil
}

func nodeIndexIncludeColumns(columns tree.IndexElemList) ([]string, error) {
	if len(columns) == 0 {
		return nil, nil
	}
	includeColumns := make([]string, len(columns))
	for i, column := range columns {
		if column.Expr != nil {
			return nil, pgerror.New(pgcode.FeatureNotSupported, "expressions are not supported in included columns")
		}
		if column.Collation != "" {
			return nil, pgerror.New(pgcode.FeatureNotSupported, "including column does not support a collation")
		}
		if column.OpClass != nil {
			return nil, pgerror.New(pgcode.FeatureNotSupported, "including column does not support an operator class")
		}
		switch column.Direction {
		case tree.DefaultDirection:
		default:
			return nil, pgerror.New(pgcode.FeatureNotSupported, "including column does not support ASC/DESC options")
		}
		switch column.NullsOrder {
		case tree.DefaultNullsOrder:
		default:
			return nil, pgerror.New(pgcode.FeatureNotSupported, "including column does not support NULLS FIRST/LAST options")
		}
		includeColumns[i] = string(column.Column)
	}
	return includeColumns, nil
}

func nodeIndexPredicate(predicate tree.Expr) (string, []string, error) {
	if predicate == nil {
		return "", nil, nil
	}
	columns := referencedIndexColumns(predicate)
	return indexPredicateDefinition(predicate), columns, nil
}

func nodeIndexRelOptions(params tree.StorageParams) ([]string, error) {
	if len(params) == 0 {
		return nil, nil
	}
	relOptions := make([]string, 0, len(params))
	seen := make(map[string]struct{}, len(params))
	for _, param := range params {
		key := strings.ToLower(strings.TrimSpace(string(param.Key)))
		if _, ok := seen[key]; ok {
			return nil, pgerror.Newf(pgcode.InvalidParameterValue, "index storage parameter %s is specified more than once", key)
		}
		seen[key] = struct{}{}
		switch key {
		case "fillfactor":
			fillfactor, err := nodeIndexStorageParamInt(param.Value)
			if err != nil {
				return nil, pgerror.New(pgcode.InvalidParameterValue, "fillfactor must be an integer")
			}
			if fillfactor < 10 || fillfactor > 100 {
				return nil, pgerror.New(pgcode.InvalidParameterValue, "fillfactor must be between 10 and 100")
			}
			relOptions = append(relOptions, fmt.Sprintf("fillfactor=%d", fillfactor))
		default:
			return nil, pgerror.Newf(pgcode.InvalidParameterValue, "index storage parameter %s is not yet supported", key)
		}
	}
	return relOptions, nil
}

func nodeIndexStorageParamInt(expr tree.Expr) (int, error) {
	if expr == nil {
		return 0, errors.Errorf("missing value")
	}
	switch v := expr.(type) {
	case *tree.NumVal:
		val, err := v.AsInt64()
		if err != nil {
			return 0, err
		}
		return int(val), nil
	case *tree.DInt:
		return int(*v), nil
	default:
		val, err := strconv.Atoi(strings.Trim(tree.AsString(expr), "'"))
		if err != nil {
			return 0, err
		}
		return val, nil
	}
}

func requiresMetadataBackedBtreeIndex(columns tree.IndexElemList) bool {
	expressionCount := 0
	for _, column := range columns {
		if column.Expr != nil {
			expressionCount++
		}
	}
	return expressionCount > 1 || expressionCount == 1 && len(columns) > 1
}

func applyLogicalIndexMetadata(metadata *indexmetadata.Metadata, columns tree.IndexElemList) error {
	metadata.AccessMethod = indexmetadata.AccessMethodBtree
	metadata.Columns = make([]string, len(columns))
	metadata.StorageColumns = make([]string, len(columns))
	metadata.ExpressionColumns = make([]bool, len(columns))
	for i, column := range columns {
		if column.Expr == nil {
			columnName := string(column.Column)
			metadata.Columns[i] = columnName
			metadata.StorageColumns[i] = columnName
			continue
		}
		storageColumn, err := firstReferencedIndexColumn(column.Expr)
		if err != nil {
			return err
		}
		metadata.Columns[i] = indexExpressionDefinition(column.Expr)
		metadata.StorageColumns[i] = storageColumn
		metadata.ExpressionColumns[i] = true
	}
	return nil
}

func metadataBackedBtreeIndexFields(columns tree.IndexElemList) ([]*vitess.IndexField, error) {
	indexFields := make([]*vitess.IndexField, 0, len(columns))
	seen := make(map[string]struct{}, len(columns))
	for _, column := range columns {
		storageColumn := string(column.Column)
		if column.Expr != nil {
			var err error
			storageColumn, err = firstReferencedIndexColumn(column.Expr)
			if err != nil {
				return nil, err
			}
		}
		storageColumnKey := strings.ToLower(storageColumn)
		if _, ok := seen[storageColumnKey]; ok {
			continue
		}
		seen[storageColumnKey] = struct{}{}
		order := vitess.AscScr
		if column.Direction == tree.Descending {
			order = vitess.DescScr
		}
		indexFields = append(indexFields, &vitess.IndexField{
			Column: vitess.NewColIdent(storageColumn),
			Order:  order,
		})
	}
	return indexFields, nil
}

func firstReferencedIndexColumn(expr tree.Expr) (string, error) {
	visitor := indexColumnReferenceVisitor{
		seen: map[string]struct{}{},
	}
	tree.WalkExprConst(&visitor, expr)
	if len(visitor.names) == 0 {
		return "", errors.Errorf("expression indexes without column references are not yet supported")
	}
	return visitor.names[0], nil
}

func referencedIndexColumns(expr tree.Expr) []string {
	visitor := indexColumnReferenceVisitor{
		seen: map[string]struct{}{},
	}
	tree.WalkExprConst(&visitor, expr)
	return visitor.names
}

type indexColumnReferenceVisitor struct {
	names []string
	seen  map[string]struct{}
}

func (v *indexColumnReferenceVisitor) VisitPre(expr tree.Expr) (bool, tree.Expr) {
	switch expr := expr.(type) {
	case *tree.ColumnItem:
		v.add(expr.Column())
	case *tree.UnresolvedName:
		normalized, err := expr.NormalizeVarName()
		if err != nil {
			return true, expr
		}
		if column, ok := normalized.(*tree.ColumnItem); ok {
			v.add(column.Column())
		}
	}
	return true, expr
}

func (v *indexColumnReferenceVisitor) VisitPost(expr tree.Expr) tree.Expr {
	return expr
}

func (v *indexColumnReferenceVisitor) add(name string) {
	key := strings.ToLower(name)
	if _, ok := v.seen[key]; ok {
		return
	}
	v.seen[key] = struct{}{}
	v.names = append(v.names, name)
}

func indexExpressionDefinition(expr tree.Expr) string {
	return trimIndexExpressionParens(tree.AsString(expr))
}

func indexPredicateDefinition(expr tree.Expr) string {
	predicate := indexExpressionDefinition(expr)
	if predicate == "" {
		return ""
	}
	if isBareColumnPredicate(expr) {
		return predicate
	}
	return "(" + predicate + ")"
}

func isBareColumnPredicate(expr tree.Expr) bool {
	switch e := expr.(type) {
	case *tree.ParenExpr:
		return isBareColumnPredicate(e.Expr)
	case *tree.ColumnItem:
		return e.TableName == nil
	case *tree.UnresolvedName:
		normalized, err := e.NormalizeVarName()
		if err != nil {
			return false
		}
		column, ok := normalized.(*tree.ColumnItem)
		return ok && column.TableName == nil
	default:
		return false
	}
}

func trimIndexExpressionParens(expr string) string {
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
			if depth == 0 && i != len(expr)-1 {
				return expr
			}
		}
	}
	if depth != 0 {
		return expr
	}
	return strings.TrimSpace(expr[1 : len(expr)-1])
}
