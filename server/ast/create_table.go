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
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"

	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/postgres/parser/sem/tree"
	"github.com/dolthub/doltgresql/server/auth"
	pgnodes "github.com/dolthub/doltgresql/server/node"
	"github.com/dolthub/doltgresql/server/tablemetadata"
)

// nodeCreateTable handles *tree.CreateTable nodes.
func nodeCreateTable(ctx *Context, node *tree.CreateTable) (vitess.Statement, error) {
	if node == nil {
		return nil, nil
	}
	relOptions, err := nodeTableRelOptions(node.StorageParams)
	if err != nil {
		return nil, err
	}
	tableName, err := nodeTableName(ctx, &node.Table)
	if err != nil {
		return nil, err
	}
	var isTemporary bool
	relPersistence := "p"
	switch node.Persistence {
	case tree.PersistencePermanent:
		isTemporary = false
	case tree.PersistenceTemporary:
		isTemporary = true
		relPersistence = "t"
	case tree.PersistenceUnlogged:
		isTemporary = false
		relPersistence = "u"
	default:
		return nil, errors.Errorf("unknown persistence strategy encountered")
	}
	if node.OnCommit != tree.CreateTableOnCommitUnset && !isTemporary {
		return nil, errors.Errorf("ON COMMIT can only be used on temporary tables")
	}
	if isTemporary {
		if dbName := tableName.DbQualifier.String(); dbName != "" {
			return nil, errors.Errorf("cannot create temporary relation in non-temporary schema")
		}
		if schemaName := tableName.SchemaQualifier.String(); schemaName != "" && !strings.EqualFold(schemaName, "pg_temp") {
			return nil, errors.Errorf("cannot create temporary relation in non-temporary schema")
		}
	}
	var optSelect *vitess.OptSelect
	if node.Using != "" && !strings.EqualFold(node.Using, "heap") {
		// PostgreSQL ships with only heap as the table access method in
		// stock builds; doltgres has no extension hooks for additional
		// table AMs, so reject other targets with the same catalog-style
		// error PostgreSQL produces.
		return nil, errors.Errorf(`access method "%s" does not exist`, node.Using)
	}
	if node.Tablespace != "" && !strings.EqualFold(string(node.Tablespace), "pg_default") {
		// pg_default is the only tablespace doltgres exposes. Spelling
		// it out is a no-op; any other target name would not resolve.
		return nil, errors.Errorf(`tablespace "%s" does not exist`, string(node.Tablespace))
	}
	foreignServer := string(node.ForeignServer)
	if foreignServer != "" {
		var exists bool
		auth.LockRead(func() {
			_, exists = auth.GetForeignServer(foreignServer)
		})
		if !exists {
			return nil, errors.Errorf(`server "%s" does not exist`, foreignServer)
		}
	}
	if node.OfType != nil {
		if node.As() {
			return nil, errors.Errorf("CREATE TABLE OF cannot use AS")
		}
		if len(node.Inherits) > 0 {
			return nil, errors.Errorf("CREATE TABLE OF cannot use INHERITS")
		}
		if node.PartitionBy != nil {
			return nil, errors.Errorf("CREATE TABLE OF cannot use PARTITION BY")
		}
		if node.PartitionOf.Table() != "" {
			return nil, errors.Errorf("CREATE TABLE OF cannot use PARTITION OF")
		}
		typedTableOptions, typedTableChildren, err := nodeTypedTableOptions(ctx, tableName.Name.String(), node.Defs)
		if err != nil {
			return nil, err
		}
		typeName, err := nodeUnresolvedObjectName(ctx, node.OfType)
		if err != nil {
			return nil, err
		}
		return vitess.InjectedStatement{
			Statement: pgnodes.NewCreateTypedTable(
				node.IfNotExists,
				isTemporary,
				tableName.DbQualifier.String(),
				tableName.SchemaQualifier.String(),
				tableName.Name.String(),
				typeName.SchemaQualifier.String(),
				typeName.Name.String(),
				typedTableOptions,
			),
			Auth:     createTableAuthInfo(tableName, isTemporary),
			Children: typedTableChildren,
		}, nil
	}
	if likeDef, ok := singleLikeTableDef(node.Defs); ok && !node.As() && len(node.Inherits) == 0 {
		likeTableName, err := nodeTableName(ctx, &likeDef.Name)
		if err != nil {
			return nil, err
		}
		return vitess.InjectedStatement{
			Statement: pgnodes.NewCreateTableLike(
				node.IfNotExists,
				isTemporary,
				tableName.DbQualifier.String(),
				tableName.SchemaQualifier.String(),
				tableName.Name.String(),
				likeTableName.DbQualifier.String(),
				likeTableName.SchemaQualifier.String(),
				likeTableName.Name.String(),
				createTableLikeOptions(likeDef.Options),
			),
			Auth: createTableAuthInfo(tableName, isTemporary),
		}, nil
	}
	if node.AsExecute != nil {
		if err = validateCreateTableAsColumnDefs(node.Defs); err != nil {
			return nil, err
		}
		params := make([]string, len(node.AsExecute.Params))
		for i, param := range node.AsExecute.Params {
			params[i] = tree.AsString(param)
		}
		return vitess.InjectedStatement{
			Statement: pgnodes.CreateTableAsExecuteStatement{
				CreatePrefix: createTableAsExecutePrefix(node),
				Execute: pgnodes.ExecuteStatement{
					Name:        string(node.AsExecute.Name),
					Params:      params,
					DiscardRows: node.AsExecute.DiscardRows,
				},
				WithNoData: node.WithNoData,
			},
			Auth: createTableAuthInfo(tableName, isTemporary),
		}, nil
	}
	if node.AsSource != nil {
		selectStmt, err := nodeSelect(ctx, node.AsSource)
		if err != nil {
			return nil, err
		}
		if node.WithNoData {
			// PostgreSQL semantics: WITH NO DATA creates the table from the
			// query's column types but does not evaluate the query. Forcing
			// LIMIT 0 reproduces that: GMS still infers the projection schema,
			// but no rows are produced so row-time expression evaluation
			// (e.g., a literal `1/0` in a projected column) cannot fire.
			selectStmt = forceLimitZero(selectStmt)
		}
		optSelect = &vitess.OptSelect{
			Select: selectStmt,
		}
	}
	var optLike *vitess.OptLike
	if len(node.Inherits) > 0 {
		optLike = &vitess.OptLike{
			LikeTables: []vitess.TableName{},
		}
		for _, table := range node.Inherits {
			likeTable, err := nodeTableName(ctx, &table)
			if err != nil {
				return nil, err
			}
			optLike.LikeTables = append(optLike.LikeTables, likeTable)
		}
	}
	ddl := &vitess.DDL{
		Action:      vitess.CreateStr,
		Table:       tableName,
		IfNotExists: node.IfNotExists,
		Temporary:   isTemporary,
		OptSelect:   optSelect,
		OptLike:     optLike,
		Auth:        createTableAuthInfo(tableName, isTemporary),
	}
	if node.AsSource != nil {
		if err = validateCreateTableAsColumnDefs(node.Defs); err != nil {
			return nil, err
		}
	} else {
		if err = assignTableDefs(ctx, node.Defs, ddl); err != nil {
			return nil, err
		}
		setColumnIdentityMetadata(ddl.TableSpec, node.Defs)
	}
	if len(relOptions) > 0 {
		if ddl.TableSpec == nil {
			ddl.TableSpec = &vitess.TableSpec{}
		}
		setTableMetadataCommentOption(ddl.TableSpec, func(comment string) string {
			return tablemetadata.SetRelOptions(comment, relOptions)
		})
	}
	if relPersistence != "p" {
		if ddl.TableSpec == nil {
			ddl.TableSpec = &vitess.TableSpec{}
		}
		setTableMetadataCommentOption(ddl.TableSpec, func(comment string) string {
			return tablemetadata.SetRelPersistence(comment, relPersistence)
		})
	}
	if foreignServer != "" {
		if ddl.TableSpec == nil {
			ddl.TableSpec = &vitess.TableSpec{}
		}
		setTableMetadataCommentOption(ddl.TableSpec, func(comment string) string {
			return tablemetadata.SetForeignTable(comment, foreignServer, fdwOptionsToStrings(node.ForeignOptions))
		})
	}
	if onCommitOption := createTableOnCommitOption(node.OnCommit); onCommitOption != "" {
		if ddl.TableSpec == nil {
			ddl.TableSpec = &vitess.TableSpec{}
		}
		ddl.TableSpec.TableOpts = append(ddl.TableSpec.TableOpts, &vitess.TableOption{
			Name:  pgnodes.TableOptionTemporaryOnCommit,
			Value: onCommitOption,
		})
	}

	if node.PartitionBy != nil {
		switch node.PartitionBy.Type {
		case tree.PartitionByList:
			if len(node.PartitionBy.Elems) != 1 {
				return nil, errors.Errorf("PARTITION BY LIST must have a single column or expression")
			}
		}

		// GMS does not support PARTITION BY, so we parse it and ignore it
		if ddl.TableSpec != nil {
			ddl.TableSpec.PartitionOpt = &vitess.PartitionOption{
				PartitionType: string(node.PartitionBy.Type),
				Expr:          vitess.NewColName(string(node.PartitionBy.Elems[0].Column)),
			}
		}
	}
	if node.PartitionOf.Table() != "" {
		return nil, errors.Errorf("PARTITION OF is not yet supported")
	}
	return ddl, nil
}

func createTableAuthInfo(tableName vitess.TableName, isTemporary bool) vitess.AuthInformation {
	authInfo := vitess.AuthInformation{
		AuthType:    auth.AuthType_CREATE,
		TargetType:  auth.AuthTargetType_SchemaIdentifiers,
		TargetNames: []string{tableName.DbQualifier.String(), tableName.SchemaQualifier.String()},
	}
	if isTemporary {
		auth.AppendAdditionalAuth(&authInfo, vitess.AuthInformation{
			AuthType:    auth.AuthType_TEMPORARY,
			TargetType:  auth.AuthTargetType_DatabaseIdentifiers,
			TargetNames: []string{tableName.DbQualifier.String()},
		})
	}
	return authInfo
}

func createTableOnCommitOption(onCommit tree.CreateTableOnCommitSetting) string {
	switch onCommit {
	case tree.CreateTableOnCommitPreserveRows:
		return string(pgnodes.TemporaryTableOnCommitPreserveRows)
	case tree.CreateTableOnCommitDeleteRows:
		return string(pgnodes.TemporaryTableOnCommitDeleteRows)
	case tree.CreateTableOnCommitDrop:
		return string(pgnodes.TemporaryTableOnCommitDrop)
	default:
		return ""
	}
}

func setColumnIdentityMetadata(tableSpec *vitess.TableSpec, defs tree.TableDefs) {
	if tableSpec == nil {
		return
	}
	for _, def := range defs {
		columnDef, ok := def.(*tree.ColumnTableDef)
		if !ok || !columnDef.IsComputed() || columnDef.Computed.Expr != nil {
			continue
		}
		identity := "a"
		if columnDef.Computed.ByDefault {
			identity = "d"
		}
		columnName := string(columnDef.Name)
		setTableMetadataCommentOption(tableSpec, func(comment string) string {
			return tablemetadata.SetColumnIdentity(comment, columnName, identity)
		})
	}
}

// forceLimitZero rewrites a SelectStatement so it produces no rows while still
// exposing the projection schema. Used to implement CREATE TABLE AS ... WITH
// NO DATA: PostgreSQL creates the table from the query's column types without
// running the query, so any row-time side effects (errors, sequence advance,
// volatile function calls) must not fire.
func forceLimitZero(stmt vitess.SelectStatement) vitess.SelectStatement {
	zero := &vitess.Limit{Rowcount: vitess.NewIntVal([]byte("0"))}
	switch s := stmt.(type) {
	case *vitess.Select:
		s.Limit = zero
	case *vitess.SetOp:
		s.Limit = zero
	case *vitess.ParenSelect:
		s.Select = forceLimitZero(s.Select)
	}
	return stmt
}

func singleLikeTableDef(defs tree.TableDefs) (*tree.LikeTableDef, bool) {
	if len(defs) != 1 {
		return nil, false
	}
	likeDef, ok := defs[0].(*tree.LikeTableDef)
	return likeDef, ok
}

func createTableLikeOptions(options []tree.LikeTableOption) pgnodes.CreateTableLikeOptions {
	var opts pgnodes.CreateTableLikeOptions
	for _, option := range options {
		var bit pgnodes.CreateTableLikeOptions
		switch option.Opt {
		case tree.LikeTableOptConstraints:
			bit = pgnodes.CreateTableLikeOptionConstraints
		case tree.LikeTableOptDefaults:
			bit = pgnodes.CreateTableLikeOptionDefaults
		case tree.LikeTableOptIdentity:
			bit = pgnodes.CreateTableLikeOptionIdentity
		case tree.LikeTableOptGenerated:
			bit = pgnodes.CreateTableLikeOptionGenerated
		case tree.LikeTableOptIndexes:
			bit = pgnodes.CreateTableLikeOptionIndexes
		case tree.LikeTableOptAll:
			bit = pgnodes.CreateTableLikeOptionAll
		}
		if option.Excluded {
			opts &^= bit
		} else {
			opts |= bit
		}
	}
	return opts
}

func validateCreateTableAsColumnDefs(defs tree.TableDefs) error {
	for _, def := range defs {
		if _, ok := def.(*tree.ColumnTableDef); !ok {
			return errors.Errorf("CREATE TABLE AS column list may only contain column names")
		}
	}
	return nil
}

func createTableAsExecutePrefix(node *tree.CreateTable) string {
	var b strings.Builder
	b.WriteString("CREATE ")
	switch node.Persistence {
	case tree.PersistenceTemporary:
		b.WriteString("TEMPORARY ")
	case tree.PersistenceUnlogged:
		b.WriteString("UNLOGGED ")
	}
	b.WriteString("TABLE ")
	if node.IfNotExists {
		b.WriteString("IF NOT EXISTS ")
	}
	b.WriteString(tree.AsString(&node.Table))
	if len(node.Defs) > 0 {
		b.WriteString(" (")
		b.WriteString(tree.AsString(&node.Defs))
		b.WriteByte(')')
	}
	if node.Using != "" {
		b.WriteString(" USING ")
		b.WriteString(node.Using)
	}
	if node.StorageParams != nil {
		b.WriteString(tree.AsString(&node.StorageParams))
	}
	if node.Tablespace != "" {
		b.WriteString(" TABLESPACE ")
		b.WriteString(tree.AsString(&node.Tablespace))
	}
	b.WriteString(" AS ")
	return b.String()
}

func nodeTypedTableOptions(ctx *Context, tableName string, defs tree.TableDefs) (pgnodes.TypedTableOptions, vitess.Exprs, error) {
	var options pgnodes.TypedTableOptions
	var children vitess.Exprs
	seenColumns := make(map[string]struct{}, len(defs))
	for _, def := range defs {
		switch def := def.(type) {
		case *tree.ColumnTableDef:
			if def.Type != nil {
				return options, children, errors.Errorf("CREATE TABLE OF cannot redefine column types")
			}
			name := string(def.Name)
			columnKey := strings.ToLower(name)
			if _, ok := seenColumns[columnKey]; ok {
				return options, children, errors.Errorf(`column "%s" specified more than once`, name)
			}
			seenColumns[columnKey] = struct{}{}
			columnOption, defaultExpr, checks, foreignKeys, err := nodeTypedTableColumnOptions(ctx, tableName, def, len(children))
			if err != nil {
				return options, children, err
			}
			if defaultExpr != nil {
				children = append(children, defaultExpr)
			}
			options.ColumnOptions = append(options.ColumnOptions, columnOption)
			options.CheckConstraints = append(options.CheckConstraints, checks...)
			options.ForeignKeyConstraints = append(options.ForeignKeyConstraints, foreignKeys...)
		case *tree.UniqueConstraintTableDef:
			constraintKind := "unique constraint"
			if def.PrimaryKey {
				constraintKind = "primary key"
			}
			constraint, err := nodeTypedTableUniqueConstraint(tableName, def, constraintKind)
			if err != nil {
				return options, children, err
			}
			if def.PrimaryKey {
				if len(options.PrimaryKeyColumns) > 0 {
					return options, children, errors.Errorf("multiple primary keys for table are not allowed")
				}
				options.PrimaryKeyColumns = constraint.Columns
				options.PrimaryKeyInclude = constraint.IncludeColumns
				options.PrimaryKeyRelOptions = constraint.RelOptions
			} else {
				options.UniqueConstraints = append(options.UniqueConstraints, constraint)
			}
		case *tree.CheckConstraintTableDef:
			check, err := nodeTypedTableCheckConstraint(ctx, string(def.Name), def.Expr, def.NoInherit)
			if err != nil {
				return options, children, err
			}
			options.CheckConstraints = append(options.CheckConstraints, check)
		case *tree.ForeignKeyConstraintTableDef:
			foreignKey, err := nodeTypedTableForeignKeyConstraint(ctx, tableName, def)
			if err != nil {
				return options, children, err
			}
			options.ForeignKeyConstraints = append(options.ForeignKeyConstraints, foreignKey)
		case *tree.ExcludeConstraintTableDef:
			return options, children, errors.Errorf("CREATE TABLE OF exclude constraints are not supported")
		case *tree.IndexTableDef:
			return options, children, errors.Errorf("CREATE TABLE OF indexes are not yet supported")
		case *tree.LikeTableDef:
			return options, children, errors.Errorf("CREATE TABLE OF LIKE definitions are not supported")
		default:
			return options, children, errors.Errorf("unknown CREATE TABLE OF definition encountered")
		}
	}
	return options, children, nil
}

func nodeTypedTableColumnOptions(ctx *Context, tableName string, def *tree.ColumnTableDef, childIndex int) (pgnodes.TypedTableColumnOptions, vitess.Expr, []pgnodes.TypedTableCheckConstraint, []pgnodes.TypedTableForeignKeyConstraint, error) {
	option := pgnodes.TypedTableColumnOptions{Name: string(def.Name)}
	var defaultExpr vitess.Expr
	checks := make([]pgnodes.TypedTableCheckConstraint, 0, len(def.CheckExprs))
	var foreignKeys []pgnodes.TypedTableForeignKeyConstraint
	if def.HasDefaultExpr() {
		var err error
		defaultExpr, err = nodeExpr(ctx, def.DefaultExpr.Expr)
		if err != nil {
			return option, nil, nil, nil, err
		}
		option.HasDefault = true
		option.DefaultLiteral = typedTableDefaultExprIsLiteral(def.DefaultExpr.Expr)
		option.DefaultParenthesized = typedTableDefaultExprIsParenthesized(def.DefaultExpr.Expr)
		option.DefaultChildIndex = childIndex
		childIndex++
	}
	for _, checkExpr := range def.CheckExprs {
		check, err := nodeTypedTableCheckConstraint(ctx, string(checkExpr.ConstraintName), checkExpr.Expr, checkExpr.NoInherit)
		if err != nil {
			return option, nil, nil, nil, err
		}
		checks = append(checks, check)
	}
	if def.References.Table != nil {
		if len(def.References.Col) == 0 {
			return option, nil, nil, nil, errors.Errorf("implicit primary key matching on column foreign key is not yet supported")
		}
		foreignKey, err := nodeTypedTableForeignKeyConstraint(ctx, tableName, &tree.ForeignKeyConstraintTableDef{
			Name:       def.References.ConstraintName,
			Table:      *def.References.Table,
			FromCols:   tree.NameList{def.Name},
			ToCols:     tree.NameList{def.References.Col},
			Actions:    def.References.Actions,
			Match:      def.References.Match,
			Deferrable: def.References.Deferrable,
			Initially:  def.References.Initially,
		})
		if err != nil {
			return option, nil, nil, nil, err
		}
		foreignKeys = append(foreignKeys, foreignKey)
	}
	if def.Unique && !def.PrimaryKey.IsPrimaryKey {
		option.Unique = true
		option.UniqueName = typedTableConstraintName(def.UniqueConstraintName, defaultUniqueConstraintNameFromNames(tableName, []string{option.Name}))
		option.UniqueNullsNotDistinct = def.UniqueNullsNotDistinct
	}
	if def.Computed.Computed {
		if def.Computed.Expr == nil {
			return option, nil, nil, nil, errors.Errorf("CREATE TABLE OF identity columns are not supported")
		}
		if def.HasDefaultExpr() {
			return option, nil, nil, nil, errors.Errorf(`both default and generation expression specified for column "%s"`, def.Name)
		}
		option.HasGenerated = true
		option.GeneratedExprString = tree.AsString(def.Computed.Expr)
	}
	switch def.Nullable.Nullability {
	case tree.NotNull:
		option.NullableSet = true
		option.Nullable = false
	case tree.Null:
		option.NullableSet = true
		option.Nullable = true
	case tree.SilentNull:
	default:
		return option, nil, nil, nil, errors.Errorf("unknown NULL type encountered")
	}
	option.PrimaryKey = def.PrimaryKey.IsPrimaryKey
	return option, defaultExpr, checks, foreignKeys, nil
}

func nodeTypedTableCheckConstraint(ctx *Context, name string, expr tree.Expr, noInherit bool) (pgnodes.TypedTableCheckConstraint, error) {
	if noInherit {
		return pgnodes.TypedTableCheckConstraint{}, errors.Errorf("NO INHERIT is not yet supported for check constraints")
	}
	if _, err := nodeExpr(ctx, expr); err != nil {
		return pgnodes.TypedTableCheckConstraint{}, err
	}
	return pgnodes.TypedTableCheckConstraint{
		Name:       name,
		Expression: tree.AsStringWithFlags(expr, tree.FmtParsable),
	}, nil
}

func nodeTypedTableForeignKeyConstraint(ctx *Context, tableName string, def *tree.ForeignKeyConstraintTableDef) (pgnodes.TypedTableForeignKeyConstraint, error) {
	foreignKey, err := nodeForeignKeyConstraintTableDef(ctx, tableName, def)
	if err != nil {
		return pgnodes.TypedTableForeignKeyConstraint{}, err
	}
	if len(foreignKey.ReferencedColumns) == 0 {
		return pgnodes.TypedTableForeignKeyConstraint{}, errors.Errorf("implicit primary key matching on foreign key is not yet supported")
	}
	return pgnodes.TypedTableForeignKeyConstraint{
		Name:               string(def.Name),
		Columns:            typedTableColIdentsToStrings(foreignKey.Source),
		ParentDatabaseName: foreignKey.ReferencedTable.DbQualifier.String(),
		ParentSchemaName:   foreignKey.ReferencedTable.SchemaQualifier.String(),
		ParentTableName:    foreignKey.ReferencedTable.Name.String(),
		ParentColumns:      typedTableColIdentsToStrings(foreignKey.ReferencedColumns),
		OnDelete:           typedTableForeignKeyAction(foreignKey.OnDelete),
		OnUpdate:           typedTableForeignKeyAction(foreignKey.OnUpdate),
	}, nil
}

func typedTableForeignKeyAction(action vitess.ReferenceAction) sql.ForeignKeyReferentialAction {
	switch action {
	case vitess.Restrict:
		return sql.ForeignKeyReferentialAction_Restrict
	case vitess.Cascade:
		return sql.ForeignKeyReferentialAction_Cascade
	case vitess.NoAction:
		return sql.ForeignKeyReferentialAction_NoAction
	case vitess.SetNull:
		return sql.ForeignKeyReferentialAction_SetNull
	case vitess.SetDefault:
		return sql.ForeignKeyReferentialAction_SetDefault
	default:
		return sql.ForeignKeyReferentialAction_DefaultAction
	}
}

func typedTableColIdentsToStrings(cols []vitess.ColIdent) []string {
	strings := make([]string, len(cols))
	for i, col := range cols {
		strings[i] = col.String()
	}
	return strings
}

func typedTableDefaultExprIsLiteral(expr tree.Expr) bool {
	switch expr := expr.(type) {
	case nil:
		return false
	case *tree.ParenExpr:
		return typedTableDefaultExprIsLiteral(expr.Expr)
	case *tree.UnaryExpr:
		if expr.Operator != tree.UnaryMinus {
			return false
		}
		return typedTableDefaultExprIsNumericLiteral(expr.Expr)
	case *tree.StrVal, *tree.NumVal, *tree.DBool, *tree.DDecimal, *tree.DFloat, *tree.DInt, *tree.DString:
		return true
	case tree.NullLiteral:
		return true
	default:
		return false
	}
}

func typedTableDefaultExprIsParenthesized(expr tree.Expr) bool {
	_, ok := expr.(*tree.ParenExpr)
	return ok
}

func typedTableDefaultExprIsNumericLiteral(expr tree.Expr) bool {
	switch expr := expr.(type) {
	case *tree.ParenExpr:
		return typedTableDefaultExprIsNumericLiteral(expr.Expr)
	case *tree.NumVal, *tree.DDecimal, *tree.DFloat, *tree.DInt:
		return true
	default:
		return false
	}
}

func nodeTypedTableUniqueConstraint(tableName string, def *tree.UniqueConstraintTableDef, constraintKind string) (pgnodes.TypedTableUniqueConstraint, error) {
	columns := make([]string, len(def.Columns))
	for i, column := range def.Columns {
		if column.Expr != nil || column.Column == "" {
			return pgnodes.TypedTableUniqueConstraint{}, errors.Errorf("CREATE TABLE OF %s expressions are not yet supported", constraintKind)
		}
		columns[i] = string(column.Column)
	}
	includeColumns, err := nodeIndexIncludeColumns(def.IndexParams.IncludeColumns)
	if err != nil {
		return pgnodes.TypedTableUniqueConstraint{}, err
	}
	relOptions, err := nodeIndexRelOptions(def.IndexParams.StorageParams)
	if err != nil {
		return pgnodes.TypedTableUniqueConstraint{}, err
	}
	if def.IndexParams.Tablespace != "" && !isDefaultIndexTablespace(def.IndexParams.Tablespace) {
		return pgnodes.TypedTableUniqueConstraint{}, errors.Errorf("TABLESPACE is not yet supported for indexes")
	}
	name := typedTableConstraintName(def.Name, defaultUniqueConstraintNameFromNames(tableName, append(columns, includeColumns...)))
	return pgnodes.TypedTableUniqueConstraint{
		Name:             name,
		Columns:          columns,
		IncludeColumns:   includeColumns,
		RelOptions:       relOptions,
		NullsNotDistinct: def.NullsNotDistinct,
	}, nil
}

func typedTableConstraintName(explicit tree.Name, defaultName string) string {
	if explicit != "" {
		return string(explicit)
	}
	return defaultName
}

func defaultUniqueConstraintNameFromNames(tableName string, columns []string) string {
	indexElems := make(tree.IndexElemList, len(columns))
	for i, column := range columns {
		indexElems[i] = tree.IndexElem{Column: tree.Name(column)}
	}
	return defaultUniqueConstraintName(tableName, indexElems)
}
