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

	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/postgres/parser/sem/tree"
	"github.com/dolthub/doltgresql/server/auth"
	pgnodes "github.com/dolthub/doltgresql/server/node"
)

// nodeCreateTable handles *tree.CreateTable nodes.
func nodeCreateTable(ctx *Context, node *tree.CreateTable) (vitess.Statement, error) {
	if node == nil {
		return nil, nil
	}
	if len(node.StorageParams) > 0 {
		return nil, errors.Errorf("storage parameters are not yet supported")
	}
	if node.OnCommit != tree.CreateTableOnCommitUnset {
		return nil, errors.Errorf("ON COMMIT is not yet supported")
	}
	tableName, err := nodeTableName(ctx, &node.Table)
	if err != nil {
		return nil, err
	}
	var isTemporary bool
	switch node.Persistence {
	case tree.PersistencePermanent:
		isTemporary = false
	case tree.PersistenceTemporary:
		isTemporary = true
	case tree.PersistenceUnlogged:
		return nil, errors.Errorf("UNLOGGED is not yet supported")
	default:
		return nil, errors.Errorf("unknown persistence strategy encountered")
	}
	var optSelect *vitess.OptSelect
	if node.Using != "" {
		return nil, errors.Errorf("USING is not yet supported")
	}
	if node.Tablespace != "" {
		return nil, errors.Errorf("TABLESPACE is not yet supported")
	}
	if node.OfType != nil {
		if node.AsSource != nil {
			return nil, errors.Errorf("CREATE TABLE OF cannot use AS")
		}
		if len(node.Inherits) > 0 {
			return nil, errors.Errorf("CREATE TABLE OF cannot use INHERITS")
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
			Auth: vitess.AuthInformation{
				AuthType:    auth.AuthType_CREATE,
				TargetType:  auth.AuthTargetType_SchemaIdentifiers,
				TargetNames: []string{tableName.DbQualifier.String(), tableName.SchemaQualifier.String()},
			},
			Children: typedTableChildren,
		}, nil
	}
	if node.AsSource != nil {
		selectStmt, err := nodeSelect(ctx, node.AsSource)
		if err != nil {
			return nil, err
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
	if node.WithNoData {
		return nil, errors.Errorf("WITH NO DATA is not yet supported")
	}
	ddl := &vitess.DDL{
		Action:      vitess.CreateStr,
		Table:       tableName,
		IfNotExists: node.IfNotExists,
		Temporary:   isTemporary,
		OptSelect:   optSelect,
		OptLike:     optLike,
		Auth: vitess.AuthInformation{
			AuthType:    auth.AuthType_CREATE,
			TargetType:  auth.AuthTargetType_SchemaIdentifiers,
			TargetNames: []string{tableName.DbQualifier.String(), tableName.SchemaQualifier.String()},
		},
	}
	if err = assignTableDefs(ctx, node.Defs, ddl); err != nil {
		return nil, err
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
			columnOption, defaultExpr, checks, err := nodeTypedTableColumnOptions(ctx, tableName, def, len(children))
			if err != nil {
				return options, children, err
			}
			if defaultExpr != nil {
				children = append(children, defaultExpr)
			}
			options.ColumnOptions = append(options.ColumnOptions, columnOption)
			options.CheckConstraints = append(options.CheckConstraints, checks...)
		case *tree.UniqueConstraintTableDef:
			constraintKind := "unique constraint"
			if def.PrimaryKey {
				constraintKind = "primary key"
			}
			columns, err := nodeTypedTableConstraintColumns(def, constraintKind)
			if err != nil {
				return options, children, err
			}
			if def.PrimaryKey {
				if len(options.PrimaryKeyColumns) > 0 {
					return options, children, errors.Errorf("multiple primary keys for table are not allowed")
				}
				options.PrimaryKeyColumns = columns
			} else {
				if def.NullsNotDistinct {
					return options, children, errors.Errorf("CREATE TABLE OF UNIQUE NULLS NOT DISTINCT constraints are not yet supported")
				}
				options.UniqueConstraints = append(options.UniqueConstraints, pgnodes.TypedTableUniqueConstraint{
					Name:    typedTableConstraintName(def.Name, defaultUniqueConstraintNameFromNames(tableName, columns)),
					Columns: columns,
				})
			}
		case *tree.CheckConstraintTableDef:
			check, err := nodeTypedTableCheckConstraint(ctx, string(def.Name), def.Expr, def.NoInherit)
			if err != nil {
				return options, children, err
			}
			options.CheckConstraints = append(options.CheckConstraints, check)
		case *tree.ForeignKeyConstraintTableDef:
			return options, children, errors.Errorf("CREATE TABLE OF FOREIGN KEY constraints are not yet supported")
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

func nodeTypedTableColumnOptions(ctx *Context, tableName string, def *tree.ColumnTableDef, defaultChildIndex int) (pgnodes.TypedTableColumnOptions, vitess.Expr, []pgnodes.TypedTableCheckConstraint, error) {
	option := pgnodes.TypedTableColumnOptions{Name: string(def.Name)}
	var defaultExpr vitess.Expr
	checks := make([]pgnodes.TypedTableCheckConstraint, 0, len(def.CheckExprs))
	if def.HasDefaultExpr() {
		if !typedTableDefaultExprIsLiteral(def.DefaultExpr.Expr) {
			return option, nil, nil, errors.Errorf("CREATE TABLE OF non-literal column defaults are not yet supported")
		}
		var err error
		defaultExpr, err = nodeExpr(ctx, def.DefaultExpr.Expr)
		if err != nil {
			return option, nil, nil, err
		}
		option.HasDefault = true
		option.DefaultLiteral = true
		option.DefaultChildIndex = defaultChildIndex
	}
	for _, checkExpr := range def.CheckExprs {
		check, err := nodeTypedTableCheckConstraint(ctx, string(checkExpr.ConstraintName), checkExpr.Expr, checkExpr.NoInherit)
		if err != nil {
			return option, nil, nil, err
		}
		checks = append(checks, check)
	}
	if def.References.Table != nil {
		return option, nil, nil, errors.Errorf("CREATE TABLE OF column FOREIGN KEY constraints are not yet supported")
	}
	if def.Unique && !def.PrimaryKey.IsPrimaryKey {
		if def.UniqueNullsNotDistinct {
			return option, nil, nil, errors.Errorf("CREATE TABLE OF UNIQUE NULLS NOT DISTINCT constraints are not yet supported")
		}
		option.Unique = true
		option.UniqueName = typedTableConstraintName(def.UniqueConstraintName, defaultUniqueConstraintNameFromNames(tableName, []string{option.Name}))
	}
	if def.Computed.Computed {
		return option, nil, nil, errors.Errorf("CREATE TABLE OF generated columns are not supported")
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
		return option, nil, nil, errors.Errorf("unknown NULL type encountered")
	}
	option.PrimaryKey = def.PrimaryKey.IsPrimaryKey
	return option, defaultExpr, checks, nil
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

func nodeTypedTableConstraintColumns(def *tree.UniqueConstraintTableDef, constraintKind string) ([]string, error) {
	if len(def.IndexParams.StorageParams) > 0 {
		return nil, errors.Errorf("STORAGE parameters not yet supported for indexes")
	}
	if def.IndexParams.Tablespace != "" {
		return nil, errors.Errorf("TABLESPACE is not yet supported")
	}
	columns := make([]string, len(def.Columns))
	for i, column := range def.Columns {
		if column.Expr != nil || column.Column == "" {
			return nil, errors.Errorf("CREATE TABLE OF %s expressions are not yet supported", constraintKind)
		}
		if column.Collation != "" || column.OpClass != nil || column.Direction != tree.DefaultDirection || column.NullsOrder != tree.DefaultNullsOrder {
			return nil, errors.Errorf("CREATE TABLE OF %s index options are not yet supported", constraintKind)
		}
		columns[i] = string(column.Column)
	}
	return columns, nil
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
