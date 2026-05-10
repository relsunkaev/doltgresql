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
		typedTableOptions, err := nodeTypedTableOptions(node.Defs)
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

func nodeTypedTableOptions(defs tree.TableDefs) (pgnodes.TypedTableOptions, error) {
	var options pgnodes.TypedTableOptions
	seenColumns := make(map[string]struct{}, len(defs))
	for _, def := range defs {
		switch def := def.(type) {
		case *tree.ColumnTableDef:
			if def.Type != nil {
				return options, errors.Errorf("CREATE TABLE OF cannot redefine column types")
			}
			name := string(def.Name)
			columnKey := strings.ToLower(name)
			if _, ok := seenColumns[columnKey]; ok {
				return options, errors.Errorf(`column "%s" specified more than once`, name)
			}
			seenColumns[columnKey] = struct{}{}
			columnOption, err := nodeTypedTableColumnOptions(def)
			if err != nil {
				return options, err
			}
			options.ColumnOptions = append(options.ColumnOptions, columnOption)
		case *tree.UniqueConstraintTableDef:
			if !def.PrimaryKey {
				return options, errors.Errorf("CREATE TABLE OF UNIQUE constraints are not yet supported")
			}
			primaryKeyColumns, err := nodeTypedTablePrimaryKeyColumns(def)
			if err != nil {
				return options, err
			}
			if len(options.PrimaryKeyColumns) > 0 {
				return options, errors.Errorf("multiple primary keys for table are not allowed")
			}
			options.PrimaryKeyColumns = primaryKeyColumns
		case *tree.CheckConstraintTableDef:
			return options, errors.Errorf("CREATE TABLE OF CHECK constraints are not yet supported")
		case *tree.ForeignKeyConstraintTableDef:
			return options, errors.Errorf("CREATE TABLE OF FOREIGN KEY constraints are not yet supported")
		case *tree.IndexTableDef:
			return options, errors.Errorf("CREATE TABLE OF indexes are not yet supported")
		case *tree.LikeTableDef:
			return options, errors.Errorf("CREATE TABLE OF LIKE definitions are not supported")
		default:
			return options, errors.Errorf("unknown CREATE TABLE OF definition encountered")
		}
	}
	return options, nil
}

func nodeTypedTableColumnOptions(def *tree.ColumnTableDef) (pgnodes.TypedTableColumnOptions, error) {
	option := pgnodes.TypedTableColumnOptions{Name: string(def.Name)}
	if def.HasDefaultExpr() {
		return option, errors.Errorf("CREATE TABLE OF column defaults are not yet supported")
	}
	if len(def.CheckExprs) > 0 {
		return option, errors.Errorf("CREATE TABLE OF column CHECK constraints are not yet supported")
	}
	if def.References.Table != nil {
		return option, errors.Errorf("CREATE TABLE OF column FOREIGN KEY constraints are not yet supported")
	}
	if def.Unique && !def.PrimaryKey.IsPrimaryKey {
		return option, errors.Errorf("CREATE TABLE OF UNIQUE constraints are not yet supported")
	}
	if def.Computed.Computed {
		return option, errors.Errorf("CREATE TABLE OF generated columns are not supported")
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
		return option, errors.Errorf("unknown NULL type encountered")
	}
	option.PrimaryKey = def.PrimaryKey.IsPrimaryKey
	return option, nil
}

func nodeTypedTablePrimaryKeyColumns(def *tree.UniqueConstraintTableDef) ([]string, error) {
	if len(def.IndexParams.StorageParams) > 0 {
		return nil, errors.Errorf("STORAGE parameters not yet supported for indexes")
	}
	if def.IndexParams.Tablespace != "" {
		return nil, errors.Errorf("TABLESPACE is not yet supported")
	}
	columns := make([]string, len(def.Columns))
	for i, column := range def.Columns {
		if column.Expr != nil || column.Column == "" {
			return nil, errors.Errorf("CREATE TABLE OF primary key expressions are not yet supported")
		}
		if column.Collation != "" || column.OpClass != nil || column.Direction != tree.DefaultDirection || column.NullsOrder != tree.DefaultNullsOrder {
			return nil, errors.Errorf("CREATE TABLE OF primary key index options are not yet supported")
		}
		columns[i] = string(column.Column)
	}
	return columns, nil
}
