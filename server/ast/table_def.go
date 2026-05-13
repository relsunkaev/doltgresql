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
	"sort"
	"strings"

	"github.com/cockroachdb/errors"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/postgres/parser/sem/tree"
	"github.com/dolthub/doltgresql/server/indexmetadata"
	"github.com/dolthub/doltgresql/utils"
)

// assignTableDef handles tree.TableDef nodes for *vitess.DDL targets. Some table defs, such as indexes, affect other
// defs, such as columns, and they're therefore dependent on columns being handled first. It is up to the caller to
// ensure that all defs have been ordered properly before calling. assignTableDefs handles the sort for you, so this
// notice is only relevant when individually calling assignTableDef.
func assignTableDef(ctx *Context, node tree.TableDef, target *vitess.DDL) error {
	switch node := node.(type) {
	case *tree.CheckConstraintTableDef:
		if target.TableSpec == nil {
			target.TableSpec = &vitess.TableSpec{}
		}
		expr, err := nodeExpr(ctx, node.Expr)
		if err != nil {
			return err
		}
		target.TableSpec.Constraints = append(target.TableSpec.Constraints, &vitess.ConstraintDefinition{
			Name: core.EncodePhysicalConstraintName(string(node.Name)),
			Details: &vitess.CheckConstraintDefinition{
				Expr:     expr,
				Enforced: !node.NotEnforced,
			},
		})
		return nil
	case *tree.ColumnTableDef:
		if target.TableSpec == nil {
			target.TableSpec = &vitess.TableSpec{}
		}
		columnDef, err := nodeColumnTableDef(ctx, node, target.Table.SchemaQualifier.String())
		if err != nil {
			return err
		}
		assignDefaultColumnCheckConstraintName(columnDef, target.Table.Name.String(), node.Name)
		if node.Unique {
			columnDef.Type.KeyOpt = vitess.ColumnKeyOption(0)
		}
		target.TableSpec.AddColumn(columnDef)
		if err := appendAdditionalColumnCheckConstraints(ctx, target.TableSpec, target.Table.Name.String(), node); err != nil {
			return err
		}
		if node.Unique {
			indexDef, err := columnUniqueIndexDefinition(ctx, target.Table.Name.String(), node.Name, node.UniqueConstraintName, node.UniqueNullsNotDistinct)
			if err != nil {
				return err
			}
			target.TableSpec.Indexes = append(target.TableSpec.Indexes, indexDef)
		}
		if node.PrimaryKey.IsPrimaryKey {
			setPrimaryKeyConstraintTableOption(target.TableSpec, bareIdentifier(node.UniqueConstraintName))
		}
		if node.References.Table != nil {
			fkDef, err := nodeForeignKeyDefinitionFromColumnTableDef(ctx, target.Table.Name.String(), node.Name, node)
			if err != nil {
				return err
			}
			target.TableSpec.Constraints = append(target.TableSpec.Constraints, &vitess.ConstraintDefinition{
				Details: fkDef,
			})
		}
		return nil
	case *tree.ForeignKeyConstraintTableDef:
		if target.TableSpec == nil {
			target.TableSpec = &vitess.TableSpec{}
		}
		fkDef, err := nodeForeignKeyConstraintTableDef(ctx, target.Table.Name.String(), node)
		if err != nil {
			return err
		}
		target.TableSpec.Constraints = append(target.TableSpec.Constraints, &vitess.ConstraintDefinition{
			Name:    string(node.Name),
			Details: fkDef,
		})
		return nil
	case *tree.NotNullConstraintTableDef:
		if target.TableSpec == nil {
			target.TableSpec = &vitess.TableSpec{}
		}
		column := tableSpecColumn(target.TableSpec, string(node.Column))
		if column == nil {
			return errors.Errorf(`column "%s" named in NOT NULL constraint does not exist`, node.Column)
		}
		column.Type.Null = false
		column.Type.NotNull = true
		return nil
	case *tree.IndexTableDef:
		if target.TableSpec == nil {
			target.TableSpec = &vitess.TableSpec{}
		}
		indexDef, err := nodeIndexTableDef(ctx, node)
		if err != nil {
			return err
		}
		target.TableSpec.Indexes = append(target.TableSpec.Indexes, indexDef)
		return nil
	case *tree.ExcludeConstraintTableDef:
		if target.TableSpec == nil {
			target.TableSpec = &vitess.TableSpec{}
		}
		indexDef, err := nodeExcludeConstraintTableDef(ctx, target.Table.Name.String(), node)
		if err != nil {
			return err
		}
		target.TableSpec.Indexes = append(target.TableSpec.Indexes, indexDef)
		return nil
	case *tree.LikeTableDef:
		if len(node.Options) > 0 {
			return errors.Errorf("options for LIKE are not yet supported")
		}
		tableName, err := nodeTableName(ctx, &node.Name)
		if err != nil {
			return err
		}
		target.OptLike = &vitess.OptLike{
			LikeTables: []vitess.TableName{tableName},
		}
		return nil
	case *tree.UniqueConstraintTableDef:
		if target.TableSpec == nil {
			target.TableSpec = &vitess.TableSpec{}
		}
		indexTableDef := node.IndexTableDef
		if !node.PrimaryKey && bareIdentifier(indexTableDef.Name) == "" {
			indexTableDef.Name = tree.Name(defaultUniqueConstraintNameForDef(target.Table.Name.String(), node))
		}
		if node.PrimaryKey {
			setPrimaryKeyConstraintTableOption(target.TableSpec, bareIdentifier(indexTableDef.Name))
		}
		indexDef, err := nodeIndexTableDefAllowingStorageParams(ctx, &indexTableDef)
		if err != nil {
			return err
		}
		indexOptions, err := uniqueConstraintIndexOptionsForDef(node)
		if err != nil {
			return err
		}
		indexDef.Info.Unique = true
		indexDef.Info.Primary = node.PrimaryKey
		indexDef.Options = append(indexDef.Options, indexOptions...)
		// If we're setting a primary key, then we need to make sure that all of the columns are also set to NOT NULL
		if indexDef.Info.Primary {
			tableColumns := utils.SliceToMapValues(target.TableSpec.Columns, func(col *vitess.ColumnDefinition) string {
				return col.Name.String()
			})
			for _, indexedColumn := range indexDef.Fields {
				if column, ok := tableColumns[indexedColumn.Column.String()]; ok {
					column.Type.Null = false
					column.Type.NotNull = true
				}
			}
		}
		target.TableSpec.Indexes = append(target.TableSpec.Indexes, indexDef)
		return nil
	case nil:
		return nil
	default:
		return errors.Errorf("unknown table definition encountered")
	}
}

func nodeExcludeConstraintTableDef(ctx *Context, tableName string, node *tree.ExcludeConstraintTableDef) (*vitess.IndexDefinition, error) {
	if node == nil {
		return nil, nil
	}
	if indexmetadata.NormalizeAccessMethod(node.Using) != indexmetadata.AccessMethodBtree {
		return nil, errors.Errorf("EXCLUDE constraints only support btree equality operators")
	}
	if node.Predicate != nil {
		return nil, errors.Errorf("EXCLUDE constraint predicates are not yet supported")
	}
	fields, err := exclusionConstraintIndexFields(ctx, node.Columns)
	if err != nil {
		return nil, err
	}
	if len(fields) == 0 {
		return nil, errors.Errorf("EXCLUDE constraint requires at least one column")
	}
	name := bareIdentifier(node.Name)
	if name == "" {
		name = defaultExclusionConstraintName(tableName, node.Columns)
	}
	metadata := indexmetadata.Metadata{
		AccessMethod: indexmetadata.AccessMethodBtree,
		Unique:       true,
		Constraint:   "exclusion",
	}
	return &vitess.IndexDefinition{
		Info: &vitess.IndexInfo{
			Name:   vitess.NewColIdent(core.EncodePhysicalIndexName(name)),
			Unique: true,
		},
		Fields: fields,
		Options: []*vitess.IndexOption{
			indexMetadataCommentOption(metadata),
		},
	}, nil
}

func exclusionConstraintIndexFields(ctx *Context, columns tree.IndexElemList) ([]*vitess.IndexField, error) {
	indexColumns := make(tree.IndexElemList, len(columns))
	for i, column := range columns {
		op, ok := column.ExcludeOp.(tree.ComparisonOperator)
		if !ok || op != tree.EQ {
			return nil, errors.Errorf("EXCLUDE constraints only support btree equality operators")
		}
		if column.Expr != nil {
			return nil, errors.Errorf("EXCLUDE constraint expressions are not yet supported")
		}
		indexColumns[i] = column
		indexColumns[i].ExcludeOp = nil
	}
	return nodeIndexElemList(ctx, indexColumns)
}

func defaultExclusionConstraintName(tableName string, columns tree.IndexElemList) string {
	parts := make([]string, 0, len(columns)+2)
	parts = append(parts, sanitizeIndexNamePart(tableName, "table"))
	for _, column := range columns {
		part := ""
		if column.Column != "" {
			part = string(column.Column)
		} else if column.Expr != nil {
			part = tree.AsString(column.Expr)
		}
		parts = append(parts, sanitizeIndexNamePart(part, "expr"))
	}
	parts = append(parts, "excl")
	return strings.Join(parts, "_")
}

// nodeForeignKeyDefinitionFromColumnTableDef returns a vitess ForeignKeyDefinition from the specified column
// definition |node|.
func nodeForeignKeyDefinitionFromColumnTableDef(ctx *Context, childTable string, fromColumn tree.Name, node *tree.ColumnTableDef) (*vitess.ForeignKeyDefinition, error) {
	if node == nil {
		return nil, nil
	}

	references := node.References
	fkConstraintTableDef := &tree.ForeignKeyConstraintTableDef{
		Name:       references.ConstraintName,
		FromCols:   []tree.Name{fromColumn},
		Table:      *references.Table,
		ToCols:     []tree.Name{references.Col},
		Actions:    references.Actions,
		Match:      references.Match,
		Deferrable: references.Deferrable,
		Initially:  references.Initially,
	}

	return nodeForeignKeyConstraintTableDef(ctx, childTable, fkConstraintTableDef)
}

// assignTableDefs handles tree.TableDefs nodes for *vitess.DDL targets. This also sorts table defs by whether they're
// dependent on other table defs evaluating first. Some table defs, such as indexes, affect other defs, such as columns,
// and they're therefore dependent on columns being handled first.
func assignTableDefs(ctx *Context, node tree.TableDefs, target *vitess.DDL) error {
	sortedNode := make(tree.TableDefs, len(node))
	copy(sortedNode, node)
	sort.Slice(sortedNode, func(i, j int) bool {
		var cmps [2]int
		for cmpsIdx, def := range []tree.TableDef{sortedNode[i], sortedNode[j]} {
			switch def.(type) {
			case *tree.IndexTableDef:
				cmps[cmpsIdx] = 1
			case *tree.NotNullConstraintTableDef:
				cmps[cmpsIdx] = 1
			case *tree.UniqueConstraintTableDef:
				cmps[cmpsIdx] = 2
			default:
				cmps[cmpsIdx] = 0
			}
		}
		return cmps[0] < cmps[1]
	})
	for i := range sortedNode {
		if err := assignTableDef(ctx, sortedNode[i], target); err != nil {
			return err
		}
	}
	return nil
}

func appendAdditionalColumnCheckConstraints(ctx *Context, tableSpec *vitess.TableSpec, tableName string, columnDef *tree.ColumnTableDef) error {
	if tableSpec == nil || columnDef == nil || len(columnDef.CheckExprs) <= 1 {
		return nil
	}
	for _, checkExpr := range columnDef.CheckExprs[1:] {
		expr, err := nodeExpr(ctx, checkExpr.Expr)
		if err != nil {
			return err
		}
		name := string(checkExpr.ConstraintName)
		if name == "" {
			name = defaultColumnCheckConstraintName(tableName, columnDef.Name)
		}
		tableSpec.Constraints = append(tableSpec.Constraints, &vitess.ConstraintDefinition{
			Name: core.EncodePhysicalConstraintName(name),
			Details: &vitess.CheckConstraintDefinition{
				Expr:     expr,
				Enforced: true,
			},
		})
	}
	return nil
}

func tableSpecColumn(tableSpec *vitess.TableSpec, name string) *vitess.ColumnDefinition {
	if tableSpec == nil {
		return nil
	}
	for _, column := range tableSpec.Columns {
		if strings.EqualFold(column.Name.String(), name) {
			return column
		}
	}
	return nil
}

func assignDefaultColumnCheckConstraintName(columnDef *vitess.ColumnDefinition, tableName string, column tree.Name) {
	if columnDef == nil || columnDef.Type.Constraint == nil || columnDef.Type.Constraint.Name != "" {
		return
	}
	columnDef.Type.Constraint.Name = defaultColumnCheckConstraintName(tableName, column)
}
