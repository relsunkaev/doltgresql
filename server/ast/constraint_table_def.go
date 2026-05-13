// Copyright 2024 Dolthub, Inc.
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
	"github.com/dolthub/doltgresql/server/indexmetadata"
	"github.com/dolthub/doltgresql/server/tablemetadata"
)

// nodeCheckConstraintTableDef converts a tree.CheckConstraintTableDef instance
// into a vitess.DDL instance that can be executed by GMS. |tableName| identifies
// the table being altered, and |ifExists| indicates whether the IF EXISTS clause
// was specified.
func nodeCheckConstraintTableDef(
	ctx *Context,
	node *tree.CheckConstraintTableDef,
	tableName vitess.TableName,
	ifExists bool) (*vitess.DDL, error) {

	if node.NoInherit {
		return nil, errors.Errorf("NO INHERIT is not yet supported for check constraints")
	}

	expr, err := nodeExpr(ctx, node.Expr)
	if err != nil {
		return nil, err
	}

	return &vitess.DDL{
		Action:           "alter",
		Table:            tableName,
		IfExists:         ifExists,
		ConstraintAction: "add",
		TableSpec: &vitess.TableSpec{
			Constraints: []*vitess.ConstraintDefinition{
				{
					Name: node.Name.String(),
					Details: &vitess.CheckConstraintDefinition{
						Expr:     expr,
						Enforced: true,
					},
				},
			},
		},
	}, nil
}

// nodeAlterTableDropConstraint converts a tree.AlterTableDropConstraint instance
// into a vitess.DDL instance that can be executed by GMS. |tableName| identifies
// the table being altered, and |ifExists| indicates whether the IF EXISTS clause
// was specified.
func nodeAlterTableDropConstraint(
	ctx *Context,
	node *tree.AlterTableDropConstraint,
	tableName vitess.TableName,
	ifExists bool) (*vitess.DDL, error) {

	if node.DropBehavior == tree.DropCascade {
		return nil, errors.Errorf("CASCADE is not yet supported for drop constraint")
	}

	return &vitess.DDL{
		Action:             "alter",
		Table:              tableName,
		IfExists:           ifExists,
		ConstraintAction:   "drop",
		ConstraintIfExists: node.IfExists,
		TableSpec: &vitess.TableSpec{
			Constraints: []*vitess.ConstraintDefinition{
				{Name: node.Constraint.String()},
			},
		},
	}, nil
}

// nodeUniqueConstraintTableDef converts a tree.UniqueConstraintTableDef instance
// into a vitess.DDL instance that can be executed by GMS. |tableName| identifies
// the table being altered, and |ifExists| indicates whether the IF EXISTS clause
// was specified.
func nodeUniqueConstraintTableDef(
	ctx *Context,
	node *tree.UniqueConstraintTableDef,
	tableName vitess.TableName,
	ifExists bool) (*vitess.DDL, error) {

	indexTableDef := node.IndexTableDef
	if !node.PrimaryKey && bareIdentifier(indexTableDef.Name) == "" {
		indexTableDef.Name = tree.Name(defaultUniqueConstraintNameForDef(tableName.Name.String(), node))
	}
	indexDef, err := nodeIndexTableDefAllowingStorageParams(ctx, &indexTableDef)
	if err != nil {
		return nil, err
	}
	indexOptions, err := uniqueConstraintIndexOptionsForDef(node)
	if err != nil {
		return nil, err
	}

	indexType := "unique"
	if node.PrimaryKey {
		indexType = "primary"
	}

	ddl := &vitess.DDL{
		Action:   "alter",
		Table:    tableName,
		IfExists: ifExists,
		IndexSpec: &vitess.IndexSpec{
			ToName:  indexDef.Info.Name,
			Action:  "create",
			Type:    indexType,
			Fields:  indexDef.Fields,
			Options: indexOptions,
		},
	}
	return ddl, nil
}

func uniqueConstraintIndexOptions(nullsNotDistinct bool) []*vitess.IndexOption {
	if !nullsNotDistinct {
		return nil
	}
	metadata := indexmetadata.Metadata{
		AccessMethod:     indexmetadata.AccessMethodBtree,
		NullsNotDistinct: true,
	}
	return []*vitess.IndexOption{indexMetadataCommentOption(metadata)}
}

func uniqueConstraintIndexOptionsForDef(node *tree.UniqueConstraintTableDef) ([]*vitess.IndexOption, error) {
	includeColumns, err := nodeIndexIncludeColumns(node.IndexParams.IncludeColumns)
	if err != nil {
		return nil, err
	}
	relOptions, err := nodeIndexRelOptions(node.IndexParams.StorageParams)
	if err != nil {
		return nil, err
	}
	if len(includeColumns) == 0 && len(relOptions) == 0 && !node.NullsNotDistinct {
		return nil, nil
	}
	metadata := indexmetadata.Metadata{
		AccessMethod:     indexmetadata.AccessMethodBtree,
		IncludeColumns:   includeColumns,
		RelOptions:       relOptions,
		NullsNotDistinct: node.NullsNotDistinct,
	}
	return []*vitess.IndexOption{indexMetadataCommentOption(metadata)}, nil
}

func primaryKeyConstraintMetadataDDL(tableName vitess.TableName, ifExists bool, name string) *vitess.DDL {
	return &vitess.DDL{
		Action:   "alter",
		Table:    tableName,
		IfExists: ifExists,
		AlterCommentSpec: &vitess.AlterCommentSpec{
			Comment: tablemetadata.SetPrimaryKeyConstraintName("", name),
		},
	}
}

func setPrimaryKeyConstraintTableOption(tableSpec *vitess.TableSpec, name string) {
	if tableSpec == nil || name == "" {
		return
	}
	setTableMetadataCommentOption(tableSpec, func(comment string) string {
		return tablemetadata.SetPrimaryKeyConstraintName(comment, name)
	})
}

func setTableMetadataCommentOption(tableSpec *vitess.TableSpec, update func(string) string) {
	if tableSpec == nil {
		return
	}
	comment := update("")
	for _, option := range tableSpec.TableOpts {
		if strings.EqualFold(option.Name, "comment") {
			option.Value = update(option.Value)
			return
		}
	}
	if comment == "" {
		return
	}
	tableSpec.TableOpts = append(tableSpec.TableOpts, &vitess.TableOption{
		Name:  "comment",
		Value: comment,
	})
}

func defaultUniqueConstraintName(tableName string, columns tree.IndexElemList) string {
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
	parts = append(parts, "key")
	return strings.Join(parts, "_")
}

func defaultUniqueConstraintNameForDef(tableName string, node *tree.UniqueConstraintTableDef) string {
	columns := make(tree.IndexElemList, 0, len(node.Columns)+len(node.IndexParams.IncludeColumns))
	columns = append(columns, node.Columns...)
	columns = append(columns, node.IndexParams.IncludeColumns...)
	return defaultUniqueConstraintName(tableName, columns)
}

func defaultColumnCheckConstraintName(tableName string, column tree.Name) string {
	return strings.Join([]string{
		sanitizeIndexNamePart(tableName, "table"),
		sanitizeIndexNamePart(string(column), "column"),
		"check",
	}, "_")
}

func columnUniqueIndexDefinition(ctx *Context, tableName string, column tree.Name, constraintName tree.Name, nullsNotDistinct bool) (*vitess.IndexDefinition, error) {
	columns := tree.IndexElemList{{Column: column}}
	fields, err := nodeIndexElemList(ctx, columns)
	if err != nil {
		return nil, err
	}
	indexName := bareIdentifier(constraintName)
	if indexName == "" {
		indexName = defaultUniqueConstraintName(tableName, columns)
	}
	return &vitess.IndexDefinition{
		Info: &vitess.IndexInfo{
			Type:   "unique",
			Name:   vitess.NewColIdent(indexName),
			Unique: true,
		},
		Fields:  fields,
		Options: uniqueConstraintIndexOptions(nullsNotDistinct),
	}, nil
}

func indexMetadataCommentOption(metadata indexmetadata.Metadata) *vitess.IndexOption {
	return &vitess.IndexOption{
		Name:  vitess.KeywordString(vitess.COMMENT_KEYWORD),
		Value: vitess.NewStrVal([]byte(indexmetadata.EncodeComment(metadata))),
	}
}
