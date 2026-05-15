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

	"github.com/dolthub/doltgresql/core"
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

	expr, err := nodeExpr(ctx, node.Expr)
	if err != nil {
		return nil, err
	}

	name := node.Name
	if node.NoInherit {
		name = tree.Name(EncodeNoInheritCheckConstraintName(string(name)))
	}

	return &vitess.DDL{
		Action:           "alter",
		Table:            tableName,
		IfExists:         ifExists,
		ConstraintAction: "add",
		TableSpec: &vitess.TableSpec{
			Constraints: []*vitess.ConstraintDefinition{
				{
					Name: physicalCheckConstraintName(name),
					Details: &vitess.CheckConstraintDefinition{
						Expr:     expr,
						Enforced: !node.NotEnforced,
					},
				},
			},
		},
	}, nil
}

func physicalCheckConstraintName(name tree.Name) string {
	logicalName := name.String()
	if HasCheckConstraintNameOptionMarker(string(name)) {
		logicalName = string(name)
	}
	return core.EncodePhysicalConstraintName(logicalName)
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

	constraintName := core.EncodePhysicalConstraintName(node.Constraint.String())
	if node.DropBehavior == tree.DropCascade {
		constraintName = EncodeDropConstraintCascade(constraintName)
	}

	return &vitess.DDL{
		Action:             "alter",
		Table:              tableName,
		IfExists:           ifExists,
		ConstraintAction:   "drop",
		ConstraintIfExists: node.IfExists,
		TableSpec: &vitess.TableSpec{
			Constraints: []*vitess.ConstraintDefinition{
				{Name: constraintName},
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
	if node.PrimaryKey && uniqueConstraintIsDeferrable(node.Deferrable) && bareIdentifier(indexTableDef.Name) == "" {
		indexTableDef.Name = tree.Name(defaultPrimaryKeyConstraintName(tableName.Name.String()))
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
	if uniqueConstraintIsDeferrable(node.Deferrable) {
		indexType = ""
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

func uniqueConstraintIndexOptions(nullsNotDistinct bool, primary bool, deferrable tree.DeferrableMode, initially tree.InitiallyMode) []*vitess.IndexOption {
	if !nullsNotDistinct && !uniqueConstraintIsDeferrable(deferrable) {
		return nil
	}
	metadata := indexmetadata.Metadata{
		AccessMethod:     indexmetadata.AccessMethodBtree,
		NullsNotDistinct: nullsNotDistinct,
	}
	applyUniqueConstraintTimingMetadata(&metadata, primary, deferrable, initially)
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
	withoutOverlaps, err := uniqueConstraintWithoutOverlapsColumn(node.Columns)
	if err != nil {
		return nil, err
	}
	isDeferrable := uniqueConstraintIsDeferrable(node.Deferrable)
	if len(includeColumns) == 0 && len(relOptions) == 0 && !node.NullsNotDistinct && !isDeferrable && withoutOverlaps == "" {
		return nil, nil
	}
	metadata := indexmetadata.Metadata{
		AccessMethod:     indexmetadata.AccessMethodBtree,
		IncludeColumns:   includeColumns,
		RelOptions:       relOptions,
		NullsNotDistinct: node.NullsNotDistinct,
		WithoutOverlaps:  withoutOverlaps,
	}
	applyUniqueConstraintTimingMetadata(&metadata, node.PrimaryKey, node.Deferrable, node.Initially)
	return []*vitess.IndexOption{indexMetadataCommentOption(metadata)}, nil
}

func uniqueConstraintWithoutOverlapsColumn(columns tree.IndexElemList) (string, error) {
	var withoutOverlaps string
	for _, column := range columns {
		if !column.WithoutOverlaps {
			continue
		}
		if withoutOverlaps != "" {
			return "", errors.Errorf("only one WITHOUT OVERLAPS column is supported")
		}
		if column.Expr != nil || column.Column == "" {
			return "", errors.Errorf("WITHOUT OVERLAPS expression indexes are not yet supported")
		}
		withoutOverlaps = string(column.Column)
	}
	return withoutOverlaps, nil
}

func uniqueConstraintIsDeferrable(deferrable tree.DeferrableMode) bool {
	return deferrable == tree.Deferrable
}

func uniqueConstraintInitiallyDeferred(initially tree.InitiallyMode) bool {
	return initially == tree.InitiallyDeferred
}

func applyUniqueConstraintTimingMetadata(metadata *indexmetadata.Metadata, primary bool, deferrable tree.DeferrableMode, initially tree.InitiallyMode) {
	if metadata == nil || !uniqueConstraintIsDeferrable(deferrable) {
		return
	}
	metadata.Unique = true
	metadata.Deferrable = true
	metadata.InitiallyDeferred = uniqueConstraintInitiallyDeferred(initially)
	if primary {
		metadata.Constraint = "primary"
	} else {
		metadata.Constraint = "unique"
	}
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

func defaultPrimaryKeyConstraintName(tableName string) string {
	return sanitizeIndexNamePart(tableName, "table") + "_pkey"
}

func defaultColumnCheckConstraintName(tableName string, column tree.Name) string {
	return strings.Join([]string{
		sanitizeIndexNamePart(tableName, "table"),
		sanitizeIndexNamePart(string(column), "column"),
		"check",
	}, "_")
}

func columnUniqueIndexDefinition(ctx *Context, tableName string, column tree.Name, constraintName tree.Name, nullsNotDistinct bool, primary bool, deferrable tree.DeferrableMode, initially tree.InitiallyMode) (*vitess.IndexDefinition, error) {
	columns := tree.IndexElemList{{Column: column}}
	fields, err := nodeIndexElemList(ctx, columns)
	if err != nil {
		return nil, err
	}
	indexName := bareIdentifier(constraintName)
	if indexName == "" {
		if primary {
			indexName = defaultPrimaryKeyConstraintName(tableName)
		} else {
			indexName = defaultUniqueConstraintName(tableName, columns)
		}
	}
	nativeUnique := !uniqueConstraintIsDeferrable(deferrable)
	indexType := "unique"
	if !nativeUnique {
		indexType = ""
	}
	return &vitess.IndexDefinition{
		Info: &vitess.IndexInfo{
			Type:    indexType,
			Name:    vitess.NewColIdent(indexName),
			Unique:  nativeUnique,
			Primary: primary && nativeUnique,
		},
		Fields:  fields,
		Options: uniqueConstraintIndexOptions(nullsNotDistinct, primary, deferrable, initially),
	}, nil
}

func indexMetadataCommentOption(metadata indexmetadata.Metadata) *vitess.IndexOption {
	return &vitess.IndexOption{
		Name:  vitess.KeywordString(vitess.COMMENT_KEYWORD),
		Value: vitess.NewStrVal([]byte(indexmetadata.EncodeComment(metadata))),
	}
}
