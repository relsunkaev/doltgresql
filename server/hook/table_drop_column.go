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

package hook

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
	"github.com/dolthub/doltgresql/server/comments"
	"github.com/dolthub/doltgresql/server/tablemetadata"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// BeforeTableDropColumn rejects schema changes that would leave foreign-key
// metadata referring to a removed parent column.
func BeforeTableDropColumn(ctx *sql.Context, _ sql.StatementRunner, nodeInterface sql.Node) (sql.Node, error) {
	n, ok := nodeInterface.(*plan.DropColumn)
	if !ok {
		return nil, errors.Errorf("DROP COLUMN pre-hook expected `*plan.DropColumn` but received `%T`", nodeInterface)
	}
	doltTable := core.SQLNodeToDoltTable(n.Table)
	if doltTable == nil {
		return n, nil
	}
	if dependentColumn, ok := generatedColumnDependingOnDroppedColumn(ctx, n.TargetSchema(), n.Column); ok {
		tableName := doltTable.TableName()
		return nil, pgerror.Newf(pgcode.DependentObjectsStillExist, `cannot drop column %s of table %s because generated column %s depends on it`, n.Column, tableName.Name, dependentColumn)
	}
	fkTable, ok := any(doltTable).(sql.ForeignKeyTable)
	if !ok {
		return n, nil
	}
	parentFks, err := fkTable.GetReferencedForeignKeys(ctx)
	if err != nil {
		return nil, err
	}
	for _, fk := range parentFks {
		for _, parentColumn := range fk.ParentColumns {
			if strings.EqualFold(parentColumn, n.Column) {
				tableName := doltTable.TableName()
				return nil, pgerror.Newf(pgcode.DependentObjectsStillExist, "cannot drop column %s of table %s because other objects depend on it", n.Column, tableName.Name)
			}
		}
	}
	return n, nil
}

func generatedColumnDependingOnDroppedColumn(ctx *sql.Context, schema sql.Schema, droppedColumn string) (string, bool) {
	for _, col := range schema {
		if col.Generated == nil || strings.EqualFold(col.Name, droppedColumn) {
			continue
		}
		if plan.ColumnReferencedInDefaultValueExpression(ctx, col.Generated, droppedColumn) {
			return col.Name, true
		}
	}
	return "", false
}

// AfterTableDropColumn handles updating various table columns, alongside other validation that's unique to Doltgres.
func AfterTableDropColumn(ctx *sql.Context, runner sql.StatementRunner, nodeInterface sql.Node) error {
	n, ok := nodeInterface.(*plan.DropColumn)
	if !ok {
		return errors.Errorf("DROP COLUMN post-hook expected `*plan.DropColumn` but received `%T`", nodeInterface)
	}

	// Grab the table being altered
	doltTable := core.SQLNodeToDoltTable(n.Table)
	if doltTable == nil {
		// If this table isn't a Dolt table then we don't have anything to do
		return nil
	}
	_, root, err := core.GetRootFromContext(ctx)
	if err != nil {
		return err
	}
	tableName := doltTable.TableName()
	tableAsType := id.NewType(tableName.Schema, tableName.Name)
	allTableNames, err := root.GetAllTableNames(ctx, false)
	if err != nil {
		return err
	}
	sch := n.TargetSchema()
	clearDroppedColumnComment(tableName.Schema, tableName.Name, n.Column, sch)
	if err = recordDroppedColumnMetadata(ctx, n, sch); err != nil {
		return err
	}
	if err = dropSequencesOwnedByColumn(ctx, tableName, n.Column); err != nil {
		return err
	}
	updatedTable, err := alteredTableFromNode(ctx, n.Database(), n.Table)
	if err != nil {
		return err
	}
	updatedType, err := tableRowTypeFromSQLTable(ctx, tableName, updatedTable)
	if err != nil {
		return err
	}

	for _, otherTableName := range allTableNames {
		if doltdb.IsSystemTable(otherTableName) {
			// System tables don't use any table types
			continue
		}
		otherTable, ok, err := root.GetTable(ctx, otherTableName)
		if err != nil {
			return err
		}
		if !ok {
			return errors.Errorf("root returned table name `%s` but it could not be found?", otherTableName.String())
		}
		otherTableSch, err := otherTable.GetSchema(ctx)
		if err != nil {
			return err
		}
		for _, otherCol := range otherTableSch.GetAllCols().GetColumns() {
			colType := otherCol.TypeInfo.ToSqlType()
			dgtype, ok := colType.(*pgtypes.DoltgresType)
			if !ok {
				// If this isn't a Doltgres type, then it can't be a table type so we can ignore it
				continue
			}
			if dgtype.ID != tableAsType {
				// This column isn't our table type, so we can ignore it
				continue
			}
			// Build the UPDATE statement that we'll run for this table
			trimIdx := -1
			for i, col := range sch {
				if col.Name == n.Column {
					trimIdx = i
					break
				}
			}
			if trimIdx == -1 {
				return errors.New("DROP COLUMN post-hook could not find the index of the column to remove")
			}
			if err = updateDependentColumnType(ctx, databaseNameForSQLDatabase(n.Database()), otherTableName, otherCol.Name, updatedType); err != nil {
				return err
			}
			// The UPDATE changes the values in the table
			updateStr := fmt.Sprintf(`UPDATE "%s"."%s" SET "%s" = dolt_recordtrim("%s", %d)::"%s"."%s";`,
				otherTableName.Schema, otherTableName.Name, otherCol.Name, otherCol.Name, trimIdx, tableName.Schema, tableName.Name)
			// We run the statements as though they were interpreted since we're running new statements inside the original
			_, err = sql.RunInterpreted(ctx, func(subCtx *sql.Context) ([]sql.Row, error) {
				_, rowIter, _, err := runner.QueryWithBindings(subCtx, updateStr, nil, nil, nil)
				if err != nil {
					return nil, err
				}
				_, err = sql.RowIterToRows(subCtx, rowIter)
				if err != nil {
					return nil, err
				}
				return nil, nil
			})
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func clearDroppedColumnComment(schemaName string, tableName string, columnName string, sch sql.Schema) {
	for idx, col := range sch {
		if col.Name != columnName {
			continue
		}
		comments.Set(comments.Key{
			ObjOID:   id.Cache().ToOID(id.NewTable(schemaName, tableName).AsId()),
			ClassOID: comments.ClassOID("pg_class"),
			ObjSubID: int32(idx + 1),
		}, nil)
		return
	}
}

func recordDroppedColumnMetadata(ctx *sql.Context, n *plan.DropColumn, sch sql.Schema) error {
	table, err := alteredTableFromNode(ctx, n.Database(), n.Table)
	if err != nil {
		return err
	}
	commented, ok := table.(sql.CommentedTable)
	if !ok {
		return sql.ErrAlterTableCommentNotSupported.New(table.Name())
	}
	attnum := droppedColumnAttNum(commented.Comment(), sch, n.Column)
	if attnum == 0 {
		return errors.New("DROP COLUMN post-hook could not find the index of the column to remove")
	}
	alterable, ok := table.(sql.CommentAlterableTable)
	if !ok {
		return sql.ErrAlterTableCommentNotSupported.New(table.Name())
	}
	return alterable.ModifyComment(ctx, tablemetadata.AddDroppedColumn(commented.Comment(), n.Column, attnum))
}

func droppedColumnAttNum(comment string, sch sql.Schema, columnName string) int16 {
	droppedColumns := tablemetadata.DroppedColumns(comment)
	nextDropped := 0
	attnum := int16(0)
	for _, col := range sch {
		if col.HiddenSystem {
			continue
		}
		attnum++
		for nextDropped < len(droppedColumns) && droppedColumns[nextDropped].AttNum == attnum {
			nextDropped++
			attnum++
		}
		if col.Name == columnName {
			return attnum
		}
	}
	return 0
}
