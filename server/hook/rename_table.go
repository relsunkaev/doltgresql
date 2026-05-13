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

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/auth"
	"github.com/dolthub/doltgresql/server/rowsecurity"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// AfterTableRename handles updating various columns using the table type, alongside other validation that's unique
// to Doltgres.
func AfterTableRename(ctx *sql.Context, runner sql.StatementRunner, nodeInterface sql.Node) error {
	n, ok := nodeInterface.(*plan.RenameTable)
	if !ok {
		return errors.Errorf("RENAME TABLE post-hook expected `*plan.RenameTable` but received `%T`", nodeInterface)
	}

	oldTableName := doltdb.TableName{Schema: "public", Name: n.OldNames[0]}
	newTableName := doltdb.TableName{Schema: "public", Name: n.NewNames[0]}

	// Grab the table being altered (so we know the schema)
	sqlTable, ok := n.TableExists(ctx, n.NewNames[0])
	if !ok {
		// Views do not manifest as tables, but their auth and RLS metadata still
		// follows relation renames.
		return renameRelationMetadata(ctx, oldTableName, newTableName)
	}
	doltTable := core.SQLTableToDoltTable(sqlTable)
	if doltTable == nil {
		return renameRelationMetadata(ctx, oldTableName, newTableName)
	}
	_, root, err := core.GetRootFromContext(ctx)
	if err != nil {
		return err
	}
	tableName := doltTable.TableName()
	oldTableName.Schema = tableName.Schema
	oldTableName.Name = n.OldNames[0]
	newTableName.Schema = tableName.Schema
	newTableName.Name = n.NewNames[0]
	if err = renameRelationMetadata(ctx, oldTableName, newTableName); err != nil {
		return err
	}
	tableAsType := id.NewType(oldTableName.Schema, oldTableName.Name)
	allTableNames, err := root.GetAllTableNames(ctx, false)
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
			// The ALTER updates the type on the schema since it still has the old one
			alterStr := fmt.Sprintf(`ALTER TABLE "%s"."%s" ALTER COLUMN "%s" TYPE "%s"."%s";`,
				otherTableName.Schema, otherTableName.Name, otherCol.Name, oldTableName.Schema, n.NewNames[0])
			// We run the statement as though it's interpreted since we're running new statements inside the original
			_, err = sql.RunInterpreted(ctx, func(subCtx *sql.Context) ([]sql.Row, error) {
				_, rowIter, _, err := runner.QueryWithBindings(subCtx, alterStr, nil, nil, nil)
				if err != nil {
					return nil, err
				}
				return sql.RowIterToRows(subCtx, rowIter)
			})
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func renameRelationMetadata(ctx *sql.Context, oldTableName doltdb.TableName, newTableName doltdb.TableName) error {
	var err error
	auth.LockWrite(func() {
		auth.RenameRelationOwner(oldTableName, newTableName)
		auth.RenameTablePrivileges(oldTableName, newTableName)
		err = auth.PersistChanges()
	})
	if err != nil {
		return err
	}
	rowsecurity.RenameTable(uint32(ctx.Session.ID()), ctx.GetCurrentDatabase(), oldTableName.Schema, oldTableName.Name, newTableName.Schema, newTableName.Name)
	return nil
}
