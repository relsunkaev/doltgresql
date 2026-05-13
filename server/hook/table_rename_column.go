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
	"github.com/dolthub/doltgresql/postgres/parser/parser"
	"github.com/dolthub/doltgresql/postgres/parser/sem/tree"
	"github.com/dolthub/doltgresql/server/auth"
	"github.com/dolthub/doltgresql/server/rowsecurity"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// AfterTableRenameColumn handles updating various table columns, alongside other validation that's unique to Doltgres.
func AfterTableRenameColumn(ctx *sql.Context, runner sql.StatementRunner, nodeInterface sql.Node) error {
	n, ok := nodeInterface.(*plan.RenameColumn)
	if !ok {
		return errors.Errorf("RENAME COLUMN post-hook expected `*plan.RenameColumn` but received `%T`", nodeInterface)
	}
	if n.ColumnName == n.NewColumnName {
		return nil
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
	freshTable, ok, err := n.Database().GetTableInsensitive(ctx, tableName.Name)
	if err != nil {
		return err
	}
	if !ok {
		return sql.ErrTableNotFound.New(tableName.Name)
	}
	if err = rewriteRenamedColumnCheckConstraints(ctx, freshTable, n.ColumnName, n.NewColumnName); err != nil {
		return err
	}
	var persistErr error
	auth.LockWrite(func() {
		auth.RenameTableColumnPrivileges(tableName, n.ColumnName, n.NewColumnName)
		persistErr = auth.PersistChanges()
	})
	if persistErr != nil {
		return persistErr
	}
	rowsecurity.RenameColumn(ctx.GetCurrentDatabase(), tableName.Schema, tableName.Name, n.ColumnName, n.NewColumnName)
	tableAsType := id.NewType(tableName.Schema, tableName.Name)
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
				otherTableName.Schema, otherTableName.Name, otherCol.Name, tableName.Schema, tableName.Name)
			// We run the statement as though it were interpreted since we're running new statements inside the original
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

func rewriteRenamedColumnCheckConstraints(ctx *sql.Context, table sql.Table, oldColumnName, newColumnName string) error {
	checkTable, ok := table.(sql.CheckTable)
	if !ok {
		checkTable, ok = sql.GetUnderlyingTable(table).(sql.CheckTable)
	}
	if !ok {
		return nil
	}
	checkAlterable, ok := table.(sql.CheckAlterableTable)
	if !ok {
		checkAlterable, ok = sql.GetUnderlyingTable(table).(sql.CheckAlterableTable)
	}
	if !ok {
		return nil
	}
	checks, err := checkTable.GetChecks(ctx)
	if err != nil {
		return err
	}
	for _, check := range checks {
		rewritten, changed, err := rewriteCheckConstraintColumnReference(check, oldColumnName, newColumnName)
		if err != nil {
			return err
		}
		if !changed {
			continue
		}
		if err = checkAlterable.DropCheck(ctx, check.Name); err != nil {
			return err
		}
		if err = checkAlterable.CreateCheck(ctx, &rewritten); err != nil {
			return err
		}
	}
	return nil
}

func rewriteCheckConstraintColumnReference(check sql.CheckDefinition, oldColumnName, newColumnName string) (sql.CheckDefinition, bool, error) {
	if !strings.Contains(strings.ToLower(check.CheckExpression), strings.ToLower(oldColumnName)) {
		return check, false, nil
	}
	statements, err := parser.Parse("SELECT " + check.CheckExpression)
	if err != nil || len(statements) != 1 {
		return check, false, err
	}
	selectStmt, ok := statements[0].AST.(*tree.Select)
	if !ok {
		return check, false, nil
	}
	selectClause, ok := selectStmt.Select.(*tree.SelectClause)
	if !ok || len(selectClause.Exprs) != 1 {
		return check, false, nil
	}
	rewrittenExpr, changed, err := rewriteColumnReferenceExpr(selectClause.Exprs[0].Expr, oldColumnName, newColumnName)
	if err != nil || !changed {
		return check, changed, err
	}
	check.CheckExpression = rewrittenExpr.String()
	return check, true, nil
}

func rewriteColumnReferenceExpr(expr tree.Expr, oldColumnName, newColumnName string) (tree.Expr, bool, error) {
	if expr == nil {
		return nil, false, nil
	}
	changed := false
	rewritten, err := tree.SimpleVisit(expr, func(expr tree.Expr) (bool, tree.Expr, error) {
		switch typedExpr := expr.(type) {
		case *tree.UnresolvedName:
			if typedExpr.NumParts == 0 || !strings.EqualFold(typedExpr.Parts[0], oldColumnName) {
				return true, expr, nil
			}
			rewrittenName := *typedExpr
			rewrittenName.Parts[0] = newColumnName
			changed = true
			return true, &rewrittenName, nil
		case *tree.ColumnItem:
			if !strings.EqualFold(string(typedExpr.ColumnName), oldColumnName) {
				return true, expr, nil
			}
			rewrittenColumn := *typedExpr
			rewrittenColumn.ColumnName = tree.Name(newColumnName)
			changed = true
			return true, &rewrittenColumn, nil
		default:
			return true, expr, nil
		}
	})
	return rewritten, changed, err
}
