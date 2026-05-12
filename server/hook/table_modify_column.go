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
	"io"
	"reflect"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// BeforeTableModifyColumn handles validation that's unique to Doltgres.
func BeforeTableModifyColumn(ctx *sql.Context, runner sql.StatementRunner, nodeInterface sql.Node) (sql.Node, error) {
	n, ok := nodeInterface.(*plan.ModifyColumn)
	if !ok {
		return nil, errors.Errorf("MODIFY COLUMN pre-hook expected `*plan.ModifyColumn` but received `%T`", nodeInterface)
	}

	// PostgreSQL checks row-type dependents for ALTER COLUMN TYPE even when
	// the requested type matches the existing type.
	doltTable := core.SQLNodeToDoltTable(n.Table)
	if doltTable == nil {
		// If this table isn't a Dolt table then we don't have anything to do
		return n, nil
	}
	_, root, err := core.GetRootFromContext(ctx)
	if err != nil {
		return n, nil
	}
	tableName := doltTable.TableName()
	if err = validateDomainAlterColumnExistingRows(ctx, runner, tableName, n.NewColumn()); err != nil {
		return nil, err
	}
	if modifiesOnlyNullability(ctx, n) {
		return n, nil
	}
	tableAsType := id.NewType(tableName.Schema, tableName.Name)
	allTableNames, err := root.GetAllTableNames(ctx, false)
	if err != nil {
		return nil, err
	}

	for _, otherTableName := range allTableNames {
		if doltdb.IsSystemTable(otherTableName) {
			// System tables don't use any table types
			continue
		}
		otherTable, ok, err := root.GetTable(ctx, otherTableName)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, errors.Errorf("root returned table name `%s` but it could not be found?", otherTableName.String())
		}
		otherTableSch, err := otherTable.GetSchema(ctx)
		if err != nil {
			return nil, err
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
			return nil, errors.Errorf(`cannot alter table "%s" because column "%s.%s" uses its row type`,
				tableName.Name, otherTableName.Name, otherCol.Name)
		}
	}
	return n, nil
}

func modifiesOnlyNullability(ctx *sql.Context, n *plan.ModifyColumn) bool {
	newColumn := n.NewColumn()
	if newColumn == nil {
		return false
	}
	schema := n.TargetSchema()
	if len(schema) == 0 {
		schema = n.Table.Schema(ctx)
	}
	for _, existingColumn := range schema {
		if strings.EqualFold(existingColumn.Name, n.Column()) {
			return existingColumn.Nullable != newColumn.Nullable && columnsEqualExceptNullability(existingColumn, newColumn)
		}
	}
	return false
}

func columnsEqualExceptNullability(left *sql.Column, right *sql.Column) bool {
	return strings.EqualFold(left.Name, right.Name) &&
		strings.EqualFold(left.Source, right.Source) &&
		strings.EqualFold(left.DatabaseSource, right.DatabaseSource) &&
		left.Type.Equals(right.Type) &&
		reflect.DeepEqual(left.Default, right.Default) &&
		reflect.DeepEqual(left.Generated, right.Generated) &&
		reflect.DeepEqual(left.OnUpdate, right.OnUpdate) &&
		left.Comment == right.Comment &&
		left.Extra == right.Extra &&
		left.PrimaryKey == right.PrimaryKey &&
		left.Virtual == right.Virtual &&
		left.AutoIncrement == right.AutoIncrement &&
		left.Hidden == right.Hidden &&
		left.HiddenSystem == right.HiddenSystem
}

func validateDomainAlterColumnExistingRows(ctx *sql.Context, runner sql.StatementRunner, tableName doltdb.TableName, newColumn *sql.Column) error {
	if newColumn == nil {
		return nil
	}
	domainType, ok := newColumn.Type.(*pgtypes.DoltgresType)
	if !ok || domainType.TypType != pgtypes.TypeType_Domain {
		return nil
	}
	if runner == nil {
		return errors.New("statement runner is not available for ALTER COLUMN TYPE domain validation")
	}
	query := fmt.Sprintf(
		"SELECT %s::%s FROM %s;",
		quoteModifyColumnIdentifier(newColumn.Name),
		quoteModifyColumnQualifiedIdentifier(domainType.Schema(), domainType.Name()),
		quoteModifyColumnQualifiedIdentifier(tableName.Schema, tableName.Name),
	)
	_, err := sql.RunInterpreted(ctx, func(subCtx *sql.Context) ([]sql.Row, error) {
		_, rowIter, _, err := runner.QueryWithBindings(subCtx, query, nil, nil, nil)
		if err != nil {
			return nil, err
		}
		defer rowIter.Close(subCtx)
		for {
			_, err = rowIter.Next(subCtx)
			if err == io.EOF {
				return nil, nil
			}
			if err != nil {
				return nil, err
			}
		}
	})
	return err
}

func quoteModifyColumnQualifiedIdentifier(schema string, name string) string {
	if schema == "" {
		return quoteModifyColumnIdentifier(name)
	}
	return quoteModifyColumnIdentifier(schema) + "." + quoteModifyColumnIdentifier(name)
}

func quoteModifyColumnIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}
