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

package information_schema

import (
	"sort"

	"github.com/dolthub/go-mysql-server/sql"
	gmsinformation "github.com/dolthub/go-mysql-server/sql/information_schema"

	"github.com/dolthub/doltgresql/server/auth"
	"github.com/dolthub/doltgresql/server/functions"
)

const RoutinePrivilegesTableName = "routine_privileges"

func newTablePrivilegesTable() *gmsinformation.InformationSchemaTable {
	return &gmsinformation.InformationSchemaTable{
		TableName:   gmsinformation.TablePrivilegesTableName,
		TableSchema: tablePrivilegesSchema,
		Reader:      tablePrivilegesRowIter,
	}
}

func newColumnPrivilegesTable() *gmsinformation.InformationSchemaTable {
	return &gmsinformation.InformationSchemaTable{
		TableName:   gmsinformation.ColumnPrivilegesTableName,
		TableSchema: columnPrivilegesSchema,
		Reader:      columnPrivilegesRowIter,
	}
}

func newRoutinePrivilegesTable() *gmsinformation.InformationSchemaTable {
	return &gmsinformation.InformationSchemaTable{
		TableName:   RoutinePrivilegesTableName,
		TableSchema: routinePrivilegesSchema,
		Reader:      routinePrivilegesRowIter,
	}
}

var tablePrivilegesSchema = sql.Schema{
	{Name: "grantee", Type: sql_identifier, Default: nil, Nullable: false, Source: gmsinformation.TablePrivilegesTableName},
	{Name: "table_catalog", Type: sql_identifier, Default: nil, Nullable: false, Source: gmsinformation.TablePrivilegesTableName},
	{Name: "table_schema", Type: sql_identifier, Default: nil, Nullable: false, Source: gmsinformation.TablePrivilegesTableName},
	{Name: "table_name", Type: sql_identifier, Default: nil, Nullable: false, Source: gmsinformation.TablePrivilegesTableName},
	{Name: "privilege_type", Type: character_data, Default: nil, Nullable: false, Source: gmsinformation.TablePrivilegesTableName},
	{Name: "is_grantable", Type: yes_or_no, Default: nil, Nullable: false, Source: gmsinformation.TablePrivilegesTableName},
}

var columnPrivilegesSchema = sql.Schema{
	{Name: "grantee", Type: sql_identifier, Default: nil, Nullable: false, Source: gmsinformation.ColumnPrivilegesTableName},
	{Name: "table_catalog", Type: sql_identifier, Default: nil, Nullable: false, Source: gmsinformation.ColumnPrivilegesTableName},
	{Name: "table_schema", Type: sql_identifier, Default: nil, Nullable: false, Source: gmsinformation.ColumnPrivilegesTableName},
	{Name: "table_name", Type: sql_identifier, Default: nil, Nullable: false, Source: gmsinformation.ColumnPrivilegesTableName},
	{Name: "column_name", Type: sql_identifier, Default: nil, Nullable: false, Source: gmsinformation.ColumnPrivilegesTableName},
	{Name: "privilege_type", Type: character_data, Default: nil, Nullable: false, Source: gmsinformation.ColumnPrivilegesTableName},
	{Name: "is_grantable", Type: yes_or_no, Default: nil, Nullable: false, Source: gmsinformation.ColumnPrivilegesTableName},
}

var routinePrivilegesSchema = sql.Schema{
	{Name: "grantor", Type: sql_identifier, Default: nil, Nullable: false, Source: RoutinePrivilegesTableName},
	{Name: "grantee", Type: sql_identifier, Default: nil, Nullable: false, Source: RoutinePrivilegesTableName},
	{Name: "specific_catalog", Type: sql_identifier, Default: nil, Nullable: false, Source: RoutinePrivilegesTableName},
	{Name: "specific_schema", Type: sql_identifier, Default: nil, Nullable: false, Source: RoutinePrivilegesTableName},
	{Name: "specific_name", Type: sql_identifier, Default: nil, Nullable: false, Source: RoutinePrivilegesTableName},
	{Name: "routine_catalog", Type: sql_identifier, Default: nil, Nullable: false, Source: RoutinePrivilegesTableName},
	{Name: "routine_schema", Type: sql_identifier, Default: nil, Nullable: false, Source: RoutinePrivilegesTableName},
	{Name: "routine_name", Type: sql_identifier, Default: nil, Nullable: false, Source: RoutinePrivilegesTableName},
	{Name: "privilege_type", Type: character_data, Default: nil, Nullable: false, Source: RoutinePrivilegesTableName},
	{Name: "is_grantable", Type: yes_or_no, Default: nil, Nullable: false, Source: RoutinePrivilegesTableName},
}

type informationSchemaRelationKey struct {
	schema string
	name   string
}

func tablePrivilegesRowIter(ctx *sql.Context, _ sql.Catalog) (sql.RowIter, error) {
	rows := make([]sql.Row, 0)
	relationsBySchema, relationCatalogs, err := currentRelationCatalogs(ctx)
	if err != nil {
		return nil, err
	}
	for _, info := range auth.GetTablePrivilegeInfo() {
		if info.Column != "" || !privilegeInfoVisibleToCurrentUser(ctx, info.GranteeID, info.GrantorIDs...) {
			continue
		}
		if info.Table.Name == "" {
			for _, relationName := range relationsBySchema[info.Table.Schema] {
				catalogName := relationCatalogs[informationSchemaRelationKey{schema: info.Table.Schema, name: relationName}]
				rows = append(rows, tablePrivilegeRow(info, catalogName, info.Table.Schema, relationName))
			}
			continue
		}
		key := informationSchemaRelationKey{schema: info.Table.Schema, name: info.Table.Name}
		catalogName, ok := relationCatalogs[key]
		if !ok {
			continue
		}
		rows = append(rows, tablePrivilegeRow(info, catalogName, info.Table.Schema, info.Table.Name))
	}
	return sql.RowsToRowIter(rows...), nil
}

func columnPrivilegesRowIter(ctx *sql.Context, _ sql.Catalog) (sql.RowIter, error) {
	rows := make([]sql.Row, 0)
	_, relationCatalogs, err := currentRelationCatalogs(ctx)
	if err != nil {
		return nil, err
	}
	for _, info := range auth.GetTablePrivilegeInfo() {
		if info.Column == "" || !privilegeInfoVisibleToCurrentUser(ctx, info.GranteeID, info.GrantorIDs...) {
			continue
		}
		key := informationSchemaRelationKey{schema: info.Table.Schema, name: info.Table.Name}
		catalogName, ok := relationCatalogs[key]
		if !ok {
			continue
		}
		rows = append(rows, sql.Row{
			info.Grantee,            // grantee
			catalogName,             // table_catalog
			info.Table.Schema,       // table_schema
			info.Table.Name,         // table_name
			info.Column,             // column_name
			info.Privilege.String(), // privilege_type
			yesNo(info.IsGrantable), // is_grantable
		})
	}
	return sql.RowsToRowIter(rows...), nil
}

func routinePrivilegesRowIter(ctx *sql.Context, _ sql.Catalog) (sql.RowIter, error) {
	rows := make([]sql.Row, 0)
	catalogName := ctx.GetCurrentDatabase()
	for _, info := range auth.GetRoutinePrivilegeInfo() {
		if info.Name == "" || !privilegeInfoVisibleToCurrentUser(ctx, info.GranteeID, info.GrantorID) {
			continue
		}
		specificName := routineSpecificName(info)
		rows = append(rows, sql.Row{
			info.Grantor,            // grantor
			info.Grantee,            // grantee
			catalogName,             // specific_catalog
			info.Schema,             // specific_schema
			specificName,            // specific_name
			catalogName,             // routine_catalog
			info.Schema,             // routine_schema
			info.Name,               // routine_name
			info.Privilege.String(), // privilege_type
			yesNo(info.IsGrantable), // is_grantable
		})
	}
	return sql.RowsToRowIter(rows...), nil
}

func currentRelationCatalogs(ctx *sql.Context) (map[string][]string, map[informationSchemaRelationKey]string, error) {
	relationsBySchema := make(map[string][]string)
	relationCatalogs := make(map[informationSchemaRelationKey]string)
	err := functions.IterateCurrentDatabase(ctx, functions.Callbacks{
		Table: func(ctx *sql.Context, schema functions.ItemSchema, table functions.ItemTable) (cont bool, err error) {
			if isMaterializedViewTable(table.Item) {
				return true, nil
			}
			addInformationSchemaRelation(relationsBySchema, relationCatalogs, schema.Item.Name(), schema.Item.SchemaName(), table.Item.Name())
			return true, nil
		},
		View: func(ctx *sql.Context, schema functions.ItemSchema, view functions.ItemView) (cont bool, err error) {
			addInformationSchemaRelation(relationsBySchema, relationCatalogs, schema.Item.Name(), schema.Item.SchemaName(), view.Item.Name)
			return true, nil
		},
	})
	if err != nil {
		return nil, nil, err
	}
	for schema := range relationsBySchema {
		sort.Strings(relationsBySchema[schema])
	}
	return relationsBySchema, relationCatalogs, nil
}

func addInformationSchemaRelation(relationsBySchema map[string][]string, relationCatalogs map[informationSchemaRelationKey]string, catalogName string, schemaName string, relationName string) {
	key := informationSchemaRelationKey{schema: schemaName, name: relationName}
	if _, ok := relationCatalogs[key]; ok {
		return
	}
	relationCatalogs[key] = catalogName
	relationsBySchema[schemaName] = append(relationsBySchema[schemaName], relationName)
}

func tablePrivilegeRow(info auth.TablePrivilegeInfo, catalogName string, schemaName string, tableName string) sql.Row {
	return sql.Row{
		info.Grantee,            // grantee
		catalogName,             // table_catalog
		schemaName,              // table_schema
		tableName,               // table_name
		info.Privilege.String(), // privilege_type
		yesNo(info.IsGrantable), // is_grantable
	}
}

func privilegeInfoVisibleToCurrentUser(ctx *sql.Context, grantee auth.RoleID, grantors ...auth.RoleID) bool {
	user := ctx.Client().User
	if user == "" {
		return true
	}
	enabledRoleIDs, superuser := auth.GetEnabledRoleIDs(user)
	if superuser {
		return true
	}
	if _, ok := enabledRoleIDs[grantee]; ok {
		return true
	}
	for _, grantor := range grantors {
		if _, ok := enabledRoleIDs[grantor]; ok {
			return true
		}
	}
	return false
}

func routineSpecificName(info auth.RoutinePrivilegeInfo) string {
	if info.ArgTypes == "" {
		return info.Name
	}
	return info.Name + "(" + info.ArgTypes + ")"
}

func yesNo(value bool) string {
	if value {
		return "YES"
	}
	return "NO"
}
