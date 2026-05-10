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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/information_schema"

	"github.com/dolthub/doltgresql/server/deferrable"
	"github.com/dolthub/doltgresql/server/functions"
	"github.com/dolthub/doltgresql/server/indexmetadata"
)

// TableConstraintsViewName is the name of the TABLE_CONSTRAINTS view.
const TableConstraintsViewName = "table_constraints"

// newTableConstraintsView creates a doltgres override for
// information_schema.TABLE_CONSTRAINTS that uses PostgreSQL-style
// constraint names (e.g. <table>_pkey for primary keys) and emits PK,
// UNIQUE, FOREIGN KEY, and CHECK rows. The default GMS implementation
// reports PK constraint names as the literal "PRIMARY" (MySQL
// convention), which breaks ORM introspection paths that join
// table_constraints to constraint_column_usage by constraint_name.
func newTableConstraintsView() *information_schema.InformationSchemaTable {
	return &information_schema.InformationSchemaTable{
		TableName:   TableConstraintsViewName,
		TableSchema: tableConstraintsSchema,
		Reader:      tableConstraintsRowIter,
	}
}

// tableConstraintsSchema is the schema for information_schema.TABLE_CONSTRAINTS.
// Columns match the PostgreSQL-documented set; ENFORCED is included for
// compatibility with the GMS shape so SELECT * shows the same column count.
var tableConstraintsSchema = sql.Schema{
	{Name: "constraint_catalog", Type: sql_identifier, Default: nil, Nullable: true, Source: TableConstraintsViewName},
	{Name: "constraint_schema", Type: sql_identifier, Default: nil, Nullable: true, Source: TableConstraintsViewName},
	{Name: "constraint_name", Type: sql_identifier, Default: nil, Nullable: true, Source: TableConstraintsViewName},
	{Name: "table_catalog", Type: sql_identifier, Default: nil, Nullable: true, Source: TableConstraintsViewName},
	{Name: "table_schema", Type: sql_identifier, Default: nil, Nullable: true, Source: TableConstraintsViewName},
	{Name: "table_name", Type: sql_identifier, Default: nil, Nullable: true, Source: TableConstraintsViewName},
	{Name: "constraint_type", Type: character_data, Default: nil, Nullable: false, Source: TableConstraintsViewName},
	{Name: "is_deferrable", Type: yes_or_no, Default: nil, Nullable: false, Source: TableConstraintsViewName},
	{Name: "initially_deferred", Type: yes_or_no, Default: nil, Nullable: false, Source: TableConstraintsViewName},
	{Name: "enforced", Type: yes_or_no, Default: nil, Nullable: false, Source: TableConstraintsViewName},
}

func tableConstraintsRowIter(ctx *sql.Context, _ sql.Catalog) (sql.RowIter, error) {
	var rows []sql.Row

	err := functions.IterateCurrentDatabase(ctx, functions.Callbacks{
		Index: func(ctx *sql.Context, schema functions.ItemSchema, table functions.ItemTable, index functions.ItemIndex) (cont bool, err error) {
			constraintType := ""
			switch {
			case index.Item.ID() == "PRIMARY":
				constraintType = "PRIMARY KEY"
			case index.Item.IsUnique() && !indexmetadata.IsStandaloneIndex(index.Item.Comment()):
				constraintType = "UNIQUE"
			default:
				return true, nil
			}
			rows = append(rows, sql.Row{
				schema.Item.Name(),       // constraint_catalog
				schema.Item.SchemaName(), // constraint_schema
				indexmetadata.DisplayNameForTable(index.Item, table.Item), // constraint_name
				schema.Item.Name(),       // table_catalog
				schema.Item.SchemaName(), // table_schema
				table.Item.Name(),        // table_name
				constraintType,           // constraint_type
				"NO",                     // is_deferrable
				"NO",                     // initially_deferred
				"YES",                    // enforced
			})
			return true, nil
		},
		ForeignKey: func(ctx *sql.Context, schema functions.ItemSchema, table functions.ItemTable, fk functions.ItemForeignKey) (cont bool, err error) {
			timing, err := deferrable.ForeignKeyTimingForID(ctx, fk.OID, fk.Item)
			if err != nil {
				return false, err
			}
			isDeferrable := "NO"
			if timing.Deferrable {
				isDeferrable = "YES"
			}
			initiallyDeferred := "NO"
			if timing.InitiallyDeferred {
				initiallyDeferred = "YES"
			}
			rows = append(rows, sql.Row{
				schema.Item.Name(),       // constraint_catalog
				schema.Item.SchemaName(), // constraint_schema
				fk.Item.Name,             // constraint_name
				schema.Item.Name(),       // table_catalog
				schema.Item.SchemaName(), // table_schema
				table.Item.Name(),        // table_name
				"FOREIGN KEY",            // constraint_type
				isDeferrable,             // is_deferrable
				initiallyDeferred,        // initially_deferred
				"YES",                    // enforced
			})
			return true, nil
		},
		Check: func(ctx *sql.Context, schema functions.ItemSchema, table functions.ItemTable, check functions.ItemCheck) (cont bool, err error) {
			enforced := "YES"
			if !check.Item.Enforced {
				enforced = "NO"
			}
			rows = append(rows, sql.Row{
				schema.Item.Name(),       // constraint_catalog
				schema.Item.SchemaName(), // constraint_schema
				check.Item.Name,          // constraint_name
				schema.Item.Name(),       // table_catalog
				schema.Item.SchemaName(), // table_schema
				table.Item.Name(),        // table_name
				"CHECK",                  // constraint_type
				"NO",                     // is_deferrable
				"NO",                     // initially_deferred
				enforced,                 // enforced
			})
			return true, nil
		},
	})
	if err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(rows...), nil
}
