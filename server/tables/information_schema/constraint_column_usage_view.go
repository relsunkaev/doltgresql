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

	"github.com/dolthub/doltgresql/server/functions"
	"github.com/dolthub/doltgresql/server/indexmetadata"
)

// ConstraintColumnUsageViewName is the name of the CONSTRAINT_COLUMN_USAGE view.
const ConstraintColumnUsageViewName = "constraint_column_usage"

// newConstraintColumnUsageView creates a new information_schema.CONSTRAINT_COLUMN_USAGE view.
func newConstraintColumnUsageView() *information_schema.InformationSchemaTable {
	return &information_schema.InformationSchemaTable{
		TableName:   ConstraintColumnUsageViewName,
		TableSchema: constraintColumnUsageSchema,
		Reader:      constraintColumnUsageRowIter,
	}
}

// constraintColumnUsage is the schema for the information_schema.CONSTRAINT_COLUMN_USAGE view.
var constraintColumnUsageSchema = sql.Schema{
	{Name: "table_catalog", Type: sql_identifier, Default: nil, Nullable: true, Source: ConstraintColumnUsageViewName},
	{Name: "table_schema", Type: sql_identifier, Default: nil, Nullable: true, Source: ConstraintColumnUsageViewName},
	{Name: "table_name", Type: sql_identifier, Default: nil, Nullable: true, Source: ConstraintColumnUsageViewName},
	{Name: "column_name", Type: character_data, Default: nil, Nullable: true, Source: ConstraintColumnUsageViewName},
	{Name: "constraint_catalog", Type: character_data, Default: nil, Nullable: true, Source: ConstraintColumnUsageViewName},
	{Name: "constraint_schema", Type: yes_or_no, Default: nil, Nullable: true, Source: ConstraintColumnUsageViewName},
	{Name: "constraint_name", Type: yes_or_no, Default: nil, Nullable: true, Source: ConstraintColumnUsageViewName},
}

// constraintColumnUsageRowIter implements the sql.RowIter for the information_schema.CONSTRAINT_COLUMN_USAGE view.
//
// Per the PostgreSQL spec, this view emits one row per constraint per
// constrained column for primary-key, unique, foreign-key, and check
// constraints. ORM introspection paths (drizzle-kit, Prisma db pull,
// Alembic autogenerate) join this view to information_schema.table_constraints
// to discover which columns participate in each constraint.
func constraintColumnUsageRowIter(ctx *sql.Context, catalog sql.Catalog) (sql.RowIter, error) {
	var rows []sql.Row

	err := functions.IterateCurrentDatabase(ctx, functions.Callbacks{
		Index: func(ctx *sql.Context, schema functions.ItemSchema, table functions.ItemTable, index functions.ItemIndex) (cont bool, err error) {
			isPK := index.Item.ID() == "PRIMARY"
			isUnique := index.Item.IsUnique() && !indexmetadata.IsStandaloneIndex(index.Item.Comment())
			if !isPK && !isUnique {
				return true, nil
			}
			constraintName := indexmetadata.DisplayNameForTable(index.Item, table.Item)
			for _, expr := range index.Item.Expressions() {
				rows = append(rows, sql.Row{
					schema.Item.Name(),       // table_catalog
					schema.Item.SchemaName(), // table_schema
					table.Item.Name(),        // table_name
					exprColumnName(expr),     // column_name
					schema.Item.Name(),       // constraint_catalog
					schema.Item.SchemaName(), // constraint_schema
					constraintName,           // constraint_name
				})
			}
			return true, nil
		},
		ForeignKey: func(ctx *sql.Context, schema functions.ItemSchema, table functions.ItemTable, fk functions.ItemForeignKey) (cont bool, err error) {
			// constraint_column_usage names the *referenced* (parent)
			// columns for FKs — the columns the constraint targets,
			// not the constrained columns. ORM introspection joins
			// this against table_constraints by constraint_name to
			// resolve the referenced side.
			parentTable, ok, err := schema.Item.GetTableInsensitive(ctx, fk.Item.ParentTable)
			if err != nil {
				return false, err
			}
			parentTableName := fk.Item.ParentTable
			if ok {
				parentTableName = parentTable.Name()
			}
			for _, col := range fk.Item.ParentColumns {
				rows = append(rows, sql.Row{
					schema.Item.Name(),       // table_catalog
					schema.Item.SchemaName(), // table_schema (of referenced table)
					parentTableName,          // table_name (referenced table)
					col,                      // column_name (referenced column)
					schema.Item.Name(),       // constraint_catalog
					schema.Item.SchemaName(), // constraint_schema
					fk.Item.Name,             // constraint_name
				})
			}
			return true, nil
		},
		Check: func(ctx *sql.Context, schema functions.ItemSchema, table functions.ItemTable, check functions.ItemCheck) (cont bool, err error) {
			rows = append(rows, sql.Row{
				schema.Item.Name(),       // table_catalog
				schema.Item.SchemaName(), // table_schema
				table.Item.Name(),        // table_name
				nil,                      // column_name (TODO: parse check expression)
				schema.Item.Name(),       // constraint_catalog
				schema.Item.SchemaName(), // constraint_schema
				check.Item.Name,          // constraint_name
			})
			return true, nil
		},
	})
	if err != nil {
		return nil, err
	}

	return sql.RowsToRowIter(rows...), nil
}

// exprColumnName extracts the bare column name from an index expression
// like "tablename.columnname". Returns the input unchanged if no dot
// separator is present (e.g. expression indexes that emit raw text).
func exprColumnName(expr string) string {
	for i := len(expr) - 1; i >= 0; i-- {
		if expr[i] == '.' {
			return expr[i+1:]
		}
	}
	return expr
}
