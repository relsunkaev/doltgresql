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

package pgcatalog

import (
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/functions"
)

func pgStatTableRows(ctx *sql.Context, includeUserTables bool, includeSystemTables bool) ([]sql.Row, error) {
	rows := make([]sql.Row, 0)
	if includeUserTables {
		err := functions.IterateCurrentDatabase(ctx, functions.Callbacks{
			Table: func(ctx *sql.Context, schema functions.ItemSchema, table functions.ItemTable) (cont bool, err error) {
				schemaName := schema.Item.SchemaName()
				if schemaName == PgCatalogName || schemaName == sql.InformationSchemaDatabaseName || isMaterializedViewTable(table.Item) {
					return true, nil
				}
				rows = append(rows, pgStatTableRow(id.NewTable(schemaName, table.Item.Name()).AsId(), schemaName, table.Item.Name()))
				return true, nil
			},
		})
		if err != nil {
			return nil, err
		}
	}
	if includeSystemTables {
		rows = append(rows, pgStatTableRow(id.NewTable(PgCatalogName, PgClassName).AsId(), PgCatalogName, PgClassName))
	}
	return rows, nil
}

func pgStatXactTableRows(ctx *sql.Context, includeUserTables bool, includeSystemTables bool) ([]sql.Row, error) {
	rows := make([]sql.Row, 0)
	if includeUserTables {
		err := functions.IterateCurrentDatabase(ctx, functions.Callbacks{
			Table: func(ctx *sql.Context, schema functions.ItemSchema, table functions.ItemTable) (cont bool, err error) {
				schemaName := schema.Item.SchemaName()
				if schemaName == PgCatalogName || schemaName == sql.InformationSchemaDatabaseName || isMaterializedViewTable(table.Item) {
					return true, nil
				}
				rows = append(rows, pgStatXactTableRow(id.NewTable(schemaName, table.Item.Name()).AsId(), schemaName, table.Item.Name()))
				return true, nil
			},
		})
		if err != nil {
			return nil, err
		}
	}
	if includeSystemTables {
		rows = append(rows, pgStatXactTableRow(id.NewTable(PgCatalogName, PgClassName).AsId(), PgCatalogName, PgClassName))
	}
	return rows, nil
}

func pgStatioTableRows(ctx *sql.Context, includeUserTables bool, includeSystemTables bool) ([]sql.Row, error) {
	rows := make([]sql.Row, 0)
	if includeUserTables {
		err := functions.IterateCurrentDatabase(ctx, functions.Callbacks{
			Table: func(ctx *sql.Context, schema functions.ItemSchema, table functions.ItemTable) (cont bool, err error) {
				schemaName := schema.Item.SchemaName()
				if schemaName == PgCatalogName || schemaName == sql.InformationSchemaDatabaseName || isMaterializedViewTable(table.Item) {
					return true, nil
				}
				rows = append(rows, pgStatioTableRow(id.NewTable(schemaName, table.Item.Name()).AsId(), schemaName, table.Item.Name()))
				return true, nil
			},
		})
		if err != nil {
			return nil, err
		}
	}
	if includeSystemTables {
		rows = append(rows, pgStatioTableRow(id.NewTable(PgCatalogName, PgClassName).AsId(), PgCatalogName, PgClassName))
	}
	return rows, nil
}

func pgStatTableRow(relID id.Id, schemaName string, tableName string) sql.Row {
	return sql.Row{
		relID,      // relid
		schemaName, // schemaname
		tableName,  // relname
		int64(0),   // seq_scan
		nil,        // last_seq_scan
		int64(0),   // seq_tup_read
		int64(0),   // idx_scan
		nil,        // last_idx_scan
		int64(0),   // idx_tup_fetch
		int64(0),   // n_tup_ins
		int64(0),   // n_tup_upd
		int64(0),   // n_tup_del
		int64(0),   // n_tup_hot_upd
		int64(0),   // n_tup_newpage_upd
		int64(0),   // n_live_tup
		int64(0),   // n_dead_tup
		int64(0),   // n_mod_since_analyze
		int64(0),   // n_ins_since_vacuum
		nil,        // last_vacuum
		nil,        // last_autovacuum
		nil,        // last_analyze
		nil,        // last_autoanalyze
		int64(0),   // vacuum_count
		int64(0),   // autovacuum_count
		int64(0),   // analyze_count
		int64(0),   // autoanalyze_count
	}
}

func pgStatXactTableRow(relID id.Id, schemaName string, tableName string) sql.Row {
	return sql.Row{
		relID,      // relid
		schemaName, // schemaname
		tableName,  // relname
		int64(0),   // seq_scan
		int64(0),   // seq_tup_read
		int64(0),   // idx_scan
		int64(0),   // idx_tup_fetch
		int64(0),   // n_tup_ins
		int64(0),   // n_tup_upd
		int64(0),   // n_tup_del
		int64(0),   // n_tup_hot_upd
		int64(0),   // n_tup_newpage_upd
	}
}

func pgStatioTableRow(relID id.Id, schemaName string, tableName string) sql.Row {
	return sql.Row{
		relID,      // relid
		schemaName, // schemaname
		tableName,  // relname
		int64(0),   // heap_blks_read
		int64(0),   // heap_blks_hit
		int64(0),   // idx_blks_read
		int64(0),   // idx_blks_hit
		int64(0),   // toast_blks_read
		int64(0),   // toast_blks_hit
		int64(0),   // tidx_blks_read
		int64(0),   // tidx_blks_hit
	}
}
