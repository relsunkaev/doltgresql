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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/server/functions"
)

type pgIndexStatsScope int

const (
	pgIndexStatsScopeAll pgIndexStatsScope = iota
	pgIndexStatsScopeUser
	pgIndexStatsScopeSystem
)

func pgStatIndexRows(ctx *sql.Context, scope pgIndexStatsScope) (sql.RowIter, error) {
	var rows []sql.Row
	err := functions.IterateCurrentDatabase(ctx, functions.Callbacks{
		Index: func(ctx *sql.Context, schema functions.ItemSchema, table functions.ItemTable, index functions.ItemIndex) (cont bool, err error) {
			if !includeIndexStatsSchema(schema.Item.SchemaName(), scope) {
				return true, nil
			}
			rows = append(rows, sql.Row{
				table.OID.AsId(),            // relid
				index.OID.AsId(),            // indexrelid
				schema.Item.SchemaName(),    // schemaname
				table.Item.Name(),           // relname
				formatIndexName(index.Item), // indexrelname
				int64(0),                    // idx_scan
				nil,                         // last_idx_scan
				int64(0),                    // idx_tup_read
				int64(0),                    // idx_tup_fetch
			})
			return true, nil
		},
	})
	if err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(rows...), nil
}

func pgStatioIndexRows(ctx *sql.Context, scope pgIndexStatsScope) (sql.RowIter, error) {
	var rows []sql.Row
	err := functions.IterateCurrentDatabase(ctx, functions.Callbacks{
		Index: func(ctx *sql.Context, schema functions.ItemSchema, table functions.ItemTable, index functions.ItemIndex) (cont bool, err error) {
			if !includeIndexStatsSchema(schema.Item.SchemaName(), scope) {
				return true, nil
			}
			rows = append(rows, sql.Row{
				table.OID.AsId(),            // relid
				index.OID.AsId(),            // indexrelid
				schema.Item.SchemaName(),    // schemaname
				table.Item.Name(),           // relname
				formatIndexName(index.Item), // indexrelname
				int64(0),                    // idx_blks_read
				int64(0),                    // idx_blks_hit
			})
			return true, nil
		},
	})
	if err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(rows...), nil
}

func includeIndexStatsSchema(schema string, scope pgIndexStatsScope) bool {
	isSystem := isIndexStatsSystemSchema(schema)
	switch scope {
	case pgIndexStatsScopeAll:
		return true
	case pgIndexStatsScopeUser:
		return !isSystem
	case pgIndexStatsScopeSystem:
		return isSystem
	default:
		return false
	}
}

func isIndexStatsSystemSchema(schema string) bool {
	switch strings.ToLower(schema) {
	case "pg_catalog", "information_schema":
		return true
	default:
		return false
	}
}
