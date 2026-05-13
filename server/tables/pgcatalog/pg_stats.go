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

package pgcatalog

import (
	"strings"

	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dtables"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/server/functions"
	"github.com/dolthub/doltgresql/server/tables"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// PgStatsName is a constant to the pg_stats name.
const PgStatsName = "pg_stats"

// InitPgStats handles registration of the pg_stats handler.
func InitPgStats() {
	tables.AddHandler(PgCatalogName, PgStatsName, PgStatsHandler{})
}

// PgStatsHandler is the handler for the pg_stats table.
type PgStatsHandler struct{}

var _ tables.Handler = PgStatsHandler{}

// Name implements the interface tables.Handler.
func (p PgStatsHandler) Name() string {
	return PgStatsName
}

// RowIter implements the interface tables.Handler.
func (p PgStatsHandler) RowIter(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	doltSession := dsess.DSessFromSess(ctx.Session)
	statsProvider, ok := doltSession.StatsProvider().(dtables.BranchStatsProvider)
	if !ok {
		return emptyRowIter()
	}

	branch := pgStatsBranch(ctx, doltSession)
	dbName := strings.ToLower(ctx.GetCurrentDatabase())
	var rows []sql.Row
	err := functions.IterateCurrentDatabase(ctx, functions.Callbacks{
		Table: func(ctx *sql.Context, schema functions.ItemSchema, table functions.ItemTable) (cont bool, err error) {
			schemaName := schema.Item.SchemaName()
			tableName := table.Item.Name()
			tableStats, err := statsProvider.GetTableDoltStats(ctx, branch, dbName, strings.ToLower(schemaName), strings.ToLower(tableName))
			if err != nil || len(tableStats) == 0 {
				return err == nil, err
			}
			for _, col := range table.Item.Schema(ctx) {
				if col.HiddenSystem {
					continue
				}
				rows = append(rows, sql.Row{
					schemaName, // schemaname
					tableName,  // tablename
					col.Name,   // attname
					false,      // inherited
					float32(0), // null_frac
					int32(0),   // avg_width
					float32(0), // n_distinct
					nil,        // most_common_vals
					nil,        // most_common_freqs
					nil,        // histogram_bounds
					nil,        // correlation
					nil,        // most_common_elems
					nil,        // most_common_elem_freqs
					nil,        // elem_count_histogram
				})
			}
			return true, nil
		},
	})
	if err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(rows...), nil
}

// Schema implements the interface tables.Handler.
func (p PgStatsHandler) PkSchema() sql.PrimaryKeySchema {
	return sql.PrimaryKeySchema{
		Schema:     pgStatsSchema,
		PkOrdinals: nil,
	}
}

// pgStatsSchema is the schema for pg_stats.
var pgStatsSchema = sql.Schema{
	{Name: "schemaname", Type: pgtypes.Name, Default: nil, Nullable: true, Source: PgStatsName},
	{Name: "tablename", Type: pgtypes.Name, Default: nil, Nullable: true, Source: PgStatsName},
	{Name: "attname", Type: pgtypes.Name, Default: nil, Nullable: true, Source: PgStatsName},
	{Name: "inherited", Type: pgtypes.Bool, Default: nil, Nullable: true, Source: PgStatsName},
	{Name: "null_frac", Type: pgtypes.Float32, Default: nil, Nullable: true, Source: PgStatsName},
	{Name: "avg_width", Type: pgtypes.Int32, Default: nil, Nullable: true, Source: PgStatsName},
	{Name: "n_distinct", Type: pgtypes.Float32, Default: nil, Nullable: true, Source: PgStatsName},
	{Name: "most_common_vals", Type: pgtypes.AnyArray, Default: nil, Nullable: true, Source: PgStatsName},
	{Name: "most_common_freqs", Type: pgtypes.Float32Array, Default: nil, Nullable: true, Source: PgStatsName},
	{Name: "histogram_bounds", Type: pgtypes.AnyArray, Default: nil, Nullable: true, Source: PgStatsName},
	{Name: "correlation", Type: pgtypes.Float32, Default: nil, Nullable: true, Source: PgStatsName},
	{Name: "most_common_elems", Type: pgtypes.AnyArray, Default: nil, Nullable: true, Source: PgStatsName},
	{Name: "most_common_elem_freqs", Type: pgtypes.Float32Array, Default: nil, Nullable: true, Source: PgStatsName},
	{Name: "elem_count_histogram", Type: pgtypes.Float32Array, Default: nil, Nullable: true, Source: PgStatsName},
}

func pgStatsBranch(ctx *sql.Context, doltSession *dsess.DoltSession) string {
	if doltSession == nil {
		return env.DefaultInitBranch
	}
	branch, err := doltSession.GetBranch(ctx)
	if err != nil || branch == "" {
		return env.DefaultInitBranch
	}
	return strings.ToLower(branch)
}
