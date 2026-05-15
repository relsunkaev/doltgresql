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
	"strconv"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/functions"
	"github.com/dolthub/doltgresql/server/tablemetadata"
	"github.com/dolthub/doltgresql/server/tables"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// PgStatisticExtName is a constant to the pg_statistic_ext name.
const PgStatisticExtName = "pg_statistic_ext"

// InitPgStatisticExt handles registration of the pg_statistic_ext handler.
func InitPgStatisticExt() {
	tables.AddHandler(PgCatalogName, PgStatisticExtName, PgStatisticExtHandler{})
}

// PgStatisticExtHandler is the handler for the pg_statistic_ext table.
type PgStatisticExtHandler struct{}

var _ tables.Handler = PgStatisticExtHandler{}

// Name implements the interface tables.Handler.
func (p PgStatisticExtHandler) Name() string {
	return PgStatisticExtName
}

// RowIter implements the interface tables.Handler.
func (p PgStatisticExtHandler) RowIter(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	var rows []sql.Row
	err := functions.IterateCurrentDatabase(ctx, functions.Callbacks{
		Table: func(ctx *sql.Context, schema functions.ItemSchema, table functions.ItemTable) (cont bool, err error) {
			schemaName := schema.Item.SchemaName()
			comment := tableComment(table.Item)
			for _, statistic := range tablemetadata.ExtendedStatistics(comment) {
				stxKeys, ok := statisticKeyText(ctx, table.Item, statistic.Columns)
				if !ok {
					continue
				}
				rows = append(rows, sql.Row{
					id.NewId(id.Section_OID, PgStatisticExtName, schemaName, statistic.Name), // oid
					table.OID.AsId(),                      // stxrelid
					statistic.Name,                        // stxname
					id.NewNamespace(schemaName).AsId(),    // stxnamespace
					id.NewId(id.Section_User, "postgres"), // stxowner
					int32(-1),                             // stxstattarget
					stxKeys,                               // stxkeys
					statisticKindArray(statistic.Kinds),   // stxkind
					nil,                                   // stxexprs
					id.NewTable(PgCatalogName, PgStatisticExtName).AsId(), // tableoid
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
func (p PgStatisticExtHandler) PkSchema() sql.PrimaryKeySchema {
	return sql.PrimaryKeySchema{
		Schema:     pgStatisticExtSchema,
		PkOrdinals: nil,
	}
}

// pgStatisticExtSchema is the schema for pg_statistic_ext.
var pgStatisticExtSchema = sql.Schema{
	{Name: "oid", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgStatisticExtName},
	{Name: "stxrelid", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgStatisticExtName},
	{Name: "stxname", Type: pgtypes.Name, Default: nil, Nullable: false, Source: PgStatisticExtName},
	{Name: "stxnamespace", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgStatisticExtName},
	{Name: "stxowner", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgStatisticExtName},
	{Name: "stxstattarget", Type: pgtypes.Int32, Default: nil, Nullable: false, Source: PgStatisticExtName},
	{Name: "stxkeys", Type: pgtypes.Text, Default: nil, Nullable: false, Source: PgStatisticExtName}, // TODO: int2vector type
	{Name: "stxkind", Type: pgtypes.InternalCharArray, Default: nil, Nullable: false, Source: PgStatisticExtName},
	{Name: "stxexprs", Type: pgtypes.Text, Default: nil, Nullable: true, Source: PgStatisticExtName}, // TODO: collation C, pg_node_tree type
	{Name: "tableoid", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgStatisticExtName, Hidden: true},
}

func statisticKeyText(ctx *sql.Context, table sql.Table, columns []string) (string, bool) {
	columnIndexes := make(map[string]int, len(table.Schema(ctx)))
	for i, column := range table.Schema(ctx) {
		if column.HiddenSystem {
			continue
		}
		columnIndexes[column.Name] = i + 1
	}
	keys := make([]string, 0, len(columns))
	for _, column := range columns {
		idx, ok := columnIndexes[column]
		if !ok {
			return "", false
		}
		keys = append(keys, strconv.Itoa(idx))
	}
	return strings.Join(keys, " "), true
}

func statisticKindArray(kinds []string) []any {
	ret := make([]any, 0, len(kinds))
	for _, kind := range kinds {
		ret = append(ret, kind)
	}
	return ret
}
