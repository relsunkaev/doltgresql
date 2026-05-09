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
	"io"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/functions"
	"github.com/dolthub/doltgresql/server/tables"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// PgAttrdefName is a constant to the pg_attrdef name.
const PgAttrdefName = "pg_attrdef"

// InitPgAttrdef handles registration of the pg_attrdef handler.
func InitPgAttrdef() {
	tables.AddHandler(PgCatalogName, PgAttrdefName, PgAttrdefHandler{})
}

// PgAttrdefHandler is the handler for the pg_attrdef table.
type PgAttrdefHandler struct{}

var _ tables.Handler = PgAttrdefHandler{}

// Name implements the interface tables.Handler.
func (p PgAttrdefHandler) Name() string {
	return PgAttrdefName
}

// RowIter implements the interface tables.Handler.
func (p PgAttrdefHandler) RowIter(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	// Use cached data from this process if it exists
	pgCatalogCache, err := getPgCatalogCache(ctx)
	if err != nil {
		return nil, err
	}

	if pgCatalogCache.attrdefCols == nil {
		var attrdefCols []functions.ItemColumnDefault
		var attrdefTableOIDs []id.Id
		err := functions.IterateCurrentDatabase(ctx, functions.Callbacks{
			ColumnDefault: func(ctx *sql.Context, _ functions.ItemSchema, table functions.ItemTable, col functions.ItemColumnDefault) (cont bool, err error) {
				attrdefCols = append(attrdefCols, col)
				attrdefTableOIDs = append(attrdefTableOIDs, table.OID.AsId())
				return true, nil
			},
		})
		if err != nil {
			return nil, err
		}
		pgCatalogCache.attrdefCols = attrdefCols
		pgCatalogCache.attrdefTableOIDs = attrdefTableOIDs
	}

	return &pgAttrdefRowIter{
		cols:      pgCatalogCache.attrdefCols,
		tableOIDs: pgCatalogCache.attrdefTableOIDs,
		idx:       0,
	}, nil
}

// Schema implements the interface tables.Handler.
func (p PgAttrdefHandler) PkSchema() sql.PrimaryKeySchema {
	return sql.PrimaryKeySchema{
		Schema:     pgAttrdefSchema,
		PkOrdinals: nil,
	}
}

// pgAttrdefSchema is the schema for pg_attrdef.
var pgAttrdefSchema = sql.Schema{
	{Name: "oid", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgAttrdefName},
	{Name: "adrelid", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgAttrdefName},
	{Name: "adnum", Type: pgtypes.Int16, Default: nil, Nullable: false, Source: PgAttrdefName},
	{Name: "adbin", Type: pgtypes.Text, Default: nil, Nullable: false, Source: PgAttrdefName}, // TODO: collation C, type pg_node_tree
	{Name: "tableoid", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgAttrdefName},
}

// pgAttrdefRowIter is the sql.RowIter for the pg_attrdef table.
type pgAttrdefRowIter struct {
	cols      []functions.ItemColumnDefault
	tableOIDs []id.Id
	idx       int
}

var _ sql.RowIter = (*pgAttrdefRowIter)(nil)

// Next implements the interface sql.RowIter.
func (iter *pgAttrdefRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	if iter.idx >= len(iter.cols) {
		return nil, io.EOF
	}
	iter.idx++
	col := iter.cols[iter.idx-1]
	tableOid := iter.tableOIDs[iter.idx-1]

	return sql.Row{
		col.OID.AsId(),                  // oid
		tableOid,                        // adrelid
		int16(col.Item.ColumnIndex + 1), // adnum
		columnDefaultText(col.Item.Column.Default),       // adbin
		id.NewTable(PgCatalogName, PgAttrdefName).AsId(), // tableoid
	}, nil
}

// Close implements the interface sql.RowIter.
func (iter *pgAttrdefRowIter) Close(ctx *sql.Context) error {
	return nil
}

func columnDefaultText(def *sql.ColumnDefaultValue) string {
	if def == nil {
		return ""
	}
	if def.Expr != nil {
		return stripRedundantOuterParens(def.Expr.String())
	}
	return def.String()
}

func stripRedundantOuterParens(expr string) string {
	for {
		trimmed := strings.TrimSpace(expr)
		if len(trimmed) < 2 || trimmed[0] != '(' || trimmed[len(trimmed)-1] != ')' {
			return trimmed
		}
		depth := 0
		wrapsWholeExpr := true
		for i := 0; i < len(trimmed); i++ {
			switch trimmed[i] {
			case '(':
				depth++
			case ')':
				depth--
				if depth == 0 && i < len(trimmed)-1 {
					wrapsWholeExpr = false
				}
			}
			if depth < 0 {
				return trimmed
			}
		}
		if depth != 0 || !wrapsWholeExpr {
			return trimmed
		}
		expr = trimmed[1 : len(trimmed)-1]
	}
}
