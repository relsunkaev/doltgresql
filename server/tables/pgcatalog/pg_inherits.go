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

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/functions"
	"github.com/dolthub/doltgresql/server/tablemetadata"
	"github.com/dolthub/doltgresql/server/tables"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// PgInheritsName is a constant to the pg_inherits name.
const PgInheritsName = "pg_inherits"

// InitPgInherits handles registration of the pg_inherits handler.
func InitPgInherits() {
	tables.AddHandler(PgCatalogName, PgInheritsName, PgInheritsHandler{})
}

// PgInheritsHandler is the handler for the pg_inherits table.
type PgInheritsHandler struct{}

var _ tables.Handler = PgInheritsHandler{}

// Name implements the interface tables.Handler.
func (p PgInheritsHandler) Name() string {
	return PgInheritsName
}

// RowIter implements the interface tables.Handler.
func (p PgInheritsHandler) RowIter(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	var rows []sql.Row
	err := functions.IterateCurrentDatabase(ctx, functions.Callbacks{
		Table: func(ctx *sql.Context, schema functions.ItemSchema, table functions.ItemTable) (cont bool, err error) {
			parents := tablemetadata.Inherits(tableComment(table.Item))
			for idx, parent := range parents {
				parentSchema := parent.Schema
				if parentSchema == "" {
					parentSchema = schema.Item.SchemaName()
				}
				rows = append(rows, sql.Row{
					table.OID.AsId(),
					id.NewTable(parentSchema, parent.Name).AsId(),
					int32(idx + 1),
					false,
				})
			}
			return true, nil
		},
	})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return emptyRowIter()
	}
	return &pgInheritsRowIter{rows: rows}, nil
}

// Schema implements the interface tables.Handler.
func (p PgInheritsHandler) PkSchema() sql.PrimaryKeySchema {
	return sql.PrimaryKeySchema{
		Schema:     pgInheritsSchema,
		PkOrdinals: nil,
	}
}

// pgInheritsSchema is the schema for pg_inherits.
var pgInheritsSchema = sql.Schema{
	{Name: "inhrelid", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgInheritsName},
	{Name: "inhparent", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgInheritsName},
	{Name: "inhseqno", Type: pgtypes.Int32, Default: nil, Nullable: false, Source: PgInheritsName},
	{Name: "inhdetachpending", Type: pgtypes.Bool, Default: nil, Nullable: false, Source: PgInheritsName},
}

// pgInheritsRowIter is the sql.RowIter for the pg_inherits table.
type pgInheritsRowIter struct {
	rows []sql.Row
	idx  int
}

var _ sql.RowIter = (*pgInheritsRowIter)(nil)

// Next implements the interface sql.RowIter.
func (iter *pgInheritsRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	if iter.idx >= len(iter.rows) {
		return nil, io.EOF
	}
	row := iter.rows[iter.idx]
	iter.idx++
	return row, nil
}

// Close implements the interface sql.RowIter.
func (iter *pgInheritsRowIter) Close(ctx *sql.Context) error {
	return nil
}
